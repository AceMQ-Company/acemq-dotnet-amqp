// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>
/// A connection to a broker, and the entry point to everything else.
/// </summary>
/// <remarks>
/// <para>
/// The Java library calls this type <c>AceMq</c>. It cannot be called that here:
/// the namespace is <c>AceMq.Amqp</c>, and a type named <c>AceMq</c> inside it makes
/// every reference to the namespace ambiguous. The name differs because the CLR
/// requires it to, not because the concept did.
/// </para>
/// <para>
/// One instance per application. It owns the connection, so creating one per
/// message turns a cheap publish into a TCP handshake and a broker that runs out of
/// file descriptors under load.
/// </para>
/// </remarks>
public sealed class AceMqConnection : IDisposable
{
    private readonly ITransportConnection _connection;
    private readonly ConnectionConfig _config;
    private readonly ICodec _codec;
    private readonly SemaphoreSlim _inFlight;
    private readonly List<IDisposable> _owned = new List<IDisposable>();
    private readonly List<IHealthContributor> _health = new List<IHealthContributor>();
    private volatile TaskCompletionSource<bool>? _consumingPaused;
    private volatile bool _publishingPaused;
    private bool _disposed;

    private AceMqConnection(
        ITransportConnection connection, ConnectionConfig config, ICodec codec, ITransport transport)
    {
        _connection = connection;
        _config = config;
        _codec = codec;
        _inFlight = new SemaphoreSlim(config.MaxOutstandingPublishes);
        TransportName = transport.Name;
        Capabilities = transport.Capabilities;
    }

    /// <summary>Connects using the scheme in the URL to choose a transport.</summary>
    public static Task<AceMqConnection> ConnectAsync(string url) =>
        ConnectAsync(ConnectionConfig.ForUrl(url).Build(), new JsonCodec(), CancellationToken.None);

    /// <summary>Connects with a codec other than JSON.</summary>
    public static Task<AceMqConnection> ConnectAsync(string url, ICodec codec) =>
        ConnectAsync(ConnectionConfig.ForUrl(url).Build(), codec, CancellationToken.None);

    public static Task<AceMqConnection> ConnectAsync(ConnectionConfig config) =>
        ConnectAsync(config, new JsonCodec(), CancellationToken.None);

    public static async Task<AceMqConnection> ConnectAsync(
        ConnectionConfig config, ICodec codec, CancellationToken cancellationToken)
    {
        if (config == null) throw new ArgumentNullException(nameof(config));
        if (codec == null) throw new ArgumentNullException(nameof(codec));

        var transport = Transports.ForScheme(config.Scheme);
        var connection = await transport.ConnectAsync(config, cancellationToken).ConfigureAwait(false);
        return new AceMqConnection(connection, config, codec, transport);
    }

    /// <summary>Name of the transport underneath, for logs.</summary>
    public string TransportName { get; }

    /// <summary>What this broker can do.</summary>
    public IReadOnlyCollection<Capability> Capabilities { get; }

    public bool Supports(Capability capability)
    {
        foreach (var c in Capabilities) if (c == capability) return true;
        return false;
    }

    public bool IsOpen => !_disposed && _connection.IsOpen;

    /// <summary>Whether the broker has stopped accepting publishes, normally for resource alarms.</summary>
    public bool IsBlocked => _connection.IsBlocked;

    public string? BlockedReason => _connection.BlockedReason;

    public async Task<AceMqConnection> DeclareExchangeAsync(string name, string type)
    {
        await _connection.DeclareExchangeAsync(name, type, true, CancellationToken.None)
            .ConfigureAwait(false);
        return this;
    }

    public Task<AceMqConnection> DeclareQueueAsync(string name) =>
        DeclareQueueAsync(name, QueueType.Classic, null);

    public async Task<AceMqConnection> DeclareQueueAsync(
        string name, QueueType type, IReadOnlyDictionary<string, object>? arguments)
    {
        await _connection.DeclareQueueAsync(name, type, true, arguments, CancellationToken.None)
            .ConfigureAwait(false);
        return this;
    }

    public async Task<AceMqConnection> BindAsync(string queue, string exchange, string routingKey)
    {
        await _connection.BindQueueAsync(queue, exchange, routingKey, CancellationToken.None)
            .ConfigureAwait(false);
        return this;
    }

    public Task<long> MessageCountAsync(string queue) =>
        _connection.MessageCountAsync(queue, CancellationToken.None);

    public Task DeleteQueueAsync(string name) =>
        _connection.DeleteQueueAsync(name, CancellationToken.None);

    public Task<bool> QueueExistsAsync(string name) =>
        _connection.QueueExistsAsync(name, CancellationToken.None);

    /// <summary>A publisher for one exchange and routing key.</summary>
    public IPublisher<T> Publisher<T>(string exchange, string routingKey) =>
        Publisher<T>(exchange, routingKey, PublishOptions.Defaults());

    public IPublisher<T> Publisher<T>(string exchange, string routingKey, PublishOptions options) =>
        Publisher<T>(exchange, routingKey, options, null);

    /// <summary>
    /// A publisher whose messages name a queue to reply on.
    /// </summary>
    /// <remarks>
    /// Used by <see cref="RequesterAsync"/>. A responder answers to whatever a
    /// request names here, so a request published without one cannot be answered.
    /// </remarks>
    public IPublisher<T> Publisher<T>(
        string exchange, string routingKey, PublishOptions options, string? replyTo)
    {
        EnsureOpen();
        var publisher = new Publisher<T>(
            _connection, _codec, exchange, routingKey, options, _inFlight,
            _config.ConfirmTimeout, replyTo, () => _publishingPaused);
        lock (_owned) _owned.Add(publisher);
        return publisher;
    }

    /// <summary>Starts consuming a queue.</summary>
    public Task<IMessageConsumer> ConsumeAsync<T>(string queue, Func<IMessage<T>, Task<Ack>> handler) =>
        ConsumeAsync(queue, ConsumerOptions.Defaults(), handler);

    public Task<IMessageConsumer> ConsumeAsync<T>(
        string queue, ConsumerOptions options, Func<IMessage<T>, Task<Ack>> handler) =>
        ConsumeCoreAsync(queue, options, null, handler);

    /// <summary>Consumes a stream queue from a chosen offset.</summary>
    internal Task<IMessageConsumer> ConsumeStreamAsync<T>(
        string queue, ConsumerOptions options, StreamOffset offset,
        Func<IMessage<T>, Task<Ack>> handler) =>
        // x-stream-offset is a consumer argument rather than a queue argument: two
        // readers of the same stream sit at different offsets, so it cannot belong
        // to the queue.
        ConsumeCoreAsync(
            queue, options,
            new Dictionary<string, object> { ["x-stream-offset"] = offset.Value },
            handler);

    private async Task<IMessageConsumer> ConsumeCoreAsync<T>(
        string queue, ConsumerOptions options,
        IReadOnlyDictionary<string, object>? consumerArguments,
        Func<IMessage<T>, Task<Ack>> handler)
    {
        EnsureOpen();
        if (handler == null) throw new ArgumentNullException(nameof(handler));
        var codec = options.Codec ?? _codec;

        // Attempts are counted here rather than read off the wire.
        //
        // A broker redelivers the same bytes: RabbitMQ's basic.nack with requeue
        // puts the original message back, envelope and all, so the attempt header a
        // publisher wrote never advances no matter how many times a handler retries.
        // Reading it back would make "give up after five attempts" a loop that never
        // ends. Counting redeliveries in the consumer makes the number mean what the
        // handler needs it to mean, and makes every transport agree.
        //
        // The count lives in this process and is dropped when the message is
        // accepted or dead-lettered. A consumer restart therefore starts the count
        // again, which is the honest limit of counting without persisting.
        var attempts = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();

        var subscription = await _connection.SubscribeAsync(
            queue, options.PrefetchCount, consumerArguments,
            async delivery =>
            {
                Envelope envelope;
                T payload;
                try
                {
                    envelope = Envelope.FromWire(
                        delivery.Headers, delivery.RoutingKey, delivery.MessageId);
                    payload = (T)codec.Decode(delivery.Body, typeof(T));
                }
                catch (Exception e)
                {
                    // A message this consumer cannot decode will not decode on the
                    // next attempt either. Retrying it forever is how a poison
                    // message becomes an outage, so it goes straight to the
                    // dead-letter queue with the reason attached.
                    return Ack.DeadLetter($"could not decode as {typeof(T).Name}: {e.Message}");
                }

                // Held here rather than rejected, so a paused consumer keeps its
                // place in the queue and resumes with the same message instead of
                // cycling it to the back.
                var paused = _consumingPaused;
                if (paused != null)
                {
                    var resumed = await Task.WhenAny(paused.Task, Task.Delay(TimeSpan.FromSeconds(30)))
                        .ConfigureAwait(false);
                    if (resumed != paused.Task) return Ack.Release();
                }

                var attempt = attempts.AddOrUpdate(envelope.Id, envelope.Attempt, (_, n) => n + 1);

                // Claimed before the handler runs, so a redelivery that arrives while
                // the first attempt is still in flight is not handled twice in
                // parallel. Released on failure, or the retry would look like a
                // duplicate and be dropped.
                if (options.Idempotency != null)
                {
                    if (!await options.Idempotency.ClaimAsync(envelope.Id).ConfigureAwait(false))
                    {
                        return Ack.Accept();
                    }
                }

                var message = new ReceivedMessage<T>(payload, envelope, delivery, attempt);

                // Continues the publisher's trace, which reaches here through the
                // traceparent header the envelope already reserves -- including from
                // a Java publisher, which writes the same header.
                using var span = AceMqTelemetry.StartConsume(queue, delivery.Headers);
                var clock = System.Diagnostics.Stopwatch.StartNew();
                AceMqTelemetry.EnteredHandler();

                Ack ack;
                try
                {
                    ack = await handler(message).ConfigureAwait(false);
                }
                catch (AceFatalException e)
                {
                    ack = Ack.DeadLetter(e.Message);
                }
                catch (Exception e)
                {
                    ack = options.RequeueOnFailure
                        ? Ack.Release()
                        : Ack.Retry(options.RetryDelay, e.Message);
                }
                finally
                {
                    AceMqTelemetry.LeftHandler();
                }

                // A policy turns "retry after a fixed delay forever" into a bounded
                // number of attempts that then dead-letters, which is the difference
                // between a transient failure recovering and a poison message
                // occupying a consumer indefinitely.
                if (ack.IsRetry && options.RetryPolicy != null)
                {
                    var age = DateTimeOffset.UtcNow - envelope.FirstSeen;
                    var next = options.RetryPolicy.NextDelay(attempt, age);
                    ack = next.HasValue
                        ? Ack.Retry(next.Value, ack.Reason ?? "retrying")
                        : Ack.DeadLetter(
                            $"gave up after {attempt} attempt(s): {ack.Reason ?? "no reason given"}");
                }

                if (options.Idempotency != null)
                {
                    if (ack.IsAccept)
                    {
                        await options.Idempotency.ConfirmAsync(envelope.Id).ConfigureAwait(false);
                    }
                    else
                    {
                        await options.Idempotency.ReleaseAsync(envelope.Id).ConfigureAwait(false);
                    }
                }

                RecordConsume(queue, envelope, attempt, ack, clock.Elapsed, span);

                // The message is finished with, either way. Keeping its counter would
                // leak an entry per message handled.
                if (ack.IsAccept || ack.IsDeadLetter) attempts.TryRemove(envelope.Id, out _);
                return ack;
            },
            CancellationToken.None).ConfigureAwait(false);

        var consumer = new MessageConsumer(subscription);
        lock (_owned) _owned.Add(consumer);
        return consumer;
    }

    /// <summary>
    /// Stops handing messages to handlers, without closing anything.
    /// </summary>
    /// <remarks>
    /// Messages already in a handler run to completion. Anything the broker has
    /// delivered but not yet handed over stays unacknowledged, so it is redelivered
    /// to this consumer or another one — nothing is lost by pausing.
    /// </remarks>
    public void PauseConsuming()
    {
        if (_consumingPaused != null) return;
        _consumingPaused =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public void ResumeConsuming()
    {
        var gate = _consumingPaused;
        _consumingPaused = null;
        gate?.TrySetResult(true);
    }

    public bool IsConsumingPaused => _consumingPaused != null;

    /// <summary>Refuses further publishes with <see cref="PublishingPausedException"/>.</summary>
    public void PausePublishing() => _publishingPaused = true;

    public void ResumePublishing() => _publishingPaused = false;

    public bool IsPublishingPaused => _publishingPaused;

    /// <summary>Messages currently inside a handler.</summary>
    public long InFlight => AceMqTelemetry.InFlight;

    /// <summary>
    /// Pauses consuming and waits for handlers already running to finish.
    /// </summary>
    /// <returns>True if everything finished within the timeout.</returns>
    /// <remarks>
    /// What to call before shutting down. Disposing the connection while handlers are
    /// mid-flight abandons their work: the messages were never acknowledged so they
    /// come back, but any side effect already applied has happened twice by the time
    /// they do. Draining first turns a rolling deploy into an orderly handover.
    /// </remarks>
    public async Task<bool> DrainConsumersAsync(TimeSpan timeout)
    {
        PauseConsuming();
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (InFlight == 0) return true;
            await Task.Delay(25).ConfigureAwait(false);
        }
        return InFlight == 0;
    }

    /// <summary>Adds something to the health report.</summary>
    public void RegisterHealth(IHealthContributor contributor)
    {
        if (contributor == null) throw new ArgumentNullException(nameof(contributor));
        lock (_health) _health.Add(contributor);
    }

    /// <summary>
    /// The health of the connection and everything registered with it.
    /// </summary>
    /// <remarks>
    /// The worst report wins. Ordered queues register themselves, so a halted
    /// partition shows up here — which matters, because a halted partition is a
    /// consumer that has stopped without the connection or the process noticing.
    /// </remarks>
    public AggregateHealth Health()
    {
        var reports = new List<HealthReport>();

        var connection = new Dictionary<string, string>
        {
            ["open"] = IsOpen ? "true" : "false",
            ["blocked"] = IsBlocked ? "true" : "false",
            ["transport"] = TransportName,
            ["inFlight"] = InFlight.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        if (IsConsumingPaused) connection["consuming"] = "paused";
        if (IsPublishingPaused) connection["publishing"] = "paused";
        if (BlockedReason != null) connection["blockedReason"] = BlockedReason;
        reports.Add(new HealthReport(
            "connection",
            !IsOpen ? HealthStatus.Down : IsBlocked ? HealthStatus.Degraded : HealthStatus.Up,
            connection));

        List<IHealthContributor> contributors;
        lock (_health) contributors = new List<IHealthContributor>(_health);
        foreach (var contributor in contributors)
        {
            try
            {
                reports.Add(contributor.Report());
            }
            catch (Exception e)
            {
                // A contributor that throws is itself a health problem, and must not
                // take the whole report down with it.
                reports.Add(new HealthReport(
                    contributor.Name, HealthStatus.Down,
                    new Dictionary<string, string> { ["error"] = e.Message }));
            }
        }

        return new AggregateHealth(reports);
    }

    /// <summary>Applies a topology, declaring whatever is missing.</summary>
    public async Task<TopologyPlan> ApplyAsync(Topology topology) =>
        await ApplyAsync(topology, ApplyMode.Declare).ConfigureAwait(false);

    /// <summary>
    /// Applies a topology, or reports what applying it would do.
    /// </summary>
    /// <remarks>
    /// <see cref="ApplyMode.DryRun"/> asks the broker what already exists and changes
    /// nothing, which is what makes a topology reviewable before a deployment rather
    /// than after it. Exchanges and bindings cannot be inspected over AMQP, so they
    /// are reported as <see cref="TopologyActionKind.Unknown"/> rather than guessed
    /// at — saying "would create" about something that already exists is the kind of
    /// plausible-looking output that stops being read.
    /// </remarks>
    public async Task<TopologyPlan> ApplyAsync(Topology topology, ApplyMode mode)
    {
        EnsureOpen();
        if (topology == null) throw new ArgumentNullException(nameof(topology));

        var actions = new List<TopologyAction>();
        var dryRun = mode == ApplyMode.DryRun;

        foreach (var exchange in topology.Exchanges)
        {
            actions.Add(new TopologyAction(
                dryRun ? TopologyActionKind.Unknown : TopologyActionKind.Create,
                $"exchange {exchange.Name} ({exchange.Type})"));
            if (!dryRun)
            {
                await _connection.DeclareExchangeAsync(
                    exchange.Name, exchange.Type, exchange.Durable, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }

        foreach (var queue in topology.Queues)
        {
            var exists = await _connection.QueueExistsAsync(queue.Name, CancellationToken.None)
                .ConfigureAwait(false);
            actions.Add(new TopologyAction(
                exists ? TopologyActionKind.Present : TopologyActionKind.Create,
                $"queue {queue.Name} ({queue.Type.ToString().ToLowerInvariant()})"));
            if (!dryRun)
            {
                await _connection.DeclareQueueAsync(
                    queue.Name, queue.Type, queue.Durable,
                    queue.Arguments.Count == 0 ? null : queue.Arguments, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }

        foreach (var binding in topology.Bindings)
        {
            actions.Add(new TopologyAction(
                dryRun ? TopologyActionKind.Unknown : TopologyActionKind.Create,
                $"bind {binding.Queue} to {binding.Exchange} on '{binding.RoutingKey}'"));
            if (!dryRun)
            {
                await _connection.BindQueueAsync(
                    binding.Queue, binding.Exchange, binding.RoutingKey, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }

        return TopologyPlan.Of(actions);
    }

    /// <summary>Starts a requester, with its own reply queue.</summary>
    public async Task<Requester> RequesterAsync()
    {
        EnsureOpen();
        var requester = await Requester.StartAsync(this, _codec).ConfigureAwait(false);
        lock (_owned) _owned.Add(requester);
        return requester;
    }

    /// <summary>Answers requests arriving on a queue.</summary>
    public Task<Responder> RespondAsync<TRequest, TResponse>(
        string queue, Func<TRequest, Task<TResponse>> handler) =>
        RespondAsync(queue, ConsumerOptions.Defaults(), handler);

    public async Task<Responder> RespondAsync<TRequest, TResponse>(
        string queue, ConsumerOptions options, Func<TRequest, Task<TResponse>> handler)
    {
        EnsureOpen();
        var responder = await Responder
            .StartAsync(this, _codec, queue, options, handler).ConfigureAwait(false);
        lock (_owned) _owned.Add(responder);
        return responder;
    }

    /// <summary>A set of queues that keep order within a key.</summary>
    public OrderedQueueBuilder<T> Ordered<T>(string name)
    {
        EnsureOpen();
        return new OrderedQueueBuilder<T>(this, name);
    }

    /// <summary>A chain of steps, each on its own queue.</summary>
    public PipelineBuilder<T, T> Pipeline<T>(string name)
    {
        EnsureOpen();
        return new PipelineBuilder<T, T>(
            this, name, new List<PipelineStep>(), _codec);
    }

    /// <summary>Publishes what an outbox store has been given.</summary>
    public OutboxRelay Outbox(IOutboxStore store)
    {
        EnsureOpen();
        var relay = new OutboxRelay(this, store);
        lock (_owned) _owned.Add(relay);
        return relay;
    }

    /// <summary>Reads a stream queue.</summary>
    public StreamReader<T> Stream<T>(string queue)
    {
        EnsureOpen();
        return new StreamReader<T>(this, queue);
    }

    /// <summary>
    /// Declares a stream queue, optionally bounded by age or size.
    /// </summary>
    /// <remarks>
    /// A stream keeps everything written to it until one of these limits removes it,
    /// so declaring one without a limit is declaring a queue that grows until the
    /// disk is full.
    /// </remarks>
    public Task<AceMqConnection> DeclareStreamAsync(
        string name, TimeSpan? maxAge, long? maxLengthBytes)
    {
        var arguments = new Dictionary<string, object>();
        if (maxAge.HasValue)
        {
            arguments["x-max-age"] =
                ((long)maxAge.Value.TotalSeconds)
                    .ToString(System.Globalization.CultureInfo.InvariantCulture) + "s";
        }
        if (maxLengthBytes.HasValue) arguments["x-max-length-bytes"] = maxLengthBytes.Value;
        return DeclareQueueAsync(name, QueueType.Stream, arguments);
    }

    /// <summary>Moves messages off a queue and republishes them.</summary>
    public Replay Replay(string queue)
    {
        EnsureOpen();
        return new Replay(_connection, queue);
    }

    /// <summary>
    /// The underlying transport connection, for anything this library does not expose.
    /// </summary>
    /// <remarks>
    /// An escape hatch, and deliberately a typed one: cast it to the transport's own
    /// connection type to reach the client underneath. A library without a way down
    /// to the driver makes every gap in its own API a blocking one, and gaps are
    /// certain in a pre-1.0 library. Using this ties your code to a particular
    /// transport, which is the trade being offered rather than hidden.
    /// </remarks>
    public ITransportConnection Transport => _connection;

    private static void RecordConsume(
        string queue, Envelope envelope, int attempt, Ack ack, TimeSpan elapsed,
        System.Diagnostics.Activity? span)
    {
        var outcome = ack.Kind switch
        {
            AckKind.Accept => MetricNames.OutcomeAcked,
            AckKind.Retry => MetricNames.OutcomeRetried,
            AckKind.DeadLetter => MetricNames.OutcomeDeadLettered,
            _ => MetricNames.OutcomeRejected,
        };

        var tags = new System.Diagnostics.TagList
        {
            { MetricNames.TagQueue, queue },
            { MetricNames.TagMessageType, envelope.Type },
            { MetricNames.TagOutcome, outcome },
        };

        AceMqTelemetry.ConsumeDuration.Record(elapsed.TotalSeconds, tags);
        AceMqTelemetry.ConsumeTotal.Add(1, tags);
        AceMqTelemetry.ConsumeAttempts.Record(attempt, tags);

        if (ack.IsRetry) AceMqTelemetry.RetriedTotal.Add(1, tags);
        if (ack.IsDeadLetter) AceMqTelemetry.DeadLetteredTotal.Add(1, tags);

        span?.SetTag(MetricNames.TagOutcome, outcome);
        span?.SetTag("acemq.attempt", attempt);
        if (!ack.IsAccept)
        {
            span?.SetStatus(
                System.Diagnostics.ActivityStatusCode.Error, ack.Reason ?? outcome);
        }
    }

    private void EnsureOpen()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(AceMqConnection));
        if (!_connection.IsOpen) throw new TransportException("the connection is closed");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        lock (_owned)
        {
            foreach (var d in _owned) d.Dispose();
            _owned.Clear();
        }
        _connection.Dispose();
        _inFlight.Dispose();
    }
}
