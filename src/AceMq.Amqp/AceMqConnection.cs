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
    private readonly List<IPublishInterceptor> _publishInterceptors = new List<IPublishInterceptor>();
    private readonly List<IConsumeInterceptor> _consumeInterceptors = new List<IConsumeInterceptor>();
    private volatile TaskCompletionSource<bool>? _consumingPaused;
    private volatile bool _publishingPaused;

    // This connection's own in-flight count, not the process-wide gauge. Two
    // connections in one process must be drainable independently; waiting on the
    // global number makes draining one block on the other's traffic.
    private long _inFlightHandlers;
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

    /// <summary>Declares a durable quorum queue.</summary>
    /// <remarks>
    /// Quorum is the default for the same two reasons it is the default in
    /// <see cref="Topology.Builder.Queue(string)"/> and in Java's
    /// <c>AceMq.declareQueue(String)</c>: a queue that survives losing its node is
    /// what almost everyone wants and almost nobody remembers to ask for, and the
    /// type is part of what two services sharing a queue have to agree on. Pass
    /// <see cref="QueueType.Classic"/> to the overload where classic is wanted —
    /// which it is for anything exclusive or auto-delete, since RabbitMQ refuses a
    /// quorum queue declared either way.
    /// </remarks>
    public Task<AceMqConnection> DeclareQueueAsync(string name) =>
        DeclareQueueAsync(name, QueueType.Quorum, null);

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

    /// <summary>Removes an exchange and every binding on it.</summary>
    /// <remarks>
    /// The counterpart to <see cref="DeclareExchangeAsync"/>. Anything that declares
    /// an exchange it owns for a while — a test, a migration, a temporary fan-out —
    /// needs a way to take it away again, and until this existed there was none: a
    /// suite could delete its queues and had to leave its exchanges on the broker.
    /// </remarks>
    public Task DeleteExchangeAsync(string name) =>
        _connection.DeleteExchangeAsync(name, CancellationToken.None);

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
        string exchange, string routingKey, PublishOptions options, string? replyTo) =>
        Publisher<T>(exchange, routingKey, options, replyTo, _codec);

    /// <summary>
    /// A publisher that encodes with a codec of its own rather than the
    /// connection's.
    /// </summary>
    /// <remarks>
    /// Internal, and used by the outbox relay. A record's payload was serialised
    /// inside the writer's transaction, so the relay's job is to put those bytes
    /// on the wire unchanged — sending them through the connection's codec would
    /// encode them a second time.
    /// </remarks>
    internal IPublisher<T> Publisher<T>(
        string exchange, string routingKey, PublishOptions options, string? replyTo, ICodec codec)
    {
        EnsureOpen();
        IPublishInterceptor[] interceptors;
        lock (_publishInterceptors) interceptors = _publishInterceptors.ToArray();

        var publisher = new Publisher<T>(
            _connection, codec, exchange, routingKey, options, _inFlight,
            _config.ConfirmTimeout, replyTo, () => _publishingPaused, interceptors);
        lock (_owned) _owned.Add(publisher);
        return publisher;
    }

    /// <summary>The codec this connection encodes and decodes with.</summary>
    internal ICodec Codec => _codec;

    /// <summary>Starts consuming a queue.</summary>
    public Task<IMessageConsumer> ConsumeAsync<T>(string queue, Func<IMessage<T>, Task<Ack>> handler) =>
        ConsumeAsync(queue, ConsumerOptions.Defaults(), handler);

    public Task<IMessageConsumer> ConsumeAsync<T>(
        string queue, ConsumerOptions options, Func<IMessage<T>, Task<Ack>> handler) =>
        ConsumeCoreAsync(queue, options, null, handler, ownedByCaller: true);

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
            handler, ownedByCaller: true);

    /// <summary>
    /// Consumes a queue this library invented, rather than one a caller named.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two callers: the reply queue a <see cref="Requester"/> reads its answers on,
    /// and the <c>acemq.schedule.due</c> control queue a <see cref="Scheduler"/> reads
    /// expired messages from. Every other queue consumed here is one an application
    /// chose and will keep choosing — <c>orders.new</c> is <c>orders.new</c> on every
    /// restart, so the <c>{queue}.dlq</c> and <c>{queue}.parked</c> declared beside it
    /// are the same two queues every time, and an operator can find them.
    /// </para>
    /// <para>
    /// Neither of these is that. A reply queue is <c>acemq.reply.{a fresh guid}</c>, a
    /// name that exists once and is never used again, so declaring a durable pair
    /// beside it would leave two queues on the broker per requester ever constructed
    /// and no name by which anything could find them later. The scheduler's control
    /// queue is the opposite problem and the same answer: the name is fixed and
    /// shared, so <c>acemq.schedule.due.dlq</c> and <c>acemq.schedule.due.parked</c>
    /// would appear on the broker of every service that ever constructed a scheduler,
    /// two durable queues nothing publishes to and nobody drains.
    /// </para>
    /// <para>
    /// They are also the two consumers here with no failure path to serve. Both decode
    /// to <c>byte[]</c>, which cannot fail, and both handlers return
    /// <see cref="Ack.Accept"/> on every path — so neither can reach
    /// <c>{queue}.dlq</c> nor <c>{queue}.parked</c>, and queues nothing can reach are
    /// not worth the litter. Give either one a handler that can give up and it needs
    /// the argument turned back on.
    /// </para>
    /// </remarks>
    internal Task<IMessageConsumer> ConsumePrivateQueueAsync<T>(
        string queue, ConsumerOptions options, Func<IMessage<T>, Task<Ack>> handler) =>
        ConsumeCoreAsync(queue, options, null, handler, ownedByCaller: false);

    private async Task<IMessageConsumer> ConsumeCoreAsync<T>(
        string queue, ConsumerOptions options,
        IReadOnlyDictionary<string, object>? consumerArguments,
        Func<IMessage<T>, Task<Ack>> handler,
        bool ownedByCaller)
    {
        EnsureOpen();
        if (handler == null) throw new ArgumentNullException(nameof(handler));
        var codec = options.Codec ?? _codec;
        var policy = options.RetryPolicy;

        // Every queue this consumer can send a message to that is not the one it is
        // reading: the rungs a long wait is spent in, and the two a message ends up in
        // when it is not coming back. Worked out and declared before anything is
        // subscribed, rather than at the moment one is first needed. A queue that does
        // not exist loses the message rather than reporting anything — an unroutable
        // publish is dropped — so the moment to find out is the one where nothing has
        // failed yet, and a queue an operator can see from start-up is one they can
        // alert on before the first failure rather than after it.
        //
        // With no policy this declares {queue}.dlq and {queue}.parked and nothing else.
        // A consumer without a retry schedule still dead-letters — Ack.DeadLetter, a
        // body that will not decode — so it needs those two; it has no rungs, so it
        // needs no retry exchange and no binding to one.
        var ladder = RetryLadder.For(queue, policy ?? RetryPolicy.None());
        if (ownedByCaller)
        {
            await ladder.DeclareAsync(_connection, CancellationToken.None).ConfigureAwait(false);
        }

        var subscription = await _connection.SubscribeAsync(
            queue, options.PrefetchCount, consumerArguments,
            async delivery =>
            {
                var envelope = Envelope.FromWire(
                    delivery.Headers, delivery.RoutingKey, delivery.MessageId);

                T payload;
                try
                {
                    payload = codec.Decode<T>(delivery.Body, delivery.ContentType);
                }
                catch (Exception e)
                {
                    // A body that will not decode decodes no better next time, so it
                    // is parked rather than retried. Parked and not dead-lettered: a
                    // message that failed five times and a message nothing could read
                    // are two different problems, and whoever drains the dead letters
                    // should not have to sort them by hand.
                    var parked = await SettleAsync(
                        queue, ladder, policy, delivery, envelope,
                        Ack.Park($"could not decode as {typeof(T).Name}: {e.Message}"),
                        span: null)
                        .ConfigureAwait(false);
                    return parked.Ack;
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

                // Read off the wire, not counted here. A retry republishes with this
                // advanced, so the number travels with the message: a fleet of
                // consumers all agree on it, a message that moves between them keeps
                // it, and a restart does not forget it.
                var attempt = envelope.Attempt;

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
                IConsumeInterceptor[] interceptors;
                lock (_consumeInterceptors) interceptors = _consumeInterceptors.ToArray();
                var interceptorContext = new ConsumeContext(queue, envelope, payload);
                foreach (var interceptor in interceptors)
                {
                    interceptor.BeforeHandle(interceptorContext);
                }

                using var span = AceMqTelemetry.StartConsume(queue, envelope, delivery.Headers);
                var clock = System.Diagnostics.Stopwatch.StartNew();
                AceMqTelemetry.EnteredHandler();
                Interlocked.Increment(ref _inFlightHandlers);

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
                    foreach (var interceptor in interceptors)
                    {
                        interceptor.OnError(interceptorContext, e);
                    }
                    ack = options.RequeueOnFailure
                        ? Ack.Release()
                        : Ack.Retry(options.RetryDelay, e.Message);
                }
                finally
                {
                    AceMqTelemetry.LeftHandler();
                    Interlocked.Decrement(ref _inFlightHandlers);
                }

                foreach (var interceptor in interceptors)
                {
                    interceptor.AfterHandle(interceptorContext, ack);
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

                // Stopped before settling, not after: a short backoff is waited inside
                // SettleAsync, and folding that wait into "how long the handler took"
                // would make every retrying consumer look slow.
                var elapsed = clock.Elapsed;

                // Settled first, then recorded. The handler's answer is a request, not
                // the outcome: Ack.Retry on the last attempt a policy allows becomes a
                // dead-letter, and recording before settling is what used to tag that
                // span outcome="retried" and count it as a retry that never happened.
                var settled = await SettleAsync(
                        queue, ladder, policy, delivery, envelope, ack, span)
                    .ConfigureAwait(false);

                RecordConsume(queue, envelope, attempt, ack, settled.Outcome, elapsed, span);

                return settled.Ack;
            },
            CancellationToken.None).ConfigureAwait(false);

        var consumer = new MessageConsumer(subscription);
        lock (_owned) _owned.Add(consumer);
        return consumer;
    }

    /// <summary>
    /// Carries out a handler's decision and tells the transport what to do with the
    /// original delivery.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Everything except accepting and releasing is done by <em>republishing</em> and
    /// then acknowledging the original. Acknowledging a failure looks wrong and is
    /// what makes this reliable: by the time the acknowledgement happens the message
    /// has been safely republished elsewhere, so the original is a copy that has been
    /// dealt with.
    /// </para>
    /// <para>
    /// The alternatives are both worse. Rejecting without requeue hands the message to
    /// whatever dead-lettering the queue happens to be declared with — nothing, in the
    /// common case, which discards it — and neither the broker nor the queue can write
    /// <c>x-acemq-error</c> onto it, which is the one thing whoever finds it in the
    /// dead-letter queue actually needs. Rejecting <em>with</em> requeue hands back the
    /// bytes the broker was given, so <c>x-acemq-attempt</c> never advances and the
    /// count has to live in this process — where it is per-process, lost on restart,
    /// and wrong the moment a second consumer joins the queue.
    /// </para>
    /// <para>
    /// What it costs is that a retried message goes to the back of its queue rather
    /// than the front, and that a crash between the republish and the acknowledgement
    /// delivers it twice. Both are the right way round: at-least-once is what the rest
    /// of this library is built to survive.
    /// </para>
    /// </remarks>
    private async Task<Settled> SettleAsync(
        string queue, RetryLadder ladder, RetryPolicy? policy,
        InboundDelivery delivery, Envelope envelope, Ack ack,
        System.Diagnostics.Activity? span)
    {
        switch (ack.Kind)
        {
            case AckKind.Accept:
                return new Settled(ack, MetricNames.OutcomeAcked);

            case AckKind.Release:
                return new Settled(ack, MetricNames.OutcomeRejected);

            case AckKind.Park:
            {
                var reason = ack.Reason ?? "parked with no reason given";
                AceMqDiagnostics.Report(
                    AceMqDiagnostics.Parked, DiagnosticLevel.Warning,
                    reason, queue, ladder.ParkedQueue, envelope.Id, envelope.Attempt, null);

                // Parked counts and traces as dead-lettered, for the same reason the
                // metric does: the number anyone alerts on is "messages this consumer
                // could not handle", and the destination on the event says which of
                // the two queues it went to.
                AceMqTelemetry.MessageDeadLettered(span, ladder.ParkedQueue, reason);

                var moved = await MoveAsync(
                        ladder.ParkedQueue, delivery, envelope.WithError(reason))
                    .ConfigureAwait(false);
                return new Settled(moved, MetricNames.OutcomeDeadLettered);
            }

            case AckKind.DeadLetter:
            {
                var reason = ack.Reason ?? "no reason given";
                AceMqDiagnostics.Report(
                    AceMqDiagnostics.DeadLettered, DiagnosticLevel.Warning,
                    reason, queue, ladder.DeadLetterQueue, envelope.Id, envelope.Attempt, null);

                // Dead-lettered, but reported as rejected. Both end in the dead-letter
                // queue and only the word keeps them apart: this is a decision somebody
                // took about this message, where dead_lettered is the engine running out
                // of room to try again. Go, Python and Ruby have always drawn the line
                // here; this library and Java drew it in the wrong place until 0.6.0.
                AceMqTelemetry.MessageRejected(span, ladder.DeadLetterQueue, reason);

                var moved = await MoveAsync(
                        ladder.DeadLetterQueue, delivery, envelope.WithError(reason))
                    .ConfigureAwait(false);
                return new Settled(moved, MetricNames.OutcomeRejected);
            }

            default:
                return await RetryAsync(queue, ladder, policy, delivery, envelope, ack, span)
                    .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// What actually became of a delivery, and the outcome that names it.
    /// </summary>
    /// <remarks>
    /// The outcome is carried out of <see cref="SettleAsync"/> rather than derived
    /// from the handler's <see cref="Ack"/>, because only the settle knows it: a
    /// handler that asks for a retry on its last permitted attempt gets a dead-letter,
    /// and both the span and the counters have to say so.
    /// </remarks>
    private readonly struct Settled
    {
        internal Settled(Ack ack, string outcome)
        {
            Ack = ack;
            Outcome = outcome;
        }

        /// <summary>What the transport is told to do with the original delivery.</summary>
        internal Ack Ack { get; }

        /// <summary>One of the <c>MetricNames.Outcome*</c> constants.</summary>
        internal string Outcome { get; }
    }

    /// <summary>
    /// Works out whether there is another attempt, how long it waits and where, and
    /// republishes the message accordingly.
    /// </summary>
    /// <remarks>
    /// A short wait is spent here, holding one prefetch slot; a long one is spent in a
    /// rung queue, because a consumer that sleeps through a five-minute backoff loses
    /// the whole wait when it restarts — the broker redelivers the unacknowledged
    /// message at once, and a five-minute policy delivers in none. Giving up is decided
    /// here too, and not left to the broker, because the broker cannot say why.
    /// </remarks>
    private async Task<Settled> RetryAsync(
        string queue, RetryLadder ladder, RetryPolicy? policy,
        InboundDelivery delivery, Envelope envelope, Ack ack,
        System.Diagnostics.Activity? span)
    {
        var reason = ack.Reason ?? "no reason given";
        var advanced = envelope.WithAttempt(envelope.Attempt + 1);

        if (policy == null)
        {
            // No policy means no give-up: the delay the handler asked for, waited
            // here, for ever. Bounding it is exactly what a policy is for, which is
            // why one is worth configuring.
            var asked = ack.Delay ?? TimeSpan.Zero;
            AceMqTelemetry.MessageRetried(span, asked, queue, reason);
            if (asked > TimeSpan.Zero) await Task.Delay(asked).ConfigureAwait(false);
            var requeued = await MoveAsync(queue, delivery, advanced).ConfigureAwait(false);
            return new Settled(requeued, MetricNames.OutcomeRetried);
        }

        var next = policy.NextWait(envelope.Attempt, AgeOf(envelope));
        if (next == null)
        {
            var gaveUp = $"{GaveUp(policy, envelope)}: {reason}";
            AceMqDiagnostics.Report(
                AceMqDiagnostics.DeadLettered, DiagnosticLevel.Warning,
                gaveUp, queue, ladder.DeadLetterQueue, envelope.Id, envelope.Attempt, null);

            // The moment the message stops being retried. Emitted here and nowhere
            // else, so a trace backend searching for dead letters finds the ones the
            // policy decided as well as the ones a handler asked for.
            AceMqTelemetry.MessageDeadLettered(span, ladder.DeadLetterQueue, gaveUp);

            var buried = await MoveAsync(
                    ladder.DeadLetterQueue, delivery, envelope.WithError(gaveUp))
                .ConfigureAwait(false);
            return new Settled(buried, MetricNames.OutcomeDeadLettered);
        }

        if (next.InBroker)
        {
            var rung = ladder.RungFor(next.Delay);
            if (rung != null)
            {
                // Nothing is set on the message itself. A per-message expiration looks
                // like the flexible answer and is a trap: RabbitMQ expires messages
                // only from the head of a queue, so one long wait at the front holds
                // back every shorter one behind it.
                AceMqTelemetry.MessageRetried(span, next.Delay, rung, reason);
                var held = await MoveAsync(rung, delivery, advanced).ConfigureAwait(false);
                return new Settled(held, MetricNames.OutcomeRetried);
            }

            // The policy wanted the broker to hold this and the ladder has nowhere to
            // put it, so the wait falls back to this process — where a restart loses
            // it, which is the whole thing the rungs exist to prevent. It should not
            // be reachable: the rungs come from the same policy. If it is, the ladder
            // was built from a different policy than the one deciding the delay, and
            // that is worth saying out loud rather than absorbing silently.
            AceMqDiagnostics.Report(
                AceMqDiagnostics.RungMissing, DiagnosticLevel.Warning,
                $"no rung for a broker wait of {Naming.Describe(next.Delay)}; " +
                "waiting in the consumer instead, where a restart loses the wait",
                queue, null, envelope.Id, envelope.Attempt, null);
        }

        AceMqTelemetry.MessageRetried(span, next.Delay, queue, reason);
        if (next.Delay > TimeSpan.Zero) await Task.Delay(next.Delay).ConfigureAwait(false);
        var again = await MoveAsync(queue, delivery, advanced).ConfigureAwait(false);
        return new Settled(again, MetricNames.OutcomeRetried);
    }

    /// <summary>
    /// Publishes the message somewhere else and, only once the broker has taken it,
    /// accepts the original.
    /// </summary>
    /// <remarks>
    /// Nothing is declared here, and that is the whole of the change ADR-032 made.
    /// Every destination this method is given — the source queue, a rung,
    /// <c>{queue}.dlq</c>, <c>{queue}.parked</c> — was declared before the consumer
    /// subscribed: the source by whoever owns it, the other three by
    /// <see cref="RetryLadder.DeclareAsync"/>. The dead-letter and parking queues used
    /// to be declared right here instead, the first time one was needed, which reached
    /// the same end state one failure later and left a second place that could declare
    /// a queue this library also declares elsewhere. Two declarations of one queue
    /// that do not agree, argument for argument, is a <c>PRECONDITION_FAILED</c> that
    /// stops a consumer starting, so there is now one of them.
    /// <para>
    /// What it costs is a queue somebody deletes while a consumer is running: the move
    /// then fails to route, and the message is released back to the broker rather than
    /// acknowledged. Reported and recoverable, which a lost message would not be.
    /// </para>
    /// </remarks>
    private async Task<Ack> MoveAsync(
        string destination, InboundDelivery delivery, Envelope envelope)
    {
        try
        {
            // Started from what arrived rather than from the envelope alone, so a
            // header this version does not materialise — a routing slip, a claim
            // check, anything a newer library added — survives the move. The envelope
            // is then laid over the top, which is what advances the attempt and writes
            // the reason.
            var headers = new Dictionary<string, object>(
                (IDictionary<string, object>)delivery.Headers);
            foreach (var pair in envelope.ToWire()) headers[pair.Key] = pair.Value;
            if (envelope.Error == null) headers.Remove(AceHeaders.Error);

            var result = await _connection.SendAsync(
                    new OutboundMessage(
                        string.Empty, destination, delivery.Body, headers,
                        envelope.Id, delivery.ContentType,
                        persistent: true, mandatory: true, expiration: null, priority: null,
                        replyTo: delivery.ReplyTo),
                    CancellationToken.None)
                .ConfigureAwait(false);

            if (!result.Confirmed || !result.Routed)
            {
                throw new PublishFailedException(
                    $"moving a message to '{destination}' " +
                    (result.Reason ?? "matched no queue"));
            }

            return Ack.Accept();
        }
        catch (Exception e)
        {
            // The move did not happen, so the original must not be acknowledged.
            // Released instead: the broker keeps it and hands it to somebody, which is
            // the only outcome here that does not lose it. The attempt does not
            // advance, which is the honest consequence — the message really has not
            // been anywhere.
            var failure = $"could not move a message to '{destination}': {e.Message}";
            System.Diagnostics.Activity.Current?.SetStatus(
                System.Diagnostics.ActivityStatusCode.Error, failure);

            // The span alone was not enough. Unless the process was already exporting
            // traces, and unless somebody went looking at the right one, a message the
            // library could not move and handed back to the broker left no trace at
            // all — and it is the single event on this path an operator most needs to
            // see, because a release with no advance is a redelivery loop waiting to
            // happen.
            AceMqDiagnostics.Report(
                AceMqDiagnostics.MoveFailed, DiagnosticLevel.Error, failure,
                delivery.Queue, destination, envelope.Id, envelope.Attempt, e);
            return Ack.Release();
        }
    }

    /// <summary>How old the message is, as the give-up rule means it.</summary>
    /// <remarks>
    /// A message with no <c>x-acemq-first-seen</c> reads back as the epoch, which would
    /// make everything published by something that does not write the header look
    /// fifty years old and be given up on immediately. Unknown is treated as new.
    /// </remarks>
    private static TimeSpan AgeOf(Envelope envelope)
    {
        if (envelope.FirstSeen <= DateTimeOffset.FromUnixTimeMilliseconds(0)) return TimeSpan.Zero;
        var age = DateTimeOffset.UtcNow - envelope.FirstSeen;
        return age < TimeSpan.Zero ? TimeSpan.Zero : age;
    }

    /// <summary>Which of the two limits was reached, in words the reason can carry.</summary>
    private static string GaveUp(RetryPolicy policy, Envelope envelope) =>
        envelope.Attempt >= policy.MaxAttempts
            ? $"gave up after {policy.MaxAttempts} attempt(s)"
            : $"gave up on a message older than {policy.MaxMessageAge}";

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

    /// <summary>Messages currently inside a handler on this connection.</summary>
    public long InFlight => Interlocked.Read(ref _inFlightHandlers);

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

    /// <summary>
    /// Adds an interceptor that runs around every publish.
    /// </summary>
    /// <remarks>
    /// Registered before the publishers that use it. A publisher takes the
    /// interceptors present when it is created, so one added afterwards does not
    /// apply to it — which is deliberate: an interceptor appearing part way through a
    /// process's life would make two otherwise identical publishers behave
    /// differently, for reasons nothing in the code shows.
    /// </remarks>
    public AceMqConnection Intercept(IPublishInterceptor interceptor)
    {
        if (interceptor == null) throw new ArgumentNullException(nameof(interceptor));
        lock (_publishInterceptors)
        {
            _publishInterceptors.Add(interceptor);
            _publishInterceptors.Sort((a, b) => a.Order.CompareTo(b.Order));
        }
        return this;
    }

    /// <summary>Adds an interceptor that runs around every handled message.</summary>
    public AceMqConnection Intercept(IConsumeInterceptor interceptor)
    {
        if (interceptor == null) throw new ArgumentNullException(nameof(interceptor));
        lock (_consumeInterceptors)
        {
            _consumeInterceptors.Add(interceptor);
            _consumeInterceptors.Sort((a, b) => a.Order.CompareTo(b.Order));
        }
        return this;
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
            var check = await _connection.CheckQueueAsync(
                queue.Name, queue.Type, queue.Durable,
                queue.Arguments.Count == 0 ? null : queue.Arguments, CancellationToken.None)
                .ConfigureAwait(false);

            var kind = check.Result switch
            {
                QueueCheckResult.Absent => TopologyActionKind.Create,
                QueueCheckResult.Matches => TopologyActionKind.Present,
                QueueCheckResult.Differs => TopologyActionKind.Drift,
                _ => TopologyActionKind.Unknown,
            };
            var description = $"queue {queue.Name} ({queue.Type.ToString().ToLowerInvariant()})";
            if (check.Detail != null) description += $" -- {check.Detail}";
            actions.Add(new TopologyAction(kind, description));

            // Drift is reported, never corrected. A queue's type and arguments are
            // fixed when it is created, so "fixing" it would mean deleting a queue
            // that has messages in it -- which is not something a library should do
            // because a declaration did not match.
            if (!dryRun && check.Result == QueueCheckResult.Differs)
            {
                throw new AceFatalException(
                    $"queue {queue.Name} already exists with different settings: {check.Detail}. " +
                    "A queue's type and arguments are fixed at creation, so this needs the " +
                    "queue drained and redeclared rather than changed in place.");
            }
            if (!dryRun && check.Result != QueueCheckResult.Matches)
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
        string name, TimeSpan? maxAge, long? maxLengthBytes) =>
        DeclareStreamAsync(name, maxAge, maxLengthBytes, null);

    /// <summary>
    /// Declares a stream queue, optionally bounded by age or size, and optionally
    /// with a segment size.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <paramref name="segmentBytes"/> is how large each file on disk gets. Retention
    /// happens a whole segment at a time, so a very large segment makes retention
    /// coarse: nothing is discarded until an entire segment can be, and a stream
    /// bounded at 1 GB with 500 MB segments keeps rather more than 1 GB.
    /// </para>
    /// <para>
    /// <strong>Absent unless asked for.</strong> There is no default here on purpose.
    /// The broker has one, it is the right one nearly always, and a library that
    /// picked its own would make a stream declared from C# subtly different from the
    /// same stream declared from Go, Python or Ruby — where this option is opt-in too
    /// — and a mismatched argument fails a redeclaration rather than being ignored.
    /// </para>
    /// </remarks>
    public Task<AceMqConnection> DeclareStreamAsync(
        string name, TimeSpan? maxAge, long? maxLengthBytes, long? segmentBytes)
    {
        var arguments = new Dictionary<string, object>();
        if (maxAge.HasValue)
        {
            arguments["x-max-age"] =
                ((long)maxAge.Value.TotalSeconds)
                    .ToString(System.Globalization.CultureInfo.InvariantCulture) + "s";
        }
        if (maxLengthBytes.HasValue) arguments["x-max-length-bytes"] = maxLengthBytes.Value;
        if (segmentBytes.HasValue)
        {
            arguments[StreamArguments.SegmentBytes] = segmentBytes.Value;
        }
        return DeclareQueueAsync(name, QueueType.Stream, arguments);
    }

    /// <summary>
    /// Sends a message along a route it carries with it.
    /// </summary>
    /// <remarks>
    /// The slip's current step names the queue. Each step's handler calls
    /// <see cref="ForwardAsync{T}"/> to pass it on, so the route can be changed part
    /// way through by whatever handled the last step.
    /// </remarks>
    public async Task<string> SendAlongAsync<T>(RoutingSlip slip, T payload)
    {
        EnsureOpen();
        if (slip == null) throw new ArgumentNullException(nameof(slip));
        if (slip.IsFinished)
        {
            throw new ArgumentException("this routing slip has no steps left", nameof(slip));
        }
        return await ForwardAsync(slip, payload, Envelope.Of(slip.Current!).Build())
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Sends a message to the slip's current step, keeping its envelope.
    /// </summary>
    /// <remarks>
    /// Call <see cref="RoutingSlip.Advance"/> before this to move it on. A handler
    /// that forwards without advancing sends the message back to itself, which is a
    /// loop rather than a route.
    /// </remarks>
    public async Task<string> ForwardAsync<T>(RoutingSlip slip, T payload, Envelope envelope)
    {
        EnsureOpen();
        if (slip == null) throw new ArgumentNullException(nameof(slip));
        if (envelope == null) throw new ArgumentNullException(nameof(envelope));
        if (slip.IsFinished)
        {
            throw new ArgumentException("this routing slip has no steps left", nameof(slip));
        }

        // The slip rides in reserved headers, which the envelope strips from the
        // application's view on the way back out -- so a handler sees its payload
        // and asks for the slip explicitly rather than finding routing machinery
        // mixed into its own headers.
        var builder = Envelope.Of(envelope.Type)
            .Id(envelope.Id)
            .CorrelationId(envelope.CorrelationId)
            .CausationId(envelope.CausationId)
            .Attempt(envelope.Attempt)
            .FirstSeen(envelope.FirstSeen);
        foreach (var header in envelope.Headers)
        {
            if (!AceHeaders.IsAceHeader(header.Key)) builder.Header(header.Key, header.Value);
        }

        var carried = builder.Build();
        var publisher = Publisher<T>(string.Empty, slip.Current!);
        await ((Publisher<T>)publisher)
            .SendWithHeadersAsync(payload, carried, slip.ToHeaders(), CancellationToken.None)
            .ConfigureAwait(false);
        return carried.Id;
    }

    /// <summary>Moves messages off a queue and republishes them.</summary>
    /// <summary>
    /// Takes one message from a queue without starting a consumer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// For a tool draining a dead-letter queue, a job that runs on a schedule
    /// rather than continuously, or a test that wants exactly one message.
    /// </para>
    /// <para>
    /// It is the wrong shape for ordinary work. Polling costs a round trip per
    /// message whether one is there or not, and
    /// <see cref="ConsumeAsync{T}(string, Func{IMessage{T}, Task{Ack}})"/> is both
    /// faster and kinder to the broker.
    /// </para>
    /// <para>
    /// <strong>The message is held until it is settled.</strong> Losing the
    /// returned value without acknowledging or rejecting it leaves the message
    /// unacknowledged until the connection closes, and the broker then gives it
    /// to somebody else. Settle it in a <c>finally</c> if the work between can
    /// throw.
    /// </para>
    /// <returns>The message, or null when nothing arrived before the timeout.</returns>
    /// </remarks>
    public async Task<PulledMessage<T>?> PullAsync<T>(string queue, TimeSpan timeout)
    {
        if (queue == null) throw new ArgumentNullException(nameof(queue));

        var pulled = await _connection.PullAsync(queue, timeout, CancellationToken.None)
            .ConfigureAwait(false);
        if (pulled == null) return null;

        var delivery = pulled.Delivery;
        var envelope = Envelope.FromWire(delivery.Headers, delivery.RoutingKey, delivery.MessageId);

        T payload;
        try
        {
            payload = _codec.Decode<T>(delivery.Body, delivery.ContentType);
        }
        catch
        {
            // Returned rather than swallowed: the caller decides whether a body
            // it cannot read should go back on the queue or be dead-lettered,
            // and leaving it unsettled while an exception unwinds would hold it
            // until the connection closes.
            await pulled.RejectAsync(false, CancellationToken.None).ConfigureAwait(false);
            throw;
        }

        return new PulledMessage<T>(payload, envelope, delivery, pulled);
    }

    /// <summary>Takes one message, waiting up to five seconds for it.</summary>
    public Task<PulledMessage<T>?> PullAsync<T>(string queue) =>
        PullAsync<T>(queue, TimeSpan.FromSeconds(5));

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

    /// <summary>
    /// Records what became of one delivery, under the outcome the settle decided.
    /// </summary>
    /// <remarks>
    /// The outcome is an argument rather than something worked out from
    /// <paramref name="ack"/>. Deriving it from the handler's answer was the bug: a
    /// message that used up its last attempt was counted as retried and its span
    /// tagged <c>retried</c>, so <c>acemq.messages.dead.lettered.total</c> only ever
    /// saw the dead-letters a handler asked for by name, and a trace backend queried
    /// for dead-lettered messages found none. Parking still reports as dead-lettered:
    /// the queues differ because the two need different people, but the number an
    /// operator alerts on is "messages this consumer could not handle", and splitting
    /// it would mean every dashboard had to add the two back together.
    /// </remarks>
    private static void RecordConsume(
        string queue, Envelope envelope, int attempt, Ack ack, string outcome,
        TimeSpan elapsed, System.Diagnostics.Activity? span)
    {
        var tags = new System.Diagnostics.TagList
        {
            { MetricNames.TagQueue, queue },
            { MetricNames.TagMessageType, envelope.Type },
            { MetricNames.TagOutcome, outcome },
        };

        AceMqTelemetry.ConsumeDuration.Record(elapsed.TotalSeconds, tags);
        AceMqTelemetry.ConsumeTotal.Add(1, tags);
        AceMqTelemetry.ConsumeAttempts.Record(attempt, tags);

        if (outcome == MetricNames.OutcomeRetried) AceMqTelemetry.RetriedTotal.Add(1, tags);
        if (outcome == MetricNames.OutcomeDeadLettered) AceMqTelemetry.DeadLetteredTotal.Add(1, tags);

        // Set last, and deliberately: MessageRetried tagged this span `retried` while
        // the settle was still deciding, and on the last permitted attempt the settle
        // decides dead_lettered. The counter above and the span below therefore always
        // carry the same value, which is the invariant the test asserts.
        AceMqTelemetry.Outcome(span, outcome);
        span?.SetTag(AceMqTelemetry.AttrAttempt, (long)attempt);
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
