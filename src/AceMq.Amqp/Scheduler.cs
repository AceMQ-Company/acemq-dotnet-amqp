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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>
/// Delivering a message later.
/// </summary>
/// <remarks>
/// <para>
/// <code>
/// using var scheduler = await Scheduler.OnAsync(mq);
/// await scheduler.InAsync(TimeSpan.FromHours(4), "billing", "invoice.due", invoice);
/// await scheduler.AtAsync(renewalDate, "policies", "policy.renew", policy);
/// </code>
/// </para>
/// <para>
/// <strong>Why not a per-message time to live.</strong> The obvious implementation is
/// to set <c>expiration</c> on the message, drop it in a queue nobody consumes, and
/// let it dead-letter to its destination. It is what most articles suggest and it is
/// wrong for anything but a single fixed delay, because <strong>a classic queue
/// expires messages only at its head</strong>.
/// </para>
/// <para>
/// Put a four-hour message in, then a one-minute message behind it, and the one-minute
/// message is delivered in four hours. Nothing reports this: the queue looks healthy,
/// the message is not lost, it is simply late by a factor nobody predicted. It is the
/// single most common way a home-made scheduler fails, and it fails in production
/// under mixed load rather than in testing under uniform load.
/// </para>
/// <para>
/// <strong>What this does instead.</strong> A small ladder of queues, each with a
/// <em>uniform</em> time to live, and a message hops through them until it is due:
/// </para>
/// <para>
/// <c>acemq.schedule.1s acemq.schedule.10s acemq.schedule.1m acemq.schedule.10m
/// acemq.schedule.1h</c>
/// </para>
/// <para>
/// Every message in a given rung has the same delay, so head-of-line expiry is not a
/// problem — the head is always the message due soonest. Each expiry returns the
/// message to this scheduler, which either delivers it or puts it in the largest rung
/// that does not overshoot. A four-hour delay is four one-hour hops; a ninety-second
/// delay is one minute, then three tens.
/// </para>
/// <para>
/// The cost is honest and worth stating: a long delay is several broker round trips
/// rather than one, and delivery is accurate to about the smallest rung rather than to
/// the second. A scheduler that must fire at 09:00:00.000 exactly is a scheduler, not
/// a message broker.
/// </para>
/// <para>
/// The alternative is RabbitMQ's delayed-message-exchange plugin, which does this
/// properly and is a plugin — so it is not available everywhere, and a library that
/// silently required it would be a library that works on your laptop.
/// </para>
/// <para>
/// <strong>This is a wire contract.</strong> Every name, argument and header below is
/// shared with <c>org.acemq.amqp.patterns.Scheduler</c> and is not free to change
/// here. A .NET service and a Java service scheduling through one broker declare the
/// same five rungs and the same control queue; a difference in a single argument is a
/// <c>PRECONDITION_FAILED</c> on whichever starts second, and a difference in a header
/// name is a message that waits its delay and is then dropped for want of a
/// destination.
/// </para>
/// </remarks>
public sealed class Scheduler : IDisposable
{
    /// <summary>Where a message waits, and where it comes back to be re-examined.</summary>
    public const string Exchange = "acemq.schedule";

    /// <summary>The queue every expired message returns to.</summary>
    public const string ControlQueue = "acemq.schedule.due";

    // Deliberately not the "x-acemq-" prefix. That one is reserved:
    // AceHeaders.IsAceHeader matches it, Envelope.FromWire drops every header carrying
    // it from the application's view on the way in, and Envelope.Builder.Header
    // refuses to write one at all. A scheduler header using it would be rejected on
    // publish here and silently gone on consume in Java -- which is exactly what
    // happened while the Java class was being written, and cost an afternoon.

    /// <summary>Where a scheduled message should eventually go.</summary>
    public const string TargetExchangeHeader = "x-schedule-exchange";

    /// <summary>The routing key it should eventually carry.</summary>
    public const string TargetRoutingKeyHeader = "x-schedule-routing-key";

    /// <summary>When it is due, as epoch milliseconds.</summary>
    /// <remarks>
    /// An integer, not an ISO-8601 string, because Java writes
    /// <c>Instant.toEpochMilli()</c> and the two implementations have to put the same
    /// bytes on the wire. It is read back with <see cref="Convert.ToInt64(object)"/>
    /// so that a broker or client which widens or narrows the AMQP integer type in
    /// transit does not turn a due date into a parse failure.
    /// </remarks>
    public const string DueAtHeader = "x-schedule-due-at";

    /// <summary>
    /// What the payload was encoded as when it was scheduled.
    /// </summary>
    /// <remarks>
    /// Carried because the scheduler republishes bytes rather than objects, and a
    /// consumer picks its codec from the content type. Publishing pre-encoded bytes
    /// under <c>application/octet-stream</c> produces a message the intended consumer
    /// cannot decode — it arrives, it is the right bytes, and nothing can read it. The
    /// outbox relay had exactly this bug once; unlike the relay, a scheduler moves
    /// whatever it is given, so it cannot assume JSON and has to remember.
    /// </remarks>
    public const string ContentTypeHeader = "x-schedule-content-type";

    /// <summary>The message type the scheduler's own publishes carry.</summary>
    private const string ScheduledType = "ScheduledMessage";

    /// <summary>
    /// The rungs, longest first.
    /// </summary>
    /// <remarks>
    /// Five of them, spanning a second to an hour. More rungs mean finer accuracy and
    /// more queues; fewer mean more hops for a long delay. This spread delivers a
    /// one-day message in twenty-four hops and a one-minute message in one, which is
    /// the right way round — short delays are common and want to be cheap.
    /// </remarks>
    private static readonly TimeSpan[] RungLadder =
    {
        TimeSpan.FromHours(1),
        TimeSpan.FromMinutes(10),
        TimeSpan.FromMinutes(1),
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(1),
    };

    private readonly AceMqConnection _mq;
    private readonly Dictionary<string, IPublisher<byte[]>> _publishers =
        new Dictionary<string, IPublisher<byte[]>>();
    private IMessageConsumer? _due;
    private long _scheduled;
    private long _delivered;
    private long _hops;
    private bool _disposed;

    private Scheduler(AceMqConnection mq) => _mq = mq;

    /// <summary>The rungs, longest first, as they are declared.</summary>
    public static IReadOnlyList<TimeSpan> Rungs => (TimeSpan[])RungLadder.Clone();

    /// <summary>The queue a delay of this length waits in.</summary>
    public static string RungName(TimeSpan rung) => Exchange + "." + Describe(rung);

    /// <summary>
    /// Starts a scheduler on an open connection, declaring its queues.
    /// </summary>
    /// <remarks>
    /// The topology is shared, so two schedulers on one broker declare the same six
    /// queues and one exchange rather than a set each. Constructing a second one is
    /// therefore cheap and not a mistake — but it does start a second consumer on
    /// <see cref="ControlQueue"/>, and the work is then split between them.
    /// </remarks>
    public static async Task<Scheduler> OnAsync(AceMqConnection mq)
    {
        if (mq == null) throw new ArgumentNullException(nameof(mq));

        var scheduler = new Scheduler(mq);
        await scheduler.DeclareTopologyAsync().ConfigureAwait(false);

        // Raw bytes: this scheduler never looks inside a payload, and decoding one it
        // has no business understanding is how a scheduler acquires opinions about
        // message formats. Java uses the same prefetch.
        //
        // A private queue, and that is the load-bearing part. Every ordinary consumer
        // declares {queue}.dlq and {queue}.parked when it starts, so consuming the
        // control queue the ordinary way would put acemq.schedule.due.dlq and
        // acemq.schedule.due.parked on the broker of every service that ever
        // constructed a Scheduler -- two durable queues nothing publishes to and
        // nobody drains. The handler below earns that opt-out by never giving up: it
        // decodes to byte[], which cannot fail, and it returns Ack.Accept on every
        // path including the ones it reports as errors.
        scheduler._due = await mq.ConsumePrivateQueueAsync<byte[]>(
            ControlQueue,
            ConsumerOptions.Prefetch(50).As(new BytesCodec()),
            message => scheduler.ForwardAsync(message)).ConfigureAwait(false);

        return scheduler;
    }

    /// <summary>Delivers a message after a delay.</summary>
    /// <param name="delay">How long to wait; zero or negative delivers immediately.</param>
    /// <param name="exchange">Where it should eventually go.</param>
    /// <param name="routingKey">The routing key it should eventually carry.</param>
    /// <param name="payload">The message.</param>
    public Task InAsync(TimeSpan delay, string exchange, string routingKey, object payload) =>
        AtAsync(DateTimeOffset.UtcNow.Add(delay), exchange, routingKey, payload);

    /// <summary>Delivers a message at a moment.</summary>
    /// <param name="when">The moment; anything in the past delivers immediately.</param>
    /// <param name="exchange">Where it should eventually go.</param>
    /// <param name="routingKey">The routing key it should eventually carry.</param>
    /// <param name="payload">The message.</param>
    public Task AtAsync(DateTimeOffset when, string exchange, string routingKey, object payload)
    {
        if (exchange == null) throw new ArgumentNullException(nameof(exchange));
        if (routingKey == null) throw new ArgumentNullException(nameof(routingKey));
        EnsureOpen();

        // Encoded once, here, and carried as bytes from then on. The content type goes
        // with them, because that is how the eventual consumer chooses a codec.
        var codec = _mq.Codec;
        var headers = new Dictionary<string, object>
        {
            [TargetExchangeHeader] = exchange,
            [TargetRoutingKeyHeader] = routingKey,
            [DueAtHeader] = when.ToUnixTimeMilliseconds(),
            [ContentTypeHeader] = codec.ContentType,
        };

        Interlocked.Increment(ref _scheduled);
        return RouteAsync(codec.Encode(payload), headers, when);
    }

    /// <summary>Messages handed to this scheduler.</summary>
    public long Scheduled => Interlocked.Read(ref _scheduled);

    /// <summary>Messages that reached their destination.</summary>
    public long Delivered => Interlocked.Read(ref _delivered);

    /// <summary>
    /// How many times a message moved between rungs.
    /// </summary>
    /// <remarks>
    /// Divided by <see cref="Delivered"/> this is the average number of hops, which is
    /// the number to look at if the scheduler is busier than expected: long delays
    /// cost hops.
    /// </remarks>
    public long Hops => Interlocked.Read(ref _hops);

    /// <summary>Called for every message that has come out of a rung.</summary>
    private async Task<Ack> ForwardAsync(IMessage<byte[]> message)
    {
        // Never throws, and never gives up. See the note on the private-queue
        // subscription above: this consumer has no dead-letter queue to give up into,
        // by design, so anything it cannot handle is reported and dropped rather than
        // republished into a queue that is not there.
        try
        {
            var headers = message.Headers;
            if (!headers.TryGetValue(DueAtHeader, out var dueAt) || dueAt == null
                || !headers.TryGetValue(TargetExchangeHeader, out var exchange) || exchange == null
                || !headers.TryGetValue(TargetRoutingKeyHeader, out var routingKey) || routingKey == null)
            {
                AceMqDiagnostics.Report(
                    AceMqDiagnostics.ScheduleForeign, DiagnosticLevel.Error,
                    $"a message reached {ControlQueue} without the headers a scheduled message"
                    + " carries, and was dropped. Something else is publishing into the"
                    + " scheduler's queues, which it must not: they are an implementation"
                    + " detail of this class.",
                    ControlQueue, null, message.Envelope.Id, message.Attempt, null);
                return Ack.Accept();
            }

            var carried = new Dictionary<string, object>
            {
                [TargetExchangeHeader] = Text(exchange),
                [TargetRoutingKeyHeader] = Text(routingKey),
                [DueAtHeader] = Convert.ToInt64(dueAt, CultureInfo.InvariantCulture),
            };
            if (headers.TryGetValue(ContentTypeHeader, out var contentType) && contentType != null)
            {
                carried[ContentTypeHeader] = Text(contentType);
            }

            var when = DateTimeOffset.FromUnixTimeMilliseconds(
                Convert.ToInt64(dueAt, CultureInfo.InvariantCulture));
            await RouteAsync(message.Payload, carried, when).ConfigureAwait(false);
            return Ack.Accept();
        }
        catch (Exception failure)
        {
            AceMqDiagnostics.Report(
                AceMqDiagnostics.ScheduleForeign, DiagnosticLevel.Error,
                $"a message from {ControlQueue} could not be moved on and was dropped:"
                + $" {failure.Message}",
                ControlQueue, null, message.Envelope.Id, message.Attempt, failure);
            return Ack.Accept();
        }
    }

    /// <summary>
    /// Delivers if it is due, and otherwise puts it in the largest rung that does not
    /// overshoot.
    /// </summary>
    private Task RouteAsync(byte[] payload, IDictionary<string, object> headers, DateTimeOffset when)
    {
        var remaining = when - DateTimeOffset.UtcNow;
        var smallest = RungLadder[RungLadder.Length - 1];

        if (remaining <= TimeSpan.Zero || remaining < smallest)
        {
            // Due, or so nearly due that another hop would cost more than the accuracy
            // it buys.
            return DeliverAsync(payload, headers);
        }

        var rung = smallest;
        foreach (var candidate in RungLadder)
        {
            if (candidate <= remaining) { rung = candidate; break; }
        }

        Interlocked.Increment(ref _hops);
        var envelope = Envelope.Of(ScheduledType);
        foreach (var pair in headers) envelope.Header(pair.Key, pair.Value);

        return PublisherFor(Exchange, RungName(rung), new BytesCodec())
            .SendAsync(payload, envelope.Build());
    }

    private Task DeliverAsync(byte[] payload, IDictionary<string, object> headers)
    {
        var exchange = Text(headers[TargetExchangeHeader]);
        var routingKey = Text(headers[TargetRoutingKeyHeader]);
        var contentType = headers.TryGetValue(ContentTypeHeader, out var declared) && declared != null
            ? Text(declared)
            : "application/json";

        // The scheduler's own headers are not passed on: they are bookkeeping, and a
        // consumer that started depending on them would be depending on how a message
        // got to it.
        Interlocked.Increment(ref _delivered);
        return PublisherFor(exchange, routingKey, new VerbatimCodec(contentType))
            .SendAsync(payload, Envelope.Of(ScheduledType).Build());
    }

    /// <summary>
    /// A publisher for one destination, cached.
    /// </summary>
    /// <remarks>
    /// Publishers are long lived and the connection keeps every one it hands out until
    /// it is disposed, so building one per hop would grow that list by one entry per
    /// message. The outbox relay caches for the same reason.
    /// </remarks>
    private IPublisher<byte[]> PublisherFor(string exchange, string routingKey, ICodec codec)
    {
        var key = codec.ContentType + " " + exchange + " " + routingKey;
        lock (_publishers)
        {
            if (_publishers.TryGetValue(key, out var existing)) return existing;
            var publisher = _mq.Publisher<byte[]>(
                exchange, routingKey, PublishOptions.Defaults(), null, codec);
            _publishers[key] = publisher;
            return publisher;
        }
    }

    /// <summary>
    /// The exchange, the five rungs and the control queue.
    /// </summary>
    /// <remarks>
    /// Idempotent, and identical to what Java declares — the same queue names, the
    /// same three arguments per rung in the same shape, and the same bindings. That
    /// identity is the contract: a broker that already holds Java's version of these
    /// queues accepts this declaration unchanged, and would refuse it outright if any
    /// argument differed.
    /// </remarks>
    private async Task DeclareTopologyAsync()
    {
        await _mq.DeclareExchangeAsync(Exchange, "direct").ConfigureAwait(false);

        foreach (var rung in RungLadder)
        {
            var arguments = new Dictionary<string, object>
            {
                [RetryLadder.MessageTtlArgument] = (long)rung.TotalMilliseconds,
                [RetryLadder.DeadLetterExchangeArgument] = Exchange,
                [RetryLadder.DeadLetterRoutingKeyArgument] = ControlQueue,
            };

            // Classic, and every message in this queue has the same delay, so the head
            // is always the one due soonest. That is what makes head-of-line expiry
            // harmless here -- and it is the reason the delay lives on the queue rather
            // than on the message.
            var name = RungName(rung);
            await _mq.DeclareQueueAsync(name, QueueType.Classic, arguments).ConfigureAwait(false);
            await _mq.BindAsync(name, Exchange, name).ConfigureAwait(false);
        }

        await _mq.DeclareQueueAsync(ControlQueue, QueueType.Classic, null).ConfigureAwait(false);
        await _mq.BindAsync(ControlQueue, Exchange, ControlQueue).ConfigureAwait(false);
    }

    /// <summary>The rung suffix: hours, else minutes, else seconds.</summary>
    private static string Describe(TimeSpan rung)
    {
        var millis = (long)rung.TotalMilliseconds;
        if (millis % 3600000 == 0)
        {
            return (millis / 3600000).ToString(CultureInfo.InvariantCulture) + "h";
        }
        if (millis % 60000 == 0)
        {
            return (millis / 60000).ToString(CultureInfo.InvariantCulture) + "m";
        }
        return (millis / 1000).ToString(CultureInfo.InvariantCulture) + "s";
    }

    /// <summary>
    /// A header value as text.
    /// </summary>
    /// <remarks>
    /// A string header comes back as a string from this library's transports, which
    /// convert the AMQP long-string themselves. Going through
    /// <see cref="Convert.ToString(object, IFormatProvider)"/> anyway costs nothing
    /// and means a client that hands back something else does not produce a cast
    /// exception at the point a message is being delivered.
    /// </remarks>
    private static string Text(object value) =>
        Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

    private void EnsureOpen()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(Scheduler));
    }

    /// <summary>Stops consuming. The queues stay: they are shared and may hold messages.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _due?.Dispose();
        lock (_publishers)
        {
            foreach (var publisher in _publishers.Values) publisher.Dispose();
            _publishers.Clear();
        }
    }

    public override string ToString() =>
        $"Scheduler{{rungs={RungLadder.Length}, scheduled={Scheduled}, delivered={Delivered}}}";

    /// <summary>
    /// Writes already-encoded bytes out unchanged, under the content type they were
    /// encoded as.
    /// </summary>
    /// <remarks>
    /// Publishing them through an ordinary codec would encode them a second time, and
    /// what arrives is JSON containing JSON. Publishing them as raw bytes loses the
    /// content type, and what arrives cannot be decoded by the consumer that was
    /// waiting for it.
    /// </remarks>
    private sealed class VerbatimCodec : ICodec
    {
        internal VerbatimCodec(string contentType) => ContentType = contentType;

        public string ContentType { get; }

        public byte[] Encode(object payload) =>
            payload as byte[] ?? throw new AceFatalException(
                "the scheduler publishes the bytes it was given, not " + payload?.GetType().Name);

        public object Decode(byte[] body, Type target) =>
            throw new NotSupportedException("the scheduler only publishes");

        public bool CanDecode(string? contentType) => false;

        public override string ToString() => $"Scheduler.Verbatim[{ContentType}]";
    }
}
