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
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>What to do with a message once the handler has seen it.</summary>
/// <remarks>
/// <para>
/// A handler returns one of these rather than throwing to signal success or
/// failure. An exception escaping the handler is still handled — it is treated as
/// <see cref="Retry"/> — but returning the disposition says what was meant, and
/// distinguishes "this will never work" from "try again shortly", which an
/// exception on its own cannot.
/// </para>
/// </remarks>
public sealed class Ack
{
    private Ack(AckKind kind, TimeSpan? delay, string? reason)
    {
        Kind = kind;
        Delay = delay;
        Reason = reason;
    }

    /// <summary>Handled. The broker may forget it.</summary>
    public static Ack Accept() => new Ack(AckKind.Accept, null, null);

    /// <summary>Deliver it again after a delay, because this attempt failed recoverably.</summary>
    public static Ack Retry(TimeSpan after, string reason) =>
        new Ack(AckKind.Retry, after, reason ?? throw new ArgumentNullException(nameof(reason)));

    /// <summary>
    /// Send it to the dead-letter queue: this message will never succeed, and
    /// retrying only delays the discovery.
    /// </summary>
    public static Ack DeadLetter(string reason) =>
        new Ack(AckKind.DeadLetter, null, reason ?? throw new ArgumentNullException(nameof(reason)));

    /// <summary>
    /// Give it back to the broker for someone else, without counting an attempt.
    /// </summary>
    public static Ack Release() => new Ack(AckKind.Release, null, null);

    public AckKind Kind { get; }

    /// <summary>How long to wait before redelivery, when this is a retry.</summary>
    public TimeSpan? Delay { get; }

    /// <summary>Why, when there is a why. Carried onto the dead-letter header.</summary>
    public string? Reason { get; }

    public bool IsAccept => Kind == AckKind.Accept;
    public bool IsRetry => Kind == AckKind.Retry;
    public bool IsDeadLetter => Kind == AckKind.DeadLetter;
    public bool IsRelease => Kind == AckKind.Release;

    public override string ToString() =>
        Reason == null ? Kind.ToString() : $"{Kind}({Reason})";
}

/// <summary>The dispositions a handler can return.</summary>
public enum AckKind
{
    Accept,
    Retry,
    DeadLetter,
    Release,
}

/// <summary>A decoded message and everything that arrived with it.</summary>
public interface IMessage<out T>
{
    T Payload { get; }

    /// <summary>Identity, causation and the retry counters.</summary>
    Envelope Envelope { get; }

    /// <summary>Application headers. Never contains anything in the reserved namespace.</summary>
    IReadOnlyDictionary<string, object> Headers { get; }

    string? RoutingKey { get; }

    string Queue { get; }

    DateTimeOffset ReceivedAt { get; }

    /// <summary>
    /// Delivery attempt, starting at 1.
    /// </summary>
    /// <remarks>
    /// Counted by this consumer rather than read from the envelope, because a broker
    /// redelivers the original bytes and the header a publisher wrote never advances.
    /// The count is per process: a consumer restart begins it again.
    /// </remarks>
    int Attempt { get; }

    bool IsFirstAttempt { get; }

    string? ContentType { get; }

    string? ReplyTo { get; }
}

internal sealed class ReceivedMessage<T> : IMessage<T>
{
    internal ReceivedMessage(T payload, Envelope envelope, InboundDelivery delivery, int attempt)
    {
        Payload = payload;
        Envelope = envelope;
        Attempt = attempt;
        Headers = envelope.Headers;
        RoutingKey = delivery.RoutingKey;
        Queue = delivery.Queue;
        ContentType = delivery.ContentType;
        ReplyTo = delivery.ReplyTo;
        ReceivedAt = DateTimeOffset.UtcNow;
    }

    public T Payload { get; }
    public Envelope Envelope { get; }
    public IReadOnlyDictionary<string, object> Headers { get; }
    public string? RoutingKey { get; }
    public string Queue { get; }
    public DateTimeOffset ReceivedAt { get; }
    public int Attempt { get; }
    public bool IsFirstAttempt => Attempt <= 1;
    public string? ContentType { get; }
    public string? ReplyTo { get; }
}

/// <summary>How a consumer should behave.</summary>
public sealed class ConsumerOptions
{
    private ConsumerOptions(int prefetch, ICodec? codec, bool requeueOnFailure, TimeSpan retryDelay)
    {
        PrefetchCount = prefetch;
        Codec = codec;
        RequeueOnFailure = requeueOnFailure;
        RetryDelay = retryDelay;
    }

    public static ConsumerOptions Defaults() =>
        new ConsumerOptions(20, null, false, TimeSpan.FromSeconds(5));

    /// <summary>
    /// How many unacknowledged messages the broker may have outstanding with this
    /// consumer.
    /// </summary>
    /// <remarks>
    /// The default of 20 is deliberately modest. An unbounded prefetch hands a
    /// single consumer the whole queue, which turns a rolling deploy into a stall
    /// while one instance works through everything it was given.
    /// </remarks>
    public static ConsumerOptions Prefetch(int prefetch)
    {
        if (prefetch < 1) throw new ArgumentException("must be at least 1", nameof(prefetch));
        return new ConsumerOptions(prefetch, null, false, TimeSpan.FromSeconds(5));
    }

    public ConsumerOptions WithPrefetch(int prefetch) =>
        new ConsumerOptions(prefetch, Codec, RequeueOnFailure, RetryDelay);

    /// <summary>Decodes with this codec rather than the connection's.</summary>
    public ConsumerOptions As(ICodec codec) =>
        new ConsumerOptions(PrefetchCount, codec, RequeueOnFailure, RetryDelay);

    /// <summary>
    /// Returns a failed message to the queue rather than dead-lettering it.
    /// </summary>
    /// <remarks>
    /// Off by default, because requeueing a message that fails deterministically
    /// produces a hot loop that looks like throughput.
    /// </remarks>
    public ConsumerOptions RequeueingOnFailure() =>
        new ConsumerOptions(PrefetchCount, Codec, true, RetryDelay);

    public ConsumerOptions WithRetryDelay(TimeSpan delay) =>
        new ConsumerOptions(PrefetchCount, Codec, RequeueOnFailure, delay);

    public int PrefetchCount { get; }
    public ICodec? Codec { get; }
    public bool RequeueOnFailure { get; }

    /// <summary>Delay applied when a handler throws rather than returning a disposition.</summary>
    public TimeSpan RetryDelay { get; }
}

/// <summary>A running consumer. Disposing it stops delivery.</summary>
public interface IMessageConsumer : IDisposable
{
    string Queue { get; }
    bool IsActive { get; }
}

internal sealed class MessageConsumer : IMessageConsumer
{
    private readonly ISubscription _subscription;

    internal MessageConsumer(ISubscription subscription) => _subscription = subscription;

    public string Queue => _subscription.Queue;
    public bool IsActive => _subscription.IsActive;
    public void Dispose() => _subscription.Dispose();
}
