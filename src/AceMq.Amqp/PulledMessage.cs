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
/// One message taken from a queue by <see cref="AceMqConnection.PullAsync{T}(string, TimeSpan)"/>,
/// still held by the broker until it is settled.
/// </summary>
/// <remarks>
/// <para>
/// The difference from a consumed message is who decides when it is finished.
/// A handler returns an <see cref="Ack"/> and the consumer settles for it; here
/// the caller settles, which is what lets a job do work of its own between
/// taking the message and admitting it is done.
/// </para>
/// <para>
/// <strong>Settle it.</strong> A message that is never acknowledged or rejected
/// stays unacknowledged until the connection closes, and the broker then hands
/// it to somebody else — so the work happens twice. Where the work between can
/// throw, settle in a <c>finally</c>:
/// </para>
/// <example>
/// <code>
/// var message = await mq.PullAsync&lt;OrderPlaced&gt;("orders");
/// if (message == null) return;
///
/// try
/// {
///     await Place(message.Payload);
///     await message.AcknowledgeAsync();
/// }
/// catch (Exception)
/// {
///     await message.RejectAsync(requeue: true);
///     throw;
/// }
/// </code>
/// </example>
/// </remarks>
public sealed class PulledMessage<T>
{
    private readonly IPulledDelivery _delivery;
    private int _settled;

    internal PulledMessage(
        T payload, Envelope envelope, InboundDelivery delivery, IPulledDelivery pulled)
    {
        Payload = payload;
        Envelope = envelope;
        Queue = delivery.Queue;
        RoutingKey = delivery.RoutingKey;
        ContentType = delivery.ContentType;
        Redelivered = delivery.Redelivered;
        Body = delivery.Body;

        var application = new Dictionary<string, object>();
        foreach (var pair in delivery.Headers)
        {
            // Reserved headers are the engine's and never reach the
            // application, exactly as they do not for a consumed message.
            if (!AceHeaders.IsAceHeader(pair.Key)) application[pair.Key] = pair.Value;
        }
        Headers = application;

        _delivery = pulled;
    }

    /// <summary>The body, read through the connection's codec.</summary>
    public T Payload { get; }

    /// <summary>Identity, causation and the counters.</summary>
    public Envelope Envelope { get; }

    /// <summary>Application headers. Never contains anything reserved.</summary>
    public IReadOnlyDictionary<string, object> Headers { get; }

    /// <summary>The queue it came from.</summary>
    public string Queue { get; }

    /// <summary>The key it arrived under.</summary>
    public string RoutingKey { get; }

    /// <summary>What the sender said the body was.</summary>
    public string? ContentType { get; }

    /// <summary>The broker saying it has handed this over before.</summary>
    public bool Redelivered { get; }

    /// <summary>The undecoded body, for a caller that wants the bytes.</summary>
    public byte[] Body { get; }

    /// <summary>Whether this message has already been settled.</summary>
    public bool IsSettled => Volatile.Read(ref _settled) != 0;

    /// <summary>Confirms it. The broker removes it from the queue.</summary>
    /// <remarks>
    /// Settling twice does nothing rather than failing: a caller with an
    /// explicit call and a <c>finally</c> is the ordinary way to arrive here
    /// twice, and on RabbitMQ a second acknowledgement is a channel error.
    /// </remarks>
    public Task AcknowledgeAsync() => AcknowledgeAsync(CancellationToken.None);

    /// <inheritdoc cref="AcknowledgeAsync()"/>
    public Task AcknowledgeAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0) return Task.CompletedTask;
        return _delivery.AcknowledgeAsync(cancellationToken);
    }

    /// <summary>
    /// Returns it. With <paramref name="requeue"/> it goes back on the queue;
    /// without, it is dead-lettered or dropped as the queue is configured.
    /// </summary>
    public Task RejectAsync(bool requeue) => RejectAsync(requeue, CancellationToken.None);

    /// <inheritdoc cref="RejectAsync(bool)"/>
    public Task RejectAsync(bool requeue, CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0) return Task.CompletedTask;
        return _delivery.RejectAsync(requeue, cancellationToken);
    }

    public override string ToString() =>
        $"PulledMessage[{Envelope.Id} from {Queue}, settled={IsSettled}]";
}
