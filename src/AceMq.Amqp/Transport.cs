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

/// <summary>What a broker can do, so callers can ask instead of assuming.</summary>
public enum Capability
{
    PublisherConfirms,
    DeadLettering,
    DelayedDelivery,
    Streams,
    Priority,
    Transactions,
}

/// <summary>The kinds of queue a transport may support.</summary>
public enum QueueType
{
    Classic,
    Quorum,
    Stream,
}

/// <summary>A message on its way to the broker, after encoding.</summary>
public sealed class OutboundMessage
{
    public OutboundMessage(
        string exchange, string routingKey, byte[] body,
        IReadOnlyDictionary<string, object> headers,
        string? messageId, string? contentType,
        bool persistent, bool mandatory,
        TimeSpan? expiration, int? priority, string? replyTo)
    {
        Exchange = exchange;
        RoutingKey = routingKey;
        Body = body;
        Headers = headers;
        MessageId = messageId;
        ContentType = contentType;
        Persistent = persistent;
        Mandatory = mandatory;
        Expiration = expiration;
        Priority = priority;
        ReplyTo = replyTo;
    }

    public string Exchange { get; }
    public string RoutingKey { get; }
    public byte[] Body { get; }
    public IReadOnlyDictionary<string, object> Headers { get; }
    public string? MessageId { get; }
    public string? ContentType { get; }
    public bool Persistent { get; }

    /// <summary>Whether the broker must report the message as unroutable rather than dropping it.</summary>
    public bool Mandatory { get; }

    public TimeSpan? Expiration { get; }
    public int? Priority { get; }
    public string? ReplyTo { get; }
}

/// <summary>A message as it arrived, before decoding.</summary>
public sealed class InboundDelivery
{
    public InboundDelivery(
        string queue, string exchange, string routingKey, byte[] body,
        IReadOnlyDictionary<string, object> headers,
        string? messageId, string? contentType, bool redelivered, string? replyTo)
    {
        Queue = queue;
        Exchange = exchange;
        RoutingKey = routingKey;
        Body = body;
        Headers = headers;
        MessageId = messageId;
        ContentType = contentType;
        Redelivered = redelivered;
        ReplyTo = replyTo;
    }

    public string Queue { get; }
    public string Exchange { get; }
    public string RoutingKey { get; }
    public byte[] Body { get; }
    public IReadOnlyDictionary<string, object> Headers { get; }
    public string? MessageId { get; }
    public string? ContentType { get; }

    /// <summary>Whether the broker has delivered this message before.</summary>
    public bool Redelivered { get; }

    public string? ReplyTo { get; }
}

/// <summary>What the broker said about a publish.</summary>
public sealed class ConfirmResult
{
    private ConfirmResult(bool confirmed, bool routed, string? reason)
    {
        Confirmed = confirmed;
        Routed = routed;
        Reason = reason;
    }

    public static ConfirmResult Ok(bool routed) => new ConfirmResult(true, routed, null);
    public static ConfirmResult Rejected(string reason) => new ConfirmResult(false, false, reason);

    /// <summary>Whether the broker took responsibility for the message.</summary>
    public bool Confirmed { get; }

    /// <summary>Whether it reached at least one queue.</summary>
    public bool Routed { get; }

    public string? Reason { get; }
}

/// <summary>A running subscription, cancelled by disposing it.</summary>
public interface ISubscription : IDisposable
{
    string Queue { get; }
    bool IsActive { get; }
}

/// <summary>
/// What the engine asks of a broker. Implement this to add a transport; nothing
/// above it knows which broker is underneath.
/// </summary>
public interface ITransportConnection : IDisposable
{
    Task DeclareExchangeAsync(string name, string type, bool durable, CancellationToken cancellationToken);

    Task DeclareQueueAsync(
        string name, QueueType type, bool durable,
        IReadOnlyDictionary<string, object>? arguments, CancellationToken cancellationToken);

    Task BindQueueAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken);

    Task<ConfirmResult> SendAsync(OutboundMessage message, CancellationToken cancellationToken);

    /// <summary>
    /// Starts delivering to <paramref name="handler"/>, which returns the disposition
    /// for each message. The transport acknowledges according to that result.
    /// </summary>
    /// <remarks>
    /// <paramref name="arguments"/> carries consumer arguments such as a stream's
    /// starting offset, and is null for an ordinary queue.
    /// </remarks>
    Task<ISubscription> SubscribeAsync(
        string queue, int prefetch,
        IReadOnlyDictionary<string, object>? arguments,
        Func<InboundDelivery, Task<Ack>> handler,
        CancellationToken cancellationToken);

    /// <summary>
    /// Takes one message off a queue, or returns null if none arrives within
    /// <paramref name="timeout"/>.
    /// </summary>
    /// <remarks>
    /// Pulling rather than subscribing is what lets a replay drain a fixed number of
    /// messages and stop. A subscription would keep receiving the ones it just
    /// republished.
    /// </remarks>
    Task<InboundDelivery?> ReceiveAsync(
        string queue, TimeSpan timeout, CancellationToken cancellationToken);

    Task<long> MessageCountAsync(string queue, CancellationToken cancellationToken);

    Task DeleteQueueAsync(string name, CancellationToken cancellationToken);

    Task<bool> QueueExistsAsync(string name, CancellationToken cancellationToken);

    /// <summary>
    /// Compares an existing queue against what would be declared.
    /// </summary>
    /// <remarks>
    /// Separate from declaring it, so a topology can be reviewed before it is
    /// applied. A transport that cannot tell returns
    /// <see cref="QueueCheckResult.Unsupported"/> rather than guessing: a plan that
    /// says "would create" about something already there stops being read.
    /// </remarks>
    Task<QueueCheck> CheckQueueAsync(
        string name, QueueType type, bool durable,
        IReadOnlyDictionary<string, object>? arguments, CancellationToken cancellationToken);

    bool IsOpen { get; }

    bool IsBlocked { get; }

    string? BlockedReason { get; }
}

/// <summary>A broker this library can talk to.</summary>
public interface ITransport
{
    /// <summary>URL schemes this transport answers to.</summary>
    IReadOnlyCollection<string> Schemes { get; }

    /// <summary>Name for logs and metrics.</summary>
    string Name { get; }

    IReadOnlyCollection<Capability> Capabilities { get; }

    Task<ITransportConnection> ConnectAsync(ConnectionConfig config, CancellationToken cancellationToken);
}

/// <summary>
/// The transports available to <see cref="AceMqConnection.ConnectAsync(string)"/>,
/// keyed by URL scheme.
/// </summary>
/// <remarks>
/// Registration is explicit rather than discovered by scanning assemblies. The Java
/// library finds its transports with the service loader, and the common first
/// failure there is a connection refused with <em>no transport for scheme amqp</em>
/// because a runtime-only dependency was left off the classpath. Here the
/// registration is a line of code you either wrote or did not, and its absence is a
/// compile-time missing reference rather than a runtime surprise.
/// </remarks>
public static class Transports
{
    private static readonly Dictionary<string, ITransport> Registered =
        new Dictionary<string, ITransport>(StringComparer.OrdinalIgnoreCase);

    static Transports() => Register(new InMemoryTransport());

    /// <summary>Registers a transport for every scheme it claims.</summary>
    public static void Register(ITransport transport)
    {
        if (transport == null) throw new ArgumentNullException(nameof(transport));
        lock (Registered)
        {
            foreach (var scheme in transport.Schemes) Registered[scheme] = transport;
        }
    }

    /// <summary>The transport for a scheme.</summary>
    /// <exception cref="AceFatalException">when nothing is registered for it.</exception>
    public static ITransport ForScheme(string scheme)
    {
        lock (Registered)
        {
            if (Registered.TryGetValue(scheme, out var transport)) return transport;
            var known = string.Join(", ", Names());
            throw new AceFatalException(
                $"no transport registered for scheme '{scheme}'. Registered: {known}. " +
                "Add a reference to the transport package and call " +
                "Transports.Register(new RabbitMqTransport()) before connecting.");
        }
    }

    /// <summary>Schemes with a transport registered, in order.</summary>
    public static IReadOnlyCollection<string> Names()
    {
        lock (Registered)
        {
            var names = new List<string>(Registered.Keys);
            names.Sort(StringComparer.Ordinal);
            return names;
        }
    }
}
