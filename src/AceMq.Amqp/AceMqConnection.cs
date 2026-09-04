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

    public IPublisher<T> Publisher<T>(string exchange, string routingKey, PublishOptions options)
    {
        EnsureOpen();
        var publisher = new Publisher<T>(
            _connection, _codec, exchange, routingKey, options, _inFlight, _config.ConfirmTimeout);
        lock (_owned) _owned.Add(publisher);
        return publisher;
    }

    /// <summary>Starts consuming a queue.</summary>
    public Task<IMessageConsumer> ConsumeAsync<T>(string queue, Func<IMessage<T>, Task<Ack>> handler) =>
        ConsumeAsync(queue, ConsumerOptions.Defaults(), handler);

    public async Task<IMessageConsumer> ConsumeAsync<T>(
        string queue, ConsumerOptions options, Func<IMessage<T>, Task<Ack>> handler)
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
            queue, options.PrefetchCount,
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

                var attempt = attempts.AddOrUpdate(envelope.Id, envelope.Attempt, (_, n) => n + 1);
                var message = new ReceivedMessage<T>(payload, envelope, delivery, attempt);

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
