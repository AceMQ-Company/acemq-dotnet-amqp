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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AceMq.Amqp;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace AceMq.Amqp.RabbitMq;

/// <summary>
/// Talks to RabbitMQ, over <c>amqp://</c> and <c>amqps://</c>.
/// </summary>
/// <remarks>
/// Register it before connecting:
/// <code>
/// Transports.Register(new RabbitMqTransport());
/// var mq = await AceMqConnection.ConnectAsync("amqp://localhost");
/// </code>
/// </remarks>
public sealed class RabbitMqTransport : ITransport
{
    public IReadOnlyCollection<string> Schemes => new[] { "amqp", "amqps" };

    public string Name => "rabbitmq";

    public IReadOnlyCollection<Capability> Capabilities => new[]
    {
        Capability.PublisherConfirms,
        Capability.DeadLettering,
        Capability.Streams,
        Capability.Priority,
        Capability.Transactions,
    };

    public async Task<ITransportConnection> ConnectAsync(
        ConnectionConfig config, CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = new Uri(config.Url),
            ClientProvidedName = config.ClientName,
            RequestedConnectionTimeout = config.ConnectionTimeout,
            AutomaticRecoveryEnabled = true,
        };
        if (config.Username != null) factory.UserName = config.Username;
        if (config.Password != null) factory.Password = config.Password;
        if (config.VirtualHost != null) factory.VirtualHost = config.VirtualHost;

        // Asked at connect time rather than held, so a rotated secret is picked up
        // by the next reconnection instead of the next restart.
        if (config.CredentialsProvider != null)
        {
            var credentials = config.CredentialsProvider.Get();
            if (credentials.Username.Length > 0) factory.UserName = credentials.Username;
            factory.Password = credentials.Secret;
        }

        ApplyTls(factory, config);

        try
        {
            var connection = await factory.CreateConnectionAsync(cancellationToken)
                .ConfigureAwait(false);

            // Confirmation tracking makes BasicPublishAsync await the broker's ack,
            // which is what turns "written to a socket" into "the broker has it".
            var options = new CreateChannelOptions(
                publisherConfirmationsEnabled: config.PublisherConfirms,
                publisherConfirmationTrackingEnabled: config.PublisherConfirms);
            var channel = await connection.CreateChannelAsync(options, cancellationToken)
                .ConfigureAwait(false);

            return new Connection(connection, channel, config);
        }
        catch (BrokerUnreachableException e)
        {
            throw new TransportException($"could not reach the broker at {config.Url}", e);
        }
    }

    /// <summary>Turns <see cref="TlsOptions"/> into the client's TLS settings.</summary>
    private static void ApplyTls(ConnectionFactory factory, ConnectionConfig config)
    {
        var tls = config.Tls;
        if (tls.Mode == TlsMode.Disabled)
        {
            factory.Ssl = new SslOption { Enabled = false };
            return;
        }

        // The name the certificate is checked against. Without it the handshake has
        // no target host and hostname verification cannot happen at all, so this is
        // set even in Insecure mode.
        var serverName = tls.ServerName ?? new Uri(config.Url).Host;

        var ssl = new SslOption
        {
            Enabled = true,
            ServerName = serverName,
            // None lets the OS negotiate the best available version. Pinning a
            // version here would freeze the connection at whatever was current when
            // this was written, and stop it benefiting from later ones.
            Version = System.Security.Authentication.SslProtocols.None,
            CheckCertificateRevocation = tls.CheckRevocation,
            Certs = tls.ClientCertificates,
        };

        if (tls.Mode == TlsMode.Insecure)
        {
            // Everything is accepted. Documented at the call site as development only.
            ssl.AcceptablePolicyErrors =
                System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch
                | System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors
                | System.Net.Security.SslPolicyErrors.RemoteCertificateNotAvailable;
        }
        else if (tls.CertificateAuthority != null)
        {
            ssl.CertificateValidationCallback = TrustOnly(tls.CertificateAuthority, tls.CheckRevocation);
        }

        factory.Ssl = ssl;
    }

    /// <summary>
    /// Accepts a certificate chain that terminates at one specific authority.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The usual mistake is to set <c>AllowUnknownCertificateAuthority</c> and stop
    /// there. That accepts <em>any</em> chain the presenter builds, including a
    /// self-signed one it made a moment ago, which is not far off accepting anything.
    /// The root actually reached has to be compared against the authority being
    /// trusted, which is what the final check does.
    /// </para>
    /// <para>
    /// A hostname mismatch is still rejected. Only the chain's trust anchor is being
    /// overridden, not whether the certificate belongs to the host being talked to.
    /// </para>
    /// </remarks>
    private static System.Net.Security.RemoteCertificateValidationCallback TrustOnly(
        System.Security.Cryptography.X509Certificates.X509Certificate2 authority,
        bool checkRevocation) =>
        (sender, certificate, chain, errors) =>
        {
            if (errors == System.Net.Security.SslPolicyErrors.None) return true;

            // Neither of these is about which authority signed the certificate.
            if ((errors & System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
            {
                return false;
            }
            if ((errors & System.Net.Security.SslPolicyErrors.RemoteCertificateNotAvailable) != 0)
            {
                return false;
            }
            if (certificate == null) return false;

            using var rebuilt = new System.Security.Cryptography.X509Certificates.X509Chain();
            rebuilt.ChainPolicy.ExtraStore.Add(authority);
            rebuilt.ChainPolicy.VerificationFlags = System.Security.Cryptography.X509Certificates
                .X509VerificationFlags.AllowUnknownCertificateAuthority;
            rebuilt.ChainPolicy.RevocationMode = checkRevocation
                ? System.Security.Cryptography.X509Certificates.X509RevocationMode.Online
                : System.Security.Cryptography.X509Certificates.X509RevocationMode.NoCheck;

            var leaf = new System.Security.Cryptography.X509Certificates.X509Certificate2(
                certificate.Export(
                    System.Security.Cryptography.X509Certificates.X509ContentType.Cert));
            if (!rebuilt.Build(leaf)) return false;

            // The chain built, but AllowUnknownCertificateAuthority means it would
            // have built against anything. This is the check that makes it mean
            // something: the anchor has to be the authority we were given.
            var anchor = rebuilt.ChainElements[rebuilt.ChainElements.Count - 1].Certificate;
            return anchor.RawData.Length == authority.RawData.Length
                   && System.Linq.Enumerable.SequenceEqual(anchor.RawData, authority.RawData);
        };

    private sealed class Connection : ITransportConnection
    {
        private readonly IConnection _connection;
        private readonly IChannel _channel;
        private readonly ConnectionConfig _config;
        private readonly SemaphoreSlim _publishLock = new SemaphoreSlim(1, 1);
        private volatile string? _blockedReason;

        internal Connection(IConnection connection, IChannel channel, ConnectionConfig config)
        {
            _connection = connection;
            _channel = channel;
            _config = config;

            _connection.ConnectionBlockedAsync += (_, e) =>
            {
                _blockedReason = e.Reason;
                return Task.CompletedTask;
            };
            _connection.ConnectionUnblockedAsync += (_, _) =>
            {
                _blockedReason = null;
                return Task.CompletedTask;
            };
        }

        public bool IsOpen => _connection.IsOpen && _channel.IsOpen;
        public bool IsBlocked => _blockedReason != null;
        public string? BlockedReason => _blockedReason;

        public Task DeclareExchangeAsync(
            string name, string type, bool durable, CancellationToken cancellationToken) =>
            _channel.ExchangeDeclareAsync(
                name, type, durable, autoDelete: false, arguments: null,
                cancellationToken: cancellationToken);

        public Task DeclareQueueAsync(
            string name, QueueType type, bool durable,
            IReadOnlyDictionary<string, object>? arguments, CancellationToken cancellationToken)
        {
            var args = new Dictionary<string, object?>();
            if (arguments != null)
            {
                foreach (var pair in arguments) args[pair.Key] = pair.Value;
            }

            // A queue's type is fixed at declaration. Declaring an existing queue with
            // a different type fails rather than converting it, which is the broker
            // protecting the messages already in it.
            switch (type)
            {
                case QueueType.Quorum: args["x-queue-type"] = "quorum"; break;
                case QueueType.Stream: args["x-queue-type"] = "stream"; break;
            }

            return _channel.QueueDeclareAsync(
                name, durable, exclusive: false, autoDelete: false,
                arguments: args.Count == 0 ? null : args,
                cancellationToken: cancellationToken);
        }

        public Task BindQueueAsync(
            string queue, string exchange, string routingKey, CancellationToken cancellationToken) =>
            _channel.QueueBindAsync(queue, exchange, routingKey,
                cancellationToken: cancellationToken);

        public async Task<ConfirmResult> SendAsync(
            OutboundMessage message, CancellationToken cancellationToken)
        {
            if (_blockedReason != null) throw new ConnectionBlockedException(_blockedReason);

            var properties = new BasicProperties
            {
                Persistent = message.Persistent,
                ContentType = message.ContentType,
                MessageId = message.MessageId,
            };
            if (message.ReplyTo != null) properties.ReplyTo = message.ReplyTo;
            if (message.Priority.HasValue) properties.Priority = (byte)message.Priority.Value;
            if (message.Expiration.HasValue)
            {
                properties.Expiration = ((long)message.Expiration.Value.TotalMilliseconds)
                    .ToString(System.Globalization.CultureInfo.InvariantCulture);
            }
            if (message.Headers.Count > 0)
            {
                var headers = new Dictionary<string, object?>();
                foreach (var pair in message.Headers)
                {
                    // The client writes strings as byte arrays on the wire, and reads
                    // them back the same way. Encoding here keeps what Java wrote and
                    // what this writes byte-identical.
                    headers[pair.Key] = pair.Value is string s
                        ? Encoding.UTF8.GetBytes(s)
                        : pair.Value;
                }
                properties.Headers = headers;
            }

            // One publish at a time on a channel: IChannel is not safe for concurrent
            // publishing, and the symptom of ignoring that is interleaved frames
            // rather than a clean error.
            await _publishLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _channel.BasicPublishAsync(
                    message.Exchange, message.RoutingKey, message.Mandatory,
                    properties, message.Body, cancellationToken).ConfigureAwait(false);
                return ConfirmResult.Ok(true);
            }
            catch (PublishException e)
            {
                // The broker either rejected the message or could not route it. Both
                // arrive here; only the second is a topology problem.
                return e.IsReturn
                    ? ConfirmResult.Ok(routed: false)
                    : ConfirmResult.Rejected(e.Message);
            }
            finally
            {
                _publishLock.Release();
            }
        }

        public async Task<ISubscription> SubscribeAsync(
            string queue, int prefetch,
            IReadOnlyDictionary<string, object>? arguments,
            Func<InboundDelivery, Task<Ack>> handler,
            CancellationToken cancellationToken)
        {
            var channel = await _connection.CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: false,
                    publisherConfirmationTrackingEnabled: false),
                cancellationToken).ConfigureAwait(false);

            await channel.BasicQosAsync(0, (ushort)prefetch, false, cancellationToken)
                .ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, delivered) =>
            {
                var headers = new Dictionary<string, object>();
                if (delivered.BasicProperties.Headers != null)
                {
                    foreach (var pair in delivered.BasicProperties.Headers)
                    {
                        if (pair.Value == null) continue;
                        // Strings arrive as byte arrays; anything else is left alone.
                        headers[pair.Key] = pair.Value is byte[] bytes
                            ? Encoding.UTF8.GetString(bytes)
                            : pair.Value;
                    }
                }

                var inbound = new InboundDelivery(
                    queue, delivered.Exchange, delivered.RoutingKey,
                    delivered.Body.ToArray(), headers,
                    delivered.BasicProperties.MessageId,
                    delivered.BasicProperties.ContentType,
                    delivered.Redelivered,
                    delivered.BasicProperties.ReplyTo);

                Ack ack;
                try
                {
                    ack = await handler(inbound).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    ack = Ack.Retry(TimeSpan.FromSeconds(5), e.Message);
                }

                switch (ack.Kind)
                {
                    case AckKind.Accept:
                        await channel.BasicAckAsync(delivered.DeliveryTag, false)
                            .ConfigureAwait(false);
                        break;
                    case AckKind.Release:
                        await channel.BasicNackAsync(delivered.DeliveryTag, false, requeue: true)
                            .ConfigureAwait(false);
                        break;
                    case AckKind.DeadLetter:
                        // requeue: false is what sends it to the queue's configured
                        // dead-letter exchange. Without one it is discarded, which is
                        // why the topology matters more than the disposition does.
                        await channel.BasicNackAsync(delivered.DeliveryTag, false, requeue: false)
                            .ConfigureAwait(false);
                        break;
                    case AckKind.Retry:
                        if (ack.Delay.HasValue && ack.Delay.Value > TimeSpan.Zero)
                        {
                            await Task.Delay(ack.Delay.Value).ConfigureAwait(false);
                        }
                        await channel.BasicNackAsync(delivered.DeliveryTag, false, requeue: true)
                            .ConfigureAwait(false);
                        break;
                }
            };

            Dictionary<string, object?>? consumerArguments = null;
            if (arguments != null && arguments.Count > 0)
            {
                consumerArguments = new Dictionary<string, object?>();
                foreach (var pair in arguments) consumerArguments[pair.Key] = pair.Value;
            }

            var tag = await channel.BasicConsumeAsync(
                queue, autoAck: false, consumerTag: string.Empty, noLocal: false,
                exclusive: false, arguments: consumerArguments, consumer: consumer,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            return new Subscription(queue, channel, tag);
        }

        /// <summary>Pulls one message with basic.get, acknowledging it.</summary>
        /// <remarks>
        /// basic.get rather than a subscription, because a replay has to take a fixed
        /// number of messages and stop. A subscription would immediately be handed the
        /// messages the replay had just republished.
        /// </remarks>
        public async Task<InboundDelivery?> ReceiveAsync(
            string queue, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (true)
            {
                var result = await _channel.BasicGetAsync(queue, autoAck: true, cancellationToken)
                    .ConfigureAwait(false);
                if (result != null)
                {
                    var headers = new Dictionary<string, object>();
                    if (result.BasicProperties.Headers != null)
                    {
                        foreach (var pair in result.BasicProperties.Headers)
                        {
                            if (pair.Value == null) continue;
                            headers[pair.Key] = pair.Value is byte[] bytes
                                ? Encoding.UTF8.GetString(bytes)
                                : pair.Value;
                        }
                    }
                    return new InboundDelivery(
                        queue, result.Exchange, result.RoutingKey, result.Body.ToArray(),
                        headers, result.BasicProperties.MessageId,
                        result.BasicProperties.ContentType, result.Redelivered,
                        result.BasicProperties.ReplyTo);
                }
                if (DateTime.UtcNow >= deadline) return null;
                await Task.Delay(25, cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task<long> MessageCountAsync(string queue, CancellationToken cancellationToken)
        {
            var ok = await _channel.QueueDeclarePassiveAsync(queue, cancellationToken)
                .ConfigureAwait(false);
            return ok.MessageCount;
        }

        public Task DeleteQueueAsync(string name, CancellationToken cancellationToken) =>
            _channel.QueueDeleteAsync(name, ifUnused: false, ifEmpty: false,
                cancellationToken: cancellationToken);

        public async Task<bool> QueueExistsAsync(string name, CancellationToken cancellationToken)
        {
            // A passive declare of a missing queue closes the channel, so this asks on
            // a throwaway one rather than taking the publishing channel down with it.
            try
            {
                using var probe = await _connection.CreateChannelAsync(
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                await probe.QueueDeclarePassiveAsync(name, cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (OperationInterruptedException)
            {
                return false;
            }
        }

        /// <summary>
        /// Asks the broker whether a queue matches what would be declared.
        /// </summary>
        /// <remarks>
        /// <para>
        /// AMQP has no way to read a queue's arguments back. The only thing that
        /// answers the question is a declaration: the broker replies
        /// PRECONDITION_FAILED when an existing queue was created with different
        /// ones, which is the drift being looked for.
        /// </para>
        /// <para>
        /// A failed declaration closes the channel, so this runs on a throwaway one.
        /// Doing it on the working channel would take publishing down as a side
        /// effect of asking a question.
        /// </para>
        /// </remarks>
        public async Task<QueueCheck> CheckQueueAsync(
            string name, QueueType type, bool durable,
            IReadOnlyDictionary<string, object>? arguments, CancellationToken cancellationToken)
        {
            if (!await QueueExistsAsync(name, cancellationToken).ConfigureAwait(false))
            {
                return QueueCheck.Absent();
            }

            try
            {
                using var probe = await _connection.CreateChannelAsync(
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                var args = new Dictionary<string, object?>();
                if (arguments != null)
                {
                    foreach (var pair in arguments) args[pair.Key] = pair.Value;
                }
                switch (type)
                {
                    case QueueType.Quorum: args["x-queue-type"] = "quorum"; break;
                    case QueueType.Stream: args["x-queue-type"] = "stream"; break;
                }

                await probe.QueueDeclareAsync(
                    name, durable, exclusive: false, autoDelete: false,
                    arguments: args.Count == 0 ? null : args,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                return QueueCheck.Matches();
            }
            catch (OperationInterruptedException e)
            {
                // 406 PRECONDITION_FAILED is the broker saying the queue exists with
                // different settings. Anything else is a different problem and
                // should not be reported as drift.
                if (e.ShutdownReason?.ReplyCode == 406)
                {
                    return QueueCheck.Differs(e.ShutdownReason.ReplyText);
                }
                return QueueCheck.Unsupported();
            }
        }

        public void Dispose()
        {
            try { _channel.Dispose(); } catch { /* closing a closed channel is not news */ }
            try { _connection.Dispose(); } catch { /* nor is closing a closed connection */ }
            _publishLock.Dispose();
        }
    }

    private sealed class Subscription : ISubscription
    {
        private readonly IChannel _channel;
        private readonly string _tag;
        private bool _cancelled;

        internal Subscription(string queue, IChannel channel, string tag)
        {
            Queue = queue;
            _channel = channel;
            _tag = tag;
        }

        public string Queue { get; }
        public bool IsActive => !_cancelled && _channel.IsOpen;

        /// <summary>Stops delivery.</summary>
        /// <remarks>
        /// The cancellation is run on the thread pool rather than awaited inline.
        /// A handler's continuation can run on the client's consumer dispatch
        /// thread, so a caller may well reach this from that thread — and blocking
        /// it on work the same thread has to perform deadlocks. Handing the cancel
        /// to the pool and waiting with a bound keeps disposal safe from anywhere,
        /// including from inside a handler.
        /// </remarks>
        public void Dispose()
        {
            if (_cancelled) return;
            _cancelled = true;
            try
            {
                Task.Run(async () =>
                {
                    await _channel.BasicCancelAsync(_tag).ConfigureAwait(false);
                    _channel.Dispose();
                }).Wait(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // Cancelling a consumer on a connection that has already gone is the
                // normal shutdown order, not an error worth reporting.
            }
        }
    }
}
