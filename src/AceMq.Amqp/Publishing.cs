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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>How a message should be published.</summary>
public sealed class PublishOptions
{
    private PublishOptions(bool persistent, bool mandatory, TimeSpan? expiration, int? priority)
    {
        Persistent = persistent;
        Mandatory = mandatory;
        Expiration = expiration;
        Priority = priority;
    }

    /// <summary>Persistent, and unroutable messages reported rather than dropped.</summary>
    public static PublishOptions Defaults() => new PublishOptions(true, true, null, null);

    /// <summary>
    /// Not written to disk: faster, and lost on a broker restart.
    /// </summary>
    /// <remarks>
    /// Reasonable for telemetry that is superseded within seconds. Not reasonable
    /// for anything a person would notice the absence of.
    /// </remarks>
    public static PublishOptions TransientDelivery() => new PublishOptions(false, true, null, null);

    /// <summary>
    /// Lets the broker discard a message that matches no binding.
    /// </summary>
    /// <remarks>
    /// The default reports it as a failure instead, because an unroutable message
    /// is nearly always a topology mistake and this is the cheapest moment to find
    /// out. Use this where the discard is the intent.
    /// </remarks>
    public PublishOptions AllowUnroutable() =>
        new PublishOptions(Persistent, false, Expiration, Priority);

    public PublishOptions ExpiringAfter(TimeSpan expiration) =>
        new PublishOptions(Persistent, Mandatory, expiration, Priority);

    public PublishOptions WithPriority(int priority) =>
        new PublishOptions(Persistent, Mandatory, Expiration, priority);

    public bool Persistent { get; }
    public bool Mandatory { get; }
    public TimeSpan? Expiration { get; }
    public int? Priority { get; }
}

/// <summary>What happened to a publish.</summary>
public sealed class PublishResult
{
    public PublishResult(string messageId, bool routed, TimeSpan latency)
    {
        MessageId = messageId;
        Routed = routed;
        Latency = latency;
    }

    /// <summary>The envelope id the message went out with.</summary>
    public string MessageId { get; }

    /// <summary>Whether the broker put it on at least one queue.</summary>
    public bool Routed { get; }

    /// <summary>Time from the call to the broker's confirmation.</summary>
    public TimeSpan Latency { get; }

    public override string ToString() =>
        $"PublishResult[{MessageId}, routed={Routed}, {Latency.TotalMilliseconds:F1}ms]";
}

/// <summary>Publishes payloads of one type to one exchange and routing key.</summary>
public interface IPublisher<in T> : IDisposable
{
    Task<PublishResult> SendAsync(T payload);

    Task<PublishResult> SendAsync(T payload, Envelope envelope);

    Task<PublishResult> SendAsync(T payload, Envelope envelope, CancellationToken cancellationToken);

    /// <summary>Publishes each payload, returning the results in the same order.</summary>
    Task<IReadOnlyList<PublishResult>> SendAllAsync(IEnumerable<T> payloads);
}

internal sealed class Publisher<T> : IPublisher<T>
{
    private readonly ITransportConnection _connection;
    private readonly ICodec _codec;
    private readonly string _exchange;
    private readonly string _routingKey;
    private readonly PublishOptions _options;
    private readonly SemaphoreSlim _inFlight;
    private readonly TimeSpan _confirmTimeout;
    private readonly string? _replyTo;
    private bool _disposed;

    internal Publisher(
        ITransportConnection connection, ICodec codec, string exchange, string routingKey,
        PublishOptions options, SemaphoreSlim inFlight, TimeSpan confirmTimeout,
        string? replyTo = null)
    {
        _replyTo = replyTo;
        _connection = connection;
        _codec = codec;
        _exchange = exchange;
        _routingKey = routingKey;
        _options = options;
        _inFlight = inFlight;
        _confirmTimeout = confirmTimeout;
    }

    public Task<PublishResult> SendAsync(T payload) =>
        SendAsync(payload, Envelope.Of(_routingKey).Build(), CancellationToken.None);

    public Task<PublishResult> SendAsync(T payload, Envelope envelope) =>
        SendAsync(payload, envelope, CancellationToken.None);

    public async Task<PublishResult> SendAsync(
        T payload, Envelope envelope, CancellationToken cancellationToken)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(Publisher<T>));
        if (envelope == null) throw new ArgumentNullException(nameof(envelope));

        var body = _codec.Encode(payload!);
        var headers = envelope.ToWire();
        var message = new OutboundMessage(
            _exchange, _routingKey, body,
            new Dictionary<string, object>(headers),
            envelope.Id, _codec.ContentType,
            _options.Persistent, _options.Mandatory,
            _options.Expiration, _options.Priority, _replyTo);

        // The semaphore is the back pressure. Without it a caller in a loop can
        // queue more unconfirmed publishes than the broker will ever confirm, and
        // the failure arrives as memory growth rather than as a slow publish.
        await _inFlight.WaitAsync(cancellationToken).ConfigureAwait(false);
        var clock = Stopwatch.StartNew();
        try
        {
            using var timeout = new CancellationTokenSource(_confirmTimeout);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, timeout.Token);

            ConfirmResult confirm;
            try
            {
                confirm = await _connection.SendAsync(message, linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeout.IsCancellationRequested
                                                     && !cancellationToken.IsCancellationRequested)
            {
                throw new PublishFailedException(
                    $"the broker did not confirm {envelope.Id} within " +
                    $"{_confirmTimeout.TotalSeconds:F0}s. The message may or may not have been " +
                    "stored, so treat it as neither sent nor lost.");
            }

            if (!confirm.Confirmed)
            {
                throw new PublishFailedException(
                    $"the broker rejected {envelope.Id}: {confirm.Reason ?? "no reason given"}");
            }

            if (!confirm.Routed && _options.Mandatory)
            {
                throw new PublishFailedException(
                    $"{envelope.Id} reached the broker but matched no queue bound to " +
                    $"'{_exchange}' for '{_routingKey}'. This is usually a topology mistake; " +
                    "if the discard is intended, publish with PublishOptions.AllowUnroutable().");
            }

            return new PublishResult(envelope.Id, confirm.Routed, clock.Elapsed);
        }
        finally
        {
            _inFlight.Release();
        }
    }

    public async Task<IReadOnlyList<PublishResult>> SendAllAsync(IEnumerable<T> payloads)
    {
        if (payloads == null) throw new ArgumentNullException(nameof(payloads));
        var results = new List<PublishResult>();
        foreach (var payload in payloads)
        {
            results.Add(await SendAsync(payload).ConfigureAwait(false));
        }
        return results;
    }

    public void Dispose() => _disposed = true;
}
