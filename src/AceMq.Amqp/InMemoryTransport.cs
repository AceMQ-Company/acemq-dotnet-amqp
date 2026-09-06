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
using System.Globalization;
using System.Linq;

using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>
/// A broker that exists only in this process, for tests.
/// </summary>
/// <remarks>
/// <para>
/// It routes the way RabbitMQ routes — direct, fanout and topic, with <c>*</c>
/// matching one word and <c>#</c> matching zero or more — so a test that passes
/// here is testing its own bindings rather than a simplification of them. What it
/// does not do is durability, clustering, flow control or network failure: for
/// those the test needs a real broker, and pretending otherwise is how a test suite
/// becomes reassuring rather than useful.
/// </para>
/// <para>
/// Use it with a <c>memory://</c> URL. It is registered by default.
/// </para>
/// </remarks>
public sealed class InMemoryTransport : ITransport
{
    private static readonly ConcurrentDictionary<string, Broker> Brokers =
        new ConcurrentDictionary<string, Broker>(StringComparer.Ordinal);

    public IReadOnlyCollection<string> Schemes => new[] { "memory" };

    public string Name => "in-memory";

    public IReadOnlyCollection<Capability> Capabilities => new[]
    {
        Capability.PublisherConfirms,
        Capability.DeadLettering,
        Capability.DelayedDelivery,
        Capability.Priority,
    };

    /// <summary>AMQP topic matching: <c>*</c> is one word, <c>#</c> is zero or more.</summary>
    /// <remarks>
    /// Matched word by word rather than by translating the pattern into a regular
    /// expression. A <c>#</c> has to absorb the separators around it as well as the
    /// words, which the obvious translation gets wrong at the ends of a pattern —
    /// and the resulting bug is a binding that silently matches slightly too much.
    /// </remarks>
    internal static bool TopicMatches(string pattern, string routingKey)
    {
        var p = pattern.Split('.');
        var k = routingKey.Split('.');

        // matches[i, j] is true when the first i pattern words consume the first
        // j routing-key words.
        var matches = new bool[p.Length + 1, k.Length + 1];
        matches[0, 0] = true;

        for (var i = 1; i <= p.Length; i++)
        {
            // A leading run of '#' can match nothing at all.
            if (p[i - 1] == "#") matches[i, 0] = matches[i - 1, 0];
        }

        for (var i = 1; i <= p.Length; i++)
        {
            for (var j = 1; j <= k.Length; j++)
            {
                matches[i, j] = p[i - 1] switch
                {
                    // Either '#' has finished, or it takes this word too.
                    "#" => matches[i - 1, j] || matches[i, j - 1],
                    "*" => matches[i - 1, j - 1],
                    _ => matches[i - 1, j - 1] && p[i - 1] == k[j - 1],
                };
            }
        }

        return matches[p.Length, k.Length];
    }

    /// <summary>Forgets every broker, so one test cannot see another's messages.</summary>
    public static void Reset() => Brokers.Clear();

    /// <summary>Messages dead-lettered from a queue, for a test to assert on.</summary>
    public static IReadOnlyList<InboundDelivery> DeadLettered(string brokerName, string queue)
    {
        if (!Brokers.TryGetValue(brokerName, out var broker)) return Array.Empty<InboundDelivery>();
        return broker.DeadLetteredFrom(queue);
    }

    public Task<ITransportConnection> ConnectAsync(
        ConnectionConfig config, CancellationToken cancellationToken)
    {
        // Everything after memory:// names the broker, so two tests can share one
        // or keep to themselves by using different names.
        var name = config.Url.Substring("memory://".Length);
        if (name.Length == 0) name = "default";
        var broker = Brokers.GetOrAdd(name, _ => new Broker());
        return Task.FromResult<ITransportConnection>(new Connection(broker));
    }

    private sealed class Broker
    {
        internal readonly ConcurrentDictionary<string, string> Exchanges =
            new ConcurrentDictionary<string, string>(StringComparer.Ordinal);

        internal readonly ConcurrentDictionary<string, Queue> Queues =
            new ConcurrentDictionary<string, Queue>(StringComparer.Ordinal);

        internal readonly List<Binding> Bindings = new List<Binding>();

        internal IReadOnlyList<InboundDelivery> DeadLetteredFrom(string queue) =>
            Queues.TryGetValue(queue, out var q) ? q.DeadLetterSnapshot() : Array.Empty<InboundDelivery>();
    }

    private sealed class Binding
    {
        internal Binding(string queue, string exchange, string routingKey)
        {
            Queue = queue;
            Exchange = exchange;
            RoutingKey = routingKey;
        }

        internal string Queue { get; }
        internal string Exchange { get; }
        internal string RoutingKey { get; }
    }

    private sealed class Queue
    {
        private readonly ConcurrentQueue<InboundDelivery> _messages =
            new ConcurrentQueue<InboundDelivery>();
        private readonly List<InboundDelivery> _deadLettered = new List<InboundDelivery>();

        internal QueueType DeclaredType { get; private set; } = QueueType.Classic;

        internal IReadOnlyDictionary<string, object> Arguments { get; private set; } =
            new Dictionary<string, object>();

        /// <summary>
        /// Records what the queue was declared with, so drift can be reported.
        /// </summary>
        /// <remarks>
        /// The first declaration wins, as it does on a real broker: a queue's type
        /// and arguments are fixed when it is created, and a later declaration with
        /// different ones is a mismatch rather than a change.
        /// </remarks>
        internal void Declared(QueueType type, IReadOnlyDictionary<string, object>? arguments)
        {
            if (_declared) return;
            _declared = true;
            DeclaredType = type;
            Arguments = arguments == null
                ? new Dictionary<string, object>()
                : new Dictionary<string, object>((IDictionary<string, object>)arguments);
        }

        private bool _declared;

        internal void Enqueue(InboundDelivery delivery) => _messages.Enqueue(delivery);

        internal bool TryDequeue(out InboundDelivery delivery) => _messages.TryDequeue(out delivery!);

        /// <summary>Puts a pulled message back, marked as a redelivery.</summary>
        /// <remarks>
        /// A ConcurrentQueue cannot push to the front, so this goes to the back.
        /// A broker returns a requeued message closer to the head, which means
        /// order after a reject differs here — worth knowing before relying on
        /// it, and not worth a lock on the hot path to imitate.
        /// </remarks>
        internal void Requeue(InboundDelivery delivery) =>
            _messages.Enqueue(new InboundDelivery(
                delivery.Queue, delivery.Exchange, delivery.RoutingKey, delivery.Body,
                delivery.Headers, delivery.MessageId, delivery.ContentType,
                redelivered: true, delivery.ReplyTo));

        internal int Count => _messages.Count;

        internal void DeadLetter(InboundDelivery delivery, string reason)
        {
            var headers = new Dictionary<string, object>(
                (IDictionary<string, object>)delivery.Headers) { [AceHeaders.Error] = reason };
            lock (_deadLettered)
            {
                _deadLettered.Add(new InboundDelivery(
                    delivery.Queue, delivery.Exchange, delivery.RoutingKey, delivery.Body,
                    headers, delivery.MessageId, delivery.ContentType, delivery.Redelivered,
                    delivery.ReplyTo));
            }
        }

        internal IReadOnlyList<InboundDelivery> DeadLetterSnapshot()
        {
            lock (_deadLettered) return _deadLettered.ToArray();
        }
    }

    private sealed class Connection : ITransportConnection
    {
        private readonly Broker _broker;
        private readonly List<Subscription> _subscriptions = new List<Subscription>();

        internal Connection(Broker broker) => _broker = broker;

        public bool IsOpen { get; private set; } = true;
        public bool IsBlocked => false;
        public string? BlockedReason => null;

        public Task DeclareExchangeAsync(
            string name, string type, bool durable, CancellationToken cancellationToken)
        {
            _broker.Exchanges[name] = type;
            return Task.CompletedTask;
        }

        public Task DeclareQueueAsync(
            string name, QueueType type, bool durable,
            IReadOnlyDictionary<string, object>? arguments, CancellationToken cancellationToken)
        {
            var queue = _broker.Queues.GetOrAdd(name, _ => new Queue());
            queue.Declared(type, arguments);
            return Task.CompletedTask;
        }

        public Task BindQueueAsync(
            string queue, string exchange, string routingKey, CancellationToken cancellationToken)
        {
            lock (_broker.Bindings) _broker.Bindings.Add(new Binding(queue, exchange, routingKey));
            return Task.CompletedTask;
        }

        public Task<ConfirmResult> SendAsync(OutboundMessage message, CancellationToken cancellationToken)
        {
            var matched = Route(message);
            foreach (var queueName in matched)
            {
                var queue = _broker.Queues.GetOrAdd(queueName, _ => new Queue());
                queue.Enqueue(new InboundDelivery(
                    queueName, message.Exchange, message.RoutingKey, message.Body,
                    new Dictionary<string, object>((IDictionary<string, object>)message.Headers),
                    message.MessageId, message.ContentType, false, message.ReplyTo));
            }
            return Task.FromResult(ConfirmResult.Ok(matched.Count > 0));
        }

        private List<string> Route(OutboundMessage message)
        {
            // Publishing to the default exchange addresses a queue by name, which is
            // how RabbitMQ behaves and how most first examples are written.
            if (message.Exchange.Length == 0)
            {
                return _broker.Queues.ContainsKey(message.RoutingKey)
                    ? new List<string> { message.RoutingKey }
                    : new List<string>();
            }

            var type = _broker.Exchanges.TryGetValue(message.Exchange, out var t) ? t : "direct";
            List<Binding> bindings;
            lock (_broker.Bindings)
            {
                bindings = _broker.Bindings.Where(b => b.Exchange == message.Exchange).ToList();
            }

            var matched = new List<string>();
            foreach (var binding in bindings)
            {
                var hit = type switch
                {
                    "fanout" => true,
                    "topic" => InMemoryTransport.TopicMatches(binding.RoutingKey, message.RoutingKey),
                    _ => binding.RoutingKey == message.RoutingKey,
                };
                if (hit && !matched.Contains(binding.Queue)) matched.Add(binding.Queue);
            }
            return matched;
        }

        public Task<ISubscription> SubscribeAsync(
            string queue, int prefetch,
            IReadOnlyDictionary<string, object>? arguments,
            Func<InboundDelivery, Task<Ack>> handler,
            CancellationToken cancellationToken)
        {
            var q = _broker.Queues.GetOrAdd(queue, _ => new Queue());
            var subscription = new Subscription(queue, q, handler);
            lock (_subscriptions) _subscriptions.Add(subscription);
            subscription.Start();
            return Task.FromResult<ISubscription>(subscription);
        }

        public async Task<IPulledDelivery?> PullAsync(
            string queue, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var q = _broker.Queues.GetOrAdd(queue, _ => new Queue());
            var deadline = DateTime.UtcNow + timeout;
            while (true)
            {
                if (q.TryDequeue(out var delivery))
                {
                    // Held rather than gone: rejecting with requeue has to put
                    // it back, or this transport would be more forgiving than a
                    // broker and would certify code that loses messages.
                    return new PulledDelivery(q, delivery);
                }
                if (DateTime.UtcNow >= deadline) return null;
                await Task.Delay(5, cancellationToken).ConfigureAwait(false);
            }
        }

        private sealed class PulledDelivery : IPulledDelivery
        {
            private readonly Queue _queue;
            private int _settled;

            internal PulledDelivery(Queue queue, InboundDelivery delivery)
            {
                _queue = queue;
                Delivery = delivery;
            }

            public InboundDelivery Delivery { get; }

            public Task AcknowledgeAsync(CancellationToken cancellationToken)
            {
                // Already off the queue, so there is nothing to do but record
                // that it was settled.
                Interlocked.Exchange(ref _settled, 1);
                return Task.CompletedTask;
            }

            public Task RejectAsync(bool requeue, CancellationToken cancellationToken)
            {
                if (Interlocked.Exchange(ref _settled, 1) == 0 && requeue)
                {
                    _queue.Requeue(Delivery);
                }
                return Task.CompletedTask;
            }
        }

        public async Task<InboundDelivery?> ReceiveAsync(
            string queue, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var q = _broker.Queues.GetOrAdd(queue, _ => new Queue());
            var deadline = DateTime.UtcNow + timeout;
            while (true)
            {
                if (q.TryDequeue(out var delivery)) return delivery;
                if (DateTime.UtcNow >= deadline) return null;
                await Task.Delay(5, cancellationToken).ConfigureAwait(false);
            }
        }

        public Task<long> MessageCountAsync(string queue, CancellationToken cancellationToken) =>
            Task.FromResult<long>(_broker.Queues.TryGetValue(queue, out var q) ? q.Count : 0);

        public Task DeleteQueueAsync(string name, CancellationToken cancellationToken)
        {
            _broker.Queues.TryRemove(name, out _);
            return Task.CompletedTask;
        }

        public Task<bool> QueueExistsAsync(string name, CancellationToken cancellationToken) =>
            Task.FromResult(_broker.Queues.ContainsKey(name));

        public Task<QueueCheck> CheckQueueAsync(
            string name, QueueType type, bool durable,
            IReadOnlyDictionary<string, object>? arguments, CancellationToken cancellationToken)
        {
            if (!_broker.Queues.TryGetValue(name, out var queue))
            {
                return Task.FromResult(QueueCheck.Absent());
            }

            if (queue.DeclaredType != type)
            {
                return Task.FromResult(QueueCheck.Differs(
                    $"declared as {queue.DeclaredType.ToString().ToLowerInvariant()}, " +
                    $"asked for {type.ToString().ToLowerInvariant()}"));
            }

            var wanted = arguments ?? new Dictionary<string, object>();
            foreach (var pair in wanted)
            {
                if (!queue.Arguments.TryGetValue(pair.Key, out var existing))
                {
                    return Task.FromResult(QueueCheck.Differs($"missing argument {pair.Key}"));
                }
                var a = Convert.ToString(existing, CultureInfo.InvariantCulture);
                var b = Convert.ToString(pair.Value, CultureInfo.InvariantCulture);
                if (a != b)
                {
                    return Task.FromResult(QueueCheck.Differs(
                        $"{pair.Key} is '{a}', asked for '{b}'"));
                }
            }

            return Task.FromResult(QueueCheck.Matches());
        }

        public void Dispose()
        {
            IsOpen = false;
            lock (_subscriptions)
            {
                foreach (var s in _subscriptions) s.Dispose();
                _subscriptions.Clear();
            }
        }
    }

    private sealed class Subscription : ISubscription
    {
        private readonly Queue _queue;
        private readonly Func<InboundDelivery, Task<Ack>> _handler;
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();

        internal Subscription(string queue, Queue q, Func<InboundDelivery, Task<Ack>> handler)
        {
            Queue = queue;
            _queue = q;
            _handler = handler;
        }

        public string Queue { get; }
        public bool IsActive => !_stop.IsCancellationRequested;

        internal void Start() => Task.Run(PumpAsync);

        private async Task PumpAsync()
        {
            while (!_stop.IsCancellationRequested)
            {
                if (!_queue.TryDequeue(out var delivery))
                {
                    try { await Task.Delay(5, _stop.Token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { return; }
                    continue;
                }

                Ack ack;
                try
                {
                    ack = await _handler(delivery).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    ack = Ack.Retry(TimeSpan.FromMilliseconds(10), e.Message);
                }

                switch (ack.Kind)
                {
                    case AckKind.Accept:
                        break;
                    case AckKind.Release:
                        _queue.Enqueue(Redelivered(delivery));
                        break;
                    case AckKind.Retry:
                        var delay = ack.Delay ?? TimeSpan.FromMilliseconds(10);
                        var requeued = Redelivered(delivery);
                        _ = Task.Run(async () =>
                        {
                            try { await Task.Delay(delay, _stop.Token).ConfigureAwait(false); }
                            catch (OperationCanceledException) { return; }
                            _queue.Enqueue(requeued);
                        });
                        break;
                    case AckKind.DeadLetter:
                        _queue.DeadLetter(delivery, ack.Reason ?? "no reason given");
                        break;
                }
            }
        }

        /// <summary>
        /// The same message going round again, marked as a redelivery.
        /// </summary>
        /// <remarks>
        /// The headers are passed through untouched, exactly as a real broker
        /// requeues the original bytes. Advancing the attempt header here would make
        /// this transport more helpful than RabbitMQ, and a test that relied on it
        /// would pass here and loop forever in production.
        /// </remarks>
        private static InboundDelivery Redelivered(InboundDelivery delivery) =>
            new InboundDelivery(
                delivery.Queue, delivery.Exchange, delivery.RoutingKey, delivery.Body,
                new Dictionary<string, object>((IDictionary<string, object>)delivery.Headers),
                delivery.MessageId, delivery.ContentType, true, delivery.ReplyTo);

        public void Dispose() => _stop.Cancel();
    }
}
