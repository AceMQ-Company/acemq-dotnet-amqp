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

/// <summary>What an ordered queue does when a message fails.</summary>
public enum PartitionFailure
{
    /// <summary>
    /// Stop that partition. Nothing after the failed message is delivered until
    /// somebody intervenes.
    /// </summary>
    Stop,

    /// <summary>Keep retrying the same message, holding everything behind it.</summary>
    RetryInPlace,

    /// <summary>Dead-letter it and carry on with the next message.</summary>
    Skip,
}

/// <summary>
/// A set of queues that preserve order per key.
/// </summary>
/// <remarks>
/// <para>
/// Order in AMQP survives only while one consumer reads one queue with a prefetch
/// it processes serially. Two consumers on a queue, or one consumer handling two
/// messages at once, and the order the broker sent them in stops being the order
/// they are handled in. So parallelism comes from splitting keys across several
/// queues rather than from several consumers on one.
/// </para>
/// <para>
/// Every message with the same key lands on the same queue, and each queue is read
/// by exactly one handler at a time. Throughput scales with partitions; order holds
/// within a key.
/// </para>
/// <para>
/// <strong>Failure is the interesting part.</strong> If a message fails and the next
/// one is handled anyway, order is broken exactly when it matters most — the
/// operation that should have come second has been applied first. So the default is
/// <see cref="PartitionFailure.Stop"/>: the partition halts and says so, rather than
/// carrying on and being subtly wrong.
/// </para>
/// </remarks>
public sealed class OrderedQueue<T> : IDisposable, IHealthContributor
{
    private readonly AceMqConnection _mq;
    private readonly Func<T, string> _key;
    private readonly PartitionFailure _onFailure;
    private readonly int _attempts;
    private readonly TimeSpan _delay;
    private readonly int _prefetch;
    private readonly List<IMessageConsumer> _consumers = new List<IMessageConsumer>();
    private readonly ConcurrentDictionary<int, byte> _halted = new ConcurrentDictionary<int, byte>();
    private readonly ConcurrentDictionary<int, TaskCompletionSource<bool>> _resuming =
        new ConcurrentDictionary<int, TaskCompletionSource<bool>>();
    private readonly CancellationTokenSource _stop = new CancellationTokenSource();
    private long _handled;
    private long _failed;
    private long _skipped;
    private bool _disposed;

    internal OrderedQueue(
        AceMqConnection mq, string name, int partitions, Func<T, string> key,
        PartitionFailure onFailure, int attempts, TimeSpan delay, int prefetch)
    {
        _mq = mq;
        Name = name;
        Partitions = partitions;
        _key = key;
        _onFailure = onFailure;
        _attempts = attempts;
        _delay = delay;
        _prefetch = prefetch;
    }

    public string Name { get; }
    public int Partitions { get; }

    /// <summary>The queue a partition's messages land on.</summary>
    public string QueueFor(int partition) =>
        $"{Name}.{partition.ToString(CultureInfo.InvariantCulture)}";

    public IReadOnlyList<string> Queues =>
        Enumerable.Range(0, Partitions).Select(QueueFor).ToArray();

    /// <summary>Partitions that have stopped because a message failed.</summary>
    public IReadOnlyCollection<int> HaltedPartitions => _halted.Keys.ToArray();

    public long Handled => Interlocked.Read(ref _handled);
    public long Failed => Interlocked.Read(ref _failed);
    public long Skipped => Interlocked.Read(ref _skipped);

    internal async Task DeclareAsync()
    {
        for (var i = 0; i < Partitions; i++)
        {
            await _mq.DeclareQueueAsync(QueueFor(i)).ConfigureAwait(false);
        }
    }

    /// <summary>Publishes a payload to the partition its key belongs to.</summary>
    public async Task<int> SendAsync(T payload) => await SendAsync(payload, null).ConfigureAwait(false);

    public async Task<int> SendAsync(T payload, Envelope? envelope)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(OrderedQueue<T>));
        var key = _key(payload);
        var partition = Partitioning.PartitionFor(key, Partitions);
        var queue = QueueFor(partition);

        var publisher = _mq.Publisher<T>(string.Empty, queue);
        await publisher.SendAsync(payload, envelope ?? Envelope.Of(queue).Build())
            .ConfigureAwait(false);
        return partition;
    }

    /// <summary>Starts one handler per partition.</summary>
    public async Task<OrderedQueue<T>> ConsumeAsync(Func<IMessage<T>, Task> handler)
    {
        if (handler == null) throw new ArgumentNullException(nameof(handler));

        for (var partition = 0; partition < Partitions; partition++)
        {
            var p = partition;
            // Prefetch is per partition, and the handler runs one message at a time
            // on each. A prefetch above one is only a buffer; it never makes two
            // messages on the same partition run concurrently.
            // Retries happen inside the handler rather than by returning Ack.Retry.
            //
            // A returned retry puts the message back on the queue -- at the *back*,
            // behind everything already waiting. The next message is then handled
            // first, which breaks the ordering this whole type exists to provide,
            // and breaks it precisely when it matters: the operation that should
            // have come second has been applied while the first one is still
            // failing. Holding the message in the handler keeps the partition on it,
            // because prefetch is per queue and nothing else is delivered until this
            // returns.
            var consumer = await _mq.ConsumeAsync<T>(
                QueueFor(p), ConsumerOptions.Prefetch(_prefetch),
                async message =>
                {
                    for (var attempt = 1; ; attempt++)
                    {
                        if (_disposed || _stop.IsCancellationRequested) return Ack.Release();

                        try
                        {
                            await handler(message).ConfigureAwait(false);
                            Interlocked.Increment(ref _handled);
                            return Ack.Accept();
                        }
                        catch (Exception e)
                        {
                            Interlocked.Increment(ref _failed);

                            if (_onFailure == PartitionFailure.Skip)
                            {
                                Interlocked.Increment(ref _skipped);
                                return Ack.DeadLetter(e.Message);
                            }

                            var exhausted = _onFailure == PartitionFailure.Stop
                                            && attempt >= _attempts;
                            if (exhausted)
                            {
                                // Out of attempts. Halting is the honest outcome:
                                // carrying on would apply the next operation for this
                                // key before the one that failed. The message is held,
                                // unacknowledged, until somebody resumes the partition
                                // -- which is also what stops anything behind it being
                                // delivered.
                                _halted[p] = 1;
                                await WaitForResumeAsync(p).ConfigureAwait(false);
                                if (_disposed || _stop.IsCancellationRequested)
                                {
                                    return Ack.Release();
                                }
                                attempt = 0;
                                continue;
                            }

                            try
                            {
                                await Task.Delay(_delay, _stop.Token).ConfigureAwait(false);
                            }
                            catch (OperationCanceledException)
                            {
                                return Ack.Release();
                            }
                        }
                    }
                }).ConfigureAwait(false);

            lock (_consumers) _consumers.Add(consumer);
        }

        return this;
    }

    /// <summary>
    /// Reports halted partitions, so a stopped consumer is visible to a health check.
    /// </summary>
    /// <remarks>
    /// Degraded rather than down: the other partitions are still working, and a
    /// process that reports itself down gets restarted, which loses the held message
    /// and fixes nothing.
    /// </remarks>
    public HealthReport Report()
    {
        var halted = HaltedPartitions;
        var details = new Dictionary<string, string>
        {
            ["partitions"] = Partitions.ToString(CultureInfo.InvariantCulture),
            ["handled"] = Handled.ToString(CultureInfo.InvariantCulture),
            ["failed"] = Failed.ToString(CultureInfo.InvariantCulture),
            ["skipped"] = Skipped.ToString(CultureInfo.InvariantCulture),
        };
        if (halted.Count > 0)
        {
            details["halted"] = string.Join(",", halted.OrderBy(p => p));
        }
        return new HealthReport(
            "ordered:" + Name,
            halted.Count > 0 ? HealthStatus.Degraded : HealthStatus.Up,
            details);
    }

    string IHealthContributor.Name => "ordered:" + Name;

    /// <summary>Restarts a partition that halted, once the cause has been dealt with.</summary>
    /// <remarks>
    /// The held message is tried again from the first attempt. It is still the
    /// oldest message on the partition, so order is preserved across the halt.
    /// </remarks>
    public void Resume(int partition)
    {
        _halted.TryRemove(partition, out _);
        if (_resuming.TryRemove(partition, out var waiter)) waiter.TrySetResult(true);
    }

    private Task WaitForResumeAsync(int partition)
    {
        var waiter = _resuming.GetOrAdd(
            partition,
            _ => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
        return Task.WhenAny(waiter.Task, Task.Delay(Timeout.Infinite, _stop.Token));
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _stop.Cancel();
        foreach (var waiter in _resuming.Values) waiter.TrySetResult(false);
        lock (_consumers)
        {
            foreach (var c in _consumers) c.Dispose();
            _consumers.Clear();
        }
        _stop.Dispose();
    }

    public override string ToString() =>
        $"OrderedQueue[{Name}, {Partitions} partition(s), {_halted.Count} halted]";
}

/// <summary>Builds an <see cref="OrderedQueue{T}"/>.</summary>
public sealed class OrderedQueueBuilder<T>
{
    private readonly AceMqConnection _mq;
    private readonly string _name;
    private int _partitions = 4;
    private Func<T, string>? _key;
    private PartitionFailure _onFailure = PartitionFailure.Stop;
    private int _attempts = 3;
    private TimeSpan _delay = TimeSpan.FromSeconds(1);
    private int _prefetch = 1;

    internal OrderedQueueBuilder(AceMqConnection mq, string name)
    {
        _mq = mq;
        _name = name;
    }

    public OrderedQueueBuilder<T> Partitions(int partitions)
    {
        if (partitions < 1) throw new ArgumentException("must be at least 1", nameof(partitions));
        _partitions = partitions;
        return this;
    }

    /// <summary>
    /// How to get the ordering key out of a payload.
    /// </summary>
    /// <remarks>
    /// Everything with the same key is ordered against everything else with that
    /// key, and nothing else. Choosing the key is choosing what ordering means:
    /// an account id orders one account's operations, a constant orders everything
    /// and gives up all parallelism.
    /// </remarks>
    public OrderedQueueBuilder<T> KeyedBy(Func<T, string> key)
    {
        _key = key ?? throw new ArgumentNullException(nameof(key));
        return this;
    }

    public OrderedQueueBuilder<T> Prefetch(int prefetch)
    {
        _prefetch = prefetch;
        return this;
    }

    public OrderedQueueBuilder<T> OnFailure(PartitionFailure onFailure) =>
        OnFailure(onFailure, _attempts, _delay);

    public OrderedQueueBuilder<T> OnFailure(PartitionFailure onFailure, int attempts, TimeSpan delay)
    {
        _onFailure = onFailure;
        _attempts = attempts;
        _delay = delay;
        return this;
    }

    /// <summary>Declares the partition queues and returns the ordered queue.</summary>
    public async Task<OrderedQueue<T>> DeclareAsync()
    {
        if (_key == null)
        {
            throw new InvalidOperationException(
                "an ordered queue needs KeyedBy(...): without a key there is nothing to order by");
        }
        var queue = new OrderedQueue<T>(
            _mq, _name, _partitions, _key, _onFailure, _attempts, _delay, _prefetch);
        await queue.DeclareAsync().ConfigureAwait(false);
        // Registered so a halted partition reaches the health endpoint without the
        // application having to remember to wire it up.
        _mq.RegisterHealth(queue);
        return queue;
    }
}
