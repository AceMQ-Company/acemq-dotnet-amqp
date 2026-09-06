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
/// Several consumers over one queue, started and stopped together.
/// </summary>
/// <remarks>
/// <para>
/// Two things this saves you from. Starting workers by hand means remembering to
/// close every one, and a partial shutdown leaves messages held by a consumer
/// nobody is waiting for. And a group can be sized from configuration, which is
/// the number most often changed after a service is running.
/// </para>
/// <para>
/// <strong>Prefetch, or a group?</strong> <see cref="ConsumerOptions.WithPrefetch"/>
/// lets one consumer hold more unacknowledged messages, and the handler still
/// runs them one at a time on that consumer's channel. A group runs several
/// consumers, each with its own channel and its own prefetch. Reach for the group
/// when handlers are slow enough that one channel is the limit, or when a fair
/// share across processes matters: the broker round-robins between consumers, so
/// four here compete evenly with four in another instance.
/// </para>
/// <para>
/// Ordering is the cost. One consumer on a queue sees messages in order; four do
/// not. Where later messages about the same entity must not overtake earlier
/// ones, use <see cref="OrderedQueue{T}"/> inside the handler, or route by key so
/// each key reaches one consumer.
/// </para>
/// <example>
/// <code>
/// var group = await ConsumerGroup.StartAsync&lt;OrderPlaced&gt;(
///     mq, "orders", 4, message => Handle(message));
///
/// // and at shutdown
/// group.Dispose();
/// </code>
/// </example>
/// </remarks>
public sealed class ConsumerGroup : IDisposable
{
    private readonly List<IMessageConsumer> _consumers;
    private readonly object _gate = new object();
    private bool _disposed;

    private ConsumerGroup(string queue, List<IMessageConsumer> consumers)
    {
        Queue = queue;
        _consumers = consumers;
    }

    /// <summary>The queue every consumer in the group reads.</summary>
    public string Queue { get; }

    /// <summary>How many consumers are running.</summary>
    public int Size
    {
        get { lock (_gate) return _consumers.Count; }
    }

    /// <summary>Starts <paramref name="size"/> consumers over one queue.</summary>
    /// <remarks>
    /// If any fails to start, those already started are closed before the
    /// exception reaches the caller. A half-started group would hold messages
    /// nothing is going to handle.
    /// </remarks>
    public static Task<ConsumerGroup> StartAsync<T>(
        AceMqConnection connection, string queue, int size, Func<IMessage<T>, Task<Ack>> handler) =>
        StartAsync(connection, queue, size, ConsumerOptions.Defaults(), handler);

    /// <summary>Starts a group with options shared by every consumer in it.</summary>
    public static async Task<ConsumerGroup> StartAsync<T>(
        AceMqConnection connection, string queue, int size,
        ConsumerOptions options, Func<IMessage<T>, Task<Ack>> handler)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        if (queue == null) throw new ArgumentNullException(nameof(queue));
        if (handler == null) throw new ArgumentNullException(nameof(handler));
        if (size < 1)
        {
            throw new ArgumentException(
                $"a consumer group needs at least one consumer, got {size}", nameof(size));
        }

        var consumers = new List<IMessageConsumer>(size);
        try
        {
            for (var i = 0; i < size; i++)
            {
                consumers.Add(await connection.ConsumeAsync(queue, options, handler)
                    .ConfigureAwait(false));
            }
        }
        catch
        {
            // Whatever started has to stop. Leaving consumers attached after a
            // failed start is worse than the failure: they hold messages and
            // nobody holds them.
            foreach (var started in consumers)
            {
                try { started.Dispose(); } catch { /* the original failure is the one to report */ }
            }
            throw;
        }

        return new ConsumerGroup(queue, consumers);
    }

    /// <summary>
    /// Stops every consumer and waits for handlers already running.
    /// </summary>
    /// <remarks>
    /// All of them are closed even if one throws, because leaving the rest
    /// running after a failed shutdown is worse than the failure. The first
    /// exception is rethrown once the others have been closed.
    /// </remarks>
    public void Dispose()
    {
        List<IMessageConsumer> consumers;
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            consumers = new List<IMessageConsumer>(_consumers);
            _consumers.Clear();
        }

        Exception? first = null;
        foreach (var consumer in consumers)
        {
            try
            {
                consumer.Dispose();
            }
            catch (Exception failure)
            {
                first ??= failure;
            }
        }

        if (first != null) throw first;
    }
}
