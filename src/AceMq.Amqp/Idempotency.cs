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
/// Remembers which messages have already been handled.
/// </summary>
/// <remarks>
/// <para>
/// Every broker worth using delivers at least once, so a consumer will eventually see
/// the same message twice: a redelivery after a crash, a retry that actually
/// succeeded, an outbox relay that published before it recorded doing so. If handling
/// a message twice is not safe, something has to remember.
/// </para>
/// <para>
/// The three states matter. <see cref="ClaimAsync"/> takes the message and returns
/// false if somebody already has it; <see cref="ConfirmAsync"/> records that it
/// finished; <see cref="ReleaseAsync"/> gives the claim back when it did not, so a
/// retry is not mistaken for a duplicate. A store with only "seen" and "not seen"
/// cannot tell a message that failed from one that succeeded, and will drop the
/// retry of a message that never completed.
/// </para>
/// </remarks>
public interface IIdempotencyStore
{
    /// <summary>Takes the message. False when it is already claimed or confirmed.</summary>
    Task<bool> ClaimAsync(string messageId);

    /// <summary>Records that handling finished.</summary>
    Task ConfirmAsync(string messageId);

    /// <summary>Gives the claim back, so a later attempt may take it.</summary>
    Task ReleaseAsync(string messageId);

    Task<bool> IsConfirmedAsync(string messageId);
}

/// <summary>An idempotency store in memory, bounded by age and size.</summary>
/// <remarks>
/// <strong>Per process, and lost on restart.</strong> It deduplicates the redeliveries
/// a single running consumer sees, which is most of them. It cannot deduplicate across
/// instances or across a restart — for that the store has to be the database the
/// handler already writes to, ideally in the same transaction as the work.
/// </remarks>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    private readonly TimeSpan _retention;
    private readonly int _maxEntries;
    private readonly object _lock = new object();
    private readonly Dictionary<string, Entry> _entries = new Dictionary<string, Entry>(StringComparer.Ordinal);
    private long _evictions;

    public InMemoryIdempotencyStore(TimeSpan retention) : this(retention, 100_000) { }

    public InMemoryIdempotencyStore(TimeSpan retention, int maxEntries)
    {
        if (maxEntries < 1) throw new ArgumentException("must be at least 1", nameof(maxEntries));
        _retention = retention;
        _maxEntries = maxEntries;
    }

    /// <summary>
    /// Keeps message ids for a day.
    /// </summary>
    /// <remarks>
    /// Retention is the window duplicates are caught in. Too short and a redelivery
    /// after a long outage is handled twice; too long and the store grows without
    /// bound. A day covers the redeliveries a broker actually produces.
    /// </remarks>
    public static InMemoryIdempotencyStore ForOneDay() =>
        new InMemoryIdempotencyStore(TimeSpan.FromDays(1));

    public Task<bool> ClaimAsync(string messageId)
    {
        if (messageId == null) throw new ArgumentNullException(nameof(messageId));
        lock (_lock)
        {
            Evict();
            if (_entries.TryGetValue(messageId, out var existing) && !existing.Expired(_retention))
            {
                return Task.FromResult(false);
            }
            _entries[messageId] = new Entry(DateTimeOffset.UtcNow, confirmed: false);
            return Task.FromResult(true);
        }
    }

    public Task ConfirmAsync(string messageId)
    {
        lock (_lock) _entries[messageId] = new Entry(DateTimeOffset.UtcNow, confirmed: true);
        return Task.CompletedTask;
    }

    public Task ReleaseAsync(string messageId)
    {
        lock (_lock) _entries.Remove(messageId);
        return Task.CompletedTask;
    }

    public Task<bool> IsConfirmedAsync(string messageId)
    {
        lock (_lock)
        {
            return Task.FromResult(
                _entries.TryGetValue(messageId, out var entry)
                && entry.Confirmed
                && !entry.Expired(_retention));
        }
    }

    public int Count { get { lock (_lock) return _entries.Count; } }

    /// <summary>Entries dropped because the store was full or they had aged out.</summary>
    public long Evictions => Interlocked.Read(ref _evictions);

    private void Evict()
    {
        var now = DateTimeOffset.UtcNow;
        var expired = new List<string>();
        foreach (var pair in _entries)
        {
            if (pair.Value.Expired(_retention)) expired.Add(pair.Key);
        }
        foreach (var key in expired)
        {
            _entries.Remove(key);
            Interlocked.Increment(ref _evictions);
        }

        if (_entries.Count < _maxEntries) return;

        // Full. Dropping the oldest is the least bad option: the alternative is
        // growing until the process runs out of memory, and an idempotency store
        // that takes the application down has not helped anybody.
        var oldest = new List<KeyValuePair<string, Entry>>(_entries);
        oldest.Sort((a, b) => a.Value.At.CompareTo(b.Value.At));
        var drop = _entries.Count - _maxEntries + 1;
        for (var i = 0; i < drop && i < oldest.Count; i++)
        {
            _entries.Remove(oldest[i].Key);
            Interlocked.Increment(ref _evictions);
        }
    }

    private readonly struct Entry
    {
        internal Entry(DateTimeOffset at, bool confirmed)
        {
            At = at;
            Confirmed = confirmed;
        }

        internal DateTimeOffset At { get; }
        internal bool Confirmed { get; }

        internal bool Expired(TimeSpan retention) => DateTimeOffset.UtcNow - At > retention;
    }
}
