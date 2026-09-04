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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>A message written down to be published later.</summary>
public sealed class OutboxRecord
{
    public OutboxRecord(
        string id, string exchange, string routingKey, string type, string payload,
        string? correlationId, string? causationId, DateTimeOffset createdAt,
        int attempts, string? lastError)
    {
        Id = id;
        Exchange = exchange;
        RoutingKey = routingKey;
        Type = type;
        Payload = payload;
        CorrelationId = correlationId;
        CausationId = causationId;
        CreatedAt = createdAt;
        Attempts = attempts;
        LastError = lastError;
    }

    public static OutboxRecord Of(string exchange, string routingKey, Envelope envelope, string payload) =>
        new OutboxRecord(
            envelope.Id, exchange, routingKey, envelope.Type, payload,
            envelope.CorrelationId, envelope.CausationId, DateTimeOffset.UtcNow, 0, null);

    public string Id { get; }
    public string Exchange { get; }
    public string RoutingKey { get; }
    public string Type { get; }

    /// <summary>The encoded body, as text, so a store can hold it in one column.</summary>
    public string Payload { get; }

    public string? CorrelationId { get; }
    public string? CausationId { get; }
    public DateTimeOffset CreatedAt { get; }
    public int Attempts { get; }
    public string? LastError { get; }

    /// <summary>The envelope this record was written with.</summary>
    public Envelope Envelope()
    {
        var builder = Amqp.Envelope.Of(Type).Id(Id).FirstSeen(CreatedAt);
        if (CorrelationId != null) builder.CorrelationId(CorrelationId);
        if (CausationId != null) builder.CausationId(CausationId);
        return builder.Build();
    }

    public override string ToString() => $"OutboxRecord[{Id}, {Type}, attempts={Attempts}]";
}

/// <summary>Where outbox records are kept between being written and being published.</summary>
/// <remarks>
/// The store is expected to be the same database as the business data, written in
/// the same transaction. That is the whole point: a record and the message announcing
/// it either both exist or neither does. A store in a different database is not an
/// outbox, it is a second thing that can fail.
/// </remarks>
public interface IOutboxStore
{
    Task AddAsync(OutboxRecord record);

    /// <summary>
    /// Takes up to <paramref name="batchSize"/> unpublished records and holds them
    /// for <paramref name="lease"/>, so two relays do not publish the same message.
    /// </summary>
    Task<IReadOnlyList<OutboxRecord>> ClaimBatchAsync(int batchSize, TimeSpan lease);

    Task MarkPublishedAsync(string id);

    Task MarkFailedAsync(string id, string reason);

    Task<long> PendingCountAsync();
}

/// <summary>An outbox store in memory, for tests and for demonstrating the shape.</summary>
/// <remarks>
/// <strong>Not an outbox.</strong> It shares the process's lifetime, so it cannot be
/// written in the same transaction as anything durable, and everything in it is lost
/// on a restart — which is precisely the failure the pattern exists to prevent. Use
/// it to exercise a relay; use a database-backed store in production.
/// </remarks>
public sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly object _lock = new object();
    private readonly List<Entry> _entries = new List<Entry>();

    public Task AddAsync(OutboxRecord record)
    {
        lock (_lock) _entries.Add(new Entry(record));
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<OutboxRecord>> ClaimBatchAsync(int batchSize, TimeSpan lease)
    {
        var now = DateTimeOffset.UtcNow;
        lock (_lock)
        {
            var claimed = _entries
                .Where(e => !e.Published && e.LeasedUntil <= now)
                .OrderBy(e => e.Record.CreatedAt)
                .Take(batchSize)
                .ToList();
            foreach (var e in claimed) e.LeasedUntil = now + lease;
            return Task.FromResult<IReadOnlyList<OutboxRecord>>(
                claimed.Select(e => e.Record).ToArray());
        }
    }

    public Task MarkPublishedAsync(string id)
    {
        lock (_lock)
        {
            var entry = _entries.FirstOrDefault(e => e.Record.Id == id);
            if (entry != null) entry.Published = true;
        }
        return Task.CompletedTask;
    }

    public Task MarkFailedAsync(string id, string reason)
    {
        lock (_lock)
        {
            var entry = _entries.FirstOrDefault(e => e.Record.Id == id);
            if (entry == null) return Task.CompletedTask;
            var r = entry.Record;
            // The lease is released so the next pass picks it up again, and the
            // attempt count carries so a record that always fails is visible.
            entry.Record = new OutboxRecord(
                r.Id, r.Exchange, r.RoutingKey, r.Type, r.Payload, r.CorrelationId,
                r.CausationId, r.CreatedAt, r.Attempts + 1, reason);
            entry.LeasedUntil = DateTimeOffset.MinValue;
        }
        return Task.CompletedTask;
    }

    public Task<long> PendingCountAsync()
    {
        lock (_lock) return Task.FromResult((long)_entries.Count(e => !e.Published));
    }

    /// <summary>Everything still unpublished, for a test to assert on.</summary>
    public IReadOnlyList<OutboxRecord> Pending()
    {
        lock (_lock) return _entries.Where(e => !e.Published).Select(e => e.Record).ToArray();
    }

    private sealed class Entry
    {
        internal Entry(OutboxRecord record) => Record = record;
        internal OutboxRecord Record;
        internal bool Published;
        internal DateTimeOffset LeasedUntil = DateTimeOffset.MinValue;
    }
}

/// <summary>
/// Publishes what an <see cref="IOutboxStore"/> has been given.
/// </summary>
/// <remarks>
/// <para>
/// The pattern this completes: a service writes its business change and an outbox
/// record in one database transaction, and this publishes the record afterwards.
/// Publishing inside the transaction instead would send a message about a change
/// that might still be rolled back; publishing after committing, without an outbox,
/// loses the message if the process dies in between.
/// </para>
/// <para>
/// Delivery is therefore <strong>at least once</strong>. A relay that publishes and
/// then dies before marking the record will publish it again, so consumers have to
/// tolerate duplicates — the envelope's id is the idempotency key for that.
/// </para>
/// </remarks>
public sealed class OutboxRelay : IDisposable
{
    private readonly AceMqConnection _mq;
    private readonly IOutboxStore _store;
    private readonly int _batchSize;
    private readonly TimeSpan _pollInterval;
    private readonly TimeSpan _lease;
    private readonly CancellationTokenSource _stop = new CancellationTokenSource();
    private long _published;
    private long _failed;
    private bool _running;
    private bool _disposed;

    public OutboxRelay(AceMqConnection mq, IOutboxStore store)
        : this(mq, store, 100, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)) { }

    public OutboxRelay(
        AceMqConnection mq, IOutboxStore store, int batchSize,
        TimeSpan pollInterval, TimeSpan lease)
    {
        _mq = mq ?? throw new ArgumentNullException(nameof(mq));
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _batchSize = batchSize;
        _pollInterval = pollInterval;
        _lease = lease;
    }

    public long Published => Interlocked.Read(ref _published);
    public long Failed => Interlocked.Read(ref _failed);
    public bool IsRunning => _running && !_stop.IsCancellationRequested;

    /// <summary>Starts polling in the background.</summary>
    public void Start()
    {
        if (_running) return;
        _running = true;
        _ = Task.Run(async () =>
        {
            while (!_stop.IsCancellationRequested)
            {
                try
                {
                    var moved = await DrainOnceAsync().ConfigureAwait(false);
                    if (moved == 0)
                    {
                        await Task.Delay(_pollInterval, _stop.Token).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) { return; }
                catch
                {
                    // A relay that stops on the first failure stops publishing
                    // everything, not just the record that failed. Individual
                    // failures are recorded against the record by DrainOnceAsync.
                    try { await Task.Delay(_pollInterval, _stop.Token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { return; }
                }
            }
        });
    }

    /// <summary>Publishes one batch and returns how many went out.</summary>
    public async Task<int> DrainOnceAsync()
    {
        var batch = await _store.ClaimBatchAsync(_batchSize, _lease).ConfigureAwait(false);
        var moved = 0;

        foreach (var record in batch)
        {
            try
            {
                var publisher = _mq.Publisher<string>(record.Exchange, record.RoutingKey);
                await publisher.SendAsync(record.Payload, record.Envelope()).ConfigureAwait(false);
                await _store.MarkPublishedAsync(record.Id).ConfigureAwait(false);
                Interlocked.Increment(ref _published);
                moved++;
            }
            catch (Exception e)
            {
                Interlocked.Increment(ref _failed);
                await _store.MarkFailedAsync(record.Id, e.Message).ConfigureAwait(false);
            }
        }

        return moved;
    }

    /// <summary>Publishes batches until the store has nothing left.</summary>
    public async Task<int> DrainAsync()
    {
        var total = 0;
        while (true)
        {
            var moved = await DrainOnceAsync().ConfigureAwait(false);
            if (moved == 0) return total;
            total += moved;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _running = false;
        _stop.Cancel();
        _stop.Dispose();
    }

    public override string ToString() =>
        $"OutboxRelay[published={Published}, failed={Failed}, running={IsRunning}]";
}
