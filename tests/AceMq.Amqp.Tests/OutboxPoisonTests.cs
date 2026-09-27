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

// A record the broker will never take must stop being tried.
//
// The relay claims a batch, publishes each record and records the ones that
// failed, releasing their lease so the next pass picks them up again. That is
// right for a broker that is down and wrong for a record it will never take --
// an exchange somebody deleted, a payload a policy will always refuse. Nothing
// bounded it: the `attempts` column was written from the first version and read
// by nothing, so a poison record was claimed on every pass for ever, spending a
// place in every batch and a publish attempt on a message that cannot go
// anywhere.
//
// Python, Go and Ruby stop offering a record after ten failures and keep it for
// somebody to look at. This is that bound for .NET.

using System;
using System.Data.Common;
using System.Threading.Tasks;
using AceMq.Amqp;
using Microsoft.Data.Sqlite;
using Xunit;

namespace AceMq.Amqp.Tests;

public sealed class OutboxPoisonTests : IDisposable
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");

    // One shared in-memory database per test, kept alive by holding a connection:
    // SQLite drops an in-memory database when the last connection to it closes.
    private readonly string _db = "Data Source=file:" + Guid.NewGuid().ToString("N")
                                 + "?mode=memory&cache=shared";
    private SqliteConnection? _keepAlive;

    private DbConnection Connect()
    {
        var connection = new SqliteConnection(_db);
        connection.Open();
        return connection;
    }

    private void WithSchema(string sql)
    {
        _keepAlive = new SqliteConnection(_db);
        _keepAlive.Open();
        foreach (var statement in sql.Split(';'))
        {
            if (statement.Trim().Length == 0) continue;
            using var command = _keepAlive.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }
    }

    public void Dispose() => _keepAlive?.Dispose();

    // "nowhere"/"nothing" is a destination the memory transport refuses, which is
    // what LeavesAFailedOutboxRecordForTheNextPass relies on too.
    private static OutboxRecord Poison() =>
        OutboxRecord.Of("nowhere", "nothing", Envelope.Of("order.placed").Build(), "\"A-1\"");

    [Fact]
    public async Task StopsClaimingARecordTheBrokerWillNeverTake()
    {
        var store = new DbOutboxStore(Connect, "acemq_outbox", "@", maxAttempts: 2);
        WithSchema(store.CreateTableSql());
        await store.AddAsync(Poison());

        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var relay = mq.Outbox(store);

        // Two passes, two failures. Each one releases the lease and counts the
        // attempt, which is right while there is any chance the next pass succeeds.
        Assert.Equal(0, await relay.DrainOnceAsync());
        Assert.Equal(0, await relay.DrainOnceAsync());

        // And now it is left alone rather than claimed again for ever.
        Assert.Empty(await store.ClaimBatchAsync(10, TimeSpan.FromSeconds(30)));
        Assert.Equal(0, await relay.DrainOnceAsync());
    }

    [Fact]
    public async Task ARetiredRecordIsKeptRatherThanDeleted()
    {
        var store = new DbOutboxStore(Connect, "acemq_outbox", "@", maxAttempts: 1);
        WithSchema(store.CreateTableSql());
        await store.AddAsync(Poison());

        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var relay = mq.Outbox(store);
        Assert.Equal(0, await relay.DrainOnceAsync());

        // Still owed, so still counted: a stuck outbox must not read as an empty one.
        Assert.Equal(1, await store.PendingCountAsync());

        // A record nothing could publish is evidence. Somebody has to be able to read
        // it, fix whatever refuses it and release it by putting its attempts back to
        // zero, and nothing but this surfaces it -- ClaimBatchAsync exists to skip it.
        var retired = Assert.Single(await store.RetiredAsync());
        Assert.Equal(1, retired.Attempts);
        Assert.False(string.IsNullOrEmpty(retired.LastError));
        // The payload and the envelope have to survive, or what is kept is not
        // something anybody can release.
        Assert.Equal("\"A-1\"", retired.Payload);
        Assert.Equal("order.placed", retired.Type);
    }

    [Fact]
    public async Task AGoodRecordStillGoesOutWhileAPoisonOneIsStuck()
    {
        var store = new DbOutboxStore(Connect, "acemq_outbox", "@", maxAttempts: 1);
        WithSchema(store.CreateTableSql());

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("poison-neighbours");

        await store.AddAsync(Poison());
        await store.AddAsync(OutboxRecord.Of(
            "", "poison-neighbours", Envelope.Of("order.placed").Build(), "\"A-2\""));

        using var relay = mq.Outbox(store);

        Assert.Equal(1, await relay.DrainOnceAsync());
        Assert.Equal(1, await mq.MessageCountAsync("poison-neighbours"));

        // One left, and it is the retired one rather than anything still to send.
        Assert.Equal(1, await store.PendingCountAsync());
        Assert.Single(await store.RetiredAsync());
        Assert.Empty(await store.ClaimBatchAsync(10, TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task TheInMemoryStoreRetiresARecordTheSameWay()
    {
        var store = new InMemoryOutboxStore(maxAttempts: 2);
        await store.AddAsync(Poison());

        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var relay = mq.Outbox(store);

        Assert.Equal(0, await relay.DrainOnceAsync());
        Assert.Equal(0, await relay.DrainOnceAsync());

        Assert.Empty(await store.ClaimBatchAsync(10, TimeSpan.FromSeconds(30)));

        var retired = Assert.Single(store.Retired());
        Assert.Equal(2, retired.Attempts);
        Assert.False(string.IsNullOrEmpty(retired.LastError));

        // Kept, not thrown away.
        Assert.Single(store.Pending());
        Assert.Equal(1, await store.PendingCountAsync());
    }

    [Fact]
    public async Task ARecordThatSucceedsIsNotCountedAgainstItsAttempts()
    {
        var store = new InMemoryOutboxStore(maxAttempts: 1);

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("poison-good-path");
        await store.AddAsync(OutboxRecord.Of(
            "", "poison-good-path", Envelope.Of("order.placed").Build(), "\"A-3\""));

        using var relay = mq.Outbox(store);

        Assert.Equal(1, await relay.DrainOnceAsync());
        Assert.Empty(store.Pending());
        Assert.Empty(store.Retired());
    }

    [Fact]
    public void AStoreThatRetiresBeforeTryingOnceIsRefused()
    {
        // A store with a maximum below one publishes nothing at all, which is a
        // silent outage rather than a configuration choice.
        Assert.Throws<ArgumentException>(() => new InMemoryOutboxStore(0));
        Assert.Throws<ArgumentException>(
            () => new DbOutboxStore(Connect, "acemq_outbox", "@", 0));
    }
}
