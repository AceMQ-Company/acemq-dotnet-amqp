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

// A claim is a lease, not a fact.
//
// A handler that throws releases its claim, so an ordinary failure is retried. A
// handler whose *process* dies releases nothing — nothing runs when a process is
// killed — and the row it left behind said "somebody has this message" for as long
// as the store remembered it. Retention is the duplicate-catching window and is
// meant to be long: a day, in the documentation. So a consumer killed mid-handler
// suppressed that message's redelivery for a day, and the work never happened.
//
// Not a duplicate, which this pattern is allowed to produce — a silent loss, in the
// pattern that exists to prevent exactly that. Java, Go, Python and Ruby all honour
// an unconfirmed claim for five minutes and then let another consumer take it. .NET
// was the last of the five without the lease, which is the same defect Go fixed in
// its 0.8.0.
using System;
using System.Data.Common;
using System.Threading.Tasks;
using AceMq.Amqp;
using Microsoft.Data.Sqlite;
using Xunit;

namespace AceMq.Amqp.Tests;

public sealed class ClaimLeaseTests : IDisposable
{
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

    // A claim window short enough to watch expire, and a retention long enough that
    // the row is still there when it does — which is the pair that matters: the bug
    // was that the only expiry was retention.
    private static readonly TimeSpan ShortLease = TimeSpan.FromMilliseconds(80);
    private static readonly TimeSpan LongRetention = TimeSpan.FromDays(1);

    [Fact]
    public async Task AnUnconfirmedClaimCanBeTakenOverAfterItsLeaseRunsOut()
    {
        var store = new DbIdempotencyStore(Connect, LongRetention, "acemq_idempotency", "@", ShortLease);
        WithSchema(store.CreateTableSql());

        // A consumer takes the message and then dies: no Confirm, no Release,
        // because nothing runs when a process is killed.
        Assert.True(await store.ClaimAsync("m-1"));
        Assert.False(await store.ClaimAsync("m-1"));

        await Task.Delay(200);

        Assert.True(await store.ClaimAsync("m-1"),
            "the redelivery was suppressed: the claim of a consumer that died is honoured " +
            "until the retention window ends, so the work never happens");
    }

    [Fact]
    public async Task AConfirmedMessageIsNeverTakenOver()
    {
        var store = new DbIdempotencyStore(Connect, LongRetention, "acemq_idempotency", "@", ShortLease);
        WithSchema(store.CreateTableSql());

        Assert.True(await store.ClaimAsync("m-1"));
        await store.ConfirmAsync("m-1");

        await Task.Delay(200);

        // Confirmed is done, and stays done for the retention window. A lease that
        // expired a completed message would hand the work out a second time, which
        // is the duplicate this store exists to stop.
        Assert.False(await store.ClaimAsync("m-1"),
            "a confirmed message was handed out again after the claim window passed");
        Assert.True(await store.IsConfirmedAsync("m-1"));
    }

    [Fact]
    public async Task AClaimInsideItsWindowBelongsToWhoeverTookIt()
    {
        var store = new DbIdempotencyStore(
            Connect, LongRetention, "acemq_idempotency", "@", TimeSpan.FromMinutes(5));
        WithSchema(store.CreateTableSql());

        Assert.True(await store.ClaimAsync("m-1"));

        // Five minutes have not passed. A second consumer must not get it, or two
        // handlers run at once on one message — which is what the lease has to be
        // longer than the slowest handler for.
        Assert.False(await store.ClaimAsync("m-1"));
        Assert.False(await store.ClaimAsync("m-1"));
    }

    [Fact]
    public async Task OnlyOneConsumerWinsTheRaceToTakeOverAStaleClaim()
    {
        var store = new DbIdempotencyStore(Connect, LongRetention, "acemq_idempotency", "@", ShortLease);
        WithSchema(store.CreateTableSql());

        Assert.True(await store.ClaimAsync("m-1"));
        await Task.Delay(200);

        // The steal is a conditional UPDATE, so the database does the mutual
        // exclusion here exactly as the primary key does it for a fresh claim. Two
        // consumers both finding the claim stale must not both proceed.
        var attempts = new Task<bool>[8];
        for (var i = 0; i < attempts.Length; i++) attempts[i] = store.ClaimAsync("m-1");
        var results = await Task.WhenAll(attempts);

        var winners = 0;
        foreach (var won in results)
        {
            if (won) winners++;
        }
        Assert.Equal(1, winners);
    }

    [Fact]
    public async Task TheInMemoryStoreLeasesAClaimTheSameWay()
    {
        var store = new InMemoryIdempotencyStore(LongRetention, 1000, ShortLease);

        Assert.True(await store.ClaimAsync("m-1"));
        Assert.False(await store.ClaimAsync("m-1"));

        await Task.Delay(200);
        Assert.True(await store.ClaimAsync("m-1"));

        // And a confirmed one still is not reissued.
        await store.ConfirmAsync("m-1");
        await Task.Delay(200);
        Assert.False(await store.ClaimAsync("m-1"));
    }

    [Fact]
    public void AClaimWindowOfZeroIsRefused()
    {
        // A lease that has already expired hands every message to every consumer,
        // which is an idempotency store that deduplicates nothing.
        Assert.Throws<ArgumentException>(
            () => new InMemoryIdempotencyStore(LongRetention, 1000, TimeSpan.Zero));
        Assert.Throws<ArgumentException>(
            () => new DbIdempotencyStore(Connect, LongRetention, "acemq_idempotency", "@", TimeSpan.Zero));
    }

    [Fact]
    public async Task TheDefaultClaimWindowIsTheFamilysFiveMinutes()
    {
        // Named rather than inferred: a message stuck in one language should be stuck
        // for the same length of time in all five.
        Assert.Equal(TimeSpan.FromMinutes(5), InMemoryIdempotencyStore.DefaultClaimTimeout);

        // And the constructors that do not mention it use it.
        var store = new DbIdempotencyStore(Connect, LongRetention);
        WithSchema(store.CreateTableSql());
        Assert.True(await store.ClaimAsync("m-1"));
        Assert.False(await store.ClaimAsync("m-1"));
    }

    [Fact]
    public async Task AnInsertThatFailsForAnotherReasonIsNotTakenForADuplicate()
    {
        // The claim is an insert, and a duplicate key is one reason an insert fails.
        // It is not the only one: a lock timeout, a deadlock victim, a full disk. Any
        // DbException used to be read as "somebody already has this message", so the
        // claim answered false, the consumer acknowledged the message as a duplicate,
        // and a message nothing had handled was gone. Java fixed the same thing by
        // asking whether the row is actually there; this asks the same question.
        var store = new DbIdempotencyStore(Connect, LongRetention, "acemq_idempotency", "@", ShortLease);
        WithSchema(store.CreateTableSql());
        using (var refuse = _keepAlive!.CreateCommand())
        {
            refuse.CommandText =
                "CREATE TRIGGER refuse BEFORE INSERT ON acemq_idempotency " +
                "BEGIN SELECT RAISE(ABORT, 'database is locked'); END";
            refuse.ExecuteNonQuery();
        }

        await Assert.ThrowsAnyAsync<DbException>(() => store.ClaimAsync("m-1"));
    }

    // ---- why a claim was refused -------------------------------------------
    //
    // ClaimAsync answers false for two different things. Confirmed is finished work,
    // and a redelivery of it is a duplicate to acknowledge. Claimed and unconfirmed
    // is work nobody has finished: acknowledging that redelivery lost the message
    // whenever the first handler had failed and its release failed too.

    [Fact]
    public async Task TheDatabaseStoreSaysWhetherARefusedClaimIsDoneOrInProgress()
    {
        var store = new DbIdempotencyStore(Connect, LongRetention, "acemq_idempotency", "@", ShortLease);
        WithSchema(store.CreateTableSql());
        await SaysWhyAClaimWasRefused(store);
    }

    [Fact]
    public async Task TheInMemoryStoreSaysWhetherARefusedClaimIsDoneOrInProgress()
    {
        await SaysWhyAClaimWasRefused(new InMemoryIdempotencyStore(LongRetention, 1000, ShortLease));
    }

    private static async Task SaysWhyAClaimWasRefused(IClaimingIdempotencyStore store)
    {
        Assert.Equal(ClaimResult.Claimed, await store.TryClaimAsync("m-1"));
        Assert.Equal(ClaimResult.InProgress, await store.TryClaimAsync("m-1"));

        // An expired lease is still taken over, not reported as in progress.
        await Task.Delay(200);
        Assert.Equal(ClaimResult.Claimed, await store.TryClaimAsync("m-1"));

        await store.ConfirmAsync("m-1");
        Assert.Equal(ClaimResult.Duplicate, await store.TryClaimAsync("m-1"));
        await Task.Delay(200);
        Assert.Equal(ClaimResult.Duplicate, await store.TryClaimAsync("m-1"));

        // And the two-state answer is unchanged.
        Assert.False(await store.ClaimAsync("m-1"));
        Assert.True(await store.ClaimAsync("m-2"));
        Assert.False(await store.ClaimAsync("m-2"));
    }
}
