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

using System.Collections.Concurrent;
using System.Data.Common;
using System.Text;
using AceMq.Amqp;
using Microsoft.Data.Sqlite;

namespace AceMq.Amqp.Tests;

public sealed class Order
{
    public string Id { get; set; } = "";
    public decimal Total { get; set; }
}

public sealed class ExtensibilityTests : IDisposable
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");
    private readonly string _q = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);

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

    private void WithSchema(params string[] statements)
    {
        _keepAlive = new SqliteConnection(_db);
        _keepAlive.Open();
        foreach (var sql in statements)
        {
            foreach (var statement in sql.Split(';'))
            {
                if (statement.Trim().Length == 0) continue;
                using var command = _keepAlive.CreateCommand();
                command.CommandText = statement;
                command.ExecuteNonQuery();
            }
        }
    }

    public void Dispose() => _keepAlive?.Dispose();

    private static async Task Eventually(Func<bool> probe, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (probe()) return;
            await Task.Delay(10);
        }
        throw new TimeoutException($"timed out waiting for {what}");
    }

    // ---- codecs ----------------------------------------------------------

    [Fact]
    public void RoundTripsThroughXml()
    {
        var codec = new XmlCodec();
        var order = new Order { Id = "A-1", Total = 42.5m };

        var back = (Order)codec.Decode(codec.Encode(order), typeof(Order));

        Assert.Equal("A-1", back.Id);
        Assert.Equal(42.5m, back.Total);
        Assert.True(codec.CanDecode("application/xml"));
        Assert.False(codec.CanDecode("application/json"));
    }

    [Fact]
    public void RefusesXmlThatPullsInAnExternalEntity()
    {
        // The XXE class of attack: a document that makes the parser read a local
        // file and hand it back inside the deserialized object. "We accept XML"
        // becomes "we read files off the consumer's disk on request".
        var hostile = Encoding.UTF8.GetBytes(
            "<?xml version=\"1.0\"?><!DOCTYPE Order [<!ENTITY x SYSTEM \"file:///etc/passwd\">]>"
            + "<Order><Id>&x;</Id></Order>");

        Assert.ThrowsAny<Exception>(() => new XmlCodec().Decode(hostile, typeof(Order)));
    }

    [Fact]
    public void ReadsEitherFormatDuringAMigration()
    {
        var composite = CompositeCodec.Of(new JsonCodec(), new XmlCodec());
        var order = new Order { Id = "A-2", Total = 1m };

        // The first codec encodes; either can decode. That is what lets a service
        // start publishing the new format while still reading the old one.
        Assert.Equal("application/json", composite.ContentType);

        var asXml = new XmlCodec().Encode(order);
        var back = (Order)composite.Decode(asXml, typeof(Order), "application/xml");
        Assert.Equal("A-2", back.Id);

        var asJson = new JsonCodec().Encode(order);
        Assert.Equal("A-2", ((Order)composite.Decode(asJson, typeof(Order), "application/json")).Id);
    }

    [Fact]
    public void FindsCodecsByName()
    {
        Assert.Equal(new[] { "bytes", "json", "string", "xml" }, CodecRegistry.Names());
        Assert.IsType<XmlCodec>(CodecRegistry.ByName("xml"));

        var error = Assert.Throws<AceFatalException>(() => CodecRegistry.ByName("avro"));
        Assert.Contains("no codec named 'avro'", error.Message);
    }

    // ---- encryption ------------------------------------------------------

    [Fact]
    public void EncryptsTheBodySoTheBrokerCannotReadIt()
    {
        var key = EncryptionKey.Generate("k1");
        var codec = EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(key));
        var order = new Order { Id = "SECRET-1", Total = 99m };

        var wire = codec.Encode(order);

        // What the broker stores must not contain the payload.
        Assert.DoesNotContain("SECRET-1", Encoding.UTF8.GetString(wire));
        Assert.Equal("k1", EncryptedCodec.KeyIdOf(wire));

        var back = (Order)codec.Decode(wire, typeof(Order));
        Assert.Equal("SECRET-1", back.Id);
    }

    [Fact]
    public void ProducesADifferentCiphertextEachTime()
    {
        var codec = EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(EncryptionKey.Generate("k1")));
        var order = new Order { Id = "A-1", Total = 1m };

        // A fresh IV per message. Identical ciphertexts for identical plaintexts
        // would tell an observer which messages are repeats without decrypting any.
        Assert.NotEqual(
            Convert.ToBase64String(codec.Encode(order)),
            Convert.ToBase64String(codec.Encode(order)));
    }

    [Fact]
    public void RejectsAnAlteredBodyBeforeDecryptingIt()
    {
        var codec = EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(EncryptionKey.Generate("k1")));
        var wire = codec.Encode(new Order { Id = "A-1", Total = 1m });

        // Flip a bit in the ciphertext. Authenticating before decrypting is what
        // stops this becoming a padding oracle.
        wire[wire.Length - 40] ^= 0x01;

        var error = Assert.Throws<AceFatalException>(() => codec.Decode(wire, typeof(Order)));
        Assert.Contains("failed authentication", error.Message);
    }

    [Fact]
    public void RejectsABodyWhoseKeyIdWasTamperedWith()
    {
        var keyring = Keyring.Builder()
            .Add(EncryptionKey.Generate("old"))
            .Current(EncryptionKey.Generate("new"))
            .Build();
        var codec = EncryptedCodec.Wrapping(new JsonCodec(), keyring);
        var wire = codec.Encode(new Order { Id = "A-1", Total = 1m });

        // The key id is inside what the tag covers, so steering a consumer at a
        // different key breaks authentication rather than succeeding quietly.
        wire[2] = (byte)'o'; wire[3] = (byte)'l'; wire[4] = (byte)'d';

        Assert.Throws<AceFatalException>(() => codec.Decode(wire, typeof(Order)));
    }

    [Fact]
    public void ReadsMessagesEncryptedWithARotatedOutKey()
    {
        var old = EncryptionKey.Generate("2025");
        var current = EncryptionKey.Generate("2026");

        var before = EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(old));
        var wire = before.Encode(new Order { Id = "A-1", Total = 1m });

        // After rotation the old key stays readable, because messages encrypted
        // with it are still sitting in queues.
        var after = EncryptedCodec.Wrapping(
            new JsonCodec(), Keyring.Builder().Add(old).Current(current).Build());

        Assert.Equal("A-1", ((Order)after.Decode(wire, typeof(Order))).Id);

        var withoutOld = EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(current));
        var error = Assert.Throws<AceFatalException>(() => withoutOld.Decode(wire, typeof(Order)));
        Assert.Contains("not on the keyring", error.Message);
    }

    [Fact]
    public async Task CarriesAnEncryptedPayloadThroughTheBroker()
    {
        var codec = EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(EncryptionKey.Generate("k1")));
        using var mq = await AceMqConnection.ConnectAsync(_url, codec);
        await mq.DeclareQueueAsync(_q);

        Order? received = null;
        using var consumer = await mq.ConsumeAsync<Order>(_q, m =>
        {
            received = m.Payload;
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<Order>("", _q).SendAsync(new Order { Id = "A-9", Total = 5m });

        await Eventually(() => received != null, "the encrypted message");
        Assert.Equal("A-9", received!.Id);
    }

    // ---- schema registry -------------------------------------------------

    [Fact]
    public void GivesTheSameSchemaTheSameId()
    {
        var registry = new InMemorySchemaRegistry();
        var schema = new SchemaDefinition("json", "order.placed", "{\"type\":\"object\"}");
        var same = new SchemaDefinition("json", "order.placed", "{\"type\":\"object\"}");
        var different = new SchemaDefinition("json", "order.placed", "{\"type\":\"string\"}");

        var id = registry.IdFor(schema);
        Assert.Equal(id, registry.IdFor(same));
        Assert.NotEqual(id, registry.IdFor(different));
        Assert.Equal(schema, registry.SchemaFor(id));
        Assert.Equal(2, registry.Count);
    }

    [Fact]
    public void SaysWhyAnUnknownSchemaIdCannotBeResolved()
    {
        var error = Assert.Throws<AceFatalException>(() => new InMemorySchemaRegistry().SchemaFor(7));
        // The reason is nearly always that the id came from another process.
        Assert.Contains("per process", error.Message);
    }

    // ---- interceptors ----------------------------------------------------

    [Fact]
    public async Task LetsAnInterceptorAddAHeaderToEveryPublish()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        mq.Intercept(new TenantStamp("acme"));

        await mq.DeclareQueueAsync(_q);

        IMessage<Order>? received = null;
        using var consumer = await mq.ConsumeAsync<Order>(_q, m =>
        {
            received = m;
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<Order>("", _q).SendAsync(new Order { Id = "A-1", Total = 1m });

        await Eventually(() => received != null, "the intercepted message");
        Assert.Equal("acme", received!.Headers["x-tenant"]);
    }

    [Fact]
    public async Task TellsInterceptorsWhatHappenedToThePublish()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var recorder = new Recorder();
        mq.Intercept(recorder);
        await mq.DeclareExchangeAsync("orders", "topic");

        await Assert.ThrowsAsync<PublishFailedException>(
            () => mq.Publisher<Order>("orders", _q).SendAsync(new Order { Id = "A-2", Total = 1m }));

        Assert.Equal(1, recorder.Errors);
        Assert.Equal(0, recorder.Confirms);
    }

    [Fact]
    public async Task DoesNotFailAPublishBecauseAnInterceptorThrewAfterwards()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        mq.Intercept(new ThrowsAfterConfirm());
        await mq.DeclareQueueAsync(_q);

        // The broker already has the message. Reporting a failure would have the
        // caller publish it a second time.
        var result = await mq.Publisher<Order>("", _q).SendAsync(new Order { Id = "A-3", Total = 1m });
        Assert.True(result.Routed);
    }

    [Fact]
    public async Task RunsConsumeInterceptorsAroundTheHandler()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var seen = new ConcurrentQueue<string>();
        mq.Intercept(new ConsumeRecorder(seen));
        await mq.DeclareQueueAsync(_q);

        using var consumer = await mq.ConsumeAsync<Order>(
            _q, _ => Task.FromResult(Ack.Accept()));
        await mq.Publisher<Order>("", _q).SendAsync(new Order { Id = "A-4", Total = 1m });

        await Eventually(() => seen.Count >= 2, "before and after");
        Assert.Equal(new[] { "before", "after:Accept" }, seen.Take(2).ToArray());
    }

    private sealed class TenantStamp : PublishInterceptor
    {
        private readonly string _tenant;
        internal TenantStamp(string tenant) => _tenant = tenant;

        public override PublishContext BeforePublish(PublishContext context) =>
            context.WithEnvelope(
                Envelope.Of(context.Envelope.Type)
                    .Id(context.Envelope.Id)
                    .CorrelationId(context.Envelope.CorrelationId)
                    .Header("x-tenant", _tenant)
                    .Build());
    }

    private sealed class Recorder : PublishInterceptor
    {
        internal int Confirms;
        internal int Errors;
        public override void AfterConfirm(PublishContext context, PublishResult result) => Confirms++;
        public override void OnError(PublishContext context, Exception failure) => Errors++;
    }

    private sealed class ThrowsAfterConfirm : PublishInterceptor
    {
        public override void AfterConfirm(PublishContext context, PublishResult result) =>
            throw new InvalidOperationException("the audit log is down");
    }

    private sealed class ConsumeRecorder : ConsumeInterceptor
    {
        private readonly ConcurrentQueue<string> _seen;
        internal ConsumeRecorder(ConcurrentQueue<string> seen) => _seen = seen;
        public override void BeforeHandle(ConsumeContext context) => _seen.Enqueue("before");
        public override void AfterHandle(ConsumeContext context, Ack ack) => _seen.Enqueue("after:" + ack.Kind);
    }

    // ---- database-backed stores -----------------------------------------

    [Fact]
    public async Task KeepsAnOutboxInADatabase()
    {
        var store = new DbOutboxStore(Connect);
        WithSchema(store.CreateTableSql());

        await store.AddAsync(OutboxRecord.Of(
            "", "events", Envelope.Of("order.placed").Build(), "\"A-1\""));
        Assert.Equal(1, await store.PendingCountAsync());

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("events");
        using var relay = mq.Outbox(store);

        Assert.Equal(1, await relay.DrainAsync());
        Assert.Equal(0, await store.PendingCountAsync());
        Assert.Equal(1, await mq.MessageCountAsync("events"));
    }

    [Fact]
    public async Task WritesTheOutboxRecordInsideTheCallersTransaction()
    {
        var store = new DbOutboxStore(Connect);
        WithSchema(store.CreateTableSql(), "CREATE TABLE orders (id VARCHAR(64) PRIMARY KEY)");

        using (var connection = Connect())
        using (var transaction = connection.BeginTransaction())
        {
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "INSERT INTO orders (id) VALUES ('A-1')";
                command.ExecuteNonQuery();
            }
            await store.AddAsync(
                OutboxRecord.Of("", "events", Envelope.Of("order.placed").Build(), "\"A-1\""),
                transaction);

            // Rolled back, not committed. The business change and the message have
            // to disappear together -- that is the entire guarantee.
            transaction.Rollback();
        }

        Assert.Equal(0, await store.PendingCountAsync());
    }

    [Fact]
    public async Task LeavesAFailedOutboxRecordForTheNextPass()
    {
        var store = new DbOutboxStore(Connect);
        WithSchema(store.CreateTableSql());

        await store.AddAsync(OutboxRecord.Of(
            "nowhere", "nothing", Envelope.Of("order.placed").Build(), "\"A-1\""));

        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var relay = mq.Outbox(store);

        Assert.Equal(0, await relay.DrainOnceAsync());
        Assert.Equal(1, await store.PendingCountAsync());

        // The lease was released and the attempt recorded, so the next pass sees it.
        var again = await store.ClaimBatchAsync(10, TimeSpan.FromSeconds(30));
        Assert.Equal(1, again.Count);
        Assert.Equal(1, again[0].Attempts);
        Assert.NotNull(again[0].LastError);
    }

    [Fact]
    public async Task LetsOnlyOneClaimantHaveAMessage()
    {
        var store = new DbIdempotencyStore(Connect, TimeSpan.FromMinutes(5));
        WithSchema(store.CreateTableSql());

        // The primary key does the mutual exclusion, so two consumers racing for
        // the same message cannot both proceed.
        Assert.True(await store.ClaimAsync("m-1"));
        Assert.False(await store.ClaimAsync("m-1"));

        await store.ReleaseAsync("m-1");
        Assert.True(await store.ClaimAsync("m-1"));

        await store.ConfirmAsync("m-1");
        Assert.True(await store.IsConfirmedAsync("m-1"));
        Assert.False(await store.IsConfirmedAsync("m-2"));
    }

    [Fact]
    public async Task DeduplicatesAcrossConsumersThroughTheDatabase()
    {
        var store = new DbIdempotencyStore(Connect, TimeSpan.FromMinutes(5));
        WithSchema(store.CreateTableSql());

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        var handled = 0;
        using var consumer = await mq.ConsumeAsync<Order>(
            _q, ConsumerOptions.Defaults().Idempotent(store),
            _ => { Interlocked.Increment(ref handled); return Task.FromResult(Ack.Accept()); });

        var envelope = Envelope.Of("order.placed").Build();
        var publisher = mq.Publisher<Order>("", _q);
        await publisher.SendAsync(new Order { Id = "A-1", Total = 1m }, envelope);
        await publisher.SendAsync(new Order { Id = "A-1", Total = 1m }, envelope);

        await Eventually(() => handled >= 1, "the first delivery");
        await Task.Delay(200);
        Assert.Equal(1, handled);
    }
}
