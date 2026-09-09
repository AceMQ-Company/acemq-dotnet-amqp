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
using AceMq.Amqp;
using Microsoft.Data.Sqlite;

namespace AceMq.Amqp.Tests;

public sealed class RoutingAndDriftTests : IDisposable
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");
    private readonly string _q = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);
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

    // ---- routing slips ---------------------------------------------------

    [Fact]
    public void WalksItsStepsInOrder()
    {
        var slip = RoutingSlip.StartOf("validate", "price", "ship");

        Assert.Equal("validate", slip.Current);
        Assert.Equal("price", slip.Next);
        Assert.False(slip.IsFinished);

        var next = slip.Advance();
        Assert.Equal("price", next.Current);
        Assert.Equal(slip.RunId, next.RunId);

        Assert.True(next.Advance().Advance().IsFinished);
        Assert.Null(next.Advance().Advance().Current);
    }

    [Fact]
    public void SurvivesTheRoundTripThroughHeaders()
    {
        var slip = RoutingSlip.StartOf("a", "b", "c").Advance();
        var back = RoutingSlip.From(slip.ToHeaders())!;

        Assert.Equal(new[] { "a", "b", "c" }, back.Steps);
        Assert.Equal(1, back.Position);
        Assert.Equal("b", back.Current);
        Assert.Equal(slip.RunId, back.RunId);
    }

    [Fact]
    public void RefusesAStepNameThatWouldSplitTheRoute()
    {
        // The route is comma-separated on the wire, so a comma in a name would
        // become two destinations that do not exist.
        var error = Assert.Throws<ArgumentException>(
            () => RoutingSlip.StartOf("validate", "price,ship"));
        Assert.Contains("cannot contain a comma", error.Message);
    }

    [Fact]
    public void HasNoSlipWhenTheMessageCarriesNone()
    {
        Assert.Null(RoutingSlip.From(new Dictionary<string, object>()));
    }

    [Fact]
    public async Task CarriesAMessageThroughItsRoute()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var visited = new ConcurrentQueue<string>();
        var finished = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var steps = new[] { _q + ".one", _q + ".two", _q + ".three" };
        foreach (var step in steps) await mq.DeclareQueueAsync(step);

        var consumers = new List<IMessageConsumer>();
        foreach (var step in steps)
        {
            var here = step;
            consumers.Add(await mq.ConsumeAsync<string>(here, async message =>
            {
                visited.Enqueue(here);
                var slip = RoutingSlip.Of(message)!.Advance();
                if (slip.IsFinished) finished.TrySetResult(true);
                else await mq.ForwardAsync(slip, message.Payload, message.Envelope);
                return Ack.Accept();
            }));
        }

        await mq.SendAlongAsync(RoutingSlip.StartOf(steps), "an order");
        await finished.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(steps, visited.ToArray());
        foreach (var consumer in consumers) consumer.Dispose();
    }

    [Fact]
    public async Task LetsAStepChangeTheRestOfTheRoute()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var visited = new ConcurrentQueue<string>();
        var finished = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var check = _q + ".check";
        var fraud = _q + ".fraud";
        var ship = _q + ".ship";
        foreach (var step in new[] { check, fraud, ship }) await mq.DeclareQueueAsync(step);

        // The route is decided per message rather than once at build time. That is
        // the whole difference from a pipeline.
        using var c1 = await mq.ConsumeAsync<string>(check, async message =>
        {
            visited.Enqueue("check");
            var slip = RoutingSlip.Of(message)!;
            // A small order skips the fraud step.
            var onward = message.Payload == "small" ? slip.AdvanceTo(2) : slip.Advance();
            await mq.ForwardAsync(onward, message.Payload, message.Envelope);
            return Ack.Accept();
        });
        using var c2 = await mq.ConsumeAsync<string>(fraud, async message =>
        {
            visited.Enqueue("fraud");
            await mq.ForwardAsync(RoutingSlip.Of(message)!.Advance(), message.Payload, message.Envelope);
            return Ack.Accept();
        });
        using var c3 = await mq.ConsumeAsync<string>(ship, message =>
        {
            visited.Enqueue("ship");
            finished.TrySetResult(true);
            return Task.FromResult(Ack.Accept());
        });

        await mq.SendAlongAsync(RoutingSlip.StartOf(check, fraud, ship), "small");
        await finished.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(new[] { "check", "ship" }, visited.ToArray());
    }

    [Fact]
    public async Task KeepsTheRoutingHeadersOutOfTheApplicationsView()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        IMessage<string>? received = null;
        using var consumer = await mq.ConsumeAsync<string>(_q, m =>
        {
            received = m;
            return Task.FromResult(Ack.Accept());
        });

        await mq.SendAlongAsync(RoutingSlip.StartOf(_q), "payload");
        await Eventually(() => received != null, "the routed message");

        // A handler sees its own headers, and asks for the slip explicitly.
        Assert.DoesNotContain(received!.Headers.Keys, AceHeaders.IsAceHeader);
        Assert.NotNull(RoutingSlip.Of(received));
        Assert.Equal(_q, RoutingSlip.Of(received)!.Current);
    }

    // ---- stream retention ------------------------------------------------

    [Fact]
    public async Task DeclaresAStreamWithASegmentSizeWhenOneIsAskedFor()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        await mq.DeclareStreamAsync(_q, TimeSpan.FromHours(1), 1024, 512);

        // Redeclaring with the same arguments is not drift, which is the whole use of
        // this: a stream declared with a segment size from Go, Python or Ruby can be
        // declared identically from here.
        var same = await mq.ApplyAsync(
            Topology.Define()
                .Queue(_q, QueueType.Stream, new Dictionary<string, object>
                {
                    [StreamArguments.MaxAge] = "3600s",
                    [StreamArguments.MaxLengthBytes] = 1024L,
                    [StreamArguments.SegmentBytes] = 512L,
                })
                .Build(),
            ApplyMode.DryRun);
        Assert.False(same.HasDrift);

        var different = await mq.ApplyAsync(
            Topology.Define()
                .Queue(_q, QueueType.Stream, new Dictionary<string, object>
                {
                    [StreamArguments.SegmentBytes] = 4096L,
                })
                .Build(),
            ApplyMode.DryRun);
        Assert.True(different.HasDrift);
        Assert.Contains(
            "x-stream-max-segment-size-bytes is '512', asked for '4096'", different.Render());
    }

    [Fact]
    public async Task LeavesTheSegmentSizeOffAStreamThatDidNotAskForOne()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        // No default, on purpose. The broker has one, it is the right one nearly
        // always, and a library that picked its own would make a stream declared from
        // C# quietly different from the same stream declared from the other four --
        // where this option is opt-in too. A mismatched argument fails a redeclaration
        // rather than being ignored, so the difference would surface as an outage.
        await mq.DeclareStreamAsync(_q, TimeSpan.FromHours(1), 1024);

        var plan = await mq.ApplyAsync(
            Topology.Define()
                .Queue(_q, QueueType.Stream, new Dictionary<string, object>
                {
                    [StreamArguments.SegmentBytes] = 512L,
                })
                .Build(),
            ApplyMode.DryRun);

        Assert.True(plan.HasDrift);
        Assert.Contains("missing argument x-stream-max-segment-size-bytes", plan.Render());
    }

    // ---- topology drift --------------------------------------------------

    [Fact]
    public async Task ReportsAQueueDeclaredWithDifferentArguments()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        await mq.DeclareQueueAsync(_q, QueueType.Classic,
            new Dictionary<string, object> { ["x-message-ttl"] = 60000 });

        var plan = await mq.ApplyAsync(
            Topology.Define()
                .Queue(_q, QueueType.Classic,
                    new Dictionary<string, object> { ["x-message-ttl"] = 30000 })
                .Build(),
            ApplyMode.DryRun);

        Assert.True(plan.HasDrift);
        Assert.Contains("x-message-ttl is '60000', asked for '30000'", plan.Render());
    }

    [Fact]
    public async Task RefusesToApplyOverAQueueThatDiffers()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q, QueueType.Classic,
            new Dictionary<string, object> { ["x-message-ttl"] = 60000 });

        // Drift is reported, never corrected: a queue's arguments are fixed at
        // creation, so "fixing" it would mean deleting a queue with messages in it.
        var error = await Assert.ThrowsAsync<AceFatalException>(
            () => mq.ApplyAsync(Topology.Define()
                .Queue(_q, QueueType.Classic,
                    new Dictionary<string, object> { ["x-message-ttl"] = 30000 })
                .Build()));
        Assert.Contains("drained and redeclared", error.Message);
    }

    [Fact]
    public async Task ReportsAQueueThatMatchesAsPresent()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var arguments = new Dictionary<string, object> { ["x-message-ttl"] = 60000 };
        await mq.DeclareQueueAsync(_q, QueueType.Classic, arguments);

        var plan = await mq.ApplyAsync(
            Topology.Define().Queue(_q, QueueType.Classic, arguments).Build(), ApplyMode.DryRun);

        Assert.False(plan.HasDrift);
        Assert.False(plan.HasChanges);
        Assert.Equal(TopologyActionKind.Present, plan.Actions.Single().Kind);
    }

    [Fact]
    public async Task ReportsAQueueTypeThatWasChanged()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q, QueueType.Classic, null);

        var plan = await mq.ApplyAsync(
            Topology.Define().Queue(_q, QueueType.Quorum).Build(), ApplyMode.DryRun);

        Assert.True(plan.HasDrift);
        Assert.Contains("declared as classic, asked for quorum", plan.Render());
    }

    // ---- queue types, which five libraries have to agree on ---------------

    /// <summary>
    /// A source queue is quorum, everywhere it can be named.
    /// </summary>
    /// <remarks>
    /// The point of this test is interoperability rather than durability. A queue's
    /// type is fixed when it is created, so a Java service declaring <c>orders</c> as
    /// quorum and a .NET service declaring the same name as classic do not both get a
    /// queue: the second is refused with <c>PRECONDITION_FAILED</c> and cannot consume
    /// at all. Java's <c>Topology.Builder.queue</c> has said quorum since before it
    /// had deployments, and an existing quorum queue cannot be redeclared as classic,
    /// so this is the side that moved.
    /// </remarks>
    [Fact]
    public void DeclaresEverySourceQueueAsQuorum()
    {
        var plain = Topology.Define().Queue("orders").Build();
        Assert.Equal(QueueType.Quorum, plain.Queues.Single().Type);

        var dead = Topology.Define().QueueWithDeadLetter("orders").Build();
        Assert.Equal(QueueType.Quorum, dead.Queues.Single(q => q.Name == "orders").Type);

        var retry = Topology.Define()
            .QueueWithRetry("orders", RetryPolicy.Fixed(3, TimeSpan.FromMinutes(1)))
            .Build();
        Assert.Equal(QueueType.Quorum, retry.Queues.Single(q => q.Name == "orders").Type);
    }

    /// <summary>
    /// Everything the library creates around a source queue stays classic.
    /// </summary>
    /// <remarks>
    /// Java's <c>RetryTopology</c> declares the rungs, <c>{q}.dlq</c> and
    /// <c>{q}.parked</c> as <c>QueueType.CLASSIC</c>, so any broker a Java service has
    /// touched already has them as classic queues. Declaring them quorum here would be
    /// the same <c>PRECONDITION_FAILED</c> this change exists to remove, pointed the
    /// other way.
    /// </remarks>
    [Fact]
    public void KeepsTheRungsAndTheDeadLetterQueuesClassic()
    {
        var topology = Topology.Define()
            .QueueWithRetry("orders", RetryPolicy.Fixed(3, TimeSpan.FromMinutes(1)))
            .Build();

        foreach (var queue in topology.Queues.Where(q => q.Name != "orders"))
        {
            Assert.Equal(QueueType.Classic, queue.Type);
        }

        // Named rather than only counted, so that a rung quietly disappearing from the
        // ladder cannot pass this.
        Assert.Contains(topology.Queues, q => q.Name == "orders.dlq");
        Assert.Contains(topology.Queues, q => q.Name == "orders.parked");
        Assert.Contains(topology.Queues, q => q.Name == "orders.retry.1m");
    }

    [Fact]
    public void StillDeclaresClassicWhenClassicIsAskedFor()
    {
        var plain = Topology.Define().Queue("orders", QueueType.Classic).Build();
        Assert.Equal(QueueType.Classic, plain.Queues.Single().Type);

        var retry = Topology.Define()
            .QueueWithRetry(
                "orders", RetryPolicy.Fixed(3, TimeSpan.FromMinutes(1)), QueueType.Classic, null)
            .Build();
        Assert.Equal(QueueType.Classic, retry.Queues.Single(q => q.Name == "orders").Type);

        // And a stream is still a stream. The type argument is not a two-valued flag.
        var stream = Topology.Define().Queue("events", QueueType.Stream).Build();
        Assert.Equal(QueueType.Stream, stream.Queues.Single().Type);
    }

    /// <summary>
    /// A reply queue is classic, and asked for rather than inherited.
    /// </summary>
    /// <remarks>
    /// This is the one the change could have broken silently. RabbitMQ refuses a
    /// quorum queue that is exclusive or auto-delete, so a per-process queue that
    /// picked up the new default — here, or the day somebody adds the auto-delete flag
    /// a reply queue deserves — would stop being declarable at all. Java's
    /// <c>Requester</c> pins the same queue to classic for the same reason.
    /// </remarks>
    [Fact]
    public async Task DeclaresAReplyQueueAsClassic()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var requester = await mq.RequesterAsync();

        // Asked of the broker rather than of the builder: this queue is declared on
        // the way past, by the requester itself, and never appears in a Topology.
        var plan = await mq.ApplyAsync(
            Topology.Define().Queue(requester.ReplyQueue).Build(), ApplyMode.DryRun);

        Assert.True(plan.HasDrift);
        Assert.Contains("declared as classic, asked for quorum", plan.Render());
    }

    // ---- shared schema registry ------------------------------------------

    [Fact]
    public void GivesTheSameSchemaTheSameIdAcrossProcesses()
    {
        var registry = new DbSchemaRegistry(Connect);
        WithSchema(registry.CreateTableSql());

        var schema = new SchemaDefinition("json", "order.placed", "{\"type\":\"object\"}");
        var id = registry.IdFor(schema);

        // A second registry over the same database is what another process is.
        var elsewhere = new DbSchemaRegistry(Connect);
        Assert.Equal(id, elsewhere.IdFor(schema));
        Assert.Equal(schema, elsewhere.SchemaFor(id));

        var other = new SchemaDefinition("json", "order.placed", "{\"type\":\"string\"}");
        Assert.NotEqual(id, registry.IdFor(other));
    }

    [Fact]
    public void SaysWhenAnIdWasNeverRegistered()
    {
        var registry = new DbSchemaRegistry(Connect);
        WithSchema(registry.CreateTableSql());

        Assert.Throws<AceFatalException>(() => registry.SchemaFor(99));
    }
}
