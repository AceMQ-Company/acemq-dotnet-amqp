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
