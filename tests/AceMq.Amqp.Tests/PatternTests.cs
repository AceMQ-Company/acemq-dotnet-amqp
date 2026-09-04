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
using AceMq.Amqp;

namespace AceMq.Amqp.Tests;

public sealed class PatternTests : IDisposable
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");

    // See MessagingTests: Reset is process-wide and these classes run in parallel.
    public void Dispose() { }

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

    // ---- topology --------------------------------------------------------

    [Fact]
    public async Task DeclaresAQueueAndItsDeadLetterQueueTogether()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        var topology = Topology.Define()
            .Exchange("orders", "topic")
            .QueueWithDeadLetter("orders.placed")
            .Bind("orders.placed", "orders", "order.placed")
            .Build();

        await mq.ApplyAsync(topology);

        // The point of declaring them as one unit: the dead-letter exchange and the
        // queue bound to it exist, so Ack.DeadLetter has somewhere to put a message.
        // Wiring these by hand and forgetting one loses messages silently.
        Assert.True(await mq.QueueExistsAsync("orders.placed"));
        Assert.True(await mq.QueueExistsAsync("orders.placed.dead"));

        var queue = topology.Queues.Single(q => q.Name == "orders.placed");
        Assert.Equal("orders.placed.dlx", queue.Arguments["x-dead-letter-exchange"]);
    }

    [Fact]
    public async Task ReportsWhatApplyingWouldDoWithoutDoingIt()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        var topology = Topology.Define().Queue("reports").Build();
        var plan = await mq.ApplyAsync(topology, ApplyMode.DryRun);

        Assert.False(await mq.QueueExistsAsync("reports"));
        Assert.Contains("queue reports", plan.Render());
    }

    [Fact]
    public async Task ReportsAQueueThatIsAlreadyThereAsPresent()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("already");

        var plan = await mq.ApplyAsync(
            Topology.Define().Queue("already").Build(), ApplyMode.DryRun);

        Assert.Equal(TopologyActionKind.Present, plan.Actions.Single().Kind);
        Assert.False(plan.HasChanges);
    }

    // ---- request and reply -----------------------------------------------

    [Fact]
    public async Task AnswersARequest()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("pricing");

        using var responder = await mq.RespondAsync<string, string>(
            "pricing", request => Task.FromResult(request.ToUpperInvariant()));

        using var requester = await mq.RequesterAsync();
        var answer = await requester.RequestAsync<string, string>("", "pricing", "quote me");

        Assert.Equal("QUOTE ME", answer);
        Assert.Equal(1, responder.Answered);
    }

    [Fact]
    public async Task GivesUpOnARequestNobodyAnswers()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("silent");

        using var requester = await mq.RequesterAsync();

        await Assert.ThrowsAsync<RequestTimedOutException>(
            () => requester.RequestAsync<string, string>(
                "", "silent", "anyone there?", TimeSpan.FromMilliseconds(300),
                CancellationToken.None));

        Assert.Equal(1, requester.TimedOut);
    }

    [Fact]
    public async Task MatchesEachReplyToItsOwnRequest()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("echo");

        // A slow first request and a fast second one. If replies were taken off the
        // queue in arrival order rather than matched by correlation, these would
        // come back swapped -- the failure that makes a shared reply queue unsafe.
        using var responder = await mq.RespondAsync<string, string>("echo", async request =>
        {
            if (request == "slow") await Task.Delay(200);
            return "reply:" + request;
        });

        using var requester = await mq.RequesterAsync();
        var slow = requester.RequestAsync<string, string>("", "echo", "slow");
        var fast = requester.RequestAsync<string, string>("", "echo", "fast");

        Assert.Equal("reply:fast", await fast);
        Assert.Equal("reply:slow", await slow);
    }

    // ---- replay ----------------------------------------------------------

    [Fact]
    public async Task ReplaysDeadLetteredMessagesBackOntoTheirQueue()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("work");
        await mq.DeclareQueueAsync("work.dead");

        var publisher = mq.Publisher<string>("", "work.dead");
        await publisher.SendAsync("first");
        await publisher.SendAsync("second");

        var replay = mq.Replay("work.dead");
        Assert.Equal("work", replay.To);
        Assert.Equal(2, await replay.PendingAsync());

        var moved = await replay.ReplayAllAsync();

        Assert.Equal(2, moved);
        Assert.Equal(2, await mq.MessageCountAsync("work"));
        Assert.Equal(0, await mq.MessageCountAsync("work.dead"));
    }

    [Fact]
    public async Task LeavesBehindWhatTheFilterRejects()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("work");
        await mq.DeclareQueueAsync("work.dead");

        var publisher = mq.Publisher<string>("", "work.dead");
        await publisher.SendAsync("keep");
        await publisher.SendAsync("skip");

        var moved = await mq.Replay("work.dead").ReplayAsync(
            10, d => System.Text.Encoding.UTF8.GetString(d.Body).Contains("keep"));

        // The rejected message goes back rather than being discarded. Losing messages
        // as a side effect of looking at them would be a poor trade.
        Assert.Equal(1, moved);
        await Eventually(
            () => mq.MessageCountAsync("work.dead").Result == 1,
            "the rejected message to be back on the queue");
    }

    // ---- partitioning and ordering ---------------------------------------

    [Fact]
    public void HashesAKeyTheSameWayEveryTime()
    {
        // Not string.GetHashCode(): .NET randomises that per process, so the same
        // key would land in a different partition after a restart and the ordering
        // guarantee would quietly stop holding.
        Assert.Equal(Partitioning.Hash("account-1"), Partitioning.Hash("account-1"));
        Assert.NotEqual(Partitioning.Hash("account-1"), Partitioning.Hash("account-2"));
        Assert.InRange(Partitioning.PartitionFor("account-1", 8), 0, 7);
    }

    [Fact]
    public async Task KeepsEveryKeyOnOnePartition()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        var ordered = await mq.Ordered<string>("ledger")
            .Partitions(4)
            .KeyedBy(payload => payload.Split(':')[0])
            .DeclareAsync();

        var first = await ordered.SendAsync("account-7:deposit");
        var second = await ordered.SendAsync("account-7:withdraw");
        var other = await ordered.SendAsync("account-8:deposit");

        Assert.Equal(first, second);
        Assert.Equal(4, ordered.Queues.Count);
        Assert.InRange(other, 0, 3);
    }

    [Fact]
    public async Task HandlesMessagesForOneKeyInOrder()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var seen = new ConcurrentQueue<string>();

        var ordered = await mq.Ordered<string>("ledger")
            .Partitions(1)
            .KeyedBy(_ => "same")
            .DeclareAsync();

        await ordered.ConsumeAsync(message =>
        {
            seen.Enqueue(message.Payload);
            return Task.CompletedTask;
        });

        foreach (var op in new[] { "a", "b", "c", "d" }) await ordered.SendAsync(op);

        await Eventually(() => seen.Count == 4, "all four messages");
        Assert.Equal(new[] { "a", "b", "c", "d" }, seen.ToArray());
        ordered.Dispose();
    }

    [Fact]
    public async Task StopsAPartitionRatherThanHandlingTheNextMessageOutOfOrder()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var handled = new ConcurrentQueue<string>();

        var ordered = await mq.Ordered<string>("ledger")
            .Partitions(1)
            .KeyedBy(_ => "same")
            .OnFailure(PartitionFailure.Stop, attempts: 2, delay: TimeSpan.FromMilliseconds(10))
            .DeclareAsync();

        await ordered.ConsumeAsync(message =>
        {
            if (message.Payload == "poison") throw new InvalidOperationException("no");
            handled.Enqueue(message.Payload);
            return Task.CompletedTask;
        });

        await ordered.SendAsync("poison");
        await ordered.SendAsync("after");

        // "after" must not be handled: applying it while the operation before it
        // failed is exactly the corruption ordering exists to prevent.
        await Eventually(() => ordered.HaltedPartitions.Count == 1, "the partition to halt");
        Assert.DoesNotContain("after", handled);
        ordered.Dispose();
    }

    // ---- pipeline --------------------------------------------------------

    [Fact]
    public async Task MovesAMessageThroughEveryStep()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        using var pipeline = await mq.Pipeline<string>("orders")
            .Step("validate", (string order) => Task.FromResult<string?>(order.Trim()))
            .Step("enrich", (string order) => Task.FromResult<string?>(order + ":enriched"))
            .Step("store", (string order) => Task.FromResult<string?>(order + ":stored"))
            .BuildAsync();

        Assert.Equal(new[] { "validate", "enrich", "store" }, pipeline.StepNames);

        await pipeline.SendAsync("  A-1  ");

        await Eventually(() => pipeline.Completed == 1, "the message to reach the end");
        Assert.Equal(1, pipeline.Entered);
        Assert.Equal(0, pipeline.EndedEarly);
    }

    [Fact]
    public async Task StopsAMessageAtAStepThatReturnsNull()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        using var pipeline = await mq.Pipeline<string>("orders")
            .Step("validate", (string order) =>
                Task.FromResult<string?>(order.StartsWith("good") ? order : null))
            .Step("store", (string order) => Task.FromResult<string?>(order + ":stored"))
            .BuildAsync();

        await pipeline.SendAsync("bad-1");

        // Rejection is an outcome, not a failure, so it is counted apart from both
        // success and error rather than looking like a lost message.
        await Eventually(() => pipeline.EndedEarly == 1, "the message to be filtered out");
        Assert.Equal(0, pipeline.Completed);
    }

    // ---- outbox ----------------------------------------------------------

    [Fact]
    public async Task PublishesWhatTheOutboxWasGiven()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("events");

        var store = new InMemoryOutboxStore();
        await store.AddAsync(OutboxRecord.Of(
            "", "events", Envelope.Of("order.placed").Build(), "\"A-1\""));
        await store.AddAsync(OutboxRecord.Of(
            "", "events", Envelope.Of("order.placed").Build(), "\"A-2\""));

        Assert.Equal(2, await store.PendingCountAsync());

        using var relay = mq.Outbox(store);
        var moved = await relay.DrainAsync();

        Assert.Equal(2, moved);
        Assert.Equal(0, await store.PendingCountAsync());
        Assert.Equal(2, await mq.MessageCountAsync("events"));
    }

    [Fact]
    public async Task LeavesARecordPendingWhenPublishingItFails()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        var store = new InMemoryOutboxStore();
        // Nothing is bound, so the publish fails. The record has to survive that:
        // an outbox that drops what it could not send is not an outbox.
        await store.AddAsync(OutboxRecord.Of(
            "nowhere", "nothing", Envelope.Of("order.placed").Build(), "\"A-1\""));

        using var relay = mq.Outbox(store);
        var moved = await relay.DrainOnceAsync();

        Assert.Equal(0, moved);
        Assert.Equal(1, await store.PendingCountAsync());
        Assert.Equal(1, relay.Failed);
        Assert.NotNull(store.Pending().Single().LastError);
    }
}
