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

using AceMq.Amqp;

namespace AceMq.Amqp.Tests;

/// <summary>
/// What the library publishes for a caller, on a connection made
/// <c>WithoutPublisherConfirms()</c>.
/// </summary>
/// <remarks>
/// Without a confirm a return can never be ruled out, so an unconfirmed publish is
/// reported as routed. Every publish the library makes on a caller's behalf settles
/// something on that answer, and each one marked its work done after the message
/// went nowhere. The in-memory transport mirrors the mode: the caller's own publishes
/// are reported as routed, the library's are answered truthfully.
/// </remarks>
public sealed class WithoutConfirmsTests
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");

    private Task<AceMqConnection> ConnectAsync() =>
        AceMqConnection.ConnectAsync(ConnectionConfig.ForUrl(_url).WithoutPublisherConfirms().Build());

    [Fact]
    public async Task TheCallersOwnPublishIsStillUnconfirmed()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareExchangeAsync("orders", "direct");

        // Nothing bound, and the publisher cannot know: that is the mode it asked for.
        var result = await mq.Publisher<string>("orders", "nobody").SendAsync("x");
        Assert.True(result.Routed);
    }

    [Fact]
    public async Task ADeadLetterWhoseQueueWasDeletedIsKept()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("work");
        var deliveries = 0;
        using (await mq.ConsumeAsync<string>("work", _ =>
               {
                   Interlocked.Increment(ref deliveries);
                   return Task.FromResult(Ack.DeadLetter("give up"));
               }))
        {
            await mq.DeleteQueueAsync(Naming.DeadLetterQueue("work"));
            await mq.Publisher<string>("", "work").SendAsync("keep me");
            await Eventually(() => Volatile.Read(ref deliveries) >= 2);
        }

        Assert.True(deliveries >= 2, "the move was taken as done");
        // A delivery in flight at disposal is handed back a moment later.
        await Eventually(() => mq.MessageCountAsync("work").Result == 1);
        Assert.Equal(1, await mq.MessageCountAsync("work"));
    }

    [Fact]
    public async Task AReplayIntoAQueueThatDoesNotExistKeepsTheMessage()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orphans.dlq");
        await mq.Publisher<string>("", "orphans.dlq").SendAsync("recover me");

        await Assert.ThrowsAsync<PublishFailedException>(
            () => mq.Replay("orphans.dlq").Into("not-there").ReplayAllAsync());

        Assert.Equal(1, await mq.MessageCountAsync("orphans.dlq"));
    }

    [Fact]
    public async Task AnOutboxRecordNothingIsBoundToStays()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareExchangeAsync("events", "direct");
        var store = new InMemoryOutboxStore();
        await store.AddAsync(OutboxRecord.For(mq, "events", "nobody.listens", "payload"));
        using var relay = new OutboxRelay(mq, store);

        Assert.Equal(0, await relay.DrainOnceAsync());
        Assert.Equal(1, await store.PendingCountAsync());
    }

    [Fact]
    public async Task ARoutingSlipHopToNowhereThrows()
    {
        using var mq = await ConnectAsync();
        await Assert.ThrowsAsync<PublishFailedException>(() => mq.ForwardAsync(
            RoutingSlip.StartOf(new[] { "never-declared" }), "an order", Envelope.Of("order").Build()));
    }

    [Fact]
    public async Task APipelineHopToADeletedStepIsRetriedNotAcknowledged()
    {
        using var mq = await ConnectAsync();
        var first = 0;
        using var pipeline = await mq.Pipeline<string>("p")
            .Step("first", (string s) => { Interlocked.Increment(ref first); return Task.FromResult<string?>(s); })
            .Step("second", (string s) => Task.FromResult<string?>(s))
            .WithRetryDelay(TimeSpan.FromMilliseconds(50))
            .BuildAsync();
        await mq.DeleteQueueAsync(pipeline.QueueFor("second"));

        await pipeline.SendAsync("an order");
        await Eventually(() => Volatile.Read(ref first) >= 2);

        Assert.True(first >= 2, "the first step's hop was taken as done");
        Assert.Equal(0, pipeline.Completed);
    }

    [Fact]
    public async Task AReplyThatCannotBeDeliveredLeavesTheRequestUnacknowledged()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("pricing");
        var calls = 0;
        using var responder = await mq.RespondAsync<string, string>("pricing", request =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(request);
        });

        // A requester that has gone: its reply queue does not exist.
        await mq.Publisher<string>("", "pricing", PublishOptions.Defaults(), "acemq.reply.gone")
            .SendAsync("quote me");
        await Eventually(() => Volatile.Read(ref calls) >= 2, seconds: 15);

        Assert.True(calls >= 2, "the request was acknowledged after its reply went nowhere");
    }

    [Fact]
    public async Task AScheduledMessageDueWithNothingBoundIsKept()
    {
        using var mq = await ConnectAsync();
        using var scheduler = await Scheduler.OnAsync(mq);
        await mq.DeclareExchangeAsync("later", "direct");
        await mq.DeclareQueueAsync("later.q");

        await scheduler.InAsync(TimeSpan.FromSeconds(1.5), "later", "due", "on time");
        await Task.Delay(TimeSpan.FromSeconds(3));
        await mq.BindAsync("later.q", "later", "due");

        await Eventually(() => mq.MessageCountAsync("later.q").Result == 1, seconds: 15);
        Assert.Equal(1, await mq.MessageCountAsync("later.q"));
    }

    private static async Task Eventually(Func<bool> probe, int seconds = 10)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < deadline && !probe()) await Task.Delay(25);
    }
}
