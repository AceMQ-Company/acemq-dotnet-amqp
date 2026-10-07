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
using AceMq.Amqp.RabbitMq;
using RabbitMQ.Client;

namespace AceMq.Amqp.RabbitMq.Tests;

/// <summary>
/// Every place the library publishes on the caller's behalf and then settles
/// something, against a destination nothing is bound to.
/// </summary>
/// <remarks>
/// A broker confirms an unroutable publish and drops it. Unless the publish is
/// mandatory and the return is read before the original is acknowledged or the record
/// marked done, the message is gone and nothing says so. Only a real broker can show
/// the return arriving; the in-memory transport routes by its own rules.
/// </remarks>
public class UnroutablePublishTests : IAsyncLifetime
{
    protected readonly string _url =
        Environment.GetEnvironmentVariable("ACEMQ_TEST_AMQP_URL")
        ?? "amqp://guest:guest@localhost:5672";

    /// <summary>How the connection under test is made.</summary>
    protected virtual ConnectionConfig Config() => ConnectionConfig.ForUrl(_url).Build();

    private readonly string _suffix = Guid.NewGuid().ToString("N").Substring(0, 8);
    private readonly List<string> _declared = new List<string>();
    protected AceMqConnection _mq = null!;

    protected string Name(string what) => $"acemq.test.{_suffix}.{what}";

    public async Task InitializeAsync()
    {
        Transports.Register(new RabbitMqTransport());
        _mq = await AceMqConnection.ConnectAsync(Config());
    }

    public async Task DisposeAsync()
    {
        foreach (var queue in _declared)
        {
            foreach (var name in new[] { queue, Naming.DeadLetterQueue(queue), Naming.ParkedQueue(queue) })
            {
                try { await _mq.DeleteQueueAsync(name); } catch { /* already gone */ }
            }
        }
        try { await _mq.DeleteExchangeAsync(Name("x")); } catch { /* never declared */ }
        _mq.Dispose();
    }

    protected async Task<string> QueueAsync(string what)
    {
        var name = Name(what);
        await _mq.DeclareQueueAsync(name);
        _declared.Add(name);
        return name;
    }

    [Fact]
    public async Task ADeadLetterWhoseQueueWasDeletedIsReleasedNotAcknowledged()
    {
        var queue = await QueueAsync("work");
        var deliveries = 0;
        using (await _mq.ConsumeAsync<string>(queue, _ =>
               {
                   Interlocked.Increment(ref deliveries);
                   return Task.FromResult(Ack.DeadLetter("give up"));
               }))
        {
            // Declared by the consumer at start-up, deleted from under it.
            await _mq.DeleteQueueAsync(Naming.DeadLetterQueue(queue));
            await _mq.Publisher<string>("", queue).SendAsync("keep me");
            await EventuallyAsync(() => Volatile.Read(ref deliveries) >= 2);

        // Put the queue back and the same message completes: it was kept, not lost.
        // Also leaves no handler mid-retry when the consumer is disposed.
            await _mq.DeclareQueueAsync(Naming.DeadLetterQueue(queue));
            await EventuallyAsync(async () => await _mq.MessageCountAsync(Naming.DeadLetterQueue(queue)) == 1);
        }

        Assert.True(deliveries >= 2, "the move was taken as done");
        Assert.Equal(1, await _mq.MessageCountAsync(Naming.DeadLetterQueue(queue)));
    }

    [Fact]
    public async Task ARoutingSlipHopToNowhereThrowsSoTheStepIsNotAcknowledged()
    {
        // A step's handler forwards and then accepts; the forward throwing is what
        // keeps the accept from happening, and the consumer retries the message.
        var slip = RoutingSlip.StartOf(new[] { Name("never-declared") });
        await Assert.ThrowsAsync<PublishFailedException>(
            () => _mq.ForwardAsync(slip, "an order", Envelope.Of("order").Build()));
    }

    [Fact]
    public async Task AnOutboxRecordNothingIsBoundToStaysInTheOutbox()
    {
        await _mq.DeclareExchangeAsync(Name("x"), "direct");
        var store = new InMemoryOutboxStore();
        await store.AddAsync(OutboxRecord.For(_mq, Name("x"), "nobody.listens", "payload"));
        using var relay = new OutboxRelay(_mq, store);

        Assert.Equal(0, await relay.DrainOnceAsync());
        Assert.Equal(1, relay.Failed);
        Assert.Equal(1, await store.PendingCountAsync());
    }

    [Fact]
    public async Task AReplayIntoAQueueThatDoesNotExistKeepsTheMessage()
    {
        var source = await QueueAsync("dead");
        await _mq.Publisher<string>("", source).SendAsync("recover me");

        await Assert.ThrowsAsync<PublishFailedException>(
            () => _mq.Replay(source).Into(Name("not-there")).ReplayAllAsync());

        // Handed back with basic.reject, which the broker answers with nothing.
        await EventuallyAsync(async () => await _mq.MessageCountAsync(source) == 1, seconds: 10);
        Assert.Equal(1, await _mq.MessageCountAsync(source));
    }

    [Fact]
    public async Task AReplayOfAMessageWithNoIdIntoAQueueThatDoesNotExistKeepsIt()
    {
        var source = await QueueAsync("foreign");

        // Published by something that is not this library: no message id, which is
        // what the transport correlates a return by.
        var factory = new ConnectionFactory { Uri = new Uri(_url) };
        await using (var raw = await factory.CreateConnectionAsync())
        await using (var channel = await raw.CreateChannelAsync())
        {
            await channel.BasicPublishAsync("", source, true, new BasicProperties(), "no id"u8.ToArray());
        }
        await EventuallyAsync(async () => await _mq.MessageCountAsync(source) == 1);

        await Assert.ThrowsAsync<PublishFailedException>(
            () => _mq.Replay(source).Into(Name("not-there")).ReplayAllAsync());

        // Handed back with basic.reject, which the broker answers with nothing.
        await EventuallyAsync(async () => await _mq.MessageCountAsync(source) == 1, seconds: 10);
        Assert.Equal(1, await _mq.MessageCountAsync(source));
    }

    [Fact]
    public async Task AScheduledMessageDueWhileNothingIsBoundIsDeliveredOnceSomethingIs()
    {
        await _mq.DeclareExchangeAsync(Name("x"), "direct");
        var target = await QueueAsync("later");
        using var scheduler = await Scheduler.OnAsync(_mq);

        // Long enough to go through a rung, so it is the scheduler's own consumer that
        // finds it due with nothing bound, not this call.
        await scheduler.InAsync(TimeSpan.FromSeconds(1.5), Name("x"), "due", "on time");
        await Task.Delay(TimeSpan.FromSeconds(4));
        await _mq.BindAsync(target, Name("x"), "due");

        await EventuallyAsync(async () => await _mq.MessageCountAsync(target) == 1, seconds: 20);
        Assert.Equal(1, await _mq.MessageCountAsync(target));
    }

    [Fact]
    public async Task APipelineHopToADeletedStepIsRetriedNotAcknowledged()
    {
        var first = 0;
        var name = Name("p");
        using var pipeline = await _mq.Pipeline<string>(name)
            .Step("first", (string s) => { Interlocked.Increment(ref first); return Task.FromResult<string?>(s); })
            .Step("second", (string s) => Task.FromResult<string?>(s))
            .WithRetryDelay(TimeSpan.FromMilliseconds(50))
            .BuildAsync();
        _declared.Add(pipeline.QueueFor("first"));
        _declared.Add(pipeline.QueueFor("second"));
        await _mq.DeleteQueueAsync(pipeline.QueueFor("second"));

        await pipeline.SendAsync("an order");
        await EventuallyAsync(() => Volatile.Read(ref first) >= 2);
        Assert.True(first >= 2, "the first step's hop was taken as done");
        Assert.Equal(0, pipeline.Completed);

        // Put the queue back and the same message completes: it was kept, not lost.
        // Also leaves no handler mid-retry when the consumer is disposed.
        // The second step's consumer went with its queue, so the hop waits there.
        await _mq.DeclareQueueAsync(pipeline.QueueFor("second"));
        await EventuallyAsync(async () => await _mq.MessageCountAsync(pipeline.QueueFor("second")) == 1);
        Assert.Equal(1, await _mq.MessageCountAsync(pipeline.QueueFor("second")));
    }

    [Fact]
    public async Task AReplyThatCannotBeDeliveredLeavesTheRequestUnacknowledged()
    {
        var queue = await QueueAsync("pricing");
        var calls = 0;
        using var responder = await _mq.RespondAsync<string, string>(queue, request =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(request);
        });

        // A requester that has gone: its reply queue does not exist.
        await _mq.Publisher<string>("", queue, PublishOptions.Defaults(), Name("reply-gone"))
            .SendAsync("quote me");
        await EventuallyAsync(() => Volatile.Read(ref calls) >= 2, seconds: 15);
        Assert.True(calls >= 2, "the request was acknowledged after its reply went nowhere");

        // Put the queue back and the same message completes: it was kept, not lost.
        // Also leaves no handler mid-retry when the consumer is disposed.
        var replies = await QueueAsync("reply-gone");
        await EventuallyAsync(async () => await _mq.MessageCountAsync(replies) == 1, seconds: 15);
        Assert.Equal(1, await _mq.MessageCountAsync(replies));
    }

    protected static Task EventuallyAsync(Func<bool> probe, int seconds = 30) =>
        EventuallyAsync(() => Task.FromResult(probe()), seconds);

    protected static async Task EventuallyAsync(Func<Task<bool>> probe, int seconds = 30)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < deadline)
        {
            if (await probe()) return;
            await Task.Delay(100);
        }
    }
}

/// <summary>
/// The same, on a connection made <c>WithoutPublisherConfirms()</c>: the library
/// still confirms what it publishes for the caller.
/// </summary>
public sealed class UnroutablePublishWithoutConfirmsTests : UnroutablePublishTests
{
    protected override ConnectionConfig Config() =>
        ConnectionConfig.ForUrl(_url).WithoutPublisherConfirms().Build();

    [Fact]
    public async Task TheCallersOwnPublishIsStillUnconfirmed()
    {
        await _mq.DeclareExchangeAsync(Name("x"), "direct");

        // Nothing bound, and the publisher cannot know: that is the mode it asked for.
        var result = await _mq.Publisher<string>(Name("x"), "nobody").SendAsync("x");
        Assert.True(result.Routed);
    }

    [Fact]
    public async Task TheConfirmedChannelComesBackWithTheConnection()
    {
        var name = Name("client");
        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(_url).ClientName(name).WithoutPublisherConfirms().Build());
        var source = await QueueAsync("recovered");
        await mq.Publisher<string>("", source).SendAsync("recover me");

        var pid = StreamRecoveryTests.Control("list_connections", "--silent", "pid", "client_properties")
            .Split('\n').Where(line => line.Contains(name)).Select(line => line.Split('\t')[0].Trim()).Single();
        StreamRecoveryTests.Control("close_connection", pid, "confirmed channel recovery test");
        await EventuallyAsync(() => !mq.IsOpen, seconds: 10);
        await EventuallyAsync(() => mq.IsOpen);
        Assert.True(mq.IsOpen, "the connection did not recover");

        // Still confirmed, still watched for returns: the copy goes nowhere and the
        // original stays.
        await Assert.ThrowsAsync<PublishFailedException>(
            () => mq.Replay(source).Into(Name("not-there")).ReplayAllAsync());
        await EventuallyAsync(async () => await _mq.MessageCountAsync(source) == 1, seconds: 10);
        Assert.Equal(1, await _mq.MessageCountAsync(source));
    }
}
