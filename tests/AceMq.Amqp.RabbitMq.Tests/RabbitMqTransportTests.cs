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

using System.Linq;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

namespace AceMq.Amqp.RabbitMq.Tests;

public sealed class OrderPlaced
{
    public string OrderId { get; set; } = "";
    public decimal Total { get; set; }
}

/// <summary>
/// The RabbitMQ transport, against a real broker.
/// </summary>
/// <remarks>
/// <para>
/// This project exists separately from the unit tests because it cannot run without
/// a broker. Making it skip itself when one is absent would turn "nobody ran this"
/// into a green tick, which is the failure mode worth avoiding: an integration suite
/// that silently does nothing is worse than one that is obviously not running.
/// </para>
/// <para>
/// Point it at a broker with <c>ACEMQ_TEST_AMQP_URL</c>. CI supplies one as a
/// service container.
/// </para>
/// </remarks>
public sealed class RabbitMqTransportTests : IAsyncLifetime
{
    private readonly string _url =
        Environment.GetEnvironmentVariable("ACEMQ_TEST_AMQP_URL")
        ?? "amqp://guest:guest@localhost:5672";

    private readonly string _suffix = Guid.NewGuid().ToString("N").Substring(0, 8);
    private AceMqConnection _mq = null!;

    private string Exchange => $"acemq.test.{_suffix}";
    private string Queue => $"acemq.test.{_suffix}.q";

    public async Task InitializeAsync()
    {
        Transports.Register(new RabbitMqTransport());
        _mq = await AceMqConnection.ConnectAsync(_url);
        await _mq.DeclareExchangeAsync(Exchange, "topic");
        await _mq.DeclareQueueAsync(Queue);
        await _mq.BindAsync(Queue, Exchange, "order.placed");
    }

    public async Task DisposeAsync()
    {
        try { await _mq.DeleteQueueAsync(Queue); } catch { /* the test may have failed before declaring */ }
        _mq.Dispose();
    }

    [Fact]
    public async Task PublishesAndConsumesOverARealBroker()
    {
        // RunContinuationsAsynchronously matters here: without it the awaiting test
        // resumes on the client's consumer dispatch thread, and anything it then does
        // that the dispatch thread must service blocks that thread against itself.
        var arrived = new TaskCompletionSource<IMessage<OrderPlaced>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var consumer = await _mq.ConsumeAsync<OrderPlaced>(Queue, message =>
        {
            arrived.TrySetResult(message);
            return Task.FromResult(Ack.Accept());
        });

        var envelope = Envelope.Of("order.placed")
            .CorrelationId("corr-1")
            .Header("x-tenant", "acme")
            .Build();

        var publisher = _mq.Publisher<OrderPlaced>(Exchange, "order.placed");
        var result = await publisher.SendAsync(
            new OrderPlaced { OrderId = "A-1", Total = 42.5m }, envelope);

        Assert.True(result.Routed);

        var received = await arrived.Task.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.Equal("A-1", received.Payload.OrderId);
        Assert.Equal(42.5m, received.Payload.Total);

        // The envelope survived a real AMQP round trip, where string headers travel
        // as byte arrays and come back needing to be decoded again.
        Assert.Equal(envelope.Id, received.Envelope.Id);
        Assert.Equal("corr-1", received.Envelope.CorrelationId);
        Assert.Equal("acme", received.Headers["x-tenant"]);
        Assert.Equal(1, received.Attempt);
    }

    [Fact]
    public async Task ReportsAPublishThatMatchesNoQueue()
    {
        // Mandatory publishing means the broker returns the message rather than
        // dropping it, and the library turns that return into a failed publish.
        var publisher = _mq.Publisher<OrderPlaced>(Exchange, "order.cancelled");

        var error = await Assert.ThrowsAsync<PublishFailedException>(
            () => publisher.SendAsync(new OrderPlaced { OrderId = "A-2" }));
        Assert.Contains("matched no queue", error.Message);
    }

    [Fact]
    public async Task CountsWhatIsWaitingOnAQueue()
    {
        var publisher = _mq.Publisher<OrderPlaced>(Exchange, "order.placed");
        await publisher.SendAsync(new OrderPlaced { OrderId = "A-3" });
        await publisher.SendAsync(new OrderPlaced { OrderId = "A-4" });

        // The broker's count is eventually consistent with the publish confirms.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        long count = 0;
        while (DateTime.UtcNow < deadline)
        {
            count = await _mq.MessageCountAsync(Queue);
            if (count >= 2) break;
            await Task.Delay(100);
        }

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task RedeliversARetriedMessageWithTheAttemptAdvanced()
    {
        var attempts = new List<int>();
        var thirdAttempt = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var consumer = await _mq.ConsumeAsync<OrderPlaced>(Queue, message =>
        {
            lock (attempts) attempts.Add(message.Attempt);
            if (message.Attempt >= 3)
            {
                thirdAttempt.TrySetResult(true);
                return Task.FromResult(Ack.Accept());
            }
            return Task.FromResult(Ack.Retry(TimeSpan.FromMilliseconds(50), "not yet"));
        });

        var envelope = Envelope.Of("order.placed").Build();
        var publisher = _mq.Publisher<OrderPlaced>(Exchange, "order.placed");
        await publisher.SendAsync(new OrderPlaced { OrderId = "A-5" }, envelope);

        await thirdAttempt.Task.WaitAsync(TimeSpan.FromSeconds(20));

        // RabbitMQ requeues the original bytes, so the envelope's attempt header is
        // unchanged on every redelivery. The counter the handler sees is the
        // consumer's own, which is the whole reason it is kept there: read off the
        // wire it would be 1 forever and this loop would never end.
        lock (attempts) Assert.Equal(new[] { 1, 2, 3 }, attempts.Take(3).ToArray());
    }

    [Fact]
    public async Task ReportsTheBrokerItIsTalkingTo()
    {
        Assert.Equal("rabbitmq", _mq.TransportName);
        Assert.True(_mq.Supports(Capability.PublisherConfirms));
        Assert.True(_mq.IsOpen);
        Assert.False(_mq.IsBlocked);
        Assert.True(await _mq.QueueExistsAsync(Queue));
        Assert.False(await _mq.QueueExistsAsync("acemq.test.definitely-not-declared"));
    }
}

/// <summary>
/// The patterns, against a real broker.
/// </summary>
/// <remarks>
/// The in-memory transport agreeing with itself proves nothing about RabbitMQ. These
/// exercise the parts that depend on real broker behaviour: replay uses basic.get,
/// streams use a consumer argument, and request/reply depends on the reply-to
/// property surviving a round trip.
/// </remarks>
public sealed class RabbitMqPatternTests : IAsyncLifetime
{
    private readonly string _url =
        Environment.GetEnvironmentVariable("ACEMQ_TEST_AMQP_URL")
        ?? "amqp://guest:guest@localhost:5672";

    private readonly string _suffix = Guid.NewGuid().ToString("N").Substring(0, 8);
    private AceMqConnection _mq = null!;
    private readonly List<string> _declared = new List<string>();

    private string Name(string what) => $"acemq.test.{_suffix}.{what}";

    public async Task InitializeAsync()
    {
        Transports.Register(new RabbitMqTransport());
        _mq = await AceMqConnection.ConnectAsync(_url);
    }

    public async Task DisposeAsync()
    {
        foreach (var queue in _declared)
        {
            try { await _mq.DeleteQueueAsync(queue); } catch { /* already gone */ }
        }
        _mq.Dispose();
    }

    private async Task<string> QueueAsync(string what)
    {
        var name = Name(what);
        await _mq.DeclareQueueAsync(name);
        _declared.Add(name);
        return name;
    }

    [Fact]
    public async Task AppliesATopologyAndReportsIt()
    {
        var queue = Name("orders");
        var topology = Topology.Define().QueueWithDeadLetter(queue).Build();

        var plan = await _mq.ApplyAsync(topology);
        _declared.Add(queue);
        _declared.Add(queue + ".dead");

        Assert.True(await _mq.QueueExistsAsync(queue));
        Assert.True(await _mq.QueueExistsAsync(queue + ".dead"));
        Assert.Contains("exchange " + queue + ".dlx", plan.Render());
    }

    [Fact]
    public async Task DeadLettersIntoTheQueueTheTopologyDeclared()
    {
        var queue = Name("dl");
        await _mq.ApplyAsync(Topology.Define().QueueWithDeadLetter(queue).Build());
        _declared.Add(queue);
        _declared.Add(queue + ".dead");

        using (var consumer = await _mq.ConsumeAsync<string>(
            queue, _ => Task.FromResult(Ack.DeadLetter("not today"))))
        {
            await _mq.Publisher<string>("", queue).SendAsync("doomed");

            // The whole point of declaring the pair together: a nack with requeue
            // false lands in the dead-letter queue instead of vanishing.
            var deadline = DateTime.UtcNow.AddSeconds(20);
            while (DateTime.UtcNow < deadline)
            {
                if (await _mq.MessageCountAsync(queue + ".dead") > 0) break;
                await Task.Delay(100);
            }
        }

        Assert.Equal(1, await _mq.MessageCountAsync(queue + ".dead"));
    }

    [Fact]
    public async Task AnswersARequestOverARealBroker()
    {
        var queue = await QueueAsync("pricing");

        using var responder = await _mq.RespondAsync<string, string>(
            queue, request => Task.FromResult(request.ToUpperInvariant()));
        using var requester = await _mq.RequesterAsync();
        _declared.Add(requester.ReplyQueue);

        var answer = await requester.RequestAsync<string, string>(
            "", queue, "quote me", TimeSpan.FromSeconds(20), CancellationToken.None);

        Assert.Equal("QUOTE ME", answer);
    }

    [Fact]
    public async Task ReplaysMessagesWithBasicGet()
    {
        var source = await QueueAsync("parked");
        var target = await QueueAsync("live");

        var publisher = _mq.Publisher<string>("", source);
        await publisher.SendAsync("one");
        await publisher.SendAsync("two");

        var moved = await _mq.Replay(source).Into(target).ReplayAllAsync();

        Assert.Equal(2, moved);
        Assert.Equal(2, await _mq.MessageCountAsync(target));
    }

    [Fact]
    public async Task ReadsAStreamFromTheBeginning()
    {
        var stream = Name("events");
        await _mq.DeclareStreamAsync(stream, TimeSpan.FromHours(1), 10_000_000);
        _declared.Add(stream);

        var publisher = _mq.Publisher<string>("", stream);
        await publisher.SendAsync("first");
        await publisher.SendAsync("second");

        var seen = new System.Collections.Concurrent.ConcurrentQueue<string>();
        using var reader = await _mq.Stream<string>(stream)
            .FromFirst()
            .Prefetch(10)
            .ConsumeAsync(message =>
            {
                seen.Enqueue(message.Payload);
                return Task.CompletedTask;
            });

        // A stream keeps what it holds, so a reader starting at the beginning sees
        // messages published before it existed. A queue would have handed them to
        // nobody and dropped them.
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline && seen.Count < 2) await Task.Delay(100);

        Assert.Equal(new[] { "first", "second" }, seen.ToArray());
    }

    [Fact]
    public async Task KeepsOrderWithinAPartition()
    {
        var name = Name("ledger");
        var ordered = await _mq.Ordered<string>(name)
            .Partitions(2)
            .KeyedBy(payload => payload.Split(':')[0])
            .DeclareAsync();
        foreach (var q in ordered.Queues) _declared.Add(q);

        var seen = new System.Collections.Concurrent.ConcurrentQueue<string>();
        await ordered.ConsumeAsync(message =>
        {
            seen.Enqueue(message.Payload);
            return Task.CompletedTask;
        });

        foreach (var op in new[] { "acct:a", "acct:b", "acct:c" })
        {
            await ordered.SendAsync(op);
        }

        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline && seen.Count < 3) await Task.Delay(100);
        ordered.Dispose();

        Assert.Equal(new[] { "acct:a", "acct:b", "acct:c" }, seen.ToArray());
    }
}
