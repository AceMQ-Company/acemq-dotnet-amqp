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
using System.Linq;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;
using Xunit.Abstractions;

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
    private readonly ITestOutputHelper _output;
    private AceMqConnection _mq = null!;

    /// <summary>
    /// Queues this test declared beyond the one every test gets, so that the suite
    /// leaves the broker exactly as it found it.
    /// </summary>
    /// <remarks>
    /// Counted rather than trusted: a suite that leaks a queue per run turns a broker
    /// into a list of everything anybody has ever tested, and the retry rungs are
    /// precisely the queues nobody declared by hand and so nobody thinks to delete.
    /// </remarks>
    private readonly ConcurrentBag<string> _alsoDeclared = new ConcurrentBag<string>();

    /// <summary>
    /// Exchanges this test declared, which until <c>DeleteExchangeAsync</c> existed
    /// could not be removed at all.
    /// </summary>
    /// <remarks>
    /// Every run left <c>acemq.test.{suffix}</c> behind, for ever. The two the
    /// library owns — <c>acemq.retry</c> and <c>acemq.dlx</c> — are deliberately not
    /// in here: they are declared once and shared by every queue on the broker, the
    /// same way a real deployment has them, so deleting them would be this suite
    /// tearing down somebody else's topology rather than its own.
    /// </remarks>
    private readonly ConcurrentBag<string> _declaredExchanges = new ConcurrentBag<string>();

    public RabbitMqTransportTests(ITestOutputHelper output) => _output = output;

    private string Exchange => $"acemq.test.{_suffix}";
    private string Queue => $"acemq.test.{_suffix}.q";

    public async Task InitializeAsync()
    {
        Transports.Register(new RabbitMqTransport());
        _mq = await AceMqConnection.ConnectAsync(_url);
        await _mq.DeclareExchangeAsync(Exchange, "topic");
        _declaredExchanges.Add(Exchange);
        await _mq.DeclareQueueAsync(Queue);
        await _mq.BindAsync(Queue, Exchange, "order.placed");
    }

    public async Task DisposeAsync()
    {
        foreach (var queue in _alsoDeclared)
        {
            try { await _mq.DeleteQueueAsync(queue); } catch { /* it may never have been declared */ }
        }
        try { await _mq.DeleteQueueAsync(Queue); } catch { /* the test may have failed before declaring */ }
        foreach (var exchange in _declaredExchanges)
        {
            try { await _mq.DeleteExchangeAsync(exchange); } catch { /* likewise */ }
        }
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

    /// <summary>
    /// The retry rungs, against a real broker, with the consumer switched off for the
    /// duration of the wait.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the test the design exists for, and it cannot be written against a
    /// fake. What it has to show is that the wait belongs to the broker: the message
    /// sits on a queue with an <c>x-message-ttl</c>, the queue it came from is empty,
    /// and then — with the consumer disposed, so nothing in this process is sleeping,
    /// holding a delivery or counting anything — it comes back on its own, one attempt
    /// further on.
    /// </para>
    /// <para>
    /// A one-second threshold rather than the default thirty, so the test takes
    /// seconds. The arithmetic under test is the same arithmetic either way; what is
    /// being proved is where the waiting happens, not how long it is.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task WaitsALongRetryInTheBrokerWithNoConsumerRunning()
    {
        var delay = TimeSpan.FromSeconds(3);
        var policy = RetryPolicy.Fixed(3, delay).WaitInBrokerFrom(TimeSpan.FromSeconds(1));
        var ladder = RetryLadder.For(Queue, policy);

        // Printed so the declaration can be read against the Python and Ruby libraries'
        // by eye.
        _output.WriteLine(ladder.Describe());

        var rung = AceMq.Amqp.Naming.RetryQueue(Queue, delay);
        _alsoDeclared.Add(rung);
        _alsoDeclared.Add(ladder.DeadLetterQueue);
        _alsoDeclared.Add(ladder.ParkedQueue);

        var attempts = new ConcurrentQueue<int>();
        var consumer = await _mq.ConsumeAsync<OrderPlaced>(
            Queue,
            ConsumerOptions.Defaults().WithRetry(policy),
            message =>
            {
                attempts.Enqueue(message.Envelope.Attempt);
                throw new InvalidOperationException("the warehouse is down");
            });

        await _mq.Publisher<OrderPlaced>(Exchange, "order.placed")
            .SendAsync(new OrderPlaced { OrderId = "A-9", Total = 1m });

        // The first attempt failed and the message went onto the rung.
        await Eventually(
            async () => await _mq.MessageCountAsync(rung) == 1,
            "the message to be sitting on the rung");

        // Nothing is waiting on the queue it came from either, which is the half of
        // this that a per-message expiration would also satisfy — so the next two
        // assertions are the ones that matter.
        Assert.Equal(0, await _mq.MessageCountAsync(Queue));
        _output.WriteLine(
            $"waiting: {rung} holds 1, {Queue} holds 0, no consumer attached");

        // Off entirely. From here nothing in this process can produce a redelivery,
        // hold a prefetch slot, or shorten a wait — which is the difference between a
        // retry that survives a deploy and one that does not.
        consumer.Dispose();
        Assert.Single(attempts);

        await Eventually(
            async () => await _mq.MessageCountAsync(Queue) == 1,
            "the broker to hand the message back when the time-to-live expired");
        Assert.Equal(0, await _mq.MessageCountAsync(rung));

        // Back one attempt further on, because the retry republished with
        // x-acemq-attempt advanced rather than requeueing the bytes the broker had.
        var returned = await _mq.Transport.ReceiveAsync(
            Queue, TimeSpan.FromSeconds(10), CancellationToken.None);
        Assert.NotNull(returned);
        var envelope = Envelope.FromWire(returned!.Headers, returned.RoutingKey, returned.MessageId);
        Assert.Equal(2, envelope.Attempt);
        _output.WriteLine($"returned: attempt {envelope.Attempt} on {Queue}");
    }

    private static async Task Eventually(Func<Task<bool>> probe, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (await probe()) return;
            await Task.Delay(100);
        }
        throw new TimeoutException($"timed out waiting for {what}");
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
    private readonly ITestOutputHelper _output;
    private AceMqConnection _mq = null!;
    private readonly List<string> _declared = new List<string>();

    public RabbitMqPatternTests(ITestOutputHelper output) => _output = output;

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

    /// <summary>
    /// The whole dead-letter topology, applied to a real broker and printed.
    /// </summary>
    /// <remarks>
    /// The plan is written to the test output on purpose. Five libraries declare
    /// these queues, and the only check that catches a difference all five have
    /// written a passing test for is a person reading the five outputs side by side.
    /// </remarks>
    [Fact]
    public async Task AppliesATopologyAndReportsIt()
    {
        var queue = Name("orders");
        var topology = Topology.Define().QueueWithDeadLetter(queue).Build();

        var plan = await _mq.ApplyAsync(topology);
        _declared.Add(queue);
        _declared.Add(AceMq.Amqp.Naming.DeadLetterQueue(queue));
        _declared.Add(AceMq.Amqp.Naming.ParkedQueue(queue));

        _output.WriteLine("dead-letter topology for " + queue + ":");
        _output.WriteLine(plan.Render());

        Assert.True(await _mq.QueueExistsAsync(queue));
        Assert.True(await _mq.QueueExistsAsync(queue + ".dlq"));
        Assert.True(await _mq.QueueExistsAsync(queue + ".parked"));

        // One exchange for the broker, named the same thing as Java's, rather than
        // one per queue named after it.
        Assert.Contains("exchange acemq.dlx (direct)", plan.Render());
        Assert.Contains($"bind {queue}.dlq to acemq.dlx on '{queue}.dlq'", plan.Render());
        Assert.Contains($"bind {queue}.parked to acemq.dlx on '{queue}.parked'", plan.Render());
        Assert.DoesNotContain(".dead", plan.Render());
    }

    [Fact]
    public async Task DeadLettersByRepublishingWithTheReasonAttached()
    {
        var queue = await QueueAsync("dl");

        // Declared here as well as by the consumer's failure path, because the poll
        // below asks for its depth: a passive declare of a queue that does not exist
        // yet is a 404 that closes the channel it ran on, and the race is this test's
        // rather than the library's.
        var dead = AceMq.Amqp.Naming.DeadLetterQueue(queue);
        await _mq.DeclareQueueAsync(dead);
        _declared.Add(dead);

        using (var consumer = await _mq.ConsumeAsync<string>(
            queue, _ => Task.FromResult(Ack.DeadLetter("not today"))))
        {
            await _mq.Publisher<string>("", queue).SendAsync("doomed");

            var deadline = DateTime.UtcNow.AddSeconds(20);
            while (DateTime.UtcNow < deadline)
            {
                if (await _mq.MessageCountAsync(dead) > 0) break;
                await Task.Delay(100);
            }
        }

        Assert.Equal(1, await _mq.MessageCountAsync(dead));

        // Republished and then acknowledged, rather than rejected. That is what puts
        // x-acemq-error on it: a broker dead-lettering a rejected message writes its
        // own x-death bookkeeping and nothing about what the handler could not do, and
        // a queue with no dead-letter exchange configured discards it entirely.
        var delivery = await _mq.Transport.ReceiveAsync(
            dead, TimeSpan.FromSeconds(10), CancellationToken.None);
        Assert.NotNull(delivery);
        Assert.Equal("not today", Envelope.FromWire(delivery!.Headers).Error);

        // And nothing was left behind on the queue it was rejected from.
        Assert.Equal(0, await _mq.MessageCountAsync(queue));
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

    [Fact]
    public async Task ReportsAQueueTheBrokerAlreadyHasWithDifferentArguments()
    {
        var queue = Name("drift");
        await _mq.DeclareQueueAsync(queue, QueueType.Classic,
            new Dictionary<string, object> { ["x-message-ttl"] = 60000 });
        _declared.Add(queue);

        // AMQP cannot read a queue's arguments back. The only thing that answers is
        // a declaration, which the broker refuses with 406 PRECONDITION_FAILED when
        // the settings differ -- on a throwaway channel, because a failed declare
        // closes the one it ran on.
        var plan = await _mq.ApplyAsync(
            Topology.Define()
                .Queue(queue, QueueType.Classic,
                    new Dictionary<string, object> { ["x-message-ttl"] = 30000 })
                .Build(),
            ApplyMode.DryRun);

        Assert.True(plan.HasDrift);
        Assert.Contains("PRECONDITION_FAILED", plan.Render());

        // The connection still works: asking the question must not break publishing.
        Assert.True(_mq.IsOpen);
        Assert.Equal(0, await _mq.MessageCountAsync(queue));
    }

    [Fact]
    public async Task ReportsAMatchingQueueAsPresent()
    {
        var queue = Name("match");
        var arguments = new Dictionary<string, object> { ["x-message-ttl"] = 60000 };
        await _mq.DeclareQueueAsync(queue, QueueType.Classic, arguments);
        _declared.Add(queue);

        var plan = await _mq.ApplyAsync(
            Topology.Define().Queue(queue, QueueType.Classic, arguments).Build(),
            ApplyMode.DryRun);

        Assert.False(plan.HasDrift);
        Assert.Equal(TopologyActionKind.Present, plan.Actions.Single().Kind);
    }

    [Fact]
    public async Task CarriesARoutingSlipThroughARealBroker()
    {
        var steps = new[] { Name("r1"), Name("r2") };
        foreach (var step in steps) { await _mq.DeclareQueueAsync(step); _declared.Add(step); }

        var visited = new System.Collections.Concurrent.ConcurrentQueue<string>();
        var finished = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var consumers = new List<IMessageConsumer>();
        foreach (var step in steps)
        {
            var here = step;
            consumers.Add(await _mq.ConsumeAsync<string>(here, async message =>
            {
                visited.Enqueue(here);
                var slip = RoutingSlip.Of(message)!.Advance();
                if (slip.IsFinished) finished.TrySetResult(true);
                else await _mq.ForwardAsync(slip, message.Payload, message.Envelope);
                return Ack.Accept();
            }));
        }

        await _mq.SendAlongAsync(RoutingSlip.StartOf(steps), "an order");
        await finished.Task.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.Equal(steps, visited.ToArray());
        foreach (var consumer in consumers) consumer.Dispose();
    }
}
