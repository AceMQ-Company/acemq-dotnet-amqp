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

public sealed class Invoice
{
    public string InvoiceId { get; set; } = "";
    public long AmountCents { get; set; }
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

    /// <summary>
    /// Deletes a queue and the two a consumer declares beside it.
    /// </summary>
    /// <remarks>
    /// The <c>.dlq</c> and <c>.parked</c> half is the reason this exists. Since
    /// ADR-032 a consumer declares both when it starts rather than on the first
    /// failure, so every queue this suite consumes now leaves a pair behind whether
    /// or not anything in the test ever failed — and a suite that leaks a queue per
    /// run turns a broker into a list of everything anybody has ever tested. Named
    /// rather than pattern-matched, because deleting by prefix on a shared broker is
    /// how one suite tears down another's queues.
    /// </remarks>
    private async Task DeleteWithFailureQueuesAsync(string queue)
    {
        foreach (var name in new[] { queue, Naming.DeadLetterQueue(queue), Naming.ParkedQueue(queue) })
        {
            try { await _mq.DeleteQueueAsync(name); } catch { /* it may never have been declared */ }
        }
    }

    public async Task DisposeAsync()
    {
        foreach (var queue in _alsoDeclared) await DeleteWithFailureQueuesAsync(queue);
        await DeleteWithFailureQueuesAsync(Queue);
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

        // Recreated through the builder this test later dry-runs against.
        //
        // The shared setup declares the queue plainly, and QueueWithRetry now puts
        // x-dead-letter-exchange and x-dead-letter-routing-key on the source queue --
        // so the two descriptions genuinely differ, and AMQP will not change a queue's
        // arguments in place. That is a real migration cost of the fix and not
        // something to hide: a queue declared by an earlier version has to be drained
        // and recreated. Here there is nothing in it yet, so dropping it is honest and
        // cheap; the binding has to be restored because it went with the queue.
        await _mq.DeleteQueueAsync(Queue);
        await _mq.ApplyAsync(
            Topology.Define().QueueWithRetry(Queue, policy).Build(), ApplyMode.Declare);
        await _mq.BindAsync(Queue, Exchange, "order.placed");

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

        // And the round trip just made was a quorum source queue dead-lettering into a
        // classic rung and back. Asked of the broker rather than of the builder,
        // because quorum queues do not dead-letter by the same machinery classic ones
        // do and "it compiled" would prove nothing about that. A dry run reports drift
        // whenever the broker's answer differs from the declaration, so no drift here
        // is the broker confirming every type in the ladder.
        var declared = await _mq.ApplyAsync(
            Topology.Define().QueueWithRetry(Queue, policy).Build(), ApplyMode.DryRun);

        _output.WriteLine(declared.Render());
        Assert.False(declared.HasDrift);
        Assert.Contains($"queue {Queue} (quorum)", declared.Render());
        Assert.Contains($"queue {rung} (classic)", declared.Render());
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
        // The queue and the two a consumer declares beside it. Since ADR-032 those two
        // are there from the moment a consumer starts rather than from the first
        // failure, so a suite that deletes only what it named by hand now leaks a pair
        // per queue it consumed.
        foreach (var queue in _declared)
        {
            foreach (var name in
                     new[] { queue, Naming.DeadLetterQueue(queue), Naming.ParkedQueue(queue) })
            {
                try { await _mq.DeleteQueueAsync(name); } catch { /* already gone */ }
            }
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

    /// <summary>
    /// A queue this library declares is a queue the Java library can consume.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The claim being tested is not "we send <c>x-queue-type=quorum</c>" — that is
    /// visible in the source. It is that a second service, declaring the same queue
    /// the way Java declares it, is accepted by the broker; and that the classic
    /// declaration this library used to send is refused. Only a real broker can
    /// answer either question, because both answers are the broker's.
    /// </para>
    /// <para>
    /// The arguments are written out as literals rather than built from
    /// <see cref="AceMq.Amqp.Naming"/>. Reusing the constants would test that this
    /// library agrees with itself; the strings below are the ones Java's
    /// <c>Topology.queueWithDeadLetter</c> puts on the wire, and copying them here is
    /// the whole point.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task LetsAJavaServiceDeclareTheSameQueueAndRefusesTheClassicOne()
    {
        var queue = Name("shared");
        await _mq.ApplyAsync(Topology.Define().QueueWithDeadLetter(queue).Build());
        _declared.Add(queue);
        _declared.Add(AceMq.Amqp.Naming.DeadLetterQueue(queue));
        _declared.Add(AceMq.Amqp.Naming.ParkedQueue(queue));

        var asJavaWritesIt = new Dictionary<string, object>
        {
            ["x-dead-letter-exchange"] = "acemq.dlx",
            ["x-dead-letter-routing-key"] = queue + ".dlq",
        };

        // A second service, on its own connection, declaring what Java declares. This
        // is the declare that used to fail.
        using (var java = await AceMqConnection.ConnectAsync(_url))
        {
            await java.DeclareQueueAsync(queue, QueueType.Quorum, asJavaWritesIt);
            _output.WriteLine(
                $"accepted: {queue} redeclared as quorum with " +
                "x-dead-letter-exchange=acemq.dlx and " +
                $"x-dead-letter-routing-key={queue}.dlq");
        }

        // And the same declaration as a classic queue, which is what this library sent
        // before today. The broker's refusal is the other half of the claim: these two
        // declarations cannot both work, which is why the five libraries had to pick
        // one.
        using (var stale = await AceMqConnection.ConnectAsync(_url))
        {
            var refused = await Assert.ThrowsAnyAsync<Exception>(
                () => stale.DeclareQueueAsync(queue, QueueType.Classic, asJavaWritesIt));

            Assert.Contains("PRECONDITION_FAILED", refused.Message);
            Assert.Contains("x-queue-type", refused.Message);
            _output.WriteLine("refused:  " + refused.Message);
        }
    }

    /// <summary>
    /// Every queue, exchange and binding the library declares, printed.
    /// </summary>
    /// <remarks>
    /// Five libraries declare this topology and no test any one of them writes can
    /// catch a difference with the other four. What catches it is a person reading
    /// five outputs side by side, so this prints one in a shape that can be read that
    /// way: the queue, its type, and its arguments in a stable order.
    /// </remarks>
    [Fact]
    public async Task PrintsTheWholeTopologyForComparisonWithTheOtherLibraries()
    {
        var queue = Name("orders.new");
        var policy = RetryPolicy.Fixed(3, TimeSpan.FromSeconds(40))
            .WaitInBrokerFrom(TimeSpan.FromSeconds(30));
        var topology = Topology.Define().QueueWithRetry(queue, policy).Build();

        await _mq.ApplyAsync(topology);
        foreach (var declared in topology.Queues) _declared.Add(declared.Name);

        _output.WriteLine("queues");
        foreach (var spec in topology.Queues)
        {
            var arguments = spec.Arguments.Count == 0
                ? "-"
                : string.Join(", ", spec.Arguments
                    .OrderBy(a => a.Key, StringComparer.Ordinal)
                    .Select(a => $"{a.Key}={a.Value}"));
            _output.WriteLine(
                $"  {spec.Name}  [{spec.Type.ToString().ToLowerInvariant()}, " +
                $"durable={spec.Durable}]  {arguments}");
        }

        _output.WriteLine("exchanges");
        foreach (var exchange in topology.Exchanges)
        {
            _output.WriteLine(
                $"  {exchange.Name}  [{exchange.Type}, durable={exchange.Durable}]");
        }

        _output.WriteLine("bindings");
        foreach (var binding in topology.Bindings)
        {
            _output.WriteLine(
                $"  {binding.Queue}  <- {binding.Exchange}  on '{binding.RoutingKey}'");
        }

        // The shape itself, so that a rung or a dead-letter queue changing type is a
        // failure rather than a line somebody did not read.
        Assert.Equal(QueueType.Quorum, topology.Queues.Single(q => q.Name == queue).Type);
        Assert.All(
            topology.Queues.Where(q => q.Name != queue),
            q => Assert.Equal(QueueType.Classic, q.Type));
        Assert.Equal(
            new[] { "acemq.dlx", "acemq.retry" },
            topology.Exchanges.Select(e => e.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task DeadLettersByRepublishingWithTheReasonAttached()
    {
        var queue = await QueueAsync("dl");

        // Declared here as well as by the consumer's failure path, because the poll
        // below asks for its depth: a passive declare of a queue that does not exist
        // yet is a 404 that closes the channel it ran on, and the race is this test's
        // rather than the library's.
        //
        // Classic, explicitly, because that is what the failure path declares a
        // dead-letter queue as. Declaring it here on the library default — quorum
        // since source queues became quorum — makes the consumer's own declare a
        // PRECONDITION_FAILED, and the message never arrives.
        var dead = AceMq.Amqp.Naming.DeadLetterQueue(queue);
        await _mq.DeclareQueueAsync(dead, QueueType.Classic, null);
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

    // ---- the scheduler ---------------------------------------------------
    //
    // The scheduler's own six queues and one exchange are deliberately not deleted
    // afterwards, for the same reason acemq.retry and acemq.dlx are not: they are a
    // fixed, shared topology that every service using the pattern declares, the same
    // way a real deployment has one of them rather than one per caller. Deleting them
    // would be this suite tearing down whatever else on the broker is scheduling.

    /// <summary>
    /// Java's declaration, transcribed rather than referenced.
    /// </summary>
    /// <remarks>
    /// Written out as literals on purpose. Building this table from
    /// <see cref="Scheduler"/>'s own constants would prove that the class agrees with
    /// itself, which is not the question — the question is whether a broker holding
    /// Java's version of these queues accepts .NET's declaration unchanged. So this is
    /// a hand transcription of <c>Scheduler.declareTopology</c> in
    /// <c>acemq-amqp-patterns</c>, and it fails if either side drifts.
    /// </remarks>
    private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, object>> JavasRungs() =>
        new Dictionary<string, IReadOnlyDictionary<string, object>>
        {
            ["acemq.schedule.1h"] = RungArguments(3_600_000L),
            ["acemq.schedule.10m"] = RungArguments(600_000L),
            ["acemq.schedule.1m"] = RungArguments(60_000L),
            ["acemq.schedule.10s"] = RungArguments(10_000L),
            ["acemq.schedule.1s"] = RungArguments(1_000L),
        };

    private static IReadOnlyDictionary<string, object> RungArguments(long ttlMillis) =>
        new Dictionary<string, object>
        {
            ["x-message-ttl"] = ttlMillis,
            ["x-dead-letter-exchange"] = "acemq.schedule",
            ["x-dead-letter-routing-key"] = "acemq.schedule.due",
        };

    /// <summary>
    /// The topology a .NET scheduler declares is the one a Java scheduler declares.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the whole reason the scheduler's names and arguments are a contract
    /// rather than an implementation detail. Two services on one broker declare the
    /// same six queues; if one of them disagreed about a single argument, the second
    /// to start would get 406 <c>PRECONDITION_FAILED</c> and stay down. Only a real
    /// broker answers this question — AMQP has no way to read a queue's arguments
    /// back, so the only thing that reports a difference is a redeclaration.
    /// </para>
    /// <para>
    /// The acceptance half runs on a second connection, so a refusal would close that
    /// channel rather than this test's. The refusal half goes through
    /// <see cref="ApplyMode.DryRun"/>, which asks the same question on a throwaway
    /// channel for exactly that reason.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task DeclaresTheSameSchedulerTopologyJavaDeclares()
    {
        using var scheduler = await Scheduler.OnAsync(_mq);

        _output.WriteLine("exchange  acemq.schedule (direct, durable)");
        _output.WriteLine("queue     acemq.schedule.due (classic, no arguments)");
        _output.WriteLine("binding   acemq.schedule.due <- acemq.schedule [acemq.schedule.due]");

        // Redeclared from a second connection with Java's literal table. The broker
        // accepts an identical declaration and refuses any other, so this returning is
        // the assertion.
        using (var java = await AceMqConnection.ConnectAsync(_url))
        {
            await java.DeclareExchangeAsync("acemq.schedule", "direct");
            foreach (var rung in JavasRungs())
            {
                await java.DeclareQueueAsync(rung.Key, QueueType.Classic, rung.Value);
                await java.BindAsync(rung.Key, "acemq.schedule", rung.Key);

                _output.WriteLine(
                    $"queue     {rung.Key} (classic) "
                    + $"x-message-ttl={rung.Value["x-message-ttl"]} "
                    + $"x-dead-letter-exchange={rung.Value["x-dead-letter-exchange"]} "
                    + $"x-dead-letter-routing-key={rung.Value["x-dead-letter-routing-key"]}");
                _output.WriteLine($"binding   {rung.Key} <- acemq.schedule [{rung.Key}]");
            }

            await java.DeclareQueueAsync("acemq.schedule.due", QueueType.Classic, null);
            await java.BindAsync("acemq.schedule.due", "acemq.schedule", "acemq.schedule.due");

            Assert.True(java.IsOpen, "the broker closed the connection over a declaration");
        }

        // And the same table read back as a topology reports no drift at all.
        var define = Topology.Define().Exchange("acemq.schedule", "direct");
        foreach (var rung in JavasRungs())
        {
            define = define.Queue(rung.Key, QueueType.Classic, rung.Value)
                .Bind(rung.Key, "acemq.schedule", rung.Key);
        }
        var plan = await _mq.ApplyAsync(define.Build(), ApplyMode.DryRun);

        _output.WriteLine(plan.Render());
        Assert.False(plan.HasDrift, plan.Render());
    }

    /// <summary>One argument different and the broker says no.</summary>
    /// <remarks>
    /// The other half of the proof. A test that only showed the matching table being
    /// accepted would pass just as well against a broker that never checks anything.
    /// </remarks>
    [Fact]
    public async Task RefusesASchedulerRungDeclaredWithADifferentArgument()
    {
        using var scheduler = await Scheduler.OnAsync(_mq);

        // A millisecond off the hour. Everything else is Java's.
        var wrong = new Dictionary<string, object>
        {
            ["x-message-ttl"] = 3_600_001L,
            ["x-dead-letter-exchange"] = "acemq.schedule",
            ["x-dead-letter-routing-key"] = "acemq.schedule.due",
        };

        var plan = await _mq.ApplyAsync(
            Topology.Define().Queue("acemq.schedule.1h", QueueType.Classic, wrong).Build(),
            ApplyMode.DryRun);

        _output.WriteLine(plan.Render());
        Assert.True(plan.HasDrift);
        Assert.Contains("PRECONDITION_FAILED", plan.Render());

        // A different dead-letter target is refused just as firmly, which is what stops
        // one service quietly re-pointing everybody else's ladder.
        var elsewhere = new Dictionary<string, object>
        {
            ["x-message-ttl"] = 3_600_000L,
            ["x-dead-letter-exchange"] = "acemq.schedule",
            ["x-dead-letter-routing-key"] = "somewhere.else",
        };
        var second = await _mq.ApplyAsync(
            Topology.Define().Queue("acemq.schedule.1h", QueueType.Classic, elsewhere).Build(),
            ApplyMode.DryRun);

        Assert.True(second.HasDrift);
        Assert.Contains("PRECONDITION_FAILED", second.Render());

        // Asking the question did not break the connection.
        Assert.True(_mq.IsOpen);
    }

    /// <summary>
    /// A scheduled message waits in the broker and arrives intact.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The test the whole ladder exists for, and it cannot be written against a fake:
    /// what has to be shown is that the waiting belongs to the broker. The message
    /// leaves this process immediately, sits on a queue with an <c>x-message-ttl</c>,
    /// and comes back on its own — with the bytes and the content type it was
    /// published under, which is what lets a typed consumer on the far side decode it.
    /// </para>
    /// <para>
    /// <strong>Accurate to about the smallest rung, in either direction.</strong> The
    /// last remainder under a second is delivered rather than waited out, because
    /// another hop would cost more than the accuracy it buys — so a message due in
    /// 3.5s arrives at about 3s. Java's arithmetic is the same and arrives at the same
    /// moment; a scheduler that must fire at 09:00:00.000 exactly is a scheduler, not
    /// a message broker. What is asserted here is therefore the guarantee the ladder
    /// actually makes: the delay minus one rung, and nothing at all before that.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task SchedulesAMessageThroughARealBrokerAndDeliversItLate()
    {
        var exchange = Name("sched");
        var queue = await QueueAsync("sched.target");
        await _mq.DeclareExchangeAsync(exchange, "topic");
        try
        {
            await _mq.BindAsync(queue, exchange, "invoice.due");

            var arrived = new TaskCompletionSource<IMessage<Invoice>>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            using var consumer = await _mq.ConsumeAsync<Invoice>(queue, message =>
            {
                arrived.TrySetResult(message);
                return Task.FromResult(Ack.Accept());
            });

            using var scheduler = await Scheduler.OnAsync(_mq);

            var delay = TimeSpan.FromMilliseconds(3500);
            var sent = DateTimeOffset.UtcNow;
            await scheduler.InAsync(
                delay, exchange, "invoice.due",
                new Invoice { InvoiceId = "INV-42", AmountCents = 1999 });

            // It went straight onto a rung and nothing in this process is waiting.
            Assert.Equal(1, scheduler.Hops);
            Assert.Equal(0, scheduler.Delivered);
            // At least: the rungs are shared, so anything else on the broker using the
            // pattern is waiting in the same queue, and an exact count here would be a
            // test that fails for the wrong reason.
            Assert.True(await _mq.MessageCountAsync("acemq.schedule.1s") >= 1);

            // Not delivered early. This is the half a broken scheduler passes without:
            // one that ignored the delay entirely would already have arrived.
            await Task.Delay(TimeSpan.FromMilliseconds(1500));
            Assert.False(arrived.Task.IsCompleted, "the message was delivered early");

            var received = await arrived.Task.WaitAsync(TimeSpan.FromSeconds(30));
            var waited = DateTimeOffset.UtcNow - sent;
            _output.WriteLine(
                $"scheduled for +{delay.TotalSeconds:0.0}s, arrived after "
                + $"{waited.TotalSeconds:0.00}s in {scheduler.Hops} hop(s)");

            // The guarantee: the delay less one rung, because the last remainder under
            // a second is delivered rather than waited out.
            var floor = delay - TimeSpan.FromSeconds(1);
            Assert.True(waited >= floor, $"arrived after only {waited}, expected at least {floor}");

            // Several hops, not one: a 3.5s delay is three seconds on the 1s rung and
            // then a remainder too small to be worth another.
            Assert.True(scheduler.Hops >= 2, $"only {scheduler.Hops} hop(s)");

            // The payload survived the ladder as opaque bytes and decoded on arrival,
            // which it can only do because the content type travelled with it.
            Assert.Equal("INV-42", received.Payload.InvoiceId);
            Assert.Equal(1999, received.Payload.AmountCents);
            Assert.Equal("application/json", received.ContentType);
            Assert.Equal("invoice.due", received.RoutingKey);

            // The scheduler's bookkeeping is not passed on to the consumer.
            Assert.False(received.Headers.ContainsKey(Scheduler.TargetExchangeHeader));
            Assert.False(received.Headers.ContainsKey(Scheduler.DueAtHeader));
        }
        finally
        {
            try { await _mq.DeleteExchangeAsync(exchange); } catch { /* best effort */ }
        }
    }

    /// <summary>
    /// The control consumer declares no dead-letter queues.
    /// </summary>
    /// <remarks>
    /// Since ADR-032 every consumer declares <c>{queue}.dlq</c> and
    /// <c>{queue}.parked</c> when it starts. The scheduler's control queue has a fixed,
    /// shared name, so an ordinary consumer would leave those two on the broker of
    /// every service that ever constructed a scheduler — durable queues nothing
    /// publishes to and nobody drains. It consumes as a private queue instead.
    /// </remarks>
    [Fact]
    public async Task LeavesNoDeadLetterQueuesBehindForTheControlQueue()
    {
        // Cleared first, in case an earlier version of this library left them here.
        foreach (var name in new[] { "acemq.schedule.due.dlq", "acemq.schedule.due.parked" })
        {
            try { await _mq.DeleteQueueAsync(name); } catch { /* never existed */ }
        }

        using var scheduler = await Scheduler.OnAsync(_mq);

        // The default exchange and a routing key nothing is bound to, so the eventual
        // delivery has somewhere legal to go and nowhere to land.
        await scheduler.InAsync(TimeSpan.FromHours(4), "", Name("never"), "x");

        Assert.False(await _mq.QueueExistsAsync("acemq.schedule.due.dlq"));
        Assert.False(await _mq.QueueExistsAsync("acemq.schedule.due.parked"));
        Assert.False(await _mq.QueueExistsAsync("acemq.schedule.1h.dlq"));
        Assert.False(await _mq.QueueExistsAsync("acemq.schedule.1h.parked"));

        // Tidy the message off the rung: it would sit there for an hour otherwise. Read
        // as raw bytes, because that is what a rung holds and this test has no more
        // business decoding it than the scheduler does.
        using var inspector = await AceMqConnection.ConnectAsync(_url, new BytesCodec());
        var waiting = await inspector.PullAsync<byte[]>("acemq.schedule.1h", TimeSpan.FromSeconds(5));
        if (waiting != null) await waiting.AcknowledgeAsync();
    }
}
