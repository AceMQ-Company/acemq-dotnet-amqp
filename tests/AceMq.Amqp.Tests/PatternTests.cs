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
using System.Globalization;
using AceMq.Amqp;

namespace AceMq.Amqp.Tests;

public sealed class OutboxOrder
{
    public string OrderId { get; set; } = "";
    public long TotalCents { get; set; }
}

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
        // queues bound to it exist, so Ack.DeadLetter and Ack.Park have somewhere to
        // put a message. Wiring these by hand and forgetting one loses messages
        // silently.
        Assert.True(await mq.QueueExistsAsync("orders.placed"));
        Assert.True(await mq.QueueExistsAsync("orders.placed.dlq"));
        Assert.True(await mq.QueueExistsAsync("orders.placed.parked"));

        // The names the whole family uses. This builder used to produce
        // orders.placed.dlx and orders.placed.dead, which meant one repository held
        // three conventions and an operator had to know which part of the library
        // created the queue before they could find the message.
        Assert.Equal("orders.placed.dlq", Naming.DeadLetterQueue("orders.placed"));
        Assert.Equal("orders.placed.parked", Naming.ParkedQueue("orders.placed"));

        var queue = topology.Queues.Single(q => q.Name == "orders.placed");
        Assert.Equal("acemq.dlx", queue.Arguments["x-dead-letter-exchange"]);

        // Without this the broker would dead-letter into acemq.dlx under the routing
        // key the message arrived with, which matches no binding on a direct
        // exchange and is dropped.
        Assert.Equal("orders.placed.dlq", queue.Arguments["x-dead-letter-routing-key"]);

        var exchange = topology.Exchanges.Single(e => e.Name == "acemq.dlx");
        Assert.Equal("direct", exchange.Type);
        Assert.True(exchange.Durable);

        Assert.Contains(
            topology.Bindings,
            b => b.Queue == "orders.placed.dlq"
                && b.Exchange == "acemq.dlx"
                && b.RoutingKey == "orders.placed.dlq");
        Assert.Contains(
            topology.Bindings,
            b => b.Queue == "orders.placed.parked"
                && b.Exchange == "acemq.dlx"
                && b.RoutingKey == "orders.placed.parked");
    }

    [Fact]
    public void DeclaresOneSharedDeadLetterExchangeForEveryQueue()
    {
        var topology = Topology.Define()
            .QueueWithDeadLetter("orders.placed")
            .QueueWithDeadLetter("orders.cancelled")
            .Build();

        // One exchange, not one per queue. A plan that lists acemq.dlx once per
        // queue is a plan somebody stops reading, and a broker that grows an
        // exchange per queue is a management UI nobody can scan.
        Assert.Single(topology.Exchanges, e => e.Name == "acemq.dlx");
        Assert.Equal(4, topology.Bindings.Count);
    }

    [Fact]
    public void SendsARetryTopologysGiveUpQueuesToTheSamePlace()
    {
        var topology = Topology.Define()
            .QueueWithRetry("orders.placed", RetryPolicy.Fixed(3, TimeSpan.FromMinutes(1)))
            .Build();

        // The retry builder and the dead-letter builder have to agree, or which
        // call declared the queue decides where an operator looks for the message.
        Assert.Contains(topology.Queues, q => q.Name == "orders.placed.dlq");
        Assert.Contains(topology.Queues, q => q.Name == "orders.placed.parked");
        Assert.Contains(topology.Exchanges, e => e.Name == "acemq.dlx");
        Assert.Contains(
            topology.Bindings,
            b => b.Queue == "orders.placed.dlq" && b.Exchange == "acemq.dlx");
    }

    /// <summary>
    /// The counterpart to declaring an exchange, which the transport did not have.
    /// </summary>
    /// <remarks>
    /// Its absence was not a gap in an API nobody used; it was a leak. Anything that
    /// declared an exchange it owned for a while had no way to take it away, so the
    /// integration suite left one behind on every run and a broker used for testing
    /// became a list of everything anybody had ever tested.
    /// </remarks>
    [Fact]
    public async Task RemovesAnExchangeAndTheBindingsOnIt()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareExchangeAsync("temporary", "topic");
        await mq.DeclareQueueAsync("listeners");
        await mq.BindAsync("listeners", "temporary", "#");

        await mq.Publisher<string>("temporary", "anything").SendAsync("first");
        Assert.Equal(1, await mq.MessageCountAsync("listeners"));

        await mq.DeleteExchangeAsync("temporary");

        // The binding went with the exchange, the way a broker drops it, so a
        // publish that used to be routed is now reported as unroutable rather than
        // quietly delivered by a binding that outlived what it was bound to.
        await Assert.ThrowsAsync<PublishFailedException>(
            () => mq.Publisher<string>("temporary", "anything").SendAsync("second"));
        Assert.Equal(1, await mq.MessageCountAsync("listeners"));
    }

    [Fact]
    public async Task DropsAQueuesBindingsWithTheQueue()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareExchangeAsync("events", "topic");
        await mq.DeclareQueueAsync("watchers");
        await mq.BindAsync("watchers", "events", "#");

        await mq.DeleteQueueAsync("watchers");
        await mq.DeclareQueueAsync("watchers");

        // Redeclared, and not still subscribed to everything on 'events'. A binding
        // that outlives its queue is routing nobody asked for.
        await Assert.ThrowsAsync<PublishFailedException>(
            () => mq.Publisher<string>("events", "anything").SendAsync("ignored"));
        Assert.Equal(0, await mq.MessageCountAsync("watchers"));
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

    [Fact]
    public async Task WritesBothReplyAddressesOnEveryRequest()
    {
        // The requester sets AMQP's native reply-to and the acemq-reply-to header to
        // the same value, so a Go, Python or Ruby responder -- which reads only the
        // header -- can answer a .NET requester at all. Asserted on the wire rather
        // than through a .NET responder, which would pass on either half alone.
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("addresses");

        var seen = new BlockingCollection<(string? Native, string? Header)>();
        using var spy = await mq.ConsumeAsync<string>("addresses", message =>
        {
            message.Headers.TryGetValue(Requester.ReplyToHeader, out var header);
            seen.Add((message.ReplyTo, header as string));
            return Task.FromResult(Ack.Accept());
        });

        using var requester = await mq.RequesterAsync();
        var pending = requester.RequestAsync<string, string>(
            "", "addresses", "quote me", TimeSpan.FromSeconds(2), CancellationToken.None);

        Assert.True(seen.TryTake(out var addresses, TimeSpan.FromSeconds(5)));
        Assert.Equal(requester.ReplyQueue, addresses.Native);
        Assert.Equal(requester.ReplyQueue, addresses.Header);

        await Assert.ThrowsAsync<RequestTimedOutException>(() => pending);
    }

    [Fact]
    public async Task AnswersARequestCarryingOnlyTheNativeReplyToProperty()
    {
        // A Java requester, or a .NET one from before the header existed. The
        // responder falls back to the property when the header is absent.
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("pricing.native");
        await mq.DeclareQueueAsync("replies.native");

        using var responder = await mq.RespondAsync<string, string>(
            "pricing.native", request => Task.FromResult(request.ToUpperInvariant()));

        var replies = new BlockingCollection<string>();
        using var reader = await mq.ConsumeAsync<string>("replies.native", message =>
        {
            replies.Add(message.Payload);
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<string>("", "pricing.native", PublishOptions.Defaults(), "replies.native")
            .SendAsync("quote me");

        Assert.True(replies.TryTake(out var answer, TimeSpan.FromSeconds(5)));
        Assert.Equal("QUOTE ME", answer);
        Assert.Equal(0, responder.Unanswerable);
    }

    [Fact]
    public async Task AnswersARequestCarryingOnlyTheAcemqReplyToHeader()
    {
        // A Go, Python or Ruby requester: the header and nothing in the native
        // property. This is the direction that did not work at all before 0.6.0.
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("pricing.header");
        await mq.DeclareQueueAsync("replies.header");

        using var responder = await mq.RespondAsync<string, string>(
            "pricing.header", request => Task.FromResult(request.ToUpperInvariant()));

        var replies = new BlockingCollection<string>();
        using var reader = await mq.ConsumeAsync<string>("replies.header", message =>
        {
            replies.Add(message.Payload);
            return Task.FromResult(Ack.Accept());
        });

        var envelope = Envelope.Of("pricing.header")
            .Header(Requester.ReplyToHeader, "replies.header")
            .Build();
        await mq.Publisher<string>("", "pricing.header").SendAsync("quote me", envelope);

        Assert.True(replies.TryTake(out var answer, TimeSpan.FromSeconds(5)));
        Assert.Equal("QUOTE ME", answer);
        Assert.Equal(0, responder.Unanswerable);
    }

    [Fact]
    public async Task CountsARequestWithNeitherReplyAddressAsUnanswerable()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("pricing.nowhere");

        using var responder = await mq.RespondAsync<string, string>(
            "pricing.nowhere", request => Task.FromResult(request.ToUpperInvariant()));

        await mq.Publisher<string>("", "pricing.nowhere").SendAsync("quote me");

        await Eventually(() => responder.Unanswerable == 1, "the request to be counted unanswerable");
        Assert.Equal(0, responder.Answered);
    }

    [Fact]
    public async Task CountsTheAnswerBeforeTheReplyCanBeSeen()
    {
        // The ordering driven rather than waited for. AfterConfirm runs inside the
        // responder's own publish, at the first instant the reply exists: the broker
        // has taken it and SendAsync has not returned yet. Everything that can see
        // that reply -- the caller holding it, a dashboard reading the counter --
        // happens after this moment, so what Answered reads here is the smallest
        // value any of them can observe. Reading zero here is a responder telling a
        // caller who is holding the answer that nothing has been answered, and it is
        // what the request/reply example had to sleep around.
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("pricing.ordered");
        await mq.DeclareQueueAsync("replies.ordered");

        Responder? responder = null;
        var atTheReply = new TaskCompletionSource<long>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        mq.Intercept(new ReadsACounterAsTheReplyIsConfirmed(
            "replies.ordered", () => responder!.Answered, atTheReply));

        responder = await mq.RespondAsync<string, string>(
            "pricing.ordered", request => Task.FromResult(request.ToUpperInvariant()));

        var envelope = Envelope.Of("pricing.ordered")
            .Header(Requester.ReplyToHeader, "replies.ordered")
            .Build();
        await mq.Publisher<string>("", "pricing.ordered").SendAsync("quote me", envelope);

        var arrived = await Task.WhenAny(atTheReply.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(atTheReply.Task, arrived);
        Assert.Equal(1, await atTheReply.Task);
    }

    [Fact]
    public async Task CountsARequestHandedOverWhileItIsStillStarting()
    {
        // The start-up window made certain instead of unlikely. This transport hands
        // the request over from inside SubscribeAsync, so it is answered before
        // ConsumeAsync has returned a consumer -- and therefore before anything could
        // have built a Responder around one. A broker delivering during the subscribe
        // is what a queue with a backlog looks like from in here, and the counters
        // have to be reachable by then or the request is answered and never recorded.
        var transport = new DeliversDuringSubscribe("pricing.startup", "replies.startup");
        Transports.Register(transport);

        using var mq = await AceMqConnection.ConnectAsync(transport.Url);
        using var responder = await mq.RespondAsync<string, string>(
            "pricing.startup", request => Task.FromResult(request.ToUpperInvariant()));

        // Nothing to wait for: the reply was on the wire before RespondAsync returned.
        Assert.Single(transport.Replies);
        Assert.Equal(1, responder.Answered);
    }

    /// <summary>
    /// Reads a number at the instant a reply reaches the broker.
    /// </summary>
    /// <remarks>
    /// An interceptor rather than a second consumer, because this has to run inside
    /// the publish. A consumer of the reply queue would only prove that the counter
    /// moved eventually, which is the thing a sleep already proves and the thing that
    /// was never in doubt.
    /// </remarks>
    private sealed class ReadsACounterAsTheReplyIsConfirmed : PublishInterceptor
    {
        private readonly string _replyQueue;
        private readonly Func<long> _counter;
        private readonly TaskCompletionSource<long> _read;

        internal ReadsACounterAsTheReplyIsConfirmed(
            string replyQueue, Func<long> counter, TaskCompletionSource<long> read)
        {
            _replyQueue = replyQueue;
            _counter = counter;
            _read = read;
        }

        public override void AfterConfirm(PublishContext context, PublishResult result)
        {
            if (context.RoutingKey == _replyQueue) _read.TrySetResult(_counter());
        }
    }

    /// <summary>
    /// A broker that hands one request over from inside the subscribe call.
    /// </summary>
    /// <remarks>
    /// Everything not needed to deliver one request and take one reply throws. This
    /// exists to close the window between subscribing and having somewhere to count,
    /// and a test that reached one of the other methods would be testing something
    /// else.
    /// </remarks>
    private sealed class DeliversDuringSubscribe : ITransport, ITransportConnection
    {
        private readonly string _requestQueue;
        private readonly string _replyQueue;
        private int _delivered;

        internal DeliversDuringSubscribe(string requestQueue, string replyQueue)
        {
            _requestQueue = requestQueue;
            _replyQueue = replyQueue;
            Url = "startup-window://" + Guid.NewGuid().ToString("N");
        }

        internal string Url { get; }

        internal ConcurrentQueue<string> Replies { get; } = new ConcurrentQueue<string>();

        public IReadOnlyCollection<string> Schemes => new[] { "startup-window" };

        public string Name => "startup-window";

        public IReadOnlyCollection<Capability> Capabilities => Array.Empty<Capability>();

        public Task<ITransportConnection> ConnectAsync(
            ConnectionConfig config, CancellationToken cancellationToken) =>
            Task.FromResult<ITransportConnection>(this);

        public async Task<ISubscription> SubscribeAsync(
            string queue, int prefetch, IReadOnlyDictionary<string, object>? arguments,
            Func<InboundDelivery, Task<Ack>> handler, CancellationToken cancellationToken)
        {
            // Awaited here, not started here: the request is answered before this
            // method returns a subscription, so nothing above can have wrapped one.
            if (queue == _requestQueue && Interlocked.Exchange(ref _delivered, 1) == 0)
            {
                await handler(new InboundDelivery(
                        queue, string.Empty, queue, new JsonCodec().Encode("quote me"),
                        new Dictionary<string, object>(), Guid.NewGuid().ToString("N"),
                        "application/json", false, _replyQueue))
                    .ConfigureAwait(false);
            }

            return new Idle(queue);
        }

        public Task<ConfirmResult> SendAsync(
            OutboundMessage message, CancellationToken cancellationToken)
        {
            if (message.RoutingKey == _replyQueue)
            {
                Replies.Enqueue(System.Text.Encoding.UTF8.GetString(message.Body));
            }

            return Task.FromResult(ConfirmResult.Ok(true));
        }

        public Task DeclareExchangeAsync(
            string name, string type, bool durable, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task DeclareQueueAsync(
            string name, QueueType type, bool durable,
            IReadOnlyDictionary<string, object>? arguments, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task BindQueueAsync(
            string queue, string exchange, string routingKey, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public bool IsOpen => true;

        public bool IsBlocked => false;

        public string? BlockedReason => null;

        public void Dispose() { }

        private static Exception OneRequestOnly() =>
            new NotSupportedException("this broker delivers one request and takes one reply");

        public Task<InboundDelivery?> ReceiveAsync(
            string queue, TimeSpan timeout, CancellationToken cancellationToken) =>
            throw OneRequestOnly();

        public Task<IPulledDelivery?> PullAsync(
            string queue, TimeSpan timeout, CancellationToken cancellationToken) =>
            throw OneRequestOnly();

        public Task<long> MessageCountAsync(string queue, CancellationToken cancellationToken) =>
            throw OneRequestOnly();

        public Task DeleteQueueAsync(string name, CancellationToken cancellationToken) =>
            throw OneRequestOnly();

        public Task DeleteExchangeAsync(string name, CancellationToken cancellationToken) =>
            throw OneRequestOnly();

        public Task<bool> QueueExistsAsync(string name, CancellationToken cancellationToken) =>
            throw OneRequestOnly();

        public Task<QueueCheck> CheckQueueAsync(
            string name, QueueType type, bool durable,
            IReadOnlyDictionary<string, object>? arguments, CancellationToken cancellationToken) =>
            throw OneRequestOnly();

        private sealed class Idle : ISubscription
        {
            internal Idle(string queue) => Queue = queue;

            public string Queue { get; }

            public bool IsActive => true;

            public void Dispose() { }
        }
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
    public async Task GivesAReplayedMessageAFreshSetOfAttempts()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("work");
        await mq.DeclareQueueAsync("work.dlq");

        // A message dead-lettered on the last attempt of its policy, which is what a
        // dead-letter queue is full of.
        await mq.Publisher<string>("", "work.dlq").SendAsync(
            "exhausted",
            Envelope.Of("job").Attempt(5).Error("gave up after 5 attempt(s)").Build());

        Assert.Equal("work", mq.Replay("work.dlq").To);
        Assert.Equal(1, await mq.Replay("work.dlq").ReplayAllAsync());

        // Back on attempt one. Without the reset it would arrive still on attempt five,
        // be given up on before any handler saw it, and the operator who has just fixed
        // the bug would have moved the whole queue to the same queue.
        var back = await mq.Transport.ReceiveAsync(
            "work", TimeSpan.FromSeconds(2), CancellationToken.None);
        Assert.NotNull(back);
        var envelope = Envelope.FromWire(back!.Headers);
        Assert.Equal(1, envelope.Attempt);
        Assert.Null(envelope.Error);
    }

    [Fact]
    public async Task StampsAReplayedMessageWhereAHandlerCanSeeIt()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("work");
        await mq.DeclareQueueAsync("work.dlq");

        await mq.Publisher<string>("", "work.dlq").SendAsync(
            "stamped", Envelope.Of("job").Build());

        Assert.Equal(1, await mq.Replay("work.dlq").ReplayAllAsync());

        var back = await mq.Transport.ReceiveAsync(
            "work", TimeSpan.FromSeconds(2), CancellationToken.None);
        Assert.NotNull(back);

        // Through Envelope.FromWire rather than off the raw delivery, because that is
        // the reading a handler gets and the one the reserved namespace used to eat.
        var headers = Envelope.FromWire(back!.Headers).Headers;
        Assert.Equal("work.dlq", headers[AceHeaders.ReplayedFrom]);
        Assert.Equal(1, Convert.ToInt32(
            headers[AceHeaders.ReplayCount], CultureInfo.InvariantCulture));

        // The shape the other four write: whole seconds, and a Z. Not "o", which
        // carries an offset and seven fractional digits that nothing else produces.
        var at = Assert.IsType<string>(headers[AceHeaders.ReplayedAt]);
        Assert.Matches(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", at);
        Assert.Equal(
            at,
            DateTimeOffset.Parse(at, CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind)
                .UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));

        // And nothing left behind under the old spelling: one name for one fact.
        Assert.DoesNotContain(AceHeaders.LegacyReplayedFrom, back.Headers.Keys);
        Assert.DoesNotContain(AceHeaders.LegacyReplayedAt, back.Headers.Keys);
        Assert.DoesNotContain(AceHeaders.LegacyReplayCount, back.Headers.Keys);
    }

    [Fact]
    public async Task CarriesOnCountingFromAStampWrittenBefore0Point6()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("work");
        await mq.DeclareQueueAsync("work.dlq");

        // A message a 0.5.0 service replayed twice: the count is on the wire under the
        // reserved spelling, which Envelope.FromWire will not show anybody. Reading it
        // anyway is the difference between a third replay and a first.
        await mq.Transport.SendAsync(
            new OutboundMessage(
                "", "work.dlq", new byte[] { 1 },
                new Dictionary<string, object>
                {
                    [AceHeaders.Id] = "old-1",
                    [AceHeaders.Type] = "job",
                    [AceHeaders.LegacyReplayedFrom] = "work.dlq",
                    [AceHeaders.LegacyReplayedAt] = "2026-02-03T04:05:06.789Z",
                    [AceHeaders.LegacyReplayCount] = 2,
                },
                "old-1", "application/json",
                persistent: true, mandatory: true, expiration: null, priority: null,
                replyTo: null),
            CancellationToken.None);

        Assert.Equal(1, await mq.Replay("work.dlq").ReplayAllAsync());

        var back = await mq.Transport.ReceiveAsync(
            "work", TimeSpan.FromSeconds(2), CancellationToken.None);
        Assert.NotNull(back);

        var headers = Envelope.FromWire(back!.Headers).Headers;
        Assert.Equal(3, Convert.ToInt32(
            headers[AceHeaders.ReplayCount], CultureInfo.InvariantCulture));
        Assert.DoesNotContain(AceHeaders.LegacyReplayCount, back.Headers.Keys);
    }

    [Fact]
    public async Task PutsBackExactlyWhatWasThereWhenAskedTo()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("work");
        await mq.DeclareQueueAsync("work.dlq");

        await mq.Publisher<string>("", "work.dlq").SendAsync(
            "audited", Envelope.Of("job").Attempt(5).Build());

        // For an audit, or for a queue read by something that counts attempts itself.
        Assert.Equal(1, await mq.Replay("work.dlq").KeepingAttempts().ReplayAllAsync());

        var back = await mq.Transport.ReceiveAsync(
            "work", TimeSpan.FromSeconds(2), CancellationToken.None);
        Assert.NotNull(back);
        Assert.Equal(5, Envelope.FromWire(back!.Headers).Attempt);
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

    // The property a message count cannot see: what the relay published is
    // readable by an ordinary typed consumer.
    //
    // The payload was serialised inside the writer's transaction, so publishing
    // it through the connection's codec would encode it a second time and put
    // JSON containing JSON on the queue. Everything would still look right --
    // the record is marked published, the count is 1 -- and the only consumer
    // able to read it would be one taking a string and parsing it by hand.
    [Fact]
    public async Task PublishesAPayloadATypedConsumerCanRead()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("outbox-typed");

        var arrived = new TaskCompletionSource<OutboxOrder>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var consumer = await mq.ConsumeAsync<OutboxOrder>("outbox-typed", message =>
        {
            arrived.TrySetResult(message.Payload);
            return Task.FromResult(Ack.Accept());
        });

        var store = new InMemoryOutboxStore();
        await store.AddAsync(OutboxRecord.Of(
            "", "outbox-typed", Envelope.Of("order.placed").Build(),
            "{\"orderId\":\"o-1\",\"totalCents\":1999}"));

        using var relay = mq.Outbox(store);
        Assert.Equal(1, await relay.DrainAsync());

        var order = await arrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("o-1", order.OrderId);
        Assert.Equal(1999, order.TotalCents);
    }

    // The same, without the caller writing JSON by hand.
    [Fact]
    public async Task RecordsAPayloadWithTheConnectionsCodec()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("outbox-for");

        var arrived = new TaskCompletionSource<OutboxOrder>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var consumer = await mq.ConsumeAsync<OutboxOrder>("outbox-for", message =>
        {
            arrived.TrySetResult(message.Payload);
            return Task.FromResult(Ack.Accept());
        });

        var store = new InMemoryOutboxStore();
        await store.AddAsync(OutboxRecord.For(
            mq, "", "outbox-for", new OutboxOrder { OrderId = "o-3", TotalCents = 75 }));

        using var relay = mq.Outbox(store);
        Assert.Equal(1, await relay.DrainAsync());

        var order = await arrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("o-3", order.OrderId);
        Assert.Equal(75, order.TotalCents);
    }

    // And the envelope survives the trip, so a consumer can still correlate.
    [Fact]
    public async Task PublishesTheEnvelopeItWasGiven()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("outbox-envelope");

        var arrived = new TaskCompletionSource<IMessage<OutboxOrder>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var consumer = await mq.ConsumeAsync<OutboxOrder>("outbox-envelope", message =>
        {
            arrived.TrySetResult(message);
            return Task.FromResult(Ack.Accept());
        });

        var store = new InMemoryOutboxStore();
        await store.AddAsync(OutboxRecord.Of(
            "", "outbox-envelope",
            Envelope.Of("order.placed").CorrelationId("checkout-9").Build(),
            "{\"orderId\":\"o-2\",\"totalCents\":250}"));

        using var relay = mq.Outbox(store);
        await relay.DrainAsync();

        var message = await arrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("order.placed", message.Envelope.Type);
        Assert.Equal("checkout-9", message.Envelope.CorrelationId);
    }

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
