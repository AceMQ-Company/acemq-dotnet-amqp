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

/// <summary>
/// What <see cref="IPublisher{T}.SendAllAsync"/> promises: everything goes out
/// before anything is awaited, the results come back in the order the payloads were
/// given, and a batch that half succeeded says so with counts.
/// </summary>
/// <remarks>
/// These run against a broker that hands confirmation back only when this test says
/// so, one message at a time. The in-memory transport confirms as it is called,
/// which cannot tell a pipelined batch from a loop of single sends: both pass. A
/// broker that holds every confirm until the whole batch has arrived can only be
/// satisfied by the pipelined one, so the round trip per message this method exists
/// to avoid cannot come back unnoticed.
/// </remarks>
public sealed class BatchPublishTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task ReturnsTheResultsInThePayloadOrderWhateverOrderTheConfirmsArriveIn()
    {
        var broker = HeldConfirms.Registered();
        using var mq = await AceMqConnection.ConnectAsync(broker.Url);
        var publisher = mq.Publisher<string>("orders", "order.placed");
        var payloads = new[] { "one", "two", "three", "four", "five" };

        var batch = publisher.SendAllAsync(payloads);
        await Await(broker.WhenSent(payloads.Length), "every message to reach the broker");

        // Backwards, so that a result list built in completion order would come back
        // reversed rather than accidentally right.
        for (var i = payloads.Length - 1; i >= 0; i--) broker.Confirm(i, ConfirmResult.Ok(true));

        var results = await Await(batch, "the batch to be confirmed");

        Assert.Equal(payloads.Length, results.Count);
        for (var i = 0; i < payloads.Length; i++)
        {
            Assert.Equal(payloads[i], broker.SentAs(results[i].MessageId));
            Assert.True(results[i].Routed);
        }
    }

    [Fact]
    public async Task PublishesEveryMessageBeforeAwaitingAnyConfirm()
    {
        var broker = HeldConfirms.Registered();
        using var mq = await AceMqConnection.ConnectAsync(broker.Url);
        var publisher = mq.Publisher<string>("orders", "order.placed");
        var payloads = new[] { "one", "two", "three", "four", "five" };

        var batch = publisher.SendAllAsync(payloads);

        // Nothing has been confirmed yet, so a call that awaited each send in turn
        // would still be sitting on the first one. This completing at all is the
        // assertion: five messages are on the wire with nothing confirmed.
        await Await(broker.WhenSent(payloads.Length), "every message to reach the broker");
        Assert.Equal(payloads, broker.Bodies);

        for (var i = 0; i < payloads.Length; i++) broker.Confirm(i, ConfirmResult.Ok(true));
        var results = await Await(batch, "the batch to be confirmed");

        Assert.Equal(payloads.Length, results.Count);
    }

    [Fact]
    public async Task AwaitsTheRestOfTheBatchAfterAFailureAndReportsHowManySucceeded()
    {
        var broker = HeldConfirms.Registered();
        using var mq = await AceMqConnection.ConnectAsync(broker.Url);
        var publisher = mq.Publisher<string>("orders", "order.placed");
        var payloads = new[] { "one", "two", "three", "four", "five" };

        var batch = publisher.SendAllAsync(payloads);
        await Await(broker.WhenSent(payloads.Length), "every message to reach the broker");

        broker.Confirm(0, ConfirmResult.Ok(true));
        broker.Confirm(1, ConfirmResult.Ok(true));
        broker.Confirm(2, ConfirmResult.Rejected("the queue is full"));

        // The failure has been seen and the last two are still outstanding. Throwing
        // here is what loses the count, so the batch must still be waiting.
        await Task.Delay(100);
        Assert.False(batch.IsCompleted);

        broker.Confirm(3, ConfirmResult.Ok(true));
        broker.Confirm(4, ConfirmResult.Ok(true));

        var failure = await Assert.ThrowsAsync<PublishFailedException>(
            () => Await(batch, "the batch to fail"));

        Assert.StartsWith("1 of 5 messages were not confirmed; 4 were.", failure.Message);
        Assert.Contains("The first failure was: ", failure.Message);
        Assert.Contains("the queue is full", failure.Message);

        // The underlying failure, not just a count: a caller that has to tell a
        // rejection from a timeout needs the exception that was actually raised.
        var cause = Assert.IsType<PublishFailedException>(failure.InnerException);
        Assert.Contains("the queue is full", cause.Message);
    }

    [Fact]
    public async Task CountsEveryFailureInABatchWhereMoreThanOneFailed()
    {
        var broker = HeldConfirms.Registered();
        using var mq = await AceMqConnection.ConnectAsync(broker.Url);
        var publisher = mq.Publisher<string>("orders", "order.placed");
        var payloads = new[] { "one", "two", "three", "four", "five" };

        var batch = publisher.SendAllAsync(payloads);
        await Await(broker.WhenSent(payloads.Length), "every message to reach the broker");

        for (var i = 0; i < payloads.Length; i++)
        {
            broker.Confirm(i, i % 2 == 0 ? ConfirmResult.Ok(true) : ConfirmResult.Rejected("nack " + i));
        }

        var failure = await Assert.ThrowsAsync<PublishFailedException>(
            () => Await(batch, "the batch to fail"));

        Assert.StartsWith("2 of 5 messages were not confirmed; 3 were.", failure.Message);
        // The first failure in payload order, not the first one the broker answered.
        Assert.Contains("nack 1", failure.Message);
    }

    [Fact]
    public async Task RunsThePublishChainForEveryMessageBeforeAnyConfirmIsAwaited()
    {
        var broker = HeldConfirms.Registered();
        using var mq = await AceMqConnection.ConnectAsync(broker.Url);

        // Registered before the publisher exists: a publisher takes the interceptors
        // present when it is created.
        var chain = new CountingInterceptor();
        mq.Intercept(chain);
        var publisher = mq.Publisher<string>("orders", "order.placed");
        var payloads = new[] { "one", "two", "three", "four", "five" };

        var batch = publisher.SendAllAsync(payloads);
        await Await(broker.WhenSent(payloads.Length), "every message to reach the broker");

        // Once per message, and the whole chain ran before anything was confirmed.
        // A loop that awaited each send in turn would be sitting on the first
        // BeforePublish with four still to come.
        Assert.Equal(payloads.Length, chain.Before);
        Assert.Equal(0, chain.Confirms);

        for (var i = 0; i < payloads.Length; i++) broker.Confirm(i, ConfirmResult.Ok(true));
        var results = await Await(batch, "the batch to be confirmed");

        Assert.Equal(payloads.Length, results.Count);
        Assert.Equal(payloads.Length, chain.Confirms);
        Assert.Equal(0, chain.Errors);
    }

    [Fact]
    public async Task FailsOnlyTheMessageWhoseInterceptorRefusedIt()
    {
        var broker = HeldConfirms.Registered();
        using var mq = await AceMqConnection.ConnectAsync(broker.Url);

        mq.Intercept(new RefusesOne("three"));
        var publisher = mq.Publisher<string>("orders", "order.placed");
        var payloads = new[] { "one", "two", "three", "four", "five" };

        var batch = publisher.SendAllAsync(payloads);

        // Four, not five: the refused one never reached the broker, and the rest
        // were not abandoned because of it.
        await Await(broker.WhenSent(4), "the messages that were not refused");
        for (var i = 0; i < 4; i++) broker.Confirm(i, ConfirmResult.Ok(true));

        var failure = await Assert.ThrowsAsync<PublishFailedException>(
            () => Await(batch, "the batch to fail"));

        Assert.StartsWith("1 of 5 messages were not confirmed; 4 were.", failure.Message);
        Assert.Contains("refusing three", failure.Message);
    }

    [Fact]
    public async Task RejectsANullBatch()
    {
        var broker = HeldConfirms.Registered();
        using var mq = await AceMqConnection.ConnectAsync(broker.Url);
        var publisher = mq.Publisher<string>("orders", "order.placed");

        await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.SendAllAsync(null!));
    }

    /// <summary>Counts the three moments of the publish chain.</summary>
    private sealed class CountingInterceptor : PublishInterceptor
    {
        private int _before;
        private int _confirms;
        private int _errors;

        internal int Before => Volatile.Read(ref _before);
        internal int Confirms => Volatile.Read(ref _confirms);
        internal int Errors => Volatile.Read(ref _errors);

        public override PublishContext BeforePublish(PublishContext context)
        {
            Interlocked.Increment(ref _before);
            return context;
        }

        public override void AfterConfirm(PublishContext context, PublishResult result) =>
            Interlocked.Increment(ref _confirms);

        public override void OnError(PublishContext context, Exception failure) =>
            Interlocked.Increment(ref _errors);
    }

    /// <summary>A policy gate that lets everything through but one payload.</summary>
    private sealed class RefusesOne : PublishInterceptor
    {
        private readonly string _refused;
        internal RefusesOne(string refused) => _refused = refused;

        public override PublishContext BeforePublish(PublishContext context)
        {
            if (Equals(context.Payload, _refused))
            {
                throw new InvalidOperationException("refusing " + _refused);
            }

            return context;
        }
    }

    private static async Task<T> Await<T>(Task<T> task, string what)
    {
        if (await Task.WhenAny(task, Task.Delay(Patience)).ConfigureAwait(false) != task)
        {
            throw new TimeoutException($"timed out waiting for {what}");
        }

        return await task.ConfigureAwait(false);
    }

    private static async Task Await(Task task, string what)
    {
        if (await Task.WhenAny(task, Task.Delay(Patience)).ConfigureAwait(false) != task)
        {
            throw new TimeoutException($"timed out waiting for {what}");
        }

        await task.ConfigureAwait(false);
    }

    /// <summary>
    /// A broker that takes messages and answers for them only when told to.
    /// </summary>
    /// <remarks>
    /// Everything that is not publishing throws. This exists to hold confirms open,
    /// and a test that reached one of the other methods would be testing something
    /// else.
    /// </remarks>
    private sealed class HeldConfirms : ITransport, ITransportConnection
    {
        private readonly List<TaskCompletionSource<ConfirmResult>> _held =
            new List<TaskCompletionSource<ConfirmResult>>();

        private readonly List<string> _bodies = new List<string>();

        private readonly ConcurrentDictionary<string, string> _byMessageId =
            new ConcurrentDictionary<string, string>(StringComparer.Ordinal);

        private readonly object _gate = new object();

        private TaskCompletionSource<bool>? _expected;
        private int _expecting;

        private HeldConfirms() =>
            // A scheme of its own per test: the registry is process-wide and these
            // classes run in parallel, so a shared name would have one test's broker
            // answering another's publishes.
            Url = "held-confirms-" + Guid.NewGuid().ToString("N") + "://broker";

        internal static HeldConfirms Registered()
        {
            var broker = new HeldConfirms();
            Transports.Register(broker);
            return broker;
        }

        internal string Url { get; }

        /// <summary>The payloads handed over so far, in the order they arrived.</summary>
        internal IReadOnlyList<string> Bodies
        {
            get { lock (_gate) return _bodies.ToArray(); }
        }

        /// <summary>The payload published under an envelope id.</summary>
        internal string SentAs(string messageId) => _byMessageId[messageId];

        /// <summary>Completes once <paramref name="count"/> messages have arrived.</summary>
        internal Task WhenSent(int count)
        {
            lock (_gate)
            {
                if (_bodies.Count >= count) return Task.CompletedTask;
                _expecting = count;
                _expected = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                return _expected.Task;
            }
        }

        /// <summary>Answers the nth message the broker was given.</summary>
        internal void Confirm(int index, ConfirmResult result)
        {
            TaskCompletionSource<ConfirmResult> held;
            lock (_gate) held = _held[index];
            held.TrySetResult(result);
        }

        public Task<ConfirmResult> SendAsync(OutboundMessage message, CancellationToken cancellationToken)
        {
            var body = (string)new JsonCodec().Decode(message.Body, typeof(string));
            if (message.MessageId != null) _byMessageId[message.MessageId] = body;

            TaskCompletionSource<ConfirmResult> held;
            TaskCompletionSource<bool>? arrived = null;
            lock (_gate)
            {
                held = new TaskCompletionSource<ConfirmResult>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                _held.Add(held);
                _bodies.Add(body);
                if (_expected != null && _bodies.Count >= _expecting)
                {
                    arrived = _expected;
                    _expected = null;
                }
            }

            arrived?.TrySetResult(true);
            return held.Task;
        }

        public IReadOnlyCollection<string> Schemes => new[] { Url.Substring(0, Url.IndexOf("://", StringComparison.Ordinal)) };

        public string Name => "held-confirms";

        public IReadOnlyCollection<Capability> Capabilities => new[] { Capability.PublisherConfirms };

        public Task<ITransportConnection> ConnectAsync(
            ConnectionConfig config, CancellationToken cancellationToken) =>
            Task.FromResult<ITransportConnection>(this);

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

        private static Exception PublishingOnly() =>
            new NotSupportedException("this broker only takes publishes");

        public Task<ISubscription> SubscribeAsync(
            string queue, int prefetch, IReadOnlyDictionary<string, object>? arguments,
            Func<InboundDelivery, Task<Ack>> handler, CancellationToken cancellationToken) =>
            throw PublishingOnly();

        public Task<InboundDelivery?> ReceiveAsync(
            string queue, TimeSpan timeout, CancellationToken cancellationToken) =>
            throw PublishingOnly();

        public Task<IPulledDelivery?> PullAsync(
            string queue, TimeSpan timeout, CancellationToken cancellationToken) =>
            throw PublishingOnly();

        public Task<long> MessageCountAsync(string queue, CancellationToken cancellationToken) =>
            throw PublishingOnly();

        public Task DeleteQueueAsync(string name, CancellationToken cancellationToken) =>
            throw PublishingOnly();

        public Task DeleteExchangeAsync(string name, CancellationToken cancellationToken) =>
            throw PublishingOnly();

        public Task<bool> QueueExistsAsync(string name, CancellationToken cancellationToken) =>
            throw PublishingOnly();

        public Task<QueueCheck> CheckQueueAsync(
            string name, QueueType type, bool durable,
            IReadOnlyDictionary<string, object>? arguments, CancellationToken cancellationToken) =>
            throw PublishingOnly();
    }
}
