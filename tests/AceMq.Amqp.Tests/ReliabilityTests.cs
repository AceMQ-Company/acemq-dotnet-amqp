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

public sealed class ReliabilityTests : IDisposable
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");
    private readonly string _q = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);

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

    // ---- idempotency -----------------------------------------------------

    [Fact]
    public async Task HandlesADuplicateOnlyOnce()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        var handled = 0;
        var store = InMemoryIdempotencyStore.ForOneDay();

        using var consumer = await mq.ConsumeAsync<string>(
            _q, ConsumerOptions.Defaults().Idempotent(store),
            _ =>
            {
                Interlocked.Increment(ref handled);
                return Task.FromResult(Ack.Accept());
            });

        // The same envelope twice, as a broker redelivering after a crash would.
        var envelope = Envelope.Of("order.placed").Build();
        var publisher = mq.Publisher<string>("", _q);
        await publisher.SendAsync("once", envelope);
        await publisher.SendAsync("once", envelope);

        await Eventually(() => handled >= 1, "the first delivery");
        await Task.Delay(200);

        Assert.Equal(1, handled);
    }

    [Fact]
    public async Task LetsAFailedMessageBeTriedAgain()
    {
        var store = InMemoryIdempotencyStore.ForOneDay();

        Assert.True(await store.ClaimAsync("m-1"));
        Assert.False(await store.ClaimAsync("m-1"));

        // Released rather than confirmed, because the attempt failed. A store that
        // could not express this would treat the retry as a duplicate and drop a
        // message that was never handled.
        await store.ReleaseAsync("m-1");
        Assert.True(await store.ClaimAsync("m-1"));

        await store.ConfirmAsync("m-1");
        Assert.True(await store.IsConfirmedAsync("m-1"));
        Assert.False(await store.ClaimAsync("m-1"));
    }

    [Fact]
    public async Task ForgetsEntriesOnceTheyAgeOut()
    {
        var store = new InMemoryIdempotencyStore(TimeSpan.FromMilliseconds(50));
        Assert.True(await store.ClaimAsync("m-2"));
        await Task.Delay(120);

        // Retention is the window duplicates are caught in, not a permanent record.
        Assert.True(await store.ClaimAsync("m-2"));
        Assert.True(store.Evictions >= 1);
    }

    // ---- retry policy ----------------------------------------------------

    [Fact]
    public void BacksOffExponentiallyAndCaps()
    {
        var policy = RetryPolicy
            .Exponential(5, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(4))
            .WithJitter(0);

        Assert.Equal(
            new[] { 1.0, 2.0, 4.0, 4.0 },
            policy.Schedule().Select(d => d.TotalSeconds).ToArray());
    }

    [Fact]
    public void StopsAfterTheLastAttempt()
    {
        var policy = RetryPolicy.Fixed(3, TimeSpan.FromMilliseconds(10)).WithJitter(0);

        Assert.NotNull(policy.NextDelay(1, TimeSpan.Zero));
        Assert.NotNull(policy.NextDelay(2, TimeSpan.Zero));
        Assert.Null(policy.NextDelay(3, TimeSpan.Zero));
    }

    [Fact]
    public void GivesUpOnAMessageThatIsTooOldHoweverFewAttemptsItHasHad()
    {
        var policy = RetryPolicy
            .Fixed(100, TimeSpan.FromSeconds(1))
            .GiveUpAfter(TimeSpan.FromMinutes(5));

        // Attempts alone cannot express "this has stopped being worth doing".
        Assert.NotNull(policy.NextDelay(1, TimeSpan.FromMinutes(1)));
        Assert.Null(policy.NextDelay(1, TimeSpan.FromMinutes(6)));
    }

    [Fact]
    public void SpreadsRetriesOutSoConsumersDoNotRetryInStep()
    {
        var policy = RetryPolicy.Fixed(10, TimeSpan.FromSeconds(1)).WithJitter(0.5);
        var delays = Enumerable.Range(0, 40)
            .Select(_ => policy.NextDelay(1, TimeSpan.Zero)!.Value.TotalSeconds)
            .ToArray();

        // Without jitter every consumer that failed together retries together, and
        // the dependency gets the whole herd at once.
        Assert.True(delays.Distinct().Count() > 1, "jitter produced identical delays");
        Assert.All(delays, d => Assert.InRange(d, 0.5, 1.5));
    }

    [Fact]
    public async Task DeadLettersAMessageOnceThePolicyIsExhausted()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        var attempts = new ConcurrentQueue<int>();
        using var consumer = await mq.ConsumeAsync<string>(
            _q,
            ConsumerOptions.Defaults().WithRetry(
                RetryPolicy.Fixed(3, TimeSpan.FromMilliseconds(5)).WithJitter(0)),
            message =>
            {
                attempts.Enqueue(message.Attempt);
                throw new InvalidOperationException("always fails");
            });

        await mq.Publisher<string>("", _q).SendAsync("doomed");

        var broker = _url.Substring("memory://".Length);
        await Eventually(
            () => InMemoryTransport.DeadLettered(broker, _q).Count > 0,
            "the message to be given up on");

        // Three attempts, then dead-lettered -- not retried forever.
        Assert.Equal(3, attempts.Count);
        var dead = InMemoryTransport.DeadLettered(broker, _q).Single();
        Assert.Contains("gave up after 3 attempt", dead.Headers[AceHeaders.Error].ToString());
    }

    // ---- health ----------------------------------------------------------

    [Fact]
    public async Task ReportsAHealthyConnection()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var health = mq.Health();

        Assert.Equal(HealthStatus.Up, health.Status);
        var connection = health.Reports.Single(r => r.Name == "connection");
        Assert.Equal("true", connection.Details["open"]);
        Assert.Equal("in-memory", connection.Details["transport"]);
    }

    [Fact]
    public async Task ReportsAHaltedPartitionAsDegraded()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        var ordered = await mq.Ordered<string>("ledger" + _q)
            .Partitions(1)
            .KeyedBy(_ => "same")
            .OnFailure(PartitionFailure.Stop, attempts: 1, delay: TimeSpan.FromMilliseconds(5))
            .DeclareAsync();

        await ordered.ConsumeAsync(_ => throw new InvalidOperationException("no"));
        await ordered.SendAsync("poison");

        await Eventually(() => ordered.HaltedPartitions.Count == 1, "the partition to halt");

        // Degraded, not down: the other partitions still work, and a process that
        // reports itself down gets restarted, which loses the held message.
        var health = mq.Health();
        Assert.Equal(HealthStatus.Degraded, health.Status);
        var report = health.Reports.Single(r => r.Name.StartsWith("ordered:"));
        Assert.Equal("0", report.Details["halted"]);

        ordered.Dispose();
    }

    // ---- pipeline idempotency --------------------------------------------

    [Fact]
    public async Task RunsEachPipelineStepOnceForADuplicate()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var ran = new ConcurrentQueue<string>();

        using var pipeline = await mq.Pipeline<string>("p" + _q)
            .Idempotent(InMemoryIdempotencyStore.ForOneDay())
            .Step("one", (string x) => { ran.Enqueue("one"); return Task.FromResult<string?>(x); })
            .Step("two", (string x) => { ran.Enqueue("two"); return Task.FromResult<string?>(x); })
            .BuildAsync();

        var envelope = Envelope.Of("job").Build();
        await pipeline.SendAsync("work", envelope);
        await pipeline.SendAsync("work", envelope);

        await Eventually(() => pipeline.Completed >= 1, "the pipeline to finish");
        await Task.Delay(200);

        // Each step claims under its own key, so step two still runs after step one
        // has claimed the same envelope. Keyed by message alone, step two would
        // consider it a duplicate of step one and drop it.
        Assert.Equal(1, ran.Count(s => s == "one"));
        Assert.Equal(1, ran.Count(s => s == "two"));
    }

    // ---- graceful shutdown -----------------------------------------------

    [Fact]
    public async Task RefusesToPublishWhilePaused()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);
        var publisher = mq.Publisher<string>("", _q);

        await publisher.SendAsync("before");
        mq.PausePublishing();

        await Assert.ThrowsAsync<PublishingPausedException>(() => publisher.SendAsync("during"));

        mq.ResumePublishing();
        await publisher.SendAsync("after");
        Assert.Equal(2, await mq.MessageCountAsync(_q));
    }

    [Fact]
    public async Task StopsHandingMessagesOverWhilePausedAndResumesWithTheSameOne()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        var handled = new ConcurrentQueue<string>();
        using var consumer = await mq.ConsumeAsync<string>(_q, m =>
        {
            handled.Enqueue(m.Payload);
            return Task.FromResult(Ack.Accept());
        });

        mq.PauseConsuming();
        Assert.True(mq.IsConsumingPaused);
        await mq.Publisher<string>("", _q).SendAsync("held");

        await Task.Delay(200);
        Assert.Empty(handled);

        // Nothing was lost: the message was never acknowledged, so resuming hands
        // over the same one rather than skipping it.
        mq.ResumeConsuming();
        await Eventually(() => handled.Count == 1, "the held message after resuming");
        Assert.Equal("held", handled.Single());
    }

    [Fact]
    public async Task DrainsBeforeShuttingDown()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        var finished = 0;
        using var consumer = await mq.ConsumeAsync<string>(_q, async _ =>
        {
            await Task.Delay(200);
            Interlocked.Increment(ref finished);
            return Ack.Accept();
        });

        await mq.Publisher<string>("", _q).SendAsync("slow");
        await Eventually(() => mq.InFlight == 1, "the handler to start");

        // Disposing here would abandon a handler mid-flight: its side effects have
        // happened, its message has not been acknowledged, and it comes back.
        var drained = await mq.DrainConsumersAsync(TimeSpan.FromSeconds(5));

        Assert.True(drained);
        Assert.Equal(0, mq.InFlight);
        Assert.Equal(1, finished);
        Assert.True(mq.IsConsumingPaused);
    }

    [Fact]
    public async Task ReportsWhenItGaveUpWaitingToDrain()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        using var consumer = await mq.ConsumeAsync<string>(_q, async _ =>
        {
            await Task.Delay(3000);
            return Ack.Accept();
        });

        await mq.Publisher<string>("", _q).SendAsync("very slow");
        await Eventually(() => mq.InFlight == 1, "the handler to start");

        // False rather than an exception: shutting down anyway is a legitimate
        // choice, and the caller needs to know which one it is making.
        Assert.False(await mq.DrainConsumersAsync(TimeSpan.FromMilliseconds(300)));
    }
}
