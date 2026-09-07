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
using Xunit.Abstractions;

namespace AceMq.Amqp.Tests;

/// <summary>
/// The retry schedule and the rung declaration, which have to match the other
/// languages exactly.
/// </summary>
/// <remarks>
/// <para>
/// These are cross-language contract rather than local behaviour. The same message can
/// be retried by a consumer written in Java, Go, .NET, Python or Ruby, so the same
/// policy has to produce the same delays in all five; and two services consuming the
/// same queue declare the same rung <em>by name</em>, so a rung declared with different
/// arguments in one of them is a PRECONDITION_FAILED that stops the other consuming at
/// all. The Ruby library shipped a named <c>acemq.retry</c> exchange against Python's
/// default one for a few hours and would have broken exactly that way.
/// </para>
/// <para>
/// The numbers here are copied from the Python library's <c>tests/test_retry.py</c> and
/// the Ruby library's <c>spec/retry_policy_spec.rb</c> and
/// <c>spec/retry_ladder_spec.rb</c>. When they disagree, one of the three is wrong and
/// the question is which — not which one is more convenient to change.
/// </para>
/// </remarks>
public sealed class RetryContractTests
{
    private static readonly TimeSpan Second = TimeSpan.FromSeconds(1);

    private readonly ITestOutputHelper _output;

    public RetryContractTests(ITestOutputHelper output) => _output = output;

    private static double[] Seconds(IEnumerable<TimeSpan> delays) =>
        delays.Select(d => d.TotalSeconds).ToArray();

    // ---- the schedule ----------------------------------------------------

    [Fact]
    public void TheScheduleDoublesAndIsTheSameEverywhere()
    {
        // The same four numbers Java, Go, Python and Ruby produce for this policy. A
        // message retried by a .NET consumer and then by a Go one must not wait
        // different amounts for the same attempt.
        Assert.Equal(
            new[] { 1.0, 2.0, 4.0, 8.0 },
            Seconds(RetryPolicy.Exponential(5, Second, TimeSpan.FromMinutes(1)).Schedule()));
    }

    [Fact]
    public void TheCeilingHolds()
    {
        Assert.Equal(
            new[] { 1.0, 2.0, 4.0, 4.0, 4.0 },
            Seconds(RetryPolicy.Exponential(6, Second, TimeSpan.FromSeconds(4)).Schedule()));
    }

    [Fact]
    public void TheCeilingIsAppliedInsideTheLoopAsWellAsAfterIt()
    {
        // Without the cap inside the loop the running delay climbs towards infinity
        // before the ceiling is ever consulted, and arrives at a number that
        // overflowed on the way — which reads back as an immediate retry rather than
        // a long one.
        var reckless = RetryPolicy.Exponential(200, Second, 10, TimeSpan.FromMinutes(1));

        Assert.All(
            reckless.Schedule(),
            delay => Assert.InRange(delay, TimeSpan.Zero, TimeSpan.FromMinutes(1)));
        Assert.Equal(TimeSpan.FromMinutes(1), reckless.NextDelay(199, TimeSpan.Zero, jitter: false));
    }

    [Fact]
    public void NoRetryMeansOneDelivery()
    {
        Assert.Empty(RetryPolicy.None().Schedule());
        Assert.Null(RetryPolicy.None().NextDelay(1, TimeSpan.Zero));
    }

    [Fact]
    public void FixedWaitsTheSameEveryTime()
    {
        Assert.Equal(
            new[] { 30.0, 30.0, 30.0 },
            Seconds(RetryPolicy.Fixed(4, TimeSpan.FromSeconds(30)).Schedule()));
    }

    [Fact]
    public void TheLastAttemptHasNoNextDelay()
    {
        var policy = RetryPolicy.Exponential(3, Second);

        Assert.NotNull(policy.NextDelay(1, TimeSpan.Zero));
        Assert.NotNull(policy.NextDelay(2, TimeSpan.Zero));
        // The third delivery is the last one. Asking for a fourth is how a message is
        // retried for ever by a library that counts wrong.
        Assert.Null(policy.NextDelay(3, TimeSpan.Zero));
    }

    [Fact]
    public void AnOldMessageIsGivenUpOnHoweverFewAttemptsItHasHad()
    {
        var policy = RetryPolicy.Exponential(10, Second).GiveUpAfter(TimeSpan.FromHours(1));

        Assert.NotNull(policy.NextDelay(1, TimeSpan.FromMinutes(59)));
        // One attempt, four hours old: a paused queue produces exactly this, and
        // delivering it now helps nobody.
        Assert.Null(policy.NextDelay(1, TimeSpan.FromHours(4)));
    }

    [Fact]
    public void GivingUpAfterNothingMeansNever()
    {
        // Zero is "no age limit" in every one of the five libraries. A .NET caller who
        // reads it as "give up immediately" would find their queue draining into the
        // dead letters.
        var policy = RetryPolicy.Exponential(10, Second).GiveUpAfter(TimeSpan.Zero);

        Assert.NotNull(policy.NextDelay(1, TimeSpan.FromDays(400)));
    }

    // ---- jitter ----------------------------------------------------------

    [Fact]
    public void JitterMovesBothWaysAndStaysInsideTheFactor()
    {
        var policy = RetryPolicy.Exponential(2, TimeSpan.FromSeconds(10));
        var delays = Enumerable.Range(0, 200)
            .Select(_ => policy.NextDelay(1, TimeSpan.Zero)!.Value.TotalSeconds)
            .ToArray();

        // 20% either side of ten seconds, and genuinely either side: jitter that only
        // ever delays turns a thundering herd into a slower thundering herd.
        Assert.All(delays, d => Assert.InRange(d, 8.0, 12.0));
        Assert.Contains(delays, d => d < 10);
        Assert.Contains(delays, d => d > 10);
    }

    [Fact]
    public void ADelayIsNeverNegative()
    {
        var reckless = RetryPolicy.Exponential(2, Second).WithJitter(1);
        var delays = Enumerable.Range(0, 200)
            .Select(_ => reckless.NextDelay(1, TimeSpan.Zero)!.Value);

        Assert.All(delays, d => Assert.True(d >= TimeSpan.Zero, $"{d} is negative"));
    }

    [Fact]
    public void ABrokerWaitIsNeverJittered()
    {
        // A rung queue's time-to-live is fixed when it is declared, so a moved delay
        // would name a queue that is not there. The spread is free anyway: each
        // message's time-to-live starts when it arrives rather than when the batch
        // failed.
        var policy = RetryPolicy.Exponential(2, TimeSpan.FromMinutes(1));
        var waits = Enumerable.Range(0, 50).Select(_ => policy.NextWait(1, TimeSpan.Zero));

        Assert.All(waits, w => Assert.Equal(TimeSpan.FromMinutes(1), w!.Delay));
    }

    // ---- where the wait happens ------------------------------------------

    [Fact]
    public void AShortWaitIsSpentInTheConsumer()
    {
        // Below the threshold the seconds a restart loses are only seconds, and a held
        // prefetch slot is cheaper than a queue nobody asked for.
        var wait = RetryPolicy.Fixed(2, TimeSpan.FromSeconds(5)).NextWait(1, TimeSpan.Zero);

        Assert.NotNull(wait);
        Assert.False(wait!.InBroker);
    }

    [Fact]
    public void ALongWaitIsSpentInTheBroker()
    {
        // A consumer sleeping on a five-minute backoff loses the whole wait when it
        // restarts: the broker redelivers the unacknowledged message at once, so a
        // five-minute policy delivers in none.
        var wait = RetryPolicy.Fixed(2, TimeSpan.FromMinutes(5)).NextWait(1, TimeSpan.Zero);

        Assert.NotNull(wait);
        Assert.True(wait!.InBroker);
        Assert.Equal(TimeSpan.FromMinutes(5), wait.Delay);
    }

    [Fact]
    public void TheThresholdIsReachedRatherThanPassed()
    {
        Assert.Equal(TimeSpan.FromSeconds(30), RetryPolicy.DefaultBrokerWaitThreshold);

        var atIt = RetryPolicy.Fixed(2, TimeSpan.FromSeconds(30)).NextWait(1, TimeSpan.Zero);
        var belowIt = RetryPolicy.Fixed(2, TimeSpan.FromSeconds(29)).NextWait(1, TimeSpan.Zero);

        Assert.True(atIt!.InBroker);
        Assert.False(belowIt!.InBroker);
    }

    [Fact]
    public void TheThresholdMovesAndZeroTurnsTheRungsOff()
    {
        var policy = RetryPolicy.Fixed(2, TimeSpan.FromSeconds(5))
            .WaitInBrokerFrom(Second);
        Assert.True(policy.NextWait(1, TimeSpan.Zero)!.InBroker);

        // Zero is the way out, for a service that may not declare queues on its
        // broker: nothing is long enough to reach one.
        var off = policy.WaitInBrokerFrom(TimeSpan.Zero);
        Assert.False(off.NextWait(1, TimeSpan.Zero)!.InBroker);
        Assert.Equal(TimeSpan.FromSeconds(5), off.NextWait(1, TimeSpan.Zero)!.Delay);
        Assert.Empty(RetryLadder.For("orders.new", off).Rungs);
    }

    [Fact]
    public void TheRungsAreTheScheduleAboveTheThresholdWithoutRepeats()
    {
        // Finite, because the schedule is. That is what lets the queues be declared
        // with the topology rather than conjured when a consumer first fails.
        var policy = RetryPolicy.Exponential(6, TimeSpan.FromSeconds(10));

        Assert.Equal(new[] { 10.0, 20.0, 40.0, 80.0, 160.0 }, Seconds(policy.Schedule()));
        Assert.Equal(new[] { 40.0, 80.0, 160.0 }, Seconds(policy.BrokerRungs()));

        // A fixed policy that waits a minute three times needs one queue, not three.
        Assert.Equal(
            new[] { 60.0 },
            Seconds(RetryPolicy.Fixed(4, TimeSpan.FromMinutes(1)).BrokerRungs()));
        Assert.Empty(RetryPolicy.None().BrokerRungs());
    }

    // ---- names -----------------------------------------------------------

    [Fact]
    public void WhereAMessageGoesWhenItCannotBeHandled()
    {
        // Convention rather than protocol, which is why it has to be identical
        // everywhere: an operator looking for the dead letters of orders.new should not
        // have to know which language gave up on them.
        Assert.Equal("orders.new.dlq", Naming.DeadLetterQueue("orders.new"));
        Assert.Equal("orders.new.parked", Naming.ParkedQueue("orders.new"));
    }

    [Fact]
    public void ARetryQueueIsNamedForItsDelay()
    {
        // The wait is fixed at declaration by x-message-ttl, so a policy with four
        // different waits needs four queues, and the name is how an operator tells them
        // apart.
        Assert.Equal("orders.new.retry.30s", Naming.RetryQueue("orders.new", TimeSpan.FromSeconds(30)));
        Assert.Equal("orders.new.retry.5m", Naming.RetryQueue("orders.new", TimeSpan.FromMinutes(5)));
        Assert.Equal("orders.new.retry.2h", Naming.RetryQueue("orders.new", TimeSpan.FromHours(2)));
        Assert.Equal("orders.new.retry.90s", Naming.RetryQueue("orders.new", TimeSpan.FromSeconds(90)));
    }

    // ---- the rung declaration, which is the contract ---------------------

    [Fact]
    public void ARungCarriesExactlyThreeArgumentsAndNoOthers()
    {
        var ladder = RetryLadder.For(
            "orders.new", RetryPolicy.Fixed(2, TimeSpan.FromSeconds(30)));
        var rung = Assert.Single(ladder.Rungs);

        // Printed so the table can be read against the Python and Ruby libraries' by
        // eye, which is the only comparison that catches a difference all three of them
        // have written a passing test for.
        _output.WriteLine(ladder.Describe());

        Assert.Equal("orders.new.retry.30s", rung.Queue);
        Assert.Equal(3, rung.Arguments.Count);

        // A long rather than an int, which is what the Java client sends.
        Assert.Equal(30000L, rung.Arguments[RetryLadder.MessageTtlArgument]);

        // The one part of this table the five libraries do not yet agree on. Read from
        // the constant rather than written out, so that when the decision lands the
        // edit is one line and this test moves with it instead of against it.
        Assert.Equal(
            RetryLadder.RetryExchange,
            rung.Arguments[RetryLadder.DeadLetterExchangeArgument]);
        Assert.Equal(
            RetryLadder.RoutingKeyFor("orders.new"),
            rung.Arguments[RetryLadder.DeadLetterRoutingKeyArgument]);

        // The routing key is the source queue whichever exchange is chosen: with the
        // default exchange it is the address, with a named one it is the binding key.
        Assert.Equal("orders.new", rung.Arguments[RetryLadder.DeadLetterRoutingKeyArgument]);

        // Nothing else. An extra argument here is a queue a second service cannot
        // declare, and so a queue it cannot consume.
        Assert.Equal(
            new[]
            {
                RetryLadder.DeadLetterExchangeArgument,
                RetryLadder.DeadLetterRoutingKeyArgument,
                RetryLadder.MessageTtlArgument,
            },
            rung.Arguments.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void APolicyWithSeveralLongWaitsGetsOneRungEach()
    {
        var ladder = RetryLadder.For(
            "orders.new", RetryPolicy.Exponential(6, TimeSpan.FromSeconds(10)));

        _output.WriteLine(ladder.Describe());

        Assert.Equal(
            new[] { "orders.new.retry.40s", "orders.new.retry.80s", "orders.new.retry.160s" },
            ladder.Queues.ToArray());
        Assert.Equal("orders.new.dlq", ladder.DeadLetterQueue);
        Assert.Equal("orders.new.parked", ladder.ParkedQueue);
    }

    [Fact]
    public void ADelayBetweenRungsIsRoundedUpToTheNextOne()
    {
        var ladder = RetryLadder.For(
            "orders.new", RetryPolicy.Exponential(6, TimeSpan.FromSeconds(10)));

        // Up rather than down: waiting slightly too long is harmless, and retrying
        // early defeats the backoff.
        Assert.Equal("orders.new.retry.80s", ladder.RungFor(TimeSpan.FromSeconds(50)));
        // Below the threshold there is no rung, and that is an answer rather than a
        // failure — the consumer waits.
        Assert.Null(ladder.RungFor(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task DeclaresTheRungsBeforeAnythingCanFail()
    {
        var url = "memory://" + Guid.NewGuid().ToString("N");
        using var mq = await AceMqConnection.ConnectAsync(url);
        await mq.DeclareQueueAsync("orders.new");

        using var consumer = await mq.ConsumeAsync<string>(
            "orders.new",
            ConsumerOptions.Defaults().WithRetry(
                RetryPolicy.Fixed(3, TimeSpan.FromSeconds(30))),
            _ => Task.FromResult(Ack.Accept()));

        // Declared when the consumer started, not when it first failed. A rung that
        // does not exist loses the message rather than reporting anything, so the
        // moment to find out is the one where nothing has gone wrong yet.
        Assert.True(await mq.QueueExistsAsync("orders.new.retry.30s"));
    }

    // ---- the whole path --------------------------------------------------

    [Fact]
    public async Task WaitsInTheBrokerRatherThanHoldingTheMessage()
    {
        var url = "memory://" + Guid.NewGuid().ToString("N");
        using var mq = await AceMqConnection.ConnectAsync(url);
        var queue = "orders" + Guid.NewGuid().ToString("N").Substring(0, 8);
        await mq.DeclareQueueAsync(queue);

        var attempts = new ConcurrentQueue<int>();
        using var consumer = await mq.ConsumeAsync<string>(
            queue,
            // A one-second threshold rather than the default thirty, so the test does
            // not take thirty seconds. The arithmetic under test is the same.
            ConsumerOptions.Defaults().WithRetry(
                RetryPolicy.Fixed(3, TimeSpan.FromSeconds(1)).WaitInBrokerFrom(Second)),
            message =>
            {
                attempts.Enqueue(message.Envelope.Attempt);
                throw new InvalidOperationException("still failing");
            });

        await mq.Publisher<string>("", queue).SendAsync("waits in the broker");

        var rung = Naming.RetryQueue(queue, Second);
        await Eventually(
            async () => await mq.MessageCountAsync(rung) == 1,
            "the message to be sitting on the rung");

        // The consumer is holding nothing. That is the whole point: a restart now
        // would not shorten the wait by a millisecond, because the wait belongs to the
        // broker.
        Assert.Equal(0, await mq.MessageCountAsync(queue));

        await Eventually(() => attempts.Count >= 3, "the rung to hand it back twice");
        Assert.Equal(new[] { 1, 2, 3 }, attempts.Take(3).ToArray());
    }

    private static async Task Eventually(Func<Task<bool>> probe, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (await probe()) return;
            await Task.Delay(10);
        }
        throw new TimeoutException($"timed out waiting for {what}");
    }

    private static Task Eventually(Func<bool> probe, string what) =>
        Eventually(() => Task.FromResult(probe()), what);
}
