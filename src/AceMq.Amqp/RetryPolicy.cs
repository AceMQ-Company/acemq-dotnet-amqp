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

using System;
using System.Collections.Generic;

namespace AceMq.Amqp;

/// <summary>
/// How long before the next attempt, and where the message spends the wait.
/// </summary>
/// <remarks>
/// Two answers rather than one because they cannot be worked out separately: jitter
/// applies only to a wait spent in the consumer, so a caller given a delay alone
/// could not tell whether it had already been moved — and a jittered delay does not
/// name a rung queue.
/// </remarks>
public sealed class Wait
{
    internal Wait(TimeSpan delay, bool inBroker)
    {
        Delay = delay;
        InBroker = inBroker;
    }

    /// <summary>How long the message waits.</summary>
    public TimeSpan Delay { get; }

    /// <summary>Whether it waits on a rung queue rather than in this process.</summary>
    public bool InBroker { get; }

    public override string ToString() =>
        InBroker ? $"{Delay} in the broker" : $"{Delay} here";
}

/// <summary>
/// How many times to try, how long to wait, and where.
/// </summary>
/// <remarks>
/// <para>
/// The arithmetic here is part of the cross-language contract rather than a local
/// choice: the same policy must produce the same delays in Java, Go, .NET, Python and
/// Ruby, because the same message can be retried by a consumer written in any of
/// them. Jitter is the one exception — it is random by definition — so
/// <see cref="Schedule"/> exposes the delays <em>without</em> it, which is what to
/// read when deciding whether a policy is the one you meant.
/// </para>
/// <para>
/// A fixed delay is fine for a dependency that is briefly unavailable. It is the
/// wrong shape for one that is overloaded: every consumer retries in step, and the
/// retries arrive as a burst exactly when the dependency can least take one. That is
/// what exponential backoff and jitter are for, and why jitter is on by default in
/// <see cref="Exponential(int, TimeSpan, TimeSpan)"/>. The jitter moves a delay
/// <em>both</em> ways, which matters more than it looks: one-sided jitter only ever
/// delays, so it turns a thundering herd into a slower thundering herd rather than
/// dispersing it.
/// </para>
/// <para>
/// Waits below <see cref="BrokerWaitThreshold"/> are spent in the consumer, which
/// holds the delivery and one prefetch slot for the duration. Waits at or above it
/// are spent in a <c>{queue}.retry.{delay}</c> queue whose <c>x-message-ttl</c> is
/// the wait and whose dead-letter target is the source queue, so the broker returns
/// the message when the time is up and a consumer restart cannot shorten it. See
/// <see cref="RetryLadder"/> for why the split is at a threshold rather than being
/// one rule or the other.
/// </para>
/// <para>
/// <see cref="GiveUpAfter"/> bounds by the message's age rather than by attempts.
/// Attempts alone cannot express "this is too old to be worth doing" — a message
/// retried five times over a weekend has usually stopped being useful, however few
/// attempts that took.
/// </para>
/// </remarks>
public sealed class RetryPolicy
{
    private static readonly Random Jitter = new Random();

    /// <summary>
    /// Waits this long or longer are spent in the broker rather than in the consumer.
    /// </summary>
    /// <remarks>
    /// Thirty seconds is roughly where the two costs cross: below it, the seconds a
    /// restart loses are only seconds and a held prefetch slot is cheap; above it, a
    /// consumer that restarts mid-wait loses the wait entirely, because the broker
    /// redelivers the unacknowledged message at once and a five-minute backoff
    /// becomes instant.
    /// </remarks>
    public static readonly TimeSpan DefaultBrokerWaitThreshold = TimeSpan.FromSeconds(30);

    private RetryPolicy(
        int maxAttempts, TimeSpan initialDelay, double multiplier, TimeSpan maxDelay,
        TimeSpan maxMessageAge, double jitterFactor, TimeSpan brokerWaitThreshold)
    {
        MaxAttempts = maxAttempts;
        InitialDelay = initialDelay;
        Multiplier = multiplier;
        MaxDelay = maxDelay;
        MaxMessageAge = maxMessageAge;
        JitterFactor = jitterFactor;
        BrokerWaitThreshold = brokerWaitThreshold;
    }

    /// <summary>One attempt, no retry.</summary>
    public static RetryPolicy None() =>
        new RetryPolicy(
            1, TimeSpan.Zero, 1, TimeSpan.Zero, TimeSpan.Zero, 0, DefaultBrokerWaitThreshold);

    /// <summary>The same delay every time, with no jitter.</summary>
    public static RetryPolicy Fixed(int maxAttempts, TimeSpan delay)
    {
        if (maxAttempts < 1) throw new ArgumentException("must be at least 1", nameof(maxAttempts));
        return new RetryPolicy(
            maxAttempts, delay, 1, TimeSpan.Zero, TimeSpan.Zero, 0, DefaultBrokerWaitThreshold);
    }

    /// <summary>Doubling delays with 20% jitter, and no ceiling.</summary>
    public static RetryPolicy Exponential(int maxAttempts, TimeSpan initialDelay) =>
        Exponential(maxAttempts, initialDelay, 2, TimeSpan.Zero);

    /// <summary>Doubling delays with 20% jitter, capped at <paramref name="maxDelay"/>.</summary>
    public static RetryPolicy Exponential(int maxAttempts, TimeSpan initialDelay, TimeSpan maxDelay) =>
        Exponential(maxAttempts, initialDelay, 2, maxDelay);

    public static RetryPolicy Exponential(
        int maxAttempts, TimeSpan initialDelay, double multiplier, TimeSpan maxDelay)
    {
        if (maxAttempts < 1) throw new ArgumentException("must be at least 1", nameof(maxAttempts));
        if (multiplier < 1) throw new ArgumentException("must be at least 1", nameof(multiplier));
        return new RetryPolicy(
            maxAttempts, initialDelay, multiplier, maxDelay, TimeSpan.Zero, 0.2,
            DefaultBrokerWaitThreshold);
    }

    /// <summary>
    /// Stops retrying a message older than this, however few attempts it has had.
    /// </summary>
    /// <remarks>
    /// The honest limit when a queue has been paused: attempts say nothing about how
    /// long a message has been waiting, and a message four days old is usually one
    /// nobody wants delivered now. <see cref="TimeSpan.Zero"/> means never.
    /// </remarks>
    public RetryPolicy GiveUpAfter(TimeSpan maxMessageAge) =>
        new RetryPolicy(
            MaxAttempts, InitialDelay, Multiplier, MaxDelay, maxMessageAge, JitterFactor,
            BrokerWaitThreshold);

    /// <summary>Spreads retries out, as a fraction of the delay, between 0 and 1.</summary>
    public RetryPolicy WithJitter(double jitterFactor)
    {
        if (jitterFactor < 0 || jitterFactor > 1)
        {
            throw new ArgumentException("must be between 0 and 1", nameof(jitterFactor));
        }
        return new RetryPolicy(
            MaxAttempts, InitialDelay, Multiplier, MaxDelay, MaxMessageAge, jitterFactor,
            BrokerWaitThreshold);
    }

    /// <summary>
    /// Moves the line between waiting here and waiting in the broker.
    /// </summary>
    /// <remarks>
    /// <see cref="TimeSpan.Zero"/> is the way out: with no threshold nothing is long
    /// enough to reach the broker, so every wait is spent in the consumer and no rung
    /// queue is needed. That is the right setting for a service whose broker it may
    /// not declare queues on, and the wrong one for a policy with delays measured in
    /// minutes.
    /// </remarks>
    public RetryPolicy WaitInBrokerFrom(TimeSpan threshold)
    {
        if (threshold < TimeSpan.Zero) throw new ArgumentException("cannot be negative", nameof(threshold));
        return new RetryPolicy(
            MaxAttempts, InitialDelay, Multiplier, MaxDelay, MaxMessageAge, JitterFactor, threshold);
    }

    /// <summary>Total deliveries including the first. 1 means no retry.</summary>
    public int MaxAttempts { get; }

    /// <summary>The wait before the second attempt.</summary>
    public TimeSpan InitialDelay { get; }

    /// <summary>What the delay is multiplied by each time.</summary>
    public double Multiplier { get; }

    /// <summary>The ceiling, or <see cref="TimeSpan.Zero"/> for none.</summary>
    public TimeSpan MaxDelay { get; }

    /// <summary>Give up on anything older, or <see cref="TimeSpan.Zero"/> for never.</summary>
    public TimeSpan MaxMessageAge { get; }

    /// <summary>How far a delay may move either side, 0 to 1.</summary>
    public double JitterFactor { get; }

    /// <summary>
    /// Waits this long or longer are spent in the broker, or
    /// <see cref="TimeSpan.Zero"/> to spend every wait in the consumer.
    /// </summary>
    public TimeSpan BrokerWaitThreshold { get; }

    /// <summary>
    /// How long to wait before <paramref name="attempt"/> + 1, or null to give up.
    /// </summary>
    /// <remarks>
    /// The delay a consumer-side wait would use, jitter included; a wait that belongs
    /// in the broker comes back unjittered, because a rung queue's time-to-live is
    /// fixed at declaration and a moved delay names no queue.
    /// </remarks>
    public TimeSpan? NextDelay(int attempt, TimeSpan messageAge)
    {
        var wait = NextWait(attempt, messageAge);
        return wait == null ? (TimeSpan?)null : wait.Delay;
    }

    /// <summary>
    /// How long to wait before <paramref name="attempt"/> + 1, optionally without
    /// jitter, or null to give up.
    /// </summary>
    /// <remarks>
    /// Ask for it unjittered when the number has to line up with something: a retry
    /// that waits in the broker waits in a queue named after its delay, and a jittered
    /// number names no queue.
    /// </remarks>
    public TimeSpan? NextDelay(int attempt, TimeSpan messageAge, bool jitter)
    {
        if (!HasAnotherAttempt(attempt, messageAge)) return null;
        var delay = Unjittered(attempt);
        return jitter ? Jittered(delay) : delay;
    }

    /// <summary>
    /// The whole answer: how long to wait, and where, or null to give up.
    /// </summary>
    public Wait? NextWait(int attempt, TimeSpan messageAge)
    {
        if (!HasAnotherAttempt(attempt, messageAge)) return null;

        var delay = Unjittered(attempt);

        if (WaitsInBroker(delay))
        {
            // Deliberately not jittered. A rung queue's time-to-live is fixed when it
            // is declared, so a moved delay would name a queue that does not exist;
            // and the spread jitter buys is already there, because each message's
            // time-to-live starts when it arrives rather than when the batch failed.
            return new Wait(delay, inBroker: true);
        }

        return new Wait(Jittered(delay), inBroker: false);
    }

    /// <summary>Whether a wait of this length belongs on a rung queue.</summary>
    /// <param name="delay">An unjittered delay, as <see cref="Schedule"/> reports them.</param>
    public bool WaitsInBroker(TimeSpan delay) =>
        BrokerWaitThreshold > TimeSpan.Zero
        && delay > TimeSpan.Zero
        && delay >= BrokerWaitThreshold;

    /// <summary>
    /// The same delay, moved either side by the jitter factor, never below zero.
    /// </summary>
    /// <remarks>
    /// Public because the retry engine applies it separately from working the delay
    /// out: a wait that happens in the broker needs no jitter at all.
    /// </remarks>
    public TimeSpan Jittered(TimeSpan delay)
    {
        if (JitterFactor <= 0 || delay <= TimeSpan.Zero) return delay;

        double factor;
        lock (Jitter) factor = 1 + ((Jitter.NextDouble() * 2 - 1) * JitterFactor);
        var moved = Multiply(delay, factor);
        return moved < TimeSpan.Zero ? TimeSpan.Zero : moved;
    }

    /// <summary>
    /// The delays this policy would use, without jitter.
    /// </summary>
    /// <remarks>
    /// What to look at when deciding whether a policy is the one you meant:
    /// <c>Exponential(5, 1s, 1m).Schedule()</c> is <c>[1s, 2s, 4s, 8s]</c>, and four
    /// numbers are easier to argue with than three parameters.
    /// </remarks>
    public IReadOnlyList<TimeSpan> Schedule()
    {
        var delays = new List<TimeSpan>();
        var delay = InitialDelay;
        for (var attempt = 1; attempt < MaxAttempts; attempt++)
        {
            delays.Add(Capped(delay) ? MaxDelay : delay);
            delay = Multiply(delay, Multiplier);
        }
        return delays;
    }

    /// <summary>
    /// The delays this policy needs a rung queue for, longest last.
    /// </summary>
    /// <remarks>
    /// Exactly the entries of <see cref="Schedule"/> that are at or above the
    /// threshold, with repeats removed — a fixed policy that waits a minute three
    /// times needs one queue, not three. It is a finite list because the schedule is,
    /// which is what makes the queues declarable up front rather than conjured by a
    /// consumer at the moment it first fails.
    /// </remarks>
    public IReadOnlyList<TimeSpan> BrokerRungs()
    {
        var rungs = new List<TimeSpan>();
        foreach (var delay in Schedule())
        {
            if (WaitsInBroker(delay) && !rungs.Contains(delay)) rungs.Add(delay);
        }
        return rungs;
    }

    private bool HasAnotherAttempt(int attempt, TimeSpan messageAge)
    {
        if (attempt >= MaxAttempts) return false;
        // Zero means never, as it does in the Python and Ruby libraries.
        // TimeSpan.MaxValue means the same thing and is accepted because an earlier
        // version of this class used it as the sentinel.
        if (MaxMessageAge > TimeSpan.Zero
            && MaxMessageAge != TimeSpan.MaxValue
            && messageAge >= MaxMessageAge)
        {
            return false;
        }
        return true;
    }

    /// <summary>
    /// The delay after <paramref name="attempt"/>, before jitter and before the
    /// threshold is considered.
    /// </summary>
    /// <remarks>
    /// Capped inside the loop as well as after it. Without the first, a policy with a
    /// large multiplier and many attempts runs the delay towards infinity before the
    /// ceiling is ever applied, and arrives at a number that overflowed on the way.
    /// </remarks>
    private TimeSpan Unjittered(int attempt)
    {
        var delay = InitialDelay;
        for (var i = 1; i < attempt; i++)
        {
            delay = Multiply(delay, Multiplier);
            if (Capped(delay)) { delay = MaxDelay; break; }
        }
        return Capped(delay) ? MaxDelay : delay;
    }

    /// <summary>Whether a ceiling exists and this delay is over it.</summary>
    private bool Capped(TimeSpan delay) => MaxDelay > TimeSpan.Zero && delay > MaxDelay;

    /// <summary>
    /// A duration scaled by a factor, saturating rather than wrapping.
    /// </summary>
    /// <remarks>
    /// <c>TimeSpan.FromTicks((long)(ticks * factor))</c> silently produces a negative
    /// duration once the product passes <c>long.MaxValue</c>, which is how an
    /// uncapped policy with many attempts ends up retrying immediately rather than
    /// eventually.
    /// </remarks>
    private static TimeSpan Multiply(TimeSpan delay, double factor)
    {
        var ticks = delay.Ticks * factor;
        if (ticks >= long.MaxValue) return TimeSpan.MaxValue;
        if (ticks <= long.MinValue) return TimeSpan.MinValue;
        return TimeSpan.FromTicks((long)ticks);
    }

    public override string ToString() =>
        $"RetryPolicy[attempts={MaxAttempts}, initial={InitialDelay}, x{Multiplier}, " +
        $"max={MaxDelay}, jitter={JitterFactor}, brokerFrom={BrokerWaitThreshold}]";
}
