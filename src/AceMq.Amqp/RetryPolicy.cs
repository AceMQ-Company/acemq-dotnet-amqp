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
/// How long to wait before another attempt, and when to stop.
/// </summary>
/// <remarks>
/// <para>
/// A fixed delay is fine for a dependency that is briefly unavailable. It is the
/// wrong shape for one that is overloaded: every consumer retries in step, and the
/// retries arrive as a burst exactly when the dependency can least take one. That is
/// what exponential backoff and jitter are for, and why jitter is on by default here.
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

    private RetryPolicy(
        int maxAttempts, TimeSpan initialDelay, double multiplier, TimeSpan maxDelay,
        TimeSpan maxMessageAge, double jitterFactor)
    {
        MaxAttempts = maxAttempts;
        InitialDelay = initialDelay;
        Multiplier = multiplier;
        MaxDelay = maxDelay;
        MaxMessageAge = maxMessageAge;
        JitterFactor = jitterFactor;
    }

    /// <summary>One attempt, no retry.</summary>
    public static RetryPolicy None() =>
        new RetryPolicy(1, TimeSpan.Zero, 1, TimeSpan.Zero, TimeSpan.MaxValue, 0);

    /// <summary>The same delay every time.</summary>
    public static RetryPolicy Fixed(int maxAttempts, TimeSpan delay) =>
        new RetryPolicy(maxAttempts, delay, 1, delay, TimeSpan.MaxValue, 0.2);

    /// <summary>Doubling delays, capped.</summary>
    public static RetryPolicy Exponential(int maxAttempts, TimeSpan initialDelay, TimeSpan maxDelay) =>
        Exponential(maxAttempts, initialDelay, 2, maxDelay);

    public static RetryPolicy Exponential(
        int maxAttempts, TimeSpan initialDelay, double multiplier, TimeSpan maxDelay)
    {
        if (maxAttempts < 1) throw new ArgumentException("must be at least 1", nameof(maxAttempts));
        if (multiplier < 1) throw new ArgumentException("must be at least 1", nameof(multiplier));
        return new RetryPolicy(maxAttempts, initialDelay, multiplier, maxDelay, TimeSpan.MaxValue, 0.2);
    }

    /// <summary>Stops retrying a message older than this, however few attempts it has had.</summary>
    public RetryPolicy GiveUpAfter(TimeSpan maxMessageAge) =>
        new RetryPolicy(MaxAttempts, InitialDelay, Multiplier, MaxDelay, maxMessageAge, JitterFactor);

    /// <summary>
    /// Spreads retries out, as a fraction of the delay.
    /// </summary>
    /// <remarks>
    /// Without jitter, every consumer that failed at the same moment retries at the
    /// same moment. The dependency that was struggling then gets the whole herd at
    /// once, which is how a brief problem becomes a sustained one.
    /// </remarks>
    public RetryPolicy WithJitter(double jitterFactor)
    {
        if (jitterFactor < 0 || jitterFactor > 1)
        {
            throw new ArgumentException("must be between 0 and 1", nameof(jitterFactor));
        }
        return new RetryPolicy(MaxAttempts, InitialDelay, Multiplier, MaxDelay, MaxMessageAge, jitterFactor);
    }

    public int MaxAttempts { get; }
    public TimeSpan InitialDelay { get; }
    public double Multiplier { get; }
    public TimeSpan MaxDelay { get; }
    public TimeSpan MaxMessageAge { get; }
    public double JitterFactor { get; }

    /// <summary>
    /// How long to wait before <paramref name="attempt"/> + 1, or null to give up.
    /// </summary>
    public TimeSpan? NextDelay(int attempt, TimeSpan messageAge)
    {
        if (attempt >= MaxAttempts) return null;
        if (MaxMessageAge != TimeSpan.MaxValue && messageAge >= MaxMessageAge) return null;

        var delay = InitialDelay;
        for (var i = 1; i < attempt; i++)
        {
            delay = TimeSpan.FromTicks((long)(delay.Ticks * Multiplier));
            if (delay > MaxDelay) { delay = MaxDelay; break; }
        }
        if (delay > MaxDelay) delay = MaxDelay;

        if (JitterFactor > 0 && delay > TimeSpan.Zero)
        {
            double factor;
            lock (Jitter) factor = 1 + ((Jitter.NextDouble() * 2 - 1) * JitterFactor);
            delay = TimeSpan.FromTicks((long)(delay.Ticks * factor));
        }
        return delay < TimeSpan.Zero ? TimeSpan.Zero : delay;
    }

    /// <summary>The delays this policy produces, without jitter, for inspection.</summary>
    public IReadOnlyList<TimeSpan> Schedule()
    {
        var delays = new List<TimeSpan>();
        var delay = InitialDelay;
        for (var attempt = 1; attempt < MaxAttempts; attempt++)
        {
            delays.Add(delay > MaxDelay ? MaxDelay : delay);
            delay = TimeSpan.FromTicks((long)(delay.Ticks * Multiplier));
        }
        return delays;
    }

    public override string ToString() =>
        $"RetryPolicy[attempts={MaxAttempts}, initial={InitialDelay}, x{Multiplier}, " +
        $"max={MaxDelay}, jitter={JitterFactor}]";
}
