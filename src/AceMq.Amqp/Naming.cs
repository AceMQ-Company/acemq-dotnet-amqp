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
using System.Globalization;

namespace AceMq.Amqp;

/// <summary>
/// Where a message goes when it cannot be handled.
/// </summary>
/// <remarks>
/// These names are convention rather than protocol, which is exactly why they have to
/// be identical everywhere: an operator looking for the dead letters of
/// <c>orders.new</c> should find them in <c>orders.new.dlq</c> whether the consumer
/// that gave up was written in Java, Go, .NET, Python or Ruby.
/// </remarks>
public static class Naming
{
    /// <summary>Where a message goes when every attempt has been used.</summary>
    public const string DeadLetterSuffix = ".dlq";

    /// <summary>Where a message goes when a person has to look at it.</summary>
    public const string ParkedSuffix = ".parked";

    /// <summary>Infix of a retry rung queue, between the source name and the delay.</summary>
    public const string RetryInfix = ".retry.";

    /// <summary><c>orders.new</c> becomes <c>orders.new.dlq</c>.</summary>
    public static string DeadLetterQueue(string queue) =>
        Require(queue) + DeadLetterSuffix;

    /// <summary><c>orders.new</c> becomes <c>orders.new.parked</c>.</summary>
    public static string ParkedQueue(string queue) =>
        Require(queue) + ParkedSuffix;

    /// <summary>
    /// <c>orders.new</c> and thirty seconds become <c>orders.new.retry.30s</c>.
    /// </summary>
    /// <remarks>
    /// The delay is in the name because a delay queue is per-delay: its
    /// <c>x-message-ttl</c> is fixed at declaration, so a policy with four different
    /// waits needs four queues, and an operator should be able to tell which is which
    /// without reading their arguments.
    /// </remarks>
    public static string RetryQueue(string queue, TimeSpan delay) =>
        Require(queue) + RetryInfix + Describe(delay);

    /// <summary>
    /// A duration as the shortest thing that reads as one: <c>30s</c>, <c>5m</c>,
    /// <c>2h</c>.
    /// </summary>
    /// <remarks>
    /// Queue names end up in dashboards and alerts, so <c>orders.new.retry.5s</c> is
    /// worth the small amount of code it takes to avoid <c>orders.new.retry.PT5S</c>.
    /// <para>
    /// Sub-second delays render as milliseconds, which is what the Java library does;
    /// the Python and Ruby libraries round them to <c>0s</c> instead. The difference
    /// is unreachable in practice — a rung only exists for a delay at or above the
    /// broker-wait threshold, and no sane threshold is under a second — but it is a
    /// difference, and it is written down here rather than discovered.
    /// </para>
    /// </remarks>
    public static string Describe(TimeSpan delay)
    {
        var millis = (long)delay.TotalMilliseconds;
        if (millis <= 0) return "0s";
        if (millis >= 3600000 && millis % 3600000 == 0) return Number(millis / 3600000) + "h";
        if (millis >= 60000 && millis % 60000 == 0) return Number(millis / 60000) + "m";
        if (millis >= 1000 && millis % 1000 == 0) return Number(millis / 1000) + "s";
        return Number(millis) + "ms";
    }

    private static string Number(long value) =>
        value.ToString(CultureInfo.InvariantCulture);

    private static string Require(string queue)
    {
        if (string.IsNullOrEmpty(queue))
        {
            throw new ArgumentException("a queue name is needed", nameof(queue));
        }
        return queue;
    }
}
