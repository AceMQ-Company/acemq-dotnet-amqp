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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>
/// The queues a long retry waits in, so that the wait is the broker's and not this
/// process's.
/// </summary>
/// <remarks>
/// <para>
/// A consumer that sleeps for a five-minute backoff is holding an unacknowledged
/// message. Restart it — a deploy, a crash, an autoscaler — and the broker redelivers
/// at once, so a five-minute policy becomes instant. That is a correctness bug rather
/// than a throughput one, and it is why delays past a threshold are handed to the
/// broker instead: the message is published into a rung queue whose
/// <c>x-message-ttl</c> is the delay and whose dead-letter target is the queue it came
/// from, and the broker returns it when the time is up. Nothing consumes a rung; the
/// time-to-live is the only thing that ever takes a message out of one.
/// </para>
/// <para>
/// For <c>orders.new</c> with delays of 1s, 30s and 60s and the default threshold:
/// </para>
/// <code>
/// orders.new.retry.30s   ttl 30000ms  -> orders.new
/// orders.new.retry.1m    ttl 60000ms  -> orders.new
/// </code>
/// <para>
/// The one-second delay gets no queue. Below the threshold the wait happens in the
/// consumer, where a second lost to a restart is a second, and the broker is spared a
/// queue per rung of a schedule that mostly runs in the time it takes to notice. Java
/// gave every delay a rung and is gaining a threshold of its own; thirty seconds is
/// what this library, Go, Python and Ruby all use.
/// </para>
/// <para>
/// The rungs are exactly <see cref="RetryPolicy.BrokerRungs"/>, which is a finite list
/// known before anything is published — which is what makes them declarable up front
/// rather than discovered one failure at a time.
/// </para>
/// </remarks>
public sealed class RetryLadder
{
    /// <summary>
    /// The exchange a rung dead-letters through on its way back to the source queue.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <strong>This constant, and <see cref="RoutingKeyFor"/> beside it, are the whole
    /// of the choice.</strong> The five libraries have settled on Java's shape: a named
    /// <c>acemq.retry</c> direct exchange with each source queue bound to it, rather
    /// than the default exchange Python and Ruby were using, which routes by queue name
    /// and so needs no exchange and no binding at all.
    /// </para>
    /// <para>
    /// Setting this to the empty string switches the whole library to the default
    /// exchange: <see cref="ArgumentsFor"/> writes the empty exchange,
    /// <see cref="IsNamedExchange"/> becomes false, and <see cref="DeclareAsync"/>
    /// stops declaring an exchange and stops binding. Nothing else has to change,
    /// which is the point of it being one constant.
    /// </para>
    /// </remarks>
    public const string RetryExchange = "acemq.retry";

    /// <summary>The type the retry exchange is declared with, when it is named.</summary>
    /// <remarks>
    /// Direct, because the routing key is a queue name and the match has to be exact.
    /// A topic exchange here would deliver <c>orders.new</c>'s expired retries to
    /// anything bound with <c>orders.#</c>.
    /// </remarks>
    public const string RetryExchangeType = "direct";

    /// <summary>How long a message may sit on a rung before the broker expires it.</summary>
    /// <remarks>
    /// On the queue, never on the message. RabbitMQ expires messages only from the
    /// head of a queue, so one queue carrying per-message expirations lets a long wait
    /// at the front hold back every shorter one behind it, and the delays that come
    /// out bear no relation to the ones that went in. A queue per distinct delay is
    /// more queues and is the only arrangement that actually delivers the schedule it
    /// was given.
    /// </remarks>
    public const string MessageTtlArgument = "x-message-ttl";

    /// <summary>Where the broker sends a message this queue expires.</summary>
    public const string DeadLetterExchangeArgument = "x-dead-letter-exchange";

    /// <summary>What routing key it is sent under.</summary>
    public const string DeadLetterRoutingKeyArgument = "x-dead-letter-routing-key";

    /// <summary>Whether the rungs dead-letter through a named exchange or the default one.</summary>
    public static bool IsNamedExchange => RetryExchange.Length > 0;

    /// <summary>
    /// The routing key an expiring rung sends the message back under.
    /// </summary>
    /// <remarks>
    /// The source queue name either way. With the default exchange that <em>is</em>
    /// the address; with a named exchange it is the key the source queue is bound
    /// under. Derived here, beside <see cref="RetryExchange"/>, so the two cannot be
    /// changed apart.
    /// </remarks>
    public static string RoutingKeyFor(string sourceQueue) => sourceQueue;

    /// <summary>
    /// The arguments a rung queue has to carry, and nothing else.
    /// </summary>
    /// <remarks>
    /// These three arguments are the cross-language contract for a rung. Two services
    /// consuming the same queue declare the same rung by name, so if one of them
    /// declares it with different arguments the second gets PRECONDITION_FAILED and
    /// cannot consume at all. That is why the table is pinned by a test rather than
    /// left to be read off this method: changing it has to be deliberate.
    /// </remarks>
    /// <param name="sourceQueue">The queue an expired message goes back to.</param>
    /// <param name="delay">How long a message waits on the rung.</param>
    public static IReadOnlyDictionary<string, object> ArgumentsFor(string sourceQueue, TimeSpan delay)
    {
        if (string.IsNullOrEmpty(sourceQueue))
        {
            throw new ArgumentException("a queue name is needed", nameof(sourceQueue));
        }

        // A long rather than an int, which is what the Java client sends. RabbitMQ
        // compares queue arguments for equivalence when a second declaration arrives,
        // and matching the type the oldest library uses removes one way for that
        // comparison to fail.
        return new Dictionary<string, object>
        {
            [MessageTtlArgument] = (long)delay.TotalMilliseconds,
            [DeadLetterExchangeArgument] = RetryExchange,
            [DeadLetterRoutingKeyArgument] = RoutingKeyFor(sourceQueue),
        };
    }

    private RetryLadder(
        string source, TimeSpan threshold, IReadOnlyList<Rung> rungs,
        string deadLetterQueue, string parkedQueue)
    {
        Source = source;
        Threshold = threshold;
        Rungs = rungs;
        DeadLetterQueue = deadLetterQueue;
        ParkedQueue = parkedQueue;
    }

    /// <summary>
    /// Works out the ladder a policy needs, touching no broker.
    /// </summary>
    /// <param name="sourceQueue">The queue being consumed.</param>
    /// <param name="policy">Whose schedule the rungs are.</param>
    public static RetryLadder For(string sourceQueue, RetryPolicy policy)
    {
        if (string.IsNullOrEmpty(sourceQueue))
        {
            throw new ArgumentException("a queue name is needed", nameof(sourceQueue));
        }
        if (policy == null) throw new ArgumentNullException(nameof(policy));

        // Keyed by name rather than by delay. Two delays inside the same second render
        // to the same name, and a second queue by the same name with a different
        // time-to-live is not a second rung — it is a PRECONDITION_FAILED at
        // declaration time.
        var byName = new Dictionary<string, Rung>(StringComparer.Ordinal);
        var rungs = new List<Rung>();
        foreach (var delay in policy.BrokerRungs())
        {
            var name = Naming.RetryQueue(sourceQueue, delay);
            if (byName.ContainsKey(name)) continue;
            var rung = new Rung(delay, name, ArgumentsFor(sourceQueue, delay));
            byName[name] = rung;
            rungs.Add(rung);
        }

        return new RetryLadder(
            sourceQueue, policy.BrokerWaitThreshold, rungs,
            Naming.DeadLetterQueue(sourceQueue), Naming.ParkedQueue(sourceQueue));
    }

    /// <summary>The queue being consumed.</summary>
    public string Source { get; }

    /// <summary>Delays at or above this got a rung.</summary>
    public TimeSpan Threshold { get; }

    /// <summary>The rungs, in the order the schedule reaches them.</summary>
    public IReadOnlyList<Rung> Rungs { get; }

    /// <summary>Where a message goes when every attempt has been used.</summary>
    public string DeadLetterQueue { get; }

    /// <summary>Where a message goes when a person has to look at it.</summary>
    public string ParkedQueue { get; }

    /// <summary>
    /// Whether this policy needs no rungs at all.
    /// </summary>
    /// <remarks>
    /// The common case: a schedule that runs in seconds waits in the consumer and
    /// costs the broker nothing.
    /// </remarks>
    public bool IsEmpty => Rungs.Count == 0;

    /// <summary>The rung queue names, in schedule order.</summary>
    public IReadOnlyList<string> Queues
    {
        get
        {
            var names = new List<string>(Rungs.Count);
            foreach (var rung in Rungs) names.Add(rung.Queue);
            return names;
        }
    }

    /// <summary>
    /// The rung a delay belongs in, or null when the consumer should wait.
    /// </summary>
    /// <remarks>
    /// Null is the answer for anything below the threshold, and it is an answer a
    /// caller acts on rather than a failure — waiting here is the other half of the
    /// design, not a fallback.
    /// <para>
    /// A delay that is not exactly a rung is rounded up to the next one, which cannot
    /// happen for a delay this ladder's own policy produced but can for one a caller
    /// worked out some other way. Up rather than down because waiting slightly too
    /// long is harmless and retrying early defeats the backoff.
    /// </para>
    /// </remarks>
    public string? RungFor(TimeSpan delay)
    {
        if (IsEmpty || delay < Threshold) return null;

        Rung? best = null;
        Rung? longest = null;
        foreach (var rung in Rungs)
        {
            if (rung.Delay >= delay && (best == null || rung.Delay < best.Delay)) best = rung;
            if (longest == null || rung.Delay > longest.Delay) longest = rung;
        }
        return (best ?? longest)!.Queue;
    }

    /// <summary>
    /// Declares the rungs, and the queues a message ends up in when it runs out of
    /// them.
    /// </summary>
    /// <remarks>
    /// Called before anything is subscribed, and not on the failure path. A rung that
    /// does not exist loses the message rather than reporting anything — an
    /// unroutable publish is dropped — so the moment to find out is the one where
    /// nothing has failed yet. Declaring is idempotent, and a duplicate declaration is
    /// a great deal cheaper than a lost message.
    /// </remarks>
    public async Task DeclareAsync(ITransportConnection connection, CancellationToken cancellationToken)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        if (IsEmpty) return;

        if (IsNamedExchange)
        {
            await connection
                .DeclareExchangeAsync(RetryExchange, RetryExchangeType, true, cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (var rung in Rungs)
        {
            await connection
                .DeclareQueueAsync(rung.Queue, QueueType.Classic, true, rung.Arguments, cancellationToken)
                .ConfigureAwait(false);
        }

        if (IsNamedExchange)
        {
            // One binding brings every expired message back to the queue it came from.
            // The default exchange needs none: every queue is bound to it by its own
            // name from the moment it exists.
            await connection
                .BindQueueAsync(Source, RetryExchange, RoutingKeyFor(Source), cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The ladder as text: one line per rung, with the arguments it is declared with.
    /// </summary>
    /// <remarks>
    /// Printed by the tests so the table can be compared against the Python and Ruby
    /// libraries' by eye, which is the only comparison that catches a difference the
    /// five of them have all written a passing test for.
    /// </remarks>
    public string Describe()
    {
        if (IsEmpty) return $"no retry rungs for {Source}";

        var text = new StringBuilder();
        text.Append("retry rungs for ").Append(Source)
            .Append(" (threshold ").Append(Naming.Describe(Threshold)).Append("):\n");
        foreach (var rung in Rungs)
        {
            text.Append("  ").Append(rung.Queue).Append('\n');
            foreach (var pair in rung.Arguments)
            {
                text.Append("      ").Append(pair.Key).Append(" = ")
                    .Append(Render(pair.Value)).Append('\n');
            }
        }
        if (IsNamedExchange)
        {
            text.Append("  binding: ").Append(Source).Append(" <- ").Append(RetryExchange)
                .Append(" (").Append(RoutingKeyFor(Source)).Append(")\n");
        }
        return text.ToString();
    }

    private static string Render(object value) =>
        value is string s ? "\"" + s + "\"" : Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!;

    public override string ToString() =>
        IsEmpty
            ? $"RetryLadder[{Source}, no rungs]"
            : $"RetryLadder[{Source}, {string.Join(", ", Queues)}]";

    /// <summary>
    /// One step of the ladder: a delay, the queue that expresses it, and the arguments
    /// that queue has to be declared with for it to mean anything.
    /// </summary>
    public sealed class Rung
    {
        internal Rung(TimeSpan delay, string queue, IReadOnlyDictionary<string, object> arguments)
        {
            Delay = delay;
            Queue = queue;
            Arguments = arguments;
        }

        /// <summary>How long a message waits here.</summary>
        public TimeSpan Delay { get; }

        /// <summary>The queue it waits in.</summary>
        public string Queue { get; }

        /// <summary>Exactly the three arguments from <see cref="ArgumentsFor"/>.</summary>
        public IReadOnlyDictionary<string, object> Arguments { get; }

        public override string ToString() =>
            $"{Queue} (ttl {(long)Delay.TotalMilliseconds}ms)";
    }
}
