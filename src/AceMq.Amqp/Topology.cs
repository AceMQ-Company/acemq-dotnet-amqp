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
using System.Linq;
using System.Text;

namespace AceMq.Amqp;

/// <summary>
/// A description of the exchanges, queues and bindings a service needs.
/// </summary>
/// <remarks>
/// <para>
/// Declaring topology as data rather than as a sequence of calls buys two things.
/// It can be inspected before it is applied — see <see cref="TopologyPlan"/> — and a
/// dead-letter queue can be declared as one unit with the queue that dead-letters
/// into it. That second point is not cosmetic: <see cref="Ack.DeadLetter"/> nacks
/// without requeueing, and a queue with no dead-letter exchange configured discards
/// the message. Wiring the two by hand and forgetting one line loses messages
/// silently, which is the failure this library exists to avoid.
/// </para>
/// </remarks>
public sealed class Topology
{
    private Topology(
        IReadOnlyList<ExchangeSpec> exchanges,
        IReadOnlyList<QueueSpec> queues,
        IReadOnlyList<BindingSpec> bindings)
    {
        Exchanges = exchanges;
        Queues = queues;
        Bindings = bindings;
    }

    public IReadOnlyList<ExchangeSpec> Exchanges { get; }
    public IReadOnlyList<QueueSpec> Queues { get; }
    public IReadOnlyList<BindingSpec> Bindings { get; }

    /// <summary>Starts describing a topology.</summary>
    public static Builder Define() => new Builder();

    public override string ToString() =>
        $"Topology[{Exchanges.Count} exchange(s), {Queues.Count} queue(s), {Bindings.Count} binding(s)]";

    /// <summary>An exchange to declare.</summary>
    public sealed class ExchangeSpec
    {
        internal ExchangeSpec(string name, string type, bool durable)
        {
            Name = name;
            Type = type;
            Durable = durable;
        }

        public string Name { get; }
        public string Type { get; }
        public bool Durable { get; }
    }

    /// <summary>A queue to declare.</summary>
    public sealed class QueueSpec
    {
        internal QueueSpec(
            string name, QueueType type, bool durable, IReadOnlyDictionary<string, object> arguments)
        {
            Name = name;
            Type = type;
            Durable = durable;
            Arguments = arguments;
        }

        public string Name { get; }
        public QueueType Type { get; }
        public bool Durable { get; }
        public IReadOnlyDictionary<string, object> Arguments { get; }
    }

    /// <summary>A binding to declare.</summary>
    public sealed class BindingSpec
    {
        internal BindingSpec(string queue, string exchange, string routingKey)
        {
            Queue = queue;
            Exchange = exchange;
            RoutingKey = routingKey;
        }

        public string Queue { get; }
        public string Exchange { get; }
        public string RoutingKey { get; }
    }

    /// <summary>Builds a <see cref="Topology"/>.</summary>
    public sealed class Builder
    {
        private readonly List<ExchangeSpec> _exchanges = new List<ExchangeSpec>();
        private readonly List<QueueSpec> _queues = new List<QueueSpec>();
        private readonly List<BindingSpec> _bindings = new List<BindingSpec>();

        internal Builder() { }

        public Builder Exchange(string name, string type)
        {
            _exchanges.Add(new ExchangeSpec(name, type, true));
            return this;
        }

        /// <summary>Adds an exchange unless this topology already names it.</summary>
        /// <remarks>
        /// For the two exchanges the library owns rather than the caller. Declaring
        /// one twice is harmless at the broker, but a plan that lists
        /// <c>acemq.dlx</c> once per queue is a plan somebody stops reading.
        /// </remarks>
        private Builder SharedExchange(string name, string type)
        {
            foreach (var exchange in _exchanges)
            {
                if (string.Equals(exchange.Name, name, StringComparison.Ordinal)) return this;
            }
            return Exchange(name, type);
        }

        /// <summary>A durable quorum queue, which is the default everywhere in this library.</summary>
        /// <remarks>
        /// <para>
        /// Quorum because a queue that survives losing its node is what almost everyone
        /// wants and almost nobody remembers to ask for — and because the type is part
        /// of what two services have to agree on. A queue's type is fixed at
        /// declaration, so a Java service declaring <c>orders</c> as quorum and a .NET
        /// service declaring the same name as classic do not both get a queue: the
        /// second is refused with <c>PRECONDITION_FAILED</c> and cannot consume at all.
        /// Java has the deployments and an existing quorum queue cannot be redeclared
        /// as classic, so this is the side that moved.
        /// </para>
        /// <para>
        /// Ask for <see cref="QueueType.Classic"/> explicitly where it is wanted. It
        /// still is, in three places this builder produces on its own — the retry
        /// rungs, <c>{name}.dlq</c> and <c>{name}.parked</c> — and in anything that has
        /// to be exclusive or auto-delete, which RabbitMQ does not allow a quorum queue
        /// to be.
        /// </para>
        /// </remarks>
        public Builder Queue(string name) =>
            Queue(name, QueueType.Quorum, null);

        public Builder Queue(string name, QueueType type) =>
            Queue(name, type, null);

        public Builder Queue(string name, QueueType type, IReadOnlyDictionary<string, object>? arguments)
        {
            _queues.Add(new QueueSpec(
                name, type, true,
                arguments ?? new Dictionary<string, object>()));
            return this;
        }

        public Builder Bind(string queue, string exchange, string routingKey)
        {
            _bindings.Add(new BindingSpec(queue, exchange, routingKey));
            return this;
        }

        /// <summary>
        /// Declares a queue together with the dead-letter exchange and queue that
        /// receive what it gives up on.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Several declarations and their bindings, as one call, because they are
        /// only correct together. A queue whose <c>x-dead-letter-exchange</c> points
        /// at an exchange nobody declared, or at one with no queue bound to it,
        /// throws messages away exactly as if dead-lettering had never been
        /// configured — and nothing reports it.
        /// </para>
        /// <para>
        /// The names are <see cref="Naming.DeadLetterQueue"/> and
        /// <see cref="Naming.ParkedQueue"/> reached through
        /// <see cref="Naming.DeadLetterExchange"/>, which is what Java, Go, Python and
        /// Ruby produce. This method used to produce <c>{name}.dlx</c> and
        /// <c>{name}.dead</c>, which was a third convention living in the same
        /// repository as the consumer path's <c>.dlq</c> and <c>.parked</c> — three
        /// places to look for one message, decided by which part of the library
        /// happened to create the queue.
        /// </para>
        /// <para>
        /// The queue itself is a durable quorum queue, the same default
        /// <see cref="Queue(string)"/> uses and the same one Java's
        /// <c>queueWithDeadLetter</c> uses. The two queues it dead-letters into are
        /// classic, deliberately — see <c>WithDeadLetterQueues</c>.
        /// </para>
        /// </remarks>
        public Builder QueueWithDeadLetter(string name) =>
            QueueWithDeadLetter(name, QueueType.Quorum, null);

        public Builder QueueWithDeadLetter(
            string name, QueueType type, IReadOnlyDictionary<string, object>? arguments)
        {
            var args = new Dictionary<string, object>();
            if (arguments != null)
            {
                foreach (var pair in arguments) args[pair.Key] = pair.Value;
            }
            args[RetryLadder.DeadLetterExchangeArgument] = Naming.DeadLetterExchange;

            // The routing key has to be overridden, and this line is what makes a
            // shared exchange work at all. A message the broker dead-letters keeps
            // the routing key it arrived under, so without this it would reach
            // acemq.dlx as 'orders.placed', match no binding, and be dropped —
            // which is the silent loss this method exists to prevent. A per-queue
            // fanout did not need it, which is why it was not here before.
            args[RetryLadder.DeadLetterRoutingKeyArgument] = Naming.DeadLetterQueue(name);

            Queue(name, type, args);
            return WithDeadLetterQueues(name);
        }

        /// <summary>
        /// The two queues a message ends up in when it cannot be handled, and the
        /// exchange they are reached through.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Neither of them gets dead-lettering of its own. A dead-letter queue that
        /// dead-letters is a loop, and a loop is how a poison message becomes an
        /// outage.
        /// </para>
        /// <para>
        /// Both are classic where the source queue is quorum, and that is a decision
        /// rather than an oversight. Java's <c>RetryTopology</c> declares
        /// <c>{name}.dlq</c> and <c>{name}.parked</c> as <c>QueueType.CLASSIC</c>, so
        /// a queue of either name already exists as classic on any broker a Java
        /// service has touched; declaring it quorum here would be refused with
        /// <c>PRECONDITION_FAILED</c>, which is the exact failure this alignment
        /// exists to remove. It is also the right shape on its own terms: these hold
        /// what nobody could process, they are read by a person rather than by a
        /// service, and replicating them buys availability for traffic that has
        /// already stopped flowing.
        /// </para>
        /// </remarks>
        private Builder WithDeadLetterQueues(string name)
        {
            SharedExchange(Naming.DeadLetterExchange, Naming.DeadLetterExchangeType);

            var dead = Naming.DeadLetterQueue(name);
            Queue(dead, QueueType.Classic, null);
            Bind(dead, Naming.DeadLetterExchange, dead);

            var parked = Naming.ParkedQueue(name);
            Queue(parked, QueueType.Classic, null);
            Bind(parked, Naming.DeadLetterExchange, parked);

            return this;
        }

        /// <summary>
        /// Declares a queue together with the rungs its retry policy's long waits use,
        /// and the queues a message ends up in when it runs out of them.
        /// </summary>
        /// <remarks>
        /// <para>
        /// It takes the policy rather than a list of delays on purpose. The rungs a
        /// consumer will publish into are derived from the policy it is running, so
        /// anything else here would be a second copy of the same list, free to drift
        /// from the first — and the way that drift shows up is a retry published to a
        /// queue nobody declared, at the moment the service is already failing. One
        /// value produces both, or the topology is not a description of what the
        /// service needs.
        /// </para>
        /// <para>
        /// A consumer declares the same rungs when it starts, which is belt and
        /// braces rather than duplication: this is the version somebody reviews in a
        /// deployment plan, and that one is the version that runs when nobody
        /// reviewed anything.
        /// </para>
        /// <para>
        /// The queue being consumed is a durable quorum queue, as everywhere else
        /// here. The rungs are not: each is classic, for the same reason the
        /// dead-letter queues are.
        /// </para>
        /// </remarks>
        /// <param name="name">The queue being consumed.</param>
        /// <param name="policy">Whose schedule the rungs are.</param>
        public Builder QueueWithRetry(string name, RetryPolicy policy) =>
            QueueWithRetry(name, policy, QueueType.Quorum, null);

        public Builder QueueWithRetry(
            string name, RetryPolicy policy, QueueType type,
            IReadOnlyDictionary<string, object>? arguments)
        {
            var ladder = RetryLadder.For(name, policy);

            // The source queue is pointed at acemq.dlx, exactly as QueueWithDeadLetter
            // points it. Without these two arguments this method declared {name}.dlq
            // and {name}.parked, bound them to acemq.dlx, and then left the broker with
            // no route into either: the queues existed and nothing the broker itself
            // dead-lettered could ever reach them.
            //
            // The library's own give-up path still worked, because it republishes
            // straight to {name}.dlq rather than relying on the broker. What was
            // missing is the backstop underneath it -- a source-queue TTL expiring, an
            // x-max-length drop, a reject from something that is not this library.
            // Those were being discarded silently.
            var args = new Dictionary<string, object>();
            if (arguments is not null)
            {
                foreach (var pair in arguments) args[pair.Key] = pair.Value;
            }

            args[RetryLadder.DeadLetterExchangeArgument] = Naming.DeadLetterExchange;
            args[RetryLadder.DeadLetterRoutingKeyArgument] = Naming.DeadLetterQueue(name);

            Queue(name, type, args);

            // The same two queues, the same exchange and the same bindings the
            // dead-letter builder produces. A message that ran out of attempts and a
            // message a handler gave up on land in one place, whichever call declared
            // it.
            WithDeadLetterQueues(name);

            if (ladder.IsEmpty) return this;

            if (RetryLadder.IsNamedExchange)
            {
                SharedExchange(RetryLadder.RetryExchange, RetryLadder.RetryExchangeType);
                Bind(name, RetryLadder.RetryExchange, RetryLadder.RoutingKeyFor(name));
            }

            // Classic, and pinned rather than inherited from the source queue's type.
            // Java's RetryTopology declares every rung QueueType.CLASSIC, and a rung is
            // the queue two services are most likely to declare independently — the
            // consumer declares its own ladder at start-up — so a disagreement about
            // its type is a PRECONDITION_FAILED at the moment a service starts. A rung
            // also holds nothing worth replicating: a message sits in it doing nothing
            // until a time-to-live sends it home, and losing the node means losing a
            // wait rather than losing the work.
            foreach (var rung in ladder.Rungs)
            {
                Queue(rung.Queue, QueueType.Classic, rung.Arguments);
            }

            return this;
        }

        public Topology Build() =>
            new Topology(_exchanges.ToArray(), _queues.ToArray(), _bindings.ToArray());
    }
}

/// <summary>What applying a <see cref="Topology"/> would do, or did.</summary>
public sealed class TopologyPlan
{
    private TopologyPlan(IReadOnlyList<TopologyAction> actions) => Actions = actions;

    internal static TopologyPlan Of(IReadOnlyList<TopologyAction> actions) => new TopologyPlan(actions);

    public IReadOnlyList<TopologyAction> Actions { get; }

    /// <summary>The actions that would change the broker.</summary>
    public IReadOnlyList<TopologyAction> Changes =>
        Actions.Where(a => a.Kind == TopologyActionKind.Create).ToArray();

    /// <summary>Where the broker already differs from what was asked for.</summary>
    public IReadOnlyList<TopologyAction> Drift =>
        Actions.Where(a => a.Kind == TopologyActionKind.Drift).ToArray();

    public bool HasChanges => Changes.Count > 0;
    public bool HasDrift => Drift.Count > 0;

    /// <summary>The plan as lines of text, for a log or a deployment review.</summary>
    public string Render()
    {
        var text = new StringBuilder();
        foreach (var action in Actions)
        {
            var mark = action.Kind switch
            {
                TopologyActionKind.Create => "+",
                TopologyActionKind.Present => " ",
                TopologyActionKind.Drift => "!",
                _ => "?",
            };
            text.Append(mark).Append(' ').Append(action.Description).Append('\n');
        }
        return text.ToString();
    }

    public override string ToString() =>
        $"TopologyPlan[{Changes.Count} change(s), {Drift.Count} drift]";
}

/// <summary>What a planned action would do.</summary>
public enum TopologyActionKind
{
    /// <summary>Does not exist and would be created.</summary>
    Create,

    /// <summary>Exists already and matches.</summary>
    Present,

    /// <summary>Exists but differs from what was asked for.</summary>
    Drift,

    /// <summary>Cannot be determined without applying it.</summary>
    Unknown,
}

/// <summary>One line of a <see cref="TopologyPlan"/>.</summary>
public sealed class TopologyAction
{
    internal TopologyAction(TopologyActionKind kind, string description)
    {
        Kind = kind;
        Description = description;
    }

    public TopologyActionKind Kind { get; }
    public string Description { get; }

    public override string ToString() => $"{Kind}: {Description}";
}

/// <summary>What comparing a queue against a specification found.</summary>
public enum QueueCheckResult
{
    /// <summary>It is there and matches.</summary>
    Matches,

    /// <summary>It is not there.</summary>
    Absent,

    /// <summary>It is there but differs from what was asked for.</summary>
    Differs,

    /// <summary>The transport cannot tell.</summary>
    Unsupported,
}

/// <summary>The result of comparing a declared queue against a specification.</summary>
public sealed class QueueCheck
{
    private QueueCheck(QueueCheckResult result, string? detail)
    {
        Result = result;
        Detail = detail;
    }

    public static QueueCheck Matches() => new QueueCheck(QueueCheckResult.Matches, null);
    public static QueueCheck Absent() => new QueueCheck(QueueCheckResult.Absent, null);
    public static QueueCheck Differs(string detail) => new QueueCheck(QueueCheckResult.Differs, detail);
    public static QueueCheck Unsupported() => new QueueCheck(QueueCheckResult.Unsupported, null);

    public QueueCheckResult Result { get; }

    /// <summary>What differs, when the broker said.</summary>
    public string? Detail { get; }

    public override string ToString() =>
        Detail == null ? Result.ToString() : $"{Result}: {Detail}";
}

/// <summary>How much of a topology to apply.</summary>
public enum ApplyMode
{
    /// <summary>Declare everything. Existing objects that match are left alone.</summary>
    Declare,

    /// <summary>Report what would happen and change nothing.</summary>
    DryRun,
}
