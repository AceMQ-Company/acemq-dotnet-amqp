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

        public Builder Queue(string name) =>
            Queue(name, QueueType.Classic, null);

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
        /// Three declarations and a binding, as one call, because they are only
        /// correct together. A queue whose <c>x-dead-letter-exchange</c> points at an
        /// exchange nobody declared, or at one with no queue bound to it, throws
        /// messages away exactly as if dead-lettering had never been configured — and
        /// nothing reports it. The dead-letter queue is named after the queue it
        /// serves, so the pairing is visible in the broker's UI.
        /// </remarks>
        public Builder QueueWithDeadLetter(string name) =>
            QueueWithDeadLetter(name, QueueType.Classic, null);

        public Builder QueueWithDeadLetter(
            string name, QueueType type, IReadOnlyDictionary<string, object>? arguments)
        {
            var exchange = name + ".dlx";
            var dead = name + ".dead";

            var args = new Dictionary<string, object>();
            if (arguments != null)
            {
                foreach (var pair in arguments) args[pair.Key] = pair.Value;
            }
            args["x-dead-letter-exchange"] = exchange;

            Exchange(exchange, "fanout");
            Queue(name, type, args);
            Queue(dead, type, null);
            Bind(dead, exchange, string.Empty);
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

/// <summary>How much of a topology to apply.</summary>
public enum ApplyMode
{
    /// <summary>Declare everything. Existing objects that match are left alone.</summary>
    Declare,

    /// <summary>Report what would happen and change nothing.</summary>
    DryRun,
}
