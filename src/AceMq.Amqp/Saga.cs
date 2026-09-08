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
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>
/// What a saga did.
/// </summary>
/// <remarks>
/// Returned rather than thrown, because a failed saga is not an exceptional
/// condition to a caller that has to decide what happens next — and because the
/// interesting part is not the exception but <see cref="Unresolved"/>, the list of
/// things that could not be undone.
/// </remarks>
public sealed class SagaResult
{
    private SagaResult(
        string saga,
        string? failedAt,
        Exception? failure,
        IReadOnlyList<string> completed,
        IReadOnlyList<string> unresolved)
    {
        Saga = saga;
        FailedAt = failedAt;
        Failure = failure;
        Completed = completed;
        Unresolved = unresolved;
    }

    internal static SagaResult OfCompleted(string saga, IReadOnlyList<string> completed) =>
        new SagaResult(saga, null, null, completed, new string[0]);

    internal static SagaResult OfCompensated(
        string saga,
        string failedAt,
        Exception failure,
        IReadOnlyList<string> completed,
        IReadOnlyList<string> unresolved) =>
        new SagaResult(saga, failedAt, failure, completed, unresolved);

    /// <summary>What the saga is called.</summary>
    public string Saga { get; }

    /// <summary>Whether every step ran.</summary>
    public bool IsComplete => FailedAt == null;

    /// <summary>Whether a step failed and the earlier ones were undone.</summary>
    public bool Compensated => FailedAt != null;

    /// <summary>The step that failed, or null when none did.</summary>
    public string? FailedAt { get; }

    /// <summary>Why it failed, or null when it did not.</summary>
    public Exception? Failure { get; }

    /// <summary>The steps that ran, in order, before the failure.</summary>
    public IReadOnlyList<string> Completed { get; }

    /// <summary>
    /// The steps whose compensation failed.
    /// </summary>
    /// <remarks>
    /// <strong>This is the list to alert on.</strong> Everything else a saga reports
    /// is recoverable by construction; these are real-world effects that happened,
    /// were meant to be undone, and were not. Nothing else in the system knows about
    /// them, and no retry will resolve them — a person has to.
    /// </remarks>
    public IReadOnlyList<string> Unresolved { get; }

    /// <summary>Whether anything was left in a state nobody intended.</summary>
    public bool HasUnresolved => Unresolved.Count > 0;

    public override string ToString()
    {
        if (IsComplete)
        {
            return "SagaResult{" + Saga + " completed: " + string.Join(" -> ", Completed) + "}";
        }
        return "SagaResult{" + Saga + " failed at " + FailedAt
            + ", compensated [" + string.Join(", ", Completed) + "]"
            + (HasUnresolved ? ", UNRESOLVED [" + string.Join(", ", Unresolved) + "]" : "")
            + "}";
    }
}

/// <summary>
/// A sequence of steps where each one knows how to undo itself.
/// </summary>
/// <remarks>
/// <para>
/// <code>
/// var booking = Saga&lt;Order&gt;.Named("place-order")
///     .Step("take-payment", order =&gt; payments.ChargeAsync(order))
///         .CompensateWith(order =&gt; payments.RefundAsync(order))
///     .Step("reserve-stock", order =&gt; inventory.ReserveAsync(order))
///         .CompensateWith(order =&gt; inventory.ReleaseAsync(order))
///     .Step("book-courier", order =&gt; couriers.BookAsync(order))
///     .Build();
///
/// var result = await booking.RunAsync(order);
/// </code>
/// </para>
/// <para>
/// If <c>book-courier</c> throws, the stock is released and the payment refunded, in
/// that order, and <see cref="SagaResult.Compensated"/> says so.
/// </para>
/// <para>
/// <strong>Not a distributed transaction.</strong> Nothing is isolated: after
/// <c>take-payment</c> the customer's money really has moved, and anybody looking sees
/// that it has. If <c>book-courier</c> then fails, the refund is a <em>new</em> fact
/// rather than an erasure of the old one, and for a few seconds the world contained a
/// charge that should not have happened. That is not a defect in this class; it is
/// what compensating a real-world action means, and a saga is honest about it where a
/// two-phase commit pretends otherwise.
/// </para>
/// <para>
/// So the steps must be things that can be undone by doing something else. Sending an
/// email cannot be compensated — the apology is a second email, not an unsend — and a
/// saga step that sends one should be the last step, after everything that can still
/// fail.
/// </para>
/// <para>
/// <strong>Not durable.</strong> This runs in one process and its state is on the
/// stack. A crash midway leaves the saga half-applied with nothing to resume it, which
/// is the honest limitation of the in-process form. Where a saga must survive the
/// process, the steps have to be messages and the state has to be in a database — a
/// much larger thing, and it is not this.
/// </para>
/// <para>
/// <strong>When compensation itself fails.</strong> It is tried, it is reported to
/// <see cref="AceMqDiagnostics"/>, and the remaining compensations still run. The
/// alternative — stopping — leaves more undone than continuing does. What comes back
/// is a <see cref="SagaResult"/> listing what could not be undone, and that list is
/// the thing to alert on: it is the set of facts a human now has to reconcile by hand.
/// </para>
/// <para>
/// <strong>This is not a message pattern.</strong> Nothing here is published and no
/// header is set, so unlike the envelope or the scheduler it has no wire contract to
/// hold to; only the behaviour matches Java's <c>org.acemq.amqp.patterns.Saga</c>.
/// </para>
/// </remarks>
/// <typeparam name="T">What the saga operates on.</typeparam>
public sealed class Saga<T>
{
    private readonly string _name;
    private readonly Step[] _steps;

    private Saga(string name, Step[] steps)
    {
        _name = name;
        _steps = steps;
    }

    /// <summary>Starts building a saga with this name, used in results and diagnostics.</summary>
    public static Builder Named(string name) =>
        new Builder(name ?? throw new ArgumentNullException(nameof(name)));

    /// <summary>The step names, in order.</summary>
    public IReadOnlyList<string> StepNames
    {
        get
        {
            var names = new string[_steps.Length];
            for (var i = 0; i < _steps.Length; i++) names[i] = _steps[i].Name;
            return names;
        }
    }

    /// <summary>Runs the steps, compensating in reverse if one fails.</summary>
    public Task<SagaResult> RunAsync(T subject) => RunAsync(subject, CancellationToken.None);

    /// <summary>
    /// Runs the steps, compensating in reverse if one fails.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Never throws for a step failure, because a caller needs the compensation report
    /// more than it needs a stack trace.
    /// </para>
    /// <para>
    /// The token cancels the forward path only. A step that observes it and throws
    /// <see cref="OperationCanceledException"/> is a failed step like any other and the
    /// earlier ones are undone — a cancelled saga is precisely the one that most needs
    /// undoing. Compensations are then run with <see cref="CancellationToken.None"/>:
    /// cancelling the undo as well would turn one interrupted saga into a set of
    /// effects nobody reversed.
    /// </para>
    /// </remarks>
    /// <param name="subject">What to operate on.</param>
    /// <param name="cancellationToken">Cancels the forward path, not the compensations.</param>
    /// <returns>What happened.</returns>
    public async Task<SagaResult> RunAsync(T subject, CancellationToken cancellationToken)
    {
        var completed = new List<Step>();

        foreach (var step in _steps)
        {
            try
            {
                await step.Action(subject, cancellationToken).ConfigureAwait(false);
                completed.Add(step);
            }
            catch (Exception failure)
            {
                // Broad on purpose, and the counterpart of Java catching RuntimeException:
                // the point of a saga is that whatever a step did wrong, the steps before
                // it still get undone. An exception that escaped here would leave the
                // world half-changed and the caller holding a stack trace instead of a
                // report of what is still outstanding.
                AceMqDiagnostics.Report(
                    AceMqDiagnostics.SagaCompensating, DiagnosticLevel.Warning,
                    $"saga '{_name}' failed at step '{step.Name}': {failure.Message}. "
                    + $"Compensating {completed.Count} completed step(s) in reverse.",
                    null, null, null, 0, failure);

                var unresolved = await CompensateAsync(subject, completed).ConfigureAwait(false);
                return SagaResult.OfCompensated(
                    _name, step.Name, failure, NamesOf(completed), unresolved);
            }
        }

        return SagaResult.OfCompleted(_name, NamesOf(completed));
    }

    /// <summary>
    /// Undoes what was done, most recent first.
    /// </summary>
    /// <remarks>
    /// Reverse order because that is the order the world was changed in, and a
    /// compensation often depends on the state a later step has not yet altered.
    /// </remarks>
    /// <returns>The steps whose compensation failed, which is what a human has to reconcile.</returns>
    private static async Task<IReadOnlyList<string>> CompensateAsync(T subject, List<Step> completed)
    {
        var unresolved = new List<string>();

        for (var i = completed.Count - 1; i >= 0; i--)
        {
            var step = completed[i];
            if (step.Compensation == null)
            {
                // Nothing to undo, which is legitimate: a step that only read something,
                // or one whose effect is harmless, needs no compensation.
                continue;
            }

            try
            {
                await step.Compensation(subject, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception failure)
            {
                // Reported and carried on. Stopping here would leave more undone than
                // continuing, and the caller is told exactly which ones did not come back.
                AceMqDiagnostics.Report(
                    AceMqDiagnostics.SagaUnresolved, DiagnosticLevel.Error,
                    $"could not compensate step '{step.Name}': {failure.Message}. "
                    + "The effect of that step is still in place and no retry will remove it.",
                    null, null, null, 0, failure);
                unresolved.Add(step.Name);
            }
        }

        return unresolved;
    }

    private static IReadOnlyList<string> NamesOf(List<Step> steps)
    {
        var names = new string[steps.Count];
        for (var i = 0; i < steps.Count; i++) names[i] = steps[i].Name;
        return names;
    }

    public override string ToString() =>
        "Saga{" + _name + ": " + string.Join(" -> ", StepNames) + "}";

    /// <summary>One step and the thing that undoes it.</summary>
    private sealed class Step
    {
        internal Step(
            string name,
            Func<T, CancellationToken, Task> action,
            Func<T, CancellationToken, Task>? compensation)
        {
            Name = name;
            Action = action;
            Compensation = compensation;
        }

        internal string Name { get; }

        internal Func<T, CancellationToken, Task> Action { get; }

        /// <summary>Null when the step needs no undoing, which is legitimate and is checked before use.</summary>
        internal Func<T, CancellationToken, Task>? Compensation { get; }
    }

    /// <summary>Collects the steps of a <see cref="Saga{T}"/>.</summary>
    public sealed class Builder
    {
        private readonly string _name;
        private readonly List<Step> _steps = new List<Step>();

        internal Builder(string name) => _name = name;

        /// <summary>
        /// Adds a step with no compensation.
        /// </summary>
        /// <remarks>
        /// Legitimate for a step that changed nothing, and a mistake for one that did.
        /// There is no warning for the second case, because a library cannot tell them
        /// apart — which is the argument for writing the compensation first and the
        /// action second.
        /// </remarks>
        public Builder Step(string stepName, Func<T, CancellationToken, Task> action)
        {
            if (stepName == null) throw new ArgumentNullException(nameof(stepName));
            if (action == null) throw new ArgumentNullException(nameof(action));
            foreach (var existing in _steps)
            {
                if (existing.Name == stepName)
                {
                    throw new ArgumentException(
                        $"saga {_name} already has a step called '{stepName}'. Names identify a"
                        + " step in the compensation report, so two of them would make that"
                        + " report ambiguous.", nameof(stepName));
                }
            }
            _steps.Add(new Step(stepName, action, null));
            return this;
        }

        /// <summary>Adds a step whose work does not need the token.</summary>
        public Builder Step(string stepName, Func<T, Task> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            return Step(stepName, (subject, _) => action(subject));
        }

        /// <summary>Adds a step that does its work synchronously.</summary>
        public Builder Step(string stepName, Action<T> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            return Step(stepName, (subject, _) =>
            {
                action(subject);
                return CompletedTask;
            });
        }

        /// <summary>Gives the most recently added step something that undoes it.</summary>
        public Builder CompensateWith(Func<T, CancellationToken, Task> compensation)
        {
            if (compensation == null) throw new ArgumentNullException(nameof(compensation));
            if (_steps.Count == 0)
            {
                throw new InvalidOperationException(
                    "there is no step to compensate yet: call Step(...) first");
            }
            var last = _steps[_steps.Count - 1];
            _steps[_steps.Count - 1] = new Step(last.Name, last.Action, compensation);
            return this;
        }

        /// <summary>Gives the most recently added step something that undoes it.</summary>
        public Builder CompensateWith(Func<T, Task> compensation)
        {
            if (compensation == null) throw new ArgumentNullException(nameof(compensation));
            return CompensateWith((subject, _) => compensation(subject));
        }

        /// <summary>Gives the most recently added step something that undoes it, synchronously.</summary>
        public Builder CompensateWith(Action<T> compensation)
        {
            if (compensation == null) throw new ArgumentNullException(nameof(compensation));
            return CompensateWith((subject, _) =>
            {
                compensation(subject);
                return CompletedTask;
            });
        }

        /// <summary>The saga.</summary>
        /// <exception cref="InvalidOperationException">If it has no steps.</exception>
        public Saga<T> Build()
        {
            if (_steps.Count == 0)
            {
                throw new InvalidOperationException("saga " + _name + " has no steps");
            }
            return new Saga<T>(_name, _steps.ToArray());
        }

        // netstandard2.0 has Task.CompletedTask, but allocating one cached instance
        // here keeps the synchronous overloads free of a per-step allocation.
        private static readonly Task CompletedTask = Task.FromResult(0);
    }
}
