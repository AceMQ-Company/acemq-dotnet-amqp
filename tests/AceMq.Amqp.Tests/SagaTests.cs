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

/// <summary>What the saga steps operate on: a log of what really happened, in order.</summary>
public sealed class Ledger
{
    private readonly List<string> _entries = new List<string>();

    public void Record(string entry) { lock (_entries) _entries.Add(entry); }

    public IReadOnlyList<string> Entries { get { lock (_entries) return _entries.ToArray(); } }
}

/// <summary>
/// The saga, which is behaviour rather than wire contract.
/// </summary>
/// <remarks>
/// Nothing here touches a broker: a saga is in-process and publishes nothing, so what
/// these tests pin is the order things are undone in and what happens when undoing one
/// of them fails. Those are the two behaviours the Java implementation is deliberate
/// about, and they are the two a port gets wrong.
/// </remarks>
public sealed class SagaTests
{
    [Fact]
    public async Task RunsEveryStepInOrderAndReportsThemCompleted()
    {
        var ledger = new Ledger();
        var saga = Saga<Ledger>.Named("place-order")
            .Step("take-payment", l => l.Record("charged"))
            .CompensateWith(l => l.Record("refunded"))
            .Step("reserve-stock", l => l.Record("reserved"))
            .CompensateWith(l => l.Record("released"))
            .Step("book-courier", l => l.Record("booked"))
            .Build();

        var result = await saga.RunAsync(ledger);

        Assert.True(result.IsComplete);
        Assert.False(result.Compensated);
        Assert.Null(result.FailedAt);
        Assert.Null(result.Failure);
        Assert.False(result.HasUnresolved);
        Assert.Equal(new[] { "take-payment", "reserve-stock", "book-courier" }, result.Completed);
        Assert.Equal(new[] { "charged", "reserved", "booked" }, ledger.Entries);
        Assert.Equal(new[] { "take-payment", "reserve-stock", "book-courier" }, saga.StepNames);
    }

    /// <summary>
    /// The compensations run newest first.
    /// </summary>
    /// <remarks>
    /// Reverse order because that is the order the world was changed in, and a
    /// compensation often depends on state a later step has not yet altered. Asserted
    /// on the ledger rather than on the result, because the result would look the same
    /// if they had run forwards.
    /// </remarks>
    [Fact]
    public async Task CompensatesCompletedStepsInReverseOrder()
    {
        var ledger = new Ledger();
        var saga = Saga<Ledger>.Named("place-order")
            .Step("take-payment", l => l.Record("charged"))
            .CompensateWith(l => l.Record("refunded"))
            .Step("reserve-stock", l => l.Record("reserved"))
            .CompensateWith(l => l.Record("released"))
            .Step("book-courier", _ => throw new InvalidOperationException("no couriers"))
            .Build();

        var result = await saga.RunAsync(ledger);

        Assert.False(result.IsComplete);
        Assert.True(result.Compensated);
        Assert.Equal("book-courier", result.FailedAt);
        Assert.IsType<InvalidOperationException>(result.Failure);
        Assert.Equal("no couriers", result.Failure!.Message);

        // Only the two that ran, in the order they ran.
        Assert.Equal(new[] { "take-payment", "reserve-stock" }, result.Completed);

        // The undo is the reverse: stock released before the payment is refunded.
        Assert.Equal(
            new[] { "charged", "reserved", "released", "refunded" }, ledger.Entries);

        // Everything came back, so there is nothing for a person to reconcile.
        Assert.Empty(result.Unresolved);
        Assert.False(result.HasUnresolved);
    }

    /// <summary>
    /// A completed step with no compensation is skipped, not an error.
    /// </summary>
    /// <remarks>
    /// A step that only read something needs no undo, and treating the absence as a
    /// failure would put a step in <see cref="SagaResult.Unresolved"/> that nothing
    /// was ever going to reverse — which is the list an operator is meant to be able
    /// to trust.
    /// </remarks>
    [Fact]
    public async Task SkipsACompletedStepThatHasNoCompensation()
    {
        var ledger = new Ledger();
        var saga = Saga<Ledger>.Named("check-and-charge")
            .Step("read-credit-limit", l => l.Record("read"))
            .Step("take-payment", l => l.Record("charged"))
            .CompensateWith(l => l.Record("refunded"))
            .Step("book-courier", _ => throw new InvalidOperationException("no couriers"))
            .Build();

        var result = await saga.RunAsync(ledger);

        Assert.True(result.Compensated);
        Assert.Equal(new[] { "read-credit-limit", "take-payment" }, result.Completed);

        // The read was skipped silently; only the charge was undone.
        Assert.Equal(new[] { "read", "charged", "refunded" }, ledger.Entries);
        Assert.False(result.HasUnresolved);
    }

    /// <summary>
    /// A compensation that throws does not stop the ones after it.
    /// </summary>
    /// <remarks>
    /// Stopping would leave more undone than continuing does. What the caller gets
    /// instead is the name of the one that did not come back, which is the set of
    /// facts a human now has to reconcile by hand.
    /// </remarks>
    [Fact]
    public async Task CarriesOnWhenACompensationItselfFails()
    {
        var ledger = new Ledger();
        var saga = Saga<Ledger>.Named("place-order")
            .Step("take-payment", l => l.Record("charged"))
            .CompensateWith(l => l.Record("refunded"))
            .Step("reserve-stock", l => l.Record("reserved"))
            .CompensateWith(_ => throw new InvalidOperationException("warehouse offline"))
            .Step("notify-warehouse", l => l.Record("notified"))
            .CompensateWith(l => l.Record("un-notified"))
            .Step("book-courier", _ => throw new InvalidOperationException("no couriers"))
            .Build();

        var result = await saga.RunAsync(ledger);

        // The compensation after the failing one still ran, and so did the one before.
        Assert.Equal(
            new[] { "charged", "reserved", "notified", "un-notified", "refunded" },
            ledger.Entries);

        // And the one that did not come back is named.
        Assert.True(result.HasUnresolved);
        Assert.Equal(new[] { "reserve-stock" }, result.Unresolved);

        // The failure reported is still the step that failed, not the compensation.
        Assert.Equal("book-courier", result.FailedAt);
    }

    [Fact]
    public async Task CollectsEveryUnresolvedStepInTheOrderCompensationWasAttempted()
    {
        var saga = Saga<Ledger>.Named("three-bad-undos")
            .Step("one", _ => { })
            .CompensateWith(_ => throw new InvalidOperationException("one"))
            .Step("two", _ => { })
            .CompensateWith(_ => throw new InvalidOperationException("two"))
            .Step("three", _ => throw new InvalidOperationException("boom"))
            .Build();

        var result = await saga.RunAsync(new Ledger());

        // Reverse order, because that is the order they were attempted in.
        Assert.Equal(new[] { "two", "one" }, result.Unresolved);
    }

    /// <summary>The two saga events reach the diagnostics seam rather than a logger of its own.</summary>
    [Fact]
    public async Task ReportsCompensationAndUnresolvedStepsToDiagnostics()
    {
        var sink = new CollectingSink();
        AceMqDiagnostics.Subscribe(sink);
        try
        {
            var saga = Saga<Ledger>.Named("audited")
                .Step("one", _ => { })
                .CompensateWith(_ => throw new InvalidOperationException("cannot undo"))
                .Step("two", _ => throw new InvalidOperationException("boom"))
                .Build();

            var result = await saga.RunAsync(new Ledger());
            Assert.True(result.HasUnresolved);
        }
        finally
        {
            AceMqDiagnostics.Unsubscribe(sink);
        }

        var compensating = sink.Named(AceMqDiagnostics.SagaCompensating);
        Assert.Single(compensating);
        Assert.Equal(DiagnosticLevel.Warning, compensating[0].Level);
        Assert.Contains("audited", compensating[0].Message);
        Assert.IsType<InvalidOperationException>(compensating[0].Failure);

        // The one worth waking somebody for is an error, and it names the step.
        var unresolved = sink.Named(AceMqDiagnostics.SagaUnresolved);
        Assert.Single(unresolved);
        Assert.Equal(DiagnosticLevel.Error, unresolved[0].Level);
        Assert.Contains("'one'", unresolved[0].Message);
    }

    /// <summary>
    /// Cancellation is a step failure, and the undo is not cancelled with it.
    /// </summary>
    /// <remarks>
    /// A cancelled saga is precisely the one that needs undoing. Cancelling the
    /// compensations as well would turn one interrupted saga into a set of effects
    /// nobody reversed, which is the worst of the available outcomes.
    /// </remarks>
    [Fact]
    public async Task CompensatesWhenTheForwardPathIsCancelled()
    {
        var ledger = new Ledger();
        using var cancellation = new CancellationTokenSource();

        var saga = Saga<Ledger>.Named("interrupted")
            .Step("take-payment", l => l.Record("charged"))
            .CompensateWith((l, token) =>
            {
                // The token the compensation is given is not the cancelled one.
                Assert.False(token.IsCancellationRequested);
                l.Record("refunded");
                return Task.CompletedTask;
            })
            .Step("reserve-stock", (l, token) =>
            {
                cancellation.Cancel();
                token.ThrowIfCancellationRequested();
                l.Record("reserved");
                return Task.CompletedTask;
            })
            .Build();

        var result = await saga.RunAsync(ledger, cancellation.Token);

        Assert.True(result.Compensated);
        Assert.Equal("reserve-stock", result.FailedAt);
        Assert.IsAssignableFrom<OperationCanceledException>(result.Failure);
        Assert.Equal(new[] { "charged", "refunded" }, ledger.Entries);
        Assert.False(result.HasUnresolved);
    }

    [Fact]
    public async Task RunsAsynchronousStepsAndCompensations()
    {
        var ledger = new Ledger();
        var saga = Saga<Ledger>.Named("async")
            .Step("one", async l => { await Task.Yield(); l.Record("one"); })
            .CompensateWith(async l => { await Task.Yield(); l.Record("un-one"); })
            .Step("two", async _ => { await Task.Yield(); throw new InvalidOperationException("boom"); })
            .Build();

        var result = await saga.RunAsync(ledger);

        Assert.True(result.Compensated);
        Assert.Equal(new[] { "one", "un-one" }, ledger.Entries);
    }

    [Fact]
    public void RefusesTwoStepsWithTheSameName()
    {
        var builder = Saga<Ledger>.Named("duplicate").Step("one", _ => { });

        var failure = Assert.Throws<ArgumentException>(() => builder.Step("one", _ => { }));
        Assert.Contains("already has a step called 'one'", failure.Message);
    }

    [Fact]
    public void RefusesACompensationBeforeAnyStep() =>
        Assert.Throws<InvalidOperationException>(
            () => Saga<Ledger>.Named("empty").CompensateWith(_ => { }));

    [Fact]
    public void RefusesASagaWithNoSteps() =>
        Assert.Throws<InvalidOperationException>(() => Saga<Ledger>.Named("empty").Build());

    [Fact]
    public async Task ReadsAsASentenceWhenPrinted()
    {
        var saga = Saga<Ledger>.Named("place-order")
            .Step("take-payment", _ => { })
            .Step("book-courier", _ => { })
            .Build();

        Assert.Equal("Saga{place-order: take-payment -> book-courier}", saga.ToString());

        var completed = await saga.RunAsync(new Ledger());
        Assert.Equal(
            "SagaResult{place-order completed: take-payment -> book-courier}",
            completed.ToString());
    }

    private sealed class CollectingSink : IDiagnosticSink
    {
        private readonly ConcurrentBag<DiagnosticEvent> _events = new ConcurrentBag<DiagnosticEvent>();

        public void Record(DiagnosticEvent report) => _events.Add(report);

        /// <summary>
        /// The events with this name.
        /// </summary>
        /// <remarks>
        /// Filtered by name because sinks are process-wide and the test classes run in
        /// parallel, so anything else running at the same time reports into this sink
        /// too.
        /// </remarks>
        public IReadOnlyList<DiagnosticEvent> Named(string name) =>
            _events.Where(e => e.Name == name).ToArray();
    }
}
