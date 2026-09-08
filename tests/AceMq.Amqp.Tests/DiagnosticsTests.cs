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
using AceMq.Amqp.Diagnostics;
using Microsoft.Extensions.Logging;

namespace AceMq.Amqp.Tests;

/// <summary>
/// A sink that keeps what it was given, for one queue.
/// </summary>
/// <remarks>
/// Filtered by queue because sinks are process-wide and these classes run in
/// parallel: without it, a dead-lettering test elsewhere in the suite would land in
/// this one's list and the assertion would depend on scheduling.
/// </remarks>
internal sealed class RecordingSink : IDiagnosticSink
{
    private readonly string _queue;

    internal RecordingSink(string queue) => _queue = queue;

    internal ConcurrentQueue<DiagnosticEvent> Seen { get; } =
        new ConcurrentQueue<DiagnosticEvent>();

    public void Record(DiagnosticEvent report)
    {
        if (report.Queue == _queue) Seen.Enqueue(report);
    }
}

public sealed class DiagnosticsTests
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");
    private readonly string _queue = "work-" + Guid.NewGuid().ToString("N").Substring(0, 8);

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

    /// <summary>
    /// The event the seam exists for.
    /// </summary>
    /// <remarks>
    /// A republish that fails leaves the message with the broker on Ack.Release and
    /// does not advance the attempt, so it will be handed straight back — a
    /// redelivery loop with nothing to explain it. Before this there was a status on
    /// Activity.Current and nothing else, which is invisible to a process that is not
    /// exporting traces.
    /// </remarks>
    [Fact]
    public async Task SaysSoWhenAMessageCouldNotBeMovedAndWasHandedBack()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_queue);

        var sink = new RecordingSink(_queue);
        AceMqDiagnostics.Subscribe(sink);
        try
        {
            using var consumer = await mq.ConsumeAsync<string>(_queue, async _ =>
            {
                // The queue goes out from under the consumer, which is what a hasty
                // clean-up or a policy engine does. The retry has nowhere to
                // republish to, so it cannot happen.
                await mq.DeleteQueueAsync(_queue);
                return Ack.Retry(TimeSpan.Zero, "the warehouse said no");
            });

            await mq.Publisher<string>("", _queue).SendAsync("doomed");

            await Eventually(() => !sink.Seen.IsEmpty, "the failed move to be reported");
        }
        finally
        {
            AceMqDiagnostics.Unsubscribe(sink);
        }

        Assert.True(sink.Seen.TryDequeue(out var report));
        Assert.Equal(AceMqDiagnostics.MoveFailed, report!.Name);
        Assert.Equal(DiagnosticLevel.Error, report.Level);
        Assert.Equal(_queue, report.Queue);
        Assert.Equal(_queue, report.Destination);
        Assert.NotNull(report.MessageId);
        Assert.NotNull(report.Failure);
        Assert.Contains("could not move a message", report.Message);
    }

    [Fact]
    public async Task SaysSoWhenAMessageIsDeadLettered()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_queue);

        var sink = new RecordingSink(_queue);
        AceMqDiagnostics.Subscribe(sink);
        try
        {
            using var consumer = await mq.ConsumeAsync<string>(
                _queue, _ => Task.FromResult(Ack.DeadLetter("the order was already shipped")));
            await mq.Publisher<string>("", _queue).SendAsync("doomed");

            await Eventually(() => !sink.Seen.IsEmpty, "the dead letter to be reported");
        }
        finally
        {
            AceMqDiagnostics.Unsubscribe(sink);
        }

        Assert.True(sink.Seen.TryDequeue(out var report));
        Assert.Equal(AceMqDiagnostics.DeadLettered, report!.Name);
        Assert.Equal(DiagnosticLevel.Warning, report.Level);
        Assert.Equal(Naming.DeadLetterQueue(_queue), report.Destination);
        Assert.Equal("the order was already shipped", report.Message);
        Assert.Equal(1, report.Attempt);
    }

    [Fact]
    public async Task SaysSoWhenAMessageIsParked()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_queue);

        var sink = new RecordingSink(_queue);
        AceMqDiagnostics.Subscribe(sink);
        try
        {
            using var consumer = await mq.ConsumeAsync<string>(
                _queue, _ => Task.FromResult(Ack.Park("nobody knows what this is")));
            await mq.Publisher<string>("", _queue).SendAsync("unreadable");

            await Eventually(() => !sink.Seen.IsEmpty, "the parking to be reported");
        }
        finally
        {
            AceMqDiagnostics.Unsubscribe(sink);
        }

        Assert.True(sink.Seen.TryDequeue(out var report));
        Assert.Equal(AceMqDiagnostics.Parked, report!.Name);
        Assert.Equal(Naming.ParkedQueue(_queue), report.Destination);

        // Parked and dead-lettered are different events, because "failed five times"
        // and "nothing could read it" want different people.
        Assert.NotEqual(AceMqDiagnostics.DeadLettered, report.Name);
    }

    /// <summary>
    /// A sink that throws must not become the failure it was there to report.
    /// </summary>
    [Fact]
    public void KeepsGoingWhenASinkThrows()
    {
        var thrower = new ThrowingSink();
        var quiet = new RecordingSink("q");
        AceMqDiagnostics.Subscribe(thrower);
        AceMqDiagnostics.Subscribe(quiet);
        try
        {
            AceMqDiagnostics.Record(new DiagnosticEvent(
                AceMqDiagnostics.MoveFailed, DiagnosticLevel.Error, "boom",
                "q", "q.dlq", "id-1", 2, null));
        }
        finally
        {
            AceMqDiagnostics.Unsubscribe(thrower);
            AceMqDiagnostics.Unsubscribe(quiet);
        }

        // Every call here is on a path where a message has already gone wrong.
        // Letting a broken logger turn that into a crashed consumer would make the
        // diagnostics more dangerous than what they describe.
        Assert.Single(quiet.Seen);
    }

    [Fact]
    public void StopsSendingOnceUnsubscribed()
    {
        var sink = new RecordingSink("q");
        AceMqDiagnostics.Subscribe(sink);
        Assert.True(AceMqDiagnostics.IsEnabled);
        Assert.True(AceMqDiagnostics.Unsubscribe(sink));
        Assert.False(AceMqDiagnostics.Unsubscribe(sink));

        AceMqDiagnostics.Record(new DiagnosticEvent(
            AceMqDiagnostics.Parked, DiagnosticLevel.Warning, "ignored",
            "q", null, null, 0, null));

        Assert.Empty(sink.Seen);
    }

    [Fact]
    public void RendersAnEventAsOneReadableLine()
    {
        var report = new DiagnosticEvent(
            AceMqDiagnostics.MoveFailed, DiagnosticLevel.Error,
            "could not move a message to 'orders.placed.dlq': matched no queue",
            "orders.placed", "orders.placed.dlq", "id-7", 3, null);

        // A console logger shows the text and nothing else, so everything an
        // operator needs has to survive into it.
        var line = report.ToString();
        Assert.Contains("acemq.move.failed", line);
        Assert.Contains("queue=orders.placed", line);
        Assert.Contains("destination=orders.placed.dlq", line);
        Assert.Contains("message=id-7", line);
        Assert.Contains("attempt=3", line);
    }

    [Fact]
    public void BridgesToAnILoggerAtTheMatchingLevel()
    {
        var logger = new CapturingLogger();
        using var bridge = LoggerSink.SubscribedTo(logger);

        AceMqDiagnostics.Record(new DiagnosticEvent(
            AceMqDiagnostics.MoveFailed, DiagnosticLevel.Error, "could not move it",
            "orders.placed", "orders.placed.dlq", "id-9", 4,
            new InvalidOperationException("the broker said no")));
        AceMqDiagnostics.Record(new DiagnosticEvent(
            AceMqDiagnostics.Parked, DiagnosticLevel.Warning, "cannot read it",
            "orders.placed", "orders.placed.parked", "id-10", 1, null));

        // This test's own two events and not everything that arrived.
        // AceMqDiagnostics is a process-wide sink and xUnit runs test collections in
        // parallel, so for as long as this bridge is subscribed it also receives
        // whatever any other test's consumer dead-letters or parks. Asserting on
        // Lines[0] made that somebody else's warning about one run in ten, which is a
        // failure about test isolation wearing the costume of a failure about logging.
        var mine = logger.LinesSoFar()
            .Where(l => l.Text.Contains("could not move it") || l.Text.Contains("cannot read it"))
            .ToList();

        Assert.Equal(2, mine.Count);
        Assert.Equal(LogLevel.Error, mine[0].Level);
        Assert.Contains("could not move it", mine[0].Text);
        Assert.IsType<InvalidOperationException>(mine[0].Failure);
        Assert.Equal(LogLevel.Warning, mine[1].Level);

        // The fields go in as a scope as well as into the text, so a structured
        // backend can be queried by queue or by message id.
        Assert.Contains(logger.Scopes, s => s.Contains("acemq.queue"));
        Assert.Contains(logger.Scopes, s => s.Contains("acemq.message_id"));
    }

    [Fact]
    public void UnsubscribesTheBridgeWhenItIsDisposed()
    {
        var logger = new CapturingLogger();
        var bridge = LoggerSink.SubscribedTo(logger);
        bridge.Dispose();

        AceMqDiagnostics.Record(new DiagnosticEvent(
            AceMqDiagnostics.Parked, DiagnosticLevel.Warning, "ignored",
            "q", null, null, 0, null));

        // A sink outliving the logger factory it writes to is an
        // ObjectDisposedException thrown from a dead-lettering path.
        Assert.Empty(logger.Lines);
    }

    private sealed class ThrowingSink : IDiagnosticSink
    {
        public void Record(DiagnosticEvent report) =>
            throw new InvalidOperationException("this logger is broken");
    }

    private sealed class CapturingLogger : ILogger
    {
        // Locked, because the sink this is bridged to is process-wide and xUnit runs
        // test collections in parallel: a consumer settling a message on another
        // thread writes here while the test reading it enumerates, and an unguarded
        // List does not survive that.
        private readonly object _guard = new();

        internal List<(LogLevel Level, string Text, Exception? Failure)> Lines { get; } = new();

        internal List<string> Scopes { get; } = new();

        /// <summary>The lines so far, safe to enumerate while more arrive.</summary>
        internal List<(LogLevel Level, string Text, Exception? Failure)> LinesSoFar()
        {
            lock (_guard) return new List<(LogLevel, string, Exception?)>(Lines);
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull
        {
            lock (_guard)
            {
                Scopes.Add(state.ToString() ?? "");
                if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
                {
                    Scopes.Add(string.Join(",", pairs.Select(p => p.Key)));
                }
            }
            return new Nothing();
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_guard) Lines.Add((logLevel, formatter(state, exception), exception));
        }

        private sealed class Nothing : IDisposable
        {
            public void Dispose() { }
        }
    }
}
