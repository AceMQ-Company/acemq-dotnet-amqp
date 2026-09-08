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
using System.Globalization;
using AceMq.Amqp;

namespace AceMq.Amqp.Tests;

public sealed class Invoice
{
    public string InvoiceId { get; set; } = "";
    public long AmountCents { get; set; }
}

/// <summary>
/// The scheduler, against the in-memory broker.
/// </summary>
/// <remarks>
/// <para>
/// Unlike the saga, this one is a wire contract: the queue names, the three arguments
/// on each rung and the four header names are shared with Java and a difference in any
/// of them is a <c>PRECONDITION_FAILED</c> on a broker two languages share. What is
/// pinned here is everything that can be pinned without a broker — the names, the
/// header values, which rung a delay lands in, and that a message really does arrive
/// late rather than early. The declaration itself is proved against RabbitMQ in
/// <c>AceMq.Amqp.RabbitMq.Tests</c>, because only a real broker refuses a
/// disagreement.
/// </para>
/// <para>
/// The in-memory transport expires a message off a queue with an
/// <c>x-message-ttl</c> and re-routes it through the declared dead-letter exchange, so
/// the ladder really does hop here rather than being simulated.
/// </para>
/// </remarks>
public sealed class SchedulerTests
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");

    /// <summary>The five rung names, spelled the way Java spells them.</summary>
    [Fact]
    public void NamesEachRungTheWayJavaDoes()
    {
        Assert.Equal("acemq.schedule.1h", Scheduler.RungName(TimeSpan.FromHours(1)));
        Assert.Equal("acemq.schedule.10m", Scheduler.RungName(TimeSpan.FromMinutes(10)));
        Assert.Equal("acemq.schedule.1m", Scheduler.RungName(TimeSpan.FromMinutes(1)));
        Assert.Equal("acemq.schedule.10s", Scheduler.RungName(TimeSpan.FromSeconds(10)));
        Assert.Equal("acemq.schedule.1s", Scheduler.RungName(TimeSpan.FromSeconds(1)));

        // Longest first, which is what makes "the largest rung that does not
        // overshoot" a first match rather than a search.
        Assert.Equal(
            new[]
            {
                TimeSpan.FromHours(1), TimeSpan.FromMinutes(10), TimeSpan.FromMinutes(1),
                TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(1),
            },
            Scheduler.Rungs);

        // The rendering is by divisibility, not by unit, so a rung of 90 minutes
        // would be 90m and one of 90 seconds would be 90s.
        Assert.Equal("acemq.schedule.90m", Scheduler.RungName(TimeSpan.FromMinutes(90)));
        Assert.Equal("acemq.schedule.90s", Scheduler.RungName(TimeSpan.FromSeconds(90)));
        Assert.Equal("acemq.schedule.2h", Scheduler.RungName(TimeSpan.FromHours(2)));
    }

    [Fact]
    public void SpellsTheHeadersOutsideTheReservedNamespace()
    {
        // The whole point of these four names: x-acemq- is reserved, and Envelope
        // drops every header carrying it on the way in. A scheduler header using the
        // prefix would be written on publish and gone on consume.
        foreach (var header in new[]
        {
            Scheduler.TargetExchangeHeader, Scheduler.TargetRoutingKeyHeader,
            Scheduler.DueAtHeader, Scheduler.ContentTypeHeader,
        })
        {
            Assert.False(AceHeaders.IsAceHeader(header), header + " must survive a round trip");
        }

        Assert.Equal("x-schedule-exchange", Scheduler.TargetExchangeHeader);
        Assert.Equal("x-schedule-routing-key", Scheduler.TargetRoutingKeyHeader);
        Assert.Equal("x-schedule-due-at", Scheduler.DueAtHeader);
        Assert.Equal("x-schedule-content-type", Scheduler.ContentTypeHeader);
        Assert.Equal("acemq.schedule", Scheduler.Exchange);
        Assert.Equal("acemq.schedule.due", Scheduler.ControlQueue);
    }

    [Fact]
    public async Task DeclaresTheLadderAndTheControlQueue()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var scheduler = await Scheduler.OnAsync(mq);

        foreach (var name in new[]
        {
            "acemq.schedule.1h", "acemq.schedule.10m", "acemq.schedule.1m",
            "acemq.schedule.10s", "acemq.schedule.1s", "acemq.schedule.due",
        })
        {
            Assert.True(await mq.QueueExistsAsync(name), name + " was not declared");
        }
    }

    /// <summary>
    /// The control consumer leaves no dead-letter queues behind.
    /// </summary>
    /// <remarks>
    /// Since ADR-032 a consumer declares <c>{queue}.dlq</c> and <c>{queue}.parked</c>
    /// when it starts. The control queue's name is fixed and shared, so an ordinary
    /// consumer here would put two durable queues nothing publishes to on the broker of
    /// every service that ever constructed a scheduler. It consumes as a private queue
    /// instead, which is the same opt-out <c>Requester</c> uses.
    /// </remarks>
    [Fact]
    public async Task LeavesNoDeadLetterQueuesBehindForItsControlQueue()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var scheduler = await Scheduler.OnAsync(mq);

        Assert.False(await mq.QueueExistsAsync("acemq.schedule.due.dlq"));
        Assert.False(await mq.QueueExistsAsync("acemq.schedule.due.parked"));

        // Nor for the rungs, which nothing consumes at all.
        Assert.False(await mq.QueueExistsAsync("acemq.schedule.1s.dlq"));
        Assert.False(await mq.QueueExistsAsync("acemq.schedule.1s.parked"));
    }

    /// <summary>
    /// A long delay waits in the largest rung that does not overshoot, carrying the
    /// four headers that say where it is going and when.
    /// </summary>
    /// <remarks>
    /// Pulled out of the rung rather than waited for, because the point is where it is
    /// sitting and what it is carrying. An hour's delay lands in <c>acemq.schedule.1h</c>
    /// and would need twenty-four hops for a day.
    /// </remarks>
    [Fact]
    public async Task PutsALongDelayInTheLargestRungThatDoesNotOvershoot()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var scheduler = await Scheduler.OnAsync(mq);

        var due = DateTimeOffset.UtcNow.AddHours(4);
        await scheduler.AtAsync(due, "billing", "invoice.due", new Invoice
        {
            InvoiceId = "INV-1",
            AmountCents = 4200,
        });

        Assert.Equal(1, scheduler.Scheduled);
        Assert.Equal(1, scheduler.Hops);
        Assert.Equal(0, scheduler.Delivered);

        // A second connection reading as raw bytes, because what is on a rung is
        // whatever the caller encoded and this test has no business decoding it either.
        using var inspector = await AceMqConnection.ConnectAsync(_url, new BytesCodec());
        var waiting = await inspector.PullAsync<byte[]>("acemq.schedule.1h", TimeSpan.FromSeconds(5));
        Assert.NotNull(waiting);
        await waiting!.AcknowledgeAsync();

        Assert.Equal("billing", waiting.Headers[Scheduler.TargetExchangeHeader]);
        Assert.Equal("invoice.due", waiting.Headers[Scheduler.TargetRoutingKeyHeader]);

        // Epoch milliseconds, an integer, because Java writes Instant.toEpochMilli().
        var dueAt = Convert.ToInt64(waiting.Headers[Scheduler.DueAtHeader], CultureInfo.InvariantCulture);
        Assert.Equal(due.ToUnixTimeMilliseconds(), dueAt);

        // The payload's own content type travelled with it, because the scheduler
        // republishes bytes and the eventual consumer picks a codec from this.
        Assert.Equal("application/json", waiting.Headers[Scheduler.ContentTypeHeader]);

        // And the body is the encoded payload, untouched.
        Assert.Contains("INV-1", System.Text.Encoding.UTF8.GetString(waiting.Body));

        // The message on a rung is opaque bytes: the scheduler does not decode what it
        // is moving.
        Assert.Equal("application/octet-stream", waiting.ContentType);
    }

    /// <summary>
    /// Which rung a delay lands in.
    /// </summary>
    /// <remarks>
    /// Given as a moment rather than a duration, because "in exactly one hour" is a
    /// boundary rather than a case: by the time the remaining delay is measured a
    /// fraction of it has already gone, so an exact hour is a shade under an hour and
    /// lands one rung down. Java computes it the same way and lands in the same place;
    /// the boundary is exercised deliberately in the last row.
    /// </remarks>
    [Theory]
    [InlineData(240, "acemq.schedule.1h")]
    [InlineData(61, "acemq.schedule.1h")]
    [InlineData(59, "acemq.schedule.10m")]
    [InlineData(9, "acemq.schedule.1m")]
    [InlineData(60, "acemq.schedule.10m")]
    public async Task ChoosesTheRungByHowMuchDelayIsLeft(int minutes, string expected)
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var scheduler = await Scheduler.OnAsync(mq);

        await scheduler.AtAsync(
            DateTimeOffset.UtcNow.AddMinutes(minutes), "billing", "invoice.due",
            new Invoice { InvoiceId = "X" });

        Assert.Equal(1, await mq.MessageCountAsync(expected));
    }

    /// <summary>A moment already past is delivered rather than laddered.</summary>
    [Fact]
    public async Task DeliversImmediatelyWhenTheMomentHasAlreadyPassed()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareExchangeAsync("billing", "topic");
        await mq.DeclareQueueAsync("billing.due", QueueType.Classic, null);
        await mq.BindAsync("billing.due", "billing", "invoice.due");

        using var scheduler = await Scheduler.OnAsync(mq);

        await scheduler.AtAsync(
            DateTimeOffset.UtcNow.AddMinutes(-5), "billing", "invoice.due",
            new Invoice { InvoiceId = "LATE-1" });

        Assert.Equal(0, scheduler.Hops);
        Assert.Equal(1, scheduler.Delivered);
        Assert.Equal(1, await mq.MessageCountAsync("billing.due"));
    }

    /// <summary>
    /// The one that matters: it really is late, and it really does arrive.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A short delay, so the test takes a couple of seconds rather than an hour. The
    /// early check is the half of this that a broken implementation passes without — a
    /// scheduler that delivers straight away satisfies "it arrived" and fails "it was
    /// not there yet".
    /// </para>
    /// <para>
    /// The guarantee asserted is the delay less one rung. The last remainder under a
    /// second is delivered rather than waited out, because another hop would cost more
    /// than the accuracy it buys, so a 2.5s message arrives at about 2s. Java's
    /// arithmetic is identical and lands in the same place.
    /// </para>
    /// </remarks>
    [Fact]
    public async Task DeliversLateRatherThanEarlyAndKeepsThePayloadAndContentType()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareExchangeAsync("billing", "topic");
        await mq.DeclareQueueAsync("billing.due", QueueType.Classic, null);
        await mq.BindAsync("billing.due", "billing", "invoice.due");

        var arrived = new TaskCompletionSource<IMessage<Invoice>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var consumer = await mq.ConsumeAsync<Invoice>("billing.due", message =>
        {
            arrived.TrySetResult(message);
            return Task.FromResult(Ack.Accept());
        });

        using var scheduler = await Scheduler.OnAsync(mq);

        var delay = TimeSpan.FromMilliseconds(2500);
        var sent = DateTimeOffset.UtcNow;
        await scheduler.InAsync(
            delay, "billing", "invoice.due",
            new Invoice { InvoiceId = "INV-7", AmountCents = 999 });

        // Not there yet, and that is the assertion a broken scheduler fails.
        await Task.Delay(700);
        Assert.False(arrived.Task.IsCompleted, "delivered early");
        Assert.Equal(0, scheduler.Delivered);

        var received = await arrived.Task.WaitAsync(TimeSpan.FromSeconds(20));
        var waited = DateTimeOffset.UtcNow - sent;

        var floor = delay - TimeSpan.FromSeconds(1);
        Assert.True(waited >= floor, $"delivered after only {waited}, expected at least {floor}");

        // The payload came through the ladder as bytes and decoded on the far side,
        // which it can only do because the content type travelled with it.
        Assert.Equal("INV-7", received.Payload.InvoiceId);
        Assert.Equal(999, received.Payload.AmountCents);
        Assert.Equal("application/json", received.ContentType);

        // The scheduler's own headers are bookkeeping and are not passed on.
        Assert.False(received.Headers.ContainsKey(Scheduler.TargetExchangeHeader));
        Assert.False(received.Headers.ContainsKey(Scheduler.DueAtHeader));
        Assert.False(received.Headers.ContainsKey(Scheduler.ContentTypeHeader));

        Assert.Equal(1, scheduler.Scheduled);
        Assert.Equal(1, scheduler.Delivered);

        // Two seconds on the 1s rung, then a remainder too small to be worth another.
        Assert.Equal(2, scheduler.Hops);
    }

    /// <summary>A delay of two hops takes two hops.</summary>
    [Fact]
    public async Task HopsDownTheLadderUntilTheMessageIsDue()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareExchangeAsync("billing", "topic");
        await mq.DeclareQueueAsync("billing.due", QueueType.Classic, null);
        await mq.BindAsync("billing.due", "billing", "invoice.due");

        using var scheduler = await Scheduler.OnAsync(mq);
        await scheduler.InAsync(
            TimeSpan.FromMilliseconds(2400), "billing", "invoice.due",
            new Invoice { InvoiceId = "INV-8" });

        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (scheduler.Delivered == 0 && DateTime.UtcNow < deadline) await Task.Delay(50);

        Assert.Equal(1, scheduler.Delivered);

        // 2.4s: one second in the 1s rung, then 1.4s remaining is another second in the
        // 1s rung, then the last 0.4s is under the smallest rung and is delivered.
        Assert.Equal(2, scheduler.Hops);
    }

    /// <summary>
    /// A message nothing scheduled is dropped, loudly.
    /// </summary>
    /// <remarks>
    /// The control consumer has no dead-letter queue by design, so there is nowhere to
    /// put a message it cannot forward. Dropping it is the only option left and the
    /// diagnostic is the only record that it existed — which is why the diagnostic is
    /// an error rather than a warning.
    /// </remarks>
    [Fact]
    public async Task DropsAMessageThatWasNotScheduledAndReportsIt()
    {
        var sink = new CollectingSink();
        AceMqDiagnostics.Subscribe(sink);
        try
        {
            using var mq = await AceMqConnection.ConnectAsync(_url);
            using var scheduler = await Scheduler.OnAsync(mq);

            var intruder = mq.Publisher<string>(Scheduler.Exchange, Scheduler.ControlQueue);
            await intruder.SendAsync("not a scheduled message");

            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (sink.Named(AceMqDiagnostics.ScheduleForeign).Count == 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }

            var reported = sink.Named(AceMqDiagnostics.ScheduleForeign);
            Assert.Single(reported);
            Assert.Equal(DiagnosticLevel.Error, reported[0].Level);
            Assert.Equal(Scheduler.ControlQueue, reported[0].Queue);

            // Accepted, not requeued: it would come back for ever otherwise.
            Assert.Equal(0, await mq.MessageCountAsync(Scheduler.ControlQueue));
            Assert.Equal(0, scheduler.Delivered);
        }
        finally
        {
            AceMqDiagnostics.Unsubscribe(sink);
        }
    }

    [Fact]
    public async Task RefusesToScheduleOnceDisposed()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var scheduler = await Scheduler.OnAsync(mq);
        scheduler.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => scheduler.InAsync(TimeSpan.FromHours(1), "billing", "invoice.due", "x"));

        // Disposing twice is not an error, and the queues stay: they are shared and may
        // hold somebody else's messages.
        scheduler.Dispose();
        Assert.True(await mq.QueueExistsAsync("acemq.schedule.1h"));
    }

    private sealed class CollectingSink : IDiagnosticSink
    {
        private readonly ConcurrentBag<DiagnosticEvent> _events = new ConcurrentBag<DiagnosticEvent>();

        public void Record(DiagnosticEvent report) => _events.Add(report);

        /// <summary>Filtered by name: sinks are process-wide and test classes run in parallel.</summary>
        public IReadOnlyList<DiagnosticEvent> Named(string name) =>
            _events.Where(e => e.Name == name).ToArray();
    }
}
