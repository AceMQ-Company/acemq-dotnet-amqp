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

using System.Diagnostics;
using System.Diagnostics.Metrics;
using AceMq.Amqp;

namespace AceMq.Amqp.Tests;

/// <summary>
/// What the library reports about itself.
/// </summary>
/// <remarks>
/// The metric names are the contract, not an implementation detail: a dashboard
/// written against the Java library has to work against this one, so these assert the
/// exact strings rather than that "some metric" was recorded.
/// </remarks>
public sealed class TelemetryTests : IDisposable
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");

    // The meter is process-global, so this listener sees every other test class's
    // traffic too. Queue names are unique per test and every assertion filters on
    // them; asserting on "the only publish" would pass alone and fail in a full run.
    private readonly string _q = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);
    private readonly List<(string Name, double Value, Dictionary<string, string> Tags)> _measurements = new();
    private readonly MeterListener _listener;

    public TelemetryTests()
    {
        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == MetricNames.Meter) listener.EnableMeasurementEvents(instrument);
            },
        };
        _listener.SetMeasurementEventCallback<long>((i, m, t, _) => Record(i.Name, m, t));
        _listener.SetMeasurementEventCallback<int>((i, m, t, _) => Record(i.Name, m, t));
        _listener.SetMeasurementEventCallback<double>((i, m, t, _) => Record(i.Name, m, t));
        _listener.Start();
    }

    private void Record(string name, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var map = new Dictionary<string, string>();
        foreach (var tag in tags) map[tag.Key] = tag.Value?.ToString() ?? "";
        lock (_measurements) _measurements.Add((name, value, map));
    }

    private IReadOnlyList<(string Name, double Value, Dictionary<string, string> Tags)> Taken()
    {
        lock (_measurements) return _measurements.ToArray();
    }

    public void Dispose() => _listener.Dispose();

    private async Task Eventually(Func<bool> probe, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (probe()) return;
            await Task.Delay(10);
        }
        throw new TimeoutException($"timed out waiting for {what}");
    }

    [Fact]
    public async Task RecordsAPublishUnderTheNameJavaUses()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);
        await mq.Publisher<string>("", _q).SendAsync("hello");

        var total = Taken().Single(
            m => m.Name == MetricNames.PublishTotal && m.Tags[MetricNames.TagRoutingKey] == _q);
        Assert.Equal("acemq.publish.total", total.Name);
        Assert.Equal(1, total.Value);
        Assert.Equal(MetricNames.OutcomeConfirmed, total.Tags[MetricNames.TagOutcome]);

        Assert.Contains(Taken(), m =>
            m.Name == MetricNames.PublishDuration && m.Tags[MetricNames.TagRoutingKey] == _q);
    }

    [Fact]
    public async Task ReportsAnUnroutablePublishAsSuchRatherThanAsAFailure()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareExchangeAsync("orders", "topic");

        await Assert.ThrowsAsync<PublishFailedException>(
            () => mq.Publisher<string>("orders", _q).SendAsync("hello"));

        // The distinction matters on a dashboard: unroutable is a topology mistake,
        // failed is the broker or the network.
        var total = Taken().Single(
            m => m.Name == MetricNames.PublishTotal && m.Tags[MetricNames.TagRoutingKey] == _q);
        Assert.Equal(MetricNames.OutcomeUnroutable, total.Tags[MetricNames.TagOutcome]);
    }

    [Fact]
    public async Task RecordsWhatAConsumerDidWithAMessage()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        using var consumer = await mq.ConsumeAsync<string>(
            _q, _ => Task.FromResult(Ack.DeadLetter("no")));
        await mq.Publisher<string>("", _q).SendAsync("hello");

        await Eventually(
            () => Taken().Any(m => m.Name == MetricNames.ConsumeTotal
                                   && m.Tags[MetricNames.TagQueue] == _q),
            "the delivery to be counted");

        // The handler gave up by name, so the outcome is `rejected`. The message
        // still went to the dead-letter queue; what changed in 0.6.0 is the word,
        // and `dead_lettered` is now the engine's give-ups only.
        var consumed = Taken().First(
            m => m.Name == MetricNames.ConsumeTotal && m.Tags[MetricNames.TagQueue] == _q);
        Assert.Equal(MetricNames.OutcomeRejected, consumed.Tags[MetricNames.TagOutcome]);
        Assert.DoesNotContain(Taken(), m =>
            m.Name == MetricNames.DeadLetteredTotal && m.Tags[MetricNames.TagQueue] == _q);
        Assert.Contains(Taken(), m =>
            m.Name == MetricNames.ConsumeAttempts && m.Tags[MetricNames.TagQueue] == _q);
        Assert.Contains(Taken(), m =>
            m.Name == MetricNames.ConsumeDuration && m.Tags[MetricNames.TagQueue] == _q);
    }

    [Fact]
    public async Task CarriesTheTraceFromThePublisherToTheConsumer()
    {
        using var source = new ActivitySource("test");
        using var recorder = new ActivityListener
        {
            ShouldListenTo = s => s.Name == MetricNames.ActivitySource || s.Name == "test",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(recorder);

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        string? consumerTrace = null;
        using var consumer = await mq.ConsumeAsync<string>(_q, _ =>
        {
            consumerTrace = Activity.Current?.TraceId.ToString();
            return Task.FromResult(Ack.Accept());
        });

        using var root = source.StartActivity("caller");
        var expected = root!.TraceId.ToString();
        await mq.Publisher<string>("", _q).SendAsync("hello");

        await Eventually(() => consumerTrace != null, "the consumer to run");

        // The publisher wrote traceparent into the envelope and the consumer picked
        // it up, so one trace spans both sides -- and a Java consumer reading the
        // same header joins the same trace.
        Assert.Equal(expected, consumerTrace);
    }

    [Fact]
    public async Task SaysDeadLetteredOnTheSpanOfAMessageThatRanOutOfAttempts()
    {
        // The assertion this test exists for.
        //
        // The handler asks for a retry every time -- it just throws -- so the ack
        // reaching the engine is always Ack.Retry. On the last attempt the policy
        // allows, the engine dead-letters it anyway. Up to 0.3.0 the span was tagged
        // from the handler's ack before the engine had decided, so this span said
        // outcome="retried" about a message nothing would ever try again, and anyone
        // querying a trace backend for dead letters found nothing at all.
        var spans = new List<Activity>();
        using var recorder = new ActivityListener
        {
            ShouldListenTo = s => s.Name == MetricNames.ActivitySource,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => { lock (spans) spans.Add(a); },
        };
        ActivitySource.AddActivityListener(recorder);

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        using var consumer = await mq.ConsumeAsync<string>(
            _q,
            ConsumerOptions.Defaults().WithRetry(
                RetryPolicy.Fixed(2, TimeSpan.FromMilliseconds(5))),
            _ => throw new InvalidOperationException("nope"));

        await mq.Publisher<string>("", _q).SendAsync("hello");

        await Eventually(
            () => Taken().Any(m => m.Name == MetricNames.DeadLetteredTotal
                                   && m.Tags[MetricNames.TagQueue] == _q),
            "the message to be given up on");

        Activity[] mine;
        lock (spans) mine = spans.Where(a => a.DisplayName == _q + MetricNames.SpanProcessSuffix).ToArray();

        // Every attempt but the last is a retry; the last one is the dead-letter.
        var final = mine.Single(
            a => a.GetTagItem(AceMqTelemetry.AttrOutcome) as string == MetricNames.OutcomeDeadLettered);
        Assert.Equal(MetricNames.OutcomeDeadLettered, final.GetTagItem(AceMqTelemetry.AttrOutcome));
        Assert.NotEqual(MetricNames.OutcomeRetried, final.GetTagItem(AceMqTelemetry.AttrOutcome) as string);

        // And the event carries the reason, so the trace says why it was given up on
        // rather than only that it was.
        var buried = final.Events.Single(e => e.Name == AceMqTelemetry.EventDeadLettered);
        var reason = buried.Tags.Single(t => t.Key == AceMqTelemetry.EventTagReason).Value as string;
        Assert.Contains("nope", reason);
        Assert.Contains(
            Naming.DeadLetterQueue(_q),
            buried.Tags.Single(t => t.Key == AceMqTelemetry.EventTagDestination).Value as string);

        // The counter agrees with the span. It used to count this as a retry.
        var counted = Taken().Single(
            m => m.Name == MetricNames.DeadLetteredTotal && m.Tags[MetricNames.TagQueue] == _q);
        Assert.Equal(MetricNames.OutcomeDeadLettered, counted.Tags[MetricNames.TagOutcome]);
    }

    [Fact]
    public async Task RecordsTheDelayTheEngineActuallyChoseOnARetry()
    {
        // The delay on the event is the policy's, not the handler's suggestion, so a
        // trace shows the wait that happened rather than the one that was asked for.
        var spans = new List<Activity>();
        using var recorder = new ActivityListener
        {
            ShouldListenTo = s => s.Name == MetricNames.ActivitySource,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => { lock (spans) spans.Add(a); },
        };
        ActivitySource.AddActivityListener(recorder);

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        var attempts = 0;
        using var consumer = await mq.ConsumeAsync<string>(
            _q,
            ConsumerOptions.Defaults().WithRetry(
                RetryPolicy.Fixed(3, TimeSpan.FromMilliseconds(40))),
            _ =>
            {
                if (Interlocked.Increment(ref attempts) == 1) throw new InvalidOperationException("once");
                return Task.FromResult(Ack.Accept());
            });

        await mq.Publisher<string>("", _q).SendAsync("hello");

        await Eventually(
            () => Taken().Any(m => m.Name == MetricNames.ConsumeTotal
                                   && m.Tags[MetricNames.TagQueue] == _q
                                   && m.Tags[MetricNames.TagOutcome] == MetricNames.OutcomeAcked),
            "the second attempt to succeed");

        Activity[] mine;
        lock (spans) mine = spans.Where(a => a.DisplayName == _q + MetricNames.SpanProcessSuffix).ToArray();

        var retried = mine.Single(
            a => a.GetTagItem(AceMqTelemetry.AttrOutcome) as string == MetricNames.OutcomeRetried);
        var scheduled = retried.Events.Single(e => e.Name == AceMqTelemetry.EventRetried);
        Assert.Equal(
            40L, scheduled.Tags.Single(t => t.Key == AceMqTelemetry.EventTagDelayMs).Value);
    }

    [Fact]
    public async Task TagsTheConsumeCounterAndTheSpanOfOneDeliveryWithTheSameOutcome()
    {
        // The invariant, asserted rather than reasoned about. RecordConsume takes the
        // outcome the settle decided and writes it to both the counter and the span,
        // in that order, so the two cannot drift -- but "cannot" is a claim about code
        // that changes, and this is what catches it changing. Before 0.4.0 the span
        // took its outcome from the handler's ack, so a message that used up its last
        // attempt had a span saying `retried` and a counter saying `dead_lettered`.
        var spans = new List<Activity>();
        using var recorder = new ActivityListener
        {
            ShouldListenTo = s => s.Name == MetricNames.ActivitySource,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => { lock (spans) spans.Add(a); },
        };
        ActivitySource.AddActivityListener(recorder);

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        // Two attempts, both throwing: attempt one is retried, attempt two exhausts
        // the ladder and is dead-lettered by the engine over the handler's head.
        using var consumer = await mq.ConsumeAsync<string>(
            _q,
            ConsumerOptions.Defaults().WithRetry(
                RetryPolicy.Fixed(2, TimeSpan.FromMilliseconds(5))),
            _ => throw new InvalidOperationException("nope"));

        await mq.Publisher<string>("", _q).SendAsync("hello");

        await Eventually(
            () => Taken().Count(m => m.Name == MetricNames.ConsumeTotal
                                     && m.Tags[MetricNames.TagQueue] == _q) == 2,
            "both attempts to be counted");

        // Ordered by attempt, so span n and measurement n are the same delivery. The
        // counter has no attempt tag -- deliberately, it would be a per-attempt series
        // -- so the pairing is by order, which is safe here because one message is
        // redelivered strictly sequentially.
        Activity[] mine;
        lock (spans)
        {
            mine = spans
                .Where(a => a.DisplayName == _q + MetricNames.SpanProcessSuffix)
                .OrderBy(a => (long)(a.GetTagItem(AceMqTelemetry.AttrAttempt) ?? 0L))
                .ToArray();
        }

        var counted = Taken()
            .Where(m => m.Name == MetricNames.ConsumeTotal && m.Tags[MetricNames.TagQueue] == _q)
            .Select(m => m.Tags[MetricNames.TagOutcome])
            .ToArray();

        Assert.Equal(2, mine.Length);
        Assert.Equal(counted.Length, mine.Length);

        var onSpans = mine.Select(a => a.GetTagItem(AceMqTelemetry.AttrOutcome) as string).ToArray();
        Assert.Equal(counted, onSpans);

        // And the last of them is the dead-letter, not a retry -- otherwise the two
        // could agree on the wrong answer and this test would still pass.
        Assert.Equal(
            new[] { MetricNames.OutcomeRetried, MetricNames.OutcomeDeadLettered },
            onSpans);
    }

    [Fact]
    public async Task SeparatesAHandlersOwnGiveUpFromTheEngineRunningOutOfAttempts()
    {
        // Both end in the dead-letter queue and only the word keeps them apart. A
        // handler calling Ack.DeadLetter is a decision somebody took about this
        // message and reports `rejected`; a retry policy running out is the engine
        // giving up and reports `dead_lettered`. This library and Java called both
        // `dead_lettered` until 0.6.0, so a dashboard could not tell an unprocessable
        // message from a dependency that was down -- which is the only question the
        // two counts are ever asked.
        var spans = new List<Activity>();
        using var recorder = new ActivityListener
        {
            ShouldListenTo = s => s.Name == MetricNames.ActivitySource,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => { lock (spans) spans.Add(a); },
        };
        ActivitySource.AddActivityListener(recorder);

        var refused = _q + ".refused";
        var exhausted = _q + ".exhausted";

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(refused);
        await mq.DeclareQueueAsync(exhausted);

        using var rejecting = await mq.ConsumeAsync<string>(
            refused,
            ConsumerOptions.Defaults().WithRetry(RetryPolicy.Fixed(5, TimeSpan.FromMilliseconds(5))),
            _ => Task.FromResult(Ack.DeadLetter("this order has no customer")));

        // One attempt permitted, and the handler asks for another: the engine decides
        // over the handler's head, which is the case that must stay `dead_lettered`.
        using var giving = await mq.ConsumeAsync<string>(
            exhausted,
            ConsumerOptions.Defaults().WithRetry(RetryPolicy.Fixed(1, TimeSpan.FromMilliseconds(5))),
            _ => throw new InvalidOperationException("the pricing service is down"));

        await mq.Publisher<string>("", refused).SendAsync("hello");
        await mq.Publisher<string>("", exhausted).SendAsync("hello");

        await Eventually(
            () => Outcomes(refused).Count == 1 && Outcomes(exhausted).Count == 1,
            "both deliveries to be counted");

        Assert.Equal(new[] { MetricNames.OutcomeRejected }, Outcomes(refused));
        Assert.Equal(new[] { MetricNames.OutcomeDeadLettered }, Outcomes(exhausted));

        // And the span says the same word as the counter, for each of them. The two
        // disagreeing is the failure this whole file exists to catch.
        Assert.Equal(MetricNames.OutcomeRejected, OutcomeOfSpan(spans, refused));
        Assert.Equal(MetricNames.OutcomeDeadLettered, OutcomeOfSpan(spans, exhausted));

        // Both still reach the dead-letter queue -- the reporting changed, not where
        // the message went -- and both still carry the event a trace search for dead
        // letters finds, which is why a rejection is not invisible to a trace backend.
        Assert.Contains(
            SpansFor(spans, refused).SelectMany(a => a.Events),
            e => e.Name == AceMqTelemetry.EventDeadLettered);

        var buried = new List<string>();
        using var drain = await mq.ConsumeAsync<string>(
            refused + Naming.DeadLetterSuffix,
            message => { lock (buried) buried.Add(message.Payload); return Task.FromResult(Ack.Accept()); });
        await Eventually(() => { lock (buried) return buried.Count == 1; },
            "the rejected message to be in the dead-letter queue");

        List<string> Outcomes(string queue) => Taken()
            .Where(m => m.Name == MetricNames.ConsumeTotal && m.Tags[MetricNames.TagQueue] == queue)
            .Select(m => m.Tags[MetricNames.TagOutcome])
            .ToList();

        static Activity[] SpansFor(List<Activity> recorded, string queue)
        {
            lock (recorded)
            {
                return recorded
                    .Where(a => a.DisplayName == queue + MetricNames.SpanProcessSuffix)
                    .ToArray();
            }
        }

        static string? OutcomeOfSpan(List<Activity> recorded, string queue) =>
            SpansFor(recorded, queue).Single().GetTagItem(AceMqTelemetry.AttrOutcome) as string;
    }

    [Fact]
    public async Task NamesEverySpanAttributeTheWayTheOtherFourLibrariesDo()
    {
        // OpenTelemetry's messaging semantic conventions, plus messaging.acemq.* for
        // the things they have no name for. This library used to put its metric tag
        // keys on spans -- `queue`, `outcome`, `acemq.attempt` -- so a trace query
        // written against Java, Go, Python or Ruby matched none of its spans.
        var spans = new List<Activity>();
        using var recorder = new ActivityListener
        {
            ShouldListenTo = s => s.Name == MetricNames.ActivitySource,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => { lock (spans) spans.Add(a); },
        };
        ActivitySource.AddActivityListener(recorder);

        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);
        using var consumer = await mq.ConsumeAsync<string>(_q, _ => Task.FromResult(Ack.Accept()));

        await mq.Publisher<string>("", _q).SendAsync("hello", Envelope.Of("Greeting").Build());

        await Eventually(
            () => Taken().Any(m => m.Name == MetricNames.ConsumeTotal
                                   && m.Tags[MetricNames.TagQueue] == _q),
            "the message to be handled");

        Activity publish, process;
        lock (spans)
        {
            publish = spans.Single(a => a.DisplayName == _q + MetricNames.SpanPublishSuffix);
            process = spans.Single(a => a.DisplayName == _q + MetricNames.SpanProcessSuffix);
        }

        Assert.Equal("rabbitmq", publish.GetTagItem(AceMqTelemetry.AttrSystem));
        Assert.Equal(_q, publish.GetTagItem(AceMqTelemetry.AttrDestination));
        Assert.Equal("publish", publish.GetTagItem(AceMqTelemetry.AttrOperation));
        Assert.Equal(_q, publish.GetTagItem(AceMqTelemetry.AttrRoutingKey));
        Assert.Equal("Greeting", publish.GetTagItem(AceMqTelemetry.AttrMessageType));
        Assert.NotNull(publish.GetTagItem(AceMqTelemetry.AttrMessageId));
        Assert.Equal(MetricNames.OutcomeConfirmed, publish.GetTagItem(AceMqTelemetry.AttrOutcome));

        Assert.Equal("rabbitmq", process.GetTagItem(AceMqTelemetry.AttrSystem));
        Assert.Equal(_q, process.GetTagItem(AceMqTelemetry.AttrDestination));
        Assert.Equal("process", process.GetTagItem(AceMqTelemetry.AttrOperation));
        Assert.Equal("Greeting", process.GetTagItem(AceMqTelemetry.AttrMessageType));
        Assert.Equal(1L, process.GetTagItem(AceMqTelemetry.AttrAttempt));
        Assert.Equal(MetricNames.OutcomeAcked, process.GetTagItem(AceMqTelemetry.AttrOutcome));

        // The metric keys are gone from the span, not merely joined by the new ones.
        // Both sets present would be a third vocabulary no other library writes.
        Assert.Null(process.GetTagItem(MetricNames.TagOutcome));
        Assert.Null(process.GetTagItem(MetricNames.TagQueue));
        Assert.Null(process.GetTagItem("acemq.attempt"));
    }

    [Fact]
    public async Task CountsWhatTheOutboxRelayPublishedAndHowFarBehindItWas()
    {
        // acemq.outbox.lag is the one number that reveals a stopped relay: a committed
        // unpublished row is a message that exists, is owed to somebody, and appears in
        // no queue depth anywhere. Java's OutboxRelay has reported it since 0.1; this
        // library published the records and told nobody.
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        var store = new InMemoryOutboxStore();
        await store.AddAsync(OutboxRecord.Of("", _q, Envelope.Of("Greeting").Build(), "hello"));

        using var relay = new OutboxRelay(mq, store);
        Assert.Equal(1, await relay.DrainOnceAsync());

        var counted = Taken().Single(
            m => m.Name == MetricNames.OutboxTotal && m.Tags[MetricNames.TagRoutingKey] == _q);
        Assert.Equal(MetricNames.OutcomePublished, counted.Tags[MetricNames.TagOutcome]);
        Assert.Equal(string.Empty, counted.Tags[MetricNames.TagExchange]);

        Assert.Contains(Taken(), m =>
            m.Name == MetricNames.OutboxLag && m.Tags[MetricNames.TagRoutingKey] == _q);
    }

    [Fact]
    public async Task CountsAPipelineRunAsCompletedWhenItsLastStepReturnsNothing()
    {
        // Two things at once, because they are the same bug. The last step of a route
        // is almost always a terminal action with nothing to return, and reading its
        // null as "ended early" reported every completed run as a filtered one -- so
        // Pipeline.Completed stayed at zero for a pipeline that worked perfectly.
        // Java's Pipeline asks "is there a step after this one" first for exactly this
        // reason, and now so does this one, and the run is reported either way.
        using var mq = await AceMqConnection.ConnectAsync(_url);

        var landed = new TaskCompletionSource<string>();
        using var pipeline = await mq.Pipeline<string>(_q)
            .Step("upper", (string order) => Task.FromResult<string?>(order.ToUpperInvariant()))
            .Step("finish", (string order) =>
            {
                landed.TrySetResult(order);
                // Nothing to hand on: this is the end of the route.
                return Task.FromResult<string?>(null);
            })
            .BuildAsync();

        await pipeline.SendAsync("hello");
        Assert.Equal("HELLO", await landed.Task.WaitAsync(TimeSpan.FromSeconds(5)));

        await Eventually(
            () => Taken().Any(m => m.Name == MetricNames.PipelineRunTotal
                                   && m.Tags[MetricNames.TagPipeline] == _q),
            "the pipeline run to be counted");

        var run = Taken().Single(
            m => m.Name == MetricNames.PipelineRunTotal && m.Tags[MetricNames.TagPipeline] == _q);
        Assert.Equal(MetricNames.OutcomeCompleted, run.Tags[MetricNames.TagOutcome]);
        Assert.Equal("finish", run.Tags[MetricNames.TagStep]);

        Assert.Contains(Taken(), m =>
            m.Name == MetricNames.PipelineRunDuration && m.Tags[MetricNames.TagPipeline] == _q);

        Assert.Equal(1L, pipeline.Completed);
        Assert.Equal(0L, pipeline.EndedEarly);
    }

    [Fact]
    public async Task TimesTheRoundTripARequesterActuallyWaitedFor()
    {
        // Neither the publish span nor the reply's delivery span was the thing the
        // caller waited for; the round trip was the gap between them, and a gap is not
        // a measurement. Java has had acemq.request.duration since 0.2.
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync(_q);

        using var responder = await mq.RespondAsync<string, string>(
            _q, request => Task.FromResult(request + "!"));
        using var requester = await mq.RequesterAsync();

        Assert.Equal("hello!", await requester.RequestAsync<string, string>("", _q, "hello"));

        var counted = Taken().Single(
            m => m.Name == MetricNames.RequestTotal && m.Tags[MetricNames.TagRoutingKey] == _q);
        Assert.Equal(MetricNames.OutcomeAnswered, counted.Tags[MetricNames.TagOutcome]);

        Assert.Contains(Taken(), m =>
            m.Name == MetricNames.RequestDuration && m.Tags[MetricNames.TagRoutingKey] == _q);
    }

    [Fact]
    public void NamesEveryMetricExactlyAsTheJavaLibraryDoes()
    {
        // Copied from org.acemq.amqp.api.MetricNames, and every member of it -- the
        // point of asserting all of them rather than a sample is that the file's own
        // claim to be identical rotted quietly once already, when Java grew the
        // request, outbox and pipeline names and this one did not.
        Assert.Equal("acemq.publish.duration", MetricNames.PublishDuration);
        Assert.Equal("acemq.publish.total", MetricNames.PublishTotal);
        Assert.Equal("acemq.consume.duration", MetricNames.ConsumeDuration);
        Assert.Equal("acemq.consume.total", MetricNames.ConsumeTotal);
        Assert.Equal("acemq.consume.attempts", MetricNames.ConsumeAttempts);
        Assert.Equal("acemq.consume.in.flight", MetricNames.ConsumeInFlight);
        Assert.Equal("acemq.messages.retried.total", MetricNames.RetriedTotal);
        Assert.Equal("acemq.messages.dead.lettered.total", MetricNames.DeadLetteredTotal);
        Assert.Equal("acemq.messages.set.aside.failed", MetricNames.SetAsideFailed);
        Assert.Equal("acemq.retry.rung.missing", MetricNames.RungMissing);
        Assert.Equal("acemq.request.duration", MetricNames.RequestDuration);
        Assert.Equal("acemq.request.total", MetricNames.RequestTotal);
        Assert.Equal("acemq.outbox.lag", MetricNames.OutboxLag);
        Assert.Equal("acemq.outbox.total", MetricNames.OutboxTotal);
        Assert.Equal("acemq.pipeline.run.duration", MetricNames.PipelineRunDuration);
        Assert.Equal("acemq.pipeline.run.total", MetricNames.PipelineRunTotal);

        Assert.Equal("exchange", MetricNames.TagExchange);
        Assert.Equal("routing.key", MetricNames.TagRoutingKey);
        Assert.Equal("queue", MetricNames.TagQueue);
        Assert.Equal("transport", MetricNames.TagTransport);
        Assert.Equal("message.type", MetricNames.TagMessageType);
        Assert.Equal("target", MetricNames.TagTarget);
        Assert.Equal("outcome", MetricNames.TagOutcome);
        Assert.Equal("pipeline", MetricNames.TagPipeline);
        Assert.Equal("step", MetricNames.TagStep);

        Assert.Equal("confirmed", MetricNames.OutcomeConfirmed);
        Assert.Equal("unroutable", MetricNames.OutcomeUnroutable);
        Assert.Equal("failed", MetricNames.OutcomeFailed);
        Assert.Equal("acked", MetricNames.OutcomeAcked);
        Assert.Equal("retried", MetricNames.OutcomeRetried);
        Assert.Equal("dead_lettered", MetricNames.OutcomeDeadLettered);
        Assert.Equal("parked", MetricNames.OutcomeParked);
        Assert.Equal("rejected", MetricNames.OutcomeRejected);
        Assert.Equal("answered", MetricNames.OutcomeAnswered);
        Assert.Equal("timed_out", MetricNames.OutcomeTimedOut);
        Assert.Equal("published", MetricNames.OutcomePublished);
        Assert.Equal("completed", MetricNames.OutcomeCompleted);
        Assert.Equal("ended_early", MetricNames.OutcomeEndedEarly);

        Assert.Equal(" publish", MetricNames.SpanPublishSuffix);
        Assert.Equal(" process", MetricNames.SpanProcessSuffix);
        Assert.Equal(" request", MetricNames.SpanRequestSuffix);
    }

    [Fact]
    public void NamesEverySpanEventTheWayTheOtherFourLibrariesDo()
    {
        // Java records these four as OpenTelemetry span events under exactly these
        // names, and Go, Python and Ruby copy them. This library used to prefix them
        // `acemq.` and hyphenate `dead-lettered`, so none of the four matched.
        Assert.Equal("message.retried", AceMqTelemetry.EventRetried);
        Assert.Equal("message.dead_lettered", AceMqTelemetry.EventDeadLettered);
        Assert.Equal("outbox.publish_failed", AceMqTelemetry.EventOutboxPublishFailed);
        Assert.Equal("pipeline.run_finished", AceMqTelemetry.EventPipelineRunFinished);

        Assert.Equal("messaging.system", AceMqTelemetry.AttrSystem);
        Assert.Equal("messaging.destination.name", AceMqTelemetry.AttrDestination);
        Assert.Equal("messaging.operation", AceMqTelemetry.AttrOperation);
        Assert.Equal("messaging.message.id", AceMqTelemetry.AttrMessageId);
        Assert.Equal("messaging.message.conversation_id", AceMqTelemetry.AttrConversationId);
        Assert.Equal("messaging.rabbitmq.destination.routing_key", AceMqTelemetry.AttrRoutingKey);
        Assert.Equal("messaging.acemq.message_type", AceMqTelemetry.AttrMessageType);
        Assert.Equal("messaging.acemq.attempt", AceMqTelemetry.AttrAttempt);
        Assert.Equal("messaging.acemq.outcome", AceMqTelemetry.AttrOutcome);
        Assert.Equal("messaging.acemq.reason", AceMqTelemetry.AttrReason);
        Assert.Equal("messaging.acemq.retry_delay_ms", AceMqTelemetry.AttrRetryDelayMs);
        Assert.Equal("messaging.acemq.outbox_lag_ms", AceMqTelemetry.AttrOutboxLagMs);
        Assert.Equal("messaging.acemq.run_age_ms", AceMqTelemetry.AttrRunAgeMs);
    }
}
