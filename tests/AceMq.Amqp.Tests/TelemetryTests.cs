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
            () => Taken().Any(m => m.Name == MetricNames.DeadLetteredTotal
                                   && m.Tags[MetricNames.TagQueue] == _q),
            "the dead-letter to be counted");

        var consumed = Taken().First(
            m => m.Name == MetricNames.ConsumeTotal && m.Tags[MetricNames.TagQueue] == _q);
        Assert.Equal(MetricNames.OutcomeDeadLettered, consumed.Tags[MetricNames.TagOutcome]);
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
    public void NamesEveryMetricExactlyAsTheJavaLibraryDoes()
    {
        // Copied from org.acemq.amqp.api.MetricNames. If one of these changes, a
        // dashboard written for the other language stops matching.
        Assert.Equal("acemq.publish.duration", MetricNames.PublishDuration);
        Assert.Equal("acemq.publish.total", MetricNames.PublishTotal);
        Assert.Equal("acemq.consume.duration", MetricNames.ConsumeDuration);
        Assert.Equal("acemq.consume.total", MetricNames.ConsumeTotal);
        Assert.Equal("acemq.consume.attempts", MetricNames.ConsumeAttempts);
        Assert.Equal("acemq.consume.in.flight", MetricNames.ConsumeInFlight);
        Assert.Equal("acemq.messages.retried.total", MetricNames.RetriedTotal);
        Assert.Equal("acemq.messages.dead.lettered.total", MetricNames.DeadLetteredTotal);
        Assert.Equal("exchange", MetricNames.TagExchange);
        Assert.Equal("routing.key", MetricNames.TagRoutingKey);
        Assert.Equal("queue", MetricNames.TagQueue);
        Assert.Equal("outcome", MetricNames.TagOutcome);
        Assert.Equal("dead_lettered", MetricNames.OutcomeDeadLettered);
    }
}
