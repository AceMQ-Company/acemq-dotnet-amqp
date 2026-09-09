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
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace AceMq.Amqp;

/// <summary>
/// The instruments this library records to.
/// </summary>
/// <remarks>
/// <para>
/// Built on <see cref="System.Diagnostics.Metrics.Meter"/> and
/// <see cref="System.Diagnostics.ActivitySource"/> — the runtime's own instrumentation
/// APIs — and on nothing else. There is no OpenTelemetry dependency and no Prometheus
/// dependency, because the application has to own its exporter and its SDK version.
/// A library that pins those causes exactly the version conflicts that make it painful
/// to adopt.
/// </para>
/// <para>
/// Instrumentation is always on. An instrument nobody is listening to costs a
/// null check per call, so there is no switch to forget to turn on in production and
/// no configuration that makes the metrics disappear.
/// </para>
/// <para>
/// To collect: point the OpenTelemetry SDK at the meter named
/// <see cref="MetricNames.Meter"/>, or use <c>AceMq.Amqp.Diagnostics</c>, which serves
/// them over HTTP without an SDK at all.
/// </para>
/// </remarks>
public static class AceMqTelemetry
{
    /// <summary>
    /// The span event recorded when the engine schedules another attempt.
    /// </summary>
    /// <remarks>
    /// Not in <see cref="MetricNames"/>, which is kept character for character
    /// identical to Java's. Java carries the same two moments as methods on its
    /// <c>Telemetry</c> interface — <c>messageRetried</c> and
    /// <c>messageDeadLettered</c> — rather than as named constants, so these two
    /// strings live here instead of pretending to a contract Java does not have.
    /// </remarks>
    public const string EventRetried = "acemq.message.retried";

    /// <summary>The span event recorded when the engine gives up on a message.</summary>
    public const string EventDeadLettered = "acemq.message.dead-lettered";

    /// <summary>How long the retry the engine just scheduled will wait, in milliseconds.</summary>
    public const string EventTagDelayMs = "acemq.retry.delay.ms";

    /// <summary>The queue the message was moved to.</summary>
    public const string EventTagDestination = "acemq.destination";

    /// <summary>Why, as the handler or the policy put it.</summary>
    public const string EventTagReason = "acemq.reason";

    internal static readonly Meter Meter = new Meter(MetricNames.Meter, ThisVersion());

    internal static readonly ActivitySource Activity =
        new ActivitySource(MetricNames.ActivitySource, ThisVersion());

    internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
        MetricNames.PublishDuration, "s", "How long a publish took, including the broker's confirmation");

    internal static readonly Counter<long> PublishTotal = Meter.CreateCounter<long>(
        MetricNames.PublishTotal, "messages", "Messages published, by outcome");

    internal static readonly Histogram<double> ConsumeDuration = Meter.CreateHistogram<double>(
        MetricNames.ConsumeDuration, "s", "How long a handler took");

    internal static readonly Counter<long> ConsumeTotal = Meter.CreateCounter<long>(
        MetricNames.ConsumeTotal, "messages", "Messages handled, by outcome");

    internal static readonly Histogram<int> ConsumeAttempts = Meter.CreateHistogram<int>(
        MetricNames.ConsumeAttempts, "attempts", "Which attempt a message was handled on");

    internal static readonly Counter<long> RetriedTotal = Meter.CreateCounter<long>(
        MetricNames.RetriedTotal, "messages", "Messages sent back for another attempt");

    internal static readonly Counter<long> DeadLetteredTotal = Meter.CreateCounter<long>(
        MetricNames.DeadLetteredTotal, "messages", "Messages given up on");

    private static long _inFlight;

    static AceMqTelemetry()
    {
        // Observable, because in-flight is a level rather than an event: asking for
        // it when the exporter collects is cheaper and more accurate than tracking a
        // gauge on every delivery.
        Meter.CreateObservableGauge(
            MetricNames.ConsumeInFlight,
            () => System.Threading.Interlocked.Read(ref _inFlight),
            "messages", "Messages currently being handled");
    }

    internal static void EnteredHandler() => System.Threading.Interlocked.Increment(ref _inFlight);

    internal static void LeftHandler() => System.Threading.Interlocked.Decrement(ref _inFlight);

    /// <summary>Messages currently inside a handler.</summary>
    public static long InFlight => System.Threading.Interlocked.Read(ref _inFlight);

    private static string ThisVersion() =>
        typeof(AceMqTelemetry).Assembly.GetName().Version?.ToString() ?? "0.0.0";

    /// <summary>
    /// Starts a publish span and writes the trace context into the envelope's headers.
    /// </summary>
    /// <remarks>
    /// The envelope already reserves <c>traceparent</c> and <c>tracestate</c>, so a
    /// trace started here continues in whatever consumes the message — including a
    /// Java consumer, which reads the same two headers.
    /// </remarks>
    internal static Activity? StartPublish(
        string exchange, string routingKey, IDictionary<string, object> headers)
    {
        var destination = exchange.Length == 0 ? routingKey : exchange;
        var activity = Activity.StartActivity(
            destination + MetricNames.SpanPublishSuffix, ActivityKind.Producer);
        if (activity == null) return null;

        activity.SetTag("messaging.system", "acemq");
        activity.SetTag("messaging.destination.name", destination);
        activity.SetTag(MetricNames.TagRoutingKey, routingKey);

        headers[AceHeaders.TraceParent] = activity.Id ?? string.Empty;
        if (!string.IsNullOrEmpty(activity.TraceStateString))
        {
            headers[AceHeaders.TraceState] = activity.TraceStateString!;
        }
        return activity;
    }

    /// <summary>Starts a processing span, continuing the publisher's trace if there is one.</summary>
    internal static Activity? StartConsume(string queue, IReadOnlyDictionary<string, object> headers)
    {
        ActivityContext parent = default;
        if (headers.TryGetValue(AceHeaders.TraceParent, out var raw) && raw != null)
        {
            headers.TryGetValue(AceHeaders.TraceState, out var state);
            ActivityContext.TryParse(
                Convert.ToString(raw, System.Globalization.CultureInfo.InvariantCulture),
                state == null
                    ? null
                    : Convert.ToString(state, System.Globalization.CultureInfo.InvariantCulture),
                out parent);
        }

        var activity = Activity.StartActivity(
            queue + MetricNames.SpanProcessSuffix, ActivityKind.Consumer, parent);
        activity?.SetTag("messaging.system", "acemq");
        activity?.SetTag(MetricNames.TagQueue, queue);
        return activity;
    }

    /// <summary>
    /// Records that the engine chose another attempt, and how long that attempt waits.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Called at the moment the decision is made rather than at the moment the handler
    /// returns, because they are not the same moment and the difference is where the
    /// old bug lived: a handler asking for a retry on its last permitted attempt is
    /// dead-lettered, and a span tagged from the handler's answer said <c>retried</c>
    /// about a message nothing would ever try again.
    /// </para>
    /// <para>
    /// The delay is the one actually chosen — the policy's, not the handler's
    /// suggestion — so a trace shows the wait that happened.
    /// </para>
    /// </remarks>
    internal static void MessageRetried(
        Activity? span, TimeSpan delay, string destination, string reason)
    {
        // Nothing listening means no Activity was ever created, so this is a null
        // check and a return. The ActivityEvent and its tag collection are only
        // allocated for a span something is going to read.
        if (span == null) return;

        span.SetTag(MetricNames.TagOutcome, MetricNames.OutcomeRetried);
        span.AddEvent(new ActivityEvent(EventRetried, tags: new ActivityTagsCollection
        {
            { EventTagDelayMs, (long)delay.TotalMilliseconds },
            { EventTagDestination, destination },
            { EventTagReason, reason },
        }));
    }

    /// <summary>Records that the engine gave up on a message, and why.</summary>
    /// <remarks>
    /// The outcome tag is set here as well as counted afterwards, so the span of a
    /// message dead-lettered on a path that records no metrics — a body that would
    /// not decode, parked before a handler ever ran — still says what happened to it.
    /// </remarks>
    internal static void MessageDeadLettered(
        Activity? span, string destination, string reason)
    {
        if (span == null) return;

        span.SetTag(MetricNames.TagOutcome, MetricNames.OutcomeDeadLettered);
        span.AddEvent(new ActivityEvent(EventDeadLettered, tags: new ActivityTagsCollection
        {
            { EventTagDestination, destination },
            { EventTagReason, reason },
        }));
    }
}
