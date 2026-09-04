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
}
