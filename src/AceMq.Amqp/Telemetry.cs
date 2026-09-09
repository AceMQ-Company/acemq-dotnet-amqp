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
    /// What <c>messaging.system</c> says on every span this library starts.
    /// </summary>
    /// <remarks>
    /// The same value the Java, Go, Python and Ruby libraries write. It names the
    /// broker protocol a trace backend groups by, not this library — a span tagged
    /// <c>acemq</c>, which is what this used to write, sorts into a messaging system
    /// of one and away from every other AMQP span in the same trace.
    /// </remarks>
    public const string DefaultSystem = "rabbitmq";

    // ---------- span attribute names ----------
    //
    // OpenTelemetry's messaging semantic conventions, plus messaging.acemq.* for the
    // three things those conventions have no name for. Identical to the keys Java's
    // OpenTelemetryTelemetry sets, and to Go's, Python's and Ruby's.
    //
    // These are not the metric tag keys, and the difference is deliberate on all five
    // libraries: a metric tag is short because it is repeated on every series
    // (`outcome`), a span attribute is namespaced because it shares a flat key space
    // with every other instrumentation in the process (`messaging.acemq.outcome`).
    // This library used to put the metric keys on its spans, so a trace query written
    // against any of the other four found nothing here.

    /// <summary>The messaging system a span belongs to.</summary>
    public const string AttrSystem = "messaging.system";

    /// <summary>Exchange published to, or queue consumed from.</summary>
    public const string AttrDestination = "messaging.destination.name";

    /// <summary>One of <c>publish</c>, <c>process</c>, <c>request</c>.</summary>
    public const string AttrOperation = "messaging.operation";

    /// <summary>The envelope id.</summary>
    public const string AttrMessageId = "messaging.message.id";

    /// <summary>The envelope's correlation id.</summary>
    public const string AttrConversationId = "messaging.message.conversation_id";

    /// <summary>The routing key a publish used.</summary>
    public const string AttrRoutingKey = "messaging.rabbitmq.destination.routing_key";

    /// <summary>The envelope's logical message type.</summary>
    public const string AttrMessageType = "messaging.acemq.message_type";

    /// <summary>Which attempt this delivery was.</summary>
    public const string AttrAttempt = "messaging.acemq.attempt";

    /// <summary>What became of the operation; one of the <c>MetricNames.Outcome*</c> values.</summary>
    public const string AttrOutcome = "messaging.acemq.outcome";

    /// <summary>Why, as the handler or the policy put it. Unbounded text a metric would not tolerate.</summary>
    public const string AttrReason = "messaging.acemq.reason";

    /// <summary>How long the retry the engine just scheduled will wait, in milliseconds.</summary>
    public const string AttrRetryDelayMs = "messaging.acemq.retry_delay_ms";

    /// <summary>How long an outbox record waited to be published, in milliseconds.</summary>
    public const string AttrOutboxLagMs = "messaging.acemq.outbox_lag_ms";

    /// <summary>How old the message was when it left a pipeline, in milliseconds.</summary>
    public const string AttrRunAgeMs = "messaging.acemq.run_age_ms";

    // ---------- span event names ----------

    /// <summary>The span event recorded when the engine schedules another attempt.</summary>
    public const string EventRetried = "message.retried";

    /// <summary>The span event recorded when the engine gives up on a message.</summary>
    public const string EventDeadLettered = "message.dead_lettered";

    /// <summary>The span event recorded when the outbox relay could not publish a record.</summary>
    public const string EventOutboxPublishFailed = "outbox.publish_failed";

    /// <summary>The span event recorded when a pipeline run ends.</summary>
    public const string EventPipelineRunFinished = "pipeline.run_finished";

    /// <summary>The queue the message was moved to.</summary>
    /// <remarks>The same key as <see cref="AttrDestination"/>, named for its use on an event.</remarks>
    public const string EventTagDestination = AttrDestination;

    /// <summary>How long the retry the engine just scheduled will wait, in milliseconds.</summary>
    public const string EventTagDelayMs = AttrRetryDelayMs;

    /// <summary>Why, as the handler or the policy put it.</summary>
    public const string EventTagReason = AttrReason;

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

    internal static readonly Histogram<double> RequestDuration = Meter.CreateHistogram<double>(
        MetricNames.RequestDuration, "s", "Round trip of a request, as the caller experienced it");

    internal static readonly Counter<long> RequestTotal = Meter.CreateCounter<long>(
        MetricNames.RequestTotal, "requests", "Request/reply calls, by outcome");

    internal static readonly Histogram<double> OutboxLag = Meter.CreateHistogram<double>(
        MetricNames.OutboxLag, "s",
        "How long an outbox record waited between being committed and published");

    internal static readonly Counter<long> OutboxTotal = Meter.CreateCounter<long>(
        MetricNames.OutboxTotal, "records", "Outbox records the relay has handled, by outcome");

    internal static readonly Histogram<double> PipelineRunDuration = Meter.CreateHistogram<double>(
        MetricNames.PipelineRunDuration, "s", "How long a message had existed when it left a pipeline");

    internal static readonly Counter<long> PipelineRunTotal = Meter.CreateCounter<long>(
        MetricNames.PipelineRunTotal, "runs", "Pipeline runs that finished, by outcome");

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
        string exchange, string routingKey, Envelope envelope, IDictionary<string, object> headers)
    {
        var destination = exchange.Length == 0 ? routingKey : exchange;
        var activity = Activity.StartActivity(
            destination + MetricNames.SpanPublishSuffix, ActivityKind.Producer);
        if (activity == null) return null;

        activity.SetTag(AttrSystem, DefaultSystem);
        activity.SetTag(AttrDestination, destination);
        activity.SetTag(AttrOperation, "publish");
        activity.SetTag(AttrMessageId, envelope.Id);
        activity.SetTag(AttrConversationId, envelope.CorrelationId);
        activity.SetTag(AttrRoutingKey, routingKey);
        activity.SetTag(AttrMessageType, envelope.Type);

        headers[AceHeaders.TraceParent] = activity.Id ?? string.Empty;
        if (!string.IsNullOrEmpty(activity.TraceStateString))
        {
            headers[AceHeaders.TraceState] = activity.TraceStateString!;
        }
        return activity;
    }

    /// <summary>Starts a processing span, continuing the publisher's trace if there is one.</summary>
    internal static Activity? StartConsume(
        string queue, Envelope envelope, IReadOnlyDictionary<string, object> headers)
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
        if (activity == null) return null;

        activity.SetTag(AttrSystem, DefaultSystem);
        activity.SetTag(AttrDestination, queue);
        activity.SetTag(AttrOperation, "process");
        activity.SetTag(AttrMessageId, envelope.Id);
        activity.SetTag(AttrConversationId, envelope.CorrelationId);
        activity.SetTag(AttrMessageType, envelope.Type);
        activity.SetTag(AttrAttempt, (long)envelope.Attempt);
        return activity;
    }

    /// <summary>
    /// Starts a span covering a whole request/reply round trip.
    /// </summary>
    /// <remarks>
    /// A <see cref="ActivityKind.Client"/> span with the publish and the reply's
    /// delivery as its children, because neither of those two was the thing the caller
    /// waited for: "how long did asking take" was the gap between them, and a gap is
    /// not a measurement. Java's <c>Requester</c> opens the same span.
    /// </remarks>
    internal static Activity? StartRequest(string destination, Envelope envelope)
    {
        var activity = Activity.StartActivity(
            destination + MetricNames.SpanRequestSuffix, ActivityKind.Client);
        if (activity == null) return null;

        activity.SetTag(AttrSystem, DefaultSystem);
        activity.SetTag(AttrDestination, destination);
        activity.SetTag(AttrOperation, "request");
        activity.SetTag(AttrMessageId, envelope.Id);
        activity.SetTag(AttrConversationId, envelope.CorrelationId);
        activity.SetTag(AttrMessageType, envelope.Type);
        return activity;
    }

    /// <summary>
    /// Records how an operation ended, on the span and in the counter that pairs with it.
    /// </summary>
    /// <remarks>
    /// One call sets both, because the two disagreeing is the failure this library keeps
    /// finding: a span with no outcome where the counter said <c>failed</c> means a
    /// dashboard shows failures a trace search cannot find.
    /// </remarks>
    internal static void Outcome(Activity? span, string outcome)
    {
        if (span == null) return;
        span.SetTag(AttrOutcome, outcome);
        if (outcome == MetricNames.OutcomeUnroutable
            || outcome == MetricNames.OutcomeFailed
            || outcome == MetricNames.OutcomeDeadLettered
            || outcome == MetricNames.OutcomeTimedOut)
        {
            span.SetStatus(ActivityStatusCode.Error, outcome);
        }
    }

    /// <summary>Records an outbox record that reached the broker, and how far behind it was.</summary>
    internal static void OutboxPublished(string exchange, string routingKey, TimeSpan lag)
    {
        var tags = new TagList
        {
            { MetricNames.TagExchange, exchange ?? string.Empty },
            { MetricNames.TagRoutingKey, routingKey ?? string.Empty },
        };

        // A histogram rather than a counter, because the question is never "how many"
        // but "how far behind", and a percentile answers that where a total does not.
        OutboxLag.Record(lag.TotalSeconds, tags);

        var counted = tags;
        counted.Add(MetricNames.TagOutcome, MetricNames.OutcomePublished);
        OutboxTotal.Add(1, counted);

        var current = System.Diagnostics.Activity.Current;
        if (current != null)
        {
            current.SetTag(AttrOutboxLagMs, (long)lag.TotalMilliseconds);
        }
    }

    /// <summary>Records an outbox record the relay could not publish.</summary>
    internal static void OutboxFailed(string exchange, string routingKey, string reason)
    {
        // The reason is not a tag, for the same cardinality reason a dead-letter reason
        // is not one. It belongs on the event, which tolerates unbounded text.
        var tags = new TagList
        {
            { MetricNames.TagExchange, exchange ?? string.Empty },
            { MetricNames.TagRoutingKey, routingKey ?? string.Empty },
            { MetricNames.TagOutcome, MetricNames.OutcomeFailed },
        };
        OutboxTotal.Add(1, tags);

        var current = System.Diagnostics.Activity.Current;
        if (current == null) return;
        current.AddEvent(new ActivityEvent(EventOutboxPublishFailed, tags: new ActivityTagsCollection
        {
            { AttrDestination, exchange ?? string.Empty },
            { AttrReason, reason ?? string.Empty },
        }));
    }

    /// <summary>Records a pipeline run that ended, at the step it ended on.</summary>
    /// <remarks>
    /// The age is the envelope's, so this is the whole run rather than this step: the
    /// envelope was created when the message entered and carried through every hop.
    /// </remarks>
    internal static void PipelineRunFinished(
        string pipeline, string step, string outcome, TimeSpan age)
    {
        var tags = new TagList
        {
            { MetricNames.TagPipeline, pipeline },
            { MetricNames.TagStep, step },
            { MetricNames.TagOutcome, outcome },
        };
        PipelineRunDuration.Record(age.TotalSeconds, tags);
        PipelineRunTotal.Add(1, tags);

        var current = System.Diagnostics.Activity.Current;
        if (current == null) return;

        // An event on whatever span is current rather than a span of its own: the run
        // is already covered by the step's processing span, and a zero-length span at
        // the end of a trace adds a row and no information. The three keys here are
        // bare, not namespaced, and match Java's -- `outcome` on this event is the
        // pipeline's outcome, a different thing from the span's own
        // messaging.acemq.outcome, which is still `acked`.
        current.AddEvent(new ActivityEvent(EventPipelineRunFinished, tags: new ActivityTagsCollection
        {
            { MetricNames.TagPipeline, pipeline },
            { MetricNames.TagStep, step },
            { MetricNames.TagOutcome, outcome },
            { AttrRunAgeMs, (long)age.TotalMilliseconds },
        }));
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

        Outcome(span, MetricNames.OutcomeRetried);
        span.AddEvent(new ActivityEvent(EventRetried, tags: new ActivityTagsCollection
        {
            { EventTagDestination, destination },
            { EventTagDelayMs, (long)delay.TotalMilliseconds },
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

        Outcome(span, MetricNames.OutcomeDeadLettered);
        span.AddEvent(new ActivityEvent(EventDeadLettered, tags: new ActivityTagsCollection
        {
            { EventTagDestination, destination },
            { EventTagReason, reason },
        }));
    }
}
