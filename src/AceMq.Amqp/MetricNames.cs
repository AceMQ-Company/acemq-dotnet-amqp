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

namespace AceMq.Amqp;

/// <summary>
/// The metric and tag names this library reports.
/// </summary>
/// <remarks>
/// <para>
/// Every string below is identical to its counterpart in
/// <c>org.acemq.amqp.api.MetricNames</c>, character for character. That is the point:
/// a dashboard, an alert or a recording rule written against the Java library works
/// unchanged against this one, and a service rewritten from Java to C# does not take
/// its observability with it. <c>MetricNamesTests</c> asserts every one of them, so
/// the claim cannot rot quietly the way it did when this file was missing the request,
/// outbox and pipeline names it now carries.
/// </para>
/// <para>
/// <see cref="Meter"/> and <see cref="ActivitySource"/> have no Java counterpart, and
/// are the only two members here that do not: they name the .NET plumbing an exporter
/// subscribes to, where Java names a Micrometer registry and an OpenTelemetry tracer
/// the application already owns.
/// </para>
/// <para>
/// The names are dotted here and appear underscored in Prometheus —
/// <c>acemq.publish.duration</c> is scraped as <c>acemq_publish_duration_seconds</c>.
/// That translation is the exporter's, not this library's, and the same translation
/// happens on the Java side.
/// </para>
/// </remarks>
public static class MetricNames
{
    /// <summary>Name of the meter every instrument below belongs to.</summary>
    public const string Meter = "AceMq.Amqp";

    /// <summary>Name of the activity source spans are created on.</summary>
    public const string ActivitySource = "AceMq.Amqp";

    /// <summary>Time from calling send to the broker confirming, tagged with the outcome.</summary>
    public const string PublishDuration = "acemq.publish.duration";

    /// <summary>Messages published, tagged with the outcome.</summary>
    public const string PublishTotal = "acemq.publish.total";

    /// <summary>Time spent in a handler, tagged with the outcome.</summary>
    public const string ConsumeDuration = "acemq.consume.duration";

    /// <summary>Deliveries handled, tagged with the outcome.</summary>
    public const string ConsumeTotal = "acemq.consume.total";

    /// <summary>Which attempt a delivery was, so a rising distribution shows a struggling dependency.</summary>
    public const string ConsumeAttempts = "acemq.consume.attempts";

    /// <summary>Deliveries currently in a handler, bounded by prefetch times concurrency.</summary>
    public const string ConsumeInFlight = "acemq.consume.in.flight";

    /// <summary>Messages sent to a retry queue.</summary>
    public const string RetriedTotal = "acemq.messages.retried.total";

    /// <summary>Messages the engine gave up on, or parked.</summary>
    /// <remarks>
    /// A message a handler rejected by name is not counted here — it is counted on
    /// <see cref="ConsumeTotal"/> under <see cref="OutcomeRejected"/>, the same
    /// division Go, Python and Ruby make. Both still reach the dead-letter queue;
    /// this counter is the engine's give-ups, so an alert on it is an alert on a
    /// retry policy running out rather than on a handler doing its job.
    /// </remarks>
    public const string DeadLetteredTotal = "acemq.messages.dead.lettered.total";

    /// <summary>Round trip of a request/reply call, as the caller experienced it.</summary>
    public const string RequestDuration = "acemq.request.duration";

    /// <summary>Request/reply calls, tagged with <see cref="TagOutcome"/>.</summary>
    public const string RequestTotal = "acemq.request.total";

    /// <summary>
    /// How long an outbox record waited between being committed and being published.
    /// </summary>
    /// <remarks>
    /// The one number that reveals a stopped relay. A committed, unpublished row is a
    /// message that exists and is owed to somebody, and it appears in no queue depth
    /// anywhere.
    /// </remarks>
    public const string OutboxLag = "acemq.outbox.lag";

    /// <summary>Outbox records the relay has handled, tagged with <see cref="TagOutcome"/>.</summary>
    public const string OutboxTotal = "acemq.outbox.total";

    /// <summary>How long a message had existed when it left a pipeline.</summary>
    public const string PipelineRunDuration = "acemq.pipeline.run.duration";

    /// <summary>
    /// Pipeline runs that finished, tagged with <see cref="TagOutcome"/> and
    /// <see cref="TagStep"/>.
    /// </summary>
    public const string PipelineRunTotal = "acemq.pipeline.run.total";

    // ---------- tag keys ----------

    /// <summary>Exchange a message was published to; empty string for the default exchange.</summary>
    public const string TagExchange = "exchange";

    /// <summary>Routing key used, or the queue name when publishing without an exchange.</summary>
    public const string TagRoutingKey = "routing.key";

    /// <summary>Queue a delivery came from.</summary>
    public const string TagQueue = "queue";

    /// <summary>Transport short name, such as <c>rabbitmq</c>.</summary>
    public const string TagTransport = "transport";

    /// <summary>Logical message type from the envelope.</summary>
    public const string TagMessageType = "message.type";

    /// <summary>What happened. One of the <c>Outcome*</c> constants below.</summary>
    public const string TagOutcome = "outcome";

    /// <summary>Pipeline a run belongs to.</summary>
    public const string TagPipeline = "pipeline";

    /// <summary>Step a pipeline run was at when it finished.</summary>
    public const string TagStep = "step";

    // ---------- outcome values ----------

    public const string OutcomeConfirmed = "confirmed";
    public const string OutcomeUnroutable = "unroutable";
    public const string OutcomeFailed = "failed";
    public const string OutcomeAcked = "acked";
    public const string OutcomeRetried = "retried";

    /// <summary>
    /// The engine gave up: the attempts ran out or the message aged past the policy.
    /// </summary>
    /// <remarks>
    /// Not a handler's own decision — that is <see cref="OutcomeRejected"/>. Both end
    /// in the dead-letter queue and only the word keeps them apart.
    /// </remarks>
    public const string OutcomeDeadLettered = "dead_lettered";

    /// <summary>A handler gave up on the message by name, or released it unhandled.</summary>
    /// <remarks>
    /// <c>Ack.DeadLetter</c> reports this rather than <see cref="OutcomeDeadLettered"/>,
    /// which is what Go, Python and Ruby have always reported for the same decision.
    /// </remarks>
    public const string OutcomeRejected = "rejected";
    public const string OutcomeAnswered = "answered";
    public const string OutcomeTimedOut = "timed_out";
    public const string OutcomePublished = "published";
    public const string OutcomeCompleted = "completed";
    public const string OutcomeEndedEarly = "ended_early";

    // ---------- span names ----------

    /// <summary>Appended to the destination to name a publish span.</summary>
    public const string SpanPublishSuffix = " publish";

    /// <summary>Appended to the queue to name a processing span.</summary>
    public const string SpanProcessSuffix = " process";

    /// <summary>Appended to the destination to name a request/reply span.</summary>
    public const string SpanRequestSuffix = " request";
}
