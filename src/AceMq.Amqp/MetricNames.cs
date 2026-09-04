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
/// Identical to <c>org.acemq.amqp.api.MetricNames</c>, character for character. That
/// is the point: a dashboard, an alert or a recording rule written against the Java
/// library works unchanged against this one, and a service rewritten from Java to C#
/// does not take its observability with it.
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

    public const string PublishDuration = "acemq.publish.duration";
    public const string PublishTotal = "acemq.publish.total";
    public const string ConsumeDuration = "acemq.consume.duration";
    public const string ConsumeTotal = "acemq.consume.total";
    public const string ConsumeAttempts = "acemq.consume.attempts";
    public const string ConsumeInFlight = "acemq.consume.in.flight";
    public const string RetriedTotal = "acemq.messages.retried.total";
    public const string DeadLetteredTotal = "acemq.messages.dead.lettered.total";

    public const string TagExchange = "exchange";
    public const string TagRoutingKey = "routing.key";
    public const string TagQueue = "queue";
    public const string TagTransport = "transport";
    public const string TagMessageType = "message.type";
    public const string TagOutcome = "outcome";

    public const string OutcomeConfirmed = "confirmed";
    public const string OutcomeUnroutable = "unroutable";
    public const string OutcomeFailed = "failed";
    public const string OutcomeAcked = "acked";
    public const string OutcomeRetried = "retried";
    public const string OutcomeDeadLettered = "dead_lettered";
    public const string OutcomeRejected = "rejected";

    /// <summary>Appended to the destination to name a publish span.</summary>
    public const string SpanPublishSuffix = " publish";

    /// <summary>Appended to the queue to name a processing span.</summary>
    public const string SpanProcessSuffix = " process";
}
