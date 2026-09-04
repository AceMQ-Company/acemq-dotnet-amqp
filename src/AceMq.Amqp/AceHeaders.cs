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
/// The header names AceMQ puts on the wire.
/// </summary>
/// <remarks>
/// <para>
/// Transliterated from <c>org.acemq.amqp.api.AceHeaders</c> and pinned by fixtures
/// generated from the Java implementation rather than copied from its documentation.
/// A port that hand-copies a wire contract acquires a difference nobody notices until
/// two languages disagree in production, which is the failure this whole arrangement
/// exists to prevent.
/// </para>
/// <para>
/// <strong><see cref="Prefix"/> is reserved.</strong> A header carrying it is the
/// engine's: it is materialised onto the <see cref="Envelope"/> if this version knows
/// it, and dropped from the application's headers either way. Use your own namespace
/// — <c>x-yourcompany-</c> — for anything that must survive the round trip.
/// </para>
/// </remarks>
public static class AceHeaders
{
    /// <summary>Prefix shared by every AceMQ-defined header.</summary>
    public const string Prefix = "x-acemq-";

    /// <summary>Unique message identifier, and the default idempotency key.</summary>
    public const string Id = Prefix + "id";

    /// <summary>Logical message type, for example <c>order.placed</c>.</summary>
    public const string Type = Prefix + "type";

    /// <summary>Schema version of the payload, as an integer.</summary>
    public const string Version = Prefix + "version";

    /// <summary>Business correlation identifier, propagated unchanged across hops.</summary>
    public const string Correlation = Prefix + "correlation";

    /// <summary>Identifier of the message that caused this one to be published.</summary>
    public const string Causation = Prefix + "causation";

    /// <summary>Delivery attempt counter, starting at 1.</summary>
    public const string Attempt = Prefix + "attempt";

    /// <summary>Epoch milliseconds of the first publish, used for age-based give-up.</summary>
    public const string FirstSeen = Prefix + "first-seen";

    /// <summary>Identifier of the publishing process, conventionally <c>service@host</c>.</summary>
    public const string Origin = Prefix + "origin";

    /// <summary>URI of the externalised payload when the claim-check pattern is in use.</summary>
    public const string Claim = Prefix + "claim";

    /// <summary>Why a message was dead-lettered. Present only in a dead-letter queue.</summary>
    public const string Error = Prefix + "error";

    /// <summary>Queue a message was replayed from.</summary>
    public const string ReplayedFrom = Prefix + "replayed-from";

    /// <summary>When the message was last replayed, as an ISO-8601 instant.</summary>
    /// <remarks>
    /// A string, unlike <see cref="FirstSeen"/>, which is an integer. The two
    /// timestamps on the wire are encoded differently and it is not an oversight to
    /// be tidied up here: matching the Java implementation is the entire point.
    /// </remarks>
    public const string ReplayedAt = Prefix + "replayed-at";

    /// <summary>How many times the message has been replayed.</summary>
    public const string ReplayCount = Prefix + "replay-count";

    /// <summary>W3C trace context.</summary>
    public const string TraceParent = "traceparent";

    /// <summary>W3C trace state.</summary>
    public const string TraceState = "tracestate";

    /// <summary>Whether a header name belongs to the engine's reserved namespace.</summary>
    public static bool IsAceHeader(string headerName) =>
        headerName != null && headerName.StartsWith(Prefix, System.StringComparison.Ordinal);
}
