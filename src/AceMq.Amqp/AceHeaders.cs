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
/// <para>
/// <see cref="SharedPrefix"/> is the other half of the arrangement: names AceMQ
/// defines and does not reserve, because a pattern writes them and a handler has to
/// be able to read them.
/// </para>
/// </remarks>
public static class AceHeaders
{
    /// <summary>
    /// The engine's namespace, shared by every reserved AceMQ header.
    /// </summary>
    /// <remarks>
    /// <strong>Reserved.</strong> A header carrying this prefix is the engine's:
    /// materialised onto the <see cref="Envelope"/> if this version knows it, and
    /// dropped from the application's headers either way. Putting your own header
    /// here means writing it on publish and finding it gone on consume, with nothing
    /// reporting the loss.
    /// </remarks>
    public const string Prefix = "x-acemq-";

    /// <summary>
    /// The namespace AceMQ defines but does not reserve.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A header here is one AceMQ writes and reads, and an ordinary application
    /// header on the wire: <see cref="IsAceHeader"/> does not match it,
    /// <c>Envelope.Builder.Header</c> accepts it, and it reaches the handler like any
    /// other. That is the whole point. A responder has to read the reply address out
    /// of the message it was handed, and a handler has to be able to see that the
    /// message it is looking at came back off a dead-letter queue. Put either in
    /// <see cref="Prefix"/> and the engine eats it on the way in.
    /// </para>
    /// <para>
    /// Java, Go, Python and Ruby use exactly these strings for exactly these headers.
    /// This library put the replay three in the reserved namespace through 0.5.0, and
    /// was the last of the five to do so.
    /// </para>
    /// </remarks>
    public const string SharedPrefix = "acemq-";

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

    /// <summary>Queue a message was replayed from, set by <see cref="Replay"/>.</summary>
    /// <remarks>
    /// In <see cref="SharedPrefix"/> and not <see cref="Prefix"/>, deliberately. The
    /// engine does not materialise this onto an <see cref="Envelope"/>, so a reserved
    /// spelling of it would reach the wire and then be dropped before any handler saw
    /// it — which is the one question the stamp exists to answer.
    /// </remarks>
    public const string ReplayedFrom = SharedPrefix + "replayed-from";

    /// <summary>When the message was last replayed, as an RFC 3339 instant.</summary>
    /// <remarks>
    /// A string, unlike <see cref="FirstSeen"/>, which is an integer. The two
    /// timestamps on the wire are encoded differently and it is not an oversight to
    /// be tidied up here: all five libraries agree on both encodings.
    /// </remarks>
    public const string ReplayedAt = SharedPrefix + "replayed-at";

    /// <summary>How many times the message has been replayed.</summary>
    /// <remarks>
    /// Worth carrying separately from <see cref="Attempt"/>, which a replay resets. A
    /// message on its fifth trip through a dead-letter queue is saying something a
    /// fresh-looking attempt counter would hide.
    /// </remarks>
    public const string ReplayCount = SharedPrefix + "replay-count";

    /// <summary>What <see cref="ReplayedFrom"/> was called up to and including 0.5.0.</summary>
    /// <remarks>
    /// <para>
    /// Read, never written. A message replayed by a 0.5.0 service carries the reserved
    /// spelling, and forgetting it on the way past would throw away the provenance of
    /// every message already sitting on a queue at the moment of the upgrade.
    /// </para>
    /// <para>
    /// It is only ever visible on raw wire headers — <c>InboundDelivery.Headers</c> or
    /// <c>PulledMessage&lt;T&gt;.WireHeaders</c>. <see cref="Envelope.FromWire"/> drops
    /// it like every other reserved name it does not understand, which is exactly the
    /// bug these three constants exist to record.
    /// </para>
    /// </remarks>
    public const string LegacyReplayedFrom = Prefix + "replayed-from";

    /// <summary>What <see cref="ReplayedAt"/> was called up to and including 0.5.0.</summary>
    /// <remarks>Read, never written. See <see cref="LegacyReplayedFrom"/>.</remarks>
    public const string LegacyReplayedAt = Prefix + "replayed-at";

    /// <summary>What <see cref="ReplayCount"/> was called up to and including 0.5.0.</summary>
    /// <remarks>Read, never written. See <see cref="LegacyReplayedFrom"/>.</remarks>
    public const string LegacyReplayCount = Prefix + "replay-count";

    /// <summary>W3C trace context.</summary>
    public const string TraceParent = "traceparent";

    /// <summary>W3C trace state.</summary>
    public const string TraceState = "tracestate";

    /// <summary>Whether a header name belongs to the engine's reserved namespace.</summary>
    public static bool IsAceHeader(string headerName) =>
        headerName != null && headerName.StartsWith(Prefix, System.StringComparison.Ordinal);
}
