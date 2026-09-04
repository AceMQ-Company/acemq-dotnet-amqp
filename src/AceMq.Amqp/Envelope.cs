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
using System.Globalization;

namespace AceMq.Amqp;

/// <summary>
/// What travels with a message besides its body: identity, causation, and the
/// counters the retry engine keeps.
/// </summary>
/// <remarks>
/// The defaults are part of the wire contract, not conveniences, and are pinned by
/// the fixtures in the test project: a type that falls back to the routing key, a
/// correlation that falls back to the id, an origin of <c>acemq@{host}</c>, version
/// and attempt starting at 1.
/// </remarks>
public sealed class Envelope
{
    private Envelope(
        string id, string type, int version, string correlationId, string? causationId,
        int attempt, DateTimeOffset firstSeen, string? origin, string? error,
        IReadOnlyDictionary<string, object> headers)
    {
        Id = id;
        Type = type;
        Version = version;
        CorrelationId = correlationId;
        CausationId = causationId;
        Attempt = attempt;
        FirstSeen = firstSeen;
        Origin = origin;
        Error = error;
        Headers = headers;
    }

    /// <summary>Unique message identifier, and the default idempotency key.</summary>
    public string Id { get; }

    /// <summary>Logical message type. Defaults to the routing key when not set.</summary>
    public string Type { get; }

    /// <summary>Schema version of the payload. Starts at 1.</summary>
    public int Version { get; }

    /// <summary>Correlation identifier. Defaults to <see cref="Id"/> when not set.</summary>
    public string CorrelationId { get; }

    /// <summary>The message that caused this one, when there was one.</summary>
    public string? CausationId { get; }

    /// <summary>Delivery attempt, starting at 1.</summary>
    public int Attempt { get; }

    /// <summary>When the message was first published.</summary>
    public DateTimeOffset FirstSeen { get; }

    /// <summary>The publishing process, conventionally <c>service@host</c>.</summary>
    public string? Origin { get; }

    /// <summary>Why the message was dead-lettered, when it was.</summary>
    public string? Error { get; }

    /// <summary>Application headers. Never contains anything in the reserved namespace.</summary>
    public IReadOnlyDictionary<string, object> Headers { get; }

    /// <summary>Starts building an envelope for a message type.</summary>
    public static Builder Of(string type) => new Builder(type);

    /// <summary>
    /// Reads an envelope back off the wire.
    /// </summary>
    /// <param name="headers">Every header on the message, engine and application alike.</param>
    /// <param name="routingKey">Used for <see cref="Type"/> when the header is absent.</param>
    /// <param name="messageId">Used for <see cref="Id"/> when the header is absent.</param>
    public static Envelope FromWire(
        IReadOnlyDictionary<string, object> headers, string? routingKey = null, string? messageId = null)
    {
        if (headers == null) throw new ArgumentNullException(nameof(headers));

        var application = new Dictionary<string, object>();
        foreach (var pair in headers)
        {
            // Reserved headers are the engine's and never reach the application,
            // whether or not this version understands them.
            if (!AceHeaders.IsAceHeader(pair.Key)) application[pair.Key] = pair.Value;
        }

        var id = Str(headers, AceHeaders.Id) ?? messageId ?? Guid.NewGuid().ToString();
        return new Envelope(
            id,
            Str(headers, AceHeaders.Type) ?? routingKey ?? string.Empty,
            Int(headers, AceHeaders.Version) ?? 1,
            Str(headers, AceHeaders.Correlation) ?? id,
            Str(headers, AceHeaders.Causation),
            Int(headers, AceHeaders.Attempt) ?? 1,
            DateTimeOffset.FromUnixTimeMilliseconds(Long(headers, AceHeaders.FirstSeen) ?? 0L),
            Str(headers, AceHeaders.Origin),
            Str(headers, AceHeaders.Error),
            application);
    }

    /// <summary>
    /// Renders this envelope as the headers to put on the wire.
    /// </summary>
    /// <remarks>
    /// An absent value is an absent header, never a null one. The Java implementation
    /// omits <c>x-acemq-causation</c> entirely when there is no causation rather than
    /// writing a null, and a port that writes nulls produces messages that differ
    /// from Java's for the same logical content.
    /// </remarks>
    public IDictionary<string, object> ToWire()
    {
        var wire = new Dictionary<string, object>
        {
            [AceHeaders.Id] = Id,
            [AceHeaders.Type] = Type,
            [AceHeaders.Version] = Version,
            [AceHeaders.Correlation] = CorrelationId,
            [AceHeaders.Attempt] = Attempt,
            [AceHeaders.FirstSeen] = FirstSeen.ToUnixTimeMilliseconds(),
        };

        if (CausationId != null) wire[AceHeaders.Causation] = CausationId;
        if (Origin != null) wire[AceHeaders.Origin] = Origin;
        if (Error != null) wire[AceHeaders.Error] = Error;

        foreach (var pair in Headers)
        {
            if (!AceHeaders.IsAceHeader(pair.Key)) wire[pair.Key] = pair.Value;
        }

        return wire;
    }

    private static string? Str(IReadOnlyDictionary<string, object> h, string k) =>
        h.TryGetValue(k, out var v) && v != null ? Convert.ToString(v, CultureInfo.InvariantCulture) : null;

    private static int? Int(IReadOnlyDictionary<string, object> h, string k) =>
        h.TryGetValue(k, out var v) && v != null ? Convert.ToInt32(v, CultureInfo.InvariantCulture) : (int?)null;

    private static long? Long(IReadOnlyDictionary<string, object> h, string k) =>
        h.TryGetValue(k, out var v) && v != null ? Convert.ToInt64(v, CultureInfo.InvariantCulture) : (long?)null;

    /// <summary>Builds an envelope, applying the same defaults the Java library applies.</summary>
    public sealed class Builder
    {
        private readonly string _type;
        private readonly Dictionary<string, object> _headers = new Dictionary<string, object>();
        private string? _id;
        private string? _correlationId;
        private string? _causationId;
        private string? _origin;
        private string? _error;
        private int _version = 1;
        private int _attempt = 1;
        private DateTimeOffset? _firstSeen;

        internal Builder(string type) =>
            _type = type ?? throw new ArgumentNullException(nameof(type));

        public Builder Id(string id) { _id = id; return this; }
        public Builder Version(int version) { _version = version; return this; }
        public Builder CorrelationId(string? id) { _correlationId = id; return this; }
        public Builder CausationId(string? id) { _causationId = id; return this; }
        public Builder Attempt(int attempt) { _attempt = attempt; return this; }
        public Builder FirstSeen(DateTimeOffset when) { _firstSeen = when; return this; }
        public Builder Origin(string? origin) { _origin = origin; return this; }
        public Builder Error(string? error) { _error = error; return this; }

        public Builder Header(string name, object value)
        {
            if (AceHeaders.IsAceHeader(name))
            {
                throw new ArgumentException(
                    $"'{name}' is in AceMQ's reserved namespace and would be dropped on consume. " +
                    "Use a namespace of your own, such as x-yourcompany-.", nameof(name));
            }
            _headers[name] = value;
            return this;
        }

        public Envelope Build()
        {
            var id = _id ?? Guid.NewGuid().ToString();
            return new Envelope(
                id, _type, _version,
                _correlationId ?? id,
                _causationId, _attempt,
                _firstSeen ?? DateTimeOffset.UtcNow,
                _origin ?? DefaultOrigin(),
                _error,
                _headers);
        }

        private static string DefaultOrigin()
        {
            string host;
            try { host = Environment.MachineName; }
            catch { host = "unknown"; }
            return "acemq@" + host;
        }
    }
}
