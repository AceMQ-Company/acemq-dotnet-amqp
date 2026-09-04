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

using System.Text.Json;
using AceMq.Amqp;

namespace AceMq.Amqp.Tests;

/// <summary>
/// Asserts this library against headers produced by the Java one.
/// </summary>
/// <remarks>
/// <para>
/// The fixture in <c>fixtures/envelope-fixtures.json</c> is generated, not written:
/// a program publishes through <c>acemq-java-amqp</c> and pulls the message back at
/// the transport level, where the engine's own headers are still visible. So these
/// tests compare against what Java actually put on the wire, rather than against
/// what its documentation says it puts there.
/// </para>
/// <para>
/// That distinction is the point of the exercise. Two implementations agreeing with
/// the same prose is not interoperability; agreeing with the same bytes is.
/// </para>
/// </remarks>
public sealed class EnvelopeConformanceTests
{
    private static JsonElement Cases()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "fixtures", "envelope-fixtures.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        return doc.RootElement.GetProperty("cases").Clone();
    }

    private static JsonElement Case(string name)
    {
        foreach (var c in Cases().EnumerateArray())
        {
            if (c.GetProperty("case").GetString() == name) return c;
        }
        throw new InvalidOperationException($"no fixture case '{name}'");
    }

    private static Dictionary<string, object> HeadersOf(JsonElement fixture)
    {
        var headers = new Dictionary<string, object>();
        foreach (var h in fixture.GetProperty("headers").EnumerateObject())
        {
            headers[h.Name] = h.Value.ValueKind == JsonValueKind.Number
                ? h.Value.GetInt64()
                : (object)h.Value.GetString()!;
        }
        return headers;
    }

    [Fact]
    public void ReadsEveryFieldJavaWroteOnAPopulatedMessage()
    {
        var fixture = Case("populated");
        var envelope = Envelope.FromWire(HeadersOf(fixture), fixture.GetProperty("routingKey").GetString());

        Assert.Equal("11111111-2222-3333-4444-555555555555", envelope.Id);
        Assert.Equal("order.placed", envelope.Type);
        Assert.Equal(3, envelope.Version);
        Assert.Equal("corr-1", envelope.CorrelationId);
        Assert.Equal("cause-1", envelope.CausationId);
        Assert.Equal("orders@host-7", envelope.Origin);
        Assert.Equal(1, envelope.Attempt);
        Assert.Equal(1767323045678L, envelope.FirstSeen.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void AppliesTheSameDefaultsJavaAppliesToAMinimalPublish()
    {
        var fixture = Case("minimal");
        var headers = HeadersOf(fixture);
        var envelope = Envelope.FromWire(headers, fixture.GetProperty("routingKey").GetString());

        // Java defaults the type to the routing key and the correlation to the id.
        // Both are contract, not convenience.
        Assert.Equal("fx.plain", envelope.Type);
        Assert.Equal(envelope.Id, envelope.CorrelationId);
        Assert.Equal(1, envelope.Version);
        Assert.Equal(1, envelope.Attempt);
        Assert.StartsWith("acemq@", envelope.Origin);
        Assert.Null(envelope.CausationId);
    }

    [Fact]
    public void StripsTheReservedNamespaceAndKeepsApplicationHeaders()
    {
        var envelope = Envelope.FromWire(HeadersOf(Case("populated")));

        // x-tenant is the application's and survives; every x-acemq-* header is the
        // engine's and is materialised onto the envelope instead.
        Assert.Equal("acme", envelope.Headers["x-tenant"]);
        Assert.DoesNotContain(envelope.Headers.Keys, AceHeaders.IsAceHeader);
    }

    [Theory]
    [InlineData("minimal")]
    [InlineData("populated")]
    public void RoundTripsToTheSameHeadersJavaWrote(string caseName)
    {
        var fixture = Case(caseName);
        var expected = HeadersOf(fixture);
        var actual = Envelope
            .FromWire(expected, fixture.GetProperty("routingKey").GetString())
            .ToWire();

        // Same key set: a header Java writes and this does not is a message that
        // arrives subtly different, and one it writes that Java does not is worse.
        Assert.Equal(expected.Keys.OrderBy(k => k), actual.Keys.OrderBy(k => k));

        foreach (var key in expected.Keys)
        {
            Assert.Equal(
                Convert.ToString(expected[key], System.Globalization.CultureInfo.InvariantCulture),
                Convert.ToString(actual[key], System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public void FirstSeenIsEpochMillisAndNotAnIsoString()
    {
        // The two timestamps on the wire are encoded differently: first-seen is an
        // integer, replayed-at is ISO-8601. Getting this wrong produces a header
        // that parses on one side and not the other.
        var wire = Envelope.Of("t")
            .FirstSeen(DateTimeOffset.FromUnixTimeMilliseconds(1767323045678L))
            .Build()
            .ToWire();

        Assert.IsType<long>(wire[AceHeaders.FirstSeen]);
        Assert.Equal(1767323045678L, wire[AceHeaders.FirstSeen]);
    }

    [Fact]
    public void RefusesToLetAnApplicationWriteIntoTheReservedNamespace()
    {
        // Java drops these silently on consume. Failing at the call site is kinder
        // than a header that vanishes in transit with nothing reporting the loss.
        var error = Assert.Throws<ArgumentException>(
            () => Envelope.Of("t").Header(AceHeaders.Id, "mine"));
        Assert.Contains("reserved namespace", error.Message);
    }
}
