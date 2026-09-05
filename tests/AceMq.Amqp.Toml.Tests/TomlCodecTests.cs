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

using System.Text;
using AceMq.Amqp;

namespace AceMq.Amqp.Toml.Tests;

public sealed class FeatureFlags
{
    public string Service { get; set; } = "";
    public bool Enabled { get; set; }
    public List<string> Regions { get; set; } = new List<string>();
}

public sealed class TomlCodecTests
{
    private readonly TomlCodec _codec = new TomlCodec();

    private static FeatureFlags SomeFlags() => new FeatureFlags
    {
        Service = "orders",
        Enabled = true,
        Regions = new List<string> { "eu-west-1", "us-east-1" },
    };

    [Fact]
    public void RoundTripsAMessage()
    {
        var back = (FeatureFlags)_codec.Decode(_codec.Encode(SomeFlags()), typeof(FeatureFlags));

        Assert.Equal("orders", back.Service);
        Assert.True(back.Enabled);
        Assert.Equal(new[] { "eu-west-1", "us-east-1" }, back.Regions);
    }

    [Fact]
    public void WritesSomethingAPersonCanEdit()
    {
        var toml = Encoding.UTF8.GetString(_codec.Encode(SomeFlags()));

        Assert.Contains("service = \"orders\"", toml);
        Assert.Contains("enabled = true", toml);
        Assert.Contains("regions = [\"eu-west-1\", \"us-east-1\"]", toml);
    }

    [Fact]
    public void CamelCasesKeysSoCSharpAndJavaAgree()
    {
        var toml = Encoding.UTF8.GetString(_codec.Encode(SomeFlags()));

        Assert.Contains("service =", toml);
        Assert.DoesNotContain("Service =", toml);
    }

    [Fact]
    public void HasNoNorwayProblem()
    {
        // The reason to prefer TOML over YAML for something a person edits. In YAML
        // `country: NO` is the boolean false; here an unquoted NO is not a value at
        // all, so the mistake is a parse error rather than a country turning into
        // false somewhere downstream.
        var document = Encoding.UTF8.GetBytes("service = \"orders\"\ncountry = NO\n");

        Assert.ThrowsAny<AceMqException>(() => _codec.Decode(document, typeof(FeatureFlags)));
    }

    [Fact]
    public void ReadsAKeyWhoeverEditedItCapitalised()
    {
        // Written one way, read leniently: a hand-edited message should not fail
        // over a capital letter.
        var document = Encoding.UTF8.GetBytes("Service = \"orders\"\nEnabled = true\n");

        var back = (FeatureFlags)_codec.Decode(document, typeof(FeatureFlags));
        Assert.Equal("orders", back.Service);
    }

    [Fact]
    public void SaysSoWhenThePayloadIsNotTableShaped()
    {
        // TOML is a table format. A bare list or number is not a document, and this
        // is worth naming rather than emitting something that is not TOML.
        var error = Assert.Throws<AceFatalException>(
            () => _codec.Encode(new List<string> { "a", "b" }));

        Assert.Contains("table format", error.Message);
        Assert.Contains("JsonCodec", error.Message);
    }

    [Fact]
    public void RefusesADocumentWhoseTopLevelIsNotATable()
    {
        Assert.ThrowsAny<AceMqException>(
            () => _codec.Decode(Encoding.UTF8.GetBytes("[1, 2, 3]"), typeof(FeatureFlags)));
    }

    [Fact]
    public void NeverVolunteersForAMessageWithNoContentType()
    {
        // Same reasoning as the YAML codec: answering for an untyped message would
        // record traffic under a format nobody sent.
        Assert.False(_codec.CanDecode(null));
        Assert.False(_codec.CanDecode(""));
        Assert.False(_codec.CanDecode("application/json"));
    }

    [Fact]
    public void ClaimsTheContentTypesThatSayToml()
    {
        Assert.Equal("application/toml", _codec.ContentType);
        Assert.True(_codec.CanDecode("application/toml"));
        Assert.True(_codec.CanDecode("text/toml"));
        Assert.True(_codec.CanDecode("application/vnd.acme.flags+toml"));
    }

    [Fact]
    public void RefusesADuplicateKeyRatherThanPickingOne()
    {
        // TOML forbids them. Silently taking the first or the last would be a
        // message that means something different from what it looks like.
        var document = Encoding.UTF8.GetBytes("service = \"orders\"\nservice = \"payments\"\n");

        Assert.ThrowsAny<AceMqException>(() => _codec.Decode(document, typeof(FeatureFlags)));
    }

    [Fact]
    public void TreatsMalformedTomlAsFatalRatherThanRetryable()
    {
        var broken = Encoding.UTF8.GetBytes("service = \nenabled = ");
        Assert.ThrowsAny<AceMqException>(() => _codec.Decode(broken, typeof(FeatureFlags)));
    }

    [Fact]
    public async Task CarriesAMessageThroughTheBroker()
    {
        using var mq = await AceMqConnection.ConnectAsync(
            "memory://" + Guid.NewGuid().ToString("N"), _codec);
        var queue = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);
        await mq.DeclareQueueAsync(queue);

        FeatureFlags? received = null;
        using var consumer = await mq.ConsumeAsync<FeatureFlags>(queue, message =>
        {
            received = message.Payload;
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<FeatureFlags>("", queue).SendAsync(SomeFlags());

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (received == null && DateTime.UtcNow < deadline) await Task.Delay(10);

        Assert.NotNull(received);
        Assert.Equal("orders", received!.Service);
        Assert.True(received.Enabled);
    }

    [Fact]
    public void IsAvailableByNameOnceRegistered()
    {
        CodecRegistry.Register("toml", () => new TomlCodec());

        Assert.IsType<TomlCodec>(CodecRegistry.ByName("toml"));
        Assert.Contains("toml", CodecRegistry.Names());
    }
}
