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

using System.Diagnostics;
using System.Text;
using AceMq.Amqp;

namespace AceMq.Amqp.Yaml.Tests;

public sealed class DeploymentRequested
{
    public string Service { get; set; } = "";
    public string Version { get; set; } = "";
    public List<string> Regions { get; set; } = new List<string>();
}

public sealed class YamlCodecTests
{
    private readonly YamlCodec _codec = new YamlCodec();

    private static DeploymentRequested ADeployment() => new DeploymentRequested
    {
        Service = "orders",
        Version = "1.4.2",
        Regions = new List<string> { "eu-west-1", "us-east-1" },
    };

    [Fact]
    public void RoundTripsAMessage()
    {
        var back = (DeploymentRequested)_codec.Decode(
            _codec.Encode(ADeployment()), typeof(DeploymentRequested));

        Assert.Equal("orders", back.Service);
        Assert.Equal("1.4.2", back.Version);
        Assert.Equal(new[] { "eu-west-1", "us-east-1" }, back.Regions);
    }

    [Fact]
    public void WritesBlockStyleBecauseThatIsTheWholePoint()
    {
        var yaml = Encoding.UTF8.GetString(_codec.Encode(ADeployment()));

        // Flow style would produce something all but indistinguishable from JSON and
        // leave nothing to justify YAML costing more to parse.
        Assert.Contains("service: orders", yaml);
        Assert.Contains("- eu-west-1", yaml);
        Assert.DoesNotContain("{", yaml);
        Assert.DoesNotContain("[", yaml);
    }

    [Fact]
    public void CamelCasesKeysSoCSharpAndJavaAgree()
    {
        var yaml = Encoding.UTF8.GetString(_codec.Encode(ADeployment()));

        Assert.Contains("service:", yaml);
        Assert.DoesNotContain("Service:", yaml);
    }

    [Fact]
    public void NeverVolunteersForAMessageWithNoContentType()
    {
        // The trap this guards against: YAML is a superset of JSON, so this parser
        // reads JSON bytes happily. Answering for an untyped message would give the
        // right value from the wrong codec, and be discovered much later as messages
        // recorded under a format nobody sent.
        Assert.False(_codec.CanDecode(null));
        Assert.False(_codec.CanDecode(""));
        Assert.False(_codec.CanDecode("application/json"));
    }

    [Fact]
    public void ItReallyDoesParseJsonWhichIsWhyThatMatters()
    {
        // Demonstrating the hazard rather than asserting it exists.
        var json = new JsonCodec().Encode(ADeployment());
        var back = (DeploymentRequested)_codec.Decode(json, typeof(DeploymentRequested));

        Assert.Equal("orders", back.Service);
    }

    [Fact]
    public void ClaimsTheContentTypesThatSayYaml()
    {
        Assert.Equal("application/yaml", _codec.ContentType);
        Assert.True(_codec.CanDecode("application/yaml"));
        Assert.True(_codec.CanDecode("application/x-yaml"));
        Assert.True(_codec.CanDecode("text/yaml"));
        Assert.True(_codec.CanDecode("application/vnd.acme.deploy+yaml"));
    }

    [Fact]
    public void IgnoresKeysItDoesNotKnow()
    {
        // A producer adding a field must not break a consumer that has not been
        // redeployed.
        var yaml = Encoding.UTF8.GetBytes(
            "service: orders\nversion: 1.4.2\nregions:\n- eu-west-1\napprovedBy: someone\n");

        var back = (DeploymentRequested)_codec.Decode(yaml, typeof(DeploymentRequested));
        Assert.Equal("orders", back.Service);
    }

    [Fact]
    public void TreatsMalformedYamlAsFatalRatherThanRetryable()
    {
        var broken = Encoding.UTF8.GetBytes("service: orders\n  bad: [indent\n");
        Assert.ThrowsAny<AceMqException>(
            () => _codec.Decode(broken, typeof(DeploymentRequested)));
    }

    [Fact]
    public void RefusesToBuildAnArbitraryTypeNamedByTheMessage()
    {
        // A YAML tag naming a type is the deserialisation attack this format is
        // known for in other ecosystems. YamlDotNet does not honour one by default,
        // and this pins that rather than trusting it to stay true.
        var hostile = Encoding.UTF8.GetBytes(
            "!<System.Diagnostics.Process,System.Diagnostics.Process>\nstartInfo:\n  fileName: /bin/sh\n");

        Assert.ThrowsAny<Exception>(() => _codec.Decode(hostile, typeof(DeploymentRequested)));
    }

    [Fact]
    public void DoesNotExpandAliasesIntoAnAvalanche()
    {
        // The billion-laughs shape: a small document that names 10^10 nodes. The
        // parser shares aliases rather than materialising them, so this is bounded.
        // Measured rather than assumed, because if it ever stopped being true this
        // codec would be a denial of service anybody could post.
        var document = new StringBuilder("a0: &a0 [x,x,x,x,x,x,x,x,x,x]\n");
        for (var i = 1; i <= 9; i++)
        {
            document.Append($"a{i}: &a{i} [")
                    .Append(string.Join(",", Enumerable.Repeat($"*a{i - 1}", 10)))
                    .Append("]\n");
        }

        var clock = Stopwatch.StartNew();
        try
        {
            _codec.Decode(Encoding.UTF8.GetBytes(document.ToString()), typeof(DeploymentRequested));
        }
        catch (AceMqException)
        {
            // Refusing it is just as good an outcome as reading it quickly.
        }

        Assert.True(clock.ElapsedMilliseconds < 2000,
            $"a 500-byte document took {clock.ElapsedMilliseconds}ms");
    }

    [Fact]
    public async Task CarriesAMessageThroughTheBroker()
    {
        using var mq = await AceMqConnection.ConnectAsync(
            "memory://" + Guid.NewGuid().ToString("N"), _codec);
        var queue = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);
        await mq.DeclareQueueAsync(queue);

        DeploymentRequested? received = null;
        using var consumer = await mq.ConsumeAsync<DeploymentRequested>(queue, message =>
        {
            received = message.Payload;
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<DeploymentRequested>("", queue).SendAsync(ADeployment());

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (received == null && DateTime.UtcNow < deadline) await Task.Delay(10);

        Assert.NotNull(received);
        Assert.Equal("orders", received!.Service);
    }

    [Fact]
    public void IsAvailableByNameOnceRegistered()
    {
        CodecRegistry.Register("yaml", () => new YamlCodec());

        Assert.IsType<YamlCodec>(CodecRegistry.ByName("yaml"));
        Assert.Contains("yaml", CodecRegistry.Names());
    }
}
