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

using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

namespace AceMq.Amqp.Tls.Tests;

/// <summary>
/// TLS, against a broker presenting a privately issued certificate.
/// </summary>
/// <remarks>
/// <para>
/// A separate project because it needs a broker configured for TLS and a certificate
/// authority on disk, which the CI job builds before running it. There is no skip
/// path: this is the most security-sensitive code in the library, and a suite that
/// quietly does nothing when the broker is missing would report a green tick for the
/// one thing nobody wants unverified.
/// </para>
/// <para>
/// <c>ACEMQ_TEST_AMQPS_URL</c> and <c>ACEMQ_TEST_CA</c> point at them.
/// </para>
/// </remarks>
public sealed class TlsTests
{
    private readonly string _url =
        Environment.GetEnvironmentVariable("ACEMQ_TEST_AMQPS_URL")
        ?? "amqps://guest:guest@localhost:5671";

    private readonly string _caPath =
        Environment.GetEnvironmentVariable("ACEMQ_TEST_CA") ?? "certs/ca.crt";

    private X509Certificate2 Ca() => X509CertificateLoader.LoadCertificateFromFile(_caPath);

    private async Task<AceMqConnection> ConnectAsync(TlsOptions tls)
    {
        Transports.Register(new RabbitMqTransport());
        var config = ConnectionConfig.ForUrl(_url).Tls(tls).Build();
        return await AceMqConnection.ConnectAsync(config, new JsonCodec(), CancellationToken.None);
    }

    [Fact]
    public async Task PublishesOverTlsWhenTheAuthorityIsTrusted()
    {
        using var mq = await ConnectAsync(
            TlsOptions.Required().TrustCertificateAuthority(Ca()).WithoutRevocationChecking());

        var queue = "tls." + Guid.NewGuid().ToString("N").Substring(0, 8);
        await mq.DeclareQueueAsync(queue);
        var result = await mq.Publisher<string>("", queue).SendAsync("over TLS");

        Assert.True(result.Routed);
        await mq.DeleteQueueAsync(queue);
    }

    [Fact]
    public async Task RefusesACertificateNoTrustedAuthorityIssued()
    {
        // The system trust store has never heard of this broker's authority, and
        // that has to be a refusal rather than a warning.
        await Assert.ThrowsAsync<TransportException>(
            () => ConnectAsync(TlsOptions.Required()));
    }

    [Fact]
    public async Task RefusesAChainThatDoesNotReachTheTrustedAuthority()
    {
        using var rsa = RSA.Create(2048);
        var somebodyElse = new CertificateRequest(
                "CN=Some Other CA", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
            .CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        // This is the check that makes trusting a private authority meaningful.
        // Rebuilding the chain with AllowUnknownCertificateAuthority and stopping
        // there would accept any self-consistent chain, including one the presenter
        // minted a moment ago. The anchor reached has to be the authority given.
        await Assert.ThrowsAsync<TransportException>(
            () => ConnectAsync(TlsOptions.Required()
                .TrustCertificateAuthority(somebodyElse)
                .WithoutRevocationChecking()));
    }

    [Fact]
    public async Task ConnectsWithVerificationOffBecauseThatIsWhatInsecureMeans()
    {
        // Proving the escape hatch works is also proving how much it gives away:
        // the same broker the trusted-authority test had to be told about is
        // accepted here with no configuration at all.
        using var mq = await ConnectAsync(TlsOptions.Insecure());
        Assert.True(mq.IsOpen);
    }

    [Fact]
    public async Task TakesCredentialsFromAProviderRatherThanTheUrl()
    {
        var url = new Uri(_url);
        var withoutCredentials =
            $"{url.Scheme}://{url.Host}:{url.Port}";

        Transports.Register(new RabbitMqTransport());
        var config = ConnectionConfig.ForUrl(withoutCredentials)
            .Tls(TlsOptions.Required().TrustCertificateAuthority(Ca()).WithoutRevocationChecking())
            .Credentials(CredentialsProviders.Of("guest", "guest"))
            .Build();

        using var mq = await AceMqConnection.ConnectAsync(
            config, new JsonCodec(), CancellationToken.None);

        // The password never appeared in the URL, so it cannot reach a log through
        // one.
        Assert.DoesNotContain("guest:guest", config.ToString());
        Assert.True(mq.IsOpen);
    }
}
