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

using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AceMq.Amqp;

namespace AceMq.Amqp.DevCerts;

/// <summary>What was written, and where.</summary>
public sealed class GeneratedCertificates
{
    internal GeneratedCertificates(
        string directory, string caCertificate, string serverCertificate,
        string serverKey, string clientPfx, string? brokerConfig, DateTimeOffset expires)
    {
        Directory = directory;
        CaCertificate = caCertificate;
        ServerCertificate = serverCertificate;
        ServerKey = serverKey;
        ClientPfx = clientPfx;
        BrokerConfig = brokerConfig;
        Expires = expires;
    }

    public string Directory { get; }

    /// <summary>The authority to trust, for <c>TrustCertificateAuthority</c>.</summary>
    public string CaCertificate { get; }

    public string ServerCertificate { get; }
    public string ServerKey { get; }

    /// <summary>A client certificate with its key, for brokers using EXTERNAL auth.</summary>
    public string ClientPfx { get; }

    /// <summary>The <c>rabbitmq.conf</c> written, unless that was skipped.</summary>
    public string? BrokerConfig { get; }

    public DateTimeOffset Expires { get; }
}

/// <summary>
/// Makes certificates for talking to a broker on a developer's machine.
/// </summary>
/// <remarks>
/// <para>
/// Everything it writes carries <see cref="TlsOptions.DevelopmentMarker"/> in its
/// subject, and the library refuses any chain containing it unless
/// <see cref="TlsOptions.AllowDevelopmentCertificates"/> was called. That is the
/// point of generating them here rather than telling people to run <c>openssl</c>:
/// a throwaway authority that drifts into production is worse than no encryption,
/// because everything looks protected and nothing is.
/// </para>
/// <para>
/// It uses the framework's own certificate API rather than shelling out to
/// <c>openssl</c>, so it behaves the same on Windows, macOS and Linux — and on
/// Windows, where openssl is not something to assume.
/// </para>
/// <para>
/// Thirty days by default. Long enough not to be a nuisance, short enough that one
/// of these reaching a server is a problem that expires by itself.
/// </para>
/// </remarks>
public static class DevelopmentCertificates
{
    /// <summary>What the tool writes when nothing says otherwise.</summary>
    public const string DefaultPassword = "acemq-dev";

    /// <summary>
    /// Writes a certificate authority, a server certificate and a client certificate.
    /// </summary>
    /// <param name="directory">Where to write them. Created if absent.</param>
    /// <param name="broker">The name the server certificate is issued for.</param>
    /// <param name="days">How long they are valid.</param>
    /// <param name="password">Protects the client PKCS#12 file.</param>
    /// <param name="brokerCertificateDirectory">
    /// The path the generated <c>rabbitmq.conf</c> should point at — where the files
    /// will be inside the broker's container, which is rarely where they are written.
    /// </param>
    /// <param name="writeBrokerConfig">Whether to write a <c>rabbitmq.conf</c> at all.</param>
    public static GeneratedCertificates Create(
        string directory,
        string broker = "localhost",
        int days = 30,
        string password = DefaultPassword,
        string brokerCertificateDirectory = "/certs",
        bool writeBrokerConfig = true)
    {
        if (string.IsNullOrWhiteSpace(directory))
        {
            throw new ArgumentException("a directory is needed", nameof(directory));
        }
        if (days < 1) throw new ArgumentException("must be at least a day", nameof(days));

        System.IO.Directory.CreateDirectory(directory);

        var from = DateTimeOffset.UtcNow.AddMinutes(-5);
        var until = DateTimeOffset.UtcNow.AddDays(days);

        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest(
            $"CN=AceMQ development CA, O={TlsOptions.DevelopmentMarker}",
            caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caRequest.CertificateExtensions.Add(
            new X509KeyUsageExtension(
                X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        using var ca = caRequest.CreateSelfSigned(from, until);

        using var serverKey = RSA.Create(2048);
        var server = Issue(
            $"CN={broker}, O={TlsOptions.DevelopmentMarker}",
            serverKey, ca, from, until, broker, serverAuth: true);

        using var clientKey = RSA.Create(2048);
        using var client = Issue(
            $"CN=acemq-dev-client, O={TlsOptions.DevelopmentMarker}",
            clientKey, ca, from, until, host: null, serverAuth: false);

        var caPath = Path.Combine(directory, "ca.crt");
        var serverCertPath = Path.Combine(directory, "server.crt");
        var serverKeyPath = Path.Combine(directory, "server.key");
        var clientPath = Path.Combine(directory, "client.pfx");

        File.WriteAllText(caPath, Pem(ca, "CERTIFICATE"));
        File.WriteAllText(serverCertPath, Pem(server, "CERTIFICATE"));
        // The broker reads the key as PEM, separately from the certificate, which is
        // why this is not written as a PKCS#12 like the client's.
        File.WriteAllText(serverKeyPath, PemKey(serverKey));
        File.WriteAllBytes(clientPath, client.Export(X509ContentType.Pkcs12, password));

        // 0600 where the platform has such a thing. A private key readable by every
        // account on the machine is a bad habit to teach even in development.
        Restrict(serverKeyPath);
        Restrict(clientPath);

        string? configPath = null;
        if (writeBrokerConfig)
        {
            configPath = Path.Combine(directory, "rabbitmq.conf");
            File.WriteAllText(configPath, BrokerConfig(brokerCertificateDirectory));
        }

        server.Dispose();
        return new GeneratedCertificates(
            Path.GetFullPath(directory), caPath, serverCertPath, serverKeyPath,
            clientPath, configPath, until);
    }

    private static X509Certificate2 Issue(
        string subject, RSA key, X509Certificate2 issuer,
        DateTimeOffset from, DateTimeOffset until, string? host, bool serverAuth)
    {
        var request = new CertificateRequest(
            subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension(
                new OidCollection
                {
                    new Oid(serverAuth ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2"),
                },
                true));

        if (host != null)
        {
            // Without a subject alternative name the certificate fails hostname
            // verification on every modern client, which would make the generated
            // certificates usable only with verification turned off -- the opposite
            // of the point.
            var names = new SubjectAlternativeNameBuilder();
            names.AddDnsName(host);
            if (IPAddress.TryParse(host, out var address)) names.AddIpAddress(address);
            if (host == "localhost")
            {
                names.AddIpAddress(IPAddress.Loopback);
                names.AddIpAddress(IPAddress.IPv6Loopback);
            }
            request.CertificateExtensions.Add(names.Build());
        }

        var serial = new byte[16];
        RandomNumberGenerator.Fill(serial);

        using var issued = request.Create(issuer, from, until, serial);
        return issued.CopyWithPrivateKey(key);
    }

    private static string Pem(X509Certificate2 certificate, string label) =>
        PemEncode(certificate.Export(X509ContentType.Cert), label);

    private static string PemKey(RSA key) => PemEncode(key.ExportPkcs8PrivateKey(), "PRIVATE KEY");

    private static string PemEncode(byte[] der, string label)
    {
        var base64 = Convert.ToBase64String(der);
        var body = new System.Text.StringBuilder();
        for (var i = 0; i < base64.Length; i += 64)
        {
            body.Append(base64, i, Math.Min(64, base64.Length - i)).Append('\n');
        }
        return $"-----BEGIN {label}-----\n{body}-----END {label}-----\n";
    }

    private static void Restrict(string path)
    {
        try
        {
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }
        }
        catch
        {
            // Best effort. A file system that cannot express this is not a reason to
            // fail generating development certificates.
        }
    }

    private static string BrokerConfig(string directory) =>
        $@"# Written by AceMQ's development certificate generator. Development only.
#
# Mount the generated directory at {directory} in the broker, and this file at
# /etc/rabbitmq/rabbitmq.conf.
listeners.ssl.default = 5671
ssl_options.cacertfile = {directory}/ca.crt
ssl_options.certfile   = {directory}/server.crt
ssl_options.keyfile    = {directory}/server.key

# verify_peer with a client certificate is the other half of EXTERNAL auth. Left
# off here because the common case is verifying the broker, not the client, and a
# broker that demands a certificate nobody configured refuses every connection
# with an error that names none of this.
ssl_options.verify     = verify_none
ssl_options.fail_if_no_peer_cert = false
";
}
