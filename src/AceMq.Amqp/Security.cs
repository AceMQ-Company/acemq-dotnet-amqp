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
using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace AceMq.Amqp;

/// <summary>Something about the security configuration is wrong.</summary>
public class SecurityConfigurationException : AceMqException
{
    public SecurityConfigurationException(string message) : base(message) { }
    public SecurityConfigurationException(string message, Exception? cause) : base(message, cause) { }
}

/// <summary>How much the connection is protected.</summary>
public enum TlsMode
{
    /// <summary>No TLS. Credentials and message bodies travel in clear text.</summary>
    Disabled,

    /// <summary>TLS with the server's certificate and hostname both verified.</summary>
    Required,

    /// <summary>
    /// TLS with verification turned off. Encrypted, but not authenticated.
    /// </summary>
    /// <remarks>
    /// Anything able to intercept the connection can present its own certificate and
    /// read everything, including the credentials used to authenticate. This is a
    /// development convenience, not a weaker form of security.
    /// </remarks>
    Insecure,
}

/// <summary>A username and secret, kept out of logs.</summary>
public sealed class Credentials
{
    private Credentials(string username, string secret)
    {
        Username = username;
        Secret = secret;
    }

    public static Credentials Of(string username, string password) =>
        new Credentials(
            username ?? throw new ArgumentNullException(nameof(username)),
            password ?? throw new ArgumentNullException(nameof(password)));

    /// <summary>A bearer token, which RabbitMQ takes as the password.</summary>
    public static Credentials Token(string token) => Of(string.Empty, token);

    public string Username { get; }

    public string Secret { get; }

    /// <summary>Never includes the secret.</summary>
    /// <remarks>
    /// A credential that renders itself into a log is a credential in the log
    /// aggregator, in the backup of the log aggregator, and in whatever ingests it.
    /// </remarks>
    public override string ToString() =>
        $"Credentials[{(Username.Length == 0 ? "token" : Username)}, secret redacted]";
}

/// <summary>Where credentials come from, asked each time a connection is made.</summary>
/// <remarks>
/// Asked rather than held, so a deployment that rotates a secret does not need a
/// restart to pick up the new one.
/// </remarks>
public interface ICredentialsProvider
{
    Credentials Get();
}

/// <summary>Ready-made credential providers.</summary>
public static class CredentialsProviders
{
    /// <summary>The same credentials every time.</summary>
    public static ICredentialsProvider Of(string username, string password) =>
        new Fixed(Credentials.Of(username, password));

    /// <summary>
    /// Read from environment variables on every call.
    /// </summary>
    /// <remarks>
    /// Read each time rather than once, so a secret rotated into the environment is
    /// used by the next reconnection.
    /// </remarks>
    public static ICredentialsProvider FromEnvironment(
        string usernameVariable, string passwordVariable) =>
        new FromEnvironmentVariables(usernameVariable, passwordVariable);

    /// <summary>
    /// Read from a file on every call, one line for the username and one for the secret.
    /// </summary>
    /// <remarks>
    /// The shape a mounted Kubernetes secret takes. Read each time, because the
    /// projected file changes in place when the secret is updated.
    /// </remarks>
    public static ICredentialsProvider FromFile(string path) => new FromSecretFile(path);

    private sealed class Fixed : ICredentialsProvider
    {
        private readonly Credentials _credentials;
        internal Fixed(Credentials credentials) => _credentials = credentials;
        public Credentials Get() => _credentials;
    }

    private sealed class FromEnvironmentVariables : ICredentialsProvider
    {
        private readonly string _username;
        private readonly string _password;

        internal FromEnvironmentVariables(string username, string password)
        {
            _username = username;
            _password = password;
        }

        public Credentials Get()
        {
            var user = Environment.GetEnvironmentVariable(_username);
            var secret = Environment.GetEnvironmentVariable(_password);
            if (user == null || secret == null)
            {
                throw new SecurityConfigurationException(
                    $"{_username} and {_password} must both be set in the environment");
            }
            return Credentials.Of(user, secret);
        }
    }

    private sealed class FromSecretFile : ICredentialsProvider
    {
        private readonly string _path;
        internal FromSecretFile(string path) => _path = path;

        public Credentials Get()
        {
            if (!File.Exists(_path))
            {
                throw new SecurityConfigurationException($"no credentials file at {_path}");
            }
            var lines = File.ReadAllLines(_path);
            if (lines.Length < 2)
            {
                throw new SecurityConfigurationException(
                    $"{_path} should hold the username on the first line and the secret on the second");
            }
            return Credentials.Of(lines[0].Trim(), lines[1].Trim());
        }
    }
}

/// <summary>
/// How the connection is secured.
/// </summary>
/// <remarks>
/// <para>
/// An <c>amqps://</c> URL turns TLS on with verification. <c>amqp://</c> leaves it
/// off. Everything here is for the cases that need more than that: a private
/// certificate authority, a client certificate, or a deliberate relaxation in
/// development.
/// </para>
/// </remarks>
public sealed class TlsOptions
{
    /// <summary>
    /// Stamped into the subject of every certificate the development generator
    /// makes, so this library can refuse it.
    /// </summary>
    /// <remarks>
    /// Identical to the Java library's, because the same generated certificates
    /// are used from both.
    /// </remarks>
    public const string DevelopmentMarker = "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST";

    private TlsOptions(TlsMode mode)
    {
        Mode = mode;
        // On by default. A revoked certificate is one that is known to be in the
        // wrong hands, and not checking is how a compromise stays usable.
        CheckRevocation = mode == TlsMode.Required;
    }

    /// <summary>TLS, with the certificate and hostname verified. The normal choice.</summary>
    public static TlsOptions Required() => new TlsOptions(TlsMode.Required);

    /// <summary>No TLS.</summary>
    public static TlsOptions Disabled() => new TlsOptions(TlsMode.Disabled);

    /// <summary>
    /// TLS with no verification at all. <strong>Development only.</strong>
    /// </summary>
    /// <remarks>
    /// Encrypted but unauthenticated: anything that can intercept the connection can
    /// present its own certificate, and will be handed the credentials used to
    /// authenticate. Reach for <see cref="TrustCertificateAuthority(X509Certificate2)"/> before this —
    /// a private CA is almost always the actual problem, and trusting it keeps
    /// verification switched on.
    /// </remarks>
    public static TlsOptions Insecure() => new TlsOptions(TlsMode.Insecure);

    public TlsMode Mode { get; }

    /// <summary>The certificate authority to trust in addition to the system's, if any.</summary>
    public X509Certificate2? CertificateAuthority { get; private set; }

    /// <summary>Client certificates, for brokers that authenticate with EXTERNAL.</summary>
    public X509CertificateCollection ClientCertificates { get; } = new X509CertificateCollection();

    /// <summary>The name the server's certificate must match. Defaults to the URL's host.</summary>
    public string? ServerName { get; private set; }

    /// <summary>Whether the certificate is checked against revocation lists.</summary>
    public bool CheckRevocation { get; private set; }

    /// <summary>Whether certificates the development generator made are permitted.</summary>
    public bool DevelopmentCertificatesAllowed { get; private set; }

    /// <summary>
    /// Trusts a private certificate authority, keeping verification on.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is what to use when the broker has an internally issued certificate. The
    /// chain is rebuilt against this authority and the hostname is still checked, so
    /// a broker presenting somebody else's certificate is still rejected.
    /// </para>
    /// <para>
    /// It is deliberately not the same as turning validation off, which is what most
    /// people do at this point. <see cref="TlsMode.Insecure"/> accepts any
    /// certificate at all; this accepts exactly the ones your authority issued.
    /// </para>
    /// </remarks>
    public TlsOptions TrustCertificateAuthority(X509Certificate2 authority)
    {
        CertificateAuthority = authority ?? throw new ArgumentNullException(nameof(authority));
        return this;
    }

    /// <summary>Trusts a certificate authority read from a PEM or DER file.</summary>
    public TlsOptions TrustCertificateAuthority(string path)
    {
        if (!File.Exists(path))
        {
            throw new SecurityConfigurationException($"no certificate authority file at {path}");
        }
        try
        {
            return TrustCertificateAuthority(new X509Certificate2(path));
        }
        catch (Exception e)
        {
            throw new SecurityConfigurationException($"could not read a certificate from {path}", e);
        }
    }

    /// <summary>Presents a client certificate, for a broker configured for EXTERNAL auth.</summary>
    public TlsOptions WithClientCertificate(X509Certificate2 certificate)
    {
        if (certificate == null) throw new ArgumentNullException(nameof(certificate));
        if (!certificate.HasPrivateKey)
        {
            // A certificate without its key cannot complete the handshake, and the
            // failure surfaces as a bare connection reset from the broker.
            throw new SecurityConfigurationException(
                "a client certificate needs its private key; load a .pfx rather than a .cer");
        }
        ClientCertificates.Add(certificate);
        return this;
    }

    /// <summary>Loads a client certificate from a PKCS#12 file.</summary>
    public TlsOptions WithClientCertificate(string path, string? password)
    {
        if (!File.Exists(path))
        {
            throw new SecurityConfigurationException($"no client certificate file at {path}");
        }
        try
        {
            return WithClientCertificate(new X509Certificate2(path, password));
        }
        catch (SecurityConfigurationException) { throw; }
        catch (Exception e)
        {
            throw new SecurityConfigurationException($"could not read a certificate from {path}", e);
        }
    }

    /// <summary>Verifies the certificate against this name rather than the URL's host.</summary>
    /// <remarks>
    /// Needed when connecting through an address the certificate does not name — a
    /// load balancer, or a host reached by IP.
    /// </remarks>
    public TlsOptions WithServerName(string serverName)
    {
        ServerName = serverName ?? throw new ArgumentNullException(nameof(serverName));
        return this;
    }

    /// <summary>
    /// Permits certificates the development generator produced.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Needed on a developer's machine and nowhere else. Without it, a certificate
    /// carrying <see cref="DevelopmentMarker"/> is refused however the trust is
    /// configured — including when a private authority is trusted, and including
    /// <see cref="TlsMode.Insecure"/>.
    /// </para>
    /// <para>
    /// The reason for refusing by default is that a throwaway certificate authority
    /// which drifts into production is worse than no encryption: everything looks
    /// protected and nothing is. Making it explicit means the one line that permits
    /// it is visible in a diff, in a review, and in a search across a codebase.
    /// </para>
    /// </remarks>
    public TlsOptions AllowDevelopmentCertificates()
    {
        DevelopmentCertificatesAllowed = true;
        return this;
    }

    /// <summary>
    /// Turns revocation checking off.
    /// </summary>
    /// <remarks>
    /// A deliberate weakening: a certificate that has been revoked because its key
    /// leaked will still be accepted. The case that justifies it is an isolated
    /// network with no route to the issuer's responder, where the check does not fail
    /// closed so much as hang.
    /// </remarks>
    public TlsOptions WithoutRevocationChecking()
    {
        CheckRevocation = false;
        return this;
    }

    public override string ToString() =>
        $"TlsOptions[{Mode}, ca={(CertificateAuthority != null ? "custom" : "system")}, " +
        $"clientCerts={ClientCertificates.Count}, revocation={CheckRevocation}" +
        (DevelopmentCertificatesAllowed ? ", development certificates allowed" : "") + "]";
}
