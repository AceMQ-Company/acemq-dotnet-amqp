# TLS and credentials

An `amqps://` URL turns TLS on with the certificate and hostname both verified. An
`amqp://` URL does not use TLS at all.

```csharp
using var mq = await AceMqConnection.ConnectAsync("amqps://broker.example.com");
```

Nothing else is needed when the broker's certificate comes from an authority the
machine already trusts. Everything below is for the cases where it does not.

## A privately issued certificate

Most internal brokers have a certificate from a company CA the machine has never
heard of, and the connection fails:

```
TransportException: could not reach the broker at amqps://broker.internal
```

**The fix is to trust that authority, not to stop checking.**

```csharp
var config = ConnectionConfig.ForUrl("amqps://broker.internal")
    .Tls(TlsOptions.Required().TrustCertificateAuthority("/etc/ssl/company-ca.crt"))
    .Build();
```

Verification stays on. The chain is rebuilt against that authority and the hostname
is still checked, so a broker presenting somebody else's certificate is still
refused.

This is worth insisting on, because the usual response to that error is
`TlsOptions.Insecure()`, which accepts *any* certificate — including one an
interceptor generated a moment ago, along with the credentials it is handed.
Trusting the CA accepts exactly the certificates your authority issued.

There is a subtlety in doing this correctly, and the library handles it: rebuilding
a chain with `AllowUnknownCertificateAuthority` and stopping there accepts any
self-consistent chain, which is barely better than accepting anything. The root the
chain actually reaches is compared against the authority you supplied. A test proves
a *different* authority is refused.

## Client certificates

For a broker configured for `EXTERNAL` authentication:

```csharp
TlsOptions.Required()
    .WithClientCertificate("/etc/ssl/client.pfx", password)
```

The certificate needs its private key — a `.pfx`, not a `.cer`. Without the key the
handshake cannot complete, and the failure arrives as a bare connection reset from
the broker rather than as anything that names the cause, so this is checked when the
certificate is loaded.

## When the address does not match the certificate

Through a load balancer, or connecting by IP:

```csharp
TlsOptions.Required().WithServerName("broker.example.com")
```

The certificate is verified against that name instead of the URL's host. This is the
right answer to a hostname mismatch; turning verification off is not.

## Revocation checking

**On by default** when TLS is required. A revoked certificate is one known to be in
the wrong hands, and not checking is how a compromise stays usable.

```csharp
TlsOptions.Required().WithoutRevocationChecking()
```

The case that justifies turning it off is an isolated network with no route to the
issuer's responder, where the check does not so much fail as hang. It is a deliberate
weakening either way.

## Insecure mode

```csharp
TlsOptions.Insecure()   // development only
```

Encrypted, **not authenticated**. Anything able to intercept the connection can
present its own certificate and will be given the credentials used to authenticate.
It is not a weaker form of security; it is the absence of the part that makes TLS
worth having.

Reach for `TrustCertificateAuthority` first — a private CA is almost always what the
problem actually was.

## Credentials

Credentials in a URL end up in configuration files, in process listings, and in
logs. A provider keeps them out:

```csharp
ConnectionConfig.ForUrl("amqps://broker.example.com")
    .Credentials(CredentialsProviders.FromEnvironment("MQ_USER", "MQ_PASSWORD"))
    .Build();
```

| | |
|---|---|
| `CredentialsProviders.Of(user, password)` | the same every time |
| `CredentialsProviders.FromEnvironment(userVar, passwordVar)` | read on every connection |
| `CredentialsProviders.FromFile(path)` | username on the first line, secret on the second |

The provider is asked **on every connection**, not once at start-up. A secret rotated
into the environment or into a mounted file is picked up by the next reconnection
rather than waiting for a restart. `FromFile` matches the shape a Kubernetes secret
mount takes, which is replaced in place when the secret changes.

`Credentials.ToString()` never includes the secret, and `ConnectionConfig.ToString()`
redacts the password out of the URL. A credential that renders itself into a log is a
credential in the log aggregator, in its backups, and in whatever ingests them.

## The scheme has to match

```csharp
ConnectionConfig.ForUrl("amqp://broker").Tls(TlsOptions.Required()).Build();
// SecurityConfigurationException: TLS was configured but the URL scheme is 'amqp'
```

`amqp` and `amqps` are different ports and the broker will not upgrade one to the
other. Left to itself this fails at connect time looking like a network fault, which
is a long way from the actual mistake — so it is refused when the configuration is
built.

The reverse is guarded too: an `amqps://` URL defaults to TLS **required**, so a
scheme cannot quietly leave a production connection in clear text.

## What is not here yet

Payload encryption. The Java library has an `EncryptedCodec` and a keyring for
encrypting message bodies at rest in the broker; there is no .NET equivalent yet.
TLS protects messages in transit only — anything with access to the broker's storage
sees the bodies.
