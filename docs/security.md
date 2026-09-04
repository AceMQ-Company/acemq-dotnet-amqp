# Security

The defaults are strict, and every way round them is named rather than implied.

## The rule

**The URL decides whether the connection is encrypted. The options decide how
strictly the certificate is checked.** They are separate on purpose: an `amqp://`
URL that quietly upgraded itself to TLS would be a connection nobody could reason
about, and options that silently downgraded `amqps://` would be worse.

```csharp
await AceMqConnection.ConnectAsync("amqp://localhost");         // plaintext, and says so
await AceMqConnection.ConnectAsync("amqps://broker:5671");      // TLS, verified, hostname checked
```

Asking for TLS on an `amqp://` URL is **refused**, not honoured. If you asked for
TLS and then pointed at the plaintext port, one of the two was a mistake and the
library will not guess which.

## What this protects, and what it does not

| | |
|---|---|
| In transit, service to broker | **TLS** — on with `amqps://`, verified by default |
| The message body at rest in the broker | **`EncryptedCodec`** — not on by default |
| Headers, routing keys, queue names | **Nothing.** The broker routes on them and the library reads them |
| Who may publish or consume what | **The broker's** users and permissions, not this library |
| The metrics and health endpoints | **Nothing.** They are unauthenticated — bind them to loopback |

The third row is the one people are surprised by. Anything secret belongs in the
payload, never in a header or a routing key.

## Getting started

Nothing is needed when the broker's certificate comes from an authority the machine
already trusts:

```csharp
using var mq = await AceMqConnection.ConnectAsync("amqps://broker.example.com");
```

Everything below is for the cases where it does not.

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

## Certificates for development

Talking to a broker over TLS on your own machine needs a certificate authority and
a server certificate. There is a tool for that, so nobody has to remember an
`openssl` incantation:

```bash
# The feed is a static one, so the tool is downloaded first -- see below.
curl -fsSLO https://acemq.org/nuget/v3/flatcontainer/acemq.amqp.devcerts/0.1.6/acemq.amqp.devcerts.0.1.6.nupkg
dotnet tool install -g AceMq.Amqp.DevCerts --version 0.1.6 --add-source .

acemq-certs --out certs --broker localhost --days 30
```

`dotnet tool install --add-source https://acemq.org/nuget/index.json` does **not**
work: it fails with an unhandled `NullReferenceException`. The feed is a static
directory tree serving only the flat container, and the tool installer wants
resources it does not have — the same limitation that makes `dotnet package search`
fail, except it surfaces as a crash rather than a message. Downloading the package
first and pointing `--add-source` at the folder works, which is what the release
pipeline does to verify it.

Ordinary `PackageReference` restores are unaffected; this applies only to installing
tools.

```
certs/ca.crt         trust this
certs/server.crt     the broker's certificate
certs/server.key     the broker's key
certs/client.pfx     for EXTERNAL auth, password: acemq-dev
certs/rabbitmq.conf  mount at /etc/rabbitmq/rabbitmq.conf
```

```bash
docker run -d -p 5671:5671 \
  -v "$PWD/certs":/certs:ro \
  -v "$PWD/certs/rabbitmq.conf":/etc/rabbitmq/rabbitmq.conf:ro \
  rabbitmq:4-management
```

It uses the framework's certificate API rather than shelling out, so it behaves the
same on Windows, macOS and Linux. `DevelopmentCertificates.Create(...)` is the same
thing callable from code, which is how the test suites make theirs.

### They are refused unless you say so

Everything it writes is stamped:

```
O=ACEMQ DEVELOPMENT ONLY - DO NOT TRUST, CN=localhost
```

and **the library refuses any chain carrying that marker**:

```csharp
TlsOptions.Required().TrustCertificateAuthority("certs/ca.crt")
// TransportException — the certificate says it must not be trusted
```

Trusting the authority is not enough. Neither is `Insecure()`, which accepts any
certificate *except* one that says it must not be trusted — the two are separate
decisions and both have to be made:

```csharp
TlsOptions.Required()
    .TrustCertificateAuthority("certs/ca.crt")
    .AllowDevelopmentCertificates()      // the line that makes it work
```

That line is deliberately conspicuous. A throwaway certificate authority which
drifts into production is worse than no encryption, because everything looks
protected and nothing is — so the thing that permits it should be visible in a diff,
in a review, and in a search across a codebase. The Java library does the same, with
the same marker, so certificates generated by either are refused by both.

They expire in thirty days by default: long enough not to be a nuisance, short
enough that one reaching a server is a problem that ends by itself.

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

## Encrypting the payload

TLS protects messages in transit only. Anything with access to the broker's storage
— or a backup of it — reads the bodies. `EncryptedCodec` encrypts the body itself, so
what the broker holds is unreadable without a key the broker does not have. See
[serialization](serialization.md#encrypting-the-payload).

### Decide the operations story first

The encryption is the easy part. What needs deciding before you turn it on:

- **Where the key lives**, and how it reaches the process. A key in configuration
  alongside the connection string protects against a stolen broker backup and
  nothing else.
- **How it rotates**, and who keeps the old one until the queues holding its
  messages are drained. Removing a key too early makes those messages unreadable —
  permanently.
- **How a failed message gets triaged.** A dead-lettered encrypted message cannot be
  read in the broker's management UI. Somebody will need to look at one at three in
  the morning, and "we cannot" is a bad answer to discover then.

## The endpoints are not authenticated

`AceMq.Amqp.Diagnostics` serves metrics, health and version over plain HTTP with no
authentication. They report queue names, broker state and traffic rates.

It binds to **loopback** by default for that reason. Setting `Host` to `+` or
`0.0.0.0` publishes all of it to anything that can reach the port — let the scraper
come over loopback, through a sidecar, or behind a network policy instead.

## Production checklist

- `amqps://`, and `TlsOptions.Required()` — the default, so this is really a check
  that nothing downgraded it.
- Credentials from a secret store through an `ICredentialsProvider`, not from a URL
  or a settings file. A password in a URL ends up in logs, in `ps` output, and in
  exception messages.
- A broker user per service, with permissions limited to the exchanges and queues
  that service uses. RabbitMQ's `guest` cannot connect remotely by default; do not
  "fix" that.
- `AllowDevelopmentCertificates()` appears **nowhere** in the deployment. Grep for
  it — that is what it is named for.
- `TlsOptions.Insecure()` likewise.
- The actuator bound to loopback, or behind something that authenticates.
- `EncryptedCodec` where the broker's operators should not be able to read the
  messages — with the key rotation and triage decisions written down.

---

**Found a vulnerability?** See [SECURITY.md](https://github.com/AceMQ-Company/acemq-dotnet-amqp/blob/main/SECURITY.md).

**Need this reviewed?** [AceMQ Enterprise support](https://acemq.com) covers TLS
configuration, certificate rotation, per-service permission design, and the
RabbitMQ-side hardening this page cannot do for you.
