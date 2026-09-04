# Reporting a vulnerability

Email **security@acemq.com** with what you found and how to reproduce it. Please do
not open a public issue for anything exploitable.

You should get an acknowledgement within two working days, and an assessment of
whether it is a vulnerability, what is affected, and a rough timeline within a week.
If a fix is warranted, we will tell you when it is released and credit you unless you
would rather we did not.

## What is in scope

Everything in this repository: `AceMq.Amqp`, `AceMq.Amqp.RabbitMq`,
`AceMq.Amqp.Diagnostics` and `AceMq.Amqp.DevCerts`.

Things worth reporting even if they feel minor:

- A way to reach a broker without the certificate verification the configuration
  asked for.
- A development certificate accepted without `AllowDevelopmentCertificates()`.
- Anything that renders a credential, a key, or a message body into a log or an
  exception message.
- A message body that can be altered without `EncryptedCodec` rejecting it.
- Anything in `AceMq.Amqp.Diagnostics` that exposes more than metrics, health and
  version.

## What is not

- **The diagnostics endpoints being unauthenticated.** That is documented, and they
  bind to loopback by default. A report that they are reachable when deliberately
  bound to `0.0.0.0` is not a vulnerability in the library.
- **`TlsOptions.Insecure()` accepting any certificate.** That is what it is for, it
  says so, and it is refused for development certificates on top.
- **Vulnerabilities in RabbitMQ itself** — report those to Broadcom.
- Findings from a scanner with no demonstrated impact.

## Supported versions

Pre-1.0, only the latest release. There are no maintenance branches yet, so a fix
means a new patch version.

## What this library does not do for you

It secures the connection and, optionally, the message body. It does not manage
broker users or permissions, hold your keys, or authenticate the diagnostics
endpoints. The [security guide](https://acemq.org/acemq-dotnet-amqp/security.html)
says which is which, and ends with a production checklist.
