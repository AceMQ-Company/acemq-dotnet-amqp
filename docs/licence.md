# Licence and warranty

AceMQ for .NET is [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
You may use it in production, commercially, without asking and without paying.

## No warranty

The licence disclaims warranties and limits liability — sections 7 and 8. In plain
terms: this is provided as it is, and if it loses your messages that is your risk to
have taken.

That is not a formality to skim. It is a young library: pre-1.0, with an API still
free to change, and its own documentation says which parts have been proven against a
real broker and which have not. Read [testing](testing.md) for what the in-memory
broker does and does not verify.

If you need somebody accountable for it working, that is what
[Enterprise support](https://acemq.com) is for. The library is complete and free
without it, and is not crippled to sell it.

## What you must do

Keep the licence and the copyright notice with any copy or derivative, and state what
you changed. That is the whole obligation.

## Dependencies

| | |
|---|---|
| `System.Text.Json` | MIT, Microsoft |
| `System.Diagnostics.DiagnosticSource` | MIT, Microsoft |
| `RabbitMQ.Client` | Apache-2.0 or MPL-2.0, Broadcom |

`AceMq.Amqp.Diagnostics` adds nothing — it uses `HttpListener` from the framework.

## Trademarks

RabbitMQ is a trademark of Broadcom Inc. and/or its subsidiaries. .NET is a
trademark of Microsoft. AceMQ is an independent project, affiliated with and endorsed
by neither.
