# Examples

Two of them, one per language, both runnable with nothing installed — they use the
in-process broker.

```bash
dotnet run --project examples/csharp
dotnet run --project examples/vb
```

They print **identical output**, which is the claim VB support rests on: C# and
VB.NET compile to the same IL and reference the same assembly, so there is no
separate VB library and there will not be one.

Between them they exercise publishing with confirms, consuming with dispositions,
the envelope, an unroutable publish failing rather than vanishing, topology declared
as one unit with its dead-letter wiring, request and reply, and ordering by key.

CI compiles **and runs** both. That is not ceremony: the first VB example written for
this repository failed to compile on a case-insensitivity rule C# does not have, and
the second failed because VB has no async `Main`. Neither would have been found by
reading the code.

## Pointing them at a real broker

Both use `memory://`. For RabbitMQ, add the transport package and change two lines:

```csharp
using AceMq.Amqp.RabbitMq;

Transports.Register(new RabbitMqTransport());
using var mq = await AceMqConnection.ConnectAsync("amqp://guest:guest@localhost:5672");
```

Nothing else changes.

## Longer worked examples

The [tutorials](https://acemq.org/acemq-dotnet-amqp/tutorials.html) go further than
these do — four of them, in order, each ending with something that runs, and covering
retries, dead-lettering, idempotency, the outbox and observability against a real
broker.

The [interop harness](../../acemq-amqp-libraries/scripts/dotnet/interop) is the other
thing worth seeing: a Java service publishes, this library consumes, and every
envelope field is checked. It is not in this repository because it needs the Java
library built alongside it.
