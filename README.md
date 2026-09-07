# acemq-dotnet-amqp

[![ci](https://github.com/AceMQ-Company/acemq-dotnet-amqp/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-dotnet-amqp/actions/workflows/ci.yml)
[![release](https://github.com/AceMQ-Company/acemq-dotnet-amqp/actions/workflows/release.yml/badge.svg)](https://github.com/AceMQ-Company/acemq-dotnet-amqp/actions/workflows/release.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-dotnet-amqp/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-dotnet-amqp/actions/workflows/attribution-guard.yml)
[![version](https://img.shields.io/badge/version-0.1.7-blue)](https://acemq.org/nuget/)
[![packages](https://img.shields.io/badge/packages-acemq.org%2Fnuget-blue)](https://acemq.org/nuget/)
[![docs](https://img.shields.io/badge/docs-acemq.org-blue)](https://acemq.org/acemq-dotnet-amqp/)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-netstandard2.0-512BD4)](#requirements)
[![brokers](https://img.shields.io/badge/broker-RabbitMQ-lightgrey)](#requirements)

AceMQ for .NET. The same message envelope, the same patterns and the same metric
names as [acemq-java-amqp](https://github.com/AceMQ-Company/acemq-java-amqp) — with a
native API rather than a transliterated Java one.

> **Status: pre-1.0, not published.** Publishing, consuming, topology, retries and
> dead-lettering work against a real broker, and the integration suite runs against
> RabbitMQ in CI. There is no package on any feed yet and the API is still free to
> change.

## What is here

| | |
|---|---|
| `AceMqConnection` | Connect, declare topology, create publishers and consumers |
| `IPublisher<T>` | Publishing with confirms, back pressure, unroutable messages reported |
| `ConsumeAsync<T>` | Handlers returning a disposition: accept, retry, dead-letter, park, release |
| `ICodec` | JSON by default, camelCase on the wire so C# and Java agree; raw bytes available |
| `Topology` | exchanges, queues and dead-letter wiring declared as one unit |
| `Requester` / `Responder` | request and reply, correlated on a shared reply queue |
| `OrderedQueue<T>` | order per key across partitions; a partition halts rather than reorder |
| `Pipeline<T>` | steps on their own queues, type-checked against each other |
| `OutboxRelay` | publish what was written in the same transaction as your data |
| `RetryLadder` | the `{queue}.retry.{delay}` queues a long backoff waits in |
| `Replay` | put dead-lettered messages back, on a fresh set of attempts |
| `StreamReader<T>` | read a stream from an offset |
| Codecs | JSON, XML, text, bytes, composite, and `EncryptedCodec` for payload encryption |
| `AceMq.Amqp.Protobuf` | Protocol Buffers, in its own package so the core keeps no serialization dependency |
| `AceMq.Amqp.Avro` | Avro, with a fixed schema or a registry that makes adding a field safe |
| `AceMq.Amqp.Yaml` | YAML, for messages a person will read |
| `AceMq.Amqp.Toml` | TOML, the same but without the ambiguity |
| `DbOutboxStore` / `DbIdempotencyStore` | ADO.NET, so the outbox commits with your data |
| Interceptors | run around every publish and every handled message |
| `RoutingSlip` | a route the message carries, changeable at each step |
| `DbSchemaRegistry` | schema ids that mean the same thing in every process |
| `TlsOptions` / `ICredentialsProvider` | TLS verified by default, private CAs trusted properly, secrets out of the URL |
| `AceMq.Amqp.DevCerts` | a tool that generates development certificates the library then refuses by default |
| `RetryPolicy` / `IIdempotencyStore` | bounded backoff with two-sided jitter, waited here or in the broker, and at-least-once made safe |
| `AceMqTelemetry` | Meter and ActivitySource instrumentation, Java's metric names, no OTel dependency |
| `AceMqActuator` | `/acemq-metrics`, `/acemq-health`, `/acemq-info` over HTTP, no ASP.NET Core |
| `RabbitMqTransport` | RabbitMQ, over `amqp://` and `amqps://` |
| `InMemoryTransport` | An in-process broker for tests, routing the way RabbitMQ routes |
| `AceHeaders`, `Envelope` | The header names and the envelope, pinned by conformance tests |

```csharp
using var mq = await AceMqConnection.ConnectAsync("amqp://localhost");

using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", async message =>
{
    await orders.RecordAsync(message.Payload);
    return Ack.Accept();
});

var publisher = mq.Publisher<OrderPlaced>("orders", "order.placed");
await publisher.SendAsync(new OrderPlaced("A-1", 42.50m));
```

Documentation: **<https://acemq.org/acemq-dotnet-amqp/>**

## How a retry actually works

Five decisions here are cross-language contract rather than local preference. The same
message can be retried by a consumer written in Java, Go, .NET, Python or Ruby, and two
services consuming the same queue declare the same broker objects, so a difference in
any of these is not a difference in style.

**A retry is republished, not requeued.** The message goes back onto its queue as a new
publish with `x-acemq-attempt` advanced, and only then is the original acknowledged. A
requeue hands back the bytes the broker was given, so the attempt header never advances
and the count has to live in the consumer's memory — where it is per-process, lost on
restart, and wrong the moment a second consumer joins the queue, because a requeued
message can come back to one that has never seen it and calls it attempt one. What
republishing costs is that a retried message goes to the back of its queue rather than
the front, and that a crash between the publish and the acknowledgement delivers it
twice.

**Dead-lettering and parking are republishes too.** A message that has run out of
attempts is republished to `{queue}.dlq` with the reason on `x-acemq-error`, and one
whose body will not decode goes to `{queue}.parked` — a different queue because "failed
five times" and "nothing could read it" want different people. Rejecting the message
instead would hand it to whatever dead-lettering the queue happens to be declared with,
which is usually nothing, and neither the broker nor the queue can write down what the
handler was unable to do.

**Short waits happen here, long ones happen in the broker.** Below
`RetryPolicy.DefaultBrokerWaitThreshold` — thirty seconds, and configurable with
`WaitInBrokerFrom`, where zero turns broker waits off entirely — the consumer holds the
delivery and sleeps. At or above it the message is published into a
`{queue}.retry.{delay}` queue whose `x-message-ttl` is the wait and whose dead-letter
target is the queue it came from, and the broker hands it back when the time is up. The
reason for the split is that a consumer sleeping on a five-minute backoff is holding an
unacknowledged message: restart it and the broker redelivers at once, so a five-minute
policy delivers in none. Below thirty seconds that costs seconds and a prefetch slot;
above it, it costs the whole wait.

**One queue per distinct delay, never a per-message TTL.** RabbitMQ expires messages
only from the head of a queue, so a single queue of per-message expirations lets one
long wait at the front hold back every shorter one behind it, and the delays that come
out bear no relation to the ones that went in.

**A replay restarts the attempt counter.** `mq.Replay(queue).ReplayAllAsync()` puts each
message back on attempt one with its error cleared, because a message dead-lettered on
the last attempt of its policy would otherwise be dead-lettered again before a handler
saw it — and the operator who has just fixed the bug would have moved two thousand
messages from one queue to the same queue. `KeepingAttempts()` puts back exactly what
was there, for an audit.

```csharp
var policy = RetryPolicy.Exponential(6, TimeSpan.FromSeconds(10));

policy.Schedule();     // 10s, 20s, 40s, 80s, 160s -- without jitter, so it can be read
policy.BrokerRungs();  // 40s, 80s, 160s -- the ones that need a queue

using var consumer = await mq.ConsumeAsync<OrderPlaced>(
    "orders.placed",
    ConsumerOptions.Defaults().WithRetry(policy),
    async message => await warehouse.ReserveAsync(message.Payload)
        ? Ack.Accept()
        : Ack.Retry("the warehouse said no"));
```

The schedule doubles, the ceiling is applied inside the loop as well as after it, and
the 20% jitter moves a delay **both** ways — one-sided jitter only ever delays, which
turns a thundering herd into a slower thundering herd rather than dispersing it. Jitter
is never applied to a broker wait: a rung's time-to-live is fixed when the queue is
declared, so a moved delay would name a queue that is not there, and the spread is free
anyway because each message's time-to-live starts when it arrives. `GiveUpAfter` bounds
by the message's age as well, because attempts alone cannot say that something has
stopped being worth doing.

A rung is declared with exactly three arguments and no others, because two services on
the same queue declare the same rung by name and differing arguments are a
`PRECONDITION_FAILED` that stops the second consuming at all:

```
orders.placed.retry.40s
    x-message-ttl             = 40000
    x-dead-letter-exchange    = "acemq.retry"
    x-dead-letter-routing-key = "orders.placed"
```

> The exchange on that middle line is the one part of this the five libraries do not yet
> agree on: Java declares a named `acemq.retry` direct exchange with a binding per source
> queue, while Python and Ruby use the default exchange, which routes by queue name and
> needs neither. This library follows Java for now because that is what most of the
> released code does. The choice is `RetryLadder.RetryExchange` and the routing key
> derived beside it, and nothing else reads the decision — switching is a one-line edit,
> and `RetryContractTests` pins the table by reading the constant rather than repeating
> it.

## Why the fixtures matter more than the code

`tests/AceMq.Amqp.Tests/fixtures/envelope-fixtures.json` is generated by publishing
through the Java library and reading the message back at the transport level. It is
never hand-written.

Every port that copies a wire format out of documentation acquires a difference
nobody notices until two languages disagree in production. Two implementations
agreeing with the same prose is not interoperability; agreeing with the same bytes
is. See `tools/fixture-generator/`.

Things the fixtures pinned that no document stated plainly:

- `x-acemq-type` defaults to the **routing key**
- `x-acemq-correlation` defaults to the **message id**
- `x-acemq-origin` defaults to `acemq@{hostname}`
- `x-acemq-first-seen` is an **integer of epoch milliseconds**, while
  `x-acemq-replayed-at` is an **ISO-8601 string** — two timestamps, two encodings
- `x-acemq-causation` is **absent** when unset, never null
- the AMQP `messageId` property mirrors `x-acemq-id`

## Target framework

`netstandard2.0`, which reaches .NET Framework 4.6.2+, .NET Core and modern .NET
from one assembly. That is deliberate rather than conservative: the applications
most likely to want a supported AMQP library are the ones that cannot move, and
`netstandard2.0` is the only target that reaches all of them.

A `net8.0` target should be added alongside it for modern consumers; it is absent
only because the SDK on the machine this was written on has no net8.0 targeting
pack.

## VB.NET

Not a separate library — VB and C# compile to the same IL, so a VB application
references this assembly directly. What it costs is an API that stays callable from
VB: no members differing only by case, no `ref struct` or `Span<T>` on the surface,
no overloads separable only by optional arguments. **That audit has to happen before
the API freezes**; afterwards it is a breaking change.

CI compiles *and runs* both examples, which has already caught two differences:
`Dim envelope = Envelope.Of(...)` fails with BC30980 because VB is
case-insensitive, and VB has no async `Main`. Both compile fine in C#.

## Documentation

**<https://acemq.org/acemq-dotnet-amqp/>** — the guide, four tutorials, and the
[API reference](https://acemq.org/acemq-dotnet-amqp/apidocs/).

## Building

```bash
dotnet build
dotnet test tests/AceMq.Amqp.Tests/AceMq.Amqp.Tests.csproj
```

The integration suite needs a broker, and has no skip path — a suite that quietly
does nothing when the broker is missing reports a green tick for work nobody did:

```bash
ACEMQ_TEST_AMQP_URL=amqp://guest:guest@localhost:5672 \
  dotnet test tests/AceMq.Amqp.RabbitMq.Tests/AceMq.Amqp.RabbitMq.Tests.csproj
```

## Next

A C# service consuming a message a **Java** service published, envelope intact.
The conformance fixtures already pin the two implementations to the same bytes;
what has not been demonstrated is the two libraries talking to one broker at the
same time.

After that: outbox and idempotency, request/reply, streams, and OpenTelemetry —
the parts of the Java library this does not have yet.

## Licence

Apache-2.0. RabbitMQ is a trademark of Broadcom Inc.; this project is not affiliated
with it.
