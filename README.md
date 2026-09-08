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
| `Topology` | exchanges, queues and dead-letter wiring declared as one unit, on `acemq.dlx` |
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
| `AceMq.Amqp.Xml` | XML that Java, Go, Python and Ruby read and write, refusing every DTD |
| `DbOutboxStore` / `DbIdempotencyStore` | ADO.NET, so the outbox commits with your data |
| Interceptors | run around every publish and every handled message |
| `RoutingSlip` | a route the message carries, changeable at each step |
| `Saga<T>` | steps that undo themselves, compensated in reverse, reporting what could not be |
| `Scheduler` | deliver later, through a ladder of TTL queues rather than one that expires only at its head |
| `DbSchemaRegistry` | schema ids that mean the same thing in every process |
| `TlsOptions` / `ICredentialsProvider` | TLS verified by default, private CAs trusted properly, secrets out of the URL |
| `AceMq.Amqp.DevCerts` | a tool that generates development certificates the library then refuses by default |
| `RetryPolicy` / `IIdempotencyStore` | bounded backoff with two-sided jitter, waited here or in the broker, and at-least-once made safe |
| `AceMqTelemetry` | Meter and ActivitySource instrumentation, Java's metric names, no OTel dependency |
| `AceMqDiagnostics` | the events an operator needs said out loud, bridged to `ILogger` |
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

Both queues are bound by their own names to one durable direct exchange, `acemq.dlx`,
which is what Java declares and what Go, Python and Ruby are converging on:

```
acemq.dlx  (direct, durable)
    orders.placed.dlq     (classic)  bound on 'orders.placed.dlq'
    orders.placed.parked  (classic)  bound on 'orders.placed.parked'

orders.placed  (quorum)
    x-dead-letter-exchange    = "acemq.dlx"
    x-dead-letter-routing-key = "orders.placed.dlq"
```

**A source queue is a durable quorum queue, and the queues around it are classic.**
`Topology.Builder.Queue`, `QueueWithDeadLetter`, `QueueWithRetry` and
`DeclareQueueAsync(name)` all declare quorum, because Java, Go, Python and Ruby do.
This is interoperability before it is durability: a queue's type is fixed when it is
created, so a Java service declaring `orders` as quorum and a .NET service declaring
the same name as classic do not get one queue each — the second is refused with
`PRECONDITION_FAILED` and cannot consume at all. Pass `QueueType.Classic` where a
single node is what you want.

The retry rungs, `{queue}.dlq` and `{queue}.parked` stay classic, because that is what
the other four libraries declare them as and a disagreement in either direction is the
same refusal. So does anything **exclusive or auto-delete**: RabbitMQ does not allow a
quorum queue to be either, so a reply queue or a per-connection temporary queue has to
be classic, and `Requester` asks for classic explicitly rather than inheriting the
default.

`Topology.QueueWithDeadLetter` produced `{queue}.dlx` and `{queue}.dead` until 0.1.9,
which meant one repository held three conventions and which one you got depended on
whether the queue was created by a topology or by a consumer. It now produces the same
shape as everything else. The routing-key override is not decoration: a message the
broker dead-letters keeps the routing key it arrived under, so on a shared direct
exchange it would otherwise match no binding and be dropped. `Replay` still recognises
a `.dead` suffix, because brokers already running have queues by that name with
messages in them and `mq.Replay("orders.dead")` must not start draining a queue into
itself.

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

> The exchange on that middle line was the last part of this the five libraries had not
> settled. It is settled now, on Java's shape: a named `acemq.retry` direct exchange with
> a binding per source queue, rather than the default exchange Python and Ruby were
> using. The choice is `RetryLadder.RetryExchange` and the routing key derived beside it,
> and nothing else reads the decision — `RetryContractTests` pins the table by reading
> the constant rather than repeating it.

## When something goes wrong with a message

Metrics say how many and traces say where, and neither of them tells an operator
*which message and why* unless the process happens to be exporting traces and somebody
happens to look at the right span. `AceMqDiagnostics` is the channel for that. Four
events, each with the queue, the destination, the message id and the attempt:

| | |
|---|---|
| `acemq.move.failed` | a republish failed, so the message was handed back to the broker with the attempt **not** advanced — a redelivery loop with nothing to explain it |
| `acemq.message.dead-lettered` | attempts exhausted, or a handler gave up |
| `acemq.message.parked` | the body could not be read |
| `acemq.retry.rung-missing` | a broker wait was asked for with no rung to spend it in, so the wait fell back to this process where a restart loses it |

```csharp
// AceMq.Amqp.Diagnostics, which is where the Microsoft.Extensions.Logging
// dependency lives rather than in the core package.
using var logging = LoggerSink.SubscribedTo(loggerFactory);
```

Implement `IDiagnosticSink` directly if the application logs to something else — it is
one method, and a sink that throws is swallowed rather than turned into a crashed
consumer. The core package deliberately does not reference
`Microsoft.Extensions.Logging.Abstractions`: on `netstandard2.0` it brings
`Microsoft.Extensions.DependencyInjection.Abstractions`, `System.Buffers` and
`System.Memory` with it, which is four packages added to a library that has two and a
dependency-injection abstraction handed to every .NET Framework application that only
wanted to publish a message. The same line the library takes on OpenTelemetry for
metrics and ASP.NET Core for the actuator: seam in the core, adapter in the optional
package.

## Why the fixtures matter more than the code

### How we know the five libraries agree

Java, Go, .NET, Python and Ruby each carry a byte-identical copy of two generated
fixtures and assert against them. Neither is hand-written, and neither is written
here: the Java library's test suite generates both, CI regenerates them, and every
other repository commits the same bytes.

| Fixture | Generated from | What it pins |
| --- | --- | --- |
| `tests/AceMq.Amqp.Tests/fixtures/envelope-fixtures.json` | publishing through the Java library and reading the message back at the transport level | the wire headers |
| `tests/AceMq.Amqp.Tests/fixtures/contract-fixtures.json` | Java's `ContractFixtures` generator | the retry schedule, the queue names, the rung arguments and the declared topology |

A third fixture pins a narrower thing and is not part of that set:
`tests/AceMq.Amqp.Xml.Tests/fixtures/xml-interop-samples.json` carries XML message
bodies exactly as the Java and Go libraries wrote them, copied from the Ruby and
Python repositories' own copies. `AceMq.Amqp.Xml` is asserted against those bytes
rather than against its own output, because a codec that decodes what it encoded has
proved nothing about reading a Java message. It is what caught the one real
divergence in the format: Jackson wraps a list in an element of its own where Go's
`encoding/xml` repeats the sibling, and both shapes are in the file because both are
real.

`../scripts/check-fixtures.sh` compares all five copies of both files and fails on a
single changed character. That check is the load-bearing part. A library that quietly
edits its own copy still passes its own suite — it is simply agreeing with the wrong
file, in private, which is the exact failure the fixtures exist to prevent applied to
the fixtures themselves.

The assertions live in `EnvelopeConformanceTests` and `ContractConformanceTests`.
They derive their expectations wherever they can rather than reading a number out of
the fixture and comparing it to the same number read out of this library, which would
prove only that both can be read: the doubling is checked by asserting each delay is
twice its predecessor, the broker-wait table is recomputed from the rule the fixture
states in prose, the rung names are rendered a second time by a separate
implementation, and the jitter bounds are sampled from this library rather than read
back.

This matters because it is not hypothetical. Java shipped `exponential` with a
multiplier of five and ten percent jitter for ten releases while the other four
libraries all doubled with twenty — `exponential(5, 1s, 1m)` gave `1s, 5s, 25s, 60s`
there and `1s, 2s, 4s, 8s` here — and nobody noticed for months, because every library
tested its own arithmetic against its own expectations and passed. Three more
divergences turned up the same way in a single day: the retry rung's dead-letter
exchange, the source queue's dead-letter arguments, and the queue type. All four were
found by a person reading five codebases side by side, which is not a process.

### Where this library and the fixture still disagree

Recorded rather than smoothed over, and each one asserted as a named exception in
`ContractConformanceTests` so that a *new* disagreement fails the build and a resolved
one does too:

- **Sub-second rung names.** This library renders a 500ms rung as
  `orders.new.retry.500ms`, which is what Java does; Go, Python and Ruby render
  `orders.new.retry.0s`. Unreachable through the default thirty-second broker-wait
  threshold, and not resolvable from one repository.
- **The default message-age ceiling.** Java gives every policy a maximum message age
  of 365 days, so it refuses to retry a message that has reached exactly that age.
  Here — and, per its own comment, in Python and Ruby — zero means never, and such a
  message is still retried.
- **The jitter floor.** Java's `applyJitter` ends in `Math.max(1, …)`, so its smallest
  jittered wait is 1ms. This library multiplies a `TimeSpan` and floors at zero, so
  `Fixed(2, 1ms).WithJitter(1.0)` can produce a sub-millisecond wait. Latent rather
  than live: no policy in this repository has a consumer wait of a millisecond.

One that used to be on this list and no longer is, since it is the shape of the thing
these fixtures are for. **When the dead-letter queues are declared** was a difference
about *when* rather than *what*: Java's consumer declared `acemq.dlx`, `{queue}.dlq` and
`{queue}.parked` at start-up, and this library declared the same three things — same
types, same durability, same bindings — on the settle path, the first time a message was
actually dead-lettered or parked. The end state on a broker was identical, which is why
it looked like a preference. It was not. A consumer that gives up republishes to
`{queue}.dlq`, and until something first failed there was nothing on the broker for an
operator to see, to alert on, or for the broker's own dead-lettering to reach. ADR-032
settled it Java's way, and this library now declares all three when a consumer starts —
whether or not it has a retry policy, since giving up is not something a retry policy
switches on.

Things the fixtures pinned that no document stated plainly:

- `x-acemq-type` defaults to the **routing key**
- `x-acemq-correlation` defaults to the **message id**
- `x-acemq-origin` defaults to `acemq@{hostname}`
- `x-acemq-first-seen` is an **integer of epoch milliseconds**, while
  `x-acemq-replayed-at` is an **ISO-8601 string** — two timestamps, two encodings
- `x-acemq-causation` is **absent** when unset, never null
- the AMQP `messageId` property mirrors `x-acemq-id`
- a retry rung carries **exactly three** arguments — a fourth is a
  `PRECONDITION_FAILED` for the second service that declares that queue
- the source queue is **quorum**; the rungs, `.dlq` and `.parked` are **classic**

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

It leaves the broker as it found it — both counts, not just the queues. Until
`ITransportConnection.DeleteExchangeAsync` existed there was no way to undo
`DeclareExchangeAsync`, so every run left an `acemq.test.{suffix}` exchange behind for
ever and a broker used for integration testing became a list of everything anybody had
ever tested. `acemq.retry` and `acemq.dlx` do survive a run, deliberately: they are
declared once and shared by every queue on the broker, exactly as in a real deployment,
so deleting them would be the suite tearing down somebody else's topology rather than
its own.

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
