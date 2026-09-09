# The envelope

What travels with a message besides its body: identity, causation, and the counters
the retry engine keeps. It is the one thing every AceMQ library must agree on
exactly, in every language, or two services stop understanding each other.

## The headers

| Header | Type | Meaning |
|---|---|---|
| `x-acemq-id` | string | Unique message identifier, and the default idempotency key |
| `x-acemq-type` | string | Logical message type, for example `order.placed` |
| `x-acemq-version` | integer | Schema version of the payload |
| `x-acemq-correlation` | string | Business correlation, propagated unchanged across hops |
| `x-acemq-causation` | string | The message that caused this one |
| `x-acemq-attempt` | integer | Delivery attempt, starting at 1 |
| `x-acemq-first-seen` | **integer** | Epoch **milliseconds** of the first publish |
| `x-acemq-origin` | string | The publishing process, conventionally `service@host` |
| `x-acemq-error` | string | Why a message was dead-lettered |
| `acemq-replayed-from` | string | Queue a message was replayed out of |
| `acemq-replayed-at` | **string** | RFC 3339 instant, when last replayed |
| `acemq-replay-count` | integer | How many times it has been replayed |
| `traceparent` / `tracestate` | string | W3C trace context |

Note the two timestamps are encoded **differently** — `first-seen` is an integer,
`replayed-at` is a string. That is not an inconsistency to tidy up in a port. It is
the contract, and a port that "fixes" it produces messages Java cannot read.

## `acemq-` is defined but not reserved

The three replay stamps do **not** carry the `x-acemq-` prefix, and neither do
`acemq-reply-to` or `acemq-routing-slip`. They are written by a pattern rather than
by the engine, and they have to reach the handler: a header in the reserved namespace
is stripped on consume, which for these would mean writing them and finding them
gone. `AceHeaders.SharedPrefix` names this namespace, and `AceHeaders.IsAceHeader`
deliberately does not match it.

**These three changed spelling after 0.5.0.** Up to and including that release this
library wrote `x-acemq-replayed-from`, `x-acemq-replayed-at` and
`x-acemq-replay-count` — the reserved namespace, which every AceMQ library including
this one strips before a handler sees it. A message a .NET operator replayed
therefore reached a Java, Go, Python or Ruby handler with its provenance silently
removed, and reached a .NET handler the same way. Java made the same move in 0.5.0
and was the second-to-last to make it. **Anything matching on the old names — a
shovel policy, a dashboard, a firehose consumer — needs the new ones.** The old
spelling is still *read*, so a message a 0.5.0 service replayed keeps its replay
count when a newer service replays it again; it is never written.

## `x-acemq-` is reserved

A header carrying that prefix belongs to the engine. It is materialised onto the
envelope if this version knows it, and **dropped from the application's headers
either way**.

Use your own namespace — `x-yourcompany-` — for anything that must survive the
round trip. In this library, writing into the reserved namespace throws at the call
site rather than vanishing in transit:

```csharp
Envelope.Of("t").Header("x-acemq-id", "mine");   // ArgumentException
```

Java drops it silently on consume. Failing early is kinder than a header that
disappears with nothing reporting the loss.

## The defaults are contract, not convenience

Generated fixtures pinned these. Every one would have been a guess if transcribed
from prose:

| When you do not set | It becomes |
|---|---|
| `type` | the **routing key** |
| `correlation` | the **message id** |
| `origin` | `acemq@{hostname}` |
| `version`, `attempt` | `1` |
| `causation` | **absent** — the header is omitted, not written as null |

The AMQP `messageId` property also mirrors `x-acemq-id`.

## How this is kept honest

`tests/AceMq.Amqp.Tests/fixtures/envelope-fixtures.json` is **generated**, never
written. A program publishes through `acemq-java-amqp` and pulls the message back at
the transport level — the only place the engine's own headers are still visible,
since the consumer API strips them by design.

The C# tests then assert against those bytes, in `EnvelopeConformanceTests`.

The generator lives in the Java repository's own test suite, so CI regenerates the
fixtures on every change and fails when they differ from what is committed. That
turns "the port drifted" from something a customer finds into a red build on the
commit that caused it.

A second fixture, `tests/AceMq.Amqp.Tests/fixtures/contract-fixtures.json`, pins the
behaviour either side of the wire rather than the wire itself: the retry schedule, the
queue names, the rung arguments and the declared topology. It is generated the same
way, carried byte-identically by all five libraries, and asserted in
`ContractConformanceTests`. `../scripts/check-fixtures.sh` compares every copy and
fails on a single changed character. See [Testing](testing.md#cross-language-conformance)
for what it pins and where this library still disagrees with it.
