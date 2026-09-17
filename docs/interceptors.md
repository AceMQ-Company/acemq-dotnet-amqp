# Interceptors

Every organisation has something that belongs to every message and no library can
guess: a tenant identifier, an audit record, a policy check on what is allowed out,
a counter. Without a seam these get copied into every call site, where one of them
is eventually forgotten — and nobody finds out until the message that needed it is
the one that went without it.

```csharp
mq.Intercept(new TenantStamp(tenant));   // around every publish
mq.Intercept(new AuditTrail(audit));     // around every handled message
```

Both overloads return the connection, so they chain, and both are meant to be
called at start-up.

## Two chains, not one

Publishing and consuming have separate interfaces, and an interceptor is in one
chain or the other. Which overload of `Intercept` you reach is decided by which
interface the object implements, so a class may implement both and be registered
twice.

| `IPublishInterceptor` | `IConsumeInterceptor` |
|---|---|
| `BeforePublish(context)` → `PublishContext` | `BeforeHandle(context)` |
| `AfterConfirm(context, result)` | `AfterHandle(context, ack)` |
| `OnError(context, failure)` | `OnError(context, failure)` |
| `Order` | `Order` |

Inherit `PublishInterceptor` or `ConsumeInterceptor` and override only what you
care about. The base classes exist rather than default interface members because
C# cannot carry an implementation on an interface on `netstandard2.0`, and VB
cannot call one at all — see [where it runs](index.md). Without them an
interceptor that cares about one of three moments would have to write two empty
methods.

```csharp
sealed class SizeLimit : PublishInterceptor
{
    public override int Order => -100;

    public override PublishContext BeforePublish(PublishContext context)
    {
        if (context.Payload is string body && body.Length > 128 * 1024)
        {
            throw new InvalidOperationException(
                $"{context.RoutingKey} is too big to publish");
        }
        return context;
    }
}
```

Everything an interceptor is handed is public API. It is registered through a
public method and given a context whose every member is public, which is the same
rule the [patterns](patterns.md) follow: anything an interceptor can do,
application code could have done, and nothing here reaches inside the library.

## What a publish interceptor can change

`PublishContext` is immutable, and one member of it can be replaced:

| | |
|---|---|
| `Exchange` | where it is going — **read-only** |
| `RoutingKey` | what it is published under — **read-only** |
| `Payload` | the object about to be encoded — **read-only** |
| `Envelope` | the metadata — replaced with `WithEnvelope(...)` |

**Only the envelope can be changed.** An interceptor that could rewrite the
destination would be able to send a message somewhere the caller never asked for,
and one that could rewrite the payload could change what a caller believes it
sent — a surprising amount of power for something usually added to attach a
header. `Payload` is visible, before the codec runs, so a check can be made
against the object rather than against bytes; it cannot be substituted.

Return the context you were given to leave the publish alone. Returning `null`
throws `AceFatalException` naming the interceptor, because a chain that silently
dropped the context would publish an empty message and say nothing.

### Rebuilding an envelope copies nothing

There is no `SetHeader` here and no copy constructor on `Envelope`. `WithEnvelope`
takes a whole replacement, and `Envelope.Of(type)` starts a **fresh** builder:
anything you do not carry across is gone, replaced by the builder's defaults — a
new `FirstSeen`, `Version` back to 1, `Attempt` back to 1, `Origin` recomputed,
and every header the caller set dropped.

That is the one real trap on this page. Copy the fields:

```csharp
sealed class TenantStamp : PublishInterceptor
{
    private readonly string _tenant;
    public TenantStamp(string tenant) => _tenant = tenant;

    public override PublishContext BeforePublish(PublishContext context)
    {
        var from = context.Envelope;
        var to = Envelope.Of(from.Type)
            .Id(from.Id)
            .Version(from.Version)
            .CorrelationId(from.CorrelationId)
            .CausationId(from.CausationId)
            .Attempt(from.Attempt)
            .FirstSeen(from.FirstSeen)
            .Origin(from.Origin)
            .Error(from.Error);

        foreach (var header in from.Headers) to.Header(header.Key, header.Value);
        to.Header("x-tenant", _tenant);

        return context.WithEnvelope(to.Build());
    }
}
```

`Builder.Header` still refuses the reserved `x-acemq-` namespace, whoever is
calling it — see [the envelope](envelope.md). A routing slip's headers are merged
on top of the envelope's *after* the chain has run, so an interceptor neither sees
a slip nor can disturb one.

## What a consume interceptor can change

Nothing about the message. `BeforeHandle` returns `void` and `ConsumeContext` —
`Queue`, `Envelope`, `Payload` — is read-only throughout.

What it *can* do is refuse the message: throwing from `BeforeHandle` stops the
handler running and dead-letters the delivery with the reason on it, which is
[described below](#throwing-means-different-things-in-different-places) and is
the same thing a Go consume interceptor does by returning an error.

That still rules out two of the three things people arrive looking for: a header
added on the way in so the handler and any dead letter both carry it, and a
payload decrypted or redacted before the handler sees it. Neither can be written
as a consume interceptor here. Python and Ruby can do both; this library cannot,
and a [pipeline](patterns.md#pipelines) around the one handler that needs it is
the way to get there today.

## Where each chain sits

On a publish:

```
BeforePublish  →  codec  →  span starts  →  broker  →  confirm  →  AfterConfirm
                                                                   OnError
```

`BeforePublish` runs **before the codec**, so an envelope it changes reaches the
wire, and **before the telemetry span is started**, so `Activity.Current` there is
whatever the caller's was — not the publish span. Time spent in `BeforePublish` is
also outside `acemq.publish.duration`, which starts with the send. `AfterConfirm`
and `OnError` run inside the span's scope but after the duration has been
recorded, so they can add attributes to `Activity.Current` and cannot affect the
metric.

On a consume:

```
decode  →  idempotency claim  →  span starts  →  BeforeHandle  →  handler
                                                 (or refuse)   →  OnError (it threw)
                                                               →  AfterHandle  →  settle
```

The consume span is started **after** the body has been decoded and **before** the
chain runs, so an interceptor on this side runs inside it — a refusal has a span
to close and tag `dead_lettered`, which is why it is started there. Decode time is
in neither the span nor `acemq.consume.duration`, and `acemq.consume.duration`
still starts after the chain has run: time spent in `BeforeHandle` is not counted
as time the handler took. If you are adding an interceptor to measure how long a
message takes, what you will measure is the handler and not the delivery. See
[metrics and tracing](observability.md#tracing) for what is measured instead.

Two deliveries never reach the chain at all:

- **A body that will not decode** is parked before the context is built — see
  [consuming](consuming.md#a-message-that-cannot-be-decoded-is-not-retried). A
  consume interceptor is therefore not a complete record of what arrived.
- **A duplicate refused by an idempotency store** is accepted before the chain
  runs, because the claim is taken first — see
  [duplicates](reliability.md#duplicates).

## The `Ack` is what the handler asked for

`AfterHandle` is given the handler's disposition, and the delivery is settled
*after* it returns. Those are not the same thing. A handler asking for another
attempt when the [retry policy](reliability.md#retry-policies) has none left is
dead-lettered, and an interceptor that recorded the `Ack` would report a retry
that never happened — which is how a dashboard ends up with no dead letters on it
and a dead-letter queue that is full.

There is no way to close that gap from an interceptor in this library. Python has
`when_settled` and Ruby has `context.settlement`; .NET has neither, and the
outcome the consumer actually chose is visible only on the
[span and the counters](observability.md#what-it-records). Read the outcome there
rather than inferring it from the `Ack`.

## Throwing means different things in different places

On purpose, mostly.

**From `BeforePublish` it stops the publish** and the caller sees the exception.
That is the point of intercepting rather than observing: a message that must not
go out is stopped once, here, rather than in every publisher. Note what does *not*
happen — `OnError` is not called, because nothing was published to fail, and
interceptors earlier in the chain that already ran are not told. An interceptor
that has to undo something it did cannot rely on being called back.

**From `AfterConfirm` or `OnError` on a publish it is swallowed.** The broker
already has the message; reporting a failure would have the caller send it a
second time. An interceptor that must not lose an audit record has to make that
record durable itself.

**From `BeforeHandle` it refuses the message, which is dead-lettered.** The
handler does not run, interceptors registered after the one that threw do not run,
and the message goes to `{queue}.dlq` with the reason on its envelope:

```
an interceptor refused it: tenant "beta" is not served by this process
```

Dead-lettered rather than retried, because an interceptor that says no to a
message will say no to it again and a retry ladder would only spend its attempts
finding that out. It is counted as `dead_lettered` on `acemq.consume.total` and
reaches `acemq.messages.dead.lettered.total` like any other give-up, and the
`acemq.message.dead.lettered` [diagnostic event](observability.md) carries the
exception. That is Go's behaviour, in Go's words, so an operator draining a
dead-letter queue fed by services in two languages reads one sentence rather than
two.

Up to and including 0.5.0 this was the one hole in the ladder: the exception was
not caught at all, so it reached the transport, which turned it into a bare
requeue. No attempt counter advanced — a requeue hands back the bytes the broker
was given — nothing was recorded and nothing gave up, so an interceptor that
always threw was an infinite redelivery loop that no panel could see.

**From `AfterHandle` or `OnError` on a consume it is swallowed**, the same as
`AfterConfirm` on the publish side and for the same reason. `AfterHandle` is the
case worth spelling out: by the time it runs the handler has finished and whatever
it did is done — rows written, an email sent, a payment taken. Retrying would do
all of that a second time and dead-lettering would file a message that was handled
perfectly well, so **the disposition the handler asked for stands** and the
delivery is settled exactly as it would have been. `OnError` is the same case one
step earlier: a throw from it must not replace the failure the handler actually
had, which is the one the message is dead-lettered or retried with.

Neither is silent. Both report `acemq.interceptor.failed` through
`AceMqDiagnostics` at warning level, naming the interceptor and the moment, and
that report is the only record the exception leaves — so an interceptor that must
not lose an audit record has to make that record durable itself.

**`AceFatalException` from the handler skips `OnError`.** It is converted straight
to a dead-letter, because it means an attempt that cannot be fixed by another
attempt; only ordinary exceptions reach `OnError`. Either way `AfterHandle` still
runs, with the disposition the failure produced.

## Order

`Order` decides the sequence, lowest first, and it is read once when the
interceptor is registered. Two things follow that differ from the other libraries:

- **Ties are not stable.** Registration order is not preserved among interceptors
  sharing an `Order`; give them distinct values if the sequence matters.
- **The chain is not reversed on the way out.** `AfterConfirm` and `AfterHandle`
  run in the same order as `BeforePublish` and `BeforeHandle`, so interceptors do
  not nest. A pair that opens something on the way in and closes it on the way out
  closes them in the order they were opened, not the reverse.

There is also no way to override an `Order` at registration; it comes from the
object.

## When the list is read

The two chains differ, which is easy to trip over.

A **publisher takes the publish interceptors that exist when it is created**, so
one registered afterwards does not apply to publishers that already exist. That is
deliberate: an interceptor appearing part way through a process's life would make
two otherwise identical publishers behave differently, for reasons nothing in the
code shows.

A **consumer reads the consume interceptors on every delivery**, so one registered
afterwards *does* apply to consumers that already exist — though not to a message
already being handled.

Register both at start-up and the difference never comes up.

Every publish the library makes for you goes through the same publisher factory,
so the publish chain also runs for replies, pipeline forwards, routing-slip
forwards, scheduled messages, replays and the outbox relay. The relay is the one
worth knowing about: its payload was serialised inside the writer's transaction,
so `context.Payload` there is the bytes, not your object.

## Batch publishes

`SendAllAsync` runs the publish chain **once per message**, the same as if you had
called `SendAsync` in a loop. Nothing is batched about the interceptors.

What changed when `SendAllAsync` was rewritten to pipeline the batch — see
[several at once](publishing.md#several-at-once) — is *when* they run. Every
message is handed to the transport before any confirm is awaited, and
`BeforePublish` runs before the handover, so:

- **Every `BeforePublish` in the batch runs before the first `AfterConfirm`.** It
  is no longer one message's round trip at a time, so an interceptor that assumed
  it would see message two only after message one was confirmed is wrong now. This
  holds under back pressure too: `MaxOutstandingPublishes` blocks after the chain
  has run, not before it.
- **`AfterConfirm` fires in the order the broker answers**, which is not
  necessarily the order of the payloads. The `PublishResult` list the caller gets
  back is still in payload order; the interceptor callbacks are not.
- **A `BeforePublish` that throws fails that one message, not the batch.** The
  rest are still published, and the `PublishFailedException` at the end counts it
  among the failures — so an interceptor used as a policy gate rejects individual
  messages out of a batch rather than stopping it.

Each message in a batch is published under an envelope the library builds, so
`context.Envelope` there is a fresh one with the routing key as its type. A batch
cannot carry envelopes of the caller's own.

## Threads

An interceptor is called on whichever thread is publishing or handling, so one
that keeps state has to be safe to call from several at once. Registration is
itself thread-safe at any time.

The `PublishContext` handed to `AfterConfirm` is **not** the same instance as the
one `BeforePublish` returned — it is rebuilt around the final envelope — so
identity cannot be used to carry state from one to the other. On the consume side
the *same* `ConsumeContext` instance is passed to `BeforeHandle`, `OnError` and
`AfterHandle` for one delivery, so a `ConditionalWeakTable` keyed on it works
there. There is no state bag on either context.

## What to reach for instead

An interceptor runs around *every* message on the connection. To wrap one handler,
that is a [pipeline](patterns.md#pipelines) — and on the consume side it is the
only thing that can change what the handler sees.

Interceptors are cross-cutting policy; pipelines are one consumer's business.
A timeout in an interceptor would also apply to the queue whose handler is meant to
take four minutes.

## Next

- [Publishing](publishing.md) — what `SendAsync` returns is what the interceptors left
- [Patterns](patterns.md#pipelines) — the per-handler version
- [Metrics and tracing](observability.md) — the numbers you would otherwise write an interceptor for
