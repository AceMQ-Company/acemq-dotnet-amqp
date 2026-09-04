# Exchanges, queues and bindings

Declaring topology is idempotent: declaring something that already exists with the
same settings succeeds and changes nothing.

```csharp
await mq.DeclareExchangeAsync("orders", "topic");
await mq.DeclareQueueAsync("orders.placed");
await mq.BindAsync("orders.placed", "orders", "order.placed");
```

## Declaring it as one thing

Three calls are fine for one queue. For a service, describe the whole topology and
apply it:

```csharp
var topology = Topology.Define()
    .Exchange("orders", "topic")
    .QueueWithDeadLetter("orders.placed")
    .Bind("orders.placed", "orders", "order.placed")
    .Build();

await mq.ApplyAsync(topology);
```

`QueueWithDeadLetter` is the reason this exists. It declares four things that are
only correct together — the queue, its dead-letter exchange, the queue bound to that
exchange, and the `x-dead-letter-exchange` argument pointing at it:

```
orders.placed        with x-dead-letter-exchange = orders.placed.dlx
orders.placed.dlx    fanout
orders.placed.dead   bound to orders.placed.dlx
```

Wired by hand, forgetting any one of them means `Ack.DeadLetter` discards the
message and nothing reports it — the queue nacks without requeueing, the broker finds
no dead-letter exchange, and the message is gone. That is the failure this library
exists to prevent, so the four declarations are one call.

## Seeing what it would do

```csharp
var plan = await mq.ApplyAsync(topology, ApplyMode.DryRun);
Console.WriteLine(plan.Render());
```

```
? exchange orders (topic)
  queue orders.placed (classic)
+ queue orders.placed.dead (classic)
! queue orders.parked (classic) -- PRECONDITION_FAILED - inequivalent arg 'x-message-ttl'
? bind orders.placed.dead to orders.placed.dlx on ''
```

`+` would be created, a blank is already there and matches, `!` exists but differs,
`?` cannot be determined.

```csharp
plan.HasChanges   // something would be created
plan.HasDrift     // something exists but differs
plan.Drift        // what
```

### How drift is found

AMQP has no way to read a queue's arguments back. The only thing that answers the
question is a declaration: the broker replies `406 PRECONDITION_FAILED` when the
queue exists with different settings. That is run on a **throwaway channel**, because
a failed declaration closes the channel it ran on — asking the question must not take
publishing down as a side effect.

Exchanges and bindings stay `?`. Nothing can ask whether they exist, and reporting
them as "would create" would be a guess. Plausible-looking output that is sometimes
wrong stops being read.

### Applying over drift is refused

```
AceFatalException: queue orders.parked already exists with different settings:
PRECONDITION_FAILED - inequivalent arg 'x-message-ttl'. A queue's type and arguments
are fixed at creation, so this needs the queue drained and redeclared rather than
changed in place.
```

Drift is reported, never corrected. "Fixing" it would mean deleting a queue that has
messages in it, which is not something a library should do because a declaration did
not match.

## Exchange types

| Type | Routes to |
|---|---|
| `direct` | queues bound with exactly this routing key |
| `topic` | queues whose pattern matches the routing key |
| `fanout` | every bound queue, ignoring the routing key |
| `headers` | matched on headers rather than the routing key |

Publishing to the **default exchange** — the empty string — addresses a queue by
name, which is what most first examples do:

```csharp
var publisher = mq.Publisher<Job>("", "work");
```

## Topic patterns

`*` matches exactly one word. `#` matches zero or more.

| Pattern | `order.placed` | `order.placed.eu` | `order` |
|---|---|---|---|
| `order.placed` | yes | no | no |
| `order.*` | yes | no | no |
| `order.#` | yes | yes | **yes** |
| `#.eu` | no | yes | no |

`order.#` matching bare `order` is the case worth remembering: `#` matches *zero*
words, so it absorbs the separator before it as well. The in-memory transport
implements this rule exactly, so a binding that matches in a test matches on a real
broker — a fake router that is more permissive than the real one is worse than no
test, because it passes and then the deployment does not.

## Queue types

```csharp
await mq.DeclareQueueAsync("orders.placed", QueueType.Quorum, null);
```

| | |
|---|---|
| `Classic` | the default |
| `Quorum` | replicated, and the right choice when losing messages is not acceptable |
| `Stream` | append-only, re-readable from an offset |

**A queue's type is fixed when it is created.** Declaring an existing queue with a
different type fails rather than converting it. That is the broker protecting the
messages already in it, not an error to work around — moving a queue between types
means draining it and declaring a new one.

## Arguments

```csharp
await mq.DeclareQueueAsync("orders.placed", QueueType.Classic,
    new Dictionary<string, object>
    {
        ["x-dead-letter-exchange"] = "orders.dlx",
        ["x-message-ttl"] = 86_400_000,
        ["x-max-length"] = 100_000,
    });
```

Arguments are also fixed at declaration. Changing one on a queue that already exists
fails; the queue has to be drained and redeclared.

## Where to declare it

Declaring topology at start-up, in the service that owns the queue, keeps the
declaration next to the code that depends on it. What that does not survive is two
services declaring the same queue with different arguments — the second one to start
fails, and it fails on a detail nobody changed deliberately.

For anything shared, declare it in one place and have the others assume it exists.

```csharp
if (!await mq.QueueExistsAsync("orders.placed"))
{
    throw new InvalidOperationException(
        "orders.placed has not been declared; the orders service owns it");
}
```
