# Patterns

Seven things most services building on a broker end up writing themselves.

## Ordering by key

Order in AMQP survives only while one consumer reads one queue and handles one
message at a time. Two consumers, or one consumer handling two messages at once, and
the order the broker sent them in stops being the order they are applied in.

So parallelism comes from splitting keys across queues, not from adding consumers to
one:

```csharp
var ledger = await mq.Ordered<LedgerEntry>("ledger")
    .Partitions(8)
    .KeyedBy(entry => entry.AccountId)
    .DeclareAsync();

await ledger.ConsumeAsync(async message => await _ledger.ApplyAsync(message.Payload));

await ledger.SendAsync(new LedgerEntry("acct-7", 100m));
```

Every entry for `acct-7` lands on the same queue and is handled in order. Different
accounts run in parallel. Throughput scales with partitions; order holds within a
key.

**Choosing the key is choosing what ordering means.** An account id orders one
account's operations against each other and nothing else. A constant orders
everything and gives up all parallelism.

### What happens when one fails

This is the part worth reading. If a message fails and the next one is handled
anyway, order breaks exactly where it matters — the operation that should have come
second has been applied while the first is still failing.

So the default is `PartitionFailure.Stop`:

```csharp
.OnFailure(PartitionFailure.Stop, attempts: 3, delay: TimeSpan.FromSeconds(1))
```

After three attempts the partition **halts**. The failed message is held,
unacknowledged, and nothing behind it is delivered until you resume:

```csharp
ledger.HaltedPartitions   // which ones stopped
ledger.Resume(partition); // try it again from the first attempt
```

A halted partition is visible and recoverable. Silently carrying on is neither.

| | |
|---|---|
| `Stop` | halt the partition — order preserved, throughput lost |
| `RetryInPlace` | keep retrying, holding everything behind it |
| `Skip` | dead-letter it and continue — throughput preserved, order broken |

Retries are made **in the handler**, not by returning `Ack.Retry`. A returned retry
puts the message back at the *back* of the queue, behind everything waiting — which
would break ordering while appearing to preserve it.

## Pipelines

A chain of steps, each on its own queue:

```csharp
using var pipeline = await mq.Pipeline<Order>("orders")
    .Step("validate", async (Order o) => await _validator.CheckAsync(o))
    .Step("enrich",   async (Order o) => await _customers.EnrichAsync(o))
    .Step("store",    async (Order o) => await _orders.SaveAsync(o))
    .BuildAsync();

await pipeline.SendAsync(order);
```

Every step is a queue, which is what separates this from calling three methods in a
row: a step that fails retries on its own without re-running the ones before it, a
slow step builds a visible backlog instead of blocking its predecessors, and each
step scales independently.

The steps are type-checked against each other at compile time — a step added after
one producing `Order` can only accept an `Order`. A mismatch is a compile error
rather than a decode failure at the third step in production.

**A step returning `null` ends the message there.** That is how a filter is
expressed, and it is counted apart from both success and failure:

```csharp
pipeline.Entered      // went in
pipeline.Completed    // came out the end
pipeline.EndedEarly   // a step filtered it out
pipeline.InFlight     // somewhere in between
```

Rejection is a normal outcome, so it does not throw, and it does not look like a lost
message.

## The outbox

The problem: a service changes its database and publishes a message about it. Publish
inside the transaction and you announce a change that might still roll back. Publish
after committing and you lose the message if the process dies in between.

The outbox writes the message down **in the same transaction as the business change**,
and something publishes it afterwards:

```csharp
// in your transaction, with your data
await store.AddAsync(OutboxRecord.Of(
    "orders", "order.placed", Envelope.Of("order.placed").Build(), json));

// somewhere in the application
using var relay = mq.Outbox(store);
relay.Start();
```

Either both the row and the record exist, or neither does.

`DbOutboxStore` is the one that makes this work, because the record has to be in the
same database as the business change:

```csharp
var store = new DbOutboxStore(() => new SqlConnection(connectionString));

using var transaction = connection.BeginTransaction();
await orders.SaveAsync(order, transaction);
await store.AddAsync(record, transaction);   // the same transaction
transaction.Commit();
```

**Pass the transaction.** The overload without one opens its own connection and is
therefore not in yours, which quietly gives up the entire guarantee.
`store.CreateTableSql()` gives you the schema for a migration.

`InMemoryOutboxStore` ships for tests and to show the shape — it is **not an outbox**,
because it cannot be written in the same transaction as anything durable and loses
everything on a restart, which is the exact failure the pattern prevents.

Delivery is **at least once**. A relay that publishes and then dies before marking
the record will publish again, so consumers must tolerate duplicates — the envelope's
id is the idempotency key.

## Interceptors

For the concerns that belong to every message rather than to one call site: a tenant
header, an audit trail, a policy check on what goes out.

```csharp
mq.Intercept(new TenantStamp(tenant));      // publishes
mq.Intercept(new AuditTrail(audit));        // handled messages
```

Inherit `PublishInterceptor` or `ConsumeInterceptor` and override what you care
about — C# interfaces cannot carry default implementations on `netstandard2.0`, and
VB cannot use them at all, so the base classes are what save you writing two empty
methods.

```csharp
sealed class TenantStamp : PublishInterceptor
{
    public override PublishContext BeforePublish(PublishContext context) =>
        context.WithEnvelope(/* the envelope with a header added */);
}
```

**Only the envelope can be changed.** Not the payload, not the destination. An
interceptor that could rewrite either would be able to send a message somewhere the
caller never asked for — a surprising amount of power for something usually added to
attach a header.

Interceptors are taken when a publisher is created, so register them at start-up.
One added later does not apply to publishers that already exist, which is deliberate:
otherwise two identical publishers would behave differently for reasons nothing in
the code shows.

**An interceptor that throws after a confirm does not fail the publish.** The broker
already has the message; reporting a failure would have the caller send it twice.
`BeforePublish` is different — it runs before anything is sent, so a throw there does
stop the publish, which is what makes a policy check possible.

`Order` decides the sequence, lowest first.

## Routing slips

A pipeline decides its route once and every message takes it. A routing slip is the
other arrangement: **the route travels with the message**, so it can differ per
message and be changed by whatever handled the last step.

```csharp
await mq.SendAlongAsync(RoutingSlip.StartOf("validate", "price", "ship"), order);
```

Each step forwards it:

```csharp
await mq.ConsumeAsync<Order>("validate", async message =>
{
    await _validator.CheckAsync(message.Payload);

    var slip = RoutingSlip.Of(message)!.Advance();
    if (!slip.IsFinished)
    {
        await mq.ForwardAsync(slip, message.Payload, message.Envelope);
    }
    return Ack.Accept();
});
```

A step can change what happens next — skip ahead when a check is unnecessary, or go
back to an earlier step:

```csharp
var slip = RoutingSlip.Of(message)!;
var onward = order.Total < 100 ? slip.AdvanceTo(2) : slip.Advance();   // skip fraud
```

Going backwards repeats work, so the steps it revisits have to tolerate that.

The slip rides in reserved headers, so it does **not** appear in `message.Headers` —
a handler sees its own headers and asks for the slip explicitly with
`RoutingSlip.Of(message)`.

**A slip is not a transaction.** Each step commits as it finishes, so a failure at
step four does not undo steps one to three. If that matters, the route needs
compensating steps of its own.

Use a pipeline where every message takes the same path: it says so more clearly and
does not pay to carry the route around.

## Replay

Dead-lettering keeps the messages. This puts them back once whatever broke is fixed:

```csharp
var replay = mq.Replay("orders.placed.dlq");

await replay.PendingAsync();     // how many are waiting
await replay.ReplayAllAsync();   // all of them
await replay.ReplayAsync(100);   // the first hundred
```

By default they go back to the queue the dead-letter queue is named after, so
`orders.placed.dlq` replays into `orders.placed`. `.parked` and `.dead` are recognised
the same way, and `Into("somewhere.else")` overrides it.

Selective replay takes a filter:

```csharp
await replay.ReplayAsync(1000, delivery =>
    delivery.Headers.TryGetValue("x-tenant", out var t) && (string)t == "acme");
```

**What the filter rejects is put back, not discarded.** Selective replay is normally
about picking out one tenant or one kind of failure, and losing the rest as a side
effect of looking at them would be a poor trade.

Replayed messages carry `x-acemq-replayed-from`, `x-acemq-replayed-at` and
`x-acemq-replay-count`, and the failure reason is cleared — it belonged to the
attempt that failed, and leaving it on would make every replayed message look like it
had already failed again.

**`x-acemq-attempt` is reset to 1 as well.** A dead-letter queue is full of messages that
were given up on at the *last* attempt of their policy, and the attempt counter now
travels with the message — so without the reset each one would arrive back on attempt
five of five and be dead-lettered again before a handler saw it. The operator who has
just fixed the bug would have moved two thousand messages from one queue to the same
queue. `KeepingAttempts()` puts back exactly what was there, for an audit or for a queue
read by something that counts attempts itself.

## Sagas

A sequence of steps where each one knows how to undo itself. If a later step fails,
the earlier ones are undone in reverse:

```csharp
var booking = Saga<Order>.Named("place-order")
    .Step("take-payment", order => _payments.ChargeAsync(order))
        .CompensateWith(order => _payments.RefundAsync(order))
    .Step("reserve-stock", order => _inventory.ReserveAsync(order))
        .CompensateWith(order => _inventory.ReleaseAsync(order))
    .Step("book-courier", order => _couriers.BookAsync(order))
    .Build();

var result = await booking.RunAsync(order);
```

If `book-courier` throws, the stock is released and *then* the payment refunded —
reverse order, because that is the order the world was changed in and a compensation
often depends on state a later step has not yet altered.

**A step with no compensation is legitimate**, not an oversight the library will warn
about. `book-courier` above has none because nothing after it can fail; a step that
only read something needs no undo either. A library cannot tell that case apart from a
forgotten one, which is the argument for writing the compensation first and the action
second.

**Nothing is thrown.** A failed saga is not an exceptional condition to a caller that
has to decide what happens next, and the interesting part is not the exception:

```csharp
if (result.HasUnresolved)
{
    // These are the ones to alert on.
    _alerts.Raise($"{result.Saga} left {string.Join(", ", result.Unresolved)} undone");
}
```

`Unresolved` is the list of steps whose *compensation* failed. **When a compensation
throws, it is reported and the remaining ones still run** — stopping there would leave
more undone than continuing does. Everything else a saga reports is recoverable by
construction; these are real-world effects that happened, were meant to be undone, and
were not. Nothing else in the system knows about them and no retry will resolve them.

Both events go to `AceMqDiagnostics` — `acemq.saga.compensating` as a warning,
`acemq.saga.unresolved` as an error.

The token passed to `RunAsync(subject, cancellationToken)` cancels the forward path
only. A step that observes it and throws is a failed step like any other and the
earlier ones are undone; the compensations then run uncancelled, because a cancelled
saga is precisely the one that most needs undoing.

**This is not a distributed transaction.** Nothing is isolated: after `take-payment`
the customer's money really has moved and anybody looking sees that it has. The refund
is a *new* fact rather than an erasure of the old one. So the steps have to be things
that can be undone by doing something else — sending an email cannot be compensated,
and a step that sends one belongs last, after everything that can still fail.

**It is not durable either.** This runs in one process with its state on the stack, so
a crash midway leaves the saga half-applied with nothing to resume it. Where a saga
must survive the process, the steps have to be messages and the state has to be in a
database — a much larger thing, and it is not this.

## Scheduling

Delivering a message later:

```csharp
using var scheduler = await Scheduler.OnAsync(mq);

await scheduler.InAsync(TimeSpan.FromHours(4), "billing", "invoice.due", invoice);
await scheduler.AtAsync(renewalDate, "policies", "policy.renew", policy);
```

### Why not a per-message time to live

The obvious implementation is to set `expiration` on the message, drop it in a queue
nobody consumes and let it dead-letter to its destination. It is what most articles
suggest and it is wrong for anything but a single fixed delay, because **a classic
queue expires messages only at its head.**

Put a four-hour message in, then a one-minute message behind it, and the one-minute
message is delivered in four hours. Nothing reports this: the queue looks healthy, the
message is not lost, it is simply late by a factor nobody predicted — and it fails in
production under mixed load rather than in testing under uniform load.

### The ladder

A small set of queues, each with a *uniform* time to live, and a message hops through
them until it is due:

| Queue | `x-message-ttl` | dead-letters to |
|---|---|---|
| `acemq.schedule.1h` | 3600000 | `acemq.schedule` / `acemq.schedule.due` |
| `acemq.schedule.10m` | 600000 | `acemq.schedule` / `acemq.schedule.due` |
| `acemq.schedule.1m` | 60000 | `acemq.schedule` / `acemq.schedule.due` |
| `acemq.schedule.10s` | 10000 | `acemq.schedule` / `acemq.schedule.due` |
| `acemq.schedule.1s` | 1000 | `acemq.schedule` / `acemq.schedule.due` |

Every message in a given rung has the same delay, so head-of-line expiry is not a
problem — the head is always the message due soonest. Each expiry returns the message
to `acemq.schedule.due`, where the scheduler either delivers it or puts it in the
largest rung that does not overshoot. A four-hour delay is four one-hour hops; a
ninety-second delay is one minute, then three tens. A one-day message takes
twenty-four hops and a one-minute message takes one, which is the right way round:
short delays are common and want to be cheap.

`Scheduled`, `Delivered` and `Hops` are on the scheduler. `Hops` divided by
`Delivered` is the average hop count, which is the number to look at when the
scheduler is busier than expected.

The cost is worth stating plainly: a long delay is several broker round trips rather
than one, and **delivery is accurate to about the smallest rung in either
direction** — the last remainder under a second is delivered rather than waited out,
because another hop would cost more than the accuracy it buys. A scheduler that must
fire at 09:00:00.000 exactly is a scheduler, not a message broker.

The alternative is RabbitMQ's delayed-message-exchange plugin, which does this
properly and is a plugin — so it is not available everywhere, and a library that
silently required it would be a library that works on your laptop.

### The wire contract

The queue names, the three arguments on each rung and the four headers are shared with
the Java library, and a .NET service and a Java service scheduling through one broker
declare exactly the same topology. A difference in one argument is `PRECONDITION_FAILED`
on whichever starts second.

| Header | |
|---|---|
| `x-schedule-exchange` | where it should eventually go |
| `x-schedule-routing-key` | the routing key it should eventually carry |
| `x-schedule-due-at` | when it is due, as epoch milliseconds |
| `x-schedule-content-type` | what the payload was encoded as |

**These deliberately do not use the `x-acemq-` prefix.** That one is reserved: the
envelope drops every header carrying it from the application's view on the way in, so
a scheduler header using it would be written on publish and gone on consume.

The content type is carried because the scheduler republishes *bytes* rather than
objects, and a consumer picks its codec from the content type. Publishing pre-encoded
bytes under `application/octet-stream` produces a message the intended consumer cannot
decode — it arrives, it is the right bytes, and nothing can read it. The scheduler's
own headers are not passed on to the destination: they are bookkeeping, and a consumer
depending on them would be depending on how a message got to it.

**The control consumer declares no dead-letter queues.** Every other consumer declares
`{queue}.dlq` and `{queue}.parked` when it starts; the control queue's name is fixed
and shared, so doing that here would put two durable queues nothing publishes to on
the broker of every service that ever constructed a scheduler. It consumes as a
private queue instead, and earns that by never giving up — it reads raw bytes, which
cannot fail to decode, and accepts on every path. A message that reaches
`acemq.schedule.due` without the headers a scheduled message carries is dropped and
reported as `acemq.schedule.foreign.message`, because nothing else should be
publishing into these queues at all.

`Dispose` stops the consumer and leaves the queues: they are shared, and may be
holding somebody else's messages.
