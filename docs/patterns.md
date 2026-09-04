# Patterns

Five things most services building on a broker end up writing themselves.

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

## Replay

Dead-lettering keeps the messages. This puts them back once whatever broke is fixed:

```csharp
var replay = mq.Replay("orders.placed.dead");

await replay.PendingAsync();     // how many are waiting
await replay.ReplayAllAsync();   // all of them
await replay.ReplayAsync(100);   // the first hundred
```

By default they go back to the queue the dead-letter queue is named after, so
`orders.placed.dead` replays into `orders.placed`. `Into("somewhere.else")` overrides
that.

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
