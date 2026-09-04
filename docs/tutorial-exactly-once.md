# 3. Never processing twice

**25 minutes. Needs Docker.**

"Exactly once" is a phrase brokers sell and none deliver. What is actually available
is **at least once delivery** plus **effects that happen once**, and this is how to
build the second.

## Watch a duplicate happen

```csharp
var envelope = Envelope.Of("order.placed").Build();
var publisher = mq.Publisher<OrderPlaced>("", "orders.placed");

await publisher.SendAsync(new OrderPlaced("A-1", 42.50m), envelope);
await publisher.SendAsync(new OrderPlaced("A-1", 42.50m), envelope);   // same envelope
```

```
charged A-1
charged A-1
```

The customer was charged twice. That is not a contrived case — it is what a broker
does after a consumer crashes between doing the work and acknowledging, and what an
outbox relay does when it publishes and dies before recording it.

## Remember what you handled

```csharp
var store = InMemoryIdempotencyStore.ForOneDay();

var options = ConsumerOptions.Defaults().Idempotent(store);
using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", options, Handle);
```

```
charged A-1
```

The key is the envelope's id, which is why publishing the same envelope twice is
recognised as one message.

### Three states, not two

`Claim`, `Confirm`, `Release`. A store that only recorded "seen" could not tell a
message that **failed** from one that **succeeded**, and would drop the retry of
something never handled. The claim is taken before the handler runs and released if
it throws.

### The in-memory one is not enough

It is per process. Run two instances of your consumer and a duplicate delivered to
the *other* one is handled twice — it does not fail, it just deduplicates less than
it looks like it does.

```csharp
var store = new DbIdempotencyStore(() => new SqlConnection(connectionString),
                                   TimeSpan.FromDays(1));
```

`store.CreateTableSql()` gives you the table for a migration. The claim is an
**insert**, so the primary key does the mutual exclusion: two consumers racing, one
insert succeeds.

## The other half: publishing exactly once

Your service saves an order and publishes a message about it.

```csharp
await _orders.SaveAsync(order);          // committed
await publisher.SendAsync(orderPlaced);  // process dies here
```

The order exists and nobody was told. Swap them and you announce an order that might
still roll back.

Neither ordering works, because there are two systems and no transaction across them.

## The outbox

Write the message **in the same transaction as the order**:

```csharp
var store = new DbOutboxStore(() => new SqlConnection(connectionString));

using var connection = new SqlConnection(connectionString);
connection.Open();
using var transaction = connection.BeginTransaction();

await _orders.SaveAsync(order, transaction);
await store.AddAsync(
    OutboxRecord.Of("orders", "order.placed", Envelope.Of("order.placed").Build(), json),
    transaction);                        // the same transaction

transaction.Commit();
```

Either both exist or neither does. Then something publishes them:

```csharp
using var relay = mq.Outbox(store);
relay.Start();
```

**Pass the transaction.** The overload without one opens its own connection and is
therefore not in yours, which quietly gives up the whole guarantee.

## Why this is not exactly once

The relay publishes, then records that it did. A crash between the two republishes
the message — the record was never marked.

So delivery is still **at least once**, and it always will be. What you have built is
the other half: the consumer recognises the duplicate and the effect happens once.
That is what people mean when they say exactly once, and it is worth knowing that it
is two mechanisms rather than a broker feature.

## What you have

Duplicates that do nothing, and messages that cannot be lost between your database
and the broker. Next: [seeing what happens](tutorial-observability.md).
