# Consuming

A handler receives a decoded message and returns what should happen to it.

```csharp
using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", async message =>
{
    await _orders.RecordAsync(message.Payload);
    return Ack.Accept();
});
```

Disposing the consumer stops delivery.

## The disposition is a return value, not an exception

Four things can happen to a message, and the handler says which:

| | |
|---|---|
| `Ack.Accept()` | Handled. The broker may forget it. |
| `Ack.Retry(after, reason)` | Failed in a way another attempt might survive. |
| `Ack.DeadLetter(reason)` | Will never succeed. Stop now and keep the evidence. |
| `Ack.Release()` | Give it back for someone else, without counting an attempt. |

An exception escaping the handler is still handled — it becomes a retry — but
returning the disposition says what was *meant*. An exception cannot distinguish
"the payment service is down, try shortly" from "this order references a customer
that does not exist and never will", and those need opposite treatment. Retrying the
second forever is how one bad message becomes an outage.

```csharp
return await _payments.ChargeAsync(order) switch
{
    ChargeOutcome.Ok        => Ack.Accept(),
    ChargeOutcome.Declined  => Ack.DeadLetter("card declined"),
    ChargeOutcome.Unreachable => Ack.Retry(TimeSpan.FromSeconds(30), "payments unreachable"),
    _ => Ack.Release(),
};
```

Throwing `AceFatalException` from a handler dead-letters the message, which is the
shorthand for the same decision when you are deep in a call stack.

## What arrives with the message

```csharp
message.Payload        // decoded
message.Envelope       // identity, correlation, causation, attempt
message.Headers        // application headers, without the reserved namespace
message.Attempt        // 1 on the first delivery
message.IsFirstAttempt
message.RoutingKey
message.Queue
message.ReceivedAt
```

`Attempt` is the counter to branch on when a message keeps coming back:

```csharp
if (message.Attempt >= 5) return Ack.DeadLetter("five attempts, giving up");
```

## A message that cannot be decoded is not retried

If the body does not parse as the handler's type, the message is dead-lettered
immediately with the reason attached, and the handler is never called. It would not
parse on the next attempt either, and a poison message on an infinite retry loop
looks exactly like throughput until someone reads the queue depth.

## Prefetch

```csharp
await mq.ConsumeAsync<OrderPlaced>(
    "orders.placed", ConsumerOptions.Prefetch(50), Handle);
```

Twenty by default. This is how many unacknowledged messages the broker will hand
this consumer at once, and it is deliberately modest: an unbounded prefetch gives one
consumer the whole queue, which turns a rolling deploy into a stall while a single
instance works through everything it was handed.

## Options

```csharp
ConsumerOptions.Defaults()
    .WithPrefetch(50)
    .WithRetryDelay(TimeSpan.FromSeconds(10))   // when a handler throws
    .As(new BytesCodec())                       // decode differently from the connection
    .RequeueingOnFailure()                      // return to the queue instead of dead-lettering
```

`RequeueingOnFailure` is off by default. Requeueing a message that fails
deterministically produces a hot loop that shows up on a dashboard as work being
done.

## Dead-lettering needs somewhere to go

`Ack.DeadLetter` nacks the message without requeueing it. On RabbitMQ that sends it
to the queue's configured dead-letter exchange — **and if the queue has none, the
broker discards it**. The disposition is not enough on its own; the topology has to
be declared:

```csharp
await mq.DeclareExchangeAsync("orders.dlx", "fanout");
await mq.DeclareQueueAsync("orders.dead", QueueType.Classic, null);
await mq.BindAsync("orders.dead", "orders.dlx", "");

await mq.DeclareQueueAsync("orders.placed", QueueType.Classic,
    new Dictionary<string, object> { ["x-dead-letter-exchange"] = "orders.dlx" });
```

## Retries, duplicates and shutdown

`ConsumerOptions` also takes a retry policy and an idempotency store:

```csharp
ConsumerOptions.Defaults()
    .WithRetry(RetryPolicy.Exponential(5, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)))
    .Idempotent(InMemoryIdempotencyStore.ForOneDay())
```

Without a policy a failing handler is retried forever; without a store a redelivery
is handled twice. [Reliability](reliability.md) covers both, and how to drain
consumers before shutting down.

## Concurrency

The handler may be invoked concurrently up to the prefetch, so it must be safe to
call from more than one thread. If your handler mutates shared state, it needs its
own locking — the library does not serialise you, because doing so would quietly cap
your throughput at one message at a time.
