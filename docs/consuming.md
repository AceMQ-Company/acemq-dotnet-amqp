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

Five things can happen to a message, and the handler says which:

| | |
|---|---|
| `Ack.Accept()` | Handled. The broker may forget it. |
| `Ack.Retry(reason)` | Failed in a way another attempt might survive. The policy decides when. |
| `Ack.DeadLetter(reason)` | Will never succeed. Stop now and keep the evidence, in `{queue}.dlq`. |
| `Ack.Park(reason)` | A person has to look at this one. Goes to `{queue}.parked`. |
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

**Both are reported as `rejected`, not `dead_lettered`.** The message goes to
`{queue}.dlq` either way; the word says who decided. `dead_lettered` is kept for the
engine giving up — a `RetryPolicy` running out of attempts — because a decision
somebody took about this message and a dependency that stayed down for six attempts
want different people looking at them. See
[Observability](observability.md#what-it-records) for what that means on a dashboard.

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

The number is read off the wire, from `x-acemq-attempt`, not counted in this process. A
retry republishes the message with it advanced, so it means the same thing to every
consumer on the queue, survives a restart, and does not start again at one when a
message moves between instances.

## A message that cannot be decoded is not retried

If the body does not parse as the handler's type, the message is sent to
`{queue}.parked` with the reason attached, and the handler is never called. It would
not parse on the next attempt either, and a poison message on an infinite retry loop
looks exactly like throughput until someone reads the queue depth.

Parked rather than dead-lettered, because they are different problems: a message that
failed five times wants the person who owns the dependency, and a message nothing could
read wants the person who owns the publisher. Whoever drains the dead letters should not
have to sort them by hand.

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
    .RequeueingOnFailure()                      // hand it straight back to the broker
```

`RequeueingOnFailure` is off by default, and should stay off unless a queue's failures
really do mean "somebody else should take this". Requeueing a message that fails
deterministically produces a hot loop that shows up on a dashboard as work being done —
and it hands the broker back the bytes it gave out, so `x-acemq-attempt` never advances
and the retry policy never reaches its limit.

## Where a dead letter goes

`Ack.DeadLetter` **republishes** the message to `{queue}.dlq` with the reason on
`x-acemq-error`, and only then acknowledges the original. Acknowledging a failure looks
wrong and is what makes it reliable: by the time the acknowledgement happens the message
is already somewhere else.

Rejecting it instead — `basic.nack` with no requeue — would hand it to whatever
dead-lettering the queue happens to be declared with, and **if the queue has none the
broker discards it**. Nor can a broker write onto a message it is rejecting, so the one
thing whoever finds it actually needs — what the handler was unable to do — would not be
there.

Nothing has to be declared for this to work. **A consumer declares `acemq.dlx`,
`{queue}.dlq` and `{queue}.parked` when it starts**, before it subscribes and before
anything has failed — with or without a retry policy, because giving up is not something
a retry policy switches on. They are there in the management UI from the moment the
service connects, which is what makes them something an operator can watch rather than
something that appears on the day of the first incident.

The same declarations, argument for argument, are what a topology makes, so it is safe
to do both and in either order:

```csharp
await mq.ApplyAsync(
    Topology.Define()
        .QueueWithRetry("orders.placed", RetryPolicy.Exponential(6, TimeSpan.FromSeconds(10)))
        .Build());
```

Declaring the same queue twice with the *same* arguments is how AMQP is meant to be
used; declaring it twice with different ones is a `PRECONDITION_FAILED` that stops the
second consumer starting at all. That is why the consumer's declaration and
`QueueWithRetry`'s come from one place: both queues classic, durable, and with no
arguments of their own — a dead-letter queue that dead-letters is a loop.

These were declared on the settle path until ADR-032 — the first time a message was
actually dead-lettered or parked. The end state was the same; the window before it was
not, and Java's `RetryTopology.declare` had been declaring them at start-up all along,
which is what the shared contract fixture records.

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
