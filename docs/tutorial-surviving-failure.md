# 2. Surviving failure

**20 minutes. Needs Docker.**

Everything so far assumed the handler works. This is about when it does not.

```bash
docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

(For TLS instead of plaintext, `acemq-certs` generates everything a broker needs —
see [security](security.md#certificates-for-development).)

## Declare somewhere for failures to go

```csharp
Transports.Register(new RabbitMqTransport());
using var mq = await AceMqConnection.ConnectAsync("amqp://guest:guest@localhost:5672");

await mq.ApplyAsync(Topology.Define().QueueWithDeadLetter("orders.placed").Build());
```

That one call declares four things: the queue, a dead-letter exchange, a queue bound
to it, and the `x-dead-letter-exchange` argument pointing at it.

They are only correct together. Wire them by hand, forget one, and `Ack.DeadLetter`
**silently discards the message** — the broker nacks it, finds no dead-letter
exchange, and drops it. Nothing reports that.

Look at <http://localhost:15672/#/queues>: `orders.placed` and `orders.placed.dead`.

## Fail on purpose

```csharp
using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", message =>
{
    Console.WriteLine($"attempt {message.Attempt} for {message.Payload.OrderId}");
    throw new InvalidOperationException("the pricing service is down");
});

await mq.Publisher<OrderPlaced>("", "orders.placed").SendAsync(new OrderPlaced("A-1", 42.50m));
await Task.Delay(30_000);
```

```
attempt 1 for A-1
attempt 2 for A-1
attempt 3 for A-1
...
```

Forever. An exception is treated as a retry, and with no policy there is nothing to
stop it. That consumer is now occupied by one message that will never succeed.

## Bound it

```csharp
var options = ConsumerOptions.Defaults()
    .WithRetry(RetryPolicy.Exponential(3, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10)));

using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", options, message => ...);
```

```
attempt 1 for A-1
attempt 2 for A-1
attempt 3 for A-1
```

Then it stops, and the message is in `orders.placed.dead` with the reason on it.
Check the management UI: one message.

**Jitter is on by default.** The delays are not exactly 1s and 2s — they are spread
±20%. Without that, every consumer that failed at the same moment retries at the same
moment, and a dependency that was struggling gets the whole herd at once.

## Say what you mean instead of throwing

An exception cannot distinguish "try again shortly" from "this will never work".
Returning the disposition can:

```csharp
return await _pricing.QuoteAsync(order) switch
{
    QuoteOutcome.Ok          => Ack.Accept(),
    QuoteOutcome.NoSuchItem  => Ack.DeadLetter("no such item"),   // never works
    QuoteOutcome.Unreachable => Ack.Retry(TimeSpan.FromSeconds(30), "pricing down"),
    _ => Ack.Release(),
};
```

Retrying the second case forever is how one bad message becomes an outage.

## Fix the cause, replay the messages

The pricing service is back. The messages are still in the dead-letter queue:

```csharp
var replay = mq.Replay("orders.placed.dead");
Console.WriteLine($"{await replay.PendingAsync()} waiting");
Console.WriteLine($"replayed {await replay.ReplayAllAsync()}");
```

They go back to `orders.placed` — the queue the dead-letter queue is named after —
carrying `x-acemq-replay-count`, with the old failure reason cleared.

Replay only some of them:

```csharp
await replay.ReplayAsync(100, d =>
    d.Headers.TryGetValue("x-tenant", out var t) && (string)t == "acme");
```

What the filter rejects goes **back**, not away. Losing the rest as a side effect of
looking at them would be a poor trade.

## Shut down without losing work

Stop the process mid-handler and the message comes back — but any side effect already
applied has happened twice by the time it does.

```csharp
Console.CancelKeyPress += async (_, e) =>
{
    e.Cancel = true;
    Console.WriteLine("draining...");
    var drained = await mq.DrainConsumersAsync(TimeSpan.FromSeconds(30));
    Console.WriteLine(drained ? "finished cleanly" : "gave up waiting");
    mq.Dispose();
};
```

Drain stops new messages being handed over and waits for the ones in progress. It
returns `false` rather than throwing when it runs out of time — shutting down anyway
is a legitimate choice, and you need to know which one you are making.

## What you have

Bounded retries, a dead-letter queue that exists, a way to put messages back, and a
clean shutdown. Next: [never processing twice](tutorial-exactly-once.md).
