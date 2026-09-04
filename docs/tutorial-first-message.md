# 1. Your first message

**10 minutes. No broker needed.**

By the end you will have published a message and consumed it, and know what each
line was for.

## Connect to nothing

Start with the in-process broker. It routes the way RabbitMQ routes, so what you
learn here holds later:

```csharp
using AceMq.Amqp;

using var mq = await AceMqConnection.ConnectAsync("memory://tutorial");
Console.WriteLine($"connected to {mq.TransportName}");
```

```bash
dotnet run
# connected to in-memory
```

`using` matters: the connection owns publishers, consumers and the transport, and
disposing it stops them.

## Declare where messages go

```csharp
await mq.DeclareExchangeAsync("orders", "topic");
await mq.DeclareQueueAsync("orders.placed");
await mq.BindAsync("orders.placed", "orders", "order.placed");
```

Three things, and they are different things. An **exchange** receives published
messages. A **queue** holds them for a consumer. A **binding** is the rule that
connects them — without it a message reaches the exchange and goes nowhere.

Declaring is idempotent, so this is safe to run at every start-up.

## Publish

```csharp
public sealed record OrderPlaced(string OrderId, decimal Total);
```

```csharp
var publisher = mq.Publisher<OrderPlaced>("orders", "order.placed");
var result = await publisher.SendAsync(new OrderPlaced("A-1", 42.50m));

Console.WriteLine($"published {result.MessageId}, routed {result.Routed}");
```

`SendAsync` returns once **the broker has confirmed the message**, not when the
bytes reach the socket. Those are different events, and the gap between them is
where messages are lost on a broker restart.

## Delete the binding and watch it fail

Comment out the `BindAsync` line and run it again:

```
PublishFailedException: 7d3f… reached the broker but matched no queue bound to
'orders' for 'order.placed'. This is usually a topology mistake...
```

Most brokers drop that message silently. Finding out hours later, from an absence
of messages, is the failure this library exists to avoid. Put the binding back.

## Consume

```csharp
using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", message =>
{
    Console.WriteLine($"got {message.Payload.OrderId} for {message.Payload.Total:0.00}");
    return Task.FromResult(Ack.Accept());
});
```

Start the consumer **before** publishing, then run:

```
published 7d3f…, routed True
got A-1 for 42.50
```

The handler returns a disposition rather than throwing or returning nothing:

| | |
|---|---|
| `Ack.Accept()` | handled; the broker may forget it |
| `Ack.Retry(after, why)` | failed, but another attempt might work |
| `Ack.DeadLetter(why)` | will never work; stop and keep the evidence |
| `Ack.Release()` | give it back for someone else |

Try returning `Ack.Retry(TimeSpan.FromMilliseconds(500), "not yet")` and watch it
arrive again. Then print `message.Attempt` and watch it climb.

## What came with the message

```csharp
Console.WriteLine(message.Envelope.Id);
Console.WriteLine(message.Envelope.CorrelationId);
Console.WriteLine(message.Attempt);
```

Every message carries an [envelope](envelope.md) — identity, correlation,
causation, first seen. You did not set any of it; the defaults are contract, and a
Java consumer reads exactly the same fields.

## Point it at a real broker

```bash
docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Two changes:

```csharp
using AceMq.Amqp.RabbitMq;

Transports.Register(new RabbitMqTransport());
using var mq = await AceMqConnection.ConnectAsync("amqp://guest:guest@localhost:5672");
```

Nothing else changes. Run it, then open <http://localhost:15672> (guest/guest) and
watch the queue.

Registering the transport is a line you write. The Java library discovers transports
automatically, and its usual first failure is *no transport for scheme amqp* from a
runtime dependency left off the classpath. Here it is a missing reference the
compiler tells you about.

## What you have

A publisher, a consumer, a topology, and a message that confirmed. Next:
[surviving failure](tutorial-surviving-failure.md) — what happens when the handler
throws.
