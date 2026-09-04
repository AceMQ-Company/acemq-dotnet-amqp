# Getting started

> **Pre-1.0, and not published yet.** The library publishes and consumes over a real
> broker today, and the tests pass. What it does not have is a release: there is no
> package on any feed yet.

## Install

When the first package is published it will come from a **static NuGet feed served
over GitHub Pages**, the same arrangement the JVM libraries use for Maven —
anonymous, no credentials, no account:

```bash
dotnet nuget add source https://acemq.org/nuget/index.json --name acemq
dotnet add package AceMq.Amqp
dotnet add package AceMq.Amqp.RabbitMq
```

Both are needed to reach a real broker: the first is the library, the second is the
RabbitMQ transport.

GitHub Packages was considered and rejected for the public feed: it requires
authentication even for public packages, which would mean every consumer needs a
token just to restore. The Maven repository promises "no credentials needed" and the
.NET feed keeps the same promise. nuget.org is for 1.0, when the coordinates and the
API have stopped moving — publishing there is permanent.

## Send and receive a message

```csharp
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

Transports.Register(new RabbitMqTransport());

using var mq = await AceMqConnection.ConnectAsync("amqp://localhost");

await mq.DeclareExchangeAsync("orders", "topic");
await mq.DeclareQueueAsync("orders.placed");
await mq.BindAsync("orders.placed", "orders", "order.placed");

using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", message =>
{
    Console.WriteLine($"{message.Payload.OrderId} for {message.Payload.Total:0.00}");
    return Task.FromResult(Ack.Accept());
});

var publisher = mq.Publisher<OrderPlaced>("orders", "order.placed");
var result = await publisher.SendAsync(new OrderPlaced("A-1", 42.50m));

Console.WriteLine($"published {result.MessageId}, routed {result.Routed}");

public sealed record OrderPlaced(string OrderId, decimal Total);
```

The same program [in VB.NET](vbnet.md) is the same assembly and the same API.

## Without a broker

Use a `memory://` URL and nothing needs to be installed or running. The in-process
broker routes the way RabbitMQ routes, so bindings behave as they will in
production:

```csharp
using var mq = await AceMqConnection.ConnectAsync("memory://demo");
```

No transport registration is needed for that one — it is built in. See
[testing](testing.md) for what the in-memory broker does and, more importantly, what
it does not.

## Registering the transport

`Transports.Register(new RabbitMqTransport())` is a line you write once, at
start-up. The Java library discovers its transports through the service loader
instead, and the usual first failure there is *no transport for scheme amqp*, from a
runtime-only dependency left off the classpath. Here a missing transport is a
missing reference the compiler tells you about.

## Where to next

- [Publishing](publishing.md) — confirms, unroutable messages, options
- [Consuming](consuming.md) — dispositions, retries, dead-lettering
- [Exchanges, queues and bindings](topology.md) — routing and queue types
- [Testing](testing.md) — the in-memory broker, and its limits
- [The envelope](envelope.md) — what travels with every message
- [C#](csharp.md) and [VB.NET](vbnet.md) — the same library from either language
