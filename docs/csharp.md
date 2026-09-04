# C#

The primary language for this library. Everything on this page compiles against
`AceMq.Amqp` today.

## Connecting

```csharp
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

Transports.Register(new RabbitMqTransport());
using var mq = await AceMqConnection.ConnectAsync("amqp://localhost");
```

The type is `AceMqConnection`, where the Java library calls it `AceMq`. It cannot be
called that here: the namespace is `AceMq.Amqp`, and a type named `AceMq` inside it
makes every reference to the namespace ambiguous. The name differs because the CLR
requires it to, not because the concept did.

One instance per application. It owns the connection, so creating one per message
turns a cheap publish into a TCP handshake.

## Publishing and consuming

```csharp
using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", async message =>
{
    await _orders.RecordAsync(message.Payload);
    return Ack.Accept();
});

var publisher = mq.Publisher<OrderPlaced>("orders", "order.placed");
PublishResult result = await publisher.SendAsync(new OrderPlaced("A-1", 42.50m));
```

[Publishing](publishing.md) and [consuming](consuming.md) go into what the results
and the dispositions mean.

## Building an envelope

```csharp
using AceMq.Amqp;

var envelope = Envelope.Of("order.placed")
    .Version(3)
    .CorrelationId("corr-1")
    .CausationId("cause-1")
    .Origin("orders@host-7")
    .Header("x-tenant", "acme")
    .Build();
```

Anything not set takes the contract default: `type` from the routing key,
`correlation` from the id, `origin` from the hostname, `version` and `attempt` at 1.

## Reading one off the wire

```csharp
var envelope = Envelope.FromWire(headers, routingKey, messageId);

Console.WriteLine(envelope.Id);
Console.WriteLine(envelope.CorrelationId);
Console.WriteLine(envelope.Attempt);
Console.WriteLine(envelope.FirstSeen.ToUnixTimeMilliseconds());

// Application headers only. Every x-acemq-* header has been materialised
// onto the envelope and removed from here.
foreach (var pair in envelope.Headers)
{
    Console.WriteLine($"{pair.Key} = {pair.Value}");
}
```

## Writing one back

```csharp
IDictionary<string, object> wire = envelope.ToWire();
```

An absent value is an absent header, never a null one — `x-acemq-causation` is
omitted entirely when there is no causation, matching Java exactly.

## The reserved namespace

```csharp
Envelope.Of("t").Header("x-acemq-id", "mine");
// ArgumentException: 'x-acemq-id' is in AceMQ's reserved namespace and would be
// dropped on consume. Use a namespace of your own, such as x-yourcompany-.
```

## Runnable example

`examples/csharp/` — build and run it:

```bash
cd examples/csharp
dotnet run
```

It publishes and consumes a message over the in-memory broker, so it runs with
nothing installed. [The VB example](vbnet.md) is the same program and prints the
same output.
