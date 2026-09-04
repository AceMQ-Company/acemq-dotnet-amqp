# C#

The primary language for this library. Everything on this page compiles against
`AceMq.Amqp` today.

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
