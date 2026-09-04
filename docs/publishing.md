# Publishing

A publisher sends payloads of one type to one exchange and routing key. Create it
once and keep it: it holds no connection of its own, but it does hold the back
pressure that stops a loop from queueing more unconfirmed publishes than the broker
will ever confirm.

```csharp
using var mq = await AceMqConnection.ConnectAsync("amqp://localhost");
var publisher = mq.Publisher<OrderPlaced>("orders", "order.placed");

PublishResult result = await publisher.SendAsync(new OrderPlaced("A-1", 42.50m));
```

`PublishResult` carries the envelope id the message went out with, whether the
broker routed it, and how long the confirmation took.

## What "sent" means

`SendAsync` returns after **the broker has confirmed the message**, not after the
bytes reach the socket. Those are different events, and the gap between them is
where messages are lost on a broker restart.

Confirms are on by default. Turning them off is a deliberate act:

```csharp
var config = ConnectionConfig.ForUrl("amqp://localhost")
    .WithoutPublisherConfirms()
    .Build();
```

Without them a publish reports success as soon as it is written. That is faster and
it is a weaker promise; the library will not make it quietly on your behalf.

## Unroutable messages fail

A message published to an exchange with no matching binding is a failure, not a
silent discard:

```
PublishFailedException: 7d3f… reached the broker but matched no queue bound to
'orders' for 'order.placed'. This is usually a topology mistake; if the discard is
intended, publish with PublishOptions.AllowUnroutable().
```

This is almost always a binding that was never declared, or a routing key with a
typo in it. The cheapest moment to discover that is the publish call. The
alternative — the broker's default — is that the message disappears and the problem
surfaces hours later as an absence nobody can account for.

Where the discard really is intended:

```csharp
var publisher = mq.Publisher<Telemetry>(
    "metrics", "cpu.sample", PublishOptions.Defaults().AllowUnroutable());
```

## Options

```csharp
PublishOptions.Defaults()            // persistent, unroutable reported
PublishOptions.TransientDelivery()   // not written to disk
    .ExpiringAfter(TimeSpan.FromMinutes(5))
    .WithPriority(4)
```

`TransientDelivery` is reasonable for a reading that is superseded within seconds.
It is not reasonable for anything a person would notice the absence of, because a
broker restart takes those messages with it.

## Sending an envelope you built

The default envelope is created for you, with the type defaulted to the routing key
and the correlation id defaulted to the message id. Supply your own to set
correlation, causation or application headers:

```csharp
var envelope = Envelope.Of("order.placed")
    .CorrelationId(incoming.Envelope.CorrelationId)   // continue the trace
    .CausationId(incoming.Envelope.Id)                // this message caused that one
    .Header("x-tenant", "acme")
    .Build();

await publisher.SendAsync(order, envelope);
```

Carrying the correlation id from the message you are handling is what makes a
request traceable across services. Setting causation to the incoming message's id is
what makes the chain reconstructable afterwards.

Application headers must not use the `x-acemq-` prefix — that namespace is the
engine's, and writing into it throws rather than letting the header vanish on the
way back out. See [the envelope](envelope.md).

## Several at once

```csharp
IReadOnlyList<PublishResult> results = await publisher.SendAllAsync(orders);
```

Each is confirmed in turn, and the results come back in the order the payloads did.
This is not a batch: it is a loop with one confirmation each, so a failure part way
through leaves the earlier messages published.

## Back pressure

`MaxOutstandingPublishes` — 1000 by default — caps how many publishes may be in
flight before `SendAsync` starts waiting. Without that cap, a caller in a tight loop
can outrun the broker's ability to confirm, and the failure arrives as memory growth
rather than as a slow publish.

## When the broker stops accepting

A broker under a memory or disk alarm blocks its publishers. That state is
visible rather than hidden as a hang:

```csharp
if (mq.IsBlocked) Console.WriteLine(mq.BlockedReason);
```

A publish attempted on a blocked connection throws `ConnectionBlockedException`.
