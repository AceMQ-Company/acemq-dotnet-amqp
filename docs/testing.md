# Testing

The library ships an in-process broker. Use a `memory://` URL and nothing needs to
be installed or running.

```csharp
using var mq = await AceMqConnection.ConnectAsync("memory://" + Guid.NewGuid());
```

Naming the broker after the test keeps two tests from seeing each other's messages.
`InMemoryTransport.Reset()` forgets every broker, which is worth calling between
tests that share a name.

## What it does, exactly

It routes the way RabbitMQ routes: direct, fanout and topic, with `*` matching one
word and `#` matching zero or more. A binding that matches here matches on a real
broker. That is the property that makes it worth using — the topic rules are
[tested against the same table](topology.md#topic-patterns) the documentation states.

Dispositions behave as they do in production: `Retry` redelivers with the attempt
counter advanced, `Release` redelivers without advancing it, `DeadLetter` removes the
message and keeps it where a test can assert on it.

```csharp
var dead = InMemoryTransport.DeadLettered(brokerName, "orders.placed");
Assert.Single(dead);
```

## What it does not do

Durability, clustering, flow control, network failure, memory alarms, or anything
that depends on a broker restart. It cannot tell you that your quorum queue survives
a node loss, and it will happily let a test pass that a real broker would reject.

That is a real limit, not a caveat to skim: **an in-memory broker verifies your
logic, not your operations.** Both need testing, and only one of them can be done
without a broker.

## Against a real broker

```csharp
Transports.Register(new RabbitMqTransport());
using var mq = await AceMqConnection.ConnectAsync("amqp://localhost");
```

Testcontainers is the usual way to get one per test run. The things worth testing
there are the ones the in-memory broker cannot answer: that your dead-letter
topology is actually declared, that a quorum queue is configured the way you think,
and that your consumer recovers from a dropped connection.

## Waiting for a message

Consumption is asynchronous, so a test has to wait for it. Waiting on a signal is
better than sleeping:

```csharp
var arrived = new TaskCompletionSource<IMessage<OrderPlaced>>();

using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", message =>
{
    arrived.TrySetResult(message);
    return Task.FromResult(Ack.Accept());
});

await publisher.SendAsync(new OrderPlaced("A-1", 42.50m));

var received = await arrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
```

A fixed `Task.Delay` in place of that is the most common way a messaging test suite
becomes slow and flaky at the same time: too short and it fails on a loaded build
machine, too long and the suite takes minutes.

**Create the source with `RunContinuationsAsynchronously`.** Without it,
`TrySetResult` inside the handler resumes the awaiting test *on the client's consumer
dispatch thread*. Anything the test then does that the dispatch thread itself has to
service — disposing the consumer, for one — blocks that thread against itself, and
the test hangs rather than failing:

```csharp
var arrived = new TaskCompletionSource<IMessage<OrderPlaced>>(
    TaskCreationOptions.RunContinuationsAsynchronously);
```

This is not hypothetical. It is how the first version of this library's own
integration suite behaved, and the hang looked like a broker problem for long enough
to be worth writing down.
