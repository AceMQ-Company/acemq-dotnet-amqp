# Streams

A stream keeps its messages after they are read. That is the whole difference from a
queue, and everything else follows from it: two readers can be at different places in
the same data, a new reader can start from the beginning, and nothing is removed by
being consumed.

```csharp
await mq.DeclareStreamAsync("events", TimeSpan.FromDays(7), 10_000_000_000);

using var reader = await mq.Stream<OrderPlaced>("events")
    .FromFirst()
    .Prefetch(100)
    .ConsumeAsync(async message => await _projection.ApplyAsync(message.Payload));
```

## Declare with a limit

```csharp
await mq.DeclareStreamAsync(name, maxAge: TimeSpan.FromDays(7), maxLengthBytes: 10_000_000_000);
```

A stream keeps everything written to it until one of these removes it. **Declaring one
without a limit is declaring a queue that grows until the disk is full**, and a full
disk stops the whole broker, not just this stream. Both arguments accept null, and
both being null is almost always a mistake.

## Where to start reading

```csharp
.FromFirst()                          // everything still held
.FromLast()                           // the last message written, and onward
.FromNext()                           // only what is written from now on
.FromOffset(12345)                    // an exact offset
.FromTime(DateTimeOffset.UtcNow.AddHours(-1))
.FromLast(TimeSpan.FromHours(1))      // however far back an hour reaches
```

The offset is a **consumer** setting, not a queue setting — two readers of the same
stream sit at different places, so it cannot belong to the queue.

`FromNext()` is the default, because it is the only one that does not replay history
the first time a reader starts. A reader that defaults to `FromFirst()` reprocesses
everything the moment it is deployed, which is a surprise the first time and an
outage if the stream is large.

## Prefetch is required

Streams need one, and the default here is 100. A stream will otherwise hand over its
entire history as fast as the network allows.

## What it is for

An audit log, an event log a new consumer needs to catch up on, or anything where
"what happened" matters after it has been handled.

It is the wrong shape for distributing work. Consuming does not remove anything, so
two workers reading the same stream both do the same job. Work distribution wants a
queue.

## Offsets are not stored for you

A reader starts where you tell it to, every time it starts. Nothing is remembered
between runs, so a restarting reader with `FromFirst()` reprocesses everything and
one with `FromNext()` misses whatever arrived while it was down. The Java library
behaves the same way; this is a property of the design, not a gap in the port.

To resume exactly where a previous run stopped, record the offset as you go and start
from it:

```csharp
using var reader = await mq.Stream<OrderPlaced>("events")
    .FromOffset(await _checkpoints.LastAsync("projection"))
    .ConsumeAsync(async message =>
    {
        await _projection.ApplyAsync(message.Payload);
        await _checkpoints.SaveAsync("projection", message.Headers["x-stream-offset"]);
    });
```

The consumer also reports where it has got to:

```csharp
reader.LastHandledOffset   // null until the first message is handled
reader.Handled
reader.Failed
reader.Skipped
```

Saving the checkpoint after handling means a crash between the two replays the last
message. Saving it before means a crash loses one. Neither is avoidable without a
transaction spanning the broker and your store, so pick the one your handler can
tolerate — and if it can tolerate neither, make the handler idempotent on the
envelope's id.
