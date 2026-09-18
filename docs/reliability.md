# Retries, duplicates and shutdown

Three things that decide whether a consumer survives contact with production.

## Retry policies

Without one, a handler that throws is retried after a fixed delay, forever. A policy
bounds that:

```csharp
await mq.ConsumeAsync<OrderPlaced>(
    "orders.placed",
    ConsumerOptions.Defaults().WithRetry(
        RetryPolicy.Exponential(5, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1))),
    Handle);
```

Five attempts, doubling from a second, capped at a minute. Each retry republishes the
message with `x-acemq-attempt` advanced rather than handing it back to the broker
unchanged, so the count travels with the message instead of living in one process. After
the last attempt it is republished to `{queue}.dlq` with the reason attached, rather than
occupying a consumer indefinitely.

```csharp
RetryPolicy.None()                                    // one attempt
RetryPolicy.Fixed(3, TimeSpan.FromSeconds(5))
RetryPolicy.Exponential(5, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1))
    .GiveUpAfter(TimeSpan.FromHours(6))
    .WithJitter(0.3)
```

### Why jitter is on by default

A fixed delay is fine for a dependency that is briefly unavailable. It is the wrong
shape for one that is *overloaded*: every consumer that failed at the same moment
retries at the same moment, and the dependency that was struggling gets the whole
herd at once. That is how a brief problem becomes a sustained one.

Jitter defaults to 0.2 — each delay is spread ±20%, **both** ways. One-sided jitter only
ever delays, which turns a thundering herd into a slower thundering herd rather than
dispersing it. Set it to zero only when you want reproducible timings in a test.

### Where the waiting happens

A wait shorter than thirty seconds is spent in the consumer, which holds the delivery
for the duration. A wait of thirty seconds or more is spent in the broker: the message
is republished into a `{queue}.retry.{delay}` queue whose `x-message-ttl` is the wait and
whose dead-letter target is the queue it came from, and the broker hands it back when the
time is up.

```csharp
RetryPolicy.Exponential(6, TimeSpan.FromSeconds(10))
    .WaitInBrokerFrom(TimeSpan.FromMinutes(1))   // move the line
    .WaitInBrokerFrom(TimeSpan.Zero);            // or wait for everything here
```

The reason for the split is that a consumer sleeping on a five-minute backoff is holding
an *unacknowledged* message. Restart it — a deploy, a crash, an autoscaler — and the
broker redelivers at once, so a five-minute policy delivers in none. Below thirty seconds
that costs seconds and one prefetch slot; above it, it costs the whole wait, which is the
bug worth spending a queue on.

Broker waits are never jittered: a rung's time-to-live is fixed when the queue is
declared, so a moved delay would name a queue that is not there — and the spread comes
free anyway, because each message's time-to-live starts when it arrives rather than when
the batch failed.

`policy.Schedule()` lists the delays without jitter and `policy.BrokerRungs()` lists the
ones that need a queue, which is what to read when deciding whether a policy is the one
you meant. `Topology.Define().QueueWithRetry(name, policy)` declares them, and a consumer
declares them again when it starts — a rung that does not exist loses the message rather
than reporting anything.

### Giving up by age, not only by attempts

```csharp
.GiveUpAfter(TimeSpan.FromHours(6))
```

Attempts alone cannot express "this has stopped being worth doing". A message retried
five times over a weekend is usually no longer useful, however few attempts that took.
The age is measured from the envelope's `x-acemq-first-seen`, which the original
publisher set.

## Duplicates

Every broker worth using delivers **at least once**. A consumer will eventually see
the same message twice — a redelivery after a crash, a retry that actually succeeded,
an outbox relay that published before recording it. If handling a message twice is
not safe, something has to remember:

```csharp
var store = InMemoryIdempotencyStore.ForOneDay();

await mq.ConsumeAsync<OrderPlaced>(
    "orders.placed", ConsumerOptions.Defaults().Idempotent(store), Handle);
```

The message id — the envelope's — is the key.

### Three states, not two

`Claim`, `Confirm`, `Release`. A store with only "seen" and "not seen" cannot tell a
message that **failed** from one that **succeeded**, so it drops the retry of a
message that was never handled. The claim is taken before the handler runs and
released if it fails, so a retry is not mistaken for a duplicate.

### What the in-memory store is and is not

It deduplicates what one running consumer sees, which is most redeliveries. It is
**per process and lost on restart**, so it cannot deduplicate across instances or
across a deploy.

For that, use the database-backed store:

```csharp
var store = new DbIdempotencyStore(() => new SqlConnection(connectionString),
                                   TimeSpan.FromDays(1));
```

`store.CreateTableSql()` gives you the table to put in a migration — the library
hands you the statement rather than running DDL against your database uninvited.

The claim is an **insert**, so the primary key does the mutual exclusion: two
consumers racing for the same message, one insert succeeds and the other is refused
by the database. A select-then-insert would have a window between the two.

Retention is the window duplicates are caught in. Too short and a redelivery after a
long outage is handled twice; too long and the store grows. A day covers what a
broker actually produces.

## Shutting down

Disposing the connection while handlers are mid-flight abandons their work. The
messages were never acknowledged so they come back — but any side effect already
applied has happened twice by the time they do.

```csharp
await mq.DrainConsumersAsync(TimeSpan.FromSeconds(30));
mq.Dispose();
```

Drain stops new messages being handed over and waits for the ones in progress. It
returns **false** if they did not finish in time, rather than throwing — shutting
down anyway is a legitimate choice, and the caller needs to know which one it is
making.

An orchestrator's deadline is not this timeout, so there is an overload that takes
the orchestrator's token:

```csharp
await mq.DrainConsumersAsync(TimeSpan.FromSeconds(30), stoppingToken);
```

**Cancelling abandons the wait, not the work.** Handlers already running are not
interrupted — nothing here can interrupt them — and consuming stays paused, because
resuming on the way out hands more messages to a process that is leaving. Cancellation
throws `OperationCanceledException` where the timeout returns `false`, deliberately:
`false` is the library saying it was given long enough, cancellation is the caller
changing its mind, and a caller that cannot tell them apart logs the wrong one.

```csharp
mq.PauseConsuming();     // stop taking new work; nothing is lost
mq.ResumeConsuming();    // the same message is handed over again
mq.PausePublishing();    // further publishes throw PublishingPausedException
mq.InFlight              // messages inside a handler right now
mq.Held                  // fetched, waiting at the pause gate, never handled
```

Pausing loses nothing. A message the broker has delivered but which was never handed
to a handler stays unacknowledged, so it is redelivered — to this consumer when it
resumes, or to another instance.

### What a successful drain does and does not mean

`true` means **every handler finished**. It does not mean every message the broker
sent was handled: a delivery that arrived while paused is decoded and then held at the
pause gate, and `InFlight` counts handlers, which that delivery never reached. `Held`
is the number sitting there. Nothing is lost either way — none of them was
acknowledged, so the broker redelivers them and no work was half done — but a caller
deciding whether a shutdown was clean should read both.

How many can pile up is bounded by the transport's dispatch concurrency, not by the
prefetch: the rest of a prefetch window stays in the client's buffer undecoded. The
other libraries differ here. Go runs every fetched delivery through a handler, so a Go
drain is bounded by the **prefetch** and can be much longer; Python nacks the unstarted
ones with requeue. All three end with the message back on the broker.

## Health

Everything above shows up in the health report, and in the actuator's
`/acemq-health`:

```csharp
var health = mq.Health();
health.Status                    // Up, Degraded or Down
health.Reports                   // per component
```

Ordered queues register themselves, so a **halted partition appears here** — which
matters, because a halted partition is a consumer that has stopped without the
connection or the process noticing.

A halted partition reports **Degraded, not Down**, and the actuator answers 200 for
it. The other partitions are still working, and a process that reports itself down
gets restarted — which loses the held message and fixes nothing. Down is reserved
for a connection that is actually closed.

### A blocked connection is Up, with the reason

RabbitMQ blocks a connection when it is low on memory or disk. The `connection` report
says **Up**, with `blocked` and `blockedReason` among its details:

```csharp
var connection = mq.Health().Reports.Single(r => r.Name == "connection");
connection.Status                          // Up
connection.Details["blocked"]              // "true"
connection.Details["blockedReason"]        // "low on memory"
```

Back pressure is the broker protecting itself. An application that fails its own health
check for it is an application an orchestrator restarts into the same blocked broker,
having thrown away whatever it was holding — and a fleet doing that together stops
draining the queues at the moment the broker most needs them drained. Java's Spring
Boot health indicator reports it the same way, and `acemq-go-amqp`'s lifecycle guide
documents the same trap.

This changed in 0.7.0; before it, the connection report said Degraded. Because an
aggregate takes the **worst** report, that reading overruled any more careful one a
caller composed alongside it, and callers were reconstructing the connection report by
hand to get out of its way. To get the old reading back, make it your own policy:

```csharp
sealed class BlockedIsDegraded : IHealthContributor
{
    private readonly AceMqConnection _mq;
    public BlockedIsDegraded(AceMqConnection mq) => _mq = mq;

    public string Name => "broker-pressure";

    public HealthReport Report() =>
        _mq.IsBlocked
            ? new HealthReport(Name, HealthStatus.Degraded,
                new Dictionary<string, string> { ["reason"] = _mq.BlockedReason ?? "" })
            : HealthReport.Up(Name);
}

mq.RegisterHealth(new BlockedIsDegraded(mq));
```

Registered, it is one component's opinion that a reader can see the name of, rather
than a verdict on the connection itself that nothing can get out from under.

### `HealthStatus` collides with the framework's

`AceMq.Amqp.HealthStatus` and
`Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus` share a name, and the
collision has a sharp edge. **Inside a namespace that is itself under `AceMq.Amqp`**
— `AceMq.Amqp.Hosting`, say — the enclosing namespace beats a file-level `using`
alias, so an unqualified `HealthStatus` resolves to this library's however the file
aliases it. Outside `AceMq.Amqp.*` the alias wins as written and there is nothing to
think about.

Alias both, or alias this one and leave the framework's bare:

```csharp
using AceHealthStatus = AceMq.Amqp.HealthStatus;
using HealthStatus = Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus;
```

They are not interchangeable in any case: Up/Degraded/Down against
Healthy/Degraded/Unhealthy, and the mapping between them is a decision — see above for
why blocked is not Degraded here.
