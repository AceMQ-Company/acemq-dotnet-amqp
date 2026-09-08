# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Added

- **`Saga<T>`**, a sequence of steps where each one knows how to undo itself. When a
  step fails the completed ones are compensated in reverse — the order the world was
  changed in, and the order a compensation depending on later state needs. A step with
  no compensation is skipped rather than treated as an error, because a step that only
  read something needs no undo and a library cannot tell that apart from a forgotten
  one.

  **A compensation that itself throws does not stop the others.** It is reported and
  the remaining ones still run; stopping there would leave more undone than continuing
  does. The step's name is collected into `SagaResult.Unresolved`, and `HasUnresolved`
  is the flag to alert on: everything else a saga reports is recoverable by
  construction, and these are real-world effects that happened, were meant to be
  undone, and were not.

  Nothing is thrown for a step failure. `RunAsync` returns a `SagaResult` carrying
  whether it completed or compensated, the step that failed, the failure, the steps
  that ran and the ones nobody could undo. `RunAsync(subject, cancellationToken)`
  cancels the forward path only — a cancelled step is a failed step and the earlier
  ones are undone, with the compensations run uncancelled, because a cancelled saga is
  precisely the one that most needs undoing.

  In-process and not durable, and it publishes nothing: it matches the behaviour of
  Java's `org.acemq.amqp.patterns.Saga` but has no wire contract to hold to, so the
  API is C#'s — async steps, `CancellationToken`, and synchronous overloads for steps
  that do not need either.

- **`Scheduler`**, delayed delivery through a ladder of TTL queues. `InAsync` and
  `AtAsync` take an exchange, a routing key and a payload; the message waits on the
  broker and arrives later.

  The obvious implementation — one queue, `expiration` per message, dead-lettered to
  the destination — is wrong for anything but a single fixed delay, because a classic
  queue expires messages only at its head. A four-hour message in front of a
  one-minute message delivers the one-minute message in four hours, and nothing
  reports it. Instead there are five rungs with *uniform* time to live —
  `acemq.schedule.1h`, `.10m`, `.1m`, `.10s`, `.1s` — each dead-lettering into
  `acemq.schedule.due`, where the scheduler either delivers the message or puts it in
  the largest rung that does not overshoot. Every message in a rung has the same
  delay, so the head is always the one due soonest.

  **This is a wire contract.** The exchange, the six queue names, the three arguments
  on each rung and the four headers (`x-schedule-exchange`,
  `x-schedule-routing-key`, `x-schedule-due-at` as epoch milliseconds, and
  `x-schedule-content-type`) are exactly what Java's
  `org.acemq.amqp.patterns.Scheduler` declares and writes. A .NET service and a Java
  service scheduling through one broker declare the same topology; a difference in one
  argument would be `PRECONDITION_FAILED` on whichever started second, and the
  integration suite proves both halves — Java's literal argument table accepted, and a
  table one millisecond different refused.

  The headers deliberately avoid the `x-acemq-` prefix, which is reserved and dropped
  from the application's view on the way in. The content type travels with the payload
  because the scheduler republishes bytes rather than objects, and a consumer picks its
  codec from it.

  **The control consumer declares no dead-letter queues.** Since 0.3.0 every consumer
  declares `{queue}.dlq` and `{queue}.parked` when it starts, and the control queue's
  name is fixed and shared — so doing that here would leave
  `acemq.schedule.due.dlq` and `acemq.schedule.due.parked` on the broker of every
  service that ever constructed a scheduler, two durable queues nothing publishes to
  and nobody drains. It consumes as a private queue, the same opt-out `Requester`
  uses, and earns it by never giving up: it reads raw bytes, which cannot fail to
  decode, and accepts on every path.

- **`AceMqDiagnostics.SagaCompensating`, `SagaUnresolved` and `ScheduleForeign`** —
  `acemq.saga.compensating`, `acemq.saga.unresolved` and
  `acemq.schedule.foreign-message`. The last is how a message that reached
  `acemq.schedule.due` without the headers a scheduled message carries is reported:
  the control consumer has nowhere to put it, so it is dropped, and the event is the
  only record that it existed.

## [0.3.0] - 2026-09-08

### Changed

- **A consumer declares the dead-letter half of its topology when it starts**,
  not the first time it needs it. `acemq.dlx`, `{queue}.dlq`, `{queue}.parked`
  and the two bindings are declared before the consumer subscribes, alongside
  the retry rungs, which is what Java's `RetryTopology.declare` has always done
  and what the shared contract fixture records as `declaredBy: both`
  (ADR-032). They were declared on the settle path until now — the first time a
  message was actually dead-lettered or parked. The end state on a broker was
  identical either way; the window before it was not. A consumer that gives up
  republishes to `{queue}.dlq`, and until something first failed there was
  nothing on the broker for an operator to see, to alert on, or for the
  broker's own dead-lettering to reach.

  The declarations are idempotent and identical to `Topology.Builder`'s, so a
  service may apply a topology and start a consumer in either order.

  **This declares two queues per consumed queue that were not there before.**
  A service consuming `orders.new` now has `orders.new.dlq` and
  `orders.new.parked` on the broker from start-up whether or not anything has
  ever failed on it.

- **The dead-letter half is declared with or without a retry policy.** Giving
  up is not something a retry policy switches on: `Ack.DeadLetter` from a
  handler and a body that will not decode both republish out of the consumer
  either way. The retry exchange is the opposite case and is still only
  declared when there are rungs to expire through it — a consumer with no
  broker waits leaves no `acemq.retry` and no binding behind. Java does not
  reach this case at all, since it builds no retry topology without a policy.

- The lazy declaration on the settle path is **gone** rather than kept beside
  the new one. Two places that can declare one queue is two places that can
  declare it differently, and a second declaration that disagrees is the
  `PRECONDITION_FAILED` this whole line of work exists to prevent. A queue
  deleted while a consumer is running now fails the move and releases the
  message back to the broker, which is reported and recoverable, rather than
  being silently redeclared underneath it.

- `Requester` consumes its reply queue as a private queue and so declares no
  dead-letter queues beside it. Its name is `acemq.reply.{a fresh guid}`, used
  once and never again, so a durable pair per requester would be litter under a
  name nothing could look up later — and that consumer decodes to `byte[]` and
  always accepts, so it can reach neither queue.

## [0.2.0] — 2026-09-07

> ### ⚠ Migrating: one dead-letter convention instead of three
>
> `Topology.Builder.QueueWithDeadLetter` produced `{name}.dlx` and
> `{name}.dead`. It now produces `{name}.dlq` and `{name}.parked`, bound to a
> shared durable direct exchange `acemq.dlx`, which is what the consumer path
> already used and what the other four libraries declare.
>
> **A queue that already exists under the old names is not migrated.** Drain
> `{name}.dead` before upgrading, or leave it in place and drain it afterwards —
> `Replay` still recognises the `.dead` suffix precisely so an existing queue can
> be emptied. It is kept for that reason and not for new topologies.
>
> The source queue is also declared with `x-dead-letter-exchange: acemq.dlx` and
> `x-dead-letter-routing-key: {name}.dlq`. A queue that already exists without
> those arguments cannot be redeclared with them; AMQP forbids changing a
> queue's arguments in place and the declare is refused with
> `PRECONDITION_FAILED`.

> ### ⚠ Migrating: a source queue is now a quorum queue
>
> `Topology.Builder.Queue(name)`, `QueueWithDeadLetter(name)`,
> `QueueWithRetry(name, policy)` and `AceMqConnection.DeclareQueueAsync(name)`
> declared a classic queue. They now declare a durable **quorum** queue, which is
> what the Java, Go, Python and Ruby libraries declare.
>
> This is about two services sharing a queue rather than about replication. A
> queue's type is fixed when it is created, so a Java service declaring `orders`
> as quorum and a .NET service declaring the same name as classic do not get one
> queue each: the second declare is refused and that service cannot consume at
> all.
>
> **A queue that already exists as classic cannot be redeclared as quorum.** The
> broker refuses the declare with `PRECONDITION_FAILED` and the application does
> not start. Such a queue has to be drained and recreated — there is no in-place
> conversion, and this library will not delete a queue that has messages in it on
> a declaration's behalf. Where that is not acceptable, ask for the old type
> explicitly: `Queue(name, QueueType.Classic)`,
> `QueueWithRetry(name, policy, QueueType.Classic, null)` and
> `DeclareQueueAsync(name, QueueType.Classic, null)` all still do exactly what
> they did.
>
> The retry rungs, `{name}.dlq` and `{name}.parked` are **unchanged and still
> classic**, as they are in every other AceMQ library, so nothing already on a
> broker has to move. Reply queues are classic too, and asked for explicitly:
> RabbitMQ does not allow a quorum queue to be exclusive or auto-delete.

> ### ⚠ Migrating: `QueueWithRetry` now wires the source queue to `acemq.dlx`
>
> `Topology.Builder.QueueWithRetry` declared `{name}.dlq` and `{name}.parked` and
> bound them to `acemq.dlx`, but left the source queue without
> `x-dead-letter-exchange` and `x-dead-letter-routing-key`. The dead-letter
> queues existed and the broker had no route into either of them.
>
> The library's own give-up path was unaffected — it republishes straight to
> `{name}.dlq` rather than relying on the broker. What was missing is the
> backstop underneath it: a source-queue TTL expiring, an `x-max-length` drop, a
> rejection from something that is not this library. Those were being discarded
> silently. `QueueWithDeadLetter` always set both arguments; only the retry
> builder did not.
>
> **A queue already declared by `QueueWithRetry` cannot be redeclared with the
> arguments**, because AMQP will not change a queue's arguments in place — the
> declare is refused with `PRECONDITION_FAILED`. Drain the queue and recreate it.
> Nothing on the queue is lost by upgrading on its own; the queue simply has to be
> replaced before the new declaration will be accepted.

### Added

- **A retry ladder.** Delays at or above 30 seconds wait in the broker, in a
  `{queue}.retry.{delay}` queue whose `x-message-ttl` is the wait, returning to
  the source queue through the durable direct exchange `acemq.retry`. Shorter
  delays wait in the consumer. `WaitInBrokerFrom(TimeSpan.Zero)` keeps every
  wait in the process.
- `AckKind.Park`, `Ack.Park` and `Ack.Retry(reason)`.
- `Topology.Builder.QueueWithRetry(name, policy)`, which takes the policy rather
  than a list of delays — a second copy of the list is free to drift from the
  first, and the way that drift shows up is a retry addressed to a queue nobody
  declared, at the moment the service is already failing.
- **`AceMqDiagnostics`**, a diagnostic seam in the core, raised for
  `acemq.move.failed`, `acemq.message.dead-lettered`, `acemq.message.parked` and
  `acemq.retry.rung-missing`. A message that could not be moved and was handed
  back to the broker is exactly the event an operator needs to see, and until
  now the only trace was an `Activity` status. `LoggerSink` lives in
  `AceMq.Amqp.Diagnostics` rather than putting a DI abstraction in the core.
- `DeleteExchangeAsync` on the transport, in both implementations.
- `Replay` resets `x-acemq-attempt` to 1, with `KeepingAttempts()` to opt out.

### Changed

- **A retry is republished with the attempt advanced, not requeued**, and
  dead-lettering and parking republish with the reason and then acknowledge the
  original rather than using `BasicNack(requeue: false)`. The attempt count now
  travels on the message instead of in a `ConcurrentDictionary` that is per
  process and unbounded across a fleet.
- **`RetryPolicy.Fixed` no longer applies jitter.** It defaulted to 0.2, which
  the Python and Ruby libraries do not.
- `MaxDelay == TimeSpan.Zero` now means no ceiling, and `MaxMessageAge ==
  TimeSpan.Zero` means never give up on age, matching the other libraries.
- The in-memory transport honours `x-message-ttl`, so it stops certifying code
  that a real broker breaks.
- **A source queue is declared as a durable quorum queue**, matching the other
  four libraries, and the retry rungs, `{name}.dlq` and `{name}.parked` stay
  classic for the same reason. See the migration note above.
- `Requester` declares its reply queue as `QueueType.Classic` explicitly instead
  of taking the library default, because a quorum queue cannot be exclusive or
  auto-delete and a reply queue holds answers nobody will read once the process
  asking is gone.

### Fixed

- The integration suite leaked six `acemq.test.*` exchanges per run, because the
  transport had no way to delete one.
