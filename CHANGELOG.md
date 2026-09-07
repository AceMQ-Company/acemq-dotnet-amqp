# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

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

### Fixed

- The integration suite leaked six `acemq.test.*` exchanges per run, because the
  transport had no way to delete one.
