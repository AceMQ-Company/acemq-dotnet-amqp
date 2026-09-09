# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Changed

- **BREAKING, and it is a change to the wire format: encrypted message bodies are
  now AES-256-GCM in the framing the Java, Python, Ruby and Go libraries write.
  Bodies this library wrote up to 0.3.0 are AES-256-CBC with HMAC-SHA-256 and are
  not that.** They still decrypt here; they never decrypted anywhere else, and they
  never will. If a queue holds encrypted bodies written by 0.3.0 and anything other
  than .NET has to read them, they must be republished by a .NET consumer running
  this release. Read the migration note below before upgrading a service that
  encrypts.

  The content type has been `application/vnd.acemq.encrypted` in all five libraries
  from the beginning, and the bytes under it were never the same. On the wire now:

  ```
  0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
  ```

  Against, up to 0.3.0:

  ```
  0x01  len  key identifier   16-byte IV   ciphertext   HMAC-SHA-256, 32 bytes
  ```

  Every encryption test this library had passed throughout, because every one of
  them decrypted what it had itself encrypted — which proves nothing about reading
  another language's message, since both sides share the bug. The new suite
  decrypts bodies the Java, Python and Ruby libraries really wrote, and pins the
  exact bytes this package produces for a known key, key identifier and nonce
  against the vector the other four agree on.

  **The old cryptography was not the defect.** AES-256-CBC with encrypt-then-MAC is
  a sound construction, it verified the tag before decrypting anything, and it was
  chosen because `System.Security.Cryptography.AesGcm` does not exist on
  `netstandard2.0` — a real constraint, not an oversight. What was wrong is that it
  travelled under a content type promising a format it could not read.

- **BREAKING: `EncryptedCodec`, `Keyring`, `KeyringBuilder`, `EncryptionKey` and
  `IKeyring` have moved out of the core assembly into a new `AceMq.Amqp.Crypto`
  package, and out of the `AceMq.Amqp` namespace into `AceMq.Amqp.Crypto`.** A
  consumer that encrypts adds one `PackageReference` and one `using`; the compiler
  names every line that needs it. Nothing else in the core moved.

  This is how the constraint above was resolved, and the reasoning is kept in
  `src/AceMq.Amqp.Crypto/AceMq.Amqp.Crypto.csproj` beside the target it explains,
  because that is the file somebody will edit when they next try to retarget it.
  In short: multi-targeting the core `netstandard2.0;net8.0` would have put two
  cryptographic code paths behind one wire format, where a divergence shows up only
  at runtime, on one target, in somebody else's queue — and would have left
  `netstandard2.0` with no GCM at all. Throwing `PlatformNotSupportedException`
  there was worse: `netstandard2.0` is the .NET Framework 4.6.2 consumer this
  library exists to reach. So encryption became its own package, the way
  `AceMq.Amqp.Avro`, `.Yaml`, `.Toml` and `.Xml` already isolate an optional
  dependency, and takes AES-GCM from **BouncyCastle**, which has it on
  `netstandard2.0`. One implementation, one set of bytes, identical on .NET
  Framework 4.6.2 and on .NET 10.

  The core keeps its `netstandard2.0` target and its dependency list, and a
  consumer who does not encrypt never acquires a cryptography library.

### Deprecated

- **Reading the pre-0.4.0 .NET framing.** `EncryptedCodec.Decode` still opens a body
  beginning `0x01`, which is what 0.3.0 wrote, and `EncryptedCodec.KeyIdOf` still
  names its key. The two framings are told apart with certainty rather than guessed
  at — the family framing begins `0xAE`, the old .NET one begins `0x01`, and
  anything else is refused naming both — so no body is ever tried one way and then
  the other, which would have made a wrong key indistinguishable from an unknown
  format.

  **This is a migration affordance, not a feature, and there is no way to write that
  framing any more.** `EncryptedCodec.IsLegacyDotNetBody(body)` answers the question
  that has to be answered before it goes: is there anything left in this queue that
  only .NET can read. **The reader and that method will be removed in 1.0.0.** Drain
  those queues before then.

### Added

- **`AceMq.Amqp.Xml`**, a package whose `InteropXmlCodec` reads and writes the XML the
  Java, Go, Python and Ruby libraries write. It writes `application/xml` and reads
  `application/xml`, `text/xml` and any `+xml` suffix type; like the YAML and TOML
  codecs it never volunteers for a message whose sender set no content type, because
  that case belongs to `JsonCodec` and `BytesCodec`.

  It depends on nothing. `System.Xml` is part of the framework and `System.Text.Json`
  arrives with the core, which already pins it — so this is the cheapest of the format
  packages to take on. It is a separate package for the other reason those exist: so
  the core's list of formats does not grow every time somebody wants a different one.

  **This is not the core's `XmlCodec`, which stays where it is.** That one is built on
  `XmlSerializer` and remains right for .NET talking to .NET and for documents that
  have to match an XSD. It is wrong for reading a message another language sent, and
  wrong quietly: `XmlSerializer` matches element names case-sensitively, so Java's
  `<orderId>` does not bind to a C# `OrderId` — and it does not complain. It returns an
  object with every field at its default, so a service that reached for it to read a
  Java queue would see empty orders and no errors. `AceMq.Amqp.Xml.Tests` decodes a
  real Java body with each codec and asserts exactly that difference. The names differ
  so a consumer importing both namespaces gets a choice rather than a `CS0104`.

  **Every document type declaration is refused, and the refusal is not
  configurable** — the same decision Java, Python and Ruby took. The usual reason
  given for this is external entities, and it is the wrong one: there is no
  `XmlResolver`, so `file:///etc/passwd` is already inert. Internal entity expansion is
  not. Measured against this runtime with `DtdProcessing.Parse` rather than read off a
  documentation table, 201 characters of three nested entities expand to 1,000, 248
  characters of four expand to 10,000, and 311 characters of six expand to a
  million — the last still inside the SDK's own 10,000,000-character entity cap, so
  that cap is not what saves a consumer. The measurement is a test, so a runtime that
  changed this fails the build rather than leaving the reasoning quietly untrue.

  The refusal is in two layers because one is not enough. The body is scanned for
  `<!DOCTYPE` before a parser sees it, so the failure names what is wrong instead of
  surfacing an `XmlException` about a security property the caller never chose; then
  the reader is created with `DtdProcessing.Prohibit` and `XmlResolver = null`, which
  catches what the scan cannot see — a UTF-16 body, where `<!DOCTYPE` is not a UTF-8
  substring but the reader sniffs the encoding and would read that DTD perfectly well.

  **Both list shapes decode.** Jackson wraps a list in an element of its own where
  Go's `encoding/xml` repeats the sibling, and a one-item list is indistinguishable
  from a scalar in either. All three arrive as the same `List<string>` or `T[]`; the
  unwrapping is driven by the declared member rather than guessed from the document,
  which is the only way to tell a wrapped list from an object holding a field of its
  own name. The codec writes the repeated-sibling form, which Jackson reads too.

  The interop tests assert against `xml-interop-samples.json`, message bodies exactly
  as the Java and Go libraries wrote them, copied from the Ruby and Python
  repositories' fixtures — not against this codec's own output, which would prove
  nothing about reading a Java message.

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
