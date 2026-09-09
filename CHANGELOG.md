# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Added

- **A pipeline now puts a routing slip on every message, so a replay resumes
  instead of restarting.** `Pipeline<T>` routed positionally — each consumer knew
  its own index and published to the next one — and attached nothing to the
  message. A message dead-lettered at step three therefore said nothing about
  where it had got to, so the only safe place to replay it was the entrance, and
  every step it had already passed ran again. That is survivable for a step that
  validates and not for one that charges a card. The slip now travels with the
  message and `Pipeline<T>.ResumeAsync` puts a half-finished message back at the
  step it had reached. A slip also lets one message skip a step the pipeline
  declares, which is the thing a positional chain cannot do at all.
- **Both wire forms of a routing slip are now read and written.** AceMQ has two
  and this library had one. `x-acemq-route` — step names, comma-joined, with a
  position — is what Java writes and what `RoutingSlip` has always been.
  `acemq-routing-slip`, a JSON itinerary carrying each stop's exchange and routing
  key plus the stops already done, is what Go, Python and Ruby write, and is now
  `Itinerary`. `Route.From(headers)` reads whichever is present, so a slip written
  by any of the five libraries is followed here. The JSON is byte-compatible with
  the other three: `steps` and `done`, each step an object with `exchange`,
  `routingKey`, and `name` and `completedAt` when they are not empty.
  `SendAlongAsync` and `ForwardAsync` take either form, and a pipeline writes the
  declared form unless told `WritingSlipAs(SlipForm.Itinerary)`.
- **The claim-check pattern**, which Java, Python and Ruby have had and this
  library did not. `ClaimCheckCodec.Wrapping(codec, store)` sends payloads at or
  above 64 KiB to an `IClaimCheckStore` and puts the key on the wire; anything
  smaller travels inline, exactly as it would without the codec. The framing is
  the three bytes the other three libraries write — `0xAC 0x01 0x00` for an inline
  payload, `0xAC 0x01 0x01` for a key — and an unframed body is read as the
  delegate would read it, so adding this to a live queue is safe. Ships with
  `InMemoryClaimCheckStore` for tests and `FilesystemClaimCheckStore` for a shared
  durable mount, both in the core package because neither needs a dependency.
  `ClaimCheckCodec.KeyOf(body)` answers the dead-letter-queue question — which
  object does this need — without fetching it.
- **`PulledMessage<T>.WireHeaders`**, the escape hatch `IMessage<T>` already had.
  A tool draining a dead-letter queue pulls rather than consumes, and the routing
  slip that says how far a message got is in the reserved namespace that `Headers`
  deliberately hides — so the one caller who most needs the slip was the one who
  could not see it.
- **A stream's segment size.** `DeclareStreamAsync(name, maxAge, maxLengthBytes,
  segmentBytes)` sets `x-stream-max-segment-size-bytes`, which Go, Python and Ruby
  expose and this library had no way to express — so a stream declared with a
  segment size anywhere else could not be declared identically here, and a queue
  redeclared with a different argument is refused rather than adjusted. **Absent
  unless asked for**, exactly as it is in the other three: the broker's default is
  the right one nearly always, and a default invented here would reintroduce the
  same mismatch from the other side. The argument names are on `StreamArguments`
  for a caller declaring through `Topology`.
- **`MetricNames.SetAsideFailed`, `RungMissing`, `TagTarget` and `OutcomeParked`**,
  mirroring the names Java added. `acemq.messages.set.aside.failed` is the counter
  that separates two failures which look identical from anywhere else: a message
  dead-lettered normally leaves the source queue and appears in the dead-letter
  queue, and one whose dead-letter queue was never declared leaves the source queue
  and appears nowhere. Queue depths show the same picture in both cases.
- **The outbox, the pipeline and request/reply now report themselves.** All three
  ran silently: the relay published records and told nobody, a pipeline run ended
  and counted nothing, and a request's round trip — the one duration the caller
  actually waited for — was the gap between two other spans. `acemq.outbox.lag`,
  `acemq.outbox.total`, `acemq.pipeline.run.duration`, `acemq.pipeline.run.total`,
  `acemq.request.duration` and `acemq.request.total` are now recorded under the
  names Java has used since 0.2, with the same tags. `acemq.outbox.lag` is the one
  number that reveals a stopped relay: a committed, unpublished row is a message
  that exists, is owed to somebody, and appears in no queue depth anywhere.
- **`Envelope.Age`**, the elapsed time since `FirstSeen`, matching Java's
  `Envelope.age()`.

### Changed

- **Spans are named the way the other four libraries name them.** Every span
  attribute now uses OpenTelemetry's messaging semantic conventions, plus
  `messaging.acemq.*` for the three things those conventions have no name for —
  the exact keys Java, Go, Python and Ruby all write. This library used to put its
  *metric* tag keys on spans (`queue`, `outcome`, `acemq.attempt`) and to set
  `messaging.system` to `acemq`, so a trace query written against any of the other
  four matched none of its spans. The four span events are renamed to match too:
  `message.retried`, `message.dead_lettered`, `outbox.publish_failed` and
  `pipeline.run_finished`, in place of `acemq.message.retried` and
  `acemq.message.dead-lettered`.

  **This is a breaking change for anything reading the old attribute names.** The
  constants on `AceMqTelemetry` are the supported way to refer to them, and they
  have been updated in step.

- **`MetricNames` is complete, and now says something checkable.** The file claimed
  to be identical to `org.acemq.amqp.api.MetricNames` character for character while
  missing fourteen of its members — the request, outbox and pipeline names, the
  `pipeline` and `step` tag keys, five outcome values and the request span suffix.
  All of them are there, and `TelemetryTests` asserts every single one rather than
  a sample, which is what the old claim needed in order not to rot again.

- **A handler's own give-up is reported as `rejected`, not `dead_lettered`.**
  `Ack.DeadLetter` now tags `acemq.consume.total` and its span `rejected`, and
  `dead_lettered` is left to mean what it means in Go, Python and Ruby: the engine
  giving up, when a `RetryPolicy` runs out of attempts or a message is parked. This
  library and Java reported both as `dead_lettered` — three libraries against two,
  and the three were right, because a decision somebody took about a message and an
  exhaustion the engine reached are different events with different answers. Java is
  moving in step. The message still goes to `{queue}.dlq` either way and still
  records a `message.dead_lettered` span event, so a trace search for dead letters
  finds it; what changed is the word.

  **This is visible on a dashboard.** `acemq.messages.dead.lettered.total` no longer
  counts handler rejections, and `acemq.consume.total` moves them to
  `outcome = rejected`. A panel or an alert built on the old numbers will show
  dead-letters falling and rejections appearing without anything changing in your
  service; add the two together to get the old figure.

- **Diagnostic codes are dotted, not hyphenated.** `acemq.message.dead-lettered`,
  `acemq.retry.rung-missing` and `acemq.schedule.foreign-message` are now
  `acemq.message.dead.lettered`, `acemq.retry.rung.missing` and
  `acemq.schedule.foreign.message`. Go, Python and Ruby use dots throughout and Java
  has no diagnostics channel to appeal to, so this library was alone in three of its
  seven codes.

  **This is a breaking change to public constants.** Code referring to them through
  `AceMqDiagnostics.DeadLettered`, `AceMqDiagnostics.RungMissing` and
  `AceMqDiagnostics.ScheduleForeign` needs a recompile and nothing more; anything
  matching the strings — a log filter, an alert rule, a log-based metric — has to be
  updated by hand.

- **`JsonCodec` accepts `text/json`.** A legacy alias that predates the registration
  of `application/json` and is still what some older producers and a few gateways
  stamp on a body that is plainly JSON. Go, Python and Ruby have always accepted it
  and Java is adding it in parallel; refusing it here made a message four other
  libraries could read unreadable in this one. The write side does not change —
  `JsonCodec.ContentType` is still `application/json`, because writing an alias only
  moves the problem to whoever reads next.

### Fixed

- **A .NET requester and a Go, Python or Ruby responder could not talk.** The reply
  address was carried on AMQP's native `reply-to` property here and in Java, and on
  an `acemq-reply-to` application header in Go, Python and Ruby — so neither side
  could see where the other wanted its answer sent, in either direction, and no
  fixture covered it. Every library now **writes both and reads either**:
  `Requester` sets the native property and `Requester.ReplyToHeader` to the same
  value, and `Responder` reads the header first and falls back to the property. The
  order is identical in all five. `PatternTests` pins a request carrying only the
  property, a request carrying only the header, a request carrying both, and one
  carrying neither.

- **A pipeline encoded every payload twice, so every step after the first read
  nonsense.** A step encodes its own output; the publisher carrying that output to
  the next step then sent those bytes through the connection's codec as well, so
  what arrived was base64 of JSON of the payload. It went unnoticed because a step
  that trims or appends a string succeeds just as well on nonsense, and because no
  test had ever asserted a payload's value past the first hop. A Java, Go, Python
  or Ruby consumer reading a `<pipeline>.<step>` queue got the same nonsense. Both
  the entry publish and every hop now send the encoded bytes verbatim, under the
  content type the sending step encoded with.
- **A pipeline run whose last step returned nothing was counted as filtered out
  rather than completed.** The last step of a route is almost always a terminal
  action with nothing to return, so `Pipeline.Completed` stayed at zero for a
  pipeline that was working perfectly and `EndedEarly` counted every success.
  Whether there is a step after this one is now asked first, which is the order
  Java's `Pipeline` uses and warns about in a comment.
- **A pipeline reset the message clock at every hop.** The envelope handed to the
  next step was rebuilt without `FirstSeen`, so a message looked newly published at
  each step: an age-bounded `RetryPolicy` could never expire it, and a run's
  duration measured the last step instead of the run. `FirstSeen`, `Version` and
  `Origin` now travel with it.
- **A broker's refusal to accept a publish was counted as `rejected`.** A publish
  has three outcomes in this family's vocabulary — `confirmed`, `unroutable`,
  `failed` — and `rejected` belongs to a delivery, where it means a handler
  released the message. A nack therefore put a value on `acemq.publish.total` that
  no other library writes, and a panel filtering publishes by outcome silently
  dropped every broker refusal. It is `failed`, as it is in Java.
- **A requester's reply queue outlived the process that made it.** Java's
  `Requester` declares its reply queue with `x-expires`; this one did not, so every
  requester that died without disposing left a durable queue behind a guid nothing
  could ever look up again, and a long-lived service accumulated one per restart.
  The queue now expires after the same ten minutes idle.
- **`AceMq.Amqp.Yaml` refused `text/x-yaml`.** The fourth of the four names YAML
  has gone by, accepted by the Java, Python and Ruby codecs and refused here, so a
  body labelled that way by any of them arrived undecodable.
- **`AceMq.Amqp.XmlCodec` no longer returns an empty object for a body it could not
  bind.** `XmlSerializer` matches element names case-sensitively, so the
  `<orderId>` a Java, Go, Python or Ruby publisher writes never bound to a C#
  `OrderId` — and up to 0.3.0 it said nothing about that. It returned a
  fully-formed `Order` with every member at its default, and the message was gone
  with no exception anywhere. `Decode` now throws `AceFatalException` when a
  document had content and none of it reached a member, and the message names
  `AceMq.Amqp.Xml.InteropXmlCodec` as the codec that would have worked.

  **This is a breaking change**, and a deliberate one: there is no version of
  "relied on getting an empty object back" that was working. It is an
  `AceFatalException`, so such a message is dead-lettered rather than retried
  forever — the same bytes bind no better on the next attempt.

  The refusal is narrow. It fires only when *nothing* bound. A document with some
  elements bound and some unknown still decodes, because that is what a producer
  adding a field looks like and breaking forward compatibility would be a worse bug
  than the one being fixed; an empty document that legitimately decodes to an
  all-default object still decodes, because nothing in it was unknown.

  `XmlCodec` was **not** marked `[Obsolete]`, which was the other candidate. It is
  correct for .NET talking to .NET and for a document that has to match an XSD, and
  an obsolete warning on a correct use is noise that teaches people to suppress
  warnings. The defect was silence, not existence.

- **A message that exhausts its retry policy now reports `dead_lettered` rather
  than `retried`.** The consume outcome was derived from the handler's `Ack` before
  the engine had settled the delivery, so a handler asking for a retry on its last
  permitted attempt produced a span tagged `outcome="retried"` and a
  `acemq.messages.retried.total` increment — for a message nothing would ever try
  again. `acemq.messages.dead.lettered.total` therefore only ever counted the
  dead-letters a handler asked for by name, and a trace backend queried for
  dead-lettered messages found none at all.

  The outcome is now decided by the settle, which is the only place that knows it.
  A dashboard built on 0.3.0 numbers will show retries falling and dead-letters
  rising without anything changing in the service being measured.

### Added

- **Span events at the two moments a failing message reaches.** A `<queue> process`
  span now carries `acemq.message.retried` (tagged with `acemq.retry.delay.ms`,
  `acemq.destination` and `acemq.reason`) when the engine schedules another
  attempt, and `acemq.message.dead-lettered` (tagged with `acemq.destination` and
  `acemq.reason`) when it gives up. The delay is the one the engine actually chose
  — the policy's, not the handler's suggestion — so a trace shows the wait that
  happened.

  The names are constants on `AceMqTelemetry` (`EventRetried`, `EventDeadLettered`,
  `EventTagDelayMs`, `EventTagDestination`, `EventTagReason`) rather than on
  `MetricNames`, which is kept character for character identical to Java's
  `org.acemq.amqp.api.MetricNames` and has no span-event constants in it. Java
  carries the same two moments as `messageRetried` and `messageDeadLettered` on its
  `Telemetry` interface.

  Nothing is allocated when nothing is listening: no listener means no `Activity`
  was created, so emitting an event is a null check and a return.

- **`ProtobufCodec` reads `application/vnd.google.protobuf`**, the content type
  Google's own tooling writes. It has no `+protobuf` suffix to be caught by, so it
  had to be named; until it was, .NET refused a message Go and Ruby read without
  complaint. The full read set is now `application/x-protobuf`,
  `application/protobuf`, `application/vnd.google.protobuf` and any `*+protobuf`
  suffix type, exposed as `ProtobufCodec.ReadableContentTypes`. The write side is
  unchanged: `application/x-protobuf`, the same as Java's.

- The API reference now covers every optional package — `AceMq.Amqp.Avro`,
  `.Protobuf`, `.Toml`, `.Xml` and `.Yaml` alongside the core, `.RabbitMq`,
  `.Diagnostics` and `.Crypto`. A consumer looking up `InteropXmlCodec` gets a
  page. It roughly doubles the docfx step, from about 6 seconds to about 12.

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
