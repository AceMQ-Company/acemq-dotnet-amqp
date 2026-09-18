# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Changed

- **`Health()` reports a blocked connection as `Up`, with the reason, where it used
  to report `Degraded`. This is a behaviour change to a published API.** The
  `connection` report's status is now `Down` when the connection is not open and `Up`
  otherwise; `blocked` and `blockedReason` stay in its details, unchanged, and the
  actuator still answers 200 as it did before.

  RabbitMQ blocks a connection when it is low on memory or disk. That is the broker
  protecting itself, and an application that fails its own health check for it is an
  application an orchestrator restarts into the same blocked broker, having thrown
  away whatever it was holding — a fleet doing that together stops draining the
  queues at the moment the broker most needs them drained. Java's Spring Boot health
  indicator has always reported it as up with the reason, and
  `acemq-go-amqp/docs/lifecycle.md` documents the same trap; this brings .NET into
  line with both.

  The sharp part was not the reading itself but that nothing could get out from under
  it. `AggregateHealth` takes the **worst** report, so the built-in `connection`
  report overruled any more careful answer a caller composed beside it —
  `acemq-dotnet-amqp-hosting` ended up rebuilding the connection report from
  `IsOpen`/`IsBlocked`/`BlockedReason` and filtering the library's own out of the
  aggregate, which is not something a downstream package should have to do.

  **If you want the old reading**, make it your own policy rather than the library's,
  by registering a contributor that says so:

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

  Registered, it is one named component's opinion, which a reader can see the source
  of and a caller can choose not to register. `docs/reliability.md` carries this, and
  `docs/observability.md` no longer claims `/acemq-health` answers 503 for a blocked
  connection — it never did, and the page was describing an intention rather than the
  code.

  Asserted against a broker that is really blocked, in
  `tests/AceMq.Amqp.RabbitMq.Tests/BlockedConnectionTests.cs`: it drops the memory
  high watermark with `rabbitmqctl` until RabbitMQ raises the alarm, publishes so the
  connection is one the broker blocks, waits for `connection.blocked` to arrive, and
  puts the watermark back. A test that sets a flag and asserts on the flag proves the
  flag is readable and nothing else. It reaches `rabbitmqctl` through
  `ACEMQ_TEST_RABBITMQCTL`, a command prefix — `docker exec <container> rabbitmqctl`,
  or just `rabbitmqctl` — and fails rather than skipping when it is unset. CI names
  the service container and sets it.

### Added

- **`DrainConsumersAsync` takes a `CancellationToken`.** A drain is exactly the
  operation an orchestrator bounds, and the old signature took only its own timeout,
  so every caller had to race it from outside and cancel out from underneath it —
  which is what `acemq-dotnet-amqp-hosting` was doing. An overload rather than an
  optional argument: the existing one-argument call still binds, and the VB audit
  rejects optional arguments because VB resolves them differently.

  **Cancelling abandons the wait, not the work.** Handlers already running are not
  interrupted — nothing here can interrupt them — and consuming stays paused, because
  resuming on the way out hands more messages to a process that is leaving. What the
  caller gets back is its own deadline; `InFlight` and `Held` then say what was left
  behind, and those messages are unacknowledged, so the broker redelivers them.

  Cancellation throws `OperationCanceledException` where the timeout returns `false`,
  deliberately: `false` is this connection saying it was given long enough and the
  handlers did not finish, cancellation is the caller changing its mind, and a caller
  that cannot tell the two apart logs the wrong one.

- **`Held`: deliveries fetched from the broker and waiting at the pause gate.**
  `InFlight` is incremented *after* that gate, so a delivery that arrived while paused
  — decoded, held so that resuming hands over the same message rather than cycling it
  to the back of the queue — was counted by nothing. A drain could therefore answer
  `true` with messages sitting fetched and unhandled.

  **What happens to those messages is unchanged, deliberately.** Nothing is lost
  today: a held delivery was never acknowledged, so it is redelivered here on resume
  or to another instance, and no work on it was half done. Go answers this differently
  — it runs every fetched delivery through a handler, which makes a drain bounded by
  the *prefetch* rather than by the dispatch concurrency, a cost its own lifecycle
  guide calls invisible until a deployment starts timing out. Python nacks the
  unstarted ones with requeue, which is where .NET already ends up, and doing it
  eagerly during a drain would have the broker redeliver into a consumer that is still
  subscribed — a ping-pong for the whole shutdown window, bought for nothing. So the
  defect here is the *report*, and the report is what changed.

  `DrainConsumersAsync` returning `true` means **every handler finished**. It does not
  mean every message the broker sent was handled; `Held` is the rest of that answer,
  and it is in the health report's `connection` details as `held` alongside `inFlight`.

### Documentation

- **`AceMq.Amqp.HealthStatus` collides with
  `Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus`, and the collision has
  a sharp edge inside `AceMq.Amqp.*`.** In a namespace nested under `AceMq.Amqp` the
  enclosing namespace beats a file-level `using HealthStatus = …` alias, so an
  unqualified `HealthStatus` resolves to this library's however the file aliases it.
  Code outside `AceMq.Amqp.*` is unaffected — there the alias wins as written — but
  `acemq-dotnet-amqp-hosting` lives under that prefix and hit it. Noted on the enum
  and in `docs/reliability.md`, with the two-alias form that works and the reminder
  that the two enums are not interchangeable anyway: Up/Degraded/Down against
  Healthy/Degraded/Unhealthy, with a mapping that is a decision rather than a rename.

## [0.6.0] - 2026-09-17

### Added

- **Avro schema resolution is pinned against the shared fixture, both columns of
  it.** `tests/AceMq.Amqp.Tests/fixtures/avro-resolution-fixtures.json` is
  generated by `acemq-java-amqp` and carried byte for byte by every library in the
  family; `../scripts/check-fixtures.sh` fails if a copy drifts. It records one
  rule — resolution happens when the library has a reader schema to resolve onto,
  and not otherwise — two Confluent-framed bodies written by Java, and what each
  decodes to under both behaviours, as `resolved` and `writerShape`.

  This library is listed under `resolved`, because `AvroCodec.Registered(registry,
  schema)` hands that schema in as the reader schema on every message. That is now
  asserted rather than asserted about: a registry answering ids 101 and 102 with
  the fixture's writer schemas, the fixture's own base64 bodies, and the decoded
  record compared field by field and in declaration order. The case that carries
  the weight is the field the writer removed, whose reader schema defaults
  `currency` to `"GBP"` rather than to `""` — a default that is also the type's
  zero value passes whether resolution happened or not, and `"GBP"` can only have
  come from the reader schema.

  The `writerShape` column is asserted too, through `WithoutReaderSchema()`. Those
  are the first tests to point the opt-out at another language's bytes rather than
  at bytes this library wrote a moment earlier, which is the only way the claim it
  makes can actually be checked: a message framed by Java, read with the shape Java
  gave it, spare field and all. Each of the six assertions was run once against the
  other column to confirm it fails there, so a test that agrees with the fixture is
  agreeing with something.

  `docs/serialization.md` gains a **Schema resolution** section carrying the
  cross-language wording the other four libraries also carry, with the rule, the
  five-language table and the pointer at the fixture, and a .NET closing on the
  three calls that decide it. That table was reproduced verbatim from Java and
  arrived carrying Java's stale .NET row — "Always | The codec is constructed with
  a schema", written before the opt-out existed and contradicted by the .NET prose
  directly beneath it. It now reads "By default", and names the two calls that
  leave this library somewhere other than its default. The Go, Java, Python and
  Ruby rows are unchanged.

  The note this entry used to carry for whoever regenerates the fixture has been
  answered. Its entry for this library read "the codec is constructed with a
  schema, so there is always one to resolve onto", and its entry for Java claimed
  Java "is the only one of the five that shows both columns" — both written before
  `WithoutReaderSchema()` landed. Java has regenerated the fixture and this library
  carries the new copy. The correction is prose only: the two cases, their bodies,
  their schema ids and both columns' values are unchanged, so the conformance tests
  pass untouched. This library's `why` now reads "there is always one to resolve
  onto -- unless the caller declines it", names `Registered(registry, schema,
  readerSchema)` and `WithoutReaderSchema()`, and ends "resolved is .NET's default,
  not the only answer it has". The `resolved` column is still this library's and
  still asserted; what changed is that the fixture no longer calls it the only
  answer.

- **The documentation build now fails on a link that does not resolve, anchors
  included.** Nothing checked before, which is how the other libraries in this
  family each published a dead link at least once. Every internal `href` on a
  rendered page must land on a file the site actually contains, and every
  `#fragment` must match an `id` on the page it points at — the second half is the
  one that matters, because a renamed heading leaves a link that still returns 200
  and drops the reader at the top of the page instead of the section they asked
  for. Same-page links are checked the same way; `#section` is the commonest form
  and the first casualty of a rename. Links that climb out of `site/` are refused
  outright rather than resolved against the disk, because `../README.md` is written
  by somebody reading `docs/` on GitHub and the site has no such file. External
  schemes are left alone, and so is the DocFX reference under `apidocs/`: its own
  anchors are that tool's business, though a link from a prose page into it is
  still checked to land somewhere real.

  It lives in `.github/scripts/build-docs-site.sh` rather than in the docs
  workflow, so that a local build says exactly what CI says. Put in the workflow it
  would only ever speak after a push.

  On its first run the rendered site was clean: 23 pages, 730 anchors, nothing
  dangling. The same sweep over the repository's other markdown was not: the
  interop harness in `examples/README.md` was linked as
  `../../acemq-amqp-libraries/scripts/dotnet/interop`, a path into the private
  development workspace that resolves nowhere in a clone and was a 404 for every
  reader it ever had. The sentence now describes the harness without pretending
  there is somewhere to click.

  There is no badge check here, unlike the Ruby library's version of this pass.
  That one exists because YARD renders whatever it finds as `README*` as the API
  reference's index unless argued out of it, and that is how six shields.io images
  reached a published page. DocFX has no equivalent default — `etc/apidocs/docfx.json`
  names its content file by file — and pandoc renders `docs/*.md` and nothing else,
  so no tool here can pick up the README on its own.

- **The documentation build now also checks the links in `docs/*.md` themselves,
  before it renders anything.** The check above reads the rendered site, and the
  build rewrites `page.md` to `page.html` on the way there, so by the time that
  check runs the difference between a link written correctly and one written as
  `.html` has been erased — both arrive as `page.html`, and both pass. It is not a
  gap in how thoroughly the site is checked; it is a question no site-scoped check
  can be asked. Java found this the expensive way, with 75 cross-page links written
  as `.html`: correct on the published site, 404 for everyone reading `docs/` on
  GitHub, and invisible to a full link-and-anchor pass over the rendered output.

  So the source is checked on its own terms. Every `](target)` in `docs/*.md` that
  is not external, `mailto:` or a bare `#fragment` must exist as a file inside
  `docs/`. A `.html` target is reported as "a docs page link belongs in .md" rather
  than as a missing file, because that is what it is and the fix is different.
  `apidocs/` is exempt: DocFX writes that reference as HTML and it is `.html` in
  the repository and on the site alike, so a link into it is right as written.

  It runs first in the script, needing neither pandoc nor docfx — a bad link is
  cheapest to find before a minute of rendering, and the build now stops before
  `site/` is created rather than after. On its first run `docs/` was clean: 23
  source pages, 66 internal links, every one resolving. This library had been
  writing them as `.md` throughout, which is why the site check never had anything
  to hide — but nothing had been enforcing it.

- **Interceptors have a documentation page of their own, `docs/interceptors.md`,
  and the section in `docs/patterns.md` is now a pointer at it.** They were
  described only as one section of a patterns catalogue, which is the wrong shelf
  for a cross-cutting mechanism: somebody adding a tenant header to every message
  is not reading about sagas and routing slips, and Python and Ruby have shipped a
  page for this since their own docs were written. The new page says what each
  side can and cannot change — the publish chain may replace the envelope and
  nothing else, and the consume chain may change nothing at all — where each chain
  sits relative to the codec and the telemetry span, what throwing does in each of
  the six places it can be done, and what happens in a batch publish.

  Three things it documents were true before and written down nowhere. An
  interceptor rebuilding an envelope gets no copy constructor and no `SetHeader`,
  so `Envelope.Of(...)` starts from the builder's defaults and silently drops every
  header and field not carried across by hand; the page gives the full-copy idiom.
  A consume interceptor that throws was not contained — unlike the publish side's
  after-hooks, which are swallowed — so it escaped past the retry ladder to the
  transport and became a plain redelivery that advanced no attempt counter and
  eventually gave up on nothing; the entry under **Changed** below is what closed
  that, and the page describes the behaviour as it now is. And the `Ack` handed to
  `AfterHandle` is what the handler asked for rather than what the consumer did
  with it, so a last attempt that was dead-lettered is reported as a retry; Python's
  `when_settled` and Ruby's `Settlement` close that gap and this library has no
  equivalent, which the page says rather than works around.

- **Every package multi-targets `netstandard2.0;net8.0`.** The `net8.0` target was
  missing for one reason and it was never a good one: the SDK on the machine this
  library was written on had no net8.0 targeting pack, and the README said so in
  place of a decision. The SDK does have it now, so the target is there —
  `AceMq.Amqp` and all eight optional packages, nine `lib/netstandard2.0` and nine
  `lib/net8.0`.

  **`netstandard2.0` is not going anywhere**, and it is not going anywhere from any
  package. It is the target this library exists for: .NET Framework 4.6.2+ as well
  as modern .NET, from one assembly, and the applications most likely to want a
  supported AMQP library are the ones that cannot move. Multi-targeting is addition,
  not migration.

  What the second target buys is what NuGet does with it. A `net8.0` application
  resolving to the `netstandard2.0` asset acquires the compatibility shims that
  target needs — `System.Text.Json`, `System.Diagnostics.DiagnosticSource`,
  `System.Memory` and what they drag behind them — as package references, at an
  application whose runtime already has all of it. That stops. Nothing in the
  library is conditioned on which target a consumer gets: the same source compiles
  twice and the two assemblies do the same thing, which is deliberate and is the
  rule for anything added later.

  **`AceMq.Amqp.Crypto` multi-targets with the rest, and that is not a reversal of
  the decision recorded in its project file.** What was rejected there was two
  *cryptographic code paths* behind one wire format — BouncyCastle on
  `netstandard2.0` and the framework's `AesGcm` on the modern target — a divergence
  that shows up only at runtime, on one target, in somebody else's queue. This
  package still takes AES-GCM from BouncyCastle on both targets. One implementation,
  one set of bytes, identical on .NET Framework 4.6.2 and .NET 10, matching Java,
  Python, Ruby and Go. The objection was to conditioning the cryptography, never to
  building twice.

  The CI check that guards the reach promise had to change with it, and is stronger
  for it. It used to grep each project file for a literal
  `<TargetFramework>netstandard2.0</TargetFramework>`, which a multi-target breaks
  even when both targets are present — and a check that fails on the correct answer
  gets relaxed rather than fixed. It now asserts that
  `bin/Release/netstandard2.0/{package}.dll` was built and that the packed `.nupkg`
  contains both `lib/netstandard2.0` and `lib/net8.0`, for all nine. An output
  directory cannot be fooled by a project file. The workflows also install the .NET
  8 SDK alongside 10, so the net8.0 half of the build depends on the runner rather
  than on a NuGet fetch of the targeting pack.

- **The VB.NET API audit has been carried out, and `tools/vb-audit` now checks
  every rule it was supposed to.** `docs/vbnet.md` lists five constraints the public
  surface must hold to stay callable from VB. The audit checked three of them.
  Rule 4 — no `unsafe`, no pointer types, no C#-only operator tricks — and rule 5 —
  async methods return plain `Task`/`Task(Of T)` — were written down and enforced by
  nothing, which is the exact failure the audit exists to prevent: a rule nobody
  checks is a rule that has already been broken.

  Both are checked now, along with four more things that are a compile error for a
  VB consumer and appeared on no list:

  - **a member colliding by case with an inherited one.** The same collision as
    rule 1, and invisible to a check that looks at one type at a time, because
    neither the derived type nor the base has two names of its own.
  - **an `init`-only setter.** The accessor carries a modreq on `IsExternalInit`,
    which VB has no syntax to satisfy, so the property is read-only to VB and
    settable from C#.
  - **a `required` member.** VB cannot satisfy the compiler's initialization check,
    so the type is not awkward from VB — it is unconstructable.
  - **a default interface member.** VB can neither call nor implement one. The
    library already knows this — it is why `PublishInterceptor` and
    `ConsumeInterceptor` exist as abstract classes — and nothing checked that a
    later interface had not quietly grown one.

  The scan also got wider. It walked methods; it now walks methods, operators,
  **constructors**, properties and fields. Constructors were missed entirely —
  `GetMethods` does not return them — so a `Span<T>` or a pointer in a public
  constructor was invisible to the one check meant to find it.

  And every rule self-checks. The case rule always did, against a deliberately bad
  type, on the principle that a check which never fires is indistinguishable from a
  check that is not running. All twelve do now: the audit runs each rule against a
  type written to trip exactly it and exits non-zero if any rule stays silent. That
  matters more here than usual, because **none of these twelve has ever fired on
  this library's own surface** — without the probes, a clean result would be
  evidence of nothing.

  **The findings: none.** The whole public surface — 141 exported types, 822 public
  methods across all ten shipped assemblies — is clean against all twelve rules,
  including everything added in this release: `SendAllAsync` and the batch
  `PublishFailedException`, `AvroCodec.ReaderSchema`, the three-argument
  `Registered` and `WithoutReaderSchema()`, and the envelope's `Claim`, `WithClaim`
  and `Builder.Claim`. Nothing had to be fixed and nothing had to be deferred as a
  breaking change, which is the outcome the constraint was imposed to produce.

  Both examples were extended to call the new surface rather than only to have it
  audited: the reflection scan proves the shape, and only a compiler proves the
  shape is callable. `examples/vb` and `examples/csharp` both now exercise
  `SendAllAsync`, a failed batch, the envelope claim and all three ways to set the
  Avro reader schema — and both print the same output, which is the claim VB
  support has always rested on.

- **`BatchPublishTests` covers what interceptors do in a pipelined batch.** The
  behaviour was a consequence of the rewrite below and nothing asserted it: the
  chain runs once per message, every `BeforePublish` in the batch completes before
  the first confirm is awaited, and a `BeforePublish` that throws fails that one
  message and leaves the rest of the batch to be published and counted.

### Changed

- **`claim` is a real envelope field now, so a claim set by a Python or Ruby
  publisher reaches a .NET handler.** `AceHeaders.Claim` — `x-acemq-claim` — has
  been in this library since the header names were transliterated from Java, and
  nothing read it and nothing wrote it. That is the worst of both: the name was
  reserved, so `x-acemq-` stripping removed it from the application's headers on
  the way in, and no field materialised it, so it went nowhere instead. A message
  a Python or Ruby service published with a claim on it arrived at a .NET handler
  with the claim gone, silently, with nothing reporting the loss — the same shape
  of bug the replay stamps had.

  It is `Envelope.Claim` now, read by `FromWire`, written by `ToWire` when it is
  set, set by `Envelope.Of(...).Claim(...)`, replaced on a copy by
  `envelope.WithClaim(...)` — which is Ruby's `envelope.with(claim:)` — and carried
  through `WithAttempt` and `WithError`, so a retry and a dead-letter keep it. It is
  **absent rather than empty** when unset, like causation, origin and error: a
  header carrying `""` is one somebody has to write a special case for at the other
  end, and the other four libraries omit it. Nothing in the engine reads it or acts
  on it; it is the application's field, kept in the engine's namespace so that all
  five libraries agree on the spelling and so that it cannot be mistaken for an
  application header.

  **This is not the claim-check pattern and does not change it.** `ClaimCheckCodec`
  frames the body itself — a marker byte, then the key — so a message either is a
  claim check or is not, and no header is involved. That is deliberate and
  untouched. This is an optional field a message may carry alongside a body it
  already has; the two are usable together or separately.

- **Avro schema resolution has a name and an opt-out. `AvroCodec.ReaderSchema`,
  `Registered(registry, schema, readerSchema)` and `WithoutReaderSchema()` are
  new; the default is unchanged, and the reason it is unchanged is worth stating.**
  A registered codec resolved every message onto the schema it was built with, and
  did it unconditionally: the schema went in as the thing the codec *writes*, came
  out as the thing every message is *read as*, and nothing named the second job or
  let a caller decline it. A consumer that wanted to see exactly what a producer
  sent — a bridge, an inspector, something draining a dead-letter queue full of
  versions it was never compiled against — could not ask for it at any price.

  The concept is now called the reader schema, which is what it is called in the
  other four: Java's `registered(registry, readerSchema)`, Python's
  `reader_schema`, Ruby's `reader_schema:` and Go's `ReaderSchema`. It is readable
  as `AvroCodec.ReaderSchema`, and there are three ways to set it.

  ```csharp
  // Resolve onto the schema this codec writes. The default, unchanged.
  AvroCodec.Registered(registry, schemaJson);

  // Resolve onto a different one, for a service that publishes one version and
  // consumes another. Only `schema` is registered. Java's two-argument form.
  AvroCodec.Registered(registry, publishedJson, consumedJson);

  // Do not resolve at all: read every message with the shape its writer gave it.
  // ReaderSchema is null. Java's and Go's behaviour when no reader schema is given.
  AvroCodec.Registered(registry, schemaJson).WithoutReaderSchema();
  ```

  **The default is deliberately not a behaviour change, and the five do not agree
  on it.** Java and Go read with the writer's shape unless a reader schema is
  asked for; Python and Ruby resolve onto the codec's own schema unless
  `reader_schema` says otherwise, which is exactly what this library has always
  done. There is no default .NET can take that makes all five the same, so
  changing it would have traded agreement with two libraries for agreement with
  two others and broken every existing caller on the way past. The naming converges
  — which was the part that could converge — and the capability that was missing is
  there.

  **What a caller must write:** nothing, to keep what they have.
  `AvroCodec.Registered(registry, schema)` resolves onto `schema` exactly as
  before, and `AvroCodec.Of(schema)` is untouched — a fixed codec has no framing
  and so no writer's schema to read with, and `WithoutReaderSchema()` on one throws
  saying so rather than quietly doing nothing. To get the behaviour that was not
  available before, add `.WithoutReaderSchema()`.

  One internal change comes with it: the reflection cache that maps a plain class
  onto a schema is now kept per schema rather than per type. It had to be — with
  resolution off, a type is read against a different schema for every version on
  the queue, and one cache mapped onto one schema would map the class onto fields
  it does not have. Nothing about the default path changes; the cache it uses is
  loaded from the same schema it always was.

- **A consume interceptor that throws from `BeforeHandle` now refuses the message,
  and the message is dead-lettered. This changes what happens to a message.** The
  three consume hooks were called with no `try` at all, so an exception from one
  went straight past the retry ladder to the transport — which has no vocabulary
  for it and turned it into a bare `Ack.Retry`. Nothing about that was a decision:
  the attempt counter did not advance, because a requeue hands back the bytes the
  broker was given; nothing was dead-lettered, because the ladder never saw it;
  no outcome reached `acemq.consume.total` or the span; and the same message came
  back a few seconds later to be refused again, for ever. A queue whose every
  message was being refused looked perfectly idle on every panel.

  A refusal is now a first-class outcome, and it is Go's, down to the wording. The
  handler does not run, interceptors registered after the one that threw do not
  run, and the message is republished to `{queue}.dlq` with

  ```
  an interceptor refused it: tenant "beta" is not served by this process
  ```

  on `x-acemq-error`. It is counted `dead_lettered` on `acemq.consume.total`,
  reaches `acemq.messages.dead.lettered.total`, tags the span `dead_lettered` and
  raises the `acemq.message.dead.lettered` diagnostic event with the exception
  attached. Dead-lettered rather than retried because an interceptor that says no
  to a message will say no to it again, and a ladder would only spend its attempts
  finding that out. An idempotency claim taken before the chain ran is released, so
  a later replay of that dead letter is not mistaken for a duplicate.

  **`AfterHandle` and `OnError` go the other way: a throw from either is swallowed,
  and the disposition the handler asked for stands.** `AfterHandle` is the case
  that had to be decided explicitly, because it is the worse one — by the time it
  runs the handler has finished and whatever it did is done. Rows are written, an
  email is sent, a payment is taken. Turning that into a retry would do all of it
  again, and turning it into a dead-letter would file a message that was handled
  perfectly well; neither is an improvement on a message that has already been
  dealt with. So the delivery settles exactly as it would have, which is the same
  rule the publish side's `AfterConfirm` has always followed. `OnError` is the same
  case one step earlier: it is told the handler failed, and a throw from it must
  not replace the failure the message is actually retried or dead-lettered with.

  Neither is silent. Both report the new `AceMqDiagnostics.InterceptorFailed`
  event — `acemq.interceptor.failed` — at warning level, naming the interceptor and
  which moment threw, with the exception attached. That report is the only record
  the exception leaves, so an interceptor that must not lose an audit record still
  has to make that record durable itself.

  **What a caller must do about it:** an interceptor that was throwing from
  `BeforeHandle` as a way of rejecting a message was, until now, asking for an
  endless redelivery. It now dead-letters that message, which is almost certainly
  what was wanted — but if something downstream was relying on the message coming
  back round, it will not. An interceptor that threw from `AfterHandle` by accident
  was silently causing every message it touched to be handled repeatedly; that
  stops, and the throws appear on `acemq.interceptor.failed` instead.

  The consume span now starts **before** the interceptor chain rather than after
  it, so a refusal has a span to close and to tag. `acemq.consume.duration` is
  unaffected: its clock still starts after the chain has run, so time spent in
  `BeforeHandle` is not counted as time the handler took.

- **The RabbitMQ transport no longer holds its publish lock while it waits for a
  confirm, so batch publishing finally buys the throughput it has always claimed
  to.** `SendAllAsync` was rewritten to pipeline in the entry below, and it did —
  at the library layer, where every message was handed to the transport before any
  answer was awaited. Underneath, it was still one broker round trip at a time.
  The channel was created with both `publisherConfirmationsEnabled` and
  `publisherConfirmationTrackingEnabled` set, which makes RabbitMQ.Client v7's
  `BasicPublishAsync` await the broker's acknowledgement inside the call — and that
  await happened inside `_publishLock`, the semaphore that stops two publishes
  interleaving their frames on one channel. So the lock serialised the round trips,
  not the writes, and a batch of two hundred cost two hundred round trips however
  well the layer above pipelined them. Against a loopback broker the batch took
  307 ms and the same two hundred messages one at a time took 323 ms: the same
  thing, measured.

  The confirms are tracked here now, by publish sequence number, the way
  `RabbitMqConnection` has tracked them in the Java library all along. The
  client's own tracking is switched off; `BasicAcksAsync`, `BasicNacksAsync` and
  `BasicReturnAsync` complete a `TaskCompletionSource` per outstanding publish, a
  `multiple` confirm completes every sequence number it covers rather than only
  the one it names, and a channel that shuts down fails everything still waiting
  with a message saying those publishes may or may not have arrived — which is the
  honest answer and better than waiting for ever for one that cannot come. The
  lock is now held across exactly two things that must happen together: taking the
  sequence number and writing the frame. Taking them apart is what would file two
  publishes' confirms under each other's numbers. **The frame write is still
  serialised** — `IChannel` is not safe for concurrent publishing and the symptom
  of ignoring that is interleaved frames rather than a clean error.

  The same two hundred messages now take **6 ms as a batch against 249 ms one at a
  time**, and `PublishesABatchInFarLessThanARoundTripPerMessage` asserts the ratio
  against a real broker rather than trusting it.

  **`MaxOutstandingPublishes` is the real bound now.** It was a setting the lock
  made irrelevant: with one publish allowed to be unconfirmed at a time, a cap of a
  thousand capped nothing. The transport acquires it before the write, as Java's
  `sendAsync` acquires its `Semaphore` before `basicPublish`, so the number of
  publishes waiting for an answer is bounded deliberately rather than by accident.
  A caller who had tuned it will find it doing what its name says for the first
  time; the default of 1000 is unchanged and nothing needs to be reconfigured.

  Two consequences worth knowing. An unroutable message published with the
  mandatory flag used to arrive as a `PublishException` out of `BasicPublishAsync`
  carrying `IsReturn`; with the client's tracking off, the `basic.return` arrives
  on the return listener instead and is matched back to its publish **by message
  id**, exactly as Java matches it. Every message this library publishes carries
  one, because the envelope's id is always written to the AMQP `messageId`
  property — a caller driving `ITransportConnection.SendAsync` directly with
  `MessageId` left null gets a message reported as routed, which is the same
  limitation Java has. And a publish whose caller cancels is now removed from the
  pending map rather than left in it, so a cancelled batch leaks nothing.

- **`IPublisher<T>.SendAllAsync` publishes the whole batch before it awaits any
  confirm, and a failure is now reported after the batch has finished rather than
  at the first bad answer.** It was a `foreach` that awaited each `SendAsync` in
  turn — a full broker round trip per message, which is the single-send loop a
  caller could have written for themselves and none of the throughput a batch API
  exists for. On a link with 5 ms of latency, a hundred messages cost half a
  second of waiting that nothing needed. Every payload is handed to the transport
  first now and all of the confirms are awaited afterwards, which is what Java's
  `sendAll` has always done; `MaxOutstandingPublishes` still bounds how many may
  be unconfirmed at once, so the back pressure that stops a caller outrunning the
  broker is unchanged.

  **The second half of this is a behaviour change to a shipped API.** The old loop
  threw out of the first failed await, which abandoned the rest of the batch and
  discarded the results already collected — so a caller learned only *that* it had
  failed, not that ninety-eight of a hundred messages were sitting on the broker.
  Resending the batch was then the only safe thing to do, and it duplicated every
  message that had already arrived. Every send is awaited now, whatever an earlier
  one did, and the `PublishFailedException` that follows names the counts, in the
  same words Java uses:

  ```
  2 of 100 messages were not confirmed; 98 were. The first failure was: ...
  ```

  `InnerException` carries the first underlying failure in payload order — the
  `PublishFailedException` the individual send raised — so a caller that has to
  tell a broker rejection from a confirm timeout still can. The results, when the
  batch succeeds, come back in the order the payloads were given, whatever order
  the broker answered in; that part has not changed.

  **What a caller must do about it:** if you relied on the call returning as soon
  as one message failed, it no longer does — it returns once every confirm has
  been answered or timed out, so the worst case is now the confirm timeout rather
  than the first failure. If you caught `PublishFailedException` from
  `SendAllAsync` and resent the batch, read the counts before you do: some of
  those messages are already on the broker, and the exception is the only place
  that says how many. This is not atomic and never was — AMQP has no way to
  publish a hundred messages such that all or none arrive — and the doc comment on
  `SendAllAsync` now says so, along with the ordering and the partial-batch
  reporting it had left unstated.

- **A replay's provenance moved out of the reserved header namespace, which
  changes the bytes on the wire.** `Replay` wrote `x-acemq-replayed-from`,
  `x-acemq-replayed-at` and `x-acemq-replay-count`; it now writes
  `acemq-replayed-from`, `acemq-replayed-at` and `acemq-replay-count`, which is
  what Java, Go, Python and Ruby write. This library was the last of the five on
  the old spelling, and it was the wrong namespace twice over: `x-acemq-` is the
  engine's, and every AceMQ library — this one included — drops a header carrying
  it that the engine does not materialise onto the envelope. So the one question
  the stamps exist to answer, *did this message come back off a dead-letter
  queue?*, could not be asked of the headers a handler was handed, in any
  language. **A message a .NET operator replayed reached a Go, Python, Ruby or
  Java handler with its provenance silently removed** — an audit trail lost
  rather than a message, and nothing reported the loss.

  The three stamps are ordinary application headers now and reach the handler on
  `IMessage<T>.Headers` and `Envelope.Headers` like any other.
  `AceHeaders.SharedPrefix` names the namespace they live in — the one AceMQ
  defines and does not reserve, which `acemq-reply-to` and `acemq-routing-slip`
  were already in and which `AceHeaders.IsAceHeader` deliberately does not match.

  **The old spelling is still read.** `AceHeaders.LegacyReplayedFrom`,
  `LegacyReplayedAt` and `LegacyReplayCount` name it, and a replay takes its
  count from whichever spelling is on the message — so a message a 0.5.0 service
  replayed twice is replayed a third time rather than a first. Nothing writes the
  old names any more, and a replay strips them on the way past so one fact
  travels under one name. Reading either and writing only the new one is the
  shape Java used for `acemq-replayed-at`'s encoding change, and the shape this
  library and Go both use for the encryption framing.

  **Anything matching on the old names — a shovel policy, a dashboard, a
  firehose consumer — needs the new ones.** The shared `envelope-fixtures.json`
  already carries a `replayed` case with the new spellings, and
  `EnvelopeConformanceTests` now asserts against it.

- **`acemq-replayed-at` is written to the second with a `Z`**, for example
  `2026-02-03T04:05:06Z`, rather than .NET's `"o"` round-trip format. Both are
  legitimate RFC 3339 and Java's reader takes either, but `"o"` produced the one
  shape in the family carrying *both* a `+00:00` offset and seven fractional
  digits — and the offset form has already cost Java a widened reader once, since
  `Instant.parse` refuses it on a Java 11 runtime. Go and Ruby write exactly this
  form, Java writes it with an optional fraction, and the shared fixture pins the
  `Z`. Whole seconds lose nothing anybody uses: this is the stamp on an operator
  draining a queue by hand. **Anything parsing that header with a format string
  expecting an offset or a fraction needs to accept this instead.**

### Fixed

- **`Responder.Answered` lagged the reply it was counting, so a caller holding
  its answer could read zero.** The counter was incremented after the reply was
  published, which left a window in which the answer had reached the broker — and
  could already be in the caller's hands — while the responder still reported
  nothing answered. It is incremented before the publish now, which is the only
  ordering a reader can rely on: the count is behind the reply in every possible
  interleaving. A publish that throws takes its increment back, so this still
  counts replies that were sent rather than replies that were attempted, and
  `Answered` never runs ahead of the work. Anything that read the counter
  immediately after a round trip — a test, a dashboard scraped on a fast loop,
  the request/reply example — had to wait before it could trust the number, and
  no longer does. **Java's `Responder` increments after the send and has the same
  window; this side is the one that is right.**

- **A request delivered while a responder was still starting was answered and
  counted by neither `Answered` nor `Unanswerable`.** The handler reached for the
  responder through a local that was only assigned once the subscription had been
  established, so a broker that handed a request over from inside the subscribe —
  which is what a queue with a backlog looks like from in there — got a correct
  reply and no record of it. The counters are created before the subscription
  now, and no delivery can be handled before they are reachable. Narrow, silent,
  and always at start-up, where the first number of the day was the one being
  lost.

- **The documentation gave the wrong release for six changes that shipped in
  0.5.0.** `docs/observability.md` said the `rejected` outcome arrived "from
  0.6.0" and the engine-owned outcome "since 0.4.0"; `docs/request-reply.md` said
  the reply-address duplication arrived "from 0.6.0"; `README.md` said the same
  of `rejected` and dated the hyphenated diagnostic codes to "0.5.x";
  `docs/serialization.md` dated the `XmlCodec` throw to 0.4.0 and titled the
  legacy-crypto section "Bodies written before 0.4.0"; `docs/security.md` linked
  to that title and repeated the version. All of it landed in 0.5.0, and this
  library never released an 0.4.0 at all — so a reader checking whether they had
  the behaviour was told to wait for a version that had already shipped, or to
  look for one that does not exist.

## [0.5.0] - 2026-09-09

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
- **`acemq.retry.rung.missing` is now a metric and not just a name.** The constant
  was in `MetricNames` and the engine raised the `AceMqDiagnostics.RungMissing`
  event, but nothing incremented a counter — so the one sign of a half-declared
  retry topology reached an application that had subscribed a diagnostics callback
  and no dashboard at all. Java, Go, Python and Ruby all count it. It is tagged
  `queue` and `rung`, and the `rung` tag is the queue to declare, which is what
  makes the count actionable rather than merely alarming. The delay is deliberately
  not a tag: a policy names a fixed handful of rungs, where a duration has no bound
  on its values.
- **`MetricNames.TagRung`**, the `rung` tag key, mirroring Java's `TAG_RUNG`. It was
  the one constant Java had that this library did not.

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
  a sample, which is what the old claim needed in order not to rot again. Asserting
  each name was still not enough on its own — `TAG_RUNG` was added to Java, was
  missing here, and every one of those assertions passed — so the test now also does
  a literal set difference in **both** directions between the values Java declares
  and the values this class does. A constant added on either side and not the other
  fails it.

- **A handler's own give-up is reported as `rejected`, not `dead_lettered`.**
  `Ack.DeadLetter` now tags `acemq.consume.total` and its span `rejected`, and
  `dead_lettered` is left to mean what it means in Go, Python and Ruby: the engine
  giving up, when a `RetryPolicy` runs out of attempts. This
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

- **A parked message is reported as `parked`, not `dead_lettered`.** `Ack.Park` and
  a body the codec cannot decode now tag `acemq.consume.total` and their span
  `parked`. `MetricNames.OutcomeParked` existed and was used nowhere, which left
  this library the last of the five to put a payload nothing can read and a
  dependency that stayed down on the same series — and those are different problems
  for different people: a park is a schema or a deploy and will not fix itself,
  where an exhausted policy usually comes back on its own. Sharing one outcome value
  made neither actionable.

  What did **not** change is the total. A park still increments
  `acemq.messages.dead.lettered.total`, now tagged `outcome = parked` alongside the
  engine's give-ups tagged `outcome = dead_lettered`, which is the rule Java states
  in `MicrometerTelemetry.messageParked` and Go, Python and Ruby follow: both are a
  message set aside, and an operator asking "how much is this queue giving up on"
  wants one number that can then be split. `parked` is not an error status on the
  span, for the same reason `rejected` is not — the message was set aside on
  purpose — and a park still records the `message.dead_lettered` span event, so one
  trace query still finds every message a consumer gave up on.

  **This is visible on a dashboard.** `acemq.consume.total` moves parked deliveries
  from `outcome = dead_lettered` to `outcome = parked`; sum the two for the old
  figure. `acemq.messages.dead.lettered.total` keeps the same total and gains an
  `outcome` tag, so anything reading it unfiltered is unaffected.

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
