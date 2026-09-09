# Metrics and tracing

The library instruments itself with `System.Diagnostics.Metrics.Meter` and
`System.Diagnostics.ActivitySource` — the runtime's own APIs — and takes **no
OpenTelemetry or Prometheus dependency**. Your application owns its exporter and its
SDK version; a library that pins those causes exactly the conflicts that make it
painful to adopt.

Instrumentation is always on. An instrument nobody listens to costs a null check per
call, so there is no switch to forget in production and no configuration that makes
the metrics disappear.

## What it records

Every name is identical to the Java library's, character for character. A dashboard,
an alert or a recording rule written for one works unchanged against the other, and a
service rewritten from Java to C# keeps its observability.

| Metric | Type | Tags |
|---|---|---|
| `acemq.publish.duration` | histogram, seconds | exchange, routing.key, message.type, outcome |
| `acemq.publish.total` | counter | exchange, routing.key, message.type, outcome |
| `acemq.consume.duration` | histogram, seconds | queue, message.type, outcome |
| `acemq.consume.total` | counter | queue, message.type, outcome |
| `acemq.consume.attempts` | histogram, attempts | queue, message.type, outcome |
| `acemq.consume.in.flight` | gauge | — |
| `acemq.messages.retried.total` | counter | queue, message.type |
| `acemq.messages.dead.lettered.total` | counter | queue, message.type |
| `acemq.request.duration` | histogram, seconds | routing.key, message.type, outcome |
| `acemq.request.total` | counter | routing.key, message.type, outcome |
| `acemq.outbox.lag` | histogram, seconds | exchange, routing.key |
| `acemq.outbox.total` | counter | exchange, routing.key, outcome |
| `acemq.pipeline.run.duration` | histogram, seconds | pipeline, step, outcome |
| `acemq.pipeline.run.total` | counter | pipeline, step, outcome |

Publish outcomes are `confirmed`, `unroutable` and `failed`. The distinction is worth
alerting on separately: **unroutable is a topology mistake**, `failed` is the broker
or the network, and treating them as one hides the difference between a bad binding
and an outage. Up to 0.3.0 a broker's nack was tagged `rejected` here — a value no
other library writes, and one that belongs to a delivery — so a panel filtering
publishes by outcome dropped every refusal.

Consume outcomes are `acked`, `retried`, `rejected` and `dead_lettered`. **`rejected`
is a handler's own decision — `Ack.DeadLetter`, or a release — and `dead_lettered` is
the engine giving up when a `RetryPolicy` runs out of attempts.** Both messages end in
the same dead-letter queue; only the word keeps them apart, and the difference is the
only question the two counts are ever asked: an unprocessable message is a producer
problem, an exhausted policy is usually a dependency that is down. Request
outcomes are `answered`, `timed_out` and `failed`. An outbox record is `published` or
`failed`. A pipeline run is `completed` or `ended_early`.

`acemq.outbox.lag` is the one number that reveals a stopped relay. A committed,
unpublished row is a message that exists, is owed to somebody, and **appears in no
queue depth anywhere** — nothing else in this list can see it. It is measured from
when the row was committed, not from when the relay claimed it: the claim is part of
the answer to "how long has somebody been owed this", not the start of it.

`acemq.request.duration` exists because neither of the spans that already covered a
request/reply call was the thing the caller waited for. The publish is timed and the
reply's delivery is timed; the round trip was the gap between them, and a gap is not
a measurement.

**The outcome is the engine's, not the handler's.** A handler that throws is asking
for a retry, but on the last attempt a `RetryPolicy` allows, the engine dead-letters
the message instead. Up to 0.3.0 the outcome was tagged from the handler's answer
before the engine had decided, so a message that ran out of attempts was counted as
`retried` and its span said `retried` — and `acemq.messages.dead.lettered.total` only
ever saw the dead-letters a handler asked for by name. Since 0.4.0 both the counter
and the span carry `dead_lettered`, which means a dashboard built on 0.3.0 numbers
will show retries falling and dead-letters rising without anything changing in your
service.

**A handler's own give-up is `rejected`, not `dead_lettered`.** Up to 0.5.x this
library and Java reported `Ack.DeadLetter` as `dead_lettered`, while Go, Python and
Ruby reported it as `rejected` and kept `dead_lettered` for the engine exhausting a
policy. Three against two, and the three were right: a decision and an exhaustion are
different events. From 0.6.0 the counter and the span both read `rejected` for a
handler's decision, and `acemq.messages.dead.lettered.total` counts the engine's
give-ups and parked messages only. **A dashboard or an alert filtering
`acemq.consume.total` by `outcome = dead_lettered`, or reading
`acemq.messages.dead.lettered.total`, will show a drop that is not a change in your
service** — the same deliveries are now under `outcome = rejected`. Add the two
together to get the old number.

Names are dotted here and underscored when scraped: `acemq.publish.duration` becomes
`acemq_publish_duration_seconds`, and `routing.key` becomes `routing_key`. That
translation is the exporter's, and Java's exporters do the same.

## Tracing

A publish starts a span named `<destination> publish` and writes the W3C trace
context into `traceparent`, which the envelope already reserves. A consumer continues
it as `<queue> process`. A blocking request opens `<destination> request` around the
whole round trip, with the publish and the reply as its children.

That means a trace crosses the broker — and crosses languages, because the Java
library reads and writes the same two headers. A C# service consuming a message a
Java service published is one trace, not two.

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(MetricNames.ActivitySource))
    .WithMetrics(m => m.AddMeter(MetricNames.Meter));
```

### Span attributes

OpenTelemetry's messaging semantic conventions, plus `messaging.acemq.*` for the
things those conventions have no name for. These are the **exact keys the Java, Go,
Python and Ruby libraries write**, so one trace query works across a polyglot estate.

| Attribute | On | Meaning |
|---|---|---|
| `messaging.system` | every span | `rabbitmq` |
| `messaging.destination.name` | every span | exchange published to, or queue consumed from |
| `messaging.operation` | every span | `publish`, `process` or `request` |
| `messaging.message.id` | every span | the envelope id |
| `messaging.message.conversation_id` | every span | the correlation id |
| `messaging.rabbitmq.destination.routing_key` | publish | the routing key used |
| `messaging.acemq.message_type` | every span | the envelope's logical type |
| `messaging.acemq.attempt` | process | which attempt this delivery was |
| `messaging.acemq.outcome` | every span | the same value the counter carries |

Up to 0.3.0 this library put its *metric* tag keys on spans instead — `queue`,
`outcome`, `acemq.attempt` — and set `messaging.system` to `acemq`. A trace query
written against any of the other four libraries matched none of its spans. The keys
are constants on `AceMqTelemetry` (`AttrOutcome`, `AttrAttempt`, and so on).

**The outcome on the span is the outcome on the counter**, always, for the same
delivery. They are written from one value in one place, and `TelemetryTests` asserts
that a message which exhausts its attempts has them agreeing.

### Span events

| Event | Attributes |
|---|---|
| `message.retried` | `messaging.destination.name`, `messaging.acemq.retry_delay_ms`, `messaging.acemq.reason` |
| `message.dead_lettered` | `messaging.destination.name`, `messaging.acemq.reason` |
| `outbox.publish_failed` | `messaging.destination.name`, `messaging.acemq.reason` |
| `pipeline.run_finished` | `pipeline`, `step`, `outcome`, `messaging.acemq.run_age_ms` |

Java records these four under exactly these names, and Go, Python and Ruby copy them.
Up to 0.3.0 the first two were `acemq.message.retried` and
`acemq.message.dead.lettered` here, which matched nothing anywhere else.

The three keys on `pipeline.run_finished` are bare rather than namespaced, and that
is deliberate on all five libraries: `outcome` there is the *run's* outcome, a
different thing from the span's own `messaging.acemq.outcome`, which still reads
`acked`.

`messaging.acemq.retry_delay_ms` is the delay the engine **actually chose** — the
policy's, not the handler's suggestion — so a trace shows the wait that happened.
Where the message went is on `messaging.destination.name`: the source queue for a
wait held in the consumer, a rung queue for a wait held in the broker, `{queue}.dlq`
or `{queue}.parked` for a message given up on.

None of this is allocated when nothing is listening. No listener means no `Activity`
was ever created, so emitting an event is a null check and a return.

## Getting it to Prometheus

Three routes, and which one you want depends on what you are running.

### An ASP.NET Core application

Use the OpenTelemetry SDK and let it own the exporter:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter(MetricNames.Meter).AddPrometheusExporter());

app.MapPrometheusScrapingEndpoint();
```

**`OpenTelemetry.Exporter.Prometheus.AspNetCore` is still a beta package**, and it
requires .NET 8 or newer. Worth knowing before it goes into a production dependency
list.

### Anything else — the actuator

Worker services, console applications and anything on .NET Framework have no HTTP
server to hang an endpoint on, and cannot use the ASP.NET Core exporter at all.
`AceMq.Amqp.Diagnostics` is a small actuator for exactly that case, in the spirit of
the one the Spring Boot starter provides on the JVM side:

```bash
dotnet add package AceMq.Amqp.Diagnostics
```

```csharp
using var actuator = AceMqActuator.Start(mq);
```

| Path | |
|---|---|
| `/acemq-metrics` | Prometheus text format, scraped directly |
| `/acemq-health` | 200 or **503**, with connection state and in-flight count |
| `/acemq-info` | library version, transport, broker capabilities |

Namespaced so they cannot collide with an application's own `/metrics` or `/health`.
Port 9464 by default, the OpenTelemetry Prometheus convention.

`/acemq-health` answers **503 when the connection is closed or the broker has blocked
it**, so a Kubernetes probe or a load balancer reads the status code without parsing
the body.

No ASP.NET Core, no OpenTelemetry, no beta package — it reads the meter through
`MeterListener`, which is part of the runtime.

```yaml
scrape_configs:
  - job_name: acemq
    metrics_path: /acemq-metrics
    static_configs:
      - targets: ['localhost:9464']
```

### Already running a collector

The OTLP exporter is **stable** and targets everything the library does, including
.NET Framework. If you have a collector, this is the least surprising route:

```csharp
.WithMetrics(m => m.AddMeter(MetricNames.Meter).AddOtlpExporter());
```

## Changing the paths and the port

```csharp
using var actuator = AceMqActuator.Start(mq, new ActuatorOptions
{
    Port = 9464,
    MetricsPath = "/metrics",
    HealthPath = "/healthz",
});
```

Overriding a path removes the default — `/acemq-metrics` returns 404 afterwards. A
Prometheus job left pointing at the old path reports the target as **down**, not as
misconfigured, so change the scrape config in the same commit.

## The endpoints are not authenticated

They report queue names, broker state and traffic rates. The actuator binds to
**localhost** by default for that reason.

Setting `Host` to `+` or `0.0.0.0` publishes that to anything that can reach the
port. Prefer letting the scraper reach it over loopback, through a sidecar, or behind
a network policy — and if it genuinely must be reachable from elsewhere, put
something in front of it that authenticates.

On Windows, binding to anything other than loopback also needs a URL reservation
(`netsh http add urlacl`); the actuator says so rather than failing obscurely.
