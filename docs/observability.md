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

Publish outcomes are `confirmed`, `unroutable`, `rejected` and `failed`. The
distinction is worth alerting on separately: **unroutable is a topology mistake**,
`failed` is the broker or the network, and treating them as one hides the difference
between a bad binding and an outage.

Consume outcomes are `acked`, `retried`, `dead_lettered` and `rejected`.

Names are dotted here and underscored when scraped: `acemq.publish.duration` becomes
`acemq_publish_duration_seconds`, and `routing.key` becomes `routing_key`. That
translation is the exporter's, and Java's exporters do the same.

## Tracing

A publish starts a span named `<destination> publish` and writes the W3C trace
context into `traceparent`, which the envelope already reserves. A consumer continues
it as `<queue> process`.

That means a trace crosses the broker — and crosses languages, because the Java
library reads and writes the same two headers. A C# service consuming a message a
Java service published is one trace, not two.

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(MetricNames.ActivitySource))
    .WithMetrics(m => m.AddMeter(MetricNames.Meter));
```

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
