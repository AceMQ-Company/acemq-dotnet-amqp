# 4. Seeing what happens

**20 minutes. Needs Docker.**

A message that fails silently is the whole problem. This is how to see the traffic,
and how to read it when something is wrong.

## The metrics already exist

Nothing has to be turned on. The library instruments itself with `Meter` and
`ActivitySource` — the runtime's own APIs — and an instrument nobody listens to costs
a null check.

The question is how to get them out. If your service is ASP.NET Core, use the
OpenTelemetry SDK. If it is a worker or a console application — or anything on .NET
Framework — the actuator is the answer, because the OpenTelemetry Prometheus exporter
for ASP.NET Core is still beta and needs .NET 8.

```bash
dotnet add package AceMq.Amqp.Diagnostics
```

```csharp
using AceMq.Amqp.Diagnostics;

using var actuator = AceMqActuator.Start(mq);
Console.WriteLine($"metrics on {actuator.Url}acemq-metrics");
```

## Look at it

Publish a few messages, dead-letter some, then:

```bash
curl -s localhost:9464/acemq-metrics | grep -E '^acemq_(publish|consume)_total'
```

```
acemq_publish_total{exchange="",message_type="orders.placed",outcome="confirmed",routing_key="orders.placed"} 20
acemq_consume_total{message_type="orders.placed",outcome="acked",queue="orders.placed"} 16
acemq_consume_total{message_type="orders.placed",outcome="dead_lettered",queue="orders.placed"} 4
```

Twenty published, sixteen handled, four given up on. The `outcome` label is what
makes that readable — and it is worth alerting on the difference between
`unroutable` and `failed`, because one is a binding that was never declared and the
other is the broker or the network.

The names are dotted in code and underscored when scraped: `acemq.publish.duration`
becomes `acemq_publish_duration_seconds`. **They are identical to the Java
library's**, so a dashboard written for one works against the other.

## Health, for a probe

```bash
curl -s -o /dev/null -w '%{http_code}\n' localhost:9464/acemq-health   # 200
curl -s localhost:9464/acemq-health
```

```json
{"status":"UP","inFlight":0,"components":{"connection":{"status":"UP","open":"true","blocked":"false","transport":"rabbitmq","inFlight":"0"}}}
```

Stop the broker and ask again: `503`, and `"status":"DOWN"`. A Kubernetes probe reads
the status code without parsing anything.

Ordered queues register themselves here, so a halted partition shows up — which
matters, because a halted partition is a consumer that stopped without the connection
or the process noticing. It reports **DEGRADED and still answers 200**: the other
partitions work, and a process that reports itself down gets restarted, which loses
the held message and fixes nothing.

## Scrape it

```yaml
scrape_configs:
  - job_name: acemq
    metrics_path: /acemq-metrics
    static_configs:
      - targets: ['localhost:9464']
```

The paths are namespaced so they cannot collide with your application's own
`/metrics`. Change them if you like — but change the scrape config in the same
commit, because Prometheus reports a wrong path as the target being **down**, not as
misconfigured.

## Follow one message across services

```csharp
using var listener = new ActivityListener
{
    ShouldListenTo = s => s.Name == MetricNames.ActivitySource,
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
    ActivityStopped = a => Console.WriteLine($"{a.TraceId} {a.DisplayName} {a.Duration.TotalMilliseconds:0.0}ms"),
};
ActivitySource.AddActivityListener(listener);
```

```
4bf92f3577b34da6 orders.placed publish 8.2ms
4bf92f3577b34da6 orders.placed process 41.7ms
```

**The same trace id on both.** The publish wrote `traceparent` into the envelope and
the consumer picked it up — so a trace crosses the broker rather than stopping at it.
It also crosses languages: a C# consumer of a Java publisher's message joins the same
trace, because both libraries read the same two headers.

In production, hand the source to your SDK instead:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(MetricNames.ActivitySource))
    .WithMetrics(m => m.AddMeter(MetricNames.Meter));
```

## Reading it when something is wrong

| What you see | What it usually means |
|---|---|
| `publish_total{outcome="unroutable"}` climbing | a binding was never declared, or a routing key has a typo |
| `consume_attempts` bucket above `le="1"` filling | a dependency is flapping; look at the retry reasons |
| `dead_lettered_total` climbing | messages are being given up on — check the dead-letter queue |
| `consume_in_flight` at the prefetch and flat | handlers are stuck, not slow |
| `/acemq-health` DEGRADED | an ordered partition halted; something needs a `Resume` |

## What you have

Metrics a dashboard can read, health a probe can read, and traces that cross the
broker. That is the set — the [guide](index.md) has the detail on each.
