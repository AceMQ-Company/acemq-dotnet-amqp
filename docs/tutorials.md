# Tutorials

Step by step, in order, each one ending with something that runs.

The [guide](index.md) explains how a thing works and why it is that way. These are
the other shape: start with nothing, finish with a working service, and understand
what you typed by the end rather than before the beginning.

| | | |
|---|---|---|
| 1 | [Your first message](tutorial-first-message.md) | Connect, declare, publish, consume. No broker needed | 10 min |
| 2 | [Surviving failure](tutorial-surviving-failure.md) | Retries that do not block, dead letters, and replaying them | 20 min |
| 3 | [Never processing twice](tutorial-exactly-once.md) | Idempotency, the outbox, and why "exactly once" is a lie | 25 min |
| 4 | [Seeing what happens](tutorial-observability.md) | Metrics and traces, and reading them when something is wrong | 20 min |

Tutorial 1 needs nothing but the .NET SDK — it runs against the in-process broker.
The rest use Docker for a real RabbitMQ, because what they teach is about how a
broker behaves and an in-memory one would be teaching you a simplification.

## Before you start

```bash
dotnet new console -n Orders && cd Orders
```

```xml
<!-- nuget.config, beside the project -->
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="acemq" value="https://acemq.org/nuget/index.json" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
```

```bash
dotnet add package AceMq.Amqp
dotnet add package AceMq.Amqp.RabbitMq
```

Everything here is C#. It works identically from VB.NET — the same assembly, the
same types, `(Of T)` where C# writes `<T>`. See [VB.NET](vbnet.md).
