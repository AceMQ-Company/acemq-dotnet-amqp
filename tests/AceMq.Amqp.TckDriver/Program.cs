// The AceMQ TCK driver for this library.
//
// Started by the TCK runner (github.com/AceMQ-Company/acemq-amqp-tck), which
// writes one JSON request per line to stdin and reads one JSON answer per line
// from stdout. Every operation goes through the public API; nothing here asserts,
// because the runner checks the outcome on the broker. The protocol is
// docs/driver-contract.md in the TCK repository.
//
//     dotnet tests/AceMq.Amqp.TckDriver/bin/Release/net10.0/AceMq.Amqp.TckDriver.dll

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

namespace AceMq.Amqp.TckDriver;

internal static class Program
{
    private static async Task Main()
    {
        // The protocol owns stdout. Anything else that writes to the console, the
        // library included, goes to stderr instead.
        var protocol = new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true };
        Console.SetOut(Console.Error);

        Transports.Register(new RabbitMqTransport());
        var driver = new Driver(
            Environment.GetEnvironmentVariable("ACEMQ_TCK_AMQP_URL")
            ?? "amqp://guest:guest@localhost:5672");

        string? line;
        while ((line = await Console.In.ReadLineAsync().ConfigureAwait(false)) != null)
        {
            if (line.Trim().Length == 0) continue;
            var request = JsonNode.Parse(line)!.AsObject();
            var answer = new JsonObject { ["id"] = request["id"]?.DeepClone() };
            try
            {
                answer["result"] = await driver.RunAsync(request).ConfigureAwait(false);
                answer["ok"] = true;
            }
            catch (Exception failure)
            {
                answer["ok"] = false;
                answer["error"] = new JsonObject
                {
                    ["kind"] = KindOf(failure),
                    ["type"] = failure.GetType().Name,
                    ["message"] = failure.Message,
                };
            }
            await protocol.WriteLineAsync(answer.ToJsonString()).ConfigureAwait(false);
        }
        await driver.EndAsync().ConfigureAwait(false);
    }

    private static string KindOf(Exception failure) => failure switch
    {
        NotSupportedException => "unsupported",
        RequestTimedOutException => "timeout",
        ResponderFailedException => "responder",
        // The JSON codec throws the serializer's own exception for a body it cannot read.
        AceFatalException or JsonException => "decode",
        PublishFailedException { Unroutable: true } => "unroutable",
        PublishFailedException { Nacked: true } => "nacked",
        // Thrown before anything is written: the library declined to send.
        ConnectionBlockedException or PublishingPausedException => "paused",
        _ => "other",
    };
}

/// <summary>What a consumer or stream reader under one handle has seen.</summary>
internal sealed class Tally
{
    private readonly ConcurrentDictionary<string, int> _counts = new();
    private readonly ConcurrentDictionary<string, int> _redelivered = new();
    private readonly ConcurrentQueue<string> _order = new();

    /// <summary>Counts a delivery and returns how many of this id there have been.</summary>
    internal int Count(IMessage<JsonElement> message)
    {
        var id = message.Payload.ValueKind == JsonValueKind.Object
                 && message.Payload.TryGetProperty("id", out var value) ? value.ToString() : "";
        return Count(id, message.Redelivered);
    }

    internal int Count(string id, bool redelivered)
    {
        _order.Enqueue(id);
        if (redelivered) _redelivered.AddOrUpdate(id, 1, (_, n) => n + 1);
        return _counts.AddOrUpdate(id, 1, (_, n) => n + 1);
    }

    internal JsonObject ToJson()
    {
        static JsonObject Of(ConcurrentDictionary<string, int> c) =>
            new(c.Select(p => KeyValuePair.Create(p.Key, (JsonNode?)p.Value)));
        return new JsonObject
        {
            ["counts"] = Of(_counts),
            ["redelivered"] = Of(_redelivered),
            ["order"] = new JsonArray(_order.Select(id => (JsonNode)id).ToArray()),
        };
    }
}

internal sealed class Driver
{
    private static readonly string[] Ops =
    [
        "hello", "begin", "end", "publish", "consume", "handled", "schedule", "request",
        "declareStream", "readStream", "metrics", "replay", "pipeline",
    ];

    private readonly string _url;
    private AceMqConnection? _connection;
    private readonly List<IDisposable> _consumers = new();
    private readonly Dictionary<string, Tally> _tallies = new();
    private Scheduler? _scheduler;
    private readonly Dictionary<(string, string), Requester> _requesters = new();
    private MeterListener? _listener;
    private readonly ConcurrentQueue<(string Name, JsonObject Tags)> _samples = new();

    internal Driver(string url) => _url = url;

    private AceMqConnection Mq => _connection ?? throw new InvalidOperationException("begin was not called");

    internal async Task<JsonNode> RunAsync(JsonObject a)
    {
        switch ((string?)a["op"])
        {
            case "hello": return Hello();
            case "begin": await BeginAsync(a).ConfigureAwait(false); return new JsonObject();
            case "end": await EndAsync().ConfigureAwait(false); return new JsonObject();
            case "publish": return await PublishAsync(a).ConfigureAwait(false);
            case "consume": await ConsumeAsync(a).ConfigureAwait(false); return new JsonObject();
            case "handled": return _tallies[(string)a["handle"]!].ToJson();
            case "schedule": await ScheduleAsync(a).ConfigureAwait(false); return new JsonObject();
            case "request": return await RequestAsync(a).ConfigureAwait(false);
            case "declareStream": await DeclareStreamAsync(a).ConfigureAwait(false); return new JsonObject();
            case "readStream": await ReadStreamAsync(a).ConfigureAwait(false); return new JsonObject();
            case "metrics": return Metrics();
            case "replay": return await ReplayAsync(a).ConfigureAwait(false);
            case "pipeline": await PipelineAsync(a).ConfigureAwait(false); return new JsonObject();
            default: throw new NotSupportedException((string?)a["op"]);
        }
    }

    private static JsonObject Hello()
    {
        var version = typeof(AceMqConnection).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "";
        var plus = version.IndexOf('+', StringComparison.Ordinal);
        return new JsonObject
        {
            ["library"] = "acemq-dotnet-amqp",
            ["version"] = plus < 0 ? version : version[..plus],
            ["ops"] = new JsonArray(Ops.Select(op => (JsonNode)op).ToArray()),
        };
    }

    private async Task BeginAsync(JsonObject a)
    {
        await EndAsync().ConfigureAwait(false);

        // The library's telemetry seam is its System.Diagnostics.Metrics meter.
        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == MetricNames.Meter) listener.EnableMeasurementEvents(instrument);
            },
        };
        _listener.SetMeasurementEventCallback<long>((i, _, tags, _) => Sample(i, tags));
        _listener.SetMeasurementEventCallback<int>((i, _, tags, _) => Sample(i, tags));
        _listener.SetMeasurementEventCallback<double>((i, _, tags, _) => Sample(i, tags));
        _listener.Start();

        var clientName = (string?)a["clientName"] ?? "tck";
        var config = ConnectionConfig.ForUrl(_url)
            .ClientName(clientName)
            .Origin($"{clientName}@{Environment.MachineName}");
        if ((bool?)a["confirms"] == false) config.WithoutPublisherConfirms();
        _connection = await AceMqConnection.ConnectAsync(config.Build()).ConfigureAwait(false);
    }

    private void Sample(Instrument instrument, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var json = new JsonObject();
        foreach (var tag in tags) json[tag.Key] = tag.Value?.ToString();
        _samples.Enqueue((instrument.Name, json));
    }

    internal async Task EndAsync()
    {
        foreach (var requester in _requesters.Values) requester.Dispose();
        _requesters.Clear();
        _scheduler?.Dispose();
        _scheduler = null;
        foreach (var consumer in _consumers) consumer.Dispose();
        _consumers.Clear();
        _tallies.Clear();
        if (_connection != null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            _connection = null;
        }
        _listener?.Dispose();
        _listener = null;
        _samples.Clear();
    }

    private async Task<JsonNode> PublishAsync(JsonObject a)
    {
        var routingKey = (string)a["routingKey"]!;
        // Absent is the library's default, which is mandatory; false is the opt-out.
        var options = (bool?)a["mandatory"] == false
            ? PublishOptions.Defaults().AllowUnroutable()
            : PublishOptions.Defaults();
        var publisher = Mq.Publisher<JsonElement>((string?)a["exchange"] ?? "", routingKey, options);
        var body = Element(a["body"]);
        PublishResult result;
        if (a["envelope"] is JsonObject fields)
        {
            // Envelope.Of needs a type, so an absent one is the publisher's own default.
            var builder = Envelope.Of(
                (string?)fields["type"] ?? (routingKey.Length == 0 ? "message" : routingKey));
            if (fields["id"] is { } id) builder.Id((string)id!);
            if (fields["version"] is { } version) builder.Version((int)version!);
            if (fields["correlationId"] is { } correlation) builder.CorrelationId((string?)correlation);
            if (fields["causationId"] is { } causation) builder.CausationId((string?)causation);
            if (fields["headers"] is JsonObject headers)
            {
                foreach (var (name, value) in headers)
                {
                    builder.Header(name, value is JsonValue v && v.TryGetValue<string>(out var s)
                        ? s
                        : value?.ToJsonString() ?? "");
                }
            }
            result = await publisher.SendAsync(body, builder.Build()).ConfigureAwait(false);
        }
        else
        {
            result = await publisher.SendAsync(body).ConfigureAwait(false);
        }
        return new JsonObject { ["messageId"] = result.MessageId };
    }

    private async Task ConsumeAsync(JsonObject a)
    {
        var queue = (string)a["queue"]!;
        // retry absent is no policy at all: the consumer as the library builds it
        // when the caller configures none.
        var policy = a["retry"] is JsonObject retry
            ? RetryPolicy.Fixed(
                (int?)retry["maxAttempts"] ?? 3,
                TimeSpan.FromMilliseconds((double?)retry["delayMs"] ?? 1000))
            : null;
        var behaviour = a["behaviour"] as JsonObject;
        var failFirst = FailFirst(behaviour);
        var sleep = TimeSpan.FromMilliseconds((double?)behaviour?["sleepMs"] ?? 0);
        var tally = new Tally();

        async Task<Ack> HandleAsync(IMessage<JsonElement> message)
        {
            var seen = tally.Count(message);
            var id = message.Payload.TryGetProperty("id", out var value) ? value.ToString() : "";
            if (sleep > TimeSpan.Zero) await Task.Delay(sleep).ConfigureAwait(false);
            if (seen <= failFirst.GetValueOrDefault(id))
            {
                throw new AceRetryableException($"tck: failing {id} on purpose, delivery {seen}");
            }
            return Ack.Accept();
        }

        var topology = policy == null
            ? Topology.Define().QueueWithDeadLetter(queue)
            : Topology.Define().QueueWithRetry(queue, policy);
        await Mq.ApplyAsync(topology.Build()).ConfigureAwait(false);
        var options = ConsumerOptions.Prefetch((int?)a["prefetch"] ?? 10);
        if (policy != null) options = options.WithRetry(policy);
        // The count is taken in the handler, inside the library's idempotent wrapper.
        if ((bool?)a["idempotent"] == true) options = options.Idempotent(InMemoryIdempotencyStore.ForOneDay());
        _consumers.Add(await Mq.ConsumeAsync<JsonElement>(queue, options, HandleAsync).ConfigureAwait(false));
        _tallies[(string)a["handle"]!] = tally;
    }

    private async Task DeclareStreamAsync(JsonObject a)
    {
        var maxAge = (double?)a["maxAgeMs"] is { } ms ? TimeSpan.FromMilliseconds(ms) : (TimeSpan?)null;
        await Mq.DeclareStreamAsync((string)a["queue"]!, maxAge, null).ConfigureAwait(false);
    }

    private async Task ReadStreamAsync(JsonObject a)
    {
        var reader = Mq.Stream<JsonElement>((string)a["queue"]!);
        reader = (string?)a["offset"] switch
        {
            "first" => reader.FromFirst(),
            "next" or null => reader.FromNext(),
            var other => throw new NotSupportedException($"offset {other}"),
        };
        var failFirst = FailFirst(a["behaviour"] as JsonObject);
        var tally = new Tally();
        _consumers.Add(await reader.ConsumeAsync(message =>
        {
            var seen = tally.Count(message);
            var id = message.Payload.ValueKind == JsonValueKind.Object
                     && message.Payload.TryGetProperty("id", out var value) ? value.ToString() : "";
            if (seen <= failFirst.GetValueOrDefault(id))
            {
                throw new InvalidOperationException($"tck: failing {id} on purpose, delivery {seen}");
            }
            return Task.CompletedTask;
        }).ConfigureAwait(false));
        _tallies[(string)a["handle"]!] = tally;
    }

    // The library declares the exchange, the step queues and their bindings itself.
    private async Task PipelineAsync(JsonObject a)
    {
        var tally = new Tally();
        _tallies[(string)a["handle"]!] = tally;
        var builder = Mq.Pipeline<JsonNode>((string)a["name"]!);
        // behaviour names a step: that step's own handler fails, as consume's does. No
        // retry policy or delay is set anywhere: the pipeline as the library builds it.
        var behaviours = a["behaviour"] as JsonObject;
        foreach (var step in a["steps"]!.AsArray())
        {
            var behaviour = behaviours?[(string)step!] as JsonObject;
            var failFirst = FailFirst(behaviour);
            var sleep = TimeSpan.FromMilliseconds((double?)behaviour?["sleepMs"] ?? 0);
            var seenHere = new ConcurrentDictionary<string, int>();
            builder = builder.Step<JsonNode>((string)step!, async node =>
            {
                var id = node is JsonObject o && o["id"] is JsonValue v && v.TryGetValue<string>(out var s) ? s : "";
                tally.Count(id, false);
                var seen = seenHere.AddOrUpdate(id, 1, (_, n) => n + 1);
                if (sleep > TimeSpan.Zero) await Task.Delay(sleep).ConfigureAwait(false);
                if (seen <= failFirst.GetValueOrDefault(id))
                {
                    throw new AceRetryableException($"tck: failing {id} on purpose, delivery {seen}");
                }
                return node;
            });
        }
        _consumers.Add(await builder.BuildAsync().ConfigureAwait(false));
    }

    private static Dictionary<string, int> FailFirst(JsonObject? behaviour) =>
        (behaviour?["failFirst"] as JsonObject)?
            .ToDictionary(p => p.Key, p => (int)p.Value!) ?? new Dictionary<string, int>();

    private JsonObject Metrics()
    {
        var samples = _samples.ToArray();
        return new JsonObject
        {
            ["names"] = new JsonArray(samples.Select(s => s.Name).Distinct().Select(n => (JsonNode)n).ToArray()),
            ["samples"] = new JsonArray(samples
                .Select(s => (JsonNode)new JsonObject { ["name"] = s.Name, ["tags"] = s.Tags.DeepClone() })
                .ToArray()),
        };
    }

    private async Task ScheduleAsync(JsonObject a)
    {
        _scheduler ??= await Scheduler.OnAsync(Mq).ConfigureAwait(false);
        await _scheduler.InAsync(
                TimeSpan.FromMilliseconds((double)a["delayMs"]!),
                (string?)a["exchange"] ?? "",
                (string)a["routingKey"]!,
                Element(a["body"]))
            .ConfigureAwait(false);
    }

    private async Task<JsonNode> RequestAsync(JsonObject a)
    {
        var key = ((string?)a["exchange"] ?? "", (string)a["routingKey"]!);
        if (!_requesters.TryGetValue(key, out var requester))
        {
            requester = await Mq.RequesterAsync().ConfigureAwait(false);
            _requesters[key] = requester;
        }
        var reply = await requester.RequestAsync<JsonElement, JsonElement>(
                key.Item1, key.Item2, Element(a["body"]),
                TimeSpan.FromMilliseconds((double)a["timeoutMs"]!), CancellationToken.None)
            .ConfigureAwait(false);
        return new JsonObject { ["reply"] = JsonNode.Parse(reply.GetRawText()) };
    }

    // One call, answered the moment it returns: the case reads the source queue next.
    private async Task<JsonNode> ReplayAsync(JsonObject a)
    {
        var ids = a["ids"]!.AsArray().Select(id => (string)id!).ToHashSet();
        var replayed = await Mq.Replay((string)a["from"]!)
            .ReplayAsync(int.MaxValue, delivery =>
            {
                try
                {
                    using var body = JsonDocument.Parse(delivery.Body);
                    return body.RootElement.ValueKind == JsonValueKind.Object
                           && body.RootElement.TryGetProperty("id", out var id)
                           && id.ValueKind == JsonValueKind.String
                           && ids.Contains(id.GetString()!);
                }
                catch (JsonException)
                {
                    return false;
                }
            })
            .ConfigureAwait(false);
        return new JsonObject { ["replayed"] = replayed };
    }

    private static JsonElement Element(JsonNode? node) =>
        JsonSerializer.SerializeToElement(node);
}
