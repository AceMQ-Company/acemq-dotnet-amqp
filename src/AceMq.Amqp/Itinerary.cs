// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace AceMq.Amqp;

/// <summary>Where a message goes next: an exchange and a routing key.</summary>
/// <remarks>
/// A <see cref="RoutingSlip"/> names its steps and publishes each to a queue on the
/// default exchange; an <see cref="Itinerary"/> carries the exchange and routing key
/// for each stop. This is the shape both reduce to, so the code that moves a message
/// along does not have to know which of the two it is following.
/// </remarks>
public sealed class RouteDestination
{
    public RouteDestination(string exchange, string routingKey)
        : this(exchange, routingKey, null) { }

    public RouteDestination(string exchange, string routingKey, string? label)
    {
        Exchange = exchange ?? throw new ArgumentNullException(nameof(exchange));
        RoutingKey = routingKey ?? throw new ArgumentNullException(nameof(routingKey));
        Label = string.IsNullOrEmpty(label)
            ? (Exchange.Length == 0 ? RoutingKey : Exchange + "/" + RoutingKey)
            : label!;
    }

    /// <summary>Empty for the default exchange, which is where a queue name goes.</summary>
    public string Exchange { get; }

    public string RoutingKey { get; }

    /// <summary>What to call this stop in a log, which is the step's name when it has one.</summary>
    public string Label { get; }

    public override string ToString() => Label;
}

/// <summary>
/// A route a message carries with it, in whichever of the two forms it was written.
/// </summary>
/// <remarks>
/// <para>
/// AceMQ has two wire forms for a routing slip and this library reads and writes both,
/// because the five libraries did not agree on one and a message written by any of
/// them has to be followable here:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <c>x-acemq-route</c> — the step names, comma-joined, with a position and a run
///     identifier beside them. Java's form, and <see cref="RoutingSlip"/> here. The
///     names are resolved against a declared route: a <see cref="Pipeline{T}"/> knows
///     which queue each name belongs to.
///   </description></item>
///   <item><description>
///     <c>acemq-routing-slip</c> — a JSON itinerary carrying the exchange and routing
///     key of every stop, and the ones already done. Go, Python and Ruby's form, and
///     <see cref="Itinerary"/> here. Nothing has to be declared anywhere for it to be
///     followed, because the message says where it is going.
///   </description></item>
/// </list>
/// <para>
/// Read either with <see cref="Route.From"/>. Which one gets written is a choice —
/// see <see cref="SlipForm"/>.
/// </para>
/// </remarks>
public interface IRoute
{
    /// <summary>Whether every stop has been visited.</summary>
    bool IsFinished { get; }

    /// <summary>Where the message this route is on should be delivered, or null at the end.</summary>
    RouteDestination? Destination { get; }

    /// <summary>The route with the current stop behind it.</summary>
    IRoute Advance();

    /// <summary>The headers this route travels in.</summary>
    IReadOnlyDictionary<string, object> ToHeaders();
}

/// <summary>Which of the two wire forms to write a route in.</summary>
public enum SlipForm
{
    /// <summary>
    /// <c>x-acemq-route</c>: step names, comma-joined, against a declared route.
    /// </summary>
    /// <remarks>
    /// The default for a <see cref="Pipeline{T}"/>, and what Java writes. A pipeline
    /// has already declared its steps and their queues, so an itinerary would repeat
    /// that declaration on every message to say nothing new — and the shorter form
    /// stays readable in a management console, where <c>validate,enrich,dispatch</c>
    /// at position 1 says where a message is without anybody decoding anything.
    /// </remarks>
    Declared,

    /// <summary>
    /// <c>acemq-routing-slip</c>: a JSON itinerary carrying each stop's address.
    /// </summary>
    /// <remarks>
    /// What Go, Python and Ruby write. Worth choosing when the consumers are in those
    /// languages, or when a stop is somewhere this library has not declared — the
    /// message carries its own addresses, so nothing has to be resolved against
    /// anything.
    /// </remarks>
    Itinerary,
}

/// <summary>Reads whichever routing slip a message is carrying.</summary>
public static class Route
{
    /// <summary>
    /// The route on these headers, or null when they carry none.
    /// </summary>
    /// <remarks>
    /// <c>acemq-routing-slip</c> is looked for first. A message with both is
    /// unusual and the itinerary is the one to believe: it carries the addresses
    /// rather than names that have to be resolved somewhere, so following it needs
    /// nothing that could be missing.
    /// </remarks>
    public static IRoute? From(IReadOnlyDictionary<string, object> headers)
    {
        if (headers == null) throw new ArgumentNullException(nameof(headers));
        return (IRoute?)Itinerary.From(headers) ?? RoutingSlip.From(headers);
    }

    /// <summary>The route a message is carrying, or null when it carries none.</summary>
    public static IRoute? Of<T>(IMessage<T> message) =>
        message == null ? null : From(message.WireHeaders);
}

/// <summary>One stop on an <see cref="Itinerary"/>.</summary>
public sealed class ItineraryStep
{
    public ItineraryStep(string exchange, string routingKey)
        : this(exchange, routingKey, null, null) { }

    public ItineraryStep(string exchange, string routingKey, string? name)
        : this(exchange, routingKey, name, null) { }

    public ItineraryStep(string exchange, string routingKey, string? name, string? completedAt)
    {
        Exchange = exchange ?? throw new ArgumentNullException(nameof(exchange));
        RoutingKey = routingKey ?? throw new ArgumentNullException(nameof(routingKey));
        Name = name ?? string.Empty;
        CompletedAt = completedAt ?? string.Empty;
    }

    /// <summary>Where this step's message goes; empty for the default exchange.</summary>
    public string Exchange { get; }

    /// <summary>What it is published under, or a queue name on the default exchange.</summary>
    public string RoutingKey { get; }

    /// <summary>What to call this step when a slip is read in a log. May be empty.</summary>
    public string Name { get; }

    /// <summary>When it finished, filled in as the slip advances. Empty until then.</summary>
    public string CompletedAt { get; }

    internal ItineraryStep CompletedNow() =>
        new ItineraryStep(Exchange, RoutingKey, Name, Timestamp());

    internal RouteDestination Destination() =>
        new RouteDestination(Exchange, RoutingKey, Name.Length == 0 ? null : Name);

    /// <summary>
    /// RFC 3339 in UTC, to the second.
    /// </summary>
    /// <remarks>
    /// The same rendering Ruby writes, and one Go parses as RFC 3339 without
    /// complaint. The three libraries that write this field do not agree on its
    /// precision — Python writes microseconds and an offset — so this is a choice
    /// rather than a copy, and the choice is the one every reader accepts.
    /// </remarks>
    private static string Timestamp() =>
        DateTimeOffset.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    public override string ToString() =>
        Name.Length == 0 ? Exchange + "/" + RoutingKey : Name;
}

/// <summary>
/// An itinerary a message carries with it, in the form Go, Python and Ruby write.
/// </summary>
/// <remarks>
/// <para>
/// The alternative to a central orchestrator. Each service does its part and sends
/// the message to the next stop written on the slip, so the route is decided once —
/// by whoever started the work — and travels with the message rather than living in a
/// component every service has to be able to reach.
/// </para>
/// <code>
/// var slip = new Itinerary()
///     .Then("orders-events", "order.validate", "validate")
///     .Then("orders-events", "order.charge", "charge")
///     .Then("orders-events", "order.ship", "ship");
///
/// await mq.SendAlongAsync(slip, order);
/// </code>
/// <para>
/// It travels as JSON in <c>acemq-routing-slip</c>, an ordinary application header
/// rather than a reserved one, so it survives every hop and is readable by anything
/// that can read JSON. The field names inside are <c>routingKey</c> and
/// <c>completedAt</c> rather than anything more idiomatic here, because the wire is
/// where five libraries have to agree and three of them defined this one.
/// </para>
/// <para>
/// Immutable: <see cref="Advance"/> returns a new itinerary. By the time anybody
/// reads a slip it is on a message that has already been published, and one that
/// changed under a handler would describe a journey that did not happen.
/// </para>
/// </remarks>
public sealed class Itinerary : IRoute
{
    /// <summary>The header the itinerary travels in.</summary>
    /// <remarks>
    /// No <see cref="AceHeaders.Prefix"/>, deliberately. The reserved namespace is
    /// stripped from an application's view of its headers, and this name is the one
    /// Go, Python and Ruby publish and read — changing it here would make a slip
    /// written by any of them invisible.
    /// </remarks>
    public const string Header = "acemq-routing-slip";

    private static readonly ItineraryStep[] None = new ItineraryStep[0];

    public Itinerary() : this(None, None) { }

    public Itinerary(IReadOnlyList<ItineraryStep> steps, IReadOnlyList<ItineraryStep> done)
    {
        Steps = (steps ?? throw new ArgumentNullException(nameof(steps))).ToArray();
        Done = (done ?? throw new ArgumentNullException(nameof(done))).ToArray();
    }

    /// <summary>What is still to do, in order.</summary>
    public IReadOnlyList<ItineraryStep> Steps { get; }

    /// <summary>
    /// What has already happened, oldest first.
    /// </summary>
    /// <remarks>
    /// Carried rather than dropped, so a slip that fails half way says how far it
    /// got. Whoever finds the message in a dead-letter queue is asking exactly that.
    /// </remarks>
    public IReadOnlyList<ItineraryStep> Done { get; }

    /// <summary>An itinerary with one more stop on the end.</summary>
    public Itinerary Then(string exchange, string routingKey) =>
        Then(exchange, routingKey, null);

    public Itinerary Then(string exchange, string routingKey, string? name) =>
        new Itinerary(
            Steps.Concat(new[] { new ItineraryStep(exchange, routingKey, name) }).ToArray(),
            Done);

    /// <summary>The stop this message is going to, or null at the end of the route.</summary>
    public ItineraryStep? Next => Steps.Count > 0 ? Steps[0] : null;

    public bool IsFinished => Steps.Count == 0;

    public RouteDestination? Destination => Next?.Destination();

    /// <summary>An itinerary with the first step moved to <see cref="Done"/>, stamped with now.</summary>
    public Itinerary Advance()
    {
        if (Steps.Count == 0) return this;
        return new Itinerary(
            Steps.Skip(1).ToArray(),
            Done.Concat(new[] { Steps[0].CompletedNow() }).ToArray());
    }

    IRoute IRoute.Advance() => Advance();

    /// <summary>The itinerary rendered for the wire.</summary>
    public string ToHeader()
    {
        var buffer = new System.IO.MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            WriteSteps(writer, "steps", Steps);
            // Omitted when empty, which is what Go's `omitempty` and Ruby both do.
            // Python writes an empty array instead; every reader accepts either, and
            // two of the three that define this form leave it out.
            if (Done.Count > 0) WriteSteps(writer, "done", Done);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    public IReadOnlyDictionary<string, object> ToHeaders() =>
        new Dictionary<string, object> { [Header] = ToHeader() };

    /// <summary>Reads an itinerary off a message's headers, or null when it carries none.</summary>
    /// <exception cref="AceFatalException">
    /// When there is one and it cannot be read. Fatal rather than retryable: a slip
    /// that will not parse will not parse on the next attempt either, and spending
    /// five retries on it only delays whoever has to look at it.
    /// </exception>
    public static Itinerary? From(IReadOnlyDictionary<string, object> headers)
    {
        if (headers == null) throw new ArgumentNullException(nameof(headers));
        if (!headers.TryGetValue(Header, out var raw) || raw == null) return null;

        var text = raw switch
        {
            string s => s,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            _ => throw new AceFatalException(
                $"the routing slip is a {raw.GetType().Name}, not text"),
        };

        try
        {
            using var document = JsonDocument.Parse(text);
            return new Itinerary(
                StepsOf(document.RootElement, "steps"),
                StepsOf(document.RootElement, "done"));
        }
        catch (JsonException e)
        {
            throw new AceFatalException($"cannot read the routing slip: {e.Message}", e);
        }
    }

    /// <summary>The itinerary a message is carrying, or null when it carries none.</summary>
    public static Itinerary? Of<T>(IMessage<T> message) =>
        message == null ? null : From(message.WireHeaders);

    public override string ToString() =>
        "Itinerary[done: " + string.Join(" -> ", Done.Select(s => s.ToString()))
        + " | next: " + string.Join(" -> ", Steps.Select(s => s.ToString())) + "]";

    private static void WriteSteps(
        Utf8JsonWriter writer, string name, IReadOnlyList<ItineraryStep> steps)
    {
        writer.WriteStartArray(name);
        foreach (var step in steps)
        {
            writer.WriteStartObject();
            writer.WriteString("exchange", step.Exchange);
            writer.WriteString("routingKey", step.RoutingKey);
            // Both optional, and left out when they are empty rather than written as
            // empty strings, because that is what the three libraries defining this
            // form do and a byte-for-byte match is the point.
            if (step.Name.Length > 0) writer.WriteString("name", step.Name);
            if (step.CompletedAt.Length > 0) writer.WriteString("completedAt", step.CompletedAt);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
    }

    private static ItineraryStep[] StepsOf(JsonElement root, string name)
    {
        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty(name, out var array)
            || array.ValueKind != JsonValueKind.Array)
        {
            return None;
        }

        var steps = new List<ItineraryStep>();
        foreach (var element in array.EnumerateArray())
        {
            if (element.ValueKind != JsonValueKind.Object)
            {
                throw new AceFatalException(
                    "a routing slip step must be an object, not " + element.ValueKind);
            }
            steps.Add(new ItineraryStep(
                Text(element, "exchange"), Text(element, "routingKey"),
                Text(element, "name"), Text(element, "completedAt")));
        }
        return steps.ToArray();
    }

    private static string Text(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? string.Empty
            : string.Empty;
}
