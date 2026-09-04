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

namespace AceMq.Amqp;

/// <summary>
/// An itinerary a message carries with it.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="Pipeline{T}"/> decides the route once, at build time, and every
/// message takes the same path. A routing slip is the other arrangement: the route
/// travels in the message's headers, so it can differ per message and be decided by
/// whatever handled the last step.
/// </para>
/// <para>
/// That is what it is for — an order that needs a fraud check only above a
/// threshold, or a document that visits different approvers depending on who
/// submitted it. Where every message takes the same path, a pipeline says so more
/// clearly and does not pay to carry the route around.
/// </para>
/// <para>
/// The slip is not a transaction. Each step commits as it finishes, so a failure at
/// step four does not undo steps one to three; if that matters, the route needs
/// compensating steps of its own.
/// </para>
/// </remarks>
public sealed class RoutingSlip
{
    /// <summary>Header carrying the remaining steps, comma-separated.</summary>
    public const string RouteHeader = AceHeaders.Prefix + "route";

    /// <summary>Header carrying how far along the route the message is.</summary>
    public const string PositionHeader = AceHeaders.Prefix + "route-position";

    /// <summary>Header identifying this journey, so its steps can be correlated.</summary>
    public const string RunIdHeader = AceHeaders.Prefix + "route-id";

    private RoutingSlip(IReadOnlyList<string> steps, int position, string runId)
    {
        Steps = steps;
        Position = position;
        RunId = runId;
    }

    /// <summary>Starts a journey through these steps, in order.</summary>
    public static RoutingSlip StartOf(params string[] steps) =>
        StartOf((IReadOnlyList<string>)(steps ?? throw new ArgumentNullException(nameof(steps))));

    public static RoutingSlip StartOf(IReadOnlyList<string> steps)
    {
        if (steps == null) throw new ArgumentNullException(nameof(steps));
        if (steps.Count == 0)
        {
            throw new ArgumentException("a routing slip needs at least one step", nameof(steps));
        }
        foreach (var step in steps)
        {
            if (string.IsNullOrWhiteSpace(step))
            {
                throw new ArgumentException("a step cannot be blank", nameof(steps));
            }
            if (step.IndexOf(',') >= 0)
            {
                // The route is comma-separated on the wire, so a comma in a step
                // name would split it into two destinations that do not exist.
                throw new ArgumentException($"a step name cannot contain a comma: '{step}'", nameof(steps));
            }
        }
        return new RoutingSlip(steps.ToArray(), 0, Guid.NewGuid().ToString("N"));
    }

    /// <summary>Reads a slip off a message's headers, or null if it carries none.</summary>
    public static RoutingSlip? From(IReadOnlyDictionary<string, object> headers)
    {
        if (headers == null) throw new ArgumentNullException(nameof(headers));
        if (!headers.TryGetValue(RouteHeader, out var route) || route == null) return null;

        var steps = Convert.ToString(route, CultureInfo.InvariantCulture)
            ?.Split(',')
            .Where(s => s.Length > 0)
            .ToArray();
        if (steps == null || steps.Length == 0) return null;

        var position = headers.TryGetValue(PositionHeader, out var p) && p != null
            ? Convert.ToInt32(p, CultureInfo.InvariantCulture)
            : 0;
        var runId = headers.TryGetValue(RunIdHeader, out var r) && r != null
            ? Convert.ToString(r, CultureInfo.InvariantCulture) ?? string.Empty
            : string.Empty;

        // A position past the end means the slip is finished, which is legitimate;
        // a negative one is corrupt and is treated as the start rather than
        // throwing, because a malformed header should not stop a message being
        // handled.
        if (position < 0) position = 0;
        return new RoutingSlip(steps, position, runId);
    }

    /// <summary>The slip a message is carrying, or null if it carries none.</summary>
    public static RoutingSlip? Of<T>(IMessage<T> message) =>
        message == null ? null : From(message.WireHeaders);

    /// <summary>The whole route, including the steps already done.</summary>
    public IReadOnlyList<string> Steps { get; }

    /// <summary>How many steps have been completed.</summary>
    public int Position { get; }

    /// <summary>Identifies this journey across its steps.</summary>
    public string RunId { get; }

    /// <summary>The step the message is at, or null when the route is finished.</summary>
    public string? Current => Position < Steps.Count ? Steps[Position] : null;

    /// <summary>The step after the current one, or null when this is the last.</summary>
    public string? Next => Position + 1 < Steps.Count ? Steps[Position + 1] : null;

    /// <summary>Whether every step has been done.</summary>
    public bool IsFinished => Position >= Steps.Count;

    /// <summary>The slip one step further along.</summary>
    public RoutingSlip Advance() => new RoutingSlip(Steps, Position + 1, RunId);

    /// <summary>
    /// The slip moved to a chosen position.
    /// </summary>
    /// <remarks>
    /// For skipping ahead — a step deciding the next two are unnecessary — or for
    /// sending a message back to an earlier step. Going backwards will repeat work,
    /// so the steps it revisits need to tolerate that.
    /// </remarks>
    public RoutingSlip AdvanceTo(int position)
    {
        if (position < 0) throw new ArgumentException("cannot be negative", nameof(position));
        return new RoutingSlip(Steps, position, RunId);
    }

    /// <summary>The headers this slip travels in.</summary>
    public IReadOnlyDictionary<string, object> ToHeaders() =>
        new Dictionary<string, object>
        {
            [RouteHeader] = string.Join(",", Steps),
            [PositionHeader] = Position,
            [RunIdHeader] = RunId,
        };

    public override string ToString() =>
        IsFinished
            ? $"RoutingSlip[{RunId}, finished after {Steps.Count} step(s)]"
            : $"RoutingSlip[{RunId}, at {Current} ({Position + 1}/{Steps.Count})]";
}
