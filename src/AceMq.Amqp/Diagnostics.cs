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
using System.Text;

namespace AceMq.Amqp;

/// <summary>How much attention an event wants.</summary>
public enum DiagnosticLevel
{
    /// <summary>Something happened that is worth a record but not a look.</summary>
    Info,

    /// <summary>A message needs a person, eventually.</summary>
    Warning,

    /// <summary>The library could not do what it was asked and said so to the broker.</summary>
    Error,
}

/// <summary>
/// Something the library did that an operator would want to know about.
/// </summary>
/// <remarks>
/// The fields are the ones an operator asks for in the order they ask: which queue,
/// which message, where it was going, which attempt, and what went wrong. A sink that
/// writes structured logs can lift them out; one that writes a line can call
/// <see cref="ToString"/>.
/// </remarks>
public sealed class DiagnosticEvent
{
    public DiagnosticEvent(
        string name, DiagnosticLevel level, string message,
        string? queue, string? destination, string? messageId, int attempt,
        Exception? failure)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Level = level;
        Message = message ?? string.Empty;
        Queue = queue;
        Destination = destination;
        MessageId = messageId;
        Attempt = attempt;
        Failure = failure;
    }

    /// <summary>A stable identifier, from the constants on <see cref="AceMqDiagnostics"/>.</summary>
    /// <remarks>
    /// Stable so that an alert can be written against it. The wording of
    /// <see cref="Message"/> is not a contract and this is.
    /// </remarks>
    public string Name { get; }

    public DiagnosticLevel Level { get; }

    /// <summary>What happened, in a sentence.</summary>
    public string Message { get; }

    /// <summary>The queue the message was consumed from, when there is one.</summary>
    public string? Queue { get; }

    /// <summary>Where the message was being sent, when it was being sent anywhere.</summary>
    public string? Destination { get; }

    /// <summary>The envelope id, so this can be joined to the message.</summary>
    public string? MessageId { get; }

    /// <summary>Which attempt this was, or zero when the notion does not apply.</summary>
    public int Attempt { get; }

    /// <summary>The exception behind it, when there was one.</summary>
    public Exception? Failure { get; }

    public override string ToString()
    {
        var text = new StringBuilder(Name).Append(": ").Append(Message);
        if (Queue != null) text.Append(" [queue=").Append(Queue).Append(']');
        if (Destination != null) text.Append(" [destination=").Append(Destination).Append(']');
        if (MessageId != null) text.Append(" [message=").Append(MessageId).Append(']');
        if (Attempt > 0) text.Append(" [attempt=").Append(Attempt).Append(']');
        return text.ToString();
    }
}

/// <summary>Somewhere the library's diagnostic events can be sent.</summary>
/// <remarks>
/// One method, and it must not throw — see <see cref="AceMqDiagnostics.Record"/> for
/// what happens if it does. Implement it over whatever the application already logs
/// to, or use the ready-made bridge to <c>Microsoft.Extensions.Logging</c> in the
/// <c>AceMq.Amqp.Diagnostics</c> package.
/// </remarks>
public interface IDiagnosticSink
{
    void Record(DiagnosticEvent report);
}

/// <summary>
/// Where the library says the things a span cannot.
/// </summary>
/// <remarks>
/// <para>
/// The gap this fills was a real one. When a republish failed, the message was handed
/// back to the broker with <see cref="Ack.Release"/> and the only trace was a status
/// on <c>Activity.Current</c> — so unless the process was already exporting traces,
/// and unless somebody went looking at the right span, a message that could not be
/// moved left no record at all. That is exactly the event an operator needs to see,
/// and it was the one event that was invisible.
/// </para>
/// <para>
/// <strong>Why this and not <c>Microsoft.Extensions.Logging.Abstractions</c>
/// directly.</strong> That package is the idiomatic answer and is the one the bridge
/// in <c>AceMq.Amqp.Diagnostics</c> is written against. It is not a dependency of the
/// core, because on <c>netstandard2.0</c> it brings
/// <c>Microsoft.Extensions.DependencyInjection.Abstractions</c>, <c>System.Buffers</c>
/// and <c>System.Memory</c> with it — four packages added to a library that has two,
/// and a dependency-injection abstraction handed to every .NET Framework application
/// that only wanted to publish a message. The library takes the same line here that it
/// takes on OpenTelemetry for metrics and ASP.NET Core for the actuator: the seam is
/// here, the adapter is in the optional package, and an application that wants
/// <c>ILogger</c> adds one line and one package reference.
/// </para>
/// <para>
/// Sinks are process-wide rather than per connection, which is the same shape as
/// <see cref="AceMqTelemetry"/>'s meter and is what a logging setup written once in
/// <c>Main</c> expects.
/// </para>
/// </remarks>
public static class AceMqDiagnostics
{
    /// <summary>A message could not be moved and was handed back to the broker.</summary>
    public const string MoveFailed = "acemq.move.failed";

    /// <summary>A message ran out of attempts, or a handler gave up on it.</summary>
    public const string DeadLettered = "acemq.message.dead-lettered";

    /// <summary>A message could not be read and needs a person.</summary>
    public const string Parked = "acemq.message.parked";

    /// <summary>A broker wait was asked for and no rung existed to spend it in.</summary>
    public const string RungMissing = "acemq.retry.rung-missing";

    /// <summary>A saga step failed and the steps before it are being undone.</summary>
    public const string SagaCompensating = "acemq.saga.compensating";

    /// <summary>
    /// A compensation failed, so something a saga did is still done.
    /// </summary>
    /// <remarks>
    /// The one saga event worth waking somebody for. The others describe a system
    /// that put itself back; this one describes a real-world effect that happened,
    /// was meant to be undone, and was not — and no retry will resolve it, because
    /// nothing is retrying.
    /// </remarks>
    public const string SagaUnresolved = "acemq.saga.unresolved";

    /// <summary>
    /// A message in the scheduler's control queue was not one the scheduler put there.
    /// </summary>
    /// <remarks>
    /// The scheduler's queues are an implementation detail of <see cref="Scheduler"/>,
    /// and a message arriving without the headers a scheduled message carries has no
    /// destination to be sent to. It is dropped, because the control consumer has no
    /// dead-letter queue by design — see <c>Scheduler.OnAsync</c> — so this event is
    /// the only record that it existed.
    /// </remarks>
    public const string ScheduleForeign = "acemq.schedule.foreign-message";

    private static readonly List<IDiagnosticSink> Sinks = new List<IDiagnosticSink>();

    // Read on every event and written only when a sink is added or removed, so the
    // common case -- nobody subscribed -- costs a volatile read rather than a lock.
    private static volatile IDiagnosticSink[] _snapshot = new IDiagnosticSink[0];

    /// <summary>Whether anything is listening.</summary>
    /// <remarks>
    /// Worth checking before building an event whose message costs something to
    /// assemble. The library checks it.
    /// </remarks>
    public static bool IsEnabled => _snapshot.Length > 0;

    /// <summary>Starts sending events to a sink.</summary>
    public static void Subscribe(IDiagnosticSink sink)
    {
        if (sink == null) throw new ArgumentNullException(nameof(sink));
        lock (Sinks)
        {
            if (Sinks.Contains(sink)) return;
            Sinks.Add(sink);
            _snapshot = Sinks.ToArray();
        }
    }

    /// <summary>Stops sending events to a sink. Returns whether it was there.</summary>
    public static bool Unsubscribe(IDiagnosticSink sink)
    {
        if (sink == null) return false;
        lock (Sinks)
        {
            if (!Sinks.Remove(sink)) return false;
            _snapshot = Sinks.ToArray();
            return true;
        }
    }

    /// <summary>Removes every sink. For tests, which must not leak one into the next.</summary>
    public static void Clear()
    {
        lock (Sinks)
        {
            Sinks.Clear();
            _snapshot = new IDiagnosticSink[0];
        }
    }

    /// <summary>
    /// Sends an event to every sink.
    /// </summary>
    /// <remarks>
    /// A sink that throws is swallowed, and deliberately. Every call here is on a
    /// path where something has already gone wrong with a message; letting a broken
    /// logger turn "this message was dead-lettered" into "this consumer crashed"
    /// would make the diagnostics more dangerous than the problem they describe.
    /// </remarks>
    public static void Record(DiagnosticEvent report)
    {
        if (report == null) return;
        var sinks = _snapshot;
        for (var i = 0; i < sinks.Length; i++)
        {
            try
            {
                sinks[i].Record(report);
            }
            catch
            {
                // See above.
            }
        }
    }

    /// <summary>The library's own call, kept short at each of the call sites.</summary>
    internal static void Report(
        string name, DiagnosticLevel level, string message,
        string? queue, string? destination, string? messageId, int attempt,
        Exception? failure)
    {
        if (!IsEnabled) return;
        Record(new DiagnosticEvent(
            name, level, message, queue, destination, messageId, attempt, failure));
    }
}
