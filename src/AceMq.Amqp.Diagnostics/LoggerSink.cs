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
using AceMq.Amqp;
using Microsoft.Extensions.Logging;

namespace AceMq.Amqp.Diagnostics;

/// <summary>
/// Sends the library's diagnostic events to an <see cref="ILogger"/>.
/// </summary>
/// <remarks>
/// <para>
/// The idiomatic destination for these, and the reason
/// <see cref="AceMqDiagnostics"/> exists as a seam rather than as an
/// <c>ILogger</c> field on the connection. This package already depends on nothing an
/// application would object to and is optional, so
/// <c>Microsoft.Extensions.Logging.Abstractions</c> lives here instead of in the
/// core — where, on <c>netstandard2.0</c>, it would bring a dependency-injection
/// abstraction along with it for every .NET Framework application that only wanted to
/// publish a message.
/// </para>
/// <para>
/// Written for the two lines it takes to use:
/// </para>
/// <code>
/// using var logging = LoggerSink.SubscribedTo(loggerFactory);
/// </code>
/// <para>
/// Disposing it unsubscribes, so a sink cannot outlive the logger factory it writes
/// to — which in an application that rebuilds its logging on configuration reload is
/// the difference between a rotated log and an <see cref="ObjectDisposedException"/>
/// thrown from a dead-lettering path.
/// </para>
/// </remarks>
public sealed class LoggerSink : IDiagnosticSink, IDisposable
{
    /// <summary>The category events are logged under.</summary>
    /// <remarks>
    /// The namespace rather than a type name, so that a filter written as
    /// <c>"AceMq.Amqp": "Warning"</c> in <c>appsettings.json</c> catches these along
    /// with anything else the library ever logs.
    /// </remarks>
    public const string Category = "AceMq.Amqp";

    private readonly ILogger _logger;
    private bool _subscribed;
    private bool _disposed;

    public LoggerSink(ILogger logger) =>
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    public LoggerSink(ILoggerFactory factory)
        : this((factory ?? throw new ArgumentNullException(nameof(factory)))
            .CreateLogger(Category))
    {
    }

    /// <summary>Creates a sink and subscribes it. Dispose it to unsubscribe.</summary>
    public static LoggerSink SubscribedTo(ILoggerFactory factory)
    {
        var sink = new LoggerSink(factory);
        AceMqDiagnostics.Subscribe(sink);
        sink._subscribed = true;
        return sink;
    }

    /// <summary>Creates a sink over one logger and subscribes it.</summary>
    public static LoggerSink SubscribedTo(ILogger logger)
    {
        var sink = new LoggerSink(logger);
        AceMqDiagnostics.Subscribe(sink);
        sink._subscribed = true;
        return sink;
    }

    /// <summary>
    /// Writes one event.
    /// </summary>
    /// <remarks>
    /// The fields go in as a scope rather than only into the message text, so a
    /// structured logging backend can be queried by queue or by message id. The
    /// message text carries them too, because a console logger shows the text and
    /// nothing else.
    /// </remarks>
    public void Record(DiagnosticEvent report)
    {
        if (report == null || _disposed) return;

        var level = report.Level switch
        {
            DiagnosticLevel.Error => LogLevel.Error,
            DiagnosticLevel.Warning => LogLevel.Warning,
            _ => LogLevel.Information,
        };

        if (!_logger.IsEnabled(level)) return;

        var state = new List<KeyValuePair<string, object?>>(6)
        {
            new KeyValuePair<string, object?>("acemq.event", report.Name),
        };
        if (report.Queue != null)
        {
            state.Add(new KeyValuePair<string, object?>("acemq.queue", report.Queue));
        }
        if (report.Destination != null)
        {
            state.Add(new KeyValuePair<string, object?>("acemq.destination", report.Destination));
        }
        if (report.MessageId != null)
        {
            state.Add(new KeyValuePair<string, object?>("acemq.message_id", report.MessageId));
        }
        if (report.Attempt > 0)
        {
            state.Add(new KeyValuePair<string, object?>("acemq.attempt", report.Attempt));
        }

        using (_logger.BeginScope(state))
        {
            // Not a formatted template: the message is already composed, and passing
            // it as a template would make a queue name containing a brace throw from
            // inside a dead-lettering path.
            _logger.Log(level, default, report, report.Failure, static (r, _) => r.ToString());
        }
    }

    /// <summary>Unsubscribes, if this sink subscribed itself.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        if (_subscribed) AceMqDiagnostics.Unsubscribe(this);
    }
}
