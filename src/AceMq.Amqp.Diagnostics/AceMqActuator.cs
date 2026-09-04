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
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp.Diagnostics;

/// <summary>How the actuator is exposed.</summary>
public sealed class ActuatorOptions
{
    /// <summary>Port to listen on. 9464 is the OpenTelemetry Prometheus convention.</summary>
    public int Port { get; set; } = 9464;

    /// <summary>
    /// Host to bind. Loopback by default.
    /// </summary>
    /// <remarks>
    /// <strong>These endpoints are unauthenticated.</strong> They report queue names,
    /// broker state and traffic rates, and binding to <c>+</c> or <c>0.0.0.0</c>
    /// publishes that to anything that can reach the port. Bind to loopback and let
    /// the scraper reach it through the same host, a sidecar, or a network policy —
    /// and if it must be reachable from elsewhere, put something in front that
    /// authenticates.
    /// </remarks>
    public string Host { get; set; } = "localhost";

    /// <summary>
    /// Path the metrics are served from.
    /// </summary>
    /// <remarks>
    /// Namespaced by default so it cannot collide with an application's own
    /// <c>/metrics</c>. Change it if you like — but the scrape configuration has to
    /// change with it, and a Prometheus job pointed at the old path reports the
    /// target as down rather than as misconfigured.
    /// </remarks>
    public string MetricsPath { get; set; } = "/acemq-metrics";

    /// <summary>Path the health report is served from.</summary>
    public string HealthPath { get; set; } = "/acemq-health";

    /// <summary>Path the build and transport information is served from.</summary>
    public string InfoPath { get; set; } = "/acemq-info";
}

/// <summary>
/// Serves AceMQ's metrics, health and version over HTTP.
/// </summary>
/// <remarks>
/// <para>
/// A small actuator, in the spirit of the one the Spring Boot starter provides on the
/// JVM side, for applications that have no HTTP server of their own — worker services,
/// console applications, and anything on .NET Framework.
/// </para>
/// <para>
/// It exists because the usual route does not reach those consumers.
/// <c>OpenTelemetry.Exporter.Prometheus.AspNetCore</c> is still beta and requires
/// .NET 8, so a .NET Framework 4.6.2 service — which this library deliberately
/// supports — cannot use it at all. This has no OpenTelemetry dependency and no
/// ASP.NET Core dependency, and runs on everything the library runs on.
/// </para>
/// <para>
/// An ASP.NET Core application should generally use the OpenTelemetry SDK instead and
/// let it own the exporter. This is for the applications that cannot.
/// </para>
/// </remarks>
public sealed class AceMqActuator : IDisposable
{
    private readonly HttpListener _listener = new HttpListener();
    private readonly PrometheusCollector _collector;
    private readonly AceMqConnection? _mq;
    private readonly ActuatorOptions _options;
    private readonly CancellationTokenSource _stop = new CancellationTokenSource();
    private bool _disposed;

    private AceMqActuator(AceMqConnection? mq, ActuatorOptions options)
    {
        _mq = mq;
        _options = options;
        _collector = new PrometheusCollector();
    }

    /// <summary>Starts the actuator on the default port and paths.</summary>
    public static AceMqActuator Start(AceMqConnection mq) => Start(mq, new ActuatorOptions());

    /// <summary>Starts the actuator.</summary>
    public static AceMqActuator Start(AceMqConnection? mq, ActuatorOptions options)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));

        var actuator = new AceMqActuator(mq, options);
        var prefix = $"http://{options.Host}:{options.Port.ToString(CultureInfo.InvariantCulture)}/";
        actuator._listener.Prefixes.Add(prefix);

        try
        {
            actuator._listener.Start();
        }
        catch (HttpListenerException e)
        {
            actuator.Dispose();
            throw new AceMqException(
                $"could not listen on {prefix}. On Windows a non-loopback prefix needs " +
                "a URL reservation (netsh http add urlacl), and the port may be in use.", e);
        }

        actuator.Url = prefix;
        _ = Task.Run(actuator.ServeAsync);
        return actuator;
    }

    /// <summary>Where it is listening.</summary>
    public string Url { get; private set; } = string.Empty;

    /// <summary>The metrics, exactly as a scrape would see them.</summary>
    public string Metrics() => _collector.Render();

    private async Task ServeAsync()
    {
        while (!_stop.IsCancellationRequested && _listener.IsListening)
        {
            HttpListenerContext context;
            try
            {
                context = await _listener.GetContextAsync().ConfigureAwait(false);
            }
            catch (HttpListenerException) { return; }
            catch (ObjectDisposedException) { return; }

            try
            {
                Respond(context);
            }
            catch
            {
                // A failure serving diagnostics must never take down the application
                // it is reporting on.
                try { context.Response.Abort(); } catch { }
            }
        }
    }

    private void Respond(HttpListenerContext context)
    {
        var path = context.Request.Url?.AbsolutePath ?? "/";

        if (path == _options.MetricsPath)
        {
            // Prometheus needs this exact content type, including the version.
            Write(context, 200, "text/plain; version=0.0.4; charset=utf-8", _collector.Render());
        }
        else if (path == _options.HealthPath)
        {
            var health = _mq?.Health();
            // 503 only when something is actually down. Degraded stays 200: a halted
            // partition needs attention, but reporting the process unhealthy gets it
            // restarted, which loses the held message and fixes nothing.
            var status = health == null || health.Status != HealthStatus.Down ? 200 : 503;
            Write(context, status, "application/json; charset=utf-8", Health(health));
        }
        else if (path == _options.InfoPath)
        {
            Write(context, 200, "application/json; charset=utf-8", Info());
        }
        else
        {
            Write(context, 404, "text/plain; charset=utf-8",
                $"Nothing here. Try {_options.MetricsPath}, {_options.HealthPath} " +
                $"or {_options.InfoPath}.\n");
        }
    }

    private string Health(AggregateHealth? health)
    {
        var parts = new List<string>
        {
            $"\"status\":\"{(health == null ? "UP" : health.Status.ToString().ToUpperInvariant())}\"",
            $"\"inFlight\":{AceMqTelemetry.InFlight.ToString(CultureInfo.InvariantCulture)}",
        };

        if (health != null)
        {
            // Every contributor by name, so a halted ordered-queue partition is
            // visible here rather than only in the metrics -- a halted partition is
            // a consumer that has stopped without the connection noticing.
            var components = new List<string>();
            foreach (var report in health.Reports)
            {
                var details = new List<string>
                {
                    $"\"status\":\"{report.Status.ToString().ToUpperInvariant()}\"",
                };
                foreach (var detail in report.Details)
                {
                    details.Add($"\"{Json(detail.Key)}\":\"{Json(detail.Value)}\"");
                }
                components.Add($"\"{Json(report.Name)}\":{{{string.Join(",", details)}}}");
            }
            parts.Add($"\"components\":{{{string.Join(",", components)}}}");
        }

        return "{" + string.Join(",", parts) + "}\n";
    }

    private string Info()
    {
        var version = typeof(AceMqConnection).Assembly.GetName().Version?.ToString() ?? "unknown";
        var parts = new List<string>
        {
            $"\"library\":\"AceMq.Amqp\"",
            $"\"version\":\"{Json(version)}\"",
        };
        if (_mq != null)
        {
            parts.Add($"\"transport\":\"{Json(_mq.TransportName)}\"");
            var capabilities = new List<string>();
            foreach (var c in _mq.Capabilities) capabilities.Add($"\"{c}\"");
            parts.Add($"\"capabilities\":[{string.Join(",", capabilities)}]");
        }
        return "{" + string.Join(",", parts) + "}\n";
    }

    private static string Json(string value) =>
        value.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private static void Write(HttpListenerContext context, int status, string contentType, string body)
    {
        var bytes = Encoding.UTF8.GetBytes(body);
        context.Response.StatusCode = status;
        context.Response.ContentType = contentType;
        context.Response.ContentLength64 = bytes.Length;
        context.Response.OutputStream.Write(bytes, 0, bytes.Length);
        context.Response.OutputStream.Close();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _stop.Cancel();
        try { if (_listener.IsListening) _listener.Stop(); } catch { }
        _listener.Close();
        _collector.Dispose();
        _stop.Dispose();
    }
}
