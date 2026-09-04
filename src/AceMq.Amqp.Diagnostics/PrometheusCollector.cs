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
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq;
using System.Text;

namespace AceMq.Amqp.Diagnostics;

/// <summary>
/// Aggregates this process's AceMQ instruments and renders them for Prometheus.
/// </summary>
/// <remarks>
/// <para>
/// Built on <see cref="MeterListener"/>, the runtime's own subscription API, so there
/// is no OpenTelemetry SDK involved. That matters for the consumers this library
/// targets: the OpenTelemetry Prometheus exporters are still beta, and the ASP.NET
/// Core one requires .NET 8, which the .NET Framework applications this library
/// deliberately supports cannot use at all.
/// </para>
/// <para>
/// Counters are cumulative and histograms are bucketed here, because Prometheus wants
/// both cumulative. A process restart resets them, which is what Prometheus expects
/// and detects.
/// </para>
/// </remarks>
public sealed class PrometheusCollector : IDisposable
{
    // Seconds. Chosen for message latency: sub-millisecond at the bottom because a
    // publish to a local broker is faster than a millisecond, and ten seconds at the
    // top because anything slower is already an incident.
    private static readonly double[] LatencyBuckets =
        { 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 };

    // Small counts, for a histogram of attempts. Latency buckets applied to an
    // attempt number put every value in the top bucket and answer no question at
    // all -- the histogram looks populated and means nothing.
    private static readonly double[] CountBuckets = { 1, 2, 3, 5, 8, 13, 21, 34 };

    /// <summary>Which buckets suit an instrument, judged by the unit it declares.</summary>
    private static double[] BucketsFor(Instrument instrument) =>
        instrument.Unit == "s" ? LatencyBuckets : CountBuckets;

    private readonly MeterListener _listener;
    private readonly object _lock = new object();
    private readonly Dictionary<string, Series> _series = new Dictionary<string, Series>(StringComparer.Ordinal);
    private bool _disposed;

    public PrometheusCollector() : this(MetricNames.Meter) { }

    /// <summary>Collects from one meter by name.</summary>
    public PrometheusCollector(string meterName)
    {
        MeterName = meterName ?? throw new ArgumentNullException(nameof(meterName));

        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == MeterName) listener.EnableMeasurementEvents(instrument);
            },
        };

        _listener.SetMeasurementEventCallback<long>((i, m, t, _) => Observe(i, m, t));
        _listener.SetMeasurementEventCallback<int>((i, m, t, _) => Observe(i, m, t));
        _listener.SetMeasurementEventCallback<double>((i, m, t, _) => Observe(i, m, t));
        _listener.SetMeasurementEventCallback<float>((i, m, t, _) => Observe(i, m, t));
        _listener.SetMeasurementEventCallback<short>((i, m, t, _) => Observe(i, m, t));
        _listener.SetMeasurementEventCallback<byte>((i, m, t, _) => Observe(i, m, t));
        _listener.SetMeasurementEventCallback<decimal>((i, m, t, _) => Observe(i, (double)m, t));

        _listener.Start();
    }

    public string MeterName { get; }

    private void Observe(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var labels = Labels(tags);
        var key = instrument.Name + "|" + labels;

        lock (_lock)
        {
            if (!_series.TryGetValue(key, out var series))
            {
                series = new Series(instrument, labels);
                _series[key] = series;
            }
            series.Observe(value);
        }
    }

    private static string Labels(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (tags.Length == 0) return string.Empty;
        var parts = new List<string>(tags.Length);
        foreach (var tag in tags)
        {
            var value = Convert.ToString(tag.Value, CultureInfo.InvariantCulture) ?? string.Empty;
            parts.Add($"{Sanitise(tag.Key)}=\"{Escape(value)}\"");
        }
        parts.Sort(StringComparer.Ordinal);
        return string.Join(",", parts);
    }

    /// <summary>
    /// Renders everything collected so far in the Prometheus text exposition format.
    /// </summary>
    public string Render()
    {
        // Observable instruments only produce a measurement when asked, so this is
        // what turns a gauge into a number.
        _listener.RecordObservableInstruments();

        var text = new StringBuilder();
        List<Series> snapshot;
        lock (_lock) snapshot = _series.Values.ToList();

        foreach (var group in snapshot.GroupBy(s => s.Instrument.Name).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            var first = group.First();
            var name = MetricName(first);
            var type = first.IsHistogram ? "histogram" : first.IsGauge ? "gauge" : "counter";

            if (!string.IsNullOrEmpty(first.Instrument.Description))
            {
                text.Append("# HELP ").Append(name).Append(' ')
                    .Append(first.Instrument.Description!.Replace("\\", "\\\\").Replace("\n", " "))
                    .Append('\n');
            }
            text.Append("# TYPE ").Append(name).Append(' ').Append(type).Append('\n');

            foreach (var series in group.OrderBy(s => s.Labels, StringComparer.Ordinal))
            {
                series.Render(text, name);
            }
        }

        return text.ToString();
    }

    /// <summary>
    /// The Prometheus name for an instrument: dots become underscores, and a duration
    /// in seconds gains the <c>_seconds</c> suffix its conventions require.
    /// </summary>
    private static string MetricName(Series series)
    {
        var name = Sanitise(series.Instrument.Name);
        if (series.Instrument.Unit == "s" && !name.EndsWith("_seconds", StringComparison.Ordinal))
        {
            name += "_seconds";
        }
        return name;
    }

    private static string Sanitise(string name)
    {
        var text = new StringBuilder(name.Length);
        foreach (var c in name)
        {
            text.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        }
        return text.ToString();
    }

    private static string Escape(string value) =>
        value.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n");

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _listener.Dispose();
    }

    private sealed class Series
    {
        private readonly double[] _buckets;
        private readonly double[] _bucketCounts;
        private double _sum;
        private long _count;
        private double _last;

        internal Series(Instrument instrument, string labels)
        {
            Instrument = instrument;
            Labels = labels;
            _buckets = BucketsFor(instrument);
            _bucketCounts = new double[_buckets.Length + 1];
            IsHistogram = instrument.GetType().Name.StartsWith("Histogram", StringComparison.Ordinal);
            IsGauge = instrument.GetType().Name.StartsWith("Observable", StringComparison.Ordinal);
        }

        internal Instrument Instrument { get; }
        internal string Labels { get; }
        internal bool IsHistogram { get; }
        internal bool IsGauge { get; }

        internal void Observe(double value)
        {
            if (IsGauge)
            {
                // A gauge is a level, so the newest reading replaces the last rather
                // than adding to it.
                _last = value;
                return;
            }

            _sum += value;
            _count++;

            if (!IsHistogram) return;

            var bucket = _buckets.Length;
            for (var i = 0; i < _buckets.Length; i++)
            {
                if (value <= _buckets[i]) { bucket = i; break; }
            }
            _bucketCounts[bucket]++;
        }

        internal void Render(StringBuilder text, string name)
        {
            var labels = Labels;

            if (IsGauge)
            {
                Line(text, name, labels, _last);
                return;
            }

            if (!IsHistogram)
            {
                Line(text, name, labels, _sum);
                return;
            }

            // Prometheus histogram buckets are cumulative: each le counts everything
            // at or below it, not only what fell in that band.
            double cumulative = 0;
            for (var i = 0; i < _buckets.Length; i++)
            {
                cumulative += _bucketCounts[i];
                Line(text, name + "_bucket",
                    Join(labels, $"le=\"{_buckets[i].ToString(CultureInfo.InvariantCulture)}\""),
                    cumulative);
            }
            cumulative += _bucketCounts[_buckets.Length];
            Line(text, name + "_bucket", Join(labels, "le=\"+Inf\""), cumulative);
            Line(text, name + "_sum", labels, _sum);
            Line(text, name + "_count", labels, _count);
        }

        private static string Join(string labels, string extra) =>
            labels.Length == 0 ? extra : labels + "," + extra;

        private static void Line(StringBuilder text, string name, string labels, double value)
        {
            text.Append(name);
            if (labels.Length > 0) text.Append('{').Append(labels).Append('}');
            text.Append(' ')
                .Append(value.ToString("G17", CultureInfo.InvariantCulture))
                .Append('\n');
        }
    }
}
