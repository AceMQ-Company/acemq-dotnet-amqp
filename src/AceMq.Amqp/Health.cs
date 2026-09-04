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
using System.Linq;

namespace AceMq.Amqp;

/// <summary>Whether something is working.</summary>
public enum HealthStatus
{
    Up,

    /// <summary>Working, but something needs attention before it stops working.</summary>
    Degraded,

    Down,
}

/// <summary>What one part of the system reports about itself.</summary>
public sealed class HealthReport
{
    public HealthReport(string name, HealthStatus status, IReadOnlyDictionary<string, string> details)
    {
        Name = name;
        Status = status;
        Details = details;
    }

    public static HealthReport Up(string name) =>
        new HealthReport(name, HealthStatus.Up, new Dictionary<string, string>());

    public string Name { get; }
    public HealthStatus Status { get; }
    public IReadOnlyDictionary<string, string> Details { get; }

    public override string ToString() => $"{Name}: {Status}";
}

/// <summary>Something that can say whether it is working.</summary>
/// <remarks>
/// Implement this and register it with
/// <see cref="AceMqConnection.RegisterHealth"/> to have it appear in the actuator's
/// health report alongside the library's own.
/// </remarks>
public interface IHealthContributor
{
    string Name { get; }

    HealthReport Report();
}

/// <summary>The health of everything reporting, taken together.</summary>
public sealed class AggregateHealth
{
    internal AggregateHealth(IReadOnlyList<HealthReport> reports)
    {
        Reports = reports;
        // The worst report wins. Averaging or majority-voting health hides exactly
        // the one component that has stopped, which is the one worth knowing about.
        Status = reports.Count == 0
            ? HealthStatus.Up
            : reports.Max(r => r.Status);
    }

    public HealthStatus Status { get; }
    public IReadOnlyList<HealthReport> Reports { get; }

    public override string ToString() =>
        $"{Status} ({string.Join(", ", Reports.Select(r => r.ToString()))})";
}
