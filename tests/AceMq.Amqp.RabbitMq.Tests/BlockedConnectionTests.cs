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

using System.Diagnostics;
using System.Linq;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

// Nothing in this project may run beside a test that blocks the broker: the memory
// alarm is node-wide, so a publish in another class would hang on it and fail for a
// reason that has nothing to do with what it was testing. xUnit already serialises
// the tests inside a class; this serialises the classes as well.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace AceMq.Amqp.RabbitMq.Tests;

/// <summary>
/// What the library reports while the broker has the connection blocked.
/// </summary>
/// <remarks>
/// <para>
/// A broker under memory or disk pressure sends <c>connection.blocked</c> and stops
/// reading from the connections that publish. This is the one state that cannot be
/// faked usefully — a test that sets a flag and asserts on the flag proves that the
/// flag is readable — so this suite makes a real broker do it, by dropping the memory
/// high watermark until every connection is over it and putting it back afterwards.
/// </para>
/// <para>
/// It needs a way to run <c>rabbitmqctl</c> against the broker under test, which AMQP
/// itself does not offer. <c>ACEMQ_TEST_RABBITMQCTL</c> is that way, and it is a
/// command prefix rather than a path so it can reach a broker in a container:
/// </para>
/// <code>
/// ACEMQ_TEST_RABBITMQCTL="docker exec acemq-ci-rabbit rabbitmqctl"
/// ACEMQ_TEST_RABBITMQCTL="rabbitmqctl"
/// </code>
/// <para>
/// Unset, this fails rather than skipping. A suite that quietly does nothing when it
/// cannot reach its subject reports a green tick for work nobody did, which is the
/// failure mode the rest of this project is arranged to avoid.
/// </para>
/// </remarks>
public sealed class BlockedConnectionTests : IAsyncLifetime
{
    private const string WatermarkVariable = "ACEMQ_TEST_RABBITMQCTL";

    /// <summary>Low enough that any running broker is already over it.</summary>
    private const string Blocking = "0.0001";

    /// <summary>RabbitMQ's own default, restored rather than assumed.</summary>
    private const string Normal = "0.4";

    private readonly string _url =
        Environment.GetEnvironmentVariable("ACEMQ_TEST_AMQP_URL")
        ?? "amqp://guest:guest@localhost:5672";

    private readonly string _queue =
        "acemq.test.blocked." + Guid.NewGuid().ToString("N").Substring(0, 8);

    private AceMqConnection _mq = null!;

    public async Task InitializeAsync()
    {
        Transports.Register(new RabbitMqTransport());
        _mq = await AceMqConnection.ConnectAsync(_url);
        await _mq.DeclareQueueAsync(_queue);
    }

    public async Task DisposeAsync()
    {
        // The watermark first and unconditionally. A failed assertion in the middle of
        // this suite must not leave the broker blocked for whatever runs next.
        try { Control("set_vm_memory_high_watermark", Normal); } catch { /* best effort */ }
        try { await _mq.DeleteQueueAsync(_queue); } catch { /* already gone */ }
        _mq.Dispose();
    }

    [Fact]
    public async Task ReportsABlockedConnectionAsUpWithTheReason()
    {
        var publisher = _mq.Publisher<string>("", _queue);

        // Publishing is what makes RabbitMQ block a connection: the alarm is
        // node-wide, but connection.blocked is sent to a connection when it next
        // publishes under it. Not awaited, because the whole point of being blocked is
        // that the broker has stopped reading -- this send does not return until the
        // watermark goes back up.
        Control("set_vm_memory_high_watermark", Blocking);
        var held = Task.Run(async () =>
        {
            try { await publisher.SendAsync("under an alarm"); } catch { /* unblocked below */ }
        });

        await Eventually(() => _mq.IsBlocked, "the broker to block the connection");

        // Up, with the reason. Not degraded: an aggregate takes the worst report, so a
        // degraded connection would overrule any more careful answer a caller had
        // composed alongside it -- and the orchestrator reading it would restart the
        // application into the same blocked broker, losing whatever it was holding.
        var health = _mq.Health();
        var connection = health.Reports.Single(r => r.Name == "connection");

        Assert.Equal(HealthStatus.Up, connection.Status);
        Assert.Equal(HealthStatus.Up, health.Status);
        Assert.Equal("true", connection.Details["blocked"]);
        Assert.Equal("true", connection.Details["open"]);

        // The reason the broker gave, carried through rather than summarised: "low on
        // memory" and "low on disk" are different incidents.
        Assert.True(connection.Details.ContainsKey("blockedReason"));
        Assert.False(string.IsNullOrWhiteSpace(connection.Details["blockedReason"]));
        Assert.Equal(_mq.BlockedReason, connection.Details["blockedReason"]);

        Control("set_vm_memory_high_watermark", Normal);
        await Eventually(() => !_mq.IsBlocked, "the broker to unblock the connection");

        // And back to a plain Up with no reason on it, so an alert on this clears.
        var recovered = _mq.Health().Reports.Single(r => r.Name == "connection");
        Assert.Equal(HealthStatus.Up, recovered.Status);
        Assert.Equal("false", recovered.Details["blocked"]);
        Assert.False(recovered.Details.ContainsKey("blockedReason"));

        await held.WaitAsync(TimeSpan.FromSeconds(30));
    }

    /// <summary>Runs one rabbitmqctl subcommand and throws if it fails.</summary>
    private static void Control(params string[] arguments)
    {
        var prefix = Environment.GetEnvironmentVariable(WatermarkVariable);
        if (string.IsNullOrWhiteSpace(prefix))
        {
            throw new InvalidOperationException(
                $"{WatermarkVariable} is not set, so this suite cannot make the broker " +
                "block a connection and has nothing to assert. Set it to a command " +
                "prefix that reaches the broker under test, for example " +
                "\"docker exec acemq-ci-rabbit rabbitmqctl\" or \"rabbitmqctl\".");
        }

        var words = prefix.Split(' ').Where(w => w.Length > 0).ToArray();
        var start = new ProcessStartInfo
        {
            FileName = words[0],
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var word in words.Skip(1)) start.ArgumentList.Add(word);
        foreach (var argument in arguments) start.ArgumentList.Add(argument);

        using var process = Process.Start(start)
            ?? throw new InvalidOperationException($"could not run {prefix}");
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        if (!process.WaitForExit(30_000))
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException(
                $"{prefix} {string.Join(" ", arguments)} did not finish in 30s");
        }

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"{prefix} {string.Join(" ", arguments)} exited {process.ExitCode}\n" +
                stdout + stderr);
        }
    }

    private static async Task Eventually(Func<bool> probe, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (probe()) return;
            await Task.Delay(50);
        }
        throw new TimeoutException($"timed out waiting for {what}");
    }
}
