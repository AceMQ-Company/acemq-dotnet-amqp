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

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

namespace AceMq.Amqp.RabbitMq.Tests;

/// <summary>
/// A stream reader whose connection the broker closes, and which the client then
/// recovers.
/// </summary>
/// <remarks>
/// The client re-subscribes a recovered consumer with the arguments it was first
/// given, so a stream reader asked for its original <c>x-stream-offset</c> again: one
/// that began at <c>first</c> read the whole stream a second time, and one that began
/// at <c>next</c> skipped everything appended while it was away. The connection is
/// closed by the broker through <c>ACEMQ_TEST_RABBITMQCTL</c>, which is what a real
/// outage looks like to the client and the only close it recovers from.
/// </remarks>
public sealed class StreamRecoveryTests : IAsyncLifetime
{
    private readonly string _url =
        Environment.GetEnvironmentVariable("ACEMQ_TEST_AMQP_URL")
        ?? "amqp://guest:guest@localhost:5672";

    private readonly string _suffix = Guid.NewGuid().ToString("N").Substring(0, 8);
    private string Stream => $"acemq.test.{_suffix}.stream";
    private string ReaderName => $"acemq-test-stream-reader-{_suffix}";

    private AceMqConnection _writer = null!;
    private AceMqConnection _reader = null!;
    private readonly ConcurrentDictionary<string, int> _seen = new ConcurrentDictionary<string, int>();

    public async Task InitializeAsync()
    {
        Transports.Register(new RabbitMqTransport());
        _writer = await AceMqConnection.ConnectAsync(_url);
        _reader = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(_url).ClientName(ReaderName).Build());
        await _writer.DeclareStreamAsync(Stream, TimeSpan.FromHours(1), 10_000_000);
    }

    public async Task DisposeAsync()
    {
        _reader.Dispose();
        foreach (var name in new[] { Stream, Naming.DeadLetterQueue(Stream), Naming.ParkedQueue(Stream) })
        {
            try { await _writer.DeleteQueueAsync(name); } catch { /* never declared */ }
        }
        _writer.Dispose();
    }

    [Fact]
    public async Task AReaderFromFirstDoesNotReadTheStreamAgainAfterRecovery()
    {
        await AppendAsync("a", 20);
        using var reader = await ReadAsync(StreamOffset.First());
        await WaitForAsync(20);

        await LoseTheConnectionAndAppendAsync(10);

        Assert.Equal((30, 30), (_seen.Values.Sum(), _seen.Count));
    }

    [Fact]
    public async Task AReaderFromNextDoesNotSkipWhatWasAppendedWhileItWasAway()
    {
        using var reader = await ReadAsync(StreamOffset.Next());
        await Task.Delay(500);
        await AppendAsync("a", 20);
        await WaitForAsync(20);

        await LoseTheConnectionAndAppendAsync(10);

        Assert.Equal((30, 30), (_seen.Values.Sum(), _seen.Count));
    }

    [Fact]
    public async Task AQueueConsumerStillRecovers()
    {
        var queue = $"acemq.test.{_suffix}.queue";
        await _writer.DeclareQueueAsync(queue);
        try
        {
            await _reader.ConsumeAsync<string>(queue, message =>
            {
                _seen.AddOrUpdate(message.Payload, 1, (_, n) => n + 1);
                return Task.FromResult(Ack.Accept());
            });
            await AppendAsync("a", 5, queue);
            await WaitForAsync(5);

            await CloseReaderConnectionAsync(append: () => AppendAsync("b", 5, queue));
            await WaitForAsync(10);

            Assert.Equal((10, 10), (_seen.Values.Sum(), _seen.Count));
        }
        finally
        {
            foreach (var name in new[] { queue, Naming.DeadLetterQueue(queue), Naming.ParkedQueue(queue) })
            {
                try { await _writer.DeleteQueueAsync(name); } catch { /* never declared */ }
            }
        }
    }

    private Task<IStreamConsumer> ReadAsync(StreamOffset from) =>
        _reader.Stream<string>(Stream).From(from).Prefetch(10).ConsumeAsync(message =>
        {
            _seen.AddOrUpdate(message.Payload, 1, (_, n) => n + 1);
            return Task.CompletedTask;
        });

    private async Task AppendAsync(string prefix, int count, string? queue = null)
    {
        var publisher = _writer.Publisher<string>("", queue ?? Stream);
        for (var i = 0; i < count; i++) await publisher.SendAsync($"{prefix}{i}");
    }

    private async Task LoseTheConnectionAndAppendAsync(int count)
    {
        // Appended while the reader is away: the client waits before reconnecting.
        await CloseReaderConnectionAsync(append: () => AppendAsync("b", count));
        await WaitForAsync(_seen.Count + count);
        // Long enough for a replay from the start to have arrived, had there been one.
        await Task.Delay(TimeSpan.FromSeconds(2));
    }

    private async Task WaitForAsync(int deliveries)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline && _seen.Values.Sum() < deliveries) await Task.Delay(50);
    }

    /// <summary>Has the broker close the reader's connection, as an outage would.</summary>
    private async Task CloseReaderConnectionAsync(Func<Task> append)
    {
        // The connection name travels in client_properties, which every supported
        // broker version lists; user_provided_name is not a column on all of them.
        var pids = Control("list_connections", "--silent", "pid", "client_properties")
            .Split('\n')
            .Where(line => line.Contains(ReaderName))
            .Select(line => line.Split('\t')[0].Trim())
            .ToList();
        Assert.Single(pids);
        Control("close_connection", pids[0], "stream recovery test");

        // Down, then back.
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline && _reader.IsOpen) await Task.Delay(20);
        await append();
        while (DateTime.UtcNow < deadline && !_reader.IsOpen) await Task.Delay(50);
        Assert.True(_reader.IsOpen, "the reader's connection did not recover");
        await Task.Delay(TimeSpan.FromSeconds(1));
    }

    internal static string Control(params string[] arguments)
    {
        const string variable = "ACEMQ_TEST_RABBITMQCTL";
        var prefix = Environment.GetEnvironmentVariable(variable);
        if (string.IsNullOrWhiteSpace(prefix))
        {
            throw new InvalidOperationException(
                $"{variable} is not set, so this suite cannot have the broker close a " +
                "connection. Set it to a command prefix that reaches the broker under " +
                "test, for example \"docker exec acemq-ci-rabbit rabbitmqctl\".");
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
        process.WaitForExit(30_000);
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"{prefix} {string.Join(" ", arguments)} exited {process.ExitCode}\n{stdout}{stderr}");
        }
        return stdout;
    }
}
