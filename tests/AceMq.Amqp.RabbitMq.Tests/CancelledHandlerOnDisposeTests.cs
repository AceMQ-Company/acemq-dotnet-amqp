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
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

namespace AceMq.Amqp.RabbitMq.Tests;

/// <summary>
/// A drain that overruns: the handler is cancelled, the consumer and the connection
/// are disposed, and the message the handler never finished comes back.
/// </summary>
/// <remarks>
/// What a host does on shutdown. In 0.7.10 the message stayed unacknowledged and
/// the connection stayed open for as long as the process ran, but only once some
/// other consumer had already come and gone in the same process.
/// </remarks>
public class CancelledHandlerOnDisposeTests
{
    private readonly string _url =
        Environment.GetEnvironmentVariable("ACEMQ_TEST_AMQP_URL")
        ?? "amqp://guest:guest@localhost:5672";

    private static string NewQueue(string what) =>
        $"acemq.test.{Guid.NewGuid().ToString("N").Substring(0, 8)}.{what}";

    private async Task<AceMqConnection> ConnectAsync()
    {
        Transports.Register(new RabbitMqTransport());
        return await AceMqConnection.ConnectAsync(_url);
    }

    [Fact]
    public async Task AMessageWhoseHandlerWasCancelledComesBackAndTheConnectionCloses()
    {
        // Another consumer first, on its own connection, as another test would.
        var first = NewQueue("first");
        using (var before = await ConnectAsync())
        {
            await before.DeclareQueueAsync(first);
            var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using (await before.ConsumeAsync<string>(first, _ =>
                   {
                       handled.TrySetResult();
                       return Task.FromResult(Ack.Accept());
                   }))
            {
                await before.Publisher<string>("", first).SendAsync("m");
                await handled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            }
            foreach (var name in new[] { first, Naming.DeadLetterQueue(first), Naming.ParkedQueue(first) })
            {
                await before.DeleteQueueAsync(name);
            }
        }

        var queue = NewQueue("overrun");
        var mq = await ConnectAsync();
        await mq.DeclareQueueAsync(queue);

        using var cancel = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = await mq.ConsumeAsync<string>(queue, async _ =>
        {
            entered.TrySetResult();
            await Task.Delay(TimeSpan.FromMinutes(5), cancel.Token);
            return Ack.Accept();
        });

        await mq.Publisher<string>("", queue).SendAsync("m");
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The drain overruns, the handler is cancelled, everything is disposed.
        Assert.False(await mq.DrainConsumersAsync(TimeSpan.FromSeconds(1)));
        var clock = Stopwatch.StartNew();
        cancel.Cancel();
        consumer.Dispose();
        mq.Dispose();
        clock.Stop();

        using var after = await ConnectAsync();
        try
        {
            var count = 0L;
            for (var i = 0; i < 50 && count == 0; i++)
            {
                count = await after.MessageCountAsync(queue);
                if (count == 0) await Task.Delay(200);
            }

            Assert.Equal(1, count);
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(3), $"disposing took {clock.Elapsed}");
        }
        finally
        {
            foreach (var name in new[] { queue, Naming.DeadLetterQueue(queue), Naming.ParkedQueue(queue) })
            {
                try { await after.DeleteQueueAsync(name); } catch { /* already gone */ }
            }
        }
    }

    /// <summary>
    /// A handler that ignores its cancellation and never returns in time: the connection
    /// still closes, the message comes back once, and the handler's late answer is
    /// dropped.
    /// </summary>
    /// <remarks>
    /// In 0.7.11 the client's close waited for that handler, gave up after five seconds
    /// without closing the socket, and the broker kept the connection and the
    /// unacknowledged message until the process exited.
    /// </remarks>
    [Fact]
    public async Task AHandlerThatIgnoresCancellationDoesNotHoldTheConnectionOpen()
    {
        var queue = NewQueue("stuck");
        var mq = await ConnectAsync();
        await mq.DeclareQueueAsync(queue);

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var consumer = await mq.ConsumeAsync<string>(queue, async _ =>
        {
            entered.TrySetResult();
            await release.Task; // no token: it runs on whatever happens
            returned.TrySetResult();
            return Ack.Accept();
        });

        await mq.Publisher<string>("", queue).SendAsync("m");
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.False(await mq.DrainConsumersAsync(TimeSpan.FromSeconds(1)));
        var clock = Stopwatch.StartNew();
        // Bounded here, so a dispose that never returns fails the test rather than
        // hanging it.
        var disposed = Task.Run(mq.Dispose);
        await Task.WhenAny(disposed, Task.Delay(TimeSpan.FromSeconds(15)));
        clock.Stop();

        using var after = await ConnectAsync();
        try
        {
            Assert.True(disposed.IsCompleted, "disposing did not return");
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(8), $"disposing took {clock.Elapsed}");

            // Back on the queue only once the broker has dropped the connection.
            var count = 0L;
            for (var i = 0; i < 50 && count == 0; i++)
            {
                count = await after.MessageCountAsync(queue);
                if (count == 0) await Task.Delay(200);
            }
            Assert.Equal(1, count);

            // The handler finishes late. Its acknowledgement goes nowhere: the message
            // is still there, once.
            release.SetResult();
            await returned.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await Task.Delay(500);
            Assert.Equal(1, await after.MessageCountAsync(queue));
        }
        finally
        {
            release.TrySetResult();
            foreach (var name in new[] { queue, Naming.DeadLetterQueue(queue), Naming.ParkedQueue(queue) })
            {
                try { await after.DeleteQueueAsync(name); } catch { /* already gone */ }
            }
        }
    }
}
