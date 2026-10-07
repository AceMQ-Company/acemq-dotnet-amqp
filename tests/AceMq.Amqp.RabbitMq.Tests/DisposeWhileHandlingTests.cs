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

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

namespace AceMq.Amqp.RabbitMq.Tests;

/// <summary>
/// Disposing a consumer while its handler is still waiting on the broker.
/// </summary>
/// <remarks>
/// The client answers channel.close-ok on the connection's reader loop and waits
/// there for the channel's handler to return. A handler waiting for a confirm --
/// which only that loop delivers -- then waits for ever, and every later declare or
/// delete on the connection timed out after 20 seconds. Only a real broker has the
/// reader loop.
/// </remarks>
public class DisposeWhileHandlingTests : IAsyncLifetime
{
    private readonly string _url =
        Environment.GetEnvironmentVariable("ACEMQ_TEST_AMQP_URL")
        ?? "amqp://guest:guest@localhost:5672";

    private readonly string _queue = $"acemq.test.{Guid.NewGuid().ToString("N").Substring(0, 8)}.dispose";
    private string Side => _queue + ".side";
    private AceMqConnection _mq = null!;

    public async Task InitializeAsync()
    {
        Transports.Register(new RabbitMqTransport());
        _mq = await AceMqConnection.ConnectAsync(_url);
        await _mq.DeclareQueueAsync(_queue);
        await _mq.DeclareQueueAsync(Side);
    }

    public async Task DisposeAsync()
    {
        foreach (var name in new[] { _queue, Side, Naming.DeadLetterQueue(_queue), Naming.ParkedQueue(_queue) })
        {
            try { await _mq.DeleteQueueAsync(name); } catch { /* already gone */ }
        }
        _mq.Dispose();
    }

    [Fact]
    public async Task DisposingAConsumerWhoseHandlerAwaitsAConfirmLeavesTheConnectionUsable()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var published = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var consumer = await _mq.ConsumeAsync<string>(_queue, async _ =>
        {
            entered.TrySetResult();
            await release.Task;
            // A confirmed publish: its answer comes back through the reader loop.
            await _mq.Publisher<string>("", Side).SendAsync("side");
            published.TrySetResult();
            return Ack.Accept();
        });

        await _mq.Publisher<string>("", _queue).SendAsync("m");
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        consumer.Dispose();
        // Long enough for the channel close to be answered while the handler is
        // still inside -- the order that wedged the connection.
        await Task.Delay(300);
        release.SetResult();

        await published.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await _mq.DeleteQueueAsync(Naming.ParkedQueue(_queue)).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, await _mq.MessageCountAsync(_queue).WaitAsync(TimeSpan.FromSeconds(5)));
    }
}
