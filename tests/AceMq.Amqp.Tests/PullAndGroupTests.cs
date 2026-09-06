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

namespace AceMq.Amqp.Tests;

public sealed class PulledOrder
{
    public string OrderId { get; set; } = "";
}

public sealed class PullTests
{
    private static Task<AceMqConnection> ConnectAsync() =>
        AceMqConnection.ConnectAsync("memory://" + Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task PullsOneMessage()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");
        await mq.Publisher<PulledOrder>("", "orders").SendAsync(new PulledOrder { OrderId = "o-1" });

        var pulled = await mq.PullAsync<PulledOrder>("orders", TimeSpan.FromSeconds(2));

        Assert.NotNull(pulled);
        Assert.Equal("o-1", pulled!.Payload.OrderId);
        Assert.Equal("orders", pulled.Queue);
        Assert.False(pulled.IsSettled);

        await pulled.AcknowledgeAsync();
        Assert.True(pulled.IsSettled);
    }

    [Fact]
    public async Task ReturnsNullWhenThereIsNothing()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");

        var pulled = await mq.PullAsync<PulledOrder>("orders", TimeSpan.FromMilliseconds(100));

        Assert.Null(pulled);
    }

    // The property that makes this a pull rather than a read: the broker keeps
    // the message until the caller says what happened to it.
    [Fact]
    public async Task RejectingWithRequeuePutsItBack()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");
        await mq.Publisher<PulledOrder>("", "orders").SendAsync(new PulledOrder { OrderId = "o-1" });

        var first = await mq.PullAsync<PulledOrder>("orders", TimeSpan.FromSeconds(2));
        Assert.NotNull(first);
        await first!.RejectAsync(requeue: true);

        var again = await mq.PullAsync<PulledOrder>("orders", TimeSpan.FromSeconds(2));

        Assert.NotNull(again);
        Assert.Equal("o-1", again!.Payload.OrderId);
        // And it says it has been seen before, so a caller counting attempts
        // has something to count.
        Assert.True(again.Redelivered);
        await again.AcknowledgeAsync();
    }

    [Fact]
    public async Task RejectingWithoutRequeueDropsIt()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");
        await mq.Publisher<PulledOrder>("", "orders").SendAsync(new PulledOrder { OrderId = "o-1" });

        var pulled = await mq.PullAsync<PulledOrder>("orders", TimeSpan.FromSeconds(2));
        Assert.NotNull(pulled);
        await pulled!.RejectAsync(requeue: false);

        var again = await mq.PullAsync<PulledOrder>("orders", TimeSpan.FromMilliseconds(200));
        Assert.Null(again);
    }

    // A caller with an explicit call and a finally is the ordinary way to
    // settle twice, and on RabbitMQ a second acknowledgement is a channel
    // error — so it has to be harmless here.
    [Fact]
    public async Task SettlingTwiceIsHarmless()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");
        await mq.Publisher<PulledOrder>("", "orders").SendAsync(new PulledOrder { OrderId = "o-1" });

        var pulled = await mq.PullAsync<PulledOrder>("orders", TimeSpan.FromSeconds(2));
        Assert.NotNull(pulled);

        await pulled!.AcknowledgeAsync();
        await pulled.AcknowledgeAsync();
        await pulled.RejectAsync(requeue: true);

        // The reject after the acknowledgement did nothing, so the message is
        // gone rather than back on the queue.
        var again = await mq.PullAsync<PulledOrder>("orders", TimeSpan.FromMilliseconds(200));
        Assert.Null(again);
    }

    [Fact]
    public async Task ReservedHeadersDoNotReachTheApplication()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");
        await mq.Publisher<PulledOrder>("", "orders").SendAsync(
            new PulledOrder { OrderId = "o-1" },
            Envelope.Of("order.placed").Header("x-tenant", "acme").Build());

        var pulled = await mq.PullAsync<PulledOrder>("orders", TimeSpan.FromSeconds(2));

        Assert.NotNull(pulled);
        Assert.Equal("acme", pulled!.Headers["x-tenant"]);
        Assert.DoesNotContain(pulled.Headers.Keys, key => key.StartsWith(AceHeaders.Prefix));
        await pulled.AcknowledgeAsync();
    }
}

public sealed class ConsumerGroupTests
{
    private static Task<AceMqConnection> ConnectAsync() =>
        AceMqConnection.ConnectAsync("memory://" + Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task SpreadsWorkAcrossItsConsumers()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");

        var handled = 0;
        using var group = await ConsumerGroup.StartAsync<PulledOrder>(mq, "orders", 4, _ =>
        {
            Interlocked.Increment(ref handled);
            return Task.FromResult(Ack.Accept());
        });

        Assert.Equal(4, group.Size);
        Assert.Equal("orders", group.Queue);

        var publisher = mq.Publisher<PulledOrder>("", "orders");
        for (var i = 0; i < 20; i++)
        {
            await publisher.SendAsync(new PulledOrder { OrderId = $"o-{i}" });
        }

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (Volatile.Read(ref handled) < 20 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.Equal(20, Volatile.Read(ref handled));
    }

    [Fact]
    public async Task NeedsAtLeastOneConsumer()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");

        await Assert.ThrowsAsync<ArgumentException>(() =>
            ConsumerGroup.StartAsync<PulledOrder>(
                mq, "orders", 0, _ => Task.FromResult(Ack.Accept())));
    }

    [Fact]
    public async Task ClosingTwiceIsHarmless()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");

        var group = await ConsumerGroup.StartAsync<PulledOrder>(
            mq, "orders", 2, _ => Task.FromResult(Ack.Accept()));

        group.Dispose();
        group.Dispose();

        Assert.Equal(0, group.Size);
    }

    [Fact]
    public async Task StopsDeliveringOnceClosed()
    {
        using var mq = await ConnectAsync();
        await mq.DeclareQueueAsync("orders");

        var handled = 0;
        var group = await ConsumerGroup.StartAsync<PulledOrder>(mq, "orders", 2, _ =>
        {
            Interlocked.Increment(ref handled);
            return Task.FromResult(Ack.Accept());
        });

        group.Dispose();

        await mq.Publisher<PulledOrder>("", "orders").SendAsync(new PulledOrder { OrderId = "o-1" });
        await Task.Delay(300);

        Assert.Equal(0, Volatile.Read(ref handled));
    }
}
