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
using AceMq.Amqp;

namespace AceMq.Amqp.Tests;

public sealed class OrderPlaced
{
    public string OrderId { get; set; } = "";
    public decimal Total { get; set; }
}

/// <summary>
/// The publish and consume path, over the in-memory transport.
/// </summary>
/// <remarks>
/// These assert behaviour that is contract rather than convenience: that an
/// unroutable message fails loudly, that a retry advances the attempt counter, and
/// that a message which cannot be decoded stops rather than looping.
/// </remarks>
public sealed class MessagingTests : IDisposable
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");

    // No InMemoryTransport.Reset() here. Every test names its own broker after a
    // fresh guid, which already isolates it; Reset clears every broker in the
    // process, and xUnit runs test classes in parallel, so calling it wipes another
    // class's broker out from under a test that is still running.
    public void Dispose() { }

    private static async Task<T> Eventually<T>(Func<T?> probe, string what) where T : class
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            var value = probe();
            if (value != null) return value;
            await Task.Delay(10);
        }
        throw new TimeoutException($"timed out waiting for {what}");
    }

    [Fact]
    public async Task DeliversAPublishedMessageToAConsumer()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareExchangeAsync("orders", "topic");
        await mq.DeclareQueueAsync("orders.placed");
        await mq.BindAsync("orders.placed", "orders", "order.placed");

        IMessage<OrderPlaced>? received = null;
        using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", msg =>
        {
            received = msg;
            return Task.FromResult(Ack.Accept());
        });

        var publisher = mq.Publisher<OrderPlaced>("orders", "order.placed");
        var result = await publisher.SendAsync(new OrderPlaced { OrderId = "A-1", Total = 42.5m });

        Assert.True(result.Routed);
        var message = await Eventually(() => received, "the message to arrive");
        Assert.Equal("A-1", message.Payload.OrderId);
        Assert.Equal(42.5m, message.Payload.Total);
        Assert.Equal(result.MessageId, message.Envelope.Id);
        Assert.True(message.IsFirstAttempt);
    }

    [Fact]
    public async Task FailsAPublishThatMatchesNoQueue()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareExchangeAsync("orders", "topic");

        var publisher = mq.Publisher<OrderPlaced>("orders", "order.placed");

        // Nothing is bound. Dropping this silently is how a topology mistake
        // survives into production as an absence of messages nobody can explain.
        var error = await Assert.ThrowsAsync<PublishFailedException>(
            () => publisher.SendAsync(new OrderPlaced { OrderId = "A-2" }));
        Assert.Contains("matched no queue", error.Message);
    }

    [Fact]
    public async Task AllowsAnUnroutablePublishWhenAskedTo()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareExchangeAsync("orders", "topic");

        var publisher = mq.Publisher<OrderPlaced>(
            "orders", "order.placed", PublishOptions.Defaults().AllowUnroutable());

        var result = await publisher.SendAsync(new OrderPlaced { OrderId = "A-3" });
        Assert.False(result.Routed);
    }

    [Fact]
    public async Task AdvancesTheAttemptCounterOnRetry()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("work");

        var attempts = new ConcurrentQueue<int>();
        using var consumer = await mq.ConsumeAsync<OrderPlaced>("work", msg =>
        {
            attempts.Enqueue(msg.Attempt);
            return Task.FromResult(
                msg.Attempt < 3
                    ? Ack.Retry(TimeSpan.FromMilliseconds(5), "not yet")
                    : Ack.Accept());
        });

        // The default exchange routes by queue name, which is how the first
        // example in every AMQP tutorial is written.
        var publisher = mq.Publisher<OrderPlaced>("", "work");
        await publisher.SendAsync(new OrderPlaced { OrderId = "A-4" });

        await Eventually(() => attempts.Count >= 3 ? "done" : null, "three attempts");
        Assert.Equal(new[] { 1, 2, 3 }, attempts.Take(3).ToArray());
    }

    [Fact]
    public async Task DeadLettersAMessageItCannotDecodeRatherThanLoopingOnIt()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("work");

        var handled = 0;
        using var consumer = await mq.ConsumeAsync<OrderPlaced>("work", msg =>
        {
            Interlocked.Increment(ref handled);
            return Task.FromResult(Ack.Accept());
        });

        // Publish something the consumer's type cannot parse. Retrying this forever
        // is how one malformed message becomes an outage.
        using var bytes = await AceMqConnection.ConnectAsync(_url, new BytesCodec());
        await bytes.Publisher<string>("", "work").SendAsync("not json at all");

        var broker = _url.Substring("memory://".Length);
        await Eventually(
            () => InMemoryTransport.DeadLettered(broker, "work").Count > 0 ? "dead-lettered" : null,
            "the undecodable message to be dead-lettered");

        // It reached the dead-letter list rather than the handler, and the reason
        // says what could not be read rather than only that something failed.
        Assert.Equal(0, handled);
        var dead = InMemoryTransport.DeadLettered(broker, "work").Single();
        Assert.Contains("could not decode as OrderPlaced",
            dead.Headers[AceHeaders.Error].ToString());
    }

    [Fact]
    public async Task ReportsTheBrokerAndItsCapabilities()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        Assert.Equal("in-memory", mq.TransportName);
        Assert.True(mq.Supports(Capability.PublisherConfirms));
        Assert.True(mq.IsOpen);
    }

    [Fact]
    public async Task RefusesAUrlWithNoTransportRegisteredForItsScheme()
    {
        var error = await Assert.ThrowsAsync<AceFatalException>(
            () => AceMqConnection.ConnectAsync("kafka://localhost"));
        Assert.Contains("no transport registered for scheme 'kafka'", error.Message);
    }

    [Fact]
    public async Task CountsWhatIsWaitingOnAQueue()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        await mq.DeclareQueueAsync("parked");

        var publisher = mq.Publisher<OrderPlaced>("", "parked");
        await publisher.SendAsync(new OrderPlaced { OrderId = "A-5" });
        await publisher.SendAsync(new OrderPlaced { OrderId = "A-6" });

        Assert.Equal(2, await mq.MessageCountAsync("parked"));
    }
}
