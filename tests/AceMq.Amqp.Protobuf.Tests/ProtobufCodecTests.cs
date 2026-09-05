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

using System.Text;
using AceMq.Amqp;
using Google.Protobuf;

namespace AceMq.Amqp.Protobuf.Tests;

/// <summary>
/// The Protobuf codec, against a type protoc actually generated.
/// </summary>
/// <remarks>
/// The generated type is the point. A hand-written stand-in implementing
/// <c>IMessage</c> would prove the codec calls the right methods and nothing about
/// whether the bytes are what protobuf says they should be.
/// </remarks>
public sealed class ProtobufCodecTests
{
    private readonly ProtobufCodec _codec = new ProtobufCodec();

    private static OrderPlaced AnOrder() => new OrderPlaced
    {
        OrderId = "A-1",
        TotalCents = 4250,
        Tenant = "acme",
    };

    [Fact]
    public void RoundTripsAGeneratedMessage()
    {
        var wire = _codec.Encode(AnOrder());
        var back = (OrderPlaced)_codec.Decode(wire, typeof(OrderPlaced));

        Assert.Equal("A-1", back.OrderId);
        Assert.Equal(4250, back.TotalCents);
        Assert.Equal("acme", back.Tenant);
    }

    [Fact]
    public void WritesTheSameBytesProtobufItselfWrites()
    {
        // The codec must not wrap, frame or otherwise decorate the message. A
        // consumer in another language reads protobuf, not protobuf-inside-something.
        var order = AnOrder();
        Assert.Equal(order.ToByteArray(), _codec.Encode(order));
    }

    [Fact]
    public void IsSmallerThanTheJsonItReplaces()
    {
        // The reason anybody chooses it. Asserted rather than claimed, because a
        // codec that were not smaller would be all cost and no benefit.
        var protobuf = _codec.Encode(AnOrder()).Length;
        var json = new JsonCodec().Encode(
            new { orderId = "A-1", totalCents = 4250, tenant = "acme" }).Length;

        Assert.True(protobuf < json, $"protobuf {protobuf} bytes, json {json} bytes");
    }

    [Fact]
    public void DeclaresTheContentTypeJavaDeclares()
    {
        Assert.Equal("application/x-protobuf", _codec.ContentType);

        // Both spellings are in use, and a schema registry usually names its own
        // wrapping with a +protobuf suffix.
        Assert.True(_codec.CanDecode("application/x-protobuf"));
        Assert.True(_codec.CanDecode("application/protobuf"));
        Assert.True(_codec.CanDecode("application/vnd.acme.order+protobuf"));
        Assert.False(_codec.CanDecode("application/json"));
        Assert.False(_codec.CanDecode(null));
    }

    [Fact]
    public void RefusesAPlainClassRatherThanEncodingSomethingUnreadable()
    {
        // There is no reflection-based fallback on purpose: bytes produced that way
        // would not be readable by anything else that speaks protobuf, and would
        // look like they worked until another service tried to read them.
        var error = Assert.Throws<AceFatalException>(
            () => _codec.Encode(new { OrderId = "A-1" }));
        Assert.Contains("not a generated protobuf message", error.Message);
    }

    [Fact]
    public void RefusesToDecodeIntoAPlainClass()
    {
        var error = Assert.Throws<AceFatalException>(
            () => _codec.Decode(_codec.Encode(AnOrder()), typeof(string)));
        Assert.Contains("not a generated protobuf message", error.Message);
    }

    [Fact]
    public void TreatsAMalformedBodyAsFatalRatherThanRetryable()
    {
        // A wire-format failure fails identically on every attempt. Fatal is what
        // dead-letters it instead of putting it back on the queue forever.
        var rubbish = Encoding.UTF8.GetBytes("this is definitely not protobuf");
        Assert.ThrowsAny<AceMqException>(() => _codec.Decode(rubbish, typeof(OrderPlaced)));
    }

    [Fact]
    public async Task CarriesAGeneratedMessageThroughTheBroker()
    {
        using var mq = await AceMqConnection.ConnectAsync(
            "memory://" + Guid.NewGuid().ToString("N"), _codec);
        var queue = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);
        await mq.DeclareQueueAsync(queue);

        OrderPlaced? received = null;
        using var consumer = await mq.ConsumeAsync<OrderPlaced>(queue, message =>
        {
            received = message.Payload;
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<OrderPlaced>("", queue).SendAsync(AnOrder());

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (received == null && DateTime.UtcNow < deadline) await Task.Delay(10);

        Assert.NotNull(received);
        Assert.Equal("A-1", received!.OrderId);
        Assert.Equal(4250, received.TotalCents);
    }

    [Fact]
    public async Task ReadsProtobufOnOneQueueWhileTheServiceDefaultsToJson()
    {
        // The migration case: another service publishes protobuf, this one still
        // defaults to JSON, and one consumer overrides the codec for its queue.
        var url = "memory://" + Guid.NewGuid().ToString("N");
        var queue = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);

        using var consuming = await AceMqConnection.ConnectAsync(url);          // JSON default
        using var publishing = await AceMqConnection.ConnectAsync(url, _codec); // protobuf
        await consuming.DeclareQueueAsync(queue);

        OrderPlaced? received = null;
        using var consumer = await consuming.ConsumeAsync<OrderPlaced>(
            queue, ConsumerOptions.Defaults().As(_codec), message =>
            {
                received = message.Payload;
                return Task.FromResult(Ack.Accept());
            });

        await publishing.Publisher<OrderPlaced>("", queue).SendAsync(AnOrder());

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (received == null && DateTime.UtcNow < deadline) await Task.Delay(10);

        Assert.NotNull(received);
        Assert.Equal("A-1", received!.OrderId);
    }

    [Fact]
    public void IsAvailableByNameOnceRegistered()
    {
        CodecRegistry.Register("protobuf", () => new ProtobufCodec());

        Assert.IsType<ProtobufCodec>(CodecRegistry.ByName("protobuf"));
        Assert.Contains("protobuf", CodecRegistry.Names());
    }
}
