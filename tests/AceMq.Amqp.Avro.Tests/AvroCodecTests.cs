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

using System.Data.Common;
using System.Text;
using AceMq.Amqp;
using Microsoft.Data.Sqlite;

namespace AceMq.Amqp.Avro.Tests;

public class OrderPlaced
{
    public string orderId { get; set; } = "";
    public long totalCents { get; set; }
}

/// <summary>An order with a field the first version did not have.</summary>
public class OrderPlacedV2
{
    public string orderId { get; set; } = "";
    public long totalCents { get; set; }
    public string tenant { get; set; } = "";
}

public sealed class AvroCodecTests : IDisposable
{
    private const string V1 = @"{
      ""type"":""record"",""name"":""OrderPlaced"",""namespace"":""acemq.test"",
      ""fields"":[
        {""name"":""orderId"",""type"":""string""},
        {""name"":""totalCents"",""type"":""long""}]}";

    // A field added, with a default -- which is what makes it readable by a consumer
    // that has never heard of it.
    private const string V2 = @"{
      ""type"":""record"",""name"":""OrderPlaced"",""namespace"":""acemq.test"",
      ""fields"":[
        {""name"":""orderId"",""type"":""string""},
        {""name"":""totalCents"",""type"":""long""},
        {""name"":""tenant"",""type"":""string"",""default"":""""}]}";

    private readonly string _db = "Data Source=file:" + Guid.NewGuid().ToString("N") + "?mode=memory&cache=shared";
    private SqliteConnection? _keepAlive;

    private DbConnection Connect()
    {
        var c = new SqliteConnection(_db);
        c.Open();
        return c;
    }

    private DbSchemaRegistry Registry()
    {
        var registry = new DbSchemaRegistry(Connect);
        _keepAlive = new SqliteConnection(_db);
        _keepAlive.Open();
        foreach (var statement in registry.CreateTableSql().Split(';'))
        {
            if (statement.Trim().Length == 0) continue;
            using var command = _keepAlive.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }
        return registry;
    }

    public void Dispose() => _keepAlive?.Dispose();

    private static OrderPlaced AnOrder() => new OrderPlaced { orderId = "A-1", totalCents = 4250 };

    // ---- fixed schema ----------------------------------------------------

    [Fact]
    public void RoundTripsWithAFixedSchema()
    {
        var codec = AvroCodec.Of(V1);
        var back = (OrderPlaced)codec.Decode(codec.Encode(AnOrder()), typeof(OrderPlaced));

        Assert.Equal("A-1", back.orderId);
        Assert.Equal(4250, back.totalCents);
        Assert.Equal("avro/binary", codec.ContentType);
        Assert.False(codec.IsRegistered);
    }

    [Fact]
    public void WritesNoFramingWhenTheSchemaIsFixed()
    {
        // Nothing but the Avro body. A reader on the other side is expected to hold
        // the schema already, which is the whole trade this mode makes.
        var wire = AvroCodec.Of(V1).Encode(AnOrder());

        Assert.Equal(6, wire.Length);
        Assert.NotEqual(0x00, wire[0]);
    }

    [Fact]
    public void IsMuchSmallerThanTheJsonItReplaces()
    {
        var avro = AvroCodec.Of(V1).Encode(AnOrder()).Length;
        var json = new JsonCodec().Encode(new { orderId = "A-1", totalCents = 4250 }).Length;

        Assert.True(avro < json, $"avro {avro} bytes, json {json} bytes");
    }

    // ---- registered schema -----------------------------------------------

    [Fact]
    public void FramesEachMessageTheWayConfluentDoes()
    {
        var codec = AvroCodec.Registered(Registry(), V1);
        var wire = codec.Encode(AnOrder());

        // One zero byte, then four bytes of identifier, big-endian, then the body.
        // The same layout Confluent's clients and the Java library use, which is what
        // lets any of the three read the others.
        Assert.Equal(0x00, wire[0]);
        var id = (wire[1] << 24) | (wire[2] << 16) | (wire[3] << 8) | wire[4];
        Assert.True(id > 0);
        Assert.Equal("application/vnd.acemq.avro", codec.ContentType);
        Assert.True(codec.IsRegistered);
    }

    [Fact]
    public void RoundTripsThroughTheRegistry()
    {
        var codec = AvroCodec.Registered(Registry(), V1);
        var back = (OrderPlaced)codec.Decode(codec.Encode(AnOrder()), typeof(OrderPlaced));

        Assert.Equal("A-1", back.orderId);
        Assert.Equal(4250, back.totalCents);
    }

    [Fact]
    public void LetsAProducerAddAFieldWithoutBreakingAnOlderConsumer()
    {
        // The reason the registered mode exists. The producer moves to V2; the
        // consumer still holds V1 and has never been redeployed. Avro resolves the
        // writer's schema against the reader's and the extra field is dropped.
        var registry = Registry();

        var producer = AvroCodec.Registered(registry, V2);
        var wire = producer.Encode(new OrderPlacedV2
        {
            orderId = "A-1", totalCents = 4250, tenant = "acme",
        });

        var oldConsumer = AvroCodec.Registered(registry, V1);
        var back = (OrderPlaced)oldConsumer.Decode(wire, typeof(OrderPlaced));

        Assert.Equal("A-1", back.orderId);
        Assert.Equal(4250, back.totalCents);
    }

    [Fact]
    public void LetsANewConsumerReadWhatAnOlderProducerWrote()
    {
        // The other direction, which only works because the added field has a
        // default. Without one Avro cannot invent a value and the read fails.
        var registry = Registry();

        var oldProducer = AvroCodec.Registered(registry, V1);
        var wire = oldProducer.Encode(AnOrder());

        var newConsumer = AvroCodec.Registered(registry, V2);
        var back = (OrderPlacedV2)newConsumer.Decode(wire, typeof(OrderPlacedV2));

        Assert.Equal("A-1", back.orderId);
        Assert.Equal("", back.tenant);
    }

    [Fact]
    public void GivesTheSameSchemaOneIdentifierHoweverOftenItIsUsed()
    {
        var registry = Registry();
        var codec = AvroCodec.Registered(registry, V1);

        var first = codec.Encode(AnOrder());
        var second = codec.Encode(AnOrder());

        Assert.Equal(first.Take(5), second.Take(5));
    }

    [Fact]
    public void SaysSoWhenAFramedCodecIsGivenAnUnframedMessage()
    {
        var unframed = AvroCodec.Of(V1).Encode(AnOrder());
        var framed = AvroCodec.Registered(Registry(), V1);

        // The two modes produce different bytes, and mixing them is a configuration
        // mistake worth naming rather than a decode that quietly returns rubbish.
        var error = Assert.Throws<AceFatalException>(() => framed.Decode(unframed, typeof(OrderPlaced)));
        Assert.Contains("no schema identifier", error.Message);
    }

    // ---- the rest --------------------------------------------------------

    [Fact]
    public void RecognisesTheContentTypesInUse()
    {
        var fixedCodec = AvroCodec.Of(V1);
        Assert.True(fixedCodec.CanDecode("avro/binary"));
        Assert.True(fixedCodec.CanDecode("application/avro"));
        Assert.True(fixedCodec.CanDecode("application/vnd.acme.order+avro"));
        Assert.False(fixedCodec.CanDecode("application/json"));
        Assert.False(fixedCodec.CanDecode(null));
    }

    [Fact]
    public void TreatsAMalformedBodyAsFatalRatherThanRetryable()
    {
        // The same bytes fail the same way every time, so this dead-letters rather
        // than looping.
        var rubbish = Encoding.UTF8.GetBytes("this is definitely not avro at all");
        Assert.ThrowsAny<AceMqException>(() => AvroCodec.Of(V1).Decode(rubbish, typeof(OrderPlaced)));
    }

    [Fact]
    public async Task CarriesAMessageThroughTheBroker()
    {
        var codec = AvroCodec.Registered(Registry(), V1);
        using var mq = await AceMqConnection.ConnectAsync(
            "memory://" + Guid.NewGuid().ToString("N"), codec);
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
        Assert.Equal("A-1", received!.orderId);
    }

    [Fact]
    public void IsAvailableByNameOnceRegistered()
    {
        CodecRegistry.Register("avro", () => AvroCodec.Of(V1));

        Assert.IsType<AvroCodec>(CodecRegistry.ByName("avro"));
        Assert.Contains("avro", CodecRegistry.Names());
    }
}
