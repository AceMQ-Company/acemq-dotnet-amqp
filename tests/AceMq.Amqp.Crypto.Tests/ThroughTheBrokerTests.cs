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
using AceMq.Amqp.Crypto;

namespace AceMq.Amqp.Crypto.Tests;

/// <summary>
/// The codec as a connection actually uses it, not as a unit test calls it.
/// </summary>
public sealed class ThroughTheBrokerTests
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");
    private readonly string _q = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);

    [Fact]
    public async Task CarriesAnEncryptedPayloadThroughTheBroker()
    {
        var codec = EncryptedCodec.Wrapping(
            new JsonCodec(), Keyring.Of(EncryptionKey.Generate("k1")));

        using var mq = await AceMqConnection.ConnectAsync(_url, codec);
        await mq.DeclareQueueAsync(_q);

        Order? received = null;
        using var consumer = await mq.ConsumeAsync<Order>(_q, m =>
        {
            received = m.Payload;
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<Order>("", _q).SendAsync(new Order { Id = "A-9", Total = 5m });

        await Eventually(() => received != null, "the encrypted message");
        Assert.Equal("A-9", received!.Id);
    }

    [Fact]
    public async Task StampsTheEncryptedContentTypeOnWhatItPublishes()
    {
        // Not the delegate's. A `+json` suffix would make every JSON-aware consumer
        // read ciphertext as JSON and fail in a parser rather than in a codec.
        var codec = EncryptedCodec.Wrapping(
            new JsonCodec(), Keyring.Of(EncryptionKey.Generate("k1")));

        using var mq = await AceMqConnection.ConnectAsync(_url, codec);
        await mq.DeclareQueueAsync(_q);

        string? contentType = null;
        using var consumer = await mq.ConsumeAsync<Order>(_q, m =>
        {
            contentType = m.ContentType;
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<Order>("", _q).SendAsync(new Order { Id = "A-1", Total = 1m });

        await Eventually(() => contentType != null, "the content type");
        Assert.Equal("application/vnd.acemq.encrypted", contentType);
    }

    private static async Task Eventually(Func<bool> condition, string what)
    {
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < TimeSpan.FromSeconds(5))
        {
            if (condition()) return;
            await Task.Delay(10);
        }
        Assert.Fail($"timed out waiting for {what}");
    }
}
