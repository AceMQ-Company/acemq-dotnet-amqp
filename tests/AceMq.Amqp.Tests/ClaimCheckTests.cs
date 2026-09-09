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

namespace AceMq.Amqp.Tests;

/// <summary>
/// The claim-check framing, against what Java, Python and Ruby put on the wire.
/// </summary>
/// <remarks>
/// Three bytes of header — <c>0xAC 0x01</c> and then <c>0x00</c> for an inline payload
/// or <c>0x01</c> for a key — and a threshold of 64 KiB compared strictly less than, so
/// a payload of exactly that size is offloaded. Every one of those is asserted here as
/// a number rather than described, because a port that hand-copies a wire contract
/// acquires a difference nobody notices until two languages disagree in production.
/// </remarks>
public sealed class ClaimCheckTests : IDisposable
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");
    private readonly string _q = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);
    private readonly List<string> _directories = new List<string>();

    public void Dispose()
    {
        foreach (var directory in _directories)
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    private string TempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "acemq-cc-" + Guid.NewGuid().ToString("N"));
        _directories.Add(path);
        return path;
    }

    private static byte[] Big(int size) => Encoding.UTF8.GetBytes(new string('x', size));

    // ---- the framing -----------------------------------------------------

    [Fact]
    public void KeepsTheThresholdJavaKeeps()
    {
        Assert.Equal(65536, ClaimCheckCodec.DefaultThreshold);
        Assert.Equal(64 * 1024, ClaimCheckCodec.DefaultThreshold);
    }

    [Fact]
    public void FramesASmallPayloadInlineAndLeavesItAlone()
    {
        var store = new InMemoryClaimCheckStore();
        var codec = ClaimCheckCodec.Wrapping(new BytesCodec(), store);

        var body = codec.Encode(Encoding.UTF8.GetBytes("small"));

        Assert.Equal(0xAC, body[0]);
        Assert.Equal(0x01, body[1]);
        Assert.Equal(0x00, body[2]);
        // Identical to what the delegate wrote, after the three-byte header. Offloading
        // a two-hundred-byte message would turn one broker round trip into a store
        // round trip and a broker round trip.
        Assert.Equal("small", Encoding.UTF8.GetString(body, 3, body.Length - 3));
        Assert.Equal(0, store.Count);
        Assert.Null(ClaimCheckCodec.KeyOf(body));
        Assert.False(ClaimCheckCodec.IsClaimCheck(body));
    }

    [Fact]
    public void FramesALargePayloadAsAKey()
    {
        var store = new InMemoryClaimCheckStore();
        var codec = ClaimCheckCodec.Wrapping(new BytesCodec(), store);

        var payload = Big(ClaimCheckCodec.DefaultThreshold);
        var body = codec.Encode(payload);

        Assert.Equal(0xAC, body[0]);
        Assert.Equal(0x01, body[1]);
        Assert.Equal(0x01, body[2]);
        Assert.Equal(1, store.Count);
        Assert.True(ClaimCheckCodec.IsClaimCheck(body));

        var key = ClaimCheckCodec.KeyOf(body)!;
        Assert.Equal(key, Encoding.UTF8.GetString(body, 3, body.Length - 3));
        Assert.Equal(payload, store.Get(key));

        // Three bytes plus a key, which is the whole point: a forty-megabyte report
        // does not go near the broker.
        Assert.True(body.Length < 200);
    }

    [Fact]
    public void ComparesTheThresholdStrictlyLessThan()
    {
        var store = new InMemoryClaimCheckStore();
        var codec = ClaimCheckCodec.Wrapping(new BytesCodec(), store, 100);

        // Java offloads at `encoded.length < threshold ? inline : checked`, so 99 is
        // inline and 100 is not. One byte either side, because "roughly a threshold"
        // is how two libraries end up disagreeing about a message.
        Assert.Equal(0x00, codec.Encode(Big(99))[2]);
        Assert.Equal(0x01, codec.Encode(Big(100))[2]);
        Assert.Equal(0x01, codec.Encode(Big(101))[2]);
        Assert.Equal(2, store.Count);
    }

    [Fact]
    public void OffloadsEverythingAtAThresholdOfZero()
    {
        // Occasionally what a store-backed audit trail wants.
        var store = new InMemoryClaimCheckStore();
        var codec = ClaimCheckCodec.Wrapping(new BytesCodec(), store, 0);

        Assert.Equal(0x01, codec.Encode(Encoding.UTF8.GetBytes(""))[2]);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void RefusesANegativeThreshold() =>
        Assert.Throws<ArgumentException>(
            () => ClaimCheckCodec.Wrapping(new BytesCodec(), new InMemoryClaimCheckStore(), -1));

    // ---- reading ---------------------------------------------------------

    [Fact]
    public void ReadsBackWhatItWroteEitherWay()
    {
        var store = new InMemoryClaimCheckStore();
        var codec = ClaimCheckCodec.Wrapping(new JsonCodec(), store, 64);

        var small = new Document("short", "a");
        var large = new Document("long", new string('x', 500));

        Assert.Equal(small, codec.Decode<Document>(codec.Encode(small)));
        Assert.Equal(large, codec.Decode<Document>(codec.Encode(large)));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void ReadsAMessageWrittenBeforeThisCodecExisted()
    {
        // Unframed: written by a publisher that does not use this codec, or before it
        // was introduced. Reading it as the delegate would is the only useful answer,
        // and it is what makes adding a claim check to a live queue safe.
        var codec = ClaimCheckCodec.Wrapping(new JsonCodec(), new InMemoryClaimCheckStore());
        var plain = new JsonCodec().Encode(new Document("plain", "body"));

        Assert.Equal(new Document("plain", "body"), codec.Decode<Document>(plain));
    }

    [Fact]
    public void SaysWhichPayloadIsMissingWhenRetentionExpiredUnderIt()
    {
        var store = new InMemoryClaimCheckStore();
        var codec = ClaimCheckCodec.Wrapping(new BytesCodec(), store, 0);
        var body = codec.Encode(Encoding.UTF8.GetBytes("gone"));
        var key = ClaimCheckCodec.KeyOf(body)!;

        store.Delete(key);

        var error = Assert.Throws<AceMqException>(() => codec.Decode<byte[]>(body));
        Assert.Contains(key, error.Message);
        Assert.Contains("retention", error.Message);
    }

    [Fact]
    public void KeepsTheDelegatesContentType()
    {
        // Unlike encryption, where the bytes really are something else. A
        // claim-checked document is still a document that is somewhere else, and a
        // consumer that lacks the store gets a clear failure rather than a parser error.
        var codec = ClaimCheckCodec.Wrapping(new JsonCodec(), new InMemoryClaimCheckStore());

        Assert.Equal("application/json", codec.ContentType);
        Assert.True(codec.CanDecode("application/json"));
        Assert.False(codec.CanDecode("application/xml"));
        Assert.IsType<JsonCodec>(codec.Delegate);
    }

    [Fact]
    public void ReadsNothingOutOfAKeyThatIsNotThere()
    {
        Assert.Null(ClaimCheckCodec.KeyOf(null));
        Assert.Null(ClaimCheckCodec.KeyOf(new byte[0]));
        Assert.Null(ClaimCheckCodec.KeyOf(new byte[] { 0xAC, 0x01 }));
        // The right magic and a marker byte that is neither of the two.
        Assert.Null(ClaimCheckCodec.KeyOf(new byte[] { 0xAC, 0x01, 0x09, 0x41 }));
    }

    // ---- the stores ------------------------------------------------------

    [Fact]
    public void CopiesWhatItIsGivenAndWhatItHandsBack()
    {
        // A codec is entitled to reuse a buffer, so a store that keeps somebody else's
        // array is a store whose contents change after they were stored.
        var store = new InMemoryClaimCheckStore();
        var content = Encoding.UTF8.GetBytes("original");
        var key = store.Put(content);

        content[0] = (byte)'X';
        Assert.Equal("original", Encoding.UTF8.GetString(store.Get(key)!));

        store.Get(key)![0] = (byte)'Y';
        Assert.Equal("original", Encoding.UTF8.GetString(store.Get(key)!));
    }

    [Fact]
    public void ReportsAKeyItDoesNotHold()
    {
        var store = new InMemoryClaimCheckStore();
        Assert.Null(store.Get("never-stored"));

        var key = store.Put(new byte[] { 1 });
        store.Delete(key);
        Assert.Null(store.Get(key));
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void KeepsPayloadsOnDisk()
    {
        var store = new FilesystemClaimCheckStore(TempDirectory());
        var key = store.Put(Encoding.UTF8.GetBytes("on disk"));

        Assert.True(File.Exists(Path.Combine(store.Location, key)));
        Assert.Equal("on disk", Encoding.UTF8.GetString(store.Get(key)!));

        // No half-written file left behind: the payload is staged and moved into place,
        // so a reader sees the whole payload or no payload.
        Assert.Empty(Directory.GetFiles(store.Location, "*.partial"));

        store.Delete(key);
        Assert.Null(store.Get(key));
    }

    [Fact]
    public void RefusesAKeyItDidNotIssue()
    {
        // A key becomes a path segment, and one arriving from a message is whatever a
        // publisher put there.
        var store = new FilesystemClaimCheckStore(TempDirectory());

        var error = Assert.Throws<AceMqException>(() => store.Get("../../etc/passwd"));
        Assert.Contains("checked rather than trusted", error.Message);
    }

    // ---- through a broker ------------------------------------------------

    [Fact]
    public async Task KeepsALargePayloadOffTheQueueAndHandsItBackWhole()
    {
        var store = new FilesystemClaimCheckStore(TempDirectory());
        var codec = ClaimCheckCodec.Wrapping(new JsonCodec(), store);
        using var mq = await AceMqConnection.ConnectAsync(_url, codec);

        await mq.DeclareQueueAsync(_q);

        Document? received = null;
        using var consumer = await mq.ConsumeAsync<Document>(_q, message =>
        {
            received = message.Payload;
            return Task.FromResult(Ack.Accept());
        });

        var report = new Document("scan", new string('m', 200_000));
        await mq.Publisher<Document>(string.Empty, _q).SendAsync(report);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (received == null && DateTime.UtcNow < deadline) await Task.Delay(10);

        // Whole, and what the broker carried was three bytes and a key.
        Assert.Equal(report, received);
        Assert.Single(Directory.GetFiles(store.Location));
    }

    [Fact]
    public async Task StillCarriesASmallPayloadOnTheBroker()
    {
        var store = new FilesystemClaimCheckStore(TempDirectory());
        using var mq = await AceMqConnection.ConnectAsync(
            _url, ClaimCheckCodec.Wrapping(new JsonCodec(), store));

        await mq.DeclareQueueAsync(_q);

        Document? received = null;
        using var consumer = await mq.ConsumeAsync<Document>(_q, message =>
        {
            received = message.Payload;
            return Task.FromResult(Ack.Accept());
        });

        var note = new Document("note", "two lines");
        await mq.Publisher<Document>(string.Empty, _q).SendAsync(note);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (received == null && DateTime.UtcNow < deadline) await Task.Delay(10);

        Assert.Equal(note, received);
        Assert.Empty(Directory.GetFiles(store.Location));
    }

    public sealed record Document(string Name, string Body);
}
