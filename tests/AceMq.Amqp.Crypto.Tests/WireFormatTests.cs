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
using System.Text.Json;
using AceMq.Amqp;
using AceMq.Amqp.Crypto;

namespace AceMq.Amqp.Crypto.Tests;

/// <summary>
/// The bytes, against the bytes the other libraries agree on.
/// </summary>
/// <remarks>
/// Every other test in this package could pass with a framing only .NET can read —
/// that is exactly what happened up to 0.3.0. These are the ones that cannot.
/// </remarks>
public sealed class WireFormatTests
{
    /// <summary>
    /// The interoperability vector: a known key, a known key id, a known nonce and a
    /// known plaintext, and the one string of bytes every AceMQ library produces from
    /// them. Verified against Java's compiled <c>EncryptedCodec</c>, against Ruby's
    /// and against Python's before it was written down here.
    /// </summary>
    private const string Vector =
        "ae0107323032362d3031" +                 // 0xAE, version 1, keyIdLength 7, "2026-01"
        "000102030405060708090a0b" +             // the nonce
        "2f67ba77aa632797b83b1f88ef1394bb9ff6e85641";  // ciphertext + 16-byte tag

    private static readonly byte[] VectorKey = Ascending(32);
    private static readonly byte[] VectorNonce = Ascending(12);

    private static byte[] Ascending(int count)
    {
        var bytes = new byte[count];
        for (var i = 0; i < count; i++) bytes[i] = (byte)i;
        return bytes;
    }

    private static IKeyring VectorKeyring() =>
        Keyring.Of(new EncryptionKey("2026-01", VectorKey));

    [Fact]
    public void WritesTheExactBytesEveryOtherLibraryWrites()
    {
        // The nonce is the only thing that has to be pinned; everything else about
        // the framing is determined by the key, the key id and the plaintext.
        var codec = EncryptedCodec.WithNonce(
            new StringCodec(), VectorKeyring(),
            nonce => Buffer.BlockCopy(VectorNonce, 0, nonce, 0, nonce.Length));

        Assert.Equal(Vector, Hex(codec.Encode("hello")));
    }

    [Fact]
    public void ReadsTheExactBytesEveryOtherLibraryWrites()
    {
        var codec = EncryptedCodec.Wrapping(new StringCodec(), VectorKeyring());

        Assert.Equal("hello", codec.Decode(Bytes(Vector), typeof(string)));
    }

    [Fact]
    public void PutsTheKeyIdentifierOnTheWireInTheClear()
    {
        // An operator staring at an unreadable dead-letter queue needs to know which
        // key it wants, and holds none of them.
        Assert.Equal("2026-01", EncryptedCodec.KeyIdOf(Bytes(Vector)));
    }

    [Fact]
    public void FramesTheHeaderTheWayTheFamilyDoes()
    {
        var wire = EncryptedCodec.Wrapping(new StringCodec(), VectorKeyring()).Encode("hello");

        Assert.Equal(0xAE, wire[0]);                                  // magic
        Assert.Equal(0x01, wire[1]);                                  // version
        Assert.Equal(7, wire[2]);                                     // key id length, one byte
        Assert.Equal("2026-01", Encoding.UTF8.GetString(wire, 3, 7)); // the identifier itself
        Assert.Equal(3 + 7 + 12 + 5 + 16, wire.Length);               // header, nonce, "hello", tag
    }

    [Fact]
    public void BindsTheHeaderAsAssociatedDataSoAKeyIdCannotBeSteered()
    {
        // Both ids are on the ring and both are three bytes, so rewriting one to the
        // other leaves a body that is structurally perfect and names a key the reader
        // really holds. It still must not open: the header is bound as associated
        // data, so the tag covers the id.
        var keyring = Keyring.Builder()
            .Add(EncryptionKey.Generate("old"))
            .Current(EncryptionKey.Generate("new"))
            .Build();
        var codec = EncryptedCodec.Wrapping(new StringCodec(), keyring);

        var wire = codec.Encode("hello");
        Assert.Equal("new", EncryptedCodec.KeyIdOf(wire));
        wire[3] = (byte)'o'; wire[4] = (byte)'l'; wire[5] = (byte)'d';
        Assert.Equal("old", EncryptedCodec.KeyIdOf(wire));

        var error = Assert.Throws<AceFatalException>(() => codec.Decode(wire, typeof(string)));
        Assert.Contains("did not decrypt", error.Message);
    }

    /// <summary>
    /// Bodies three other implementations really wrote, decrypted here.
    /// </summary>
    /// <remarks>
    /// A codec that decrypts what it encrypted has proved nothing: both sides share
    /// the same bug. Up to 0.3.0 this library passed every round-trip test it had and
    /// could not read a single one of these.
    /// </remarks>
    [Fact]
    public void ReadsWhatJavaPythonAndRubyWrote()
    {
        var samples = InteropSamples();
        Assert.Equal(15, samples.Count);
        Assert.Equal(
            new[] { "java", "python", "ruby" },
            samples.Select(s => s.Library).Distinct().OrderBy(n => n).ToArray());

        foreach (var sample in samples)
        {
            var codec = EncryptedCodec.Wrapping(new StringCodec(), InteropKeyring());

            Assert.Equal(sample.KeyId, EncryptedCodec.KeyIdOf(sample.Body));
            Assert.Equal(
                sample.Plaintext,
                codec.Decode(sample.Body, typeof(string)));
        }
    }

    [Fact]
    public void RefusesASampleWhenTheKeyItNamesIsNotHeld()
    {
        // The same fixture, read with a keyring holding only the other key. The
        // failure has to name the key rather than the body.
        var second = InteropSamples().First(s => s.Key == "second");
        var codec = EncryptedCodec.Wrapping(
            new StringCodec(), Keyring.Of(new EncryptionKey("2026-01", VectorKey)));

        var error = Assert.Throws<AceFatalException>(
            () => codec.Decode(second.Body, typeof(string)));
        Assert.Contains("orders-2025-06-rotated-out", error.Message);
        Assert.Contains("not on the keyring", error.Message);
    }

    // ---- the fixture -----------------------------------------------------

    private sealed record Sample(string Library, string Note, string Plaintext, string KeyId, string Key, byte[] Body);

    private static JsonDocument? _fixture;

    private static JsonDocument Fixture =>
        _fixture ??= JsonDocument.Parse(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "fixtures", "crypto-interop-samples.json")));

    private static List<Sample> InteropSamples() =>
        Fixture.RootElement.GetProperty("samples").EnumerateArray().Select(s => new Sample(
            s.GetProperty("library").GetString()!,
            s.GetProperty("note").GetString()!,
            s.GetProperty("plaintext").GetString()!,
            s.GetProperty("keyId").GetString()!,
            s.GetProperty("key").GetString()!,
            Convert.FromBase64String(s.GetProperty("body").GetString()!))).ToList();

    private static IKeyring InteropKeyring()
    {
        var keys = Fixture.RootElement.GetProperty("keys");
        var builder = Keyring.Builder();
        foreach (var name in new[] { "second", "first" })
        {
            var key = keys.GetProperty(name);
            builder.Current(new EncryptionKey(
                key.GetProperty("id").GetString()!,
                Convert.FromBase64String(key.GetProperty("base64").GetString()!)));
        }
        return builder.Build();
    }

    private static string Hex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();

    private static byte[] Bytes(string hex) => Convert.FromHexString(hex);
}
