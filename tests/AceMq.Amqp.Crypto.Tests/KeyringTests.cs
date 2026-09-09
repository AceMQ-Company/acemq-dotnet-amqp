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
using AceMq.Amqp.Crypto;

namespace AceMq.Amqp.Crypto.Tests;

public sealed class KeyringTests
{
    [Fact]
    public void RefusesAKeyThatIsNotThirtyTwoBytes()
    {
        foreach (var size in new[] { 0, 16, 24, 31, 33 })
        {
            var error = Assert.Throws<SecurityConfigurationException>(
                () => new EncryptionKey("k1", new byte[size]));
            Assert.Contains("must be 32 bytes", error.Message);
        }

        Assert.Equal(32, EncryptionKey.KeySize);
    }

    [Fact]
    public void RefusesAKeyIdLongerThanTheLengthByteCanCarry()
    {
        // One byte of length, in every AceMQ library. An id that could not be written
        // has to be refused when the key is made, not when the first message is sent.
        Assert.Equal(255, EncryptionKey.MaxIdBytes);

        var key = new byte[32];
        _ = new EncryptionKey(new string('a', 255), key);

        var error = Assert.Throws<SecurityConfigurationException>(
            () => new EncryptionKey(new string('a', 256), key));
        Assert.Contains("at most 255 bytes", error.Message);

        // Counted in UTF-8 bytes, not characters: 128 two-byte characters is 256.
        Assert.Throws<SecurityConfigurationException>(
            () => new EncryptionKey(new string('é', 128), key));
    }

    [Fact]
    public void RefusesAKeyWithNoId()
    {
        Assert.Throws<ArgumentException>(() => new EncryptionKey("", new byte[32]));
        Assert.Throws<ArgumentException>(() => new EncryptionKey(null!, new byte[32]));
        Assert.Throws<ArgumentNullException>(() => new EncryptionKey("k1", null!));
    }

    [Fact]
    public void CopiesTheKeySoTheCallersArrayCannotChangeIt()
    {
        var raw = new byte[32];
        for (var i = 0; i < 32; i++) raw[i] = (byte)i;

        var key = new EncryptionKey("2026-01", raw);
        var codec = EncryptedCodec.Wrapping(new StringCodec(), Keyring.Of(key));
        var wire = codec.Encode("hello");

        Array.Clear(raw, 0, raw.Length);

        Assert.Equal("hello", codec.Decode(wire, typeof(string)));
    }

    [Fact]
    public void GeneratesADifferentKeyEachTime()
    {
        var a = EncryptionKey.Generate("k1");
        var b = EncryptionKey.Generate("k1");

        var codec = EncryptedCodec.Wrapping(new StringCodec(), Keyring.Of(a));
        var other = EncryptedCodec.Wrapping(new StringCodec(), Keyring.Of(b));

        Assert.Throws<AceFatalException>(
            () => other.Decode(codec.Encode("hello"), typeof(string)));
    }

    [Fact]
    public void NeedsACurrentKey()
    {
        var error = Assert.Throws<SecurityConfigurationException>(() => Keyring.Builder().Build());
        Assert.Contains("needs a current key", error.Message);

        Assert.Throws<SecurityConfigurationException>(
            () => Keyring.Builder().Add(EncryptionKey.Generate("k1")).Build());
    }

    [Fact]
    public void HandsBackOnlyTheKeysItHolds()
    {
        var keyring = Keyring.Builder()
            .Add(EncryptionKey.Generate("2025-07"))
            .Current(EncryptionKey.Generate("2026-01"))
            .Build();

        Assert.Equal("2026-01", keyring.Current.Id);
        Assert.Equal("2025-07", keyring.KeyFor("2025-07")!.Id);
        Assert.Null(keyring.KeyFor("2024-01"));
        Assert.Null(keyring.KeyFor(null!));

        // Ordinal, not culture-aware: a key id is bytes on a wire, not a word.
        Assert.Null(keyring.KeyFor("2025-07 "));
    }

    [Fact]
    public void CarriesAKeyIdOfEveryLengthTheByteAllows()
    {
        foreach (var length in new[] { 1, 7, 26, 255 })
        {
            var id = new string('k', length);
            var codec = EncryptedCodec.Wrapping(
                new StringCodec(), Keyring.Of(EncryptionKey.Generate(id)));

            var wire = codec.Encode("hello");
            Assert.Equal((byte)length, wire[2]);
            Assert.Equal(id, EncryptedCodec.KeyIdOf(wire));
            Assert.Equal("hello", codec.Decode(wire, typeof(string)));
        }
    }

    [Fact]
    public void CarriesANonAsciiKeyIdAsUtf8()
    {
        var codec = EncryptedCodec.Wrapping(
            new StringCodec(), Keyring.Of(EncryptionKey.Generate("clés-2026")));

        var wire = codec.Encode("hello");
        Assert.Equal((byte)Encoding.UTF8.GetByteCount("clés-2026"), wire[2]);
        Assert.Equal("clés-2026", EncryptedCodec.KeyIdOf(wire));
    }
}
