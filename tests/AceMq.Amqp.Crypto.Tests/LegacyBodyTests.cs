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
using AceMq.Amqp.Crypto;

namespace AceMq.Amqp.Crypto.Tests;

/// <summary>
/// Reading what 0.3.0 wrote, so an upgrade does not strand a queue.
/// </summary>
/// <remarks>
/// <strong>These test a migration affordance, not a feature.</strong> Nothing writes
/// this framing any more, and the reader goes in 1.0.0. What has to hold until then
/// is that a body written by 0.3.0 still opens, and that the two framings are told
/// apart with certainty rather than guessed at.
/// </remarks>
public sealed class LegacyBodyTests
{
    /// <summary>
    /// A body AceMq.Amqp 0.3.0 really wrote, kept verbatim.
    /// </summary>
    /// <remarks>
    /// Produced by running 0.3.0's own <c>EncryptedCodec.Encode</c> over
    /// <c>StringCodec</c> with the key below and the plaintext "hello", before the
    /// writer was removed. Reimplementing the old framing in the test and asserting
    /// against that would prove only that the test and the reader agree.
    /// <code>
    /// [version:1][keyIdLength:1][keyId][iv:16][ciphertext][tag:32]
    /// </code>
    /// </remarks>
    private const string BodyWrittenBy030 =
        "01" +                                                                // version, and no magic
        "07" + "323032362d3031" +                                             // keyIdLength 7, "2026-01"
        "6e4bb408a72f15b20e6d57b68a094e2a" +                                  // the 16-byte IV
        "a6b104407f7091760414b5b107a8546e" +                                  // AES-256-CBC ciphertext
        "4ef4e054c4a694fafc89f4fdca7e3dc3c1faf726f4bf407f479908218b8b1e2f";   // HMAC-SHA-256, 32 bytes

    private static byte[] Key()
    {
        var key = new byte[32];
        for (var i = 0; i < 32; i++) key[i] = (byte)i;
        return key;
    }

    private static IKeyring Keys() => Keyring.Of(new EncryptionKey("2026-01", Key()));

    private static byte[] Legacy() => Convert.FromHexString(BodyWrittenBy030);

    [Fact]
    public void ReadsABodyWrittenByTheReleaseBeforeThisOne()
    {
        var codec = EncryptedCodec.Wrapping(new StringCodec(), Keys());

        Assert.Equal("hello", codec.Decode(Legacy(), typeof(string)));
    }

    [Fact]
    public void TellsTheTwoFramingsApartByTheirFirstByte()
    {
        var legacy = Legacy();
        var current = EncryptedCodec.Wrapping(new StringCodec(), Keys()).Encode("hello");

        // This is the whole migration story. 0x01 and 0xAE cannot be confused, so a
        // reader never has to guess and never has to try one and fall back to the
        // other -- which would have made a wrong key indistinguishable from an
        // unknown framing.
        Assert.Equal(0x01, legacy[0]);
        Assert.Equal(0xAE, current[0]);

        Assert.True(EncryptedCodec.IsLegacyDotNetBody(legacy));
        Assert.False(EncryptedCodec.IsLegacyDotNetBody(current));
    }

    [Fact]
    public void NeverWritesTheLegacyFramingAgain()
    {
        var codec = EncryptedCodec.Wrapping(new StringCodec(), Keys());

        // There is no option, no flag and no constructor that brings it back. A
        // library that can still write a format nothing else reads will keep being
        // asked to.
        foreach (var text in new[] { "", "hello", new string('x', 5000) })
        {
            Assert.Equal(0xAE, codec.Encode(text)[0]);
            Assert.False(EncryptedCodec.IsLegacyDotNetBody(codec.Encode(text)));
        }
    }

    [Fact]
    public void NamesTheKeyALegacyBodyNeedsWithoutDecryptingIt()
    {
        // The question an operator asks about a dead-lettered message, and it has to
        // keep working for the bodies that are actually stuck.
        Assert.Equal("2026-01", EncryptedCodec.KeyIdOf(Legacy()));
    }

    [Fact]
    public void RefusesALegacyBodyWhoseKeyIsNotHeld()
    {
        var elsewhere = EncryptedCodec.Wrapping(
            new StringCodec(), Keyring.Of(EncryptionKey.Generate("2027-01")));

        var error = Assert.Throws<AceFatalException>(
            () => elsewhere.Decode(Legacy(), typeof(string)));
        Assert.Contains("'2026-01'", error.Message);
        Assert.Contains("not on the keyring", error.Message);
    }

    [Fact]
    public void RejectsAnAlteredLegacyBodyBeforeDecryptingIt()
    {
        var codec = EncryptedCodec.Wrapping(new StringCodec(), Keys());
        var wire = Legacy();

        // Flip a bit in the ciphertext. Verifying the tag before decrypting is what
        // stopped this framing becoming a padding oracle, and moving off it must not
        // quietly drop that for the bodies still being read.
        wire[wire.Length - 40] ^= 0x01;

        var error = Assert.Throws<AceFatalException>(() => codec.Decode(wire, typeof(string)));
        Assert.Contains("failed authentication", error.Message);
    }

    [Fact]
    public void RejectsALegacyBodyWithATamperedKeyId()
    {
        // Both ids are on the ring, so rewriting one to the other leaves a body that
        // names a key the reader really holds. The id is inside what the HMAC covers,
        // so it still fails to authenticate rather than opening as something else.
        var codec = EncryptedCodec.Wrapping(new StringCodec(), Keyring.Builder()
            .Add(EncryptionKey.Generate("9026-01"))
            .Current(new EncryptionKey("2026-01", Key()))
            .Build());

        var wire = Legacy();
        wire[2] = (byte)'9';
        Assert.Equal("9026-01", EncryptedCodec.KeyIdOf(wire));

        var error = Assert.Throws<AceFatalException>(() => codec.Decode(wire, typeof(string)));
        Assert.Contains("failed authentication", error.Message);
    }

    [Fact]
    public void RefusesATruncatedLegacyBody()
    {
        var codec = EncryptedCodec.Wrapping(new StringCodec(), Keys());
        var wire = Legacy();

        for (var length = 1; length < wire.Length; length++)
        {
            var cut = new byte[length];
            Buffer.BlockCopy(wire, 0, cut, 0, length);
            Assert.ThrowsAny<AceFatalException>(() => codec.Decode(cut, typeof(string)));
        }
    }

    [Fact]
    public void IsLegacyDotNetBodySaysNoToAnythingThatIsNotOne()
    {
        Assert.False(EncryptedCodec.IsLegacyDotNetBody(null!));
        Assert.False(EncryptedCodec.IsLegacyDotNetBody(Array.Empty<byte>()));
        Assert.False(EncryptedCodec.IsLegacyDotNetBody(new byte[] { 0x01 }));
        Assert.False(EncryptedCodec.IsLegacyDotNetBody(new byte[] { 0x01, 0x07 }));  // too short to hold one
        Assert.False(EncryptedCodec.IsLegacyDotNetBody(System.Text.Encoding.UTF8.GetBytes("{\"id\":1}")));
    }
}
