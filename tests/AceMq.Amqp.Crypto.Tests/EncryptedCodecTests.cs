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

public sealed class Order
{
    public string Id { get; set; } = "";
    public decimal Total { get; set; }
}

public sealed class EncryptedCodecTests
{
    private static EncryptedCodec Codec(string keyId = "k1") =>
        EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(EncryptionKey.Generate(keyId)));

    [Fact]
    public void EncryptsTheBodySoTheBrokerCannotReadIt()
    {
        var codec = Codec();
        var order = new Order { Id = "SECRET-1", Total = 99m };

        var wire = codec.Encode(order);

        // What the broker stores must not contain the payload.
        Assert.DoesNotContain("SECRET-1", Encoding.UTF8.GetString(wire));
        Assert.Equal("k1", EncryptedCodec.KeyIdOf(wire));

        var back = (Order)codec.Decode(wire, typeof(Order));
        Assert.Equal("SECRET-1", back.Id);
    }

    [Fact]
    public void ProducesADifferentCiphertextEachTime()
    {
        var codec = Codec();
        var order = new Order { Id = "A-1", Total = 1m };

        // A fresh nonce per message. Identical ciphertexts for identical plaintexts
        // would tell an observer which messages are repeats without decrypting any —
        // and under GCM a repeated nonce does not weaken the encryption, it forfeits
        // it, because two messages under one key and nonce leak their difference.
        Assert.NotEqual(
            Convert.ToBase64String(codec.Encode(order)),
            Convert.ToBase64String(codec.Encode(order)));
    }

    [Fact]
    public void RejectsAnAlteredCiphertext()
    {
        var codec = Codec();
        var wire = codec.Encode(new Order { Id = "A-1", Total = 1m });

        wire[wire.Length - 20] ^= 0x01;

        var error = Assert.Throws<AceFatalException>(() => codec.Decode(wire, typeof(Order)));
        Assert.Contains("did not decrypt", error.Message);
    }

    [Fact]
    public void RejectsAnAlteredTag()
    {
        var codec = Codec();
        var wire = codec.Encode(new Order { Id = "A-1", Total = 1m });

        wire[wire.Length - 1] ^= 0x01;

        Assert.Throws<AceFatalException>(() => codec.Decode(wire, typeof(Order)));
    }

    /// <summary>
    /// The one property that has to survive the move off encrypt-then-MAC.
    /// </summary>
    /// <remarks>
    /// A wrong key and a tampered body must be one event with one answer. An error
    /// that told them apart — a different message, a different type, an earlier check
    /// that only one of them reached — would be a padding oracle with better manners.
    /// </remarks>
    [Fact]
    public void FailsIdenticallyForAWrongKeyAndForATamperedBody()
    {
        var written = EncryptedCodec.Wrapping(
            new JsonCodec(), Keyring.Of(EncryptionKey.Generate("k1")));
        var wire = written.Encode(new Order { Id = "A-1", Total = 1m });

        // A different key of the same id: the body is untouched and simply will not open.
        var wrongKey = EncryptedCodec.Wrapping(
            new JsonCodec(), Keyring.Of(EncryptionKey.Generate("k1")));
        var fromWrongKey = Assert.Throws<AceFatalException>(
            () => wrongKey.Decode(wire, typeof(Order)));

        // The right key, and a body altered after it was written.
        var tampered = (byte[])wire.Clone();
        tampered[tampered.Length - 20] ^= 0x01;
        var fromTamper = Assert.Throws<AceFatalException>(
            () => written.Decode(tampered, typeof(Order)));

        Assert.Equal(fromWrongKey.GetType(), fromTamper.GetType());
        Assert.Equal(fromWrongKey.Message, fromTamper.Message);
        Assert.Null(fromWrongKey.InnerException);
        Assert.Null(fromTamper.InnerException);
    }

    [Fact]
    public void NamesNeitherThePlaintextNorTheKeyWhenItFails()
    {
        var key = EncryptionKey.Generate("k1");
        var codec = EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(key));
        var wire = codec.Encode(new Order { Id = "SECRET-1", Total = 99m });
        wire[wire.Length - 5] ^= 0x01;

        var error = Assert.Throws<AceFatalException>(() => codec.Decode(wire, typeof(Order)));

        // The key id is on the wire in the clear and naming it is the whole point of
        // the message. The plaintext and the key itself are neither.
        Assert.Contains("k1", error.Message);
        Assert.DoesNotContain("SECRET-1", error.Message);
        Assert.DoesNotContain(Convert.ToBase64String(wire), error.Message);
        Assert.DoesNotContain("System.Byte", error.Message);
    }

    [Fact]
    public void ToStringShowsNeitherKeyNorBody()
    {
        var codec = Codec("2026-01");
        Assert.Equal("EncryptedCodec[JsonCodec]", codec.ToString());

        var key = EncryptionKey.Generate("2026-01");
        Assert.Equal("EncryptionKey[2026-01]", key.ToString());
        Assert.Equal("Keyring[current=2026-01, 1 key(s)]", Keyring.Of(key).ToString());
    }

    [Fact]
    public void ReadsMessagesEncryptedWithARotatedOutKey()
    {
        var old = EncryptionKey.Generate("2025");
        var current = EncryptionKey.Generate("2026");

        var before = EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(old));
        var wire = before.Encode(new Order { Id = "A-1", Total = 1m });

        // After rotation the old key stays readable, because messages encrypted
        // with it are still sitting in queues.
        var after = EncryptedCodec.Wrapping(
            new JsonCodec(), Keyring.Builder().Add(old).Current(current).Build());

        Assert.Equal("A-1", ((Order)after.Decode(wire, typeof(Order))).Id);
        Assert.Equal("2026", EncryptedCodec.KeyIdOf(after.Encode(new Order { Id = "A-2" })));

        var withoutOld = EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(current));
        var error = Assert.Throws<AceFatalException>(() => withoutOld.Decode(wire, typeof(Order)));
        Assert.Contains("not on the keyring", error.Message);
    }

    // ---- what it refuses -------------------------------------------------

    [Fact]
    public void RefusesABodyThatWasNeverEncrypted()
    {
        var codec = Codec();
        var plain = Encoding.UTF8.GetBytes("{\"Id\":\"A-1\",\"Total\":1}");

        var error = Assert.Throws<AceFatalException>(() => codec.Decode(plain, typeof(Order)));
        Assert.Contains("not an AceMQ encrypted body", error.Message);
        Assert.Contains("0x7b", error.Message);
    }

    [Fact]
    public void RefusesAFramingVersionItDoesNotUnderstand()
    {
        var codec = Codec();
        var wire = codec.Encode(new Order { Id = "A-1", Total = 1m });
        wire[1] = 0x02;

        var error = Assert.Throws<AceFatalException>(() => codec.Decode(wire, typeof(Order)));
        Assert.Contains("version 2", error.Message);
    }

    [Fact]
    public void RefusesAnEmptyBody()
    {
        var error = Assert.Throws<AceFatalException>(
            () => Codec().Decode(Array.Empty<byte>(), typeof(Order)));
        Assert.Contains("cannot be empty", error.Message);
    }

    [Fact]
    public void RefusesATruncatedBody()
    {
        var codec = Codec();
        var wire = codec.Encode(new Order { Id = "A-1", Total = 1m });

        for (var length = 1; length < wire.Length; length++)
        {
            var cut = new byte[length];
            Buffer.BlockCopy(wire, 0, cut, 0, length);

            // Every prefix is refused, and none of them throws something that is not
            // an AceMQ exception: an IndexOutOfRangeException off a malformed body is
            // a bug reaching the caller.
            Assert.ThrowsAny<AceFatalException>(() => codec.Decode(cut, typeof(Order)));
        }
    }

    [Fact]
    public void RefusesNull()
    {
        Assert.Throws<ArgumentNullException>(() => Codec().Decode(null!, typeof(Order)));
    }

    // ---- content type ----------------------------------------------------

    [Fact]
    public void AnnouncesItsOwnContentTypeAndVolunteersForNothingElse()
    {
        var codec = Codec();

        Assert.Equal("application/vnd.acemq.encrypted", codec.ContentType);
        Assert.Equal(EncryptedCodec.EncryptedContentType, codec.ContentType);

        Assert.True(codec.CanDecode("application/vnd.acemq.encrypted"));
        Assert.True(codec.CanDecode("APPLICATION/VND.ACEMQ.ENCRYPTED"));

        // Never JSON, and never a message whose sender set no content type. Both
        // would mean trying to decrypt plaintext and reporting the failure as a
        // decode error, which sends whoever is debugging it the wrong way.
        Assert.False(codec.CanDecode("application/json"));
        Assert.False(codec.CanDecode(null));
        Assert.False(codec.CanDecode(""));
    }

    [Fact]
    public void KeepsTheCodecItWraps()
    {
        var inner = new JsonCodec();
        Assert.Same(inner, EncryptedCodec.Wrapping(inner, Keyring.Of(EncryptionKey.Generate("k1"))).Inner);
    }

    [Fact]
    public void RefusesToBeBuiltWithoutACodecOrAKeyring()
    {
        Assert.Throws<ArgumentNullException>(
            () => EncryptedCodec.Wrapping(null!, Keyring.Of(EncryptionKey.Generate("k1"))));
        Assert.Throws<ArgumentNullException>(() => EncryptedCodec.Wrapping(new JsonCodec(), null!));
    }

    [Fact]
    public void KeyIdOfSaysNothingAboutABodyThatIsNotOne()
    {
        Assert.Null(EncryptedCodec.KeyIdOf(null!));
        Assert.Null(EncryptedCodec.KeyIdOf(Array.Empty<byte>()));
        Assert.Null(EncryptedCodec.KeyIdOf(Encoding.UTF8.GetBytes("{\"id\":\"A-1\"}")));
        Assert.Null(EncryptedCodec.KeyIdOf(new byte[] { 0xAE, 0x02, 0x01, 0x41 }));  // a version it cannot read
        Assert.Null(EncryptedCodec.KeyIdOf(new byte[] { 0xAE, 0x01, 0x40, 0x41 }));  // a length past the end
    }
}
