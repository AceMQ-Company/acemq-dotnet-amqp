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

using System;
using System.Security.Cryptography;
using System.Text;

using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Modes;
using Org.BouncyCastle.Crypto.Parameters;

namespace AceMq.Amqp.Crypto;

/// <summary>
/// Encrypts the body another codec produced, in the framing every AceMQ library reads.
/// </summary>
/// <remarks>
/// <para>
/// <code>
/// var keyring = Keyring.Of(EncryptionKey.Generate("2026-01"));
/// var codec = EncryptedCodec.Wrapping(new JsonCodec(), keyring);
///
/// using var mq = await AceMqConnection.ConnectAsync(url, codec);
/// </code>
/// </para>
/// <para>
/// TLS protects a message between this process and the broker. It does nothing
/// about the message sitting in a queue, in the broker's storage, or in a backup of
/// it. This encrypts the body itself, so what the broker holds is unreadable
/// without a key the broker does not have.
/// </para>
/// <para>
/// <strong>What is encrypted, and what is not.</strong> The body is. Headers are
/// not — the envelope, routing key and application headers stay in clear text,
/// because the broker routes on them and the library reads them. Anything secret
/// belongs in the payload, not in a header.
/// </para>
/// <para>
/// <strong>What is on the wire</strong>
/// </para>
/// <para>
/// <code>
/// 0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
/// </code>
/// </para>
/// <para>
/// Byte for byte what the Java, Python, Ruby and Go libraries write, and what they
/// read. AES-256-GCM, a 128-bit tag, and a fresh nonce per message — reusing one
/// under GCM does not weaken the encryption, it forfeits it, because two messages
/// under the same key and nonce leak their difference outright.
/// </para>
/// <para>
/// <strong>The key identifier is in the message, in the clear.</strong> That is
/// deliberate, and it is what makes rotation possible: a consumer reads which key a
/// message needs rather than assuming the current one, so a new key can be
/// introduced while messages written with the old one are still queued. Putting it
/// in an AMQP header instead would have been tidier and would have lost it —
/// headers are dropped by shovels, rewritten by federation, and absent from a
/// message recovered out of a backup, and a ciphertext whose key nobody can name is
/// gone. <see cref="KeyIdOf"/> reads it back without needing any key.
/// </para>
/// <para>
/// The header is authenticated but not encrypted: GCM binds all of it — magic,
/// version, length and identifier — as associated data, so an altered key
/// identifier makes the message fail to open rather than quietly opening as
/// something else.
/// </para>
/// <para>
/// <strong>Bodies written before 0.4.0 still read.</strong> Releases up to and
/// including 0.3.0 wrote a .NET-only framing — AES-256-CBC then HMAC-SHA-256, with
/// no magic byte — that no other AceMQ library could read. Those bodies start
/// <c>0x01</c> where this framing starts <c>0xAE</c>, so the two are told apart with
/// certainty rather than guessed at, and <see cref="Decode"/> still reads the old
/// one. Nothing writes it any more. See <see cref="IsLegacyDotNetBody"/>.
/// </para>
/// <para>
/// <strong>What this does not do.</strong> The broker can no longer read the
/// message, and neither can the people who operate it. Decide what they do instead
/// before turning this on: a dead-letter queue full of ciphertext is a queue nobody
/// can triage, and the answer is usually a small internal tool holding the keyring
/// rather than the management UI.
/// </para>
/// <para>
/// Encryption is not authorisation. Every service holding the keyring can read every
/// message encrypted with those keys; the granularity is the key, so separate
/// audiences mean separate keys. Nor does it authenticate the sender: anybody
/// holding the key can write a message this codec will happily decrypt. Nor does it
/// hide the routing — exchange, routing key, headers and message size stay in the
/// clear, and for many systems the routing key is the sensitive part.
/// </para>
/// </remarks>
public sealed class EncryptedCodec : ICodec
{
    /// <summary>
    /// Deliberately not <c>...+json</c>, whatever the plaintext underneath is.
    /// </summary>
    /// <remarks>
    /// A <c>+json</c> suffix is a promise that the bytes on the wire are JSON, and
    /// every JSON-aware consumer reads it that way. These bytes are ciphertext.
    /// Naming them <c>json</c> makes the JSON codec volunteer to decode them, which
    /// is how a message ends up failing in a parser rather than being refused by a
    /// codec that knows it cannot help.
    /// </remarks>
    public const string EncryptedContentType = "application/vnd.acemq.encrypted";

    /// <summary>
    /// Marks the framing as this codec's, so a message from elsewhere is refused
    /// rather than decrypted.
    /// </summary>
    internal const byte Magic = 0xAE;

    /// <summary>
    /// Version 1. Present so a later framing can be told apart from this one by its
    /// first two bytes.
    /// </summary>
    internal const byte Version = 0x01;

    /// <summary>The magic, the version and the key id length byte.</summary>
    internal const int PrefixBytes = 3;

    /// <summary>
    /// Fixed at 12, which is the size GCM is defined for. Any other length is hashed
    /// into shape by the construction and loses the guarantee that two different
    /// nonces stay different.
    /// </summary>
    internal const int NonceBytes = 12;

    /// <summary>A 128-bit authentication tag, as every other AceMQ library writes.</summary>
    internal const int TagBits = 128;

    internal const int TagBytes = TagBits / 8;

    /// <summary>
    /// One generator for the process. <see cref="RandomNumberGenerator.GetBytes(byte[])"/>
    /// is safe to call from several threads, and creating one per message put an
    /// allocation and a syscall on the publish path for nothing.
    /// </summary>
    private static readonly RandomNumberGenerator Entropy = RandomNumberGenerator.Create();

    private readonly ICodec _inner;
    private readonly IKeyring _keyring;
    private readonly Action<byte[]> _fillNonce;

    private EncryptedCodec(ICodec inner, IKeyring keyring, Action<byte[]> fillNonce)
    {
        _inner = inner;
        _keyring = keyring;
        _fillNonce = fillNonce;
    }

    /// <summary>Encrypts what <paramref name="inner"/> produces.</summary>
    public static EncryptedCodec Wrapping(ICodec inner, IKeyring keyring) =>
        new EncryptedCodec(
            inner ?? throw new ArgumentNullException(nameof(inner)),
            keyring ?? throw new ArgumentNullException(nameof(keyring)),
            nonce => Entropy.GetBytes(nonce));

    /// <summary>
    /// The same codec with the nonce taken from somewhere known, so a test can assert
    /// the exact bytes this package writes against the vector the other libraries agree on.
    /// </summary>
    /// <remarks>
    /// Internal on purpose. A caller who chooses nonces is a caller who can repeat
    /// one, and a repeated nonce under GCM does not weaken the encryption — it
    /// forfeits it.
    /// </remarks>
    internal static EncryptedCodec WithNonce(ICodec inner, IKeyring keyring, Action<byte[]> fillNonce) =>
        new EncryptedCodec(inner, keyring, fillNonce);

    /// <summary>The codec whose output is being encrypted.</summary>
    public ICodec Inner => _inner;

    public string ContentType => EncryptedContentType;

    public byte[] Encode(object payload)
    {
        var plaintext = _inner.Encode(payload);
        var key = _keyring.Current;
        var keyId = Encoding.UTF8.GetBytes(key.Id);

        var header = new byte[PrefixBytes + keyId.Length];
        header[0] = Magic;
        header[1] = Version;
        header[2] = (byte)keyId.Length;
        Buffer.BlockCopy(keyId, 0, header, PrefixBytes, keyId.Length);

        var nonce = new byte[NonceBytes];
        _fillNonce(nonce);

        byte[] sealedBytes;
        try
        {
            var cipher = new GcmBlockCipher(new AesEngine());
            cipher.Init(true, new AeadParameters(new KeyParameter(key.Key), TagBits, nonce, header));
            sealedBytes = new byte[cipher.GetOutputSize(plaintext.Length)];
            var written = cipher.ProcessBytes(plaintext, 0, plaintext.Length, sealedBytes, 0);
            written += cipher.DoFinal(sealedBytes, written);
            if (written != sealedBytes.Length) sealedBytes = Slice(sealedBytes, 0, written);
        }
        catch (Exception e)
        {
            // Named without the payload in it. An exception that helpfully prints what
            // could not be encrypted writes the plaintext to the log, which is the one
            // place it was never supposed to reach.
            throw new AceFatalException(
                $"could not encrypt a {payload?.GetType().Name ?? "null"} with key '{key.Id}'", e);
        }

        var framed = new byte[header.Length + NonceBytes + sealedBytes.Length];
        Buffer.BlockCopy(header, 0, framed, 0, header.Length);
        Buffer.BlockCopy(nonce, 0, framed, header.Length, NonceBytes);
        Buffer.BlockCopy(sealedBytes, 0, framed, header.Length + NonceBytes, sealedBytes.Length);
        return framed;
    }

    public object Decode(byte[] body, Type target)
    {
        if (body == null) throw new ArgumentNullException(nameof(body));
        if (body.Length == 0) throw new AceFatalException("an encrypted body cannot be empty");

        byte[] plaintext;
        if (body[0] == Magic)
        {
            plaintext = Open(body);
        }
        else if (body[0] == LegacyDotNetBody.Version)
        {
            plaintext = LegacyDotNetBody.Open(body, _keyring);
        }
        else
        {
            // Everything else is refused rather than attempted. A body that was never
            // encrypted, or was encrypted by something that is not an AceMQ library,
            // should fail here naming the framing -- not four frames deeper in a
            // parser being handed random bytes.
            throw new AceFatalException(
                $"this is not an AceMQ encrypted body: it starts 0x{body[0]:x2}, and this " +
                $"codec reads 0x{Magic:x2} (the framing every AceMQ library writes) and " +
                $"0x{LegacyDotNetBody.Version:x2} (bodies this library wrote before 0.4.0)");
        }

        // Outside the decrypt, deliberately. A delegate that cannot parse what came out
        // is a different problem from a body that would not open, and folding the two
        // together sends whoever is reading the log in the wrong direction.
        return _inner.Decode(plaintext, target);
    }

    private byte[] Open(byte[] body)
    {
        if (body.Length < PrefixBytes)
        {
            throw new AceFatalException("this encrypted body is truncated: it has no header");
        }
        if (body[1] != Version)
        {
            throw new AceFatalException(
                $"encrypted framing version {body[1]} is not one this library understands");
        }

        int keyIdLength = body[2];
        var headerLength = PrefixBytes + keyIdLength;
        if (body.Length < headerLength + NonceBytes + TagBytes)
        {
            throw new AceFatalException("this encrypted body is truncated");
        }

        var keyId = Encoding.UTF8.GetString(body, PrefixBytes, keyIdLength);
        var key = _keyring.KeyFor(keyId)
                  ?? throw new AceFatalException(
                      $"this message was encrypted with key '{keyId}', which is not on the keyring. " +
                      "A key that has been rotated out has to stay readable until the queues " +
                      "holding its messages are drained.");

        var header = Slice(body, 0, headerLength);
        var nonce = Slice(body, headerLength, NonceBytes);
        var sealedOffset = headerLength + NonceBytes;
        var sealedLength = body.Length - sealedOffset;

        try
        {
            var cipher = new GcmBlockCipher(new AesEngine());
            cipher.Init(false, new AeadParameters(new KeyParameter(key.Key), TagBits, nonce, header));
            var opened = new byte[cipher.GetOutputSize(sealedLength)];
            var written = cipher.ProcessBytes(body, sealedOffset, sealedLength, opened, 0);
            written += cipher.DoFinal(opened, written);
            return written == opened.Length ? opened : Slice(opened, 0, written);
        }
        catch (Exception)
        {
            // One answer for every way this can fail, and the catch is deliberately
            // broad so there is only ever one. GCM authenticates as well as encrypts,
            // so a wrong key and a tampered ciphertext are the same event here, and
            // they are told apart by nothing -- no separate message, no separate type,
            // no earlier check that would have distinguished them. An error that said
            // which one it was would be a padding oracle with better manners.
            //
            // The inner exception is dropped for the same reason: BouncyCastle's
            // message says which check failed.
            throw new AceFatalException(
                $"this message did not decrypt with key '{keyId}'. Either that is not the " +
                "key it was written with, or it was altered after it was written");
        }
    }

    public bool CanDecode(string? contentType) =>
        contentType != null
        && contentType.StartsWith(EncryptedContentType, StringComparison.OrdinalIgnoreCase);

    /// <summary>Which key a body was encrypted with, without decrypting it.</summary>
    /// <remarks>
    /// For working out why a message cannot be read: the answer is usually that the
    /// key it names was rotated off the keyring too early. Reads both this framing
    /// and the pre-0.4.0 .NET one, and returns null for anything that is neither.
    /// </remarks>
    public static string? KeyIdOf(byte[] body)
    {
        if (body == null || body.Length < 2) return null;

        if (body[0] == Magic)
        {
            if (body[1] != Version || body.Length < PrefixBytes) return null;
            int length = body[2];
            return body.Length < PrefixBytes + length
                ? null
                : Encoding.UTF8.GetString(body, PrefixBytes, length);
        }

        return LegacyDotNetBody.KeyIdOf(body);
    }

    /// <summary>
    /// Whether a body is in the .NET-only framing this library wrote before 0.4.0.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <strong>A migration affordance, not a feature.</strong> It exists so an
    /// operator can answer "is there anything left in this queue that only .NET can
    /// read", which is the question that has to be answered before the legacy reader
    /// is removed. It goes when the reader goes — see the deprecation note in
    /// CHANGELOG.md.
    /// </para>
    /// <para>
    /// A body in the current framing returns false, and so does anything that is not
    /// an AceMQ encrypted body at all.
    /// </para>
    /// </remarks>
    public static bool IsLegacyDotNetBody(byte[] body) => LegacyDotNetBody.Looks(body);

    internal static byte[] Slice(byte[] source, int offset, int length)
    {
        var slice = new byte[length];
        Buffer.BlockCopy(source, offset, slice, 0, length);
        return slice;
    }

    public override string ToString() => $"EncryptedCodec[{_inner}]";
}
