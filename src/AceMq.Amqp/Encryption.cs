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
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace AceMq.Amqp;

/// <summary>A named encryption key.</summary>
/// <remarks>
/// The id travels with the message so a consumer knows which key to reach for. It
/// is not a secret — it identifies the key, it does not reveal it.
/// </remarks>
public sealed class EncryptionKey
{
    /// <summary>Bytes a key must have.</summary>
    public const int KeySize = 32;

    public EncryptionKey(string id, byte[] key)
    {
        if (string.IsNullOrEmpty(id)) throw new ArgumentException("a key needs an id", nameof(id));
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (key.Length != KeySize)
        {
            throw new SecurityConfigurationException(
                $"an encryption key must be {KeySize} bytes; this one is {key.Length}");
        }
        if (Encoding.UTF8.GetByteCount(id) > 255)
        {
            // The id is length-prefixed with one byte on the wire.
            throw new SecurityConfigurationException("a key id must be at most 255 bytes of UTF-8");
        }

        Id = id;
        Key = (byte[])key.Clone();
    }

    /// <summary>Generates a key with the platform's cryptographic random source.</summary>
    public static EncryptionKey Generate(string id)
    {
        var key = new byte[KeySize];
        using (var random = RandomNumberGenerator.Create()) random.GetBytes(key);
        return new EncryptionKey(id, key);
    }

    public string Id { get; }

    internal byte[] Key { get; }

    /// <summary>Never includes the key.</summary>
    public override string ToString() => $"EncryptionKey[{Id}]";
}

/// <summary>The keys a consumer might need.</summary>
/// <remarks>
/// More than one, because rotation is the normal case: messages encrypted with the
/// previous key are still in queues when the new one starts being used, and both
/// have to be readable until they are drained.
/// </remarks>
public interface IKeyring
{
    /// <summary>The key new messages are encrypted with.</summary>
    EncryptionKey Current { get; }

    /// <summary>The key with an id, or null if this keyring does not have it.</summary>
    EncryptionKey? KeyFor(string keyId);
}

/// <summary>A keyring holding a fixed set of keys.</summary>
public sealed class Keyring : IKeyring
{
    private readonly Dictionary<string, EncryptionKey> _keys;

    private Keyring(EncryptionKey current, Dictionary<string, EncryptionKey> keys)
    {
        Current = current;
        _keys = keys;
    }

    public static IKeyring Of(EncryptionKey key) => Builder().Current(key).Build();

    public static KeyringBuilder Builder() => new KeyringBuilder();

    public EncryptionKey Current { get; }

    public EncryptionKey? KeyFor(string keyId) =>
        keyId != null && _keys.TryGetValue(keyId, out var key) ? key : null;

    public override string ToString() =>
        $"Keyring[current={Current.Id}, {_keys.Count} key(s)]";

    /// <summary>Builds a <see cref="Keyring"/>.</summary>
    public sealed class KeyringBuilder
    {
        private readonly Dictionary<string, EncryptionKey> _keys =
            new Dictionary<string, EncryptionKey>(StringComparer.Ordinal);
        private EncryptionKey? _current;

        internal KeyringBuilder() { }

        /// <summary>Adds a key that can still be read but is no longer used to encrypt.</summary>
        public KeyringBuilder Add(EncryptionKey key)
        {
            _keys[key.Id] = key ?? throw new ArgumentNullException(nameof(key));
            return this;
        }

        /// <summary>Sets the key new messages are encrypted with.</summary>
        public KeyringBuilder Current(EncryptionKey key)
        {
            Add(key);
            _current = key;
            return this;
        }

        public IKeyring Build()
        {
            if (_current == null)
            {
                throw new SecurityConfigurationException(
                    "a keyring needs a current key; call Current(...)");
            }
            return new Keyring(_current, _keys);
        }
    }
}

/// <summary>
/// Encrypts the body another codec produced.
/// </summary>
/// <remarks>
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
/// The construction is AES-256-CBC with HMAC-SHA-256 over the ciphertext, in that
/// order — encrypt, then authenticate. A ciphertext whose tag does not verify is
/// rejected before any attempt is made to decrypt it, which is what stops a
/// modified message being decrypted into something the padding oracle can be asked
/// about. AES-GCM would be the obvious choice and is not available on
/// <c>netstandard2.0</c>, which is the target this library exists to reach.
/// </para>
/// <para>
/// The wire format is:
/// <c>[version:1][keyIdLength:1][keyId][iv:16][ciphertext][tag:32]</c>. The tag
/// covers everything before it, so the version and the key id are authenticated
/// too and cannot be altered to steer a consumer at a different key.
/// </para>
/// </remarks>
public sealed class EncryptedCodec : ICodec
{
    public const string EncryptedContentType = "application/vnd.acemq.encrypted";

    private const byte Version = 1;
    private const int IvSize = 16;
    private const int TagSize = 32;

    private readonly ICodec _inner;
    private readonly IKeyring _keyring;

    private EncryptedCodec(ICodec inner, IKeyring keyring)
    {
        _inner = inner;
        _keyring = keyring;
    }

    /// <summary>Encrypts what <paramref name="inner"/> produces.</summary>
    public static EncryptedCodec Wrapping(ICodec inner, IKeyring keyring) =>
        new EncryptedCodec(
            inner ?? throw new ArgumentNullException(nameof(inner)),
            keyring ?? throw new ArgumentNullException(nameof(keyring)));

    /// <summary>The codec whose output is being encrypted.</summary>
    public ICodec Inner => _inner;

    public string ContentType => EncryptedContentType;

    public byte[] Encode(object payload)
    {
        var plaintext = _inner.Encode(payload);
        var key = _keyring.Current;
        var keyId = Encoding.UTF8.GetBytes(key.Id);

        var iv = new byte[IvSize];
        using (var random = RandomNumberGenerator.Create()) random.GetBytes(iv);

        byte[] ciphertext;
        using (var aes = Aes.Create())
        {
            aes.KeySize = 256;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            aes.Key = EncryptionKeyFor(key);
            aes.IV = iv;
            using var encryptor = aes.CreateEncryptor();
            ciphertext = encryptor.TransformFinalBlock(plaintext, 0, plaintext.Length);
        }

        var header = new byte[2 + keyId.Length];
        header[0] = Version;
        header[1] = (byte)keyId.Length;
        Buffer.BlockCopy(keyId, 0, header, 2, keyId.Length);

        var authenticated = new byte[header.Length + iv.Length + ciphertext.Length];
        Buffer.BlockCopy(header, 0, authenticated, 0, header.Length);
        Buffer.BlockCopy(iv, 0, authenticated, header.Length, iv.Length);
        Buffer.BlockCopy(ciphertext, 0, authenticated, header.Length + iv.Length, ciphertext.Length);

        byte[] tag;
        using (var hmac = new HMACSHA256(MacKeyFor(key))) tag = hmac.ComputeHash(authenticated);

        var message = new byte[authenticated.Length + TagSize];
        Buffer.BlockCopy(authenticated, 0, message, 0, authenticated.Length);
        Buffer.BlockCopy(tag, 0, message, authenticated.Length, TagSize);
        return message;
    }

    public object Decode(byte[] body, Type target)
    {
        if (body == null) throw new ArgumentNullException(nameof(body));
        if (body.Length < 2 + IvSize + TagSize)
        {
            throw new AceFatalException("this message is too short to be an encrypted body");
        }
        if (body[0] != Version)
        {
            throw new AceFatalException(
                $"encrypted body version {body[0]} is not one this library understands");
        }

        var keyIdLength = body[1];
        var headerLength = 2 + keyIdLength;
        if (body.Length < headerLength + IvSize + TagSize)
        {
            throw new AceFatalException("this encrypted body is truncated");
        }

        var keyId = Encoding.UTF8.GetString(body, 2, keyIdLength);
        var key = _keyring.KeyFor(keyId)
                  ?? throw new AceFatalException(
                      $"this message was encrypted with key '{keyId}', which is not on the keyring. " +
                      "A key that has been rotated out has to stay readable until the queues " +
                      "holding its messages are drained.");

        var authenticatedLength = body.Length - TagSize;

        // Verified before anything is decrypted. Decrypting first and checking
        // afterwards is what makes a padding oracle possible: the error differs
        // depending on whether the padding was well formed, and that difference is
        // enough to recover the plaintext a byte at a time.
        byte[] expected;
        using (var hmac = new HMACSHA256(MacKeyFor(key)))
        {
            expected = hmac.ComputeHash(body, 0, authenticatedLength);
        }
        if (!ConstantTimeEquals(expected, 0, body, authenticatedLength, TagSize))
        {
            throw new AceFatalException(
                "this encrypted body failed authentication: it was altered in transit, " +
                "or it was encrypted with a different key of the same id");
        }

        var ciphertextOffset = headerLength + IvSize;
        var iv = new byte[IvSize];
        Buffer.BlockCopy(body, headerLength, iv, 0, IvSize);

        using var aes = Aes.Create();
        aes.KeySize = 256;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.PKCS7;
        aes.Key = EncryptionKeyFor(key);
        aes.IV = iv;
        using var decryptor = aes.CreateDecryptor();
        var plaintext = decryptor.TransformFinalBlock(
            body, ciphertextOffset, authenticatedLength - ciphertextOffset);

        return _inner.Decode(plaintext, target);
    }

    public bool CanDecode(string? contentType) =>
        contentType != null
        && contentType.StartsWith(EncryptedContentType, StringComparison.OrdinalIgnoreCase);

    /// <summary>Which key a body was encrypted with, without decrypting it.</summary>
    /// <remarks>
    /// For working out why a message cannot be read: the answer is usually that the
    /// key it names was rotated off the keyring too early.
    /// </remarks>
    public static string? KeyIdOf(byte[] body)
    {
        if (body == null || body.Length < 2 || body[0] != Version) return null;
        var length = body[1];
        return body.Length < 2 + length ? null : Encoding.UTF8.GetString(body, 2, length);
    }

    /// <summary>
    /// Separate keys for encryption and authentication, derived from the one supplied.
    /// </summary>
    /// <remarks>
    /// Using the same bytes for both is a standing recommendation against, and the
    /// derivation costs nothing.
    /// </remarks>
    private static byte[] EncryptionKeyFor(EncryptionKey key) => Derive(key, "acemq-encryption");

    private static byte[] MacKeyFor(EncryptionKey key) => Derive(key, "acemq-authentication");

    private static byte[] Derive(EncryptionKey key, string label)
    {
        using var hmac = new HMACSHA256(key.Key);
        return hmac.ComputeHash(Encoding.UTF8.GetBytes(label));
    }

    /// <summary>
    /// Compares without leaking where the difference is.
    /// </summary>
    /// <remarks>
    /// A comparison that returns as soon as it finds a mismatch tells an attacker,
    /// through how long it took, how many leading bytes were right — which is enough
    /// to construct a valid tag one byte at a time.
    /// </remarks>
    [MethodImpl(MethodImplOptions.NoOptimization | MethodImplOptions.NoInlining)]
    private static bool ConstantTimeEquals(byte[] a, int aOffset, byte[] b, int bOffset, int length)
    {
        var difference = 0;
        for (var i = 0; i < length; i++) difference |= a[aOffset + i] ^ b[bOffset + i];
        return difference == 0;
    }

    public override string ToString() => $"EncryptedCodec[{_inner}]";
}
