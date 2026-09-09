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
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace AceMq.Amqp.Crypto;

/// <summary>
/// Reads the .NET-only framing this library wrote up to and including 0.3.0.
/// </summary>
/// <remarks>
/// <para>
/// <strong>Deprecated. This reads; nothing writes.</strong> It exists so a service
/// upgrading to 0.4.0 can drain queues that still hold bodies written by 0.3.0,
/// and for no other reason. It will be removed in 1.0.0. Before then, drain those
/// queues — <see cref="EncryptedCodec.IsLegacyDotNetBody"/> is how you find out
/// whether any are left.
/// </para>
/// <para>
/// The framing is <c>[version:1][keyIdLength:1][keyId][iv:16][ciphertext][tag:32]</c>:
/// AES-256-CBC, then HMAC-SHA-256 over everything before the tag, with the cipher
/// key and the MAC key derived separately from the keyring key. That construction
/// is sound — it is encrypt-then-MAC, which is the right answer where AES-GCM is not
/// available, and <c>System.Security.Cryptography.AesGcm</c> is not available on
/// <c>netstandard2.0</c>. What was wrong with it is that no other AceMQ library
/// could read it, while it travelled under the content type that says they can.
/// </para>
/// <para>
/// It is told apart from the current framing with certainty rather than guessed at:
/// this one starts <c>0x01</c>, the current one starts <c>0xAE</c>.
/// </para>
/// </remarks>
internal static class LegacyDotNetBody
{
    /// <summary>
    /// The first byte of the old framing, which held a version and no magic.
    /// </summary>
    /// <remarks>
    /// That it is not <see cref="EncryptedCodec.Magic"/> is the entire migration
    /// story: the two framings cannot be confused for one another.
    /// </remarks>
    internal const byte Version = 1;

    private const int PrefixBytes = 2;
    private const int IvSize = 16;
    private const int TagSize = 32;

    /// <summary>Whether a body is plausibly in this framing.</summary>
    internal static bool Looks(byte[] body)
    {
        if (body == null || body.Length < PrefixBytes || body[0] != Version) return false;
        int keyIdLength = body[1];
        return body.Length >= PrefixBytes + keyIdLength + IvSize + TagSize;
    }

    internal static string? KeyIdOf(byte[] body)
    {
        if (body == null || body.Length < PrefixBytes || body[0] != Version) return null;
        int length = body[1];
        return body.Length < PrefixBytes + length
            ? null
            : Encoding.UTF8.GetString(body, PrefixBytes, length);
    }

    /// <summary>Verifies the tag, then decrypts. Never the other way round.</summary>
    internal static byte[] Open(byte[] body, IKeyring keyring)
    {
        if (body.Length < PrefixBytes + IvSize + TagSize)
        {
            throw new AceFatalException("this message is too short to be an encrypted body");
        }

        int keyIdLength = body[1];
        var headerLength = PrefixBytes + keyIdLength;
        if (body.Length < headerLength + IvSize + TagSize)
        {
            throw new AceFatalException("this encrypted body is truncated");
        }

        var keyId = Encoding.UTF8.GetString(body, PrefixBytes, keyIdLength);
        var key = keyring.KeyFor(keyId)
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
        var iv = EncryptedCodec.Slice(body, headerLength, IvSize);

        using var aes = Aes.Create();
        aes.KeySize = 256;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.PKCS7;
        aes.Key = EncryptionKeyFor(key);
        aes.IV = iv;
        using var decryptor = aes.CreateDecryptor();
        return decryptor.TransformFinalBlock(
            body, ciphertextOffset, authenticatedLength - ciphertextOffset);
    }

    /// <summary>
    /// Separate keys for encryption and authentication, derived from the one supplied.
    /// </summary>
    /// <remarks>
    /// Using the same bytes for both is a standing recommendation against, and the
    /// derivation costs nothing. The current framing needs no equivalent: GCM does
    /// confidentiality and authentication with one key by construction.
    /// </remarks>
    private static byte[] EncryptionKeyFor(EncryptionKey key) => Derive(key, "acemq-encryption");

    private static byte[] MacKeyFor(EncryptionKey key) => Derive(key, "acemq-authentication");

    private static byte[] Derive(EncryptionKey key, string label)
    {
        using var hmac = new HMACSHA256(key.Key);
        return hmac.ComputeHash(Encoding.UTF8.GetBytes(label));
    }

    /// <summary>Compares without leaking where the difference is.</summary>
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
}
