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
using System.Text;
using System.Security.Cryptography;

namespace AceMq.Amqp.Crypto;

/// <summary>A named encryption key.</summary>
/// <remarks>
/// <para>
/// The id travels with the message, in the clear, so a consumer knows which key to
/// reach for. It is not a secret — it identifies the key, it does not reveal it —
/// and putting it in the body rather than in an AMQP header is deliberate: headers
/// are dropped by shovels, rewritten by federation and absent from a message
/// recovered out of a backup, and a ciphertext whose key nobody can name is gone.
/// </para>
/// </remarks>
public sealed class EncryptionKey
{
    /// <summary>Bytes a key must have.</summary>
    public const int KeySize = 32;

    /// <summary>The longest a key id can be, in bytes of UTF-8.</summary>
    /// <remarks>
    /// The id is length-prefixed with a single byte on the wire, in every AceMQ
    /// library. A longer one could not be written, and a truncated one would name a
    /// key that does not exist.
    /// </remarks>
    public const int MaxIdBytes = 255;

    public EncryptionKey(string id, byte[] key)
    {
        if (string.IsNullOrEmpty(id)) throw new ArgumentException("a key needs an id", nameof(id));
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (key.Length != KeySize)
        {
            throw new SecurityConfigurationException(
                $"an encryption key must be {KeySize} bytes; this one is {key.Length}");
        }
        if (Encoding.UTF8.GetByteCount(id) > MaxIdBytes)
        {
            throw new SecurityConfigurationException(
                $"a key id must be at most {MaxIdBytes} bytes of UTF-8");
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
