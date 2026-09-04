# Serialization

JSON by default, camelCased on the wire so a C# `OrderId` and a Java `orderId` are
the same field. That default is what makes cross-language messages work without
anybody configuring anything.

```csharp
using var mq = await AceMqConnection.ConnectAsync(url);              // JSON
using var mq = await AceMqConnection.ConnectAsync(url, new XmlCodec());
```

Per consumer, when one queue carries a different format:

```csharp
await mq.ConsumeAsync<Order>("legacy", ConsumerOptions.Defaults().As(new XmlCodec()), Handle);
```

## What ships

| Codec | Content type | |
|---|---|---|
| `JsonCodec` | `application/json` | the default |
| `XmlCodec` | `application/xml` | for systems that speak XML and will not change |
| `StringCodec` | `text/plain` | text, as UTF-8 |
| `BytesCodec` | `application/octet-stream` | bytes, untouched |
| `CompositeCodec` | first codec's | reads several, writes one |
| `EncryptedCodec` | `application/vnd.acemq.encrypted` | wraps any of the above |

Avro, Protobuf and YAML are not here. Each needs a dependency, and a messaging
library that drags a serialization stack into every application that installs it is
a library people work around. They belong in their own packages, and are not written
yet.

### XML reads are restricted

`XmlCodec` parses with DTD processing off and no external resolver, so a document
cannot pull in an external entity. Without that, "we accept XML" becomes "we read
files off the consumer's disk on request" — the XXE attack, which is old and still
works against parsers left on their defaults. A test feeds it a hostile document and
requires the refusal.

It inherits `XmlSerializer`'s constraints: a public parameterless constructor, public
read/write members only, no interfaces or dictionaries.

## Changing format without stopping

```csharp
var codec = CompositeCodec.Of(new JsonCodec(), new XmlCodec());
```

The **first** codec encodes; **any** of them can decode. So a service can start
publishing JSON while still reading the XML already in its queues, and the old codec
comes out once nothing is left that speaks it.

## Choosing a format from configuration

```csharp
var codec = CodecRegistry.ByName(settings.Format);   // "json", "xml", "string", "bytes"
CodecRegistry.Register("avro", () => new MyAvroCodec());
```

Registration is explicit. Nothing scans assemblies, so a format is available because
something registered it — not because a package happened to be installed.

## Encrypting the payload

TLS protects a message between your process and the broker. It does nothing about
the message **sitting in a queue**, in the broker's storage, or in a backup of it.

```csharp
var keyring = Keyring.Of(EncryptionKey.Generate("2026-01"));
var codec = EncryptedCodec.Wrapping(new JsonCodec(), keyring);

using var mq = await AceMqConnection.ConnectAsync(url, codec);
```

**The body is encrypted. Headers are not.** The envelope, routing key and
application headers stay readable, because the broker routes on them and the library
reads them. Anything secret belongs in the payload.

### Rotating a key

```csharp
var keyring = Keyring.Builder()
    .Add(EncryptionKey.Generate("2025-07"))      // still readable
    .Current(EncryptionKey.Generate("2026-01"))  // used for new messages
    .Build();
```

Keep the old key until the queues holding its messages are drained. Removing it too
early produces:

```
this message was encrypted with key '2025-07', which is not on the keyring
```

`EncryptedCodec.KeyIdOf(body)` says which key a message needs without decrypting it,
which is normally how that gets diagnosed.

### What the construction is

AES-256-CBC with HMAC-SHA-256 over the ciphertext — encrypt, then authenticate. A
body whose tag does not verify is rejected **before** anything is decrypted, which is
what stops a modified message being turned into a padding oracle.

```
[version:1][keyIdLength:1][keyId][iv:16][ciphertext][tag:32]
```

The tag covers everything before it, so the version and the key id are authenticated
too and cannot be edited to point a consumer at a different key. A fresh IV per
message means two identical payloads produce different ciphertexts — otherwise an
observer could tell which messages repeat without decrypting any.

AES-GCM would be the obvious choice and is not available on `netstandard2.0`, which
is the target that reaches .NET Framework. Encrypt-then-MAC is the standard answer
where it is not.

Encryption and authentication use separate keys derived from the one you supply,
because reusing one set of bytes for both is a long-standing recommendation against.

## Schemas

A registry maps a schema to a short id, so messages carry the id rather than
kilobytes of schema:

```csharp
var registry = new InMemorySchemaRegistry();
var id = registry.IdFor(new SchemaDefinition("json", "order.placed", schemaText));
```

The definition is opaque — JSON Schema, Avro, `.proto`, whatever. The registry
provides identity, not validation: the same schema always gets the same id, a changed
one gets a different id.

`InMemorySchemaRegistry` is **per process**. Ids are issued in registration order, so
two processes disagree about what an id means and a restart renumbers everything.
That makes it right for tests and wrong wherever a message outlives the process that
wrote it.

`DbSchemaRegistry` is the one to use for that:

```csharp
var registry = new DbSchemaRegistry(() => new SqlConnection(connectionString));
```

The fingerprint column is unique, which makes registering the same schema twice
idempotent even when two processes do it at the same instant: the second insert is
refused and the row already there is read back.
