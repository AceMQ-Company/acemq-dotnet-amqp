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

Those six need nothing but the framework — the core's whole dependency list is two
Microsoft packages.

Formats that need an outside library get their own package, so an application that
wants one does not acquire the rest:

| Package | Codec | Depends on |
|---|---|---|
| `AceMq.Amqp.Protobuf` | `ProtobufCodec` | `Google.Protobuf` |
| `AceMq.Amqp.Avro` | `AvroCodec` | `Apache.Avro` |
| `AceMq.Amqp.Yaml` | `YamlCodec` | `YamlDotNet` |
| `AceMq.Amqp.Toml` | `TomlCodec` | `Tomlyn` |

That is every format the Java library has.

**What each costs you.** `YamlDotNet` has no dependencies of its own.
`Google.Protobuf` has none either. `Apache.Avro` brings `Newtonsoft.Json` and
`System.CodeDom`. `Tomlyn` brings `System.Text.Json` at a **higher version than the
core pins**, and NuGet resolves to the higher one — so an application taking the TOML
package moves from `System.Text.Json` 8.0.5 to 10.0.2. Nothing breaks, but it is the
kind of thing worth knowing before it happens, and it is exactly why these are
separate packages rather than part of the core.

## Protocol Buffers

```bash
dotnet add package AceMq.Amqp.Protobuf
```

```csharp
using var mq = await AceMqConnection.ConnectAsync(url, new ProtobufCodec());
```

Or on one queue, while the rest of the service stays on JSON:

```csharp
await mq.ConsumeAsync<OrderPlaced>(
    "orders", ConsumerOptions.Defaults().As(new ProtobufCodec()), Handle);
```

The content type is `application/x-protobuf`, the same as Java's, and
`application/protobuf` and anything ending `+protobuf` are read as well — the last
because a schema registry usually names its own wrapping that way.

**It works with generated types, not with your own classes.** Protobuf encoding is
defined by a `.proto` and the code generated from it; there is no reflection-based
fallback, because bytes produced that way would not be readable by anything else
that speaks protobuf. A plain class is refused at the call rather than encoded into
something only this library could read:

```
AceFatalException: <anonymous type> is not a generated protobuf message. This codec
encodes types generated from a .proto schema; there is no reflection-based fallback...
```

Generate the types with `Grpc.Tools`, which runs `protoc` at build time and
contributes nothing at runtime:

```xml
<PackageReference Include="Grpc.Tools" Version="2.68.1" PrivateAssets="all" />
<Protobuf Include="order.proto" GrpcServices="None" />
```

A malformed body is **fatal, not retryable**. The same bytes fail the same way on
every attempt, so the message is dead-lettered rather than retried forever.

## Avro

```bash
dotnet add package AceMq.Amqp.Avro
```

Avro messages are **not self-describing**: a reader must already hold the schema the
writer used, or the bytes cannot be read. That is the design difference from JSON,
and it is why the codec is constructed with a schema rather than created empty.

There are two ways to say where the schema comes from, and the choice matters more
than it looks.

### A fixed schema

```csharp
var codec = AvroCodec.Of(schemaJson);
```

Small, fast, nothing extra to run — and the writer's schema is whatever the reader
happens to have compiled in. **The moment a producer adds a field, every consumer
still holding the old schema reads the new bytes wrongly**, and Avro will not always
notice. Sound only where the producer and every consumer are released together.

Content type `avro/binary`, and the body is nothing but Avro — no framing.

### A registry

```csharp
var codec = AvroCodec.Registered(new DbSchemaRegistry(Connect), schemaJson);
```

This writes the schema's identifier into the front of every message, so a reader
looks up exactly what the writer used and lets Avro resolve it against its own.
**This is what makes adding a field safe**, and it is the mode to use unless there is
a reason not to.

```
[0x00][schema id: 4 bytes, big-endian][avro body]
```

The layout Confluent's clients use, and the same bytes the Java library writes — any
of the three reads the others. Content type `application/vnd.acemq.avro`.

The registry has to be **shared across processes**. `InMemorySchemaRegistry` issues
ids per process, so a message written by one and read by another refers to an id the
second never issued — use `DbSchemaRegistry` or your own.

Both directions of evolution are tested: a V2 producer read by a V1 consumer, and a
V1 producer read by a V2 consumer. The second only works because the added field has
a **default** — without one Avro cannot invent a value and the read fails.

### What it encodes

A type generated by `avrogen` (`ISpecificRecord`), a `GenericRecord`, or a plain
class mapped onto the schema you supplied. There is no schema generation from a
class: Avro's model is that the schema comes first.

## YAML

```bash
dotnet add package AceMq.Amqp.Yaml
```

```csharp
using var mq = await AceMqConnection.ConnectAsync(url, new YamlCodec());
```

For messages a **person** will read as much as a program: a configuration change
broadcast to a fleet, a deployment instruction, a command replayed by hand from a
dead-letter queue.

It costs more to parse than JSON and is a poor choice for high volume. It earns its
place where somebody will actually look at the message.

Written in **block style**, which is the whole reason to pick it — flow style would
produce something all but indistinguishable from JSON:

```yaml
service: orders
version: 1.4.2
regions:
- eu-west-1
- us-east-1
```

Keys are camelCased, as with JSON, so a C# `Service` and a Java `service` are the
same key. Unknown keys are ignored, so a producer adding a field does not break a
consumer that has not been redeployed.

### It never answers for a message with no content type

**This is the part worth knowing.** YAML is a superset of JSON, so this parser reads
JSON bytes quite happily. If it volunteered for untyped messages it would give the
right value from the wrong codec — and the mistake surfaces much later, as traffic
recorded under a format nobody sent.

So `CanDecode(null)` is **false**, and it claims only `application/yaml`,
`application/x-yaml`, `text/yaml` and anything ending `+yaml`. In a
`CompositeCodec`, put JSON first and let YAML take only what is labelled.

### Hostile input

A YAML tag naming a type is the deserialisation attack this format is known for.
YamlDotNet does not honour one by default, and a test pins that rather than trusting
it to stay true.

The billion-laughs shape — a small document whose aliases name an enormous graph —
is bounded, because the parser shares aliases rather than materialising them. A
480-byte document nominally naming 10¹⁰ nodes reads in milliseconds. That is
measured in a test, because if it stopped being true this codec would be a denial of
service anybody could post.

## TOML

```bash
dotnet add package AceMq.Amqp.Toml
```

```csharp
using var mq = await AceMqConnection.ConnectAsync(url, new TomlCodec());
```

The same audience as YAML — a message a person reads and edits — with the ambiguity
removed:

```toml
service = "orders"
enabled = true
regions = ["eu-west-1", "us-east-1"]
```

One way to write a string, no significant indentation, and **no Norway problem**. In
YAML `country: NO` is the boolean false; in TOML an unquoted `NO` is not a value at
all, so the mistake is a parse error rather than a country turning into `false`
somewhere downstream. Where a human edits the message and a machine acts on it, that
matters more than terseness.

Duplicate keys are refused rather than resolved — silently taking the first or the
last would be a message that means something other than it looks like.

Keys are camelCased on the wire and read case-insensitively, so a message
hand-edited by somebody who capitalised one still reads.

### The shape has to suit it

TOML is a **table format**: a message body must be an object at the top level. A bare
list or number is not a TOML document, and the codec says so rather than emitting
something that is not TOML:

```
AceFatalException: List`1 cannot be written as TOML: ... TOML is a table format, so a
message body has to be an object at the top level. Use JsonCodec where the payload is
a list, a scalar, or a deep tree.
```

Deep nesting reads poorly too. Where the payload is a tree rather than a table, JSON
is the honest answer.

Like YAML, it **never volunteers for a message with no content type**.

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
