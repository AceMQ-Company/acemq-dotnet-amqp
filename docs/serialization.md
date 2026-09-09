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
| `XmlCodec` | `application/xml` | `XmlSerializer`, for .NET talking to .NET only — see [XML](#xml) |
| `StringCodec` | `text/plain` | text, as UTF-8 |
| `BytesCodec` | `application/octet-stream` | bytes, untouched |
| `CompositeCodec` | first codec's | reads several, writes one |

Those five need nothing but the framework — the core's whole dependency list is two
Microsoft packages.

Formats that need an outside library get their own package, so an application that
wants one does not acquire the rest:

| Package | Codec | Depends on |
|---|---|---|
| `AceMq.Amqp.Protobuf` | `ProtobufCodec` | `Google.Protobuf` |
| `AceMq.Amqp.Avro` | `AvroCodec` | `Apache.Avro` |
| `AceMq.Amqp.Yaml` | `YamlCodec` | `YamlDotNet` |
| `AceMq.Amqp.Toml` | `TomlCodec` | `Tomlyn` |
| `AceMq.Amqp.Xml` | `InteropXmlCodec` | nothing |
| `AceMq.Amqp.Crypto` | `EncryptedCodec` | `BouncyCastle.Cryptography` |

That is every format the Java library has, plus payload encryption — which is in a
package for the same reason and one of its own besides, covered in
[Encrypting the payload](#encrypting-the-payload).

**What each costs you.** `YamlDotNet` has no dependencies of its own.
`Google.Protobuf` has none either. `Apache.Avro` brings `Newtonsoft.Json` and
`System.CodeDom`. `Tomlyn` brings `System.Text.Json` at a **higher version than the
core pins**, and NuGet resolves to the higher one — so an application taking the TOML
package moves from `System.Text.Json` 8.0.5 to 10.0.2. Nothing breaks, but it is the
kind of thing worth knowing before it happens, and it is exactly why these are
separate packages rather than part of the core.

`AceMq.Amqp.Xml` costs nothing: `System.Xml` is part of the framework and
`System.Text.Json` arrives with the core, which already pins it. It is a separate
package for the other reason these exist — so the core's list of formats does not
grow by one every time somebody wants a different one.

`AceMq.Amqp.Crypto` brings `BouncyCastle.Cryptography`, which has no dependencies of
its own. It is there because AES-GCM has to come from somewhere on `netstandard2.0`,
and it is in its own package so a consumer publishing plaintext never sees it.

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

The content type written is `application/x-protobuf`, the same as Java's. Three are
read, plus a suffix:

| Read | Written | Who writes it |
|---|---|---|
| `application/x-protobuf` | yes | this library, Java, Go, Python, Ruby |
| `application/protobuf` | no | the IETF draft's spelling |
| `application/vnd.google.protobuf` | no | Google's own tooling |
| anything ending `+protobuf` | no | a schema registry wrapping the same bytes |

The asymmetry is deliberate. A wider read set costs a string comparison; a narrower
one silently refuses a message it could have read perfectly well, and the refusal
looks like a broken producer rather than a fussy consumer. `ProtobufCodec.ReadableContentTypes`
is the list, if you want to assert on it.

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
`application/x-yaml`, `text/yaml`, `text/x-yaml` and anything ending `+yaml` — the
four names YAML has gone by, which is the same set the Java, Python and Ruby codecs
take. In a
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

## XML

```bash
dotnet add package AceMq.Amqp.Xml
```

```csharp
using var mq = await AceMqConnection.ConnectAsync(url, new InteropXmlCodec());
```

Here because most estates have something that speaks XML and will not be rewritten,
and a messaging library that cannot talk to it forces a translation layer nobody
wants to own. New services should publish JSON; this exists so the ones that cannot
are not a special case.

It writes `application/xml`, and reads `application/xml`, `text/xml` and any `+xml`
suffix type — `application/soap+xml`, `application/atom+xml`, a vendor type. Like
YAML and TOML it **never volunteers for a message with no content type**: XML is
rarely what arrives unannounced, and a codec that guessed wrong there would turn a
readable message into a rejected one. That case belongs to `JsonCodec` and
`BytesCodec`.

### Two XML codecs, and which one you want

| | `XmlCodec` (core) | `InteropXmlCodec` (`AceMq.Amqp.Xml`) |
|---|---|---|
| Built on | `XmlSerializer` | `XmlReader` + `System.Text.Json` |
| Writes | declaration, `xsi`/`xsd` namespaces, PascalCase | bare elements, camelCase |
| Element names | the member's own name, matched case-**sensitively** | camelCased, matched case-insensitively |
| Reads Java and Go messages | no — it throws | yes |
| Use it for | .NET to .NET, and documents that must match an XSD | anything the other four languages send |

The second row is the whole point. `XmlSerializer` binds element names
case-sensitively, so Java's `<orderId>` does not bind to a C# `OrderId`.

**Up to 0.3.0 it did not complain about that.** It returned an object with every
field at its default. A service that reached for the core codec to read a Java queue
saw empty orders and no errors, which is the worst of both: the message was gone and
nothing said so.

Since 0.4.0 `XmlCodec` throws instead:

```
AceFatalException: nothing in the body bound to Order: all 6 of its elements and
attributes were unknown to XmlSerializer, which matches names case-sensitively. This
is what a body written by the Java, Go, Python or Ruby AceMQ library looks like here
-- camelCase elements against PascalCase members. Read it with
AceMq.Amqp.Xml.InteropXmlCodec, from the AceMq.Amqp.Xml package, which reads what the
other four write.
```

It is an `AceFatalException`, so the message is dead-lettered rather than retried
forever: the same bytes bind no better on the next attempt.

**The refusal is narrow on purpose.** It fires only when the document had content and
*none of it* reached a member. A document with some elements bound and some unknown
still decodes — that is what a producer adding a field looks like, and breaking
forward compatibility would be a worse bug than the one being fixed. An empty
document that legitimately decodes to an all-default object still decodes too,
because nothing in it was unknown.

#### Why not just mark `XmlCodec` obsolete

That was the other candidate, and it was rejected. `XmlCodec` is *correct* for .NET
talking to .NET and for a document that has to match an XSD; an obsolete warning on a
correct use is noise, and noise is what teaches people to suppress warnings. The
failure was silence, not existence, so silence is what was fixed — and the exception
arrives at exactly the moment it is useful, naming the codec that would have worked,
in front of somebody holding a stack trace.

The names still differ so that a consumer with both `using AceMq.Amqp;` and
`using AceMq.Amqp.Xml;` gets a choice rather than a `CS0104`.

This is a breaking change: code that relied on getting an empty object back now gets
an exception. That is the intent — there is no version of "relied on getting an empty
object back" that was working.

### No document type declaration, ever

A body carrying `<!DOCTYPE` is refused, and there is no way to relax it. The same
decision Java, Python and Ruby took, for the same reason: a message body has no
legitimate use for a DTD — it is one document produced by a serializer at the other
end, and none of Jackson, `encoding/xml`, `ElementTree` or REXML writes one — so the
configuration would only ever be wrong.

The usual justification given for this is external entities, and it is the wrong one
here. There is no `XmlResolver`, so `file:///etc/passwd` is already inert. **Internal
entity expansion is not.** A billion-laughs bomb needs no network access and no
readable file; it expands inside the parser. Measured against this runtime rather
than read off a table, with `DtdProcessing.Parse`:

| Nested entities | Source | Expands to |
|---|---|---|
| three | 201 characters | 1,000 characters |
| four | 248 characters | 10,000 characters |
| six | 311 characters | 1,000,000 characters |

Exponential in the depth, and the six-level case is still inside the SDK's own
default 10,000,000-character entity cap — so that cap is not what saves a consumer.
The measurement is a test, not a comment: if a future runtime made expansion inert,
it fails rather than quietly becoming untrue.

The refusal is in two layers. The body is scanned for `<!DOCTYPE` before a parser
sees it, so the failure names what is wrong instead of surfacing an `XmlException`
about a security setting the caller never chose. Then the reader is created with
`DtdProcessing.Prohibit` and `XmlResolver = null`, which catches what the scan cannot
see — a UTF-16 body, where `<!DOCTYPE` is not a UTF-8 substring but the reader sniffs
the encoding and would read the DTD perfectly well. Both are tested.

### Both list shapes decode

Jackson wraps a list in an element of its own; Go's `encoding/xml` repeats the
sibling:

```xml
<lines><lines>widget</lines><lines>gasket</lines></lines>   <!-- Jackson, over a POJO -->
<lines>widget</lines><lines>gasket</lines>                  <!-- encoding/xml, and Jackson over a Map -->
```

Both are real output from those libraries — the fixture in `AceMq.Amqp.Xml.Tests` was
produced by running them — neither is going away, and both decode into the same
`List<string>`. A one-item list is a third case, because `<lines>widget</lines>` on
its own is indistinguishable from a scalar; the declared member is the only thing
that can tell them apart, so the unwrapping is driven by the target type rather than
guessed from the document. This codec writes the repeated-sibling form, which Jackson
reads as well.

### XML has no types

Every leaf arrives as text. `<totalCents>4250</totalCents>` is the four characters
`4250`, not a number, and `<paid>true</paid>` is the four characters `true`. Decoding
reads numbers, booleans and enums back out of that text, so an `int` or a `bool`
member works; decode into `string` and you get the text unchanged. There is no null
either — an empty element reads as the empty string, and nothing here invents
`xsi:nil`, because neither Jackson nor `encoding/xml` writes it.

The root element is named after the payload's type, the way Jackson uses the class's
simple name. Coming back, the root's name is **ignored**: it names the message rather
than being part of it, which is how Java's `<Order>` and Go's `<order>` read into the
same type.

Attributes arrive as ordinary members beside the elements, and namespaces are cut
back to the local name — a consumer wants the field, not the URI it was declared in,
which is also what Jackson gives reading XML into a `Map`.

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

```bash
dotnet add package AceMq.Amqp.Crypto
```

```csharp
using AceMq.Amqp.Crypto;

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

AES-256-GCM with a 128-bit tag, and a fresh nonce per message.

```
0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
```

**Byte for byte what the Java, Python, Ruby and Go libraries write, and what they
read.** An encrypted message crosses languages; that is the point of the framing
being this one and not one of its own.

The header — magic, version, length and identifier — is authenticated but not
encrypted: GCM binds all of it as associated data, so a key identifier edited to
point a consumer at a different key makes the message fail to open rather than
quietly opening as something else. A fresh nonce per message means two identical
payloads produce different ciphertexts; it matters more than that, because two
messages under one key and one nonce do not merely lose some strength under GCM,
they leak their difference outright.

A body that does not authenticate produces one error, whether the key was wrong or
the body was altered:

```
this message did not decrypt with key '2026-01'. Either that is not the key it was
written with, or it was altered after it was written
```

The two are told apart by nothing — no separate message, no separate type, no
earlier check that only one of them reaches. An error that said which it was would
be a padding oracle with better manners.

### Where AES-GCM comes from on `netstandard2.0`

It does not. `System.Security.Cryptography.AesGcm` arrived in .NET Core 3.0 and has
never existed on `netstandard2.0` or .NET Framework, which is why this library wrote
AES-256-CBC with HMAC-SHA-256 up to 0.3.0 — sound cryptography, and a format no
other AceMQ library could read, under a content type that said they could.

`AceMq.Amqp.Crypto` gets GCM from **BouncyCastle**, which has it on
`netstandard2.0`. One implementation on every target rather than the built-in
`AesGcm` on modern .NET and something else on .NET Framework: two cryptographic code
paths behind one wire format is a divergence that only ever shows up at runtime, on
one target, in somebody else's queue. The full reasoning, including what was
rejected, is in `src/AceMq.Amqp.Crypto/AceMq.Amqp.Crypto.csproj`, next to the target
it explains.

### Bodies written before 0.4.0

Releases up to and including 0.3.0 wrote a .NET-only framing:

```
[version:1][keyIdLength:1][keyId][iv:16][ciphertext][hmac-sha-256:32]
```

`Decode` still reads it, and `EncryptedCodec.KeyIdOf` still names its key. The two
are never confused and never guessed at: the family framing begins `0xAE`, this one
begins `0x01`, and a body that is neither is refused naming both. The tag is still
verified **before** anything is decrypted, exactly as it was.

**Nothing writes it any more, and this reader will be removed in 1.0.0.** Those
bodies decrypt in .NET and nowhere else, so a queue holding them has to be drained —
or republished by a .NET consumer running 0.4.0 — before anything in another
language can read it.

```csharp
if (EncryptedCodec.IsLegacyDotNetBody(body))
{
    // written by 0.3.0; only .NET can read this one
}
```

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
