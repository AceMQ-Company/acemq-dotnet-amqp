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
using Avro;
using Avro.Generic;
using Avro.IO;
using Avro.Reflect;
using Avro.Specific;

namespace AceMq.Amqp.Avro;

/// <summary>
/// Avro, in the two arrangements that make sense for messages.
/// </summary>
/// <remarks>
/// <para>
/// Avro messages are not self-describing: a reader must already hold the schema the
/// writer used, or the bytes cannot be read. That is the whole design difference
/// from JSON, and it is why this codec is constructed with a schema rather than
/// created empty.
/// </para>
/// <para>
/// <see cref="Of(Schema)"/> fixes one schema for the codec's life. Small, fast and
/// nothing extra to run — and the writer's schema is whatever the reader happens to
/// have compiled in. The moment a producer adds a field, every consumer still
/// holding the old schema reads the new bytes wrongly, and Avro will not always
/// notice. Sound only where producer and consumer are released together.
/// </para>
/// <para>
/// <see cref="Registered(ISchemaRegistry, Schema)"/> writes the schema's identifier into the front of every
/// message, so a reader can look up exactly what the writer used and let Avro
/// resolve it against its own. This is what makes adding a field safe, and it is the
/// mode to use unless there is a reason not to.
/// </para>
/// <para>
/// The framing is one zero byte, then four bytes of identifier, big-endian, then the
/// Avro body — the layout Confluent's clients use, and the same bytes the Java
/// library writes. Messages written here can be read by either.
/// </para>
/// <para>
/// <strong>The reader schema.</strong> Resolution has two halves: the schema the
/// writer used, which comes off the registry, and the schema this consumer was
/// written against, which is <see cref="ReaderSchema"/>. Given both, Avro reconciles
/// them — a field the writer added is skipped and a field the writer never wrote is
/// filled in from the reader schema's default — so the handler sees the shape it was
/// compiled against whichever version produced the message. By default the reader
/// schema is the one this codec was built with, which is what Python's
/// <c>reader_schema</c> and Ruby's <c>reader_schema:</c> also default to.
/// <see cref="Registered(ISchemaRegistry, Schema, Schema)"/> names a different one,
/// which is Java's <c>registered(registry, readerSchema)</c>, and
/// <see cref="WithoutReaderSchema"/> turns resolution off altogether so every message
/// is read with the shape its writer gave it.
/// </para>
/// </remarks>
public sealed class AvroCodec : ICodec
{
    /// <summary>Content type when the schema is fixed at construction.</summary>
    public const string FixedContentType = "avro/binary";

    /// <summary>Content type when each message carries a schema identifier.</summary>
    public const string RegisteredContentType = "application/vnd.acemq.avro";

    private const byte Magic = 0x00;
    private const int FrameSize = 5;

    private readonly Schema _schema;
    private readonly Schema? _readerSchema;
    private readonly ISchemaRegistry? _registry;

    /// <summary>
    /// One reflection cache per type and schema it was mapped onto.
    /// </summary>
    /// <remarks>
    /// Keyed by the schema as well as the type, because there is now more than one
    /// schema a type can be read against: the codec's own, a reader schema given
    /// explicitly, or — with resolution off — whatever the writer used, which is a
    /// different schema for every version on the queue. One cache mapped onto one
    /// schema and reused for all of them would map a class onto fields it does not
    /// have.
    /// </remarks>
    private readonly Dictionary<string, ClassCache> _caches = new Dictionary<string, ClassCache>();

    private AvroCodec(Schema schema, Schema? readerSchema, ISchemaRegistry? registry)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _readerSchema = readerSchema;
        _registry = registry;
    }

    /// <summary>One schema, fixed for this codec's life.</summary>
    /// <remarks>
    /// Use this only where the producer and every consumer are released together.
    /// Otherwise a producer adding a field silently changes what older consumers
    /// read.
    /// </remarks>
    public static AvroCodec Of(Schema schema) => new AvroCodec(schema, schema, null);

    /// <summary>One schema, parsed from its JSON.</summary>
    public static AvroCodec Of(string schemaJson) =>
        Of(Schema.Parse(schemaJson ?? throw new ArgumentNullException(nameof(schemaJson))));

    /// <summary>
    /// Writes a schema identifier into each message, and resolves it on the way back.
    /// </summary>
    /// <param name="registry">Where schemas are registered and looked up.</param>
    /// <param name="schema">The schema this codec writes, and the reader schema every
    /// message is resolved onto unless one of the two calls below says otherwise.</param>
    /// <remarks>
    /// The registry has to be shared across processes for this to mean anything. An
    /// <see cref="InMemorySchemaRegistry"/> issues ids per process, so a message
    /// written by one and read by another refers to an id the second never issued —
    /// use <c>DbSchemaRegistry</c> or your own.
    /// </remarks>
    public static AvroCodec Registered(ISchemaRegistry registry, Schema schema) =>
        new AvroCodec(schema, schema, registry ?? throw new ArgumentNullException(nameof(registry)));

    public static AvroCodec Registered(ISchemaRegistry registry, string schemaJson) =>
        Registered(registry, Schema.Parse(schemaJson ?? throw new ArgumentNullException(nameof(schemaJson))));

    /// <summary>
    /// As <see cref="Registered(ISchemaRegistry, Schema)"/>, but reading every message
    /// against a schema other than the one it writes.
    /// </summary>
    /// <param name="registry">Where schemas are registered and looked up.</param>
    /// <param name="schema">The schema this codec writes. Only this one is registered.</param>
    /// <param name="readerSchema">The schema this consumer was written against, which
    /// every message is resolved onto.</param>
    /// <remarks>
    /// For a service that publishes one version and consumes another. Java spells this
    /// <c>registered(registry, readerSchema)</c>, Python <c>reader_schema=</c>, Ruby
    /// <c>reader_schema:</c> and Go <c>ReaderSchema</c>; it is the same concept under
    /// the same name in all five.
    /// </remarks>
    public static AvroCodec Registered(
        ISchemaRegistry registry, Schema schema, Schema readerSchema) =>
        new AvroCodec(
            schema,
            readerSchema ?? throw new ArgumentNullException(nameof(readerSchema)),
            registry ?? throw new ArgumentNullException(nameof(registry)));

    public static AvroCodec Registered(
        ISchemaRegistry registry, string schemaJson, string readerSchemaJson) =>
        Registered(
            registry,
            Schema.Parse(schemaJson ?? throw new ArgumentNullException(nameof(schemaJson))),
            Schema.Parse(readerSchemaJson ?? throw new ArgumentNullException(nameof(readerSchemaJson))));

    /// <summary>
    /// The same codec with resolution switched off: every message is read with the
    /// shape its writer gave it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The opt-out, and the reason <see cref="ReaderSchema"/> is nullable. Resolution
    /// is the right default and is on without being asked for, but it is a choice, and
    /// until now it was one a caller could not decline: whatever schema the codec was
    /// built with was imposed on every message, so a consumer that wanted to see
    /// exactly what a producer sent — a bridge, an inspector, a dead-letter drainer
    /// reading versions it was never compiled against — had no way to ask for it.
    /// Java and Go have always let a registered codec read without a reader schema;
    /// this is that.
    /// </para>
    /// <para>
    /// What it costs is the thing resolution buys. A field the writer added arrives
    /// rather than being skipped, and a field the writer has not started sending is
    /// absent rather than taking the reader schema's default — so the target type has
    /// to match what the writer actually wrote, version by version.
    /// </para>
    /// </remarks>
    /// <returns>A codec reading with the writer's schema. This one is unchanged.</returns>
    /// <exception cref="AceFatalException">
    /// on a fixed-schema codec, which has no writer schema to read with: nothing is
    /// framed, so the only schema on hand is the codec's own.
    /// </exception>
    public AvroCodec WithoutReaderSchema()
    {
        if (_registry == null)
        {
            throw new AceFatalException(
                "a fixed-schema codec has no writer's schema to read with: an unframed " +
                "message carries no identifier, so the schema this codec was built with " +
                "is the only one there is. Build it with Registered(...) to read what " +
                "each writer actually wrote.");
        }
        return new AvroCodec(_schema, null, _registry);
    }

    /// <summary>The schema this codec writes.</summary>
    public Schema Schema => _schema;

    /// <summary>
    /// The schema every message is resolved onto, or null when resolution is off.
    /// </summary>
    /// <remarks>
    /// The same schema as <see cref="Schema"/> unless
    /// <see cref="Registered(ISchemaRegistry, Schema, Schema)"/> named a different one,
    /// and null after <see cref="WithoutReaderSchema"/>, which reads every message with
    /// the writer's own shape instead.
    /// </remarks>
    public Schema? ReaderSchema => _readerSchema;

    /// <summary>Whether each message carries a schema identifier.</summary>
    public bool IsRegistered => _registry != null;

    public string ContentType => _registry != null ? RegisteredContentType : FixedContentType;

    public byte[] Encode(object payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));

        using var buffer = new MemoryStream();

        if (_registry != null)
        {
            var id = _registry.IdFor(Definition());
            buffer.WriteByte(Magic);
            buffer.WriteByte((byte)((id >> 24) & 0xFF));
            buffer.WriteByte((byte)((id >> 16) & 0xFF));
            buffer.WriteByte((byte)((id >> 8) & 0xFF));
            buffer.WriteByte((byte)(id & 0xFF));
        }

        var encoder = new BinaryEncoder(buffer);
        WriterFor(payload.GetType()).Write(payload, encoder);
        encoder.Flush();
        return buffer.ToArray();
    }

    public object Decode(byte[] body, Type target)
    {
        if (body == null) throw new ArgumentNullException(nameof(body));
        if (target == null) throw new ArgumentNullException(nameof(target));

        var offset = 0;
        var writerSchema = _schema;

        if (_registry != null)
        {
            if (body.Length < FrameSize || body[0] != Magic)
            {
                throw new AceFatalException(
                    "this message carries no schema identifier. A codec built with " +
                    "Registered(...) reads messages written by one; a message written with " +
                    "Of(...) has no frame and needs a codec built the same way.");
            }

            var id = (body[1] << 24) | (body[2] << 16) | (body[3] << 8) | body[4];
            offset = FrameSize;

            // The writer's schema, resolved against ours. This is the step that lets
            // a producer add a field without every consumer being redeployed first.
            var registered = _registry.SchemaFor(id);
            writerSchema = Schema.Parse(registered.Definition);
        }

        try
        {
            var decoder = new BinaryDecoder(new MemoryStream(body, offset, body.Length - offset));
            return ReaderFor(target, writerSchema).Read(null!, decoder)
                   ?? throw new AceFatalException($"the message body decoded to null as {target.Name}");
        }
        catch (AvroException e)
        {
            // A schema mismatch fails identically every time, so it is fatal rather
            // than retryable and the message is dead-lettered instead of looping.
            throw new AceFatalException(
                $"this message could not be read as {target.Name}: {e.Message}", e);
        }
    }

    public bool CanDecode(string? contentType)
    {
        if (contentType == null) return false;
        return contentType.StartsWith(ContentType, StringComparison.OrdinalIgnoreCase)
               || contentType.StartsWith("application/avro", StringComparison.OrdinalIgnoreCase)
               || contentType.IndexOf("+avro", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private SchemaDefinition Definition() =>
        new SchemaDefinition("avro", SubjectOf(_schema), _schema.ToString());

    private static string SubjectOf(Schema schema) =>
        schema is NamedSchema named ? named.Fullname : schema.Tag.ToString();

    /// <summary>
    /// The writer for a payload, chosen by what the type is rather than configured.
    /// </summary>
    /// <remarks>
    /// A type generated by <c>avrogen</c> implements <see cref="ISpecificRecord"/> and
    /// carries its own schema; a <see cref="GenericRecord"/> holds one; anything else
    /// is a plain class mapped onto the schema this codec was given.
    /// </remarks>
    private DatumWriter<object> WriterFor(Type type)
    {
        if (typeof(ISpecificRecord).IsAssignableFrom(type))
        {
            return new SpecificDatumWriter<object>(_schema);
        }
        if (typeof(GenericRecord).IsAssignableFrom(type))
        {
            return new GenericDatumWriter<object>(_schema);
        }
        return new ReflectWriterAdapter(type, _schema, Cache(type, _schema));
    }

    /// <summary>
    /// The reader for a message, given the schema its writer used.
    /// </summary>
    /// <remarks>
    /// The reader schema is <see cref="ReaderSchema"/>, and the writer's own when
    /// that is null. Handing Avro the same schema twice is what "no resolution"
    /// means to it: there is nothing to reconcile, so the bytes are read exactly as
    /// they were written.
    /// </remarks>
    private DatumReader<object> ReaderFor(Type type, Schema writerSchema)
    {
        var readerSchema = _readerSchema ?? writerSchema;

        if (typeof(ISpecificRecord).IsAssignableFrom(type))
        {
            return new SpecificDatumReader<object>(writerSchema, readerSchema);
        }
        if (typeof(GenericRecord).IsAssignableFrom(type))
        {
            return new GenericDatumReader<object>(writerSchema, readerSchema);
        }
        return new ReflectReaderAdapter(
            type, writerSchema, readerSchema, Cache(type, readerSchema));
    }

    /// <summary>
    /// The reflection cache for one type read against one schema, loaded once.
    /// </summary>
    /// <remarks>
    /// Loading it maps the class's members onto the schema's fields. Doing that per
    /// message would put reflection on the hot path for a result that never changes.
    /// </remarks>
    private ClassCache Cache(Type type, Schema schema)
    {
        var key = type.AssemblyQualifiedName + " " + schema;
        lock (_caches)
        {
            if (!_caches.TryGetValue(key, out var cache))
            {
                cache = new ClassCache();
                cache.LoadClassCache(type, schema);
                _caches[key] = cache;
            }
            return cache;
        }
    }

    /// <summary>Presents the reflect writer through the interface the rest of this uses.</summary>
    private sealed class ReflectWriterAdapter : DatumWriter<object>
    {
        private readonly ReflectDefaultWriter _writer;

        internal ReflectWriterAdapter(Type type, Schema schema, ClassCache cache)
        {
            _writer = new ReflectDefaultWriter(type, schema, cache);
            Schema = schema;
        }

        public Schema Schema { get; }

        public void Write(object datum, Encoder encoder) => _writer.Write(datum, encoder);
    }

    private sealed class ReflectReaderAdapter : DatumReader<object>
    {
        private readonly ReflectDefaultReader _reader;
        private readonly Type _type;

        internal ReflectReaderAdapter(Type type, Schema writerSchema, Schema readerSchema, ClassCache cache)
        {
            _type = type;
            _reader = new ReflectDefaultReader(type, writerSchema, readerSchema, cache);
            WriterSchema = writerSchema;
            ReaderSchema = readerSchema;
        }

        public Schema WriterSchema { get; }
        public Schema ReaderSchema { get; }

        public object Read(object reuse, Decoder decoder) => _reader.Read(reuse, decoder);
    }

    public override string ToString()
    {
        if (_registry == null) return $"AvroCodec[fixed, {SubjectOf(_schema)}]";
        var reading = _readerSchema == null
            ? "as written"
            : $"onto {SubjectOf(_readerSchema)}";
        return $"AvroCodec[registered, {SubjectOf(_schema)}, reading {reading}]";
    }
}
