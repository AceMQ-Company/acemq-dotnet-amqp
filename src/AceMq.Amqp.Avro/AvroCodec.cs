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
    private readonly ISchemaRegistry? _registry;
    private readonly ClassCache _cache = new ClassCache();
    private readonly Dictionary<Type, bool> _loaded = new Dictionary<Type, bool>();

    private AvroCodec(Schema schema, ISchemaRegistry? registry)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _registry = registry;
    }

    /// <summary>One schema, fixed for this codec's life.</summary>
    /// <remarks>
    /// Use this only where the producer and every consumer are released together.
    /// Otherwise a producer adding a field silently changes what older consumers
    /// read.
    /// </remarks>
    public static AvroCodec Of(Schema schema) => new AvroCodec(schema, null);

    /// <summary>One schema, parsed from its JSON.</summary>
    public static AvroCodec Of(string schemaJson) =>
        Of(Schema.Parse(schemaJson ?? throw new ArgumentNullException(nameof(schemaJson))));

    /// <summary>
    /// Writes a schema identifier into each message, and resolves it on the way back.
    /// </summary>
    /// <param name="registry">Where schemas are registered and looked up.</param>
    /// <param name="schema">The schema this codec writes, and reads into.</param>
    /// <remarks>
    /// The registry has to be shared across processes for this to mean anything. An
    /// <see cref="InMemorySchemaRegistry"/> issues ids per process, so a message
    /// written by one and read by another refers to an id the second never issued —
    /// use <c>DbSchemaRegistry</c> or your own.
    /// </remarks>
    public static AvroCodec Registered(ISchemaRegistry registry, Schema schema) =>
        new AvroCodec(schema, registry ?? throw new ArgumentNullException(nameof(registry)));

    public static AvroCodec Registered(ISchemaRegistry registry, string schemaJson) =>
        Registered(registry, Schema.Parse(schemaJson ?? throw new ArgumentNullException(nameof(schemaJson))));

    /// <summary>The schema this codec writes.</summary>
    public Schema Schema => _schema;

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
        return new ReflectWriterAdapter(type, _schema, Cache(type));
    }

    private DatumReader<object> ReaderFor(Type type, Schema writerSchema)
    {
        if (typeof(ISpecificRecord).IsAssignableFrom(type))
        {
            return new SpecificDatumReader<object>(writerSchema, _schema);
        }
        if (typeof(GenericRecord).IsAssignableFrom(type))
        {
            return new GenericDatumReader<object>(writerSchema, _schema);
        }
        return new ReflectReaderAdapter(type, writerSchema, _schema, Cache(type));
    }

    /// <summary>
    /// The reflection cache, loaded once per type.
    /// </summary>
    /// <remarks>
    /// Loading it maps the class's members onto the schema's fields. Doing that per
    /// message would put reflection on the hot path for a result that never changes.
    /// </remarks>
    private ClassCache Cache(Type type)
    {
        lock (_loaded)
        {
            if (!_loaded.ContainsKey(type))
            {
                _cache.LoadClassCache(type, _schema);
                _loaded[type] = true;
            }
            return _cache;
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

    public override string ToString() =>
        _registry != null
            ? $"AvroCodec[registered, {SubjectOf(_schema)}]"
            : $"AvroCodec[fixed, {SubjectOf(_schema)}]";
}
