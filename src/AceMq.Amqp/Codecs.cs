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
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml.Serialization;

namespace AceMq.Amqp;

/// <summary>Text, as UTF-8.</summary>
/// <remarks>
/// Distinct from <see cref="BytesCodec"/> in what it says about itself: this
/// declares <c>text/plain</c>, so a consumer reading the content type knows it is
/// looking at text rather than at an opaque blob.
/// </remarks>
public sealed class StringCodec : ICodec
{
    public string ContentType => "text/plain; charset=utf-8";

    public byte[] Encode(object payload) =>
        Encoding.UTF8.GetBytes(
            payload as string
            ?? Convert.ToString(payload, System.Globalization.CultureInfo.InvariantCulture)
            ?? string.Empty);

    public object Decode(byte[] body, Type target)
    {
        var text = Encoding.UTF8.GetString(body);
        if (target == typeof(string)) return text;
        // Anything else would be a silent conversion the caller did not ask for.
        throw new AceFatalException($"StringCodec decodes to string, not {target.Name}");
    }

    public bool CanDecode(string? contentType) =>
        contentType != null && contentType.StartsWith("text/", StringComparison.OrdinalIgnoreCase);

    public override string ToString() => "StringCodec";
}

/// <summary>
/// XML, via <see cref="XmlSerializer"/>, for .NET-shaped and XSD-shaped documents.
/// </summary>
/// <remarks>
/// <para>
/// For talking to systems that speak XML and will not be changed. It carries the
/// constraints <see cref="XmlSerializer"/> has always had: the type needs a public
/// parameterless constructor, only public read/write members are serialized, and
/// interfaces and dictionaries are not supported.
/// </para>
/// <para>
/// <strong>This is not the codec for reading the other AceMQ libraries.</strong>
/// <see cref="XmlSerializer"/> matches element names case-sensitively, so the
/// <c>&lt;orderId&gt;</c> a Java, Go, Python or Ruby publisher writes does not bind
/// to an <c>OrderId</c> member. Use <c>AceMq.Amqp.Xml.InteropXmlCodec</c>, in the
/// <c>AceMq.Amqp.Xml</c> package, for a queue any of those four publish to.
/// </para>
/// <para>
/// Until 0.4.0 that mismatch was silent: a body written by a sibling library came
/// back as a fully-formed object with every member at its default and no exception
/// anywhere, which is the worst failure a codec can have — the message is gone and
/// nothing says so. It now throws when a document binds to nothing at all. See
/// <see cref="Decode"/> for what that means exactly and what it deliberately does
/// not do.
/// </para>
/// <para>
/// Marking the whole type <c>[Obsolete]</c> was the other candidate and was
/// rejected: this codec is correct for .NET talking to .NET and for a document that
/// has to match an XSD, and an obsolete warning on a correct use is noise that
/// teaches people to suppress warnings. The failure was silence, not existence, so
/// silence is what was fixed.
/// </para>
/// <para>
/// Reading is deliberately restricted. The serializer is created with DTD
/// processing off and no external resolver, so a document cannot pull in an
/// external entity — the XXE class of attack, which turns "we accept XML" into
/// "we read files off the consumer's disk on request".
/// </para>
/// </remarks>
public sealed class XmlCodec : ICodec
{
    private static readonly Dictionary<Type, XmlSerializer> Serializers =
        new Dictionary<Type, XmlSerializer>();

    public string ContentType => "application/xml";

    public byte[] Encode(object payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));
        using var buffer = new MemoryStream();
        SerializerFor(payload.GetType()).Serialize(buffer, payload);
        return buffer.ToArray();
    }

    /// <summary>Reads a body, and refuses one that bound to nothing.</summary>
    /// <remarks>
    /// <para>
    /// The refusal is narrow on purpose: it fires only when the document had content
    /// <em>and none of it reached a member</em>. A document with some elements bound
    /// and some unknown still decodes, because that is what a producer adding a field
    /// looks like and breaking forward compatibility would be a worse bug than the one
    /// being fixed. An empty document that legitimately decodes to an all-default
    /// object still decodes, because nothing was unknown in it.
    /// </para>
    /// <para>
    /// The check costs a counter increment per unbound node on the way in, and the
    /// reflection that decides whether anything bound runs only for a document that
    /// already had an unbound node — never on the path where everything matched.
    /// </para>
    /// </remarks>
    /// <exception cref="AceFatalException">
    /// The body bound to no member of <paramref name="target"/> — almost always a
    /// case mismatch, and almost always a body one of the sibling libraries wrote.
    /// Fatal rather than retryable: the same bytes bind no better on the next attempt,
    /// so the message is dead-lettered instead of looping.
    /// </exception>
    public object Decode(byte[] body, Type target)
    {
        var settings = new System.Xml.XmlReaderSettings
        {
            // An XML parser that resolves external entities will fetch whatever a
            // message tells it to, including local files. Neither is ever wanted
            // for a message body.
            DtdProcessing = System.Xml.DtdProcessing.Prohibit,
            XmlResolver = null,
        };

        // Counted through XmlDeserializationEvents, which is passed to this one call,
        // rather than through the serializer's UnknownElement event, which would put
        // mutable per-message state on an instance shared by every thread decoding
        // this type.
        var unbound = 0;
        var events = new XmlDeserializationEvents
        {
            OnUnknownElement = (sender, e) => unbound++,
            OnUnknownAttribute = (sender, e) => unbound++,
        };

        using var buffer = new MemoryStream(body);
        using var reader = System.Xml.XmlReader.Create(buffer, settings);
        var decoded = SerializerFor(target).Deserialize(reader, null, events)
                      ?? throw new AceFatalException(
                          $"the message body decoded to null as {target.Name}");

        if (unbound > 0 && !BoundAnything(decoded))
        {
            throw new AceFatalException(
                $"nothing in the body bound to {target.Name}: all {unbound} of its " +
                "elements and attributes were unknown to XmlSerializer, which matches " +
                "names case-sensitively. This is what a body written by the Java, Go, " +
                "Python or Ruby AceMQ library looks like here -- camelCase elements " +
                "against PascalCase members. Read it with " +
                "AceMq.Amqp.Xml.InteropXmlCodec, from the AceMq.Amqp.Xml package, " +
                "which reads what the other four write. Returning the empty object " +
                "this used to return lost the message without saying so.");
        }

        return decoded;
    }

    /// <summary>Whether anything in the document actually reached a member.</summary>
    /// <remarks>
    /// Deliberately generous about what counts as bound. Every ambiguous answer is
    /// "yes", because a false yes leaves the old behaviour in place for one message
    /// and a false no turns a decode that worked into an exception.
    /// </remarks>
    private static bool BoundAnything(object decoded)
    {
        var type = decoded.GetType();
        if (decoded is string text) return text.Length > 0;

        var members = 0;

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanRead || property.GetIndexParameters().Length > 0) continue;
            members++;
            object? value;
            // A getter that throws says nothing about binding, and turning somebody
            // else's property bug into a decode failure would be this method's fault
            // rather than theirs.
            try { value = property.GetValue(decoded); }
            catch (Exception) { return true; }
            if (!IsDefault(value)) return true;
        }

        foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
        {
            members++;
            object? value;
            try { value = field.GetValue(decoded); }
            catch (Exception) { return true; }
            if (!IsDefault(value)) return true;
        }

        // A type with nothing to bind to -- and so no evidence either way. Not this
        // method's business, so it does not report a loss.
        return members == 0;
    }

    private static bool IsDefault(object? value)
    {
        switch (value)
        {
            case null:
                return true;
            case string text:
                return text.Length == 0;
            case System.Collections.ICollection collection:
                return collection.Count == 0;
            default:
                var type = value.GetType();
                return type.IsValueType && value.Equals(Activator.CreateInstance(type));
        }
    }

    public bool CanDecode(string? contentType) =>
        contentType != null
        && (contentType.StartsWith("application/xml", StringComparison.OrdinalIgnoreCase)
            || contentType.StartsWith("text/xml", StringComparison.OrdinalIgnoreCase));

    private static XmlSerializer SerializerFor(Type type)
    {
        // Cached because constructing one emits a dynamic assembly. Creating a
        // serializer per message leaks assemblies that are never collected, which
        // shows up as memory growing forever under load.
        lock (Serializers)
        {
            if (!Serializers.TryGetValue(type, out var serializer))
            {
                serializer = new XmlSerializer(type);
                Serializers[type] = serializer;
            }
            return serializer;
        }
    }

    public override string ToString() => "XmlCodec";
}

/// <summary>
/// Several codecs, choosing between them by content type.
/// </summary>
/// <remarks>
/// What to use during a format migration: publish in the new format while still
/// reading both, until nothing is left that speaks the old one. The first codec
/// encodes; all of them can decode.
/// </remarks>
public sealed class CompositeCodec : ICodec, IContentTypeCodec
{
    private readonly IReadOnlyList<ICodec> _codecs;

    private CompositeCodec(IReadOnlyList<ICodec> codecs) => _codecs = codecs;

    public static CompositeCodec Of(params ICodec[] codecs)
    {
        if (codecs == null || codecs.Length == 0)
        {
            throw new ArgumentException("at least one codec is needed", nameof(codecs));
        }
        return new CompositeCodec(codecs.ToArray());
    }

    /// <summary>The codecs, in the order they are tried. The first one encodes.</summary>
    public IReadOnlyList<ICodec> Codecs => _codecs;

    public string ContentType => _codecs[0].ContentType;

    public byte[] Encode(object payload) => _codecs[0].Encode(payload);

    public object Decode(byte[] body, Type target) => Decode(body, target, null);

    /// <summary>Decodes with whichever codec recognises the content type.</summary>
    /// <remarks>
    /// With no content type nothing can be ruled out, so every codec is a
    /// candidate rather than the first one being assumed right. Candidates are
    /// tried in turn and the first that reads the body wins -- so a message that
    /// claims one format and is written in another is still read -- and only when
    /// every one has refused is anything reported, with all the reasons.
    /// </remarks>
    public object Decode(byte[] body, Type target, string? contentType)
    {
        var candidates = contentType == null
            ? _codecs.ToList()
            : _codecs.Where(c => c.CanDecode(contentType)).ToList();
        if (candidates.Count == 0)
        {
            throw new AceFatalException(
                $"no codec here reads '{contentType ?? "(none)"}'. Available: " +
                string.Join(", ", _codecs.Select(c => c.ContentType)));
        }

        var refusals = new List<string>();
        foreach (var codec in candidates)
        {
            try
            {
                return codec.Decode(body, target);
            }
            catch (Exception failure)
            {
                refusals.Add($"{codec}: {failure.Message}");
            }
        }

        throw new AceFatalException(
            $"no codec could read this message as {target.Name}. Tried " +
            string.Join("; ", refusals));
    }

    public bool CanDecode(string? contentType) => _codecs.Any(c => c.CanDecode(contentType));

    public override string ToString() =>
        $"CompositeCodec[{string.Join(", ", _codecs.Select(c => c.ToString()))}]";
}

/// <summary>Something that can supply a codec by name.</summary>
public interface ICodecProvider
{
    string Name { get; }

    ICodec Create();
}

/// <summary>
/// The codecs available by name.
/// </summary>
/// <remarks>
/// For choosing a format from configuration rather than in code. Registration is
/// explicit; nothing scans assemblies, so a format is available because something
/// registered it and not because a package happened to be present.
/// </remarks>
public static class CodecRegistry
{
    public const string DefaultFormat = "json";

    private static readonly Dictionary<string, ICodecProvider> Providers =
        new Dictionary<string, ICodecProvider>(StringComparer.OrdinalIgnoreCase);

    static CodecRegistry()
    {
        Register(new Provider("json", () => new JsonCodec()));
        Register(new Provider("bytes", () => new BytesCodec()));
        Register(new Provider("string", () => new StringCodec()));
        Register(new Provider("xml", () => new XmlCodec()));
    }

    public static void Register(ICodecProvider provider)
    {
        if (provider == null) throw new ArgumentNullException(nameof(provider));
        lock (Providers) Providers[provider.Name] = provider;
    }

    /// <summary>Registers a codec under a name.</summary>
    public static void Register(string name, Func<ICodec> create) =>
        Register(new Provider(name, create));

    /// <summary>The codec registered under a name.</summary>
    /// <exception cref="AceFatalException">when nothing is registered under it.</exception>
    public static ICodec ByName(string name)
    {
        lock (Providers)
        {
            if (Providers.TryGetValue(name, out var provider)) return provider.Create();
            throw new AceFatalException(
                $"no codec named '{name}'. Registered: {string.Join(", ", Names())}");
        }
    }

    public static IReadOnlyList<string> Names()
    {
        lock (Providers)
        {
            var names = new List<string>(Providers.Keys);
            names.Sort(StringComparer.Ordinal);
            return names;
        }
    }

    private sealed class Provider : ICodecProvider
    {
        private readonly Func<ICodec> _create;

        internal Provider(string name, Func<ICodec> create)
        {
            Name = name;
            _create = create;
        }

        public string Name { get; }
        public ICodec Create() => _create();
    }
}
