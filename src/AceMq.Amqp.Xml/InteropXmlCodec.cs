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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Xml;
using System.Xml.Linq;

namespace AceMq.Amqp.Xml;

/// <summary>
/// Reads and writes the XML the other AceMQ libraries write.
/// </summary>
/// <remarks>
/// <para>
/// Here because most estates have something that speaks XML and will not be
/// rewritten, and a messaging library that cannot talk to it forces a translation
/// layer nobody wants to own. New services should publish JSON; this exists so the
/// ones that cannot are not a special case.
/// </para>
/// <para>
/// <strong>This is not <see cref="AceMq.Amqp.XmlCodec"/>.</strong> That one is in the
/// core, is built on <see cref="System.Xml.Serialization.XmlSerializer"/>, and writes
/// what <c>XmlSerializer</c> writes: an XML declaration and <c>xsi</c>/<c>xsd</c>
/// namespace attributes on the root. It is the right choice for .NET talking to .NET
/// and for documents that have to match an XSD. It is the wrong choice for reading a
/// message a Java or Go service sent, because <c>XmlSerializer</c> binds by attribute
/// and cannot be told that Jackson wraps a list and <c>encoding/xml</c> does not.
/// This codec is the cross-language one: it reads what Jackson's <c>XmlMapper</c>,
/// Go's <c>encoding/xml</c>, Python's <c>ElementTree</c> and Ruby's REXML write, and
/// writes something all four read back. The name is deliberately different so that a
/// consumer with <c>using AceMq.Amqp;</c> and <c>using AceMq.Amqp.Xml;</c> gets a
/// choice rather than a CS0104.
/// </para>
/// <para>
/// <strong>No document type declaration, ever.</strong> See <see cref="Decode"/>.
/// </para>
/// <para>
/// <strong>XML has no types.</strong> Every leaf arrives as text, so
/// <c>&lt;totalCents&gt;4250&lt;/totalCents&gt;</c> is the four characters
/// <c>4250</c> and not a number. Decoding reads numbers, booleans and enums back out
/// of those strings, which is what makes an <c>int</c> or a <c>bool</c> property
/// work; a consumer decoding into <c>string</c> gets the text unchanged. There is no
/// null in XML either — an empty element reads as the empty string, and nothing here
/// invents <c>xsi:nil</c>, because neither Jackson nor <c>encoding/xml</c> writes it.
/// </para>
/// <para>
/// <strong>Both list shapes are read.</strong> Jackson wraps a list in an element of
/// its own — <c>&lt;lines&gt;&lt;lines&gt;widget&lt;/lines&gt;&lt;lines&gt;gasket&lt;/lines&gt;&lt;/lines&gt;</c>
/// — while Go's <c>encoding/xml</c> repeats the sibling —
/// <c>&lt;lines&gt;widget&lt;/lines&gt;&lt;lines&gt;gasket&lt;/lines&gt;</c>. They are
/// both real: the fixtures in this repository were produced by running those
/// libraries. Neither is going away, so both decode into the same
/// <c>List&lt;string&gt;</c>. The unwrapping is driven by the target type rather than
/// guessed from the document, which is the only way to tell a wrapped list from an
/// object that happens to hold a field of its own name. This codec writes the
/// repeated-sibling form, because Jackson reads that too.
/// </para>
/// <para>
/// Property names are camelCased on the wire and matched case-insensitively coming
/// back, exactly as <see cref="JsonCodec"/> does, so a C# <c>OrderId</c> and a Java
/// <c>orderId</c> are the same element. The root element is named after the payload's
/// type, the way Jackson uses the class's simple name; on the way in the root's name
/// is ignored, because it names the message rather than being part of it — which is
/// how Java reads <c>&lt;Order&gt;</c> and Go reads <c>&lt;order&gt;</c> into the same
/// class.
/// </para>
/// <para>
/// <strong>Never volunteers for a message whose sender set no content type.</strong>
/// XML is rarely what arrives unannounced, and a codec that guesses wrong here turns
/// a readable message into a rejected one. That case belongs to
/// <see cref="JsonCodec"/> and <see cref="BytesCodec"/>.
/// </para>
/// </remarks>
public sealed class InteropXmlCodec : ICodec
{
    /// <summary>What this codec writes, and what Java, Go, Python and Ruby write.</summary>
    public const string XmlContentType = "application/xml";

    /// <summary>
    /// The root element used for a payload whose type name cannot be used — a
    /// dictionary, an anonymous type. Python's codec uses the same word.
    /// </summary>
    public const string DefaultRoot = "message";

    /// <summary>
    /// The refusal a body carrying a DTD gets, whatever the DTD would have said.
    /// </summary>
    internal const string DtdRefused =
        "this message carries a document type declaration. A message body has no use for " +
        "one, and a parser that reads DTDs will expand entities on behalf of whoever sent " +
        "the message: a few hundred bytes of nested internal entities become megabytes of " +
        "heap, with no network access and no readable file needed. This codec refuses every " +
        "DTD and the refusal is not configurable.";

    private readonly string _root;
    private readonly JsonSerializerOptions _options;

    /// <summary>Uses <see cref="DefaultRoot"/> and <see cref="DefaultOptions"/>.</summary>
    public InteropXmlCodec() : this(DefaultRoot) { }

    /// <summary>
    /// Uses a root element name of your own.
    /// </summary>
    /// <param name="root">
    /// The element to wrap a payload in when its type name cannot be used. It matters
    /// to a Go consumer whose struct declares an <c>XMLName</c>, and to nobody else:
    /// Jackson ignores the root name reading into a class, and so does
    /// <c>encoding/xml</c> for a struct without one.
    /// </param>
    public InteropXmlCodec(string root) : this(root, DefaultOptions()) { }

    /// <summary>Uses options you built yourself.</summary>
    /// <remarks>
    /// The options govern the mapping between elements and members only. Nothing in
    /// them can re-enable DTD processing; that is not a setting here.
    /// </remarks>
    public InteropXmlCodec(string root, JsonSerializerOptions options)
    {
        if (string.IsNullOrEmpty(root)) throw new ArgumentException("a root element name is needed", nameof(root));
        if (!IsElementName(root))
        {
            throw new ArgumentException($"'{root}' is not a name XML will take for an element", nameof(root));
        }

        _root = root;
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// camelCase element names, matched case-insensitively on the way back, and every
    /// scalar read out of the text XML makes of it.
    /// </summary>
    /// <remarks>
    /// <see cref="JsonSerializerOptions"/> because the element tree is bound to the
    /// payload type through <see cref="System.Text.Json"/>. That is not an
    /// implementation detail worth hiding: it means this codec agrees with
    /// <see cref="JsonCodec"/> about what a member is called and how it converts, so a
    /// service that publishes JSON and XML off the same type writes the same names in
    /// both.
    /// </remarks>
    public static JsonSerializerOptions DefaultOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            // XML has no numbers, only text that looks like one. Without this every
            // int and decimal property on every message from every language fails.
            NumberHandling = JsonNumberHandling.AllowReadingFromString,
        };
        options.Converters.Add(new BooleanFromTextConverter());
        options.Converters.Add(new EitherListShapeConverter());
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }

    /// <summary>The root element written for a payload that has no name of its own.</summary>
    public string Root => _root;

    public string ContentType => XmlContentType;

    /// <summary>
    /// Writes one element per member, and a repeated element per item of a list.
    /// </summary>
    /// <remarks>
    /// No XML declaration and no namespace attributes, which is what Jackson and
    /// <c>encoding/xml</c> write and therefore what the other four libraries expect to
    /// read.
    /// </remarks>
    public byte[] Encode(object payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));

        var type = payload.GetType();
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(JsonSerializer.SerializeToUtf8Bytes(payload, type, _options));
        }
        catch (JsonException e)
        {
            throw new AceFatalException($"{type.Name} cannot be written as XML: {e.Message}", e);
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind == JsonValueKind.Array)
            {
                // An XML document has exactly one root element, so a bare list has
                // nowhere to go. Saying so is better than inventing a wrapper the
                // other side would not know to unwrap.
                throw new AceFatalException(
                    $"{type.Name} cannot be written as XML: an XML document has one root element, " +
                    "so a message body has to be an object at the top level. Wrap the list in a " +
                    "type with a named member, or use JsonCodec.");
            }

            var element = new XElement(RootNameFor(type));
            Fill(element, root);
            return Encoding.UTF8.GetBytes(element.ToString(SaveOptions.DisableFormatting));
        }
    }

    /// <summary>
    /// Reads a body, refusing any document type declaration.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The refusal is in two layers, and both are needed.
    /// </para>
    /// <para>
    /// The body is scanned for <c>&lt;!DOCTYPE</c> before a parser sees it, so the
    /// failure names what is wrong instead of surfacing an
    /// <see cref="XmlException"/> about a security setting the caller never chose.
    /// Then the reader is created with
    /// <see cref="DtdProcessing"/><c>.Prohibit</c> and no
    /// <see cref="XmlReaderSettings.XmlResolver"/>, which catches what the scan
    /// cannot see — a UTF-16 body, where <c>&lt;!DOCTYPE</c> is not a UTF-8 substring
    /// but the reader sniffs the encoding and would read the DTD perfectly well.
    /// </para>
    /// <para>
    /// It is worth being exact about why, because the usual reason given is the wrong
    /// one. External entities need a resolver, and there is none here, so
    /// <c>file:///etc/passwd</c> is already inert. <strong>Internal entity expansion
    /// is not.</strong> Measured against this runtime rather than read off a table:
    /// with <c>DtdProcessing.Parse</c>, 201 characters of three nested entities expand
    /// to 1,000, 248 characters of four expand to 10,000, and 311 characters of six
    /// expand to a million — the last still inside the SDK's own
    /// 10,000,000-character entity cap, so that cap is not what saves a consumer. The
    /// growth is exponential in the nesting depth, it needs no network and no readable
    /// file, and a queue is exactly the sort of place a message from somewhere
    /// unexpected arrives. The measurement is a test rather than a comment: if a
    /// future runtime made expansion inert, it fails instead of this quietly becoming
    /// untrue.
    /// </para>
    /// <para>
    /// A message body has no legitimate use for a DTD in any case. It is one document
    /// produced by a serializer at the other end, and none of Jackson,
    /// <c>encoding/xml</c>, <c>ElementTree</c> or REXML writes one. So there is no
    /// constructor argument to relax this, matching the Java, Python and Ruby codecs,
    /// which say the same thing about the same decision: the configuration would only
    /// ever be wrong.
    /// </para>
    /// </remarks>
    public object Decode(byte[] body, Type target)
    {
        if (body == null) throw new ArgumentNullException(nameof(body));
        if (target == null) throw new ArgumentNullException(nameof(target));

        if (Encoding.UTF8.GetString(body).IndexOf("<!doctype", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            throw new AceFatalException(DtdRefused);
        }

        XElement root;
        try
        {
            var settings = new XmlReaderSettings
            {
                // Explicit although it is also the default. A security property that
                // is only right by accident is one nobody can check by reading.
                DtdProcessing = DtdProcessing.Prohibit,
                XmlResolver = null,
                CloseInput = false,
                IgnoreWhitespace = false,
                IgnoreComments = true,
                IgnoreProcessingInstructions = true,
            };

            using var buffer = new MemoryStream(body);
            using var reader = XmlReader.Create(buffer, settings);
            root = XElement.Load(reader);
        }
        catch (XmlException e) when (e.Message.IndexOf("DTD", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            // The second layer firing: a DTD the UTF-8 scan above could not see.
            throw new AceFatalException(DtdRefused, e);
        }
        catch (Exception e) when (e is XmlException || e is InvalidOperationException)
        {
            // Bytes that are not XML will not become XML on a redelivery, so this is
            // fatal and the message is dead-lettered rather than retried for ever.
            throw new AceFatalException($"this message is not XML: {e.Message}", e);
        }

        // The root's own name is dropped. It names the message rather than being part
        // of it, which is why Java's <OrderPlaced> and Go's <order> read into one type.
        var node = ToNode(root);
        try
        {
            return node.Deserialize(target, _options)
                   ?? throw new AceFatalException($"the message body decoded to null as {target.Name}");
        }
        catch (JsonException e)
        {
            throw new AceFatalException(
                $"this message is not XML that reads as {target.Name}: {e.Message}", e);
        }
        catch (NotSupportedException e)
        {
            throw new AceFatalException(
                $"this message is not XML that reads as {target.Name}: {e.Message}", e);
        }
    }

    /// <summary>
    /// Accepts <c>application/xml</c>, <c>text/xml</c> and any <c>+xml</c> suffix type.
    /// </summary>
    /// <remarks>
    /// Never a message with no content type: that belongs to <see cref="JsonCodec"/>
    /// and <see cref="BytesCodec"/>. The same three rules as Java, Go, Python and Ruby.
    /// </remarks>
    public bool CanDecode(string? contentType)
    {
        if (string.IsNullOrEmpty(contentType)) return false;
        return contentType!.StartsWith(XmlContentType, StringComparison.OrdinalIgnoreCase)
               // Predates the application/xml registration and is still what a lot of
               // tooling writes.
               || contentType.StartsWith("text/xml", StringComparison.OrdinalIgnoreCase)
               // application/soap+xml, application/atom+xml, and every vendor type.
               || contentType.IndexOf("+xml", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    public override string ToString() => "InteropXmlCodec";

    // ---- writing ---------------------------------------------------------

    private string RootNameFor(Type type)
    {
        // The type's own name, the way Jackson uses the class's simple name and Go
        // uses the struct's. A generic, a dictionary or an anonymous type has nothing
        // usable, so those fall back to the configured root.
        var name = type.Name;
        var generic = name.IndexOf('`');
        if (generic >= 0) return _root;
        return IsElementName(name) ? name : _root;
    }

    private static void Fill(XElement element, JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var member in value.EnumerateObject()) Append(element, member.Name, member.Value);
                break;
            case JsonValueKind.Array:
                // Only reached for a nested array -- a list of lists. There is no
                // element name to repeat but the parent's own, which is exactly what
                // Jackson's wrapped form looks like, and what this codec reads back.
                foreach (var item in value.EnumerateArray()) Append(element, element.Name.LocalName, item);
                break;
            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                // An empty element. XML has no null, and xsi:nil would be something
                // Java and Go do not write.
                break;
            default:
                element.Value = TextOf(value);
                break;
        }
    }

    private static void Append(XElement parent, string name, JsonElement value)
    {
        if (!IsElementName(name))
        {
            throw new AceFatalException($"'{name}' is not a name XML will take for an element");
        }

        if (value.ValueKind == JsonValueKind.Array)
        {
            // A repeated sibling, which is how encoding/xml writes a sequence and
            // what Jackson reads back into a list.
            foreach (var item in value.EnumerateArray()) Append(parent, name, item);
            return;
        }

        var child = new XElement(name);
        Fill(child, value);
        parent.Add(child);
    }

    private static string TextOf(JsonElement value) => value.ValueKind switch
    {
        // Lowercase, which is what Jackson and encoding/xml write. "True" would go on
        // the wire and nothing else would read it.
        JsonValueKind.True => "true",
        JsonValueKind.False => "false",
        JsonValueKind.String => value.GetString() ?? string.Empty,
        // The raw text, so a decimal keeps every digit it was given and nothing is
        // reformatted through the current culture.
        JsonValueKind.Number => value.GetRawText(),
        _ => string.Empty,
    };

    // ---- reading ---------------------------------------------------------

    /// <summary>
    /// One element as a string, or as an object when it has children or attributes.
    /// </summary>
    /// <remarks>
    /// Repeated child names become an array, which is what makes Go's lists readable.
    /// Namespaces are cut back to the local name: a consumer wants the field, not the
    /// URI it was declared in, and that is also what Jackson gives reading XML into a
    /// <c>Map</c>.
    /// </remarks>
    private static JsonNode ToNode(XElement element)
    {
        var attributes = element.Attributes().Where(a => !a.IsNamespaceDeclaration).ToList();
        if (!element.HasElements && attributes.Count == 0)
        {
            return JsonValue.Create(element.Value)!;
        }

        var result = new JsonObject();
        foreach (var attribute in attributes)
        {
            result[attribute.Name.LocalName] = JsonValue.Create(attribute.Value);
        }

        foreach (var group in element.Elements().GroupBy(child => child.Name.LocalName))
        {
            var children = group.ToList();
            if (children.Count == 1)
            {
                result[group.Key] = ToNode(children[0]);
                continue;
            }

            var array = new JsonArray();
            foreach (var child in children) array.Add(ToNode(child));
            result[group.Key] = array;
        }

        if (!element.HasElements)
        {
            // Attributes and text and nothing else. The text goes under the empty
            // key, which is the convention Jackson uses reading into a Map and what
            // the Ruby codec does. No member is called that, so a typed payload
            // ignores it -- it is here so nothing is silently dropped.
            result[string.Empty] = JsonValue.Create(element.Value);
        }

        return result;
    }

    private static bool IsElementName(string name)
    {
        if (string.IsNullOrEmpty(name)) return false;
        if (!char.IsLetter(name[0]) && name[0] != '_') return false;
        for (var i = 1; i < name.Length; i++)
        {
            var c = name[i];
            if (!char.IsLetterOrDigit(c) && c != '_' && c != '-' && c != '.') return false;
        }
        return true;
    }

    // ---- the two things XML's lack of types costs -------------------------

    /// <summary>
    /// Reads a <see cref="bool"/> out of the text XML makes of it.
    /// </summary>
    /// <remarks>
    /// <c>&lt;paid&gt;true&lt;/paid&gt;</c> is the four characters <c>true</c>. There
    /// is no <c>JsonNumberHandling</c> equivalent for booleans, so this is the
    /// converter that stops every <c>bool</c> property on every message from every
    /// language failing to bind. <c>1</c> and <c>0</c> are read too: XML Schema calls
    /// them booleans and some producers write them.
    /// </remarks>
    private sealed class BooleanFromTextConverter : JsonConverter<bool>
    {
        public override bool Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.True: return true;
                case JsonTokenType.False: return false;
                case JsonTokenType.Number: return reader.GetInt64() != 0;
                case JsonTokenType.String:
                    var text = reader.GetString();
                    if (bool.TryParse(text, out var parsed)) return parsed;
                    if (text == "1") return true;
                    if (text == "0") return false;
                    throw new JsonException($"'{text}' is not a boolean");
                default:
                    throw new JsonException($"a boolean cannot be read from {reader.TokenType}");
            }
        }

        public override void Write(Utf8JsonWriter writer, bool value, JsonSerializerOptions options) =>
            writer.WriteBooleanValue(value);
    }

    /// <summary>
    /// Reads a list written any of the three ways XML can write one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The repeated sibling Go writes, the wrapper element Jackson writes, and the
    /// single element that is all either of them writes for a one-item list — XML
    /// cannot tell that apart from a scalar, and the target type is the only thing
    /// that can. So the unwrapping happens here, where the declared member is known to
    /// be a list, rather than being guessed from the shape of the document.
    /// </para>
    /// <para>
    /// <c>List&lt;T&gt;</c> and <c>T[]</c>, which is what a message type declares. A
    /// member typed as some other collection is left to the serializer's own handling
    /// and gets the array form only.
    /// </para>
    /// </remarks>
    private sealed class EitherListShapeConverter : JsonConverterFactory
    {
        public override bool CanConvert(Type type) => ElementTypeOf(type) != null;

        public override JsonConverter CreateConverter(Type type, JsonSerializerOptions options)
        {
            var item = ElementTypeOf(type)!;
            var shape = type.IsArray ? typeof(ArrayConverter<>) : typeof(ListConverter<>);
            return (JsonConverter)Activator.CreateInstance(shape.MakeGenericType(item))!;
        }

        private static Type? ElementTypeOf(Type type)
        {
            // byte[] is left alone deliberately. It is an array, but the serializer
            // writes it as base64 text -- which is what Jackson's XmlMapper and Go's
            // encoding/xml both write for a byte slice. Taking it over here would put
            // a list of numbers on the wire that nothing else reads.
            if (type == typeof(byte[])) return null;
            if (type.IsArray && type.GetArrayRank() == 1) return type.GetElementType();
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
            {
                return type.GetGenericArguments()[0];
            }
            return null;
        }

        private static List<T> ReadItems<T>(ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            var read = JsonElement.ParseValue(ref reader);
            var source = read;

            if (source.ValueKind == JsonValueKind.Object)
            {
                // Jackson's wrapper: <lines><lines>a</lines><lines>b</lines></lines>
                // arrives here as { "lines": [ "a", "b" ] }. One property and no
                // more, or it is an object that genuinely is not a list and the
                // serializer should say so rather than this quietly taking a field.
                var members = source.EnumerateObject().ToList();
                if (members.Count != 1)
                {
                    throw new JsonException(
                        $"a list cannot be read from an element with {members.Count} distinct children");
                }
                source = members[0].Value;
            }

            var items = new List<T>();
            if (source.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in source.EnumerateArray()) items.Add(item.Deserialize<T>(options)!);
                return items;
            }

            if (source.ValueKind == JsonValueKind.Null || source.ValueKind == JsonValueKind.Undefined)
            {
                return items;
            }

            // One element, which is all a one-item list looks like in XML.
            items.Add(source.Deserialize<T>(options)!);
            return items;
        }

        private sealed class ListConverter<T> : JsonConverter<List<T>>
        {
            public override List<T> Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) =>
                ReadItems<T>(ref reader, options);

            public override void Write(Utf8JsonWriter writer, List<T> value, JsonSerializerOptions options)
            {
                writer.WriteStartArray();
                foreach (var item in value) JsonSerializer.Serialize(writer, item, options);
                writer.WriteEndArray();
            }
        }

        private sealed class ArrayConverter<T> : JsonConverter<T[]>
        {
            public override T[] Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) =>
                ReadItems<T>(ref reader, options).ToArray();

            public override void Write(Utf8JsonWriter writer, T[] value, JsonSerializerOptions options)
            {
                writer.WriteStartArray();
                foreach (var item in value) JsonSerializer.Serialize(writer, item, options);
                writer.WriteEndArray();
            }
        }
    }
}
