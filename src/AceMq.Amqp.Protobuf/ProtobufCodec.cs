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
using System.Reflection;
using Google.Protobuf;

namespace AceMq.Amqp.Protobuf;

/// <summary>
/// Protocol Buffers, for talking to services that already speak it.
/// </summary>
/// <remarks>
/// <para>
/// A separate package because it needs <c>Google.Protobuf</c>, and a messaging
/// library that puts a serialization stack into every application installing it is
/// one people work around. The core carries two Microsoft packages and nothing else;
/// this is here only if you asked for it.
/// </para>
/// <para>
/// <strong>It works with generated message types, not with your own classes.</strong>
/// Protobuf encoding is defined by a <c>.proto</c> schema and the code generated from
/// it — there is no reflection-based fallback that would produce bytes another
/// language could read. A plain class is refused at the call rather than encoded into
/// something nothing else can decode.
/// </para>
/// <para>
/// The content type matches the Java library's, so a Java consumer reading
/// <c>application/x-protobuf</c> reads what this writes. Reading is wider than
/// writing — see <see cref="ReadableContentTypes"/> — because a message this codec
/// could decode is not worth refusing over the name somebody else gave the bytes.
/// </para>
/// </remarks>
public sealed class ProtobufCodec : ICodec
{
    /// <summary>What this codec declares, and what Java declares.</summary>
    public const string ProtobufContentType = "application/x-protobuf";

    /// <summary>
    /// Every spelling of "these bytes are protobuf" this codec will read.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One is written and three are read, plus any <c>*+protobuf</c> suffix type, which
    /// is how a schema registry usually names its own wrapping of the same bytes.
    /// <c>application/x-protobuf</c> is what this and the Java library write;
    /// <c>application/protobuf</c> is the spelling in the IETF draft;
    /// <c>application/vnd.google.protobuf</c> is what Google's own tooling emits, and
    /// it has no <c>+protobuf</c> suffix to be caught by, so it has to be named.
    /// </para>
    /// <para>
    /// The asymmetry is deliberate. A wider read set costs a string comparison and
    /// nothing else; a narrower one refuses a message it could have read perfectly
    /// well, and the refusal looks like a broken producer rather than a fussy consumer.
    /// The Go and Ruby libraries already accept all three, and Java is being widened
    /// to match.
    /// </para>
    /// </remarks>
    public static readonly IReadOnlyList<string> ReadableContentTypes = new[]
    {
        ProtobufContentType,
        "application/protobuf",
        "application/vnd.google.protobuf",
    };

    // Parsers are found by reflection once and kept. Generated types expose a static
    // Parser property; looking it up per message would put reflection on the hot
    // path for no reason.
    private static readonly Dictionary<Type, MessageParser> Parsers = new Dictionary<Type, MessageParser>();

    public string ContentType => ProtobufContentType;

    public byte[] Encode(object payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));
        if (payload is IMessage message) return message.ToByteArray();

        throw new AceFatalException(
            $"{payload.GetType().Name} is not a generated protobuf message. This codec " +
            "encodes types generated from a .proto schema; there is no reflection-based " +
            "fallback, because bytes produced that way would not be readable by anything " +
            "else that speaks protobuf.");
    }

    public object Decode(byte[] body, Type target)
    {
        if (body == null) throw new ArgumentNullException(nameof(body));
        if (target == null) throw new ArgumentNullException(nameof(target));

        try
        {
            return ParserFor(target).ParseFrom(body);
        }
        catch (InvalidProtocolBufferException e)
        {
            // A wire-format failure is not retryable: the same bytes will fail the
            // same way next time, so this surfaces as fatal and the message is
            // dead-lettered rather than looping.
            throw new AceFatalException(
                $"this message is not a valid {target.Name}: {e.Message}", e);
        }
    }

    public bool CanDecode(string? contentType)
    {
        if (contentType == null) return false;

        foreach (var readable in ReadableContentTypes)
        {
            if (contentType.StartsWith(readable, StringComparison.OrdinalIgnoreCase)) return true;
        }

        return contentType.IndexOf("+protobuf", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    /// <summary>The parser a generated type exposes as a static <c>Parser</c> property.</summary>
    private static MessageParser ParserFor(Type target)
    {
        lock (Parsers)
        {
            if (Parsers.TryGetValue(target, out var cached)) return cached;

            if (!typeof(IMessage).IsAssignableFrom(target))
            {
                throw new AceFatalException(
                    $"{target.Name} is not a generated protobuf message. Consume it as the " +
                    "type generated from your .proto, or use a codec that does not need a " +
                    "schema, such as JsonCodec.");
            }

            var property = target.GetProperty(
                "Parser", BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
            if (property?.GetValue(null) is not MessageParser parser)
            {
                // Every type protoc generates has this. Reaching here means the type
                // implements IMessage by hand, which this codec cannot serve.
                throw new AceFatalException(
                    $"{target.Name} has no static Parser property. This codec needs the " +
                    "type protoc generates, which always has one.");
            }

            Parsers[target] = parser;
            return parser;
        }
    }

    public override string ToString() => "ProtobufCodec";
}
