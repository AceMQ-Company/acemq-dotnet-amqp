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
using System.Text;
using System.Text.Json;
using Tomlyn;

namespace AceMq.Amqp.Toml;

/// <summary>
/// Reads and writes TOML.
/// </summary>
/// <remarks>
/// <para>
/// The same audience as YAML — a message a person will read and edit — with the
/// ambiguity removed. TOML has one way to write a string, no significant
/// indentation, and no Norway problem: <c>country = NO</c> is an error rather than a
/// boolean that used to be a country. Where a human edits the message and a machine
/// acts on it, that matters more than terseness.
/// </para>
/// <para>
/// Reach for it for configuration broadcast to a fleet, feature flags, deployment
/// instructions, and anything replayed by hand from a dead-letter queue. It is a
/// poor choice for high volume: it is text, it is larger than JSON, and it parses
/// more slowly.
/// </para>
/// <para>
/// <strong>The shape of the data has to suit it.</strong> TOML is a table format, so
/// a message body must be an object at the top level — a bare list or a bare number
/// is not a TOML document, and this codec says so rather than inventing a wrapper.
/// Deep nesting reads poorly too; where the payload is a tree rather than a table,
/// JSON is the honest answer.
/// </para>
/// <para>
/// Like the YAML codec, this one <strong>never volunteers for a message whose sender
/// set no content type</strong>. Guessing wrong there would record a TOML message
/// arriving where a JSON one did.
/// </para>
/// </remarks>
public sealed class TomlCodec : ICodec
{
    /// <summary>What this codec writes, and what Java writes.</summary>
    public const string TomlContentType = "application/toml";

    private readonly TomlSerializerOptions _options;

    public TomlCodec() : this(DefaultOptions()) { }

    /// <summary>Uses options you built yourself.</summary>
    public TomlCodec(TomlSerializerOptions options) =>
        _options = options ?? throw new ArgumentNullException(nameof(options));

    /// <summary>
    /// camelCase keys, so a C# <c>Service</c> and a Java <c>service</c> are the same
    /// key, and a depth limit so a hostile document cannot exhaust the stack.
    /// </summary>
    public static TomlSerializerOptions DefaultOptions() => new TomlSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        // Read leniently, write one way. A message hand-edited by somebody who
        // capitalised a key should still be read.
        PropertyNameCaseInsensitive = true,
        // A message body is a table a person reads. Sixty-four levels is far past
        // anything legible and short of anything that would exhaust a stack.
        MaxDepth = 64,
    };

    public string ContentType => TomlContentType;

    public byte[] Encode(object payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));

        try
        {
            return Encoding.UTF8.GetBytes(
                TomlSerializer.Serialize(payload, payload.GetType(), _options));
        }
        catch (TomlException e)
        {
            // Reached when the payload is not table-shaped: a list, a number, a
            // string. Saying so here is better than emitting something that is not
            // TOML and failing on the far side.
            throw new AceFatalException(
                $"{payload.GetType().Name} cannot be written as TOML: {e.Message}. TOML is a " +
                "table format, so a message body has to be an object at the top level. " +
                "Use JsonCodec where the payload is a list, a scalar, or a deep tree.", e);
        }
    }

    public object Decode(byte[] body, Type target)
    {
        if (body == null) throw new ArgumentNullException(nameof(body));
        if (target == null) throw new ArgumentNullException(nameof(target));

        try
        {
            return TomlSerializer.Deserialize(Encoding.UTF8.GetString(body), target, _options)
                   ?? throw new AceFatalException($"the message body decoded to null as {target.Name}");
        }
        catch (TomlException e)
        {
            // Malformed TOML fails identically every time, so this is fatal and the
            // message is dead-lettered rather than retried forever.
            throw new AceFatalException(
                $"this message is not TOML that reads as {target.Name}: {e.Message}", e);
        }
    }

    /// <summary>
    /// Only content types that say TOML, and never a message that names none.
    /// </summary>
    /// <remarks>
    /// The same reasoning as the YAML codec. Answering for an untyped message would
    /// record traffic under a format nobody sent.
    /// </remarks>
    public bool CanDecode(string? contentType)
    {
        if (string.IsNullOrEmpty(contentType)) return false;
        return contentType!.StartsWith(TomlContentType, StringComparison.OrdinalIgnoreCase)
               || contentType.StartsWith("text/toml", StringComparison.OrdinalIgnoreCase)
               || contentType.IndexOf("+toml", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    public override string ToString() => "TomlCodec";
}
