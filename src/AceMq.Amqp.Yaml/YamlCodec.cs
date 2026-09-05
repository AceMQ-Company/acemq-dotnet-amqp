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
using YamlDotNet.Core;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace AceMq.Amqp.Yaml;

/// <summary>
/// Reads and writes YAML.
/// </summary>
/// <remarks>
/// <para>
/// Chosen when a message is meant to be read by a person as much as by a program: a
/// configuration change broadcast to a fleet, a deployment instruction, a command
/// replayed by hand from a dead-letter queue. It is worth saying plainly that YAML
/// costs more to parse than JSON and is a poor choice for high volume; it earns its
/// place where somebody will actually look at the message.
/// </para>
/// <para>
/// Written in block style rather than flow style, which is the whole reason to pick
/// YAML. Flow style would produce something very close to JSON and leave nothing to
/// justify the cost.
/// </para>
/// <para>
/// <strong>This codec never volunteers for a message whose sender set no content
/// type.</strong> YAML is a superset of JSON, so its parser accepts JSON bytes quite
/// happily and would answer for messages meant for the JSON codec. It would even
/// give the right value — while recording that a YAML message had arrived, which is
/// the sort of wrong that is discovered much later. So it claims only content types
/// that say YAML.
/// </para>
/// <para>
/// Property names are camelCased on the wire, as the JSON codec does, so a C#
/// <c>OrderId</c> and a Java <c>orderId</c> are the same key.
/// </para>
/// </remarks>
public sealed class YamlCodec : ICodec
{
    /// <summary>What this codec writes, and what Java writes.</summary>
    public const string YamlContentType = "application/yaml";

    private readonly ISerializer _serializer;
    private readonly IDeserializer _deserializer;

    public YamlCodec() : this(DefaultSerializer(), DefaultDeserializer()) { }

    /// <summary>Uses serializers you built yourself.</summary>
    public YamlCodec(ISerializer serializer, IDeserializer deserializer)
    {
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _deserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));
    }

    /// <summary>Block style, camelCase keys, and no quotes where none are needed.</summary>
    public static ISerializer DefaultSerializer() =>
        new SerializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            // Block style is the default and is named here because it is the point:
            // flow style would emit something all but indistinguishable from JSON.
            .WithDefaultScalarStyle(ScalarStyle.Any)
            .Build();

    /// <summary>
    /// Tolerant of keys it does not know, so a producer adding a field does not
    /// break a consumer that has not been redeployed.
    /// </summary>
    public static IDeserializer DefaultDeserializer() =>
        new DeserializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();

    public string ContentType => YamlContentType;

    public byte[] Encode(object payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));
        return Encoding.UTF8.GetBytes(_serializer.Serialize(payload));
    }

    public object Decode(byte[] body, Type target)
    {
        if (body == null) throw new ArgumentNullException(nameof(body));
        if (target == null) throw new ArgumentNullException(nameof(target));

        try
        {
            return _deserializer.Deserialize(Encoding.UTF8.GetString(body), target)
                   ?? throw new AceFatalException($"the message body decoded to null as {target.Name}");
        }
        catch (YamlException e)
        {
            // Malformed YAML fails identically on every attempt, so this is fatal
            // and the message is dead-lettered rather than retried forever.
            throw new AceFatalException(
                $"this message is not YAML that reads as {target.Name}: {e.Message}", e);
        }
    }

    /// <summary>
    /// Only content types that say YAML, and never a message that names none.
    /// </summary>
    /// <remarks>
    /// The absent case is the one worth being careful about. YAML parses JSON, so
    /// answering for an untyped message would produce the right value from the wrong
    /// codec — and the mistake shows up much later, as a message recorded under a
    /// format nobody sent.
    /// </remarks>
    public bool CanDecode(string? contentType)
    {
        if (string.IsNullOrEmpty(contentType)) return false;
        return contentType!.StartsWith(YamlContentType, StringComparison.OrdinalIgnoreCase)
               // Both were in use long before application/yaml was registered.
               || contentType.StartsWith("application/x-yaml", StringComparison.OrdinalIgnoreCase)
               || contentType.StartsWith("text/yaml", StringComparison.OrdinalIgnoreCase)
               || contentType.IndexOf("+yaml", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    public override string ToString() => "YamlCodec";
}
