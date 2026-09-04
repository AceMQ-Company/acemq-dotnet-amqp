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

namespace AceMq.Amqp;

/// <summary>Turns a payload into bytes and back.</summary>
/// <remarks>
/// <c>byte[]</c> rather than <c>Span&lt;byte&gt;</c> on purpose: a span cannot cross
/// an async boundary, and it is not callable from VB.NET. Both matter here.
/// </remarks>
public interface ICodec
{
    /// <summary>Content type this codec writes, and recognises on the way back.</summary>
    string ContentType { get; }

    byte[] Encode(object payload);

    object Decode(byte[] body, Type target);

    /// <summary>Whether this codec can read a message with the given content type.</summary>
    bool CanDecode(string? contentType);
}

/// <summary>Convenience over <see cref="ICodec"/> for a known payload type.</summary>
public static class CodecExtensions
{
    public static T Decode<T>(this ICodec codec, byte[] body) =>
        (T)codec.Decode(body, typeof(T));
}

/// <summary>JSON, and the default when nothing else is chosen.</summary>
public sealed class JsonCodec : ICodec
{
    private readonly JsonSerializerOptions _options;

    public JsonCodec() : this(DefaultOptions()) { }

    public JsonCodec(JsonSerializerOptions options) =>
        _options = options ?? throw new ArgumentNullException(nameof(options));

    /// <summary>
    /// The serializer settings the library uses when none are supplied.
    /// </summary>
    /// <remarks>
    /// Property names are camelCased and matched case-insensitively on the way in,
    /// so a C# <c>OrderId</c> and a Java <c>orderId</c> are the same field on the
    /// wire. Getting this wrong is the most common way a port stops being able to
    /// read the other language's messages, and it is invisible until it happens.
    /// </remarks>
    public static JsonSerializerOptions DefaultOptions() => new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    public string ContentType => "application/json";

    public byte[] Encode(object payload) =>
        JsonSerializer.SerializeToUtf8Bytes(payload, payload?.GetType() ?? typeof(object), _options);

    public object Decode(byte[] body, Type target)
    {
        var text = Encoding.UTF8.GetString(body);
        return JsonSerializer.Deserialize(text, target, _options)
               ?? throw new AceFatalException($"the message body decoded to null as {target.Name}");
    }

    public bool CanDecode(string? contentType) =>
        contentType == null
        || contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase);

    public override string ToString() => "JsonCodec";
}

/// <summary>Passes bytes through untouched, and strings as UTF-8.</summary>
public sealed class BytesCodec : ICodec
{
    public string ContentType => "application/octet-stream";

    public byte[] Encode(object payload) => payload switch
    {
        byte[] bytes => bytes,
        string text => Encoding.UTF8.GetBytes(text),
        null => throw new ArgumentNullException(nameof(payload)),
        _ => throw new AceFatalException(
            $"BytesCodec handles byte[] and string, not {payload.GetType().Name}"),
    };

    public object Decode(byte[] body, Type target)
    {
        if (target == typeof(byte[])) return body;
        if (target == typeof(string)) return Encoding.UTF8.GetString(body);
        throw new AceFatalException($"BytesCodec handles byte[] and string, not {target.Name}");
    }

    public bool CanDecode(string? contentType) => true;

    public override string ToString() => "BytesCodec";
}
