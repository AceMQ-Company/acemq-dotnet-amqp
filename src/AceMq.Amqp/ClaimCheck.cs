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
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace AceMq.Amqp;

/// <summary>
/// Where a payload too large for a broker actually goes.
/// </summary>
/// <remarks>
/// <para>
/// Three methods, so a store backed by S3, Azure Blob Storage, a filesystem or a
/// database table is a small class. Nothing here knows about messaging: the store
/// holds bytes under a key and hands them back, and <see cref="ClaimCheckCodec"/> is
/// what turns that into a claim check on the wire.
/// </para>
/// <para>
/// <strong>Retention is the part that goes wrong.</strong> The store and the queue
/// have different lifetimes and nothing enforces a relationship between them. A
/// message replayed a month later carries a key, and if the store expired that key
/// the replay produces a message nobody can read — worse than a lost message,
/// because it looks like a message and fails deep inside a consumer rather than
/// visibly. So the store's retention must exceed every retention that could bring a
/// message back: queue TTLs, dead-letter queues, and however long somebody might sit
/// on a message before replaying it by hand. When in doubt, longer.
/// </para>
/// <para>
/// Implementations must be safe for use by several threads: one is shared by every
/// publisher and consumer on a connection.
/// </para>
/// </remarks>
public interface IClaimCheckStore
{
    /// <summary>Stores a payload.</summary>
    /// <returns>
    /// The key the message will carry, which must be unique for the life of the store.
    /// </returns>
    string Put(byte[] content);

    /// <summary>Redeems a claim check.</summary>
    /// <returns>
    /// The payload, or null when the store no longer holds it — which is retention
    /// having expired underneath a message that outlived it.
    /// </returns>
    byte[]? Get(string key);

    /// <summary>
    /// Removes a payload.
    /// </summary>
    /// <remarks>
    /// Not called by the codec. Deleting on read would break the second consumer of
    /// the same message, and deleting on acknowledgement would break a replay — so
    /// when a payload may be removed is a retention decision, and retention decisions
    /// belong to whoever owns the data.
    /// </remarks>
    void Delete(string key);
}

/// <summary>
/// A claim-check store in a dictionary, for tests.
/// </summary>
/// <remarks>
/// <strong>Not for production, and the reason is the point of the pattern.</strong>
/// The payloads are held in the publisher's heap — which is where they were going to
/// be anyway, so this removes them from the broker and nothing else. A claim check
/// that does not outlive the process that wrote it is a message nobody else can read,
/// and every consumer in another process gets "the claim check is not in the store".
/// It is genuinely useful for a test, where the publisher and consumer are the same
/// process and the point being proved is the framing rather than the storage.
/// </remarks>
public sealed class InMemoryClaimCheckStore : IClaimCheckStore
{
    private readonly ConcurrentDictionary<string, byte[]> _contents =
        new ConcurrentDictionary<string, byte[]>();

    public string Put(byte[] content)
    {
        if (content == null) throw new ArgumentNullException(nameof(content));
        var key = Guid.NewGuid().ToString();
        // Copied, because the caller owns the array it handed over and a codec is
        // entitled to reuse a buffer. A store that keeps somebody else's array is a
        // store whose contents change after they were stored.
        _contents[key] = (byte[])content.Clone();
        return key;
    }

    public byte[]? Get(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        return _contents.TryGetValue(key, out var content) ? (byte[])content.Clone() : null;
    }

    public void Delete(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        _contents.TryRemove(key, out _);
    }

    /// <summary>How many payloads are held.</summary>
    public int Count => _contents.Count;

    /// <summary>Empties the store, which is what a test between cases wants.</summary>
    public void Clear() => _contents.Clear();

    public override string ToString() => $"InMemoryClaimCheckStore[{_contents.Count} held]";
}

/// <summary>
/// A claim-check store on a filesystem.
/// </summary>
/// <remarks>
/// <para>
/// Useful where the filesystem is shared and durable — an NFS mount, a persistent
/// volume — and the honest middle ground between a dictionary and object storage. On
/// a container's local disk it is the in-memory store with extra steps: the consumer
/// is on another host and finds nothing.
/// </para>
/// <para>
/// Object storage is the usual right answer, and a store in front of S3 or Azure Blob
/// Storage is three short methods. This one exists because "write it to the mount
/// everything already has" is a real deployment and not a bad one.
/// </para>
/// <para>
/// <strong>Writes are atomic.</strong> The payload is written to a temporary file and
/// moved into place. Without that, a consumer fast enough to read the key before the
/// writer finished gets a truncated payload and a parse error somewhere unhelpful —
/// and messaging is exactly the arrangement that makes a consumer that fast normal
/// rather than unlikely.
/// </para>
/// </remarks>
public sealed class FilesystemClaimCheckStore : IClaimCheckStore
{
    /// <summary>
    /// A key reaches the filesystem as a path segment, so it is checked rather than
    /// trusted. Every key this store issues is a GUID; one arriving from a message is
    /// whatever a publisher put there, and <c>../../etc/passwd</c> is a key too.
    /// </summary>
    private static readonly Regex SafeKey =
        new Regex("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", RegexOptions.CultureInvariant);

    private readonly string _directory;

    /// <param name="directory">where payloads are written; created if it is not there</param>
    public FilesystemClaimCheckStore(string directory)
    {
        _directory = directory ?? throw new ArgumentNullException(nameof(directory));
        try
        {
            Directory.CreateDirectory(_directory);
        }
        catch (Exception e) when (e is IOException || e is UnauthorizedAccessException)
        {
            throw new AceMqException(
                $"could not create the claim-check directory {_directory}", e);
        }
    }

    /// <summary>Where payloads are written.</summary>
    public string Location => _directory;

    public string Put(byte[] content)
    {
        if (content == null) throw new ArgumentNullException(nameof(content));
        var key = Guid.NewGuid().ToString();
        var target = PathFor(key);
        var staging = target + ".partial";
        try
        {
            File.WriteAllBytes(staging, content);
            // Moved into place, so a reader sees the whole payload or no payload.
            if (File.Exists(target)) File.Delete(target);
            File.Move(staging, target);
            return key;
        }
        catch (Exception e) when (e is IOException || e is UnauthorizedAccessException)
        {
            throw new AceMqException(
                $"could not store a claim-check payload in {_directory}", e);
        }
    }

    public byte[]? Get(string key)
    {
        var path = PathFor(key);
        try
        {
            return File.ReadAllBytes(path);
        }
        catch (FileNotFoundException)
        {
            // Absent rather than failed: a key the store no longer holds is a
            // retention answer, and the codec turns it into a message that explains
            // itself.
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    public void Delete(string key)
    {
        var path = PathFor(key);
        if (File.Exists(path)) File.Delete(path);
    }

    private string PathFor(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (!SafeKey.IsMatch(key))
        {
            throw new AceMqException(
                $"'{key}' is not a key this store issued. A key becomes a path segment, so"
                + " one arriving from a message is checked rather than trusted.");
        }
        return Path.Combine(_directory, key);
    }

    public override string ToString() => $"FilesystemClaimCheckStore[{_directory}]";
}

/// <summary>
/// Keeps large payloads off the broker.
/// </summary>
/// <remarks>
/// <para>
/// <code>
/// var store = new FilesystemClaimCheckStore("/mnt/payloads");
/// using var mq = await AceMqConnection.ConnectAsync(
///     url, ClaimCheckCodec.Wrapping(new JsonCodec(), store));
///
/// await mq.Publisher&lt;Document&gt;("policies", "document.stored").SendAsync(report);
/// await mq.ConsumeAsync&lt;Document&gt;("policies.documents", m => Read(m.Payload));
/// </code>
/// </para>
/// <para>
/// A scanned medical report is tens of megabytes. Putting it on a queue is possible
/// and is a mistake: it fills the broker's memory, it is copied to every bound queue,
/// it makes a dead-letter queue impossible to inspect, and it turns a broker into a
/// filesystem with worse tools. What travels instead is a <strong>claim check</strong>
/// — the payload goes to an <see cref="IClaimCheckStore"/>, and the message carries
/// the key.
/// </para>
/// <para>
/// <strong>Only when it is worth it.</strong> Below <see cref="DefaultThreshold"/> the
/// payload travels inline, exactly as it would without this codec. That matters more
/// than it sounds: offloading a two-hundred-byte message turns one broker round trip
/// into a store round trip <em>and</em> a broker round trip, so an unconditional claim
/// check makes the common case slower to fix the rare one.
/// </para>
/// <para>
/// The framing therefore says which of the two it is, and a consumer handles both
/// without being told. That is what allows the threshold to be changed, or this codec
/// to be introduced, without a flag day: messages written before the change are still
/// readable after it.
/// </para>
/// <para>
/// What is on the wire, byte for byte what Java, Python and Ruby write:
/// <code>
/// 0xAC  0x01  0x00  payload      inline, and identical to what the delegate wrote
/// 0xAC  0x01  0x01  key          a claim check, the key as UTF-8
/// </code>
/// </para>
/// <para>
/// The content type is the delegate's, unchanged — unlike encryption, where the bytes
/// really are something else. A claim-checked message is still a document; it is a
/// document that is somewhere else, and a consumer that lacks the store gets a clear
/// failure rather than a parser error.
/// </para>
/// </remarks>
public sealed class ClaimCheckCodec : ICodec, IContentTypeCodec
{
    /// <summary>Marks this codec's framing.</summary>
    private const byte Magic = 0xAC;

    private const byte Version = 0x01;
    private const byte Inline = 0x00;
    private const byte Checked = 0x01;
    private const int HeaderLength = 3;

    /// <summary>
    /// Below this, payloads travel inline.
    /// </summary>
    /// <remarks>
    /// 64 KiB: comfortably above an ordinary event and comfortably below the size at
    /// which a broker starts to care. RabbitMQ will accept far larger, which is the
    /// problem — nothing refuses a 40 MB message, it simply makes everything worse
    /// afterwards. The same number as Java's <c>ClaimCheckCodec.DEFAULT_THRESHOLD</c>,
    /// and the comparison is the same too: strictly below travels inline, so a payload
    /// of exactly this size is offloaded.
    /// </remarks>
    public const int DefaultThreshold = 64 * 1024;

    private readonly ICodec _delegate;
    private readonly IClaimCheckStore _store;
    private readonly int _threshold;

    private ClaimCheckCodec(ICodec codec, IClaimCheckStore store, int threshold)
    {
        _delegate = codec ?? throw new ArgumentNullException(nameof(codec));
        _store = store ?? throw new ArgumentNullException(nameof(store));
        if (threshold < 0)
        {
            throw new ArgumentException(
                $"a threshold cannot be negative, was {threshold}", nameof(threshold));
        }
        _threshold = threshold;
    }

    /// <summary>A codec offloading anything at or above <see cref="DefaultThreshold"/>.</summary>
    public static ClaimCheckCodec Wrapping(ICodec codec, IClaimCheckStore store) =>
        new ClaimCheckCodec(codec, store, DefaultThreshold);

    /// <param name="codec">the codec that turns objects into bytes</param>
    /// <param name="store">where large payloads go</param>
    /// <param name="threshold">
    /// payloads of at least this many bytes are offloaded; zero offloads everything,
    /// which is occasionally what a store-backed audit trail wants
    /// </param>
    public static ClaimCheckCodec Wrapping(ICodec codec, IClaimCheckStore store, int threshold) =>
        new ClaimCheckCodec(codec, store, threshold);

    /// <summary>The wrapped codec, whose output is what gets stored or inlined.</summary>
    public ICodec Delegate => _delegate;

    /// <summary>Where this codec's large payloads go.</summary>
    public IClaimCheckStore Store => _store;

    /// <summary>Payloads of at least this many bytes are offloaded.</summary>
    public int Threshold => _threshold;

    /// <summary>The delegate's. A claim-checked document is still a document.</summary>
    public string ContentType => _delegate.ContentType;

    public byte[] Encode(object payload)
    {
        var encoded = _delegate.Encode(payload);
        if (encoded.Length < _threshold) return Frame(Inline, encoded);
        return Frame(Checked, Encoding.UTF8.GetBytes(_store.Put(encoded)));
    }

    public object Decode(byte[] body, Type target) => Decode(body, target, null);

    public object Decode(byte[] body, Type target, string? contentType)
    {
        if (!IsFramed(body))
        {
            // Written before this codec was introduced, or by a publisher that does
            // not use it. Reading it as the delegate would is the only useful answer,
            // and it is what makes adding a claim check to a live queue safe.
            return DelegateDecode(body, target, contentType);
        }

        var rest = new byte[body.Length - HeaderLength];
        Buffer.BlockCopy(body, HeaderLength, rest, 0, rest.Length);
        if (body[2] == Inline) return DelegateDecode(rest, target, contentType);

        var key = Encoding.UTF8.GetString(rest);
        var content = _store.Get(key)
            ?? throw new AceMqException(
                $"the claim check '{key}' is not in the store, so this message cannot be read."
                + " The payload was removed while a message referring to it was still"
                + " deliverable -- the store's retention has to outlast every queue, every"
                + " dead-letter queue, and any replay somebody might do by hand.");
        return DelegateDecode(content, target, contentType);
    }

    /// <summary>
    /// Whatever the delegate accepts. A claim check does not change what the message is.
    /// </summary>
    public bool CanDecode(string? contentType) => _delegate.CanDecode(contentType);

    /// <summary>
    /// Reads the key a message refers to, without fetching it.
    /// </summary>
    /// <remarks>
    /// For the operator looking at a dead-letter queue: which object does this need,
    /// and is it still in the store? Answering that from the message alone is the
    /// difference between a five-minute check and restoring a backup.
    /// </remarks>
    /// <returns>
    /// The key, or null when the payload travelled inline or this codec did not write
    /// the message.
    /// </returns>
    public static string? KeyOf(byte[]? body)
    {
        if (!IsFramed(body) || body![2] != Checked) return null;
        return Encoding.UTF8.GetString(body, HeaderLength, body.Length - HeaderLength);
    }

    /// <summary>Whether a body is a claim check rather than an inline payload.</summary>
    public static bool IsClaimCheck(byte[]? body) => IsFramed(body) && body![2] == Checked;

    private object DelegateDecode(byte[] body, Type target, string? contentType) =>
        _delegate is IContentTypeCodec chooser
            ? chooser.Decode(body, target, contentType)
            : _delegate.Decode(body, target);

    private static bool IsFramed(byte[]? body) =>
        body != null && body.Length >= HeaderLength && body[0] == Magic && body[1] == Version
        && (body[2] == Inline || body[2] == Checked);

    private static byte[] Frame(byte kind, byte[] rest)
    {
        var framed = new byte[HeaderLength + rest.Length];
        framed[0] = Magic;
        framed[1] = Version;
        framed[2] = kind;
        Buffer.BlockCopy(rest, 0, framed, HeaderLength, rest.Length);
        return framed;
    }

    public override string ToString() => $"ClaimCheckCodec[{_delegate}, above {_threshold} bytes]";
}
