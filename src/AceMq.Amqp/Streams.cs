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
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>Where in a stream to start reading.</summary>
/// <remarks>
/// A stream keeps its messages after they are read, so a reader has to say where to
/// begin. This is the difference between a stream and a queue: consuming does not
/// remove anything, and two readers can be at different places in the same data.
/// </remarks>
public sealed class StreamOffset
{
    private StreamOffset(object value) => Value = value;

    /// <summary>Everything the stream still holds.</summary>
    public static StreamOffset First() => new StreamOffset("first");

    /// <summary>The last message written, and everything after it.</summary>
    public static StreamOffset Last() => new StreamOffset("last");

    /// <summary>Only messages written from now on.</summary>
    public static StreamOffset Next() => new StreamOffset("next");

    /// <summary>From an exact offset.</summary>
    public static StreamOffset At(long offset) => new StreamOffset(offset);

    /// <summary>From the first message written at or after an instant.</summary>
    public static StreamOffset From(DateTimeOffset timestamp) =>
        new StreamOffset(timestamp.ToUnixTimeSeconds());

    /// <summary>From however far back a duration reaches.</summary>
    public static StreamOffset LastFor(TimeSpan age) =>
        new StreamOffset(((long)age.TotalSeconds).ToString(CultureInfo.InvariantCulture) + "s");

    internal object Value { get; }

    public override string ToString() => $"StreamOffset[{Value}]";
}

/// <summary>A running stream reader. Disposing it stops delivery.</summary>
public interface IStreamConsumer : IDisposable
{
    string Queue { get; }

    bool IsActive { get; }

    /// <summary>
    /// The offset of the last message handled, or null before any has been.
    /// </summary>
    /// <remarks>
    /// Nothing stores this for you. A reader starts where it is told to every time
    /// it starts, so resuming exactly where a previous run stopped means recording
    /// this value as part of handling each message and passing it to
    /// <see cref="StreamReader{T}.FromOffset"/> next time.
    /// </remarks>
    long? LastHandledOffset { get; }

    long Handled { get; }

    long Failed { get; }

    /// <summary>Messages skipped because the handler failed and failures are skipped.</summary>
    long Skipped { get; }
}

/// <summary>
/// Reads a stream queue from a chosen offset.
/// </summary>
/// <remarks>
/// <para>
/// A stream is declared with <see cref="QueueType.Stream"/> and, unlike a queue,
/// keeps what it holds after it is read. That makes it the right shape for an audit
/// log or for a consumer that needs to catch up from the beginning, and the wrong
/// shape for work distribution.
/// </para>
/// <para>
/// Streams require a prefetch. The broker refuses a stream consumer without one,
/// because a stream will otherwise hand over its entire history as fast as the
/// network allows.
/// </para>
/// </remarks>
public sealed class StreamReader<T>
{
    private readonly AceMqConnection _mq;
    private readonly string _queue;
    private StreamOffset _offset = StreamOffset.Next();
    private int _prefetch = 100;
    private ICodec? _codec;
    private bool _skipFailures;

    internal StreamReader(AceMqConnection mq, string queue)
    {
        _mq = mq;
        _queue = queue;
    }

    /// <summary>Reads everything the stream still holds.</summary>
    public StreamReader<T> FromFirst() => From(StreamOffset.First());

    /// <summary>Reads from the last message written.</summary>
    public StreamReader<T> FromLast() => From(StreamOffset.Last());

    /// <summary>Reads only what is written from now on.</summary>
    public StreamReader<T> FromNext() => From(StreamOffset.Next());

    public StreamReader<T> FromOffset(long offset) => From(StreamOffset.At(offset));

    public StreamReader<T> FromTime(DateTimeOffset timestamp) => From(StreamOffset.From(timestamp));

    public StreamReader<T> FromLast(TimeSpan age) => From(StreamOffset.LastFor(age));

    public StreamReader<T> From(StreamOffset offset)
    {
        _offset = offset ?? throw new ArgumentNullException(nameof(offset));
        return this;
    }

    public StreamReader<T> Prefetch(int prefetch)
    {
        if (prefetch < 1) throw new ArgumentException("must be at least 1", nameof(prefetch));
        _prefetch = prefetch;
        return this;
    }

    /// <summary>
    /// Carries on past a message that cannot be handled.
    /// </summary>
    /// <remarks>
    /// Off by default. A stream reader that skips failures silently will read to the
    /// end and report success having processed none of it.
    /// </remarks>
    public StreamReader<T> SkipFailures()
    {
        _skipFailures = true;
        return this;
    }

    public StreamReader<T> As(ICodec codec)
    {
        _codec = codec ?? throw new ArgumentNullException(nameof(codec));
        return this;
    }

    /// <summary>Starts reading.</summary>
    public async Task<IStreamConsumer> ConsumeAsync(Func<IMessage<T>, Task> handler)
    {
        if (handler == null) throw new ArgumentNullException(nameof(handler));

        var options = ConsumerOptions.Prefetch(_prefetch);
        if (_codec != null) options = options.As(_codec);

        var stream = new StreamConsumer(_queue);

        stream.Attach(await _mq.ConsumeStreamAsync<T>(
            _queue, options, _offset,
            async message =>
            {
                try
                {
                    await handler(message).ConfigureAwait(false);
                    stream.Handled_(OffsetOf(message));
                    return Ack.Accept();
                }
                catch (Exception e) when (_skipFailures)
                {
                    stream.Skipped_();
                    return Ack.DeadLetter(e.Message);
                }
                catch (Exception e)
                {
                    stream.Failed_();
                    return Ack.Retry(TimeSpan.FromSeconds(5), e.Message);
                }
            }).ConfigureAwait(false));

        return stream;
    }

    public override string ToString() => $"StreamReader[{_queue} from {_offset}]";

    /// <summary>The broker reports each message's position in this header.</summary>
    private static long? OffsetOf(IMessage<T> message) =>
        message.Headers.TryGetValue("x-stream-offset", out var value) && value != null
            ? Convert.ToInt64(value, CultureInfo.InvariantCulture)
            : (long?)null;

    private sealed class StreamConsumer : IStreamConsumer
    {
        private IMessageConsumer? _consumer;
        private long _handled;
        private long _failed;
        private long _skipped;
        private long _lastOffset = -1;

        internal StreamConsumer(string queue) => Queue = queue;

        internal void Attach(IMessageConsumer consumer) => _consumer = consumer;

        internal void Handled_(long? offset)
        {
            System.Threading.Interlocked.Increment(ref _handled);
            if (offset.HasValue)
            {
                System.Threading.Interlocked.Exchange(ref _lastOffset, offset.Value);
            }
        }

        internal void Failed_() => System.Threading.Interlocked.Increment(ref _failed);

        internal void Skipped_()
        {
            System.Threading.Interlocked.Increment(ref _failed);
            System.Threading.Interlocked.Increment(ref _skipped);
        }

        public string Queue { get; }
        public bool IsActive => _consumer?.IsActive ?? false;

        public long? LastHandledOffset
        {
            get
            {
                var value = System.Threading.Interlocked.Read(ref _lastOffset);
                return value < 0 ? (long?)null : value;
            }
        }

        public long Handled => System.Threading.Interlocked.Read(ref _handled);
        public long Failed => System.Threading.Interlocked.Read(ref _failed);
        public long Skipped => System.Threading.Interlocked.Read(ref _skipped);

        public void Dispose() => _consumer?.Dispose();
    }
}
