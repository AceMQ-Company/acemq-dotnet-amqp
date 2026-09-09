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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>
/// A chain of steps, each on its own queue.
/// </summary>
/// <remarks>
/// <para>
/// A message enters at the first step and moves to the next queue as each step
/// finishes. Every step is a queue, which is what separates this from calling three
/// methods in a row: a step that fails retries on its own, a slow step builds a
/// visible backlog instead of blocking the ones before it, and each step scales
/// independently.
/// </para>
/// <para>
/// A step returning <c>null</c> ends the message's journey there, which is how a
/// filter is expressed — a validation step that rejects a message stops it rather
/// than throwing, because being rejected is a normal outcome and a failure is not.
/// </para>
/// </remarks>
public sealed class Pipeline<T> : IDisposable
{
    private readonly AceMqConnection _mq;
    private readonly IReadOnlyList<PipelineStep> _steps;
    private readonly List<IMessageConsumer> _consumers = new List<IMessageConsumer>();
    private long _entered;
    private long _completed;
    private long _endedEarly;
    private bool _disposed;

    private readonly RetryPolicy? _retryPolicy;
    private readonly IIdempotencyStore? _idempotency;

    internal Pipeline(
        AceMqConnection mq, string name, IReadOnlyList<PipelineStep> steps,
        RetryPolicy? retryPolicy, IIdempotencyStore? idempotency)
    {
        _mq = mq;
        Name = name;
        _steps = steps;
        _retryPolicy = retryPolicy;
        _idempotency = idempotency;
    }

    public string Name { get; }

    public IReadOnlyList<string> StepNames => _steps.Select(s => s.Name).ToArray();

    /// <summary>The queue a step reads from.</summary>
    public string QueueFor(string step) => $"{Name}.{step}";

    /// <summary>Messages that entered the pipeline.</summary>
    public long Entered => Interlocked.Read(ref _entered);

    /// <summary>Messages that reached the end of the last step.</summary>
    public long Completed => Interlocked.Read(ref _completed);

    /// <summary>Messages a step stopped by returning null.</summary>
    public long EndedEarly => Interlocked.Read(ref _endedEarly);

    /// <summary>Messages somewhere between the first and last step.</summary>
    public long InFlight => Entered - Completed - EndedEarly;

    internal async Task StartAsync()
    {
        // Quorum, from the library default. A pipeline queue holds work that has
        // already passed earlier steps, so losing the node holding it loses
        // partly-finished runs — and the steps that already succeeded would have to be
        // repeated, which is exactly what a non-idempotent first step cannot survive.
        // Java's Pipeline declares these queues quorum for the same reason.
        foreach (var step in _steps)
        {
            await _mq.DeclareQueueAsync(QueueFor(step.Name)).ConfigureAwait(false);
        }

        for (var i = 0; i < _steps.Count; i++)
        {
            var step = _steps[i];
            var next = i + 1 < _steps.Count ? _steps[i + 1] : null;

            var options = ConsumerOptions.Prefetch(step.Prefetch).As(new BytesCodec());
            if (_retryPolicy != null) options = options.WithRetry(_retryPolicy);

            var consumer = await _mq.ConsumeAsync<byte[]>(
                QueueFor(step.Name), options,
                async message =>
                {
                    // Keyed by step as well as by message, so a message re-entering
                    // step three is not mistaken for one that already cleared step
                    // one -- they share an envelope id all the way down the chain.
                    var key = step.Name + ":" + message.Envelope.Id;
                    if (_idempotency != null
                        && !await _idempotency.ClaimAsync(key).ConfigureAwait(false))
                    {
                        return Ack.Accept();
                    }

                    object? output;
                    try
                    {
                        output = await step.Invoke(message.Payload, message.Envelope)
                            .ConfigureAwait(false);
                    }
                    catch (AceFatalException e)
                    {
                        if (_idempotency != null)
                        {
                            await _idempotency.ReleaseAsync(key).ConfigureAwait(false);
                        }
                        return Ack.DeadLetter(e.Message);
                    }
                    catch (Exception e)
                    {
                        if (_idempotency != null)
                        {
                            await _idempotency.ReleaseAsync(key).ConfigureAwait(false);
                        }
                        return Ack.Retry(step.RetryDelay, e.Message);
                    }

                    if (_idempotency != null) await _idempotency.ConfirmAsync(key).ConfigureAwait(false);

                    // Whether there is a step after this one is asked first, and the
                    // order matters. The last step of a route is almost always a
                    // terminal action with nothing to return, so reading its null as
                    // "ended early" reported every completed run as a filtered one --
                    // which is exactly what this used to do, and what Java's Pipeline
                    // has a comment warning against.
                    if (next == null)
                    {
                        Interlocked.Increment(ref _completed);
                        AceMqTelemetry.PipelineRunFinished(
                            Name, step.Name, MetricNames.OutcomeCompleted, message.Envelope.Age);
                        return Ack.Accept();
                    }

                    if (output == null)
                    {
                        // The step filtered it out: a decision, not a failure, and
                        // counted apart from both so that "how many were filtered out"
                        // needs no log reading.
                        Interlocked.Increment(ref _endedEarly);
                        AceMqTelemetry.PipelineRunFinished(
                            Name, step.Name, MetricNames.OutcomeEndedEarly, message.Envelope.Age);
                        return Ack.Accept();
                    }

                    // The envelope travels with the message, so a correlation id set
                    // at the entrance is still on it at the exit -- and so does
                    // FirstSeen, which this used to drop. Restarting the clock at every
                    // hop made a message look newly published at each step, which is
                    // wrong twice over: an age-bounded retry policy never expires, and
                    // the run duration measures the last step instead of the run.
                    var onward = Envelope.Of(message.Envelope.Type)
                        .Id(message.Envelope.Id)
                        .Version(message.Envelope.Version)
                        .CorrelationId(message.Envelope.CorrelationId)
                        .CausationId(message.Envelope.CausationId)
                        .FirstSeen(message.Envelope.FirstSeen)
                        .Origin(message.Envelope.Origin)
                        .Build();

                    // Verbatim, and that is the whole of it. step.Encode has already
                    // turned the handler's output into this step's wire format; sending
                    // those bytes through the connection's codec encoded them a second
                    // time, so what reached the next step was base64 of JSON of the
                    // payload rather than the payload. Every step after the first
                    // received a mangled body — invisibly, because a string step that
                    // trims or appends succeeds just as well on nonsense — and a Java
                    // consumer reading the same queue got nonsense too. The content type
                    // is the sending step's, so the next step is told what it is reading.
                    var publisher = _mq.Publisher<byte[]>(
                        string.Empty, QueueFor(next.Name), PublishOptions.Defaults(), null,
                        new VerbatimCodec(step.ContentType));
                    await publisher.SendAsync(step.Encode(output), onward).ConfigureAwait(false);
                    return Ack.Accept();
                }).ConfigureAwait(false);

            lock (_consumers) _consumers.Add(consumer);
        }
    }

    /// <summary>Puts a payload in at the first step.</summary>
    public Task<string> SendAsync(T payload) => SendAsync(payload, null);

    public async Task<string> SendAsync(T payload, Envelope? envelope)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(Pipeline<T>));
        if (_steps.Count == 0) throw new InvalidOperationException("the pipeline has no steps");

        var first = _steps[0];
        var wrapper = envelope ?? Envelope.Of(Name).Build();

        // Verbatim, for the same reason the hop between two steps is: the payload has
        // already been encoded, and the connection's codec would encode it again.
        var publisher = _mq.Publisher<byte[]>(
            string.Empty, QueueFor(first.Name), PublishOptions.Defaults(), null,
            new VerbatimCodec(first.ContentType));
        await publisher.SendAsync(first.Encode(payload!), wrapper).ConfigureAwait(false);
        Interlocked.Increment(ref _entered);
        return wrapper.Id;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        lock (_consumers)
        {
            foreach (var c in _consumers) c.Dispose();
            _consumers.Clear();
        }
    }

    public override string ToString() =>
        $"Pipeline[{Name}, {_steps.Count} step(s), {InFlight} in flight]";
}

/// <summary>
/// Puts already-encoded bytes on the wire under the content type they were encoded as.
/// </summary>
/// <remarks>
/// A step encodes its own output; the publisher between two steps must not encode it
/// again. The same trade the outbox relay makes with its own verbatim codec, and for
/// the same reason: the bytes are already the message.
/// </remarks>
internal sealed class VerbatimCodec : ICodec
{
    internal VerbatimCodec(string contentType) => ContentType = contentType;

    public string ContentType { get; }

    public byte[] Encode(object payload) => (byte[])payload;

    public object Decode(byte[] body, Type target) => body;

    /// <summary>
    /// Never. This codec exists to send, and offering it for decoding would let it win
    /// a registry lookup against the codec that can actually read the format.
    /// </summary>
    public bool CanDecode(string? contentType) => false;

    public override string ToString() => $"Pipeline.Verbatim[{ContentType}]";
}

/// <summary>One step of a pipeline, after type erasure.</summary>
internal sealed class PipelineStep
{
    internal PipelineStep(
        string name, Func<byte[], Envelope, Task<object?>> invoke,
        Func<object, byte[]> encode, string contentType, int prefetch, TimeSpan retryDelay)
    {
        Name = name;
        Invoke = invoke;
        Encode = encode;
        ContentType = contentType;
        Prefetch = prefetch;
        RetryDelay = retryDelay;
    }

    internal string Name { get; }
    internal Func<byte[], Envelope, Task<object?>> Invoke { get; }
    internal Func<object, byte[]> Encode { get; }

    /// <summary>
    /// What <see cref="Encode"/> produced, so the next step is told what it is reading.
    /// </summary>
    /// <remarks>
    /// The content type belongs to the step that <em>sends</em>, not to the one that
    /// receives — the format of a message arriving at <c>store</c> is the one the step
    /// before <c>store</c> encoded with. Java's <c>Pipeline.encodingBefore</c> makes the
    /// same point, and names reading it off the destination instead as a real bug.
    /// </remarks>
    internal string ContentType { get; }

    internal int Prefetch { get; }
    internal TimeSpan RetryDelay { get; }
}

/// <summary>
/// Builds a <see cref="Pipeline{T}"/>.
/// </summary>
/// <typeparam name="TEntry">What enters the pipeline.</typeparam>
/// <typeparam name="TCurrent">What the step added so far produces.</typeparam>
/// <remarks>
/// The two type parameters are what make the chain check at compile time: a step
/// added after one producing <c>Order</c> can only accept an <c>Order</c>. A
/// mismatch is a compile error rather than a decode failure at the third step in
/// production.
/// </remarks>
public sealed class PipelineBuilder<TEntry, TCurrent>
{
    private readonly AceMqConnection _mq;
    private readonly string _name;
    private readonly List<PipelineStep> _steps;
    private readonly ICodec _codec;
    private int _prefetch = 20;
    private TimeSpan _retryDelay = TimeSpan.FromSeconds(5);
    private RetryPolicy? _retryPolicy;
    private IIdempotencyStore? _idempotency;

    internal PipelineBuilder(
        AceMqConnection mq, string name, List<PipelineStep> steps, ICodec codec)
    {
        _mq = mq;
        _name = name;
        _steps = steps;
        _codec = codec;
    }

    /// <summary>
    /// Carries the builder's settings into the one returned by <see cref="Step{TOut}"/>.
    /// </summary>
    /// <remarks>
    /// Adding a step changes the second type parameter, so it has to return a new
    /// builder. Everything configured so far has to come with it: without this,
    /// <c>.Idempotent(store).Step(...)</c> silently drops the store, and the pipeline
    /// runs without the guarantee the caller asked for and was told it had.
    /// </remarks>
    private PipelineBuilder<TEntry, TOut> Continuing<TOut>()
    {
        var next = new PipelineBuilder<TEntry, TOut>(_mq, _name, _steps, _codec)
        {
            _prefetch = _prefetch,
            _retryDelay = _retryDelay,
            _retryPolicy = _retryPolicy,
            _idempotency = _idempotency,
        };
        return next;
    }

    /// <summary>
    /// Adds a step. Returning null from the handler ends the message here.
    /// </summary>
    public PipelineBuilder<TEntry, TOut> Step<TOut>(
        string stepName, Func<TCurrent, Task<TOut?>> handler) where TOut : class
    {
        if (handler == null) throw new ArgumentNullException(nameof(handler));
        var codec = _codec;

        _steps.Add(new PipelineStep(
            stepName,
            async (body, _) =>
            {
                var input = (TCurrent)codec.Decode(body, typeof(TCurrent));
                return await handler(input).ConfigureAwait(false);
            },
            value => codec.Encode(value),
            codec.ContentType,
            _prefetch, _retryDelay));

        return Continuing<TOut>();
    }

    public PipelineBuilder<TEntry, TCurrent> Prefetch(int prefetch)
    {
        _prefetch = prefetch;
        return this;
    }

    public PipelineBuilder<TEntry, TCurrent> WithRetryDelay(TimeSpan delay)
    {
        _retryDelay = delay;
        return this;
    }

    /// <summary>Backs off between attempts and gives up according to a policy.</summary>
    public PipelineBuilder<TEntry, TCurrent> WithRetry(RetryPolicy policy)
    {
        _retryPolicy = policy ?? throw new ArgumentNullException(nameof(policy));
        return this;
    }

    /// <summary>
    /// Skips a message a step has already handled.
    /// </summary>
    /// <remarks>
    /// Applies to every step, and each step claims under its own key, so a message
    /// re-entering step three is not mistaken for one that already cleared step one.
    /// Without that, a retry anywhere in the chain would be treated as a duplicate
    /// everywhere in it.
    /// </remarks>
    public PipelineBuilder<TEntry, TCurrent> Idempotent(IIdempotencyStore store)
    {
        _idempotency = store ?? throw new ArgumentNullException(nameof(store));
        return this;
    }

    /// <summary>Declares the step queues and starts consuming them.</summary>
    public async Task<Pipeline<TEntry>> BuildAsync()
    {
        if (_steps.Count == 0)
        {
            throw new InvalidOperationException("a pipeline needs at least one step");
        }
        var pipeline = new Pipeline<TEntry>(
            _mq, _name, _steps.ToArray(), _retryPolicy, _idempotency);
        await pipeline.StartAsync().ConfigureAwait(false);
        return pipeline;
    }
}
