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

    internal Pipeline(AceMqConnection mq, string name, IReadOnlyList<PipelineStep> steps)
    {
        _mq = mq;
        Name = name;
        _steps = steps;
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
        foreach (var step in _steps)
        {
            await _mq.DeclareQueueAsync(QueueFor(step.Name)).ConfigureAwait(false);
        }

        for (var i = 0; i < _steps.Count; i++)
        {
            var step = _steps[i];
            var next = i + 1 < _steps.Count ? _steps[i + 1] : null;

            var consumer = await _mq.ConsumeAsync<byte[]>(
                QueueFor(step.Name),
                ConsumerOptions.Prefetch(step.Prefetch).As(new BytesCodec()),
                async message =>
                {
                    object? output;
                    try
                    {
                        output = await step.Invoke(message.Payload, message.Envelope)
                            .ConfigureAwait(false);
                    }
                    catch (AceFatalException e)
                    {
                        return Ack.DeadLetter(e.Message);
                    }
                    catch (Exception e)
                    {
                        return Ack.Retry(step.RetryDelay, e.Message);
                    }

                    if (output == null)
                    {
                        // The step filtered it out. That is an outcome, not a failure,
                        // so it is counted separately from both success and error.
                        Interlocked.Increment(ref _endedEarly);
                        return Ack.Accept();
                    }

                    if (next == null)
                    {
                        Interlocked.Increment(ref _completed);
                        return Ack.Accept();
                    }

                    // The envelope travels with the message, so a correlation id set
                    // at the entrance is still on it at the exit.
                    var onward = Envelope.Of(message.Envelope.Type)
                        .Id(message.Envelope.Id)
                        .CorrelationId(message.Envelope.CorrelationId)
                        .CausationId(message.Envelope.CausationId)
                        .Build();

                    var publisher = _mq.Publisher<byte[]>(string.Empty, QueueFor(next.Name));
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
        var publisher = _mq.Publisher<byte[]>(string.Empty, QueueFor(first.Name));
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

/// <summary>One step of a pipeline, after type erasure.</summary>
internal sealed class PipelineStep
{
    internal PipelineStep(
        string name, Func<byte[], Envelope, Task<object?>> invoke,
        Func<object, byte[]> encode, int prefetch, TimeSpan retryDelay)
    {
        Name = name;
        Invoke = invoke;
        Encode = encode;
        Prefetch = prefetch;
        RetryDelay = retryDelay;
    }

    internal string Name { get; }
    internal Func<byte[], Envelope, Task<object?>> Invoke { get; }
    internal Func<object, byte[]> Encode { get; }
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

    internal PipelineBuilder(
        AceMqConnection mq, string name, List<PipelineStep> steps, ICodec codec)
    {
        _mq = mq;
        _name = name;
        _steps = steps;
        _codec = codec;
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
            _prefetch, _retryDelay));

        return new PipelineBuilder<TEntry, TOut>(_mq, _name, _steps, _codec);
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

    /// <summary>Declares the step queues and starts consuming them.</summary>
    public async Task<Pipeline<TEntry>> BuildAsync()
    {
        if (_steps.Count == 0)
        {
            throw new InvalidOperationException("a pipeline needs at least one step");
        }
        var pipeline = new Pipeline<TEntry>(_mq, _name, _steps.ToArray());
        await pipeline.StartAsync().ConfigureAwait(false);
        return pipeline;
    }
}
