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
/// <para>
/// Every message carries a routing slip naming the whole route and how far along it
/// is. Nothing coordinates: each step reads the slip and publishes to whatever the
/// slip says is next. That costs two or three headers and buys the thing a positional
/// pipeline cannot do — <strong>a message dead-lettered at step three still says it
/// is at step three</strong>, so <see cref="ResumeAsync{TAt}"/> puts it back there
/// instead of at the beginning. Without a slip the only safe place to replay a
/// half-finished message is the entrance, and every step it already passed runs
/// again.
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
    private readonly SlipForm _slipForm;

    /// <summary>
    /// The one every step shares, which is what lets <see cref="ResumeAsync{TAt}"/>
    /// encode a payload for a step it did not build.
    /// </summary>
    private readonly ICodec _codec;

    internal Pipeline(
        AceMqConnection mq, string name, IReadOnlyList<PipelineStep> steps,
        RetryPolicy? retryPolicy, IIdempotencyStore? idempotency, SlipForm slipForm,
        ICodec codec)
    {
        _mq = mq;
        Name = name;
        _steps = steps;
        _retryPolicy = retryPolicy;
        _idempotency = idempotency;
        _slipForm = slipForm;
        _codec = codec;
    }

    public string Name { get; }

    public IReadOnlyList<string> StepNames => _steps.Select(s => s.Name).ToArray();

    /// <summary>Which of the two wire forms this pipeline writes its slip in.</summary>
    public SlipForm SlipForm => _slipForm;

    /// <summary>The queue a step reads from.</summary>
    public string QueueFor(string step) => $"{Name}.{step}";

    /// <summary>The slip a message entering this pipeline starts out with.</summary>
    private IRoute StartingRoute()
    {
        if (_slipForm == SlipForm.Declared) return RoutingSlip.StartOf(StepNames);

        // Each stop's address rather than its name, because that is what makes the
        // itinerary form followable by a consumer that has never heard of this
        // pipeline. The queues are on the default exchange, which is where a
        // pipeline's own steps live.
        var itinerary = new Itinerary();
        foreach (var step in _steps)
        {
            itinerary = itinerary.Then(string.Empty, QueueFor(step.Name), step.Name);
        }
        return itinerary;
    }

    /// <summary>
    /// Turns what a slip says into somewhere to publish.
    /// </summary>
    /// <remarks>
    /// A declared slip carries step names, so <c>charge</c> becomes exchange
    /// <c>orders</c>, routing key <c>charge</c> (AMQP-SLIP-001): the pipeline's direct
    /// exchange, which is where a step in another language publishes it too, and
    /// where anything bound beside the step queue sees the hop. Up to 0.7.14 this was
    /// the default exchange and queue <c>orders.charge</c>. An itinerary already
    /// carries the address. Anything the pipeline does not recognise is passed through
    /// untouched, so a slip that leaves this pipeline for somewhere else still works.
    /// </remarks>
    private RouteDestination Resolve(RouteDestination destination) =>
        destination.Exchange.Length == 0 && StepNames.Contains(destination.RoutingKey)
            ? new RouteDestination(Name, destination.RoutingKey, destination.RoutingKey)
            : destination;

    /// <summary>Messages that entered the pipeline.</summary>
    public long Entered => Interlocked.Read(ref _entered);

    /// <summary>Messages that reached the end of the last step.</summary>
    public long Completed => Interlocked.Read(ref _completed);

    /// <summary>Messages a step stopped by returning null.</summary>
    public long EndedEarly => Interlocked.Read(ref _endedEarly);

    /// <summary>Messages somewhere between the first and last step.</summary>
    public long InFlight => Entered - Completed - EndedEarly;

    /// <summary>
    /// How long a hop that was not taken waits before the step tries again, when
    /// <c>WithRetryDelay</c> set nothing. The same five seconds a consumer's
    /// <see cref="ConsumerOptions.RetryDelay"/> defaults to.
    /// </summary>
    private static readonly TimeSpan HopRetryDelay = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Headers the hop's envelope writes itself, or that must not travel past a step:
    /// the envelope's own fields, the error and claim of the delivery that was handled,
    /// and the slip in either form, which the onward route replaces.
    /// </summary>
    internal static readonly HashSet<string> NotCarried = new HashSet<string>(StringComparer.Ordinal)
    {
        AceHeaders.Id, AceHeaders.Type, AceHeaders.Version, AceHeaders.Correlation,
        AceHeaders.Causation, AceHeaders.Attempt, AceHeaders.FirstSeen, AceHeaders.Origin,
        AceHeaders.Error, AceHeaders.Claim,
        RoutingSlip.RouteHeader, RoutingSlip.PositionHeader, RoutingSlip.RunIdHeader,
        Itinerary.Header,
    };

    /// <summary>
    /// What a hop carries on besides its envelope (AMQP-SLIP-005): every header that
    /// arrived — application headers, trace context, and <c>x-acemq-</c> headers this
    /// version does not know — then the onward slip.
    /// </summary>
    /// <remarks>
    /// Up to 0.7.16 the hop rebuilt the message from the envelope's fields alone, so a
    /// tenant header, a <c>traceparent</c> and any reserved header a newer library
    /// added were gone after the first step. The same rule a retry move follows.
    /// </remarks>
    internal static IReadOnlyDictionary<string, object> HopHeaders(
        IReadOnlyDictionary<string, object> arrived, IRoute onward)
    {
        var headers = new Dictionary<string, object>(StringComparer.Ordinal);
        foreach (var pair in arrived)
        {
            if (!NotCarried.Contains(pair.Key)) headers[pair.Key] = pair.Value;
        }
        foreach (var pair in onward.ToHeaders()) headers[pair.Key] = pair.Value;
        return headers;
    }

    internal async Task StartAsync()
    {
        // Quorum, from the library default. A pipeline queue holds work that has
        // already passed earlier steps, so losing the node holding it loses
        // partly-finished runs — and the steps that already succeeded would have to be
        // repeated, which is exactly what a non-idempotent first step cannot survive.
        // Java's Pipeline declares these queues quorum for the same reason.
        //
        // Each bound to the pipeline's direct exchange with the step's name, the
        // topology AMQP-SLIP-001 gives every library, so a route written in another
        // language reaches these steps and a hop from here reaches theirs.
        await _mq.DeclareExchangeAsync(Name, "direct").ConfigureAwait(false);
        foreach (var step in _steps)
        {
            await _mq.DeclareQueueAsync(QueueFor(step.Name)).ConfigureAwait(false);
            await _mq.BindAsync(QueueFor(step.Name), Name, step.Name).ConfigureAwait(false);
        }

        for (var i = 0; i < _steps.Count; i++)
        {
            var step = _steps[i];
            var here = i;

            var options = ConsumerOptions.Prefetch(step.Prefetch).As(new BytesCodec());
            if (_retryPolicy != null) options = options.WithRetry(_retryPolicy);

            var consumer = await _mq.ConsumeAsync<byte[]>(
                QueueFor(step.Name), options,
                async message =>
                {
                    // The slip on the message wins over this consumer's place in the
                    // declaration, and that is what makes a replay resume rather than
                    // restart. A message with no slip -- published straight onto a step
                    // queue by something that knows nothing about pipelines -- is
                    // treated as being exactly where it arrived.
                    IRoute route;
                    try
                    {
                        route = Route.Of(message) ?? StartingRoute();
                    }
                    catch (AceFatalException e)
                    {
                        // An unreadable slip will not become readable on the next
                        // attempt, and a message going round the broker while nothing
                        // can tell where it is meant to go is the worst of both.
                        return Ack.DeadLetter(e.Message);
                    }
                    route = Realign(route, here);

                    // Keyed by step as well as by message, so a message re-entering
                    // step three is not mistaken for one that already cleared step
                    // one. Each hop has an id of its own since 0.7.19, but a slip
                    // written by an older library still carries one id throughout.
                    var key = step.Name + ":" + message.Envelope.Id;
                    if (_idempotency != null)
                    {
                        // A live, unconfirmed claim is not a duplicate: accepting it
                        // lost the message when the step that held it had failed and
                        // could not release it. Put back, attempt unchanged.
                        var claim = await Claims.TryClaimAsync(_idempotency, key)
                            .ConfigureAwait(false);
                        if (claim == ClaimResult.Duplicate) return Ack.Accept();
                        if (claim == ClaimResult.InProgress)
                        {
                            return Ack.InProgress(ConsumerOptions.DefaultInProgressDelay);
                        }
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
                        // No policy and no WithRetryDelay: one attempt, then
                        // {pipeline}.{step}.dlq with every header (AMQP-SLIP-004). The
                        // delay is only ever the caller's, never a default.
                        return step.RetryDelay is { } delay
                            ? Ack.Retry(delay, e.Message)
                            : Ack.Retry(e.Message);
                    }

                    if (_idempotency != null) await _idempotency.ConfirmAsync(key).ConfigureAwait(false);

                    // Whether the route has a step after this one is asked first, and
                    // the order matters. The last step of a route is almost always a
                    // terminal action with nothing to return, so reading its null as
                    // "ended early" reported every completed run as a filtered one --
                    // which is exactly what this used to do, and what Java's Pipeline
                    // has a comment warning against.
                    var onwardRoute = route.Advance();
                    if (onwardRoute.IsFinished)
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
                    //
                    // The hop is a new message with an id of its own, caused by the one
                    // this step handled (AMQP-SLIP-007, AMQP-SLIP-003). Up to 0.7.18 it
                    // kept the handled message's id, so every step of a run published
                    // the same x-acemq-id and an idempotency store keyed by id alone
                    // took the second hop for a duplicate of the first. Up to 0.7.14
                    // the causation was that message's own causation, absent at the
                    // first step.
                    var onward = Envelope.Of(message.Envelope.Type)
                        .Version(message.Envelope.Version)
                        .CorrelationId(message.Envelope.CorrelationId)
                        .CausationId(message.Envelope.Id)
                        .FirstSeen(message.Envelope.FirstSeen)
                        .Origin(message.Envelope.Origin)
                        .Build();
                    var onwardHeaders = HopHeaders(message.WireHeaders, onwardRoute);

                    // Verbatim, and that is the whole of it. step.Encode has already
                    // turned the handler's output into this step's wire format; sending
                    // those bytes through the connection's codec encoded them a second
                    // time, so what reached the next step was base64 of JSON of the
                    // payload rather than the payload. Every step after the first
                    // received a mangled body — invisibly, because a string step that
                    // trims or appends succeeds just as well on nonsense — and a Java
                    // consumer reading the same queue got nonsense too. The content type
                    // is the sending step's, so the next step is told what it is reading.
                    var onwardTo = Resolve(onwardRoute.Destination!);
                    var publisher = _mq.Publisher<byte[]>(
                        onwardTo.Exchange, onwardTo.RoutingKey, PublishOptions.OnBehalfOfCaller(), null,
                        new VerbatimCodec(step.ContentType));
                    try
                    {
                        await ((Publisher<byte[]>)publisher)
                            .SendWithHeadersAsync(
                                step.Encode(output), onward, onwardHeaders,
                                CancellationToken.None)
                            .ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        // The hop was not taken, so the message is kept and the step
                        // tried again (AMQP-PUB-002) -- with the step's own delay, so a
                        // pipeline with no policy does not dead-letter a message whose
                        // only fault is a missing next queue. The claim goes too, or the
                        // retry would be taken for a duplicate and the hop never made.
                        if (_idempotency != null)
                        {
                            await _idempotency.ReleaseAsync(key).ConfigureAwait(false);
                        }
                        return Ack.Retry(step.RetryDelay ?? HopRetryDelay, $"could not hop to {onwardTo}: {e.Message}");
                    }
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
        var route = StartingRoute();

        // Verbatim, for the same reason the hop between two steps is: the payload has
        // already been encoded, and the connection's codec would encode it again.
        var publisher = _mq.Publisher<byte[]>(
            string.Empty, QueueFor(first.Name), PublishOptions.Defaults(), null,
            new VerbatimCodec(first.ContentType));
        await ((Publisher<byte[]>)publisher)
            .SendWithHeadersAsync(
                first.Encode(payload!), wrapper, route.ToHeaders(), CancellationToken.None)
            .ConfigureAwait(false);
        Interlocked.Increment(ref _entered);
        return wrapper.Id;
    }

    /// <summary>
    /// Puts a half-finished message back at the step it had reached.
    /// </summary>
    /// <remarks>
    /// <para>
    /// For replaying something out of a dead-letter queue. The headers are the ones
    /// the failed delivery carried, and the slip among them says where the message
    /// was — so the steps it already passed are not run again. That is the whole
    /// point of carrying a slip, and the reason
    /// <see cref="SendAsync(T, Envelope?)"/> is the wrong call for a message that has
    /// already been partway through: it would start the run over, repeating every
    /// step before the one that failed, which a step that charges a card cannot
    /// survive.
    /// </para>
    /// <para>
    /// The payload is what the failing step received, which is the output of the step
    /// before it rather than what entered the pipeline — hence its own type parameter.
    /// </para>
    /// </remarks>
    /// <param name="payload">what the step it stopped at was given</param>
    /// <param name="headers">the headers of the message that failed, slip included</param>
    /// <exception cref="InvalidOperationException">
    /// when those headers carry no slip, so there is nothing to say where the message
    /// had got to
    /// </exception>
    public async Task<string> ResumeAsync<TAt>(
        TAt payload, IReadOnlyDictionary<string, object> headers)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(Pipeline<T>));
        if (headers == null) throw new ArgumentNullException(nameof(headers));

        var route = Route.From(headers);
        if (route == null || route.IsFinished)
        {
            throw new InvalidOperationException(
                "this message carries no routing slip, so nothing says which step it had"
                + " reached. Resuming it would be a guess; send it in at the entrance with"
                + " SendAsync if starting the run again is safe.");
        }

        var envelope = Envelope.FromWire(headers, null, null);
        var to = Resolve(route.Destination!);
        var publisher = _mq.Publisher<byte[]>(
            to.Exchange, to.RoutingKey, PublishOptions.Defaults(), null,
            new VerbatimCodec(_codec.ContentType));
        await ((Publisher<byte[]>)publisher)
            .SendWithHeadersAsync(
                _codec.Encode(payload!), envelope, route.ToHeaders(), CancellationToken.None)
            .ConfigureAwait(false);
        return envelope.Id;
    }

    /// <summary>
    /// Points a slip at the step whose queue the message actually arrived on.
    /// </summary>
    /// <remarks>
    /// A message replayed by hand lands on a step's queue carrying whatever slip it
    /// had, and the two can disagree — a tool that republished it to the wrong queue,
    /// or a slip advanced by a step that then failed before the publish. The queue is
    /// the fact and the slip is the claim, so the queue wins; without this the step
    /// would read the slip, advance it, and send the message to a step it has not
    /// reached, silently skipping one.
    /// <para>
    /// Only the declared form can be realigned. An itinerary consumes its steps as it
    /// goes, so there is no earlier position to move back to.
    /// </para>
    /// </remarks>
    private IRoute Realign(IRoute route, int here)
    {
        if (!(route is RoutingSlip slip)) return route;
        var at = slip.Steps.ToList().IndexOf(_steps[here].Name);
        return at < 0 || at == slip.Position ? route : slip.AdvanceTo(at);
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
        Func<object, byte[]> encode, string contentType, int prefetch, TimeSpan? retryDelay)
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
    /// <summary>What <c>WithRetryDelay</c> asked for; null when nothing did.</summary>
    internal TimeSpan? RetryDelay { get; }
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
    private TimeSpan? _retryDelay;
    private RetryPolicy? _retryPolicy;
    private IIdempotencyStore? _idempotency;
    private SlipForm _slipForm = SlipForm.Declared;

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
            _slipForm = _slipForm,
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

    /// <summary>
    /// Retries a step that throws after this delay, for ever, when there is no policy.
    /// Without it (or <see cref="WithRetry"/>) a failing step is attempted once and
    /// dead-lettered (AMQP-SLIP-004).
    /// </summary>
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

    /// <summary>
    /// Which wire form the routing slip travels in.
    /// </summary>
    /// <remarks>
    /// <see cref="SlipForm.Declared"/> unless said otherwise, which is what Java's
    /// pipeline writes: this pipeline has already declared its steps and their
    /// queues, so an itinerary would repeat that declaration on every message to say
    /// nothing new, and the short form stays readable in a management console.
    /// Choose <see cref="SlipForm.Itinerary"/> when a consumer in Go, Python or Ruby
    /// reads one of these queues — that is the form those three write and read.
    /// Either form is <em>read</em> here whatever this is set to.
    /// </remarks>
    public PipelineBuilder<TEntry, TCurrent> WritingSlipAs(SlipForm form)
    {
        _slipForm = form;
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
            _mq, _name, _steps.ToArray(), _retryPolicy, _idempotency, _slipForm, _codec);
        await pipeline.StartAsync().ConfigureAwait(false);
        return pipeline;
    }
}
