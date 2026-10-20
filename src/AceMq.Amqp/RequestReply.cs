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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>
/// Sends a request and waits for the reply.
/// </summary>
/// <remarks>
/// <para>
/// One reply queue per requester, not one per request. A queue per request costs a
/// declare and a delete on the broker for every call, which is the difference
/// between request/reply being usable at rate and being a curiosity.
/// </para>
/// <para>
/// Replies are matched by correlation id. A reply that arrives after its caller has
/// given up is counted and dropped rather than delivered to whoever asks next —
/// handing a late answer to the wrong caller is worse than no answer, and it is what
/// happens when a shared reply queue is read without matching.
/// </para>
/// </remarks>
public sealed class Requester : IDisposable
{
    /// <summary>
    /// How long an idle reply queue survives before the broker deletes it.
    /// </summary>
    /// <remarks>
    /// The same ten minutes Java's <c>Requester</c> uses. Long enough that a paused
    /// debugger does not lose the queue underneath a waiting caller, short enough that
    /// a process killed without disposing leaves nothing behind for an afternoon.
    /// </remarks>
    internal const long ReplyQueueExpiryMillis = 10 * 60 * 1000L;

    /// <summary>
    /// The application header naming the queue a responder should reply to.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Written alongside AMQP's own <c>reply-to</c> property, never instead of it.
    /// The two halves of the family had drifted: this library and Java carried the
    /// address in the native property, while Go, Python and Ruby carried it in this
    /// header — so a .NET requester and a Go responder could not talk at all. Every
    /// library now writes both and reads either, header first.
    /// </para>
    /// <para>
    /// A header rather than only the property, because it travels through the same
    /// envelope machinery as everything else and survives a hop through a service
    /// that rebuilds the message. It deliberately does not carry the
    /// <see cref="AceHeaders.Prefix"/>: that namespace is stripped before a handler
    /// sees it, so a responder could never read this one. It is in
    /// <see cref="AceHeaders.SharedPrefix"/> for the same reason the replay stamps
    /// are.
    /// </para>
    /// </remarks>
    public const string ReplyToHeader = AceHeaders.SharedPrefix + "reply-to";

    /// <summary>
    /// The application header a responder's failure travels back in.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Its value is a description of what went wrong, conventionally
    /// <c>{ExceptionType}: {message}</c>, on a reply whose body carries nothing. A
    /// requester that finds it raises <see cref="ResponderFailedException"/> without
    /// decoding the body. Go, Python and Ruby use the same header, so a failure
    /// crosses between any two of them.
    /// </para>
    /// <para>
    /// In <see cref="AceHeaders.SharedPrefix"/> for the reason
    /// <see cref="ReplyToHeader"/> is: a responder in any language has to be able to
    /// write it and a requester in any language to read it. It is not
    /// <see cref="AceHeaders.Error"/>, the reserved <c>x-acemq-error</c> a
    /// dead-lettered message carries.
    /// </para>
    /// </remarks>
    public const string ErrorHeader = AceHeaders.SharedPrefix + "error";

    private readonly AceMqConnection _mq;
    private readonly ICodec _codec;
    private IMessageConsumer? _consumer;
    private readonly ConcurrentDictionary<string, PendingRequest> _pending =
        new ConcurrentDictionary<string, PendingRequest>();
    private long _timedOut;
    private long _unmatched;
    private bool _disposed;

    private Requester(AceMqConnection mq, ICodec codec, string replyQueue)
    {
        _mq = mq;
        _codec = codec;
        ReplyQueue = replyQueue;
    }

    internal static async Task<Requester> StartAsync(AceMqConnection mq, ICodec codec)
    {
        // One queue per requester, named uniquely so two processes never read each
        // other's replies.
        var replyQueue = "acemq.reply." + Guid.NewGuid().ToString("N");

        // Classic, asked for rather than inherited. A reply queue belongs to one
        // process and holds answers nobody will read once that process is gone, so
        // there is nothing here worth replicating across a cluster — and the moment
        // this queue grows the exclusive or auto-delete flag it deserves, quorum stops
        // being an option at all: RabbitMQ refuses a quorum queue declared either way.
        // Java's Requester declares the same queue QueueType.CLASSIC for the same
        // reason. Leaving it on the library default would have made this queue quorum
        // the day that default changed, which is the failure worth naming here.
        //
        // x-expires is the same ten minutes Java's Requester sets, and it is what
        // stops this queue outliving the process. A reply queue holds answers nobody
        // will read once the asking process is gone; without the argument, every
        // requester that died left a durable queue behind a guid nothing can look up
        // again, and a long-lived service accumulated one per restart for ever.
        await mq.DeclareQueueAsync(
                replyQueue,
                QueueType.Classic,
                new Dictionary<string, object> { ["x-expires"] = ReplyQueueExpiryMillis })
            .ConfigureAwait(false);

        var requester = new Requester(mq, codec, replyQueue);

        // Replies are read as raw bytes and decoded once a caller claims them, so a
        // single reply queue can carry answers of different types.
        //
        // Consumed as a private queue, which is what stops a consumer's start-up
        // declarations following a name that is never reused. Every other consumer
        // declares {queue}.dlq and {queue}.parked when it starts; doing that here would
        // leave two durable queues per requester behind a guid nothing can look up
        // again. This one cannot dead-letter anyway — bytes always decode, and the
        // handler below always accepts.
        requester._consumer = await mq.ConsumePrivateQueueAsync<byte[]>(
            replyQueue,
            ConsumerOptions.Defaults().As(new BytesCodec()),
            message =>
            {
                requester.Complete(message);
                return Task.FromResult(Ack.Accept());
            }).ConfigureAwait(false);

        return requester;
    }

    /// <summary>The queue replies come back on.</summary>
    public string ReplyQueue { get; }

    /// <summary>Requests that gave up before an answer arrived.</summary>
    public long TimedOut => Interlocked.Read(ref _timedOut);

    /// <summary>Replies that arrived with no caller still waiting for them.</summary>
    public long Unmatched => Interlocked.Read(ref _unmatched);

    private void Complete(IMessage<byte[]> reply)
    {
        var correlation = reply.Envelope.CorrelationId;
        if (correlation != null && _pending.TryRemove(correlation, out var pending))
        {
            pending.Completion.TrySetResult(reply);
        }
        else
        {
            // The caller has already given up, or this reply belongs to a process
            // that has since restarted. Either way there is nobody to hand it to.
            Interlocked.Increment(ref _unmatched);
        }
    }

    /// <summary>Sends a request and waits for the reply.</summary>
    public Task<TResponse> RequestAsync<TRequest, TResponse>(
        string exchange, string routingKey, TRequest request) =>
        RequestAsync<TRequest, TResponse>(
            exchange, routingKey, request, TimeSpan.FromSeconds(30), CancellationToken.None);

    public async Task<TResponse> RequestAsync<TRequest, TResponse>(
        string exchange, string routingKey, TRequest request,
        TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(Requester));

        // Both addresses, the same value. The publisher below sets AMQP's native
        // reply-to; this sets the header the Go, Python and Ruby responders read.
        var envelope = Envelope.Of(routingKey)
            .Header(ReplyToHeader, ReplyQueue)
            .Build();
        var pending = new PendingRequest();
        _pending[envelope.Id] = pending;

        // Neither of the two spans that already covered this call was the thing the
        // caller waited for: the publish is timed and the reply's delivery is timed,
        // and "how long did asking take" was the gap between them. A gap is not a
        // measurement, which is why acemq.request.duration exists on every library.
        var destination = string.IsNullOrEmpty(routingKey) ? exchange : routingKey;
        using var span = AceMqTelemetry.StartRequest(destination, envelope);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var outcome = MetricNames.OutcomeFailed;

        try
        {
            var publisher = _mq.Publisher<TRequest>(
                exchange, routingKey, PublishOptions.Defaults(), ReplyQueue);
            await publisher.SendAsync(request, envelope, cancellationToken).ConfigureAwait(false);

            using var timer = new CancellationTokenSource(timeout);
            using var linked =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
            using (linked.Token.Register(() => pending.Completion.TrySetCanceled()))
            {
                var reply = await pending.Completion.Task.ConfigureAwait(false);

                // Checked before the body is decoded: a failure's body carries
                // nothing, and decoding it would report the codec's complaint
                // instead of the responder's. The outcome stays failed -- the round
                // trip completed, but the answer was that it could not be done.
                if (reply.Headers.TryGetValue(ErrorHeader, out var failure) && failure != null)
                {
                    throw new ResponderFailedException(
                        $"the responder could not answer {envelope.Id} on " +
                        $"{exchange}/{routingKey}: {failure}",
                        failure as string ?? failure.ToString() ?? string.Empty);
                }

                var answer = (TResponse)_codec.Decode(reply.Payload, typeof(TResponse));
                outcome = MetricNames.OutcomeAnswered;
                return answer;
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            Interlocked.Increment(ref _timedOut);
            outcome = MetricNames.OutcomeTimedOut;
            throw new RequestTimedOutException(
                $"no reply to {envelope.Id} on {exchange}/{routingKey} within " +
                $"{timeout.TotalSeconds:F0}s");
        }
        finally
        {
            _pending.TryRemove(envelope.Id, out _);

            // Recorded whatever happened, including the paths that threw: a request
            // that never got an answer is the one an operator most wants counted, and
            // an outcome nobody named reads as failed -- the same default the Java
            // library's meter scope has always applied to the same silence.
            var tags = new System.Diagnostics.TagList
            {
                { MetricNames.TagRoutingKey, destination },
                { MetricNames.TagMessageType, envelope.Type },
                { MetricNames.TagOutcome, outcome },
            };
            AceMqTelemetry.RequestDuration.Record(clock.Elapsed.TotalSeconds, tags);
            AceMqTelemetry.RequestTotal.Add(1, tags);
            AceMqTelemetry.Outcome(span, outcome);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _consumer?.Dispose();
        foreach (var pending in _pending.Values) pending.Completion.TrySetCanceled();
        _pending.Clear();
    }

    private sealed class PendingRequest
    {
        // Asynchronous continuations, so completing a request from the consumer's
        // dispatch thread does not run the caller's code on it.
        internal TaskCompletionSource<IMessage<byte[]>> Completion { get; } =
            new TaskCompletionSource<IMessage<byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

/// <summary>A request that was not answered in time.</summary>
public sealed class RequestTimedOutException : AceMqException
{
    public RequestTimedOutException(string message) : base(message) { }
}

/// <summary>
/// The responder answered, and the answer was that it could not do it.
/// </summary>
/// <remarks>
/// Raised when a reply carries <see cref="Requester.ErrorHeader"/>, whichever
/// language the responder was written in. The responder's exception crossed a broker
/// as text, so only its description arrives: a caller that has to tell failures apart
/// needs that distinction in the response type rather than in this exception.
/// </remarks>
public sealed class ResponderFailedException : AceMqException
{
    public ResponderFailedException(string message, string failure) : base(message)
    {
        Failure = failure;
    }

    /// <summary>What the responder said went wrong, as it wrote it.</summary>
    public string Failure { get; }
}

/// <summary>
/// Answers requests on a queue.
/// </summary>
/// <remarks>
/// <para>
/// A request whose sender named no reply queue is counted as unanswerable and
/// accepted rather than retried. Nothing about redelivering it makes a reply
/// address appear, so retrying only moves the same message round the same loop.
/// </para>
/// <para>
/// The reply address is read from <see cref="Requester.ReplyToHeader"/> first and
/// from AMQP's own <c>reply-to</c> property second, which is what lets this
/// responder answer a Go, Python or Ruby requester as well as a .NET or Java one.
/// </para>
/// </remarks>
public sealed class Responder : IDisposable
{
    private readonly IMessageConsumer _consumer;
    private readonly Counters _counters;
    private bool _disposed;

    private Responder(IMessageConsumer consumer, Counters counters)
    {
        _consumer = consumer;
        _counters = counters;
    }

    internal static async Task<Responder> StartAsync<TRequest, TResponse>(
        AceMqConnection mq, ICodec codec, string queue, ConsumerOptions options,
        Func<TRequest, Task<TResponse>> handler)
    {
        // Made before the subscription rather than after it. A responder cannot exist
        // until the consumer it wraps does, and a broker may hand the first request
        // over from inside the subscribe -- which is what a queue with a backlog looks
        // like from in here. The handler used to reach for the responder through a
        // local that was still null in that window: the request was answered, its
        // caller got the reply, and neither counter moved. Silent, and only ever at
        // start-up, which is the worst place to lose the first number of the day.
        var counters = new Counters();

        var consumer = await mq.ConsumeAsync<TRequest>(queue, options, async message =>
        {
            var replyTo = ReplyAddressOf(message);
            if (string.IsNullOrEmpty(replyTo))
            {
                counters.CountUnanswerable();
                return Ack.Accept();
            }

            // The reply carries the request's id as its correlation, which is what
            // the requester matches on. The default exchange addresses the reply
            // queue by name.
            var envelope = Envelope.Of(message.Envelope.Type)
                .CorrelationId(message.Envelope.Id)
                .CausationId(message.Envelope.Id);

            TResponse answer;
            try
            {
                answer = await handler(message.Payload).ConfigureAwait(false);
            }
            catch (Exception failure)
            {
                // The failure goes back to the caller, who would otherwise wait out
                // its timeout for an answer that is never coming. Go, Python and
                // Ruby responders do the same. The body is empty and the requester
                // never decodes it; a reply that cannot be sent throws, and the
                // request is retried like any other failed delivery, since the
                // caller has still not been told.
                //
                // Not counted as answered, as Python does not count it: a responder
                // that fails every request should not report a healthy answered rate.
                var description = $"{failure.GetType().Name}: {failure.Message}";
                try
                {
                    await mq.Publisher<byte[]>(
                            string.Empty, replyTo!, PublishOptions.OnBehalfOfCaller(), null,
                            new BytesCodec())
                        .SendAsync(
                            Array.Empty<byte>(),
                            envelope.Header(Requester.ErrorHeader, description).Build())
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    return ReplyNotSent(options, e);
                }

                // Settled rather than retried: the caller has its answer, and a retry
                // would answer the same question twice.
                return Ack.DeadLetter(description);
            }

            var publisher = mq.Publisher<TResponse>(
                string.Empty, replyTo!, PublishOptions.OnBehalfOfCaller());

            // Counted before the reply goes out, and this order is the contract.
            // The reply and the counter are two things one caller can see, and
            // publishing first leaves a window where a caller holding its answer
            // reads Answered as zero -- a dashboard reporting that nothing was
            // answered while the answer is in somebody's hands. Incrementing first
            // puts the counter behind the reply in every interleaving there is,
            // which is the only ordering a reader can rely on. Java increments
            // after the send and has the same window; this side is the one that is
            // right. A publish that throws hands its increment back on the way out,
            // so the failure mode incrementing early would otherwise have -- a send
            // that never happened counted as an answer -- does not exist either.
            counters.CountAnswered();
            try
            {
                await publisher.SendAsync(answer, envelope.Build()).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                counters.UncountAnswered();
                return ReplyNotSent(options, e);
            }

            return Ack.Accept();
        }).ConfigureAwait(false);

        return new Responder(consumer, counters);
    }

    /// <summary>
    /// A reply the broker did not take: the request is kept and tried again after the
    /// consumer's retry delay (AMQP-PUB-002), never dead-lettered for want of a policy.
    /// </summary>
    /// <remarks>
    /// An explicit delay, because a consumer with no policy dead-letters a bare
    /// failure after one attempt, and the caller has not been answered.
    /// </remarks>
    private static Ack ReplyNotSent(ConsumerOptions options, Exception failure) =>
        Ack.Retry(options.RetryDelay, $"could not send the reply: {failure.Message}");

    /// <summary>
    /// Works out where to send the answer: the header first, the native property second.
    /// </summary>
    /// <remarks>
    /// The order is the same in all five libraries and it is the order that matters.
    /// Go, Python and Ruby requesters send only <see cref="Requester.ReplyToHeader"/>;
    /// an older Java or .NET requester sends only AMQP's <c>reply-to</c>. Reading the
    /// header first and falling back to the property answers both, and answers a
    /// current requester — which sets the two to the same value — identically either
    /// way.
    /// </remarks>
    private static string? ReplyAddressOf<TRequest>(IMessage<TRequest> message)
    {
        if (message.Headers.TryGetValue(Requester.ReplyToHeader, out var header) && header != null)
        {
            var named = header as string ?? header.ToString();
            if (!string.IsNullOrEmpty(named)) return named;
        }

        return message.ReplyTo;
    }

    /// <summary>
    /// Requests answered, and answered before the reply left.
    /// </summary>
    /// <remarks>
    /// A caller holding a reply can rely on this having counted it: the increment
    /// happens before the publish, so there is no interleaving in which the answer is
    /// visible and the number is not. A publish that fails takes its increment back,
    /// so this counts replies that were sent rather than replies that were attempted.
    /// </remarks>
    public long Answered => _counters.Answered;

    /// <summary>Requests that arrived with no reply queue named.</summary>
    public long Unanswerable => _counters.Unanswerable;

    public bool IsRunning => !_disposed && _consumer.IsActive;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _consumer.Dispose();
    }

    /// <summary>
    /// The two numbers a responder reports, held apart from the responder itself.
    /// </summary>
    /// <remarks>
    /// The handler closes over this rather than over the <see cref="Responder"/>,
    /// which is what makes the counters reachable from the very first delivery. The
    /// responder is built around a consumer and so cannot exist until the subscribe
    /// has returned one; these can, and do.
    /// </remarks>
    private sealed class Counters
    {
        private long _answered;
        private long _unanswerable;

        internal long Answered => Interlocked.Read(ref _answered);

        internal long Unanswerable => Interlocked.Read(ref _unanswerable);

        internal void CountAnswered() => Interlocked.Increment(ref _answered);

        /// <summary>Takes back an increment whose publish then failed.</summary>
        internal void UncountAnswered() => Interlocked.Decrement(ref _answered);

        internal void CountUnanswerable() => Interlocked.Increment(ref _unanswerable);
    }
}
