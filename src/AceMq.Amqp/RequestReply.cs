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
        await mq.DeclareQueueAsync(replyQueue).ConfigureAwait(false);

        var requester = new Requester(mq, codec, replyQueue);

        // Replies are read as raw bytes and decoded once a caller claims them, so a
        // single reply queue can carry answers of different types.
        requester._consumer = await mq.ConsumeAsync<byte[]>(
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
            pending.Completion.TrySetResult(reply.Payload);
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

        var envelope = Envelope.Of(routingKey).Build();
        var pending = new PendingRequest();
        _pending[envelope.Id] = pending;

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
                var body = await pending.Completion.Task.ConfigureAwait(false);
                return (TResponse)_codec.Decode(body, typeof(TResponse));
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            Interlocked.Increment(ref _timedOut);
            throw new RequestTimedOutException(
                $"no reply to {envelope.Id} on {exchange}/{routingKey} within " +
                $"{timeout.TotalSeconds:F0}s");
        }
        finally
        {
            _pending.TryRemove(envelope.Id, out _);
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
        internal TaskCompletionSource<byte[]> Completion { get; } =
            new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

/// <summary>A request that was not answered in time.</summary>
public sealed class RequestTimedOutException : AceMqException
{
    public RequestTimedOutException(string message) : base(message) { }
}

/// <summary>
/// Answers requests on a queue.
/// </summary>
/// <remarks>
/// A request whose sender named no reply queue is counted as unanswerable and
/// accepted rather than retried. Nothing about redelivering it makes a reply
/// address appear, so retrying only moves the same message round the same loop.
/// </remarks>
public sealed class Responder : IDisposable
{
    private IMessageConsumer? _consumer;
    private long _answered;
    private long _unanswerable;
    private bool _disposed;

    private Responder(IMessageConsumer consumer) => _consumer = consumer;

    internal static async Task<Responder> StartAsync<TRequest, TResponse>(
        AceMqConnection mq, ICodec codec, string queue, ConsumerOptions options,
        Func<TRequest, Task<TResponse>> handler)
    {
        Responder? self = null;

        var consumer = await mq.ConsumeAsync<TRequest>(queue, options, async message =>
        {
            var replyTo = message.ReplyTo;
            if (string.IsNullOrEmpty(replyTo))
            {
                self?.CountUnanswerable();
                return Ack.Accept();
            }

            var answer = await handler(message.Payload).ConfigureAwait(false);

            // The reply carries the request's id as its correlation, which is what
            // the requester matches on. The default exchange addresses the reply
            // queue by name.
            var envelope = Envelope.Of(message.Envelope.Type)
                .CorrelationId(message.Envelope.Id)
                .CausationId(message.Envelope.Id)
                .Build();

            var publisher = mq.Publisher<TResponse>(string.Empty, replyTo!);
            await publisher.SendAsync(answer, envelope).ConfigureAwait(false);
            self?.CountAnswered();
            return Ack.Accept();
        }).ConfigureAwait(false);

        self = new Responder(consumer);
        return self;
    }

    private void CountAnswered() => Interlocked.Increment(ref _answered);
    private void CountUnanswerable() => Interlocked.Increment(ref _unanswerable);

    public long Answered => Interlocked.Read(ref _answered);

    /// <summary>Requests that arrived with no reply queue named.</summary>
    public long Unanswerable => Interlocked.Read(ref _unanswerable);

    public bool IsRunning => !_disposed && _consumer.IsActive;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _consumer.Dispose();
    }
}
