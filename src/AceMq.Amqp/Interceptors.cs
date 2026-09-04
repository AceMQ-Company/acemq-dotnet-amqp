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

namespace AceMq.Amqp;

/// <summary>A message about to be published.</summary>
public sealed class PublishContext
{
    public PublishContext(string exchange, string routingKey, Envelope envelope, object? payload)
    {
        Exchange = exchange;
        RoutingKey = routingKey;
        Envelope = envelope;
        Payload = payload;
    }

    public string Exchange { get; }
    public string RoutingKey { get; }
    public Envelope Envelope { get; }
    public object? Payload { get; }

    /// <summary>The same publish with a different envelope.</summary>
    /// <remarks>
    /// The envelope is the only part an interceptor may change. The payload and the
    /// destination are not: an interceptor that could rewrite either would be able
    /// to send a message somewhere the caller never asked for, which is a
    /// surprising amount of power for something usually added to attach a header.
    /// </remarks>
    public PublishContext WithEnvelope(Envelope replacement) =>
        new PublishContext(
            Exchange, RoutingKey,
            replacement ?? throw new ArgumentNullException(nameof(replacement)),
            Payload);

    public override string ToString() =>
        $"PublishContext[{Exchange}/{RoutingKey}, {Envelope.Id}]";
}

/// <summary>A message about to be handled.</summary>
public sealed class ConsumeContext
{
    public ConsumeContext(string queue, Envelope envelope, object? payload)
    {
        Queue = queue;
        Envelope = envelope;
        Payload = payload;
    }

    public string Queue { get; }
    public Envelope Envelope { get; }
    public object? Payload { get; }

    public override string ToString() => $"ConsumeContext[{Queue}, {Envelope.Id}]";
}

/// <summary>
/// Runs around every publish.
/// </summary>
/// <remarks>
/// For the concerns that belong to every message rather than to one call site: a
/// tenant header, an audit record, a check that outgoing messages carry what a
/// policy requires. Returning a context with a different envelope changes what is
/// published.
/// </remarks>
public interface IPublishInterceptor
{
    /// <summary>Called before the message is encoded. Return the context to publish.</summary>
    PublishContext BeforePublish(PublishContext context);

    /// <summary>Called once the broker has confirmed it.</summary>
    void AfterConfirm(PublishContext context, PublishResult result);

    /// <summary>Called when the publish failed.</summary>
    void OnError(PublishContext context, Exception failure);

    /// <summary>Lower runs first.</summary>
    int Order { get; }
}

/// <summary>Runs around every handled message.</summary>
public interface IConsumeInterceptor
{
    void BeforeHandle(ConsumeContext context);

    /// <summary>Called with the disposition the handler returned.</summary>
    void AfterHandle(ConsumeContext context, Ack ack);

    /// <summary>Called when the handler threw rather than returning.</summary>
    void OnError(ConsumeContext context, Exception failure);

    /// <summary>Lower runs first.</summary>
    int Order { get; }
}

/// <summary>
/// A publish interceptor that does nothing, to inherit from.
/// </summary>
/// <remarks>
/// C# interfaces cannot carry implementations on <c>netstandard2.0</c>, so an
/// interceptor that only cares about one of the three moments would otherwise have
/// to write two empty methods. VB cannot use default interface members either, which
/// settles it.
/// </remarks>
public abstract class PublishInterceptor : IPublishInterceptor
{
    public virtual PublishContext BeforePublish(PublishContext context) => context;

    public virtual void AfterConfirm(PublishContext context, PublishResult result) { }

    public virtual void OnError(PublishContext context, Exception failure) { }

    public virtual int Order => 0;
}

/// <summary>A consume interceptor that does nothing, to inherit from.</summary>
public abstract class ConsumeInterceptor : IConsumeInterceptor
{
    public virtual void BeforeHandle(ConsumeContext context) { }

    public virtual void AfterHandle(ConsumeContext context, Ack ack) { }

    public virtual void OnError(ConsumeContext context, Exception failure) { }

    public virtual int Order => 0;
}
