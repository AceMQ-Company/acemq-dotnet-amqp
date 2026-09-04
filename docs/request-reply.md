# Request and reply

Messaging is one-way. Request/reply builds a round trip on top of it: the sender
names a queue to answer on, and the answer comes back correlated to the question.

```csharp
using var requester = await mq.RequesterAsync();

var quote = await requester.RequestAsync<QuoteRequest, Quote>(
    "pricing", "quote.requested", new QuoteRequest("A-1"));
```

Answering:

```csharp
using var responder = await mq.RespondAsync<QuoteRequest, Quote>(
    "pricing.requests", async request => await _pricing.QuoteAsync(request));
```

The responder replies to whatever queue the request named, with the request's id as
the reply's correlation id.

## One reply queue per requester

Not one per request. A queue per request costs a declare and a delete on the broker
for every call, which is the difference between request/reply being usable at rate
and being a curiosity.

That means replies for many outstanding requests share a queue, and **matching them
is the requester's job**. Each reply is handed to the caller whose request id it
correlates to. A reply arriving for a caller that has already given up is counted as
`Unmatched` and dropped:

```csharp
requester.TimedOut    // requests that gave up before an answer arrived
requester.Unmatched   // replies with nobody left waiting for them
```

Handing a late answer to whoever happens to ask next is worse than no answer, and it
is exactly what a shared reply queue read in arrival order would do.

## Timeouts

Thirty seconds by default. A request that is not answered in time throws
`RequestTimedOutException`:

```csharp
try
{
    var quote = await requester.RequestAsync<QuoteRequest, Quote>(
        "pricing", "quote.requested", request,
        TimeSpan.FromSeconds(5), cancellationToken);
}
catch (RequestTimedOutException)
{
    // The request may still be processed. A timeout says no answer arrived,
    // not that nothing happened.
}
```

That distinction matters when the operation has an effect. A timed-out request whose
handler succeeded a moment later has still been carried out, so anything that must
not happen twice needs the request to be idempotent — the envelope's id is the key
for that.

## Requests that cannot be answered

A request published without a reply queue is counted and accepted rather than
retried:

```csharp
responder.Unanswerable
```

Redelivering it would not make a reply address appear, so retrying only moves the
same message round the same loop.

## When not to use it

Request/reply over a broker adds a network hop, a queue and a correlation to what an
HTTP call does directly, and it couples the caller to the responder's availability
for the duration — which is the property messaging usually exists to avoid.

It earns its place when the responder is already a consumer of the same broker, when
the request should queue rather than fail while the responder is redeploying, or when
the reply is genuinely optional. If the caller cannot proceed without the answer and
both sides speak HTTP, use HTTP.
