// Publishes a message and consumes it back, over the in-memory broker so the
// example runs with nothing installed. The same program exists in VB in ../vb,
// against the same assembly.
//
// Point it at a real broker by referencing AceMq.Amqp.RabbitMq, registering the
// transport, and changing the URL to amqp://localhost:
//
//     Transports.Register(new RabbitMqTransport());
//     await AceMqConnection.ConnectAsync("amqp://localhost");

using AceMq.Amqp;
using AceMq.Amqp.Avro;

using var mq = await AceMqConnection.ConnectAsync("memory://example");

await mq.DeclareExchangeAsync("orders", "topic");
await mq.DeclareQueueAsync("orders.placed");
await mq.BindAsync("orders.placed", "orders", "order.placed");

var arrived = new TaskCompletionSource<IMessage<OrderPlaced>>();

using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders.placed", message =>
{
    arrived.TrySetResult(message);
    // The handler returns what should happen to the message. Accept means the
    // broker may forget it; Retry and DeadLetter say why it should not.
    return Task.FromResult(Ack.Accept());
});

var publisher = mq.Publisher<OrderPlaced>("orders", "order.placed");

var envelope = Envelope.Of("order.placed")
    .CorrelationId("corr-1")
    .Header("x-tenant", "acme")
    .Build();

var result = await publisher.SendAsync(new OrderPlaced("A-1", 42.50m), envelope);

Console.WriteLine($"published    {result.MessageId}");
Console.WriteLine($"routed       {result.Routed}");

var received = await arrived.Task;

Console.WriteLine($"consumed     {received.Payload.OrderId} for {received.Payload.Total:0.00}");
Console.WriteLine($"correlation  {received.Envelope.CorrelationId}");
Console.WriteLine($"attempt      {received.Attempt}");
Console.WriteLine($"tenant       {received.Headers["x-tenant"]}");

// Publishing where nothing is bound fails at the call rather than disappearing.
var orphan = mq.Publisher<OrderPlaced>("orders", "order.cancelled");
try
{
    await orphan.SendAsync(new OrderPlaced("A-2", 1m));
}
catch (PublishFailedException)
{
    Console.WriteLine("unroutable   publish failed: no queue bound for that routing key");
}

// A batch: every message is written before any confirm is awaited, and the results
// come back in the order the payloads were given.
var batch = new List<OrderPlaced>
{
    new OrderPlaced("B-1", 1m),
    new OrderPlaced("B-2", 2m),
    new OrderPlaced("B-3", 3m),
};
var confirmed = await publisher.SendAllAsync(batch);
Console.WriteLine($"batch        {confirmed.Count} confirmed, all routed: {confirmed.All(r => r.Routed)}");

// A batch that fails names how many did not arrive and how many did, because
// resending a half-succeeded batch duplicates the half that already got there.
try
{
    await orphan.SendAllAsync(batch);
}
catch (PublishFailedException e)
{
    Console.WriteLine($"batch failed {e.Message.Split('.')[0]}");
}

// The claim: an optional field naming where the payload is, for a message that
// carries a reference rather than the bytes. Python and Ruby publishers set it and
// a handler here reads it off the envelope.
var claimed = Envelope.Of("order.placed")
    .Claim("s3://payloads/2026/09/A-3")
    .Build();
Console.WriteLine($"claim        {claimed.Claim}");
Console.WriteLine($"claim kept   {claimed.WithAttempt(2).Claim}");

// The Avro reader schema: the default resolves onto the codec's own schema, a named
// one resolves onto something else, and it can be declined altogether so every
// message is read with the shape its writer gave it.
var registry = new InMemorySchemaRegistry();
const string v1 = """
    {"type":"record","name":"OrderPlaced","namespace":"acemq.example",
     "fields":[{"name":"orderId","type":"string"}]}
    """;
const string v2 = """
    {"type":"record","name":"OrderPlaced","namespace":"acemq.example",
     "fields":[{"name":"orderId","type":"string"},
               {"name":"tenant","type":"string","default":""}]}
    """;

var resolving = AvroCodec.Registered(registry, v2);
var ontoV1 = AvroCodec.Registered(registry, v2, v1);
var asWritten = resolving.WithoutReaderSchema();

Console.WriteLine($"avro reader  default resolves: {resolving.ReaderSchema is not null}");
Console.WriteLine($"avro reader  named:            {ontoV1.ReaderSchema is not null}");
Console.WriteLine($"avro reader  declined:         {asWritten.ReaderSchema is null}");

// Topology as one unit: the queue, its dead-letter exchange and the queue bound to
// it. Declared separately, forgetting one loses messages with nothing reporting it.
var plan = await mq.ApplyAsync(
    Topology.Define().QueueWithDeadLetter("payments").Build());
Console.WriteLine($"topology     {plan.Actions.Count} action(s)");

// Request and reply.
await mq.DeclareQueueAsync("pricing");
using var responder = await mq.RespondAsync<string, string>(
    "pricing", request => Task.FromResult(request.ToUpperInvariant()));
using var requester = await mq.RequesterAsync();
Console.WriteLine($"replied      {await requester.RequestAsync<string, string>("", "pricing", "quote me")}");

// Ordering by key, across partitions.
var ledger = await mq.Ordered<string>("ledger")
    .Partitions(4)
    .KeyedBy(entry => entry.Split(':')[0])
    .DeclareAsync();
var first = await ledger.SendAsync("acct-7:deposit");
var second = await ledger.SendAsync("acct-7:withdraw");
Console.WriteLine($"ordering     same key, same partition: {first == second}");
ledger.Dispose();

public sealed record OrderPlaced(string OrderId, decimal Total);
