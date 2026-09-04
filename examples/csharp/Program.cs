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

public sealed record OrderPlaced(string OrderId, decimal Total);
