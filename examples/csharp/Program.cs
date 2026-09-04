// Builds an envelope, renders it to the wire, and reads it back.
// The same program exists in VB in ../vb, against the same assembly.

using AceMq.Amqp;

var envelope = Envelope.Of("order.placed")
    .Version(3)
    .CorrelationId("corr-1")
    .CausationId("cause-1")
    .Origin("orders@host-7")
    .Header("x-tenant", "acme")
    .Build();

Console.WriteLine($"type         {envelope.Type}");
Console.WriteLine($"correlation  {envelope.CorrelationId}");
Console.WriteLine($"causation    {envelope.CausationId}");
Console.WriteLine($"attempt      {envelope.Attempt}");

Console.WriteLine("\non the wire:");
var wire = envelope.ToWire();
foreach (var pair in wire.OrderBy(p => p.Key))
{
    Console.WriteLine($"  {pair.Key,-24} {pair.Value}");
}

// Read it back: the reserved namespace is stripped, the application's header stays.
var readBack = Envelope.FromWire(
    wire.ToDictionary(p => p.Key, p => p.Value), routingKey: "orders.new");

Console.WriteLine($"\nround trip preserved the id:   {readBack.Id == envelope.Id}");
Console.WriteLine($"application headers survived:  {readBack.Headers["x-tenant"]}");
Console.WriteLine($"engine headers were stripped:  {!readBack.Headers.Keys.Any(AceHeaders.IsAceHeader)}");

// Writing into the reserved namespace fails here rather than vanishing in transit.
try
{
    Envelope.Of("t").Header(AceHeaders.Id, "mine");
}
catch (ArgumentException e)
{
    Console.WriteLine($"\nreserved namespace refused: {e.Message.Split('.')[0]}.");
}
