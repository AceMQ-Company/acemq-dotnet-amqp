' The same program as ../csharp, in VB.NET, against the same assembly.
' There is no VB build of AceMQ: VB and C# compile to the same IL.
'
' Note the variable is named `message`, not `envelope`. VB is case-insensitive, so
' `Dim envelope = Envelope.Of(...)` cannot compile -- the variable and the type are
' the same identifier to VB, and it reports BC30980. The equivalent C# is fine.
' This is why the VB sample is compiled in CI rather than assumed to work.

Imports System
Imports System.Linq
Imports AceMq.Amqp

Module Program
    Sub Main()
        Dim message = Envelope.Of("order.placed") _
            .Version(3) _
            .CorrelationId("corr-1") _
            .CausationId("cause-1") _
            .Origin("orders@host-7") _
            .Header("x-tenant", "acme") _
            .Build()

        Console.WriteLine($"type         {message.Type}")
        Console.WriteLine($"correlation  {message.CorrelationId}")
        Console.WriteLine($"causation    {message.CausationId}")
        Console.WriteLine($"attempt      {message.Attempt}")

        Console.WriteLine(Environment.NewLine & "on the wire:")
        Dim wire = message.ToWire()
        For Each pair In wire.OrderBy(Function(p) p.Key)
            Console.WriteLine($"  {pair.Key,-24} {pair.Value}")
        Next

        Dim readBack = Envelope.FromWire(
            wire.ToDictionary(Function(p) p.Key, Function(p) p.Value), "orders.new")

        Console.WriteLine()
        Console.WriteLine($"round trip preserved the id:   {readBack.Id = message.Id}")
        Console.WriteLine($"application headers survived:  {readBack.Headers("x-tenant")}")
        Console.WriteLine($"engine headers were stripped:  {Not readBack.Headers.Keys.Any(AddressOf AceHeaders.IsAceHeader)}")

        Try
            Envelope.Of("t").Header(AceHeaders.Id, "mine")
        Catch e As ArgumentException
            Console.WriteLine()
            Console.WriteLine($"reserved namespace refused: {e.Message.Split("."c)(0)}.")
        End Try
    End Sub
End Module
