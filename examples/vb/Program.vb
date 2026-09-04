' The same program as ../csharp, in VB.NET, against the same assembly.
' There is no VB build of AceMQ: VB and C# compile to the same IL.
'
' Note the envelope variable is named `outgoing`, not `envelope`. VB is
' case-insensitive, so `Dim envelope = Envelope.Of(...)` cannot compile -- the
' variable and the type are the same identifier to VB, and it reports BC30980.
' The equivalent C# is fine. This is why the VB sample is compiled and run in CI
' rather than assumed to work.

Imports System
Imports System.Threading.Tasks
Imports AceMq.Amqp

Module Program
    ' VB has no async entry point. C# accepts `static async Task Main`, VB does not,
    ' and declaring `Function Main() As Task` fails with BC30737: no accessible
    ' 'Main' method with an appropriate signature. So the entry point is a plain Sub
    ' that blocks on the async work. Every async API in this library is reachable
    ' from VB; only Main is not.
    Sub Main()
        RunAsync().GetAwaiter().GetResult()
    End Sub

    Async Function RunAsync() As Task
        Using mq = Await AceMqConnection.ConnectAsync("memory://example")

            Await mq.DeclareExchangeAsync("orders", "topic")
            Await mq.DeclareQueueAsync("orders.placed")
            Await mq.BindAsync("orders.placed", "orders", "order.placed")

            Dim arrived = New TaskCompletionSource(Of IMessage(Of OrderPlaced))()

            ' The handler returns what should happen to the message. Accept means
            ' the broker may forget it; Retry and DeadLetter say why it should not.
            Using consumer = Await mq.ConsumeAsync(Of OrderPlaced)(
                "orders.placed",
                Function(message)
                    arrived.TrySetResult(message)
                    Return Task.FromResult(Ack.Accept())
                End Function)

                Dim publisher = mq.Publisher(Of OrderPlaced)("orders", "order.placed")

                Dim outgoing = Envelope.Of("order.placed") _
                    .CorrelationId("corr-1") _
                    .Header("x-tenant", "acme") _
                    .Build()

                Dim result = Await publisher.SendAsync(
                    New OrderPlaced("A-1", 42.5D), outgoing)

                Console.WriteLine($"published    {result.MessageId}")
                Console.WriteLine($"routed       {result.Routed}")

                Dim received = Await arrived.Task

                Console.WriteLine($"consumed     {received.Payload.OrderId} for {received.Payload.Total:0.00}")
                Console.WriteLine($"correlation  {received.Envelope.CorrelationId}")
                Console.WriteLine($"attempt      {received.Attempt}")
                Console.WriteLine($"tenant       {received.Headers("x-tenant")}")

                ' Publishing where nothing is bound fails at the call rather than
                ' disappearing.
                Dim orphan = mq.Publisher(Of OrderPlaced)("orders", "order.cancelled")
                Try
                    Await orphan.SendAsync(New OrderPlaced("A-2", 1D))
                Catch ex As PublishFailedException
                    Console.WriteLine("unroutable   publish failed: no queue bound for that routing key")
                End Try
            End Using
        End Using
    End Function
End Module

Public NotInheritable Class OrderPlaced
    Public Sub New()
    End Sub

    Public Sub New(id As String, amount As Decimal)
        OrderId = id
        Total = amount
    End Sub

    Public Property OrderId As String = ""
    Public Property Total As Decimal
End Class
