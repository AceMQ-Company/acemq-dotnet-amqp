' The same program as ../csharp, in VB.NET, against the same assembly.
' There is no VB build of AceMQ: VB and C# compile to the same IL.
'
' Note the envelope variable is named `outgoing`, not `envelope`. VB is
' case-insensitive, so `Dim envelope = Envelope.Of(...)` cannot compile -- the
' variable and the type are the same identifier to VB, and it reports BC30980.
' The equivalent C# is fine. This is why the VB sample is compiled and run in CI
' rather than assumed to work.

Imports System
Imports System.Collections.Generic
Imports System.Linq
Imports System.Threading.Tasks
Imports AceMq.Amqp
Imports AceMq.Amqp.Avro

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

                ' A batch: every message is written before any confirm is awaited,
                ' and the results come back in the order the payloads were given.
                Dim batch = New List(Of OrderPlaced) From {
                    New OrderPlaced("B-1", 1D),
                    New OrderPlaced("B-2", 2D),
                    New OrderPlaced("B-3", 3D)}
                Dim confirmed = Await publisher.SendAllAsync(batch)
                Console.WriteLine($"batch        {confirmed.Count} confirmed, all routed: {confirmed.All(Function(r) r.Routed)}")

                ' A batch that fails names how many did not arrive and how many did,
                ' because resending a half-succeeded batch duplicates the half that
                ' already got there.
                Try
                    Await orphan.SendAllAsync(batch)
                Catch ex As PublishFailedException
                    Console.WriteLine($"batch failed {ex.Message.Split("."c)(0)}")
                End Try

                ' The claim: an optional field naming where the payload is, for a
                ' message that carries a reference rather than the bytes. Python and
                ' Ruby publishers set it and a VB handler reads it off the envelope.
                Dim claimed = Envelope.Of("order.placed") _
                    .Claim("s3://payloads/2026/09/A-3") _
                    .Build()
                Console.WriteLine($"claim        {claimed.Claim}")
                Console.WriteLine($"claim kept   {claimed.WithAttempt(2).Claim}")

                ' The Avro reader schema, from VB. Not because this sample needs
                ' Avro, but because three of the four ways to set it are new and a
                ' reflection audit proves the shape while only a compiler proves the
                ' shape is callable.
                Dim registry = New InMemorySchemaRegistry()
                Dim v1 = "{""type"":""record"",""name"":""OrderPlaced"",""namespace"":""acemq.example""," &
                         """fields"":[{""name"":""orderId"",""type"":""string""}]}"
                Dim v2 = "{""type"":""record"",""name"":""OrderPlaced"",""namespace"":""acemq.example""," &
                         """fields"":[{""name"":""orderId"",""type"":""string""}," &
                         "{""name"":""tenant"",""type"":""string"",""default"":""""}]}"

                Dim resolving = AvroCodec.Registered(registry, v2)
                Dim ontoV1 = AvroCodec.Registered(registry, v2, v1)
                Dim asWritten = resolving.WithoutReaderSchema()

                Console.WriteLine($"avro reader  default resolves: {resolving.ReaderSchema IsNot Nothing}")
                Console.WriteLine($"avro reader  named:            {ontoV1.ReaderSchema IsNot Nothing}")
                Console.WriteLine($"avro reader  declined:         {asWritten.ReaderSchema Is Nothing}")

                ' Topology as one unit: the queue, its dead-letter exchange and the
                ' queue bound to it. Declared separately, forgetting one loses
                ' messages with nothing reporting it.
                Dim plan = Await mq.ApplyAsync(
                    Topology.Define().QueueWithDeadLetter("payments").Build())
                Console.WriteLine($"topology     {plan.Actions.Count} action(s)")

                ' Request and reply, from VB.
                Await mq.DeclareQueueAsync("pricing")
                Using responder = Await mq.RespondAsync(Of String, String)(
                    "pricing", Function(request) Task.FromResult(request.ToUpperInvariant()))

                    Using requester = Await mq.RequesterAsync()
                        Dim answer = Await requester.RequestAsync(Of String, String)(
                            "", "pricing", "quote me")
                        Console.WriteLine($"replied      {answer}")
                    End Using
                End Using

                ' Ordering by key, across partitions.
                Dim ledger = Await mq.Ordered(Of String)("ledger") _
                    .Partitions(4) _
                    .KeyedBy(Function(entry) entry.Split(":"c)(0)) _
                    .DeclareAsync()
                Dim first = Await ledger.SendAsync("acct-7:deposit")
                Dim second = Await ledger.SendAsync("acct-7:withdraw")
                Console.WriteLine($"ordering     same key, same partition: {first = second}")
                ledger.Dispose()
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
