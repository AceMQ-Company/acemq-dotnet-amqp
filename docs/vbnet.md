# VB.NET

**There is no VB library, and there will not be one.** VB.NET and C# compile to the
same IL and run on the same runtime, so a VB application references `AceMq.Amqp`
directly and uses the same types.

What VB costs is not a second implementation. It is discipline on the public API of
the first one.

## Publishing and consuming, in VB

```vb
Imports System
Imports System.Threading.Tasks
Imports AceMq.Amqp

Module Program
    ' VB has no async entry point, so Main blocks on the async work.
    Sub Main()
        RunAsync().GetAwaiter().GetResult()
    End Sub

    Async Function RunAsync() As Task
        Using mq = Await AceMqConnection.ConnectAsync("memory://example")

            Await mq.DeclareExchangeAsync("orders", "topic")
            Await mq.DeclareQueueAsync("orders.placed")
            Await mq.BindAsync("orders.placed", "orders", "order.placed")

            Using consumer = Await mq.ConsumeAsync(Of OrderPlaced)(
                "orders.placed",
                Function(message)
                    Console.WriteLine(message.Payload.OrderId)
                    Return Task.FromResult(Ack.Accept())
                End Function)

                Dim publisher = mq.Publisher(Of OrderPlaced)("orders", "order.placed")

                Dim outgoing = Envelope.Of("order.placed") _
                    .CorrelationId("corr-1") _
                    .Header("x-tenant", "acme") _
                    .Build()

                Dim result = Await publisher.SendAsync(
                    New OrderPlaced("A-1", 42.5D), outgoing)

                Console.WriteLine($"published {result.MessageId}, routed {result.Routed}")
            End Using
        End Using
    End Function
End Module
```

Generic methods take `(Of T)` where C# takes `<T>`, and that is the whole
difference. There is no VB-specific API and no wrapper.

## Two things VB will not let you write

Both were found by compiling the example, not by reasoning about the language.

**A variable cannot share a name with the type it is initialised from.** VB is
case-insensitive, so the variable and the type are the same identifier to it:

```vb
Dim envelope = Envelope.Of("order.placed").Build()
' error BC30980: Type of 'envelope' cannot be inferred from an expression
' containing 'envelope'
```

The identical C# — `var envelope = Envelope.Of(...)` — compiles. Name the variable
something else; the examples use `outgoing`.

**There is no async `Main`.** C# accepts `static async Task Main`. VB does not, and
declaring `Function Main() As Task` fails:

```
error BC30737: No accessible 'Main' method with an appropriate signature was found
```

So a VB entry point is a plain `Sub Main` that blocks on the async work, as above.
Every async API in the library is reachable from VB — only `Main` is not.

## What the API may not do, so that this keeps working

Enforced by a VB sample that CI compiles **and runs**, rather than remembered:

| Constraint | Why |
|---|---|
| No two public members differing only by case | VB is case-insensitive; `Send` and `send` would be a compile error for the consumer |
| No `ref struct` or `Span<T>` on the public surface | Not usable from VB — which is why codecs take `byte[]` |
| No overload sets separable only by optional arguments | VB resolves them differently, and ambiguously |
| No `unsafe`, pointer types, or C#-only operator tricks | No VB equivalent |
| Async methods return plain `Task` / `Task(Of T)` | VB `Await` handles those; exotic awaitables are painful |

The audit that enforces this has to happen **before the API freezes**. Afterwards it
is a breaking change, and the whole point of the constraint is that it costs a week
now instead of a major version later.

## Runnable example

`examples/vb/` — it publishes and consumes a message and prints the result:

```bash
cd examples/vb
dotnet run
```

It produces the same output as [the C# example](csharp.md), which is the claim VB
support rests on. CI runs both.
