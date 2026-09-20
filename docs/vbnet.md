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

| Constraint | Why |
|---|---|
| No two public members differing only by case | VB is case-insensitive; `Send` and `send` would be a compile error for the consumer |
| No `ref struct` or `Span<T>` on the public surface | Not usable from VB — which is why codecs take `byte[]` |
| No overload sets separable only by optional arguments | VB resolves them differently, and ambiguously |
| No `unsafe`, pointer types, or C#-only operator tricks | No VB equivalent |
| Async methods return plain `Task` / `Task(Of T)` | VB `Await` handles those; exotic awaitables are painful |

Four more belong on that list and were nowhere, because each is a compile error for
a VB consumer that C# never sees:

| Constraint | Why |
|---|---|
| No member colliding by case with an **inherited** one | The same collision as the first rule, and invisible to a check that looks at one type at a time |
| No `init`-only setters | The accessor carries a modreq VB has no syntax to satisfy, so the property is read-only to VB |
| No `required` members | VB has no way to satisfy the compiler's initialization check, so the type is unconstructable |
| No default interface members | VB can neither call nor implement one; this is why `PublishInterceptor` and `ConsumeInterceptor` exist as abstract classes |
| No `IAsyncEnumerable<T>` on the public surface | VB has no `Await For Each` |

## How that is enforced

Two things, and neither is a note to remember.

**`tools/vb-audit`**, which CI runs. It walks every exported type in all ten shipped
assemblies by reflection and checks all twelve rules against methods, operators,
**constructors**, properties and fields. Constructors used to be missed entirely —
`GetMethods` does not return them — so a `Span<T>` in a public constructor was
invisible to the one check meant to find it.

Every rule **self-checks against a type written to trip it**, and the audit exits
non-zero if any rule fails to fire. A check that never fires is indistinguishable
from a check that is not running, and none of these twelve has ever fired on this
library's own surface — so without the probes, a clean result would say nothing.

**`examples/vb`**, which CI compiles *and runs*. The reflection audit proves the
shape; only a compiler proves the shape is callable. The sample prints the same
output as [the C# example](csharp.md), which is the claim VB support rests on, and
the two are kept reaching the same API deliberately — including `SendAllAsync`, the
batch failure, the envelope's `Claim` and the Avro reader schema.

The audit had to happen **before the API freezes**. Afterwards every correction is a
breaking change, and the whole point of the constraint is that it costs a week now
instead of a major version later. It has been done: as of 0.7.0 the public surface
is clean against all twelve rules.

## Runnable example

`examples/vb/` — it publishes and consumes a message and prints the result:

```bash
cd examples/vb
dotnet run
```

It produces the same output as [the C# example](csharp.md), which is the claim VB
support rests on. CI runs both.
