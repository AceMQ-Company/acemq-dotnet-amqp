# VB.NET

**There is no VB library, and there will not be one.** VB.NET and C# compile to the
same IL and run on the same runtime, so a VB application references
`AceMq.Amqp` directly and uses the same types.

What VB costs is not a second implementation. It is discipline on the public API of
the first one.

## The same code, in VB

```vb
Imports AceMq.Amqp

Module Program
    Sub Main()
        Dim envelope = Envelope.Of("order.placed") _
            .Version(3) _
            .CorrelationId("corr-1") _
            .CausationId("cause-1") _
            .Origin("orders@host-7") _
            .Header("x-tenant", "acme") _
            .Build()

        Console.WriteLine(envelope.Id)
        Console.WriteLine(envelope.CorrelationId)
        Console.WriteLine(envelope.FirstSeen.ToUnixTimeMilliseconds())

        Dim wire = envelope.ToWire()
        For Each pair In wire
            Console.WriteLine($"{pair.Key} = {pair.Value}")
        Next
    End Sub
End Module
```

## What the API may not do, so that this keeps working

These are enforced by a VB sample compiled in CI rather than remembered:

| Constraint | Why |
|---|---|
| No two public members differing only by case | VB is case-insensitive; `Send` and `send` would be a compile error for the consumer |
| No `ref struct` or `Span<T>` on the public surface | Not usable from VB |
| No overload sets separable only by optional arguments | VB resolves them differently, and ambiguously |
| No `unsafe`, pointer types, or C#-only operator tricks | No VB equivalent |
| Async methods return plain `Task` / `Task(Of T)` | VB `Await` handles those; exotic awaitables are painful |

The audit that enforces this has to happen **before the API freezes**. Afterwards it
is a breaking change, and the whole point of the constraint is that it costs a week
now instead of a major version later.

## Runnable example

`examples/vb/` — build and run it:

```bash
cd examples/vb
dotnet run
```

It is compiled in CI for the same reason the constraints table exists: a rule
nothing checks is a rule that has already been broken.
