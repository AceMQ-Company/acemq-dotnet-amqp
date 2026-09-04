// Audits the public API for things VB.NET cannot call.
//
// VB is case-insensitive, has no ref struct or Span<T>, and resolves optional
// arguments differently from C#. Each of these is a compile error for a VB
// consumer that C# never sees, so they have to be found before the API freezes.
using System.Reflection;

var findings = new List<string>();
var assemblies = new[]
{
    typeof(AceMq.Amqp.AceMqConnection).Assembly,
    typeof(AceMq.Amqp.RabbitMq.RabbitMqTransport).Assembly,
};

foreach (var assembly in assemblies)
foreach (var type in assembly.GetExportedTypes())
{
    var members = type.GetMembers(BindingFlags.Public | BindingFlags.Instance |
                                  BindingFlags.Static | BindingFlags.DeclaredOnly);

    // Two members whose names differ only by case are one identifier to VB.
    foreach (var group in members.GroupBy(m => m.Name, StringComparer.OrdinalIgnoreCase))
    {
        var distinct = group.Select(m => m.Name).Distinct(StringComparer.Ordinal).ToList();
        if (distinct.Count > 1)
        {
            findings.Add($"case-only difference in {type.Name}: {string.Join(", ", distinct)}");
        }
    }

    // A nested type whose name matches a member is also ambiguous to VB.
    foreach (var nested in type.GetNestedTypes(BindingFlags.Public))
    {
        if (members.Any(m => string.Equals(m.Name, nested.Name, StringComparison.OrdinalIgnoreCase)
                             && !string.Equals(m.Name, nested.Name, StringComparison.Ordinal)))
        {
            findings.Add($"nested type {type.Name}.{nested.Name} collides by case with a member");
        }
    }

    foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance |
                                           BindingFlags.Static | BindingFlags.DeclaredOnly))
    {
        if (method.IsSpecialName && !method.Name.StartsWith("op_")) continue;

        foreach (var p in method.GetParameters())
        {
            var pt = p.ParameterType;
            if (pt.IsByRefLike || (pt.IsByRef && pt.GetElementType()!.IsByRefLike))
            {
                findings.Add($"ref struct on {type.Name}.{method.Name}: {pt.Name}");
            }
            if (pt.Name.StartsWith("Span`") || pt.Name.StartsWith("ReadOnlySpan`"))
            {
                findings.Add($"Span on {type.Name}.{method.Name}");
            }
        }
        if (method.ReturnType.IsByRefLike)
        {
            findings.Add($"ref struct returned by {type.Name}.{method.Name}");
        }
    }

    // Overloads that differ only in how many arguments have defaults are ambiguous
    // to VB, which fills optionals differently.
    foreach (var group in type.GetMethods(BindingFlags.Public | BindingFlags.Instance |
                                          BindingFlags.Static | BindingFlags.DeclaredOnly)
                              .Where(m => !m.IsSpecialName)
                              .GroupBy(m => m.Name))
    {
        var withOptionals = group.Where(m => m.GetParameters().Any(p => p.IsOptional)).ToList();
        if (withOptionals.Count > 0 && group.Count() > 1)
        {
            findings.Add(
                $"overload set {type.Name}.{group.Key} mixes optional arguments with " +
                $"{group.Count()} overloads");
        }
    }
}

var types = assemblies.SelectMany(a => a.GetExportedTypes()).ToList();
var methodCount = types.Sum(t => t.GetMethods(BindingFlags.Public | BindingFlags.Instance |
                                              BindingFlags.Static | BindingFlags.DeclaredOnly).Length);
Console.WriteLine($"scanned {types.Count} public types, {methodCount} public methods");

// A check that never fires is indistinguishable from a check that is not running.
// This proves the case-collision rule works against a type built to trip it.
var probeMembers = typeof(CaseProbe).GetMembers(
    BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
var tripped = probeMembers.GroupBy(m => m.Name, StringComparer.OrdinalIgnoreCase)
    .Any(g => g.Select(m => m.Name).Distinct(StringComparer.Ordinal).Count() > 1);
Console.WriteLine(tripped
    ? "self-check: the case-collision rule fires on a deliberately bad type"
    : "self-check: FAILED -- the rule did not fire, so a clean result means nothing");
if (!tripped) return 2;

if (findings.Count == 0)
{
    Console.WriteLine("VB audit: clean. Nothing on the public surface VB cannot call.");
}
else
{
    Console.WriteLine($"VB audit: {findings.Count} finding(s)");
    foreach (var f in findings.Distinct().OrderBy(f => f)) Console.WriteLine("  " + f);
}
return findings.Count == 0 ? 0 : 1;


/// <summary>Deliberately VB-hostile, to prove the audit's rules actually fire.</summary>
public sealed class CaseProbe
{
    public void Send() { }
    public void send(int x) { }
}
