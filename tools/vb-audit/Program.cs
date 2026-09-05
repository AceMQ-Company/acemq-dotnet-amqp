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
    typeof(AceMq.Amqp.Diagnostics.AceMqActuator).Assembly,
    typeof(AceMq.Amqp.Protobuf.ProtobufCodec).Assembly,
    typeof(AceMq.Amqp.Avro.AvroCodec).Assembly,
    typeof(AceMq.Amqp.DevCerts.DevelopmentCertificates).Assembly,
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

// The audit has silently covered fewer assemblies than the solution ships, twice,
// each time a package was added. Comparing against the solution turns that from
// something to remember into a failed build.
var solution = Path.GetFullPath(Path.Combine(
    AppContext.BaseDirectory, "..", "..", "..", "..", "..", "AceMq.Amqp.slnx"));
if (File.Exists(solution))
{
    var shipped = System.Text.RegularExpressions.Regex
        .Matches(File.ReadAllText(solution), @"src/([A-Za-z.]+)/\1\.csproj")
        .Select(m => m.Groups[1].Value)
        .ToHashSet(StringComparer.Ordinal);
    var scanned = assemblies.Select(a => a.GetName().Name!).ToHashSet(StringComparer.Ordinal);
    var uncovered = shipped.Except(scanned).OrderBy(n => n).ToList();
    if (uncovered.Count > 0)
    {
        Console.WriteLine(
            "audit coverage: FAILED -- the solution ships assemblies this audit does not scan:");
        foreach (var name in uncovered) Console.WriteLine("  " + name);
        Console.WriteLine("Add a ProjectReference and a typeof(...) entry for each.");
        return 3;
    }
    Console.WriteLine($"audit coverage: all {shipped.Count} shipped assemblies are scanned");
}
else
{
    Console.WriteLine($"audit coverage: FAILED -- no solution file at {solution}");
    return 3;
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
