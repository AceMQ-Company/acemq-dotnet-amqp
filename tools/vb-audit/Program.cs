// Audits the public API for things VB.NET cannot call.
//
// VB is case-insensitive, has no ref struct or Span<T>, and resolves optional
// arguments differently from C#. Each of these is a compile error for a VB
// consumer that C# never sees, so they have to be found before the API freezes.
//
// The rules are the table in docs/vbnet.md, and all five are checked here:
//
//   1. no two public members differing only by case
//   2. no ref struct or Span<T> on the public surface
//   3. no overload sets separable only by optional arguments
//   4. no unsafe, pointer types or C#-only operator tricks
//   5. async methods return plain Task / Task(Of T)
//
// Four and five were in the table and in nothing that ran. A rule nobody checks is
// a rule that has already been broken, which is the whole reason this program
// exists, so they run now -- along with four more things that are a compile error
// for a VB consumer and were checked nowhere: an init-only setter, a required
// member, a default interface member, and a member colliding by case with an
// inherited one rather than with a sibling. VB can use none of them.
//
// Every one of the twelve rules self-checks against a type built to trip it, for
// the same reason the case rule always did: a check that never fires is
// indistinguishable from a check that is not running, and none of these has ever
// fired on this library's own surface.
using System.Reflection;

var assemblies = new[]
{
    typeof(AceMq.Amqp.AceMqConnection).Assembly,
    typeof(AceMq.Amqp.RabbitMq.RabbitMqTransport).Assembly,
    typeof(AceMq.Amqp.Diagnostics.AceMqActuator).Assembly,
    typeof(AceMq.Amqp.Protobuf.ProtobufCodec).Assembly,
    typeof(AceMq.Amqp.Avro.AvroCodec).Assembly,
    typeof(AceMq.Amqp.DevCerts.DevelopmentCertificates).Assembly,
    typeof(AceMq.Amqp.Yaml.YamlCodec).Assembly,
    typeof(AceMq.Amqp.Toml.TomlCodec).Assembly,
    typeof(AceMq.Amqp.Xml.InteropXmlCodec).Assembly,
    typeof(AceMq.Amqp.Crypto.EncryptedCodec).Assembly,
};

var findings = new List<string>();
foreach (var assembly in assemblies)
foreach (var type in assembly.GetExportedTypes())
{
    Audit.Type(type, findings);
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

// Each rule against a probe written to trip exactly it.
var selfCheck = new (System.Type Probe, string Expect, string Rule)[]
{
    (typeof(CaseProbe), "case-only difference", "members differing only by case"),
    (typeof(InheritedCaseProbe), "against an inherited member",
        "a member colliding by case with an inherited one"),
    (typeof(SpanProbe), "Span", "Span<T> on the public surface"),
    (typeof(RefStructProbe), "ref struct", "ref struct on the public surface"),
    (typeof(OptionalProbe), "mixes optional arguments", "overloads separable by optional arguments"),
    (typeof(PointerProbe), "pointer", "pointer types and unsafe"),
    (typeof(OperatorProbe), "has no VB equivalent", "C#-only operators"),
    (typeof(AwaitableProbe), "exotic awaitable", "async methods returning plain Task"),
    (typeof(AsyncStreamProbe), "async stream", "async streams"),
    (typeof(InitOnlyProbe), "init-only setter", "init-only setters"),
    (typeof(RequiredProbe), "required member", "required members"),
    (typeof(IDefaultMemberProbe), "default interface member", "default interface members"),
};

var silent = new List<string>();
foreach (var (probe, expect, rule) in selfCheck)
{
    var tripped = new List<string>();
    Audit.Type(probe, tripped);
    if (!tripped.Any(f => f.Contains(expect, StringComparison.Ordinal))) silent.Add(rule);
}

if (silent.Count > 0)
{
    Console.WriteLine("self-check: FAILED -- these rules did not fire on a type built to trip them,");
    Console.WriteLine("so a clean result says nothing about them:");
    foreach (var rule in silent) Console.WriteLine("  " + rule);
    return 2;
}
Console.WriteLine(
    $"self-check: all {selfCheck.Length} rules fire on types built to trip them");

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

/// <summary>The rules, over one type at a time, reporting into a caller's list.</summary>
/// <remarks>
/// A list rather than a field, so the same rules can be run against the probes below
/// without their findings being mistaken for the library's.
/// </remarks>
internal static class Audit
{
    private const BindingFlags Surface =
        BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static |
        BindingFlags.DeclaredOnly;

    internal static void Type(System.Type type, List<string> findings)
    {
        var members = type.GetMembers(Surface);

        // Rule 1. Two members whose names differ only by case are one identifier to VB.
        foreach (var group in members.GroupBy(m => m.Name, StringComparer.OrdinalIgnoreCase))
        {
            var distinct = group.Select(m => m.Name).Distinct(StringComparer.Ordinal).ToList();
            if (distinct.Count > 1)
            {
                findings.Add($"case-only difference in {type.Name}: {string.Join(", ", distinct)}");
            }
        }

        // And across the inheritance chain, not only within one type. A class
        // declaring Send() under a base that declares send() is the same collision
        // and was invisible to a DeclaredOnly scan, because neither type on its own
        // has two names.
        var inherited = type.GetMembers(
                BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
            .Select(m => m.Name)
            .Distinct(StringComparer.Ordinal)
            .ToList();
        foreach (var declared in members.Select(m => m.Name).Distinct(StringComparer.Ordinal))
        {
            foreach (var other in inherited)
            {
                if (string.Equals(declared, other, StringComparison.Ordinal)) continue;
                if (!string.Equals(declared, other, StringComparison.OrdinalIgnoreCase)) continue;
                findings.Add(
                    $"case-only difference in {type.Name} against an inherited member: " +
                    $"{declared}, {other}");
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

        // Rules 2, 4 and 5, over every signature a VB caller can reach: methods,
        // operators, constructors, properties and fields. Constructors used to be
        // missed entirely -- GetMethods does not return them -- so a Span<T> or a
        // pointer in a public constructor was invisible to the check meant to find it.
        foreach (var member in members)
        {
            var where = $"{type.Name}.{member.Name}";

            switch (member)
            {
                case MethodInfo method:
                    if (method.IsSpecialName && !method.Name.StartsWith("op_")) break;
                    foreach (var p in method.GetParameters())
                    {
                        CheckType(p.ParameterType, where, "parameter", findings);
                    }
                    CheckType(method.ReturnType, where, "return", findings);
                    CheckOperator(method, type, findings);
                    CheckAwaitable(method, where, findings);
                    break;

                case ConstructorInfo constructor:
                    foreach (var p in constructor.GetParameters())
                    {
                        CheckType(p.ParameterType, $"{type.Name}.New", "parameter", findings);
                    }
                    break;

                case PropertyInfo property:
                    CheckType(property.PropertyType, where, "type", findings);
                    // An init-only setter is settable from C# and from nowhere in VB:
                    // the accessor carries a modreq on IsExternalInit, which VB has no
                    // syntax to satisfy, so the property is read-only to a VB consumer.
                    var setter = property.GetSetMethod();
                    if (setter != null && setter.ReturnParameter.GetRequiredCustomModifiers()
                            .Any(m => m.Name == "IsExternalInit"))
                    {
                        findings.Add($"init-only setter on {where}, which VB cannot assign");
                    }
                    break;

                case FieldInfo field:
                    CheckType(field.FieldType, where, "type", findings);
                    break;
            }

            // A required member has to be assigned in an object initializer, and VB
            // has no syntax that satisfies the compiler's check. The type becomes
            // unconstructable rather than awkward.
            if (member.GetCustomAttributesData()
                .Any(a => a.AttributeType.Name == "RequiredMemberAttribute"))
            {
                findings.Add($"required member {where}, which VB cannot construct");
            }
        }

        // Rule 3. Overloads that differ only in how many arguments have defaults are
        // ambiguous to VB, which fills optionals differently.
        foreach (var group in type.GetMethods(Surface)
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

        // A default interface member is not callable from VB at all. The library says
        // so in a comment on PublishInterceptor and carries abstract base classes
        // because of it; nothing checked that a later interface had not grown one.
        if (type.IsInterface)
        {
            foreach (var method in type.GetMethods(
                         BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                if (!method.IsAbstract)
                {
                    findings.Add($"default interface member {type.Name}.{method.Name}, " +
                                 "which VB cannot call or implement");
                }
            }
        }
    }

    /// <summary>Rules 2, 4 and 5, for one type in one position.</summary>
    private static void CheckType(
        System.Type t, string where, string position, List<string> findings)
    {
        var bare = t.IsByRef ? t.GetElementType()! : t;

        if (bare.IsByRefLike)
        {
            findings.Add($"ref struct {position} on {where}: {bare.Name}");
        }
        if (bare.Name.StartsWith("Span`") || bare.Name.StartsWith("ReadOnlySpan`"))
        {
            findings.Add($"Span {position} on {where}");
        }

        // Rule 4. A pointer or a function pointer has no VB spelling at all, and an
        // assembly compiled with /unsafe reaches a consumer exactly here: there is no
        // other way for `unsafe` to be visible on a public surface.
        if (bare.IsPointer)
        {
            findings.Add($"pointer {position} on {where}");
        }
        if (bare.IsFunctionPointer)
        {
            findings.Add($"function pointer {position} on {where}");
        }

        // Rule 5's other half. VB has no `Await For Each`, so an async stream is a
        // method a VB consumer can call and cannot consume.
        if (bare.IsGenericType)
        {
            var name = bare.GetGenericTypeDefinition().FullName;
            if (name == "System.Collections.Generic.IAsyncEnumerable`1"
                || name == "System.Collections.Generic.IAsyncEnumerator`1")
            {
                findings.Add($"async stream {position} on {where}: VB has no Await For Each");
            }
        }
    }

    /// <summary>Rule 4's operator half: the ones VB has no syntax for.</summary>
    /// <remarks>
    /// VB expresses most operator overloads. These are the ones it cannot: ++ and --
    /// have no VB equivalent, C# 11's >>> has none, and the checked operators are a
    /// C#-only concept. A type whose only way to be combined is one of these is a
    /// type VB can hold and not use.
    /// </remarks>
    private static void CheckOperator(MethodInfo method, System.Type type, List<string> findings)
    {
        if (!method.Name.StartsWith("op_")) return;

        var unusable = new[]
        {
            "op_Increment", "op_Decrement", "op_UnsignedRightShift",
            "op_CheckedAddition", "op_CheckedSubtraction", "op_CheckedMultiply",
            "op_CheckedDivision", "op_CheckedUnaryNegation", "op_CheckedIncrement",
            "op_CheckedDecrement", "op_CheckedExplicit",
        };
        if (unusable.Contains(method.Name, StringComparer.Ordinal))
        {
            findings.Add($"operator {type.Name}.{method.Name} has no VB equivalent");
        }
    }

    /// <summary>Rule 5: an async method returns plain Task or Task(Of T).</summary>
    /// <remarks>
    /// VB's Await does follow the awaitable pattern, so a ValueTask is technically
    /// awaitable from VB. The rule in docs/vbnet.md is the stricter one, because a
    /// custom awaitable is where that pattern stops being reliable and because this
    /// library has no reason to return anything else. Anything awaitable that is not
    /// Task or Task(Of T) is reported so that it is a decision rather than a drift.
    /// </remarks>
    private static void CheckAwaitable(MethodInfo method, string where, List<string> findings)
    {
        var t = method.ReturnType;
        if (t == typeof(void) || t == typeof(Task)) return;
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Task<>)) return;

        // Awaitable is a pattern, not an interface: a public GetAwaiter() taking
        // nothing, whose result carries IsCompleted and GetResult.
        var awaiter = t.GetMethod("GetAwaiter", BindingFlags.Public | BindingFlags.Instance,
            binder: null, types: System.Type.EmptyTypes, modifiers: null);
        if (awaiter == null) return;
        if (awaiter.ReturnType.GetProperty("IsCompleted") == null) return;
        if (awaiter.ReturnType.GetMethod("GetResult") == null) return;

        findings.Add($"exotic awaitable returned by {where}: {t.Name} is not Task or Task(Of T)");
    }
}

// ---- the probes ---------------------------------------------------------
//
// One per rule, each written to be exactly the thing the rule forbids. None of
// them is exported from a shipped assembly; they exist so that a clean audit means
// the rules ran and found nothing, rather than that the rules did not run.

/// <summary>Two members that are one identifier to VB.</summary>
public sealed class CaseProbe
{
    public void Send() { }
    public void send(int x) { }
}

/// <summary>A member that is one identifier to VB only once the base is counted.</summary>
public abstract class InheritedCaseProbeBase
{
    public void send(int x) { }
}

public sealed class InheritedCaseProbe : InheritedCaseProbeBase
{
    public void Send() { }
}

/// <summary>A Span on the public surface.</summary>
public sealed class SpanProbe
{
    public int Count(ReadOnlySpan<byte> body) => body.Length;
}

/// <summary>A ref struct in a public constructor, which the old audit never looked at.</summary>
public sealed class RefStructProbe
{
    public RefStructProbe(Marker marker) { }

    public ref struct Marker
    {
        public int Value;
    }
}

/// <summary>Overloads VB resolves differently, and ambiguously.</summary>
public sealed class OptionalProbe
{
    public void Send(string what) { }
    public void Send(string what, int times = 1) { }
}

/// <summary>A pointer, which is what `unsafe` looks like from outside.</summary>
public sealed unsafe class PointerProbe
{
    public int Read(byte* p) => *p;
}

/// <summary>An operator with no VB spelling.</summary>
public sealed class OperatorProbe
{
    public int Value;
    public static OperatorProbe operator ++(OperatorProbe p) => p;
}

/// <summary>An async method returning something other than Task.</summary>
public sealed class AwaitableProbe
{
    public ValueTask<int> CountAsync() => new ValueTask<int>(0);
}

/// <summary>An async stream, which VB cannot iterate.</summary>
public sealed class AsyncStreamProbe
{
    public IAsyncEnumerable<int> ReadAsync() => throw new NotSupportedException();
}

/// <summary>A property VB can read and never assign.</summary>
public sealed class InitOnlyProbe
{
    public string Name { get; init; } = "";
}

/// <summary>A type VB cannot construct.</summary>
public sealed class RequiredProbe
{
    public required string Name { get; set; }
}

/// <summary>An interface member VB cannot call or implement.</summary>
public interface IDefaultMemberProbe
{
    void Handle();

    void HandleTwice()
    {
        Handle();
        Handle();
    }
}
