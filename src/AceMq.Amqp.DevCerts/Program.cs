// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using AceMq.Amqp;
using AceMq.Amqp.DevCerts;

// The command line, in the shape `dotnet dev-certs` established -- which is what a
// .NET developer will reach for, and the reason this is a tool rather than an
// MSBuild target. A build that writes private keys into a working tree is normal
// in Maven and surprising here.
var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
var flags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

for (var i = 0; i < args.Length; i++)
{
    var arg = args[i];
    if (!arg.StartsWith("--", StringComparison.Ordinal)) continue;
    var name = arg.Substring(2);
    if (i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
    {
        options[name] = args[++i];
    }
    else
    {
        flags.Add(name);
    }
}

if (flags.Contains("help") || flags.Contains("h"))
{
    Console.WriteLine(@"acemq certs — development certificates for talking to a broker

  --out <dir>        where to write them            (default: certs)
  --broker <host>    the name the server cert names (default: localhost)
  --days <n>         how long they are valid        (default: 30)
  --password <p>     protects client.pfx            (default: acemq-dev)
  --broker-certs <d> the path rabbitmq.conf points at (default: /certs)
  --no-broker-config do not write rabbitmq.conf

Everything written is stamped ""ACEMQ DEVELOPMENT ONLY - DO NOT TRUST"" and the
library refuses it unless TlsOptions.AllowDevelopmentCertificates() is called.");
    return 0;
}

try
{
    var result = DevelopmentCertificates.Create(
        directory: options.TryGetValue("out", out var o) ? o : "certs",
        broker: options.TryGetValue("broker", out var b) ? b : "localhost",
        days: options.TryGetValue("days", out var d) ? int.Parse(d) : 30,
        password: options.TryGetValue("password", out var p) ? p : DevelopmentCertificates.DefaultPassword,
        brokerCertificateDirectory: options.TryGetValue("broker-certs", out var bc) ? bc : "/certs",
        writeBrokerConfig: !flags.Contains("no-broker-config"));

    Console.WriteLine($"wrote development certificates to {result.Directory}");
    Console.WriteLine($"  ca.crt       trust this: TlsOptions.Required().TrustCertificateAuthority(\"ca.crt\")");
    Console.WriteLine($"  server.crt   the broker's certificate");
    Console.WriteLine($"  server.key   the broker's key");
    Console.WriteLine($"  client.pfx   for EXTERNAL auth, password: {(options.TryGetValue("password", out var shown) ? shown : DevelopmentCertificates.DefaultPassword)}");
    if (result.BrokerConfig != null) Console.WriteLine($"  rabbitmq.conf mount at /etc/rabbitmq/rabbitmq.conf");
    Console.WriteLine();
    Console.WriteLine($"valid until {result.Expires:yyyy-MM-dd}. Stamped \"{TlsOptions.DevelopmentMarker}\",");
    Console.WriteLine("so the library refuses them unless you call AllowDevelopmentCertificates().");
    return 0;
}
catch (Exception e)
{
    Console.Error.WriteLine($"could not write the certificates: {e.Message}");
    return 1;
}
