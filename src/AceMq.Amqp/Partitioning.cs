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

using System;
using System.Globalization;
using System.Text;

namespace AceMq.Amqp;

/// <summary>
/// Decides which partition a key belongs to.
/// </summary>
/// <remarks>
/// <para>
/// The hash is FNV-1a over the key's UTF-8 bytes, and it is deliberately not
/// <see cref="string.GetHashCode()"/>. .NET randomises string hashing per process,
/// so the same key would land in a different partition after a restart — and the
/// ordering guarantee that partitioning exists to provide would quietly stop
/// holding. It must also match the Java implementation, or a C# producer and a Java
/// consumer disagree about where a key lives.
/// </para>
/// </remarks>
public static class Partitioning
{
    /// <summary>A stable hash of a key, the same in every process and every language.</summary>
    public static int Hash(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        // FNV-1a, 32-bit.
        unchecked
        {
            const uint offset = 2166136261;
            const uint prime = 16777619;
            var hash = offset;
            foreach (var b in Encoding.UTF8.GetBytes(key))
            {
                hash ^= b;
                hash *= prime;
            }
            return (int)(hash & 0x7FFFFFFF);
        }
    }

    /// <summary>Which of <paramref name="partitions"/> partitions a key belongs to.</summary>
    public static int PartitionFor(string key, int partitions)
    {
        if (partitions < 1) throw new ArgumentException("must be at least 1", nameof(partitions));
        return Hash(key) % partitions;
    }

    /// <summary>The routing key suffix a partition is addressed by.</summary>
    public static string RoutingKeyFor(int partition)
    {
        if (partition < 0) throw new ArgumentException("cannot be negative", nameof(partition));
        return partition.ToString(CultureInfo.InvariantCulture);
    }
}
