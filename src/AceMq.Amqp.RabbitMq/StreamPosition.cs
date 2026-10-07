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

using System.Collections.Generic;

namespace AceMq.Amqp.RabbitMq;

/// <summary>
/// How far a stream subscription has got, so a recovered one carries on from there.
/// </summary>
/// <remarks>
/// A queue forgets what it delivered, so consuming it again after a recovery takes
/// whatever is left. A stream forgets nothing, and consuming it again starts wherever
/// <c>x-stream-offset</c> says. A recovered stream subscription therefore starts at the
/// oldest entry it was given and never settled, or just after the newest it settled:
/// what a queue would redeliver, and nothing it would not. The same rule as the Go and
/// Ruby libraries.
/// </remarks>
internal sealed class StreamPosition
{
    internal const string Argument = "x-stream-offset";

    private readonly object _lock = new object();
    private Dictionary<long, int> _pending = new Dictionary<long, int>();
    private long _settled = -1;

    /// <summary>Which connection's deliveries count; moved on by every recovery.</summary>
    private int _generation;

    /// <summary>
    /// A delivery at this offset was handed to the handler. Returns the token to
    /// settle it with.
    /// </summary>
    internal int Delivered(long offset)
    {
        lock (_lock)
        {
            _pending.TryGetValue(offset, out var n);
            _pending[offset] = n + 1;
            return _generation;
        }
    }

    /// <summary>The delivery at this offset was acknowledged or rejected.</summary>
    internal void Settle(long offset, int generation)
    {
        lock (_lock)
        {
            // A copy from before the last recovery. Its offset is being delivered
            // again, and letting it move the position could carry a later recovery
            // past entries the new copies have not reached yet.
            if (generation != _generation) return;
            if (_pending.TryGetValue(offset, out var n))
            {
                if (n <= 1) _pending.Remove(offset);
                else _pending[offset] = n - 1;
            }
            if (offset > _settled) _settled = offset;
        }
    }

    /// <summary>
    /// The offset a recovered subscription starts at, or null to keep its own
    /// starting point because nothing was delivered.
    /// </summary>
    /// <remarks>
    /// The pending deliveries belonged to the channel that has gone and are delivered
    /// again by the new one, so they are forgotten here, and a handler still finishing
    /// an old copy no longer moves the position.
    /// </remarks>
    internal long? Resume()
    {
        lock (_lock)
        {
            long? oldest = null;
            foreach (var offset in _pending.Keys)
            {
                if (oldest == null || offset < oldest) oldest = offset;
            }
            _pending = new Dictionary<long, int>();
            _generation++;
            if (oldest.HasValue) return oldest;
            return _settled >= 0 ? _settled + 1 : (long?)null;
        }
    }

    /// <summary>The offset RabbitMQ stamps on every stream delivery, if present.</summary>
    internal static long? OffsetOf(IDictionary<string, object?>? headers)
    {
        if (headers == null || !headers.TryGetValue(Argument, out var value)) return null;
        switch (value)
        {
            case long l when l >= 0: return l;
            case int i when i >= 0: return i;
            default: return null;
        }
    }
}
