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
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp;

/// <summary>
/// Takes messages off a dead-letter queue and puts them back.
/// </summary>
/// <remarks>
/// <para>
/// The point of dead-lettering is that the messages are still there. This is how
/// they get another run once whatever broke has been fixed.
/// </para>
/// <para>
/// Messages are pulled one at a time rather than consumed with a subscription. A
/// subscription on the source queue would immediately be handed the messages the
/// replay had just republished if the two queues are connected, and there would be
/// no way to replay a bounded number and stop.
/// </para>
/// </remarks>
public sealed class Replay
{
    private readonly ITransportConnection _connection;
    private string _to;

    internal Replay(ITransportConnection connection, string from)
    {
        _connection = connection;
        From = from;
        // By convention a dead-letter queue is named after the queue it serves, so
        // the default destination is that queue.
        _to = from.EndsWith(".dead", StringComparison.Ordinal)
            ? from.Substring(0, from.Length - ".dead".Length)
            : from;
    }

    /// <summary>The queue being drained.</summary>
    public string From { get; }

    /// <summary>Where the messages are being sent.</summary>
    public string To => _to;

    /// <summary>Sends them to a named queue rather than the default destination.</summary>
    public Replay Into(string queue)
    {
        _to = queue ?? throw new ArgumentNullException(nameof(queue));
        return this;
    }

    /// <summary>How many messages are waiting to be replayed.</summary>
    public Task<long> PendingAsync() =>
        _connection.MessageCountAsync(From, CancellationToken.None);

    /// <summary>Replays everything currently on the queue.</summary>
    public Task<int> ReplayAllAsync() => ReplayAsync(int.MaxValue, null);

    /// <summary>Replays at most <paramref name="max"/> messages.</summary>
    public Task<int> ReplayAsync(int max) => ReplayAsync(max, null);

    /// <summary>
    /// Replays at most <paramref name="max"/> messages that match a filter.
    /// </summary>
    /// <remarks>
    /// A message the filter rejects is left where it is rather than discarded.
    /// Replaying selectively is normally about picking out one tenant or one kind of
    /// failure, and losing the rest as a side effect of looking at them would be a
    /// poor trade.
    /// </remarks>
    public async Task<int> ReplayAsync(int max, Func<InboundDelivery, bool>? filter)
    {
        if (max < 0) throw new ArgumentException("cannot be negative", nameof(max));

        var replayed = 0;
        var skipped = new List<InboundDelivery>();

        try
        {
            while (replayed < max)
            {
                var delivery = await _connection
                    .ReceiveAsync(From, TimeSpan.FromMilliseconds(200), CancellationToken.None)
                    .ConfigureAwait(false);
                if (delivery == null) break;

                if (filter != null && !filter(delivery))
                {
                    skipped.Add(delivery);
                    continue;
                }

                await _connection.SendAsync(Republished(delivery), CancellationToken.None)
                    .ConfigureAwait(false);
                replayed++;
            }
        }
        finally
        {
            // Whatever the filter passed over goes back where it was, even if the
            // replay threw part way through. Pulling a message off a queue and
            // failing to return it is data loss dressed up as an error.
            foreach (var delivery in skipped)
            {
                await _connection.SendAsync(ReturnedToSource(delivery), CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }

        return replayed;
    }

    /// <summary>
    /// The message as it should go back out: the failure reason cleared, and the
    /// replay counters advanced so a message that keeps failing is recognisable.
    /// </summary>
    private OutboundMessage Republished(InboundDelivery delivery)
    {
        var headers = new Dictionary<string, object>(
            (IDictionary<string, object>)delivery.Headers);

        // The error belongs to the attempt that failed, not to the new one. Leaving
        // it on would make every replayed message look like it had already failed
        // again.
        headers.Remove(AceHeaders.Error);
        headers[AceHeaders.ReplayedFrom] = From;
        headers[AceHeaders.ReplayedAt] =
            DateTimeOffset.UtcNow.ToString("o", CultureInfo.InvariantCulture);
        var count = headers.TryGetValue(AceHeaders.ReplayCount, out var c)
            ? Convert.ToInt32(c, CultureInfo.InvariantCulture)
            : 0;
        headers[AceHeaders.ReplayCount] = count + 1;

        return new OutboundMessage(
            string.Empty, _to, delivery.Body, headers,
            delivery.MessageId, delivery.ContentType,
            persistent: true, mandatory: true, expiration: null, priority: null,
            replyTo: delivery.ReplyTo);
    }

    private OutboundMessage ReturnedToSource(InboundDelivery delivery) =>
        new OutboundMessage(
            string.Empty, From, delivery.Body,
            new Dictionary<string, object>((IDictionary<string, object>)delivery.Headers),
            delivery.MessageId, delivery.ContentType,
            persistent: true, mandatory: true, expiration: null, priority: null,
            replyTo: delivery.ReplyTo);

    public override string ToString() => $"Replay[{From} -> {_to}]";
}
