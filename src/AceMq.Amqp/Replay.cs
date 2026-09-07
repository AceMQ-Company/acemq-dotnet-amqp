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
    private bool _restart = true;

    internal Replay(ITransportConnection connection, string from)
    {
        _connection = connection;
        From = from;
        _to = SourceOf(from);
    }

    /// <summary>
    /// The queue a dead-letter or parking queue serves, or the queue itself.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>.dlq</c> and <c>.parked</c> are the names the whole family uses and the
    /// only ones anything in this library still produces.
    /// </para>
    /// <para>
    /// <c>.dead</c> is recognised as well, and nothing creates it any more. It is
    /// what <see cref="Topology.Builder.QueueWithDeadLetter(string)"/> produced
    /// before it was settled on the group convention, so brokers already running
    /// have queues by that name with messages in them. Dropping the suffix would
    /// not break a build or fail a test; it would quietly make
    /// <c>mq.Replay("orders.dead")</c> republish to <c>orders.dead</c> instead of
    /// <c>orders</c>, which is an operator draining a queue into itself while
    /// watching a count that never falls. It costs one array element to keep, and
    /// it can go once no broker has one.
    /// </para>
    /// </remarks>
    private static string SourceOf(string from)
    {
        foreach (var suffix in new[] { Naming.DeadLetterSuffix, Naming.ParkedSuffix, ".dead" })
        {
            if (from.EndsWith(suffix, StringComparison.Ordinal))
            {
                return from.Substring(0, from.Length - suffix.Length);
            }
        }
        return from;
    }

    /// <summary>The queue being drained.</summary>
    public string From { get; }

    /// <summary>Where the messages are being sent.</summary>
    public string To => _to;

    /// <summary>Whether each replayed message gets a fresh set of attempts.</summary>
    public bool Restarts => _restart;

    /// <summary>Sends them to a named queue rather than the default destination.</summary>
    public Replay Into(string queue)
    {
        _to = queue ?? throw new ArgumentNullException(nameof(queue));
        return this;
    }

    /// <summary>
    /// Puts back exactly what was there, attempt counter and all.
    /// </summary>
    /// <remarks>
    /// Off the default path, because a replay normally wants the opposite. A message
    /// that was dead-lettered on the last attempt of its policy comes back still on
    /// that attempt, and the consumer gives up on it again before a handler sees it —
    /// so the operator who has just fixed the bug has moved two thousand messages from
    /// one queue to the same queue. Ask for this when the point is an audit, or when
    /// the queue is read by something that counts attempts itself.
    /// </remarks>
    public Replay KeepingAttempts()
    {
        _restart = false;
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
    /// The message as it should go back out: the failure reason cleared, the attempt
    /// counter restarted, and the replay counters advanced so a message that keeps
    /// failing is recognisable.
    /// </summary>
    private OutboundMessage Republished(InboundDelivery delivery)
    {
        var headers = new Dictionary<string, object>(
            (IDictionary<string, object>)delivery.Headers);

        // The error belongs to the attempt that failed, not to the new one. Leaving
        // it on would make every replayed message look like it had already failed
        // again.
        headers.Remove(AceHeaders.Error);

        // A fresh set of attempts, unless the caller asked otherwise. The counter now
        // travels on the message — a retry republishes with it advanced — so a message
        // dead-lettered on the last attempt of its policy would arrive back on that
        // same attempt and be dead-lettered again before any handler saw it. Replaying
        // two thousand messages from a queue to the same queue is not what anybody
        // means by a replay.
        if (_restart) headers[AceHeaders.Attempt] = 1;

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
