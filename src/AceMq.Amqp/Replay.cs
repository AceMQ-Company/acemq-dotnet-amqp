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
/// <para>
/// Every replayed message is stamped with <see cref="AceHeaders.ReplayedFrom"/>,
/// <see cref="AceHeaders.ReplayedAt"/> and <see cref="AceHeaders.ReplayCount"/>.
/// None of the three carries <see cref="AceHeaders.Prefix"/>, so all three reach the
/// handler — in this library and in the other four — rather than being stripped as
/// the engine's on the way in.
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

        // The count is read before the old names are dropped, so a message replayed by
        // a 0.5.0 service carries on counting rather than starting again from one. A
        // message on its fifth trip through a dead-letter queue is saying something
        // that a reset counter would hide.
        var count = PreviousReplays(headers);

        // Written once, in the shared namespace, and the reserved spellings this
        // library used up to 0.5.0 are taken off. Leaving them on would put two names
        // for the same fact on the wire, and the pair that a consumer can actually see
        // is not the pair an operator would find by grepping.
        headers.Remove(AceHeaders.LegacyReplayedFrom);
        headers.Remove(AceHeaders.LegacyReplayedAt);
        headers.Remove(AceHeaders.LegacyReplayCount);

        headers[AceHeaders.ReplayedFrom] = From;
        headers[AceHeaders.ReplayedAt] = Rfc3339(DateTimeOffset.UtcNow);
        headers[AceHeaders.ReplayCount] = count + 1;

        return new OutboundMessage(
            string.Empty, _to, delivery.Body, headers,
            delivery.MessageId, delivery.ContentType,
            persistent: true, mandatory: true, expiration: null, priority: null,
            replyTo: delivery.ReplyTo);
    }

    /// <summary>
    /// How many times this message has already been replayed, under either spelling.
    /// </summary>
    /// <remarks>
    /// The current name wins where both are present, which is only ever a message some
    /// other tool stamped by hand. A value that will not read counts as none: the
    /// number is used for reporting, and failing a replay over it would refuse to move
    /// a message because of a header nobody depends on.
    /// </remarks>
    private static int PreviousReplays(IDictionary<string, object> headers)
    {
        foreach (var name in new[] { AceHeaders.ReplayCount, AceHeaders.LegacyReplayCount })
        {
            if (!headers.TryGetValue(name, out var value) || value == null) continue;
            try
            {
                return Convert.ToInt32(value, CultureInfo.InvariantCulture);
            }
            catch (Exception e) when (e is FormatException || e is InvalidCastException
                || e is OverflowException)
            {
                return 0;
            }
        }
        return 0;
    }

    /// <summary>
    /// The instant as the other four libraries write it: seconds, and a <c>Z</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Not <c>"o"</c>, which is what this wrote up to 0.5.0. That is legitimate RFC
    /// 3339 and Java's reader takes it, but it is the one shape in the family with
    /// both a <c>+00:00</c> offset and seven fractional digits — and the offset form
    /// has already cost Java a widened reader once. Go and Ruby write exactly this;
    /// Java writes it with an optional fraction; the shared <c>envelope-fixtures.json</c>
    /// pins the <c>Z</c> form. Whole seconds lose nothing anybody uses: this is the
    /// stamp on an operator draining a queue by hand.
    /// </para>
    /// <para>
    /// The literal <c>Z</c> is quoted rather than spelled with a format specifier so
    /// it stays a <c>Z</c> — an unquoted one would be read as the era designator.
    /// </para>
    /// </remarks>
    private static string Rfc3339(DateTimeOffset at) =>
        at.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    private OutboundMessage ReturnedToSource(InboundDelivery delivery) =>
        new OutboundMessage(
            string.Empty, From, delivery.Body,
            new Dictionary<string, object>((IDictionary<string, object>)delivery.Headers),
            delivery.MessageId, delivery.ContentType,
            persistent: true, mandatory: true, expiration: null, priority: null,
            replyTo: delivery.ReplyTo);

    public override string ToString() => $"Replay[{From} -> {_to}]";
}
