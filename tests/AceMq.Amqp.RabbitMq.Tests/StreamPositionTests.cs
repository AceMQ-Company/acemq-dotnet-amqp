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

using AceMq.Amqp.RabbitMq;

namespace AceMq.Amqp.RabbitMq.Tests;

/// <summary>Where a recovered stream subscription starts. No broker needed.</summary>
public sealed class StreamPositionTests
{
    [Fact]
    public void NothingDeliveredKeepsTheSubscriptionsOwnStart() =>
        Assert.Null(new StreamPosition().Resume());

    [Fact]
    public void AllSettledResumesJustAfterTheNewest()
    {
        var p = new StreamPosition();
        foreach (var o in new long[] { 4, 5, 6 }) p.Settle(o, p.Delivered(o));
        Assert.Equal(7, p.Resume());
    }

    [Fact]
    public void AnUnsettledDeliveryIsDeliveredAgain()
    {
        var p = new StreamPosition();
        p.Settle(4, p.Delivered(4));
        p.Delivered(5);
        p.Delivered(6);
        Assert.Equal(5, p.Resume());
    }

    [Fact]
    public void AnOldCopySettledAfterARecoveryDoesNotMoveThePosition()
    {
        var p = new StreamPosition();
        p.Settle(4, p.Delivered(4));
        var old = p.Delivered(5);
        p.Delivered(6);
        Assert.Equal(5, p.Resume());

        // The old channel's handler finishes 6 after the recovery; the new copy of 5
        // has not arrived. A second recovery must still start at 5.
        p.Settle(6, old);
        Assert.Equal(5, p.Resume());
    }

    [Fact]
    public void ReadsTheOffsetHeaderAsTheBrokerSendsIt()
    {
        Assert.Equal(42, StreamPosition.OffsetOf(new Dictionary<string, object?> { ["x-stream-offset"] = 42L }));
        Assert.Null(StreamPosition.OffsetOf(new Dictionary<string, object?>()));
        Assert.Null(StreamPosition.OffsetOf(null));
    }
}
