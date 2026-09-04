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

namespace AceMq.Amqp.Tests;

/// <summary>
/// Topic routing in the in-memory transport, against RabbitMQ's rules.
/// </summary>
/// <remarks>
/// The in-memory broker is only useful if a binding that matches here matches on a
/// real broker too. A test suite whose fake router is more permissive than the real
/// one is worse than no test suite: it passes, and then the deployment does not.
/// <para>
/// The <c>#</c> cases are the ones worth having. It matches zero or more words,
/// which means it also has to absorb the separators around it — the case that a
/// pattern translated naively into a regular expression gets wrong.
/// </para>
/// </remarks>
public sealed class TopicRoutingTests
{
    [Theory]
    // Exact and single-word wildcards.
    [InlineData("order.placed", "order.placed", true)]
    [InlineData("order.placed", "order.shipped", false)]
    [InlineData("order.*", "order.placed", true)]
    [InlineData("order.*", "order.placed.eu", false)]
    [InlineData("*.placed", "order.placed", true)]
    [InlineData("*", "order", true)]
    [InlineData("*", "order.placed", false)]
    // '#' matches zero or more words, including none at all.
    [InlineData("#", "order.placed.eu", true)]
    [InlineData("#", "order", true)]
    [InlineData("order.#", "order", true)]
    [InlineData("order.#", "order.placed", true)]
    [InlineData("order.#", "order.placed.eu", true)]
    [InlineData("order.#", "shipment.placed", false)]
    [InlineData("#.eu", "order.placed.eu", true)]
    [InlineData("#.eu", "eu", true)]
    [InlineData("#.eu", "order.placed.us", false)]
    // '#' in the middle has to absorb the dots on both sides.
    [InlineData("order.#.eu", "order.eu", true)]
    [InlineData("order.#.eu", "order.placed.eu", true)]
    [InlineData("order.#.eu", "order.placed.today.eu", true)]
    [InlineData("order.#.eu", "order.placed.us", false)]
    // Mixed wildcards.
    [InlineData("*.#", "order", true)]
    [InlineData("*.#", "order.placed.eu", true)]
    [InlineData("order.*.#", "order", false)]
    [InlineData("order.*.#", "order.placed", true)]
    public void MatchesTopicsTheWayRabbitMqDoes(string pattern, string routingKey, bool expected)
    {
        Assert.Equal(expected, InMemoryTransport.TopicMatches(pattern, routingKey));
    }
}
