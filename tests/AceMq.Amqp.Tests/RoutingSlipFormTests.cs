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

using System.Collections.Concurrent;
using System.Text.Json;
using AceMq.Amqp;

namespace AceMq.Amqp.Tests;

/// <summary>
/// The two wire forms of a routing slip, and a pipeline following either of them.
/// </summary>
/// <remarks>
/// AceMQ has two. Java writes <c>x-acemq-route</c>, a comma-joined list of step names
/// resolved against a route somebody declared; Go, Python and Ruby write
/// <c>acemq-routing-slip</c>, a JSON itinerary carrying each stop's address and the
/// stops already done. This library reads and writes both, so what is asserted here
/// is that a slip written by any of the five is followed here, and that what this
/// library writes is what the other four will read.
/// </remarks>
public sealed class RoutingSlipFormTests
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");
    private readonly string _q = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);

    private static async Task Eventually(Func<bool> probe, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (probe()) return;
            await Task.Delay(10);
        }
        throw new TimeoutException($"timed out waiting for {what}");
    }

    // ---- the JSON itinerary, byte for byte -------------------------------

    [Fact]
    public void WritesTheJsonTheOtherThreeLibrariesWrite()
    {
        var slip = new Itinerary()
            .Then("orders-events", "order.validate", "validate")
            .Then("orders-events", "order.charge", "charge");

        // Field names, order and header are Go, Python and Ruby's. Three libraries to
        // one, so they define the shape and this one copies it: camelCase `routingKey`
        // rather than anything more idiomatic here, because the wire is where five
        // libraries have to agree.
        Assert.Equal(
            "{\"steps\":[" +
            "{\"exchange\":\"orders-events\",\"routingKey\":\"order.validate\",\"name\":\"validate\"}," +
            "{\"exchange\":\"orders-events\",\"routingKey\":\"order.charge\",\"name\":\"charge\"}]}",
            slip.ToHeader());
        Assert.Equal("acemq-routing-slip", Itinerary.Header);
    }

    [Fact]
    public void LeavesOutTheFieldsTheOtherThreeLeaveOut()
    {
        // An unnamed step writes neither `name` nor `completedAt`, and a slip with
        // nothing done writes no `done` at all -- which is what Go's `omitempty` and
        // Ruby both produce. Python writes an empty array instead; every reader
        // accepts either, and two of the three that define this form leave it out.
        var slip = new Itinerary().Then(string.Empty, "work");

        Assert.Equal("{\"steps\":[{\"exchange\":\"\",\"routingKey\":\"work\"}]}", slip.ToHeader());
    }

    [Fact]
    public void CarriesWhatItHasDoneSoTheSlipSaysHowFarItGot()
    {
        var slip = new Itinerary()
            .Then("x", "a", "first")
            .Then("x", "b", "second")
            .Advance();

        Assert.Single(slip.Done);
        Assert.Equal("first", slip.Done[0].Name);
        Assert.Single(slip.Steps);
        Assert.Equal("second", slip.Next!.Name);

        // Stamped as it advances, in RFC 3339 to the second -- the rendering Ruby
        // writes and one Go parses without complaint.
        Assert.Matches(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", slip.Done[0].CompletedAt);

        var written = JsonDocument.Parse(slip.ToHeader()).RootElement;
        Assert.Equal(1, written.GetProperty("done").GetArrayLength());
        Assert.Equal("first", written.GetProperty("done")[0].GetProperty("name").GetString());
        Assert.Equal(
            slip.Done[0].CompletedAt,
            written.GetProperty("done")[0].GetProperty("completedAt").GetString());
    }

    [Fact]
    public void ReadsAnItineraryWrittenSomewhereElse()
    {
        // Pasted from what a Go publisher puts on the wire, `done` included.
        const string written =
            "{\"steps\":[{\"exchange\":\"orders\",\"routingKey\":\"order.ship\",\"name\":\"ship\"}]," +
            "\"done\":[{\"exchange\":\"orders\",\"routingKey\":\"order.charge\"," +
            "\"name\":\"charge\",\"completedAt\":\"2026-09-09T10:11:12Z\"}]}";

        var slip = Itinerary.From(
            new Dictionary<string, object> { [Itinerary.Header] = written })!;

        Assert.Equal("ship", slip.Next!.Name);
        Assert.Equal("orders", slip.Next.Exchange);
        Assert.Equal("order.ship", slip.Next.RoutingKey);
        Assert.Single(slip.Done);
        Assert.Equal("charge", slip.Done[0].Name);
        Assert.Equal("2026-09-09T10:11:12Z", slip.Done[0].CompletedAt);
    }

    [Fact]
    public void ReadsAnItineraryThatArrivedAsBytes()
    {
        // A broker hands a header back as bytes rather than a string often enough
        // that refusing them would make a slip written by Go unreadable here.
        var slip = Itinerary.From(new Dictionary<string, object>
        {
            [Itinerary.Header] =
                System.Text.Encoding.UTF8.GetBytes("{\"steps\":[{\"exchange\":\"\",\"routingKey\":\"q\"}]}"),
        })!;

        Assert.Equal("q", slip.Next!.RoutingKey);
    }

    [Fact]
    public void RefusesAnItineraryItCannotRead()
    {
        // Fatal rather than retryable, as it is in the other three: a slip that will
        // not parse will not parse on the next attempt either, and spending five
        // retries on it only delays whoever has to look at it.
        var error = Assert.Throws<AceFatalException>(() => Itinerary.From(
            new Dictionary<string, object> { [Itinerary.Header] = "{not json" }));
        Assert.Contains("cannot read the routing slip", error.Message);
    }

    [Fact]
    public void HasNoItineraryWhenTheMessageCarriesNone()
    {
        Assert.Null(Itinerary.From(new Dictionary<string, object>()));
        Assert.Null(Route.From(new Dictionary<string, object>()));
    }

    // ---- reading either form --------------------------------------------

    [Fact]
    public void ReadsWhicheverFormIsOnTheMessage()
    {
        var declared = Route.From(RoutingSlip.StartOf("a", "b").ToHeaders())!;
        Assert.IsType<RoutingSlip>(declared);
        Assert.Equal("a", declared.Destination!.RoutingKey);

        var itinerary = Route.From(new Itinerary().Then("x", "a").ToHeaders())!;
        Assert.IsType<Itinerary>(itinerary);
        Assert.Equal("x", itinerary.Destination!.Exchange);
        Assert.Equal("a", itinerary.Destination.RoutingKey);
    }

    [Fact]
    public void BelievesTheItineraryWhenAMessageCarriesBoth()
    {
        // Unusual, and the itinerary is the one to believe: it carries the addresses
        // rather than names that have to be resolved against a declaration somewhere,
        // so following it needs nothing that could be missing.
        var headers = new Dictionary<string, object>();
        foreach (var h in RoutingSlip.StartOf("declared").ToHeaders()) headers[h.Key] = h.Value;
        foreach (var h in new Itinerary().Then("x", "itinerary").ToHeaders()) headers[h.Key] = h.Value;

        Assert.Equal("itinerary", Route.From(headers)!.Destination!.RoutingKey);
    }

    // ---- following either form through a broker --------------------------

    [Fact]
    public async Task CarriesAMessageAlongAnItinerary()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var visited = new ConcurrentQueue<string>();
        var finished = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var steps = new[] { _q + ".one", _q + ".two", _q + ".three" };
        foreach (var step in steps) await mq.DeclareQueueAsync(step);

        var consumers = new List<IMessageConsumer>();
        foreach (var step in steps)
        {
            var here = step;
            consumers.Add(await mq.ConsumeAsync<string>(here, async message =>
            {
                visited.Enqueue(here);
                var slip = Itinerary.Of(message)!.Advance();
                if (slip.IsFinished) finished.TrySetResult(true);
                else await mq.ForwardAsync(slip, message.Payload, message.Envelope);
                return Ack.Accept();
            }));
        }

        var itinerary = new Itinerary();
        foreach (var step in steps) itinerary = itinerary.Then(string.Empty, step, step);
        await mq.SendAlongAsync(itinerary, "an order");
        await finished.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(steps, visited.ToArray());
        foreach (var consumer in consumers) consumer.Dispose();
    }

    [Fact]
    public async Task GrowsTheDoneListAsTheItineraryTravels()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var first = _q + ".first";
        var second = _q + ".second";
        foreach (var step in new[] { first, second }) await mq.DeclareQueueAsync(step);

        Itinerary? atSecond = null;
        using var c1 = await mq.ConsumeAsync<string>(first, async message =>
        {
            await mq.ForwardAsync(
                Itinerary.Of(message)!.Advance(), message.Payload, message.Envelope);
            return Ack.Accept();
        });
        using var c2 = await mq.ConsumeAsync<string>(second, message =>
        {
            atSecond = Itinerary.Of(message);
            return Task.FromResult(Ack.Accept());
        });

        await mq.SendAlongAsync(
            new Itinerary().Then(string.Empty, first, "first").Then(string.Empty, second, "second"),
            "payload");
        await Eventually(() => atSecond != null, "the message at the second stop");

        // What has already happened, oldest first, so a slip that fails half way says
        // how far it got. Whoever finds it in a dead-letter queue is asking exactly that.
        Assert.Single(atSecond!.Done);
        Assert.Equal("first", atSecond.Done[0].Name);
        Assert.NotEqual(string.Empty, atSecond.Done[0].CompletedAt);
        Assert.Equal("second", atSecond.Next!.Name);
    }

    // ---- a pipeline, which attaches a slip of its own --------------------

    /// <summary>
    /// The message a step could not handle, as it sits in the dead-letter queue.
    /// </summary>
    /// <remarks>
    /// Read out of the dead-letter queue rather than by attaching a second consumer to
    /// the step's own queue: a second consumer competes with the pipeline's for the
    /// message, and whichever wins is a coin toss. This is also what an operator
    /// actually has to work with — the message as it was when it failed, and nothing
    /// else.
    /// </remarks>
    private static async Task<PulledMessage<string>> StuckAt(
        AceMqConnection mq, string stepQueue)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            var pulled = await mq.PullAsync<string>(stepQueue + ".dlq", TimeSpan.FromMilliseconds(50));
            if (pulled != null) return pulled;
        }
        throw new TimeoutException($"timed out waiting for a message in {stepQueue}.dlq");
    }

    [Fact]
    public async Task PutsADeclaredSlipOnEveryPipelineMessage()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        using var pipeline = await mq.Pipeline<string>(_q)
            .Step<string>("validate", Task.FromResult<string?>)
            .Step<string>("ship", _ => throw new AceFatalException("stopped here on purpose"))
            .BuildAsync();
        await mq.DeclareQueueAsync(pipeline.QueueFor("ship") + ".dlq");

        await pipeline.SendAsync("an order");

        // Asserted at the second step, so what is being read is the slip the first
        // step published rather than the one the entrance wrote.
        var stuck = await StuckAt(mq, pipeline.QueueFor("ship"));
        await stuck.AcknowledgeAsync();

        Assert.Equal(SlipForm.Declared, pipeline.SlipForm);
        var slip = (RoutingSlip)Route.From(stuck.WireHeaders)!;
        Assert.Equal(new[] { "validate", "ship" }, slip.Steps);
        Assert.Equal(1, slip.Position);
        Assert.Equal("ship", slip.Current);

        // Comma-joined step names, which is exactly what Java writes and reads.
        Assert.Equal("validate,ship", stuck.WireHeaders[RoutingSlip.RouteHeader]);
    }

    [Fact]
    public async Task PutsAnItineraryOnEveryMessageWhenAskedTo()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);

        using var pipeline = await mq.Pipeline<string>(_q)
            .WritingSlipAs(SlipForm.Itinerary)
            .Step<string>("validate", Task.FromResult<string?>)
            .Step<string>("ship", _ => throw new AceFatalException("stopped here on purpose"))
            .BuildAsync();
        await mq.DeclareQueueAsync(pipeline.QueueFor("ship") + ".dlq");

        await pipeline.SendAsync("an order");
        var stuck = await StuckAt(mq, pipeline.QueueFor("ship"));
        await stuck.AcknowledgeAsync();

        // The queue rather than the bare step name, because an itinerary is followed
        // by something that has never heard of this pipeline and cannot resolve
        // `ship` into `<name>.ship`.
        var slip = (Itinerary)Route.From(stuck.WireHeaders)!;
        Assert.Equal(pipeline.QueueFor("ship"), slip.Next!.RoutingKey);
        Assert.Equal("ship", slip.Next.Name);
        Assert.Single(slip.Done);
        Assert.Equal("validate", slip.Done[0].Name);
    }

    [Fact]
    public async Task ResumesAReplayedMessageInsteadOfStartingItAgain()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var ran = new ConcurrentQueue<string>();
        var shipped = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var chargeFails = true;

        using var pipeline = await mq.Pipeline<string>(_q)
            .Step<string>("validate", order =>
            {
                ran.Enqueue("validate");
                return Task.FromResult<string?>(order + "|validated");
            })
            .Step<string>("charge", order =>
            {
                ran.Enqueue("charge");
                if (chargeFails) throw new AceFatalException("the card processor is down");
                return Task.FromResult<string?>(order + "|charged");
            })
            .Step<string>("ship", order =>
            {
                ran.Enqueue("ship");
                shipped.TrySetResult(true);
                return Task.FromResult<string?>(order);
            })
            .BuildAsync();
        await mq.DeclareQueueAsync(pipeline.QueueFor("charge") + ".dlq");

        await pipeline.SendAsync("order-1");

        // What an operator finds in the dead-letter queue: the message exactly as it
        // was when it failed, headers and all.
        var stuck = await StuckAt(mq, pipeline.QueueFor("charge"));
        await stuck.AcknowledgeAsync();

        // The slip on the failed message says where it was. Without one there is
        // nothing on the message that says, and the only safe replay is the entrance
        // -- which would run validate a second time.
        var stalled = (RoutingSlip)Route.From(stuck.WireHeaders)!;
        Assert.Equal(new[] { "validate", "charge", "ship" }, stalled.Steps);
        Assert.Equal("charge", stalled.Current);
        Assert.Equal(1, stalled.Position);

        chargeFails = false;
        Assert.Equal("order-1|validated", stuck.Payload);

        await pipeline.ResumeAsync(stuck.Payload, stuck.WireHeaders);
        await shipped.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // validate ran once, not twice. A restart would have re-run every step before
        // the failure -- which a step that charges a card cannot survive. Resuming
        // runs the step that failed and everything after it, and nothing else.
        Assert.Equal(new[] { "validate", "charge", "charge", "ship" }, ran.ToArray());
    }

    [Fact]
    public async Task ResumesFromAnItineraryToo()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var ran = new ConcurrentQueue<string>();
        var shipped = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var chargeFails = true;

        using var pipeline = await mq.Pipeline<string>(_q)
            .WritingSlipAs(SlipForm.Itinerary)
            .Step<string>("validate", o => { ran.Enqueue("validate"); return Task.FromResult<string?>(o); })
            .Step<string>("charge", o =>
            {
                ran.Enqueue("charge");
                if (chargeFails) throw new AceFatalException("the card processor is down");
                return Task.FromResult<string?>(o);
            })
            .Step<string>("ship", o =>
            {
                ran.Enqueue("ship");
                shipped.TrySetResult(true);
                return Task.FromResult<string?>(o);
            })
            .BuildAsync();
        await mq.DeclareQueueAsync(pipeline.QueueFor("charge") + ".dlq");

        await pipeline.SendAsync("order-1");
        var stuck = await StuckAt(mq, pipeline.QueueFor("charge"));
        await stuck.AcknowledgeAsync();

        var stalled = (Itinerary)Route.From(stuck.WireHeaders)!;
        Assert.Equal("charge", stalled.Next!.Name);
        Assert.Single(stalled.Done);
        Assert.Equal("validate", stalled.Done[0].Name);

        chargeFails = false;
        await pipeline.ResumeAsync(stuck.Payload, stuck.WireHeaders);
        await shipped.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(new[] { "validate", "charge", "charge", "ship" }, ran.ToArray());
    }

    [Fact]
    public async Task RefusesToResumeAMessageThatSaysNothingAboutWhereItWas()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var pipeline = await mq.Pipeline<string>(_q)
            .Step<string>("only", Task.FromResult<string?>)
            .BuildAsync();

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => pipeline.ResumeAsync("payload", new Dictionary<string, object>()));
        Assert.Contains("carries no routing slip", error.Message);
    }

    [Fact]
    public async Task FollowsASlipThatSkipsAStepTheMessageDoesNotNeed()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        var ran = new ConcurrentQueue<string>();
        var shipped = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var pipeline = await mq.Pipeline<string>(_q)
            .Step<string>("validate", o => { ran.Enqueue("validate"); return Task.FromResult<string?>(o); })
            .Step<string>("fraud", o => { ran.Enqueue("fraud"); return Task.FromResult<string?>(o); })
            .Step<string>("ship", o =>
            {
                ran.Enqueue("ship");
                shipped.TrySetResult(true);
                return Task.FromResult<string?>(o);
            })
            .BuildAsync();

        // A route that leaves out the fraud check, on the message rather than in the
        // declaration. The pipeline is declared with three steps and this message
        // visits two of them, which is the thing a slip can do that a positional
        // pipeline cannot.
        var publisher = mq.Publisher<string>(string.Empty, pipeline.QueueFor("validate"));
        await ((Publisher<string>)publisher).SendWithHeadersAsync(
            "small order", Envelope.Of("order").Build(),
            RoutingSlip.StartOf("validate", "ship").ToHeaders(), CancellationToken.None);

        await shipped.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new[] { "validate", "ship" }, ran.ToArray());
    }
}
