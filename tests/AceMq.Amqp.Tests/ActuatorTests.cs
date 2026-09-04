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

using System.Net;
using AceMq.Amqp;
using AceMq.Amqp.Diagnostics;

namespace AceMq.Amqp.Tests;

/// <summary>
/// The actuator, scraped over real HTTP.
/// </summary>
/// <remarks>
/// Over a socket rather than by calling Render() directly, because the thing being
/// promised is that Prometheus can scrape it — which depends on the status code, the
/// content type and the exposition format, none of which a direct call exercises.
/// </remarks>
public sealed class ActuatorTests : IDisposable
{
    private readonly string _url = "memory://" + Guid.NewGuid().ToString("N");
    private readonly string _q = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);
    private readonly int _port = 9500 + Random.Shared.Next(0, 400);
    private readonly HttpClient _http = new HttpClient();

    public void Dispose() => _http.Dispose();

    [Fact]
    public async Task ServesMetricsPrometheusCanScrape()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var actuator = AceMqActuator.Start(mq, new ActuatorOptions { Port = _port });

        await mq.DeclareQueueAsync(_q);
        await mq.Publisher<string>("", _q).SendAsync("hello");

        var response = await _http.GetAsync($"http://localhost:{_port}/acemq-metrics");
        var body = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        // Prometheus checks this, version included.
        Assert.Equal("text/plain", response.Content.Headers.ContentType!.MediaType);
        Assert.Contains("version=0.0.4", response.Content.Headers.ContentType.ToString());

        // Dots become underscores and a duration in seconds gains the suffix
        // Prometheus conventions require.
        Assert.Contains("# TYPE acemq_publish_total counter", body);
        Assert.Contains("# TYPE acemq_publish_duration_seconds histogram", body);
        Assert.Contains($"routing_key=\"{_q}\"", body);
        Assert.Contains("acemq_publish_duration_seconds_bucket", body);
        Assert.Contains("le=\"+Inf\"", body);
    }

    [Fact]
    public async Task BucketsAttemptsAsCountsRatherThanSeconds()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var actuator = AceMqActuator.Start(mq, new ActuatorOptions { Port = _port + 1 });

        await mq.DeclareQueueAsync(_q);
        using var consumer = await mq.ConsumeAsync<string>(_q, _ => Task.FromResult(Ack.Accept()));
        await mq.Publisher<string>("", _q).SendAsync("hello");

        var deadline = DateTime.UtcNow.AddSeconds(5);
        string body;
        do
        {
            body = await _http.GetStringAsync($"http://localhost:{_port + 1}/acemq-metrics");
            if (body.Contains("acemq_consume_attempts_bucket")) break;
            await Task.Delay(20);
        } while (DateTime.UtcNow < deadline);

        // Latency buckets on an attempt counter put every value in the top bucket:
        // the histogram looks populated and answers nothing.
        Assert.Contains("acemq_consume_attempts_bucket", body);
        Assert.Contains("le=\"1\"", body);
        Assert.DoesNotContain("acemq_consume_attempts_bucket{le=\"0.0005\"", body);
    }

    [Fact]
    public async Task ReportsHealthAsAStatusCodeAsWellAsABody()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var actuator = AceMqActuator.Start(mq, new ActuatorOptions { Port = _port + 2 });

        var response = await _http.GetAsync($"http://localhost:{_port + 2}/acemq-health");
        var body = await response.Content.ReadAsStringAsync();

        // A probe should not have to parse the body to know the answer.
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("\"status\":\"UP\"", body);
        Assert.Contains("\"transport\":\"in-memory\"", body);
    }

    [Fact]
    public async Task ReportsTheLibraryVersionAndWhatTheBrokerCanDo()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var actuator = AceMqActuator.Start(mq, new ActuatorOptions { Port = _port + 3 });

        var body = await _http.GetStringAsync($"http://localhost:{_port + 3}/acemq-info");

        Assert.Contains("\"library\":\"AceMq.Amqp\"", body);
        Assert.Contains("PublisherConfirms", body);
    }

    [Fact]
    public async Task ServesTheNamespacedPathsByDefaultAndSaysSoOnAnythingElse()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var actuator = AceMqActuator.Start(mq, new ActuatorOptions { Port = _port + 4 });

        var options = new ActuatorOptions();
        Assert.Equal("/acemq-metrics", options.MetricsPath);
        Assert.Equal("/acemq-health", options.HealthPath);
        Assert.Equal("/acemq-info", options.InfoPath);

        // Namespaced so it cannot collide with an application's own /metrics.
        var plain = await _http.GetAsync($"http://localhost:{_port + 4}/metrics");
        Assert.Equal(HttpStatusCode.NotFound, plain.StatusCode);
        Assert.Contains("/acemq-metrics", await plain.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task LetsThePathsBeChanged()
    {
        using var mq = await AceMqConnection.ConnectAsync(_url);
        using var actuator = AceMqActuator.Start(mq, new ActuatorOptions
        {
            Port = _port + 5,
            MetricsPath = "/metrics",
            HealthPath = "/healthz",
        });

        Assert.Equal(HttpStatusCode.OK,
            (await _http.GetAsync($"http://localhost:{_port + 5}/metrics")).StatusCode);
        Assert.Equal(HttpStatusCode.OK,
            (await _http.GetAsync($"http://localhost:{_port + 5}/healthz")).StatusCode);
        // The default is gone once overridden, so a scrape config pointed at it
        // reports the target down rather than silently reading nothing.
        Assert.Equal(HttpStatusCode.NotFound,
            (await _http.GetAsync($"http://localhost:{_port + 5}/acemq-metrics")).StatusCode);
    }
}
