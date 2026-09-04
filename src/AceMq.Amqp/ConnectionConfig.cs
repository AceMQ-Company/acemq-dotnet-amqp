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

namespace AceMq.Amqp;

/// <summary>How to reach a broker, and how to behave once connected.</summary>
/// <remarks>
/// The defaults match the Java library's, because a service rewritten from Java to
/// C# should not change its timeout behaviour as a side effect of changing
/// language.
/// </remarks>
public sealed class ConnectionConfig
{
    private ConnectionConfig(Builder b)
    {
        Url = b.UrlValue;
        Username = b.UsernameValue;
        Password = b.PasswordValue;
        VirtualHost = b.VirtualHostValue;
        ClientName = b.ClientNameValue;
        ConnectionTimeout = b.ConnectionTimeoutValue;
        ConfirmTimeout = b.ConfirmTimeoutValue;
        MaxOutstandingPublishes = b.MaxOutstandingPublishesValue;
        PublisherConfirms = b.PublisherConfirmsValue;
    }

    /// <summary>Broker URL. Its scheme selects the transport.</summary>
    public string Url { get; }

    public string? Username { get; }
    public string? Password { get; }
    public string? VirtualHost { get; }

    /// <summary>Name this connection reports to the broker, so it is identifiable in the UI.</summary>
    public string ClientName { get; }

    public TimeSpan ConnectionTimeout { get; }

    /// <summary>How long to wait for the broker to confirm a publish before failing it.</summary>
    public TimeSpan ConfirmTimeout { get; }

    /// <summary>Publishes allowed in flight before <c>SendAsync</c> applies back pressure.</summary>
    public int MaxOutstandingPublishes { get; }

    /// <summary>Whether the broker is asked to confirm publishes. On by default.</summary>
    public bool PublisherConfirms { get; }

    /// <summary>The URL's scheme, which is how a transport is chosen.</summary>
    public string Scheme
    {
        get
        {
            var i = Url.IndexOf("://", StringComparison.Ordinal);
            if (i <= 0) throw new AceFatalException($"'{Url}' is not a URL: it has no scheme");
            return Url.Substring(0, i).ToLowerInvariant();
        }
    }

    /// <summary>Starts a configuration for a broker URL.</summary>
    public static Builder ForUrl(string url) => new Builder(url);

    public override string ToString() => $"ConnectionConfig[{Redacted()}, client={ClientName}]";

    /// <summary>The URL with any password in it replaced, for logging.</summary>
    private string Redacted()
    {
        var at = Url.LastIndexOf('@');
        var scheme = Url.IndexOf("://", StringComparison.Ordinal);
        if (at < 0 || scheme < 0 || at < scheme) return Url;
        return Url.Substring(0, scheme + 3) + "***@" + Url.Substring(at + 1);
    }

    /// <summary>Builds a <see cref="ConnectionConfig"/>.</summary>
    public sealed class Builder
    {
        internal string UrlValue;
        internal string? UsernameValue;
        internal string? PasswordValue;
        internal string? VirtualHostValue;
        internal string ClientNameValue = "acemq-dotnet";
        internal TimeSpan ConnectionTimeoutValue = TimeSpan.FromSeconds(30);
        internal TimeSpan ConfirmTimeoutValue = TimeSpan.FromSeconds(30);
        internal int MaxOutstandingPublishesValue = 1000;
        internal bool PublisherConfirmsValue = true;

        internal Builder(string url) =>
            UrlValue = url ?? throw new ArgumentNullException(nameof(url));

        public Builder Url(string url) { UrlValue = url; return this; }

        public Builder Credentials(string? username, string? password)
        {
            UsernameValue = username;
            PasswordValue = password;
            return this;
        }

        public Builder VirtualHost(string? virtualHost) { VirtualHostValue = virtualHost; return this; }
        public Builder ClientName(string clientName) { ClientNameValue = clientName; return this; }
        public Builder ConnectionTimeout(TimeSpan timeout) { ConnectionTimeoutValue = timeout; return this; }
        public Builder ConfirmTimeout(TimeSpan timeout) { ConfirmTimeoutValue = timeout; return this; }

        public Builder MaxOutstandingPublishes(int max)
        {
            if (max < 1) throw new ArgumentException("must be at least 1", nameof(max));
            MaxOutstandingPublishesValue = max;
            return this;
        }

        /// <summary>
        /// Turns publisher confirms off.
        /// </summary>
        /// <remarks>
        /// Without confirms a publish reports success as soon as the bytes are
        /// written, which is not the same as the broker having accepted them. It is
        /// faster and it loses messages on a broker restart. Left on by default for
        /// that reason.
        /// </remarks>
        public Builder WithoutPublisherConfirms() { PublisherConfirmsValue = false; return this; }

        public ConnectionConfig Build() => new ConnectionConfig(this);
    }
}
