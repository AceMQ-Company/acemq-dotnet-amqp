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

/// <summary>Base of every error this library raises.</summary>
public class AceMqException : Exception
{
    public AceMqException(string message) : base(message) { }
    public AceMqException(string message, Exception? cause) : base(message, cause) { }
}

/// <summary>
/// A failure that another attempt might survive: a dropped connection, a broker
/// that is busy, a timeout.
/// </summary>
/// <remarks>
/// The distinction from <see cref="AceFatalException"/> is the whole point of the
/// pair. Retrying a fatal error burns attempts on something that cannot improve,
/// and giving up on a retryable one discards a message that would have gone
/// through a second later.
/// </remarks>
public class AceRetryableException : AceMqException
{
    public AceRetryableException(string message) : base(message) { }
    public AceRetryableException(string message, Exception? cause) : base(message, cause) { }
}

/// <summary>A failure no number of attempts will change: bad credentials, a malformed payload.</summary>
public class AceFatalException : AceMqException
{
    public AceFatalException(string message) : base(message) { }
    public AceFatalException(string message, Exception? cause) : base(message, cause) { }
}

/// <summary>The transport could not carry out the operation.</summary>
public class TransportException : AceMqException
{
    public TransportException(string message) : base(message) { }
    public TransportException(string message, Exception? cause) : base(message, cause) { }
}

/// <summary>
/// The broker has blocked the connection, normally because it is out of memory or
/// disk.
/// </summary>
public sealed class ConnectionBlockedException : TransportException
{
    public ConnectionBlockedException(string reason)
        : base($"the broker has blocked this connection: {reason}") => Reason = reason;

    /// <summary>The broker's stated reason, verbatim.</summary>
    public string Reason { get; }
}

/// <summary>
/// A publish that was not confirmed by the broker, or that the broker could not
/// route.
/// </summary>
/// <remarks>
/// An unroutable message is a failure by default rather than a silent discard.
/// A message published to an exchange with no matching binding is almost always a
/// topology mistake, and the cheapest moment to learn about it is at the publish
/// call rather than from an absence of messages hours later. Where the discard is
/// deliberate, <see cref="PublishOptions.AllowUnroutable"/> says so explicitly.
/// </remarks>
public sealed class PublishFailedException : AceMqException
{
    public PublishFailedException(string message) : base(message) { }
    public PublishFailedException(string message, Exception? cause) : base(message, cause) { }
}
