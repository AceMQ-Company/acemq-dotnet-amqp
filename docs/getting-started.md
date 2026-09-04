# Getting started

> **Nothing to install yet.** This library cannot send a message: there is no
> transport, publisher or consumer. This page describes what building it looks like
> today, and will become real installation instructions when there is something to
> install.

## Building it

```bash
git clone https://github.com/AceMQ-Company/acemq-dotnet-amqp
cd acemq-dotnet-amqp
dotnet build
dotnet test
```

Requires the .NET SDK. The library targets `netstandard2.0`; the tests target a
modern .NET.

## When there is a package

It will come from a **static NuGet feed served over GitHub Pages**, the same
arrangement the JVM libraries use for Maven — anonymous, no credentials, no account
needed:

```bash
dotnet nuget add source https://acemq-company.github.io/nuget/index.json --name acemq
dotnet add package AceMq.Amqp
```

GitHub Packages was considered and rejected for the public feed: it requires
authentication even for public packages, which would mean every consumer needs a
token just to restore. AceMQ's Maven repository promises "no credentials needed"
and the .NET feed will keep the same promise. nuget.org is for 1.0, when the
coordinates and the API have stopped moving — publishing there is permanent.

## What works today

The envelope, and its conformance tests against fixtures generated from the Java
library:

```bash
dotnet test
# Passed!  - Failed: 0, Passed: 7
```

See [the envelope](envelope.md) for what those tests pin down, and why they are
generated rather than written.
