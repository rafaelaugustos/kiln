---
hide:
  - toc
---

# kiln { style="display: none" }

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github/logo-dark.png">
    <img src="https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github/logo.png" alt="kiln" width="200">
  </picture>
</p>

<p align="center">
  Background jobs for Go, kept in PostgreSQL, MySQL, SQL Server or SQLite.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/rafaelaugustos/kiln"><img src="https://pkg.go.dev/badge/github.com/rafaelaugustos/kiln.svg" alt="Go Reference"></a>
  <a href="https://github.com/rafaelaugustos/kiln/actions/workflows/ci.yml"><img src="https://github.com/rafaelaugustos/kiln/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/rafaelaugustos/kiln"><img src="https://codecov.io/gh/rafaelaugustos/kiln/graph/badge.svg" alt="Coverage"></a>
  <a href="https://github.com/rafaelaugustos/kiln/releases"><img src="https://img.shields.io/github/v/release/rafaelaugustos/kiln" alt="Latest release"></a>
  <a href="https://github.com/rafaelaugustos/kiln/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT license"></a>
</p>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github/dashboard-dark.png">
    <img src="https://raw.githubusercontent.com/rafaelaugustos/kiln/main/.github/dashboard-light.png" alt="The kiln dashboard: jobs by state, throughput over the last hour, queues and servers">
  </picture>
</p>

kiln runs background jobs for Go programs, the way Hangfire does for .NET. Jobs are rows in the database
you already have, so they survive restarts and crashes, they can be enqueued in the same transaction as the
data that produced them, and you can watch and retry them from a dashboard that comes with the library.

- **Retries** with exponential or custom backoff, snoozes, timeouts and permanent failures
- **Workflows**: jobs that wait for one or many other jobs and read their outputs, and batches, nested if
  needed, with a job that runs when the whole batch is done
- **Limits** per key, across every server: how many jobs run at once and how many start per second
- **Recurring jobs** from cron specs, with time zones, a policy for missed runs, and `SyncRecurring` to
  keep them in step with your code
- **Job console**: log lines and a progress bar from inside a handler, live in the dashboard
- **Unique jobs**, while a job is live or for a window of time, with replace and debounce
- **Transactional enqueue**: the job exists only if your transaction commits
- **Cancellation** of a running job from any process
- **Dashboard** in English or Brazilian Portuguese, and a JSON API, mounted on your own HTTP server
- **OpenTelemetry** traces from the request that enqueued a job to the handler that ran it
- **PostgreSQL, MySQL, SQL Server and SQLite**, plus an in-memory store for tests, all held to one
  conformance suite

Everything above is in this repository, under the MIT license. There is no paid edition.

## Where to start

- [Getting started](getting-started.md): a working program in five minutes, on SQLite or PostgreSQL
- [How it works](how-it-works.md): what a job is, and how servers share the work
- [Concepts](concepts/states.md): states, enqueueing, retries, workflows, limits, recurring jobs and more
- [Storage backends](backends.md): what each database needs, and how to write your own store
- [Coming from Hangfire](hangfire.md): Hangfire calls and their kiln equivalents

## Status

kiln runs in production at [Sodexo](https://www.sodexo.com), [Zeep Labs](https://github.com/zeeplabs),
[Starbem](https://github.com/Starbem) and in [Orbit](https://getcortexlabs.com/products/orbit/), by Cortex Labs.
If your team uses it too, open a pull request to add yourself here.

The API is final as of v1.0.0-rc.1. v1.0.0 follows once the release candidate has run in production for
a couple of weeks without needing an API change, and from then on [Compatibility](compatibility.md) describes
what stays stable. Every change is listed in the [changelog](changelog.md), and each release is tested against
the previous one running on the same database, so a rolling upgrade from one version to the next keeps
working.
