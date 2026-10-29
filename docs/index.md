---
layout: home
title: Background jobs for Go
description: Background jobs for Go, kept in PostgreSQL, MySQL, SQL Server or SQLite. Retries, workflows, limits, recurring jobs and a dashboard, in the database you already have.

hero:
  name: kiln
  text: Background jobs for Go.
  tagline: Kept in PostgreSQL, MySQL, SQL Server or SQLite. Retries, workflows, limits, recurring jobs and a dashboard, in the database you already have.
  image:
    src: /icon.svg
    alt: kiln
  actions:
    - theme: brand
      text: Get started
      link: /getting-started
    - theme: alt
      text: How it works
      link: /how-it-works
    - theme: alt
      text: GitHub
      link: https://github.com/rafaelaugustos/kiln

features:
  - title: Jobs in your database
    details: Jobs are rows next to your data. They survive restarts and crashes, and can be enqueued in the same transaction as the data that produced them.
    link: /concepts/enqueueing
    linkText: Enqueueing
  - title: Retries and workflows
    details: Exponential or custom backoff, snoozes and timeouts. Jobs that wait for other jobs and read their outputs, and batches with a job that runs when they finish.
    link: /concepts/workflows
    linkText: Workflows and batches
  - title: A dashboard included
    details: Watch, retry and cancel jobs from a dashboard in English or Brazilian Portuguese, mounted on your own HTTP server, with a JSON API.
    link: /dashboard
    linkText: The dashboard
---

<div class="home-body">

<p class="home-shot">
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

v1.0.0 is the first stable release. [Compatibility](compatibility.md) describes what stays stable
throughout v1: the public API, the store interface and the dashboard's JSON API. Schema changes stay
additive, and rolling upgrades are covered by the compatibility tests. Every release is listed in the
[changelog](changelog.md).

</div>
