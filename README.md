<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/logo-dark.png">
    <img src=".github/logo.png" alt="kiln" width="200">
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
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT license"></a>
</p>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/dashboard-dark.png">
    <img src=".github/dashboard-light.png" alt="The kiln dashboard: jobs by state, throughput over the last hour, queues and servers">
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

## Contents

- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Concepts](#concepts)
- [Behaviour worth knowing](#behaviour-worth-knowing)
- [Dashboard](#dashboard)
- [How kiln compares](#how-kiln-compares)
- [Performance](#performance)
- [Documentation](#documentation)
- [Status](#status)
- [Contributing](#contributing)

## Quick start

kiln needs Go 1.27 or later.

### With SQLite, nothing to install

```
go get github.com/rafaelaugustos/kiln github.com/rafaelaugustos/kiln/sqlitestore modernc.org/sqlite
```

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/sqlitestore"
	_ "modernc.org/sqlite"
)

type SendEmail struct {
	To string
}

func (SendEmail) Kind() string { return "send_email" }

func sendEmail(ctx context.Context, j *kiln.Job[SendEmail]) error {
	log.Printf("sending email to %s (attempt %d)", j.Args.To, j.Attempt)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	db, err := sql.Open("sqlite", "file:jobs.db?_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatal(err)
	}
	store, err := sqlitestore.New(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	client := kiln.NewClient(store)

	mux := kiln.NewMux()
	kiln.Handle(mux, sendEmail)
	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := client.Enqueue(ctx, SendEmail{To: "ada@example.com"}); err != nil {
		log.Fatal(err)
	}
	if err := server.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`go run .` creates `jobs.db`, enqueues the job and runs it right away. A job type is any struct with a
`Kind()`; `kiln.Handle` registers the function that runs it, and `Job[T]` hands it the decoded args along
with `Attempt`, `Meta`, `Tags` and the rest. Ctrl+C stops the server; anything not done yet stays in the
file for the next run.

### With PostgreSQL

```
docker run -d --name kiln-postgres -p 5432:5432 -e POSTGRES_PASSWORD=kiln postgres:17
go get github.com/rafaelaugustos/kiln/pgstore
```

Swap the store and keep the rest of the program, importing `github.com/jackc/pgx/v5/pgxpool` and
`github.com/rafaelaugustos/kiln/pgstore` in place of the SQLite packages:

```go
pool, err := pgxpool.New(ctx, "postgres://postgres:kiln@localhost:5432/postgres")
if err != nil {
	log.Fatal(err)
}
store, err := pgstore.New(ctx, pool)
if err != nil {
	log.Fatal(err)
}
defer store.Close()
```

`pgstore.New` copies the pool's settings into a pool of its own, 8 connections by default
(`pgstore.MaxConns` changes it), so kiln and the application never wait for each other's connections and
the application can keep using or close its pool.

`New` creates its tables in a `kiln` schema the first time. MySQL and SQL Server work the same way
through `mysqlstore` and `mssqlstore`; [Storage backends](docs/backends.md) covers what each database
needs.

Complete, runnable programs for each topic below live in [examples/](examples/): [basic](examples/basic),
[workflow](examples/workflow), [recurring](examples/recurring), [throttling](examples/throttling),
[transactional](examples/transactional) and [dashboard](examples/dashboard).

## How it works

```mermaid
flowchart LR
    app["Your code<br>client.Enqueue"] -- insert --> db[("Your database<br>PostgreSQL, MySQL, SQL Server or SQLite")]
    db -- claim --> s1["kiln server<br>runs handlers"]
    db -- claim --> s2["kiln server<br>runs handlers"]
    s1 -- results, heartbeats --> db
    s2 -- results, heartbeats --> db
    dash["Dashboard"] -- reads, requeues --> db
```

A `Client` writes jobs to the store. Any number of `Server`s, in the same process or in others, claim the
jobs of their queues, run the handler registered for each job's kind and write the outcome back. They
learn about new work from the database's notifications (`LISTEN`/`NOTIFY` on PostgreSQL, an optional
Redis bus on MySQL, SQL Server and SQLite) and by polling. One server at a time is also the leader: it fires recurring
jobs, rescues the jobs of servers that stopped heartbeating, and prunes old jobs. There is no broker and
no extra service to run.

## Concepts

### States

```mermaid
stateDiagram-v2
    direction LR
    [*] --> enqueued
    [*] --> scheduled: runs later
    [*] --> awaiting: waits for other jobs
    [*] --> throttled: held by a limit
    awaiting --> enqueued
    scheduled --> enqueued
    throttled --> enqueued
    enqueued --> processing
    processing --> succeeded
    processing --> scheduled: retry
    processing --> failed: out of attempts
    processing --> deleted: canceled
    failed --> enqueued: requeue
```

A job starts in `awaiting` if it has unresolved dependencies, `scheduled` if it runs in the future,
`throttled` if it's waiting on a `Limit` slot, otherwise `enqueued`. As in Hangfire, `failed` is not a
final state: the job sits there until an operator requeues or deletes it.

### Enqueue options

Options go after the args, and an args type can supply its own defaults with an
`InsertOptions() []kiln.InsertOption` method, so callers don't repeat them:

```go
client.Enqueue(ctx, SendEmail{To: to},
	kiln.Queue("emails"),
	kiln.Delay(10*time.Minute),
	kiln.MaxAttempts(5),
	kiln.Tags{"welcome"},
	kiln.Title("Welcome email for "+to),
)
```

`Title` names the job in the dashboard, which otherwise shows its kind; an args type can also have a
`Title() string` method. Tags can be filtered on in the dashboard's job lists.

### Retries

Every kind gets `MaxAttempts` (default 10) and a `Backoff` that computes the delay before the next
attempt from the attempt number and the error. `kiln.Exponential`, `kiln.Constant` and
`kiln.Delays` cover the common cases; a handler can also return `kiln.Permanent(err)` to fail
without retrying, or `kiln.Snooze(d)` to reschedule itself without counting as a failure.
A failed job that you requeue gets all of its attempts again. Make the retries outlast the longest
outage of what a job calls, and alert on what fails anyway: see
[Retries and alerts](docs/operating.md).

### Continuations and flows

A job can depend on others by id (`After`, for a fixed set of ids already in the store) or by
index (`Needs`, for jobs being submitted together via `EnqueueMany`, resolved against ids kiln
assigns in the same call). `AfterFinished` runs regardless of whether the parent succeeded. A
dependency on a job that can never complete cascades: the dependent is inserted straight into
`deleted`.

```go
var flow kiln.Flow
fetch := flow.Add(FetchData{URL: src})
flow.Add(ProcessData{}, kiln.Needs{fetch})
client.EnqueueMany(ctx, flow...)
```

A continuation can read what its parents produced: `j.ParentOutputs(ctx)` returns the output of each parent
that succeeded, keyed by id.

### Batches

A `Batch` groups jobs and, optionally, a continuation (`Then`) that runs once every job in the
batch has finished:

```go
b := &kiln.Batch{Description: "nightly-import"}
b.Add(ImportFile{Name: "a.csv"})
b.Add(ImportFile{Name: "b.csv"})
b.Then(SendSummary{})
client.StartBatch(ctx, b)
```

Batches nest. An outer batch finishes once its own jobs are done and every batch nested in it has
finished, and a nested batch's continuations count as part of the outer one:

```go
month := &kiln.Batch{Description: "monthly close"}
for _, acct := range accounts {
	b := &kiln.Batch{Description: acct.Name}
	b.Add(CloseAccount{ID: acct.ID})
	b.Then(EmailStatement{ID: acct.ID})
	month.AddBatch(b)
}
month.Then(SendReport{})
client.StartBatch(ctx, month)
```

A running job can also open a batch inside its own with `kiln.Batch{Parent: j.BatchID}`.

### Recurring

`SetRecurring` schedules a job on a cron spec (standard 5/6-field syntax plus `L`, `W`, `#`,
`@every`, ...), evaluated in a given `TZ`, UTC when omitted: `0 3 * * *` without
`kiln.TZ("America/Sao_Paulo")` fires at midnight in São Paulo. `Misfire` controls what happens to occurrences missed
while no server was running: `MisfireOnce` (default, catch up once), `MisfireAll` (run every
missed occurrence, capped), or `MisfireSkip` (drop stale ones). Occurrences overlap by default: if a
run is still going when the next one is due, the next one is enqueued anyway. Pass `kiln.Overlap(false)`
for jobs that must not run twice at the same time, and the occurrence is skipped instead.

`SyncRecurring` declares a group of recurring jobs at once: it sets the ones it's given and removes the
ones of that group that are no longer there, so deleting a schedule from your code deletes it from the
store on the next deploy. Jobs set with `SetRecurring` belong to no group and are left alone.

```go
client.SyncRecurring(ctx, "reports",
	kiln.RecurringSpec{ID: "daily-sales", Spec: "0 7 * * *", Args: SalesReport{}, Options: []kiln.RecurringOption{kiln.TZ("America/Sao_Paulo")}},
	kiln.RecurringSpec{ID: "weekly-stock", Spec: "@weekly", Args: StockReport{}},
)
```

### Unique jobs

`Unique{Key}` makes `Enqueue` return the id of a live job with the same kind and key instead of
inserting another one. The key is released as soon as that job succeeds, fails or is deleted.

`Unique{Key, For: d}` holds the key for `d` from the first enqueue, whatever happens to the job in the
meantime, including success. It means "at most once per `d`" (one reminder per 10 minutes), not "no
duplicates while it runs".

`Unique{Key, Replace: true}` updates a holder that hasn't started yet with the new args, meta, tags, title
and priority, so the job runs with the latest data. `Unique{Key, Debounce: d}` runs the job `d` after the
last enqueue: every new enqueue pushes a holder that is still waiting and replaces its args, which suits
work like "reindex once the user stops editing". A holder that is already running is never touched.

### Queues and workers

A server runs pools of workers, each serving its queues in order, so a busy first queue can keep the others
waiting. `Weights` shares a pool between its queues instead: under load each queue gets about its weight's
share of the claims, and a queue with nothing to do leaves its share to the others.

```go
kiln.ServerConfig{Pools: []kiln.Pool{{
	Queues:  []string{"critical", "default", "low"},
	Workers: 20,
	Weights: map[string]int{"critical": 6, "default": 3},
}}}
```

### Limits

`Limit{Key, Max}` caps how many jobs sharing a key can be `processing` at once, across the whole
cluster, independent of queue or worker pool. A job over the cap sits in `throttled` until a slot
frees up. This is kiln's equivalent of Hangfire's `DisableConcurrentExecution` and Hangfire Ace's
semaphores, without needing a separate package.

`Rate` and `Per` cap how many jobs of a key may start per period, and `Burst` how many may start back
to back (it defaults to `Rate`). When a job is admitted kiln reserves its start time, so a backlog of
10,000 jobs against a 100/s limit is released at that pace, each job written once, instead of being
retried until it fits:

```go
client.Enqueue(ctx, ChargeCard{OrderID: id}, kiln.Limit{Key: "stripe", Rate: 100, Per: time.Second})
```

`Max` and `Rate` can be combined on one key: `Max` bounds how many run at once, `Rate` how often
they start.

The rate also holds when kiln falls behind. If admission stalls for a while, because the database was
slow or a lock was held, the jobs whose start times passed in the meantime don't all start when it
resumes: `Burst` of them start and the rest get new start times at the back of the line. No window of
length `Per` sees more than `Rate + Burst` starts of a key.

### Transactional enqueue

`EnqueueTx` and `EnqueueManyTx` take a `driver.Writer` bound to your own transaction, so a job is
inserted atomically with the business data that produced it:

```go
w := store.Tx(tx)
client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
tx.Commit(ctx)
w.Notify(ctx)
```

Call `Notify` after the commit, never before. The job is already committed by then, so an error from
`Notify` changes nothing about it: log it and carry on, don't turn it into a failed request, or a client
that retries will create the order twice. Without `Notify` the job still runs, after the next poll.

Every store's transactional writer is a `driver.TxWriter` (`Notify` included, a no-op on `memstore.Tx`),
so code that runs on more than one store can hold one instead of switching on types. On PostgreSQL
through `database/sql` (sqlx, bun, GORM with pgx's stdlib driver), `store.SQLTx(tx)` takes a `*sql.Tx`.

### Cancellation

`client.Delete` on a job that is currently `processing` cancels that job's `context.Context` on
whichever server is running it, with `kiln.ErrCanceled` as the cause (`context.Cause(ctx)`). A
handler that stops and returns an error is recorded as `deleted`.

### Middleware

`Mux.Use` wraps every handler (logging, panics-to-errors, tracing); `NewClient`'s variadic
`EnqueueMiddleware` wraps every insert the same way (e.g. to stamp tenant metadata).

### Observability

`kilnotel` traces every job from the request that enqueued it to the handler that ran it, and records
job counts, durations and the delay between a job's scheduled time and its start:

```go
client := kiln.NewClient(store, kilnotel.EnqueueMiddleware())
mux.Use(kilnotel.Middleware())
unregister, err := kilnotel.Observe(server, store)
```

It depends only on the OpenTelemetry API, so it reports through whatever SDK and exporters the
application already has. `Server.Stats()` and `Server.Healthy()` cover the same ground without it.

### Console

A handler can write log lines and a progress bar that show up on the job's page in the dashboard while it
runs, like Hangfire.Console:

```go
func importRows(ctx context.Context, j *kiln.Job[Import]) error {
	for i, row := range j.Args.Rows {
		j.Logf("importing %s", row.ID)
		j.SetProgress(100 * i / len(j.Args.Rows))
	}
	return nil
}
```

Neither call takes a context or returns an error. kiln buffers them and writes them in the background,
and once more after the handler returns, so the console is complete when the job finishes. An attempt
keeps up to 1000 lines, each line up to 4 KiB; lines stay with the job, grouped by attempt, until it is
pruned.

### Testing with kilntest

`kilntest.Work` drives a job through the real frozen middleware chain and classification logic
without a server, for handler unit tests; its result includes the console lines and the progress. `RequireEnqueued`/`RequireNotEnqueued` assert on what a
piece of code actually enqueued. `drivertest.Run` is the conformance suite a `driver.Store`
implementation must pass.

## Behaviour worth knowing

- **Failed is not final.** A job that exhausts its attempts or returns `Permanent` stays `failed` until
  someone requeues or deletes it, as in Hangfire. Continuations that wait for its success (`After`,
  `Needs`) and batches that contain it wait too. Deleting the failed job deletes those continuations;
  `AfterFinished` continuations run either way.
- **Requeue starts over.** Requeueing a `failed`, `succeeded` or `deleted` job resets its attempt count,
  so it gets all of `MaxAttempts` again and its retries back off from the first delay, as if it had just
  been enqueued. Requeueing a `scheduled` job only runs it now; it keeps the attempts it has used.
- **At least once.** A job can run more than once: a worker can crash after the handler's side effect
  and before its result is stored, and a stalled worker's jobs are retried elsewhere. Make handlers
  idempotent; `SetParam` keeps checkpoints and idempotency keys across attempts.
- **History.** A job's history records transitions that have a reason (retries, snoozes, cancellations,
  requeues, rescues). A plain success adds no entry; its timestamps are on the job.
- **Dead workers.** Jobs of a server that stops heartbeating are retried once it has been silent for
  `DeadAfter`. See [Operating kiln](docs/operating.md) for the timings.

## Dashboard

```go
mux.Handle("/kiln/", dashboard.New(client, dashboard.Options{
	Prefix:    "/kiln",
	Authorize: func(r *http.Request) dashboard.Access { return dashboard.ReadOnly },
}))
```

`Authorize` runs on every request and returns `Denied`, `ReadOnly` or `ReadWrite`, so access
control plugs into whatever auth your app already has. `dashboard.AllowAll` is for local
development: it grants read/write access only to requests addressed to `localhost` or a loopback
IP, and denies everything else. The dashboard shows live counts and a succeeded/failed chart, jobs by
state with filters by queue, kind, batch and tag and bulk actions, job detail with titles, redactable
args/meta/output and the live console,
retries, recurring schedules and their groups, queues, servers, batches, and limits with what each key
is running and holding back, and mirrors all of it under a JSON API at
`<prefix>/api/...` for scripting. It's server-rendered with no external assets and a strict CSP.

The dashboard speaks English and Brazilian Portuguese. A picker in the header remembers each person's
choice; `Options.Language` sets the default, and without it the browser's language decides. Another
language is one JSON file in `dashboard/locales`.

## How kiln compares

kiln takes its model from Hangfire. In Go, the libraries people usually weigh it against are
[River](https://github.com/riverqueue/river) and [Asynq](https://github.com/hibiken/asynq), both mature and
with larger communities. This table only compares features, as each project documented them in
October 2026:

| | kiln | Hangfire | River | Asynq |
|---|---|---|---|---|
| Storage | PostgreSQL, MySQL, SQL Server, SQLite | SQL Server; Redis with Pro; others from the community | PostgreSQL; SQLite (experimental) | Redis |
| License | MIT | LGPL-3.0, paid Pro and Ace | MPL-2.0, paid Pro | MIT |
| Retries with backoff | yes | yes | yes | yes |
| Job waits for other jobs | yes | yes, several parents with Pro | Pro (workflows) | no |
| Batches with a continuation | yes | Pro | Pro, as a workflow | no |
| Concurrency limit per key | yes | a mutex; Ace for more | Pro | in the handler, with `x/rate` |
| Rate limit per key | yes | Ace | no | no |
| Unique jobs | yes | third party | yes | yes |
| Cron with time zones | yes, with a misfire policy | yes | yes; durable schedule with Pro | yes |
| Transactional enqueue | yes | yes | yes | no |
| Cancel a running job | yes | yes | yes | yes, best effort |
| Pause a queue | yes | third party | yes | yes |
| Dashboard | in the module | in the package | separate app (River UI) | separate app (Asynqmon) |
| OpenTelemetry | `kilnotel` | contrib package | `rivercontrib` | Prometheus metrics |

Moving a Hangfire application over? [Coming from Hangfire](docs/hangfire.md) maps its calls and attributes
to kiln's, one by one.

## Performance

On an Apple M4 Max, with PostgreSQL and MySQL in Docker on the same machine:

| | PostgreSQL | MySQL 8.4 | SQLite |
|---|---|---|---|
| Bulk insert | ~250k jobs/s | ~77k jobs/s | ~300k jobs/s |
| One server, 100 workers, no-op handler | ~21k jobs/s | ~6k jobs/s | ~70k jobs/s |
| Enqueue to handler start | p50 3.3ms | ~5–20ms with `redisbus` | p50 0.16ms in the same process |

Against [River](https://github.com/riverqueue/river) on the same PostgreSQL, kiln drains no-op jobs about
as fast as River tuned to a 1ms fetch cooldown, and 20 times faster than River's defaults.
[Performance](docs/performance.md) has the full numbers and how they were measured, along with a run under a
production-like load and a 24 hour chaos run: 2.2 million jobs through 2,886 `kill -9`s, 2,139 graceful
stops and 360 database restarts, with no job lost, none run more often than allowed, and every limit
held.

## Documentation

- [API reference](https://pkg.go.dev/github.com/rafaelaugustos/kiln) on pkg.go.dev
- [Storage backends](docs/backends.md): what each database needs, wakeups and the Redis bus
- [Operating kiln](docs/operating.md): timings, dead workers, connections, polling and health checks
- [Upgrading](docs/upgrading.md): rolling deploys and what each release changes in the schema
- [Compatibility](docs/compatibility.md): what stays stable across releases, and the Go versions kiln needs
- [Coming from Hangfire](docs/hangfire.md): Hangfire calls and their kiln equivalents
- [Performance](docs/performance.md): benchmarks, and the comparison with River
- [Changelog](CHANGELOG.md)

## Status

kiln runs in production at [Sodexo](https://www.sodexo.com), [Zeep Labs](https://github.com/zeeplabs) and
[Starbem](https://github.com/Starbem).
If your team uses it too, open a pull request to add yourself here.

kiln is young, and its API can still change between minor versions before 1.0. Every change is listed in
the [changelog](CHANGELOG.md), and each release is tested against the previous one running on the same
database, so a rolling upgrade from one version to the next keeps working.

## Contributing

Bug reports, questions and pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) explains how to
run the tests against each database; security issues go through [SECURITY.md](SECURITY.md).

## License

MIT, see [LICENSE](LICENSE).
