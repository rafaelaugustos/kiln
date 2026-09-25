<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/logo-dark.png">
    <img src=".github/logo.png" alt="kiln" width="200">
  </picture>
</p>

kiln is a background job library for Go, modelled on Hangfire for .NET. Jobs are rows in a
database rather than an in-memory queue, so they survive restarts and crashes.

## Install

```
go get github.com/rafaelaugustos/kiln
go get github.com/rafaelaugustos/kiln/pgstore
go get github.com/rafaelaugustos/kiln/mysqlstore
go get github.com/rafaelaugustos/kiln/sqlitestore
```

Requires Go 1.27. `kiln` itself has no external dependencies; each store module pulls in only what its
database needs (`pgstore` uses `pgx/v5`, the others take a `*sql.DB` from the driver you already use).

## Quick start

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/pgstore"
)

type SendEmail struct{ To string }

func (SendEmail) Kind() string { return "send_email" }

func sendEmail(ctx context.Context, j *kiln.Job[SendEmail]) error {
	log.Printf("sending email to %s", j.Args.To)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, os.Getenv("KILN_DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	store, err := pgstore.New(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	client := kiln.NewClient(store)

	mux := kiln.NewMux()
	kiln.Handle(mux, sendEmail)

	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{
		Queues: map[string]int{kiln.DefaultQueue: 10},
	})
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

`Handle` registers a typed handler for one `Args` kind; `Job[T]` gives you `Args`, `Attempt`,
`Meta`, `Tags` and the rest without a type assertion. `Args` implementations can supply their own
`InsertOptions() []InsertOption` (queue, retry policy, ...) so callers don't repeat them at every
`Enqueue` site.

Runnable, complete versions of this and the examples below live under `examples/`.

## Concepts

### States

`awaiting → scheduled → throttled → enqueued → processing → succeeded | failed | deleted`. A job
starts in `awaiting` if it has unresolved dependencies, `scheduled` if it runs in the future,
`throttled` if it's waiting on a `Limit` slot, otherwise `enqueued`. As in Hangfire, `failed` is
not a final state: the job sits there until an operator requeues or deletes it.

### Retries

Every kind gets `MaxAttempts` (default 10) and a `Backoff` that computes the delay before the next
attempt from the attempt number and the error. `kiln.Exponential`, `kiln.Constant` and
`kiln.Delays` cover the common cases; a handler can also return `kiln.Permanent(err)` to fail
without retrying, or `kiln.Snooze(d)` to reschedule itself without counting as a failure.

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

### Recurring

`SetRecurring` schedules a job on a cron spec (standard 5/6-field syntax plus `L`, `W`, `#`,
`@every`, ...), evaluated in a given `TZ`. `Misfire` controls what happens to occurrences missed
while no server was running: `MisfireOnce` (default, catch up once), `MisfireAll` (run every
missed occurrence, capped), or `MisfireSkip` (drop stale ones).

### Unique jobs

`Unique{Key, For}` deduplicates by kind and key: a matching live job already in the store makes
the new `Enqueue` return the existing id instead of inserting a duplicate, for the given TTL.

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

### Transactional enqueue

`EnqueueTx` and `EnqueueManyTx` take a `driver.Writer` bound to your own transaction, so a job is
inserted atomically with the business data that produced it:

```go
w := store.Tx(tx)
client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
tx.Commit(ctx)
w.Notify(ctx)
```

`Notify` wakes idle workers after commit; without it the job still runs, just after the next poll.

### Cancellation

`client.Delete` on a job that is currently `processing` cancels that job's `context.Context` on
whichever server is running it, with `kiln.ErrCanceled` as the cause (`context.Cause(ctx)`). A
handler that stops and returns an error is recorded as `deleted`.

### Middleware

`Mux.Use` wraps every handler (logging, panics-to-errors, tracing); `NewClient`'s variadic
`EnqueueMiddleware` wraps every insert the same way (e.g. to stamp tenant metadata).

### Testing with kilntest

`kilntest.Work` drives a job through the real frozen middleware chain and classification logic
without a server, for handler unit tests. `RequireEnqueued`/`RequireNotEnqueued` assert on what a
piece of code actually enqueued. `drivertest.Run` is the conformance suite a `driver.Store`
implementation must pass.

## kiln vs. Hangfire

| Feature                         | Hangfire                     | kiln                               |
|---------------------------------|------------------------------|------------------------------------|
| Retries with backoff            | built-in                     | built-in                           |
| Continuations                   | built-in                     | built-in (`After`, `Needs`/`Flow`) |
| Batches + batch continuations   | Pro                          | built-in                           |
| Concurrency limits / semaphores | Ace (Hangfire.Throttling)    | built-in (`Limit{Max}`)            |
| Rate limiting                   | Ace (window counters)        | built-in (`Limit{Rate, Per}`)      |
| Continuation with many parents  | Pro (batch continuations)    | built-in (`After{a, b}`, `Needs`)  |
| Pause and resume a queue        | third-party                  | built-in (`PauseQueue`)            |
| Unique / idempotent jobs        | third-party / manual         | built-in (`Unique`)                |
| Recurring (cron)                | built-in                     | built-in, with `TZ` + `Misfire`    |
| Transactional enqueue           | built-in (ambient tx)        | explicit (`EnqueueTx`, `Tx`)       |
| Job cancellation                | built-in (CancellationToken) | built-in (`Delete`)                |
| Dashboard                       | built-in                     | built-in                           |
| Storage                         | SQL Server/Redis/others      | PostgreSQL, MySQL, SQLite          |
| Language                        | .NET                         | Go                                 |

## Storage backends

| Backend | Package | Notes |
|---|---|---|
| PostgreSQL (CI runs 17) | `pgstore` | `LISTEN`/`NOTIFY` wakeups, pgx v5, schema option |
| MySQL 8.0.19+ (CI runs 8.4) | `mysqlstore` | any `*sql.DB`, table prefix option, servers poll for new work; due and reserved jobs are checked every 100ms |
| SQLite 3.35+ | `sqlitestore` | any `database/sql` driver, WAL, servers in the same process are notified, others poll |
| in memory | `memstore` | tests and single-process tools |

Every backend passes the same conformance suite, `drivertest.Run`, so the semantics (retries,
continuations, batches, uniqueness, limits, fencing) do not change when the database does. Writing a
new backend means implementing `driver.Store` (and optionally `driver.Notifier` and
`driver.Transactor`) and making that suite pass.

With `mysqlstore`, begin your own transactions with `sql.LevelReadCommitted` before handing them to
`store.Tx`. At `REPEATABLE READ` InnoDB takes gap locks that can make concurrent enqueues wait for
your commit.

```go
db, _ := sql.Open("mysql", "user:pass@tcp(localhost:3306)/app")
store, err := mysqlstore.New(ctx, db)
```

`sqlitestore` is tested with `modernc.org/sqlite` (no cgo), `mattn/go-sqlite3` and
`ncruces/go-sqlite3`. SQLite allows one writer at a time, so the store keeps one pooled connection
for its writes and starts every write transaction with `BEGIN IMMEDIATE`; reads run alongside it in
WAL mode.

```go
db, _ := sql.Open("sqlite", "file:app.db?_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate")
store, err := sqlitestore.New(ctx, db)
```

- `busy_timeout` has to be in the DSN: it is set per connection, and `New` rejects a pool without it.
- `New` switches the file to WAL. In-memory databases cannot use WAL; use `memstore` for those.
- Use `_txlock=immediate` for transactions you pass to `store.Tx`, and keep them short: while one is
  open, every other writer on the file waits, kiln's servers included.
- Leave `SetMaxOpenConns` at 2 or more.

## Upgrading

Every release is tested against the previous one on the same database: the `compat` module runs the
published version and the new code side by side while jobs move between them. Schema changes only ever
add things, so servers on two consecutive versions can run together during a rolling deploy.

v0.3 adds the rate limit columns. During a rolling deploy from v0.2, jobs on a key with a `Rate` and no
`Max` are released only by servers already running v0.3, at the pace the rate allows; once released, any
server may run them. On a key with both, v0.2 servers still enforce `Max` but not the rate until they are
upgraded. Everything else is processed by both versions.

Additive changes are recorded in a `schema_changes` table next to the jobs tables. If the database role
kiln runs with was granted privileges table by table, grant it the same on `schema_changes` after the
upgrade.

## Performance

Apple M4 Max. PostgreSQL and MySQL run in Docker on the same machine; SQLite writes to the local SSD
with `synchronous=NORMAL`. Medians; see the benchmark code for ranges.

| | PostgreSQL | MySQL 8.4 | SQLite (modernc) |
|---|---|---|---|
| Bulk insert, 10k jobs per call | ~250k jobs/s | ~77k jobs/s | ~300k jobs/s |
| Claim + finish, 50 per fetch | ~45k jobs/s | ~15k jobs/s | ~80k jobs/s |
| One server, 100 workers, no-op handler | ~21k jobs/s | ~6k jobs/s | ~70k jobs/s |
| Enqueue to handler start | p50 3.3ms, p99 6.6ms | bounded by `PollInterval` | p50 0.16ms in the same process |

The MySQL numbers are dominated by commit latency on this setup (binlog with `sync_binlog=1`,
4-6ms per commit); a server with a faster disk will do noticeably better.

Compared with [River](https://github.com/riverqueue/river) v0.47 on the same PostgreSQL, alternating
rounds between the two libraries ([bench/](bench/README.md)):

| Scenario | kiln | River (defaults) | River (1ms fetch cooldown) |
|---|---|---|---|
| Bulk insert | 251k jobs/s | 134k (`InsertMany`), 222k (`InsertManyFast`) | |
| 8 goroutines inserting one job at a time | 5.1k jobs/s | 4.4k jobs/s | |
| Drain 50k no-op jobs, 100 workers | 21.2k jobs/s | 1.0k jobs/s | 20.3k jobs/s |
| Drain 20k jobs with a 1ms handler | 19.9k jobs/s | 1.0k jobs/s | 13.7k jobs/s |
| Enqueue to handler start, p50 | 3.3ms | 55ms | 4.7ms |

River's defaults fetch at most once every 100ms, which is what caps it around 1k jobs/s; with that
cooldown lowered the no-op drain is a tie, and the p99 latency of the two was too noisy on this
machine to call either way.

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
state with filtering and bulk actions, job detail with redactable args/meta/output, retries,
recurring schedules, queues, servers and batches, and mirrors all of it under a JSON API at
`<prefix>/api/...` for scripting. It's server-rendered with no external assets and a strict CSP.

## License

MIT, see [LICENSE](LICENSE).
