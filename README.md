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

Optional: `kilnotel` for OpenTelemetry and `redisbus` to wake MySQL and SQLite servers through Redis.

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
A failed job that you requeue gets all of its attempts again.

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

`Unique{Key}` makes `Enqueue` return the id of a live job with the same kind and key instead of
inserting another one. The key is released as soon as that job succeeds, fails or is deleted.

`Unique{Key, For: d}` holds the key for `d` from the first enqueue, whatever happens to the job in the
meantime, including success. It means "at most once per `d`" (one reminder per 10 minutes), not "no
duplicates while it runs".

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

### Testing with kilntest

`kilntest.Work` drives a job through the real frozen middleware chain and classification logic
without a server, for handler unit tests. `RequireEnqueued`/`RequireNotEnqueued` assert on what a
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
  `DeadAfter`. See [Operating kiln](#operating-kiln) for the timings.

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
| OpenTelemetry                   | contrib package              | `kilnotel` (traces and metrics)    |
| Storage                         | SQL Server/Redis/others      | PostgreSQL, MySQL, SQLite          |
| Language                        | .NET                         | Go                                 |

## Coming from Hangfire

Hangfire serializes a method call; kiln enqueues a typed value and routes it to a handler registered
for its `Kind()`. Most calls map one to one:

| Hangfire | kiln |
|---|---|
| `BackgroundJob.Enqueue(() => mailer.Send(to))` | `client.Enqueue(ctx, SendEmail{To: to})` |
| `BackgroundJob.Schedule(() => ..., TimeSpan.FromMinutes(30))` | `client.Enqueue(ctx, args, kiln.Delay(30*time.Minute))` |
| `BackgroundJob.Schedule(() => ..., runAt)` | `client.Enqueue(ctx, args, kiln.At(runAt))` |
| `BackgroundJob.ContinueJobWith(parentId, () => ...)` | `client.Enqueue(ctx, args, kiln.After{parentID})` |
| `ContinueJobWith(..., JobContinuationOptions.OnAnyFinishedState)` | `kiln.AfterFinished{parentID}` |
| `BackgroundJob.Delete(id)` / `BackgroundJob.Requeue(id)` | `client.Delete(ctx, id)` / `client.Requeue(ctx, id)` |
| `RecurringJob.AddOrUpdate("report", () => ..., Cron.Daily())` | `client.SetRecurring(ctx, "report", "@daily", Report{})` |
| `new RecurringJobOptions { TimeZone = tz }` | `kiln.TZ("America/Sao_Paulo")` |
| `RecurringJob.TriggerJob("report")` / `RemoveIfExists("report")` | `client.TriggerRecurring(ctx, "report")` / `client.RemoveRecurring(ctx, "report")` |
| `[Queue("critical")]` | `kiln.Queue("critical")`, or `InsertOptions()` on the args type |
| `[AutomaticRetry(Attempts = 5)]` | `kiln.MaxAttempts(5)` |
| `[AutomaticRetry(DelaysInSeconds = new[] { 60, 300 })]` | `kiln.Handle(mux, h, kiln.Delays(time.Minute, 5*time.Minute))` |
| `[DisableConcurrentExecution(60)]` | `kiln.Limit{Key: "reports", Max: 1}` |
| Ace semaphore / rate limiter | `kiln.Limit{Key: k, Max: n}` / `kiln.Limit{Key: k, Rate: n, Per: time.Second}` |
| Pro `BatchJob.StartNew(x => x.Enqueue(...))` | `b := &kiln.Batch{}; b.Add(args); client.StartBatch(ctx, b)` |
| Pro `BatchJob.ContinueBatchWith(batchId, ...)` | `b.Then(args)`, or `client.Enqueue(ctx, args, kiln.AfterBatch(batchID))` |
| `CancellationToken` parameter | the handler's `ctx`, cancelled with cause `kiln.ErrCanceled` on `Delete` |
| `context.SetJobParameter("cursor", c)` | `j.SetParam(ctx, "cursor", c)` / `j.Param("cursor", &c)` |
| `IClientFilter` / `IServerFilter` | `kiln.NewClient(store, mw...)` / `mux.Use(mw...)` |
| `Enqueue<IMailer>(x => x.Send(...))` resolved from DI | a method value with its dependencies: `kiln.Handle(mux, mailer.Send)` |
| `AddHangfireServer(o => { o.WorkerCount = 20; o.Queues = ... })` | `kiln.ServerConfig{Pools: []kiln.Pool{{Queues: []string{"critical", "default"}, Workers: 20}}}` |
| `UseSqlServerStorage(conn)` | `pgstore.New(ctx, pool)`, `mysqlstore.New(ctx, db)` or `sqlitestore.New(ctx, db)` |
| `app.UseHangfireDashboard("/hangfire")` | `http.Handle("/kiln/", dashboard.New(client, dashboard.Options{Prefix: "/kiln", Authorize: auth}))` |

## Storage backends

| Backend | Package | Notes |
|---|---|---|
| PostgreSQL (CI runs 17) | `pgstore` | `LISTEN`/`NOTIFY` wakeups, pgx v5, schema option |
| MySQL 8.0.19+ (CI runs 8.4) | `mysqlstore` | any `*sql.DB`, table prefix option; servers are woken through a `Bus`, or poll without one |
| SQLite 3.35+ | `sqlitestore` | any `database/sql` driver, WAL; servers in the same process are woken directly, other processes through a `Bus` |
| in memory | `memstore` | tests and single-process tools; everything is lost when the process exits |

Every backend passes the same conformance suite, `drivertest.Run`, so the semantics (retries,
continuations, batches, uniqueness, limits, fencing) do not change when the database does. Writing a
new backend means implementing `driver.Store` (and optionally `driver.Notifier` and
`driver.Transactor`) and making that suite pass.

PostgreSQL wakes servers with `LISTEN`/`NOTIFY`. MySQL has nothing equivalent, and SQLite can only wake
servers in its own process, so both accept a `driver.Bus`. `redisbus` is one over Redis Pub/Sub:

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})
store, err := mysqlstore.New(ctx, db, mysqlstore.Bus(redisbus.New(rdb)))
```

With a bus, new jobs, released continuations and cancellations reach every server within milliseconds.
Without one, servers find new work on their next poll (`PollInterval`) and check for due jobs every
100ms. Events are only hints: a lost one delays a job until the next poll, it never loses the job.

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

v0.4 adds a nullable `admit_tat` column to the limits table. Until every server runs v0.4, the ones
still on v0.3 release jobs whose start time has come without the check that keeps them from starting
together after a stall.

## Operating kiln

| `ServerConfig` | Default | What it controls |
|---|---|---|
| `PollInterval` | 1s | How often an idle pool looks for work when no notification arrives |
| `HeartbeatInterval` | 5s | How often a server reports that it is alive |
| `DeadAfter` | 60s | Silence after which a server's running jobs are retried elsewhere |
| `LeaderTTL` | 15s | Lease of the leader that runs recurring jobs, rescue, sweep and pruning |
| `ShutdownTimeout` | 30s | How long `Run` waits for running jobs after its context is cancelled |
| `KillGrace` | 5s | Extra wait after cancelling jobs that ignored the shutdown |
| `Timeout` | 30m | Default per-job timeout |

**Recovering from a dead worker.** A server that exits cleanly finishes its running jobs, or hands them
back, before `Run` returns. A server that dies is noticed after `DeadAfter`, and within one more
`HeartbeatInterval` its jobs are rescued: they go straight back to their queues, and the rescued attempt
counts toward `MaxAttempts`. With the defaults, a job whose worker was killed starts again after roughly
60 to 70 seconds. A job rescued twice in a row waits for its backoff before the next attempt, so a job that
takes down the process running it is not handed from worker to worker without a pause. If the dead server
was also the leader, a new leader takes over after `LeaderTTL` and waits `DeadAfter + HeartbeatInterval`
before rescuing anything, which adds about 20 seconds. Lower `DeadAfter` (it must stay above `3*HeartbeatInterval + KillGrace + 5s`) to
notice dead workers sooner, and keep long jobs resumable with `SetParam` checkpoints.

**Connections.** `pgstore` opens its own pool (`MaxConns`, 8 by default) plus one connection for
`LISTEN`, on top of your application's pool: budget up to 9 connections per process that opens a store.
`mysqlstore` and `sqlitestore` use the `*sql.DB` you pass, keeping one of its connections for their own
writes, so leave `SetMaxOpenConns` at 2 or more.

**Polling.** An idle server runs one indexed query per pool every `PollInterval`, and, when it gets no
notifications (MySQL without a `Bus`), one more every 100ms to release delayed jobs on time, besides its
heartbeat and the leader's periodic work. Each query is cheap, but they add up: an API process and two
idle workers on MySQL ran about 70 queries per second with the default `PollInterval` and 200 with
200ms. A `Bus` removes the 100ms check and lets `PollInterval` stay long.

**Health.** `Server.Healthy()` fails when the server has fenced itself off, its heartbeat is stale or its
results are piling up; use it for readiness and liveness probes. `Server.Stats()` has the counters.

## Performance

Apple M4 Max. PostgreSQL and MySQL run in Docker on the same machine; SQLite writes to the local SSD
with `synchronous=NORMAL`. Medians; see the benchmark code for ranges.

| | PostgreSQL | MySQL 8.4 | SQLite (modernc) |
|---|---|---|---|
| Bulk insert, 10k jobs per call | ~250k jobs/s | ~77k jobs/s | ~300k jobs/s |
| Claim + finish, 50 per fetch | ~45k jobs/s | ~15k jobs/s | ~80k jobs/s |
| One server, 100 workers, no-op handler | ~21k jobs/s | ~6k jobs/s | ~70k jobs/s |
| Enqueue to handler start | p50 3.3ms, p99 6.6ms | ~5–20ms with `redisbus`, `PollInterval` without | p50 0.16ms in the same process |

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
