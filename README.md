# kiln

kiln is a background job library for Go, modelled on Hangfire for .NET. Jobs are rows in a
database rather than an in-memory queue, so they survive restarts and crashes.

## Install

```
go get github.com/rafaelaugustos/kiln
go get github.com/rafaelaugustos/kiln/pgstore
```

`kiln` itself has no external dependencies; `pgstore` pulls in `pgx/v5`.

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
| Concurrency limits / semaphores | Ace (Hangfire.Throttling)    | built-in (`Limit`)                 |
| Unique / idempotent jobs        | third-party / manual         | built-in (`Unique`)                |
| Recurring (cron)                | built-in                     | built-in, with `TZ` + `Misfire`    |
| Transactional enqueue           | built-in (ambient tx)        | explicit (`EnqueueTx`, `Tx`)       |
| Job cancellation                | built-in (CancellationToken) | built-in (`Delete`)                |
| Dashboard                       | built-in                     | built-in                           |
| Storage                         | SQL Server/Redis/others      | PostgreSQL, driver SPI for more    |
| Language                        | .NET                         | Go                                 |

## Storage backends

PostgreSQL is the supported backend today, via `pgstore`: a single schema, versioned migrations
run automatically on `New`, `LISTEN`/`NOTIFY` for low-latency wakeups. Any other store is a matter
of implementing `driver.Store` (and optionally `driver.Notifier`, `driver.Transactor`) from the
`driver` package and passing it `drivertest.Run`, the same conformance suite pgstore and the
in-memory `memstore` reference implementation both pass.

## Performance

Measured on an Apple M4 Max, PostgreSQL 17, running locally:

- Bulk insert: ~260k jobs/s (10k jobs per `EnqueueMany` call)
- Claim + finish: ~45k jobs/s (batches of 50)
- End-to-end, memstore: ~630k jobs/s
- End-to-end, Postgres: ~23.5k jobs/s (one server, 100 workers, no-op handler)
- Enqueue to handler start, Postgres with `LISTEN`/`NOTIFY`: p50 4.6ms, p99 8.3ms

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
