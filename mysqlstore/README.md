# mysqlstore

MySQL 8.0.19+ storage for [kiln](https://github.com/rafaelaugustos/kiln), on any `*sql.DB` opened with
`github.com/go-sql-driver/mysql`.

```go
db, _ := sql.Open("mysql", "user:pass@tcp(localhost:3306)/app")
store, err := mysqlstore.New(ctx, db)
```

## Schema

`New` creates and upgrades its tables (`kiln_*`, or the `Prefix` option) while holding a `GET_LOCK`, so
servers that start together migrate once. Numbered migrations are recorded in `kiln_migrations`, and a
binary refuses a database whose number is newer than it knows. Changes that only add something, such as
a column with a default applied with `ALGORITHM=INSTANT`, are recorded by name in `kiln_schema_changes`
instead, so servers of the previous release keep running on the upgraded tables. With `NoMigrate`, `New`
fails when the tables are behind, naming the missing version or change; run `mysqlstore.Migrate` first.

## Polling, notifications and rate limits

MySQL has no `LISTEN`/`NOTIFY`, so without a bus servers find new work by polling every `PollInterval`.
Scheduled jobs, retries and the start times reserved by a `Limit` with a `Rate` are checked more often: a
server that receives no notifications looks for due jobs every 100ms, so a reserved start is released on
time even when another process, such as an API that only enqueues, reserved it. Lower `PollInterval` if
newly enqueued jobs need to start sooner than that.

`mysqlstore.Bus(b)` connects the store to a `driver.Bus` shared by every process, such as a Redis pub/sub
implementation. After each commit the store publishes the events PostgreSQL delivers through `NOTIFY`:
the queues that received jobs to run (new, retried, released by a parent or a batch, admitted by a limit,
or given a reserved start), cancellations of running jobs, and paused or resumed queues. Servers subscribe
to the bus and start work within milliseconds, on any server. Events are published in the background, so a
slow or failing bus never fails or delays a write; they are hints, and servers still poll as a fallback.

## Enqueueing in your transaction

```go
w := store.Tx(tx)
client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
tx.Commit()
w.Notify(ctx)
```

kiln never publishes from inside your transaction. `Notify` admits the jobs with a `Limit` that the
transaction inserted, which otherwise wait for the next sweep, and publishes their queues to the bus.
