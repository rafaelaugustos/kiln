# mssqlstore

SQL Server 2019+ and Azure SQL storage for [kiln](https://github.com/rafaelaugustos/kiln), on any `*sql.DB`
opened with `github.com/microsoft/go-mssqldb`.

```go
db, _ := sql.Open("sqlserver", "sqlserver://user:pass@localhost:1433?database=app")
store, err := mssqlstore.New(ctx, db)
```

The DSN names the database; the store creates its tables in the default schema of the connection's user.
The pool must allow at least two open connections.

## READ_COMMITTED_SNAPSHOT

Turn on row versioning for READ COMMITTED once, as Azure SQL Database already does:

```sql
ALTER DATABASE app SET READ_COMMITTED_SNAPSHOT ON
```

The store is built for it: its statements read the last committed version of the rows they do not lock,
so the dashboard and other reads never wait for writers, and each statement sees one consistent state.
Rows it changes are locked with `READPAST` wherever another server may hold them, so servers claiming or
promoting at the same time skip each other's jobs instead of waiting. Transactions chosen as deadlock
victims are retried, up to five attempts in all.

## Schema

`New` creates and upgrades its tables and sequences (`kiln_*`, or the `Prefix` option) in transactions
that hold an `sp_getapplock` lock, so servers that start together migrate once and a failed migration
leaves nothing behind. Numbered migrations are recorded in `kiln_migrations`, and a binary refuses a
database whose number is newer than it knows. Changes that only add something are recorded by name in
`kiln_schema_changes` instead, so servers of the previous release keep running on the upgraded tables.
With `NoMigrate`, `New` fails when the tables are behind, naming the missing version or change; run
`mssqlstore.Migrate` first.

Job ids come from a sequence, metadata and history are JSON in `NVARCHAR(MAX)` columns, and queue names,
kinds and limit keys are compared case-sensitively whatever the database's collation.

## Polling, notifications and rate limits

SQL Server has no `LISTEN`/`NOTIFY`, so without a bus servers find new work by polling every
`PollInterval`. Scheduled jobs, retries and the start times reserved by a `Limit` with a `Rate` are
checked more often: a server that receives no notifications looks for due jobs every 100ms, so a reserved
start is released on time even when another process, such as an API that only enqueues, reserved it.
Lower `PollInterval` if newly enqueued jobs need to start sooner than that.

`mssqlstore.Bus(b)` connects the store to a `driver.Bus` shared by every process, such as the Redis
pub/sub implementation in `redisbus`. After each commit the store publishes the events PostgreSQL
delivers through `NOTIFY`: the queues that received jobs to run (new, retried, released by a parent or a
batch, admitted by a limit, or given a reserved start), cancellations of running jobs, and paused or
resumed queues. Servers subscribe to the bus and start work within milliseconds, on any server. Events
are published in the background, so a slow or failing bus never fails or delays a write; they are hints,
and servers still poll as a fallback.

## Enqueueing in your transaction

```go
tx, _ := db.BeginTx(ctx, nil)
w := store.Tx(tx)
client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
tx.Commit()
w.Notify(ctx)
```

kiln never publishes from inside your transaction. Each write runs under a savepoint, so one that fails
leaves your transaction usable. `Notify` admits the jobs with a `Limit` that the transaction inserted,
which otherwise wait for the next sweep, and publishes their queues to the bus.
