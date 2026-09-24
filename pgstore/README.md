# pgstore

PostgreSQL storage for [kiln](https://github.com/rafaelaugustos/kiln), built on pgx v5.

```go
pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/app")
store, err := pgstore.New(ctx, pool)
defer store.Close()
```

`New` copies the pool's configuration into a small pool of its own (`MaxConns`, 8 by default), so job
processing never competes with your application's queries for connections, plus one dedicated connection
for `LISTEN`.

## Options

- `Schema("jobs")` puts every table, type and sequence in that schema (default `kiln`). Two schemas in one
  database are two independent kiln installations.
- `NoMigrate()` skips migrations; `New` then fails if the schema is behind. Run `pgstore.Migrate` from a
  deploy step instead.
- `ListenConn(dsn)` opens the `LISTEN` connection with a different connection string. Use it when the pool
  goes through PgBouncer in transaction mode, which cannot deliver notifications.
- `ExecMode(pgx.QueryExecModeExec)` for poolers that do not support prepared statements.

## Enqueueing in your transaction

```go
tx, err := pool.Begin(ctx)
w := store.Tx(tx)
client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
tx.Commit(ctx)
w.Notify(ctx)
```

The job becomes visible when your transaction commits. kiln never sends `NOTIFY` from inside your
transaction, because PostgreSQL serializes the commits of notifying transactions; `Notify` wakes the
servers afterwards, and without it they pick the job up on their next poll.

## Schema

Migrations run under a transaction-scoped advisory lock with a short `lock_timeout`, so servers that start
together migrate once and never queue behind long transactions. Numbered migrations are recorded in
`migrations`, and a binary refuses a schema whose number is newer than it knows. Changes that only add
something (columns with defaults, new tables and indexes) are recorded by name in `schema_changes`, so
servers of the previous release keep running on the upgraded schema. A role that was granted privileges
table by table needs the same on `schema_changes`.
