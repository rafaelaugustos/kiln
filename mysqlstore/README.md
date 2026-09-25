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

## Polling and rate limits

MySQL has no `LISTEN`/`NOTIFY`, so servers find new work by polling every `PollInterval`. Scheduled jobs,
retries and the start times reserved by a `Limit` with a `Rate` are checked more often: a server that
receives no notifications looks for due jobs every 100ms, so a reserved start is released on time even
when another process, such as an API that only enqueues, reserved it. Lower `PollInterval` if newly
enqueued jobs need to start sooner than that.
