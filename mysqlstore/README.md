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

MySQL has no `LISTEN`/`NOTIFY`, so servers find new work by polling every `PollInterval`. When a `Limit`
with a `Rate` makes a job wait, its start is reserved: the job is stored as `scheduled` with the reserved
time as `run_at`, and a server's promoter releases it at that time. A promoter sleeps until the earliest
`run_at` it knows about and learns about new reservations when it polls and right after its own server
finishes a job with a `Limit`. A slot reserved by another process, such as an API that only enqueues, is
therefore promoted at the next poll of a promoter, up to `PollInterval` late, and jobs whose slots passed
in the meantime start together (at most `Max` at once when the key has one). Once a server is working
through a rate limited key, every finished job wakes its promoter and the following slots are released on
time. Lower `PollInterval` towards the rate's interval (50ms for 20 jobs a second) when the spacing of the
first starts matters.
