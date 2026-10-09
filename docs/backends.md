# Storage backends

kiln keeps jobs in a database through a store. Pick the one for the database you already run; switching
later means changing the line that builds the store, not the code that enqueues or handles jobs.

| Backend | Package | Notes |
|---|---|---|
| PostgreSQL (CI runs 17) | `pgstore` | `LISTEN`/`NOTIFY` wakeups, pgx v5, schema option |
| SQL Server 2019+ and Azure SQL (CI runs 2022) | `mssqlstore` | any `*sql.DB` from `go-mssqldb`, table prefix option; needs `READ_COMMITTED_SNAPSHOT`; servers are woken through a `Bus`, or poll without one |
| MySQL 8.0.19+ (CI runs 8.4) | `mysqlstore` | any `*sql.DB`, table prefix option; servers are woken through a `Bus`, or poll without one |
| SQLite 3.38+ | `sqlitestore` | any `database/sql` driver, WAL; servers in the same process are woken directly, other processes through a `Bus` |
| in memory | `memstore` | tests and single-process tools; everything is lost when the process exits |

Every backend passes the same conformance suite, `drivertest.Run`, so the semantics (retries,
continuations, batches, uniqueness, limits, fencing) do not change when the database does. Writing a
new backend means implementing `driver.Store` and making that suite pass; see
[Writing a store](custom-store.md).

| | PostgreSQL | MySQL | SQL Server | SQLite | in memory |
|---|---|---|---|---|---|
| Wakes servers | `LISTEN`/`NOTIFY` | through a `Bus` | through a `Bus` | in its process; others through a `Bus` | in its process |
| Enqueue in your transaction | `Tx(pgx.Tx)`, `SQLTx(*sql.Tx)` | `Tx(*sql.Tx)` | `Tx(*sql.Tx)` | `Tx(*sql.Tx)` | `Begin()` |
| Where its tables live | a schema (`Schema`) | a table prefix (`Prefix`) | a table prefix (`Prefix`) | a table prefix (`Prefix`) | memory |
| Job console, Limits page | yes | yes | yes | yes | yes |

PostgreSQL wakes servers with `LISTEN`/`NOTIFY`. MySQL and SQL Server have nothing equivalent, and SQLite
can only wake servers in its own process, so all three accept a `driver.Bus`. `redisbus` is one over Redis Pub/Sub:

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

`mssqlstore` takes a `*sql.DB` opened with `github.com/microsoft/go-mssqldb`, and the database needs row
versioning for READ COMMITTED, which Azure SQL Database already has on:

```sql
ALTER DATABASE app SET READ_COMMITTED_SNAPSHOT ON
```

```go
db, _ := sql.Open("sqlserver", "sqlserver://user:pass@localhost:1433?database=app")
store, err := mssqlstore.New(ctx, db)
```

Claims, finishes and admission lock rows with `READPAST`, so servers skip each other's work instead of
waiting, and transactions picked as deadlock victims are retried. Queue names, kinds and limit keys are
compared case-sensitively whatever the database's collation. It runs on compatibility level 130 or higher
and is tested on SQL Server 2022.

`sqlitestore` takes a `*sql.DB` from any `database/sql` SQLite driver; its tests and CI use
`modernc.org/sqlite`, which needs no cgo. SQLite allows one writer at a time, so the store keeps one pooled connection
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
