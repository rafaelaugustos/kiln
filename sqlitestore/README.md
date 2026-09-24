# sqlitestore

SQLite 3.35+ storage for [kiln](https://github.com/rafaelaugustos/kiln), on any `*sql.DB` from a
`database/sql` SQLite driver. It is tested with `modernc.org/sqlite` (no cgo), `mattn/go-sqlite3` and
`ncruces/go-sqlite3`.

```go
db, err := sql.Open("sqlite", "file:app.db?_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate")
store, err := sqlitestore.New(ctx, db)
```

SQLite allows one writer at a time. The store keeps one pooled connection for its own writes and starts
every write transaction with `BEGIN IMMEDIATE`, so writers in the same process wait their turn instead of
failing with `SQLITE_BUSY`. Reads use the other connections and run alongside the writer in WAL mode.

- Put `busy_timeout` in the DSN. It is set per connection, so the store cannot set it for you, and `New`
  rejects a pool without it (`mattn/go-sqlite3` already defaults to 5 seconds).
- `New` switches the file to WAL, which is stored in the file. In-memory databases cannot use WAL; use
  kiln's `memstore` for those.
- Use `_txlock=immediate` for transactions you pass to `store.Tx`, and keep them short: while one is open,
  every other writer on the file waits, kiln's servers included.
- Leave `SetMaxOpenConns` at 2 or more.
- `synchronous=NORMAL` is the usual choice with WAL: a process crash loses nothing, a power failure can
  lose the last few transactions. Keep the default `FULL` if that is not acceptable.
- A server in the same process as the code that enqueues is woken immediately; servers in other processes
  pick up new work on their next poll.
- Timestamps come from SQLite's clock, which has millisecond resolution.

Additive schema changes are recorded by name in `kiln_schema_changes`, so a server of the previous release
keeps working on an upgraded file.
