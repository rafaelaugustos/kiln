// Package sqlitestore keeps kiln's jobs in SQLite 3.35 or later, on any [sql.DB] from a
// database/sql SQLite driver. It is tested with github.com/mattn/go-sqlite3,
// github.com/ncruces/go-sqlite3 and modernc.org/sqlite, the last of which needs no cgo. [New]
// creates the store's tables, kiln_jobs and the rest, or brings them up to date:
//
//	dsn := "file:app.db?_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate"
//	db, err := sql.Open("sqlite", dsn)
//	store, err := sqlitestore.New(ctx, db)
//
// SQLite allows one writer at a time. The store keeps one of db's connections for its own writes
// and begins each of them with BEGIN IMMEDIATE, so writers in the same process wait their turn
// instead of failing with SQLITE_BUSY, while reads run on the other connections alongside the
// writer, in WAL mode. The database has to be set up for that:
//
//   - The DSN must set busy_timeout. It is a setting of each connection, so the store cannot set
//     it, and New rejects a pool without it.
//   - New switches the file to WAL, which is stored in the file. In-memory databases cannot use
//     WAL; use memstore for those.
//   - Transactions given to [Store.Tx] should begin with BEGIN IMMEDIATE, as _txlock=immediate in
//     the DSN makes them do, and stay short: while one is open, every other writer on the file
//     waits, kiln's servers included.
//   - db must allow at least two open connections.
//
// synchronous=NORMAL is the usual choice with WAL: a process crash loses nothing, and a power
// failure can lose the last few transactions. Keep the default, FULL, if that is not acceptable.
// Timestamps come from SQLite's clock, which has millisecond resolution.
//
// # Notifications
//
// A write wakes the servers subscribed to the same Store at once. Servers in other processes find
// new work on their next poll, unless every process opens the store with [Bus] on a shared
// [driver.Bus], which carries the same events between them.
//
// # Schema
//
// New migrates in one transaction, so a migration that fails leaves nothing half done. Numbered
// migrations are recorded in kiln_migrations, and a release refuses tables whose number is newer
// than it knows. Changes that only add something are recorded by name in kiln_schema_changes
// instead, so servers of the previous release keep working on the upgraded file.
package sqlitestore
