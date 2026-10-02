// Package sqlitestore keeps kiln's jobs in SQLite 3.38 or later, on any [sql.DB] from a
// database/sql SQLite driver. It is tested with github.com/mattn/go-sqlite3, which needs cgo, and
// with github.com/ncruces/go-sqlite3 and modernc.org/sqlite, which do not. [New] creates the
// store's tables, kiln_jobs and the rest, or brings them up to date:
//
//	dsn := "file:app.db?_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate"
//	db, err := sql.Open("sqlite", dsn)
//	store, err := sqlitestore.New(ctx, db)
//
// SQLite allows one writer at a time. The store keeps one of db's connections for its own writes
// and begins each of them with BEGIN IMMEDIATE, so its writes wait their turn, behind those of
// other processes too, instead of failing with SQLITE_BUSY, while reads run on db's other
// connections alongside them, in WAL mode. The database has to be set up for that:
//
//   - Connections need a busy_timeout. It is a setting of each connection, so the store cannot set
//     it: put it in the DSN unless the driver sets one, as mattn/go-sqlite3 does. New rejects a
//     pool without it.
//   - New switches the file to WAL mode, a setting the file keeps. In-memory databases cannot use
//     WAL; use memstore for those.
//   - Transactions given to [Store.Tx] should begin with BEGIN IMMEDIATE, which _txlock=immediate
//     in the DSN makes every transaction on db do, and stay short: while one is open, every other
//     writer on the file waits, kiln's servers included.
//   - db must allow at least two open connections.
//
// synchronous=NORMAL is the usual choice with WAL: a process crash loses nothing, and a power
// failure or an OS crash can lose the last few transactions. Ask for FULL, SQLite's default, if
// that is not acceptable; mattn/go-sqlite3 sets NORMAL unless its DSN says otherwise. The store's
// clock, [Store.Now], is SQLite's, which has millisecond resolution.
//
// # Notifications
//
// A write through a Store wakes the servers subscribed to that Store at once, and a write through
// [Store.Tx] does when [TxWriter.Notify] is called. Servers on other Stores, usually in other
// processes, find new work on their next poll, unless every process gives New the same
// [driver.Bus] with [Bus], which carries the events between them.
//
// # Schema
//
// New migrates in one transaction, so a migration that fails leaves nothing half done. Numbered
// migrations are recorded in kiln_migrations, and a release refuses tables whose migration number
// is newer than it knows. Changes that only add something are recorded by name in
// kiln_schema_changes instead, so servers of the previous release keep working on the upgraded
// file.
package sqlitestore
