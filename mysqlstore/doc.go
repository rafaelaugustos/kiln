// Package mysqlstore keeps kiln's jobs in MySQL 8.0.19 or later, on any [sql.DB] opened with
// github.com/go-sql-driver/mysql. [New] creates the store's tables, kiln_jobs and the rest, in the
// database the DSN names, or brings them up to date:
//
//	db, err := sql.Open("mysql", "user:pass@tcp(localhost:3306)/app")
//	store, err := mysqlstore.New(ctx, db)
//
// The store keeps one of db's connections for itself, so db must allow at least two. The
// transactions it opens run at READ COMMITTED whatever the server's default, and those that end in
// a deadlock or a lock wait timeout are retried, up to five attempts in all.
//
// # Notifications
//
// MySQL cannot tell servers that something happened, so without a bus every server, even one in
// the process that enqueued, finds new work by polling. [Bus] connects the store to a [driver.Bus]
// shared by every process, such as the one package redisbus provides: after each commit the store
// publishes the queues that received jobs to run, the running jobs whose deletion was requested
// and the queues that were paused or resumed, and servers start work within milliseconds. Events
// are published in the background, so a slow or failing bus never delays or fails a write; only
// [TxWriter.Notify] waits for the bus and reports its error.
//
// # Enqueueing in a transaction
//
// [Store.Tx] binds a writer to the application's transaction, so that jobs commit or roll back
// with the rest of the application's work. Begin the transaction at READ COMMITTED: at REPEATABLE
// READ, InnoDB takes gap locks that can make concurrent enqueues wait for its commit.
//
//	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
//	w := store.Tx(tx)
//	client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
//	tx.Commit()
//	w.Notify(ctx)
//
// [TxWriter.Notify], called after the commit, admits the jobs with a Limit that the transaction
// inserted, which would otherwise wait for the next sweep, and publishes the queues that received
// jobs to run.
//
// # Schema
//
// New migrates while holding a lock taken with GET_LOCK, so servers that start together migrate
// once. Numbered migrations are recorded in kiln_migrations, and a release refuses tables whose
// migration number is newer than it knows. Changes that only add something, such as a column
// added with ALGORITHM=INSTANT, are recorded by name in kiln_schema_changes instead, so servers of
// the previous release keep running on the upgraded tables.
package mysqlstore
