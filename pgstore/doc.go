// Package pgstore keeps kiln's jobs in PostgreSQL, through pgx v5. [New] creates the schema the
// store works in, "kiln" unless [Schema] names another, or brings it up to date:
//
//	pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/app")
//	store, err := pgstore.New(ctx, pool)
//	defer store.Close()
//
// The store runs on a pool of its own, built from the configuration of the one it is given, so
// that job processing never competes with the application's queries for connections, and
// [Store.Subscribe] adds one connection for LISTEN.
//
// # Notifications
//
// Servers learn of new work through LISTEN and NOTIFY, on three channels named after the schema:
// <schema>_jobs carries the queues that received jobs to run, <schema>_cancel the ids of running
// jobs that were deleted, and <schema>_queue the queues that were paused or resumed. The store
// notifies after its commits, in statements of their own, and never from inside the application's
// transaction, since PostgreSQL serializes the commits of transactions that notify.
//
// Behind PgBouncer in transaction mode, which cannot deliver notifications, give LISTEN a direct
// connection with [ListenConn], and set [ExecMode] to [pgx.QueryExecModeExec] if the pooler does
// not support prepared statements.
//
// # Enqueueing in a transaction
//
// [Store.Tx] binds a writer to the application's pgx transaction, and [Store.SQLTx] to one from
// database/sql, so that jobs commit or roll back with the rest of its work:
//
//	tx, err := pool.Begin(ctx)
//	w := store.Tx(tx)
//	client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
//	tx.Commit(ctx)
//	w.Notify(ctx)
//
// [TxWriter.Notify] wakes the servers after the commit; without it they find the jobs on their
// next poll.
//
// # Schema
//
// Migrations run in transactions under an advisory lock, with a short lock_timeout, so servers
// that start together migrate once and never queue behind long transactions. Numbered migrations
// are recorded in the migrations table, and a release refuses a schema whose number is newer than
// it knows. Changes that only add something, such as a column with a default or a new index, are
// recorded by name in schema_changes instead, so servers of the previous release keep running on
// the upgraded schema. A role that was granted privileges table by table needs the same on
// schema_changes.
package pgstore
