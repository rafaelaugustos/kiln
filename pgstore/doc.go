// Package pgstore keeps kiln's jobs in PostgreSQL, through pgx v5. [New] returns a store,
// creating its schema or bringing it up to date:
//
//	pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/app")
//	store, err := pgstore.New(ctx, pool)
//	defer store.Close()
//
// The schema is "kiln" unless [Schema] names another. The store runs on a pool of its own, built
// from the configuration of the one it is given, so that job processing never competes with the
// application's queries for connections. Each [Store.Subscribe] call opens one more connection,
// for LISTEN.
//
// # Notifications
//
// Servers learn of new work through LISTEN and NOTIFY, on three channels named after the schema:
// <schema>_jobs carries the queues that received jobs to run, <schema>_cancel the ids of running
// jobs whose deletion was requested, and <schema>_queue the queues that were paused or resumed.
// The store notifies after its commits, in statements of their own, and never from inside the
// application's transaction, since PostgreSQL serializes the commits of transactions that notify.
//
// Behind PgBouncer in transaction mode, which cannot deliver notifications, give LISTEN a direct
// connection with [ListenConn], and set [ExecMode] to [pgx.QueryExecModeExec] if the pooler does
// not support prepared statements, even when the pool given to New already uses that mode.
//
// # Enqueueing in a transaction
//
// [Store.Tx] binds a writer to the application's pgx transaction, and [Store.SQLTx] to one from
// database/sql, so that jobs commit or roll back with the rest of the application's work:
//
//	tx, err := pool.Begin(ctx)
//	w := store.Tx(tx)
//	client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
//	tx.Commit(ctx)
//	w.Notify(ctx)
//
// [TxWriter.Notify] admits throttled jobs and wakes the servers after the commit; without it,
// servers find the jobs on their next poll, and throttled jobs wait for the next sweep.
//
// # Schema
//
// Migrations run in transactions under an advisory lock, with a lock_timeout of 5s, so servers
// that start together migrate once and a migration fails rather than queue behind a long
// transaction. The timeout covers the advisory lock too: a server that waits more than 5s for
// another's migration fails to start. Numbered migrations are recorded in the migrations table,
// and a release refuses a schema whose migration number is newer than it knows. Changes that only
// add something, such as a column with a default or a new index, are recorded by name in
// schema_changes instead, so servers of the previous release keep running on the upgraded schema.
// A role that was granted privileges table by table needs the same on schema_changes.
package pgstore
