// Package mssqlstore keeps kiln's jobs in SQL Server 2019 or later, or Azure SQL, on any [sql.DB]
// opened with github.com/microsoft/go-mssqldb. [New] creates the store's tables and sequences,
// kiln_jobs and the rest, in the default schema of the connection's user, or brings them up to
// date:
//
//	db, err := sql.Open("sqlserver", "sqlserver://user:pass@localhost:1433?database=app")
//	store, err := mssqlstore.New(ctx, db)
//
// The store expects the database to run with READ_COMMITTED_SNAPSHOT on, as Azure SQL Database
// does by default:
//
//	ALTER DATABASE app SET READ_COMMITTED_SNAPSHOT ON
//
// Its statements then read the last committed version of the rows they do not lock, so reads,
// such as the dashboard's, never wait for writers, and each statement sees one consistent state.
// The rows it changes it locks with the READPAST hint wherever another server may hold them, so
// servers claiming or promoting at the same time skip each other's jobs instead of waiting. Its
// transactions run at READ COMMITTED, and those chosen as deadlock victims are retried, up to five
// attempts in all. Queue names, kinds, limit keys and the other names the store keeps are compared
// case-sensitively, whatever the database's collation.
//
// # Notifications
//
// SQL Server cannot tell servers that something happened, so without a bus every server, even one
// in the process that enqueued, finds new work by polling. [Bus] connects the store to a
// [driver.Bus] shared by every process, such as the one package redisbus provides: after each
// commit the store publishes the queues that received jobs to run, the running jobs whose deletion
// was requested and the queues that were paused or resumed, and servers start work within
// milliseconds. Events are published in the background, so a slow or failing bus never delays or
// fails a write; only [TxWriter.Notify] waits for the bus and reports its error.
//
// # Enqueueing in a transaction
//
// [Store.Tx] binds a writer to the application's transaction, so that jobs commit or roll back
// with the rest of the application's work:
//
//	tx, err := db.BeginTx(ctx, nil)
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
// New migrates in transactions that hold an application lock taken with sp_getapplock, so servers
// that start together migrate once and a migration that fails leaves nothing behind. Numbered
// migrations are recorded in kiln_migrations, and a release refuses tables whose migration number
// is newer than it knows. Changes that only add something are recorded by name in
// kiln_schema_changes instead, so servers of the previous release keep running on the upgraded
// tables.
package mssqlstore
