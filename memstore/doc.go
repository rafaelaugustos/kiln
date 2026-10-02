// Package memstore keeps kiln's jobs in memory. Its [Store] implements [driver.Store],
// [driver.Notifier] and [driver.Transactor], and is the reference implementation of the contract
// package driver documents.
//
// Jobs live as long as the Store, and only the clients and servers that share it see them, so it
// suits tests, examples and tools that run in a single process:
//
//	store := memstore.New()
//	client := kiln.NewClient(store)
//
// In tests, [Clock] puts the store's time under the test's control, and [Store.Begin] stands in
// for the application's transaction when the code under test enqueues inside one.
package memstore
