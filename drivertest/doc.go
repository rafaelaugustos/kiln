// Package drivertest is the conformance suite for kiln's stores. It checks a [driver.Store]
// against the contract package driver documents: the state a job is inserted in, claims and the
// fencing of the writes made for them, outcomes, dependencies, batches, unique keys, limits and
// rates, recurring jobs, leases, maintenance, operator actions and the read side, along with
// notifications and transactions when the store offers them.
//
// Every store in this repository runs it from its tests, memstore among them:
//
//	func TestConformance(t *testing.T) {
//		drivertest.Run(t, func(*testing.T) driver.Store { return memstore.New() })
//	}
package drivertest
