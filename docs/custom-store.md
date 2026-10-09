# Writing a store

A store keeps kiln's jobs in a database. Each store in this repository is a package that implements
`driver.Store`, and all of them pass the same conformance suite, so retries, dependencies, batches, unique
keys and limits behave the same on each one. A new store gets that behaviour the same way: implement the
interface, then make `drivertest.Run` pass.

The [driver package documentation](https://pkg.go.dev/github.com/rafaelaugustos/kiln/driver) is the
contract. Its overview explains states, claims, limits and rates; each method's documentation spells out
what that method must do.

## What to implement

`driver.Store` embeds five interfaces:

| Interface | What it covers |
|---|---|
| `Writer` | inserting jobs, opening and sealing batches |
| `Worker` | claiming jobs, applying their outcomes, heartbeats, `SetMeta` |
| `Coordinator` | the store's clock, the leader's lease, promotion, rescues, repairs, pruning and recurring jobs |
| `Admin` | delete, requeue, pause, and recurring definitions |
| `Inspector` | the read side the dashboard uses |

Five more are optional. kiln checks for them and works without them:

| Interface | Without it |
|---|---|
| `Notifier`, or `Bus` to wake other processes | servers find new jobs on their next poll |
| `Transactor` | `StartBatch` inserts step by step and cleans up after a failure |
| `Console` | the job console stays empty |
| `LimitReader` | the dashboard has no Limits page |

To let applications enqueue in their own transactions, also give the store a method that wraps one in a
`driver.Writer`, as `Tx` does in the stores here. A writer that can admit and notify after the commit
implements `driver.TxWriter`.

## The rules most stores get wrong first

- **One clock.** Run times, ages and expiries come from the store's clock (`Coordinator.Now`), never from
  the servers'.
- **Fenced writes.** A claim bumps the job's `Claim`, and every write for a running job (`Finish`,
  `SetMeta`, `WriteConsole`) applies only under that same claim. A server that lost a job can't overwrite
  it.
- **Finish is safe to resend.** Outcomes are independent, each applies in one atomic step with all its side
  effects, and an outcome sent twice comes back `Stale` the second time.
- **Skip what others hold.** Every server calls `Promote`, so a store with row locks should skip rows
  another transaction holds instead of waiting for them, and leave them for the next call.

## Run the conformance suite

```go
func TestConformance(t *testing.T) {
	drivertest.Run(t, func(t *testing.T) driver.Store {
		s := mystore.New(t) // a fresh, empty store for each test
		t.Cleanup(s.Close)
		return s
	})
}
```

Each test opens a store of its own, which must be empty, and the tests of a group run in parallel. The
tests sleep and poll on the real clock, so the store's clock has to move with it. The groups for the
optional interfaces skip a store that doesn't implement them.

## Where to look

- `memstore` is the reference implementation: everything under one mutex, the plainest reading of the
  contract.
- `sqlitestore` is the simplest database store, since SQLite runs one writer at a time.
- `pgstore`, `mysqlstore` and `mssqlstore` show row locks that skip what others hold (`SKIP LOCKED`,
  `READPAST`), and notifications.

Within v1 the methods of `driver.Store` don't change; new capabilities arrive as optional interfaces. See
[Compatibility](compatibility.md).
