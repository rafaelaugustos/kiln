# Enqueueing

## Enqueue options

Options go after the args, and an args type can supply its own defaults with an
`InsertOptions() []kiln.InsertOption` method, so callers don't repeat them:

```go
client.Enqueue(ctx, SendEmail{To: to},
	kiln.Queue("emails"),
	kiln.Delay(10*time.Minute),
	kiln.MaxAttempts(5),
	kiln.Tags{"welcome"},
	kiln.Title("Welcome email for "+to),
)
```

`Title` names the job in the dashboard, which otherwise shows its kind; an args type can also have a
`Title() string` method. Tags can be filtered on in the dashboard's job lists.

## Transactional enqueue

`EnqueueTx` and `EnqueueManyTx` take a `driver.Writer` bound to your own transaction, so a job is
inserted atomically with the business data that produced it:

```go
w := store.Tx(tx)
client.EnqueueTx(ctx, w, SendReceipt{OrderID: id})
tx.Commit(ctx)
w.Notify(ctx)
```

Call `Notify` after the commit, never before. The job is already committed by then, so an error from
`Notify` changes nothing about it: log it and carry on, don't turn it into a failed request, or a client
that retries will create the order twice. Without `Notify` the job still runs, after the next poll.

Every store's transactional writer is a `driver.TxWriter` (`Notify` included, a no-op on `memstore.Tx`),
so code that runs on more than one store can hold one instead of switching on types. On PostgreSQL
through `database/sql` (sqlx, bun, GORM with pgx's stdlib driver), `store.SQLTx(tx)` takes a `*sql.Tx`.
