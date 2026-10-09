// Package kiln runs background jobs, on the model of Hangfire for .NET. Jobs are rows in a
// database rather than messages in memory, so they survive restarts and crashes, and any number of
// processes can enqueue and run them.
//
// A job is a value of a type that implements [Args], whose Kind picks the handler:
//
//	type SendEmail struct{ To string }
//
//	func (SendEmail) Kind() string { return "send_email" }
//
// A [Client] inserts jobs into a store,
//
//	client := kiln.NewClient(store)
//	id, err := client.Enqueue(ctx, SendEmail{To: "ada@example.com"})
//
// and a [Server], in the same process or in another one, claims them and runs the handlers
// registered on a [Mux]:
//
//	mux := kiln.NewMux()
//	kiln.Handle(mux, func(ctx context.Context, j *kiln.Job[SendEmail]) error {
//		log.Printf("sending email to %s", j.Args.To)
//		return nil
//	})
//	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{})
//	if err != nil {
//		log.Fatal(err)
//	}
//	if err := server.Run(ctx); err != nil {
//		log.Fatal(err)
//	}
//
// The store is a [driver.Store]. Packages pgstore, mysqlstore, mssqlstore and sqlitestore keep
// jobs in PostgreSQL, MySQL, SQL Server and SQLite, and memstore keeps them in memory, for tests.
//
// # Life of a job
//
// A job starts [Awaiting] when it depends on jobs that have not finished, [Scheduled] when it
// should run later, [Throttled] when a [Limit] holds it back, and [Enqueued] otherwise. A server
// claims it, moving it to [Processing], and calls its handler. Returning nil moves the job to
// [Succeeded]. An error schedules another attempt after a [Backoff] delay, until [MaxAttempts]
// runs out and the job moves to [Failed]; [Permanent] fails it without retrying, [Snooze] runs it
// later without using up an attempt, and [Cancel] deletes it. As in Hangfire, failed is not final:
// the job stays there until someone requeues or deletes it. Succeeded and [Deleted] jobs are
// pruned after a day ([ServerConfig.Retention]).
//
// Jobs run at least once. A server can die after a handler's side effects and before its result
// is stored, and its jobs are then retried elsewhere, so handlers should be idempotent;
// [Job.SetParam] keeps checkpoints across attempts.
//
// # Enqueueing
//
// Options such as [Queue], [Priority], [Delay] and [MaxAttempts] set how a job runs. [After],
// [AfterFinished] and [Needs] make it wait for other jobs, and a [Batch] runs continuations once a
// group of jobs has finished. [Unique] keeps duplicates out, and [Limit] caps how many jobs of a
// key run together or how often they start. [Client.SetRecurring] enqueues jobs on a cron schedule,
// and [Client.EnqueueTx] inserts them in the application's own transaction.
//
// # Servers
//
// Servers coordinate through the store. Each one claims jobs from its queues, woken by the store's
// notifications when it has them and by polling otherwise. One server at a time is also the
// leader: it fires recurring jobs, rescues the jobs of servers that stopped heartbeating, and
// prunes old jobs. [ServerConfig] documents the timings.
package kiln
