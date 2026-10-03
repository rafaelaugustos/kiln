package driver

import (
	"context"
	"time"
)

// Writer inserts jobs and batches. A [Store] is a Writer that commits every call on its own;
// stores also hand out Writers bound to an application's transaction, whose writes commit or roll
// back with it.
type Writer interface {
	// Insert inserts jobs and returns one result per job, in the same order. It is all or nothing
	// except for duplicates: a job whose unique key is held is skipped and reported as a duplicate
	// (see [InsertParams.UniqueKey]) while the others are inserted, and any error means nothing was
	// inserted or replaced (see [InsertParams.UniqueReplace]). New jobs get ids above zero that
	// increase with their position in jobs, and the first state described in the package
	// documentation.
	//
	// Besides the checks of [CheckInsert], Insert fails with [ErrNotFound] when a parent job or a
	// batch does not exist, with [ErrClosed] when the job would join a finished batch, and with
	// [ErrTooLarge] or [ErrInvalid] for values over the store's own limits. A store's own Insert
	// also admits the throttled jobs of the keys it touched and, after the commit, publishes
	// [JobsReady] for every queue that received enqueued jobs; a failure to publish never fails the
	// call. A Writer bound to an application's transaction publishes nothing, since it cannot see
	// the commit, and may leave admission to [TxWriter.Notify] or a later Sweep.
	Insert(ctx context.Context, jobs []InsertParams) ([]Inserted, error)

	// OpenBatch creates an unsealed batch and returns its id. See [Batch] for its life.
	OpenBatch(ctx context.Context, b NewBatch) (int64, error)

	// SealBatch seals a batch, which allows it to finish: a sealed batch finishes as soon as none of
	// its members is live, and an empty one finishes when sealed. Sealing again is not an error; an
	// unknown id is [ErrNotFound].
	SealBatch(ctx context.Context, id int64) error
}

// Worker is the part of a store that servers use to run jobs.
type Worker interface {
	// Claim moves up to q.Limit enqueued jobs to processing for q.Server and returns them. It
	// takes jobs from q.Queues in order, moving to the next queue only when a queue has no more,
	// and within a queue by priority, highest first, then by id. It skips paused queues and, when
	// q.Kinds is not empty, jobs of other kinds. Each claimed job has its Attempt and Claim
	// incremented, AttemptedAt set to now and any cancel request cleared, and is returned with the
	// new values. Claim may return fewer jobs than are ready, but never the same job twice. Once
	// the claim has committed, a row that cannot be fully decoded, such as one with bad meta JSON,
	// is returned with what could be decoded instead of failing the call.
	Claim(ctx context.Context, q ClaimQuery) ([]Job, error)

	// Finish applies the outcomes of attempts and returns one [Result] per outcome, in the same
	// order. Outcomes are independent of each other. An outcome applies only while its job is
	// processing under the outcome's claim; otherwise it is [Stale]. An outcome with a State other
	// than Succeeded, Failed, Deleted, Scheduled or Enqueued is [Rejected].
	//
	// Applying an outcome moves the job to its State, with three adjustments: the job becomes
	// deleted if its deletion was requested and the outcome is not succeeded, a scheduled outcome
	// with no positive Delay makes it enqueued, and enqueued becomes throttled for a job with a
	// limit key. A scheduled job runs Delay after now, Refund decrements Attempt, and an Output
	// that is not empty is stored when the job ends succeeded or deleted, until it is requeued. In
	// the same transaction the store appends the history entry, releases the unique key, frees the
	// job's place under its limit and admits the next jobs, resolves or dooms the job's dependents,
	// finishes batches that can finish, and counts the outcome in the statistics of server for the
	// current minute: succeeded, failed, deleted, or retried for a scheduled outcome without Refund.
	//
	// An error means that nothing is known about any of the outcomes. The caller may send the same
	// outcomes again, so this must be safe: outcomes applied the first time come back Stale.
	Finish(ctx context.Context, server string, outs []Outcome) ([]Result, error)

	// Heartbeat records that the server s describes is alive, creating or updating its row with
	// HeartbeatAt set to now. It returns a lease for every job the server has processing, with
	// its cancel request and age, and the paused queues.
	Heartbeat(ctx context.Context, s ServerInfo) (Directives, error)

	// Unregister deletes the row of a server that is shutting down. Jobs it still has processing
	// become orphans immediately.
	Unregister(ctx context.Context, server string) error

	// SetMeta merges meta into the metadata of the job ref points to, replacing existing keys. It is
	// fenced like Finish and fails with [ErrLost] when the job is no longer processing under
	// ref's claim.
	SetMeta(ctx context.Context, ref Ref, meta map[string]string) error
}

// Coordinator is the part of a store that servers use to share work: the clock, leases, the
// promotion of due jobs, and the maintenance done by the leader.
type Coordinator interface {
	// Now returns the time on the store's clock, the clock every other method uses.
	Now(ctx context.Context) (time.Time, error)

	// Lead acquires the lease called name for holder, or renews it if holder has it, so that it
	// expires ttl from now; once expired, anyone can take it. ok reports whether holder has the
	// lease after the call, and held how long holder has had it without a break, zero when this
	// call acquired it.
	Lead(ctx context.Context, name, holder string, ttl time.Duration) (held time.Duration, ok bool, err error)

	// Resign releases the lease called name if holder has it, and does nothing otherwise.
	Resign(ctx context.Context, name, holder string) error

	// Promote moves scheduled jobs whose run time has come, up to limit of them, to enqueued, or to
	// throttled followed by admission when they have a limit key. It also runs admission for keys
	// that have granted jobs waiting in throttled. It returns how many jobs it moved, the queues that
	// received enqueued jobs, and the time until the earliest job still scheduled. Every server
	// calls it, so a store with row locks should skip rows that other transactions hold instead of
	// waiting for them. A store that can notify publishes [JobsReady] after the commit. A limit of 0
	// or less counts as 1.
	Promote(ctx context.Context, limit int) (Promoted, error)

	// Orphans returns up to limit processing jobs whose server has no row or has not sent a
	// heartbeat for deadAfter. It changes nothing: the caller turns each orphan into an outcome and
	// applies it with [Worker.Finish], fenced by its claim. A limit of 0 or less counts as 1.
	Orphans(ctx context.Context, deadAfter time.Duration, limit int) ([]Orphan, error)

	// Sweep repairs what other methods leave to it, changing at most limit rows. It resolves
	// dependencies whose parent job or batch is already final, moves awaiting jobs with nothing
	// pending out of awaiting, archives doomed children, finishes sealed batches with no live
	// member, recounts the enqueued and processing jobs of every limit key and runs admission.
	// It returns the number of rows it changed, which is zero for a consistent store, reserved
	// start times included. A few calls must be enough to leave no job stranded. A limit of 0 or
	// less counts as 1.
	Sweep(ctx context.Context, limit int) (int, error)

	// Prune deletes old rows, at most p.Limit from each table, and returns how many it deleted:
	// archived jobs finalized longer ago than p.Succeeded or p.Deleted, failed jobs finalized
	// longer ago than p.Failed, servers silent for longer than p.Servers, per-minute statistics
	// older than p.Stats once added to the all-time totals, expired unique keys, dependencies of
	// jobs that are gone, limit keys that no remaining job refers to and whose reserved start
	// times have all passed, and finished batches with no members left. A negative retention
	// keeps the jobs of that state.
	Prune(ctx context.Context, p PruneParams) (int, error)

	// Due returns up to limit recurring jobs that are not paused and whose NextRunAt has come,
	// with the store's current time. A limit of 0 or less counts as 1.
	Due(ctx context.Context, limit int) ([]Recurring, time.Time, error)

	// Fire applies f in one transaction if the recurring job is still at f.Version: it inserts
	// f.Jobs as [Writer.Insert] would, stores f.NextRunAt and f.LastRunAt, records the id of the
	// last job in LastJobID and bumps Version. It returns the results of the insert, [ErrConflict]
	// when the version has moved, and [ErrNotFound] when the recurring job is gone.
	Fire(ctx context.Context, f Fire) ([]Inserted, error)
}

// Admin is the part of a store behind the actions an operator takes from a client or the
// dashboard.
type Admin interface {
	// Delete deletes the jobs that match f and returns how many it affected. A processing job is
	// only marked: its cancel request is set, [CancelRequested] is published if the store can
	// notify, and the outcome its server reports makes it deleted. Other live jobs are deleted in
	// the same call, with a "deleted" history entry and the side effects of an outcome on unique keys,
	// limits, dependents and batches. Archived jobs, and processing jobs already marked, are left
	// alone. f must name ids or a state ([CheckFilter]); a filter by state may be applied in
	// transactions of up to 1000 jobs.
	Delete(ctx context.Context, f Filter) (int, error)

	// Requeue moves the failed, scheduled, succeeded and deleted jobs that match f to enqueued, or
	// to throttled when they have a limit key, and returns how many it moved. Archived jobs become
	// live again and RunAt becomes now. A failed, succeeded or deleted job starts over: Attempt goes
	// back to 0, so it has all of MaxAttempts again. A scheduled job keeps its Attempt, and
	// MaxAttempts is raised to Attempt+1 if it is lower. The cancel request and any grant are
	// cleared, and a "requeued" history entry is added. Jobs in other states are skipped, and so is a job whose unique key, held without
	// UniqueFor, now belongs to another job. f must name ids or a state.
	Requeue(ctx context.Context, f Filter) (int, error)

	// PauseQueue pauses or resumes queue. Claim stops returning its jobs as soon as the call
	// returns, and a store that can notify publishes [QueueChanged].
	PauseQueue(ctx context.Context, queue string, paused bool) error

	// Recurring returns the recurring job id, or [ErrNotFound].
	Recurring(ctx context.Context, id string) (Recurring, error)

	// PutRecurring stores r if r.Version is the stored version, or creates it if r.Version is 0
	// and there is no recurring job with its ID, and fails with [ErrConflict] otherwise. The store
	// bumps Version and sets CreatedAt and UpdatedAt.
	PutRecurring(ctx context.Context, r Recurring) error

	// RemoveRecurring deletes the recurring job id, or fails with [ErrNotFound]. Jobs it created
	// stay as they are.
	RemoveRecurring(ctx context.Context, id string) error
}

// Inspector is the read side of a store, used by clients and the dashboard.
type Inspector interface {
	// Job returns the job id, live or archived, with its history, output, children and pending
	// dependencies, or [ErrNotFound].
	Job(ctx context.Context, id int64) (Record, error)

	// Jobs returns a page of the jobs in q.State that match the rest of q, in this order:
	//
	//   - scheduled: by RunAt, then id
	//   - enqueued and throttled: by priority, highest first, then id
	//   - succeeded, failed and deleted: latest FinalizedAt first, then highest id
	//   - awaiting and processing: highest id first
	//
	// With q.Tag set, a store may look at a bounded number of jobs for one page, so that the page
	// holds fewer than q.Limit records, or none, while Next is set. Records may leave out History,
	// Output and Children. An invalid state is [ErrInvalid].
	Jobs(ctx context.Context, q JobQuery) (Page, error)

	// Counts returns the number of jobs in each live state, exact up to 100000, the number of
	// scheduled jobs that are retries, and the all-time totals of succeeded and deleted jobs.
	Counts(ctx context.Context) (Counts, error)

	// Series returns the statistics of the minutes that start before to, from the start of the
	// bucket that holds from, in buckets of step, which must be a positive multiple of a minute
	// ([ErrInvalid] otherwise). Buckets start at multiples of step since the Unix epoch and come in
	// ascending order; empty ones are left out.
	Series(ctx context.Context, from, to time.Time, step time.Duration) ([]Point, error)

	// Servers returns the registered servers, including those that stopped heartbeating, until
	// Prune removes them.
	Servers(ctx context.Context) ([]ServerInfo, error)

	// Queues returns every queue that has live jobs, a pause setting or a server working on it.
	Queues(ctx context.Context) ([]QueueInfo, error)

	// Recurrings returns every recurring job.
	Recurrings(ctx context.Context) ([]Recurring, error)

	// Batch returns the batch id, or [ErrNotFound].
	Batch(ctx context.Context, id int64) (Batch, error)

	// Batches returns a page of batches, newest first.
	Batches(ctx context.Context, q BatchQuery) (BatchPage, error)
}

// Store is a database as kiln sees it: everything servers, clients and the dashboard need from it.
type Store interface {
	Writer
	Worker
	Coordinator
	Admin
	Inspector
}

// Notifier is implemented by stores that can wake servers when there is work, instead of leaving
// them to find it at their next poll.
type Notifier interface {
	// Subscribe calls fn with the store's events until ctx is done, then returns nil. It
	// reconnects on its own after failures, and calls fn with a [Resync] event after every
	// subscription, the first one included; it should also send one after dropping events. Events
	// may be lost, repeated or reordered, since servers only take them as hints. fn runs on
	// Subscribe's goroutine and must not block.
	//
	// A store that is not set up to notify returns an error wrapping [errors.ErrUnsupported], and
	// the server stops calling it. Any other error is logged, and the server subscribes again
	// after a pause.
	Subscribe(ctx context.Context, fn func(Event)) error
}

// Bus carries events between processes, for stores whose database cannot tell servers in other
// processes that something happened. The store publishes its events on the bus after each commit,
// and the bus's Subscribe delivers the events published by every process, its own included.
// Package redisbus implements it over Redis Pub/Sub.
type Bus interface {
	Notifier

	// Publish sends events to the processes subscribed to the bus. Delivery is best effort. The
	// error reports events that could not be sent; a store never fails the write that produced
	// them because of it.
	Publish(ctx context.Context, events []Event) error
}

// TxWriter is a [Writer] bound to an application's transaction, such as the TxWriter of pgstore,
// mysqlstore and sqlitestore, or a memstore.Tx. Code that enqueues inside transactions can hold
// one without knowing the store.
type TxWriter interface {
	Writer

	// Notify is called after the application's transaction commits. It admits the throttled jobs
	// of the keys the transaction touched and publishes [JobsReady] for the queues that received
	// jobs. Its error is advisory: the jobs are committed either way, and without Notify they wait
	// for the next Sweep and the servers' next poll. Calling it twice, or after a rollback, is
	// harmless.
	Notify(ctx context.Context) error
}

// Console is implemented by stores that keep the console of a job: the lines its handler writes
// and its progress. Servers write to it while a job runs, and the dashboard reads it.
type Console interface {
	// WriteConsole appends lines to the console of the job ref points to and, when progress is 0
	// or more, sets the job's progress, while the job is processing under ref's claim; otherwise
	// it fails with [ErrLost] and writes nothing. Each line gets the job's Attempt, the store's
	// time and the next Seq of the job.
	WriteConsole(ctx context.Context, ref Ref, lines []string, progress int) error

	// Logs returns the lines of job id with a Seq above after, oldest first, at most limit of
	// them, or 100 when limit is 0 or less. Lines keep their Attempt across retries and requeues,
	// and [Coordinator.Prune] deletes them with the job.
	Logs(ctx context.Context, id int64, after int64, limit int) ([]LogLine, error)
}

// Transactor is implemented by stores that can run several writes in one transaction. Package kiln
// uses it to insert a batch together with its jobs.
type Transactor interface {
	// InTx calls fn with a Writer bound to a new transaction, and commits the transaction if fn
	// returns nil. Otherwise it rolls back and returns fn's error. Events are published after the
	// commit.
	InTx(ctx context.Context, fn func(w Writer) error) error
}

// LimitReader is implemented by stores that can list their limit keys with the jobs under each,
// for the Limits page of the dashboard. A store counts the jobs through its indexes, without
// scanning every job.
type LimitReader interface {
	// Limits returns limit keys after the given key, in key order, at most limit (0 or less: 100).
	// A key is listed from the first insert that names it until Prune deletes it.
	Limits(ctx context.Context, after string, limit int) ([]LimitInfo, error)
}
