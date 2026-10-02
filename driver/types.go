package driver

import "time"

// Ref identifies one claim of a job: the job's id and the value its claim counter took when it was
// claimed. Writes made for a running job carry the Ref and apply only while the job is processing
// under the same claim.
type Ref struct {
	ID    int64
	Claim int32
}

// Parent is a dependency of a job on another job. ID names an existing job, and the insert fails
// with [ErrNotFound] if there is none. With ID 0, Index is the position of the parent among the
// jobs of the same Insert call, before or after the child, but never the child itself or a cycle.
//
// The dependency is resolved when the parent reaches a state in On. A parent that ends succeeded
// or deleted outside On dooms the child, which is archived as deleted with the history reason
// "parent <id> <state>"; a parent that fails outside On leaves the dependency pending, since the
// parent may still be requeued. When nothing is pending, the child leaves awaiting for scheduled,
// throttled or enqueued, as it would have at insert. A transition only has to settle the parent's
// direct children; [Coordinator.Sweep] completes longer chains.
type Parent struct {
	ID    int64
	Index int
	On    Mask
}

// InsertParams describes a job to insert. Package kiln builds it from the args and options of an
// enqueue call.
type InsertParams struct {
	Kind        string
	Queue       string
	Args        []byte // JSON
	Meta        map[string]string
	Tags        []string
	Priority    int16 // within a queue, higher is claimed first
	MaxAttempts int
	Timeout     time.Duration // per attempt; 0 leaves it to the handler or server, negative means none

	// RunAt is when the job may run. When it is zero, the job may run Delay after the store's now.
	RunAt time.Time
	Delay time.Duration

	// UniqueKey, when set, is an opaque key that one job at a time may hold; package kiln sends a
	// 16-byte hash. With UniqueFor zero, the key is held while the job is live and not failed, and
	// released when it succeeds, fails or is deleted. With UniqueFor positive, it is held for that
	// long after the insert, whatever becomes of the job. A job whose key is held is not inserted:
	// Insert reports it as a duplicate of the holder, as it does for a job that repeats the key of
	// an earlier job in the same call.
	UniqueKey []byte
	UniqueFor time.Duration

	// LimitKey puts the job under the limit of that key, described in the package documentation.
	// LimitMax caps how many jobs of the key are enqueued or processing together, 0 meaning no cap,
	// and LimitRate jobs may start per LimitPer, up to LimitBurst of them back to back. The most
	// recent insert sets these numbers for every job of the key.
	LimitKey   string
	LimitMax   int
	LimitRate  int
	LimitPer   time.Duration
	LimitBurst int

	BatchID     int64    // batch the job joins; see [Batch]
	AfterBatch  int64    // batch whose end the job waits for
	Parents     []Parent // jobs the job waits for
	RecurringID string   // recurring job that created the job
}

// Inserted is the result of inserting one job.
type Inserted struct {
	ID        int64 // the new job, or the job that holds the unique key of a duplicate
	State     State // the state the job was inserted in, or the holder's state when known
	Duplicate bool  // the unique key was held and nothing was inserted
}

// Job is a job as a server receives it from [Worker.Claim].
type Job struct {
	Ref
	Kind        string
	Queue       string
	Args        []byte
	Meta        map[string]string
	Tags        []string
	Priority    int16
	Attempt     int // the number of this attempt, from 1
	MaxAttempts int
	Timeout     time.Duration
	RunAt       time.Time // when the job became due
	CreatedAt   time.Time
	AttemptedAt time.Time // when the job was last claimed
	BatchID     int64
	RecurringID string
	Parents     []int64 // ids of the parent jobs
	LimitKey    string
}

// Record is a job as the store keeps it, live or archived.
type Record struct {
	Job
	State           State
	FinalizedAt     time.Time // when the job became succeeded, failed or deleted; zero in other states
	Server          string    // the server that claimed it last
	CancelRequested bool      // deleted while processing, waiting for its server to cancel it
	AfterBatch      int64
	PendingDeps     int     // dependencies not resolved yet
	History         []Entry // oldest first
	Output          []byte  // JSON set by the attempt that succeeded
	Children        []int64 // jobs that depend on this one
}

// Entry is a line in a job's history. The store adds one for every outcome with a Reason and for
// every requeue and delete by an operator, and keeps the last 16, with Error cut to 2 KiB and
// Trace to 8 KiB. A success without a reason adds none.
type Entry struct {
	At      time.Time
	State   State // the state the job moved to
	Attempt int
	Reason  string // such as "retry", "snoozed" or "requeued"
	Error   string
	Trace   string // stack of a panic
	Server  string // the server that ran the attempt; empty for an operator's action
}

// Outcome is the result of an attempt, which [Worker.Finish] applies to the job.
type Outcome struct {
	Ref
	State  State         // Succeeded, Failed, Deleted, Scheduled or Enqueued
	Delay  time.Duration // for Scheduled, how long from now the job waits
	Refund bool          // give the attempt back, as a snooze or a shutdown does
	Reason string        // history reason; empty adds no entry
	Error  string
	Trace  string
	Output []byte // JSON stored with the job when not nil
}

// Result is what [Worker.Finish] did with an outcome. kiln drops Stale outcomes, sends Busy ones
// again in its next call, and rewrites a Rejected one, once, as a failure without output.
type Result uint8

const (
	Applied  Result = iota + 1 // stored
	Stale                      // the job is no longer processing under this claim
	Busy                       // another transaction holds the job's row locked
	Rejected                   // can never be stored, such as an Output that is not JSON
)

// ClaimQuery says what [Worker.Claim] may take.
type ClaimQuery struct {
	Queues []string // in order of preference
	Kinds  []string // only these kinds; empty for any
	Limit  int      // at most this many jobs
	Server string   // id of the claiming server
}

// ServerInfo describes a server, as it reports itself in every heartbeat.
type ServerInfo struct {
	ID          string
	Host        string
	PID         int
	Version     string // kiln's module version, such as "kiln/v0.3.1", or "kiln/devel"
	Queues      []string
	Kinds       []string // kinds the server has handlers for
	Workers     int
	Running     int
	StartedAt   time.Time
	HeartbeatAt time.Time // set by the store
}

// Lease is a job processing under a server, as [Worker.Heartbeat] reports it.
type Lease struct {
	Ref
	Cancel bool          // deletion was requested
	Age    time.Duration // time since the job was claimed
}

// Directives is what a server learns from a heartbeat.
type Directives struct {
	Leases []Lease  // every job processing under the server
	Paused []string // every paused queue
}

// Orphan is a processing job whose server stopped heartbeating, as [Coordinator.Orphans] returns
// it.
type Orphan struct {
	Ref
	Kind        string
	Queue       string
	Attempt     int
	MaxAttempts int
	Server      string
	Cancel      bool // deletion was requested
}

// Promoted is what [Coordinator.Promote] did.
type Promoted struct {
	Count  int           // jobs moved out of scheduled
	Queues []string      // queues that received enqueued jobs
	Next   time.Duration // time until the earliest job still scheduled; 0 when there is none
}

// Retention says how long finished jobs are kept, by state, counted from their FinalizedAt. A
// negative duration keeps them forever.
type Retention struct {
	Succeeded time.Duration
	Deleted   time.Duration
	Failed    time.Duration
}

// PruneParams says what [Coordinator.Prune] deletes.
type PruneParams struct {
	Retention
	Servers time.Duration // servers silent for longer than this; 0 means 1h
	Stats   time.Duration // per-minute statistics older than this; 0 means 14 days
	Limit   int           // rows per table and call; 0 means 1000
}

// Recurring is a recurring job as the store keeps it. Package kiln computes its occurrences; the
// store keeps the definition and applies each [Fire].
type Recurring struct {
	ID        string
	Spec      string       // cron spec, as normalized by package cron
	Location  string       // IANA time zone the spec is evaluated in
	Template  InsertParams // the job inserted at each occurrence
	Misfire   Misfire
	Overlap   bool
	Paused    bool
	NextRunAt time.Time // the next occurrence to fire; zero for none
	LastRunAt time.Time // the occurrence fired last
	LastJobID int64     // the job inserted last
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   int64 // bumped by the store on every write, for compare-and-swap
}

// Fire is one firing of a recurring job: the jobs to insert and the schedule to store, applied by
// [Coordinator.Fire] only if the recurring job is still at Version.
type Fire struct {
	ID        string
	Version   int64
	NextRunAt time.Time
	LastRunAt time.Time
	Jobs      []InsertParams // possibly none
}

// Filter selects the jobs that match every field that is set. [Admin.Delete] and [Admin.Requeue]
// need IDs or State.
type Filter struct {
	IDs         []int64
	State       State
	Queue       string
	Kind        string
	BatchID     int64
	RecurringID string
}

// JobQuery asks [Inspector.Jobs] for a page of jobs.
type JobQuery struct {
	State   State // required
	Queue   string
	Kind    string
	BatchID int64
	Limit   int    // page size, 20 when 0 and at most 500
	Cursor  string // Next of the previous page; empty for the first
}

// Page is a page of jobs.
type Page struct {
	Records []Record
	Next    string // cursor for the next page; empty means there are no more
}

// Counts summarizes the jobs of a store.
type Counts struct {
	Awaiting   int64
	Scheduled  int64
	Throttled  int64
	Enqueued   int64
	Processing int64
	Failed     int64
	Retries    int64 // scheduled jobs with an Attempt above zero
	Succeeded  int64 // all time, pruned jobs included
	Deleted    int64 // all time, pruned jobs included
	Capped     bool  // a count reached 100000 and stopped there
}

// Point is one bucket of [Inspector.Series], with the outcomes counted in it.
type Point struct {
	At        time.Time // start of the bucket, in UTC
	Succeeded int64
	Failed    int64
	Deleted   int64
	Retried   int64
}

// QueueInfo describes a queue.
type QueueInfo struct {
	Name       string
	Paused     bool
	Enqueued   int64
	Processing int64
	Scheduled  int64
	Throttled  int64
	Latency    time.Duration // how long the oldest enqueued job has been due; 0 when none is
}

// NewBatch describes a batch to open.
type NewBatch struct {
	Description string
	Meta        map[string]string
}

// Batch is a group of jobs. Jobs join it through [InsertParams.BatchID] until it finishes, which
// happens once, when it is sealed and every member has succeeded or been deleted; a failed member
// keeps it open. When it finishes, the jobs waiting for it through AfterBatch are released.
type Batch struct {
	ID          int64
	Description string
	Meta        map[string]string
	Total       int64 // jobs that ever joined
	Sealed      bool
	Counts      map[State]int64 // members by state, archived ones included
	CreatedAt   time.Time
	FinishedAt  time.Time // zero until the batch finishes
}

// BatchQuery asks [Inspector.Batches] for a page of batches.
type BatchQuery struct {
	Limit  int    // page size, 20 when 0 and at most 500
	Cursor string // Next of the previous page; empty for the first
}

// BatchPage is a page of batches.
type BatchPage struct {
	Batches []Batch
	Next    string // cursor for the next page; empty means there are no more
}

// Event is a notification from a store; see [EventKind].
type Event struct {
	Kind  EventKind
	Queue string // for JobsReady and QueueChanged
	ID    int64  // for CancelRequested
}
