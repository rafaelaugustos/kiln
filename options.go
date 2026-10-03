package kiln

import (
	"math"
	"math/rand/v2"
	"time"
)

// An InsertOption sets a property of a job at insert time. Options apply in order, after those
// from the args' InsertOptions method, and a later option replaces an earlier one of the same type;
// Meta merges key by key instead, and Tags, After, AfterFinished and Needs add up. A nil option is
// ignored.
type InsertOption interface {
	insertOption()
}

// A RecurringOption configures [Client.SetRecurring]: [TZ], [Misfire] and [Overlap] for the
// schedule, and [Queue], [Priority], [MaxAttempts], [Timeout], [Tags], [Meta] and [Limit] for the
// jobs it inserts.
type RecurringOption interface {
	recurringOption()
}

// A HandleOption configures the handler of a kind: [Timeout] or [Backoff].
type HandleOption interface {
	handleOption()
}

type (
	// Queue is the queue a job goes to, [DefaultQueue] when omitted. Names are 1 to 64 bytes of
	// lowercase letters, digits, '_', '.', ':' and '-'.
	Queue string

	// Priority orders the jobs of a queue: higher is claimed first, and jobs of equal priority are
	// claimed in insert order. It is 0 when omitted.
	Priority int16

	// Delay makes a job wait that long, measured on the store's clock, before it can run. It
	// replaces an earlier [At]; a negative Delay means no wait.
	Delay time.Duration

	// At makes a job wait until the given time. It replaces an earlier [Delay]; a time in the past
	// means no wait.
	At time.Time

	// MaxAttempts is how many attempts a job gets before it fails, [DefaultMaxAttempts] when
	// omitted. It must be at least 1.
	MaxAttempts int

	// Timeout bounds each attempt of a job: when it expires, the handler's context is canceled with
	// [ErrTimeout]. A job's own Timeout comes first, then the one its kind was registered with,
	// then [ServerConfig.Timeout]. [NoTimeout] removes the limit.
	Timeout time.Duration

	// Tags are labels stored with a job and shown in the dashboard. Tags from several options add
	// up, without duplicates, to at most 32 tags of 1 to 64 bytes.
	Tags []string

	// Title describes a job in a few words, which the dashboard shows in place of the kind. It is
	// cut to 200 bytes. A job inserted without it takes the title its args return from a method
	// Title() string, when they have one.
	Title string

	// Meta is string metadata stored with a job, which handlers read in [Job.Meta]. Keys from
	// several options are merged, the later value winning, up to 64 keys and 16 KiB in all. kiln
	// keeps its own entries under keys that start with "kiln.".
	Meta map[string]string

	// After makes a job wait until the jobs with these ids have succeeded. If one of them is
	// deleted, the job is deleted too; if one fails, the job keeps waiting, since the failed job
	// can still be requeued. An id that does not exist fails the insert with [ErrNotFound].
	After []int64

	// AfterFinished is [After] for any outcome: a job waits until each of the jobs with these ids
	// has succeeded, failed or been deleted, and then runs.
	AfterFinished []int64

	// AfterBatch makes a job wait until the batch with this id has finished; see [Batch]. A batch
	// that does not exist fails the insert with [ErrNotFound].
	AfterBatch int64

	// InBatch adds a job to a batch created with [Client.OpenBatch]. The insert fails with
	// [ErrClosed] once the batch has finished.
	InBatch int64

	// Needs makes a job wait until other jobs inserted in the same call, given by their index in
	// it, have succeeded, with the rules of [After]. The call is a [Client.EnqueueMany], or the
	// jobs or the continuations of a [Batch]; [Flow.Add] returns the indexes. [Client.Enqueue]
	// rejects Needs with [ErrInvalid].
	Needs []int

	// Deadline is the time after which a job must not start. A server that claims the job later
	// deletes it without running it, whether it is the first attempt or a retry. The check uses
	// the server's clock.
	Deadline time.Time

	// TZ is the IANA time zone a recurring spec is evaluated in, such as "America/Sao_Paulo", UTC
	// when omitted. "Local" is rejected, since servers may not agree on it.
	TZ string

	// Overlap says whether a recurring job may insert an occurrence while the job of an earlier one
	// has not succeeded, failed or been deleted. It is true when omitted; Overlap(false) skips such
	// occurrences.
	Overlap bool

	// Misfire says what a recurring job does about occurrences it missed, for instance while no
	// server was running. It is [MisfireOnce] when omitted.
	Misfire uint8
)

// Unique keeps a job out of the store while another job of the same kind holds the same Key;
// [Client.Enqueue] returns the holder's id instead. An empty Key stands for the job's encoded args.
//
// With For zero, a job holds its key until it succeeds, fails or is deleted. With For positive,
// the key is held for that long after the insert, whatever happens to the job, so it means "at
// most one job per For" rather than "no duplicates while one is pending".
//
// With Replace, a duplicate updates the holder while it has not started: the holder takes the
// duplicate's args, meta, tags, title, priority and run time. A holder that is already due takes
// only a run time in the future, and goes back to waiting for it, and a holder that a [Limit] has
// given a start or a slot keeps it. Debounce makes the job wait that long, in place of [Delay] or
// [At], and each duplicate inserted while the holder is still [Scheduled], with no start given by
// a Limit, replaces it and pushes its run time Debounce from now, so the job runs Debounce after
// the last insert. Replace and Debounce together are [ErrInvalid].
type Unique struct {
	Key      string
	For      time.Duration
	Replace  bool
	Debounce time.Duration
}

// Limit holds back jobs that share Key, across all servers and queues. Key defaults to the kind.
//
// Max caps how many of these jobs are enqueued or processing at the same time; the others wait in
// [Throttled]. Rate caps how many start per Per, 1s when zero, and Burst how many of them can
// start back to back, Rate when zero. A rate-limited job is given its start time when it is
// admitted, so a backlog of any size is released at that pace and each job is written once. The
// pace is kept over admissions, the moments jobs become claimable: if the database stalls between
// an admission and its commit, the jobs admitted just before the stall can start together with
// those admitted right after it. Max and Rate combine: Max bounds how many run together, Rate how
// often they start. With Rate zero, Max zero means 1 and the key works as a mutex; with a Rate, it
// means no cap on concurrency.
//
// The numbers belong to the key: the job inserted last with the key sets them for all of its jobs,
// and a change applies to start times given out after it.
type Limit struct {
	Key   string
	Max   int
	Rate  int
	Per   time.Duration
	Burst int
}

const (
	MisfireOnce Misfire = iota // one job for the latest missed occurrence
	MisfireAll                 // one job for every missed occurrence, 100 at a time
	MisfireSkip                // no job unless the latest occurrence is at most a minute late
)

// NoTimeout removes the time limit of a job, a kind or a server; see [Timeout].
const NoTimeout Timeout = -1

func (Queue) insertOption()         {}
func (Priority) insertOption()      {}
func (Delay) insertOption()         {}
func (At) insertOption()            {}
func (MaxAttempts) insertOption()   {}
func (Timeout) insertOption()       {}
func (Tags) insertOption()          {}
func (Title) insertOption()         {}
func (Meta) insertOption()          {}
func (Unique) insertOption()        {}
func (Limit) insertOption()         {}
func (After) insertOption()         {}
func (AfterFinished) insertOption() {}
func (AfterBatch) insertOption()    {}
func (InBatch) insertOption()       {}
func (Needs) insertOption()         {}
func (Deadline) insertOption()      {}

func (Queue) recurringOption()       {}
func (Priority) recurringOption()    {}
func (MaxAttempts) recurringOption() {}
func (Timeout) recurringOption()     {}
func (Tags) recurringOption()        {}
func (Meta) recurringOption()        {}
func (Limit) recurringOption()       {}
func (TZ) recurringOption()          {}
func (Misfire) recurringOption()     {}
func (Overlap) recurringOption()     {}

func (Timeout) handleOption() {}
func (Backoff) handleOption() {}

// Backoff returns how long a job waits before its next attempt, given the number of the attempt
// that failed, from 1, and its error. A negative result means no wait. A server uses the Backoff
// its kind was registered with, then [ServerConfig.Backoff].
type Backoff func(attempt int, err error) time.Duration

// Exponential returns a Backoff of base after the first attempt, doubling with each attempt up to
// limit. The wait is picked at random between half of that value and all of it.
func Exponential(base, limit time.Duration) Backoff {
	return func(attempt int, _ error) time.Duration {
		d := float64(base) * math.Pow(2, float64(max(attempt-1, 0)))
		if d > float64(limit) || math.IsInf(d, 0) {
			d = float64(limit)
		}
		return jitter(time.Duration(d))
	}
}

// Constant returns a Backoff that always waits d.
func Constant(d time.Duration) Backoff {
	return func(int, error) time.Duration { return d }
}

// Delays returns a Backoff that waits ds[0] after the first attempt, ds[1] after the second, and
// the last delay after every attempt beyond the list. With no delays it returns the default
// described at [ServerConfig.Backoff].
func Delays(ds ...time.Duration) Backoff {
	if len(ds) == 0 {
		return defaultBackoff
	}
	return func(attempt int, _ error) time.Duration {
		return ds[min(max(attempt-1, 0), len(ds)-1)]
	}
}

func defaultBackoff(attempt int, _ error) time.Duration {
	n := float64(max(attempt-1, 0))
	secs := math.Pow(n, 4) + 15 + float64(rand.IntN(30))*float64(attempt)
	return time.Duration(secs * float64(time.Second))
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d/2 + time.Duration(rand.Int64N(int64(d)/2+1))
}
