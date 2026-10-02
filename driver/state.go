package driver

// State is the state of a job. The package documentation describes how jobs move between states.
type State string

// The job states, in lifecycle order.
const (
	Awaiting   State = "awaiting"   // waiting for parent jobs or a batch
	Scheduled  State = "scheduled"  // waiting for its run time
	Throttled  State = "throttled"  // waiting for room under its limit
	Enqueued   State = "enqueued"   // ready to be claimed
	Processing State = "processing" // claimed by a server
	Succeeded  State = "succeeded"  // done, archived
	Failed     State = "failed"     // out of attempts or failed for good, until requeued or deleted
	Deleted    State = "deleted"    // deleted, canceled or doomed by a parent, archived
)

// States lists every state, in the same order as the constants.
var States = [...]State{Awaiting, Scheduled, Throttled, Enqueued, Processing, Succeeded, Failed, Deleted}

// Valid reports whether s is one of the eight states.
func (s State) Valid() bool {
	for _, v := range States {
		if v == s {
			return true
		}
	}
	return false
}

// Archived reports whether s is Succeeded or Deleted. Archived jobs change state again only if an
// operator requeues them.
func (s State) Archived() bool {
	return s == Succeeded || s == Deleted
}

// Live reports whether s is a valid state that is not archived. Failed is live.
func (s State) Live() bool {
	return s.Valid() && !s.Archived()
}

// Mask is a set of final states. A [Parent] uses it to say which outcomes of the parent release
// the child.
type Mask uint8

// Masks for [Parent.On]. Package kiln uses OnSucceeded for After and Needs, and OnFinished for
// AfterFinished.
const (
	OnSucceeded Mask = 1 << iota
	OnFailed
	OnDeleted

	OnFinished = OnSucceeded | OnFailed | OnDeleted // any final state
)

// Has reports whether s is in m. Only Succeeded, Failed and Deleted can be.
func (m Mask) Has(s State) bool {
	switch s {
	case Succeeded:
		return m&OnSucceeded != 0
	case Failed:
		return m&OnFailed != 0
	case Deleted:
		return m&OnDeleted != 0
	}
	return false
}

// Misfire is what a recurring job does about occurrences it missed. The store only keeps it;
// package kiln applies it when it plans a [Fire].
type Misfire uint8

const (
	MisfireOnce Misfire = iota // one job for the latest missed occurrence
	MisfireAll                 // one job for every missed occurrence
	MisfireSkip                // no job unless the latest occurrence is at most a minute late
)

// EventKind is the kind of an [Event].
type EventKind uint8

const (
	// Resync means events may have been lost. A server answers it with a heartbeat and a look at
	// every one of its queues.
	Resync EventKind = iota + 1

	// JobsReady means that Queue received enqueued jobs, or that admission reserved a start time
	// for one of its jobs.
	JobsReady

	// CancelRequested means that the processing job ID was deleted and the server running it
	// should cancel it.
	CancelRequested

	// QueueChanged means that Queue was paused or resumed.
	QueueChanged
)
