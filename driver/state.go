package driver

type State string

const (
	Awaiting   State = "awaiting"
	Scheduled  State = "scheduled"
	Throttled  State = "throttled"
	Enqueued   State = "enqueued"
	Processing State = "processing"
	Succeeded  State = "succeeded"
	Failed     State = "failed"
	Deleted    State = "deleted"
)

var States = [...]State{Awaiting, Scheduled, Throttled, Enqueued, Processing, Succeeded, Failed, Deleted}

func (s State) Valid() bool {
	for _, v := range States {
		if v == s {
			return true
		}
	}
	return false
}

func (s State) Archived() bool {
	return s == Succeeded || s == Deleted
}

func (s State) Live() bool {
	return s.Valid() && !s.Archived()
}

type Mask uint8

const (
	OnSucceeded Mask = 1 << iota
	OnFailed
	OnDeleted

	OnFinished = OnSucceeded | OnFailed | OnDeleted
)

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

type Misfire uint8

const (
	MisfireOnce Misfire = iota
	MisfireAll
	MisfireSkip
)

type EventKind uint8

const (
	Resync EventKind = iota + 1
	JobsReady
	CancelRequested
	QueueChanged
)
