package kiln

import (
	"math"
	"math/rand/v2"
	"time"
)

type InsertOption interface {
	insertOption()
}

type RecurringOption interface {
	recurringOption()
}

type HandleOption interface {
	handleOption()
}

type (
	Queue         string
	Priority      int16
	Delay         time.Duration
	At            time.Time
	MaxAttempts   int
	Timeout       time.Duration
	Tags          []string
	Meta          map[string]string
	After         []int64
	AfterFinished []int64
	AfterBatch    int64
	InBatch       int64
	Needs         []int
	Deadline      time.Time
	TZ            string
	Overlap       bool
	Misfire       uint8
)

type Unique struct {
	Key string
	For time.Duration
}

type Limit struct {
	Key string
	Max int
}

const (
	MisfireOnce Misfire = iota
	MisfireAll
	MisfireSkip
)

const NoTimeout Timeout = -1

func (Queue) insertOption()         {}
func (Priority) insertOption()      {}
func (Delay) insertOption()         {}
func (At) insertOption()            {}
func (MaxAttempts) insertOption()   {}
func (Timeout) insertOption()       {}
func (Tags) insertOption()          {}
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

type Backoff func(attempt int, err error) time.Duration

func Exponential(base, limit time.Duration) Backoff {
	return func(attempt int, _ error) time.Duration {
		d := float64(base) * math.Pow(2, float64(max(attempt-1, 0)))
		if d > float64(limit) || math.IsInf(d, 0) {
			d = float64(limit)
		}
		return jitter(time.Duration(d))
	}
}

func Constant(d time.Duration) Backoff {
	return func(int, error) time.Duration { return d }
}

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
