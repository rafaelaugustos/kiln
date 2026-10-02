package driver

import (
	"encoding/json"
	"fmt"
)

// CheckInsert makes the checks on a call to [Writer.Insert] that need no database; stores run it
// before anything else. Every job must have a kind, a queue, MaxAttempts of at least 1 and args
// that are valid JSON. Limit fields must not be negative, a limit key needs LimitMax or LimitRate
// of at least 1, and a rate needs a positive LimitPer and a LimitBurst of at least 1. Batch ids
// must not be negative, and a job cannot wait for the batch it joins. A parent needs a non-empty
// On and either a positive ID or the Index of another job in the call, with no cycle among them.
// The error wraps [ErrInvalid] and names the job.
func CheckInsert(jobs []InsertParams) error {
	refs := false
	for i := range jobs {
		p := &jobs[i]
		switch {
		case p.Kind == "":
			return fmt.Errorf("%w: job %d has no kind", ErrInvalid, i)
		case p.Queue == "":
			return fmt.Errorf("%w: job %d has no queue", ErrInvalid, i)
		case p.MaxAttempts < 1:
			return fmt.Errorf("%w: job %d max attempts %d", ErrInvalid, i, p.MaxAttempts)
		case !json.Valid(p.Args):
			return fmt.Errorf("%w: job %d args are not valid JSON", ErrInvalid, i)
		case p.LimitMax < 0 || p.LimitRate < 0 || p.LimitPer < 0 || p.LimitBurst < 0:
			return fmt.Errorf("%w: job %d limit %q has a negative field", ErrInvalid, i, p.LimitKey)
		case p.LimitKey != "" && p.LimitMax < 1 && p.LimitRate < 1:
			return fmt.Errorf("%w: job %d limit %q has neither max nor rate", ErrInvalid, i, p.LimitKey)
		case p.LimitRate > 0 && (p.LimitPer <= 0 || p.LimitBurst < 1):
			return fmt.Errorf("%w: job %d limit %q rate needs a period and a burst of at least 1", ErrInvalid, i, p.LimitKey)
		case p.BatchID < 0 || p.AfterBatch < 0:
			return fmt.Errorf("%w: job %d batch %d after batch %d", ErrInvalid, i, p.BatchID, p.AfterBatch)
		case p.BatchID != 0 && p.BatchID == p.AfterBatch:
			return fmt.Errorf("%w: job %d waits for its own batch %d", ErrInvalid, i, p.BatchID)
		}
		for _, par := range p.Parents {
			switch {
			case par.ID < 0 || par.On&OnFinished == 0:
				return fmt.Errorf("%w: job %d parent %+v", ErrInvalid, i, par)
			case par.ID > 0:
			case par.Index < 0 || par.Index >= len(jobs) || par.Index == i:
				return fmt.Errorf("%w: job %d parent index %d", ErrInvalid, i, par.Index)
			default:
				refs = true
			}
		}
	}
	if refs {
		return acyclic(jobs)
	}
	return nil
}

// CheckFilter returns an error wrapping [ErrInvalid] unless f names ids or a valid state, as
// [Admin.Delete] and [Admin.Requeue] require.
func CheckFilter(f Filter) error {
	if len(f.IDs) == 0 && f.State == "" {
		return fmt.Errorf("%w: filter needs ids or a state", ErrInvalid)
	}
	if f.State != "" && !f.State.Valid() {
		return fmt.Errorf("%w: state %q", ErrInvalid, f.State)
	}
	return nil
}

func acyclic(jobs []InsertParams) error {
	const (
		unseen = iota
		open
		closed
	)
	color := make([]uint8, len(jobs))
	var visit func(i int) bool
	visit = func(i int) bool {
		color[i] = open
		for _, par := range jobs[i].Parents {
			if par.ID != 0 {
				continue
			}
			switch color[par.Index] {
			case open:
				return false
			case unseen:
				if !visit(par.Index) {
					return false
				}
			}
		}
		color[i] = closed
		return true
	}
	for i := range jobs {
		if color[i] == unseen && !visit(i) {
			return fmt.Errorf("%w: dependency cycle through job %d", ErrInvalid, i)
		}
	}
	return nil
}
