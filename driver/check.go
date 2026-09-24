package driver

import (
	"encoding/json"
	"fmt"
)

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
		case p.LimitKey != "" && p.LimitMax < 1:
			return fmt.Errorf("%w: job %d limit max %d", ErrInvalid, i, p.LimitMax)
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
