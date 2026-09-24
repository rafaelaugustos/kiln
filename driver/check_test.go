package driver

import (
	"errors"
	"testing"
)

func TestCheckInsert(t *testing.T) {
	ok := func() InsertParams {
		return InsertParams{Kind: "k", Queue: "q", MaxAttempts: 1, Args: []byte(`{}`)}
	}
	with := func(f func(*InsertParams)) InsertParams {
		p := ok()
		f(&p)
		return p
	}
	need := func(i int) Parent { return Parent{Index: i, On: OnSucceeded} }
	tests := []struct {
		name string
		jobs []InsertParams
		ok   bool
	}{
		{"valid", []InsertParams{ok()}, true},
		{"empty call", nil, true},
		{"no kind", []InsertParams{with(func(p *InsertParams) { p.Kind = "" })}, false},
		{"no queue", []InsertParams{with(func(p *InsertParams) { p.Queue = "" })}, false},
		{"zero attempts", []InsertParams{with(func(p *InsertParams) { p.MaxAttempts = 0 })}, false},
		{"no args", []InsertParams{with(func(p *InsertParams) { p.Args = nil })}, false},
		{"bad json", []InsertParams{with(func(p *InsertParams) { p.Args = []byte(`{`) })}, false},
		{"limit without max", []InsertParams{with(func(p *InsertParams) { p.LimitKey = "x" })}, false},
		{"limit", []InsertParams{with(func(p *InsertParams) { p.LimitKey, p.LimitMax = "x", 2 })}, true},
		{"negative batch", []InsertParams{with(func(p *InsertParams) { p.BatchID = -1 })}, false},
		{"negative after batch", []InsertParams{with(func(p *InsertParams) { p.AfterBatch = -1 })}, false},
		{"own batch", []InsertParams{with(func(p *InsertParams) { p.BatchID, p.AfterBatch = 7, 7 })}, false},
		{"other batch", []InsertParams{with(func(p *InsertParams) { p.BatchID, p.AfterBatch = 7, 8 })}, true},
		{"parent without mask", []InsertParams{with(func(p *InsertParams) { p.Parents = []Parent{{ID: 3}} })}, false},
		{"negative parent", []InsertParams{with(func(p *InsertParams) { p.Parents = []Parent{{ID: -3, On: OnSucceeded}} })}, false},
		{"existing parent", []InsertParams{with(func(p *InsertParams) { p.Parents = []Parent{{ID: 3, On: OnFinished}} })}, true},
		{"self", []InsertParams{with(func(p *InsertParams) { p.Parents = []Parent{need(0)} })}, false},
		{"out of range", []InsertParams{ok(), with(func(p *InsertParams) { p.Parents = []Parent{need(2)} })}, false},
		{"negative index", []InsertParams{with(func(p *InsertParams) { p.Parents = []Parent{need(-1)} })}, false},
		{"chain", []InsertParams{ok(), with(func(p *InsertParams) { p.Parents = []Parent{need(0)} }), with(func(p *InsertParams) { p.Parents = []Parent{need(1)} })}, true},
		{"forward", []InsertParams{with(func(p *InsertParams) { p.Parents = []Parent{need(1)} }), ok()}, true},
		{"fan in", []InsertParams{ok(), ok(), with(func(p *InsertParams) { p.Parents = []Parent{need(0), need(1)} })}, true},
		{"cycle", []InsertParams{with(func(p *InsertParams) { p.Parents = []Parent{need(1)} }), with(func(p *InsertParams) { p.Parents = []Parent{need(0)} })}, false},
		{"long cycle", []InsertParams{with(func(p *InsertParams) { p.Parents = []Parent{need(2)} }), with(func(p *InsertParams) { p.Parents = []Parent{need(0)} }), with(func(p *InsertParams) { p.Parents = []Parent{need(1)} })}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckInsert(tt.jobs)
			if tt.ok && err != nil {
				t.Fatalf("CheckInsert: %v", err)
			}
			if !tt.ok && !errors.Is(err, ErrInvalid) {
				t.Fatalf("CheckInsert = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestCheckFilter(t *testing.T) {
	tests := []struct {
		name string
		f    Filter
		ok   bool
	}{
		{"empty", Filter{}, false},
		{"only queue", Filter{Queue: "q"}, false},
		{"ids", Filter{IDs: []int64{1}}, true},
		{"state", Filter{State: Failed}, true},
		{"bad state", Filter{State: "done"}, false},
		{"ids and bad state", Filter{IDs: []int64{1}, State: "done"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckFilter(tt.f)
			if tt.ok && err != nil {
				t.Fatalf("CheckFilter: %v", err)
			}
			if !tt.ok && !errors.Is(err, ErrInvalid) {
				t.Fatalf("CheckFilter = %v, want ErrInvalid", err)
			}
		})
	}
}
