package kiln

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rafaelaugustos/kiln/memstore"
)

func TestJobParentOutputs(t *testing.T) {
	t.Parallel()
	c := NewClient(memstore.New())
	m := NewMux()
	m.HandleFunc("step", func(_ context.Context, j *RawJob) error {
		var a testArgs
		if err := json.Unmarshal(j.Args, &a); err != nil {
			return err
		}
		switch a.N {
		case 1:
			return j.SetOutput(map[string]int{"n": 1})
		case 3:
			return Permanent(errors.New("boom"))
		}
		return nil
	})
	var raw map[int64]json.RawMessage
	m.Use(func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, j *RawJob) error {
			if j.Kind == "ping" {
				var err error
				if raw, err = j.ParentOutputs(ctx); err != nil {
					return err
				}
			}
			return next(ctx, j)
		}
	})
	got := make(chan map[int64]json.RawMessage, 1)
	Handle(m, func(ctx context.Context, j *Job[ping]) error {
		out, err := j.ParentOutputs(ctx)
		if err != nil {
			return err
		}
		got <- out
		return nil
	})
	runServer(t, c, m, fastConfig())

	a := mustEnqueue(t, c, testArgs{K: "step", N: 1})
	b := mustEnqueue(t, c, testArgs{K: "step", N: 2})
	f := mustEnqueue(t, c, testArgs{K: "step", N: 3}, MaxAttempts(1))
	mustEnqueue(t, c, ping{}, AfterFinished{a, b, f})
	out := receive(t, got)
	if len(out) != 1 || string(out[a]) != `{"n":1}` {
		t.Fatalf("typed outputs = %v", out)
	}
	if len(raw) != 1 || string(raw[a]) != `{"n":1}` {
		t.Fatalf("raw outputs = %v", raw)
	}
	if st := getJob(t, c, f).State; st != Failed {
		t.Fatalf("failed parent is %s", st)
	}

	out, err := (&Job[ping]{Parents: []int64{a}}).ParentOutputs(context.Background())
	if err != nil || len(out) != 0 {
		t.Fatalf("outside a server: %v %v", out, err)
	}
}
