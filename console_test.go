package kiln

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

type traced struct {
	*memstore.Store
	mu    sync.Mutex
	calls []string
}

func (s *traced) note(call string) {
	s.mu.Lock()
	s.calls = append(s.calls, call)
	s.mu.Unlock()
}

func (s *traced) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}

func (s *traced) WriteConsole(ctx context.Context, ref driver.Ref, lines []string, progress int) error {
	err := s.Store.WriteConsole(ctx, ref, lines, progress)
	s.note(fmt.Sprintf("write %q %d", lines, progress))
	return err
}

func (s *traced) Finish(ctx context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	s.note("finish")
	return s.Store.Finish(ctx, server, outs)
}

func TestConsoleFlushes(t *testing.T) {
	t.Parallel()
	st := &traced{Store: memstore.New()}
	c := NewClient(st)
	m := NewMux()
	Handle(m, func(ctx context.Context, j *Job[ping]) error {
		j.Logf("started %d\n", j.Args.N)
		j.SetProgress(40)
		for len(st.seen()) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			time.Sleep(5 * time.Millisecond)
		}
		j.Logf("finished")
		j.SetProgress(100)
		return nil
	})
	runServer(t, c, m, fastConfig())
	id := mustEnqueue(t, c, ping{N: 7})
	r := waitState(t, c, id, Succeeded)
	want := []string{`write ["started 7"] 40`, `write ["finished"] 100`, "finish"}
	if got := st.seen(); !slices.Equal(got, want) {
		t.Fatalf("store calls %q, want %q", got, want)
	}
	if r.Progress != 100 {
		t.Fatalf("progress %d, want 100", r.Progress)
	}
	ls, err := st.Logs(context.Background(), id, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 2 || ls[0].Text != "started 7" || ls[1].Text != "finished" || ls[1].Attempt != 1 {
		t.Fatalf("lines %+v", ls)
	}
}
