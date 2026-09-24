package pgstore_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/pgstore"
)

type ping struct{ N int }

func (ping) Kind() string { return "ping" }

type starts struct {
	mu sync.Mutex
	at []time.Time
}

func (s *starts) add(at time.Time) {
	s.mu.Lock()
	s.at = append(s.at, at)
	s.mu.Unlock()
}

func (s *starts) take() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	at := s.at
	s.at = nil
	slices.SortFunc(at, time.Time.Compare)
	return at
}

func unsynced(t *testing.T) *pgstore.Store {
	t.Helper()
	c := newCluster(t)
	cfg := c.pool.Config()
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s, err := pgstore.New(context.Background(), pool, pgstore.Schema(c.schema))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestE2ERateLimit(t *testing.T) {
	st := unsynced(t)
	var rec starts
	m := kiln.NewMux()
	kiln.Handle(m, func(context.Context, *kiln.Job[ping]) error {
		rec.add(time.Now())
		return nil
	})
	srv, _ := serve(t, st, m, fast())
	waitFor(t, 5*time.Second, "listen", func() bool { return srv.Stats().Listening })
	cl := kiln.NewClient(st)

	var err error
	for attempt := range 3 {
		if err = rateLimited(t, cl, &rec); err == nil {
			return
		}
		t.Logf("attempt %d: %v", attempt+1, err)
	}
	t.Fatal(err)
}

func rateLimited(t *testing.T, cl *kiln.Client, rec *starts) error {
	t.Helper()
	const (
		jobs   = 30
		gap    = 50 * time.Millisecond
		window = 200 * time.Millisecond
		most   = 5
	)
	limit := kiln.Limit{Key: "api", Rate: 20, Per: time.Second, Burst: 1}
	specs := make([]kiln.Spec, jobs)
	for i := range specs {
		specs[i] = kiln.Spec{Args: ping{N: i}, Options: []kiln.InsertOption{limit}}
	}
	res, err := cl.EnqueueMany(context.Background(), specs...)
	if err != nil {
		t.Fatal(err)
	}
	slots := make([]time.Time, len(res))
	for i, r := range res {
		slots[i] = waitState(t, cl, r.ID, kiln.Succeeded).RunAt
	}
	slices.SortFunc(slots, time.Time.Compare)
	at := rec.take()
	if len(at) != jobs {
		t.Fatalf("%d handler starts for %d jobs", len(at), jobs)
	}
	for i := 1; i < len(slots); i++ {
		if d := slots[i].Sub(slots[i-1]); d < gap-time.Millisecond {
			t.Fatalf("reserved slots %d and %d are %v apart, want at least %v", i-1, i, d, gap)
		}
	}
	offsets := make([]time.Duration, len(at))
	for i := range at {
		offsets[i] = at[i].Sub(at[0]).Round(time.Millisecond)
	}
	for i, j := 0, 0; i < len(at); i++ {
		for j < len(at) && at[j].Sub(at[i]) < window {
			j++
		}
		if j-i > most {
			return fmt.Errorf("%d starts within %v of start %d, want at most %d; starts at %v", j-i, window, i, most, offsets)
		}
	}
	return nil
}
