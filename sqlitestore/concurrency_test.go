package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type ledger struct {
	mu     sync.Mutex
	claims map[int64][]int32
}

func (l *ledger) add(t *testing.T, js []driver.Job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, j := range js {
		prev := l.claims[j.ID]
		if n := len(prev); n > 0 && prev[n-1] >= j.Claim {
			t.Errorf("job %d claimed as %d after %d", j.ID, j.Claim, prev[n-1])
		}
		l.claims[j.ID] = append(prev, j.Claim)
	}
}

func work(ctx context.Context, t *testing.T, s *Store, server string, l *ledger, done *atomic.Int64, total int64) {
	for i := 0; done.Load() < total; i++ {
		js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: 1 + i%16, Server: server})
		if err != nil {
			t.Errorf("claim: %v", err)
			return
		}
		if len(js) == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		l.add(t, js)
		outs := make([]driver.Outcome, len(js))
		final := 0
		for k, j := range js {
			outs[k] = driver.Outcome{Ref: j.Ref, State: driver.Succeeded}
			if j.ID%5 == 0 && j.Attempt == 1 {
				outs[k] = driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Reason: "retry"}
				continue
			}
			final++
		}
		rs, err := s.Finish(ctx, server, outs)
		if err != nil {
			t.Errorf("finish: %v", err)
			return
		}
		for k, r := range rs {
			if r != driver.Applied {
				t.Errorf("finish job %d: result %d", outs[k].ID, r)
			}
		}
		done.Add(int64(final))
	}
}

func fill(t *testing.T, s *Store, n int) {
	t.Helper()
	for left := n; left > 0; left -= 500 {
		ps := make([]driver.InsertParams, min(left, 500))
		for i := range ps {
			ps[i] = job("c", func(p *driver.InsertParams) { p.Priority = int16(i % 4) })
		}
		insert(t, s, ps...)
	}
}

func verify(t *testing.T, s *Store, l *ledger, n int) {
	t.Helper()
	if len(l.claims) != n {
		t.Fatalf("claimed %d distinct jobs, want %d", len(l.claims), n)
	}
	for id, cs := range l.claims {
		want := 1
		if id%5 == 0 {
			want = 2
		}
		if len(cs) != want {
			t.Fatalf("job %d claimed %d times (%v), want %d", id, len(cs), cs, want)
		}
	}
	c, err := s.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.Succeeded != int64(n) || c.Enqueued+c.Processing+c.Scheduled != 0 {
		t.Fatalf("counts %+v, want %d succeeded and nothing left", c, n)
	}
}

func TestConcurrentClaimFinish(t *testing.T) {
	t.Parallel()
	s := open(t)
	s.db.SetMaxOpenConns(8)
	const n = 3000
	fill(t, s, n)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	l := &ledger{claims: make(map[int64][]int32)}
	var (
		done atomic.Int64
		wg   sync.WaitGroup
	)
	for w := range 16 {
		wg.Go(func() { work(ctx, t, s, fmt.Sprint("w", w), l, &done, n) })
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := s.Counts(ctx); err != nil {
				t.Errorf("counts: %v", err)
				return
			}
			if _, err := s.Jobs(ctx, driver.JobQuery{State: driver.Enqueued, Limit: 50}); err != nil {
				t.Errorf("jobs: %v", err)
				return
			}
		}
	})
	readers.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if _, err := s.Promote(ctx, 100); err != nil {
				t.Errorf("promote: %v", err)
				return
			}
			if _, err := s.Heartbeat(ctx, driver.ServerInfo{ID: "w0"}); err != nil {
				t.Errorf("heartbeat: %v", err)
				return
			}
		}
	})
	wg.Wait()
	close(stop)
	readers.Wait()
	if t.Failed() {
		return
	}
	verify(t, s, l, n)
}

func TestTwoProcesses(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "shared.db")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var stores []*Store
	for range 2 {
		db := connect(t, path)
		db.SetMaxOpenConns(4)
		s, err := New(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		stores = append(stores, s)
	}
	if stores[0].w == stores[1].w {
		t.Fatal("separate *sql.DB handles share a write lock")
	}
	const n = 1500
	l := &ledger{claims: make(map[int64][]int32)}
	var (
		done     atomic.Int64
		wg       sync.WaitGroup
		inserted atomic.Int64
	)
	for p, s := range stores {
		wg.Go(func() {
			for inserted.Add(100) <= n {
				ps := make([]driver.InsertParams, 100)
				for i := range ps {
					ps[i] = job("c")
				}
				if _, err := s.Insert(ctx, ps); err != nil {
					t.Errorf("insert: %v", err)
					return
				}
			}
		})
		for w := range 4 {
			wg.Go(func() { work(ctx, t, s, fmt.Sprintf("p%dw%d", p, w), l, &done, n) })
		}
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	verify(t, stores[0], l, n)
}

func holdLock(t *testing.T, path string) *sql.Conn {
	t.Helper()
	ctx := context.Background()
	c, err := connect(t, path).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if _, err := c.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "INSERT INTO kiln_queues (name, paused, updated_at) VALUES ('held', 0, 0)"); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestBusyRetry(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "busy.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(20)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	s, err := New(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	c := holdLock(t, path)
	done := make(chan error, 1)
	go func() {
		_, err := s.Insert(ctx, []driver.InsertParams{job("a")})
		done <- err
	}()
	short, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := s.Claim(short, driver.ClaimQuery{Queues: []string{"default"}, Limit: 1, Server: "srv"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("claim behind a held write lock: %v, want the context error", err)
	}
	select {
	case err := <-done:
		t.Fatalf("insert returned %v while another connection held the write lock", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := c.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("insert after the lock was released: %v", err)
	}
	if js := claim(t, s, 1); len(js) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(js))
	}
}
