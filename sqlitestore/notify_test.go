package sqlitestore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type events struct {
	mu  sync.Mutex
	evs []driver.Event
}

func (e *events) add(ev driver.Event) {
	e.mu.Lock()
	e.evs = append(e.evs, ev)
	e.mu.Unlock()
}

func (e *events) all() []driver.Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.evs)
}

func (e *events) has(want driver.Event) bool {
	return slices.ContainsFunc(e.all(), func(ev driver.Event) bool {
		return ev.Kind == want.Kind && (want.Queue == "" || ev.Queue == want.Queue) && (want.ID == 0 || ev.ID == want.ID)
	})
}

func (e *events) wait(t *testing.T, want driver.Event) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !e.has(want) {
		if time.Now().After(deadline) {
			t.Fatalf("event %+v not received, got %+v", want, e.all())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func listen(t *testing.T, s *Store) *events {
	t.Helper()
	e := &events{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Subscribe(ctx, e.add) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, errClosed) {
			t.Errorf("subscribe: %v", err)
		}
	})
	e.wait(t, driver.Event{Kind: driver.Resync})
	return e
}

func TestNotify(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	e := listen(t, s)

	insert(t, s, job("a", queue("mail")))
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "mail"})

	parent := insert(t, s, job("p", queue("parents")))[0].ID
	insert(t, s, job("c", queue("children"), after(driver.OnSucceeded, parent)))
	j := claim(t, s, 1, "parents")[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "children"})

	running := claim(t, s, 1, "mail")[0]
	if _, err := s.Delete(ctx, driver.Filter{IDs: []int64{running.ID}}); err != nil {
		t.Fatal(err)
	}
	e.wait(t, driver.Event{Kind: driver.CancelRequested, ID: running.ID})

	if err := s.PauseQueue(ctx, "mail", true); err != nil {
		t.Fatal(err)
	}
	e.wait(t, driver.Event{Kind: driver.QueueChanged, Queue: "mail"})

	b, err := s.OpenBatch(ctx, driver.NewBatch{})
	if err != nil {
		t.Fatal(err)
	}
	insert(t, s, job("then", queue("then"), func(p *driver.InsertParams) { p.AfterBatch = b }))
	if err := s.SealBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "then"})
}

func TestNotifySlowSubscriber(t *testing.T) {
	t.Parallel()
	s := open(t)
	release := make(chan struct{})
	var (
		mu    sync.Mutex
		first = true
		kinds []driver.EventKind
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(ctx, func(e driver.Event) {
			mu.Lock()
			kinds = append(kinds, e.Kind)
			block := first && e.Kind == driver.JobsReady
			if block {
				first = false
			}
			mu.Unlock()
			if block {
				<-release
			}
		})
	}()
	began := time.Now()
	for i := range 600 {
		insert(t, s, job("a", queue(fmt.Sprint("q", i))))
	}
	if el := time.Since(began); el > 10*time.Second {
		t.Fatalf("inserts took %v behind a blocked subscriber", el)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := 0
		for _, k := range kinds {
			if k == driver.Resync {
				n++
			}
		}
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no resync after the subscriber fell behind")
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCloseEndsSubscribe(t *testing.T) {
	t.Parallel()
	s := open(t)
	done := make(chan error, 1)
	go func() { done <- s.Subscribe(context.Background(), func(driver.Event) {}) }()
	time.Sleep(20 * time.Millisecond)
	s.Close()
	select {
	case err := <-done:
		if !errors.Is(err, errClosed) {
			t.Fatalf("subscribe after close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe still running after close")
	}
}
