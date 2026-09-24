package drivertest

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var notifyTests = []test{
	{"Resync", testNotifyResync},
	{"Insert", testNotifyInsert},
	{"Promote", testNotifyPromote},
	{"Finish", testNotifyFinish},
	{"Requeue", testNotifyRequeue},
	{"Cancel", testNotifyCancel},
	{"Pause", testNotifyPause},
}

type events struct {
	mu  sync.Mutex
	evs []driver.Event
}

func (e *events) add(ev driver.Event) {
	e.mu.Lock()
	e.evs = append(e.evs, ev)
	e.mu.Unlock()
}

func (e *events) reset() {
	e.mu.Lock()
	e.evs = nil
	e.mu.Unlock()
}

func (e *events) has(want driver.Event) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.ContainsFunc(e.evs, func(ev driver.Event) bool {
		return ev.Kind == want.Kind && (want.Queue == "" || ev.Queue == want.Queue) && (want.ID == 0 || ev.ID == want.ID)
	})
}

func (e *events) wait(t *testing.T, ev driver.Event) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !e.has(ev) {
		if time.Now().After(deadline) {
			e.mu.Lock()
			defer e.mu.Unlock()
			t.Fatalf("event %+v not received, got %+v", ev, e.evs)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func subscribe(t *testing.T, s driver.Store) *events {
	t.Helper()
	n, ok := s.(driver.Notifier)
	if !ok {
		t.Skip("store does not implement driver.Notifier")
	}
	ctx, cancel := context.WithCancel(t.Context())
	e := &events{}
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = n.Subscribe(ctx, e.add)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("subscribe did not return after its context was canceled")
		}
	})
	e.wait(t, driver.Event{Kind: driver.Resync})
	select {
	case <-done:
		t.Fatalf("subscribe returned before its context was canceled: %v", err)
	default:
	}
	return e
}

func testNotifyResync(t *testing.T, s driver.Store) {
	subscribe(t, s)
}

func testNotifyInsert(t *testing.T, s driver.Store) {
	e := subscribe(t, s)
	add(t, s, task("mail"))
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "mail"})
}

func testNotifyPromote(t *testing.T, s driver.Store) {
	e := subscribe(t, s)
	p := task("later")
	p.Delay = 50 * time.Millisecond
	add(t, s, p)
	eventually(t, time.Second, func() bool { return promote(t, s, 100).Count == 1 })
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "later"})
}

func testNotifyFinish(t *testing.T, s driver.Store) {
	e := subscribe(t, s)
	parent := start(t, s, task("parents"))
	add(t, s, after("children", driver.OnSucceeded, parent.ID))
	running := start(t, s, limited("first", "k", 1))
	add(t, s, limited("waiting", "k", 1))
	retry := start(t, s, task("retry"))
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "retry"})
	e.reset()
	again := outcome(retry, driver.Scheduled)
	again.Reason = "retry"
	apply(t, s, outcome(parent, driver.Succeeded), outcome(running, driver.Succeeded), again)
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "children"})
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "waiting"})
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "retry"})
}

func testNotifyRequeue(t *testing.T, s driver.Store) {
	e := subscribe(t, s)
	j := start(t, s, task("again"))
	apply(t, s, outcome(j, driver.Failed))
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "again"})
	e.reset()
	requeueIDs(t, s, j.ID)
	e.wait(t, driver.Event{Kind: driver.JobsReady, Queue: "again"})
}

func testNotifyCancel(t *testing.T, s driver.Store) {
	e := subscribe(t, s)
	j := start(t, s, task("q"))
	deleteIDs(t, s, j.ID)
	e.wait(t, driver.Event{Kind: driver.CancelRequested, ID: j.ID})
}

func testNotifyPause(t *testing.T, s driver.Store) {
	e := subscribe(t, s)
	if err := s.PauseQueue(t.Context(), "q", true); err != nil {
		t.Fatal(err)
	}
	e.wait(t, driver.Event{Kind: driver.QueueChanged, Queue: "q"})
}
