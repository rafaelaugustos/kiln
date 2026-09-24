package pgstore

import (
	"context"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestSubscribe(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan driver.Event, 64)
	done := make(chan error, 1)
	go func() {
		done <- s.Subscribe(ctx, func(e driver.Event) {
			select {
			case events <- e:
			default:
			}
		})
	}()
	wait := func(match func(driver.Event) bool) driver.Event {
		t.Helper()
		timeout := time.After(3 * time.Second)
		for {
			select {
			case e := <-events:
				if match(e) {
					return e
				}
			case <-timeout:
				t.Fatal("timed out waiting for event")
			}
		}
	}
	wait(func(e driver.Event) bool { return e.Kind == driver.Resync })

	insert(t, s, job("a", func(p *driver.InsertParams) { p.Queue = "mail" }))
	wait(func(e driver.Event) bool { return e.Kind == driver.JobsReady && e.Queue == "mail" })

	j := claim(t, s, 1, "mail")[0]
	if _, err := s.Delete(context.Background(), driver.Filter{IDs: []int64{j.ID}}); err != nil {
		t.Fatal(err)
	}
	wait(func(e driver.Event) bool { return e.Kind == driver.CancelRequested && e.ID == j.ID })

	if err := s.PauseQueue(context.Background(), "mail", true); err != nil {
		t.Fatal(err)
	}
	wait(func(e driver.Event) bool { return e.Kind == driver.QueueChanged && e.Queue == "mail" })

	var pid int
	err := s.pool.QueryRow(context.Background(),
		"SELECT pid FROM pg_stat_activity WHERE query = $1",
		"LISTEN "+s.schema+"_jobs; LISTEN "+s.schema+"_cancel; LISTEN "+s.schema+"_queue").Scan(&pid)
	if err != nil {
		t.Fatalf("listener backend: %v", err)
	}
	if _, err := s.pool.Exec(context.Background(), "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	wait(func(e driver.Event) bool { return e.Kind == driver.Resync })
	insert(t, s, job("a", func(p *driver.InsertParams) { p.Queue = "after" }))
	wait(func(e driver.Event) bool { return e.Kind == driver.JobsReady && e.Queue == "after" })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("subscribe returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("subscribe did not return")
	}
}
