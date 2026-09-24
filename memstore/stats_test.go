package memstore

import (
	"context"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestStatsAttribution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := New()
	job := func(queue string, parents ...driver.Parent) driver.InsertParams {
		return driver.InsertParams{Kind: "k", Queue: queue, Args: []byte(`{}`), MaxAttempts: 1, Parents: parents}
	}
	add := func(p driver.InsertParams) int64 {
		res, err := s.Insert(ctx, []driver.InsertParams{p})
		if err != nil {
			t.Fatal(err)
		}
		return res[0].ID
	}
	run := func(queue string, st driver.State) {
		js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{queue}, Limit: 1, Server: "srv"})
		if err != nil || len(js) != 1 {
			t.Fatalf("claim %s: %v %v", queue, js, err)
		}
		if _, err := s.Finish(ctx, "srv", []driver.Outcome{{Ref: js[0].Ref, State: st, Reason: "x"}}); err != nil {
			t.Fatal(err)
		}
	}
	deleted := add(job("d"))
	add(job("c", driver.Parent{ID: deleted, On: driver.OnSucceeded}))
	if n, err := s.Delete(ctx, driver.Filter{IDs: []int64{deleted}}); err != nil || n != 1 {
		t.Fatalf("delete %d %v", n, err)
	}
	add(job("c", driver.Parent{ID: add(job("s")), On: driver.OnFailed}))
	run("s", driver.Succeeded)
	add(job("c", driver.Parent{ID: add(job("f")), On: driver.OnSucceeded}))
	run("f", driver.Failed)
	if _, err := s.Prune(ctx, driver.PruneParams{Succeeded: -1, Deleted: -1}); err != nil {
		t.Fatal(err)
	}
	got := make(map[string]counters)
	for k, c := range s.stats {
		sum := got[k.server]
		sum.merge(c)
		got[k.server] = sum
	}
	want := map[string]counters{"": {deleted: 3}, "srv": {succeeded: 1, failed: 1, deleted: 1}}
	if len(got) != len(want) || got[""] != want[""] || got["srv"] != want["srv"] {
		t.Fatalf("stats %+v, want %+v", got, want)
	}
}
