package drivertest

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var claimTests = []test{
	{"Order", testClaimOrder},
	{"Limit", testClaimLimit},
	{"Queues", testClaimQueues},
	{"Kinds", testClaimKinds},
	{"Paused", testClaimPaused},
	{"OnlyEnqueued", testClaimOnlyEnqueued},
	{"Concurrent", testClaimConcurrent},
}

func testClaimOrder(t *testing.T, s driver.Store) {
	prios := []int16{0, 5, 0, 10, 5, -3}
	ps := tasks(len(prios), "one")
	for i, p := range prios {
		ps[i].Priority = p
	}
	ids := insertedIDs(insert(t, s, ps...))
	want := []int64{ids[3], ids[1], ids[4], ids[0], ids[2], ids[5]}
	for _, id := range want {
		claimOne(t, s, "one", id)
	}
	if js := claim(t, s, 10, "one"); len(js) != 0 {
		t.Fatalf("claim on drained queue returned %v", jobIDs(js))
	}

	ps = tasks(len(prios), "many")
	for i, p := range prios {
		ps[i].Priority = p
	}
	ids = insertedIDs(insert(t, s, ps...))
	js := claim(t, s, 3, "many")
	if got, want := sorted(jobIDs(js)), sorted([]int64{ids[3], ids[1], ids[4]}); !slices.Equal(got, want) {
		t.Fatalf("claimed %v, want %v", got, want)
	}
}

func testClaimLimit(t *testing.T, s driver.Store) {
	ids := insertedIDs(insert(t, s, tasks(10, "q")...))
	seen := make(map[int64]bool)
	for _, want := range []int{3, 3, 3, 1, 0} {
		js := claim(t, s, 3, "q")
		if len(js) != want {
			t.Fatalf("claimed %d jobs, want %d", len(js), want)
		}
		for _, j := range js {
			if seen[j.ID] {
				t.Fatalf("job %d claimed twice", j.ID)
			}
			seen[j.ID] = true
			if j.Attempt != 1 || j.Claim < 1 || j.Queue != "q" {
				t.Fatalf("claimed %+v", j)
			}
		}
	}
	wantState(t, s, driver.Processing, ids...)
}

func testClaimQueues(t *testing.T, s driver.Store) {
	a := insertedIDs(insert(t, s, tasks(2, "a")...))
	ps := tasks(3, "b")
	ps[0].Priority, ps[1].Priority, ps[2].Priority = 10, 0, 20
	b := insertedIDs(insert(t, s, ps...))

	js := claim(t, s, 3, "a", "b")
	if got, want := sorted(jobIDs(js)), sorted([]int64{a[0], a[1], b[2]}); !slices.Equal(got, want) {
		t.Fatalf("claim a,b got %v, want %v", got, want)
	}
	js = claim(t, s, 1, "c", "b", "a")
	if got := jobIDs(js); !slices.Equal(got, []int64{b[0]}) {
		t.Fatalf("claim c,b,a got %v, want [%d]", got, b[0])
	}

	more := insertedIDs(insert(t, s, tasks(2, "a")...))
	js = claim(t, s, 2, "b", "a")
	if got, want := sorted(jobIDs(js)), sorted([]int64{b[1], more[0]}); !slices.Equal(got, want) {
		t.Fatalf("claim b,a got %v, want %v", got, want)
	}
}

func testClaimKinds(t *testing.T, s driver.Store) {
	kinds := []string{"x", "y", "x", "z", "y"}
	ps := tasks(len(kinds), "q")
	for i, k := range kinds {
		ps[i].Kind = k
	}
	ids := insertedIDs(insert(t, s, ps...))
	q := driver.ClaimQuery{Queues: []string{"q"}, Kinds: []string{"x", "z"}, Limit: 10, Server: server}
	js, err := s.Claim(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := sorted(jobIDs(js)), []int64{ids[0], ids[2], ids[3]}; !slices.Equal(got, want) {
		t.Fatalf("claim kinds x,z got %v, want %v", got, want)
	}
	for _, j := range js {
		if j.Kind != "x" && j.Kind != "z" {
			t.Fatalf("claimed kind %q", j.Kind)
		}
	}
	q.Kinds = []string{"w"}
	if js, err = s.Claim(t.Context(), q); err != nil || len(js) != 0 {
		t.Fatalf("claim kind w: %v %v", jobIDs(js), err)
	}
	if got, want := sorted(jobIDs(claim(t, s, 10, "q"))), []int64{ids[1], ids[4]}; !slices.Equal(got, want) {
		t.Fatalf("claim without kinds got %v, want %v", got, want)
	}
}

func testClaimPaused(t *testing.T, s driver.Store) {
	ctx := t.Context()
	a := insertedIDs(insert(t, s, tasks(2, "a")...))
	b := add(t, s, task("b"))
	if err := s.PauseQueue(ctx, "a", true); err != nil {
		t.Fatal(err)
	}
	if js := claim(t, s, 10, "a"); len(js) != 0 {
		t.Fatalf("claimed %v from paused queue", jobIDs(js))
	}
	if got := jobIDs(claim(t, s, 10, "a", "b")); !slices.Equal(got, []int64{b}) {
		t.Fatalf("claim a,b got %v, want [%d]", got, b)
	}
	if d := heartbeat(t, s, driver.ServerInfo{ID: server}); !slices.Contains(d.Paused, "a") || slices.Contains(d.Paused, "b") {
		t.Fatalf("paused = %v, want [a]", d.Paused)
	}
	if err := s.PauseQueue(ctx, "a", false); err != nil {
		t.Fatal(err)
	}
	if got := sorted(jobIDs(claim(t, s, 10, "a"))); !slices.Equal(got, a) {
		t.Fatalf("claim after resume got %v, want %v", got, a)
	}
	if d := heartbeat(t, s, driver.ServerInfo{ID: server}); slices.Contains(d.Paused, "a") {
		t.Fatalf("paused = %v after resume", d.Paused)
	}
}

func testClaimOnlyEnqueued(t *testing.T, s driver.Store) {
	parent := add(t, s, task("parents"))
	add(t, s, limited("other", "k", 1))
	p := task("q")
	p.Delay = time.Hour
	scheduled := add(t, s, p)
	awaiting := add(t, s, after("q", driver.OnSucceeded, parent))
	throttled := add(t, s, limited("q", "k", 1))
	failed := start(t, s, task("q"))
	apply(t, s, outcome(failed, driver.Failed))
	done := start(t, s, task("q"))
	apply(t, s, outcome(done, driver.Succeeded))
	running := start(t, s, task("q"))

	if js := claim(t, s, 10, "q"); len(js) != 0 {
		t.Fatalf("claimed %v, want nothing", jobIDs(js))
	}
	wantState(t, s, driver.Scheduled, scheduled)
	wantState(t, s, driver.Awaiting, awaiting)
	wantState(t, s, driver.Throttled, throttled)
	wantState(t, s, driver.Failed, failed.ID)
	wantState(t, s, driver.Succeeded, done.ID)
	wantState(t, s, driver.Processing, running.ID)
}

func testClaimConcurrent(t *testing.T, s driver.Store) {
	const n, workers = 2000, 16
	for range n / 500 {
		insert(t, s, tasks(500, "q")...)
	}
	ctx := t.Context()
	deadline := time.Now().Add(5 * time.Second)
	var (
		mu    sync.Mutex
		seen  = make(map[int64]int, n)
		total atomic.Int64
		wg    sync.WaitGroup
	)
	for range workers {
		wg.Go(func() {
			q := driver.ClaimQuery{Queues: []string{"q"}, Limit: 10, Server: server}
			for total.Load() < n && time.Now().Before(deadline) {
				js, err := s.Claim(ctx, q)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, j := range js {
					seen[j.ID]++
				}
				mu.Unlock()
				total.Add(int64(len(js)))
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("job %d claimed %d times", id, c)
		}
	}
	if len(seen) != n || total.Load() != n {
		t.Fatalf("claimed %d distinct jobs (%d total), want %d", len(seen), total.Load(), n)
	}
	if c := counts(t, s); c.Processing != n || c.Enqueued != 0 {
		t.Fatalf("processing %d enqueued %d, want %d and 0", c.Processing, c.Enqueued, n)
	}
}
