package drivertest

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const server = "s1"

type test struct {
	name string
	fn   func(t *testing.T, s driver.Store)
}

func Run(t *testing.T, open func(t *testing.T) driver.Store) {
	groups := []struct {
		name  string
		tests []test
	}{
		{"Insert", insertTests},
		{"Claim", claimTests},
		{"Finish", finishTests},
		{"Deps", depsTests},
		{"Batch", batchTests},
		{"Unique", uniqueTests},
		{"Limits", limitsTests},
		{"Rate", rateTests},
		{"Recurring", recurringTests},
		{"Coordinate", coordinateTests},
		{"Admin", adminTests},
		{"Inspect", inspectTests},
		{"Notify", notifyTests},
		{"Tx", txTests},
	}
	for _, g := range groups {
		t.Run(g.name, func(t *testing.T) {
			for _, c := range g.tests {
				t.Run(c.name, func(t *testing.T) {
					t.Parallel()
					c.fn(t, open(t))
				})
			}
		})
	}
}

func task(queue string) driver.InsertParams {
	return driver.InsertParams{Kind: "task", Queue: queue, Args: []byte(`{}`), MaxAttempts: 5}
}

func tasks(n int, queue string) []driver.InsertParams {
	ps := make([]driver.InsertParams, n)
	for i := range ps {
		ps[i] = task(queue)
	}
	return ps
}

func limited(queue, key string, n int) driver.InsertParams {
	p := task(queue)
	p.LimitKey, p.LimitMax = key, n
	return p
}

func after(queue string, on driver.Mask, parents ...int64) driver.InsertParams {
	p := task(queue)
	for _, id := range parents {
		p.Parents = append(p.Parents, driver.Parent{ID: id, On: on})
	}
	return p
}

func key(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:16]
}

func insert(t *testing.T, w driver.Writer, ps ...driver.InsertParams) []driver.Inserted {
	t.Helper()
	ins, err := w.Insert(t.Context(), ps)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if len(ins) != len(ps) {
		t.Fatalf("insert: %d results for %d jobs", len(ins), len(ps))
	}
	return ins
}

func add(t *testing.T, w driver.Writer, p driver.InsertParams) int64 {
	t.Helper()
	return insert(t, w, p)[0].ID
}

func insertedIDs(ins []driver.Inserted) []int64 {
	ids := make([]int64, len(ins))
	for i, in := range ins {
		ids[i] = in.ID
	}
	return ids
}

func jobIDs(js []driver.Job) []int64 {
	ids := make([]int64, len(js))
	for i, j := range js {
		ids[i] = j.ID
	}
	return ids
}

func sorted(ids []int64) []int64 {
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return ids
}

func claimAs(t *testing.T, s driver.Store, srv string, limit int, queues ...string) []driver.Job {
	t.Helper()
	js, err := s.Claim(t.Context(), driver.ClaimQuery{Queues: queues, Limit: limit, Server: srv})
	if err != nil {
		t.Fatalf("claim %v: %v", queues, err)
	}
	return js
}

func claim(t *testing.T, s driver.Store, limit int, queues ...string) []driver.Job {
	t.Helper()
	return claimAs(t, s, server, limit, queues...)
}

func claimN(t *testing.T, s driver.Store, n int, queues ...string) []driver.Job {
	t.Helper()
	js := claim(t, s, n, queues...)
	if len(js) != n {
		t.Fatalf("claim %v: got %d jobs, want %d", queues, len(js), n)
	}
	return js
}

func claimOne(t *testing.T, s driver.Store, queue string, id int64) driver.Job {
	t.Helper()
	js := claim(t, s, 1, queue)
	if len(js) != 1 || js[0].ID != id {
		t.Fatalf("claim %s: got %v, want [%d]", queue, jobIDs(js), id)
	}
	return js[0]
}

func start(t *testing.T, s driver.Store, p driver.InsertParams) driver.Job {
	t.Helper()
	return claimOne(t, s, p.Queue, add(t, s, p))
}

func outcome(j driver.Job, st driver.State) driver.Outcome {
	return driver.Outcome{Ref: j.Ref, State: st}
}

func finish(t *testing.T, s driver.Store, outs ...driver.Outcome) []driver.Result {
	t.Helper()
	rs, err := s.Finish(t.Context(), server, outs)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if len(rs) != len(outs) {
		t.Fatalf("finish: %d results for %d outcomes", len(rs), len(outs))
	}
	return rs
}

func apply(t *testing.T, s driver.Store, outs ...driver.Outcome) {
	t.Helper()
	for i, r := range finish(t, s, outs...) {
		if r != driver.Applied {
			t.Fatalf("finish job %d as %s: result %d, want applied", outs[i].ID, outs[i].State, r)
		}
	}
}

func record(t *testing.T, s driver.Store, id int64) driver.Record {
	t.Helper()
	r, err := s.Job(t.Context(), id)
	if err != nil {
		t.Fatalf("job %d: %v", id, err)
	}
	return r
}

func stateOf(t *testing.T, s driver.Store, id int64) driver.State {
	t.Helper()
	return record(t, s, id).State
}

func wantState(t *testing.T, s driver.Store, want driver.State, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		if got := stateOf(t, s, id); got != want {
			t.Fatalf("job %d state = %s, want %s", id, got, want)
		}
	}
}

func lastEntry(t *testing.T, r driver.Record) driver.Entry {
	t.Helper()
	if len(r.History) == 0 {
		t.Fatalf("job %d has no history", r.ID)
	}
	return r.History[len(r.History)-1]
}

func wantReason(t *testing.T, s driver.Store, id int64, reason string) {
	t.Helper()
	if e := lastEntry(t, record(t, s, id)); e.Reason != reason {
		t.Fatalf("job %d last history reason = %q, want %q", id, e.Reason, reason)
	}
}

func now(t *testing.T, s driver.Store) time.Time {
	t.Helper()
	n, err := s.Now(t.Context())
	if err != nil {
		t.Fatalf("now: %v", err)
	}
	return n
}

func counts(t *testing.T, s driver.Store) driver.Counts {
	t.Helper()
	c, err := s.Counts(t.Context())
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	return c
}

func wantEmpty(t *testing.T, s driver.Store) {
	t.Helper()
	if c := counts(t, s); c != (driver.Counts{}) {
		t.Fatalf("counts = %+v, want nothing stored", c)
	}
}

func deleteIDs(t *testing.T, s driver.Store, ids ...int64) int {
	t.Helper()
	n, err := s.Delete(t.Context(), driver.Filter{IDs: ids})
	if err != nil {
		t.Fatalf("delete %v: %v", ids, err)
	}
	return n
}

func requeueIDs(t *testing.T, s driver.Store, ids ...int64) int {
	t.Helper()
	n, err := s.Requeue(t.Context(), driver.Filter{IDs: ids})
	if err != nil {
		t.Fatalf("requeue %v: %v", ids, err)
	}
	return n
}

func promote(t *testing.T, s driver.Store, limit int) driver.Promoted {
	t.Helper()
	p, err := s.Promote(t.Context(), limit)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	return p
}

func sweep(t *testing.T, s driver.Store) {
	t.Helper()
	if _, err := s.Sweep(t.Context(), 1000); err != nil {
		t.Fatalf("sweep: %v", err)
	}
}

func heartbeat(t *testing.T, s driver.Store, info driver.ServerInfo) driver.Directives {
	t.Helper()
	d, err := s.Heartbeat(t.Context(), info)
	if err != nil {
		t.Fatalf("heartbeat %s: %v", info.ID, err)
	}
	return d
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func wantErr(t *testing.T, err error, targets ...error) {
	t.Helper()
	for _, target := range targets {
		if errors.Is(err, target) {
			return
		}
	}
	t.Fatalf("err = %v, want one of %v", err, targets)
}

func within(t *testing.T, name string, got, lo, hi time.Time) {
	t.Helper()
	if got.Before(lo.Add(-time.Millisecond)) || got.After(hi.Add(time.Millisecond)) {
		t.Fatalf("%s = %v, want within [%v, %v]", name, got, lo, hi)
	}
}

func sameJSON(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}
