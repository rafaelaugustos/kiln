package redisstore_test

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/drivertest"
	"github.com/rafaelaugustos/kiln/redisstore"
)

func job(queue, kind string, priority int16) driver.InsertParams {
	return driver.InsertParams{Kind: kind, Queue: queue, Args: []byte(`{}`), MaxAttempts: 3, Priority: priority}
}

func ids(ins []driver.Inserted) []int64 {
	out := make([]int64, len(ins))
	for i, in := range ins {
		out[i] = in.ID
	}
	return out
}

func TestConformancePlain(t *testing.T) {
	c := connect(t)
	drivertest.Run(t, func(t *testing.T) driver.Store {
		s := open(t, c)
		redisstore.Plain(s)
		return s
	})
}

func TestReloadsMissingFunctions(t *testing.T) {
	c := connect(t)
	s := open(t, c)
	ctx := context.Background()
	if err := c.FunctionDelete(ctx, redisstore.Library).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Insert(ctx, []driver.InsertParams{job("q", "k", 0)}); err != nil {
		t.Fatalf("insert after the library was deleted: %v", err)
	}
}

func TestRefusesEviction(t *testing.T) {
	c := connect(t)
	ctx := context.Background()
	old, err := c.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		t.Skipf("config get: %v", err)
	}
	if err := c.ConfigSet(ctx, "maxmemory-policy", "allkeys-lru").Err(); err != nil {
		t.Skipf("config set: %v", err)
	}
	t.Cleanup(func() { c.ConfigSet(context.Background(), "maxmemory-policy", old["maxmemory-policy"]) })
	prefix := "kiln-test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := redisstore.New(ctx, c, redisstore.Prefix(prefix)); err == nil {
		t.Fatal("New accepted a server that evicts keys")
	}
	if _, err := redisstore.New(ctx, c, redisstore.Prefix(prefix), redisstore.AllowEviction()); err != nil {
		t.Fatalf("New with AllowEviction: %v", err)
	}
	c.Del(ctx, "{"+prefix+"}:meta")
}

func TestInvalidPrefix(t *testing.T) {
	c := connect(t)
	for _, p := range []string{"", "a b", "a{b}", "ç"} {
		if _, err := redisstore.New(context.Background(), c, redisstore.Prefix(p)); err == nil {
			t.Errorf("prefix %q accepted", p)
		}
	}
}

func TestClaimMergesKinds(t *testing.T) {
	s := open(t, connect(t))
	ctx := context.Background()
	ins, err := s.Insert(ctx, []driver.InsertParams{
		job("q", "a", 0), job("q", "b", 5), job("q", "c", 0), job("q", "a", 5), job("q", "b", 0), job("q", "c", 9),
	})
	if err != nil {
		t.Fatal(err)
	}
	id := ids(ins)
	got, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"q"}, Kinds: []string{"a", "b"}, Limit: 3, Server: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []int64{id[1], id[3], id[0]}; !slices.EqualFunc(got, want, func(j driver.Job, w int64) bool { return j.ID == w }) {
		t.Fatalf("claimed %v, want %v", jobIDs(got), want)
	}
	got, err = s.Claim(ctx, driver.ClaimQuery{Queues: []string{"q"}, Limit: 10, Server: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []int64{id[5], id[2], id[4]}; !slices.Equal(jobIDs(got), want) {
		t.Fatalf("claimed %v, want %v", jobIDs(got), want)
	}
}

func jobIDs(js []driver.Job) []int64 {
	out := make([]int64, len(js))
	for i, j := range js {
		out[i] = j.ID
	}
	return out
}

func pages(t *testing.T, s *redisstore.Store, q driver.JobQuery) []int64 {
	t.Helper()
	var out []int64
	for {
		p, err := s.Jobs(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range p.Records {
			out = append(out, r.ID)
		}
		if p.Next == "" {
			return out
		}
		q.Cursor = p.Next
	}
}

func TestPagesThroughTies(t *testing.T) {
	s := open(t, connect(t))
	ctx := context.Background()
	at := time.Now().Add(time.Hour)
	later := make([]driver.InsertParams, 30)
	for i := range later {
		later[i] = job("ties", "k", 0)
		later[i].RunAt = at
	}
	ins, err := s.Insert(ctx, later)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := pages(t, s, driver.JobQuery{State: driver.Scheduled, Limit: 7}), ids(ins); !slices.Equal(got, want) {
		t.Fatalf("scheduled pages %v, want %v", got, want)
	}

	now := make([]driver.InsertParams, 30)
	for i := range now {
		now[i] = job("ties", "k", 0)
	}
	if ins, err = s.Insert(ctx, now); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"ties"}, Limit: 30, Server: "s"})
	if err != nil {
		t.Fatal(err)
	}
	outs := make([]driver.Outcome, len(claimed))
	for i, j := range claimed {
		outs[i] = driver.Outcome{Ref: j.Ref, State: driver.Succeeded}
	}
	if _, err := s.Finish(ctx, "s", outs); err != nil {
		t.Fatal(err)
	}
	want := ids(ins)
	slices.Reverse(want)
	if got := pages(t, s, driver.JobQuery{State: driver.Succeeded, Limit: 7}); !slices.Equal(got, want) {
		t.Fatalf("succeeded pages %v, want %v", got, want)
	}
}

func TestFiltersBeyondOnePage(t *testing.T) {
	s := open(t, connect(t))
	ctx := context.Background()
	jobs := make([]driver.InsertParams, 6000)
	for i := range jobs {
		jobs[i] = job("big", "common", 0)
	}
	jobs[5990].Tags = []string{"rare"}
	jobs[5995].Tags = []string{"rare"}
	ins, err := s.Insert(ctx, jobs)
	if err != nil {
		t.Fatal(err)
	}
	got := pages(t, s, driver.JobQuery{State: driver.Enqueued, Tag: "rare", Limit: 20})
	if want := []int64{ins[5990].ID, ins[5995].ID}; !slices.Equal(got, want) {
		t.Fatalf("tagged %v, want %v", got, want)
	}
	n, err := s.Delete(ctx, driver.Filter{State: driver.Enqueued, Queue: "big"})
	if err != nil || n != len(jobs) {
		t.Fatalf("deleted %d, %v; want %d", n, err, len(jobs))
	}
	pruned, err := s.Prune(ctx, driver.PruneParams{Limit: 10000, Retention: driver.Retention{Succeeded: -1, Failed: -1}})
	if err != nil || pruned != len(jobs) {
		t.Fatalf("pruned %d, %v; want %d", pruned, err, len(jobs))
	}
	c, err := s.Counts(ctx)
	if err != nil || c.Enqueued != 0 || c.Deleted != int64(len(jobs)) {
		t.Fatalf("counts %+v, %v", c, err)
	}
}
