package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

func decodeJSON[T any](t *testing.T, h http.Handler, path string) T {
	t.Helper()
	res := get(h, path)
	if res.code != http.StatusOK {
		t.Fatalf("%s: %d %s", path, res.code, res.body)
	}
	if ct := res.hdr.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("%s: content type %q", path, ct)
	}
	var v T
	if err := json.Unmarshal([]byte(res.body), &v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return v
}

type apiJob struct {
	ID          int64           `json:"id"`
	State       string          `json:"state"`
	Kind        string          `json:"kind"`
	Queue       string          `json:"queue"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"max_attempts"`
	Args        json.RawMessage `json:"args"`
	Meta        json.RawMessage `json:"meta"`
	Output      json.RawMessage `json:"output"`
	CreatedAt   time.Time       `json:"created_at"`
	FinalizedAt *time.Time      `json:"finalized_at"`
	Parents     []int64         `json:"parents"`
	History     []struct {
		State  string `json:"state"`
		Reason string `json:"reason"`
		Error  string `json:"error"`
		Trace  string `json:"trace"`
	} `json:"history"`
}

type apiPage struct {
	State string   `json:"state"`
	Jobs  []apiJob `json:"jobs"`
	Next  string   `json:"next"`
}

func TestAPI(t *testing.T) {
	t.Parallel()
	f := seed(t)
	h := New(f.c, Options{Authorize: grant(ReadOnly)})

	o := decodeJSON[struct {
		Counts    map[string]any `json:"counts"`
		Servers   int            `json:"servers"`
		Workers   int            `json:"workers"`
		Running   int            `json:"running"`
		Queues    int            `json:"queues"`
		Paused    int            `json:"paused_queues"`
		Recurring int            `json:"recurring"`
	}](t, h, "/api/overview")
	for k, v := range map[string]float64{"failed": 1, "retries": 1, "processing": 1, "awaiting": 1, "throttled": 1, "succeeded": 1, "deleted": 1, "scheduled": 2} {
		if o.Counts[k] != v {
			t.Errorf("counts.%s = %v, want %v", k, o.Counts[k], v)
		}
	}
	if o.Servers != 1 || o.Workers != 4 || o.Running != 1 || o.Recurring != 1 || o.Paused != 1 || o.Queues < 5 {
		t.Errorf("overview %+v", o)
	}

	failed := decodeJSON[apiPage](t, h, "/api/jobs/failed")
	if failed.State != "failed" || len(failed.Jobs) != 1 || failed.Next != "" {
		t.Fatalf("failed page %+v", failed)
	}
	j := failed.Jobs[0]
	if j.ID != f.failed || j.Kind != "email.send" || j.Queue != "work" || j.Attempt != 1 || j.MaxAttempts != 1 || j.FinalizedAt == nil {
		t.Errorf("failed job %+v", j)
	}
	var args email
	if err := json.Unmarshal(j.Args, &args); err != nil || args.To != "f@example.com" {
		t.Errorf("args %s", j.Args)
	}

	d := decodeJSON[apiJob](t, h, "/api/jobs/"+strconv.FormatInt(f.failed, 10))
	if len(d.History) != 1 || d.History[0].Reason != "exhausted" || d.History[0].Error != "smtp rejected hunter2" || !strings.HasPrefix(d.History[0].Trace, "goroutine 7") {
		t.Errorf("history %+v", d.History)
	}
	s := decodeJSON[apiJob](t, h, "/api/jobs/"+strconv.FormatInt(f.succeeded, 10))
	if string(s.Output) != `{"bytes":42}` || s.FinalizedAt == nil {
		t.Errorf("succeeded %+v", s)
	}
	if a := decodeJSON[apiJob](t, h, "/api/jobs/"+strconv.FormatInt(f.awaiting, 10)); len(a.Parents) != 1 || a.Parents[0] != f.scheduled || a.FinalizedAt != nil {
		t.Errorf("awaiting %+v", a)
	}
	if e := decodeJSON[apiJob](t, h, "/api/jobs/"+strconv.FormatInt(f.enqueued, 10)); string(e.Meta) != `{"token":"hunter2"}` {
		t.Errorf("meta %s", e.Meta)
	}

	first := decodeJSON[apiPage](t, h, "/api/jobs/scheduled?limit=1")
	if len(first.Jobs) != 1 || first.Next == "" {
		t.Fatalf("first page %+v", first)
	}
	second := decodeJSON[apiPage](t, h, "/api/jobs/scheduled?limit=1&cursor="+url.QueryEscape(first.Next))
	if len(second.Jobs) != 1 || second.Jobs[0].ID == first.Jobs[0].ID || second.Next != "" {
		t.Fatalf("second page %+v", second)
	}
	if res := get(h, "/api/jobs/scheduled?limit=501"); res.code != http.StatusBadRequest {
		t.Errorf("limit 501: %d", res.code)
	}
	if p := decodeJSON[apiPage](t, h, "/api/jobs/enqueued?queue=batchq&batch="+strconv.FormatInt(f.batch, 10)); len(p.Jobs) != 2 {
		t.Errorf("batch filter: %d jobs", len(p.Jobs))
	}
	if p := decodeJSON[apiPage](t, h, "/api/jobs/enqueued?kind=email.send"); len(p.Jobs) != 1 || p.Jobs[0].ID != f.enqueued {
		t.Errorf("kind filter %+v", p)
	}

	r := decodeJSON[apiPage](t, h, "/api/retries")
	if len(r.Jobs) != 1 || r.Jobs[0].ID != f.retry || r.Jobs[0].State != "scheduled" {
		t.Errorf("retries %+v", r)
	}

	for rng, want := range map[string]struct {
		step int64
		n    int
	}{"1h": {60, 60}, "24h": {1800, 48}} {
		c := decodeJSON[struct {
			Step   int64 `json:"step"`
			Points []struct {
				At        time.Time `json:"at"`
				Succeeded int64     `json:"succeeded"`
				Failed    int64     `json:"failed"`
			} `json:"points"`
		}](t, h, "/api/series?range="+rng)
		if c.Step != want.step || len(c.Points) != want.n {
			t.Fatalf("%s: step %d, %d points", rng, c.Step, len(c.Points))
		}
		var ok, bad int64
		for i, p := range c.Points {
			ok += p.Succeeded
			bad += p.Failed
			if i > 0 && p.At.Sub(c.Points[i-1].At) != time.Duration(c.Step)*time.Second {
				t.Fatalf("%s: uneven points", rng)
			}
		}
		if ok != 1 || bad != 1 {
			t.Errorf("%s: succeeded %d failed %d", rng, ok, bad)
		}
	}
	if res := get(h, "/api/series?range=7d"); res.code != http.StatusBadRequest {
		t.Errorf("bad range: %d", res.code)
	}

	qs := decodeJSON[struct {
		Queues []struct {
			Name      string `json:"name"`
			Paused    bool   `json:"paused"`
			Enqueued  int64  `json:"enqueued"`
			LatencyMS *int64 `json:"latency_ms"`
		} `json:"queues"`
	}](t, h, "/api/queues")
	found := false
	for _, q := range qs.Queues {
		if q.LatencyMS == nil {
			t.Errorf("queue %s lacks latency", q.Name)
		}
		if q.Name == "low" {
			found = q.Paused
		}
	}
	if !found {
		t.Error("paused queue low missing")
	}

	ss := decodeJSON[struct {
		Servers []struct {
			ID      string   `json:"id"`
			Host    string   `json:"host"`
			Queues  []string `json:"queues"`
			Workers int      `json:"workers"`
			Running int      `json:"running"`
			Age     *int64   `json:"heartbeat_age_ms"`
		} `json:"servers"`
	}](t, h, "/api/servers")
	if len(ss.Servers) != 1 || ss.Servers[0].ID != "host-a:1:ab" || ss.Servers[0].Age == nil || ss.Servers[0].Workers != 4 {
		t.Errorf("servers %+v", ss)
	}

	rs := decodeJSON[struct {
		Recurring []struct {
			ID        string          `json:"id"`
			Spec      string          `json:"spec"`
			Location  string          `json:"location"`
			Kind      string          `json:"kind"`
			Queue     string          `json:"queue"`
			Args      json.RawMessage `json:"args"`
			Misfire   string          `json:"misfire"`
			Paused    bool            `json:"paused"`
			NextRunAt time.Time       `json:"next_run_at"`
		} `json:"recurring"`
	}](t, h, "/api/recurring")
	if len(rs.Recurring) != 1 {
		t.Fatalf("recurring %+v", rs)
	}
	if rc := rs.Recurring[0]; rc.ID != "nightly" || rc.Spec != "0 3 * * *" || rc.Location != "UTC" || rc.Kind != "email.send" || rc.Queue != "mailers" || rc.Misfire != "once" || rc.NextRunAt.IsZero() {
		t.Errorf("recurring %+v", rc)
	}

	type apiBatch struct {
		ID          int64            `json:"id"`
		Description string           `json:"description"`
		Total       int64            `json:"total"`
		Sealed      bool             `json:"sealed"`
		Finished    bool             `json:"finished"`
		Counts      map[string]int64 `json:"counts"`
	}
	bs := decodeJSON[struct {
		Batches []apiBatch `json:"batches"`
		Next    string     `json:"next"`
	}](t, h, "/api/batches")
	if len(bs.Batches) != 1 || bs.Batches[0].ID != f.batch || bs.Next != "" {
		t.Fatalf("batches %+v", bs)
	}
	b := decodeJSON[apiBatch](t, h, "/api/batches/"+strconv.FormatInt(f.batch, 10))
	if b.Description != "Import catalog" || b.Total != 2 || !b.Sealed || b.Finished || b.Counts["enqueued"] != 2 {
		t.Errorf("batch %+v", b)
	}
}

func TestRetriesScan(t *testing.T) {
	t.Parallel()
	s := memstore.New()
	c := kiln.NewClient(s)
	ctx := context.Background()
	var retries []int64
	for i := range 7 {
		if _, err := c.Enqueue(ctx, resize{Key: "plain"}, kiln.Delay(time.Duration(i+1)*time.Minute)); err != nil {
			t.Fatal(err)
		}
		id, err := c.Enqueue(ctx, resize{Key: "retry"}, kiln.Queue("work"))
		if err != nil {
			t.Fatal(err)
		}
		js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"work"}, Limit: 1, Server: "a"})
		if err != nil || len(js) != 1 {
			t.Fatal(err)
		}
		if _, err := s.Finish(ctx, "a", []driver.Outcome{{Ref: js[0].Ref, State: driver.Scheduled, Delay: time.Duration(i+1)*time.Minute + time.Second, Reason: "retry"}}); err != nil {
			t.Fatal(err)
		}
		retries = append(retries, id)
	}
	h := New(c, Options{Authorize: grant(ReadOnly)})
	var got []int64
	cursor := ""
	for range 10 {
		p := decodeJSON[apiPage](t, h, "/api/retries?limit=3&cursor="+url.QueryEscape(cursor))
		if len(p.Jobs) > 3 {
			t.Fatalf("page of %d", len(p.Jobs))
		}
		for _, j := range p.Jobs {
			if j.Attempt == 0 {
				t.Fatalf("job %d never ran", j.ID)
			}
			got = append(got, j.ID)
		}
		if cursor = p.Next; cursor == "" {
			break
		}
	}
	if len(got) != len(retries) {
		t.Fatalf("got %v, want %v", got, retries)
	}
	for i := range got {
		if got[i] != retries[i] {
			t.Fatalf("got %v, want %v", got, retries)
		}
	}
	if res := get(h, "/api/retries?cursor=x"); res.code != http.StatusBadRequest {
		t.Errorf("bad cursor: %d", res.code)
	}
}

func retrying(t *testing.T, s *memstore.Store, c *kiln.Client, n int, gap time.Duration) []int64 {
	t.Helper()
	ctx := context.Background()
	ids := make([]int64, n)
	for i := range ids {
		id, err := c.Enqueue(ctx, resize{Key: "retry"}, kiln.Queue("work"))
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"work"}, Limit: n, Server: "a"})
	if err != nil || len(js) != n {
		t.Fatalf("claim: %v %d", err, len(js))
	}
	outs := make([]driver.Outcome, n)
	for i, j := range js {
		outs[i] = driver.Outcome{Ref: j.Ref, State: driver.Scheduled, Delay: gap + time.Duration(i)*time.Minute, Reason: "retry"}
	}
	if _, err := s.Finish(ctx, "a", outs); err != nil {
		t.Fatal(err)
	}
	return ids
}

func pageIDs(p apiPage) []int64 {
	ids := make([]int64, len(p.Jobs))
	for i, j := range p.Jobs {
		ids[i] = j.ID
	}
	return ids
}

func TestRetriesPageAfterRequeue(t *testing.T) {
	t.Parallel()
	s := memstore.New()
	c := kiln.NewClient(s)
	ids := retrying(t, s, c, 30, time.Minute)
	h := New(c, Options{Authorize: grant(ReadOnly)})
	first := decodeJSON[apiPage](t, h, "/api/retries?limit=10")
	if got := pageIDs(first); !slices.Equal(got, ids[:10]) || first.Next == "" {
		t.Fatalf("first page %v next %q", got, first.Next)
	}
	if _, err := c.RequeueWhere(t.Context(), kiln.Filter{IDs: ids[:5]}); err != nil {
		t.Fatal(err)
	}
	next := decodeJSON[apiPage](t, h, "/api/retries?limit=10&cursor="+url.QueryEscape(first.Next))
	if got := pageIDs(next); !slices.Equal(got, ids[10:20]) {
		t.Fatalf("second page %v, want %v", got, ids[10:20])
	}
}

func TestRetriesBeyondScan(t *testing.T) {
	t.Parallel()
	s := memstore.New()
	c := kiln.NewClient(s)
	plain := make([]driver.InsertParams, retryScan+1)
	for i := range plain {
		plain[i] = driver.InsertParams{Kind: "image.resize", Queue: "default", Args: []byte(`{}`), MaxAttempts: 1, Delay: time.Minute}
	}
	if _, err := s.Insert(t.Context(), plain); err != nil {
		t.Fatal(err)
	}
	want := retrying(t, s, c, 1, time.Hour)
	h := New(c, Options{Authorize: grant(ReadOnly)})
	res := get(h, "/retries")
	if strings.Contains(res.body, "No retries scheduled.") || !strings.Contains(res.body, `rel="next"`) {
		t.Fatalf("first page claims there are no retries: %d", res.code)
	}
	var got []int64
	cursor := ""
	for range 10 {
		p := decodeJSON[apiPage](t, h, "/api/retries?cursor="+url.QueryEscape(cursor))
		got = append(got, pageIDs(p)...)
		if cursor = p.Next; cursor == "" {
			break
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

type countingStore struct {
	driver.Store
	counts atomic.Int32
}

func (s *countingStore) Counts(ctx context.Context) (driver.Counts, error) {
	s.counts.Add(1)
	time.Sleep(20 * time.Millisecond)
	return s.Store.Counts(ctx)
}

func TestOverviewCache(t *testing.T) {
	t.Parallel()
	s := &countingStore{Store: memstore.New()}
	h := New(kiln.NewClient(s), Options{Authorize: grant(ReadOnly)})
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if res := get(h, "/api/overview"); res.code != http.StatusOK {
				t.Errorf("status %d", res.code)
			}
		})
	}
	wg.Wait()
	get(h, "/")
	get(h, "/jobs/failed")
	if n := s.counts.Load(); n != 1 {
		t.Fatalf("Counts called %d times", n)
	}
}

func TestRedact(t *testing.T) {
	t.Parallel()
	f := seed(t)
	var mu sync.Mutex
	seen := map[Part]bool{}
	h := New(f.c, Options{
		Authorize: grant(ReadOnly),
		Redact: func(kind string, p Part, v []byte) []byte {
			mu.Lock()
			seen[p] = true
			mu.Unlock()
			if kind == "email.send" || p == PartOutput {
				return []byte(strings.ReplaceAll(string(v), "hunter2", "[redacted]"))
			}
			if p == PartArgs && kind == "image.resize" {
				return []byte("not json")
			}
			return v
		},
	})
	succeeded := strconv.FormatInt(f.succeeded, 10)
	for _, path := range []string{
		"/jobs/failed", "/jobs/enqueued", "/recurring",
		"/jobs/" + strconv.FormatInt(f.failed, 10), "/jobs/" + strconv.FormatInt(f.enqueued, 10),
		"/api/jobs/failed", "/api/jobs/" + strconv.FormatInt(f.failed, 10), "/api/jobs/" + strconv.FormatInt(f.enqueued, 10),
		"/jobs/" + succeeded, "/api/jobs/" + succeeded,
	} {
		res := get(h, path)
		if res.code != http.StatusOK {
			t.Fatalf("%s: %d", path, res.code)
		}
		if strings.Contains(res.body, "hunter2") {
			t.Errorf("%s leaks the secret", path)
		}
	}
	if res := get(h, "/jobs/"+strconv.FormatInt(f.failed, 10)); !strings.Contains(res.body, "smtp rejected [redacted]") {
		t.Error("error not redacted")
	}
	a := decodeJSON[apiJob](t, h, "/api/jobs/"+succeeded)
	if string(a.Args) != `"not json"` {
		t.Errorf("non-JSON redaction result: %s", a.Args)
	}
	for _, p := range []Part{PartArgs, PartMeta, PartOutput, PartError} {
		if !seen[p] {
			t.Errorf("Redact never saw %s", p)
		}
	}
}
