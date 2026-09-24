package dashboard

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

type email struct {
	To     string `json:"to"`
	Secret string `json:"secret,omitempty"`
}

func (email) Kind() string { return "email.send" }

type resize struct {
	Key string `json:"key"`
}

func (resize) Kind() string { return "image.resize" }

type fixture struct {
	t     *testing.T
	store *memstore.Store
	c     *kiln.Client
	batch int64

	succeeded, failed, retry, processing, enqueued, scheduled, awaiting, throttled, deleted int64
}

func seed(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	s := memstore.New()
	f := &fixture{t: t, store: s, c: kiln.NewClient(s)}

	f.succeeded = f.run(resize{Key: "done.png"}, driver.Outcome{State: driver.Succeeded, Output: []byte(`{"bytes":42}`)})
	f.failed = f.run(email{To: "f@example.com", Secret: "hunter2"}, driver.Outcome{
		State: driver.Failed, Reason: "exhausted", Error: "smtp rejected hunter2", Trace: "goroutine 7 [running]:\nmain.send()",
	}, kiln.MaxAttempts(1))
	f.retry = f.run(resize{Key: "retry.png"}, driver.Outcome{State: driver.Scheduled, Delay: time.Hour, Reason: "retry", Error: "timeout"})
	f.processing = f.enqueue(resize{Key: "busy.png"}, kiln.Queue("work"))
	f.claim()
	f.enqueued = f.enqueue(email{To: "e@example.com", Secret: "hunter2"}, kiln.Queue("mailers"), kiln.Meta{"token": "hunter2"})
	f.scheduled = f.enqueue(resize{Key: "later.png"}, kiln.Delay(time.Hour))
	f.awaiting = f.enqueue(resize{Key: "after.png"}, kiln.After{f.scheduled})
	f.enqueue(resize{Key: "t1.png"}, kiln.Queue("limited"), kiln.Limit{Key: "lk", Max: 1})
	f.throttled = f.enqueue(resize{Key: "t2.png"}, kiln.Queue("limited"), kiln.Limit{Key: "lk", Max: 1})
	f.deleted = f.enqueue(resize{Key: "gone.png"})
	if _, err := f.c.Delete(ctx, f.deleted); err != nil {
		t.Fatal(err)
	}

	b := &kiln.Batch{Description: "Import catalog"}
	b.Add(resize{Key: "b1.png"}, kiln.Queue("batchq"))
	b.Add(resize{Key: "b2.png"}, kiln.Queue("batchq"))
	var err error
	if f.batch, err = f.c.StartBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := f.c.SetRecurring(ctx, "nightly", "0 3 * * *", email{To: "ops@example.com"}, kiln.Queue("mailers")); err != nil {
		t.Fatal(err)
	}
	if err := f.c.PauseQueue(ctx, "low"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Heartbeat(ctx, driver.ServerInfo{ID: "host-a:1:ab", Host: "host-a", PID: 1, Queues: []string{"work"}, Workers: 4, Running: 1}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) enqueue(a kiln.Args, opts ...kiln.InsertOption) int64 {
	f.t.Helper()
	id, err := f.c.Enqueue(context.Background(), a, opts...)
	if err != nil {
		f.t.Fatal(err)
	}
	return id
}

func (f *fixture) claim() driver.Job {
	f.t.Helper()
	js, err := f.store.Claim(context.Background(), driver.ClaimQuery{Queues: []string{"work"}, Limit: 1, Server: "host-a:1:ab"})
	if err != nil || len(js) != 1 {
		f.t.Fatalf("claim: %v %d", err, len(js))
	}
	return js[0]
}

func (f *fixture) run(a kiln.Args, o driver.Outcome, opts ...kiln.InsertOption) int64 {
	f.t.Helper()
	id := f.enqueue(a, append([]kiln.InsertOption{kiln.Queue("work")}, opts...)...)
	o.Ref = f.claim().Ref
	res, err := f.store.Finish(context.Background(), "host-a:1:ab", []driver.Outcome{o})
	if err != nil || res[0] != driver.Applied {
		f.t.Fatalf("finish: %v %v", err, res)
	}
	return id
}

func (f *fixture) state(id int64) driver.State {
	f.t.Helper()
	r, err := f.c.Get(context.Background(), id)
	if err != nil {
		f.t.Fatal(err)
	}
	return r.State
}

func grant(a Access) func(*http.Request) Access {
	return func(*http.Request) Access { return a }
}

type response struct {
	code int
	body string
	hdr  http.Header
}

func do(h http.Handler, method, target string, body io.Reader, hdr ...string) response {
	r := httptest.NewRequest(method, target, body)
	for i := 0; i+1 < len(hdr); i += 2 {
		r.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return response{code: w.Code, body: w.Body.String(), hdr: w.Header()}
}

func get(h http.Handler, target string) response {
	return do(h, http.MethodGet, target, nil)
}

func form(h http.Handler, target string, v url.Values, hdr ...string) response {
	hdr = append([]string{"Content-Type", "application/x-www-form-urlencoded", "Sec-Fetch-Site", "same-origin", "Origin", "http://example.com"}, hdr...)
	return do(h, http.MethodPost, target, strings.NewReader(v.Encode()), hdr...)
}

func postJSON(h http.Handler, target, body string, hdr ...string) response {
	hdr = append([]string{"Content-Type", "application/json", "Sec-Fetch-Site", "same-origin"}, hdr...)
	return do(h, http.MethodPost, target, strings.NewReader(body), hdr...)
}
