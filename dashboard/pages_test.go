package dashboard

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/memstore"
)

func TestPages(t *testing.T) {
	t.Parallel()
	f := seed(t)
	h := New(f.c, Options{Authorize: grant(ReadOnly), Title: "Acme jobs", Actor: func(*http.Request) string { return "ana" }})
	id := func(n int64) string { return strconv.FormatInt(n, 10) }
	tests := []struct {
		path string
		want []string
	}{
		{"/", []string{"<title>Overview · Acme jobs</title>", "Throughput", `data-chart`, `data-count="failed"`, "host-a", "Read-only", "ana"}},
		{"/jobs", nil},
		{"/jobs/enqueued", []string{"email.send", "mailers", `href="/jobs/` + id(f.enqueued) + `"`}},
		{"/jobs/processing", []string{"host-a:1:ab", "Started"}},
		{"/jobs/scheduled", []string{id(f.scheduled), id(f.retry), "Runs"}},
		{"/jobs/awaiting", []string{id(f.awaiting), "Waiting on"}},
		{"/jobs/throttled", []string{id(f.throttled), "lk"}},
		{"/jobs/succeeded", []string{id(f.succeeded), "Finished"}},
		{"/jobs/failed", []string{id(f.failed), "email.send"}},
		{"/jobs/deleted", []string{id(f.deleted)}},
		{"/jobs/failed?queue=nope", []string{"No failed jobs match these filters."}},
		{"/jobs/" + id(f.failed), []string{"exhausted", "smtp rejected", "Stack trace", "goroutine 7", "Created"}},
		{"/jobs/" + id(f.succeeded), []string{"Output", `<span class="k">&#34;bytes&#34;</span>`, `<span class="n">42</span>`}},
		{"/jobs/" + id(f.awaiting), []string{"Parents", `href="/jobs/` + id(f.scheduled) + `"`}},
		{"/retries", []string{id(f.retry), "Next retry"}},
		{"/recurring", []string{"nightly", "0 3 * * *", "UTC"}},
		{"/queues", []string{"low", "Paused", "mailers"}},
		{"/servers", []string{"host-a", "host-a:1:ab", "All kinds"}},
		{"/batches", []string{"Import catalog", `href="/batches/` + id(f.batch) + `"`}},
		{"/batches/" + id(f.batch), []string{"Import catalog", `href="/jobs/enqueued?batch=` + id(f.batch) + `"`}},
		{"/jobs/enqueued?batch=" + id(f.batch), []string{"Batch " + id(f.batch), "b1.png"}},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			res := get(h, tt.path)
			if tt.path == "/jobs" {
				if res.code != http.StatusFound || res.hdr.Get("Location") != "/jobs/enqueued" {
					t.Fatalf("got %d %q", res.code, res.hdr.Get("Location"))
				}
				return
			}
			if res.code != http.StatusOK {
				t.Fatalf("status %d: %s", res.code, res.body)
			}
			if ct := res.hdr.Get("Content-Type"); ct != "text/html; charset=utf-8" {
				t.Errorf("content type %q", ct)
			}
			for _, w := range tt.want {
				if !strings.Contains(res.body, w) {
					t.Errorf("missing %q", w)
				}
			}
			if strings.Contains(res.body, `method="post"`) {
				t.Error("read-only page renders a mutation form")
			}
			if strings.Contains(res.body, "<script>") || strings.Contains(res.body, " style=") || strings.Contains(res.body, " onclick") {
				t.Error("inline script or style")
			}
		})
	}
	if res := get(h, "/retries"); strings.Contains(res.body, `href="/jobs/`+id(f.scheduled)+`"`) {
		t.Error("retries lists a scheduled job that never ran")
	}
}

func TestWritablePages(t *testing.T) {
	t.Parallel()
	f := seed(t)
	h := New(f.c, Options{Authorize: grant(ReadWrite)})
	for path, want := range map[string][]string{
		"/jobs/failed":     {`id="bulk"`, "Requeue all matching", "Delete all matching", `formaction="/jobs/delete"`},
		"/jobs/processing": {"Cancel all matching"},
		"/jobs/" + strconv.FormatInt(f.failed, 10): {`action="/jobs/` + strconv.FormatInt(f.failed, 10) + `/requeue"`},
		"/recurring": {`action="/recurring/nightly/trigger"`, `action="/recurring/nightly/pause"`, `action="/recurring/nightly/remove"`},
		"/queues":    {`action="/queues/low/resume"`, `action="/queues/mailers/pause"`},
	} {
		res := get(h, path)
		if res.code != http.StatusOK {
			t.Fatalf("%s: status %d", path, res.code)
		}
		for _, w := range want {
			if !strings.Contains(res.body, w) {
				t.Errorf("%s: missing %q", path, w)
			}
		}
		if strings.Contains(res.body, "Read-only") {
			t.Errorf("%s: read-only tag for a writer", path)
		}
	}
	if res := get(h, "/jobs/succeeded"); strings.Contains(res.body, "Delete all matching") {
		t.Error("archived jobs offer delete")
	}
}

func TestNotFound(t *testing.T) {
	t.Parallel()
	f := seed(t)
	h := New(f.c, Options{Authorize: grant(ReadOnly)})
	for _, path := range []string{"/nope", "/jobs/bogus", "/jobs/999999", "/batches/999", "/batches/x", "/jobs/../queues", "//jobs/failed"} {
		if res := get(h, path); res.code != http.StatusNotFound {
			t.Errorf("%s: status %d", path, res.code)
		}
	}
	res := get(h, "/api/jobs/999999")
	if res.code != http.StatusNotFound || !strings.Contains(res.body, `"error"`) {
		t.Errorf("api: %d %s", res.code, res.body)
	}
	if res := get(h, "/jobs/failed?batch=x"); res.code != http.StatusBadRequest {
		t.Errorf("bad batch: %d", res.code)
	}
	if res := get(h, "/jobs/failed?cursor=bogus"); res.code != http.StatusBadRequest {
		t.Errorf("bad cursor: %d", res.code)
	}
}

func TestEscaping(t *testing.T) {
	t.Parallel()
	f := seed(t)
	id := f.enqueue(email{To: `<script>alert(1)</script>`, Secret: `"><img src=x onerror=alert(1)>`})
	h := New(f.c, Options{Authorize: grant(ReadOnly)})
	for _, path := range []string{"/jobs/enqueued", "/jobs/" + strconv.FormatInt(id, 10)} {
		res := get(h, path)
		if res.code != http.StatusOK {
			t.Fatalf("%s: %d", path, res.code)
		}
		if strings.Contains(res.body, "<script>alert") || strings.Contains(res.body, "<img src=x") {
			t.Errorf("%s: unescaped args", path)
		}
	}
}

func TestPrefix(t *testing.T) {
	t.Parallel()
	f := seed(t)
	strip := http.NewServeMux()
	strip.Handle("/kiln/", http.StripPrefix("/kiln", New(f.c, Options{Prefix: "/kiln", Authorize: grant(ReadWrite)})))
	direct := http.NewServeMux()
	direct.Handle("/kiln/", New(f.c, Options{Prefix: "/kiln/", Authorize: grant(ReadWrite)}))
	failed := strconv.FormatInt(f.failed, 10)
	for name, h := range map[string]http.Handler{"strip": strip, "direct": direct} {
		t.Run(name, func(t *testing.T) {
			res := get(h, "/kiln/")
			if res.code != http.StatusOK {
				t.Fatalf("status %d", res.code)
			}
			for _, w := range []string{`href="/kiln/jobs/enqueued"`, `href="/kiln/assets/app.`, `data-api="/kiln/api"`, `data-src="/kiln/api/series?range=1h"`} {
				if !strings.Contains(res.body, w) {
					t.Errorf("missing %q", w)
				}
			}
			if res := get(h, "/kiln/jobs/"+failed); res.code != http.StatusOK || !strings.Contains(res.body, `action="/kiln/jobs/`+failed+`/delete"`) {
				t.Errorf("detail: %d", res.code)
			}
			if res := get(h, "/kiln/api/overview"); res.code != http.StatusOK {
				t.Errorf("api: %d", res.code)
			}
			if res := get(h, "/kiln/jobs"); res.hdr.Get("Location") != "/kiln/jobs/enqueued" {
				t.Errorf("index redirect %q", res.hdr.Get("Location"))
			}
			res = form(h, "/kiln/queues/mailers/pause", nil)
			if res.code != http.StatusSeeOther || !strings.HasPrefix(res.hdr.Get("Location"), "/kiln/queues?") {
				t.Errorf("redirect %d %q", res.code, res.hdr.Get("Location"))
			}
			form(h, "/kiln/queues/mailers/resume", nil)
		})
	}
	root := New(f.c, Options{Authorize: grant(ReadWrite)})
	if res := get(root, "/"); !strings.Contains(res.body, `href="/jobs/enqueued"`) || !strings.Contains(res.body, `data-api="/api"`) {
		t.Error("root mount links")
	}
}

func TestNewPanicsWithoutAuthorize(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	New(kiln.NewClient(memstore.New()), Options{})
}
