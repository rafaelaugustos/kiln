package dashboard

import (
	"net/http"
	"net/url"
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
		{"/", []string{"<title>Overview · Acme jobs</title>", "Throughput", `data-chart`, `data-count="failed"`, "host-a", "Read-only", "ana", `data-mood="failed"`, ">failed job needs a look<", `href="/retries"`}},
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
		{"/recurring", []string{"nightly", "0 3 * * *", "UTC", `<span class="chip plain">reports</span>`}},
		{"/limits", []string{`href="/limits" class="on"`, `<span class="mono strong">lk</span>`}},
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

type report struct {
	Day string `json:"day"`
}

func (report) Kind() string { return "report.build" }

func (r report) Title() string { return "Report for " + r.Day }

func TestTitlesAndTags(t *testing.T) {
	t.Parallel()
	s := memstore.New()
	f := &fixture{t: t, store: s, c: kiln.NewClient(s)}
	id := func(n int64) string { return strconv.FormatInt(n, 10) }
	first := f.enqueue(report{Day: "monday"}, kiln.Tags{"reports", "a&b"})
	monday := id(first)
	weekly := id(f.enqueue(report{Day: "sunday"}, kiln.Title("Weekly digest"), kiln.Tags{"reports"}))
	plain := id(f.enqueue(resize{Key: "plain.png"}))
	h := New(f.c, Options{Authorize: grant(ReadWrite)})

	for _, tt := range []struct {
		path      string
		want, not []string
	}{
		{"/jobs/enqueued", []string{">Report for monday<", ">Weekly digest<", `title="report.build"`, ">image.resize<",
			`href="/jobs/enqueued?tag=a%26b"`, `href="/jobs/enqueued?tag=reports"`, "Delete all matching"}, nil},
		{"/jobs/enqueued?tag=reports", []string{`href="/jobs/` + monday + `"`, `href="/jobs/` + weekly + `"`,
			`name="tag" value="reports"`, `href="/jobs/scheduled?tag=reports"`},
			[]string{`href="/jobs/` + plain + `"`, "all matching"}},
		{"/jobs/enqueued?tag=a%26b", []string{`href="/jobs/` + monday + `"`}, []string{`href="/jobs/` + weekly + `"`}},
		{"/jobs/scheduled?tag=reports", []string{"No scheduled jobs match these filters."}, nil},
		{"/jobs/" + weekly, []string{`<h1 class="title"><span>Weekly digest</span>`, "<dt>Kind</dt>",
			`href="/jobs/enqueued?tag=reports"`}, nil},
		{"/jobs/" + plain, []string{`<span class="mono">image.resize</span>`}, []string{"<dt>Kind</dt>"}},
	} {
		res := get(h, tt.path)
		if res.code != http.StatusOK {
			t.Fatalf("%s: status %d", tt.path, res.code)
		}
		for _, w := range tt.want {
			if !strings.Contains(res.body, w) {
				t.Errorf("%s: missing %q", tt.path, w)
			}
		}
		for _, w := range tt.not {
			if strings.Contains(res.body, w) {
				t.Errorf("%s: has %q", tt.path, w)
			}
		}
	}

	page := decodeJSON[struct {
		Tag  string `json:"tag"`
		Jobs []struct {
			Title string `json:"title"`
		} `json:"jobs"`
	}](t, h, "/api/jobs/enqueued?tag=reports")
	if page.Tag != "reports" || len(page.Jobs) != 2 || page.Jobs[0].Title != "Report for monday" || page.Jobs[1].Title != "Weekly digest" {
		t.Errorf("api page %+v", page)
	}

	all := url.Values{"state": {"enqueued"}, "tag": {"reports"}, "all": {"1"}}
	if res := form(h, "/jobs/delete", all); res.code != http.StatusBadRequest {
		t.Errorf("delete all matching a tag: status %d", res.code)
	}
	if st := f.state(first); st != kiln.Enqueued {
		t.Errorf("job %d is %s after a refused delete", first, st)
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

func TestNestedBatchPages(t *testing.T) {
	t.Parallel()
	c := kiln.NewClient(memstore.New())
	outer := &kiln.Batch{Description: "Monthly close"}
	inner := &kiln.Batch{Description: "Account 7"}
	inner.Add(resize{Key: "a7.png"})
	outer.AddBatch(inner)
	id, err := c.StartBatch(t.Context(), outer)
	if err != nil {
		t.Fatal(err)
	}
	h := New(c, Options{Authorize: grant(ReadOnly)})
	sid := strconv.FormatInt(id, 10)

	r := get(h, "/batches/"+sid)
	for _, want := range []string{"Nested batches", "Account 7", "0 of 1 nested batches finished"} {
		if r.code != http.StatusOK || !strings.Contains(r.body, want) {
			t.Errorf("outer page: %d, missing %q", r.code, want)
		}
	}
	children := decodeJSON[struct {
		Batches []struct {
			ID     int64 `json:"id"`
			Parent int64 `json:"parent"`
		} `json:"batches"`
	}](t, h, "/api/batches?parent="+sid)
	if len(children.Batches) != 1 || children.Batches[0].Parent != id {
		t.Fatalf("children %+v", children)
	}
	r = get(h, "/batches/"+strconv.FormatInt(children.Batches[0].ID, 10))
	if r.code != http.StatusOK || !strings.Contains(r.body, `href="/batches/`+sid+`"`) {
		t.Errorf("inner page: %d, no link to the parent", r.code)
	}
	r = do(h, http.MethodGet, "/batches/"+sid, nil, "Accept-Language", "pt-BR")
	if !strings.Contains(r.body, "0 de 1 lotes aninhados concluídos") {
		t.Errorf("pt-BR outer page has no nested summary")
	}
}
