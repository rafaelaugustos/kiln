package dashboard

import (
	"bytes"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestHeaders(t *testing.T) {
	t.Parallel()
	f := seed(t)
	h := New(f.c, Options{Authorize: grant(ReadOnly)})
	want := map[string]string{
		"Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
		"X-Content-Type-Options":  "nosniff",
		"Referrer-Policy":         "same-origin",
		"X-Frame-Options":         "DENY",
	}
	page := get(h, "/")
	for _, path := range []string{"/", "/jobs/failed", "/api/overview", "/nope"} {
		res := get(h, path)
		for k, v := range want {
			if got := res.hdr.Get(k); got != v {
				t.Errorf("%s: %s = %q", path, k, got)
			}
		}
		if got := res.hdr.Get("Cache-Control"); got != "no-store" {
			t.Errorf("%s: Cache-Control = %q", path, got)
		}
	}
	denied := get(New(f.c, Options{Authorize: grant(Denied)}), "/")
	if denied.hdr.Get("Content-Security-Policy") == "" {
		t.Error("denied response lacks CSP")
	}
	for _, m := range regexp.MustCompile(`(?:href|src)="(/assets/[^"]+)"`).FindAllStringSubmatch(page.body, -1) {
		res := get(h, m[1])
		if res.code != http.StatusOK || !strings.Contains(res.hdr.Get("Cache-Control"), "immutable") {
			t.Errorf("%s: %d %q", m[1], res.code, res.hdr.Get("Cache-Control"))
		}
		if res.hdr.Get("Content-Security-Policy") == "" || res.hdr.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s: missing security headers", m[1])
		}
		for _, bad := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
			if strings.Contains(res.body, bad) {
				t.Errorf("%s uses %s", m[1], bad)
			}
		}
	}
	if res := get(h, "/assets/app.css"); res.code != http.StatusNotFound {
		t.Errorf("unhashed asset: %d", res.code)
	}
}

func TestDenied(t *testing.T) {
	t.Parallel()
	f := seed(t)
	h := New(f.c, Options{Authorize: grant(Denied)})
	for _, path := range []string{"/", "/jobs/failed", "/jobs/1", "/retries", "/recurring", "/queues", "/servers", "/batches", "/api/overview", "/api/jobs/failed", "/assets/" + assets["app.css"], "/nope"} {
		if res := get(h, path); res.code != http.StatusForbidden {
			t.Errorf("GET %s: %d", path, res.code)
		}
	}
	id := strconv.FormatInt(f.failed, 10)
	if res := form(h, "/jobs/"+id+"/requeue", nil); res.code != http.StatusForbidden {
		t.Errorf("form post: %d", res.code)
	}
	if res := postJSON(h, "/api/jobs/"+id+"/requeue", ""); res.code != http.StatusForbidden {
		t.Errorf("json post: %d", res.code)
	}
	if f.state(f.failed) != driver.Failed {
		t.Error("denied request changed state")
	}
}

func TestMutationGuards(t *testing.T) {
	t.Parallel()
	f := seed(t)
	ro := New(f.c, Options{Authorize: grant(ReadOnly)})
	rw := New(f.c, Options{Authorize: grant(ReadWrite)})
	id := strconv.FormatInt(f.failed, 10)
	post := "/jobs/" + id + "/requeue"
	body := `{"ids":[` + id + `]}`
	tests := []struct {
		name string
		res  func() response
		code int
	}{
		{"read-only form", func() response { return form(ro, post, nil) }, http.StatusForbidden},
		{"read-only json", func() response { return postJSON(ro, "/api/jobs/requeue", body) }, http.StatusForbidden},
		{"cross-site fetch", func() response { return form(rw, post, nil, "Sec-Fetch-Site", "cross-site") }, http.StatusForbidden},
		{"same-site fetch", func() response { return form(rw, post, nil, "Sec-Fetch-Site", "same-site") }, http.StatusForbidden},
		{"foreign origin", func() response {
			return form(rw, post, nil, "Sec-Fetch-Site", "", "Origin", "http://evil.example")
		}, http.StatusForbidden},
		{"null origin", func() response { return form(rw, post, nil, "Sec-Fetch-Site", "", "Origin", "null") }, http.StatusForbidden},
		{"json without content type", func() response {
			return do(rw, http.MethodPost, "/api/jobs/requeue", strings.NewReader(body), "Sec-Fetch-Site", "same-origin")
		}, http.StatusUnsupportedMediaType},
		{"json as text/plain", func() response {
			return postJSON(rw, "/api/jobs/requeue", body, "Content-Type", "text/plain")
		}, http.StatusUnsupportedMediaType},
		{"json as form", func() response {
			return postJSON(rw, "/api/jobs/requeue", body, "Content-Type", "application/x-www-form-urlencoded")
		}, http.StatusUnsupportedMediaType},
		{"get on form action", func() response { return get(rw, post) }, http.StatusMethodNotAllowed},
		{"get on json action", func() response { return get(rw, "/api/queues/mailers/pause") }, http.StatusMethodNotAllowed},
		{"put", func() response {
			return do(rw, http.MethodPut, "/api"+post, strings.NewReader("{}"), "Content-Type", "application/json")
		}, http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		if res := tt.res(); res.code != tt.code {
			t.Errorf("%s: got %d, want %d", tt.name, res.code, tt.code)
		}
	}
	if f.state(f.failed) != driver.Failed {
		t.Error("rejected request changed state")
	}
	if res := form(rw, post, nil, "Sec-Fetch-Site", "", "Origin", ""); res.code != http.StatusSeeOther {
		t.Errorf("non-browser client: %d", res.code)
	}
}

func TestAllowAllLoopbackOnly(t *testing.T) {
	t.Parallel()
	f := seed(t)
	h := New(f.c, Options{Authorize: AllowAll})
	for _, host := range []string{"localhost", "localhost:8080", "127.0.0.1:8080", "[::1]:8080", "127.0.0.2"} {
		if res := get(h, "http://"+host+"/api/jobs/failed"); res.code != http.StatusOK {
			t.Errorf("%s: %d", host, res.code)
		}
	}
	evil := "http://evil.example:8080"
	for _, host := range []string{"evil.example:8080", "10.0.0.1:8080", "localhost.evil.example"} {
		if res := get(h, "http://"+host+"/api/jobs/failed"); res.code != http.StatusForbidden {
			t.Errorf("%s: %d", host, res.code)
		}
	}
	res := postJSON(h, evil+"/api/jobs/delete", `{"state":"failed"}`, "Origin", evil)
	if res.code != http.StatusForbidden || f.state(f.failed) != driver.Failed {
		t.Fatalf("rebound delete: %d %s", res.code, f.state(f.failed))
	}
}

func TestActions(t *testing.T) {
	t.Parallel()
	f := seed(t)
	h := New(f.c, Options{Authorize: grant(ReadWrite)})
	str := func(n int64) string { return strconv.FormatInt(n, 10) }

	res := form(h, "/jobs/"+str(f.failed)+"/requeue", nil)
	if res.code != http.StatusSeeOther || res.hdr.Get("Location") != "/jobs/"+str(f.failed)+"?done=requeued&n=1" {
		t.Fatalf("requeue: %d %q", res.code, res.hdr.Get("Location"))
	}
	if s := f.state(f.failed); s != driver.Enqueued {
		t.Fatalf("requeued job is %s", s)
	}
	if res := get(h, res.hdr.Get("Location")); !strings.Contains(res.body, "Requeued 1 job") {
		t.Error("missing flash")
	}

	res = form(h, "/jobs/delete", url.Values{"id": {str(f.enqueued)}, "state": {"enqueued"}, "queue": {"mailers"}})
	if res.code != http.StatusSeeOther || res.hdr.Get("Location") != "/jobs/enqueued?done=deleted&n=1&queue=mailers" {
		t.Fatalf("bulk delete: %d %q", res.code, res.hdr.Get("Location"))
	}
	if s := f.state(f.enqueued); s != driver.Deleted {
		t.Fatalf("deleted job is %s", s)
	}

	res = form(h, "/jobs/requeue", url.Values{"all": {"1"}, "state": {"deleted"}})
	if res.code != http.StatusSeeOther || res.hdr.Get("Location") != "/jobs/deleted?done=requeued&n=2" {
		t.Fatalf("requeue all: %d %q", res.code, res.hdr.Get("Location"))
	}

	res = form(h, "/jobs/requeue", url.Values{"id": {str(f.retry)}, "state": {"scheduled"}, "back": {"retries"}})
	if res.hdr.Get("Location") != "/retries?done=requeued&n=1" || f.state(f.retry) != driver.Enqueued {
		t.Fatalf("retry now: %q %s", res.hdr.Get("Location"), f.state(f.retry))
	}

	res = form(h, "/jobs/requeue", url.Values{"state": {"https://evil.example"}})
	if loc := res.hdr.Get("Location"); loc != "/jobs/enqueued?done=requeued&n=0" {
		t.Fatalf("redirect escaped fixed paths: %q", loc)
	}

	res = postJSON(h, "/api/jobs/delete", `{"ids":[`+str(f.scheduled)+`]}`)
	if res.code != http.StatusOK || strings.TrimSpace(res.body) != `{"count":1}` {
		t.Fatalf("api delete: %d %s", res.code, res.body)
	}
	res = postJSON(h, "/api/jobs/"+str(f.processing)+"/delete", "")
	if res.code != http.StatusOK || strings.TrimSpace(res.body) != `{"count":1}` {
		t.Fatalf("api cancel: %d %s", res.code, res.body)
	}
	if r, _ := f.c.Get(t.Context(), f.processing); !r.CancelRequested {
		t.Error("processing job not marked for cancellation")
	}
	if res := postJSON(h, "/api/jobs/requeue", `{}`); res.code != http.StatusBadRequest {
		t.Errorf("empty filter: %d", res.code)
	}
	if res := postJSON(h, "/api/jobs/requeue", `{"idz":[1]}`); res.code != http.StatusBadRequest {
		t.Errorf("unknown field: %d", res.code)
	}
	if res := postJSON(h, "/api/jobs/1/explode", ""); res.code != http.StatusNotFound {
		t.Errorf("unknown op: %d", res.code)
	}

	res = postJSON(h, "/api/recurring/nightly/trigger", "")
	if res.code != http.StatusOK || !strings.HasPrefix(res.body, `{"id":`) {
		t.Fatalf("trigger: %d %s", res.code, res.body)
	}
	res = form(h, "/recurring/nightly/pause", nil)
	if res.hdr.Get("Location") != "/recurring?done=recurring-paused&n=0" {
		t.Fatalf("pause recurring: %q", res.hdr.Get("Location"))
	}
	if r, _ := f.store.Recurring(t.Context(), "nightly"); !r.Paused {
		t.Error("recurring not paused")
	}
	if res := postJSON(h, "/api/recurring/nightly/resume", ""); res.code != http.StatusNoContent {
		t.Errorf("resume recurring: %d", res.code)
	}
	res = form(h, "/recurring/nightly/trigger", nil)
	if loc := res.hdr.Get("Location"); !strings.HasPrefix(loc, "/recurring?done=triggered&n=") {
		t.Fatalf("trigger form: %q", loc)
	}
	if res := get(h, res.hdr.Get("Location")); !strings.Contains(res.body, "View job") {
		t.Error("trigger flash lacks job link")
	}
	if res := postJSON(h, "/api/recurring/nightly/remove", ""); res.code != http.StatusNoContent {
		t.Errorf("remove recurring: %d", res.code)
	}
	if res := postJSON(h, "/api/recurring/nightly/remove", ""); res.code != http.StatusNotFound {
		t.Errorf("remove missing recurring: %d", res.code)
	}

	if res := postJSON(h, "/api/queues/low/resume", ""); res.code != http.StatusNoContent {
		t.Errorf("resume queue: %d", res.code)
	}
	if res := postJSON(h, "/api/queues/Bad%20Queue/pause", ""); res.code != http.StatusBadRequest {
		t.Errorf("invalid queue: %d", res.code)
	}
	qs, _ := f.store.Queues(t.Context())
	for _, q := range qs {
		if q.Paused {
			t.Errorf("queue %s still paused", q.Name)
		}
	}

	big := bytes.Repeat([]byte("1,"), 1<<20)
	if res := postJSON(h, "/api/jobs/requeue", `{"ids":[`+string(big)+`1]}`); res.code != http.StatusBadRequest {
		t.Errorf("oversized body: %d", res.code)
	}
}
