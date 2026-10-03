package dashboard

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

type apiLogs struct {
	State    string `json:"state"`
	Progress int    `json:"progress"`
	Lines    []struct {
		Seq     int64  `json:"seq"`
		Attempt int    `json:"attempt"`
		Text    string `json:"text"`
	} `json:"lines"`
}

func TestConsoleAPI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := memstore.New()
	c := kiln.NewClient(s)
	id, err := c.Enqueue(ctx, resize{Key: "log.png"}, kiln.Queue("work"))
	if err != nil {
		t.Fatal(err)
	}
	js, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"work"}, Limit: 1, Server: "host-a:1:ab"})
	if err != nil || len(js) != 1 {
		t.Fatalf("claim: %v %d", err, len(js))
	}
	if err := s.WriteConsole(ctx, js[0].Ref, []string{"resizing", "<done>"}, 40); err != nil {
		t.Fatal(err)
	}
	h := New(c, Options{Authorize: grant(ReadOnly), Prefix: "/kiln"})
	base := "/kiln/api/jobs/" + strconv.FormatInt(id, 10) + "/logs"

	l := decodeJSON[apiLogs](t, h, base)
	if l.State != "processing" || l.Progress != 40 || len(l.Lines) != 2 || l.Lines[0].Text != "resizing" ||
		l.Lines[1].Text != "<done>" || l.Lines[1].Attempt != 1 {
		t.Fatalf("logs %+v", l)
	}
	next := decodeJSON[apiLogs](t, h, base+"?after="+strconv.FormatInt(l.Lines[0].Seq, 10))
	if len(next.Lines) != 1 || next.Lines[0] != l.Lines[1] {
		t.Fatalf("logs after line 1: %+v", next.Lines)
	}
	if one := decodeJSON[apiLogs](t, h, base+"?limit=1"); len(one.Lines) != 1 || one.Lines[0] != l.Lines[0] {
		t.Fatalf("logs with limit 1: %+v", one.Lines)
	}
	for path, code := range map[string]int{
		base + "?after=-1":       http.StatusBadRequest,
		base + "?limit=0":        http.StatusBadRequest,
		"/kiln/api/jobs/0/logs":  http.StatusNotFound,
		"/kiln/api/jobs/99/logs": http.StatusNotFound,
	} {
		if res := get(h, path); res.code != code {
			t.Errorf("%s: %d %s, want %d", path, res.code, res.body, code)
		}
	}

	page := get(h, "/kiln/jobs/"+strconv.FormatInt(id, 10))
	for _, want := range []string{`data-console="` + base + `"`, "data-live", "&lt;done&gt;", "data-progress",
		`width="40%"`} {
		if !strings.Contains(page.body, want) {
			t.Errorf("job page lacks %s", want)
		}
	}

	bare := New(kiln.NewClient(struct{ driver.Store }{s}), Options{Authorize: grant(ReadOnly)})
	if res := get(bare, "/api/jobs/"+strconv.FormatInt(id, 10)+"/logs"); res.code != http.StatusNotFound {
		t.Errorf("logs of a store without a console: %d", res.code)
	}
	if page := get(bare, "/jobs/"+strconv.FormatInt(id, 10)); page.code != http.StatusOK ||
		strings.Contains(page.body, "data-console") {
		t.Errorf("job page of a store without a console: %d %s", page.code, page.body)
	}
}
