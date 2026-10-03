package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const consolePage = 500

type lineView struct {
	Seq     int64     `json:"seq"`
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
	Text    string    `json:"text"`
}

type logsView struct {
	State    driver.State `json:"state"`
	Progress int          `json:"progress"`
	Lines    []lineView   `json:"lines"`
}

type consoleView struct {
	URL    string
	After  int64
	Live   bool
	Groups []lineGroup
}

type lineGroup struct {
	Attempt int
	Lines   []driver.LogLine
}

func (h *handler) console(ctx context.Context, rec *driver.Record) (*consoleView, error) {
	c, ok := h.store.(driver.Console)
	if !ok {
		return nil, nil
	}
	ls, err := c.Logs(ctx, rec.ID, 0, consolePage)
	if err != nil {
		return nil, err
	}
	v := &consoleView{
		URL:  h.link("/api/jobs/", rec.ID, "/logs"),
		Live: rec.State == driver.Processing || len(ls) == consolePage,
	}
	for _, l := range ls {
		if n := len(v.Groups); n == 0 || v.Groups[n-1].Attempt != l.Attempt {
			v.Groups = append(v.Groups, lineGroup{Attempt: l.Attempt})
		}
		g := &v.Groups[len(v.Groups)-1]
		g.Lines = append(g.Lines, l)
		v.After = l.Seq
	}
	return v, nil
}

func (h *handler) logs(w http.ResponseWriter, r *http.Request) {
	c, ok := h.store.(driver.Console)
	id, err := optID(r.PathValue("id"))
	if !ok || err != nil || id == 0 {
		h.fail(w, r, fmt.Errorf("%w: console of job %q", driver.ErrNotFound, r.PathValue("id")))
		return
	}
	q := r.URL.Query()
	var after int64
	if s := q.Get("after"); s != "" {
		if after, err = strconv.ParseInt(s, 10, 64); err != nil || after < 0 {
			h.fail(w, r, fmt.Errorf("%w: after %q", driver.ErrInvalid, s))
			return
		}
	}
	limit, err := limitParam(q.Get("limit"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	rec, err := h.c.Get(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	ls, err := c.Logs(r.Context(), id, after, limit)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	v := &logsView{State: rec.State, Progress: rec.Progress, Lines: make([]lineView, len(ls))}
	for i, l := range ls {
		v.Lines[i] = lineView(l)
	}
	writeJSON(w, http.StatusOK, v)
}
