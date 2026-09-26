package dashboard

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const countCap = 100000

type counts struct {
	Awaiting   int64 `json:"awaiting"`
	Scheduled  int64 `json:"scheduled"`
	Throttled  int64 `json:"throttled"`
	Enqueued   int64 `json:"enqueued"`
	Processing int64 `json:"processing"`
	Failed     int64 `json:"failed"`
	Retries    int64 `json:"retries"`
	Succeeded  int64 `json:"succeeded"`
	Deleted    int64 `json:"deleted"`
	Capped     bool  `json:"capped"`
}

func (c *counts) of(s driver.State) int64 {
	switch s {
	case driver.Awaiting:
		return c.Awaiting
	case driver.Scheduled:
		return c.Scheduled
	case driver.Throttled:
		return c.Throttled
	case driver.Enqueued:
		return c.Enqueued
	case driver.Processing:
		return c.Processing
	case driver.Failed:
		return c.Failed
	case driver.Succeeded:
		return c.Succeeded
	case driver.Deleted:
		return c.Deleted
	}
	return 0
}

func (c *counts) text(n int64) string {
	if c.Capped && n >= countCap {
		return num(n) + "+"
	}
	return num(n)
}

type snapshot struct {
	Counts    counts `json:"counts"`
	Servers   int    `json:"servers"`
	Workers   int    `json:"workers"`
	Running   int    `json:"running"`
	Queues    int    `json:"queues"`
	Paused    int    `json:"paused_queues"`
	Recurring int    `json:"recurring"`

	queues  []driver.QueueInfo
	servers []driver.ServerInfo
	kinds   []string
}

func (s *snapshot) count(key string) int64 {
	switch key {
	case "retries":
		return s.Counts.Retries
	case "recurring":
		return int64(s.Recurring)
	case "queues":
		return int64(s.Queues)
	case "servers":
		return int64(s.Servers)
	}
	return s.Counts.of(driver.State(key))
}

func (h *handler) loadOverview(ctx context.Context) (*snapshot, error) {
	c, err := h.store.Counts(ctx)
	if err != nil {
		return nil, err
	}
	servers, err := h.store.Servers(ctx)
	if err != nil {
		return nil, err
	}
	queues, err := h.store.Queues(ctx)
	if err != nil {
		return nil, err
	}
	recs, err := h.store.Recurrings(ctx)
	if err != nil {
		return nil, err
	}
	s := &snapshot{
		Counts:    counts(c),
		Servers:   len(servers),
		Queues:    len(queues),
		Recurring: len(recs),
		queues:    queues,
		servers:   servers,
	}
	kinds := make(map[string]struct{})
	for _, sv := range servers {
		s.Workers += sv.Workers
		s.Running += sv.Running
		for _, k := range sv.Kinds {
			kinds[k] = struct{}{}
		}
	}
	for _, r := range recs {
		kinds[r.Template.Kind] = struct{}{}
	}
	for _, q := range queues {
		if q.Paused {
			s.Paused++
		}
	}
	s.kinds = slices.Sorted(maps.Keys(kinds))
	return s, nil
}

type stat struct {
	Key   string
	Label string
	Note  string
	State driver.State
	N     int64
	Value string
	URL   string
	Sub   *stat
}

type stage struct {
	Label string
	Stats []stat
}

type overviewPage struct {
	*snapshot
	Mood       string
	Heat       string
	Load       float64
	Stages     []stage
	QueueRows  []queueView
	ServerRows []serverView
}

func (p *overviewPage) Stat(key string) stat {
	for _, g := range p.Stages {
		for _, s := range g.Stats {
			if s.Key == key {
				return s
			}
		}
	}
	return stat{}
}

func mood(s *snapshot) string {
	switch {
	case s.Servers == 0 && s.Counts == (counts{}):
		return "fresh"
	case s.Servers == 0:
		return "cold"
	case s.Counts.Failed > 0:
		return "failed"
	case s.Counts.Processing > 0:
		return "busy"
	case s.Counts.Enqueued > 0:
		return "waiting"
	}
	return "idle"
}

func heat(s *snapshot) string {
	switch {
	case s.Servers == 0:
		return "off"
	case s.Running == 0 || s.Workers == 0:
		return "idle"
	case s.Running*3 < s.Workers:
		return "low"
	case s.Running*3 < s.Workers*2:
		return "mid"
	}
	return "high"
}

func (h *handler) overview(w http.ResponseWriter, r *http.Request) {
	s, err := h.stats.get(r.Context())
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if isAPI(r) {
		writeJSON(w, http.StatusOK, s)
		return
	}
	c := &s.Counts
	state := func(st driver.State, note string) stat {
		n := c.of(st)
		return stat{Key: string(st), Label: label(st), Note: note, State: st, N: n, Value: c.text(n), URL: h.link("/jobs/", st)}
	}
	scheduled := state(driver.Scheduled, "")
	scheduled.Sub = &stat{Key: "retries", Label: "retrying", N: c.Retries, Value: c.text(c.Retries), URL: h.link("/retries")}
	succeeded := state(driver.Succeeded, "all time")
	succeeded.Value = num(c.Succeeded)
	deleted := state(driver.Deleted, "all time")
	deleted.Value = num(c.Deleted)
	p := &overviewPage{
		snapshot: s,
		Mood:     mood(s),
		Heat:     heat(s),
		Stages: []stage{
			{Label: "Held back", Stats: []stat{
				state(driver.Awaiting, "on other jobs"),
				scheduled,
				state(driver.Throttled, "by a limit"),
			}},
			{Label: "Queued & running", Stats: []stat{
				state(driver.Enqueued, "ready for a worker"),
				state(driver.Processing, "running now"),
			}},
			{Label: "Outcomes", Stats: []stat{
				succeeded,
				state(driver.Failed, "stay until handled"),
				deleted,
			}},
		},
		QueueRows:  queueViews(s.queues),
		ServerRows: serverViews(s.servers),
	}
	if s.Workers > 0 {
		p.Load = min(float64(s.Running)/float64(s.Workers)*100, 100)
	}
	h.render(w, r, http.StatusOK, "overview", "Overview", p)
}

type chart struct {
	Step   int64   `json:"step"`
	Points []point `json:"points"`
}

type point struct {
	At        time.Time `json:"at"`
	Succeeded int64     `json:"succeeded"`
	Failed    int64     `json:"failed"`
	Deleted   int64     `json:"deleted"`
	Retried   int64     `json:"retried"`
}

func (h *handler) loadSeries(ctx context.Context, window, step time.Duration) (*chart, error) {
	now, err := h.store.Now(ctx)
	if err != nil {
		return nil, err
	}
	n := int(window / step)
	from := now.Truncate(step).Add(-time.Duration(n-1) * step)
	pts, err := h.store.Series(ctx, from, now, step)
	if err != nil {
		return nil, err
	}
	c := &chart{Step: int64(step / time.Second), Points: make([]point, n)}
	for i := range c.Points {
		c.Points[i].At = from.Add(time.Duration(i) * step).UTC()
	}
	for _, p := range pts {
		i := int(p.At.Sub(from) / step)
		if i < 0 || i >= n {
			continue
		}
		q := &c.Points[i]
		q.Succeeded += p.Succeeded
		q.Failed += p.Failed
		q.Deleted += p.Deleted
		q.Retried += p.Retried
	}
	return c, nil
}

func (h *handler) series(w http.ResponseWriter, r *http.Request) {
	m := h.hour
	switch v := r.URL.Query().Get("range"); v {
	case "", "1h":
	case "24h":
		m = h.day
	default:
		h.fail(w, r, fmt.Errorf("%w: range %q", driver.ErrInvalid, v))
		return
	}
	c, err := m.get(r.Context())
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}
