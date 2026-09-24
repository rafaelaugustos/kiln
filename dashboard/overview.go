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

type card struct {
	Key   string
	Label string
	Hint  string
	State driver.State
	Value string
	URL   string
}

type overviewPage struct {
	*snapshot
	Cards      []card
	QueueRows  []queueView
	ServerRows []serverView
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
	state := func(st driver.State) card {
		return card{Key: string(st), Label: label(st), State: st, Value: c.text(c.of(st)), URL: h.link("/jobs/", st)}
	}
	p := &overviewPage{
		snapshot: s,
		Cards: []card{
			state(driver.Enqueued),
			state(driver.Processing),
			state(driver.Scheduled),
			{Key: "retries", Label: "Retries", State: driver.Scheduled, Value: c.text(c.Retries), URL: h.link("/retries")},
			state(driver.Awaiting),
			state(driver.Throttled),
			state(driver.Failed),
			{Key: "succeeded", Label: "Succeeded", Hint: "all time", State: driver.Succeeded, Value: num(c.Succeeded), URL: h.link("/jobs/", driver.Succeeded)},
			{Key: "deleted", Label: "Deleted", Hint: "all time", State: driver.Deleted, Value: num(c.Deleted), URL: h.link("/jobs/", driver.Deleted)},
			{Key: "servers", Label: "Servers", Value: num(s.Servers), URL: h.link("/servers")},
			{Key: "recurring", Label: "Recurring", Value: num(s.Recurring), URL: h.link("/recurring")},
			{Key: "queues", Label: "Queues", Value: num(s.Queues), URL: h.link("/queues")},
		},
		QueueRows:  queueViews(s.queues),
		ServerRows: serverViews(s.servers),
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
