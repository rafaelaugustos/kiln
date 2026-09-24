package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var progressOrder = []driver.State{
	driver.Succeeded, driver.Deleted, driver.Failed, driver.Processing,
	driver.Enqueued, driver.Scheduled, driver.Throttled, driver.Awaiting,
}

type batchView struct {
	ID          int64                  `json:"id"`
	Description string                 `json:"description,omitempty"`
	Meta        json.RawMessage        `json:"meta,omitempty"`
	Total       int64                  `json:"total"`
	Sealed      bool                   `json:"sealed"`
	Finished    bool                   `json:"finished"`
	Counts      map[driver.State]int64 `json:"counts"`
	CreatedAt   time.Time              `json:"created_at,omitzero"`
	FinishedAt  time.Time              `json:"finished_at,omitzero"`
}

type batchList struct {
	Batches  []*batchView `json:"batches"`
	Next     string       `json:"next,omitempty"`
	NextURL  string       `json:"-"`
	FirstURL string       `json:"-"`
}

type segment struct {
	State driver.State
	N     int64
	X     float64
	W     float64
}

func (h *handler) viewBatch(b *driver.Batch) *batchView {
	v := &batchView{
		ID:          b.ID,
		Description: b.Description,
		Total:       b.Total,
		Sealed:      b.Sealed,
		Finished:    !b.FinishedAt.IsZero(),
		Counts:      b.Counts,
		CreatedAt:   b.CreatedAt,
		FinishedAt:  b.FinishedAt,
	}
	if v.Counts == nil {
		v.Counts = map[driver.State]int64{}
	}
	if len(b.Meta) > 0 {
		m, _ := json.Marshal(b.Meta)
		v.Meta = h.redact("", PartMeta, m)
	}
	return v
}

func (b *batchView) size() int64 {
	var n int64
	for _, c := range b.Counts {
		n += c
	}
	return max(n, b.Total)
}

func (b *batchView) Done() int64 {
	return b.Counts[driver.Succeeded] + b.Counts[driver.Deleted]
}

func (b *batchView) Percent() int {
	n := b.size()
	if n == 0 {
		return 0
	}
	return int(b.Done() * 100 / n)
}

func (b *batchView) Segments() []segment {
	n := b.size()
	if n == 0 {
		return nil
	}
	var out []segment
	x := 0.0
	for _, s := range progressOrder {
		c := b.Counts[s]
		if c == 0 {
			continue
		}
		w := float64(c) / float64(n) * 100
		out = append(out, segment{State: s, N: c, X: x, W: w})
		x += w
	}
	return out
}

func (b *batchView) Rows() []segment {
	var out []segment
	for _, s := range tabOrder {
		out = append(out, segment{State: s, N: b.Counts[s]})
	}
	return out
}

func (h *handler) batches(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := limitParam(q.Get("limit"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	cursor := q.Get("cursor")
	pg, err := h.store.Batches(r.Context(), driver.BatchQuery{Limit: limit, Cursor: cursor})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	l := &batchList{Batches: make([]*batchView, len(pg.Batches)), Next: pg.Next}
	for i := range pg.Batches {
		l.Batches[i] = h.viewBatch(&pg.Batches[i])
	}
	if l.Next != "" {
		l.NextURL = withQuery(h.link("/batches"), url.Values{"cursor": {l.Next}})
	}
	if cursor != "" {
		l.FirstURL = h.link("/batches")
	}
	h.show(w, r, "batches", "Batches", l)
}

func (h *handler) batch(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		h.fail(w, r, fmt.Errorf("%w: batch %q", driver.ErrNotFound, r.PathValue("id")))
		return
	}
	b, err := h.store.Batch(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.show(w, r, "batch", "Batch "+strconv.FormatInt(id, 10), h.viewBatch(&b))
}
