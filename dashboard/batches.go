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
	ID             int64                  `json:"id"`
	Parent         int64                  `json:"parent,omitempty"`
	Description    string                 `json:"description,omitempty"`
	Meta           json.RawMessage        `json:"meta,omitempty"`
	Total          int64                  `json:"total"`
	Nested         int64                  `json:"nested,omitempty"`
	NestedFinished int64                  `json:"nested_finished,omitempty"`
	Sealed         bool                   `json:"sealed"`
	Finished       bool                   `json:"finished"`
	Counts         map[driver.State]int64 `json:"counts"`
	CreatedAt      time.Time              `json:"created_at,omitzero"`
	FinishedAt     time.Time              `json:"finished_at,omitzero"`
}

type batchPage struct {
	*batchView
	Children *batchList `json:"nested_batches,omitempty"`
}

type batchList struct {
	Batches  []*batchView `json:"batches"`
	Next     string       `json:"next,omitempty"`
	Parent   int64        `json:"-"`
	NextURL  string       `json:"-"`
	FirstURL string       `json:"-"`
}

type segment struct {
	State  driver.State
	N      int64
	X      float64
	W      float64
	Nested bool
}

func (h *handler) viewBatch(b *driver.Batch) *batchView {
	v := &batchView{
		ID:             b.ID,
		Parent:         b.Parent,
		Description:    b.Description,
		Total:          b.Total,
		Nested:         b.Nested,
		NestedFinished: b.NestedFinished,
		Sealed:         b.Sealed,
		Finished:       !b.FinishedAt.IsZero(),
		Counts:         b.Counts,
		CreatedAt:      b.CreatedAt,
		FinishedAt:     b.FinishedAt,
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
	n := b.size() + b.Nested
	if n == 0 {
		return 0
	}
	return int((b.Done() + b.NestedFinished) * 100 / n)
}

func (b *batchView) Segments() []segment {
	n := b.size() + b.Nested
	if n == 0 {
		return nil
	}
	segs := []segment{{State: driver.Succeeded, N: b.NestedFinished, Nested: true}}
	for _, s := range progressOrder {
		segs = append(segs, segment{State: s, N: b.Counts[s]})
	}
	segs = append(segs, segment{State: driver.Processing, N: b.Nested - b.NestedFinished, Nested: true})
	var out []segment
	x := 0.0
	for _, sg := range segs {
		if sg.N <= 0 {
			continue
		}
		sg.X, sg.W = x, float64(sg.N)/float64(n)*100
		out = append(out, sg)
		x += sg.W
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
	parent, err := optID(q.Get("parent"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	cursor := q.Get("cursor")
	pg, err := h.store.Batches(r.Context(), driver.BatchQuery{Limit: limit, Cursor: cursor, Parent: parent})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	l := h.listBatches(pg, parent)
	if cursor != "" {
		l.FirstURL = h.link(l.url(""))
	}
	h.show(w, r, "batches", l)
}

func (h *handler) listBatches(pg driver.BatchPage, parent int64) *batchList {
	l := &batchList{Batches: make([]*batchView, len(pg.Batches)), Next: pg.Next, Parent: parent}
	for i := range pg.Batches {
		l.Batches[i] = h.viewBatch(&pg.Batches[i])
	}
	if l.Next != "" {
		l.NextURL = h.link(l.url(l.Next))
	}
	return l
}

func (l *batchList) url(cursor string) string {
	q := url.Values{}
	if l.Parent != 0 {
		q.Set("parent", strconv.FormatInt(l.Parent, 10))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	return withQuery("/batches", q)
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
	p := &batchPage{batchView: h.viewBatch(&b)}
	if b.Nested > 0 {
		pg, err := h.store.Batches(r.Context(), driver.BatchQuery{Parent: id, Limit: 10})
		if err != nil {
			h.fail(w, r, err)
			return
		}
		p.Children = h.listBatches(pg, id)
	}
	h.show(w, r, "batch", p)
}
