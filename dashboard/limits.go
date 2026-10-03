package dashboard

import (
	"net/http"
	"net/url"
	"time"
)

type limitView struct {
	Key       string        `json:"key"`
	Max       int           `json:"max"`
	Rate      int           `json:"rate"`
	PerMS     int64         `json:"per_ms"`
	Per       time.Duration `json:"-"`
	Burst     int           `json:"burst"`
	Active    int           `json:"active"`
	Throttled int           `json:"throttled"`
	Reserved  int           `json:"reserved"`
	NextStart time.Time     `json:"next_start,omitzero"`
}

type limitList struct {
	Limits   []limitView `json:"limits"`
	Next     string      `json:"next,omitempty"`
	NextURL  string      `json:"-"`
	FirstURL string      `json:"-"`
}

func (h *handler) limits(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := limitParam(q.Get("limit"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	after := q.Get("after")
	ls, err := h.lr.Limits(r.Context(), after, limit+1)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	l := &limitList{}
	if len(ls) > limit {
		ls = ls[:limit]
		l.Next = ls[limit-1].Key
		l.NextURL = withQuery(h.link("/limits"), url.Values{"after": {l.Next}})
	}
	if after != "" {
		l.FirstURL = h.link("/limits")
	}
	l.Limits = make([]limitView, len(ls))
	for i, li := range ls {
		l.Limits[i] = limitView{
			Key:       li.Key,
			Max:       li.Max,
			Rate:      li.Rate,
			PerMS:     li.Per.Milliseconds(),
			Per:       li.Per,
			Burst:     li.Burst,
			Active:    li.Active,
			Throttled: li.Throttled,
			Reserved:  li.Reserved,
			NextStart: li.NextStart,
		}
	}
	h.show(w, r, "limits", l)
}
