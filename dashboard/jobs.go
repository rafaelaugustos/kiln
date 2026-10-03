package dashboard

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
)

const (
	pageSize  = 50
	maxIDs    = 1000
	retryScan = 2000
)

var tabOrder = []driver.State{
	driver.Enqueued, driver.Processing, driver.Scheduled, driver.Awaiting,
	driver.Throttled, driver.Failed, driver.Succeeded, driver.Deleted,
}

type tab struct {
	State   driver.State
	URL     string
	N       int64
	Capped  bool
	Counted bool
	On      bool
}

type jobList struct {
	State driver.State `json:"state"`
	Queue string       `json:"queue,omitempty"`
	Kind  string       `json:"kind,omitempty"`
	Batch int64        `json:"batch,omitempty"`
	Tag   string       `json:"tag,omitempty"`
	Jobs  []*jobView   `json:"jobs"`
	Next  string       `json:"next,omitempty"`

	Retries  bool     `json:"-"`
	Tabs     []tab    `json:"-"`
	Queues   []string `json:"-"`
	Kinds    []string `json:"-"`
	Self     string   `json:"-"`
	NextURL  string   `json:"-"`
	FirstURL string   `json:"-"`
	Unbatch  string   `json:"-"`
}

func (l *jobList) filter() url.Values {
	q := url.Values{}
	if l.Queue != "" {
		q.Set("queue", l.Queue)
	}
	if l.Kind != "" {
		q.Set("kind", l.Kind)
	}
	if l.Batch != 0 {
		q.Set("batch", strconv.FormatInt(l.Batch, 10))
	}
	if l.Tag != "" {
		q.Set("tag", l.Tag)
	}
	return q
}

func (l *jobList) Filtered() bool {
	return l.Queue != "" || l.Kind != "" || l.Batch != 0 || l.Tag != ""
}

func (l *jobList) Column() string {
	switch {
	case l.Retries:
		return "col.next_retry"
	case l.State == driver.Scheduled:
		return "col.runs"
	case l.State == driver.Awaiting:
		return "col.created"
	case l.State == driver.Throttled:
		return "col.ready"
	case l.State == driver.Enqueued:
		return "col.enqueued_at"
	case l.State == driver.Processing:
		return "col.started"
	}
	return "col.finished"
}

func (l *jobList) When(v *jobView) time.Time {
	switch l.State {
	case driver.Scheduled, driver.Throttled, driver.Enqueued:
		return v.RunAt
	case driver.Awaiting:
		return v.CreatedAt
	case driver.Processing:
		return v.AttemptedAt
	}
	return v.FinalizedAt
}

func (l *jobList) CanRequeue() bool { return requeueable(l.State) }
func (l *jobList) CanDelete() bool  { return l.State.Live() }
func (l *jobList) CanFilter() bool  { return !l.Retries }

func (l *jobList) DeleteLabel() string { return deleteLabel(l.State) }

func (l *jobList) Empty() string {
	switch {
	case l.Retries && l.Next != "":
		return "retries.empty_page"
	case l.Retries:
		return "retries.empty"
	case l.Filtered() && l.Next != "":
		return "jobs.empty_filtered_page"
	case l.Filtered():
		return "jobs.empty_filtered"
	case l.State == driver.Failed:
		return "jobs.empty_failed"
	}
	return "jobs.empty"
}

func (h *handler) jobsIndex(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, h.link("/jobs/", driver.Enqueued), http.StatusFound)
}

func (h *handler) jobs(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if id, err := strconv.ParseInt(key, 10, 64); err == nil {
		h.detail(w, r, id)
		return
	}
	st := driver.State(key)
	if !st.Valid() {
		h.fail(w, r, fmt.Errorf("%w: state %q", driver.ErrNotFound, key))
		return
	}
	q := r.URL.Query()
	l := &jobList{State: st, Queue: q.Get("queue"), Kind: q.Get("kind"), Tag: q.Get("tag")}
	var err error
	if l.Batch, err = optID(q.Get("batch")); err != nil {
		h.fail(w, r, err)
		return
	}
	limit, err := limitParam(q.Get("limit"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	cursor := q.Get("cursor")
	pg, err := h.c.List(r.Context(), kiln.JobQuery{
		State: st, Queue: l.Queue, Kind: l.Kind, BatchID: l.Batch, Tag: l.Tag, Limit: limit, Cursor: cursor,
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	l.Jobs, l.Next = h.viewAll(pg.Records), pg.Next
	if isAPI(r) {
		writeJSON(w, http.StatusOK, l)
		return
	}
	l.Self = h.link("/jobs/", st)
	f := l.filter()
	if l.Next != "" {
		q := l.filter()
		q.Set("cursor", l.Next)
		l.NextURL = withQuery(l.Self, q)
	}
	if cursor != "" {
		l.FirstURL = withQuery(l.Self, f)
	}
	if l.Batch != 0 {
		q := l.filter()
		q.Del("batch")
		l.Unbatch = withQuery(l.Self, q)
	}
	s, _ := h.stats.get(r.Context())
	for _, t := range tabOrder {
		tb := tab{State: t, URL: withQuery(h.link("/jobs/", t), f), On: t == st}
		if s != nil && !l.Filtered() {
			n := s.Counts.of(t)
			tb.N, tb.Capped, tb.Counted = n, s.Counts.capped(n), true
		}
		l.Tabs = append(l.Tabs, tb)
	}
	if s != nil {
		for _, qi := range s.queues {
			l.Queues = append(l.Queues, qi.Name)
		}
		l.Kinds = s.kinds
	}
	if l.Queue != "" && !slices.Contains(l.Queues, l.Queue) {
		l.Queues = append(l.Queues, l.Queue)
	}
	h.render(w, r, http.StatusOK, "jobs", l)
}

func (h *handler) retries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := limitParam(q.Get("limit"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	l := &jobList{State: driver.Scheduled, Retries: true, Jobs: []*jobView{}}
	l.Next, err = h.scanRetries(r.Context(), q.Get("cursor"), limit, func(rec *driver.Record) {
		l.Jobs = append(l.Jobs, h.view(rec))
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if isAPI(r) {
		writeJSON(w, http.StatusOK, l)
		return
	}
	l.Self = h.link("/retries")
	if l.Next != "" {
		l.NextURL = withQuery(l.Self, url.Values{"cursor": {l.Next}})
	}
	if q.Get("cursor") != "" {
		l.FirstURL = l.Self
	}
	h.render(w, r, http.StatusOK, "retries", l)
}

func (h *handler) scanRetries(ctx context.Context, cursor string, limit int, add func(*driver.Record)) (string, error) {
	for scanned := 0; limit > 0 && scanned < retryScan; {
		pg, err := h.scheduled(ctx, cursor, 500)
		if err != nil {
			return "", err
		}
		for n := upTo(pg.Records, limit); n > 0 && n < len(pg.Records); n = upTo(pg.Records, limit) {
			if pg, err = h.scheduled(ctx, cursor, n); err != nil {
				return "", err
			}
		}
		for i := range pg.Records {
			if pg.Records[i].Attempt > 0 {
				add(&pg.Records[i])
				limit--
			}
		}
		if cursor = pg.Next; cursor == "" {
			return "", nil
		}
		scanned += len(pg.Records)
	}
	return cursor, nil
}

func (h *handler) scheduled(ctx context.Context, cursor string, limit int) (driver.Page, error) {
	return h.c.List(ctx, kiln.JobQuery{State: driver.Scheduled, Limit: limit, Cursor: cursor})
}

func upTo(recs []driver.Record, n int) int {
	for i := range recs {
		if recs[i].Attempt == 0 {
			continue
		}
		if n--; n == 0 {
			return i + 1
		}
	}
	return 0
}

func (h *handler) detail(w http.ResponseWriter, r *http.Request, id int64) {
	rec, err := h.c.Get(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	v := h.view(&rec)
	if !isAPI(r) {
		if v.Console, err = h.console(r.Context(), &rec); err != nil {
			h.fail(w, r, err)
			return
		}
	}
	h.show(w, r, "job", v)
}

type selection struct {
	IDs   []int64      `json:"ids"`
	State driver.State `json:"state"`
	Queue string       `json:"queue"`
	Kind  string       `json:"kind"`
	Batch int64        `json:"batch"`
}

func (h *handler) bulk(w http.ResponseWriter, r *http.Request) {
	op := r.PathValue("op")
	if isAPI(r) {
		var s selection
		if err := decode(r, &s); err != nil {
			h.fail(w, r, err)
			return
		}
		if len(s.IDs) > maxIDs {
			h.fail(w, r, fmt.Errorf("%w: more than %d ids", driver.ErrInvalid, maxIDs))
			return
		}
		n, err := h.apply(r.Context(), op, kiln.Filter{IDs: s.IDs, State: s.State, Queue: s.Queue, Kind: s.Kind, BatchID: s.Batch})
		if err != nil {
			h.fail(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]int{"count": n})
		return
	}
	if err := r.ParseForm(); err != nil {
		h.fail(w, r, fmt.Errorf("%w: %v", driver.ErrInvalid, err))
		return
	}
	l := &jobList{State: driver.State(r.PostForm.Get("state")), Queue: r.PostForm.Get("queue"), Kind: r.PostForm.Get("kind"),
		Tag: r.PostForm.Get("tag")}
	batch, err := optID(r.PostForm.Get("batch"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	l.Batch = batch
	var f kiln.Filter
	if r.PostForm.Get("all") == "1" {
		if !l.State.Valid() {
			h.fail(w, r, fmt.Errorf("%w: state %q", driver.ErrInvalid, l.State))
			return
		}
		if l.Tag != "" {
			h.fail(w, r, fmt.Errorf("%w: a tag filter cannot select every matching job", driver.ErrInvalid))
			return
		}
		f = kiln.Filter{State: l.State, Queue: l.Queue, Kind: l.Kind, BatchID: l.Batch}
	} else {
		ids := r.PostForm["id"]
		if len(ids) > maxIDs {
			h.fail(w, r, fmt.Errorf("%w: more than %d ids", driver.ErrInvalid, maxIDs))
			return
		}
		for _, s := range ids {
			id, err := strconv.ParseInt(s, 10, 64)
			if err != nil || id <= 0 {
				h.fail(w, r, fmt.Errorf("%w: id %q", driver.ErrInvalid, s))
				return
			}
			f.IDs = append(f.IDs, id)
		}
	}
	n := 0
	if len(f.IDs) > 0 || f.State != "" {
		if n, err = h.apply(r.Context(), op, f); err != nil {
			h.fail(w, r, err)
			return
		}
	}
	q := done(past[op], int64(n))
	target := "/retries"
	if r.PostForm.Get("back") != "retries" {
		if !l.State.Valid() {
			l.State = driver.Enqueued
		}
		target = "/jobs/" + string(l.State)
		maps.Copy(q, l.filter())
	}
	h.redirect(w, r, target, q)
}

func (h *handler) jobAction(w http.ResponseWriter, r *http.Request) {
	id, err := optID(r.PathValue("id"))
	if err != nil || id == 0 {
		h.fail(w, r, fmt.Errorf("%w: job %q", driver.ErrNotFound, r.PathValue("id")))
		return
	}
	op := r.PathValue("op")
	n, err := h.apply(r.Context(), op, kiln.Filter{IDs: []int64{id}})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if isAPI(r) {
		writeJSON(w, http.StatusOK, map[string]int{"count": n})
		return
	}
	h.redirect(w, r, "/jobs/"+strconv.FormatInt(id, 10), done(past[op], int64(n)))
}

func (h *handler) apply(ctx context.Context, op string, f kiln.Filter) (int, error) {
	switch op {
	case "requeue":
		return h.c.RequeueWhere(ctx, f)
	case "delete":
		return h.c.DeleteWhere(ctx, f)
	}
	return 0, fmt.Errorf("%w: action %q", driver.ErrNotFound, op)
}

func optID(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: id %q", driver.ErrInvalid, s)
	}
	return n, nil
}

func limitParam(s string) (int, error) {
	if s == "" {
		return pageSize, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 500 {
		return 0, fmt.Errorf("%w: limit %q", driver.ErrInvalid, s)
	}
	return n, nil
}

func withQuery(u string, q url.Values) string {
	if len(q) == 0 {
		return u
	}
	return u + "?" + q.Encode()
}
