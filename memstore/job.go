package memstore

import (
	"cmp"
	"container/heap"
	"maps"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	historyCap = 16
	maxError   = 2 << 10
	maxTrace   = 8 << 10
)

type job struct {
	id          int64
	state       driver.State
	queue       string
	kind        string
	args        []byte
	meta        map[string]string
	tags        []string
	priority    int16
	attempt     int
	maxAttempts int
	claim       int32
	timeout     time.Duration
	runAt       time.Time
	createdAt   time.Time
	attemptedAt time.Time
	finalizedAt time.Time
	server      string
	cancel      bool
	batch       int64
	afterBatch  int64
	recurring   string
	unique      string
	uniqueFor   time.Duration
	limit       string
	limitMax    int
	deps        []dep
	pending     int
	children    []int64
	history     []driver.Entry
	output      []byte

	heap *jobHeap
	slot int
}

type dep struct {
	parent   int64
	on       driver.Mask
	batch    bool
	resolved bool
}

type change struct {
	j  *job
	st driver.State
}

func ord(s driver.State) int {
	switch s {
	case driver.Awaiting:
		return 0
	case driver.Scheduled:
		return 1
	case driver.Throttled:
		return 2
	case driver.Enqueued:
		return 3
	case driver.Processing:
		return 4
	case driver.Succeeded:
		return 5
	case driver.Failed:
		return 6
	}
	return 7
}

func busy(s driver.State) bool {
	return s == driver.Enqueued || s == driver.Processing
}

func final(s driver.State) bool {
	return s == driver.Succeeded || s == driver.Failed || s == driver.Deleted
}

func (j *job) dep(parent int64, batch bool) *dep {
	for i := range j.deps {
		if d := &j.deps[i]; d.parent == parent && d.batch == batch && !d.resolved {
			return d
		}
	}
	return nil
}

func (j *job) parents() []int64 {
	var ids []int64
	for _, d := range j.deps {
		if !d.batch {
			ids = append(ids, d.parent)
		}
	}
	return ids
}

func (j *job) view() driver.Job {
	return driver.Job{
		ID: j.id, Claim: j.claim,
		Kind:        j.kind,
		Queue:       j.queue,
		Args:        slices.Clone(j.args),
		Meta:        maps.Clone(j.meta),
		Tags:        slices.Clone(j.tags),
		Priority:    j.priority,
		Attempt:     j.attempt,
		MaxAttempts: j.maxAttempts,
		Timeout:     j.timeout,
		RunAt:       j.runAt,
		CreatedAt:   j.createdAt,
		AttemptedAt: j.attemptedAt,
		BatchID:     j.batch,
		RecurringID: j.recurring,
		Parents:     j.parents(),
		LimitKey:    j.limit,
	}
}

func (j *job) record(full bool) driver.Record {
	r := driver.Record{
		Job:             j.view(),
		State:           j.state,
		FinalizedAt:     j.finalizedAt,
		Server:          j.server,
		CancelRequested: j.cancel,
		AfterBatch:      j.afterBatch,
		PendingDeps:     j.pending,
	}
	if full {
		r.History = slices.Clone(j.history)
		r.Output = slices.Clone(j.output)
		r.Children = slices.Sorted(slices.Values(j.children))
	}
	return r
}

func byID(m map[int64]*job) []*job {
	js := slices.Collect(maps.Values(m))
	slices.SortFunc(js, func(a, b *job) int { return cmp.Compare(a.id, b.id) })
	return js
}

func (s *Store) index(j *job) {
	o := ord(j.state)
	s.states[o][j.id] = j
	q := s.queue(j.queue)
	q.counts[o]++
	if j.batch != 0 {
		b := s.batches[j.batch]
		b.counts[o]++
		if !j.state.Archived() {
			b.live++
		}
	}
	switch j.state {
	case driver.Enqueued:
		heap.Push(q.heapFor(j.kind), j)
		if !slices.Contains(s.ready, j.queue) {
			s.ready = append(s.ready, j.queue)
		}
	case driver.Scheduled:
		heap.Push(&s.due, j)
	case driver.Throttled:
		heap.Push(&s.limits[j.limit].waiting, j)
	case driver.Processing:
		m := s.running[j.server]
		if m == nil {
			m = make(map[int64]*job)
			s.running[j.server] = m
		}
		m[j.id] = j
	}
}

func (s *Store) unindex(j *job) {
	o := ord(j.state)
	delete(s.states[o], j.id)
	s.queues[j.queue].counts[o]--
	if j.batch != 0 {
		b := s.batches[j.batch]
		b.counts[o]--
		if !j.state.Archived() {
			b.live--
		}
	}
	if j.heap != nil {
		heap.Remove(j.heap, j.slot)
	}
	if j.state == driver.Processing {
		m := s.running[j.server]
		delete(m, j.id)
		if len(m) == 0 {
			delete(s.running, j.server)
		}
	}
}

func (s *Store) move(j *job, to driver.State) {
	from := j.state
	s.unindex(j)
	j.state = to
	if j.limit != "" && busy(from) != busy(to) {
		l := s.limits[j.limit]
		if busy(to) {
			l.active++
		} else {
			l.active--
			s.admitting(j.limit)
		}
	}
	if final(to) {
		j.finalizedAt = s.now
		s.release(j)
		s.bump(to)
		s.resolved = append(s.resolved, change{j, to})
	} else {
		j.finalizedAt = time.Time{}
	}
	s.index(j)
	if j.batch != 0 {
		s.complete(s.batches[j.batch])
	}
}

func (s *Store) enqueue(j *job) {
	if j.limit == "" {
		s.move(j, driver.Enqueued)
		return
	}
	s.move(j, driver.Throttled)
	s.admitting(j.limit)
}

func (s *Store) wake(j *job) {
	if j.runAt.After(s.now) {
		s.move(j, driver.Scheduled)
		return
	}
	s.enqueue(j)
}

func (s *Store) log(j *job, e driver.Entry) {
	e.At, e.Attempt = s.now, j.attempt
	e.Error, e.Trace = clip(e.Error, maxError), clip(e.Trace, maxTrace)
	if len(j.history) == historyCap {
		copy(j.history, j.history[1:])
		j.history = j.history[:historyCap-1]
	}
	j.history = append(j.history, e)
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
