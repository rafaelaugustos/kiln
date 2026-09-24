package memstore

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const countCap = 100000

func (s *Store) Job(_ context.Context, id int64) (driver.Record, error) {
	s.begin()
	defer s.end()
	j := s.jobs[id]
	if j == nil {
		return driver.Record{}, fmt.Errorf("%w: job %d", driver.ErrNotFound, id)
	}
	return j.record(true), nil
}

type pos struct{ k, id int64 }

func (a pos) compare(b pos) int {
	return cmp.Or(cmp.Compare(a.k, b.k), cmp.Compare(a.id, b.id))
}

func (a pos) String() string {
	return strconv.FormatInt(a.k, 36) + "." + strconv.FormatInt(a.id, 36)
}

func parsePos(s string) (pos, error) {
	a, b, ok := strings.Cut(s, ".")
	k, err1 := strconv.ParseInt(a, 36, 64)
	id, err2 := strconv.ParseInt(b, 36, 64)
	if !ok || err1 != nil || err2 != nil {
		return pos{}, fmt.Errorf("%w: cursor %q", driver.ErrInvalid, s)
	}
	return pos{k, id}, nil
}

func order(st driver.State) func(*job) pos {
	switch st {
	case driver.Scheduled:
		return func(j *job) pos { return pos{j.runAt.UnixNano(), j.id} }
	case driver.Enqueued, driver.Throttled:
		return func(j *job) pos { return pos{-int64(j.priority), j.id} }
	case driver.Succeeded, driver.Deleted, driver.Failed:
		return func(j *job) pos { return pos{-j.finalizedAt.UnixNano(), -j.id} }
	}
	return func(j *job) pos { return pos{0, -j.id} }
}

func (s *Store) Jobs(_ context.Context, q driver.JobQuery) (driver.Page, error) {
	if !q.State.Valid() {
		return driver.Page{}, fmt.Errorf("%w: state %q", driver.ErrInvalid, q.State)
	}
	limit := min(limitOr(q.Limit, 20), 500)
	var after *pos
	if q.Cursor != "" {
		p, err := parsePos(q.Cursor)
		if err != nil {
			return driver.Page{}, err
		}
		after = &p
	}
	key := order(q.State)
	s.begin()
	defer s.end()
	var rows []*job
	for _, j := range s.states[ord(q.State)] {
		if q.Queue != "" && j.queue != q.Queue || q.Kind != "" && j.kind != q.Kind || q.BatchID != 0 && j.batch != q.BatchID {
			continue
		}
		if after != nil && key(j).compare(*after) <= 0 {
			continue
		}
		rows = append(rows, j)
	}
	slices.SortFunc(rows, func(a, b *job) int { return key(a).compare(key(b)) })
	var p driver.Page
	if len(rows) > limit {
		rows = rows[:limit]
		p.Next = key(rows[limit-1]).String()
	}
	p.Records = make([]driver.Record, len(rows))
	for i, j := range rows {
		p.Records[i] = j.record(false)
	}
	return p, nil
}

func (s *Store) Counts(context.Context) (driver.Counts, error) {
	s.begin()
	defer s.end()
	var c driver.Counts
	capped := func(n int) int64 {
		if n >= countCap {
			c.Capped = true
			return countCap
		}
		return int64(n)
	}
	count := func(st driver.State) int64 { return capped(len(s.states[ord(st)])) }
	c.Awaiting = count(driver.Awaiting)
	c.Scheduled = count(driver.Scheduled)
	c.Throttled = count(driver.Throttled)
	c.Enqueued = count(driver.Enqueued)
	c.Processing = count(driver.Processing)
	c.Failed = count(driver.Failed)
	retries := 0
	for _, j := range s.states[ord(driver.Scheduled)] {
		if j.attempt > 0 {
			retries++
		}
	}
	c.Retries = capped(retries)
	c.Succeeded = s.total.succeeded
	c.Deleted = s.total.deleted
	return c, nil
}

func (s *Store) Series(_ context.Context, from, to time.Time, step time.Duration) ([]driver.Point, error) {
	if step < time.Minute || step%time.Minute != 0 {
		return nil, fmt.Errorf("%w: step %s", driver.ErrInvalid, step)
	}
	sec := int64(step / time.Second)
	lo, hi := floor(from.Unix(), sec), to.Unix()
	s.begin()
	defer s.end()
	sums := make(map[int64]*counters)
	for k, c := range s.stats {
		if k.at < lo || k.at > hi {
			continue
		}
		at := floor(k.at, sec)
		sum := sums[at]
		if sum == nil {
			sum = new(counters)
			sums[at] = sum
		}
		sum.merge(c)
	}
	out := make([]driver.Point, 0, len(sums))
	for _, at := range slices.Sorted(maps.Keys(sums)) {
		c := sums[at]
		out = append(out, driver.Point{
			At:        time.Unix(at, 0).UTC(),
			Succeeded: c.succeeded,
			Failed:    c.failed,
			Deleted:   c.deleted,
			Retried:   c.retried,
		})
	}
	return out, nil
}

func floor(v, step int64) int64 {
	q := v / step
	if v%step < 0 {
		q--
	}
	return q * step
}

func (s *Store) Queues(context.Context) ([]driver.QueueInfo, error) {
	s.begin()
	defer s.end()
	names := make(map[string]struct{})
	for name, q := range s.queues {
		if q.row || q.live() > 0 {
			names[name] = struct{}{}
		}
	}
	for _, info := range s.servers {
		for _, name := range info.Queues {
			names[name] = struct{}{}
		}
	}
	out := make([]driver.QueueInfo, 0, len(names))
	for _, name := range slices.Sorted(maps.Keys(names)) {
		qi := driver.QueueInfo{Name: name}
		if q := s.queues[name]; q != nil {
			qi.Paused = q.paused
			qi.Enqueued = q.counts[ord(driver.Enqueued)]
			qi.Processing = q.counts[ord(driver.Processing)]
			qi.Scheduled = q.counts[ord(driver.Scheduled)]
			qi.Throttled = q.counts[ord(driver.Throttled)]
			qi.Latency = q.latency(s.now)
		}
		out = append(out, qi)
	}
	return out, nil
}
