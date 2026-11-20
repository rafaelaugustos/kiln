package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const countCap = 100000

// Job returns the job id, live or archived, with its history, output, children and pending
// dependencies, or an error wrapping [driver.ErrNotFound].
func (s *Store) Job(ctx context.Context, id int64) (driver.Record, error) {
	var res wireJob
	if err := s.call(ctx, "job", ref{ID: id}, &res); err != nil {
		return driver.Record{}, err
	}
	return res.record(), nil
}

// Jobs returns a page of the jobs in q.State that match the rest of q, in the order
// [driver.Inspector.Jobs] specifies. Its records leave out History, Output and Children. With a
// queue, kind, batch or tag set, the store looks at up to 5000 jobs for one page, so a page can
// hold fewer than q.Limit records, or none, while Next is set.
func (s *Store) Jobs(ctx context.Context, q driver.JobQuery) (driver.Page, error) {
	if !q.State.Valid() {
		return driver.Page{}, fmt.Errorf("%w: state %q", driver.ErrInvalid, q.State)
	}
	args := struct {
		St    string `json:"st"`
		Q     string `json:"q,omitempty"`
		K     string `json:"k,omitempty"`
		B     int64  `json:"b,omitempty"`
		Tag   string `json:"tag,omitempty"`
		Limit int    `json:"limit"`
		Cur   string `json:"cur,omitempty"`
	}{string(q.State), q.Queue, q.Kind, q.BatchID, q.Tag, min(limitOr(q.Limit, 20), 500), q.Cursor}
	var res struct {
		Recs []wireJob `json:"recs"`
		Next string    `json:"next"`
	}
	if err := s.call(ctx, "jobs", args, &res); err != nil {
		return driver.Page{}, err
	}
	p := driver.Page{Records: make([]driver.Record, len(res.Recs)), Next: res.Next}
	for i := range res.Recs {
		p.Records[i] = res.Recs[i].record()
	}
	return p, nil
}

// Counts returns the number of jobs in each live state and of scheduled jobs that are retries,
// each counted up to 100000, and the all-time totals of succeeded and deleted jobs.
func (s *Store) Counts(ctx context.Context) (driver.Counts, error) {
	var res struct {
		C       map[string]int64 `json:"c"`
		Retries int64            `json:"retries"`
		Succ    int64            `json:"succ,string"`
		Del     int64            `json:"del,string"`
	}
	if err := s.call(ctx, "counts", empty{}, &res); err != nil {
		return driver.Counts{}, err
	}
	var c driver.Counts
	capped := func(n int64) int64 {
		if n >= countCap {
			c.Capped = true
			return countCap
		}
		return max(n, 0)
	}
	c.Awaiting = capped(res.C[string(driver.Awaiting)])
	c.Scheduled = capped(res.C[string(driver.Scheduled)])
	c.Throttled = capped(res.C[string(driver.Throttled)])
	c.Enqueued = capped(res.C[string(driver.Enqueued)])
	c.Processing = capped(res.C[string(driver.Processing)])
	c.Failed = capped(res.C[string(driver.Failed)])
	c.Retries = capped(res.Retries)
	c.Succeeded = res.Succ
	c.Deleted = res.Del
	return c, nil
}

// Series sums the outcomes counted between from and to into buckets of step, which must be a
// positive multiple of a minute, as [driver.Inspector.Series] describes.
func (s *Store) Series(ctx context.Context, from, to time.Time, step time.Duration) ([]driver.Point, error) {
	if step < time.Minute || step%time.Minute != 0 {
		return nil, fmt.Errorf("%w: step %s", driver.ErrInvalid, step)
	}
	sec := int64(step / time.Second)
	args := struct {
		Lo int64 `json:"lo"`
		To int64 `json:"to"`
	}{floor(from.Unix(), sec), to.Add(time.Microsecond - 1).UnixMicro()}
	var res []struct {
		At int64 `json:"at,string"`
		S  int64 `json:"s,string"`
		F  int64 `json:"f,string"`
		D  int64 `json:"d,string"`
		R  int64 `json:"r,string"`
	}
	if err := s.call(ctx, "series", args, &res); err != nil {
		return nil, err
	}
	sums := make(map[int64]*driver.Point)
	for _, r := range res {
		at := floor(r.At, sec)
		p := sums[at]
		if p == nil {
			p = &driver.Point{At: time.Unix(at, 0).UTC()}
			sums[at] = p
		}
		p.Succeeded += r.S
		p.Failed += r.F
		p.Deleted += r.D
		p.Retried += r.R
	}
	out := make([]driver.Point, 0, len(sums))
	for _, at := range slices.Sorted(maps.Keys(sums)) {
		out = append(out, *sums[at])
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

// Servers returns the registered servers, ordered by id.
func (s *Store) Servers(ctx context.Context) ([]driver.ServerInfo, error) {
	var res []struct {
		ID      string `json:"id"`
		Info    string `json:"info"`
		Started int64  `json:"started,string"`
		HB      int64  `json:"hb,string"`
	}
	if err := s.call(ctx, "servers", empty{}, &res); err != nil {
		return nil, err
	}
	out := make([]driver.ServerInfo, len(res))
	for i, r := range res {
		var w wireInfo
		json.Unmarshal([]byte(r.Info), &w)
		out[i] = driver.ServerInfo{
			ID: r.ID, Host: w.Host, PID: w.PID, Version: w.Version, Queues: w.Queues, Kinds: w.Kinds,
			Workers: w.Workers, Running: w.Running, StartedAt: when(r.Started), HeartbeatAt: when(r.HB),
		}
	}
	slices.SortFunc(out, func(a, b driver.ServerInfo) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// Queues returns every queue that has live jobs, a pause setting or a server working on it,
// ordered by name.
func (s *Store) Queues(ctx context.Context) ([]driver.QueueInfo, error) {
	var res []struct {
		Name   string            `json:"name"`
		C      map[string]string `json:"c"`
		Paused bool              `json:"paused"`
		Row    bool              `json:"row"`
		Lat    int64             `json:"lat,string"`
	}
	if err := s.call(ctx, "queues", empty{}, &res); err != nil {
		return nil, err
	}
	servers, err := s.Servers(ctx)
	if err != nil {
		return nil, err
	}
	infos := make(map[string]driver.QueueInfo)
	for _, sv := range servers {
		for _, name := range sv.Queues {
			infos[name] = driver.QueueInfo{Name: name}
		}
	}
	for _, r := range res {
		count := func(st driver.State) int64 {
			n, _ := strconv.ParseInt(r.C[string(st)], 10, 64)
			return max(n, 0)
		}
		qi := driver.QueueInfo{
			Name: r.Name, Paused: r.Paused,
			Enqueued: count(driver.Enqueued), Processing: count(driver.Processing),
			Scheduled: count(driver.Scheduled), Throttled: count(driver.Throttled),
			Latency: time.Duration(r.Lat) * time.Microsecond,
		}
		live := qi.Enqueued + qi.Processing + qi.Scheduled + qi.Throttled + count(driver.Awaiting) + count(driver.Failed)
		if _, known := infos[r.Name]; known || r.Row || live > 0 {
			infos[r.Name] = qi
		}
	}
	out := make([]driver.QueueInfo, 0, len(infos))
	for _, name := range slices.Sorted(maps.Keys(infos)) {
		out = append(out, infos[name])
	}
	return out, nil
}

type wireBatch struct {
	ID      int64             `json:"id,string"`
	Desc    string            `json:"desc"`
	Meta    string            `json:"meta"`
	Tot     int64             `json:"tot,string"`
	Seal    bool              `json:"seal"`
	C       map[string]string `json:"c"`
	Cr      int64             `json:"cr,string"`
	Fin     int64             `json:"fin,string"`
	Par     int64             `json:"par,string"`
	Nest    int64             `json:"nest,string"`
	NestFin int64             `json:"nestfin,string"`
}

func (w *wireBatch) batch() driver.Batch {
	b := driver.Batch{
		ID: w.ID, Description: w.Desc, Total: w.Tot, Sealed: w.Seal, Counts: make(map[driver.State]int64),
		CreatedAt: when(w.Cr), FinishedAt: when(w.Fin), Parent: w.Par, Nested: w.Nest, NestedFinished: w.NestFin,
	}
	if w.Meta != "" {
		json.Unmarshal([]byte(w.Meta), &b.Meta)
	}
	for st, v := range w.C {
		if n, _ := strconv.ParseInt(v, 10, 64); n > 0 {
			b.Counts[driver.State(st)] = n
		}
	}
	return b
}

// Batch returns the batch id with its members counted by state, or an error wrapping
// [driver.ErrNotFound].
func (s *Store) Batch(ctx context.Context, id int64) (driver.Batch, error) {
	var res wireBatch
	if err := s.call(ctx, "batch", ref{ID: id}, &res); err != nil {
		return driver.Batch{}, err
	}
	return res.batch(), nil
}

// Batches returns a page of batches, newest first, or of the batches nested directly in q.Parent
// when it is set.
func (s *Store) Batches(ctx context.Context, q driver.BatchQuery) (driver.BatchPage, error) {
	args := struct {
		Parent int64 `json:"parent,omitempty"`
		Limit  int   `json:"limit"`
		After  int64 `json:"after,omitempty"`
	}{Parent: max(q.Parent, 0), Limit: min(limitOr(q.Limit, 20), 500)}
	if q.Cursor != "" {
		n, err := strconv.ParseInt(q.Cursor, 10, 64)
		if err != nil {
			return driver.BatchPage{}, fmt.Errorf("%w: cursor %q", driver.ErrInvalid, q.Cursor)
		}
		args.After = n
	}
	var res struct {
		List []wireBatch `json:"list"`
		Next string      `json:"next"`
	}
	if err := s.call(ctx, "batches", args, &res); err != nil {
		return driver.BatchPage{}, err
	}
	p := driver.BatchPage{Next: res.Next}
	for i := range res.List {
		p.Batches = append(p.Batches, res.List[i].batch())
	}
	return p, nil
}

// Limits returns the limit keys after the given key, in key order, at most limit of them, 100
// when limit is 0 or less, as [driver.LimitReader] describes.
func (s *Store) Limits(ctx context.Context, after string, limit int) ([]driver.LimitInfo, error) {
	args := struct {
		After string `json:"after,omitempty"`
		Limit int    `json:"limit"`
	}{after, limitOr(limit, 100)}
	var res []struct {
		Key    string `json:"key"`
		Max    int    `json:"max"`
		Rate   int    `json:"rate"`
		Per    int64  `json:"per,string"`
		Burst  int    `json:"burst"`
		Active int    `json:"active"`
		Thr    int    `json:"thr"`
		Rsv    int    `json:"rsv"`
		Next   int64  `json:"next,string"`
	}
	if err := s.call(ctx, "limits", args, &res); err != nil {
		return nil, err
	}
	out := make([]driver.LimitInfo, len(res))
	for i, r := range res {
		out[i] = driver.LimitInfo{
			Key: r.Key, Max: r.Max, Rate: r.Rate, Burst: r.Burst, Per: time.Duration(r.Per) * time.Microsecond,
			Active: r.Active, Throttled: r.Thr, Reserved: r.Rsv, NextStart: when(r.Next),
		}
	}
	return out, nil
}
