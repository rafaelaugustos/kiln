package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const recordColumns = `j.id, j.claim, j.kind, j.queue, j.args, j.meta, j.tags, j.priority, j.attempt, j.max_attempts,
	j.timeout_ms, j.run_at, j.created_at, j.attempted_at, COALESCE(j.batch_id, 0), COALESCE(j.recurring_id, ''),
	j.parents, COALESCE(j.limit_key, ''), j.state, j.finalized_at, COALESCE(j.server, ''), COALESCE(j.after_batch, 0)`

const liveRecord = recordColumns + `, j.cancel_requested, j.deps_pending`

const archivedRecord = recordColumns + `, 0, 0`

const childrenOf = `(SELECT json_group_array(d.job_id) FROM {p}deps d WHERE d.batch = 0 AND d.parent_id = j.id)`

const sqlJob = `SELECT ` + liveRecord + `, j.history, NULL, ` + childrenOf + ` FROM {p}jobs j WHERE j.id = ?
UNION ALL
SELECT ` + archivedRecord + `, j.history, j.output, ` + childrenOf + ` FROM {p}archive j WHERE j.id = ?`

const capped = 100000

const sqlCounts = `SELECT
	(SELECT count(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'awaiting' LIMIT 100000)),
	(SELECT count(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'scheduled' LIMIT 100000)),
	(SELECT count(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'throttled' LIMIT 100000)),
	(SELECT count(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'enqueued' LIMIT 100000)),
	(SELECT count(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'processing' LIMIT 100000)),
	(SELECT count(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'failed' LIMIT 100000)),
	(SELECT count(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'scheduled' AND attempt > 0 LIMIT 100000)),
	(SELECT COALESCE(sum(succeeded), 0) FROM {p}stats),
	(SELECT COALESCE(sum(deleted), 0) FROM {p}stats)`

const sqlSeries = `SELECT bucket, sum(succeeded), sum(failed), sum(deleted), sum(retried) FROM {p}stats
WHERE bucket > 0 AND bucket >= ? AND bucket <= ?
GROUP BY bucket
ORDER BY bucket`

const sqlServers = `SELECT id, COALESCE(host, ''), COALESCE(pid, 0), COALESCE(version, ''), queues, kinds,
	COALESCE(workers, 0), COALESCE(running, 0), started_at, heartbeat_at
FROM {p}servers ORDER BY id`

const sqlQueues = `SELECT n.name, COALESCE(p.paused, 0), COALESCE(l.enqueued, 0), COALESCE(l.processing, 0),
	COALESCE(l.scheduled, 0), COALESCE(l.throttled, 0), max(COALESCE({now} - l.oldest, 0), 0)
FROM (
	SELECT queue AS name FROM {p}jobs
	UNION SELECT name FROM {p}queues
	UNION SELECT q.value FROM {p}servers s, json_each(s.queues) q
) n
LEFT JOIN (
	SELECT queue, sum(state = 'enqueued') AS enqueued, sum(state = 'processing') AS processing,
		sum(state = 'scheduled') AS scheduled, sum(state = 'throttled') AS throttled,
		min(CASE WHEN state = 'enqueued' THEN run_at END) AS oldest
	FROM {p}jobs GROUP BY queue
) l ON l.queue = n.name
LEFT JOIN {p}queues p ON p.name = n.name
ORDER BY n.name`

const batchColumns = `b.id, b.description, b.meta, b.total, b.sealed, b.created_at, b.finished_at,
	(SELECT json_group_object(c.state, c.n) FROM (
		SELECT state, count(*) AS n FROM {p}jobs WHERE batch_id = b.id GROUP BY state
		UNION ALL
		SELECT state, count(*) FROM {p}archive WHERE batch_id = b.id GROUP BY state) c)`

const sqlBatch = `SELECT ` + batchColumns + ` FROM {p}batches b WHERE b.id = ?`

const sqlBatches = `SELECT ` + batchColumns + ` FROM {p}batches b WHERE b.id < ? ORDER BY b.id DESC LIMIT ?`

func (s *Store) Job(ctx context.Context, id int64) (driver.Record, error) {
	rows, err := s.db.QueryContext(ctx, s.q.job, id, id)
	recs, err := scanRecords(rows, err, true)
	if err != nil {
		return driver.Record{}, fmt.Errorf("kiln: job: %w", err)
	}
	if len(recs) == 0 {
		return driver.Record{}, fmt.Errorf("%w: job %d", driver.ErrNotFound, id)
	}
	return recs[0], nil
}

func scanRecords(rows *sql.Rows, err error, full bool) ([]driver.Record, error) {
	var (
		out                                 []driver.Record
		meta, tags, parents, hist, children sql.RawBytes
		ms                                  int64
		run, created, attempted, finished   stamp
		state                               string
		pending                             int
	)
	err = each(rows, err, func() error {
		var r driver.Record
		dst := []any{&r.ID, &r.Claim, &r.Kind, &r.Queue, &r.Args, &meta, &tags, &r.Priority, &r.Attempt,
			&r.MaxAttempts, &ms, &run, &created, &attempted, &r.BatchID, &r.RecurringID, &parents, &r.LimitKey,
			&state, &finished, &r.Server, &r.AfterBatch, &r.CancelRequested, &pending}
		if full {
			dst = append(dst, &hist, &r.Output, &children)
		}
		if err := rows.Scan(dst...); err != nil {
			return err
		}
		r.Meta = decodeMeta(meta)
		r.Tags = decodeStrings(tags)
		r.Parents = decodeIDs(parents)
		r.Timeout = timeout(ms)
		r.RunAt, r.CreatedAt, r.AttemptedAt, r.FinalizedAt = run.Time, created.Time, attempted.Time, finished.Time
		r.State = driver.State(state)
		r.PendingDeps = pending
		if full {
			if len(hist) > 0 {
				r.History = decodeHistory(hist)
			}
			if len(r.Output) == 0 {
				r.Output = nil
			}
			r.Children = decodeIDs(children)
			slices.Sort(r.Children)
		}
		out = append(out, r)
		return nil
	})
	return out, err
}

func (s *Store) Jobs(ctx context.Context, q driver.JobQuery) (driver.Page, error) {
	if !q.State.Valid() {
		return driver.Page{}, fmt.Errorf("%w: state %q", driver.ErrInvalid, q.State)
	}
	limit := q.Limit
	switch {
	case limit <= 0:
		limit = 20
	case limit > 500:
		limit = 500
	}
	var c1, c2 int64
	if q.Cursor != "" {
		var err error
		if c1, c2, err = decodeCursor(q.Cursor); err != nil {
			return driver.Page{}, err
		}
	}
	table, cols := "jobs", liveRecord
	if q.State.Archived() {
		table, cols = "archive", archivedRecord
	}
	var b strings.Builder
	b.WriteString("SELECT " + cols + " FROM " + s.prefix + table + " j WHERE j.state = ?")
	args := []any{string(q.State)}
	and := func(cond string, vs ...any) {
		b.WriteString(" AND " + cond)
		args = append(args, vs...)
	}
	if q.Queue != "" {
		and("j.queue = ?", q.Queue)
	}
	if q.Kind != "" {
		and("j.kind = ?", q.Kind)
	}
	if q.BatchID != 0 {
		and("j.batch_id = ?", q.BatchID)
	}
	var order string
	switch q.State {
	case driver.Scheduled:
		order = "j.run_at, j.id"
		if q.Cursor != "" {
			and("(j.run_at > ? OR j.run_at = ? AND j.id > ?)", c1, c1, c2)
		}
	case driver.Enqueued, driver.Throttled:
		order = "j.priority DESC, j.id"
		if q.Cursor != "" {
			and("(j.priority < ? OR j.priority = ? AND j.id > ?)", c1, c1, c2)
		}
	case driver.Succeeded, driver.Deleted, driver.Failed:
		order = "j.finalized_at DESC, j.id DESC"
		if q.Cursor != "" {
			and("(j.finalized_at < ? OR j.finalized_at = ? AND j.id < ?)", c1, c1, c2)
		}
	default:
		order = "j.id DESC"
		if q.Cursor != "" {
			and("j.id < ?", c2)
		}
	}
	b.WriteString(" ORDER BY " + order + " LIMIT ?")
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, b.String(), args...)
	recs, err := scanRecords(rows, err, false)
	if err != nil {
		return driver.Page{}, fmt.Errorf("kiln: jobs: %w", err)
	}
	var p driver.Page
	if len(recs) > limit {
		recs = recs[:limit]
		last := recs[limit-1]
		switch q.State {
		case driver.Scheduled:
			p.Next = encodeCursor(last.RunAt.UnixMicro(), last.ID)
		case driver.Enqueued, driver.Throttled:
			p.Next = encodeCursor(int64(last.Priority), last.ID)
		case driver.Succeeded, driver.Deleted, driver.Failed:
			p.Next = encodeCursor(last.FinalizedAt.UnixMicro(), last.ID)
		default:
			p.Next = encodeCursor(0, last.ID)
		}
	}
	p.Records = recs
	return p, nil
}

func encodeCursor(a, b int64) string {
	buf := strconv.AppendInt(nil, a, 10)
	buf = append(buf, '.')
	buf = strconv.AppendInt(buf, b, 10)
	return base64.RawURLEncoding.EncodeToString(buf)
}

func decodeCursor(c string) (int64, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err == nil {
		sa, sb, ok := strings.Cut(string(raw), ".")
		if ok {
			a, errA := strconv.ParseInt(sa, 10, 64)
			b, errB := strconv.ParseInt(sb, 10, 64)
			if errA == nil && errB == nil {
				return a, b, nil
			}
		}
	}
	return 0, 0, fmt.Errorf("%w: cursor %q", driver.ErrInvalid, c)
}

func (s *Store) Counts(ctx context.Context) (driver.Counts, error) {
	var c driver.Counts
	err := s.db.QueryRowContext(ctx, s.q.counts).Scan(&c.Awaiting, &c.Scheduled, &c.Throttled, &c.Enqueued,
		&c.Processing, &c.Failed, &c.Retries, &c.Succeeded, &c.Deleted)
	if err != nil {
		return driver.Counts{}, fmt.Errorf("kiln: counts: %w", err)
	}
	for _, n := range []int64{c.Awaiting, c.Scheduled, c.Throttled, c.Enqueued, c.Processing, c.Failed, c.Retries} {
		if n >= capped {
			c.Capped = true
		}
	}
	return c, nil
}

func (s *Store) Series(ctx context.Context, from, to time.Time, step time.Duration) ([]driver.Point, error) {
	if step <= 0 || step%time.Minute != 0 {
		return nil, fmt.Errorf("%w: series step %s", driver.ErrInvalid, step)
	}
	secs := int64(step / time.Second)
	align := func(t time.Time) time.Time {
		u := t.Unix()
		u -= ((u % secs) + secs) % secs
		return time.Unix(u, 0).UTC()
	}
	var out []driver.Point
	rows, err := s.db.QueryContext(ctx, s.q.series, align(from).UnixMicro(), to.UnixMicro())
	err = each(rows, err, func() error {
		var (
			at stamp
			p  driver.Point
		)
		if err := rows.Scan(&at, &p.Succeeded, &p.Failed, &p.Deleted, &p.Retried); err != nil {
			return err
		}
		p.At = align(at.Time)
		if n := len(out); n > 0 && out[n-1].At.Equal(p.At) {
			last := &out[n-1]
			last.Succeeded += p.Succeeded
			last.Failed += p.Failed
			last.Deleted += p.Deleted
			last.Retried += p.Retried
			return nil
		}
		out = append(out, p)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kiln: series: %w", err)
	}
	return out, nil
}

func (s *Store) Servers(ctx context.Context) ([]driver.ServerInfo, error) {
	var out []driver.ServerInfo
	rows, err := s.db.QueryContext(ctx, s.q.servers)
	err = each(rows, err, func() error {
		var (
			si              driver.ServerInfo
			queues, kinds   sql.RawBytes
			started, beaten stamp
		)
		err := rows.Scan(&si.ID, &si.Host, &si.PID, &si.Version, &queues, &kinds, &si.Workers, &si.Running,
			&started, &beaten)
		if err != nil {
			return err
		}
		si.Queues, si.Kinds = decodeStrings(queues), decodeStrings(kinds)
		si.StartedAt, si.HeartbeatAt = started.Time, beaten.Time
		out = append(out, si)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kiln: servers: %w", err)
	}
	return out, nil
}

func (s *Store) Queues(ctx context.Context) ([]driver.QueueInfo, error) {
	var out []driver.QueueInfo
	rows, err := s.db.QueryContext(ctx, s.q.queues)
	err = each(rows, err, func() error {
		var (
			qi  driver.QueueInfo
			lat int64
		)
		if err := rows.Scan(&qi.Name, &qi.Paused, &qi.Enqueued, &qi.Processing, &qi.Scheduled, &qi.Throttled, &lat); err != nil {
			return err
		}
		qi.Latency = time.Duration(lat) * time.Microsecond
		out = append(out, qi)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kiln: queues: %w", err)
	}
	return out, nil
}

func (s *Store) Batch(ctx context.Context, id int64) (driver.Batch, error) {
	rows, err := s.db.QueryContext(ctx, s.q.batch, id)
	bs, err := scanBatches(rows, err)
	if err != nil {
		return driver.Batch{}, fmt.Errorf("kiln: batch: %w", err)
	}
	if len(bs) == 0 {
		return driver.Batch{}, fmt.Errorf("%w: batch %d", driver.ErrNotFound, id)
	}
	return bs[0], nil
}

func (s *Store) Batches(ctx context.Context, q driver.BatchQuery) (driver.BatchPage, error) {
	limit := q.Limit
	switch {
	case limit <= 0:
		limit = 20
	case limit > 500:
		limit = 500
	}
	before := int64(1<<63 - 1)
	if q.Cursor != "" {
		_, id, err := decodeCursor(q.Cursor)
		if err != nil {
			return driver.BatchPage{}, err
		}
		before = id
	}
	rows, err := s.db.QueryContext(ctx, s.q.batches, before, limit+1)
	bs, err := scanBatches(rows, err)
	if err != nil {
		return driver.BatchPage{}, fmt.Errorf("kiln: batches: %w", err)
	}
	var p driver.BatchPage
	if len(bs) > limit {
		bs = bs[:limit]
		p.Next = encodeCursor(0, bs[limit-1].ID)
	}
	p.Batches = bs
	return p, nil
}

func scanBatches(rows *sql.Rows, err error) ([]driver.Batch, error) {
	var out []driver.Batch
	err = each(rows, err, func() error {
		var (
			b                 driver.Batch
			meta, counts      sql.RawBytes
			created, finished stamp
		)
		if err := rows.Scan(&b.ID, &b.Description, &meta, &b.Total, &b.Sealed, &created, &finished, &counts); err != nil {
			return err
		}
		b.Meta = decodeMeta(meta)
		b.CreatedAt, b.FinishedAt = created.Time, finished.Time
		b.Counts = make(map[driver.State]int64)
		if len(counts) > 0 {
			var m map[string]int64
			if err := json.Unmarshal(counts, &m); err != nil {
				return errors.Join(driver.ErrInvalid, err)
			}
			for k, v := range m {
				b.Counts[driver.State(k)] += v
			}
		}
		out = append(out, b)
		return nil
	})
	return out, err
}
