package mysqlstore

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

const archivedRecord = recordColumns + `, FALSE, 0`

const childrenOf = `(SELECT JSON_ARRAYAGG(d.job_id) FROM {p}deps d WHERE d.batch = FALSE AND d.parent_id = j.id)`

const sqlJob = `SELECT ` + liveRecord + `, j.history, NULL, ` + childrenOf + ` FROM {p}jobs j WHERE j.id = ?
UNION ALL
SELECT ` + archivedRecord + `, j.history, j.output, ` + childrenOf + ` FROM {p}archive j WHERE j.id = ?
LIMIT 1`

const capped = 100000

const sqlCounts = `SELECT
	(SELECT COUNT(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'awaiting' LIMIT 100000) x),
	(SELECT COUNT(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'scheduled' LIMIT 100000) x),
	(SELECT COUNT(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'throttled' LIMIT 100000) x),
	(SELECT COUNT(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'enqueued' LIMIT 100000) x),
	(SELECT COUNT(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'processing' LIMIT 100000) x),
	(SELECT COUNT(*) FROM (SELECT 1 FROM {p}jobs WHERE state = 'failed' LIMIT 100000) x),
	(SELECT COUNT(*) FROM (SELECT 1 FROM {p}jobs FORCE INDEX (jobs_due) WHERE state = 'scheduled' AND attempt > 0 LIMIT 100000) x),
	(SELECT COALESCE(SUM(succeeded), 0) FROM {p}stats),
	(SELECT COALESCE(SUM(deleted), 0) FROM {p}stats)`

const sqlSeries = `SELECT bucket, SUM(succeeded), SUM(failed), SUM(deleted), SUM(retried) FROM {p}stats
WHERE bucket > ? AND bucket >= ? AND bucket < ?
GROUP BY bucket
ORDER BY bucket`

const sqlServers = `SELECT id, COALESCE(host, ''), COALESCE(pid, 0), COALESCE(version, ''), queues, kinds,
	COALESCE(workers, 0), COALESCE(running, 0), started_at, heartbeat_at
FROM {p}servers ORDER BY id`

const sqlQueues = `SELECT n.name, COALESCE(p.paused, FALSE), COALESCE(l.enqueued, 0), COALESCE(l.processing, 0),
	COALESCE(l.scheduled, 0), COALESCE(l.throttled, 0),
	GREATEST(COALESCE(TIMESTAMPDIFF(MICROSECOND, l.oldest, UTC_TIMESTAMP(6)), 0), 0)
FROM (
	SELECT queue AS name FROM {p}jobs GROUP BY queue
	UNION SELECT name FROM {p}queues
	UNION SELECT q.name FROM {p}servers s,
		JSON_TABLE(s.queues, '$[*]' COLUMNS (name VARCHAR(255) COLLATE utf8mb4_0900_bin PATH '$')) q
) n
LEFT JOIN (
	SELECT queue, SUM(state = 'enqueued') AS enqueued, SUM(state = 'processing') AS processing,
		SUM(state = 'scheduled') AS scheduled, SUM(state = 'throttled') AS throttled,
		MIN(IF(state = 'enqueued', run_at, NULL)) AS oldest
	FROM {p}jobs GROUP BY queue
) l ON l.queue = n.name
LEFT JOIN {p}queues p ON p.name = n.name
ORDER BY n.name`

const batchColumns = `b.id, b.description, b.meta, b.total, b.sealed, b.created_at, b.finished_at,
	(SELECT JSON_OBJECTAGG(c.state, c.n) FROM (
		SELECT state, COUNT(*) AS n FROM {p}jobs WHERE batch_id = b.id GROUP BY state
		UNION ALL
		SELECT state, COUNT(*) FROM {p}archive WHERE batch_id = b.id GROUP BY state) c)`

const sqlBatch = `SELECT ` + batchColumns + ` FROM {p}batches b WHERE b.id = ?`

const sqlBatches = `SELECT ` + batchColumns + ` FROM {p}batches b WHERE b.id < ? ORDER BY b.id DESC LIMIT ?`

func (s *Store) Job(ctx context.Context, id int64) (driver.Record, error) {
	rows, err := s.db.QueryContext(ctx, render(s.q.job, id, id))
	if err != nil {
		return driver.Record{}, fmt.Errorf("kiln: job: %w", err)
	}
	recs, err := scanRecords(rows, true)
	if err != nil {
		return driver.Record{}, fmt.Errorf("kiln: job: %w", err)
	}
	if len(recs) == 0 {
		return driver.Record{}, fmt.Errorf("%w: job %d", driver.ErrNotFound, id)
	}
	return recs[0], nil
}

func scanRecords(rows *sql.Rows, full bool) ([]driver.Record, error) {
	defer rows.Close()
	var (
		out                                 []driver.Record
		meta, tags, parents, hist, children sql.RawBytes
		ms                                  int64
		run, created, attempted, finished   stamp
		state                               string
		pending                             int
	)
	for rows.Next() {
		var r driver.Record
		dst := []any{&r.ID, &r.Claim, &r.Kind, &r.Queue, &r.Args, &meta, &tags, &r.Priority, &r.Attempt,
			&r.MaxAttempts, &ms, &run, &created, &attempted, &r.BatchID, &r.RecurringID, &parents, &r.LimitKey,
			&state, &finished, &r.Server, &r.AfterBatch, &r.CancelRequested, &pending}
		if full {
			dst = append(dst, &hist, &r.Output, &children)
		}
		if err := rows.Scan(dst...); err != nil {
			return nil, err
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
	}
	return out, rows.Err()
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
	b := make([]byte, 0, 1024)
	b = append(b, "SELECT "...)
	b = append(b, cols...)
	b = append(b, " FROM "...)
	b = append(b, s.prefix...)
	b = append(b, table...)
	b = appendSQL(b, " j WHERE j.state = ?", q.State)
	if q.Queue != "" {
		b = appendSQL(b, " AND j.queue = ?", q.Queue)
	}
	if q.Kind != "" {
		b = appendSQL(b, " AND j.kind = ?", q.Kind)
	}
	if q.BatchID != 0 {
		b = appendSQL(b, " AND j.batch_id = ?", q.BatchID)
	}
	var order string
	switch q.State {
	case driver.Scheduled:
		order = "j.run_at, j.id"
		if q.Cursor != "" {
			at := time.UnixMicro(c1)
			b = appendSQL(b, " AND (j.run_at > ? OR j.run_at = ? AND j.id > ?)", at, at, c2)
		}
	case driver.Enqueued, driver.Throttled:
		order = "j.priority DESC, j.id"
		if q.Cursor != "" {
			b = appendSQL(b, " AND (j.priority < ? OR j.priority = ? AND j.id > ?)", c1, c1, c2)
		}
	case driver.Succeeded, driver.Deleted, driver.Failed:
		order = "j.finalized_at DESC, j.id DESC"
		if q.Cursor != "" {
			at := time.UnixMicro(c1)
			b = appendSQL(b, " AND (j.finalized_at < ? OR j.finalized_at = ? AND j.id < ?)", at, at, c2)
		}
	default:
		order = "j.id DESC"
		if q.Cursor != "" {
			b = appendSQL(b, " AND j.id < ?", c2)
		}
	}
	b = appendSQL(b, " ORDER BY "+order+" LIMIT ?", limit+1)
	rows, err := s.db.QueryContext(ctx, string(b))
	if err != nil {
		return driver.Page{}, fmt.Errorf("kiln: jobs: %w", err)
	}
	recs, err := scanRecords(rows, false)
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
	rows, err := s.db.QueryContext(ctx, render(s.q.series, totals, align(from), to))
	if err != nil {
		return nil, fmt.Errorf("kiln: series: %w", err)
	}
	defer rows.Close()
	var out []driver.Point
	for rows.Next() {
		var (
			at stamp
			p  driver.Point
		)
		if err := rows.Scan(&at, &p.Succeeded, &p.Failed, &p.Deleted, &p.Retried); err != nil {
			return nil, fmt.Errorf("kiln: series: %w", err)
		}
		p.At = align(at.Time)
		if n := len(out); n > 0 && out[n-1].At.Equal(p.At) {
			last := &out[n-1]
			last.Succeeded += p.Succeeded
			last.Failed += p.Failed
			last.Deleted += p.Deleted
			last.Retried += p.Retried
			continue
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kiln: series: %w", err)
	}
	return out, nil
}

func (s *Store) Servers(ctx context.Context) ([]driver.ServerInfo, error) {
	rows, err := s.db.QueryContext(ctx, s.q.servers)
	if err != nil {
		return nil, fmt.Errorf("kiln: servers: %w", err)
	}
	defer rows.Close()
	var out []driver.ServerInfo
	for rows.Next() {
		var (
			si              driver.ServerInfo
			queues, kinds   sql.RawBytes
			started, beaten stamp
		)
		err := rows.Scan(&si.ID, &si.Host, &si.PID, &si.Version, &queues, &kinds, &si.Workers, &si.Running,
			&started, &beaten)
		if err != nil {
			return nil, fmt.Errorf("kiln: servers: %w", err)
		}
		si.Queues, si.Kinds = decodeStrings(queues), decodeStrings(kinds)
		si.StartedAt, si.HeartbeatAt = started.Time, beaten.Time
		out = append(out, si)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kiln: servers: %w", err)
	}
	return out, nil
}

func (s *Store) Queues(ctx context.Context) ([]driver.QueueInfo, error) {
	rows, err := s.db.QueryContext(ctx, s.q.queues)
	if err != nil {
		return nil, fmt.Errorf("kiln: queues: %w", err)
	}
	defer rows.Close()
	var out []driver.QueueInfo
	for rows.Next() {
		var (
			qi  driver.QueueInfo
			lat int64
		)
		err := rows.Scan(&qi.Name, &qi.Paused, &qi.Enqueued, &qi.Processing, &qi.Scheduled, &qi.Throttled, &lat)
		if err != nil {
			return nil, fmt.Errorf("kiln: queues: %w", err)
		}
		qi.Latency = time.Duration(lat) * time.Microsecond
		out = append(out, qi)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kiln: queues: %w", err)
	}
	return out, nil
}

func (s *Store) Batch(ctx context.Context, id int64) (driver.Batch, error) {
	rows, err := s.db.QueryContext(ctx, render(s.q.batch, id))
	if err != nil {
		return driver.Batch{}, fmt.Errorf("kiln: batch: %w", err)
	}
	bs, err := scanBatches(rows)
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
	rows, err := s.db.QueryContext(ctx, render(s.q.batches, before, limit+1))
	if err != nil {
		return driver.BatchPage{}, fmt.Errorf("kiln: batches: %w", err)
	}
	bs, err := scanBatches(rows)
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

func scanBatches(rows *sql.Rows) ([]driver.Batch, error) {
	defer rows.Close()
	var out []driver.Batch
	for rows.Next() {
		var (
			b                 driver.Batch
			meta, counts      sql.RawBytes
			created, finished stamp
		)
		if err := rows.Scan(&b.ID, &b.Description, &meta, &b.Total, &b.Sealed, &created, &finished, &counts); err != nil {
			return nil, err
		}
		b.Meta = decodeMeta(meta)
		b.CreatedAt, b.FinishedAt = created.Time, finished.Time
		b.Counts = make(map[driver.State]int64)
		if len(counts) > 0 {
			var m map[string]int64
			if err := json.Unmarshal(counts, &m); err != nil {
				return nil, errors.Join(driver.ErrInvalid, err)
			}
			for k, v := range m {
				b.Counts[driver.State(k)] += v
			}
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
