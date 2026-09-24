package pgstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rafaelaugustos/kiln/driver"
)

const liveRecord = `j.id, j.claim, j.kind, j.queue, j.args, j.meta, array_remove(j.tags, NULL), j.priority, j.attempt,
	j.max_attempts, j.timeout_ms, j.run_at, j.created_at, j.attempted_at, coalesce(j.batch_id, 0),
	coalesce(j.recurring_id, ''), array_remove(j.parents, NULL), coalesce(j.limit_key, ''), j.state::text,
	j.finalized_at, coalesce(j.server, ''), j.cancel_requested, coalesce(j.after_batch, 0), j.deps_pending`

const archivedRecord = `j.id, j.claim, j.kind, j.queue, j.args, j.meta, array_remove(j.tags, NULL), j.priority, j.attempt,
	j.max_attempts, j.timeout_ms, j.run_at, j.created_at, j.attempted_at, coalesce(j.batch_id, 0),
	coalesce(j.recurring_id, ''), array_remove(j.parents, NULL), coalesce(j.limit_key, ''), j.state::text,
	j.finalized_at, coalesce(j.server, ''), false, coalesce(j.after_batch, 0), 0`

const sqlJob = `SELECT ` + liveRecord + `, j.history, NULL::json FROM {s}.jobs j WHERE j.id = $1
UNION ALL
SELECT ` + archivedRecord + `, j.history, j.output FROM {s}.archive j WHERE j.id = $1
LIMIT 1`

const sqlChildren = `SELECT job_id FROM {s}.deps WHERE NOT batch AND parent_id = $1 ORDER BY job_id`

const sqlCounts = `SELECT
	(SELECT count(*) FROM (SELECT 1 FROM {s}.jobs WHERE state = 'awaiting' LIMIT 100000) x),
	(SELECT count(*) FROM (SELECT 1 FROM {s}.jobs WHERE state = 'scheduled' LIMIT 100000) x),
	(SELECT count(*) FROM (SELECT 1 FROM {s}.jobs WHERE state = 'throttled' LIMIT 100000) x),
	(SELECT count(*) FROM (SELECT 1 FROM {s}.jobs WHERE state = 'enqueued' LIMIT 100000) x),
	(SELECT count(*) FROM (SELECT 1 FROM {s}.jobs WHERE state = 'processing' LIMIT 100000) x),
	(SELECT count(*) FROM (SELECT 1 FROM {s}.jobs WHERE state = 'failed' LIMIT 100000) x),
	(SELECT count(*) FROM (SELECT 1 FROM {s}.jobs WHERE state = 'scheduled' AND attempt > 0 LIMIT 100000) x),
	(SELECT coalesce(sum(succeeded), 0) FROM {s}.stats),
	(SELECT coalesce(sum(deleted), 0) FROM {s}.stats)`

const sqlSeries = `SELECT date_bin($3 * interval '1 microsecond', bucket, 'epoch') AS b,
	sum(succeeded), sum(failed), sum(deleted), sum(retried)
FROM {s}.stats
WHERE bucket > '-infinity' AND bucket >= date_bin($3 * interval '1 microsecond', $1::timestamptz, 'epoch')
	AND bucket < $2
GROUP BY b
ORDER BY b`

const sqlServers = `SELECT id, coalesce(host, ''), coalesce(pid, 0), coalesce(version, ''), queues, kinds,
	coalesce(workers, 0), coalesce(running, 0), started_at, heartbeat_at
FROM {s}.servers ORDER BY id`

const sqlQueues = `WITH live AS (
	SELECT queue,
		count(*) FILTER (WHERE state = 'enqueued') AS enqueued,
		count(*) FILTER (WHERE state = 'processing') AS processing,
		count(*) FILTER (WHERE state = 'scheduled') AS scheduled,
		count(*) FILTER (WHERE state = 'throttled') AS throttled,
		min(run_at) FILTER (WHERE state = 'enqueued') AS oldest
	FROM {s}.jobs GROUP BY queue
), names AS (
	SELECT queue AS name FROM live
	UNION SELECT name FROM {s}.queues
	UNION SELECT unnest(queues) FROM {s}.servers
)
SELECT n.name, coalesce(p.paused, false), coalesce(l.enqueued, 0), coalesce(l.processing, 0),
	coalesce(l.scheduled, 0), coalesce(l.throttled, 0),
	greatest(coalesce((extract(epoch FROM now() - l.oldest) * 1000000)::bigint, 0), 0)
FROM names n
LEFT JOIN live l ON l.queue = n.name
LEFT JOIN {s}.queues p ON p.name = n.name
ORDER BY n.name`

const batchColumns = `b.id, b.description, b.meta, b.total, b.sealed, b.created_at, b.finished_at,
	(SELECT jsonb_object_agg(c.state, c.n) FROM (
		SELECT state::text AS state, count(*) AS n FROM {s}.jobs WHERE batch_id = b.id GROUP BY state
		UNION ALL
		SELECT state::text, count(*) FROM {s}.archive WHERE batch_id = b.id GROUP BY state) c)`

const sqlBatch = `SELECT ` + batchColumns + ` FROM {s}.batches b WHERE b.id = $1`

const sqlBatches = `SELECT ` + batchColumns + ` FROM {s}.batches b WHERE b.id < $1 ORDER BY b.id DESC LIMIT $2`

func (s *Store) Job(ctx context.Context, id int64) (driver.Record, error) {
	var (
		recs     []driver.Record
		children []int64
	)
	b := &pgx.Batch{}
	b.Queue(s.q.job, id).Query(func(rows pgx.Rows) error {
		var err error
		recs, err = scanRecords(rows, true)
		return err
	})
	b.Queue(s.q.children, id).Query(func(rows pgx.Rows) error {
		var err error
		children, err = pgx.CollectRows(rows, pgx.RowTo[int64])
		return err
	})
	if err := s.pool.SendBatch(ctx, b).Close(); err != nil {
		return driver.Record{}, fmt.Errorf("kiln: job: %w", err)
	}
	if len(recs) == 0 {
		return driver.Record{}, fmt.Errorf("%w: job %d", driver.ErrNotFound, id)
	}
	r := recs[0]
	r.Children = children
	return r, nil
}

func scanRecords(rows pgx.Rows, full bool) ([]driver.Record, error) {
	defer rows.Close()
	var out []driver.Record
	for rows.Next() {
		var (
			r                   driver.Record
			meta, hist, output  []byte
			ms                  int64
			attempted, finished pgtype.Timestamptz
			state               string
			pending             int32
		)
		dst := []any{&r.ID, &r.Claim, &r.Kind, &r.Queue, &r.Args, &meta, &r.Tags, &r.Priority, &r.Attempt,
			&r.MaxAttempts, &ms, &r.RunAt, &r.CreatedAt, &attempted, &r.BatchID, &r.RecurringID, &r.Parents,
			&r.LimitKey, &state, &finished, &r.Server, &r.CancelRequested, &r.AfterBatch, &pending}
		if full {
			dst = append(dst, &hist, &output)
		}
		if err := rows.Scan(dst...); err != nil {
			return nil, err
		}
		r.Meta = decodeMeta(meta)
		r.Timeout = timeout(ms)
		r.AttemptedAt, r.FinalizedAt = attempted.Time, finished.Time
		r.State = driver.State(state)
		r.PendingDeps = int(pending)
		if len(hist) > 0 {
			r.History = decodeHistory(hist)
		}
		if len(output) > 0 {
			r.Output = output
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func decodeHistory(b []byte) []driver.Entry {
	var raw []struct {
		At      time.Time    `json:"at"`
		State   driver.State `json:"state"`
		Attempt int          `json:"attempt"`
		Reason  string       `json:"reason"`
		Error   string       `json:"error"`
		Trace   string       `json:"trace"`
		Server  string       `json:"server"`
	}
	if json.Unmarshal(b, &raw) != nil {
		return nil
	}
	out := make([]driver.Entry, len(raw))
	for i, e := range raw {
		out[i] = driver.Entry(e)
	}
	return out
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
	var (
		sb   strings.Builder
		args []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	fmt.Fprintf(&sb, "SELECT %s FROM %s.%s j WHERE j.state = '%s'", cols, s.schema, table, q.State)
	if q.Queue != "" {
		sb.WriteString(" AND j.queue = " + arg(q.Queue))
	}
	if q.Kind != "" {
		sb.WriteString(" AND j.kind = " + arg(q.Kind))
	}
	if q.BatchID != 0 {
		sb.WriteString(" AND j.batch_id = " + arg(q.BatchID))
	}
	var order string
	switch q.State {
	case driver.Scheduled:
		order = "j.run_at, j.id"
		if q.Cursor != "" {
			sb.WriteString(fmt.Sprintf(" AND (j.run_at, j.id) > (%s, %s)", arg(time.UnixMicro(c1)), arg(c2)))
		}
	case driver.Enqueued, driver.Throttled:
		order = "j.priority DESC, j.id"
		if q.Cursor != "" {
			p, id := arg(int16(c1)), arg(c2)
			sb.WriteString(fmt.Sprintf(" AND (j.priority < %s OR j.priority = %s AND j.id > %s)", p, p, id))
		}
	case driver.Succeeded, driver.Deleted, driver.Failed:
		order = "j.finalized_at DESC, j.id DESC"
		if q.Cursor != "" {
			sb.WriteString(fmt.Sprintf(" AND (j.finalized_at, j.id) < (%s, %s)", arg(time.UnixMicro(c1)), arg(c2)))
		}
	default:
		order = "j.id DESC"
		if q.Cursor != "" {
			sb.WriteString(" AND j.id < " + arg(c2))
		}
	}
	fmt.Fprintf(&sb, " ORDER BY %s LIMIT %d", order, limit+1)
	rows, err := s.pool.Query(ctx, sb.String(), args...)
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
	err := s.pool.QueryRow(ctx, s.q.counts).Scan(&c.Awaiting, &c.Scheduled, &c.Throttled, &c.Enqueued,
		&c.Processing, &c.Failed, &c.Retries, &c.Succeeded, &c.Deleted)
	if err != nil {
		return driver.Counts{}, fmt.Errorf("kiln: counts: %w", err)
	}
	for _, n := range []int64{c.Awaiting, c.Scheduled, c.Throttled, c.Enqueued, c.Processing, c.Failed, c.Retries} {
		if n >= 100000 {
			c.Capped = true
		}
	}
	return c, nil
}

func (s *Store) Series(ctx context.Context, from, to time.Time, step time.Duration) ([]driver.Point, error) {
	if step <= 0 || step%time.Minute != 0 {
		return nil, fmt.Errorf("%w: series step %s", driver.ErrInvalid, step)
	}
	rows, err := s.pool.Query(ctx, s.q.series, from, to, micros(step))
	if err != nil {
		return nil, fmt.Errorf("kiln: series: %w", err)
	}
	defer rows.Close()
	var out []driver.Point
	for rows.Next() {
		var p driver.Point
		if err := rows.Scan(&p.At, &p.Succeeded, &p.Failed, &p.Deleted, &p.Retried); err != nil {
			return nil, fmt.Errorf("kiln: series: %w", err)
		}
		p.At = p.At.UTC()
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kiln: series: %w", err)
	}
	return out, nil
}

func (s *Store) Servers(ctx context.Context) ([]driver.ServerInfo, error) {
	rows, err := s.pool.Query(ctx, s.q.servers)
	if err != nil {
		return nil, fmt.Errorf("kiln: servers: %w", err)
	}
	defer rows.Close()
	var out []driver.ServerInfo
	for rows.Next() {
		var (
			si      driver.ServerInfo
			started pgtype.Timestamptz
		)
		err := rows.Scan(&si.ID, &si.Host, &si.PID, &si.Version, &si.Queues, &si.Kinds, &si.Workers, &si.Running,
			&started, &si.HeartbeatAt)
		if err != nil {
			return nil, fmt.Errorf("kiln: servers: %w", err)
		}
		si.StartedAt = started.Time
		out = append(out, si)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kiln: servers: %w", err)
	}
	return out, nil
}

func (s *Store) Queues(ctx context.Context) ([]driver.QueueInfo, error) {
	rows, err := s.pool.Query(ctx, s.q.queues)
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
	rows, err := s.pool.Query(ctx, s.q.batch, id)
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
	rows, err := s.pool.Query(ctx, s.q.batches, before, limit+1)
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

func scanBatches(rows pgx.Rows) ([]driver.Batch, error) {
	defer rows.Close()
	var out []driver.Batch
	for rows.Next() {
		var (
			b            driver.Batch
			meta, counts []byte
			finished     pgtype.Timestamptz
		)
		if err := rows.Scan(&b.ID, &b.Description, &meta, &b.Total, &b.Sealed, &b.CreatedAt, &finished, &counts); err != nil {
			return nil, err
		}
		b.Meta = decodeMeta(meta)
		b.FinishedAt = finished.Time
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
