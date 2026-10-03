package mssqlstore

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
	j.timeout_ms, j.run_at, j.created_at, j.attempted_at, COALESCE(j.batch_id, 0), COALESCE(j.recurring_id, N''),
	j.parents, COALESCE(j.limit_key, N''), j.state, j.finalized_at, COALESCE(j.server, N''), COALESCE(j.after_batch, 0)`

const liveRecord = recordColumns + `, j.cancel_requested, j.deps_pending, j.progress, COALESCE(j.title, N'')`

const archivedRecord = recordColumns + `, CAST(0 AS BIT), 0, j.progress, COALESCE(j.title, N'')`

const childrenOf = `(SELECT N'[' + STRING_AGG(CAST(d.job_id AS NVARCHAR(MAX)), N',') + N']' FROM {p}deps d
	WHERE d.batch = 0 AND d.parent_id = j.id)`

const sqlJob = `SELECT ` + liveRecord + `, j.history, NULL, ` + childrenOf + ` FROM {p}jobs j WHERE j.id = @id
UNION ALL
SELECT ` + archivedRecord + `, j.history, j.[output], ` + childrenOf + ` FROM {p}archive j WHERE j.id = @id`

const capped = 100000

const sqlCounts = `SELECT
	(SELECT COUNT(*) FROM (SELECT TOP (100000) 1 AS x FROM {p}jobs WHERE state = 'awaiting') c),
	(SELECT COUNT(*) FROM (SELECT TOP (100000) 1 AS x FROM {p}jobs WHERE state = 'scheduled') c),
	(SELECT COUNT(*) FROM (SELECT TOP (100000) 1 AS x FROM {p}jobs WHERE state = 'throttled') c),
	(SELECT COUNT(*) FROM (SELECT TOP (100000) 1 AS x FROM {p}jobs WHERE state = 'enqueued') c),
	(SELECT COUNT(*) FROM (SELECT TOP (100000) 1 AS x FROM {p}jobs WHERE state = 'processing') c),
	(SELECT COUNT(*) FROM (SELECT TOP (100000) 1 AS x FROM {p}jobs WHERE state = 'failed') c),
	(SELECT COUNT(*) FROM (SELECT TOP (100000) 1 AS x FROM {p}jobs WITH (INDEX(jobs_due))
		WHERE state = 'scheduled' AND attempt > 0) c),
	(SELECT COALESCE(SUM(succeeded), 0) FROM {p}stats),
	(SELECT COALESCE(SUM(deleted), 0) FROM {p}stats)`

const sqlSeries = `SELECT bucket, SUM(succeeded), SUM(failed), SUM(deleted), SUM(retried) FROM {p}stats
WHERE bucket > @totals AND bucket >= @from AND bucket < @to
GROUP BY bucket
ORDER BY bucket`

const sqlServers = `SELECT id, COALESCE(host, N''), COALESCE(pid, 0), COALESCE(version, N''), queues, kinds,
	COALESCE(workers, 0), COALESCE(running, 0), started_at, heartbeat_at
FROM {p}servers ORDER BY id`

const sqlQueues = `SELECT n.name, COALESCE(p.paused, CAST(0 AS BIT)), COALESCE(l.enqueued, 0), COALESCE(l.processing, 0),
	COALESCE(l.scheduled, 0), COALESCE(l.throttled, 0),
	CASE WHEN l.oldest < SYSUTCDATETIME() THEN DATEDIFF_BIG(MICROSECOND, l.oldest, SYSUTCDATETIME()) ELSE 0 END
FROM (
	SELECT queue AS name FROM {p}jobs GROUP BY queue
	UNION SELECT name FROM {p}queues
	UNION SELECT q.name COLLATE Latin1_General_100_BIN2 FROM {p}servers s
		CROSS APPLY OPENJSON(s.queues) WITH (name NVARCHAR(255) '$') q
) n
LEFT JOIN (
	SELECT queue, SUM(CASE WHEN state = 'enqueued' THEN 1 ELSE 0 END) AS enqueued,
		SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) AS processing,
		SUM(CASE WHEN state = 'scheduled' THEN 1 ELSE 0 END) AS scheduled,
		SUM(CASE WHEN state = 'throttled' THEN 1 ELSE 0 END) AS throttled,
		MIN(CASE WHEN state = 'enqueued' THEN run_at END) AS oldest
	FROM {p}jobs GROUP BY queue
) l ON l.queue = n.name
LEFT JOIN {p}queues p ON p.name = n.name
ORDER BY n.name`

const sqlLimitInfo = `SELECT TOP (@n) l.limit_key, l.[max], l.rate, l.per_us, l.burst, l.active,
	(SELECT COUNT(*) FROM {p}jobs j WHERE j.state = 'throttled' AND j.limit_key = l.limit_key),
	(SELECT COUNT(*) FROM {p}jobs j WHERE j.state = 'scheduled' AND j.granted = 1 AND j.limit_key = l.limit_key),
	l.tat, CAST(SYSUTCDATETIME() AS DATETIME2(6))
FROM {p}limits l
WHERE l.limit_key > @after
ORDER BY l.limit_key`

const batchColumns = `b.id, b.description, b.meta, b.total, b.sealed, b.created_at, b.finished_at,
	(SELECT N'{' + STRING_AGG(N'"' + c.state + N'":' + CAST(c.n AS NVARCHAR(MAX)), N',') + N'}' FROM (
		SELECT x.state, SUM(x.n) AS n FROM (
			SELECT state, COUNT(*) AS n FROM {p}jobs WHERE batch_id = b.id GROUP BY state
			UNION ALL
			SELECT state, COUNT(*) FROM {p}archive WHERE batch_id = b.id GROUP BY state) x
		GROUP BY x.state) c)`

const sqlBatch = `SELECT ` + batchColumns + ` FROM {p}batches b WHERE b.id = @id`

const sqlBatches = `SELECT TOP (@n) ` + batchColumns + ` FROM {p}batches b WHERE b.id < @before ORDER BY b.id DESC`

// Job returns the job id, live or archived, with its history, output, children and pending
// dependencies, or an error wrapping [driver.ErrNotFound].
func (s *Store) Job(ctx context.Context, id int64) (driver.Record, error) {
	rows, err := s.db.QueryContext(ctx, s.q.job, sql.Named("id", id))
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
		output                              []byte
		ms                                  int64
		run, created, attempted, finished   moment
		state                               string
		pending                             int
	)
	for rows.Next() {
		var r driver.Record
		dst := []any{&r.ID, &r.Claim, &r.Kind, &r.Queue, &r.Args, &meta, &tags, &r.Priority, &r.Attempt,
			&r.MaxAttempts, &ms, &run, &created, &attempted, &r.BatchID, &r.RecurringID, &parents, &r.LimitKey,
			&state, &finished, &r.Server, &r.AfterBatch, &r.CancelRequested, &pending, &r.Progress, &r.Title}
		if full {
			dst = append(dst, &hist, &output, &children)
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
			if len(output) > 0 {
				r.Output = output
			}
			r.Children = decodeIDs(children)
			slices.Sort(r.Children)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Jobs returns a page of the jobs in q.State that match the rest of q, in the order
// [driver.Inspector.Jobs] specifies. Its records leave out History, Output and Children.
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
	from, cols := "jobs", liveRecord
	if q.State.Archived() {
		from, cols = "archive", archivedRecord
	}
	var b strings.Builder
	b.WriteString("SELECT TOP (@n) ")
	b.WriteString(cols)
	b.WriteString(" FROM ")
	b.WriteString(s.prefix)
	b.WriteString(from)
	b.WriteString(" j WHERE j.state = CAST(@state AS VARCHAR(10))")
	args := []any{sql.Named("n", limit+1), sql.Named("state", string(q.State))}
	if q.Queue != "" {
		b.WriteString(" AND j.queue = @queue")
		args = append(args, sql.Named("queue", q.Queue))
	}
	if q.Kind != "" {
		b.WriteString(" AND j.kind = @kind")
		args = append(args, sql.Named("kind", q.Kind))
	}
	if q.BatchID != 0 {
		b.WriteString(" AND j.batch_id = @batch")
		args = append(args, sql.Named("batch", q.BatchID))
	}
	var order string
	switch q.State {
	case driver.Scheduled:
		order = "j.run_at, j.id"
		if q.Cursor != "" {
			b.WriteString(" AND (j.run_at > @c1 OR j.run_at = @c1 AND j.id > @c2)")
			args = append(args, sql.Named("c1", stamp(time.UnixMicro(c1))), sql.Named("c2", c2))
		}
	case driver.Enqueued, driver.Throttled:
		order = "j.priority DESC, j.id"
		if q.Cursor != "" {
			b.WriteString(" AND (j.priority < @c1 OR j.priority = @c1 AND j.id > @c2)")
			args = append(args, sql.Named("c1", c1), sql.Named("c2", c2))
		}
	case driver.Succeeded, driver.Deleted, driver.Failed:
		order = "j.finalized_at DESC, j.id DESC"
		if q.Cursor != "" {
			b.WriteString(" AND (j.finalized_at < @c1 OR j.finalized_at = @c1 AND j.id < @c2)")
			args = append(args, sql.Named("c1", stamp(time.UnixMicro(c1))), sql.Named("c2", c2))
		}
	default:
		order = "j.id DESC"
		if q.Cursor != "" {
			b.WriteString(" AND j.id < @c2")
			args = append(args, sql.Named("c2", c2))
		}
	}
	b.WriteString(" ORDER BY ")
	b.WriteString(order)
	rows, err := s.db.QueryContext(ctx, b.String(), args...)
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

// Counts returns, in one query, the number of jobs in each live state and of scheduled jobs that
// are retries, each counted up to 100000, and the all-time totals of succeeded and deleted jobs.
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

// Series sums the statistics between from and to into buckets of step, which must be a positive
// multiple of a minute, as [driver.Inspector.Series] describes.
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
	rows, err := s.db.QueryContext(ctx, s.q.series, sql.Named("totals", stamp(totals)), sql.Named("from", stamp(align(from))),
		sql.Named("to", stamp(to)))
	if err != nil {
		return nil, fmt.Errorf("kiln: series: %w", err)
	}
	defer rows.Close()
	var out []driver.Point
	for rows.Next() {
		var (
			at moment
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

// Servers returns the registered servers, ordered by id.
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
			started, beaten moment
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

// Queues returns every queue that has live jobs, a pause setting or a server working on it,
// ordered by name.
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

// Limits returns the limit keys after the given key, in key order, at most limit of them, 100
// when limit is 0 or less, as [driver.LimitReader] describes, in one round trip that counts the
// jobs of each key through the indexes on limit_key.
func (s *Store) Limits(ctx context.Context, after string, limit int) ([]driver.LimitInfo, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, s.q.limitInfo, sql.Named("n", limit), sql.Named("after", after))
	if err != nil {
		return nil, fmt.Errorf("kiln: limits: %w", err)
	}
	defer rows.Close()
	var out []driver.LimitInfo
	for rows.Next() {
		var (
			l        driver.LimitInfo
			per      int64
			tat, now moment
		)
		err := rows.Scan(&l.Key, &l.Max, &l.Rate, &per, &l.Burst, &l.Active, &l.Throttled, &l.Reserved, &tat, &now)
		if err != nil {
			return nil, fmt.Errorf("kiln: limits: %w", err)
		}
		l.Per = time.Duration(per) * time.Microsecond
		if l.Rate > 0 {
			tau := time.Duration(l.Burst-1) * (l.Per / time.Duration(l.Rate))
			if next := tat.Add(-tau); next.After(now.Time) {
				l.NextStart = next
			}
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kiln: limits: %w", err)
	}
	return out, nil
}

// Batch returns the batch id with its members counted by state, or an error wrapping
// [driver.ErrNotFound].
func (s *Store) Batch(ctx context.Context, id int64) (driver.Batch, error) {
	rows, err := s.db.QueryContext(ctx, s.q.batch, sql.Named("id", id))
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

// Batches returns a page of batches, newest first.
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
	rows, err := s.db.QueryContext(ctx, s.q.batches, sql.Named("before", before), sql.Named("n", limit+1))
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
			created, finished moment
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
