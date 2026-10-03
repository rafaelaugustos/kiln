package mssqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlClaim = `WITH c AS (
	SELECT TOP (@n) j.id, j.priority, q.o
	FROM OPENJSON(@queues) WITH (name NVARCHAR(255) '$.q', o INT '$.o') q
	CROSS APPLY (
		SELECT TOP (@n) x.id, x.priority FROM {p}jobs x WITH (XLOCK, READPAST, ROWLOCK, INDEX(jobs_fetch))
		WHERE x.state = 'enqueued' AND x.queue = q.name COLLATE Latin1_General_100_BIN2
			AND (@kinds IS NULL OR x.kind IN (SELECT k COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@kinds) WITH (k NVARCHAR(255) '$')))
			AND NOT EXISTS (SELECT 1 FROM {p}queues p WHERE p.name = q.name COLLATE Latin1_General_100_BIN2 AND p.paused = 1)
		ORDER BY x.priority DESC, x.id
	) j
	ORDER BY q.o, j.priority DESC, j.id
)
UPDATE j SET state = 'processing', attempt = j.attempt + 1, claim = j.claim + 1, attempted_at = SYSUTCDATETIME(),
	server = @server, cancel_requested = 0
OUTPUT inserted.id, inserted.claim, inserted.kind, inserted.queue, inserted.args, inserted.meta, inserted.tags,
	inserted.priority, inserted.attempt, inserted.max_attempts, inserted.timeout_ms, inserted.run_at,
	inserted.created_at, inserted.attempted_at, COALESCE(inserted.batch_id, 0), COALESCE(inserted.recurring_id, N''),
	inserted.parents, COALESCE(inserted.limit_key, N''), COALESCE(inserted.title, N'')
FROM {p}jobs j JOIN c ON c.id = j.id`

// Claim moves up to q.Limit enqueued jobs to processing in one statement, as
// [driver.Worker.Claim] describes. It reads them with the READPAST hint, so servers claiming at the
// same time neither wait for each other nor take the same job.
func (s *Store) Claim(ctx context.Context, q driver.ClaimQuery) ([]driver.Job, error) {
	if q.Limit <= 0 || len(q.Queues) == 0 {
		return nil, nil
	}
	var t table
	for i, name := range uniq(q.Queues) {
		t.row()
		t.str("q", name)
		t.int("o", int64(i))
	}
	var kinds any
	if len(q.Kinds) > 0 {
		kinds = stringList(q.Kinds)
	}
	rows, err := s.db.QueryContext(ctx, s.q.claim, sql.Named("n", q.Limit), sql.Named("queues", t.String()),
		sql.Named("kinds", kinds), sql.Named("server", q.Server))
	if err != nil {
		return nil, fmt.Errorf("kiln: claim: %w", err)
	}
	jobs, err := scanClaimed(rows)
	if err != nil {
		return nil, fmt.Errorf("kiln: claim: %w", err)
	}
	return jobs, nil
}

func uniq(ss []string) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}

func scanClaimed(rows *sql.Rows) ([]driver.Job, error) {
	defer rows.Close()
	var (
		jobs                    []driver.Job
		meta, tags, parents     sql.RawBytes
		ms                      int64
		run, created, attempted moment
	)
	for rows.Next() {
		var j driver.Job
		err := rows.Scan(&j.ID, &j.Claim, &j.Kind, &j.Queue, &j.Args, &meta, &tags, &j.Priority, &j.Attempt,
			&j.MaxAttempts, &ms, &run, &created, &attempted, &j.BatchID, &j.RecurringID, &parents, &j.LimitKey,
			&j.Title)
		if err != nil {
			return jobs, err
		}
		j.Meta = decodeMeta(meta)
		j.Tags = decodeStrings(tags)
		j.Parents = decodeIDs(parents)
		j.Timeout = timeout(ms)
		j.RunAt, j.CreatedAt, j.AttemptedAt = run.Time, created.Time, attempted.Time
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
