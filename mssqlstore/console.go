package mssqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlWriteConsole = `UPDATE {p}jobs SET progress = @progress
WHERE @progress >= 0 AND id = @id AND claim = @claim AND state = 'processing';
INSERT INTO {p}logs (job_id, attempt, at, text)
SELECT j.id, j.attempt, SYSUTCDATETIME(), v.value
FROM {p}jobs j WITH (REPEATABLEREAD, ROWLOCK, FORCESEEK) CROSS APPLY OPENJSON(@lines) v
WHERE j.id = @id AND j.claim = @claim AND j.state = 'processing'
ORDER BY CAST(v.[key] AS INT);
SELECT COUNT(*) FROM {p}jobs WHERE id = @id AND claim = @claim AND state = 'processing'`

const sqlLogs = `SELECT TOP (@n) seq, attempt, at, text FROM {p}logs
WHERE job_id = @id AND seq > @after ORDER BY seq`

// WriteConsole appends lines to the job's console and, when progress is 0 or more, sets its
// progress, in one batch whose statements each check that the job is processing under ref's
// claim, and fails with [driver.ErrLost] when it is not. Every job takes its Seq from one
// IDENTITY column.
func (s *Store) WriteConsole(ctx context.Context, ref driver.Ref, lines []string, progress int) error {
	var n int
	err := s.db.QueryRowContext(ctx, s.q.writeConsole, sql.Named("id", ref.ID), sql.Named("claim", ref.Claim),
		sql.Named("progress", progress), sql.Named("lines", text(encodeStrings(lines)))).Scan(&n)
	switch {
	case err != nil:
		return wrap("write console", err)
	case n == 0:
		return fmt.Errorf("%w: job %d claim %d", driver.ErrLost, ref.ID, ref.Claim)
	}
	return nil
}

// Logs returns the lines of job id with a Seq above after, oldest first, at most limit of them,
// or 100 when limit is 0 or less.
func (s *Store) Logs(ctx context.Context, id int64, after int64, limit int) ([]driver.LogLine, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, s.q.logs, sql.Named("n", limit), sql.Named("id", id),
		sql.Named("after", after))
	if err != nil {
		return nil, fmt.Errorf("kiln: logs: %w", err)
	}
	defer rows.Close()
	var out []driver.LogLine
	for rows.Next() {
		var (
			l  driver.LogLine
			at moment
		)
		if err := rows.Scan(&l.Seq, &l.Attempt, &at, &l.Text); err != nil {
			return nil, fmt.Errorf("kiln: logs: %w", err)
		}
		l.At = at.Time
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kiln: logs: %w", err)
	}
	return out, nil
}
