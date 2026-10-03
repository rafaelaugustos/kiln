package pgstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rafaelaugustos/kiln/driver"
)

const sqlWriteConsole = `WITH j AS MATERIALIZED (
	SELECT id, attempt FROM {s}.jobs WHERE id = $1 AND claim = $2 AND state = 'processing' FOR NO KEY UPDATE
), p AS (
	UPDATE {s}.jobs x SET progress = $4::smallint FROM j WHERE x.id = j.id AND $4::smallint >= 0
), l AS (
	INSERT INTO {s}.logs (job_id, attempt, text)
	SELECT j.id, j.attempt, t.text FROM j, unnest($3::text[]) WITH ORDINALITY AS t(text, n) ORDER BY t.n
)
SELECT count(*) FROM j`

const sqlLogs = `SELECT seq, attempt, at, text FROM {s}.logs
WHERE job_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`

// WriteConsole appends lines to the job's console and, when progress is 0 or more, sets its
// progress, in one statement that holds the job's row while the job is processing under ref's
// claim, and fails with [driver.ErrLost] otherwise. Every job takes its Seq from one sequence.
func (s *Store) WriteConsole(ctx context.Context, ref driver.Ref, lines []string, progress int) error {
	var n int
	err := s.pool.QueryRow(ctx, s.q.writeConsole, ref.ID, ref.Claim, lines, progress).Scan(&n)
	if err != nil {
		return wrap("write console", err)
	}
	if n == 0 {
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
	rows, _ := s.pool.Query(ctx, s.q.logs, id, after, limit)
	ls, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (driver.LogLine, error) {
		var l driver.LogLine
		err := row.Scan(&l.Seq, &l.Attempt, &l.At, &l.Text)
		return l, err
	})
	if err != nil {
		return nil, fmt.Errorf("kiln: logs: %w", err)
	}
	return ls, nil
}
