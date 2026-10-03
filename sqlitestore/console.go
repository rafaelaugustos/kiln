package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlClaimed = `SELECT 1 FROM {p}jobs WHERE id = ? AND claim = ? AND state = 'processing'`

const sqlProgress = `UPDATE {p}jobs SET progress = ? WHERE id = ?`

const sqlAppendLogs = `INSERT INTO {p}logs (job_id, seq, attempt, at, text)
SELECT j.id, COALESCE((SELECT max(l.seq) FROM {p}logs l WHERE l.job_id = j.id), 0) + t.key + 1, j.attempt,
	{now}, t.value
FROM {p}jobs j, json_each(?) t
WHERE j.id = ?`

const sqlLogs = `SELECT seq, attempt, at, text FROM {p}logs
WHERE job_id = ? AND seq > ? ORDER BY seq LIMIT ?`

const sqlDropLogs = `DELETE FROM {p}logs WHERE job_id IN (SELECT value FROM json_each(?))`

// WriteConsole appends lines to the job's console and, when progress is 0 or more, sets its
// progress, in one transaction, while the job is processing under ref's claim, and fails with
// [driver.ErrLost] otherwise. The lines of a job are numbered from 1.
func (s *Store) WriteConsole(ctx context.Context, ref driver.Ref, lines []string, progress int) error {
	lost := false
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		var one int
		err := q.QueryRowContext(ctx, s.q.claimed, ref.ID, ref.Claim).Scan(&one)
		if lost = errors.Is(err, sql.ErrNoRows); lost || err != nil {
			return err
		}
		if progress >= 0 {
			if _, err := q.ExecContext(ctx, s.q.progress, progress, ref.ID); err != nil {
				return err
			}
		}
		if len(lines) > 0 {
			_, err = q.ExecContext(ctx, s.q.appendLogs, nameList(lines), ref.ID)
		}
		return err
	})
	switch {
	case lost:
		return fmt.Errorf("%w: job %d claim %d", driver.ErrLost, ref.ID, ref.Claim)
	case err != nil:
		return wrap("write console", err)
	}
	return nil
}

// Logs returns the lines of job id with a Seq above after, oldest first, at most limit of them,
// or 100 when limit is 0 or less.
func (s *Store) Logs(ctx context.Context, id int64, after int64, limit int) ([]driver.LogLine, error) {
	if limit <= 0 {
		limit = 100
	}
	var out []driver.LogLine
	rows, err := s.db.QueryContext(ctx, s.q.logs, id, after, limit)
	err = each(rows, err, func() error {
		var (
			l  driver.LogLine
			at stamp
		)
		if err := rows.Scan(&l.Seq, &l.Attempt, &at, &l.Text); err != nil {
			return err
		}
		l.At = at.Time
		out = append(out, l)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kiln: logs: %w", err)
	}
	return out, nil
}
