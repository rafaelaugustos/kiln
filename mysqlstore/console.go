package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlAppendLogs = `INSERT INTO {p}logs (job_id, attempt, at, text)
SELECT j.id, j.attempt, UTC_TIMESTAMP(6), v.text
FROM (VALUES `

const sqlAppendLogsTail = `) AS v (n, text) STRAIGHT_JOIN {p}jobs j FORCE INDEX (PRIMARY) ON j.id = ?
WHERE j.claim = ? AND j.state = 'processing'
ORDER BY v.n`

const sqlSetProgress = `UPDATE {p}jobs SET progress = ? WHERE id = ? AND claim = ? AND state = 'processing'`

const sqlLogs = `SELECT seq, attempt, at, text FROM {p}logs
WHERE job_id = ? AND seq > ? ORDER BY seq LIMIT ?`

const sqlDropLogs = `DELETE FROM {p}logs WHERE job_id IN (?)`

// WriteConsole appends lines to the job's console and, when progress is 0 or more, sets its
// progress, while the job is processing under ref's claim, and fails with [driver.ErrLost]
// otherwise. The lines go in with an INSERT that checks the claim as it reads the job's row, and
// the progress with an UPDATE that does the same, so a write with both takes two statements.
// Every job takes its Seq from one AUTO_INCREMENT column.
func (s *Store) WriteConsole(ctx context.Context, ref driver.Ref, lines []string, progress int) error {
	held := false
	if len(lines) > 0 {
		c := &chunks{head: s.q.appendLogs, tail: render(s.q.appendLogsTail, ref.ID, ref.Claim), max: s.budget}
		for i, line := range lines {
			c.row()
			c.b = appendSQL(c.b, "ROW(?, ?)", i, line)
		}
		for _, stmt := range c.done() {
			r, err := s.db.ExecContext(ctx, stmt)
			if err != nil {
				return wrap("write console", err)
			}
			if n, err := r.RowsAffected(); err == nil && n == 0 {
				return fmt.Errorf("%w: job %d claim %d", driver.ErrLost, ref.ID, ref.Claim)
			}
		}
		held = true
	}
	if progress >= 0 {
		r, err := s.db.ExecContext(ctx, render(s.q.setProgress, progress, ref.ID, ref.Claim))
		if err != nil {
			return wrap("write console", err)
		}
		if n, err := r.RowsAffected(); err == nil && n > 0 {
			held = true
		}
	}
	if held {
		return nil
	}
	var one int
	err := s.db.QueryRowContext(ctx, render(s.q.held, ref.ID, ref.Claim)).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
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
	rows, err := s.db.QueryContext(ctx, render(s.q.logs, id, after, limit))
	if err != nil {
		return nil, fmt.Errorf("kiln: logs: %w", err)
	}
	defer rows.Close()
	var out []driver.LogLine
	for rows.Next() {
		var (
			l  driver.LogLine
			at stamp
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
