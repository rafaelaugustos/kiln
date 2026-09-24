package mysqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const recurringColumns = `id, spec, location, template, misfire, overlap, paused, next_run_at, last_run_at,
	COALESCE(last_job_id, 0), created_at, updated_at, version`

const sqlDueRecurring = `SELECT ` + recurringColumns + ` FROM {p}recurring
WHERE NOT paused AND next_run_at <= ?
ORDER BY next_run_at
LIMIT ?`

const sqlRecurring = `SELECT ` + recurringColumns + ` FROM {p}recurring WHERE id = ?`

const sqlRecurrings = `SELECT ` + recurringColumns + ` FROM {p}recurring ORDER BY id`

const sqlFire = `UPDATE {p}recurring SET version = version + 1, next_run_at = ?, last_run_at = ?, updated_at = UTC_TIMESTAMP(6)
WHERE id = ? AND version = ?`

const sqlFired = `UPDATE {p}recurring SET last_job_id = ? WHERE id = ?`

const sqlHasRecurring = `SELECT COUNT(*) FROM {p}recurring WHERE id = ?`

const sqlCreateRecurring = `INSERT INTO {p}recurring (id, spec, location, template, misfire, overlap, paused, next_run_at,
	last_run_at, last_job_id, created_at, updated_at, version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), 1)`

const sqlUpdateRecurring = `UPDATE {p}recurring SET spec = ?, location = ?, template = ?, misfire = ?, overlap = ?,
	paused = ?, next_run_at = ?, last_run_at = ?, last_job_id = ?, updated_at = UTC_TIMESTAMP(6),
	version = version + 1
WHERE id = ? AND version = ?`

const sqlRemoveRecurring = `DELETE FROM {p}recurring WHERE id = ?`

func (s *Store) Due(ctx context.Context, limit int) ([]driver.Recurring, time.Time, error) {
	now, err := s.Now(ctx)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("kiln: due: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, render(s.q.dueRecurring, now, max(limit, 1)))
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("kiln: due: %w", err)
	}
	out, err := scanRecurrings(rows)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("kiln: due: %w", err)
	}
	return out, now, nil
}

func (s *Store) Fire(ctx context.Context, f driver.Fire) ([]driver.Inserted, error) {
	var (
		res    []driver.Inserted
		limits map[string]rule
	)
	err := s.txn(ctx, func(tx *sql.Tx) error {
		res, limits = nil, nil
		r, err := tx.ExecContext(ctx, render(s.q.fire, f.NextRunAt, f.LastRunAt, f.ID, f.Version))
		if err != nil {
			return wrap("fire", err)
		}
		if n, err := r.RowsAffected(); err != nil || n == 0 {
			var found int
			if err := tx.QueryRowContext(ctx, render(s.q.hasRecurring, f.ID)).Scan(&found); err != nil {
				return wrap("fire", err)
			}
			if found == 0 {
				return fmt.Errorf("%w: recurring %s", driver.ErrNotFound, f.ID)
			}
			return fmt.Errorf("%w: recurring %s version %d", driver.ErrConflict, f.ID, f.Version)
		}
		if len(f.Jobs) == 0 {
			return nil
		}
		if res, limits, err = s.insert(ctx, tx, f.Jobs); err != nil {
			return err
		}
		var last int64
		for _, r := range res {
			if !r.Duplicate {
				last = r.ID
			}
		}
		if last == 0 {
			return nil
		}
		if _, err := tx.ExecContext(ctx, render(s.q.fired, last, f.ID)); err != nil {
			return wrap("fire", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(limits) > 0 {
		s.admitKeys(ctx, limits)
	}
	return res, nil
}

func (s *Store) Recurring(ctx context.Context, id string) (driver.Recurring, error) {
	rows, err := s.db.QueryContext(ctx, render(s.q.recurring, id))
	if err != nil {
		return driver.Recurring{}, fmt.Errorf("kiln: recurring: %w", err)
	}
	rs, err := scanRecurrings(rows)
	if err != nil {
		return driver.Recurring{}, fmt.Errorf("kiln: recurring: %w", err)
	}
	if len(rs) == 0 {
		return driver.Recurring{}, fmt.Errorf("%w: recurring %s", driver.ErrNotFound, id)
	}
	return rs[0], nil
}

func (s *Store) Recurrings(ctx context.Context) ([]driver.Recurring, error) {
	rows, err := s.db.QueryContext(ctx, s.q.recurrings)
	if err != nil {
		return nil, fmt.Errorf("kiln: recurrings: %w", err)
	}
	rs, err := scanRecurrings(rows)
	if err != nil {
		return nil, fmt.Errorf("kiln: recurrings: %w", err)
	}
	return rs, nil
}

func (s *Store) PutRecurring(ctx context.Context, r driver.Recurring) error {
	if r.ID == "" || r.Spec == "" {
		return fmt.Errorf("%w: recurring needs an id and a spec", driver.ErrInvalid)
	}
	tmpl, err := json.Marshal(r.Template)
	if err != nil {
		return fmt.Errorf("%w: recurring template: %v", driver.ErrInvalid, err)
	}
	var lastJob any
	if r.LastJobID != 0 {
		lastJob = r.LastJobID
	}
	var stmt string
	if r.Version == 0 {
		stmt = render(s.q.createRecurring, r.ID, r.Spec, r.Location, jsonText(tmpl), int16(r.Misfire), r.Overlap,
			r.Paused, r.NextRunAt, r.LastRunAt, lastJob)
	} else {
		stmt = render(s.q.updateRecurring, r.Spec, r.Location, jsonText(tmpl), int16(r.Misfire), r.Overlap, r.Paused,
			r.NextRunAt, r.LastRunAt, lastJob, r.ID, r.Version)
	}
	res, err := s.db.ExecContext(ctx, stmt)
	if me := mysqlError(err); me != nil && me.Number == errDuplicate {
		return fmt.Errorf("%w: recurring %s exists", driver.ErrConflict, r.ID)
	}
	if err != nil {
		return wrap("put recurring", err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return fmt.Errorf("%w: recurring %s version %d", driver.ErrConflict, r.ID, r.Version)
	}
	return nil
}

func (s *Store) RemoveRecurring(ctx context.Context, id string) error {
	r, err := s.db.ExecContext(ctx, render(s.q.removeRecurring, id))
	if err != nil {
		return wrap("remove recurring", err)
	}
	if n, err := r.RowsAffected(); err != nil || n == 0 {
		return fmt.Errorf("%w: recurring %s", driver.ErrNotFound, id)
	}
	return nil
}

func scanRecurrings(rows *sql.Rows) ([]driver.Recurring, error) {
	defer rows.Close()
	var out []driver.Recurring
	for rows.Next() {
		var (
			r                               driver.Recurring
			tmpl                            []byte
			misfire                         int16
			next, lastRun, created, updated stamp
		)
		err := rows.Scan(&r.ID, &r.Spec, &r.Location, &tmpl, &misfire, &r.Overlap, &r.Paused, &next, &lastRun,
			&r.LastJobID, &created, &updated, &r.Version)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(tmpl, &r.Template); err != nil {
			return nil, errors.Join(driver.ErrInvalid, err)
		}
		r.Misfire = driver.Misfire(misfire)
		r.NextRunAt, r.LastRunAt, r.CreatedAt, r.UpdatedAt = next.Time, lastRun.Time, created.Time, updated.Time
		out = append(out, r)
	}
	return out, rows.Err()
}
