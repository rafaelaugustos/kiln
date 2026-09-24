package sqlitestore

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
ORDER BY next_run_at, id
LIMIT ?`

const sqlRecurring = `SELECT ` + recurringColumns + ` FROM {p}recurring WHERE id = ?`

const sqlRecurrings = `SELECT ` + recurringColumns + ` FROM {p}recurring ORDER BY id`

const sqlFire = `UPDATE {p}recurring SET version = version + 1, next_run_at = ?, last_run_at = ?, updated_at = {now}
WHERE id = ? AND version = ?`

const sqlFired = `UPDATE {p}recurring SET last_job_id = ? WHERE id = ?`

const sqlHasRecurring = `SELECT count(*) FROM {p}recurring WHERE id = ?`

const sqlCreateRecurring = `INSERT INTO {p}recurring (id, spec, location, template, misfire, overlap, paused,
	next_run_at, last_run_at, last_job_id, created_at, updated_at, version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {now}, {now}, 1)
ON CONFLICT (id) DO NOTHING`

const sqlUpdateRecurring = `UPDATE {p}recurring SET spec = ?, location = ?, template = ?, misfire = ?, overlap = ?,
	paused = ?, next_run_at = ?, last_run_at = ?, last_job_id = ?, updated_at = {now}, version = version + 1
WHERE id = ? AND version = ?`

const sqlRemoveRecurring = `DELETE FROM {p}recurring WHERE id = ?`

func (s *Store) Due(ctx context.Context, limit int) ([]driver.Recurring, time.Time, error) {
	now, err := s.Now(ctx)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("kiln: due: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, s.q.dueRecurring, now.UnixMicro(), max(limit, 1))
	out, err := scanRecurrings(rows, err)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("kiln: due: %w", err)
	}
	return out, now, nil
}

func (s *Store) Fire(ctx context.Context, f driver.Fire) ([]driver.Inserted, error) {
	if err := driver.CheckInsert(f.Jobs); err != nil {
		return nil, err
	}
	var in *inserter
	if len(f.Jobs) > 0 {
		in = s.plan(f.Jobs)
	}
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		r, err := q.ExecContext(ctx, s.q.fire, instant(f.NextRunAt), instant(f.LastRunAt), f.ID, f.Version)
		if err != nil {
			return err
		}
		if n, err := r.RowsAffected(); err != nil || n == 0 {
			var found int
			if err := q.QueryRowContext(ctx, s.q.hasRecurring, f.ID).Scan(&found); err != nil {
				return err
			}
			if found == 0 {
				return fmt.Errorf("%w: recurring %s", driver.ErrNotFound, f.ID)
			}
			return fmt.Errorf("%w: recurring %s version %d", driver.ErrConflict, f.ID, f.Version)
		}
		if in == nil {
			return nil
		}
		if err := in.write(ctx, q); err != nil {
			return err
		}
		var last int64
		for _, r := range in.res {
			if !r.Duplicate {
				last = r.ID
			}
		}
		if last == 0 {
			return nil
		}
		_, err = q.ExecContext(ctx, s.q.fired, last, f.ID)
		return err
	})
	if err != nil {
		return nil, wrap("fire", err)
	}
	if in == nil {
		return nil, nil
	}
	s.hub.ready(in.f.queues)
	return in.res, nil
}

func (s *Store) Recurring(ctx context.Context, id string) (driver.Recurring, error) {
	rows, err := s.db.QueryContext(ctx, s.q.recurring, id)
	rs, err := scanRecurrings(rows, err)
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
	rs, err := scanRecurrings(rows, err)
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
	var n int64
	err = s.write(ctx, func(ctx context.Context, q querier) error {
		var (
			res sql.Result
			err error
		)
		if r.Version == 0 {
			res, err = q.ExecContext(ctx, s.q.createRecurring, r.ID, r.Spec, r.Location, string(tmpl), int16(r.Misfire),
				r.Overlap, r.Paused, instant(r.NextRunAt), instant(r.LastRunAt), lastJob)
		} else {
			res, err = q.ExecContext(ctx, s.q.updateRecurring, r.Spec, r.Location, string(tmpl), int16(r.Misfire),
				r.Overlap, r.Paused, instant(r.NextRunAt), instant(r.LastRunAt), lastJob, r.ID, r.Version)
		}
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	switch {
	case err != nil:
		return wrap("put recurring", err)
	case n == 0:
		return fmt.Errorf("%w: recurring %s version %d", driver.ErrConflict, r.ID, r.Version)
	}
	return nil
}

func (s *Store) RemoveRecurring(ctx context.Context, id string) error {
	var n int64
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		r, err := q.ExecContext(ctx, s.q.removeRecurring, id)
		if err != nil {
			return err
		}
		n, err = r.RowsAffected()
		return err
	})
	switch {
	case err != nil:
		return wrap("remove recurring", err)
	case n == 0:
		return fmt.Errorf("%w: recurring %s", driver.ErrNotFound, id)
	}
	return nil
}

func scanRecurrings(rows *sql.Rows, err error) ([]driver.Recurring, error) {
	var out []driver.Recurring
	err = each(rows, err, func() error {
		var (
			r                               driver.Recurring
			tmpl                            []byte
			misfire                         int16
			next, lastRun, created, updated stamp
		)
		err := rows.Scan(&r.ID, &r.Spec, &r.Location, &tmpl, &misfire, &r.Overlap, &r.Paused, &next, &lastRun,
			&r.LastJobID, &created, &updated, &r.Version)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(tmpl, &r.Template); err != nil {
			return errors.Join(driver.ErrInvalid, err)
		}
		r.Misfire = driver.Misfire(misfire)
		r.NextRunAt, r.LastRunAt, r.CreatedAt, r.UpdatedAt = next.Time, lastRun.Time, created.Time, updated.Time
		out = append(out, r)
		return nil
	})
	return out, err
}
