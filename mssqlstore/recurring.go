package mssqlstore

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
	COALESCE(last_job_id, 0), created_at, updated_at, version, COALESCE(group_name, N'')`

const sqlDueRecurring = `SELECT TOP (@n) ` + recurringColumns + ` FROM {p}recurring
WHERE paused = 0 AND next_run_at <= @now
ORDER BY next_run_at`

const sqlRecurring = `SELECT ` + recurringColumns + ` FROM {p}recurring WHERE id = @id`

const sqlRecurrings = `SELECT ` + recurringColumns + ` FROM {p}recurring ORDER BY id`

const sqlFire = `UPDATE {p}recurring SET version = version + 1, next_run_at = @next, last_run_at = @last,
	updated_at = SYSUTCDATETIME()
WHERE id = @id AND version = @version`

const sqlFired = `UPDATE {p}recurring SET last_job_id = @job WHERE id = @id`

const sqlHasRecurring = `SELECT COUNT(*) FROM {p}recurring WHERE id = @id`

const sqlCreateRecurring = `INSERT INTO {p}recurring (id, spec, location, template, misfire, overlap, paused, next_run_at,
	last_run_at, last_job_id, group_name, created_at, updated_at, version)
VALUES (@id, @spec, @location, @template, @misfire, @overlap, @paused, @next, @last, @job, NULLIF(@group, N''),
	SYSUTCDATETIME(), SYSUTCDATETIME(), 1)`

const sqlUpdateRecurring = `UPDATE {p}recurring SET spec = @spec, location = @location, template = @template,
	misfire = @misfire, overlap = @overlap, paused = @paused, next_run_at = @next, last_run_at = @last,
	last_job_id = @job, group_name = NULLIF(@group, N''), updated_at = SYSUTCDATETIME(), version = version + 1
WHERE id = @id AND version = @version`

const sqlRemoveRecurring = `DELETE FROM {p}recurring WHERE id = @id`

// Due returns up to limit recurring jobs that are not paused and whose next run has come, the
// earliest first, with the database's time.
func (s *Store) Due(ctx context.Context, limit int) ([]driver.Recurring, time.Time, error) {
	now, err := s.Now(ctx)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("kiln: due: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, s.q.dueRecurring, sql.Named("now", stamp(now)), sql.Named("n", max(limit, 1)))
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("kiln: due: %w", err)
	}
	out, err := scanRecurrings(rows)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("kiln: due: %w", err)
	}
	return out, now, nil
}

// Fire applies f in one transaction, as [driver.Coordinator.Fire] describes. After the commit it
// admits the throttled jobs of the limit keys it used and, when the store has a bus, publishes the
// queues that received jobs to run.
func (s *Store) Fire(ctx context.Context, f driver.Fire) ([]driver.Inserted, error) {
	var (
		res []driver.Inserted
		wk  wake
	)
	err := s.txn(ctx, func(tx *sql.Tx) error {
		res, wk = nil, wake{}
		r, err := tx.ExecContext(ctx, s.q.fire, sql.Named("next", stamp(f.NextRunAt)), sql.Named("last", stamp(f.LastRunAt)),
			sql.Named("id", f.ID), sql.Named("version", f.Version))
		if err != nil {
			return wrap("fire", err)
		}
		if n, err := r.RowsAffected(); err != nil || n == 0 {
			var found int
			if err := tx.QueryRowContext(ctx, s.q.hasRecurring, sql.Named("id", f.ID)).Scan(&found); err != nil {
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
		if res, wk, err = s.insert(ctx, tx, f.Jobs); err != nil {
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
		if _, err := tx.ExecContext(ctx, s.q.fired, sql.Named("job", last), sql.Named("id", f.ID)); err != nil {
			return wrap("fire", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.admitLate(ctx, &wk)
	s.nt.ready(wk.queues)
	return res, nil
}

// Recurring returns the recurring job id, or an error wrapping [driver.ErrNotFound].
func (s *Store) Recurring(ctx context.Context, id string) (driver.Recurring, error) {
	rows, err := s.db.QueryContext(ctx, s.q.recurring, sql.Named("id", id))
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

// Recurrings returns every recurring job, ordered by id.
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

// PutRecurring creates r when r.Version is 0 and no recurring job has its ID, or replaces the
// stored one when r.Version equals its Version, and fails with [driver.ErrConflict] otherwise. r
// must have an ID and a Spec, or PutRecurring fails with [driver.ErrInvalid].
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
	args := []any{sql.Named("id", r.ID), sql.Named("spec", r.Spec), sql.Named("location", r.Location),
		sql.Named("template", string(tmpl)), sql.Named("misfire", int16(r.Misfire)), sql.Named("overlap", r.Overlap),
		sql.Named("paused", r.Paused), sql.Named("next", stamp(r.NextRunAt)), sql.Named("last", stamp(r.LastRunAt)),
		sql.Named("job", lastJob), sql.Named("group", r.Group), sql.Named("version", r.Version)}
	stmt := s.q.updateRecurring
	if r.Version == 0 {
		stmt = s.q.createRecurring
	}
	res, err := s.db.ExecContext(ctx, stmt, args...)
	if duplicate(err) {
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

// RemoveRecurring deletes the recurring job id, or fails with [driver.ErrNotFound]. Jobs it
// created stay as they are.
func (s *Store) RemoveRecurring(ctx context.Context, id string) error {
	r, err := s.db.ExecContext(ctx, s.q.removeRecurring, sql.Named("id", id))
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
			next, lastRun, created, updated moment
		)
		err := rows.Scan(&r.ID, &r.Spec, &r.Location, &tmpl, &misfire, &r.Overlap, &r.Paused, &next, &lastRun,
			&r.LastJobID, &created, &updated, &r.Version, &r.Group)
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
