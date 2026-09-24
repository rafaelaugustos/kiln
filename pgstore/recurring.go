package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rafaelaugustos/kiln/driver"
)

const recurringColumns = `id, spec, location, template, misfire, overlap, paused, next_run_at, last_run_at,
	coalesce(last_job_id, 0), created_at, updated_at, version`

const sqlDue = `SELECT ` + recurringColumns + ` FROM {s}.recurring
WHERE NOT paused AND next_run_at <= now()
ORDER BY next_run_at
LIMIT $1`

const sqlRecurring = `SELECT ` + recurringColumns + ` FROM {s}.recurring WHERE id = $1`

const sqlRecurrings = `SELECT ` + recurringColumns + ` FROM {s}.recurring ORDER BY id`

const sqlFire = `UPDATE {s}.recurring SET version = version + 1, next_run_at = $3, last_run_at = $4, updated_at = now()
WHERE id = $1 AND version = $2`

const sqlFired = `UPDATE {s}.recurring SET last_job_id = $2 WHERE id = $1`

const sqlHasRecurring = `SELECT EXISTS (SELECT 1 FROM {s}.recurring WHERE id = $1)`

const sqlCreateRecurring = `INSERT INTO {s}.recurring (id, spec, location, template, misfire, overlap, paused, next_run_at,
	last_run_at, last_job_id, created_at, updated_at, version)
VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, nullif($10::bigint, 0), now(), now(), 1)
ON CONFLICT (id) DO NOTHING`

const sqlUpdateRecurring = `UPDATE {s}.recurring SET spec = $2, location = $3, template = $4::jsonb, misfire = $5, overlap = $6,
	paused = $7, next_run_at = $8, last_run_at = $9, last_job_id = nullif($10::bigint, 0), updated_at = now(),
	version = version + 1
WHERE id = $1 AND version = $11`

const sqlRemoveRecurring = `DELETE FROM {s}.recurring WHERE id = $1`

func (s *Store) Due(ctx context.Context, limit int) ([]driver.Recurring, time.Time, error) {
	var (
		now time.Time
		out []driver.Recurring
	)
	b := &pgx.Batch{}
	b.Queue(s.q.now).QueryRow(func(row pgx.Row) error { return row.Scan(&now) })
	b.Queue(s.q.due, max(limit, 1)).Query(func(rows pgx.Rows) error {
		var err error
		out, err = scanRecurrings(rows)
		return err
	})
	if err := s.pool.SendBatch(ctx, b).Close(); err != nil {
		return nil, time.Time{}, fmt.Errorf("kiln: due: %w", err)
	}
	return out, now, nil
}

func (s *Store) Fire(ctx context.Context, f driver.Fire) ([]driver.Inserted, error) {
	var (
		res []driver.Inserted
		w   wake
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, s.q.fire, f.ID, f.Version, stamp(f.NextRunAt), stamp(f.LastRunAt))
		if err != nil {
			return wrap("fire", err)
		}
		if tag.RowsAffected() == 0 {
			var found bool
			if err := tx.QueryRow(ctx, s.q.hasRecurring, f.ID).Scan(&found); err != nil {
				return wrap("fire", err)
			}
			if !found {
				return fmt.Errorf("%w: recurring %s", driver.ErrNotFound, f.ID)
			}
			return fmt.Errorf("%w: recurring %s version %d", driver.ErrConflict, f.ID, f.Version)
		}
		if len(f.Jobs) == 0 {
			return nil
		}
		res, w, err = s.insert(ctx, tx, f.Jobs)
		if err != nil {
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
		if _, err := tx.Exec(ctx, s.q.fired, f.ID, last); err != nil {
			return wrap("fire", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(w.keys) > 0 {
		s.admit(ctx, w.rules, &w)
	}
	s.nt.jobs(w.queues...)
	return res, nil
}

func (s *Store) Recurring(ctx context.Context, id string) (driver.Recurring, error) {
	rows, err := s.pool.Query(ctx, s.q.recurring, id)
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
	rows, err := s.pool.Query(ctx, s.q.recurrings)
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
	args := []any{r.ID, r.Spec, r.Location, string(tmpl), int16(r.Misfire), r.Overlap, r.Paused,
		stamp(r.NextRunAt), stamp(r.LastRunAt), r.LastJobID}
	q := s.q.createRecurring
	if r.Version != 0 {
		q = s.q.updateRecurring
		args = append(args, r.Version)
	}
	tag, err := s.pool.Exec(ctx, q, args...)
	if err != nil {
		return wrap("put recurring", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: recurring %s version %d", driver.ErrConflict, r.ID, r.Version)
	}
	return nil
}

func (s *Store) RemoveRecurring(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, s.q.removeRecurring, id)
	if err != nil {
		return fmt.Errorf("kiln: remove recurring: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: recurring %s", driver.ErrNotFound, id)
	}
	return nil
}

func scanRecurrings(rows pgx.Rows) ([]driver.Recurring, error) {
	defer rows.Close()
	var out []driver.Recurring
	for rows.Next() {
		var (
			r             driver.Recurring
			tmpl          []byte
			misfire       int16
			next, lastRun pgtype.Timestamptz
		)
		err := rows.Scan(&r.ID, &r.Spec, &r.Location, &tmpl, &misfire, &r.Overlap, &r.Paused, &next, &lastRun,
			&r.LastJobID, &r.CreatedAt, &r.UpdatedAt, &r.Version)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(tmpl, &r.Template); err != nil {
			return nil, errors.Join(driver.ErrInvalid, err)
		}
		r.Misfire = driver.Misfire(misfire)
		r.NextRunAt, r.LastRunAt = next.Time, lastRun.Time
		out = append(out, r)
	}
	return out, rows.Err()
}

func stamp(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: !t.IsZero()}
}
