package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const jobColumns = `id, claim, kind, queue, args, meta, tags, priority, attempt, max_attempts, timeout_ms, run_at,
	created_at, attempted_at, COALESCE(batch_id, 0), COALESCE(recurring_id, ''), parents, COALESCE(limit_key, '')`

const claimWhere = ` FROM {p}jobs FORCE INDEX (jobs_fetch)
WHERE state = 'enqueued' AND queue = ? AND NOT EXISTS (SELECT 1 FROM {p}queues p WHERE p.name = ? AND p.paused)`

const claimOrder = `
ORDER BY priority DESC, id LIMIT ? FOR UPDATE SKIP LOCKED`

const sqlClaim = `SELECT UTC_TIMESTAMP(6), ` + jobColumns + claimWhere + claimOrder

const sqlClaimKinds = `SELECT UTC_TIMESTAMP(6), ` + jobColumns + claimWhere + ` AND kind IN (?)` + claimOrder

const sqlTake = `UPDATE {p}jobs SET state = 'processing', attempt = attempt + 1, claim = claim + 1, attempted_at = ?,
	server = ?, cancel_requested = FALSE WHERE id IN (?)`

func (s *Store) Claim(ctx context.Context, q driver.ClaimQuery) ([]driver.Job, error) {
	if q.Limit <= 0 || len(q.Queues) == 0 {
		return nil, nil
	}
	queues := q.Queues
	if len(queues) > 1 {
		queues = uniq(queues)
	}
	var (
		jobs []driver.Job
		now  time.Time
	)
	err := s.txn(ctx, func(tx *sql.Tx) error {
		jobs, now = jobs[:0], time.Time{}
		for _, queue := range queues {
			n := q.Limit - len(jobs)
			if n <= 0 {
				break
			}
			var stmt string
			if len(q.Kinds) > 0 {
				stmt = render(s.q.claimKinds, queue, queue, q.Kinds, n)
			} else {
				stmt = render(s.q.claim, queue, queue, n)
			}
			var (
				at  time.Time
				err error
			)
			if jobs, at, err = scanClaimed(ctx, tx, stmt, jobs); err != nil {
				return err
			}
			if now.IsZero() {
				now = at
			}
		}
		if len(jobs) == 0 {
			return nil
		}
		ids := make([]int64, len(jobs))
		for i := range jobs {
			ids[i] = jobs[i].ID
		}
		slices.Sort(ids)
		_, err := tx.ExecContext(ctx, render(s.q.take, now, q.Server, ids))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("kiln: claim: %w", err)
	}
	for i := range jobs {
		j := &jobs[i]
		j.Attempt++
		j.Claim++
		j.AttemptedAt = now
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

func scanClaimed(ctx context.Context, tx *sql.Tx, stmt string, jobs []driver.Job) ([]driver.Job, time.Time, error) {
	var now stamp
	rows, err := tx.QueryContext(ctx, stmt)
	if err != nil {
		return jobs, now.Time, err
	}
	defer rows.Close()
	var (
		meta, tags, parents     sql.RawBytes
		ms                      int64
		run, created, attempted stamp
	)
	for rows.Next() {
		var j driver.Job
		err := rows.Scan(&now, &j.ID, &j.Claim, &j.Kind, &j.Queue, &j.Args, &meta, &tags, &j.Priority, &j.Attempt,
			&j.MaxAttempts, &ms, &run, &created, &attempted, &j.BatchID, &j.RecurringID, &parents, &j.LimitKey)
		if err != nil {
			return jobs, now.Time, err
		}
		j.Meta = decodeMeta(meta)
		j.Tags = decodeStrings(tags)
		j.Parents = decodeIDs(parents)
		j.Timeout = timeout(ms)
		j.RunAt, j.CreatedAt, j.AttemptedAt = run.Time, created.Time, attempted.Time
		jobs = append(jobs, j)
	}
	return jobs, now.Time, rows.Err()
}
