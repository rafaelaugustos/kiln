package sqlitestore

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

const jobColumns = `id, claim, kind, queue, args, meta, tags, priority, attempt, max_attempts, timeout_ms, run_at,
	created_at, attempted_at, COALESCE(batch_id, 0), COALESCE(recurring_id, ''), parents, COALESCE(limit_key, '')`

const claimHead = `UPDATE {p}jobs SET state = 'processing', attempt = attempt + 1, claim = claim + 1,
	attempted_at = {now}, server = ?, cancel_requested = 0
WHERE id IN (SELECT id FROM {p}jobs WHERE state = 'enqueued' AND queue = ?`

const claimTail = ` ORDER BY priority DESC, id LIMIT ?)
	AND NOT EXISTS (SELECT 1 FROM {p}queues WHERE name = ? AND paused)
RETURNING ` + jobColumns

const sqlClaim = claimHead + claimTail

const sqlClaimKinds = claimHead + ` AND kind IN (SELECT value FROM json_each(?))` + claimTail

func (s *Store) Claim(ctx context.Context, q driver.ClaimQuery) ([]driver.Job, error) {
	if q.Limit <= 0 || len(q.Queues) == 0 {
		return nil, nil
	}
	queues := q.Queues
	if len(queues) > 1 {
		queues = uniq(queues)
	}
	var kinds string
	if len(q.Kinds) > 0 {
		kinds = nameList(q.Kinds)
	}
	var jobs []driver.Job
	err := s.write(ctx, func(ctx context.Context, tx querier) error {
		jobs = jobs[:0]
		for _, queue := range queues {
			n := q.Limit - len(jobs)
			if n <= 0 {
				break
			}
			var (
				rows *sql.Rows
				err  error
			)
			if kinds == "" {
				rows, err = tx.QueryContext(ctx, s.q.claim, q.Server, queue, n, queue)
			} else {
				rows, err = tx.QueryContext(ctx, s.q.claimKinds, q.Server, queue, kinds, n, queue)
			}
			from := len(jobs)
			if jobs, err = scanJobs(rows, err, jobs); err != nil {
				return err
			}
			slices.SortFunc(jobs[from:], func(a, b driver.Job) int {
				return cmp.Or(cmp.Compare(b.Priority, a.Priority), cmp.Compare(a.ID, b.ID))
			})
		}
		return nil
	})
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

func scanJobs(rows *sql.Rows, err error, jobs []driver.Job) ([]driver.Job, error) {
	var (
		meta, tags, parents     sql.RawBytes
		ms                      int64
		run, created, attempted stamp
	)
	err = each(rows, err, func() error {
		var j driver.Job
		err := rows.Scan(&j.ID, &j.Claim, &j.Kind, &j.Queue, &j.Args, &meta, &tags, &j.Priority, &j.Attempt,
			&j.MaxAttempts, &ms, &run, &created, &attempted, &j.BatchID, &j.RecurringID, &parents, &j.LimitKey)
		if err != nil {
			return err
		}
		j.Meta = decodeMeta(meta)
		j.Tags = decodeStrings(tags)
		j.Parents = decodeIDs(parents)
		j.Timeout = timeout(ms)
		j.RunAt, j.CreatedAt, j.AttemptedAt = run.Time, created.Time, attempted.Time
		jobs = append(jobs, j)
		return nil
	})
	return jobs, err
}
