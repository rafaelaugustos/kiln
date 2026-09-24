package pgstore

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rafaelaugustos/kiln/driver"
)

const jobColumns = `j.id, j.claim, j.kind, j.queue, j.args, j.meta, array_remove(j.tags, NULL), j.priority, j.attempt,
	j.max_attempts, j.timeout_ms, j.run_at, j.created_at, j.attempted_at, coalesce(j.batch_id, 0),
	coalesce(j.recurring_id, ''), array_remove(j.parents, NULL), coalesce(j.limit_key, '')`

type claimShape struct {
	queues int
	kinds  bool
}

func (s *Store) claimSQL(shape claimShape) string {
	if q, ok := s.claims.Load(shape); ok {
		return q.(string)
	}
	var b strings.Builder
	b.WriteString("WITH ")
	for i := 1; i <= shape.queues; i++ {
		if i > 1 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "q%d AS (SELECT id FROM %s.jobs WHERE state = 'enqueued' AND queue = ($1::text[])[%d]", i, s.schema, i)
		if shape.kinds {
			b.WriteString(" AND kind = ANY($4)")
		}
		fmt.Fprintf(&b, " AND NOT EXISTS (SELECT 1 FROM %s.queues p WHERE p.name = ($1::text[])[%d] AND p.paused)", s.schema, i)
		b.WriteString(" ORDER BY priority DESC, id LIMIT ")
		if i == 1 {
			b.WriteString("$2")
		} else {
			b.WriteString("greatest($2")
			for k := 1; k < i; k++ {
				fmt.Fprintf(&b, " - (SELECT count(*) FROM q%d)", k)
			}
			b.WriteString(", 0)")
		}
		b.WriteString(" FOR NO KEY UPDATE SKIP LOCKED)")
	}
	fmt.Fprintf(&b, `
UPDATE %s.jobs j SET state = 'processing', attempt = j.attempt + 1, claim = j.claim + 1, attempted_at = now(),
	server = $3, cancel_requested = false
WHERE j.id = ANY(ARRAY(`, s.schema)
	for i := 1; i <= shape.queues; i++ {
		if i > 1 {
			b.WriteString(" UNION ALL ")
		}
		fmt.Fprintf(&b, "SELECT id FROM q%d", i)
	}
	b.WriteString("))\nRETURNING ")
	b.WriteString(jobColumns)
	q, _ := s.claims.LoadOrStore(shape, b.String())
	return q.(string)
}

func (s *Store) Claim(ctx context.Context, q driver.ClaimQuery) ([]driver.Job, error) {
	if q.Limit <= 0 || len(q.Queues) == 0 {
		return nil, nil
	}
	queues := q.Queues
	if len(queues) > 1 {
		queues = uniq(queues)
	}
	args := []any{queues, q.Limit, q.Server}
	shape := claimShape{queues: len(queues)}
	if len(q.Kinds) > 0 {
		shape.kinds = true
		args = append(args, q.Kinds)
	}
	rows, err := s.pool.Query(ctx, s.claimSQL(shape), args...)
	if err != nil {
		return nil, fmt.Errorf("kiln: claim: %w", err)
	}
	jobs, err := scanJobs(rows, q.Limit)
	if err != nil {
		return jobs, fmt.Errorf("kiln: claim: %w", err)
	}
	if len(queues) > 1 {
		slices.SortFunc(jobs, func(a, b driver.Job) int {
			return cmp.Or(
				cmp.Compare(slices.Index(queues, a.Queue), slices.Index(queues, b.Queue)),
				cmp.Compare(b.Priority, a.Priority),
				cmp.Compare(a.ID, b.ID))
		})
	} else {
		slices.SortFunc(jobs, func(a, b driver.Job) int {
			return cmp.Or(cmp.Compare(b.Priority, a.Priority), cmp.Compare(a.ID, b.ID))
		})
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

func scanJobs(rows pgx.Rows, hint int) ([]driver.Job, error) {
	defer rows.Close()
	jobs := make([]driver.Job, 0, hint)
	var (
		meta                    []byte
		ms                      int64
		run, created, attempted pgtype.Timestamptz
	)
	for rows.Next() {
		var j driver.Job
		err := rows.Scan(&j.ID, &j.Claim, &j.Kind, &j.Queue, &j.Args, &meta, &j.Tags, &j.Priority, &j.Attempt,
			&j.MaxAttempts, &ms, &run, &created, &attempted, &j.BatchID, &j.RecurringID, &j.Parents, &j.LimitKey)
		if err != nil {
			return jobs, err
		}
		j.Meta = decodeMeta(meta)
		j.Timeout = timeout(ms)
		j.RunAt, j.CreatedAt, j.AttemptedAt = run.Time, created.Time, attempted.Time
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
