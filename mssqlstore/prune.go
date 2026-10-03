package mssqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var totals = time.Date(1000, time.January, 1, 0, 0, 0, 0, time.UTC)

const cutoff = `DATEADD(MICROSECOND, -(@us % 1000000), DATEADD(SECOND, -(@us / 1000000), SYSUTCDATETIME()))`

const sqlPruneArchive = `DECLARE @gone TABLE (id BIGINT PRIMARY KEY);
WITH x AS (
	SELECT TOP (@n) id FROM {p}archive WITH (UPDLOCK, READPAST, ROWLOCK, INDEX(archive_state))
	WHERE state = CAST(@state AS VARCHAR(10)) AND finalized_at < ` + cutoff + `
	ORDER BY finalized_at
)
DELETE FROM x OUTPUT deleted.id INTO @gone;
DELETE d FROM @gone g JOIN {p}deps d WITH (FORCESEEK) ON d.job_id = g.id;
DELETE l FROM @gone g JOIN {p}logs l WITH (FORCESEEK) ON l.job_id = g.id;
SELECT COUNT(*) FROM @gone`

const sqlExpiredFailed = `SELECT TOP (@n) CAST(SYSUTCDATETIME() AS DATETIME2(6)), j.id, COALESCE(j.batch_id, 0),
	CASE WHEN EXISTS (SELECT 1 FROM {p}deps d WHERE d.batch = 0 AND d.parent_id = j.id AND d.resolved = 0) THEN 1 ELSE 0 END
FROM {p}jobs j WITH (XLOCK, READPAST, ROWLOCK, INDEX(jobs_failed))
WHERE j.state = 'failed' AND j.finalized_at < ` + cutoff + `
ORDER BY j.finalized_at`

const sqlDropFailed = `DELETE j FROM OPENJSON(@ids) WITH (id BIGINT '$') v JOIN {p}jobs j WITH (FORCESEEK) ON j.id = v.id;
DELETE d FROM OPENJSON(@ids) WITH (id BIGINT '$') v JOIN {p}deps d WITH (FORCESEEK) ON d.job_id = v.id;
DELETE l FROM OPENJSON(@ids) WITH (id BIGINT '$') v JOIN {p}logs l WITH (FORCESEEK) ON l.job_id = v.id;`

const sqlPruneServers = `DELETE TOP (@n) FROM {p}servers WHERE heartbeat_at < ` + cutoff

const sqlPruneStats = `DECLARE @gone TABLE (succeeded BIGINT, failed BIGINT, deleted BIGINT, retried BIGINT);
WITH x AS (
	SELECT TOP (@n) succeeded, failed, deleted, retried FROM {p}stats WITH (UPDLOCK, READPAST, ROWLOCK)
	WHERE bucket > @totals AND bucket < ` + cutoff + `
	ORDER BY bucket, server
)
DELETE FROM x OUTPUT deleted.succeeded, deleted.failed, deleted.deleted, deleted.retried INTO @gone;
IF @@ROWCOUNT > 0
MERGE {p}stats WITH (HOLDLOCK) AS s
USING (SELECT @totals AS bucket, N'' COLLATE Latin1_General_100_BIN2 AS server, SUM(succeeded) AS succeeded,
	SUM(failed) AS failed, SUM(deleted) AS deleted, SUM(retried) AS retried FROM @gone) AS n
ON s.bucket = n.bucket AND s.server = n.server
WHEN MATCHED THEN UPDATE SET succeeded = s.succeeded + n.succeeded, failed = s.failed + n.failed,
	deleted = s.deleted + n.deleted, retried = s.retried + n.retried
WHEN NOT MATCHED THEN INSERT (bucket, server, succeeded, failed, deleted, retried)
	VALUES (n.bucket, n.server, n.succeeded, n.failed, n.deleted, n.retried);
SELECT COUNT(*) FROM @gone`

const sqlPruneUniques = `DELETE TOP (@n) FROM {p}uniques WITH (READPAST) WHERE expires_at <= SYSUTCDATETIME()`

const holderGone = `u.expires_at IS NULL AND NOT EXISTS (
	SELECT 1 FROM {p}jobs j WHERE j.id = u.job_id AND j.state <> 'failed')`

const sqlHolderPage = `SELECT TOP (@n) u.unique_key, CASE WHEN ` + holderGone + ` THEN 1 ELSE 0 END
FROM {p}uniques u WHERE u.unique_key > @after ORDER BY u.unique_key`

const sqlDropHolders = `DELETE u FROM OPENJSON(@keys) WITH (k VARCHAR(130) '$') v
JOIN {p}uniques u WITH (READPAST, ROWLOCK, FORCESEEK) ON u.unique_key = CONVERT(VARBINARY(64), v.k, 2)
WHERE ` + holderGone

const sqlPruneLimits = `DELETE TOP (@n) l FROM {p}limits l WITH (READPAST)
WHERE l.active = 0 AND (l.declared_at IS NULL OR l.declared_at < DATEADD(HOUR, -1, SYSUTCDATETIME()))
	AND (l.tat IS NULL OR l.tat <= SYSUTCDATETIME()) AND (l.admit_tat IS NULL OR l.admit_tat <= SYSUTCDATETIME())
	AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.limit_key = l.limit_key)
	AND NOT EXISTS (SELECT 1 FROM {p}archive a WHERE a.limit_key = l.limit_key)`

const sqlPruneBatches = `DELETE TOP (@n) b FROM {p}batches b WITH (READPAST)
WHERE b.finished_at IS NOT NULL
	AND NOT EXISTS (SELECT 1 FROM {p}jobs j WHERE j.batch_id = b.id)
	AND NOT EXISTS (SELECT 1 FROM {p}archive a WHERE a.batch_id = b.id)
	AND NOT EXISTS (SELECT 1 FROM {p}batches n WHERE n.parent_id = b.id)`

// Prune deletes old rows as [driver.Coordinator.Prune] describes, in a short transaction for each
// kind of row. A failing step does not stop the others unless ctx is done; the error Prune returns
// joins those of every step that failed, and the count leaves their rows out.
func (s *Store) Prune(ctx context.Context, p driver.PruneParams) (int, error) {
	limit := p.Limit
	if limit <= 0 {
		limit = 1000
	}
	servers := p.Servers
	if servers <= 0 {
		servers = time.Hour
	}
	stats := p.Stats
	if stats <= 0 {
		stats = 14 * 24 * time.Hour
	}
	n := sql.Named("n", limit)
	var steps []func() (int, error)
	if p.Succeeded >= 0 {
		steps = append(steps, func() (int, error) { return s.pruneArchive(ctx, driver.Succeeded, p.Succeeded, limit) })
	}
	if p.Deleted >= 0 {
		steps = append(steps, func() (int, error) { return s.pruneArchive(ctx, driver.Deleted, p.Deleted, limit) })
	}
	if p.Failed >= 0 {
		steps = append(steps, func() (int, error) { return s.pruneFailed(ctx, p.Failed, limit) })
	}
	steps = append(steps,
		func() (int, error) { return s.affected(ctx, s.q.pruneServers, n, sql.Named("us", micros(servers))) },
		func() (int, error) { return s.pruneStats(ctx, stats, limit) },
		func() (int, error) { return s.affected(ctx, s.q.pruneUniques, n) },
		func() (int, error) { return s.pruneHolders(ctx, limit) },
		func() (int, error) { return s.affected(ctx, s.q.pruneLimits, n) },
		func() (int, error) { return s.affected(ctx, s.q.pruneBatches, n) },
	)
	total := 0
	var errs []error
	for _, step := range steps {
		n, err := step()
		if err == nil {
			total += n
			continue
		}
		errs = append(errs, err)
		if ctx.Err() != nil {
			break
		}
	}
	if len(errs) > 0 {
		return total, fmt.Errorf("kiln: prune: %w", errors.Join(errs...))
	}
	return total, nil
}

func (s *Store) affected(ctx context.Context, stmt string, args ...any) (int, error) {
	n := 0
	err := s.retry(ctx, func() error {
		r, err := s.db.ExecContext(ctx, stmt, args...)
		if err != nil {
			return err
		}
		affected, err := r.RowsAffected()
		n = int(affected)
		return err
	})
	return n, err
}

func (s *Store) count(ctx context.Context, stmt string, args ...any) (int, error) {
	n := 0
	err := s.txn(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, stmt, args...).Scan(&n)
	})
	return n, err
}

func (s *Store) pruneArchive(ctx context.Context, state driver.State, keep time.Duration, limit int) (int, error) {
	return s.count(ctx, s.q.pruneArchive, sql.Named("n", limit), sql.Named("state", string(state)),
		sql.Named("us", micros(keep)))
}

func (s *Store) pruneStats(ctx context.Context, keep time.Duration, limit int) (int, error) {
	return s.count(ctx, s.q.pruneStats, sql.Named("n", limit), sql.Named("totals", stamp(totals)),
		sql.Named("us", micros(keep)))
}

func (s *Store) pruneFailed(ctx context.Context, keep time.Duration, limit int) (int, error) {
	var (
		n int
		f *fallout
	)
	err := s.txn(ctx, func(tx *sql.Tx) error {
		n, f = 0, &fallout{}
		rows, err := tx.QueryContext(ctx, s.q.expiredFailed, sql.Named("n", limit), sql.Named("us", micros(keep)))
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var (
				now      moment
				id       int64
				batch    int64
				children bool
			)
			if err := rows.Scan(&now, &id, &batch, &children); err != nil {
				rows.Close()
				return err
			}
			f.now = now.Time
			ids = append(ids, id)
			f.batch(batch)
			if children {
				f.parents = append(f.parents, parent{id: id, state: driver.Deleted, label: "pruned"})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil || len(ids) == 0 {
			return err
		}
		if _, err := tx.ExecContext(ctx, s.q.dropFailed, sql.Named("ids", idList(ids))); err != nil {
			return err
		}
		n = len(ids)
		return s.settle(ctx, tx, f)
	})
	if err != nil {
		return 0, err
	}
	s.nt.ready(f.queues)
	return n, nil
}

func (s *Store) pruneHolders(ctx context.Context, limit int) (int, error) {
	s.mu.Lock()
	after := s.cursors.holder
	s.mu.Unlock()
	if after == nil {
		after = []byte{}
	}
	rows, err := s.db.QueryContext(ctx, s.q.holderPage, sql.Named("n", limit), sql.Named("after", after))
	if err != nil {
		return 0, err
	}
	var (
		seen int
		last []byte
		dead [][]byte
	)
	for rows.Next() {
		var (
			key  []byte
			gone bool
		)
		if err := rows.Scan(&key, &gone); err != nil {
			rows.Close()
			return 0, err
		}
		seen++
		last = key
		if gone {
			dead = append(dead, key)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if seen < limit {
		last = nil
	}
	s.mu.Lock()
	s.cursors.holder = last
	s.mu.Unlock()
	if dead == nil {
		return 0, nil
	}
	return s.affected(ctx, s.q.dropHolders, sql.Named("keys", keyList(dead)))
}
