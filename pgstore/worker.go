package pgstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rafaelaugustos/kiln/driver"
)

const sqlHeartbeat = `INSERT INTO {s}.servers AS s (id, host, pid, version, queues, kinds, workers, running, started_at, heartbeat_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, coalesce($9, now()), now())
ON CONFLICT (id) DO UPDATE SET host = excluded.host, pid = excluded.pid, version = excluded.version,
	queues = excluded.queues, kinds = excluded.kinds, workers = excluded.workers, running = excluded.running,
	started_at = coalesce($9, s.started_at), heartbeat_at = excluded.heartbeat_at`

const sqlLeases = `SELECT id, claim, cancel_requested, (extract(epoch FROM now() - attempted_at) * 1000000)::bigint
FROM {s}.jobs WHERE state = 'processing' AND server = $1`

const sqlPaused = `SELECT name FROM {s}.queues WHERE paused ORDER BY name`

const sqlUnregister = `DELETE FROM {s}.servers WHERE id = $1`

const sqlSetMeta = `UPDATE {s}.jobs SET meta = coalesce(meta, '{}') || $3::jsonb
WHERE id = $1 AND claim = $2 AND state = 'processing'`

func (s *Store) Heartbeat(ctx context.Context, si driver.ServerInfo) (driver.Directives, error) {
	var d driver.Directives
	started := pgtype.Timestamptz{Time: si.StartedAt, Valid: !si.StartedAt.IsZero()}
	b := &pgx.Batch{}
	b.Queue(s.q.heartbeat, si.ID, si.Host, si.PID, si.Version, si.Queues, si.Kinds, si.Workers, si.Running, started)
	b.Queue(s.q.leases, si.ID).Query(func(rows pgx.Rows) error {
		for rows.Next() {
			var (
				l   driver.Lease
				age int64
			)
			if err := rows.Scan(&l.ID, &l.Claim, &l.Cancel, &age); err != nil {
				return err
			}
			l.Age = time.Duration(age) * time.Microsecond
			d.Leases = append(d.Leases, l)
		}
		return rows.Err()
	})
	b.Queue(s.q.paused).Query(func(rows pgx.Rows) error {
		for rows.Next() {
			var q string
			if err := rows.Scan(&q); err != nil {
				return err
			}
			d.Paused = append(d.Paused, q)
		}
		return rows.Err()
	})
	if err := s.pool.SendBatch(ctx, b).Close(); err != nil {
		return driver.Directives{}, fmt.Errorf("kiln: heartbeat: %w", err)
	}
	return d, nil
}

func (s *Store) Unregister(ctx context.Context, server string) error {
	if _, err := s.pool.Exec(ctx, s.q.unregister, server); err != nil {
		return fmt.Errorf("kiln: unregister: %w", err)
	}
	return nil
}

func (s *Store) SetMeta(ctx context.Context, ref driver.Ref, meta map[string]string) error {
	m := encodeMeta(meta)
	if m == "" {
		m = "{}"
	}
	tag, err := s.pool.Exec(ctx, s.q.setMeta, ref.ID, ref.Claim, m)
	if err != nil {
		return wrap("set meta", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: job %d claim %d", driver.ErrLost, ref.ID, ref.Claim)
	}
	return nil
}
