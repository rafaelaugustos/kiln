package sqlitestore

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlHeartbeat = `INSERT INTO {p}servers (id, host, pid, version, queues, kinds, workers, running, started_at,
	heartbeat_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, {now}), {now})
ON CONFLICT (id) DO UPDATE SET host = excluded.host, pid = excluded.pid, version = excluded.version,
	queues = excluded.queues, kinds = excluded.kinds, workers = excluded.workers, running = excluded.running,
	started_at = COALESCE(?, started_at), heartbeat_at = excluded.heartbeat_at`

const sqlDirectives = `SELECT id, claim, cancel_requested, {now} - attempted_at, NULL FROM {p}jobs
WHERE state = 'processing' AND server = ?
UNION ALL
SELECT 0, 0, 0, 0, name FROM {p}queues WHERE paused`

const sqlUnregister = `DELETE FROM {p}servers WHERE id = ?`

const sqlSetMeta = `UPDATE {p}jobs SET meta = json_patch(COALESCE(meta, '{}'), ?)
WHERE id = ? AND claim = ? AND state = 'processing'`

func (s *Store) Heartbeat(ctx context.Context, si driver.ServerInfo) (driver.Directives, error) {
	var d driver.Directives
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		d = driver.Directives{}
		started := instant(si.StartedAt)
		_, err := q.ExecContext(ctx, s.q.heartbeat, si.ID, si.Host, si.PID, si.Version, text(encodeStrings(si.Queues)),
			text(encodeStrings(si.Kinds)), si.Workers, si.Running, started, started)
		if err != nil {
			return err
		}
		rows, err := q.QueryContext(ctx, s.q.directives, si.ID)
		return each(rows, err, func() error {
			var (
				l     driver.Lease
				age   int64
				queue sql.NullString
			)
			if err := rows.Scan(&l.ID, &l.Claim, &l.Cancel, &age, &queue); err != nil {
				return err
			}
			if queue.Valid {
				d.Paused = append(d.Paused, queue.String)
				return nil
			}
			l.Age = time.Duration(age) * time.Microsecond
			d.Leases = append(d.Leases, l)
			return nil
		})
	})
	if err != nil {
		return driver.Directives{}, wrap("heartbeat", err)
	}
	slices.SortFunc(d.Leases, func(a, b driver.Lease) int { return cmp.Compare(a.ID, b.ID) })
	slices.Sort(d.Paused)
	return d, nil
}

func (s *Store) Unregister(ctx context.Context, server string) error {
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		_, err := q.ExecContext(ctx, s.q.unregister, server)
		return err
	})
	if err != nil {
		return wrap("unregister", err)
	}
	return nil
}

func (s *Store) SetMeta(ctx context.Context, ref driver.Ref, meta map[string]string) error {
	patch := "{}"
	if m := encodeMeta(meta); m != nil {
		patch = string(m)
	}
	var n int64
	err := s.write(ctx, func(ctx context.Context, q querier) error {
		r, err := q.ExecContext(ctx, s.q.setMeta, patch, ref.ID, ref.Claim)
		if err != nil {
			return err
		}
		n, err = r.RowsAffected()
		return err
	})
	switch {
	case err != nil:
		return wrap("set meta", err)
	case n == 0:
		return fmt.Errorf("%w: job %d claim %d", driver.ErrLost, ref.ID, ref.Claim)
	}
	return nil
}
