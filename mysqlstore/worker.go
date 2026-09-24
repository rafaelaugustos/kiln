package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const sqlHeartbeat = `INSERT INTO {p}servers (id, host, pid, version, queues, kinds, workers, running, started_at, heartbeat_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, UTC_TIMESTAMP(6)), UTC_TIMESTAMP(6)) AS n
ON DUPLICATE KEY UPDATE host = n.host, pid = n.pid, version = n.version, queues = n.queues, kinds = n.kinds,
	workers = n.workers, running = n.running, started_at = COALESCE(?, {p}servers.started_at),
	heartbeat_at = n.heartbeat_at`

const sqlDirectives = `SELECT id, claim, cancel_requested, TIMESTAMPDIFF(MICROSECOND, attempted_at, UTC_TIMESTAMP(6)), NULL
FROM {p}jobs WHERE state = 'processing' AND server = ?
UNION ALL
SELECT 0, 0, FALSE, 0, name FROM {p}queues WHERE paused`

const sqlUnregister = `DELETE FROM {p}servers WHERE id = ?`

const sqlSetMeta = `UPDATE {p}jobs SET meta = JSON_MERGE_PATCH(COALESCE(meta, JSON_OBJECT()), ?)
WHERE id = ? AND claim = ? AND state = 'processing'`

const sqlHeld = `SELECT 1 FROM {p}jobs WHERE id = ? AND claim = ? AND state = 'processing'`

func (s *Store) Heartbeat(ctx context.Context, si driver.ServerInfo) (driver.Directives, error) {
	stmt := render(s.q.heartbeat, si.ID, si.Host, si.PID, si.Version, encodeStrings(si.Queues), encodeStrings(si.Kinds),
		si.Workers, si.Running, si.StartedAt, si.StartedAt)
	if _, err := s.db.ExecContext(ctx, stmt); err != nil {
		return driver.Directives{}, wrap("heartbeat", err)
	}
	rows, err := s.db.QueryContext(ctx, render(s.q.directives, si.ID))
	if err != nil {
		return driver.Directives{}, wrap("heartbeat", err)
	}
	defer rows.Close()
	var d driver.Directives
	for rows.Next() {
		var (
			l     driver.Lease
			age   int64
			queue sql.NullString
		)
		if err := rows.Scan(&l.ID, &l.Claim, &l.Cancel, &age, &queue); err != nil {
			return driver.Directives{}, wrap("heartbeat", err)
		}
		if queue.Valid {
			d.Paused = append(d.Paused, queue.String)
			continue
		}
		l.Age = time.Duration(age) * time.Microsecond
		d.Leases = append(d.Leases, l)
	}
	if err := rows.Err(); err != nil {
		return driver.Directives{}, wrap("heartbeat", err)
	}
	slices.Sort(d.Paused)
	return d, nil
}

func (s *Store) Unregister(ctx context.Context, server string) error {
	if _, err := s.db.ExecContext(ctx, render(s.q.unregister, server)); err != nil {
		return wrap("unregister", err)
	}
	return nil
}

func (s *Store) SetMeta(ctx context.Context, ref driver.Ref, meta map[string]string) error {
	m := encodeMeta(meta)
	if m == nil {
		m = jsonText("{}")
	}
	r, err := s.db.ExecContext(ctx, render(s.q.setMeta, m, ref.ID, ref.Claim))
	if err != nil {
		return wrap("set meta", err)
	}
	if n, err := r.RowsAffected(); err == nil && n > 0 {
		return nil
	}
	var one int
	err = s.db.QueryRowContext(ctx, render(s.q.held, ref.ID, ref.Claim)).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: job %d claim %d", driver.ErrLost, ref.ID, ref.Claim)
	case err != nil:
		return wrap("set meta", err)
	}
	return nil
}
