package pgstore

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/internal/outbox"
)

var _ driver.Outbox = (*Outbox)(nil)

const sqlOutboxSchema = `CREATE SCHEMA IF NOT EXISTS {s}`

const sqlOutboxTable = `CREATE TABLE IF NOT EXISTS {s}.outbox (
	seq bigserial PRIMARY KEY,
	id text NOT NULL UNIQUE,
	data bytea NOT NULL,
	created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
	failed text
)`

const sqlOutboxPut = `INSERT INTO {s}.outbox (id, data) VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`

const sqlOutboxOne = `SELECT data FROM {s}.outbox WHERE id = $1 AND failed IS NULL FOR UPDATE SKIP LOCKED`

const sqlOutboxNext = `SELECT id, data FROM {s}.outbox
WHERE failed IS NULL AND created_at < clock_timestamp() - interval '1 second'
ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED`

const sqlOutboxDone = `DELETE FROM {s}.outbox WHERE id = ANY($1)`

const sqlOutboxFail = `UPDATE {s}.outbox SET failed = $2 WHERE id = $1`

// Outbox is a [driver.Outbox] in PostgreSQL, for applications that keep their jobs in another
// store, such as redisstore, and still want to enqueue inside their database transactions. It is
// one small table, outbox, in the schema "kiln" unless [Schema] names another: a row holds the
// writes of one transaction from its commit until the store has applied them, which normally
// takes as long as the call to Notify.
//
//	store, err := redisstore.New(ctx, rdb)
//	ob, err := pgstore.NewOutbox(ctx, pool, store)
//	server, err := kiln.NewServer(kiln.NewClient(store), mux, kiln.ServerConfig{Outbox: ob})
//
//	tx, err := pool.Begin(ctx)
//	w := ob.Tx(tx)
//	_, err = client.EnqueueTx(ctx, w, SendEmail{To: "a@b.c"})
//	err = tx.Commit(ctx)
//	w.Notify(ctx)
//
// A row whose writes can never be applied keeps the reason in its failed column and stays in the
// table, where nothing touches it again.
type Outbox struct {
	pool   *pgxpool.Pool
	target driver.Stager
	q      outboxSQL
}

type outboxSQL struct {
	put, one, next, done, fail string
}

// NewOutbox returns an outbox in pool's database whose entries are staged for target and applied
// to it. It creates the schema and the table when they are missing, unless [NoMigrate] is given.
// Of the options, [Schema] and [NoMigrate] apply. The outbox uses pool as it is, and only for
// short transactions after a commit and in Drain.
func NewOutbox(ctx context.Context, pool *pgxpool.Pool, target driver.Stager, opts ...Option) (*Outbox, error) {
	c := newConfig(opts)
	if !validSchema(c.schema) {
		return nil, fmt.Errorf("%w: schema %q", driver.ErrInvalid, c.schema)
	}
	if target == nil {
		return nil, fmt.Errorf("%w: outbox without a store to stage for", driver.ErrInvalid)
	}
	r := strings.NewReplacer("{s}", c.schema)
	o := &Outbox{pool: pool, target: target, q: outboxSQL{
		put:  r.Replace(sqlOutboxPut),
		one:  r.Replace(sqlOutboxOne),
		next: r.Replace(sqlOutboxNext),
		done: r.Replace(sqlOutboxDone),
		fail: r.Replace(sqlOutboxFail),
	}}
	if c.noMigrate {
		return o, nil
	}
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext('kiln outbox'))"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, r.Replace(sqlOutboxSchema)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, r.Replace(sqlOutboxTable))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("kiln: outbox table: %w", err)
	}
	return o, nil
}

// Tx returns a writer bound to tx: its jobs and batches are staged in the outbox's store and
// stored in tx, so they commit or roll back with the application's work, and its Notify applies
// them after the commit. Each write is a call to the store and a statement in tx; a store that
// cannot be reached fails the write. A nil tx gives a writer whose writes fail with
// [driver.ErrNilTx].
func (o *Outbox) Tx(tx pgx.Tx) driver.TxWriter {
	if tx == nil {
		return outbox.Broken(driver.ErrNilTx)
	}
	return outbox.NewWriter(o.target, &outboxTx{o: o, exec: func(ctx context.Context, q string, args ...any) error {
		_, err := tx.Exec(ctx, q, args...)
		return err
	}})
}

// SQLTx is [Outbox.Tx] for an application on database/sql, with a [sql.DB] opened with pgx's
// stdlib driver.
func (o *Outbox) SQLTx(tx *sql.Tx) driver.TxWriter {
	if tx == nil {
		return outbox.Broken(driver.ErrNilTx)
	}
	return outbox.NewWriter(o.target, &outboxTx{o: o, exec: func(ctx context.Context, q string, args ...any) error {
		_, err := tx.ExecContext(ctx, q, args...)
		return err
	}})
}

type outboxTx struct {
	o    *Outbox
	exec func(ctx context.Context, q string, args ...any) error
}

func (t *outboxTx) Put(ctx context.Context, id string, data []byte) error {
	if err := t.exec(ctx, t.o.q.put, id, data); err != nil {
		return fmt.Errorf("kiln: outbox: %w", err)
	}
	return nil
}

func (t *outboxTx) Deliver(ctx context.Context, id string) error {
	var applyErr error
	err := pgx.BeginFunc(ctx, t.o.pool, func(tx pgx.Tx) error {
		var data []byte
		switch err := tx.QueryRow(ctx, t.o.q.one, id).Scan(&data); {
		case errors.Is(err, pgx.ErrNoRows):
			return nil
		case err != nil:
			return err
		}
		applyErr = t.o.target.Apply(ctx, id, data)
		switch {
		case applyErr == nil:
			_, err := tx.Exec(ctx, t.o.q.done, []string{id})
			return err
		case outbox.Permanent(applyErr):
			_, err := tx.Exec(ctx, t.o.q.fail, id, applyErr.Error())
			return err
		}
		return applyErr
	})
	if applyErr != nil {
		return applyErr
	}
	if err != nil {
		return fmt.Errorf("kiln: outbox: %w", err)
	}
	return nil
}

// Drain applies the writes of up to limit committed entries, oldest first, as [driver.Outbox]
// describes, in one transaction that locks the entries it takes and skips those another Drain or
// a Notify holds. Its error is the first one an entry met; the other entries are applied all the
// same.
func (o *Outbox) Drain(ctx context.Context, limit int) (int, error) {
	type entry struct {
		id   string
		data []byte
	}
	n := 0
	var first error
	err := pgx.BeginFunc(ctx, o.pool, func(tx pgx.Tx) error {
		rows, _ := tx.Query(ctx, o.q.next, max(limit, 1))
		entries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (entry, error) {
			var e entry
			err := row.Scan(&e.id, &e.data)
			return e, err
		})
		if err != nil {
			return err
		}
		var done []string
		for _, e := range entries {
			err := o.target.Apply(ctx, e.id, e.data)
			switch {
			case err == nil:
				done = append(done, e.id)
			case outbox.Permanent(err):
				if _, ferr := tx.Exec(ctx, o.q.fail, e.id, err.Error()); ferr != nil {
					return ferr
				}
				first = cmp.Or(first, fmt.Errorf("kiln: outbox entry %s set aside: %w", e.id, err))
			default:
				first = cmp.Or(first, err)
			}
		}
		if len(done) == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, o.q.done, done); err != nil {
			return err
		}
		n = len(done)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("kiln: outbox: %w", err)
	}
	return n, first
}
