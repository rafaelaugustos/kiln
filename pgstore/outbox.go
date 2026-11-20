package pgstore

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/internal/outbox"
)

var _ driver.Outbox = (*Outbox)(nil)

const sqlOutboxSchema = `CREATE SCHEMA IF NOT EXISTS {s}`

const sqlOutboxTable = `CREATE TABLE IF NOT EXISTS {s}.outbox (
	seq bigserial PRIMARY KEY,
	id text NOT NULL,
	data bytea NOT NULL,
	created_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`

const sqlOutboxFailed = `CREATE TABLE IF NOT EXISTS {s}.outbox_failed (
	seq bigint PRIMARY KEY,
	id text NOT NULL,
	data bytea NOT NULL,
	created_at timestamptz NOT NULL,
	failed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
	reason text NOT NULL
)`

const sqlOutboxAdd = `INSERT INTO {s}.outbox (id, data) VALUES ($1, $2) RETURNING seq`

const sqlOutboxSet = `UPDATE {s}.outbox SET data = $2 WHERE seq = $1`

const sqlOutboxLazy = `SET LOCAL synchronous_commit = off`

const sqlOutboxTake = `DELETE FROM {s}.outbox WHERE seq = $1 RETURNING id, data, created_at`

const sqlOutboxNext = `SELECT seq, id, data, created_at FROM {s}.outbox
WHERE created_at < clock_timestamp() - interval '1 second'
ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED`

const sqlOutboxDone = `DELETE FROM {s}.outbox WHERE seq = ANY($1)`

const sqlOutboxPark = `INSERT INTO {s}.outbox_failed (seq, id, data, created_at, reason) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (seq) DO NOTHING`

// Outbox is a [driver.Outbox] in PostgreSQL, for applications that keep their jobs in another
// store, such as redisstore, and still want to enqueue inside their database transactions. It is
// one small table, outbox, in the schema "kiln" unless [Schema] names another: a row holds the
// writes of one transaction from its commit until the store has applied them, which normally
// takes as long as the call to Notify. For the database that is one insert in the application's
// transaction and one delete after it.
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
// The row is deleted by a transaction that does not wait for its commit to reach the disk: if a
// crash loses the delete, the row is applied again, which does nothing. Writes that can never be
// applied move to the table outbox_failed with the reason, where nothing touches them again.
type Outbox struct {
	pool   *pgxpool.Pool
	target driver.Stager
	q      outboxSQL
}

type outboxSQL struct {
	add, set, take, next, done, park string
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
		add:  r.Replace(sqlOutboxAdd),
		set:  r.Replace(sqlOutboxSet),
		take: r.Replace(sqlOutboxTake),
		next: r.Replace(sqlOutboxNext),
		done: r.Replace(sqlOutboxDone),
		park: r.Replace(sqlOutboxPark),
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
		if _, err := tx.Exec(ctx, r.Replace(sqlOutboxTable)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, r.Replace(sqlOutboxFailed))
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
	return outbox.NewWriter(o.target, &outboxTx{o: o, tx: tx})
}

// SQLTx is [Outbox.Tx] for an application on database/sql, with a [sql.DB] opened with pgx's
// stdlib driver.
func (o *Outbox) SQLTx(tx *sql.Tx) driver.TxWriter {
	if tx == nil {
		return outbox.Broken(driver.ErrNilTx)
	}
	return outbox.NewWriter(o.target, &outboxTx{o: o, tx: stdRows{tx}})
}

type rower interface {
	QueryRow(ctx context.Context, q string, args ...any) pgx.Row
}

type stdRows struct{ tx *sql.Tx }

func (r stdRows) QueryRow(ctx context.Context, q string, args ...any) pgx.Row {
	return r.tx.QueryRowContext(ctx, q, args...)
}

type outboxTx struct {
	o   *Outbox
	tx  rower
	seq int64
}

func (t *outboxTx) Put(ctx context.Context, id string, data []byte) error {
	var err error
	if t.seq == 0 {
		err = t.tx.QueryRow(ctx, t.o.q.add, id, data).Scan(&t.seq)
	} else {
		var seq int64
		err = t.tx.QueryRow(ctx, t.o.q.set+" RETURNING seq", t.seq, data).Scan(&seq)
	}
	if err != nil {
		return fmt.Errorf("kiln: outbox: %w", err)
	}
	return nil
}

type outboxEntry struct {
	seq     int64
	id      string
	data    []byte
	created time.Time
}

func (t *outboxTx) Deliver(ctx context.Context) error {
	o := t.o
	e := outboxEntry{seq: t.seq}
	t.seq = 0
	if e.seq == 0 {
		return nil
	}
	var applyErr error
	err := pgx.BeginFunc(ctx, o.pool, func(tx pgx.Tx) error {
		b := &pgx.Batch{}
		b.Queue(sqlOutboxLazy)
		b.Queue(o.q.take, e.seq)
		br := tx.SendBatch(ctx, b)
		_, err := br.Exec()
		if err == nil {
			err = br.QueryRow().Scan(&e.id, &e.data, &e.created)
		}
		if cerr := br.Close(); err == nil {
			err = cerr
		}
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return nil
		case err != nil:
			return err
		}
		applyErr = o.target.Apply(ctx, e.id, e.data)
		if applyErr != nil && outbox.Permanent(applyErr) {
			_, err := tx.Exec(ctx, o.q.park, e.seq, e.id, e.data, e.created, applyErr.Error())
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
	n := 0
	var first error
	err := pgx.BeginFunc(ctx, o.pool, func(tx pgx.Tx) error {
		b := &pgx.Batch{}
		b.Queue(sqlOutboxLazy)
		b.Queue(o.q.next, max(limit, 1))
		br := tx.SendBatch(ctx, b)
		if _, err := br.Exec(); err != nil {
			br.Close()
			return err
		}
		rows, _ := br.Query()
		entries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outboxEntry, error) {
			var e outboxEntry
			err := row.Scan(&e.seq, &e.id, &e.data, &e.created)
			return e, err
		})
		if cerr := br.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		var gone []int64
		for _, e := range entries {
			err := o.target.Apply(ctx, e.id, e.data)
			switch {
			case err == nil:
				n++
				gone = append(gone, e.seq)
			case outbox.Permanent(err):
				if _, perr := tx.Exec(ctx, o.q.park, e.seq, e.id, e.data, e.created, err.Error()); perr != nil {
					return perr
				}
				gone = append(gone, e.seq)
				first = cmp.Or(first, fmt.Errorf("kiln: outbox entry %s set aside: %w", e.id, err))
			default:
				first = cmp.Or(first, err)
			}
		}
		if len(gone) == 0 {
			return nil
		}
		_, err = tx.Exec(ctx, o.q.done, gone)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("kiln: outbox: %w", err)
	}
	return n, first
}
