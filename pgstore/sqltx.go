package pgstore

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type sqlTx struct {
	tx *sql.Tx
	m  *pgtype.Map
}

func (t *sqlTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return &sqlResults{ctx: ctx, t: t, qs: b.QueuedQueries}
}

type sqlResults struct {
	ctx context.Context
	t   *sqlTx
	qs  []*pgx.QueuedQuery
	i   int
	err error
}

func (r *sqlResults) next() (*pgx.QueuedQuery, error) {
	switch {
	case r.err != nil:
		return nil, r.err
	case r.i == len(r.qs):
		return nil, errors.New("kiln: no more results in batch")
	}
	r.i++
	return r.qs[r.i-1], nil
}

func (r *sqlResults) Exec() (pgconn.CommandTag, error) {
	q, err := r.next()
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	res, err := r.t.tx.ExecContext(r.ctx, q.SQL, q.Arguments...)
	if err != nil {
		r.err = err
		return pgconn.CommandTag{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		r.err = err
		return pgconn.CommandTag{}, err
	}
	return pgconn.NewCommandTag("UPDATE " + strconv.FormatInt(n, 10)), nil
}

func (r *sqlResults) Query() (pgx.Rows, error) {
	q, err := r.next()
	if err != nil {
		return &sqlRows{t: r.t, err: err}, err
	}
	rows, err := r.t.tx.QueryContext(r.ctx, q.SQL, q.Arguments...)
	if err != nil {
		r.err = err
		return &sqlRows{t: r.t, err: err}, err
	}
	return &sqlRows{rows: rows, t: r.t}, nil
}

func (r *sqlResults) QueryRow() pgx.Row {
	rows, _ := r.Query()
	return sqlRow{rows}
}

func (r *sqlResults) Close() error {
	for r.err == nil && r.i < len(r.qs) {
		var err error
		if q := r.qs[r.i]; q.Fn != nil {
			err = q.Fn(r)
		} else {
			_, err = r.Exec()
		}
		if r.err == nil {
			r.err = err
		}
	}
	return r.err
}

type sqlRows struct {
	rows *sql.Rows
	t    *sqlTx
	err  error
}

func (r *sqlRows) Close() {
	if r.rows != nil {
		r.rows.Close()
	}
}

func (r *sqlRows) Err() error {
	if r.err == nil && r.rows != nil {
		return r.rows.Err()
	}
	return r.err
}

func (r *sqlRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *sqlRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *sqlRows) Next() bool {
	return r.rows != nil && r.rows.Next()
}

func (r *sqlRows) Scan(dest ...any) error {
	dest = slices.Clone(dest)
	for i, d := range dest {
		if t := reflect.TypeOf(d); t != nil && t.Kind() == reflect.Pointer && composite(t.Elem()) {
			dest[i] = r.TypeMap().SQLScanner(d)
		}
	}
	return r.rows.Scan(dest...)
}

func (r *sqlRows) Values() ([]any, error) { return nil, errors.ErrUnsupported }

func (r *sqlRows) RawValues() [][]byte { return nil }

func (r *sqlRows) Conn() *pgx.Conn { return nil }

func (r *sqlRows) TypeMap() *pgtype.Map {
	if r.t.m == nil {
		r.t.m = pgtype.NewMap()
	}
	return r.t.m
}

func composite(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Slice:
		return t.Elem().Kind() != reflect.Uint8
	case reflect.Array, reflect.Map:
		return true
	}
	return false
}

type sqlRow struct {
	rows pgx.Rows
}

func (r sqlRow) Scan(dest ...any) error {
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	r.rows.Close()
	return r.rows.Err()
}
