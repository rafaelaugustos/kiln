package sqlitestore

import (
	"context"
	"fmt"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

type TxWriter struct {
	s      *Store
	q      querier
	queues []string
	lost   error
}

func (w *TxWriter) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	if err := driver.CheckInsert(jobs); err != nil {
		return nil, err
	}
	in := w.s.plan(jobs)
	if err := w.atomic(ctx, func() error { return in.write(ctx, w.q) }); err != nil {
		return nil, wrap("insert", err)
	}
	w.queues = merge(w.queues, in.f.queues)
	return in.res, nil
}

func (w *TxWriter) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	var id int64
	err := w.atomic(ctx, func() (err error) {
		id, err = w.s.openBatch(ctx, w.q, nb)
		return err
	})
	if err != nil {
		return 0, wrap("open batch", err)
	}
	return id, nil
}

func (w *TxWriter) SealBatch(ctx context.Context, id int64) error {
	var f *fallout
	err := w.atomic(ctx, func() (err error) {
		f, err = w.s.seal(ctx, w.q, id)
		return err
	})
	if err != nil {
		return wrap("seal batch", err)
	}
	w.queues = merge(w.queues, f.queues)
	return nil
}

func (w *TxWriter) Notify(context.Context) error {
	queues := w.queues
	w.queues = nil
	w.s.hub.ready(queues)
	return nil
}

func (w *TxWriter) atomic(ctx context.Context, fn func() error) error {
	switch {
	case w.q == nil:
		return driver.ErrNilTx
	case w.lost != nil:
		return w.lost
	}
	if _, err := w.q.ExecContext(ctx, "SAVEPOINT kiln"); err != nil {
		return err
	}
	bg := context.WithoutCancel(ctx)
	err := fn()
	if err != nil {
		if _, rerr := w.q.ExecContext(bg, "ROLLBACK TO kiln"); rerr != nil {
			w.lost = fmt.Errorf("transaction rolled back: %w", err)
			return w.lost
		}
	}
	if _, rerr := w.q.ExecContext(bg, "RELEASE kiln"); rerr != nil {
		w.lost = fmt.Errorf("release savepoint: %w", rerr)
		return w.lost
	}
	return err
}

func (s *Store) InTx(ctx context.Context, fn func(w driver.Writer) error) error {
	var w *TxWriter
	err := s.write(ctx, func(_ context.Context, q querier) error {
		w = &TxWriter{s: s, q: q}
		return fn(w)
	})
	if err != nil {
		return err
	}
	s.hub.ready(w.queues)
	return nil
}

func merge(dst, src []string) []string {
	for _, k := range src {
		if !slices.Contains(dst, k) {
			dst = append(dst, k)
		}
	}
	return dst
}
