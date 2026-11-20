package outbox

import (
	"context"
	"errors"

	"github.com/rafaelaugustos/kiln/driver"
)

type Carrier interface {
	Put(ctx context.Context, id string, data []byte) error
	Deliver(ctx context.Context) error
}

type Writer struct {
	target driver.Stager
	c      Carrier
	st     driver.Staged
	id     string
	err    error
}

func NewWriter(target driver.Stager, c Carrier) *Writer {
	return &Writer{target: target, c: c}
}

func Broken(err error) *Writer {
	return &Writer{err: err}
}

func (w *Writer) stage(ctx context.Context) error {
	if w.err != nil || w.st != nil {
		return w.err
	}
	st, err := w.target.Stage(ctx)
	if err != nil {
		return err
	}
	w.st = st
	return nil
}

func (w *Writer) put(ctx context.Context) error {
	id, data, err := w.st.Encode()
	if err != nil {
		return err
	}
	if err := w.c.Put(ctx, id, data); err != nil {
		return err
	}
	w.id = id
	return nil
}

func (w *Writer) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if err := w.stage(ctx); err != nil {
		return nil, err
	}
	res, err := w.st.Insert(ctx, jobs)
	if err != nil || len(jobs) == 0 {
		return res, err
	}
	if err := w.put(ctx); err != nil {
		return nil, err
	}
	return res, nil
}

func (w *Writer) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	if err := w.stage(ctx); err != nil {
		return 0, err
	}
	id, err := w.st.OpenBatch(ctx, nb)
	if err != nil {
		return 0, err
	}
	if err := w.put(ctx); err != nil {
		return 0, err
	}
	return id, nil
}

func (w *Writer) SealBatch(ctx context.Context, id int64) error {
	if err := w.stage(ctx); err != nil {
		return err
	}
	if err := w.st.SealBatch(ctx, id); err != nil {
		return err
	}
	return w.put(ctx)
}

func (w *Writer) Notify(ctx context.Context) error {
	if w.id == "" {
		return nil
	}
	w.id, w.st = "", nil
	return w.c.Deliver(ctx)
}

func Permanent(err error) bool {
	return errors.Is(err, driver.ErrNotFound) || errors.Is(err, driver.ErrClosed) || errors.Is(err, driver.ErrInvalid)
}
