package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var _ driver.Transactor = (*Store)(nil)

var errDone = errors.New("kiln: transaction already committed or rolled back")

const releaseTimeout = 5 * time.Second

type wireOpen struct {
	ID     int64  `json:"id"`
	Parent int64  `json:"parent,omitempty"`
	Desc   string `json:"desc,omitempty"`
	Meta   string `json:"meta,omitempty"`
	Cr     int64  `json:"cr"`
}

type wireOp struct {
	Open *wireOpen       `json:"open,omitempty"`
	Seal int64           `json:"seal,omitempty"`
	Jobs []wireParams    `json:"jobs,omitempty"`
	Plan json.RawMessage `json:"plan,omitempty"`
}

type staged struct {
	s    *Store
	ops  []wireOp
	done bool
}

type stageArgs struct {
	Ops  []wireOp     `json:"ops,omitempty"`
	Open *wireOpen    `json:"open,omitempty"`
	Seal int64        `json:"seal,omitempty"`
	Jobs []wireParams `json:"jobs,omitempty"`
	TTL  int64        `json:"ttl"`
}

// InTx calls fn with a writer whose jobs and batches stay out of sight until fn returns nil, when
// one script applies them together, admits throttled jobs and wakes subscribers. Each write is
// checked when it is made, against the store and the earlier writes of the transaction, and
// checked again by the commit, which applies them all or none if a parent job or a batch they
// rely on was pruned, or a batch closed, in the meantime. The unique keys of the jobs it inserts
// are taken at once: an insert elsewhere with one of them is a duplicate of the transaction's
// job. If fn returns an error, nothing is applied, the keys are released and InTx returns the
// error.
func (s *Store) InTx(ctx context.Context, fn func(driver.Writer) error) error {
	t := &staged{s: s}
	if err := fn(t); err != nil {
		t.rollback(ctx)
		return err
	}
	return t.commit(ctx)
}

func (t *staged) stage(ctx context.Context, a stageArgs, out any) error {
	if t.done {
		return errDone
	}
	a.Ops, a.TTL = t.ops, micros(t.s.hold)
	return t.s.call(ctx, "stage", a, out)
}

func (t *staged) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if err := driver.CheckInsert(jobs); err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return []driver.Inserted{}, nil
	}
	ws := params(jobs)
	var res struct {
		Plan json.RawMessage `json:"plan"`
		Res  []wireInserted  `json:"res"`
	}
	if err := t.stage(ctx, stageArgs{Jobs: ws}, &res); err != nil {
		return nil, err
	}
	t.ops = append(t.ops, wireOp{Jobs: ws, Plan: res.Plan})
	return inserted(res.Res), nil
}

func (t *staged) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	o := &wireOpen{Parent: max(nb.Parent, 0), Desc: nb.Description, Meta: object(nb.Meta)}
	var res struct {
		ID string `json:"id"`
		Cr string `json:"cr"`
	}
	if err := t.stage(ctx, stageArgs{Open: o}, &res); err != nil {
		return 0, err
	}
	o.ID, _ = strconv.ParseInt(res.ID, 10, 64)
	o.Cr, _ = strconv.ParseInt(res.Cr, 10, 64)
	t.ops = append(t.ops, wireOp{Open: o})
	return o.ID, nil
}

func (t *staged) SealBatch(ctx context.Context, id int64) error {
	if err := t.stage(ctx, stageArgs{Seal: id}, nil); err != nil {
		return err
	}
	t.ops = append(t.ops, wireOp{Seal: id})
	return nil
}

func (t *staged) commit(ctx context.Context) error {
	if t.done {
		return errDone
	}
	t.done = true
	if len(t.ops) == 0 {
		return nil
	}
	args := struct {
		Ops []wireOp `json:"ops"`
	}{t.ops}
	return t.s.call(ctx, "commit", args, nil)
}

func (t *staged) rollback(ctx context.Context) {
	if t.done {
		return
	}
	t.done = true
	if len(t.ops) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	args := struct {
		Ops []wireOp `json:"ops"`
	}{t.ops}
	t.s.call(ctx, "rollback", args, nil)
}
