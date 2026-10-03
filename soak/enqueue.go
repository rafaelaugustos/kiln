package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/rafaelaugustos/kiln"
)

type enqueuer struct {
	b      *backend
	client *kiln.Client
	rate   float64
	limits []kiln.Limit
	jobs   atomic.Int64
	failed atomic.Int64
	last   atomic.Pointer[error]
}

type entry struct {
	kind string
	key  string
}

func limitsFor(rate float64) []kiln.Limit {
	var out []kiln.Limit
	for g := range max(1, int(math.Ceil(rate/50))) {
		out = append(out,
			kiln.Limit{Key: fmt.Sprintf("mutex.%d", g), Max: 1},
			kiln.Limit{Key: fmt.Sprintf("max3.%d", g), Max: 3},
			kiln.Limit{Key: fmt.Sprintf("rate.%d", g), Rate: 3, Per: time.Second, Burst: 3},
			kiln.Limit{Key: fmt.Sprintf("both.%d", g), Max: 2, Rate: 3, Per: time.Second, Burst: 2},
		)
	}
	return out
}

func (e *enqueuer) run(ctx context.Context) {
	const tick = 100 * time.Millisecond
	t := time.NewTicker(tick)
	defer t.Stop()
	var owed float64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		owed += e.rate * tick.Seconds()
		n := int(owed)
		if n == 0 {
			continue
		}
		owed -= float64(n)
		bctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := e.batch(bctx, n); err != nil {
			e.failed.Add(1)
			e.last.Store(&err)
		}
		cancel()
	}
}

func (e *enqueuer) batch(ctx context.Context, n int) error {
	specs := make([]kiln.Spec, 0, n+2)
	entries := make([]entry, 0, n+2)
	add := func(kind, key string, args kiln.Args, opts ...kiln.InsertOption) {
		specs = append(specs, kiln.Spec{Args: args, Options: opts})
		entries = append(entries, entry{kind, key})
	}
	for range n {
		switch r := rand.Float64(); {
		case r < 0.35:
			add("plain", "", plain{})
		case r < 0.45:
			fail := 1 + rand.IntN(2)
			add("flaky", "", flaky{Fail: fail}, kiln.MaxAttempts(fail+4))
		case r < 0.50:
			add("doomed", "", flaky{Fail: math.MaxInt32}, kiln.MaxAttempts(2))
		case r < 0.65:
			i := len(specs)
			add("parent", "", step{})
			add("parent", "", step{})
			add("child", "", step{}, kiln.Needs{i, i + 1})
		case r < 0.85:
			l := e.limits[rand.IntN(len(e.limits))]
			add("limited", l.Key, limited{Ms: 5 + rand.IntN(45)}, l)
		default:
			add("nap", "", nap{Ms: rand.IntN(5000)})
		}
	}
	tx, err := e.b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	w := e.b.writer(tx)
	res, err := e.client.EnqueueManyTx(ctx, w, specs...)
	if err != nil {
		return err
	}
	args := make([]any, 0, 3*len(res))
	for i, r := range res {
		args = append(args, r.ID, entries[i].kind, entries[i].key)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+e.b.enqueued+" (id, kind, lkey) VALUES "+e.b.values(len(res), 3), args...); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	w.Notify(ctx)
	e.jobs.Add(int64(len(res)))
	return nil
}
