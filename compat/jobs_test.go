package compat

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rafaelaugustos/kiln"
)

const (
	version     = "current"
	handoffBy   = "compat.by"
	handoffWait = 150 * time.Millisecond
	limitKey    = "compat"
)

var errHandoff = errors.New("compat: waiting for the other version")

type Echo struct {
	N int `json:"n"`
}

func (Echo) Kind() string { return "compat.echo" }

type FailOnce struct {
	N int `json:"n"`
}

func (FailOnce) Kind() string { return "compat.fail_once" }

type Child struct {
	N int `json:"n"`
}

func (Child) Kind() string { return "compat.child" }

type Handoff struct {
	From string `json:"from"`
}

func (Handoff) Kind() string { return "compat.handoff" }

func (Handoff) InsertOptions() []kiln.InsertOption {
	return []kiln.InsertOption{kiln.MaxAttempts(100)}
}

type Limited struct {
	N int `json:"n"`
}

func (Limited) Kind() string { return "compat.limited" }

func (Limited) InsertOptions() []kiln.InsertOption {
	return []kiln.InsertOption{kiln.Limit{Key: limitKey, Max: 2}}
}

func echo(ctx context.Context, j *kiln.Job[Echo]) error {
	if err := pause(ctx, 20*time.Millisecond); err != nil {
		return err
	}
	return j.SetOutput(map[string]int{"n": j.Args.N})
}

func failOnce(_ context.Context, j *kiln.Job[FailOnce]) error {
	if j.Attempt == 1 {
		return fmt.Errorf("compat: job %d fails its first attempt", j.ID)
	}
	return nil
}

func child(ctx context.Context, j *kiln.Job[Child]) error {
	c, _ := kiln.ClientFrom(ctx)
	for _, id := range j.Parents {
		p, err := c.Get(ctx, id)
		if err != nil {
			return err
		}
		if p.State != kiln.Succeeded {
			return kiln.Permanent(fmt.Errorf("compat: job %d ran while parent %d is %s", j.ID, id, p.State))
		}
	}
	self, err := c.Get(ctx, j.ID)
	if err != nil || self.AfterBatch == 0 {
		return err
	}
	b, err := c.Store().Batch(ctx, self.AfterBatch)
	if err != nil {
		return err
	}
	if b.FinishedAt.IsZero() {
		return kiln.Permanent(fmt.Errorf("compat: job %d ran before batch %d finished", j.ID, b.ID))
	}
	return nil
}

func handoff(ctx context.Context, j *kiln.Job[Handoff]) error {
	var by string
	seen, err := j.Param(handoffBy, &by)
	switch {
	case err != nil:
		return kiln.Permanent(err)
	case !seen && j.Args.From != version:
		return kiln.Snooze(handoffWait)
	case !seen:
		if err := j.SetParam(ctx, handoffBy, version); err != nil {
			return err
		}
		return errHandoff
	case j.Args.From == version:
		return errHandoff
	}
	return j.SetOutput(map[string]string{"from": by, "to": version})
}

func limited(ctx context.Context, _ *kiln.Job[Limited]) error {
	return pause(ctx, 30*time.Millisecond)
}

func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}

type worker struct {
	mu    sync.Mutex
	done  map[string]int
	calls map[string]int
	runs  map[int64]int
}

func newWorker() *worker {
	return &worker{done: make(map[string]int), calls: make(map[string]int), runs: make(map[int64]int)}
}

func (w *worker) mux() *kiln.Mux {
	m := kiln.NewMux()
	m.Use(w.track)
	kiln.Handle(m, echo)
	kiln.Handle(m, failOnce)
	kiln.Handle(m, child)
	kiln.Handle(m, handoff)
	kiln.Handle(m, limited)
	return m
}

func (w *worker) track(next kiln.HandlerFunc) kiln.HandlerFunc {
	return func(ctx context.Context, j *kiln.RawJob) error {
		err := next(ctx, j)
		w.mu.Lock()
		w.calls[j.Kind]++
		if err == nil {
			w.done[j.Kind]++
			w.runs[j.ID]++
		}
		w.mu.Unlock()
		return err
	}
}

func (w *worker) report() *summary {
	w.mu.Lock()
	defer w.mu.Unlock()
	return &summary{Version: version, Role: "both", Processed: w.done, Calls: w.calls, Runs: w.runs}
}

type plan struct {
	Echo    int           `json:"echo,omitempty"`
	Fail    int           `json:"fail,omitempty"`
	Handoff int           `json:"handoff,omitempty"`
	Limited int           `json:"limited,omitempty"`
	Flows   int           `json:"flows,omitempty"`
	Batches int           `json:"batches,omitempty"`
	Delayed int           `json:"delayed,omitempty"`
	Delay   time.Duration `json:"delay,omitempty"`
	After   []int64       `json:"after,omitempty"`
	Unique  []string      `json:"unique,omitempty"`
	Window  []string      `json:"window,omitempty"`
}

type result struct {
	Jobs       map[string][]int64 `json:"jobs"`
	Batches    []int64            `json:"batches,omitempty"`
	Duplicates map[string]int64   `json:"duplicates,omitempty"`
	Err        string             `json:"err,omitempty"`
}

type summary struct {
	Version   string         `json:"version"`
	Role      string         `json:"role"`
	Server    string         `json:"server,omitempty"`
	Processed map[string]int `json:"processed"`
	Calls     map[string]int `json:"calls"`
	Runs      map[int64]int  `json:"runs"`
	Stats     *kiln.Stats    `json:"stats,omitempty"`
	Errors    []string       `json:"errors"`
}

type message struct {
	Result  *result  `json:"result,omitempty"`
	Summary *summary `json:"summary,omitempty"`
}

func enqueue(ctx context.Context, c *kiln.Client, p plan, errs *errlog) *result {
	r := &result{Jobs: make(map[string][]int64), Duplicates: make(map[string]int64)}
	if err := r.add(ctx, c, p); err != nil {
		r.Err = err.Error()
		errs.add("enqueue: " + r.Err)
	}
	return r
}

func (r *result) add(ctx context.Context, c *kiln.Client, p plan) error {
	delay := []kiln.InsertOption{kiln.Delay(p.Delay)}
	for _, g := range []struct {
		name string
		n    int
		spec func(i int) kiln.Spec
	}{
		{"echo", p.Echo, func(i int) kiln.Spec { return kiln.Spec{Args: Echo{N: i}} }},
		{"fail", p.Fail, func(i int) kiln.Spec { return kiln.Spec{Args: FailOnce{N: i}} }},
		{"handoff", p.Handoff, func(int) kiln.Spec { return kiln.Spec{Args: Handoff{From: version}} }},
		{"limited", p.Limited, func(i int) kiln.Spec { return kiln.Spec{Args: Limited{N: i}} }},
		{"delayed", p.Delayed, func(i int) kiln.Spec { return kiln.Spec{Args: Echo{N: i}, Options: delay} }},
		{"after", len(p.After), func(i int) kiln.Spec {
			return kiln.Spec{Args: Child{N: i}, Options: []kiln.InsertOption{kiln.After{p.After[i]}}}
		}},
	} {
		if g.n == 0 {
			continue
		}
		specs := make([]kiln.Spec, g.n)
		for i := range specs {
			specs[i] = g.spec(i)
		}
		if err := r.insert(ctx, c, g.name, specs...); err != nil {
			return err
		}
	}
	for range p.Flows {
		var f kiln.Flow
		root := f.Add(Echo{})
		left := f.Add(Child{N: 1}, kiln.Needs{root})
		right := f.Add(Child{N: 2}, kiln.Needs{root})
		f.Add(Child{N: 3}, kiln.Needs{left, right})
		if err := r.insert(ctx, c, "flow", f...); err != nil {
			return err
		}
	}
	for range p.Batches {
		b := &kiln.Batch{Description: "compat " + version}
		for i := range 3 {
			b.Add(Echo{N: i})
		}
		b.Then(Child{})
		id, err := c.StartBatch(ctx, b)
		if err != nil {
			return fmt.Errorf("batch: %w", err)
		}
		r.Batches = append(r.Batches, id)
	}
	for _, u := range []struct {
		group string
		keys  []string
		opts  func(key string) []kiln.InsertOption
	}{
		{"unique", p.Unique, func(k string) []kiln.InsertOption { return append([]kiln.InsertOption{kiln.Unique{Key: k}}, delay...) }},
		{"window", p.Window, func(k string) []kiln.InsertOption { return []kiln.InsertOption{kiln.Unique{Key: k, For: time.Hour}} }},
	} {
		for _, k := range u.keys {
			in, err := c.EnqueueMany(ctx, kiln.Spec{Args: Echo{}, Options: u.opts(k)})
			if err != nil {
				return fmt.Errorf("%s %s: %w", u.group, k, err)
			}
			if in[0].Duplicate {
				r.Duplicates[k] = in[0].ID
				continue
			}
			r.Jobs[u.group] = append(r.Jobs[u.group], in[0].ID)
		}
	}
	return nil
}

func (r *result) insert(ctx context.Context, c *kiln.Client, group string, specs ...kiln.Spec) error {
	in, err := c.EnqueueMany(ctx, specs...)
	if err != nil {
		return fmt.Errorf("%s: %w", group, err)
	}
	for _, i := range in {
		r.Jobs[group] = append(r.Jobs[group], i.ID)
	}
	return nil
}

type errlog struct {
	mu    sync.Mutex
	lines []string
}

func (l *errlog) Write(p []byte) (int, error) {
	l.add(strings.TrimSpace(string(p)))
	return len(p), nil
}

func (l *errlog) add(s string) {
	l.mu.Lock()
	l.lines = append(l.lines, s)
	l.mu.Unlock()
}

func (l *errlog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.lines...)
}
