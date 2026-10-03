package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln"
)

type plain struct{}

func (plain) Kind() string { return "soak.plain" }

type flaky struct{ Fail int }

func (flaky) Kind() string { return "soak.flaky" }

type step struct{}

func (step) Kind() string { return "soak.step" }

type nap struct{ Ms int }

func (nap) Kind() string { return "soak.nap" }

func (nap) InsertOptions() []kiln.InsertOption { return []kiln.InsertOption{kiln.Queue("slow")} }

type limited struct{ Ms int }

func (limited) Kind() string { return "soak.limited" }

func (limited) InsertOptions() []kiln.InsertOption { return []kiln.InsertOption{kiln.Queue("limited")} }

var errFlaky = errors.New("flaky failure")

func handle(m *kiln.Mux) {
	backoff := kiln.Exponential(250*time.Millisecond, 5*time.Second)
	kiln.Handle(m, func(context.Context, *kiln.Job[plain]) error { return nil }, backoff)
	kiln.Handle(m, func(_ context.Context, j *kiln.Job[flaky]) error {
		if j.Attempt <= j.Args.Fail {
			return errFlaky
		}
		return nil
	}, backoff)
	kiln.Handle(m, func(ctx context.Context, j *kiln.Job[step]) error {
		out, err := j.ParentOutputs(ctx)
		if err != nil {
			return err
		}
		for _, p := range j.Parents {
			if string(out[p]) != strconv.FormatInt(p, 10) {
				return kiln.Permanent(fmt.Errorf("parent %d has output %q", p, out[p]))
			}
		}
		return j.SetOutput(j.ID)
	}, backoff)
	kiln.Handle(m, func(ctx context.Context, j *kiln.Job[nap]) error { return pause(ctx, j.Args.Ms) }, backoff)
	kiln.Handle(m, func(ctx context.Context, j *kiln.Job[limited]) error { return pause(ctx, j.Args.Ms) }, backoff)
}

func pause(ctx context.Context, ms int) error {
	t := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}
