package kiln

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

// Job is a job as its handler receives it, with the args decoded into Args. The fields are a copy:
// changing them does not change the stored job, which [Job.SetParam] does for Meta.
type Job[T any] struct {
	ID          int64
	Kind        string
	Queue       string
	Priority    int16
	Attempt     int // 1 on the first run; snoozes and shutdowns give the attempt back
	MaxAttempts int
	CreatedAt   time.Time // when the job was inserted, on the store's clock
	RunAt       time.Time // when the job became due, on the store's clock
	BatchID     int64     // the batch the job belongs to, or 0
	RecurringID string    // the recurring job that inserted it, or ""
	Parents     []int64   // the jobs it waited for
	Meta        map[string]string
	Tags        []string
	Title       string // the job's [Title], or ""
	Args        T

	run *run
}

// RawJob is a job with its args left as JSON. [Middleware] and the handlers registered with
// [Mux.HandleFunc] receive jobs in this form.
type RawJob = Job[json.RawMessage]

type run struct {
	ref    driver.Ref
	srv    *Server
	output []byte

	mu       sync.Mutex
	writing  sync.Mutex
	lines    []string
	logged   int
	progress int
	set      bool
	moved    bool
	armed    bool
	closed   bool
	timer    *time.Timer
}

func (j *Job[T]) state() *run {
	if j.run == nil {
		j.run = &run{ref: driver.Ref{ID: j.ID}}
	}
	return j.run
}

// SetOutput records v, encoded as JSON, as the result of the job, kept with it once it succeeds.
// A later call replaces the output, and an attempt that fails discards it. Output that encodes to
// more than 1 MiB fails the job.
func (j *Job[T]) SetOutput(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("kiln: encode output: %w", err)
	}
	j.state().output = b
	return nil
}

// Output returns the JSON recorded by [Job.SetOutput] during this attempt, or nil.
func (j *Job[T]) Output() json.RawMessage {
	if j.run == nil {
		return nil
	}
	return j.run.output
}

// ParentOutputs returns the output of each of the job's parents that succeeded, keyed by parent id,
// reading the parents from the store one at a time. A parent that did not succeed, has no output or
// has been pruned is left out, and so is every parent of a job that no server is running.
func (j *Job[T]) ParentOutputs(ctx context.Context) (map[int64]json.RawMessage, error) {
	out := make(map[int64]json.RawMessage, len(j.Parents))
	r := j.state()
	if r.srv == nil {
		return out, nil
	}
	for _, id := range j.Parents {
		rec, err := r.srv.store.Job(ctx, id)
		switch {
		case errors.Is(err, ErrNotFound):
		case err != nil:
			return nil, err
		case rec.State == Succeeded && len(rec.Output) > 0:
			out[id] = rec.Output
		}
	}
	return out, nil
}

// Param decodes the JSON value under key in the job's Meta, as stored by [Job.SetParam], into v,
// and reports whether the key was present.
func (j *Job[T]) Param(key string, v any) (bool, error) {
	s, ok := j.Meta[key]
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal([]byte(s), v); err != nil {
		return true, fmt.Errorf("kiln: decode param %q: %w", key, err)
	}
	return true, nil
}

// SetParam stores v, encoded as JSON, under key in the job's Meta, where [Job.Param] finds it in
// later attempts. It writes to the store right away and then updates j.Meta, and fails with
// [ErrLost] when the job is no longer running under this attempt's claim.
func (j *Job[T]) SetParam(ctx context.Context, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("kiln: encode param %q: %w", key, err)
	}
	r := j.state()
	if r.srv != nil {
		if err := r.srv.store.SetMeta(ctx, r.ref, map[string]string{key: string(b)}); err != nil {
			return err
		}
	}
	m := make(map[string]string, len(j.Meta)+1)
	maps.Copy(m, j.Meta)
	m[key] = string(b)
	j.Meta = m
	return nil
}

// Logf formats its arguments as [fmt.Sprintf] does, drops a final newline, and adds the line to
// the console of this attempt, which the dashboard shows. The server writes the console to the
// store every 250ms while the handler runs and once more after it returns; a write that fails is
// logged and never fails the job. An attempt keeps its first 1000 lines, each cut to 4 KiB, and
// then one line saying the console was truncated. Logf is safe for concurrent use, and does
// nothing when the store keeps no console.
func (j *Job[T]) Logf(format string, args ...any) {
	j.state().logf(fmt.Sprintf(format, args...))
}

// SetProgress sets the job's progress to percent, clamped between 0 and 100, which the dashboard
// shows while the job is processing. It reaches the store with the console, as [Job.Logf]
// describes, and is kept with the job after the attempt ends.
func (j *Job[T]) SetProgress(percent int) {
	j.state().setProgress(min(max(percent, 0), 100))
}

func rawJob(dj *driver.Job, s *Server) *RawJob {
	return &RawJob{
		ID:          dj.ID,
		Kind:        dj.Kind,
		Queue:       dj.Queue,
		Priority:    dj.Priority,
		Attempt:     dj.Attempt,
		MaxAttempts: dj.MaxAttempts,
		CreatedAt:   dj.CreatedAt,
		RunAt:       dj.RunAt,
		BatchID:     dj.BatchID,
		RecurringID: dj.RecurringID,
		Parents:     dj.Parents,
		Meta:        dj.Meta,
		Tags:        dj.Tags,
		Title:       dj.Title,
		Args:        dj.Args,
		run:         &run{ref: dj.Ref, srv: s, closed: s != nil && s.console == nil},
	}
}

func typed[T any](raw *RawJob) (*Job[T], error) {
	j := &Job[T]{
		ID:          raw.ID,
		Kind:        raw.Kind,
		Queue:       raw.Queue,
		Priority:    raw.Priority,
		Attempt:     raw.Attempt,
		MaxAttempts: raw.MaxAttempts,
		CreatedAt:   raw.CreatedAt,
		RunAt:       raw.RunAt,
		BatchID:     raw.BatchID,
		RecurringID: raw.RecurringID,
		Parents:     raw.Parents,
		Meta:        raw.Meta,
		Tags:        raw.Tags,
		Title:       raw.Title,
		run:         raw.state(),
	}
	if err := json.Unmarshal(raw.Args, &j.Args); err != nil {
		return nil, Permanent(fmt.Errorf("kiln: decode %s: %w", raw.Kind, err))
	}
	return j, nil
}
