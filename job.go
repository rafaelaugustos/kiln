package kiln

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
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
	Args        T

	run *run
}

// RawJob is a job with its args left as JSON. [Middleware] and the handlers registered with
// [Mux.HandleFunc] receive jobs in this form.
type RawJob = Job[json.RawMessage]

type run struct {
	ref    driver.Ref
	worker driver.Worker
	output []byte
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
	if r.worker != nil {
		if err := r.worker.SetMeta(ctx, r.ref, map[string]string{key: string(b)}); err != nil {
			return err
		}
	}
	m := make(map[string]string, len(j.Meta)+1)
	maps.Copy(m, j.Meta)
	m[key] = string(b)
	j.Meta = m
	return nil
}

func rawJob(dj *driver.Job, w driver.Worker) *RawJob {
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
		Args:        dj.Args,
		run:         &run{ref: dj.Ref, worker: w},
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
		run:         raw.state(),
	}
	if err := json.Unmarshal(raw.Args, &j.Args); err != nil {
		return nil, Permanent(fmt.Errorf("kiln: decode %s: %w", raw.Kind, err))
	}
	return j, nil
}
