package kiln

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type Job[T any] struct {
	ID          int64
	Kind        string
	Queue       string
	Priority    int16
	Attempt     int
	MaxAttempts int
	CreatedAt   time.Time
	BatchID     int64
	RecurringID string
	Parents     []int64
	Meta        map[string]string
	Tags        []string
	Args        T

	run *run
}

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

func (j *Job[T]) SetOutput(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("kiln: encode output: %w", err)
	}
	j.state().output = b
	return nil
}

func (j *Job[T]) Output() json.RawMessage {
	if j.run == nil {
		return nil
	}
	return j.run.output
}

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
