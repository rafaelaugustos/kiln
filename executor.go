package kiln

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	maxOutput    = 1 << 20
	maxError     = 2 << 10
	maxTrace     = 8 << 10
	unknownDelay = time.Minute
)

var errFenced = errors.New("kiln: claim fenced")

type task struct {
	context.Context
	cancel  context.CancelCauseFunc
	client  *Client
	job     *RawJob
	ref     driver.Ref
	limited bool
	timeout time.Duration
	prod    *producer
	seq     uint64
	settled atomic.Bool
}

func (t *task) Value(key any) any {
	switch key {
	case clientKey:
		return t.client
	case jobKey:
		return t.job
	}
	return t.Context.Value(key)
}

func (s *Server) start(p *producer, jobs []driver.Job) {
	seq := s.seq.Load()
	ts := p.tasks[:0]
	for i := range jobs {
		t := &task{client: s.client, job: rawJob(&jobs[i], s.store), ref: jobs[i].Ref, limited: jobs[i].LimitKey != "", timeout: jobs[i].Timeout, prod: p, seq: seq}
		t.Context, t.cancel = context.WithCancelCause(s.base)
		ts = append(ts, t)
	}
	s.wg.Add(len(ts))
	s.mu.Lock()
	fenced := s.fenced.Load()
	for _, t := range ts {
		s.tasks[t.ref.ID] = t
		if fenced {
			t.cancel(errFenced)
		}
	}
	s.mu.Unlock()
	for _, t := range ts {
		go s.work(t)
	}
	clear(ts)
	p.tasks = ts[:0]
}

func (s *Server) work(t *task) {
	o, err := s.mux.exec(t, &s.cfg)
	t.cancel(context.Canceled)
	if t.settled.CompareAndSwap(false, true) {
		if context.Cause(t) == errFenced && o.State != driver.Succeeded {
			s.forget(t)
		} else {
			s.comp.submit(pending{Outcome: o, t: t})
		}
		s.wg.Done()
	}
	t.prod.release()
	if s.log.Enabled(s.base, slog.LevelDebug) {
		s.log.Debug("kiln: job done", "id", t.ref.ID, "kind", t.job.Kind, "attempt", t.job.Attempt, "state", o.State, "err", err)
	}
}

func (s *Server) forget(t *task) {
	s.mu.Lock()
	if s.tasks[t.ref.ID] == t {
		delete(s.tasks, t.ref.ID)
	}
	s.mu.Unlock()
}

func (s *Server) cancelAll(cause error) {
	s.mu.Lock()
	for _, t := range s.tasks {
		if !t.settled.Load() {
			t.cancel(cause)
		}
	}
	s.mu.Unlock()
}

func (s *Server) abandon() {
	var outs []pending
	s.mu.Lock()
	for _, t := range s.tasks {
		if t.settled.CompareAndSwap(false, true) {
			outs = append(outs, pending{
				Ref:    t.ref,
				State:  driver.Enqueued,
				Refund: true,
				Reason: "shutdown",
				Error:  "kiln: abandoned after shutdown timeout", t: t})
		}
	}
	s.mu.Unlock()
	if len(outs) == 0 {
		return
	}
	s.stats.abandoned.Add(uint64(len(outs)))
	s.log.Warn("kiln: abandoned running jobs", "id", s.id, "count", len(outs))
	go func() {
		for _, p := range outs {
			s.comp.submit(p)
			s.wg.Done()
		}
	}()
}

func (m *Mux) exec(t *task, cfg *ServerConfig) (driver.Outcome, error) {
	j := t.job
	r := m.routes[j.Kind]
	if r == nil {
		err := fmt.Errorf("kiln: no handler for kind %q", j.Kind)
		o := driver.Outcome{Ref: t.ref, State: driver.Scheduled, Refund: true, Delay: unknownDelay, Reason: "unknown kind", Error: err.Error()}
		if time.Since(j.CreatedAt) > cfg.UnknownKindTTL {
			o.State, o.Refund, o.Delay = driver.Failed, false, 0
		}
		return o, err
	}
	if expired(j.Meta) {
		return driver.Outcome{Ref: t.ref, State: driver.Deleted, Reason: "deadline"}, nil
	}
	var timer *time.Timer
	if d := r.timeoutFor(t.timeout, time.Duration(cfg.Timeout)); d > 0 {
		timer = time.AfterFunc(d, func() { t.cancel(ErrTimeout) })
	}
	err := r.call(t, j)
	if timer != nil {
		timer.Stop()
	}
	return r.outcome(t, cfg, err)
}

func (r *route) timeoutFor(job, def time.Duration) time.Duration {
	switch {
	case job != 0:
		return job
	case r.timeout != 0:
		return r.timeout
	}
	return def
}

func (r *route) outcome(t *task, cfg *ServerConfig, err error) (o driver.Outcome, cause error) {
	defer func() {
		if v := recover(); v != nil {
			o, cause = crashed(t, &PanicError{Value: v, Stack: debug.Stack()})
		}
	}()
	return r.classify(t, cfg, err)
}

func crashed(t *task, p *PanicError) (driver.Outcome, error) {
	j := t.job
	o := driver.Outcome{Ref: t.ref, State: driver.Failed, Reason: "exhausted", Error: clean(p.Error(), maxError), Trace: clean(string(p.Stack), maxTrace)}
	if j.Attempt < j.MaxAttempts {
		o.State, o.Reason, o.Delay = driver.Scheduled, "retry", defaultBackoff(j.Attempt, p)
	}
	return o, p
}

func (r *route) classify(t *task, cfg *ServerConfig, err error) (driver.Outcome, error) {
	o := driver.Outcome{Ref: t.ref}
	j := t.job
	if err == nil {
		o.State = driver.Succeeded
		if out := j.Output(); out != nil {
			if len(out) > maxOutput || !json.Valid(out) {
				o.State, o.Reason, o.Error = driver.Failed, "output rejected", "kiln: output rejected"
			} else {
				o.Output = out
			}
		}
		return o, nil
	}
	cause := context.Cause(t)
	if cause == ErrTimeout && !errors.Is(err, ErrTimeout) {
		err = fmt.Errorf("%w: %w", ErrTimeout, err)
	}
	snooze, snoozing := snoozed(err)
	switch {
	case cause == ErrCanceled || errors.Is(err, ErrCanceled):
		o.State, o.Reason = driver.Deleted, "canceled"
	case cause == ErrShutdown:
		o.State, o.Refund, o.Reason = driver.Enqueued, true, "shutdown"
	case errors.Is(err, ErrPermanent):
		o.State, o.Reason = driver.Failed, "permanent"
	case snoozing:
		o.State, o.Refund, o.Delay, o.Reason = driver.Scheduled, true, snooze, "snoozed"
	case j.Attempt < j.MaxAttempts:
		o.State, o.Reason = driver.Scheduled, "retry"
		o.Delay = max(backoffFor(r, cfg.Backoff)(j.Attempt, err), 0)
	default:
		o.State, o.Reason = driver.Failed, "exhausted"
	}
	o.Error = clean(err.Error(), maxError)
	if p, ok := errors.AsType[*PanicError](err); ok {
		o.Trace = clean(string(p.Stack), maxTrace)
	}
	return o, err
}

func backoffFor(r *route, def Backoff) Backoff {
	switch {
	case r != nil && r.backoff != nil:
		return r.backoff
	case def != nil:
		return def
	}
	return defaultBackoff
}

func expired(meta map[string]string) bool {
	v, ok := meta[metaDeadline]
	if !ok {
		return false
	}
	d, err := time.Parse(time.RFC3339Nano, v)
	return err == nil && time.Now().After(d)
}

func clean(s string, n int) string {
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	if len(s) > n {
		for n > 0 && !utf8.RuneStart(s[n]) {
			n--
		}
		s = s[:n]
	}
	return s
}
