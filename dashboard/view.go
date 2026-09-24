package dashboard

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type jobView struct {
	ID              int64           `json:"id"`
	State           driver.State    `json:"state"`
	Kind            string          `json:"kind"`
	Queue           string          `json:"queue"`
	Priority        int16           `json:"priority"`
	Attempt         int             `json:"attempt"`
	MaxAttempts     int             `json:"max_attempts"`
	Args            json.RawMessage `json:"args"`
	Meta            json.RawMessage `json:"meta,omitempty"`
	Tags            []string        `json:"tags,omitempty"`
	Server          string          `json:"server,omitempty"`
	TimeoutMS       int64           `json:"timeout_ms,omitempty"`
	RunAt           time.Time       `json:"run_at,omitzero"`
	CreatedAt       time.Time       `json:"created_at,omitzero"`
	AttemptedAt     time.Time       `json:"attempted_at,omitzero"`
	FinalizedAt     time.Time       `json:"finalized_at,omitzero"`
	BatchID         int64           `json:"batch_id,omitempty"`
	AfterBatch      int64           `json:"after_batch,omitempty"`
	RecurringID     string          `json:"recurring_id,omitempty"`
	LimitKey        string          `json:"limit_key,omitempty"`
	Parents         []int64         `json:"parents,omitempty"`
	Children        []int64         `json:"children,omitempty"`
	PendingDeps     int             `json:"pending_deps,omitempty"`
	CancelRequested bool            `json:"cancel_requested,omitempty"`
	Output          json.RawMessage `json:"output,omitempty"`
	History         []entryView     `json:"history,omitempty"`
}

type entryView struct {
	At      time.Time    `json:"at"`
	State   driver.State `json:"state"`
	Attempt int          `json:"attempt"`
	Reason  string       `json:"reason,omitempty"`
	Error   string       `json:"error,omitempty"`
	Trace   string       `json:"trace,omitempty"`
	Server  string       `json:"server,omitempty"`
}

func (h *handler) view(r *driver.Record) *jobView {
	v := &jobView{
		ID:              r.ID,
		State:           r.State,
		Kind:            r.Kind,
		Queue:           r.Queue,
		Priority:        r.Priority,
		Attempt:         r.Attempt,
		MaxAttempts:     r.MaxAttempts,
		Args:            h.redact(r.Kind, PartArgs, r.Args),
		Tags:            r.Tags,
		Server:          r.Server,
		TimeoutMS:       r.Timeout.Milliseconds(),
		RunAt:           r.RunAt,
		CreatedAt:       r.CreatedAt,
		AttemptedAt:     r.AttemptedAt,
		FinalizedAt:     r.FinalizedAt,
		BatchID:         r.BatchID,
		AfterBatch:      r.AfterBatch,
		RecurringID:     r.RecurringID,
		LimitKey:        r.LimitKey,
		Parents:         r.Parents,
		Children:        r.Children,
		PendingDeps:     r.PendingDeps,
		CancelRequested: r.CancelRequested,
		Output:          h.redact(r.Kind, PartOutput, r.Output),
	}
	if v.Args == nil {
		v.Args = json.RawMessage("null")
	}
	if r.Timeout < 0 {
		v.TimeoutMS = -1
	}
	if len(r.Meta) > 0 {
		b, _ := json.Marshal(r.Meta)
		v.Meta = h.redact(r.Kind, PartMeta, b)
	}
	for _, e := range r.History {
		v.History = append(v.History, entryView{
			At:      e.At,
			State:   e.State,
			Attempt: e.Attempt,
			Reason:  e.Reason,
			Error:   h.redactText(r.Kind, e.Error),
			Trace:   h.redactText(r.Kind, e.Trace),
			Server:  e.Server,
		})
	}
	return v
}

func (h *handler) viewAll(rs []driver.Record) []*jobView {
	out := make([]*jobView, len(rs))
	for i := range rs {
		out[i] = h.view(&rs[i])
	}
	return out
}

func (h *handler) redact(kind string, p Part, v []byte) json.RawMessage {
	if len(v) == 0 {
		return nil
	}
	if h.opt.Redact != nil {
		v = h.opt.Redact(kind, p, v)
	}
	if json.Valid(v) {
		return v
	}
	b, _ := json.Marshal(string(v))
	return b
}

func (h *handler) redactText(kind, s string) string {
	if s == "" || h.opt.Redact == nil {
		return s
	}
	return string(h.opt.Redact(kind, PartError, []byte(s)))
}

type event struct {
	entryView
	Label string
}

func (v *jobView) Timeline() []event {
	out := make([]event, 0, len(v.History)+2)
	var last driver.State
	if n := len(v.History); n > 0 {
		last = v.History[n-1].State
	}
	switch {
	case v.State == driver.Processing:
		out = append(out, event{entryView{At: v.AttemptedAt, State: v.State, Attempt: v.Attempt, Server: v.Server}, "Processing"})
	case !v.FinalizedAt.IsZero() && last != v.State:
		out = append(out, event{entryView{At: v.FinalizedAt, State: v.State, Attempt: v.Attempt, Server: v.Server}, label(v.State)})
	}
	for _, e := range slices.Backward(v.History) {
		out = append(out, event{e, label(e.State)})
	}
	return append(out, event{entryView{At: v.CreatedAt}, "Created"})
}

func (v *jobView) Timeout() time.Duration {
	return time.Duration(v.TimeoutMS) * time.Millisecond
}

func (v *jobView) CanRequeue() bool {
	return requeueable(v.State)
}

func (v *jobView) CanDelete() bool {
	return v.State.Live()
}

func requeueable(s driver.State) bool {
	switch s {
	case driver.Failed, driver.Scheduled, driver.Succeeded, driver.Deleted:
		return true
	}
	return false
}
