package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type recurringView struct {
	ID        string          `json:"id"`
	Spec      string          `json:"spec"`
	Location  string          `json:"location"`
	Kind      string          `json:"kind"`
	Queue     string          `json:"queue"`
	Args      json.RawMessage `json:"args,omitempty"`
	Misfire   string          `json:"misfire"`
	Overlap   bool            `json:"overlap"`
	Paused    bool            `json:"paused"`
	NextRunAt time.Time       `json:"next_run_at,omitzero"`
	LastRunAt time.Time       `json:"last_run_at,omitzero"`
	LastJobID int64           `json:"last_job_id,omitempty"`
	CreatedAt time.Time       `json:"created_at,omitzero"`
	UpdatedAt time.Time       `json:"updated_at,omitzero"`
}

type recurringList struct {
	Recurring []recurringView `json:"recurring"`
}

func misfire(m driver.Misfire) string {
	switch m {
	case driver.MisfireAll:
		return "all"
	case driver.MisfireSkip:
		return "skip"
	}
	return "once"
}

func (h *handler) recurring(w http.ResponseWriter, r *http.Request) {
	rs, err := h.store.Recurrings(r.Context())
	if err != nil {
		h.fail(w, r, err)
		return
	}
	l := &recurringList{Recurring: make([]recurringView, len(rs))}
	for i, rc := range rs {
		t := &rc.Template
		l.Recurring[i] = recurringView{
			ID:        rc.ID,
			Spec:      rc.Spec,
			Location:  rc.Location,
			Kind:      t.Kind,
			Queue:     t.Queue,
			Args:      h.redact(t.Kind, PartArgs, t.Args),
			Misfire:   misfire(rc.Misfire),
			Overlap:   rc.Overlap,
			Paused:    rc.Paused,
			NextRunAt: rc.NextRunAt,
			LastRunAt: rc.LastRunAt,
			LastJobID: rc.LastJobID,
			CreatedAt: rc.CreatedAt,
			UpdatedAt: rc.UpdatedAt,
		}
	}
	h.show(w, r, "recurring", "Recurring", l)
}

func (h *handler) recurringAction(w http.ResponseWriter, r *http.Request) {
	ctx, id, op := r.Context(), r.PathValue("id"), r.PathValue("op")
	var (
		job int64
		err error
	)
	switch op {
	case "trigger":
		job, err = h.c.TriggerRecurring(ctx, id)
	case "pause":
		err = h.c.PauseRecurring(ctx, id)
	case "resume":
		err = h.c.ResumeRecurring(ctx, id)
	case "remove":
		err = h.c.RemoveRecurring(ctx, id)
	default:
		err = fmt.Errorf("%w: action %q", driver.ErrNotFound, op)
	}
	if err != nil {
		h.fail(w, r, err)
		return
	}
	switch {
	case !isAPI(r) && op == "trigger":
		h.redirect(w, r, "/recurring", done("triggered", job))
	case !isAPI(r):
		h.redirect(w, r, "/recurring", done("recurring-"+past[op], 0))
	case op == "trigger":
		writeJSON(w, http.StatusOK, map[string]int64{"id": job})
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
