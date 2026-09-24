package dashboard

import (
	"fmt"
	"net/http"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type queueView struct {
	Name       string        `json:"name"`
	Paused     bool          `json:"paused"`
	Enqueued   int64         `json:"enqueued"`
	Processing int64         `json:"processing"`
	Scheduled  int64         `json:"scheduled"`
	Throttled  int64         `json:"throttled"`
	LatencyMS  int64         `json:"latency_ms"`
	Latency    time.Duration `json:"-"`
}

type queueList struct {
	Queues []queueView `json:"queues"`
}

func queueViews(qs []driver.QueueInfo) []queueView {
	out := make([]queueView, len(qs))
	for i, q := range qs {
		out[i] = queueView{
			Name:       q.Name,
			Paused:     q.Paused,
			Enqueued:   q.Enqueued,
			Processing: q.Processing,
			Scheduled:  q.Scheduled,
			Throttled:  q.Throttled,
			LatencyMS:  q.Latency.Milliseconds(),
			Latency:    q.Latency,
		}
	}
	return out
}

func (h *handler) queues(w http.ResponseWriter, r *http.Request) {
	qs, err := h.store.Queues(r.Context())
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.show(w, r, "queues", "Queues", &queueList{Queues: queueViews(qs)})
}

func (h *handler) queueAction(w http.ResponseWriter, r *http.Request) {
	ctx, name, op := r.Context(), r.PathValue("name"), r.PathValue("op")
	var err error
	switch op {
	case "pause":
		err = h.c.PauseQueue(ctx, name)
	case "resume":
		err = h.c.ResumeQueue(ctx, name)
	default:
		err = fmt.Errorf("%w: action %q", driver.ErrNotFound, op)
	}
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if isAPI(r) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.redirect(w, r, "/queues", done("queue-"+past[op], 0))
}
