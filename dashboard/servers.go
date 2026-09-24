package dashboard

import (
	"net/http"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const staleAfter = time.Minute

type serverView struct {
	ID          string        `json:"id"`
	Host        string        `json:"host,omitempty"`
	PID         int           `json:"pid,omitempty"`
	Version     string        `json:"version,omitempty"`
	Queues      []string      `json:"queues"`
	Kinds       []string      `json:"kinds,omitempty"`
	Workers     int           `json:"workers"`
	Running     int           `json:"running"`
	StartedAt   time.Time     `json:"started_at,omitzero"`
	HeartbeatAt time.Time     `json:"heartbeat_at"`
	AgeMS       int64         `json:"heartbeat_age_ms"`
	Age         time.Duration `json:"-"`
}

type serverList struct {
	Servers []serverView `json:"servers"`
}

func (s *serverView) Stale() bool { return s.Age > staleAfter }

func (s *serverView) Load() float64 {
	if s.Workers <= 0 {
		return 0
	}
	return min(float64(s.Running)/float64(s.Workers)*100, 100)
}

func serverViews(ss []driver.ServerInfo) []serverView {
	out := make([]serverView, len(ss))
	for i, s := range ss {
		age := max(time.Since(s.HeartbeatAt), 0)
		out[i] = serverView{
			ID:          s.ID,
			Host:        s.Host,
			PID:         s.PID,
			Version:     s.Version,
			Queues:      s.Queues,
			Kinds:       s.Kinds,
			Workers:     s.Workers,
			Running:     s.Running,
			StartedAt:   s.StartedAt,
			HeartbeatAt: s.HeartbeatAt,
			AgeMS:       age.Milliseconds(),
			Age:         age,
		}
	}
	return out
}

func (h *handler) servers(w http.ResponseWriter, r *http.Request) {
	ss, err := h.store.Servers(r.Context())
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.show(w, r, "servers", "Servers", &serverList{Servers: serverViews(ss)})
}
