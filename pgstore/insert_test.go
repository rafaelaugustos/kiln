package pgstore

import (
	"context"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestInsertDelayIsScheduled(t *testing.T) {
	t.Parallel()
	s := open(t)
	parent := insert(t, s, job("p"))[0].ID
	j := claim(t, s, 1)[0]
	finish(t, s, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})

	tiny := func(p *driver.InsertParams) { p.Delay = time.Microsecond }
	jobs := make([]driver.InsertParams, 500)
	for i := range jobs {
		jobs[i] = job("a", tiny)
	}
	jobs[0].LimitKey, jobs[0].LimitMax = "l", 1
	for name, batch := range map[string][]driver.InsertParams{
		"plain":  jobs,
		"linked": {job("c", tiny, func(p *driver.InsertParams) { p.Parents = []driver.Parent{{ID: parent, On: driver.OnSucceeded}} })},
	} {
		for i, r := range insert(t, s, batch...) {
			if r.State != driver.Scheduled {
				t.Fatalf("%s job %d state %s, want scheduled", name, i, r.State)
			}
		}
	}
	p, err := s.Promote(context.Background(), 1000)
	if err != nil || p.Count != 501 {
		t.Fatalf("promoted %+v %v, want 501", p, err)
	}
}
