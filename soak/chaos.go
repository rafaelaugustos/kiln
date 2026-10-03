package main

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

func (h *harness) chaos(ctx context.Context) {
	var wg sync.WaitGroup
	every := func(lo, hi time.Duration, f func()) {
		wg.Go(func() {
			for {
				t := time.NewTimer(lo + rand.N(hi-lo))
				select {
				case <-ctx.Done():
					t.Stop()
					return
				case <-t.C:
				}
				f()
			}
		})
	}
	every(15*time.Second, 45*time.Second, func() {
		pause := 2*time.Second + rand.N(6*time.Second)
		if inst := h.fleet.kill(syscall.SIGKILL, pause); inst != "" {
			h.kills.Add(1)
			h.event("sigkill %s, back in %s", inst, pause.Round(time.Second))
		}
	})
	every(20*time.Second, time.Minute, func() {
		pause := time.Second + rand.N(3*time.Second)
		if inst := h.fleet.kill(syscall.SIGTERM, pause); inst != "" {
			h.terms.Add(1)
			h.event("sigterm %s", inst)
		}
	})
	if h.o.container != "" {
		every(3*time.Minute, 5*time.Minute, h.restart)
	}
	wg.Wait()
}

func (h *harness) restart() {
	start := time.Now()
	out, err := exec.Command("docker", "restart", h.o.container).CombinedOutput()
	if err != nil {
		h.event("docker restart %s: %v %s", h.o.container, err, bytes.TrimSpace(out))
		return
	}
	h.restarts.Add(1)
	h.event("restarted %s in %s", h.o.container, time.Since(start).Round(100*time.Millisecond))
}
