package kiln

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	maxLines       = 1000
	maxLine        = 4 << 10
	consoleEvery   = 250 * time.Millisecond
	consoleTimeout = 2 * time.Second
	truncated      = "kiln: console truncated"
)

func (r *run) logf(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.closed || r.logged > maxLines:
		return
	case r.logged == maxLines:
		line = truncated
	default:
		line = clean(strings.TrimSuffix(line, "\n"), maxLine)
	}
	r.logged++
	r.lines = append(r.lines, line)
	r.arm()
}

func (r *run) setProgress(p int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.set && r.progress == p {
		return
	}
	r.progress, r.set, r.moved = p, true, true
	r.arm()
}

func (r *run) arm() {
	if r.armed || r.srv == nil {
		return
	}
	r.armed = true
	if r.timer == nil {
		r.timer = time.AfterFunc(consoleEvery, r.tick)
		return
	}
	r.timer.Reset(consoleEvery)
}

func (r *run) tick() {
	r.flush()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed = false
	if !r.closed && (len(r.lines) > 0 || r.moved) {
		r.arm()
	}
}

func (r *run) end() {
	r.mu.Lock()
	idle := !r.armed && len(r.lines) == 0 && !r.moved
	r.closed = true
	if r.timer != nil {
		r.timer.Stop()
	}
	r.mu.Unlock()
	if !idle {
		r.flush()
	}
}

func (r *run) flush() {
	r.writing.Lock()
	defer r.writing.Unlock()
	r.mu.Lock()
	lines, p := r.lines, -1
	if r.moved {
		p = r.progress
	}
	r.lines, r.moved = nil, false
	r.mu.Unlock()
	if len(lines) == 0 && p < 0 {
		return
	}
	ctx, cancel := context.WithTimeout(r.srv.base, consoleTimeout)
	err := r.srv.console.WriteConsole(ctx, r.ref, lines, p)
	cancel()
	if err == nil {
		return
	}
	if errors.Is(err, driver.ErrLost) {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
	}
	r.srv.fail("console", err)
}
