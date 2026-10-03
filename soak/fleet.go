package main

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var errStopping = errors.New("fleet is stopping")

type proc struct {
	inst    string
	cmd     *exec.Cmd
	born    time.Time
	signal  syscall.Signal
	pause   time.Duration
	stopped bool
	exited  int64
	err     error
	done    chan struct{}
}

type fleet struct {
	self string
	args []string
	dir  string
	quit chan struct{}
	wg   sync.WaitGroup

	mu       sync.Mutex
	stopping bool
	live     []*proc
	procs    []*proc
	failures []error
	hung     []string
}

func newFleet(self, dir string, o options) *fleet {
	return &fleet{
		self: self,
		args: []string{"-backend", o.backend, "-dsn", o.dsn},
		dir:  dir,
		quit: make(chan struct{}),
		live: make([]*proc, o.workers),
	}
}

func (f *fleet) start() {
	for n := range f.live {
		f.wg.Go(func() { f.supervise(n) })
	}
}

func (f *fleet) supervise(n int) {
	for gen := 0; ; gen++ {
		pause := time.Second
		p, err := f.spawn(n, gen)
		switch {
		case errors.Is(err, errStopping):
			return
		case err != nil:
			f.mu.Lock()
			f.failures = append(f.failures, err)
			f.mu.Unlock()
		default:
			<-p.done
			f.mu.Lock()
			if p.signal != 0 {
				pause = p.pause
			}
			f.mu.Unlock()
		}
		select {
		case <-f.quit:
			return
		case <-time.After(pause):
		}
	}
}

func (f *fleet) spawn(n, gen int) (*proc, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopping {
		return nil, errStopping
	}
	inst := fmt.Sprintf("w%d.%d", n, gen)
	log, err := os.Create(filepath.Join(f.dir, inst+".log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(f.self, append([]string{"-worker", inst}, f.args...)...)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	p := &proc{inst: inst, cmd: cmd, born: time.Now(), done: make(chan struct{})}
	f.live[n] = p
	f.procs = append(f.procs, p)
	go func() {
		p.err = cmd.Wait()
		p.exited = time.Now().UnixMicro()
		log.Close()
		close(p.done)
	}()
	return p, nil
}

func (f *fleet) kill(sig syscall.Signal, pause time.Duration) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopping {
		return ""
	}
	var pick []*proc
	for _, p := range f.live {
		if p != nil && p.signal == 0 && time.Since(p.born) > 2*time.Second && !finished(p) {
			pick = append(pick, p)
		}
	}
	if len(pick) == 0 {
		return ""
	}
	p := pick[rand.IntN(len(pick))]
	p.signal, p.pause = sig, pause
	p.cmd.Process.Signal(sig)
	if sig == syscall.SIGTERM {
		time.AfterFunc(time.Minute, func() { f.reap(p) })
	}
	return p.inst
}

func (f *fleet) reap(p *proc) {
	if finished(p) {
		return
	}
	f.mu.Lock()
	f.hung = append(f.hung, p.inst)
	f.mu.Unlock()
	p.cmd.Process.Kill()
}

func (f *fleet) stop() {
	f.mu.Lock()
	f.stopping = true
	close(f.quit)
	var live []*proc
	for _, p := range f.live {
		if p != nil && !finished(p) {
			p.stopped = true
			live = append(live, p)
		}
	}
	f.mu.Unlock()
	for _, p := range live {
		time.Sleep(time.Until(p.born.Add(time.Second)))
		p.cmd.Process.Signal(syscall.SIGTERM)
		time.AfterFunc(time.Minute, func() { f.reap(p) })
	}
	f.wg.Wait()
	for _, p := range f.procs {
		<-p.done
	}
}

func finished(p *proc) bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}
