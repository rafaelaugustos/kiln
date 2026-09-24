package kiln

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	version  = "kiln/v0.1.0"
	logEvery = 10 * time.Second
)

type Server struct {
	client   *Client
	store    driver.Store
	mux      *Mux
	cfg      ServerConfig
	log      *slog.Logger
	prods    []*producer
	byQueue  map[string]*producer
	queues   []string
	capacity int
	comp     *completer
	pid      int

	started   atomic.Bool
	id        string
	startedAt time.Time
	base      context.Context
	fencer    *time.Timer
	fenceMu   sync.Mutex

	mu    sync.Mutex
	tasks map[int64]*task
	wg    sync.WaitGroup

	seq        atomic.Uint64
	lastBeat   atomic.Int64
	fenced     atomic.Bool
	paused     atomic.Pointer[[]string]
	leader     atomic.Bool
	since      atomic.Int64
	listening  atomic.Bool
	beatNow    chan struct{}
	promoteNow chan struct{}
	stats      counters

	logMu  sync.Mutex
	logged map[string]time.Time
}

func NewServer(c *Client, m *Mux, cfg ServerConfig) (*Server, error) {
	if c == nil || m == nil {
		return nil, fmt.Errorf("%w: nil client or mux", ErrInvalid)
	}
	cfg, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	if len(m.freeze()) == 0 {
		return nil, fmt.Errorf("%w: mux has no handlers", ErrInvalid)
	}
	s := &Server{
		client:     c,
		store:      c.store,
		mux:        m,
		cfg:        cfg,
		log:        cfg.Logger,
		byQueue:    make(map[string]*producer),
		pid:        os.Getpid(),
		tasks:      make(map[int64]*task),
		beatNow:    make(chan struct{}, 1),
		promoteNow: make(chan struct{}, 1),
		logged:     make(map[string]time.Time),
	}
	for _, p := range cfg.Pools {
		pr := newProducer(s, p)
		s.prods = append(s.prods, pr)
		for _, q := range p.Queues {
			s.byQueue[q] = pr
			s.queues = append(s.queues, q)
		}
		s.capacity += p.Workers
	}
	s.comp = newCompleter(s, max(s.capacity, flushSize))
	return s, nil
}

func (s *Server) ID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

func (s *Server) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("kiln: server already started")
	}
	s.mu.Lock()
	s.id = newID(s.cfg.Name, s.pid)
	s.mu.Unlock()
	s.base = context.WithoutCancel(ctx)
	s.startedAt = time.Now()
	s.fencer = time.AfterFunc(s.fenceAfter(), s.fence)
	defer s.fencer.Stop()
	if !s.join(ctx) {
		return nil
	}

	life, stop := context.WithCancel(s.base)
	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		s.heartbeat(life)
	}()
	if n, ok := s.store.(driver.Notifier); ok {
		bg.Add(1)
		go func() {
			defer bg.Done()
			s.listen(life, n)
		}()
	}
	go s.comp.run()

	var work, lead sync.WaitGroup
	for _, p := range s.prods {
		work.Add(1)
		go func() {
			defer work.Done()
			p.run(ctx)
		}()
	}
	work.Add(1)
	go func() {
		defer work.Done()
		s.promote(ctx)
	}()
	if !s.cfg.DisableMaintenance {
		lead.Add(1)
		go func() {
			defer lead.Done()
			s.lead(ctx)
		}()
	}
	s.log.Info("kiln: server started", "id", s.id, "queues", s.queues, "workers", s.capacity)

	<-ctx.Done()
	work.Wait()
	lead.Wait()
	s.drain()
	s.comp.stop()
	stop()
	bg.Wait()
	uctx, cancel := context.WithTimeout(s.base, 5*time.Second)
	if err := s.store.Unregister(uctx, s.id); err != nil {
		s.fail("unregister", err)
	}
	cancel()
	s.lastBeat.Store(0)
	s.log.Info("kiln: server stopped", "id", s.id)
	return nil
}

func (s *Server) drain() {
	if s.await(s.cfg.ShutdownTimeout) {
		return
	}
	s.cancelAll(ErrShutdown)
	if s.await(s.cfg.KillGrace) {
		return
	}
	s.abandon()
	s.await(s.cfg.KillGrace)
}

func (s *Server) await(d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (s *Server) listen(ctx context.Context, n driver.Notifier) {
	delay := 100 * time.Millisecond
	for {
		err := n.Subscribe(ctx, s.event)
		s.listening.Store(false)
		if ctx.Err() != nil {
			return
		}
		s.fail("subscribe", err)
		if !sleep(ctx, delay) {
			return
		}
		delay = min(2*delay, 30*time.Second)
	}
}

func (s *Server) event(e driver.Event) {
	switch e.Kind {
	case driver.JobsReady:
		if p := s.byQueue[e.Queue]; p != nil {
			p.poke()
		}
	case driver.CancelRequested:
		s.mu.Lock()
		if t := s.tasks[e.ID]; t != nil && !t.settled.Load() {
			t.cancel(ErrCanceled)
		}
		s.mu.Unlock()
	case driver.Resync, driver.QueueChanged:
		if e.Kind == driver.Resync {
			s.listening.Store(true)
		}
		signal(s.beatNow)
		s.pokeAll()
	}
}

func (s *Server) fail(op string, err error) {
	if errors.Is(err, context.Canceled) || s.muted(op) {
		return
	}
	s.log.Error("kiln: store error", "id", s.id, "op", op, "err", err)
}

func (s *Server) muted(op string) bool {
	now := time.Now()
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if now.Sub(s.logged[op]) < logEvery {
		return true
	}
	s.logged[op] = now
	return false
}

func newID(name string, pid int) string {
	var b [8]byte
	rand.Read(b[:])
	return name + ":" + strconv.Itoa(pid) + ":" + hex.EncodeToString(b[:])
}
