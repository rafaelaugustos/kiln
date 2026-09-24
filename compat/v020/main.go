package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/mysqlstore"
	"github.com/rafaelaugustos/kiln/pgstore"
	"github.com/rafaelaugustos/kiln/sqlitestore"
	_ "modernc.org/sqlite"
)

const (
	version = "v0.2.0"
	pragmas = "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate"
)

type config struct {
	backend  string
	dsn      string
	schema   string
	prefix   string
	role     string
	name     string
	workers  int
	poll     time.Duration
	backoff  time.Duration
	duration time.Duration
	commands bool
	plan     plan
}

type summary struct {
	Version   string         `json:"version"`
	Role      string         `json:"role"`
	Server    string         `json:"server,omitempty"`
	Processed map[string]int `json:"processed"`
	Calls     map[string]int `json:"calls"`
	Runs      map[int64]int  `json:"runs"`
	Stats     *kiln.Stats    `json:"stats,omitempty"`
	Errors    []string       `json:"errors"`
}

type message struct {
	Result  *result  `json:"result,omitempty"`
	Summary *summary `json:"summary,omitempty"`
}

func main() {
	c := config{plan: plan{Delay: 5 * time.Second}}
	flag.StringVar(&c.backend, "backend", "postgres", "postgres, mysql or sqlite")
	flag.StringVar(&c.dsn, "dsn", "", "connection string, or the database file for sqlite")
	flag.StringVar(&c.schema, "schema", "kiln", "postgres schema")
	flag.StringVar(&c.prefix, "prefix", "kiln_", "mysql and sqlite table prefix")
	flag.StringVar(&c.role, "role", "both", "worker, enqueue or both")
	flag.StringVar(&c.name, "name", "compat-v020", "server name")
	flag.IntVar(&c.workers, "workers", 8, "worker goroutines")
	flag.DurationVar(&c.poll, "poll", 100*time.Millisecond, "poll interval")
	flag.DurationVar(&c.backoff, "backoff", 150*time.Millisecond, "delay before a retry")
	flag.DurationVar(&c.duration, "duration", 0, "how long to run; 0 runs until a signal or, with -commands, the end of stdin")
	flag.BoolVar(&c.commands, "commands", false, "read more plans from stdin, one JSON object per line, and answer each with a result line")
	flag.IntVar(&c.plan.Echo, "echo", 0, "compat.echo jobs to enqueue")
	flag.IntVar(&c.plan.Fail, "fail", 0, "compat.fail_once jobs to enqueue")
	flag.IntVar(&c.plan.Handoff, "handoff", 0, "compat.handoff jobs that must start on this version")
	flag.IntVar(&c.plan.Limited, "limited", 0, "compat.limited jobs to enqueue")
	flag.IntVar(&c.plan.Flows, "flows", 0, "flows of four jobs to enqueue")
	flag.IntVar(&c.plan.Batches, "batches", 0, "batches of three jobs and a continuation to start")
	flag.IntVar(&c.plan.Delayed, "delayed", 0, "compat.echo jobs to enqueue with -delay")
	flag.DurationVar(&c.plan.Delay, "delay", c.plan.Delay, "delay of -delayed and -unique jobs")
	flag.Func("after", "comma separated job ids, each gets a compat.child continuation", func(s string) error {
		for f := range strings.SplitSeq(s, ",") {
			id, err := strconv.ParseInt(strings.TrimSpace(f), 10, 64)
			if err != nil {
				return err
			}
			c.plan.After = append(c.plan.After, id)
		}
		return nil
	})
	flag.Func("unique", "comma separated keys held while their job is live", func(s string) error {
		c.plan.Unique = append(c.plan.Unique, strings.Split(s, ",")...)
		return nil
	})
	flag.Func("window", "comma separated keys held for an hour", func(s string) error {
		c.plan.Window = append(c.plan.Window, strings.Split(s, ",")...)
		return nil
	})
	flag.Parse()
	os.Exit(run(c))
}

func run(c config) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	out := &output{enc: json.NewEncoder(os.Stdout)}
	errs := &errlog{}
	sum := &summary{Version: version, Role: c.role}
	if err := operate(ctx, c, out, errs, sum); err != nil {
		errs.add(err.Error())
	}
	sum.Errors = errs.all()
	out.send(message{Summary: sum})
	if len(sum.Errors) > 0 {
		return 1
	}
	return 0
}

func operate(ctx context.Context, c config, out *output, errs *errlog, sum *summary) error {
	if c.role != "worker" && c.role != "enqueue" && c.role != "both" {
		return fmt.Errorf("unknown role %q", c.role)
	}
	st, closeStore, err := open(ctx, c)
	if err != nil {
		return err
	}
	defer closeStore()
	client := kiln.NewClient(st)
	w := newWorker()
	defer w.report(sum)
	if c.role != "enqueue" {
		srv, err := kiln.NewServer(client, w.mux(), kiln.ServerConfig{
			Queues:            map[string]int{kiln.DefaultQueue: c.workers},
			Name:              c.name,
			PollInterval:      c.poll,
			HeartbeatInterval: 250 * time.Millisecond,
			KillGrace:         250 * time.Millisecond,
			DeadAfter:         6 * time.Second,
			LeaderTTL:         time.Second,
			ShutdownTimeout:   5 * time.Second,
			Backoff:           kiln.Constant(c.backoff),
			Logger:            slog.New(slog.NewTextHandler(errs, &slog.HandlerOptions{Level: slog.LevelWarn})),
		})
		if err != nil {
			return err
		}
		life, halt := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- srv.Run(life) }()
		defer func() {
			halt()
			if err := <-done; err != nil {
				errs.add("run: " + err.Error())
			}
			sum.Server = srv.ID()
			s := srv.Stats()
			sum.Stats = &s
		}()
	}
	if c.role != "worker" {
		out.send(message{Result: enqueue(ctx, client, c.plan, errs)})
	}
	if c.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.duration)
		defer cancel()
	}
	switch {
	case c.commands:
		listen(ctx, client, os.Stdin, out, errs)
	case c.role != "enqueue":
		<-ctx.Done()
	}
	return nil
}

func listen(ctx context.Context, c *kiln.Client, r io.Reader, out *output, errs *errlog) {
	eof := make(chan struct{})
	go func() {
		defer close(eof)
		sc := bufio.NewScanner(r)
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			var p plan
			if err := json.Unmarshal(sc.Bytes(), &p); err != nil {
				errs.add("command: " + err.Error())
				out.send(message{Result: &result{Err: err.Error()}})
				continue
			}
			out.send(message{Result: enqueue(ctx, c, p, errs)})
		}
	}()
	select {
	case <-eof:
	case <-ctx.Done():
	}
}

func open(ctx context.Context, c config) (driver.Store, func(), error) {
	switch c.backend {
	case "postgres":
		pool, err := pgxpool.New(ctx, c.dsn)
		if err != nil {
			return nil, nil, err
		}
		st, err := pgstore.New(ctx, pool, pgstore.Schema(c.schema))
		if err != nil {
			pool.Close()
			return nil, nil, err
		}
		return st, func() { st.Close(); pool.Close() }, nil
	case "mysql":
		cfg, err := mysql.ParseDSN(c.dsn)
		if err != nil {
			return nil, nil, err
		}
		conn, err := mysql.NewConnector(cfg)
		if err != nil {
			return nil, nil, err
		}
		db := sql.OpenDB(conn)
		db.SetMaxIdleConns(16)
		st, err := mysqlstore.New(ctx, db, mysqlstore.Prefix(c.prefix))
		if err != nil {
			db.Close()
			return nil, nil, err
		}
		return st, func() { st.Close(); db.Close() }, nil
	case "sqlite":
		db, err := sql.Open("sqlite", "file:"+c.dsn+pragmas)
		if err != nil {
			return nil, nil, err
		}
		st, err := sqlitestore.New(ctx, db, sqlitestore.Prefix(c.prefix))
		if err != nil {
			db.Close()
			return nil, nil, err
		}
		return st, func() { st.Close(); db.Close() }, nil
	}
	return nil, nil, fmt.Errorf("unknown backend %q", c.backend)
}

type output struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (o *output) send(m message) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.enc.Encode(m)
}

type errlog struct {
	mu    sync.Mutex
	lines []string
}

func (l *errlog) Write(p []byte) (int, error) {
	l.add(strings.TrimSpace(string(p)))
	return len(p), nil
}

func (l *errlog) add(s string) {
	l.mu.Lock()
	l.lines = append(l.lines, s)
	l.mu.Unlock()
}

func (l *errlog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.lines...)
}
