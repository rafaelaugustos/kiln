package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"
)

type options struct {
	duration  time.Duration
	workers   int
	rate      float64
	backend   string
	dsn       string
	container string
	worker    string
}

func main() {
	var o options
	flag.DurationVar(&o.duration, "duration", 15*time.Minute, "how long to enqueue jobs and inject faults before draining")
	flag.IntVar(&o.workers, "workers", 4, "number of worker processes")
	flag.Float64Var(&o.rate, "rate", 50, "enqueues per second; a flow enqueue inserts three jobs")
	flag.StringVar(&o.backend, "backend", "postgres", "postgres or mysql")
	flag.StringVar(&o.dsn, "dsn", "", "database to use instead of the backend's local test container")
	flag.StringVar(&o.container, "container", "", "docker container to restart, by default the backend's test container when -dsn is not set; none turns restarts off")
	flag.StringVar(&o.worker, "worker", "", "run as the worker process with this name")
	flag.Parse()
	if err := o.resolve(); err != nil {
		fmt.Fprintln(os.Stderr, "soak:", err)
		os.Exit(2)
	}
	run := soak
	if o.worker != "" {
		run = work
	}
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "soak:", err)
		os.Exit(1)
	}
}

func (o *options) resolve() error {
	var dsn, container string
	switch o.backend {
	case "postgres":
		dsn, container = "postgres://kiln:kiln@localhost:55432/kiln", "kiln-postgres"
	case "mysql":
		dsn, container = "kiln:kiln@tcp(localhost:53306)/kiln", "kiln-mysql"
	default:
		return fmt.Errorf("unknown backend %q", o.backend)
	}
	if o.dsn == "" {
		o.dsn = dsn
		if o.container == "" {
			o.container = container
		}
	}
	if o.container == "none" {
		o.container = ""
	}
	if o.workers < 1 || o.rate <= 0 || o.duration <= 0 {
		return errors.New("-workers, -rate and -duration must be positive")
	}
	return nil
}
