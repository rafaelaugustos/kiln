package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strings"
	"time"
)

const defaultURL = "postgres://kiln:kiln@localhost:55432/kiln?sslmode=disable"

type options struct {
	url       string
	rounds    int
	only      string
	libs      string
	out       string
	bulkJobs  int
	chunk     int
	writers   int
	perWriter int
	drainJobs int
	sleepJobs int
	workers   int
	latJobs   int
	spacing   time.Duration
	cooldown  time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.url, "db", env("KILN_DATABASE_URL", defaultURL), "PostgreSQL connection string (env KILN_DATABASE_URL)")
	flag.IntVar(&o.rounds, "rounds", 5, "measured rounds; one warm-up round runs first and is discarded")
	flag.StringVar(&o.only, "only", "", "comma-separated scenario ids to run, e.g. 1,3 (default all)")
	flag.StringVar(&o.libs, "libs", "", "comma-separated variant ids to run: kiln, river, river_fast, river_tuned (default all)")
	flag.StringVar(&o.out, "json", "", "also write raw per-round results to this file")
	flag.IntVar(&o.bulkJobs, "bulk-jobs", 100_000, "jobs inserted in the bulk insert scenario")
	flag.IntVar(&o.chunk, "chunk", 10_000, "jobs per bulk insert call")
	flag.IntVar(&o.writers, "writers", 8, "goroutines in the concurrent insert scenario")
	flag.IntVar(&o.perWriter, "per-writer", 2000, "single inserts per goroutine")
	flag.IntVar(&o.drainJobs, "drain-jobs", 50_000, "jobs in the no-op drain scenario")
	flag.IntVar(&o.sleepJobs, "sleep-jobs", 20_000, "jobs in the 1ms handler drain scenario")
	flag.IntVar(&o.workers, "workers", 100, "workers in the worker process")
	flag.IntVar(&o.latJobs, "latency-jobs", 500, "measured enqueues in the latency scenario")
	flag.DurationVar(&o.spacing, "spacing", 10*time.Millisecond, "interval between enqueues in the latency scenario")
	flag.DurationVar(&o.cooldown, "river-cooldown", time.Millisecond, "FetchCooldown of the extra tuned River variant in worker scenarios (0 disables it)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, o); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, o options) error {
	h, err := newHarness(ctx, o)
	if err != nil {
		return err
	}
	defer h.close()

	scs := h.scenarios()
	if o.only != "" {
		ids := strings.Split(o.only, ",")
		scs = slices.DeleteFunc(scs, func(s scenario) bool { return !slices.Contains(ids, s.id) })
	}
	if o.libs != "" {
		ids := strings.Split(o.libs, ",")
		for i := range scs {
			scs[i].variants = slices.DeleteFunc(slices.Clone(scs[i].variants), func(v variant) bool { return !slices.Contains(ids, v.id) })
		}
		scs = slices.DeleteFunc(scs, func(s scenario) bool { return len(s.variants) == 0 })
	}
	if len(scs) == 0 {
		return fmt.Errorf("no scenario matches -only %q -libs %q", o.only, o.libs)
	}
	if err := h.describe(ctx, os.Stdout); err != nil {
		return err
	}

	res := make(results)
	began := time.Now()
	for r := 0; r <= o.rounds; r++ {
		label := fmt.Sprintf("round %d/%d", r, o.rounds)
		if r == 0 {
			label = "warm-up"
		}
		for _, sc := range scs {
			for _, v := range order(sc.variants, r) {
				out, err := h.measure(ctx, sc, v)
				if err != nil {
					return fmt.Errorf("%s, scenario %s, %s: %w", label, sc.id, v.name, err)
				}
				fmt.Fprintf(os.Stderr, "%-11s %s %-32s %s\n", label, sc.id, v.name, out.format(sc.metrics))
				if r > 0 {
					res.add(sc.id, v.name, out)
				}
			}
		}
	}
	fmt.Fprintf(os.Stderr, "total runtime %s\n", time.Since(began).Round(time.Second))

	report(os.Stdout, scs, res)
	if o.out != "" {
		return res.save(o.out, scs)
	}
	return nil
}

func order(vs []variant, round int) []variant {
	vs = slices.Clone(vs)
	if round > 0 && round%2 == 0 {
		slices.Reverse(vs)
	}
	return vs
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
