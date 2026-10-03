package kiln

import (
	"cmp"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"time"
)

const (
	defaultWorkers        = 10
	defaultPoll           = time.Second
	defaultCooldown       = 5 * time.Millisecond
	defaultShutdown       = 30 * time.Second
	defaultKillGrace      = 5 * time.Second
	defaultTimeout        = 30 * time.Minute
	defaultHeartbeat      = 5 * time.Second
	defaultDeadAfter      = time.Minute
	defaultLeaderTTL      = 15 * time.Second
	defaultRetention      = 24 * time.Hour
	defaultUnknownKindTTL = 24 * time.Hour
	maxFetchBatch         = 200
	minInterval           = time.Millisecond
)

// Forever, as a [Retention] duration, turns off pruning for the jobs of that state.
const Forever time.Duration = -1

// Pool is a set of workers shared by one or more queues. It takes jobs from Queues in order,
// moving to a queue only when the ones before it have no job ready, unless Weights is set.
type Pool struct {
	Queues  []string
	Workers int // at least 1

	// Weights, when not empty, gives each queue a share of the pool instead of a place in line:
	// every fetch orders the queues by a weighted random draw, so that under load each queue gets
	// about its share of the jobs the pool claims and none starves, while the share of a queue
	// with no job ready goes to the others. A queue missing from Weights has weight 1. [NewServer]
	// returns [ErrInvalid] for a weight below 1 or for a queue that is not in Queues.
	Weights map[string]int
}

// ServerConfig configures a [Server]. The zero value runs 10 workers on [DefaultQueue], with the
// defaults given for each field. [NewServer] returns [ErrInvalid] when a field breaks its rules:
// durations must not be negative unless the field says otherwise, and PollInterval,
// HeartbeatInterval and LeaderTTL must be at least a millisecond.
type ServerConfig struct {
	// Queues maps queue names to worker counts; each entry becomes a pool of its own.
	Queues map[string]int

	// Pools lists pools of workers, each serving its queues in order or by weight. A queue can
	// belong to one pool only, including the pools made from Queues.
	Pools []Pool

	// Name is reported as the host of the server and starts its ID. Empty means the machine's host
	// name.
	Name string

	// PollInterval is how often an idle pool looks for jobs when nothing wakes it. Zero means 1s.
	PollInterval time.Duration

	// FetchCooldown is how long a pool waits after a fetch before the next one, so that a burst of
	// wakeups ends in a single claim; a pool draining a backlog does not wait. Zero means 5ms and
	// a negative value turns the wait off.
	FetchCooldown time.Duration

	// FetchBatch caps the jobs a pool claims in one call. Zero means the pool's worker count, and
	// it is never more than that or 200.
	FetchBatch int

	// ShutdownTimeout is how long Run waits for running jobs once its context is canceled, before
	// it cancels them with [ErrShutdown]. Zero means 30s.
	ShutdownTimeout time.Duration

	// KillGrace is how long Run then waits for handlers to return. Jobs still running after it go
	// back to their queues without using up the attempt; their handlers keep running, but what
	// they return is ignored. Zero means 5s.
	KillGrace time.Duration

	// Timeout is the time limit of an attempt when neither the job nor its kind sets one. Zero
	// means 30m, and [NoTimeout] means none.
	Timeout Timeout

	// Backoff computes the retry delays of kinds registered without one. Nil means the formula of
	// Hangfire: (attempt-1)^4 + 15 + rand(30)*attempt seconds, rand(30) being a random whole
	// number below 30.
	Backoff Backoff

	// HeartbeatInterval is how often the server reports to the store. Heartbeats also bring
	// cancellations and paused queues, and confirm that the server still holds its running jobs.
	// Zero means 5s.
	HeartbeatInterval time.Duration

	// DeadAfter is how long a server can go without a heartbeat before its running jobs are
	// rescued, and so retried elsewhere. It must be at least 3*HeartbeatInterval + KillGrace + 5s.
	// A server that has not managed to heartbeat for DeadAfter - HeartbeatInterval - KillGrace
	// fences itself: it stops fetching and cancels its running jobs until a heartbeat succeeds.
	// Zero means 60s.
	DeadAfter time.Duration

	// LeaderTTL is the lease of the leader, the one server at a time that fires recurring jobs,
	// rescues the jobs of dead servers, and sweeps and prunes the store. The leader renews it every
	// LeaderTTL/3. Zero means 15s.
	LeaderTTL time.Duration

	// Retention is how long finished jobs are kept before the leader prunes them. A zero field
	// means 24h for Succeeded and Deleted, and [Forever] for Failed; a negative one keeps the jobs
	// forever.
	Retention Retention

	// DisableMaintenance keeps the server out of the leader election, so it never fires recurring
	// jobs, rescues jobs, sweeps or prunes. At least one server sharing the store must leave it
	// off.
	DisableMaintenance bool

	// Logger receives the server's errors and lifecycle events, and one record per job at debug
	// level. Nil discards them.
	Logger *slog.Logger

	// UnknownKindTTL bounds how long a claimed job whose kind has no handler is put back, a minute
	// later and without using up an attempt, before it fails instead. It counts from the job's
	// creation. Zero means 24h.
	UnknownKindTTL time.Duration
}

func (c ServerConfig) resolve() (ServerConfig, error) {
	for _, d := range []struct {
		name  string
		v     time.Duration
		floor time.Duration
	}{
		{"PollInterval", c.PollInterval, minInterval},
		{"ShutdownTimeout", c.ShutdownTimeout, 0},
		{"KillGrace", c.KillGrace, 0},
		{"HeartbeatInterval", c.HeartbeatInterval, minInterval},
		{"DeadAfter", c.DeadAfter, 0},
		{"LeaderTTL", c.LeaderTTL, minInterval},
		{"UnknownKindTTL", c.UnknownKindTTL, 0},
	} {
		switch {
		case d.v < 0:
			return c, fmt.Errorf("%w: %s is negative", ErrInvalid, d.name)
		case d.v > 0 && d.v < d.floor:
			return c, fmt.Errorf("%w: %s %s is shorter than %s", ErrInvalid, d.name, d.v, d.floor)
		}
	}
	if c.FetchBatch < 0 {
		return c, fmt.Errorf("%w: FetchBatch is negative", ErrInvalid)
	}
	if c.Name == "" {
		c.Name = hostname()
	}
	c.PollInterval = cmp.Or(c.PollInterval, defaultPoll)
	c.FetchCooldown = max(cmp.Or(c.FetchCooldown, defaultCooldown), 0)
	c.ShutdownTimeout = cmp.Or(c.ShutdownTimeout, defaultShutdown)
	c.KillGrace = cmp.Or(c.KillGrace, defaultKillGrace)
	c.Timeout = cmp.Or(c.Timeout, Timeout(defaultTimeout))
	c.HeartbeatInterval = cmp.Or(c.HeartbeatInterval, defaultHeartbeat)
	c.DeadAfter = cmp.Or(c.DeadAfter, defaultDeadAfter)
	c.LeaderTTL = cmp.Or(c.LeaderTTL, defaultLeaderTTL)
	c.UnknownKindTTL = cmp.Or(c.UnknownKindTTL, defaultUnknownKindTTL)
	c.Retention.Succeeded = cmp.Or(c.Retention.Succeeded, defaultRetention)
	c.Retention.Deleted = cmp.Or(c.Retention.Deleted, defaultRetention)
	c.Retention.Failed = cmp.Or(c.Retention.Failed, Forever)
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	if least := 3*c.HeartbeatInterval + c.KillGrace + 5*time.Second; c.DeadAfter < least {
		return c, fmt.Errorf("%w: DeadAfter %s must be at least 3*HeartbeatInterval + KillGrace + 5s = %s", ErrInvalid, c.DeadAfter, least)
	}
	pools, err := c.pools()
	if err != nil {
		return c, err
	}
	c.Pools, c.Queues = pools, nil
	return c, nil
}

func (c *ServerConfig) pools() ([]Pool, error) {
	pools := make([]Pool, 0, len(c.Pools)+len(c.Queues))
	for _, p := range c.Pools {
		pools = append(pools, Pool{Queues: slices.Clone(p.Queues), Workers: p.Workers, Weights: maps.Clone(p.Weights)})
	}
	for _, q := range slices.Sorted(maps.Keys(c.Queues)) {
		pools = append(pools, Pool{Queues: []string{q}, Workers: c.Queues[q]})
	}
	if len(pools) == 0 {
		pools = append(pools, Pool{Queues: []string{DefaultQueue}, Workers: defaultWorkers})
	}
	seen := make(map[string]bool)
	for _, p := range pools {
		if p.Workers < 1 {
			return nil, fmt.Errorf("%w: pool %v has %d workers", ErrInvalid, p.Queues, p.Workers)
		}
		if len(p.Queues) == 0 {
			return nil, fmt.Errorf("%w: pool without queues", ErrInvalid)
		}
		for _, q := range p.Queues {
			if !validQueue(q) {
				return nil, fmt.Errorf("%w: queue %q", ErrInvalid, q)
			}
			if seen[q] {
				return nil, fmt.Errorf("%w: queue %q is in more than one pool", ErrInvalid, q)
			}
			seen[q] = true
		}
		for q, w := range p.Weights {
			if w < 1 {
				return nil, fmt.Errorf("%w: queue %q has weight %d", ErrInvalid, q, w)
			}
			if !slices.Contains(p.Queues, q) {
				return nil, fmt.Errorf("%w: weight for queue %q outside pool %v", ErrInvalid, q, p.Queues)
			}
		}
	}
	return pools, nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "localhost"
	}
	return h
}
