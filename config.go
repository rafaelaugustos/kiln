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

const Forever time.Duration = -1

type Pool struct {
	Queues  []string
	Workers int
}

type ServerConfig struct {
	Queues             map[string]int
	Pools              []Pool
	Name               string
	PollInterval       time.Duration
	FetchCooldown      time.Duration
	FetchBatch         int
	ShutdownTimeout    time.Duration
	KillGrace          time.Duration
	Timeout            Timeout
	Backoff            Backoff
	HeartbeatInterval  time.Duration
	DeadAfter          time.Duration
	LeaderTTL          time.Duration
	Retention          Retention
	DisableMaintenance bool
	Logger             *slog.Logger
	UnknownKindTTL     time.Duration
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
		pools = append(pools, Pool{Queues: slices.Clone(p.Queues), Workers: p.Workers})
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
