package redisstore

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/redis/go-redis/v9"
)

var (
	_ driver.Store       = (*Store)(nil)
	_ driver.Notifier    = (*Store)(nil)
	_ driver.LimitReader = (*Store)(nil)
	_ driver.Console     = (*Store)(nil)
)

//go:embed lua/*.lua
var sources embed.FS

const format = 1

var (
	body    = source()
	library = "kiln_" + digest(body)
	module  = "#!lua name=" + library + "\n" + body + "\nredis.register_function('" + library + "', run)\n"
	script  = redis.NewScript(body + "\nreturn run(KEYS, ARGV)\n")
)

func digest(src string) string {
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:6])
}

func source() string {
	entries, err := sources.ReadDir("lua")
	if err != nil {
		panic(err)
	}
	var b strings.Builder
	for _, e := range entries {
		src, err := sources.ReadFile("lua/" + e.Name())
		if err != nil {
			panic(err)
		}
		b.Write(src)
		b.WriteByte('\n')
	}
	return b.String()
}

// Store is a [driver.Store] that keeps jobs in Redis, and also a [driver.Notifier],
// [driver.Transactor], [driver.LimitReader] and [driver.Console]. It is safe for concurrent use.
// Every method runs as one call of a function the store loads into the server, so each call takes
// effect all at once and no call ever waits for another's locks.
type Store struct {
	c      redis.UniversalClient
	prefix string
	keys   []string
	idle   time.Duration
	hold   time.Duration
	plain  atomic.Bool
}

// New returns a store on c, after checking that the server is one the store can run on: Redis 7
// or later, and not set to evict keys under memory pressure, which would lose jobs. It loads the
// store's code into the server as a function library named after its contents, so servers and
// clients of different kiln versions can share a Redis during a rolling deploy; on a server that
// refuses FUNCTION LOAD the store runs the same code through EVALSHA instead, at a higher cost per
// call.
func New(ctx context.Context, c redis.UniversalClient, opts ...Option) (*Store, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	s := &Store{c: c, prefix: "{" + cfg.prefix + "}:", idle: 5 * time.Second, hold: time.Minute}
	s.keys = []string{s.prefix + "id"}
	if err := s.check(ctx, cfg); err != nil {
		return nil, err
	}
	if err := s.load(ctx); err != nil {
		var refused redis.Error
		if !errors.As(err, &refused) {
			return nil, fmt.Errorf("kiln: load functions: %w", err)
		}
		s.plain.Store(true)
	}
	return s, nil
}

func (s *Store) load(ctx context.Context) error {
	if cc, ok := s.c.(*redis.ClusterClient); ok {
		return cc.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error {
			return c.FunctionLoadReplace(ctx, module).Err()
		})
	}
	return s.c.FunctionLoadReplace(ctx, module).Err()
}

func (s *Store) run(ctx context.Context, op string, args []byte) (string, error) {
	if s.plain.Load() {
		return script.Run(ctx, s.c, s.keys, s.prefix, op, args).Text()
	}
	raw, err := s.c.FCall(ctx, library, s.keys, s.prefix, op, args).Text()
	if err != nil && strings.Contains(err.Error(), "Function not found") && s.load(ctx) == nil {
		return s.c.FCall(ctx, library, s.keys, s.prefix, op, args).Text()
	}
	return raw, err
}

func (s *Store) check(ctx context.Context, cfg config) error {
	info, err := s.c.Info(ctx, "server", "memory").Result()
	if err != nil {
		return fmt.Errorf("kiln: redis info: %w", err)
	}
	fields := make(map[string]string)
	for line := range strings.SplitSeq(info, "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			fields[k] = v
		}
	}
	if v := fields["redis_version"]; v != "" {
		major, _, _ := strings.Cut(v, ".")
		if n, err := strconv.Atoi(major); err == nil && n < 7 {
			return fmt.Errorf("kiln: redis %s is older than 7.0", v)
		}
	}
	if p := fields["maxmemory_policy"]; strings.HasPrefix(p, "allkeys-") && !cfg.eviction {
		return fmt.Errorf("kiln: redis maxmemory-policy %s would evict jobs; use noeviction", p)
	}
	meta := s.prefix + "meta"
	if err := s.c.HSetNX(ctx, meta, "format", format).Err(); err != nil {
		return fmt.Errorf("kiln: redis format: %w", err)
	}
	v, err := s.c.HGet(ctx, meta, "format").Int()
	if err != nil {
		return fmt.Errorf("kiln: redis format: %w", err)
	}
	if v > format {
		return fmt.Errorf("kiln: the store %q has format %d, newer than this version of kiln understands (%d)", cfg.prefix, v, format)
	}
	return nil
}

type reply struct {
	E string          `json:"e"`
	M string          `json:"m"`
	R json.RawMessage `json:"r"`
}

func (s *Store) call(ctx context.Context, op string, args, out any) error {
	b, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("kiln: %s: %w", op, err)
	}
	raw, err := s.run(ctx, op, b)
	if err != nil {
		return fmt.Errorf("kiln: %s: %w", op, err)
	}
	var r reply
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return fmt.Errorf("kiln: %s: %w", op, err)
	}
	if r.E != "" {
		return failure(r.E, r.M)
	}
	if out == nil || len(r.R) == 0 {
		return nil
	}
	if err := json.Unmarshal(r.R, out); err != nil {
		return fmt.Errorf("kiln: %s: %w", op, err)
	}
	return nil
}

func failure(code, msg string) error {
	var base error
	switch code {
	case "notfound":
		base = driver.ErrNotFound
	case "closed":
		base = driver.ErrClosed
	case "conflict":
		base = driver.ErrConflict
	case "invalid":
		base = driver.ErrInvalid
	case "lost":
		return driver.ErrLost
	case "bad":
		return errBadJobs
	default:
		return fmt.Errorf("kiln: %s: %s", code, msg)
	}
	return fmt.Errorf("%w: %s", base, msg)
}

type empty struct{}

func micros(d time.Duration) int64 {
	if d > 0 && d < time.Microsecond {
		return 1
	}
	return int64(d / time.Microsecond)
}

func stamp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMicro()
}

func when(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us)
}

func limitOr(n, def int) int {
	if n <= 0 {
		return def
	}
	return n
}

func ints(ss []string) []int64 {
	if len(ss) == 0 {
		return nil
	}
	out := make([]int64, len(ss))
	for i, v := range ss {
		out[i], _ = strconv.ParseInt(v, 10, 64)
	}
	return out
}

func sortedInts(ss []string) []int64 {
	out := ints(ss)
	slices.Sort(out)
	return out
}

// Now returns the time on the Redis server's clock, the one every other method uses.
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	t, err := s.c.Time(ctx).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("kiln: now: %w", err)
	}
	return t.Truncate(time.Microsecond), nil
}
