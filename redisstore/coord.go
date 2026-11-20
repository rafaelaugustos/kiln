package redisstore

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const pruneChunk = 500

// Lead acquires or renews the lease called name for holder, as [driver.Coordinator.Lead]
// describes.
func (s *Store) Lead(ctx context.Context, name, holder string, ttl time.Duration) (time.Duration, bool, error) {
	args := struct {
		Name   string `json:"name"`
		Holder string `json:"holder"`
		TTL    int64  `json:"ttl"`
	}{name, holder, micros(ttl)}
	var res struct {
		OK   bool  `json:"ok"`
		Held int64 `json:"held,string"`
	}
	if err := s.call(ctx, "lead", args, &res); err != nil {
		return 0, false, err
	}
	return time.Duration(res.Held) * time.Microsecond, res.OK, nil
}

// Resign releases the lease called name if holder has it, and does nothing otherwise.
func (s *Store) Resign(ctx context.Context, name, holder string) error {
	args := struct {
		Name   string `json:"name"`
		Holder string `json:"holder"`
	}{name, holder}
	return s.call(ctx, "resign", args, nil)
}

type limited struct {
	Limit int `json:"limit"`
}

// Promote moves up to limit scheduled jobs whose run time has come, earliest first, as
// [driver.Coordinator.Promote] describes.
func (s *Store) Promote(ctx context.Context, limit int) (driver.Promoted, error) {
	var res struct {
		N      int      `json:"n"`
		Queues []string `json:"queues"`
		Next   int64    `json:"next,string"`
	}
	if err := s.call(ctx, "promote", limited{max(limit, 1)}, &res); err != nil {
		return driver.Promoted{}, err
	}
	slices.Sort(res.Queues)
	return driver.Promoted{Count: res.N, Queues: res.Queues, Next: time.Duration(res.Next) * time.Microsecond}, nil
}

// Orphans returns up to limit processing jobs whose server is unknown or has not sent a heartbeat
// for deadAfter. It changes nothing.
func (s *Store) Orphans(ctx context.Context, deadAfter time.Duration, limit int) ([]driver.Orphan, error) {
	args := struct {
		Dead  int64 `json:"dead"`
		Limit int   `json:"limit"`
	}{micros(deadAfter), max(limit, 1)}
	var res []struct {
		ID     int64  `json:"id,string"`
		Claim  int32  `json:"claim"`
		K      string `json:"k"`
		Q      string `json:"q"`
		Att    int    `json:"att"`
		Max    int    `json:"max"`
		Srv    string `json:"srv"`
		Cancel bool   `json:"cancel"`
		LR     string `json:"lr"`
	}
	if err := s.call(ctx, "orphans", args, &res); err != nil {
		return nil, err
	}
	out := make([]driver.Orphan, len(res))
	for i, o := range res {
		out[i] = driver.Orphan{
			Ref: driver.Ref{ID: o.ID, Claim: o.Claim}, Kind: o.K, Queue: o.Q, Attempt: o.Att,
			MaxAttempts: o.Max, Server: o.Srv, Cancel: o.Cancel, LastReason: o.LR,
		}
	}
	return out, nil
}

// Sweep repairs what other calls leave to it, as [driver.Coordinator.Sweep] describes. A call
// looks at up to limit awaiting jobs, unfinished batches and limit keys, and the next call goes on
// from where it stopped, so a large store is covered over several calls.
func (s *Store) Sweep(ctx context.Context, limit int) (int, error) {
	var n int
	if err := s.call(ctx, "sweep", limited{max(limit, 1)}, &n); err != nil {
		return 0, err
	}
	return n, nil
}

// Prune deletes finished jobs past their retention, silent servers, old statistics, expired
// unique keys, unused limit keys and finished batches with nothing left in them, as
// [driver.Coordinator.Prune] describes, in scripts of at most 500 rows of each kind so that a
// large prune never holds the server for long.
func (s *Store) Prune(ctx context.Context, p driver.PruneParams) (int, error) {
	args := struct {
		Limit   int   `json:"limit"`
		Succ    int64 `json:"succ"`
		Del     int64 `json:"del"`
		Failed  int64 `json:"failed"`
		Servers int64 `json:"servers"`
		Stats   int64 `json:"stats"`
	}{
		Succ: keep(p.Succeeded), Del: keep(p.Deleted), Failed: keep(p.Failed),
		Servers: micros(cmp.Or(p.Servers, time.Hour)), Stats: micros(cmp.Or(p.Stats, 14*24*time.Hour)),
	}
	total := 0
	for left := limitOr(p.Limit, 1000); left > 0; left -= args.Limit {
		args.Limit = min(left, pruneChunk)
		var n int
		if err := s.call(ctx, "prune", args, &n); err != nil {
			return total, err
		}
		total += n
		if n == 0 {
			break
		}
	}
	return total, nil
}

func keep(d time.Duration) int64 {
	if d < 0 {
		return -1
	}
	return micros(d)
}

type recurringData struct {
	Group    string              `json:"group,omitempty"`
	Spec     string              `json:"spec"`
	Location string              `json:"location,omitempty"`
	Template driver.InsertParams `json:"template"`
	Misfire  driver.Misfire      `json:"misfire,omitempty"`
	Overlap  bool                `json:"overlap,omitempty"`
	Paused   bool                `json:"paused,omitempty"`
}

type wireRecurring struct {
	ID      string `json:"id"`
	Data    string `json:"data"`
	Ver     int64  `json:"ver,string"`
	Next    int64  `json:"next,string"`
	Last    int64  `json:"last,string"`
	LastJob int64  `json:"lastjob,string"`
	Cr      int64  `json:"cr,string"`
	Up      int64  `json:"up,string"`
}

func (w *wireRecurring) recurring() driver.Recurring {
	var d recurringData
	json.Unmarshal([]byte(w.Data), &d)
	return driver.Recurring{
		ID: w.ID, Group: d.Group, Spec: d.Spec, Location: d.Location, Template: d.Template,
		Misfire: d.Misfire, Overlap: d.Overlap, Paused: d.Paused,
		NextRunAt: when(w.Next), LastRunAt: when(w.Last), LastJobID: w.LastJob,
		CreatedAt: when(w.Cr), UpdatedAt: when(w.Up), Version: w.Ver,
	}
}

func recurrings(ws []wireRecurring) []driver.Recurring {
	out := make([]driver.Recurring, len(ws))
	for i := range ws {
		out[i] = ws[i].recurring()
	}
	return out
}

// Due returns up to limit recurring jobs that are not paused and whose next run has come, the
// earliest first, with the store's time.
func (s *Store) Due(ctx context.Context, limit int) ([]driver.Recurring, time.Time, error) {
	var res struct {
		Now  int64           `json:"now,string"`
		List []wireRecurring `json:"list"`
	}
	if err := s.call(ctx, "due", limited{max(limit, 1)}, &res); err != nil {
		return nil, time.Time{}, err
	}
	return recurrings(res.List), when(res.Now), nil
}

var errBadJobs = errors.New("bad")

// Fire inserts f.Jobs and stores the new schedule if the recurring job is still at f.Version, as
// [driver.Coordinator.Fire] describes, all in one script.
func (s *Store) Fire(ctx context.Context, f driver.Fire) ([]driver.Inserted, error) {
	args := struct {
		ID   string       `json:"id"`
		Ver  int64        `json:"ver"`
		Next int64        `json:"next"`
		Last int64        `json:"last"`
		Jobs []wireParams `json:"jobs,omitempty"`
		Bad  bool         `json:"bad,omitempty"`
	}{ID: f.ID, Ver: f.Version, Next: stamp(f.NextRunAt), Last: stamp(f.LastRunAt)}
	invalid := driver.CheckInsert(f.Jobs)
	if invalid != nil {
		args.Bad = true
	} else {
		args.Jobs = params(f.Jobs)
	}
	var res []wireInserted
	err := s.call(ctx, "fire", args, &res)
	if errors.Is(err, errBadJobs) {
		return nil, invalid
	}
	if err != nil {
		return nil, err
	}
	return inserted(res), nil
}

// Recurring returns the recurring job id, or an error wrapping [driver.ErrNotFound].
func (s *Store) Recurring(ctx context.Context, id string) (driver.Recurring, error) {
	var res wireRecurring
	if err := s.call(ctx, "recurring", named{ID: id}, &res); err != nil {
		return driver.Recurring{}, err
	}
	return res.recurring(), nil
}

// PutRecurring creates r when r.Version is 0 and no recurring job has its ID, or replaces the
// stored one when r.Version equals its Version, and fails with [driver.ErrConflict] otherwise. An
// empty ID is [driver.ErrInvalid].
func (s *Store) PutRecurring(ctx context.Context, r driver.Recurring) error {
	if r.ID == "" {
		return fmt.Errorf("%w: empty recurring id", driver.ErrInvalid)
	}
	blob, err := json.Marshal(recurringData{
		Group: r.Group, Spec: r.Spec, Location: r.Location, Template: r.Template,
		Misfire: r.Misfire, Overlap: r.Overlap, Paused: r.Paused,
	})
	if err != nil {
		return fmt.Errorf("%w: recurring %q: %v", driver.ErrInvalid, r.ID, err)
	}
	args := struct {
		ID      string `json:"id"`
		Data    string `json:"data"`
		Paused  bool   `json:"paused"`
		Ver     int64  `json:"ver"`
		Next    int64  `json:"next"`
		Last    int64  `json:"last"`
		LastJob int64  `json:"lastjob"`
	}{r.ID, string(blob), r.Paused, r.Version, stamp(r.NextRunAt), stamp(r.LastRunAt), r.LastJobID}
	return s.call(ctx, "put_recurring", args, nil)
}

// RemoveRecurring deletes the recurring job id, or fails with [driver.ErrNotFound]. Jobs it
// created stay as they are.
func (s *Store) RemoveRecurring(ctx context.Context, id string) error {
	return s.call(ctx, "remove_recurring", named{ID: id}, nil)
}

// Recurrings returns every recurring job, ordered by id.
func (s *Store) Recurrings(ctx context.Context) ([]driver.Recurring, error) {
	var res []wireRecurring
	if err := s.call(ctx, "recurrings", empty{}, &res); err != nil {
		return nil, err
	}
	return recurrings(res), nil
}
