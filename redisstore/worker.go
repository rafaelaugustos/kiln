package redisstore

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

// Insert inserts jobs as [driver.Writer.Insert] describes, in one script: the jobs, the admission
// of the limit keys they touched and the wakeup of the queues that received them happen together.
// The store sets no size limits of its own, so it never fails with [driver.ErrTooLarge].
func (s *Store) Insert(ctx context.Context, jobs []driver.InsertParams) ([]driver.Inserted, error) {
	if err := driver.CheckInsert(jobs); err != nil {
		return nil, err
	}
	var res []wireInserted
	if len(jobs) > 0 {
		args := struct {
			Jobs []wireParams `json:"jobs"`
		}{params(jobs)}
		if err := s.call(ctx, "insert", args, &res); err != nil {
			return nil, err
		}
	}
	return inserted(res), nil
}

// OpenBatch creates an unsealed batch and returns its id, as [driver.Writer.OpenBatch] describes.
func (s *Store) OpenBatch(ctx context.Context, nb driver.NewBatch) (int64, error) {
	args := struct {
		Parent int64  `json:"parent,omitempty"`
		Desc   string `json:"desc,omitempty"`
		Meta   string `json:"meta,omitempty"`
	}{max(nb.Parent, 0), nb.Description, object(nb.Meta)}
	var id string
	if err := s.call(ctx, "open_batch", args, &id); err != nil {
		return 0, err
	}
	return strconv.ParseInt(id, 10, 64)
}

// SealBatch seals the batch id, which finishes at once if none of its members is live and its
// nested batches have finished. It fails with [driver.ErrNotFound] for an unknown id.
func (s *Store) SealBatch(ctx context.Context, id int64) error {
	return s.call(ctx, "seal_batch", ref{ID: id}, nil)
}

type ref struct {
	ID    int64 `json:"id"`
	Claim int32 `json:"claim,omitempty"`
}

// Claim moves up to q.Limit enqueued jobs to processing for q.Server and returns them, as
// [driver.Worker.Claim] describes.
func (s *Store) Claim(ctx context.Context, q driver.ClaimQuery) ([]driver.Job, error) {
	if q.Limit <= 0 {
		return nil, nil
	}
	args := struct {
		Queues []string `json:"queues,omitempty"`
		Kinds  []string `json:"kinds,omitempty"`
		Limit  int      `json:"limit"`
		Server string   `json:"server"`
	}{q.Queues, q.Kinds, q.Limit, q.Server}
	var res []wireJob
	if err := s.call(ctx, "claim", args, &res); err != nil {
		return nil, err
	}
	out := make([]driver.Job, len(res))
	for i := range res {
		out[i] = res[i].job()
	}
	return out, nil
}

type wireOutcome struct {
	ID     int64  `json:"id"`
	Claim  int32  `json:"claim"`
	St     string `json:"st"`
	Delay  int64  `json:"delay,omitempty"`
	Refund bool   `json:"refund,omitempty"`
	Reason string `json:"reason,omitempty"`
	Err    string `json:"err,omitempty"`
	Trace  string `json:"trace,omitempty"`
	Out    string `json:"out,omitempty"`
	Bad    bool   `json:"bad,omitempty"`
}

// Finish applies outcomes as [driver.Worker.Finish] describes. It never reports [driver.Busy]:
// the script that applies the outcomes runs alone. An outcome whose Output is neither empty nor
// valid JSON is [driver.Rejected].
func (s *Store) Finish(ctx context.Context, server string, outs []driver.Outcome) ([]driver.Result, error) {
	if len(outs) == 0 {
		return []driver.Result{}, nil
	}
	args := struct {
		Server string        `json:"server"`
		Outs   []wireOutcome `json:"outs"`
	}{server, make([]wireOutcome, len(outs))}
	for i := range outs {
		o := &outs[i]
		w := wireOutcome{
			ID: o.ID, Claim: o.Claim, St: string(o.State), Delay: micros(o.Delay), Refund: o.Refund,
			Reason: o.Reason, Err: clip(o.Error, maxError), Trace: clip(o.Trace, maxTrace),
		}
		if len(o.Output) > 0 {
			if json.Valid(o.Output) {
				w.Out = string(o.Output)
			} else {
				w.Bad = true
			}
		}
		args.Outs[i] = w
	}
	var res []driver.Result
	if err := s.call(ctx, "finish", args, &res); err != nil {
		return nil, err
	}
	return res, nil
}

type wireInfo struct {
	Host    string   `json:"host"`
	PID     int      `json:"pid"`
	Version string   `json:"version"`
	Queues  []string `json:"queues"`
	Kinds   []string `json:"kinds"`
	Workers int      `json:"workers"`
	Running int      `json:"running"`
}

// Heartbeat records that the server info describes is alive and returns the leases of its
// processing jobs and the paused queues; see [driver.Worker.Heartbeat].
func (s *Store) Heartbeat(ctx context.Context, info driver.ServerInfo) (driver.Directives, error) {
	blob, _ := json.Marshal(wireInfo{
		Host: info.Host, PID: info.PID, Version: info.Version, Queues: info.Queues, Kinds: info.Kinds,
		Workers: info.Workers, Running: info.Running,
	})
	args := struct {
		ID      string `json:"id"`
		Info    string `json:"info"`
		Started int64  `json:"started,omitempty"`
	}{info.ID, string(blob), stamp(info.StartedAt)}
	var res struct {
		Leases []struct {
			ID     int64 `json:"id,string"`
			Claim  int32 `json:"claim"`
			Cancel bool  `json:"cancel"`
			Age    int64 `json:"age,string"`
		} `json:"leases"`
		Paused []string `json:"paused"`
	}
	if err := s.call(ctx, "heartbeat", args, &res); err != nil {
		return driver.Directives{}, err
	}
	d := driver.Directives{Paused: res.Paused}
	slices.Sort(d.Paused)
	for _, l := range res.Leases {
		d.Leases = append(d.Leases, driver.Lease{
			Ref:    driver.Ref{ID: l.ID, Claim: l.Claim},
			Cancel: l.Cancel,
			Age:    time.Duration(l.Age) * time.Microsecond,
		})
	}
	return d, nil
}

// Unregister forgets the server. Jobs it still has processing become orphans at once.
func (s *Store) Unregister(ctx context.Context, server string) error {
	return s.call(ctx, "unregister", named{ID: server}, nil)
}

type named struct {
	ID string `json:"id"`
}

// SetMeta merges meta into the metadata of the job ref points to while the job is processing
// under ref's claim, and fails with [driver.ErrLost] otherwise.
func (s *Store) SetMeta(ctx context.Context, r driver.Ref, meta map[string]string) error {
	blob, _ := json.Marshal(meta)
	if len(meta) == 0 {
		blob = []byte("{}")
	}
	args := struct {
		ref
		Meta string `json:"meta"`
	}{ref{ID: r.ID, Claim: r.Claim}, string(blob)}
	return s.call(ctx, "set_meta", args, nil)
}
