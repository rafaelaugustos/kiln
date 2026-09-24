package driver

import (
	"context"
	"time"
)

type Writer interface {
	Insert(ctx context.Context, jobs []InsertParams) ([]Inserted, error)
	OpenBatch(ctx context.Context, b NewBatch) (int64, error)
	SealBatch(ctx context.Context, id int64) error
}

type Worker interface {
	Claim(ctx context.Context, q ClaimQuery) ([]Job, error)
	Finish(ctx context.Context, server string, outs []Outcome) ([]Result, error)
	Heartbeat(ctx context.Context, s ServerInfo) (Directives, error)
	Unregister(ctx context.Context, server string) error
	SetMeta(ctx context.Context, ref Ref, meta map[string]string) error
}

type Coordinator interface {
	Now(ctx context.Context) (time.Time, error)
	Lead(ctx context.Context, name, holder string, ttl time.Duration) (held time.Duration, ok bool, err error)
	Resign(ctx context.Context, name, holder string) error
	Promote(ctx context.Context, limit int) (Promoted, error)
	Orphans(ctx context.Context, deadAfter time.Duration, limit int) ([]Orphan, error)
	Sweep(ctx context.Context, limit int) (int, error)
	Prune(ctx context.Context, p PruneParams) (int, error)
	Due(ctx context.Context, limit int) ([]Recurring, time.Time, error)
	Fire(ctx context.Context, f Fire) ([]Inserted, error)
}

type Admin interface {
	Delete(ctx context.Context, f Filter) (int, error)
	Requeue(ctx context.Context, f Filter) (int, error)
	PauseQueue(ctx context.Context, queue string, paused bool) error
	Recurring(ctx context.Context, id string) (Recurring, error)
	PutRecurring(ctx context.Context, r Recurring) error
	RemoveRecurring(ctx context.Context, id string) error
}

type Inspector interface {
	Job(ctx context.Context, id int64) (Record, error)
	Jobs(ctx context.Context, q JobQuery) (Page, error)
	Counts(ctx context.Context) (Counts, error)
	Series(ctx context.Context, from, to time.Time, step time.Duration) ([]Point, error)
	Servers(ctx context.Context) ([]ServerInfo, error)
	Queues(ctx context.Context) ([]QueueInfo, error)
	Recurrings(ctx context.Context) ([]Recurring, error)
	Batch(ctx context.Context, id int64) (Batch, error)
	Batches(ctx context.Context, q BatchQuery) (BatchPage, error)
}

type Store interface {
	Writer
	Worker
	Coordinator
	Admin
	Inspector
}

type Notifier interface {
	Subscribe(ctx context.Context, fn func(Event)) error
}

type Transactor interface {
	InTx(ctx context.Context, fn func(w Writer) error) error
}
