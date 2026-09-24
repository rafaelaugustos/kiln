package driver

import "time"

type Ref struct {
	ID    int64
	Claim int32
}

type Parent struct {
	ID    int64
	Index int
	On    Mask
}

type InsertParams struct {
	Kind        string
	Queue       string
	Args        []byte
	Meta        map[string]string
	Tags        []string
	Priority    int16
	MaxAttempts int
	Timeout     time.Duration
	RunAt       time.Time
	Delay       time.Duration
	UniqueKey   []byte
	UniqueFor   time.Duration
	LimitKey    string
	LimitMax    int
	BatchID     int64
	AfterBatch  int64
	Parents     []Parent
	RecurringID string
}

type Inserted struct {
	ID        int64
	State     State
	Duplicate bool
}

type Job struct {
	Ref
	Kind        string
	Queue       string
	Args        []byte
	Meta        map[string]string
	Tags        []string
	Priority    int16
	Attempt     int
	MaxAttempts int
	Timeout     time.Duration
	RunAt       time.Time
	CreatedAt   time.Time
	AttemptedAt time.Time
	BatchID     int64
	RecurringID string
	Parents     []int64
	LimitKey    string
}

type Record struct {
	Job
	State           State
	FinalizedAt     time.Time
	Server          string
	CancelRequested bool
	AfterBatch      int64
	PendingDeps     int
	History         []Entry
	Output          []byte
	Children        []int64
}

type Entry struct {
	At      time.Time
	State   State
	Attempt int
	Reason  string
	Error   string
	Trace   string
	Server  string
}

type Outcome struct {
	Ref
	State  State
	Delay  time.Duration
	Refund bool
	Reason string
	Error  string
	Trace  string
	Output []byte
}

type Result uint8

const (
	Applied Result = iota + 1
	Stale
	Busy
	Rejected
)

type ClaimQuery struct {
	Queues []string
	Kinds  []string
	Limit  int
	Server string
}

type ServerInfo struct {
	ID          string
	Host        string
	PID         int
	Version     string
	Queues      []string
	Kinds       []string
	Workers     int
	Running     int
	StartedAt   time.Time
	HeartbeatAt time.Time
}

type Lease struct {
	Ref
	Cancel bool
	Age    time.Duration
}

type Directives struct {
	Leases []Lease
	Paused []string
}

type Orphan struct {
	Ref
	Kind        string
	Queue       string
	Attempt     int
	MaxAttempts int
	Server      string
	Cancel      bool
}

type Promoted struct {
	Count  int
	Queues []string
	Next   time.Duration
}

type Retention struct {
	Succeeded time.Duration
	Deleted   time.Duration
	Failed    time.Duration
}

type PruneParams struct {
	Retention
	Servers time.Duration
	Stats   time.Duration
	Limit   int
}

type Recurring struct {
	ID        string
	Spec      string
	Location  string
	Template  InsertParams
	Misfire   Misfire
	Overlap   bool
	Paused    bool
	NextRunAt time.Time
	LastRunAt time.Time
	LastJobID int64
	CreatedAt time.Time
	UpdatedAt time.Time
	Version   int64
}

type Fire struct {
	ID        string
	Version   int64
	NextRunAt time.Time
	LastRunAt time.Time
	Jobs      []InsertParams
}

type Filter struct {
	IDs         []int64
	State       State
	Queue       string
	Kind        string
	BatchID     int64
	RecurringID string
}

type JobQuery struct {
	State   State
	Queue   string
	Kind    string
	BatchID int64
	Limit   int
	Cursor  string
}

type Page struct {
	Records []Record
	Next    string
}

type Counts struct {
	Awaiting   int64
	Scheduled  int64
	Throttled  int64
	Enqueued   int64
	Processing int64
	Failed     int64
	Retries    int64
	Succeeded  int64
	Deleted    int64
	Capped     bool
}

type Point struct {
	At        time.Time
	Succeeded int64
	Failed    int64
	Deleted   int64
	Retried   int64
}

type QueueInfo struct {
	Name       string
	Paused     bool
	Enqueued   int64
	Processing int64
	Scheduled  int64
	Throttled  int64
	Latency    time.Duration
}

type NewBatch struct {
	Description string
	Meta        map[string]string
}

type Batch struct {
	ID          int64
	Description string
	Meta        map[string]string
	Total       int64
	Sealed      bool
	Counts      map[State]int64
	CreatedAt   time.Time
	FinishedAt  time.Time
}

type BatchQuery struct {
	Limit  int
	Cursor string
}

type BatchPage struct {
	Batches []Batch
	Next    string
}

type Event struct {
	Kind  EventKind
	Queue string
	ID    int64
}
