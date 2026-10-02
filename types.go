package kiln

import "github.com/rafaelaugustos/kiln/driver"

type (
	// State is the state of a job. The package documentation describes how jobs move between
	// states.
	State = driver.State

	// Record is a job as the store keeps it, returned by [Client.Get] and [Client.List].
	Record = driver.Record

	// Entry is a line in a job's history.
	Entry = driver.Entry

	// Inserted is the result of one job of [Client.EnqueueMany].
	Inserted = driver.Inserted

	// Filter selects jobs for [Client.DeleteWhere] and [Client.RequeueWhere].
	Filter = driver.Filter

	// JobQuery asks [Client.List] for a page of jobs.
	JobQuery = driver.JobQuery

	// Page is a page of jobs returned by [Client.List].
	Page = driver.Page

	// Retention says how long finished jobs are kept; see [ServerConfig.Retention].
	Retention = driver.Retention
)

// The job states, in lifecycle order.
const (
	Awaiting   = driver.Awaiting   // waiting for parent jobs or a batch
	Scheduled  = driver.Scheduled  // waiting for its run time
	Throttled  = driver.Throttled  // waiting for room under a Limit
	Enqueued   = driver.Enqueued   // ready to be claimed
	Processing = driver.Processing // running on a server
	Succeeded  = driver.Succeeded  // the handler returned nil
	Failed     = driver.Failed     // out of attempts or Permanent, kept until requeued or deleted
	Deleted    = driver.Deleted    // by Delete or Cancel, after its Deadline, or with a deleted parent
)

// Args is implemented by the types that carry the arguments of a job. The value is stored as
// JSON, at most 1 MiB of it. A type can also have a method InsertOptions() []InsertOption, whose
// options apply to every job of the type, ahead of the options passed with each call.
type Args interface {
	// Kind names the kind of job, which selects its handler: 1 to 128 bytes of letters, digits,
	// '_', '.', ':' and '-', starting with a letter. It must return the same string for every value
	// of the type, since [Handle] calls it on the zero value.
	Kind() string
}

// Spec is one job for [Client.EnqueueMany]: its args and the options to insert it with.
type Spec struct {
	Args    Args
	Options []InsertOption
}
