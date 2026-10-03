package driver

import "errors"

var (
	// ErrNotFound is returned for a job, batch or recurring job that does not exist, including a
	// parent job or a batch named by an insert.
	ErrNotFound = errors.New("kiln: not found")

	// ErrConflict is returned by [Admin.PutRecurring] and [Coordinator.Fire] when the recurring job
	// is no longer at the version the caller read.
	ErrConflict = errors.New("kiln: conflict")

	// ErrLost is returned by [Worker.SetMeta] and [Console.WriteConsole] when the job is no longer
	// processing under the claim of the given [Ref].
	ErrLost = errors.New("kiln: claim lost")

	// ErrClosed is returned when a job is inserted, or a batch opened, into a batch that has
	// already finished.
	ErrClosed = errors.New("kiln: batch closed")

	// ErrInvalid is returned for arguments that kiln or the store does not accept. The wrapping
	// error says which one.
	ErrInvalid = errors.New("kiln: invalid argument")

	// ErrTooLarge is returned for a value over a size limit of kiln or of the store.
	ErrTooLarge = errors.New("kiln: too large")

	// ErrNilTx is returned when a writer meant for an application's transaction has none: a store
	// returns it from a writer created with a nil transaction, and package kiln when it is given a
	// nil [Writer].
	ErrNilTx = errors.New("kiln: nil transaction")
)
