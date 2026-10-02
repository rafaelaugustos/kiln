package kiln

import (
	"errors"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var (
	// ErrPermanent marks a failure that retrying cannot fix. A handler error that wraps it fails
	// the job even if attempts remain; [Permanent] wraps an error with it.
	ErrPermanent = errors.New("kiln: permanent failure")

	// ErrCanceled is the cause of a handler's context when its job is deleted while running. The
	// job then ends deleted, unless the handler returns nil. A handler error that wraps
	// ErrCanceled deletes the job as well; [Cancel] wraps an error with it.
	ErrCanceled = errors.New("kiln: job canceled")

	// ErrSnoozed matches the errors returned by [Snooze], for middleware that checks with
	// errors.Is.
	ErrSnoozed = errors.New("kiln: job snoozed")

	// ErrTimeout is the cause of a handler's context when the attempt's [Timeout] expires. If the
	// handler then returns an error, kiln wraps it with ErrTimeout and treats it like any other
	// failure.
	ErrTimeout = errors.New("kiln: job timed out")

	// ErrShutdown is the cause of a handler's context when its server is stopping and
	// ShutdownTimeout has passed. If the handler then returns an error, the job goes back to its
	// queue and the attempt does not count.
	ErrShutdown = errors.New("kiln: server shutting down")

	// ErrNotFound is returned for a job, batch or recurring job that does not exist, including a
	// parent named by [After] or a batch named by [InBatch] or [AfterBatch].
	ErrNotFound = driver.ErrNotFound

	// ErrInvalid is returned for arguments, options and configuration that kiln rejects. The
	// wrapping error says what was wrong.
	ErrInvalid = driver.ErrInvalid

	// ErrTooLarge is returned for a job over a size limit: args over 1 MiB, more than 32 tags,
	// more than 64 meta keys or 16 KiB of meta, a limit key over 200 bytes, or a limit of the
	// store itself.
	ErrTooLarge = driver.ErrTooLarge

	// ErrLost is returned by [Job.SetParam] when the job is no longer running under this attempt's
	// claim, for instance because its server was presumed dead and its jobs rescued.
	ErrLost = driver.ErrLost

	// ErrClosed is returned when a job is added with [InBatch] to a batch that has finished.
	ErrClosed = driver.ErrClosed
)

type markedError struct {
	mark error
	err  error
}

func (e *markedError) Error() string {
	if e.err == nil {
		return e.mark.Error()
	}
	return e.err.Error()
}

func (e *markedError) Unwrap() []error {
	if e.err == nil {
		return []error{e.mark}
	}
	return []error{e.mark, e.err}
}

// Permanent wraps err so that the job fails without further attempts. The result matches both
// [ErrPermanent] and err with errors.Is, and has err's message.
func Permanent(err error) error {
	return &markedError{mark: ErrPermanent, err: err}
}

// Cancel wraps err so that the job is deleted instead of retried or failed. The result matches
// both [ErrCanceled] and err with errors.Is, and has err's message.
func Cancel(err error) error {
	return &markedError{mark: ErrCanceled, err: err}
}

type snoozeError struct {
	d time.Duration
}

func (e *snoozeError) Error() string {
	return fmt.Sprintf("kiln: job snoozed for %s", e.d)
}

func (e *snoozeError) Is(target error) bool {
	return target == ErrSnoozed
}

// Snooze returns an error that makes the job run again after d without using up the attempt. A
// negative d means right away.
func Snooze(d time.Duration) error {
	return &snoozeError{d: max(d, 0)}
}

func snoozed(err error) (time.Duration, bool) {
	if s, ok := errors.AsType[*snoozeError](err); ok {
		return s.d, true
	}
	return 0, false
}

// PanicError is the error of an attempt whose handler panicked. Middleware receives it as the
// handler's error, the attempt fails like any other, and Stack is kept as the trace of the job's
// history entry.
type PanicError struct {
	Value any    // the value passed to panic
	Stack []byte // the stack of the goroutine that panicked
}

// Error returns "kiln: panic: " followed by the panic value.
func (e *PanicError) Error() string {
	return fmt.Sprintf("kiln: panic: %v", e.Value)
}
