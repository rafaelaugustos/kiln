package kiln

import (
	"errors"
	"fmt"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

var (
	ErrPermanent = errors.New("kiln: permanent failure")
	ErrCanceled  = errors.New("kiln: job canceled")
	ErrSnoozed   = errors.New("kiln: job snoozed")
	ErrTimeout   = errors.New("kiln: job timed out")
	ErrShutdown  = errors.New("kiln: server shutting down")
	ErrNotFound  = driver.ErrNotFound
	ErrInvalid   = driver.ErrInvalid
	ErrTooLarge  = driver.ErrTooLarge
	ErrLost      = driver.ErrLost
	ErrClosed    = driver.ErrClosed
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

func Permanent(err error) error {
	return &markedError{mark: ErrPermanent, err: err}
}

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

func Snooze(d time.Duration) error {
	return &snoozeError{d: max(d, 0)}
}

func snoozed(err error) (time.Duration, bool) {
	var s *snoozeError
	if errors.As(err, &s) {
		return s.d, true
	}
	return 0, false
}

type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("kiln: panic: %v", e.Value)
}
