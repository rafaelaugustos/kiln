package sqlitestore

import (
	"errors"
	"fmt"
	"strings"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	codeBusy       = 5
	codeTooBig     = 18
	codeConstraint = 19
	codeMismatch   = 20
	codeRange      = 25
)

var sentinels = []error{driver.ErrNotFound, driver.ErrConflict, driver.ErrLost, driver.ErrClosed, driver.ErrInvalid,
	driver.ErrTooLarge, driver.ErrNilTx}

type coded interface {
	error
	Code() int
}

func code(err error) int {
	if e, ok := errors.AsType[coded](err); ok {
		return e.Code() & 0xff
	}
	return 0
}

func busy(err error) bool {
	if err == nil {
		return false
	}
	return code(err) == codeBusy || strings.Contains(err.Error(), "database is locked")
}

func tooLarge(err error) bool {
	return err != nil && (code(err) == codeTooBig || strings.Contains(err.Error(), "string or blob too big"))
}

func dataError(err error) bool {
	switch code(err) {
	case codeTooBig, codeConstraint, codeMismatch, codeRange:
		return true
	}
	return tooLarge(err)
}

func wrap(op string, err error) error {
	if tooLarge(err) {
		return fmt.Errorf("%w: %s: %w", driver.ErrTooLarge, op, err)
	}
	for _, s := range sentinels {
		if errors.Is(err, s) {
			return err
		}
	}
	return fmt.Errorf("kiln: %s: %w", op, err)
}
