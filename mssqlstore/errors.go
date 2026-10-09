package mssqlstore

import (
	"errors"
	"fmt"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/rafaelaugustos/kiln/driver"
)

const (
	errNoObject    = 208
	errDeadlock    = 1205
	errLockTimeout = 1222
	errDupKey      = 2627
	errDupIndex    = 2601
)

func sqlError(err error) *mssql.Error {
	if e, ok := errors.AsType[mssql.Error](err); ok {
		return &e
	}
	return nil
}

var errMoved = errors.New("jobs moved while they were being requeued")

func retryable(err error) bool {
	if errors.Is(err, errMoved) {
		return true
	}
	e := sqlError(err)
	return e != nil && (e.Number == errDeadlock || e.Number == errLockTimeout || duplicate(err))
}

func duplicate(err error) bool {
	e := sqlError(err)
	return e != nil && (e.Number == errDupKey || e.Number == errDupIndex)
}

func dataError(err error) bool {
	e := sqlError(err)
	if e == nil {
		return false
	}
	switch e.Number {
	case 220, 232, 241, 242, 245, 515, 1700, 2628, 8114, 8115, 8152, 13607, 13608, 13609:
		return true
	}
	return false
}

func wrap(op string, err error) error {
	if dataError(err) {
		return fmt.Errorf("%w: %s: %w", driver.ErrInvalid, op, err)
	}
	return fmt.Errorf("kiln: %s: %w", op, err)
}
