package driver

import "errors"

var (
	ErrNotFound = errors.New("kiln: not found")
	ErrConflict = errors.New("kiln: conflict")
	ErrLost     = errors.New("kiln: claim lost")
	ErrClosed   = errors.New("kiln: batch closed")
	ErrInvalid  = errors.New("kiln: invalid argument")
	ErrTooLarge = errors.New("kiln: too large")
	ErrNilTx    = errors.New("kiln: nil transaction")
)
