package pgstore

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rafaelaugustos/kiln/driver"
)

const (
	codeClosed   = "KL001"
	codeNotFound = "KL002"
)

func pgError(err error) *pgconn.PgError {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe
	}
	return nil
}

func dataError(err error) bool {
	pe := pgError(err)
	if pe == nil || len(pe.Code) != 5 {
		return false
	}
	switch pe.Code[:2] {
	case "22", "23", "54":
		return true
	}
	return false
}

func wrap(op string, err error) error {
	pe := pgError(err)
	if pe == nil || len(pe.Code) != 5 {
		return fmt.Errorf("kiln: %s: %w", op, err)
	}
	switch {
	case pe.Code == codeClosed:
		return fmt.Errorf("%w: %s", driver.ErrClosed, pe.Message)
	case pe.Code == codeNotFound:
		return fmt.Errorf("%w: %s", driver.ErrNotFound, pe.Message)
	case pe.Code[:2] == "54":
		return fmt.Errorf("%w: %s: %s", driver.ErrTooLarge, op, pe.Message)
	case pe.Code[:2] == "22" || pe.Code[:2] == "23":
		return fmt.Errorf("%w: %s: %s", driver.ErrInvalid, op, pe.Message)
	}
	return fmt.Errorf("kiln: %s: %w", op, err)
}
