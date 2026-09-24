package mysqlstore

import (
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/rafaelaugustos/kiln/driver"
)

const (
	errDuplicate   = 1062
	errNoTable     = 1146
	errLockTimeout = 1205
	errDeadlock    = 1213
	errPacket      = 1153
	errKilled      = 1927
	errIdle        = 4031
)

func mysqlError(err error) *mysql.MySQLError {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me
	}
	return nil
}

func retryable(err error) bool {
	me := mysqlError(err)
	return me != nil && (me.Number == errDeadlock || me.Number == errLockTimeout)
}

func dataError(err error) bool {
	me := mysqlError(err)
	if me == nil {
		return false
	}
	switch me.Number {
	case 1292, 1300, 1366, 1406, 3140, 3141, 3146, 3157:
		return true
	}
	return me.SQLState[0] == '2' && me.SQLState[1] == '2'
}

func tooLarge(err error) bool {
	me := mysqlError(err)
	return errors.Is(err, mysql.ErrPktTooLarge) || me != nil && me.Number == errPacket
}

func wrap(op string, err error) error {
	switch {
	case tooLarge(err):
		return fmt.Errorf("%w: %s: %w", driver.ErrTooLarge, op, err)
	case dataError(err):
		return fmt.Errorf("%w: %s: %w", driver.ErrInvalid, op, err)
	}
	return fmt.Errorf("kiln: %s: %w", op, err)
}
