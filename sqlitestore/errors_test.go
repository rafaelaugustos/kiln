package sqlitestore

import (
	"context"
	"fmt"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

type errNo int

type fieldError struct {
	Code         errNo
	ExtendedCode errNo
}

func (e fieldError) Error() string { return "CHECK constraint failed: output" }

type resultCode uint8

type methodError struct{ code resultCode }

func (e *methodError) Code() resultCode { return e.code }

func (e *methodError) Error() string { return "sqlite3: constraint failed" }

func TestFinishRefused(t *testing.T) {
	t.Parallel()
	s := open(t)
	_, err := s.db.ExecContext(context.Background(), `CREATE TRIGGER kiln_refuse BEFORE INSERT ON kiln_archive
		WHEN NEW.output = '"refused"' BEGIN SELECT RAISE(ABORT, 'refused'); END`)
	if err != nil {
		t.Fatal(err)
	}
	insert(t, s, job("a"), job("b"))
	js := claim(t, s, 2)
	bad := driver.Outcome{Ref: js[0].Ref, State: driver.Succeeded, Output: []byte(`"refused"`)}
	good := driver.Outcome{Ref: js[1].Ref, State: driver.Succeeded, Output: []byte(`"kept"`)}
	if rs := finish(t, s, bad, good); rs[0] != driver.Rejected || rs[1] != driver.Applied {
		t.Fatalf("results %v, want [rejected applied]", rs)
	}

	for _, err := range []error{fieldError{Code: 19, ExtendedCode: 275}, &methodError{code: 19}} {
		if !dataError(fmt.Errorf("kiln: finish: %w", err)) {
			t.Errorf("%T with code 19 is not a data error", err)
		}
	}
}
