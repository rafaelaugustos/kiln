package cron

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

type Schedule struct {
	spec     string
	every    time.Duration
	second   uint64
	minute   uint64
	hour     uint64
	dom      uint64
	month    uint64
	last     uint64
	near     uint64
	week     [7]uint64
	lastWeek [7]uint64
	lw       bool
	domStar  bool
	dowStar  bool
	interval bool
}

func Parse(spec string) (*Schedule, error) {
	s, err := parse(spec)
	if err != nil {
		return nil, fmt.Errorf("%w: cron %q: %v", driver.ErrInvalid, spec, err)
	}
	return s, nil
}

func (s *Schedule) String() string {
	return s.spec
}

func parse(spec string) (*Schedule, error) {
	f := strings.Fields(spec)
	switch {
	case len(f) == 0:
		return nil, errors.New("empty spec")
	case f[0][0] == '@':
		return descriptor(f)
	case len(f) == 5:
		f = append([]string{"0"}, f...)
	case len(f) != 6:
		return nil, fmt.Errorf("expected 5 or 6 fields, found %d", len(f))
	}
	return parseFields(f)
}

func descriptor(f []string) (*Schedule, error) {
	name := strings.ToLower(f[0])
	if name == "@every" {
		if len(f) != 2 {
			return nil, errors.New("@every takes one duration")
		}
		d, err := time.ParseDuration(f[1])
		if err != nil {
			return nil, err
		}
		if d = d.Truncate(time.Second); d < time.Second {
			return nil, fmt.Errorf("interval %s is shorter than 1s", f[1])
		}
		return &Schedule{spec: "@every " + d.String(), every: d}, nil
	}
	if len(f) != 1 {
		return nil, fmt.Errorf("%s takes no arguments", f[0])
	}
	var expr string
	switch name {
	case "@yearly", "@annually":
		name, expr = "@yearly", "0 0 1 1 *"
	case "@monthly":
		expr = "0 0 1 * *"
	case "@weekly":
		expr = "0 0 * * 0"
	case "@daily", "@midnight":
		name, expr = "@daily", "0 0 * * *"
	case "@hourly":
		expr = "0 * * * *"
	default:
		return nil, fmt.Errorf("unknown descriptor %s", f[0])
	}
	s, err := parse(expr)
	if err != nil {
		return nil, err
	}
	s.spec = name
	return s, nil
}
