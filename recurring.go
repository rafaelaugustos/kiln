package kiln

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/cron"
	"github.com/rafaelaugustos/kiln/driver"
)

const (
	maxCatchUp     = 100
	maxOccurrences = 1 << 20
	skipWindow     = time.Minute
	casAttempts    = 3
)

func (c *Client) SetRecurring(ctx context.Context, id, spec string, args Args, opts ...RecurringOption) error {
	if !validRecurringID(id) {
		return fmt.Errorf("%w: recurring id %q", ErrInvalid, id)
	}
	sched, err := cron.Parse(spec)
	if err != nil {
		return err
	}
	want, loc, err := buildRecurring(args, opts)
	if err != nil {
		return err
	}
	want.ID, want.Spec, want.Template.RecurringID = id, sched.String(), id
	return c.updateRecurring(ctx, id, true, func(cur *driver.Recurring, now time.Time) (bool, error) {
		reschedule := cur.Version == 0 || cur.Spec != want.Spec || cur.Location != want.Location || cur.NextRunAt.IsZero()
		if !reschedule && sameRecurring(*cur, want) {
			return false, nil
		}
		cur.Spec, cur.Location, cur.Template, cur.Misfire, cur.Overlap = want.Spec, want.Location, want.Template, want.Misfire, want.Overlap
		if reschedule {
			cur.NextRunAt = nextAfter(sched, loc, now, cur.LastRunAt)
		}
		return true, nil
	})
}

func (c *Client) RemoveRecurring(ctx context.Context, id string) error {
	return c.store.RemoveRecurring(ctx, id)
}

func (c *Client) PauseRecurring(ctx context.Context, id string) error {
	return c.updateRecurring(ctx, id, false, func(cur *driver.Recurring, _ time.Time) (bool, error) {
		if cur.Paused {
			return false, nil
		}
		cur.Paused = true
		return true, nil
	})
}

func (c *Client) ResumeRecurring(ctx context.Context, id string) error {
	return c.updateRecurring(ctx, id, false, func(cur *driver.Recurring, now time.Time) (bool, error) {
		if !cur.Paused {
			return false, nil
		}
		sched, loc, err := schedule(*cur)
		if err != nil {
			return false, err
		}
		cur.Paused = false
		cur.NextRunAt = nextAfter(sched, loc, now, cur.LastRunAt)
		return true, nil
	})
}

func (c *Client) TriggerRecurring(ctx context.Context, id string) (int64, error) {
	r, err := c.store.Recurring(ctx, id)
	if err != nil {
		return 0, err
	}
	now, err := c.store.Now(ctx)
	if err != nil {
		return 0, err
	}
	res, err := c.insertAll(ctx, c.store, []driver.InsertParams{occurrence(r, now, time.Time{})})
	if err != nil {
		return 0, err
	}
	return res[0].ID, nil
}

func (c *Client) updateRecurring(ctx context.Context, id string, create bool, fn func(*driver.Recurring, time.Time) (bool, error)) error {
	for range casAttempts {
		cur, err := c.store.Recurring(ctx, id)
		switch {
		case errors.Is(err, driver.ErrNotFound) && create:
			cur = driver.Recurring{ID: id}
		case err != nil:
			return err
		}
		now, err := c.store.Now(ctx)
		if err != nil {
			return err
		}
		changed, err := fn(&cur, now)
		if err != nil || !changed {
			return err
		}
		err = c.store.PutRecurring(ctx, cur)
		if !errors.Is(err, driver.ErrConflict) {
			return err
		}
	}
	return fmt.Errorf("%w: recurring %q", driver.ErrConflict, id)
}

func schedule(r driver.Recurring) (*cron.Schedule, *time.Location, error) {
	sched, err := cron.Parse(r.Spec)
	if err != nil {
		return nil, nil, fmt.Errorf("recurring %q: %w", r.ID, err)
	}
	loc, err := time.LoadLocation(r.Location)
	if err != nil {
		return nil, nil, fmt.Errorf("kiln: recurring %q: %w", r.ID, err)
	}
	return sched, loc, nil
}

func nextAfter(sched *cron.Schedule, loc *time.Location, now, last time.Time) time.Time {
	if last.After(now) {
		now = last
	}
	return sched.Next(now.In(loc))
}

func planFire(r driver.Recurring, now time.Time) (driver.Fire, error) {
	sched, loc, err := schedule(r)
	if err != nil {
		return driver.Fire{}, err
	}
	var occs []time.Time
	next := r.NextRunAt
	switch r.Misfire {
	case driver.MisfireAll:
		for !next.IsZero() && !next.After(now) && len(occs) < maxCatchUp {
			occs = append(occs, next)
			next = sched.Next(next.In(loc))
		}
	default:
		last := latest(sched, loc, next, now)
		if !last.IsZero() && (r.Misfire != driver.MisfireSkip || now.Sub(last) <= skipWindow) {
			occs = append(occs, last)
		}
		next = nextAfter(sched, loc, now, last)
	}
	f := driver.Fire{ID: r.ID, Version: r.Version, NextRunAt: next, LastRunAt: r.LastRunAt}
	for _, o := range occs {
		f.Jobs = append(f.Jobs, occurrence(r, o, o))
		f.LastRunAt = o
	}
	if f.LastRunAt.IsZero() {
		f.LastRunAt = r.LastRunAt
	}
	return f, nil
}

func latest(sched *cron.Schedule, loc *time.Location, first, now time.Time) time.Time {
	if first.IsZero() || first.After(now) {
		return time.Time{}
	}
	last := first
	for range maxOccurrences {
		n := sched.Next(last.In(loc))
		if n.IsZero() || n.After(now) {
			break
		}
		last = n
	}
	return last
}

func occurrence(r driver.Recurring, runAt, occ time.Time) driver.InsertParams {
	p := r.Template
	p.RunAt, p.Delay, p.RecurringID = runAt, 0, r.ID
	p.Parents, p.BatchID, p.AfterBatch = nil, 0, 0
	if !occ.IsZero() {
		m := make(map[string]string, len(p.Meta)+1)
		maps.Copy(m, p.Meta)
		m[metaOccurrence] = occ.UTC().Format(time.RFC3339)
		p.Meta = m
	}
	if !r.Overlap {
		p.UniqueKey, p.UniqueFor = uniqueKey("kiln.recurring", r.ID), 0
	}
	return p
}

func sameRecurring(a, b driver.Recurring) bool {
	return a.Spec == b.Spec && a.Location == b.Location && a.Misfire == b.Misfire && a.Overlap == b.Overlap &&
		sameParams(a.Template, b.Template)
}

func sameParams(a, b driver.InsertParams) bool {
	return a.Kind == b.Kind && a.Queue == b.Queue && bytes.Equal(a.Args, b.Args) && maps.Equal(a.Meta, b.Meta) &&
		slices.Equal(a.Tags, b.Tags) && a.Priority == b.Priority && a.MaxAttempts == b.MaxAttempts &&
		a.Timeout == b.Timeout && bytes.Equal(a.UniqueKey, b.UniqueKey) && a.UniqueFor == b.UniqueFor &&
		a.LimitKey == b.LimitKey && a.LimitMax == b.LimitMax && a.RecurringID == b.RecurringID
}

func validRecurringID(s string) bool {
	if len(s) == 0 || len(s) > 200 || s == "." || s == ".." {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !isLetter(c) && !isDigit(c) && c != '_' && c != '.' && c != ':' && c != '-' {
			return false
		}
	}
	return true
}
