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

// SetRecurring creates or updates the recurring job id, which inserts a job with args and opts at
// every occurrence of the cron spec (package cron describes the syntax), evaluated in the time
// zone of the [TZ] option, UTC by default. Each job is due at its occurrence, which it also
// carries in Meta under "kiln.occurrence". Ids are 1 to 200 bytes of letters, digits, '_', '.',
// ':' and '-'.
//
// Changing the spec or the time zone reschedules the job from now; other changes keep its next
// run. A paused job stays paused, and an identical definition is not written again. Setting a job
// of a [Client.SyncRecurring] group takes it out of the group. Occurrences are fired by the
// leader, so at least one server must run with maintenance enabled.
// SetRecurring fails with [ErrInvalid] for a bad id, spec or option, and with [driver.ErrConflict]
// if concurrent updates keep it from applying after three tries.
func (c *Client) SetRecurring(ctx context.Context, id, spec string, args Args, opts ...RecurringOption) error {
	set, err := define("", id, spec, args, opts)
	if err != nil {
		return err
	}
	return c.updateRecurring(ctx, id, true, set)
}

// SyncRecurring makes the recurring jobs of group match jobs: it sets each one as
// [Client.SetRecurring] does and records group with it, then removes the jobs of group whose id is
// not among jobs. Syncing the id of an existing recurring job moves it into group, and jobs set
// with SetRecurring have no group, so no sync removes them. A group follows the rules of recurring
// ids.
//
// Every job is checked before anything is written: a bad group, id, spec or option, or an id given
// twice, fails with [ErrInvalid]. After any other error the jobs set so far stay set and none is
// removed. When two syncs of one group race, the last write to each job wins.
func (c *Client) SyncRecurring(ctx context.Context, group string, jobs ...RecurringSpec) error {
	if !validRecurringID(group) {
		return fmt.Errorf("%w: recurring group %q", ErrInvalid, group)
	}
	sets := make([]edit, len(jobs))
	keep := make(map[string]bool, len(jobs))
	for i, j := range jobs {
		if keep[j.ID] {
			return fmt.Errorf("%w: recurring id %q given twice", ErrInvalid, j.ID)
		}
		set, err := define(group, j.ID, j.Spec, j.Args, j.Options)
		if err != nil {
			return err
		}
		sets[i], keep[j.ID] = set, true
	}
	for i, j := range jobs {
		if err := c.updateRecurring(ctx, j.ID, true, sets[i]); err != nil {
			return err
		}
	}
	rs, err := c.store.Recurrings(ctx)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if r.Group != group || keep[r.ID] {
			continue
		}
		if err := c.store.RemoveRecurring(ctx, r.ID); err != nil && !errors.Is(err, driver.ErrNotFound) {
			return err
		}
	}
	return nil
}

type edit func(cur *driver.Recurring, now time.Time) (bool, error)

func define(group, id, spec string, args Args, opts []RecurringOption) (edit, error) {
	if !validRecurringID(id) {
		return nil, fmt.Errorf("%w: recurring id %q", ErrInvalid, id)
	}
	sched, err := cron.Parse(spec)
	if err != nil {
		return nil, err
	}
	want, loc, err := buildRecurring(args, opts)
	if err != nil {
		return nil, err
	}
	want.ID, want.Group, want.Spec, want.Template.RecurringID = id, group, sched.String(), id
	return func(cur *driver.Recurring, now time.Time) (bool, error) {
		reschedule := cur.Version == 0 || cur.Spec != want.Spec || cur.Location != want.Location || cur.NextRunAt.IsZero()
		if !reschedule && sameRecurring(*cur, want) {
			return false, nil
		}
		cur.Group, cur.Spec, cur.Location, cur.Template = want.Group, want.Spec, want.Location, want.Template
		cur.Misfire, cur.Overlap = want.Misfire, want.Overlap
		if reschedule {
			cur.NextRunAt = nextAfter(sched, loc, now, cur.LastRunAt)
		}
		return true, nil
	}, nil
}

// RemoveRecurring deletes the recurring job id. Jobs it already inserted are not affected. It
// fails with [ErrNotFound] if there is no such recurring job.
func (c *Client) RemoveRecurring(ctx context.Context, id string) error {
	return c.store.RemoveRecurring(ctx, id)
}

// PauseRecurring stops the recurring job id from firing until [Client.ResumeRecurring]. Pausing
// a paused job does nothing.
func (c *Client) PauseRecurring(ctx context.Context, id string) error {
	return c.updateRecurring(ctx, id, false, func(cur *driver.Recurring, _ time.Time) (bool, error) {
		if cur.Paused {
			return false, nil
		}
		cur.Paused = true
		return true, nil
	})
}

// ResumeRecurring lets the recurring job id fire again, from its first occurrence after now:
// occurrences that passed while it was paused are skipped. Resuming a job that is not paused
// does nothing.
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

// TriggerRecurring inserts a job of the recurring job id right away, outside its schedule and
// even if it is paused, and returns the job's id. The schedule does not change. With
// Overlap(false), while the job of an earlier occurrence has not finished, nothing is inserted and
// the id returned is that job's.
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

func (c *Client) updateRecurring(ctx context.Context, id string, create bool, fn edit) error {
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
	return a.Group == b.Group && a.Spec == b.Spec && a.Location == b.Location && a.Misfire == b.Misfire &&
		a.Overlap == b.Overlap && sameParams(a.Template, b.Template)
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
