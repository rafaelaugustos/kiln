package memstore

import (
	"context"
	"slices"

	"github.com/rafaelaugustos/kiln/driver"
)

// WriteConsole appends lines to the console of the job ref points to and, when progress is 0 or
// more, sets its progress, while the job is processing under ref's claim; otherwise it fails with
// [driver.ErrLost]. The lines of a job are numbered from 1.
func (s *Store) WriteConsole(_ context.Context, ref driver.Ref, lines []string, progress int) error {
	s.begin()
	defer s.end()
	j := s.jobs[ref.ID]
	if j == nil || j.state != driver.Processing || j.claim != ref.Claim {
		return driver.ErrLost
	}
	for _, text := range lines {
		seq := int64(len(j.logs)) + 1
		j.logs = append(j.logs, driver.LogLine{Seq: seq, Attempt: j.attempt, At: s.now, Text: text})
	}
	if progress >= 0 {
		j.progress = progress
	}
	return nil
}

// Logs returns the lines of job id with a Seq above after, oldest first, at most limit of them,
// or 100 when limit is 0 or less.
func (s *Store) Logs(_ context.Context, id int64, after int64, limit int) ([]driver.LogLine, error) {
	s.begin()
	defer s.end()
	j := s.jobs[id]
	if j == nil {
		return nil, nil
	}
	ls := j.logs[min(max(after, 0), int64(len(j.logs))):]
	return slices.Clone(ls[:min(len(ls), limitOr(limit, 100))]), nil
}
