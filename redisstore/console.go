package redisstore

import (
	"context"
	"encoding/json"

	"github.com/rafaelaugustos/kiln/driver"
)

// WriteConsole appends lines to the console of the job ref points to and, when progress is 0 or
// more, sets its progress, while the job is processing under ref's claim. Otherwise it fails with
// [driver.ErrLost] and writes nothing.
func (s *Store) WriteConsole(ctx context.Context, r driver.Ref, lines []string, progress int) error {
	args := struct {
		ref
		Lines []string `json:"lines,omitempty"`
		Prog  int      `json:"prog"`
	}{ref{ID: r.ID, Claim: r.Claim}, lines, progress}
	return s.call(ctx, "write_console", args, nil)
}

// Logs returns the console lines of job id with a Seq above after, oldest first, at most limit of
// them, or 100 when limit is 0 or less. A job that does not exist has no lines.
func (s *Store) Logs(ctx context.Context, id, after int64, limit int) ([]driver.LogLine, error) {
	args := struct {
		ID    int64 `json:"id"`
		After int64 `json:"after"`
		Limit int   `json:"limit"`
	}{id, after, limitOr(limit, 100)}
	var res []string
	if err := s.call(ctx, "logs", args, &res); err != nil {
		return nil, err
	}
	out := make([]driver.LogLine, 0, len(res))
	for _, raw := range res {
		var l struct {
			Seq  int64  `json:"seq"`
			Att  int    `json:"att"`
			At   int64  `json:"at,string"`
			Text string `json:"text"`
		}
		if json.Unmarshal([]byte(raw), &l) == nil {
			out = append(out, driver.LogLine{Seq: l.Seq, Attempt: l.Att, At: when(l.At), Text: l.Text})
		}
	}
	return out, nil
}
