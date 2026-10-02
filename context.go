package kiln

import "context"

type ctxKey uint8

const (
	clientKey ctxKey = iota
	jobKey
)

// ClientFrom returns the client of the server running a job, from the context passed to the job's
// handler or one derived from it. It reports false for any other context, and under kilntest.Work,
// which runs handlers without a server.
func ClientFrom(ctx context.Context) (*Client, bool) {
	c, ok := ctx.Value(clientKey).(*Client)
	return c, ok && c != nil
}

// JobFrom returns the job being run, with its args still in JSON, from the context passed to its
// handler or one derived from it. It reports false for any other context.
func JobFrom(ctx context.Context) (*RawJob, bool) {
	j, ok := ctx.Value(jobKey).(*RawJob)
	return j, ok && j != nil
}
