package kiln

import "context"

type ctxKey uint8

const (
	clientKey ctxKey = iota
	jobKey
)

func ClientFrom(ctx context.Context) (*Client, bool) {
	c, ok := ctx.Value(clientKey).(*Client)
	return c, ok && c != nil
}

func JobFrom(ctx context.Context) (*RawJob, bool) {
	j, ok := ctx.Value(jobKey).(*RawJob)
	return j, ok && j != nil
}
