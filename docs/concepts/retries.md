# Retries and cancellation

## Retries

Every kind gets `MaxAttempts` (default 10) and a `Backoff` that computes the delay before the next
attempt from the attempt number and the error. `kiln.Exponential`, `kiln.Constant` and
`kiln.Delays` cover the common cases; a handler can also return `kiln.Permanent(err)` to fail
without retrying, or `kiln.Snooze(d)` to reschedule itself without counting as a failure.
A failed job that you requeue gets all of its attempts again. Make the retries outlast the longest
outage of what a job calls, and alert on what fails anyway: see
[Retries and alerts](../operating.md).

## Cancellation

`client.Delete` on a job that is currently `processing` cancels that job's `context.Context` on
whichever server is running it, with `kiln.ErrCanceled` as the cause (`context.Cause(ctx)`). A
handler that stops and returns an error is recorded as `deleted`.
