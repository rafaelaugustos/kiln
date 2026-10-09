# Job console

A handler can write log lines and a progress bar that show up on the job's page in the dashboard while it
runs, like Hangfire.Console:

```go
func importRows(ctx context.Context, j *kiln.Job[Import]) error {
	for i, row := range j.Args.Rows {
		j.Logf("importing %s", row.ID)
		j.SetProgress(100 * i / len(j.Args.Rows))
	}
	return nil
}
```

Neither call takes a context or returns an error. kiln buffers them and writes them in the background,
and once more after the handler returns, so the console is complete when the job finishes. An attempt
keeps up to 1000 lines, each line up to 4 KiB; lines stay with the job, grouped by attempt, until it is
pruned.
