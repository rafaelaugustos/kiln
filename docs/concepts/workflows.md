# Workflows and batches

## Continuations and flows

A job can depend on others by id (`After`, for a fixed set of ids already in the store) or by
index (`Needs`, for jobs being submitted together via `EnqueueMany`, resolved against ids kiln
assigns in the same call). `AfterFinished` runs regardless of whether the parent succeeded. A
dependency on a job that can never complete cascades: the dependent is inserted straight into
`deleted`.

```go
var flow kiln.Flow
fetch := flow.Add(FetchData{URL: src})
flow.Add(ProcessData{}, kiln.Needs{fetch})
client.EnqueueMany(ctx, flow...)
```

A continuation can read what its parents produced: `j.ParentOutputs(ctx)` returns the output of each parent
that succeeded, keyed by id.

## Batches

A `Batch` groups jobs and, optionally, a continuation (`Then`) that runs once every job in the
batch has finished:

```go
b := &kiln.Batch{Description: "nightly-import"}
b.Add(ImportFile{Name: "a.csv"})
b.Add(ImportFile{Name: "b.csv"})
b.Then(SendSummary{})
client.StartBatch(ctx, b)
```

Batches nest. An outer batch finishes once its own jobs are done and every batch nested in it has
finished, and a nested batch's continuations count as part of the outer one:

```go
month := &kiln.Batch{Description: "monthly close"}
for _, acct := range accounts {
	b := &kiln.Batch{Description: acct.Name}
	b.Add(CloseAccount{ID: acct.ID})
	b.Then(EmailStatement{ID: acct.ID})
	month.AddBatch(b)
}
month.Then(SendReport{})
client.StartBatch(ctx, month)
```

A running job can also open a batch inside its own with `kiln.Batch{Parent: j.BatchID}`.
