# Testing with kilntest

`kilntest.Work` drives a job through the real frozen middleware chain and classification logic
without a server, for handler unit tests; its result includes the console lines and the progress. `RequireEnqueued`/`RequireNotEnqueued` assert on what a
piece of code actually enqueued. `drivertest.Run` is the conformance suite a `driver.Store`
implementation must pass.
