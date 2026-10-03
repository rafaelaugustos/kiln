// Package driver is the storage interface of kiln. It is written for people implementing a store;
// applications meet it only through the types package kiln re-exports and through [Writer], which
// enqueues jobs inside the application's own transaction.
//
// A store implements [Store], and may implement [Notifier] or [Bus] to wake servers,
// [Transactor] to group writes in one transaction and [Console] to keep what jobs log. Package
// memstore is the reference implementation, and drivertest.Run is the conformance suite every
// store must pass. A store must be safe for concurrent use by any number of servers and clients,
// and its errors must wrap the sentinel errors of this package wherever one applies.
//
// # Time
//
// A store keeps time with its own clock ([Coordinator.Now]) and computes run times, ages and
// expiries from it, so kiln never depends on the clocks of its servers agreeing. Durations cross
// the interface instead, such as [InsertParams.Delay] or the ttl of a lease. The instants that
// come from outside are [InsertParams.RunAt], set by the application, recurring occurrences, which
// kiln computes from times the store returned, and [ServerInfo.StartedAt], which is only displayed.
//
// # States
//
// Insert gives a job its first state: [Deleted] when a parent has already doomed it (see [Parent]),
// [Awaiting] while a dependency is unresolved, [Scheduled] when its run time is in the future,
// [Throttled] when it has a limit key, and [Enqueued] otherwise. [Worker.Claim] moves enqueued
// jobs to [Processing], and [Worker.Finish] moves them on. [Succeeded] and Deleted are archived;
// the other six states are live, [Failed] included, and a failed job stays failed until an operator
// requeues or deletes it.
//
// # Claims
//
// A claim increments the job's Attempt and Claim counters. Claim never decreases, and every write
// made for a running job ([Worker.Finish], [Worker.SetMeta], [Console.WriteConsole]) is fenced: it
// applies only while the job is processing under the same [Ref], so a server that lost a job
// cannot overwrite its state. An outcome with Refund set gives the attempt back, never the claim.
//
// # Limits
//
// A job with a limit key waits in throttled until admission moves it to enqueued. The rule of a key
// (LimitMax, LimitRate, LimitPer and LimitBurst) is stored with the key, and an insert that brings
// a different rule replaces it before admitting its own jobs. Admission runs wherever a key may
// gain room: in the store's own Insert, when an admitted job leaves enqueued and processing through
// an outcome, a rescue or a delete, and in Promote, Requeue and Sweep. It walks the throttled jobs
// of the key by priority, highest first, then by id, and admits each while fewer than LimitMax
// jobs of the key are enqueued or processing, LimitMax 0 meaning no cap.
//
// A key with a rate also paces starts with GCRA. With T = LimitPer/LimitRate and
// tau = (LimitBurst-1)*T, a job takes the start time at = max(now, tat-tau) and moves tat to
// max(tat, at)+T, an unset tat counting as now. If at is not in the future the job is admitted as
// above. Otherwise its slot is reserved: the job moves to scheduled with RunAt = at and is marked
// granted, without counting against LimitMax, and when Promote brings it back to throttled it
// needs no new slot. The walk stops at a job that should start now but finds no room, and after
// 1000 jobs of a rated key per call. Admission clears the grant, and so does a requeue, so every
// start takes a slot of its own; tat survives changes to the rule.
//
// A second GCRA state, admit_tat, keeps grants that came due together, after a stall for instance,
// from starting as a burst. With slack = T/2, a granted job may start only while admit_tat is unset
// or admit_tat-tau-slack <= now; otherwise it takes a new slot at the end of the line and stays
// granted. Each admission of a job of a rated key sets admit_tat = max(admit_tat, now)+T and then
// tat = max(tat, admit_tat).
package driver
