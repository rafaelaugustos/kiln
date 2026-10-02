// Package kilntest helps test code that uses kiln. [Work] runs a job through a handler the way a
// server would, without a store or a server, and [RequireEnqueued] and [RequireNotEnqueued] check
// what the code under test enqueued.
package kilntest
