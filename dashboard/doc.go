// Package dashboard serves a web interface for a kiln store: live counts and a chart of finished
// jobs, jobs by state with filters and bulk actions, job details with args, meta, output and
// history, retries, recurring jobs, queues, servers, batches, and the limit keys of a store that
// implements [driver.LimitReader].
//
//	http.Handle("/kiln/", dashboard.New(client, dashboard.Options{
//		Prefix:    "/kiln",
//		Authorize: func(r *http.Request) dashboard.Access { return dashboard.ReadOnly },
//	}))
//
// Pages are rendered on the server, with no external assets and under a strict Content Security
// Policy. Every request goes through [Options.Authorize]. Changes are POST requests from the same
// origin with [ReadWrite] access; API calls that change something also need a JSON body.
//
// The pages are mirrored as JSON under <prefix>/api:
//
//	GET  /api/overview                job counts and totals of servers, workers and queues
//	GET  /api/series?range=1h|24h     finished jobs per minute, or per half hour for 24h
//	GET  /api/jobs/{state}            jobs in a state; query: queue, kind, batch, limit, cursor
//	GET  /api/jobs/{id}               one job with its history
//	GET  /api/retries                 scheduled jobs with Attempt above 0; query: limit, cursor
//	GET  /api/recurring, /api/queues, /api/servers, /api/batches, /api/batches/{id}
//	GET  /api/limits                  limit keys in key order, with their jobs; query: after, limit
//	POST /api/jobs/requeue            body {"ids": [...]} or {"state", "queue", "kind", "batch"}
//	POST /api/jobs/delete             same body
//	POST /api/jobs/{id}/requeue       also /delete
//	POST /api/recurring/{id}/trigger  also /pause, /resume and /remove
//	POST /api/queues/{name}/pause     also /resume
//
// Errors come back as {"error": "..."} with status 400, 404, 409 or 500.
package dashboard
