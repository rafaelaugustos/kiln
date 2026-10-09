# Dashboard

```go
mux.Handle("/kiln/", dashboard.New(client, dashboard.Options{
	Prefix:    "/kiln",
	Authorize: func(r *http.Request) dashboard.Access { return dashboard.ReadOnly },
}))
```

`Authorize` runs on every request and returns `Denied`, `ReadOnly` or `ReadWrite`, so access
control plugs into whatever auth your app already has. `dashboard.AllowAll` is for local
development: it grants read/write access only to requests addressed to `localhost` or a loopback
IP, and denies everything else. The dashboard shows live counts and a succeeded/failed chart, jobs by
state with filters by queue, kind, batch and tag and bulk actions, job detail with titles, redactable
args/meta/output and the live console,
retries, recurring schedules and their groups, queues, servers, batches, and limits with what each key
is running and holding back, and mirrors all of it under a JSON API at
`<prefix>/api/...` for scripting. It's server-rendered with no external assets and a strict CSP.

The dashboard speaks English and Brazilian Portuguese. A picker in the header remembers each person's
choice; `Options.Language` sets the default, and without it the browser's language decides. Another
language is one JSON file in `dashboard/locales`.
