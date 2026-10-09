# Getting started

kiln needs Go 1.27 or later.

## With SQLite, nothing to install

```
go get github.com/rafaelaugustos/kiln github.com/rafaelaugustos/kiln/sqlitestore modernc.org/sqlite
```

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/sqlitestore"
	_ "modernc.org/sqlite"
)

type SendEmail struct {
	To string
}

func (SendEmail) Kind() string { return "send_email" }

func sendEmail(ctx context.Context, j *kiln.Job[SendEmail]) error {
	log.Printf("sending email to %s (attempt %d)", j.Args.To, j.Attempt)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	db, err := sql.Open("sqlite", "file:jobs.db?_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatal(err)
	}
	store, err := sqlitestore.New(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	client := kiln.NewClient(store)

	mux := kiln.NewMux()
	kiln.Handle(mux, sendEmail)
	server, err := kiln.NewServer(client, mux, kiln.ServerConfig{})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := client.Enqueue(ctx, SendEmail{To: "ada@example.com"}); err != nil {
		log.Fatal(err)
	}
	if err := server.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`go run .` creates `jobs.db`, enqueues the job and runs it right away. A job type is any struct with a
`Kind()`; `kiln.Handle` registers the function that runs it, and `Job[T]` hands it the decoded args along
with `Attempt`, `Meta`, `Tags` and the rest. Ctrl+C stops the server; anything not done yet stays in the
file for the next run.

## With PostgreSQL

```
docker run -d --name kiln-postgres -p 5432:5432 -e POSTGRES_PASSWORD=kiln postgres:17
go get github.com/rafaelaugustos/kiln/pgstore
```

Swap the store and keep the rest of the program, importing `github.com/jackc/pgx/v5/pgxpool` and
`github.com/rafaelaugustos/kiln/pgstore` in place of the SQLite packages:

```go
pool, err := pgxpool.New(ctx, "postgres://postgres:kiln@localhost:5432/postgres")
if err != nil {
	log.Fatal(err)
}
store, err := pgstore.New(ctx, pool)
if err != nil {
	log.Fatal(err)
}
defer store.Close()
```

`pgstore.New` copies the pool's settings into a pool of its own, 8 connections by default
(`pgstore.MaxConns` changes it), so kiln and the application never wait for each other's connections and
the application can keep using or close its pool.

`New` creates its tables in a `kiln` schema the first time. MySQL and SQL Server work the same way
through `mysqlstore` and `mssqlstore`; [Storage backends](backends.md) covers what each database
needs.

Complete, runnable programs for each topic below live in [examples/](https://github.com/rafaelaugustos/kiln/blob/main/examples/): [basic](https://github.com/rafaelaugustos/kiln/blob/main/examples/basic),
[workflow](https://github.com/rafaelaugustos/kiln/blob/main/examples/workflow), [recurring](https://github.com/rafaelaugustos/kiln/blob/main/examples/recurring), [throttling](https://github.com/rafaelaugustos/kiln/blob/main/examples/throttling),
[transactional](https://github.com/rafaelaugustos/kiln/blob/main/examples/transactional) and [dashboard](https://github.com/rafaelaugustos/kiln/blob/main/examples/dashboard).
