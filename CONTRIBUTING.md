# Contributing to kiln

Thanks for taking the time. Bug reports with a way to reproduce them are the most useful thing you can
send, followed by pull requests that fix one thing and come with a test.

## Layout

The repository holds several Go modules, released together under the same version:

| Module | What it is |
|---|---|
| `.` | client, server, `driver` (the storage interface), `memstore`, `drivertest`, `cron`, `dashboard`, `kilntest` |
| `pgstore`, `mysqlstore`, `sqlitestore` | the storage backends |
| `kilnotel` | OpenTelemetry instrumentation |
| `redisbus` | wakeups over Redis Pub/Sub |
| `examples` | runnable programs |
| `compat` | rolling-upgrade test against the previous release |
| `bench` | benchmarks against River, kept out of the regular test run |

Each module has `replace` directives that point at the directories next to it, so `go test` inside any of
them builds against your working copy without a `go.work`.

## Running the tests

You need Go 1.27. The root module needs nothing else:

```
go test -race ./...
```

The store modules test against real databases. With the defaults the tests expect:

```
docker run -d --name kiln-postgres -p 55432:5432 -e POSTGRES_USER=kiln -e POSTGRES_PASSWORD=kiln -e POSTGRES_DB=kiln postgres:17
docker run -d --name kiln-mysql -p 53306:3306 -e MYSQL_ROOT_PASSWORD=kiln -e MYSQL_DATABASE=kiln -e MYSQL_USER=kiln -e MYSQL_PASSWORD=kiln mysql:8.4
docker run -d --name kiln-redis -p 56379:6379 redis:7-alpine
```

The MySQL tests create a database per test, so once MySQL is up, let the `kiln` user do that:

```
docker exec kiln-mysql mysql -uroot -pkiln -e "GRANT ALL ON *.* TO 'kiln'@'%'"
```

Then run `go test -race ./...` inside `pgstore`, `mysqlstore`, `sqlitestore`, `kilnotel` and `redisbus`.
`KILN_DATABASE_URL`, `KILN_MYSQL_DSN` and `KILN_REDIS_ADDR` point the tests somewhere else; a database that
can't be reached makes its tests skip, not fail. Every test creates its own schema, tables or keys, so the
suites can run at the same time against the same containers.

`compat` downloads the previous release and runs it next to your code, see its
[README](compat/README.md):

```
cd compat && GOWORK=off go test ./...
```

## Changing how stores behave

`drivertest.Run` is the contract between kiln and its stores, and every store, `memstore` included, runs
it. A change to what a store does goes into `drivertest` first, then into `memstore`, which is the
reference, and then into each SQL store. Schema changes only add things: a new file in the store's
`changes/` directory, never an edit to a file that already shipped. `compat` checks both.

## Style

- `gofmt` and `go vet` clean, tests passing with `-race`.
- No comments in the code, except doc comments on exported identifiers and directives such as
  `//go:embed`. Names and small functions carry the meaning; if a piece of code needs a comment to be
  understood, it usually needs a better name or a test.
- Doc comments follow the usual Go style: a full sentence that starts with the name, and says what the
  signature can't, such as defaults, units, errors and edge cases.
- When a change affects behaviour that the README or `docs/` describe, update them in the same pull
  request, and add a line to the [changelog](CHANGELOG.md) under an `Unreleased` heading at the top.

## Pull requests

Keep them focused, explain the problem they solve, and mention the issue they close. A bug fix should come
with a test that fails without it.
