module github.com/rafaelaugustos/kiln/bench

go 1.27.0

toolchain go1.27.1

replace (
	github.com/rafaelaugustos/kiln => ../
	github.com/rafaelaugustos/kiln/pgstore => ../pgstore
	github.com/rafaelaugustos/kiln/redisstore => ../redisstore
)

require (
	github.com/jackc/pgx/v5 v5.12.0
	github.com/rafaelaugustos/kiln v1.1.1
	github.com/rafaelaugustos/kiln/pgstore v1.1.1
	github.com/rafaelaugustos/kiln/redisstore v1.1.1
	github.com/redis/go-redis/v9 v9.22.0
	github.com/riverqueue/river v0.47.0
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.47.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/riverqueue/river/riverdriver v0.47.0 // indirect
	github.com/riverqueue/river/rivershared v0.47.0 // indirect
	github.com/riverqueue/river/rivertype v0.47.0 // indirect
	github.com/tidwall/gjson v1.19.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)
