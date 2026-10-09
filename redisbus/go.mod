module github.com/rafaelaugustos/kiln/redisbus

go 1.27.0

toolchain go1.27.1

require (
	github.com/rafaelaugustos/kiln v0.9.0
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/rafaelaugustos/kiln => ../
