module github.com/rafaelaugustos/kiln/mysqlstore

go 1.24.0

require (
	github.com/go-sql-driver/mysql v1.10.1
	github.com/rafaelaugustos/kiln v0.1.0
)

require filippo.io/edwards25519 v1.2.0 // indirect

replace github.com/rafaelaugustos/kiln => ../
