module github.com/rafaelaugustos/kiln/mssqlstore

go 1.27.0

toolchain go1.27.1

require (
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9
	github.com/microsoft/go-mssqldb v1.11.2
	github.com/rafaelaugustos/kiln v0.8.0
)

require (
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/rafaelaugustos/kiln => ../
