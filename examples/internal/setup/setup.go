package setup

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rafaelaugustos/kiln/pgstore"
)

func DBURL() string {
	if u := os.Getenv("KILN_DATABASE_URL"); u != "" {
		return u
	}
	return "postgres://kiln:kiln@localhost:55432/kiln?sslmode=disable"
}

func Store(ctx context.Context) (*pgstore.Store, error) {
	pool, err := pgxpool.New(ctx, DBURL())
	if err != nil {
		return nil, err
	}
	return pgstore.New(ctx, pool)
}
