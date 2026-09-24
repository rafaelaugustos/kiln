package dashboard_test

import (
	"log"
	"net/http"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/dashboard"
	"github.com/rafaelaugustos/kiln/memstore"
)

func ExampleNew() {
	client := kiln.NewClient(memstore.New())

	dash := dashboard.New(client, dashboard.Options{
		Authorize: func(r *http.Request) dashboard.Access {
			if r.Header.Get("Authorization") == "Bearer devtoken" {
				return dashboard.ReadWrite
			}
			return dashboard.ReadOnly
		},
	})

	mux := http.NewServeMux()
	mux.Handle("/kiln/", http.StripPrefix("/kiln", dash))

	log.Fatal(http.ListenAndServe(":8080", mux))
}
