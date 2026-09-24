package dashboard

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/memstore"
)

func benchHandler(b *testing.B) (http.Handler, int64) {
	s := memstore.New()
	c := kiln.NewClient(s)
	var id int64
	for i := range 500 {
		var err error
		if id, err = c.Enqueue(context.Background(), email{To: "user" + strconv.Itoa(i) + "@example.com", Secret: "x"}, kiln.Queue("mailers")); err != nil {
			b.Fatal(err)
		}
	}
	return New(c, Options{Authorize: grant(ReadWrite)}), id
}

func BenchmarkPages(b *testing.B) {
	h, id := benchHandler(b)
	for _, path := range []string{"/", "/jobs/enqueued", "/jobs/" + strconv.FormatInt(id, 10), "/api/overview", "/api/jobs/enqueued"} {
		b.Run(path, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if res := get(h, path); res.code != http.StatusOK {
					b.Fatal(res.code)
				}
			}
		})
	}
}
