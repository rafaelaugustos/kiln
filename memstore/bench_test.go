package memstore_test

import (
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

func BenchmarkInsert(b *testing.B) {
	s := memstore.New()
	p := []driver.InsertParams{params("a")}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.Insert(ctx, p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClaimFinish(b *testing.B) {
	s := memstore.New()
	ps := make([]driver.InsertParams, 1000)
	for i := range ps {
		ps[i] = params("a", priority(int16(i%4)))
	}
	q := driver.ClaimQuery{Queues: []string{"default"}, Limit: 10, Server: "s"}
	outs := make([]driver.Outcome, 0, q.Limit)
	b.ReportAllocs()
	for b.Loop() {
		js, err := s.Claim(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		if len(js) == 0 {
			b.StopTimer()
			if _, err := s.Insert(ctx, ps); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			continue
		}
		outs = outs[:0]
		for _, j := range js {
			outs = append(outs, done(j, driver.Succeeded))
		}
		if _, err := s.Finish(ctx, "s", outs); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClaimParallel(b *testing.B) {
	s := memstore.New()
	ps := make([]driver.InsertParams, 1000)
	for i := range ps {
		ps[i] = params("a")
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		q := driver.ClaimQuery{Queues: []string{"default"}, Limit: 1, Server: "s"}
		for pb.Next() {
			js, err := s.Claim(ctx, q)
			if err != nil {
				b.Error(err)
				return
			}
			if len(js) == 0 {
				_, err = s.Insert(ctx, ps)
			} else {
				_, err = s.Finish(ctx, "s", []driver.Outcome{done(js[0], driver.Succeeded)})
			}
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}
