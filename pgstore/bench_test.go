package pgstore

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func batchOf(n int) []driver.InsertParams {
	jobs := make([]driver.InsertParams, n)
	for i := range jobs {
		jobs[i] = driver.InsertParams{Kind: "bench", Queue: "default", Args: []byte(`{"user":42,"email":"a@b.c"}`), MaxAttempts: 10}
	}
	return jobs
}

func BenchmarkInsert(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			s := open(b, MaxConns(8))
			ctx := context.Background()
			jobs := batchOf(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.Insert(ctx, jobs); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N*n)/b.Elapsed().Seconds(), "jobs/s")
		})
	}
}

func BenchmarkEnqueue(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		s := open(b)
		ctx := context.Background()
		jobs := batchOf(1)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := s.Insert(ctx, jobs); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/s")
	})
	b.Run("parallel", func(b *testing.B) {
		s := open(b, MaxConns(16))
		ctx := context.Background()
		b.SetParallelism(2)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			jobs := batchOf(1)
			for pb.Next() {
				if _, err := s.Insert(ctx, jobs); err != nil {
					b.Error(err)
					return
				}
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/s")
	})
}

func BenchmarkClaimFinish(b *testing.B) {
	for _, fetch := range []int{1, 10, 50} {
		b.Run("fetch="+strconv.Itoa(fetch), func(b *testing.B) {
			s := open(b, MaxConns(16))
			ctx := context.Background()
			for left := b.N; left > 0; left -= 10000 {
				if _, err := s.Insert(ctx, batchOf(min(left, 10000))); err != nil {
					b.Fatal(err)
				}
			}
			var (
				done, retried atomic.Int64
				wg            sync.WaitGroup
			)
			b.ResetTimer()
			for w := 0; w < 8; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					server := "w" + strconv.Itoa(w)
					outs := make([]driver.Outcome, 0, fetch)
					for done.Load() < int64(b.N) {
						jobs, err := s.Claim(ctx, driver.ClaimQuery{Queues: []string{"default"}, Limit: fetch, Server: server})
						if err != nil {
							b.Error(err)
							return
						}
						if len(jobs) == 0 {
							continue
						}
						outs = outs[:0]
						for _, j := range jobs {
							outs = append(outs, driver.Outcome{Ref: j.Ref, State: driver.Succeeded})
						}
						for len(outs) > 0 {
							res, err := s.Finish(ctx, server, outs)
							if err != nil {
								b.Error(err)
								return
							}
							busy := outs[:0]
							for i, r := range res {
								switch r {
								case driver.Applied:
									done.Add(1)
								case driver.Busy:
									busy = append(busy, outs[i])
									retried.Add(1)
								}
							}
							outs = busy
						}
					}
				}()
			}
			wg.Wait()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/s")
			b.ReportMetric(float64(retried.Load())/float64(b.N), "busy/job")
		})
	}
}
