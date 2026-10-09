# Queues and workers

A server runs pools of workers, each serving its queues in order, so a busy first queue can keep the others
waiting. `Weights` shares a pool between its queues instead: under load each queue gets about its weight's
share of the claims, and a queue with nothing to do leaves its share to the others.

```go
kiln.ServerConfig{Pools: []kiln.Pool{{
	Queues:  []string{"critical", "default", "low"},
	Workers: 20,
	Weights: map[string]int{"critical": 6, "default": 3},
}}}
```
