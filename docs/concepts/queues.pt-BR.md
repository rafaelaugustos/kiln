# Filas e workers

Um servidor roda pools de workers, cada um atendendo suas filas em ordem, então uma primeira fila ocupada
pode deixar as outras esperando. `Weights` divide um pool entre suas filas em vez disso: sob carga, cada
fila recebe aproximadamente a fração de jobs pegos correspondente ao seu peso, e uma fila sem trabalho a
fazer cede sua fração para as outras.

```go
kiln.ServerConfig{Pools: []kiln.Pool{{
	Queues:  []string{"critical", "default", "low"},
	Workers: 20,
	Weights: map[string]int{"critical": 6, "default": 3},
}}}
```
