package memstore

type jobHeap struct {
	jobs []*job
	less func(a, b *job) bool
}

func (h *jobHeap) Len() int           { return len(h.jobs) }
func (h *jobHeap) Less(a, b int) bool { return h.less(h.jobs[a], h.jobs[b]) }

func (h *jobHeap) Swap(a, b int) {
	h.jobs[a], h.jobs[b] = h.jobs[b], h.jobs[a]
	h.jobs[a].slot = a
	h.jobs[b].slot = b
}

func (h *jobHeap) Push(x any) {
	j := x.(*job)
	j.heap, j.slot = h, len(h.jobs)
	h.jobs = append(h.jobs, j)
}

func (h *jobHeap) Pop() any {
	n := len(h.jobs) - 1
	j := h.jobs[n]
	h.jobs[n] = nil
	h.jobs = h.jobs[:n]
	j.heap = nil
	return j
}

func (h *jobHeap) peek() *job {
	if len(h.jobs) == 0 {
		return nil
	}
	return h.jobs[0]
}

func byPriority(a, b *job) bool {
	return a.priority > b.priority || a.priority == b.priority && a.id < b.id
}

func byRunAt(a, b *job) bool {
	return a.runAt.Before(b.runAt) || a.runAt.Equal(b.runAt) && a.id < b.id
}
