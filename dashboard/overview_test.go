package dashboard

import "testing"

func TestMood(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s          snapshot
		mood, heat string
	}{
		{snapshot{}, "fresh", "off"},
		{snapshot{Counts: counts{Enqueued: 3}}, "cold", "off"},
		{snapshot{Counts: counts{Failed: 2, Processing: 1}, Servers: 1, Workers: 10, Running: 1}, "failed", "low"},
		{snapshot{Counts: counts{Processing: 5}, Servers: 1, Workers: 10, Running: 5}, "busy", "mid"},
		{snapshot{Counts: counts{Enqueued: 4}, Servers: 2, Workers: 10}, "waiting", "idle"},
		{snapshot{Counts: counts{Succeeded: 9}, Servers: 1, Workers: 3, Running: 3}, "idle", "high"},
	}
	for _, tt := range tests {
		if got := mood(&tt.s); got != tt.mood {
			t.Errorf("mood(%+v) = %q, want %q", tt.s, got, tt.mood)
		}
		if got := heat(&tt.s); got != tt.heat {
			t.Errorf("heat(%+v) = %q, want %q", tt.s, got, tt.heat)
		}
	}
}
