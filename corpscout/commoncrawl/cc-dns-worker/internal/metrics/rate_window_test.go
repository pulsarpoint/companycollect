package metrics

import (
	"testing"
	"time"
)

func TestRateWindowUsesRetainedIntervalDuration(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	window := NewRateWindow(time.Minute)
	window.Add(start.Add(time.Second), 100, time.Second)
	window.Add(start.Add(2*time.Second), 300, time.Second)
	if got := window.PerSecond(); got != 200 {
		t.Fatalf("rate = %.1f/s, want 200/s", got)
	}
}

func TestRateWindowEvictsExpiredIntervals(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	window := NewRateWindow(time.Minute)
	window.Add(start, 100, time.Second)
	window.Add(start.Add(time.Minute), 300, time.Second)
	if got := window.PerSecond(); got != 300 {
		t.Fatalf("rate after eviction = %.1f/s, want 300/s", got)
	}
}
