package metrics

import (
	"testing"
	"time"
)

func TestErrorWindowEvictsExpiredSamples(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	window := NewErrorWindow(10 * time.Minute)
	window.Add(start, 100, 10)
	window.Add(start.Add(5*time.Minute), 100, 20)
	if got := window.Percent(); got != 15 {
		t.Fatalf("five-minute error percentage = %.2f, want 15", got)
	}

	window.Add(start.Add(11*time.Minute), 100, 0)
	if got := window.Percent(); got != 10 {
		t.Fatalf("expired-window error percentage = %.2f, want 10", got)
	}
}

func TestErrorWindowHandlesIdleAndNegativeDeltas(t *testing.T) {
	window := NewErrorWindow(10 * time.Minute)
	now := time.Unix(0, 0).UTC()
	window.Add(now, 0, 0)
	window.Add(now.Add(time.Second), -1, -1)
	if got := window.Percent(); got != 0 {
		t.Fatalf("idle error percentage = %.2f, want 0", got)
	}
}
