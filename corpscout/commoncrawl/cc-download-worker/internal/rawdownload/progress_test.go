package rawdownload

import (
	"context"
	"testing"
	"time"
)

func TestThrottleCooldownPausesNewAttempts(t *testing.T) {
	cooldown := &throttleCooldown{}
	if delay := cooldown.slowDown(1); delay != time.Second {
		t.Fatalf("first throttle delay=%s, want 1s", delay)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := cooldown.wait(ctx); err == nil {
		t.Fatal("throttle cooldown did not pause a new attempt")
	}
}

func TestThrottleCooldownBackoffIsCapped(t *testing.T) {
	cooldown := &throttleCooldown{}
	if delay := cooldown.slowDown(10); delay != 16*time.Second {
		t.Fatalf("capped throttle delay=%s, want 16s", delay)
	}
}
