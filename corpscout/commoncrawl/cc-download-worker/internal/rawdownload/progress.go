package rawdownload

import (
	"context"
	"sync"
	"time"
)

// throttleCooldown pauses new logical record attempts after the source has exhausted its own retry
// budget with a throttling error. Signed S3 requests are also governed by the AWS adaptive retry
// limiter, which reacts to every 429/503 before an error reaches this layer.
type throttleCooldown struct {
	mu    sync.Mutex
	until time.Time
}

func (cooldown *throttleCooldown) slowDown(attempt int) time.Duration {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 4 {
		shift = 4
	}
	delay := time.Duration(1<<shift) * time.Second
	cooldown.mu.Lock()
	if until := time.Now().Add(delay); until.After(cooldown.until) {
		cooldown.until = until
	}
	cooldown.mu.Unlock()
	return delay
}

func (cooldown *throttleCooldown) wait(ctx context.Context) error {
	for {
		cooldown.mu.Lock()
		delay := time.Until(cooldown.until)
		cooldown.mu.Unlock()
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
