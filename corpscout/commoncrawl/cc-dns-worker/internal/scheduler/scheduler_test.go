package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPerServerPacing(t *testing.T) {
	s := New(Config{PerServerQPS: 5, Burst: 1, MaxInFlight: 100})
	ctx := context.Background()
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.Do(ctx, "1.2.3.4", func() error { return nil }) }()
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("6 calls at 5qps/burst1 took %v, want >= ~1s", elapsed)
	}
}

func TestServersAreIndependent(t *testing.T) {
	s := New(Config{PerServerQPS: 5, Burst: 3, MaxInFlight: 100})
	ctx := context.Background()
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		ip := "1.1.1.1"
		if i%2 == 0 {
			ip = "2.2.2.2"
		}
		wg.Add(1)
		go func(ip string) { defer wg.Done(); _ = s.Do(ctx, ip, func() error { return nil }) }(ip)
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("independent servers took %v, want fast", elapsed)
	}
}

func TestMaxInFlightCap(t *testing.T) {
	s := New(Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 2})
	ctx := context.Background()
	var cur, max int32
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Do(ctx, "9.9.9.9", func() error {
				n := atomic.AddInt32(&cur, 1)
				mu.Lock()
				if n > max {
					max = n
				}
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt32(&cur, -1)
				return nil
			})
		}()
	}
	wg.Wait()
	if max > 2 {
		t.Errorf("max in-flight = %d, want <= 2", max)
	}
}
