package scheduler

import (
	"context"
	"errors"
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

func frozen(s *Scheduler, clk *time.Time) {
	s.now = func() time.Time { return *clk }
}

func TestBreakerTripsAfterThresholdAndFastFails(t *testing.T) {
	s := New(Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10, BreakerThreshold: 3, BreakerCooldown: 30 * time.Second})
	clk := time.Unix(0, 0).UTC()
	frozen(s, &clk)
	ctx := context.Background()
	calls := 0
	failing := func() error { calls++; return errors.New("timeout") }

	for i := 0; i < 3; i++ {
		if err := s.Do(ctx, "1.2.3.4", failing); err == nil {
			t.Fatalf("call %d: want fn error", i)
		}
	}
	if calls != 3 {
		t.Fatalf("fn ran %d times, want 3", calls)
	}
	// 4th call: circuit open -> fast-fail, fn NOT run.
	if err := s.Do(ctx, "1.2.3.4", failing); err != ErrCircuitOpen {
		t.Fatalf("4th call err = %v, want ErrCircuitOpen", err)
	}
	if calls != 3 {
		t.Errorf("fn ran %d times after open, want still 3", calls)
	}
}

func TestBreakerCooldownHalfOpenClose(t *testing.T) {
	s := New(Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10, BreakerThreshold: 2, BreakerCooldown: 30 * time.Second})
	base := time.Unix(0, 0).UTC()
	clk := base
	frozen(s, &clk)
	ctx := context.Background()
	calls := 0
	failing := func() error { calls++; return errors.New("x") }
	ok := func() error { calls++; return nil }

	_ = s.Do(ctx, "1.1.1.1", failing)
	_ = s.Do(ctx, "1.1.1.1", failing) // opens (2 >= threshold 2)
	if err := s.Do(ctx, "1.1.1.1", failing); err != ErrCircuitOpen {
		t.Fatalf("want open, got %v", err)
	}
	before := calls // 2
	// still open before cooldown elapses
	clk = base.Add(29 * time.Second)
	if err := s.Do(ctx, "1.1.1.1", failing); err != ErrCircuitOpen {
		t.Fatalf("want still open at 29s, got %v", err)
	}
	// half-open after cooldown: fn runs; success closes the circuit
	clk = base.Add(31 * time.Second)
	if err := s.Do(ctx, "1.1.1.1", ok); err != nil {
		t.Fatalf("half-open probe should run fn, got %v", err)
	}
	if calls != before+1 {
		t.Fatalf("half-open should run fn once; calls=%d want=%d", calls, before+1)
	}
	// closed now: fn runs normally
	if err := s.Do(ctx, "1.1.1.1", ok); err != nil {
		t.Fatal(err)
	}
	if calls != before+2 {
		t.Errorf("closed circuit should run fn; calls=%d", calls)
	}
}

func TestBreakerCountsConsecutiveNotCumulative(t *testing.T) {
	s := New(Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10, BreakerThreshold: 3, BreakerCooldown: time.Second})
	clk := time.Unix(0, 0).UTC()
	frozen(s, &clk)
	ctx := context.Background()
	fail := func() error { return errors.New("x") }
	ok := func() error { return nil }
	_ = s.Do(ctx, "2.2.2.2", fail)
	_ = s.Do(ctx, "2.2.2.2", fail)
	_ = s.Do(ctx, "2.2.2.2", ok) // resets the consecutive counter
	_ = s.Do(ctx, "2.2.2.2", fail)
	_ = s.Do(ctx, "2.2.2.2", fail)
	// only 2 consecutive since the reset -> still closed
	ran := false
	if err := s.Do(ctx, "2.2.2.2", func() error { ran = true; return nil }); err != nil {
		t.Fatalf("circuit should be closed, got %v", err)
	}
	if !ran {
		t.Error("fn should run (circuit closed)")
	}
}

func TestBreakerPerServerIndependent(t *testing.T) {
	s := New(Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10, BreakerThreshold: 2, BreakerCooldown: time.Second})
	clk := time.Unix(0, 0).UTC()
	frozen(s, &clk)
	ctx := context.Background()
	fail := func() error { return errors.New("x") }
	_ = s.Do(ctx, "3.3.3.3", fail)
	_ = s.Do(ctx, "3.3.3.3", fail)
	if err := s.Do(ctx, "3.3.3.3", fail); err != ErrCircuitOpen {
		t.Fatal("3.3.3.3 should be open")
	}
	ran := false
	if err := s.Do(ctx, "4.4.4.4", func() error { ran = true; return nil }); err != nil {
		t.Fatalf("4.4.4.4 should be closed, got %v", err)
	}
	if !ran {
		t.Error("4.4.4.4 fn should run")
	}
}

func TestBreakerDisabledWhenThresholdZero(t *testing.T) {
	s := New(Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10}) // BreakerThreshold 0 -> disabled
	ctx := context.Background()
	calls := 0
	fail := func() error { calls++; return errors.New("x") }
	for i := 0; i < 10; i++ {
		_ = s.Do(ctx, "5.5.5.5", fail)
	}
	if calls != 10 {
		t.Errorf("breaker disabled: fn should run every time; calls=%d want 10", calls)
	}
}

// TestBreakerConcurrentNoRace hammers one dead server IP from many goroutines with the breaker
// enabled: it validates (under -race) that the per-server breaker state is safe under contention,
// and that once the circuit opens most calls are fast-failed rather than running fn. The frozen
// clock + long cooldown mean the circuit stays open for the whole test once tripped.
func TestBreakerConcurrentNoRace(t *testing.T) {
	s := New(Config{PerServerQPS: 100000, Burst: 100000, MaxInFlight: 8, BreakerThreshold: 5, BreakerCooldown: time.Hour})
	clk := time.Unix(0, 0).UTC()
	frozen(s, &clk)
	ctx := context.Background()
	var ran, opened int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Do(ctx, "9.9.9.9", func() error {
				atomic.AddInt64(&ran, 1)
				return errors.New("dead")
			})
			if err == ErrCircuitOpen {
				atomic.AddInt64(&opened, 1)
			}
		}()
	}
	wg.Wait()
	if opened == 0 {
		t.Error("expected an open circuit to fast-fail some concurrent calls")
	}
	if ran >= 200 {
		t.Errorf("fn ran %d times; an open circuit should have prevented most (want < 200)", ran)
	}
}
