# cc-dns-worker Per-Server Circuit Breaker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop wasting the per-query timeout budget on dead authoritative nameservers by adding a per-server-IP circuit breaker inside `internal/scheduler`: after N consecutive transport failures to a server IP, its circuit opens and further queries fast-fail for a cooldown instead of timing out.

**Architecture:** The breaker is extra per-server state in `Scheduler` (guarded by a per-server mutex) plus a check at the top of `Do` and an outcome-record after `fn`. A failure = `fn` returning an error (a transport/timeout failure; DNS rcodes like SERVFAIL are successful exchanges and don't count). Callers are unchanged — a fast-fail returns the exported `ErrCircuitOpen`, which flows through their existing "error → try next server" rotation. The existing per-server `MaxInFlight` cap bounds the half-open probe burst, so no separate probe flag is needed. A frozen/controllable injected clock makes the tests deterministic.

**Tech Stack:** Go 1.25 (existing module), `golang.org/x/time/rate`.

**Spec:** `docs/superpowers/specs/2026-07-06-cc-dns-circuit-breaker-design.md`

## Global Constraints
- Module `cc-dns-worker`; branch `main` (the shared workspace switches branches unreliably; the controller scopes review diffs to cc-dns-worker paths). Work from `commoncrawl/cc-dns-worker/`.
- go.mod floor `go 1.25.0` — do NOT change. **Do NOT run `go mod tidy`** (strips pre-fetched deps; controller reverts it). Do not edit go.mod/go.sum. Do not commit a binary/`.db`.
- Follow Conventional Commits; `go fmt ./...` + `go vet ./...` before each commit. Commit only the paths named per task.
- Breaker semantics (verbatim from spec): failure = `fn` returned an error (transport failure); success = `fn` returned nil. `fails` counts CONSECUTIVE failures since the last success. Circuit opens when `fails >= BreakerThreshold`, stays open until `now >= openUntil` (`= openTime + BreakerCooldown`), half-open on the next allowed call, closes on a success. `BreakerThreshold <= 0` DISABLES the breaker entirely (`Do` behaves exactly as before) — this keeps the existing scheduler tests unaffected.
- `scan` enables it by default: `--breaker-threshold` default `5`, `--breaker-cooldown` default `30s`, applied to BOTH the discovery and authoritative schedulers.

---

### Task 1: Circuit breaker in `scheduler`

**Files:**
- Modify: `commoncrawl/cc-dns-worker/internal/scheduler/scheduler.go` (replace the whole file)
- Test: `commoncrawl/cc-dns-worker/internal/scheduler/scheduler_test.go` (append breaker tests)

**Interfaces:**
- Consumes: `golang.org/x/time/rate`.
- Produces:
  - `scheduler.ErrCircuitOpen error` (sentinel).
  - `scheduler.Config` gains `BreakerThreshold int` and `BreakerCooldown time.Duration`.
  - `Do` unchanged signature; new fast-fail + record behavior. `Scheduler` gains an unexported `now func() time.Time` (set by in-package tests) — used by Task 2's `scan` only indirectly (Task 2 sets the two Config fields).

- [ ] **Step 1: Write the failing tests**

Append to `commoncrawl/cc-dns-worker/internal/scheduler/scheduler_test.go`:
```go
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
```
(The test file already imports `context`, `sync`, `sync/atomic`, `testing`, `time` for the existing tests; add `"errors"` to its import block.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/scheduler/ -run TestBreaker`
Expected: FAIL — `ErrCircuitOpen` undefined, `Config` has no `BreakerThreshold`/`BreakerCooldown`, `Scheduler` has no `now`.

- [ ] **Step 3: Replace `scheduler.go`**

Replace the entire contents of `commoncrawl/cc-dns-worker/internal/scheduler/scheduler.go` with:
```go
// Package scheduler paces outbound work per target server IP. Every DNS query passes through Do(),
// which grants a token from that server's bucket and a per-server in-flight slot before running fn.
// Do() also drives a per-server circuit breaker: after BreakerThreshold consecutive transport
// failures (fn returning an error) to one IP, that IP's circuit opens and Do() fast-fails with
// ErrCircuitOpen for BreakerCooldown, so a dead server stops wasting the query timeout for every
// domain that shares it. Single-process, in-memory; see the spec's shard-by-server note for the
// distributed path.
package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ErrCircuitOpen is returned by Do when the target server IP's circuit is open. Callers treat it
// like any other error and rotate to the next server.
var ErrCircuitOpen = errors.New("scheduler: circuit open")

// Config holds the per-server pacing and circuit-breaker knobs.
type Config struct {
	PerServerQPS float64
	Burst        int
	MaxInFlight  int
	// BreakerThreshold is the number of CONSECUTIVE transport failures (fn errors) to one server IP
	// that opens its circuit. <= 0 disables the breaker (Do never fast-fails).
	BreakerThreshold int
	// BreakerCooldown is how long a circuit stays open before the next call is allowed through as a
	// half-open probe.
	BreakerCooldown time.Duration
}

// Scheduler owns one limiter + in-flight semaphore + breaker per server IP, created lazily.
type Scheduler struct {
	cfg  Config
	now  func() time.Time // injectable clock for deterministic breaker tests
	mu   sync.Mutex
	lims map[string]*server
}

type server struct {
	lim  *rate.Limiter
	slot chan struct{}

	bmu       sync.Mutex // guards the breaker fields below
	fails     int        // consecutive fn errors since the last success
	openUntil time.Time  // zero = closed; a future time = open
}

// New returns a Scheduler; zero/negative pacing knobs fall back to safe defaults. BreakerThreshold
// <= 0 leaves the breaker off.
func New(cfg Config) *Scheduler {
	if cfg.PerServerQPS <= 0 {
		cfg.PerServerQPS = 10
	}
	if cfg.Burst <= 0 {
		cfg.Burst = 10
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 3
	}
	return &Scheduler{cfg: cfg, now: time.Now, lims: make(map[string]*server)}
}

func (s *Scheduler) forServer(ip string) *server {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sv, ok := s.lims[ip]; ok {
		return sv
	}
	sv := &server{
		lim:  rate.NewLimiter(rate.Limit(s.cfg.PerServerQPS), s.cfg.Burst),
		slot: make(chan struct{}, s.cfg.MaxInFlight),
	}
	s.lims[ip] = sv
	return sv
}

// allow reports whether a request may proceed: true when the circuit is closed or the cooldown has
// elapsed (half-open); false while open.
func (sv *server) allow(now time.Time) bool {
	sv.bmu.Lock()
	defer sv.bmu.Unlock()
	return sv.openUntil.IsZero() || !now.Before(sv.openUntil)
}

// record folds one outcome into the breaker: a success closes the circuit; threshold consecutive
// failures (re)open it for cooldown.
func (sv *server) record(now time.Time, ok bool, threshold int, cooldown time.Duration) {
	sv.bmu.Lock()
	defer sv.bmu.Unlock()
	if ok {
		sv.fails = 0
		sv.openUntil = time.Time{}
		return
	}
	sv.fails++
	if sv.fails >= threshold {
		sv.openUntil = now.Add(cooldown)
	}
}

// Do waits for a token and an in-flight slot for serverIP, then runs fn. If the breaker is enabled
// and serverIP's circuit is open, Do returns ErrCircuitOpen immediately — no slot, no token, no fn.
func (s *Scheduler) Do(ctx context.Context, serverIP string, fn func() error) error {
	sv := s.forServer(serverIP)
	breaker := s.cfg.BreakerThreshold > 0
	if breaker && !sv.allow(s.now()) {
		return ErrCircuitOpen
	}
	select {
	case sv.slot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-sv.slot }()
	if err := sv.lim.Wait(ctx); err != nil {
		return err
	}
	err := fn()
	if breaker {
		sv.record(s.now(), err == nil, s.cfg.BreakerThreshold, s.cfg.BreakerCooldown)
	}
	return err
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd commoncrawl/cc-dns-worker && go test ./internal/scheduler/ && go test -race ./internal/scheduler/`
Expected: PASS — all breaker tests plus the pre-existing `TestPerServerPacing`/`TestServersAreIndependent`/`TestMaxInFlightCap` (which don't set a threshold, so the breaker stays disabled and their behavior is unchanged). No data races.

- [ ] **Step 5: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./... && go vet ./internal/scheduler/
git add commoncrawl/cc-dns-worker/internal/scheduler/scheduler.go commoncrawl/cc-dns-worker/internal/scheduler/scheduler_test.go
git commit -m "feat(dns): per-server-IP circuit breaker in scheduler.Do"
```

---

### Task 2: Wire breaker flags into `scan` + document

**Files:**
- Modify: `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go` (2 flags + 2 Config literals)
- Modify: `commoncrawl/cc-dns-worker/README.md`

**Interfaces:**
- Consumes: `scheduler.Config.BreakerThreshold`/`BreakerCooldown` (Task 1).
- Produces: `--breaker-threshold` (default 5) and `--breaker-cooldown` (default 30s) flags on `scan`, applied to both schedulers. No unit test (flag wiring is gated by build/vet; the breaker itself is unit-tested in Task 1).

- [ ] **Step 1: Add the two flags**

In `commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go`, in the flag block, immediately AFTER the existing `timeout := fs.Duration("query-timeout", ...)` line, add:
```go
	breakerThreshold := fs.Int("breaker-threshold", 5, "consecutive transport failures before a server IP's circuit opens (0 disables)")
	breakerCooldown := fs.Duration("breaker-cooldown", 30*time.Second, "how long a server IP's circuit stays open before a half-open probe")
```

- [ ] **Step 2: Pass them to BOTH schedulers**

In the same file, replace the two `scheduler.New(...)` calls. Find:
```go
	discSched := scheduler.New(scheduler.Config{PerServerQPS: *discoveryQPS, Burst: max(1, int(*discoveryQPS)), MaxInFlight: *inflight})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: *qps, Burst: max(1, int(*qps)), MaxInFlight: *inflight})
```
Replace with:
```go
	discSched := scheduler.New(scheduler.Config{PerServerQPS: *discoveryQPS, Burst: max(1, int(*discoveryQPS)), MaxInFlight: *inflight, BreakerThreshold: *breakerThreshold, BreakerCooldown: *breakerCooldown})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: *qps, Burst: max(1, int(*qps)), MaxInFlight: *inflight, BreakerThreshold: *breakerThreshold, BreakerCooldown: *breakerCooldown})
```

- [ ] **Step 3: Verify build + full suite**

Run: `cd commoncrawl/cc-dns-worker && go build ./... && go vet ./... && go test ./...`
Expected: build/vet clean; all tests PASS.

- [ ] **Step 4: Confirm the flags exist + a breaker sanity run**

Run:
```bash
cd commoncrawl/cc-dns-worker && go build -o bin/cc-dns-worker ./cmd/cc-dns-worker
./bin/cc-dns-worker scan -h 2>&1 | grep -E 'breaker-threshold|breaker-cooldown'
```
Expected: both flags print with defaults `5` and `30s`.

Optional live sanity (CH env set — `CLICKHOUSE_ADDR=companycollect:9002 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=password123 CLICKHOUSE_DB=corpscout`):
```bash
rm -f /tmp/dns-brk.db*
./bin/cc-dns-worker scan --limit 30 --dispatch-batch 30 --db /tmp/dns-brk.db --scan-id brkchk --breaker-threshold 5 2>&1 | grep -E 'seeded|resolved|done'
rm -f /tmp/dns-brk.db*
```
Expected: completes normally (the breaker only changes timing on dead servers; a 30-domain run still finishes with a resolved count). Do NOT load this into ClickHouse (no cleanup needed — nothing written to CH).

- [ ] **Step 5: Update the README**

In `commoncrawl/cc-dns-worker/README.md`:
- Add `--breaker-threshold` (default 5) and `--breaker-cooldown` (default 30s) to the `scan` flags list, with accurate descriptions matching the flag help text.
- In the section describing the scheduler / rate limiting (or error handling), add a short paragraph: the per-server-IP scheduler also runs a **circuit breaker** — after `--breaker-threshold` consecutive *transport* failures (timeouts) to one authoritative NS IP, that IP's circuit opens and its queries fast-fail for `--breaker-cooldown` instead of each waiting the full `--query-timeout`; this caps the cost of dead/firewalled nameservers, especially when many domains share one. Note it counts transport failures only (a `SERVFAIL` response does not trip it) and set `--breaker-threshold 0` to disable.
- Remove the "per-server circuit breaker" bullet from the deferred/limitations list (it is now implemented); keep the disk / streaming-follow-up notes.
Match the README's existing tone; do not invent flags (every documented flag must exist in `scan.go`).

- [ ] **Step 6: Commit**

```bash
cd commoncrawl/cc-dns-worker && go fmt ./...
git add commoncrawl/cc-dns-worker/cmd/cc-dns-worker/scan.go commoncrawl/cc-dns-worker/README.md
git commit -m "feat(dns): scan --breaker-threshold/--breaker-cooldown flags + README"
```

---

## Self-review notes
- Spec §2/§5 breaker in `Do`, callers unchanged via `ErrCircuitOpen` → Task 1. ✓
- Spec §3 failure = transport error only (fn error) → `record(now, err == nil, ...)` in `Do`; SERVFAIL is a nil-fn success (unchanged exchange.go) → Task 1, asserted implicitly (tests drive fn success/failure directly). ✓
- Spec §4 minimal state (`fails` consecutive + `openUntil`), no probe flag → Task 1 `server` struct + `allow`/`record`. ✓
- Spec §6 config + on-by-default in scan + disable at threshold≤0 → Task 1 `Config`/`New`, Task 2 flags. ✓
- Spec §7 injectable clock → Task 1 `Scheduler.now`, tests set it via `frozen`. ✓
- Spec §8 tests (trip, cooldown/half-open/close, consecutive-not-cumulative, independence, disabled) → Task 1 five tests; existing pacing tests still pass (breaker off) → Step 4. ✓
- Type consistency: `Config.BreakerThreshold int`/`BreakerCooldown time.Duration` defined in Task 1 and set by name in Task 2's Config literals; `ErrCircuitOpen` sentinel used in Task 1 tests. ✓
```
