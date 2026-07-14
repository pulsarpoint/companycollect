# Part Retry with Backoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Failed parts in the range-runner pool are requeued with exponential backoff instead of failing the run; a part counts as Failed only after 8 attempts, and the circuit breaker becomes two-phase so bursty S3 coldness can no longer kill a working run.

**Architecture:** `runRangePool` is restructured from mutex-shared worker tallying into a dispatcher goroutine (the pool function itself) that owns all scheduling state — a min-heap of pending parts ordered by eligible-at time — and workers that only execute attempts and report outcomes over a channel. Spec: `docs/superpowers/specs/2026-07-14-part-retry-backoff-design.md`.

**Tech Stack:** Go 1.26 (`container/heap`, `time.Timer` with ≥1.23 reset semantics), existing test fixtures in `cmd/cc-enrich-worker/runrange_test.go`.

## Global Constraints

- Retry schedule: `1, 2, 4, 8, 16, 30, 30` minutes (cap 30), max **8** attempts per part. Constants, no new flags.
- Backoff base is the package variable `partBackoffBase` (default `time.Minute`) so tests shrink it to microseconds.
- Breaker: phase 1 (zero successful produces yet) trips on 5 consecutive attempt failures of any kind; phase 2 (after any success) trips only on 5 consecutive exhausted parts. `consecutiveFailureLimit = 5` is unchanged.
- No changes to `producePart`, markers, catalog, loader, `status`, or `maxFetchErrorRate`.
- Failure is never persisted: an exhausted part ends the run unmarked and is reproduced by the next run.
- All work happens in `cmd/cc-enrich-worker/`. Working directory for all commands: `corpscout/services/cc-processor/cc-enrich-worker/`.
- Repo rule: commit at each task boundary; Conventional Commits; `gofmt` + `go vet` clean.

---

### Task 1: Retry primitives — backoff schedule and pending-part heap

**Files:**
- Create: `cmd/cc-enrich-worker/runretry.go`
- Test: `cmd/cc-enrich-worker/runretry_test.go`

**Interfaces:**
- Consumes: nothing (self-contained).
- Produces: `const maxPartAttempts = 8`; `var partBackoffBase time.Duration`; `func partBackoff(failedAttempts int) time.Duration`; `type pendingPart struct { part uint32; attempts int; eligibleAt time.Time; seq int }`; `type partQueue` with methods `add(pendingPart)`, `peek() pendingPart`, `next() pendingPart`, `Len() int`. Task 3 depends on all of these exact names.

- [ ] **Step 1: Write the failing test**

Create `cmd/cc-enrich-worker/runretry_test.go`:

```go
package main

import (
	"testing"
	"time"
)

func TestPartBackoffSchedule(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{1, 1 * time.Minute},
		{2, 2 * time.Minute},
		{3, 4 * time.Minute},
		{4, 8 * time.Minute},
		{5, 16 * time.Minute},
		{6, 30 * time.Minute},
		{7, 30 * time.Minute},
		{99, 30 * time.Minute},
	}
	for _, c := range cases {
		if got := partBackoff(c.failures); got != c.want {
			t.Errorf("partBackoff(%d) = %v, want %v", c.failures, got, c.want)
		}
	}
}

func TestPartQueueOrdersByEligibleAtThenFIFO(t *testing.T) {
	q := &partQueue{}
	now := time.Now()
	q.add(pendingPart{part: 10}) // never attempted: zero eligibleAt, eligible immediately
	q.add(pendingPart{part: 11}) // same eligibleAt as 10 -> FIFO by insertion
	q.add(pendingPart{part: 12, attempts: 1, eligibleAt: now.Add(time.Hour)})
	q.add(pendingPart{part: 13, attempts: 1, eligibleAt: now.Add(time.Minute)})

	var got []uint32
	for q.Len() > 0 {
		got = append(got, q.next().part)
	}
	want := []uint32{10, 11, 13, 12}
	if !reflectEqualU32(got, want) {
		t.Errorf("pop order = %v, want %v", got, want)
	}
}
```

(`reflectEqualU32` already exists in `runrange_test.go`, same package.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/cc-enrich-worker/ -run 'TestPartBackoff|TestPartQueue' -v`
Expected: FAIL to build with `undefined: partBackoff`, `undefined: partQueue`, `undefined: pendingPart`

- [ ] **Step 3: Write minimal implementation**

Create `cmd/cc-enrich-worker/runretry.go`:

```go
package main

import (
	"container/heap"
	"time"
)

// Retry policy for failed parts (spec 2026-07-14-part-retry-backoff-design.md): a failed part is
// requeued with exponential backoff instead of failing the run, and counts as Failed only after
// maxPartAttempts attempts (~1.5 h of waits — sized to the observed S3-coldness recovery horizon).
const maxPartAttempts = 8

// partBackoffBase is a variable, not a constant, so unit tests shrink minutes to microseconds.
var partBackoffBase = time.Minute

// partBackoffSteps are multiples of partBackoffBase applied after the n-th failed attempt.
var partBackoffSteps = []time.Duration{1, 2, 4, 8, 16, 30, 30}

// partBackoff returns how long a part waits after its n-th failed attempt (1-based).
func partBackoff(failedAttempts int) time.Duration {
	if failedAttempts < 1 {
		failedAttempts = 1
	}
	step := failedAttempts - 1
	if step >= len(partBackoffSteps) {
		step = len(partBackoffSteps) - 1
	}
	return partBackoffSteps[step] * partBackoffBase
}

// pendingPart is one schedulable part attempt. seq breaks eligibleAt ties FIFO so initial parts
// dispatch in their original order and a requeue never overtakes a fresh part scheduled at the
// same instant. The zero eligibleAt of a never-attempted part sorts before every retry time.
type pendingPart struct {
	part       uint32
	attempts   int       // failed attempts so far
	eligibleAt time.Time // zero for never-attempted parts: eligible immediately
	seq        int       // set by partQueue.add
}

// partQueue is a min-heap of pendingPart by (eligibleAt, seq). It is not safe for concurrent use;
// only the pool dispatcher touches it.
type partQueue struct {
	items []pendingPart
	seq   int
}

func (q *partQueue) Len() int { return len(q.items) }

func (q *partQueue) Less(i, j int) bool {
	if !q.items[i].eligibleAt.Equal(q.items[j].eligibleAt) {
		return q.items[i].eligibleAt.Before(q.items[j].eligibleAt)
	}
	return q.items[i].seq < q.items[j].seq
}

func (q *partQueue) Swap(i, j int) { q.items[i], q.items[j] = q.items[j], q.items[i] }

func (q *partQueue) Push(x any) { q.items = append(q.items, x.(pendingPart)) }

func (q *partQueue) Pop() any {
	old := q.items
	n := len(old)
	item := old[n-1]
	q.items = old[:n-1]
	return item
}

// add enqueues p with the next FIFO sequence number.
func (q *partQueue) add(p pendingPart) {
	p.seq = q.seq
	q.seq++
	heap.Push(q, p)
}

// peek returns the earliest-eligible part without removing it. Callers must check Len() > 0.
func (q *partQueue) peek() pendingPart { return q.items[0] }

// next removes and returns the earliest-eligible part. Callers must check Len() > 0.
func (q *partQueue) next() pendingPart { return heap.Pop(q).(pendingPart) }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/cc-enrich-worker/ -run 'TestPartBackoff|TestPartQueue' -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add cmd/cc-enrich-worker/runretry.go cmd/cc-enrich-worker/runretry_test.go
git commit -m "feat(corpscout/cc-enrich-worker): part retry backoff schedule and pending-part heap"
```

---

### Task 2: retrywait gauge in pool progress and the stats line

**Files:**
- Modify: `cmd/cc-enrich-worker/rangestats.go` (poolProgress, poolSnapshot, formatRangeStats)
- Test: `cmd/cc-enrich-worker/rangestats_test.go`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `(*poolProgress).addRetryWait()`, `(*poolProgress).endRetryWait()` (both nil-safe), `poolSnapshot.retryWait int64`, and the stats line format `stats: parts run=%d done=%d/%d skip=%d fail=%d retrywait=%d | ...`. Task 3's dispatcher calls the two methods.

- [ ] **Step 1: Update the golden strings so the tests fail first**

In `cmd/cc-enrich-worker/rangestats_test.go`, make three edits:

Edit 1 — the full-stats test gets a non-zero gauge. Replace:

```go
	pool := poolSnapshot{inFlight: 8, produced: 42, skipped: 2, failed: 1, total: 100}
```

with:

```go
	pool := poolSnapshot{inFlight: 8, produced: 42, skipped: 2, failed: 1, retryWait: 3, total: 100}
```

Edit 2 — same test's expectation. Replace:

```go
	want := "stats: parts run=8 done=42/100 skip=2 fail=1 | pages 200.7/s (1204480 total) errs=0 | s3 1652 req/s 51.0 MiB/s 429=0 5xx=0 retries=0 | avg fetch=165ms tech=38ms"
```

with:

```go
	want := "stats: parts run=8 done=42/100 skip=2 fail=1 retrywait=3 | pages 200.7/s (1204480 total) errs=0 | s3 1652 req/s 51.0 MiB/s 429=0 5xx=0 retries=0 | avg fetch=165ms tech=38ms"
```

Edit 3 — the two zero-value expectations. Replace:

```go
	want := "stats: parts run=1 done=0/4 skip=0 fail=0 | pages 0.0/s (0 total) errs=0 | avg fetch=0ms tech=0ms"
```

with:

```go
	want := "stats: parts run=1 done=0/4 skip=0 fail=0 retrywait=0 | pages 0.0/s (0 total) errs=0 | avg fetch=0ms tech=0ms"
```

and replace:

```go
	want := "stats: parts run=8 done=1/8 skip=0 fail=0 | pages 0.2/s (10 total) errs=4 | s3 0 req/s 0.0 MiB/s 429=7 5xx=3 retries=50 | avg fetch=0ms tech=0ms"
```

with:

```go
	want := "stats: parts run=8 done=1/8 skip=0 fail=0 retrywait=0 | pages 0.2/s (10 total) errs=4 | s3 0 req/s 0.0 MiB/s 429=7 5xx=3 retries=50 | avg fetch=0ms tech=0ms"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/cc-enrich-worker/ -run TestFormatRangeStats -v`
Expected: FAIL to build with `unknown field retryWait in struct literal` (Edit 1 references a field that doesn't exist yet)

- [ ] **Step 3: Implement the gauge and format change**

In `cmd/cc-enrich-worker/rangestats.go`:

Edit 1 — add the counter to `poolProgress`. Replace:

```go
type poolProgress struct {
	inFlight, produced, skipped, failed atomic.Int64
	total                               int
}
```

with:

```go
type poolProgress struct {
	inFlight, produced, skipped, failed atomic.Int64
	retryWait                           atomic.Int64 // parts requeued and waiting out a backoff
	total                               int
}
```

Edit 2 — add the nil-safe methods directly after `addFailed`:

```go
// addRetryWait / endRetryWait track how many parts are requeued and waiting out a retry backoff.
// The dispatcher increments on requeue and decrements when the retry is dispatched.
func (p *poolProgress) addRetryWait() {
	if p != nil {
		p.retryWait.Add(1)
	}
}

func (p *poolProgress) endRetryWait() {
	if p != nil {
		p.retryWait.Add(-1)
	}
}
```

Edit 3 — extend the snapshot. Replace:

```go
type poolSnapshot struct {
	inFlight, produced, skipped, failed int64
	total                               int
}

func (p *poolProgress) snapshot() poolSnapshot {
	if p == nil {
		return poolSnapshot{}
	}
	return poolSnapshot{
		inFlight: p.inFlight.Load(),
		produced: p.produced.Load(),
		skipped:  p.skipped.Load(),
		failed:   p.failed.Load(),
		total:    p.total,
	}
}
```

with:

```go
type poolSnapshot struct {
	inFlight, produced, skipped, failed int64
	retryWait                           int64
	total                               int
}

func (p *poolProgress) snapshot() poolSnapshot {
	if p == nil {
		return poolSnapshot{}
	}
	return poolSnapshot{
		inFlight:  p.inFlight.Load(),
		produced:  p.produced.Load(),
		skipped:   p.skipped.Load(),
		failed:    p.failed.Load(),
		retryWait: p.retryWait.Load(),
		total:     p.total,
	}
}
```

Edit 4 — extend the format line. In `formatRangeStats`, replace:

```go
	fmt.Fprintf(&b, "stats: parts run=%d done=%d/%d skip=%d fail=%d",
		pool.inFlight, pool.produced, pool.total, pool.skipped, pool.failed)
```

with:

```go
	fmt.Fprintf(&b, "stats: parts run=%d done=%d/%d skip=%d fail=%d retrywait=%d",
		pool.inFlight, pool.produced, pool.total, pool.skipped, pool.failed, pool.retryWait)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/cc-enrich-worker/ -run TestFormatRangeStats -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/cc-enrich-worker/rangestats.go cmd/cc-enrich-worker/rangestats_test.go
git commit -m "feat(corpscout/cc-enrich-worker): retrywait gauge in range-run stats line"
```

---

### Task 3: Dispatcher rewrite of runRangePool with requeue and two-phase breaker

**Files:**
- Modify: `cmd/cc-enrich-worker/runrange.go` (consecutiveFailureLimit comment, rangeSummary, runRangePool + new partOutcome/runPartAttempt/poolDispatcher)
- Test: `cmd/cc-enrich-worker/runrange_test.go`

**Interfaces:**
- Consumes: `pendingPart`, `partQueue`, `partBackoff`, `maxPartAttempts`, `partBackoffBase` (Task 1); `addRetryWait`/`endRetryWait` (Task 2).
- Produces: `rangeSummary.Retries int` (Task 4 prints it). `runRangePool`'s signature is unchanged.

- [ ] **Step 1: Add the test helper and update the two existing tests whose expectations change**

In `cmd/cc-enrich-worker/runrange_test.go`:

Edit 1 — add `"time"` to the imports block (after `"testing"`).

Edit 2 — add the helper after the `outDirForTest` function:

```go
// shrinkBackoff makes retry backoffs effectively immediate so exhaustion tests run in milliseconds.
func shrinkBackoff(t *testing.T) {
	t.Helper()
	old := partBackoffBase
	partBackoffBase = time.Microsecond
	t.Cleanup(func() { partBackoffBase = old })
}
```

Edit 3 — `TestRunRangePoolFailureAndResume` now exercises in-run retries. Run 1: part 2's bytes are withheld permanently, so after parts 0/1/3 produce (phase 2) it exhausts all `maxPartAttempts` attempts and is the run's one Failed part — the original assertions hold. Run 2 is different: every healthy part *skips* via its marker, and per the spec skips do NOT count as a success — the run stays in phase 1, so part 2's 5th consecutive failure trips the breaker before it can exhaust. Add `shrinkBackoff(t)` as the first line of the test body (right after `func TestRunRangePoolFailureAndResume(t *testing.T) {`), and replace the run-2 assertions:

```go
	sum2 := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", "test-run", outDirFor, produce, nil)
	if sum2.Skipped != 3 {
		t.Errorf("run2 skipped = %d, want 3", sum2.Skipped)
	}
	if len(producedParts) != 1 || producedParts[2] != 1 {
		t.Errorf("run2 attempted parts = %v, want only {2:1}", producedParts)
	}
```

with:

```go
	sum2 := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", "test-run", outDirFor, produce, nil)
	if sum2.Skipped != 3 {
		t.Errorf("run2 skipped = %d, want 3", sum2.Skipped)
	}
	// Run 2 never produces (healthy parts all skip, and a skip is not a phase-2 transition), so the
	// still-cold part 2 trips the phase-1 breaker on its 5th consecutive failure instead of
	// exhausting all attempts. The part stays unmarked either way.
	if !sum2.Breaker {
		t.Error("run2: breaker should trip — only failures and skips, no successes")
	}
	if len(producedParts) != 1 || producedParts[2] != consecutiveFailureLimit {
		t.Errorf("run2 attempted parts = %v, want only {2:%d}", producedParts, consecutiveFailureLimit)
	}
```

(`TestRunRangePoolBreaker` needs no changes: with zero successes, the 5th consecutive attempt failure trips in phase 1 before any 1-minute retry becomes eligible, so Failed=5 and attempts=5 hold.)

- [ ] **Step 2: Add the new behavior tests**

Append to `cmd/cc-enrich-worker/runrange_test.go`:

```go
// TestRunRangePoolRetriesThenSucceeds proves a transiently-failing part is requeued with backoff,
// its stale output debris is wiped between attempts, and it ends Produced — not Failed.
func TestRunRangePoolRetriesThenSucceeds(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{{index: 0, present: true}})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	var attempts atomic.Int64
	var junk string
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		if attempts.Add(1) <= 2 {
			// Simulate a produce that failed after partial output: the debris must be wiped
			// by the stale-dir cleanup before the retry attempt.
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				t.Fatal(err)
			}
			junk = filepath.Join(outDir, "partial.tmp")
			if err := os.WriteFile(junk, []byte("debris"), 0o644); err != nil {
				t.Fatal(err)
			}
			return partResult{}, fmt.Errorf("transient S3 coldness")
		}
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0}, 1, "tech", "test-run", outDirFor, produce, nil)

	if sum.Produced != 1 || sum.Failed != 0 || sum.Retries != 2 || sum.Breaker {
		t.Fatalf("summary = %+v, want produced=1 failed=0 retries=2 breaker=false", sum)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	if !markers.Exists(markers.ProducedPath(outDirFor(0))) {
		t.Error("part 0 should be marked produced")
	}
	if _, err := os.Stat(junk); !os.IsNotExist(err) {
		t.Errorf("junk from failed attempt survived: err=%v", err)
	}
	if got := domainsFromParquet(t, outDirFor(0)); !reflectEqualStr(got, []string{"d0.com"}) {
		t.Errorf("domains = %v", got)
	}
}

// TestRunRangePoolExhaustsPersistentlyFailingPart proves a part whose WARC bytes never appear is
// attempted exactly maxPartAttempts times, then counted Failed once and left unmarked.
func TestRunRangePoolExhaustsPersistentlyFailingPart(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{
		{index: 0, present: true}, // succeeds first: the run is in phase 2 before the failures
		{index: 1, present: false},
	})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	var attempts1 atomic.Int64
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		if part == 1 {
			attempts1.Add(1)
		}
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0, 1}, 1, "tech", "test-run", outDirFor, produce, nil)

	if sum.Produced != 1 || sum.Failed != 1 || !reflectEqualU32(sum.FailedParts, []uint32{1}) || sum.Breaker {
		t.Fatalf("summary = %+v, want produced=1 failed=1 failedParts=[1] breaker=false", sum)
	}
	if sum.Retries != maxPartAttempts-1 {
		t.Errorf("retries = %d, want %d", sum.Retries, maxPartAttempts-1)
	}
	if got := attempts1.Load(); got != maxPartAttempts {
		t.Errorf("part 1 attempts = %d, want %d", got, maxPartAttempts)
	}
	if markers.Exists(markers.ProducedPath(outDirFor(1))) {
		t.Error("exhausted part must not be marked produced")
	}
}

// TestRunRangePoolTransientFailuresDoNotTripBreakerAfterSuccess: 16 attempt failures (far past the
// old 5-consecutive limit) from two cold parts must not kill a run that has already produced —
// phase 2 counts only consecutive EXHAUSTED parts, and 2 < 5.
func TestRunRangePoolTransientFailuresDoNotTripBreakerAfterSuccess(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{
		{index: 0, present: true},
		{index: 1, present: false}, {index: 2, present: false},
		{index: 3, present: true}, {index: 4, present: true},
	})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3, 4}, 1, "tech", "test-run", outDirFor, produce, nil)

	if sum.Breaker {
		t.Fatal("breaker must not trip on transient failures after a success")
	}
	if sum.Produced != 3 || sum.Failed != 2 {
		t.Fatalf("summary = %+v, want produced=3 failed=2", sum)
	}
	if sum.Retries != 2*(maxPartAttempts-1) {
		t.Errorf("retries = %d, want %d", sum.Retries, 2*(maxPartAttempts-1))
	}
}

// TestRunRangePoolExhaustedPartsTripBreaker: in phase 2, 5 consecutive EXHAUSTED parts still trip.
func TestRunRangePoolExhaustedPartsTripBreaker(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	parts := []fixturePart{{index: 0, present: true}}
	class := []uint32{0}
	for i := uint32(1); i <= 5; i++ {
		parts = append(parts, fixturePart{index: i, present: false})
		class = append(class, i)
	}
	getter := writeRangeFixture(t, base, parts)
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), class, 1, "tech", "test-run", outDirFor, produce, nil)

	if !sum.Breaker {
		t.Fatal("5 consecutive exhausted parts must trip the breaker")
	}
	if sum.Produced != 1 || sum.Failed != 5 {
		t.Fatalf("summary = %+v, want produced=1 failed=5", sum)
	}
}

// TestRunRangePoolExternalCancelDrains proves an external cancel (operator interrupt) drains the
// pool promptly — the test would hang past its deadline on a dispatcher/worker deadlock.
func TestRunRangePoolExternalCancelDrains(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{
		{index: 0, present: true}, {index: 1, present: true}, {index: 2, present: true},
	})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		if part == 0 {
			cancel()
			return partResult{}, ctx.Err()
		}
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(ctx, []uint32{0, 1, 2}, 1, "tech", "test-run", outDirFor, produce, nil)

	if sum.Produced != 0 {
		t.Errorf("produced = %d, want 0 after immediate cancel", sum.Produced)
	}
	for _, part := range []uint32{0, 1, 2} {
		if markers.Exists(markers.ProducedPath(outDirFor(part))) {
			t.Errorf("part %d must not be marked produced after cancel", part)
		}
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./cmd/cc-enrich-worker/ -run TestRunRangePool -v`
Expected: FAIL to build with `sum.Retries undefined (type rangeSummary has no field or method Retries)`

- [ ] **Step 4: Rewrite runRangePool**

In `cmd/cc-enrich-worker/runrange.go`:

Edit 1 — update the breaker-constant comment. Replace:

```go
// consecutiveFailureLimit is the circuit breaker: after this many CONSECUTIVE part failures the pool
// cancels and abandons the rest of the range, on the theory that the source is systemically broken
// (throttling, auth, dead catalog) rather than a few parts decaying independently.
const consecutiveFailureLimit = 5
```

with:

```go
// consecutiveFailureLimit is the circuit breaker threshold. Until the first successful produce it
// counts CONSECUTIVE attempt failures of any kind — a systemically broken source (dead catalog, bad
// output root) must fail fast, not spin through retry backoffs. After any success it counts only
// consecutive EXHAUSTED parts (each already retried maxPartAttempts times), so bursty S3 coldness
// cannot kill an otherwise working run.
const consecutiveFailureLimit = 5
```

Edit 2 — add `Retries` to the summary. Replace:

```go
type rangeSummary struct {
	Produced    int
	Skipped     int
	Failed      int
	FailedParts []uint32 // in failure order
	Breaker     bool     // true if the consecutive-failure breaker tripped
}
```

with:

```go
type rangeSummary struct {
	Produced    int
	Skipped     int
	Failed      int      // parts that exhausted maxPartAttempts (or fed a phase-1 breaker trip)
	FailedParts []uint32 // in failure order
	Retries     int      // failed attempts that were requeued for another try
	Breaker     bool     // true if the consecutive-failure breaker tripped
}
```

Edit 3 — replace the whole `runRangePool` function (its doc comment through its closing brace, currently the block starting `// runRangePool consumes class over min(warcParallel, len(class)) worker goroutines. Per part it` and ending with the `wg.Wait()` / `return sum` lines) with:

```go
// runRangePool consumes class over min(warcParallel, len(class)) worker goroutines, coordinated by
// a dispatcher loop (this function) that owns all scheduling state. Per attempt a worker honors the
// .produced marker (skip), preserves a complete-but-unmarked output (.loaded or a complete embed
// file) as skipped, removes a stale output dir left by a crashed or failed produce, runs the
// producer, and on success writes the .produced marker with the row counts.
//
// Failure handling (spec 2026-07-14-part-retry-backoff-design.md): a failed part is requeued with
// exponential backoff (partBackoff) and counts as Failed only after maxPartAttempts attempts. The
// breaker is two-phase (see consecutiveFailureLimit); a trip cancels the context and abandons
// everything still pending. It returns the tally — no printing, no os.Exit.
func runRangePool(
	ctx context.Context,
	class []uint32,
	warcParallel int,
	cmd, runID string,
	outDirFor func(uint32) string,
	produce partProducer,
	prog *poolProgress,
) rangeSummary {
	workers := warcParallel
	if workers > len(class) {
		workers = len(class)
	}
	if workers < 1 {
		workers = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	parts := make(chan pendingPart)
	// Buffered to the worker count so a worker never blocks reporting: at most workers attempts are
	// in flight, hence at most workers unread outcomes.
	results := make(chan partOutcome, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pd := range parts {
				results <- runPartAttempt(ctx, pd, cmd, runID, outDirFor, produce, prog)
			}
		}()
	}

	pending := &partQueue{}
	for _, part := range class {
		pending.add(pendingPart{part: part})
	}

	d := &poolDispatcher{pending: pending, prog: prog, failedSeen: map[uint32]bool{}}
	outstanding := 0
	tripped := false
	done := ctx.Done()
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	for {
		halted := tripped || ctx.Err() != nil
		if outstanding == 0 && (halted || pending.Len() == 0) {
			break
		}
		// Arm exactly one of: a dispatch of the eligible head, or a timer for when it becomes
		// eligible. Halted runs arm neither and only drain outstanding attempts.
		var dispatch chan pendingPart
		var head pendingPart
		var timerC <-chan time.Time
		timer.Stop()
		if !halted && pending.Len() > 0 {
			head = pending.peek()
			if wait := time.Until(head.eligibleAt); wait <= 0 {
				dispatch = parts
			} else {
				timer.Reset(wait)
				timerC = timer.C
			}
		}
		select {
		case dispatch <- head:
			pending.next()
			if head.attempts > 0 {
				prog.endRetryWait()
			}
			outstanding++
		case out := <-results:
			outstanding--
			if d.handle(out) {
				tripped = true
				cancel()
			}
		case <-timerC:
			// the head part's backoff expired; loop re-evaluates eligibility
		case <-done:
			done = nil // stop dispatching; keep looping to drain outstanding attempts
		}
	}
	close(parts)
	wg.Wait()

	if d.sum.Breaker && !d.producedAny {
		// Phase-1 trip: nothing succeeded and nothing is exhausted yet (a part cannot reach
		// maxPartAttempts failures before 5 consecutive failures trip). Report every part that fed
		// the breaker as failed — the run is being abandoned as systemically broken.
		d.sum.Failed = len(d.firstFailed)
		d.sum.FailedParts = d.firstFailed
	}
	return d.sum
}

// partOutcome is one worker's report of one dispatched attempt.
type partOutcome struct {
	pending pendingPart
	skipped bool
	aborted bool // the context was already cancelled; the attempt never ran and is not tallied
	err     error
}

// poolDispatcher is the single-goroutine scheduling state of runRangePool. Only the dispatcher
// loop touches it; workers communicate via partOutcome messages.
type poolDispatcher struct {
	sum         rangeSummary
	firstFailed []uint32 // distinct failed parts in first-failure order (phase-1 breaker report)
	failedSeen  map[uint32]bool
	producedAny bool
	consecutive int // phase 1: consecutive attempt failures; phase 2: consecutive exhausted parts
	pending     *partQueue
	prog        *poolProgress
}

// handle folds one outcome into the tally and requeues transient failures. It reports whether the
// breaker tripped on this outcome.
func (d *poolDispatcher) handle(out partOutcome) (trip bool) {
	if out.aborted {
		return false
	}
	if out.skipped {
		d.sum.Skipped++
		return false
	}
	if out.err == nil {
		d.sum.Produced++
		d.producedAny = true
		d.consecutive = 0
		return false
	}

	attempts := out.pending.attempts + 1
	if !d.failedSeen[out.pending.part] {
		d.failedSeen[out.pending.part] = true
		d.firstFailed = append(d.firstFailed, out.pending.part)
	}
	if !d.producedAny {
		d.consecutive++
		if d.consecutive >= consecutiveFailureLimit {
			d.sum.Breaker = true
			return true
		}
	}
	if attempts >= maxPartAttempts {
		d.sum.Failed++
		d.sum.FailedParts = append(d.sum.FailedParts, out.pending.part)
		d.prog.addFailed()
		log.Printf("range: part %d FAILED after %d attempts: %v", out.pending.part, attempts, out.err)
		if d.producedAny {
			d.consecutive++
			if d.consecutive >= consecutiveFailureLimit {
				d.sum.Breaker = true
				return true
			}
		}
		return false
	}
	backoff := partBackoff(attempts)
	d.sum.Retries++
	d.prog.addRetryWait()
	log.Printf("range: part %d failed attempt %d/%d, retrying in %s: %v",
		out.pending.part, attempts, maxPartAttempts, backoff, out.err)
	d.pending.add(pendingPart{part: out.pending.part, attempts: attempts, eligibleAt: time.Now().Add(backoff)})
	return false
}

// runPartAttempt executes one dispatched attempt of one part: marker skip, preserve/wipe of stale
// output, produce, marker write. All tallying is the dispatcher's; this only reports what happened.
func runPartAttempt(
	ctx context.Context,
	pd pendingPart,
	cmd, runID string,
	outDirFor func(uint32) string,
	produce partProducer,
	prog *poolProgress,
) partOutcome {
	// Stop cleanly once the run is cancelled: the attempt is dropped without being run or marked.
	if ctx.Err() != nil {
		return partOutcome{pending: pd, aborted: true}
	}
	outDir := outDirFor(pd.part)

	if markers.Exists(markers.ProducedPath(outDir)) {
		prog.addSkipped()
		return partOutcome{pending: pd, skipped: true}
	}
	// A non-empty output dir with no .produced marker is USUALLY debris from an attempt that
	// crashed or failed mid-write — remove it so producePart starts clean. But a complete-but-
	// unmarked output (.loaded from the retired cc-crawl lifecycle, or a complete embed file) is
	// authoritative: preserve it and skip the part rather than destroying loaded data.
	if info, statErr := os.Stat(outDir); statErr == nil && info.IsDir() {
		if preserveStaleDir(cmd, outDir) {
			log.Printf("range: preserving complete-but-unmarked output dir (skip) part=%d %s", pd.part, outDir)
			prog.addSkipped()
			return partOutcome{pending: pd, skipped: true}
		}
		log.Printf("range: removing stale output dir (crashed produce?) part=%d %s", pd.part, outDir)
		if rmErr := os.RemoveAll(outDir); rmErr != nil {
			log.Printf("range: remove stale output dir part=%d: %v", pd.part, rmErr)
		}
	}

	partStart := time.Now()
	prog.startPart()
	res, perr := produce(ctx, pd.part, outDir)
	prog.endPart()
	if perr == nil {
		perr = markers.WriteProduced(outDir, markers.Produced{
			Part:        pd.part,
			Cmd:         cmd,
			Rows:        res.Rows,
			SourceRunID: runID,
			DurationS:   time.Since(partStart).Seconds(),
			FinishedAt:  time.Now().UTC(),
		})
		if perr != nil {
			perr = fmt.Errorf("write produced marker: %w", perr)
		}
	}
	if perr == nil {
		prog.addProduced()
		log.Printf("range: part %d produced -> %s", pd.part, outDir)
	}
	return partOutcome{pending: pd, err: perr}
}
```

Note: the old function's `var mu sync.Mutex`, `consecutive`, and the feeder goroutine are all gone; `sync` stays imported (WaitGroup).

- [ ] **Step 5: Run the package tests**

Run: `go test ./cmd/cc-enrich-worker/ -v`
Expected: PASS — including the untouched `TestRunRangePoolAllSucceed`, `TestRunRangePoolParallelDeterministic`, `TestRunRangePoolBreaker`, `TestRunRangePoolPreservesLoadedDir`.

Run: `go test -race ./cmd/cc-enrich-worker/ -run TestRunRangePool`
Expected: PASS with no race reports.

- [ ] **Step 6: Commit**

```bash
git add cmd/cc-enrich-worker/runrange.go cmd/cc-enrich-worker/runrange_test.go
git commit -m "feat(corpscout/cc-enrich-worker): requeue failed parts with backoff; two-phase breaker"
```

---

### Task 4: Summary line, README, and final verification

**Files:**
- Modify: `cmd/cc-enrich-worker/runrange.go` (runRange summary print)
- Modify: `README.md` (range-runner failure paragraph, lines ~78-82)

**Interfaces:**
- Consumes: `rangeSummary.Retries` (Task 3).
- Produces: nothing downstream.

- [ ] **Step 1: Print retries in the final summary**

In `runRange` in `cmd/cc-enrich-worker/runrange.go`, replace:

```go
	fmt.Printf("produced=%d skipped=%d failed=%d%s elapsed=%s\n",
		sum.Produced, sum.Skipped, sum.Failed, partsMsg, elapsed)
```

with:

```go
	fmt.Printf("produced=%d skipped=%d failed=%d retries=%d%s elapsed=%s\n",
		sum.Produced, sum.Skipped, sum.Failed, sum.Retries, partsMsg, elapsed)
```

- [ ] **Step 2: Update the README failure paragraph**

In `README.md` (the component README at `cc-enrich-worker/README.md` — the working directory's `README.md`), replace:

```markdown
- Within a range, `.produced` markers make the run resumable: a part with an existing marker is
  skipped; an output directory with no marker (a crashed produce) is wiped and reproduced. A circuit
  breaker aborts the run after 5 CONSECUTIVE part failures (protects an unattended box from e.g. expired
  credentials silently burning hours); failed parts are logged, left unmarked, and retried on the next
  invocation. The run exits non-zero if any part failed.
```

with:

```markdown
- Within a range, `.produced` markers make the run resumable: a part with an existing marker is
  skipped; an output directory with no marker (a crashed produce) is wiped and reproduced.
- A failed part is requeued in-run with exponential backoff (1→30 min, 8 attempts, ~1.5 h span —
  sized to Common Crawl's bursty S3 coldness) and counts as failed only when it exhausts all
  attempts. The stats line shows parts waiting out a backoff as `retrywait=N`.
- A circuit breaker aborts the run after 5 consecutive attempt failures *before the first success*
  (protects an unattended box from e.g. a dead catalog silently burning hours), or 5 consecutive
  *exhausted* parts after it. Failed parts are logged, left unmarked, and retried on the next
  invocation. The run exits non-zero if any part failed.
```

- [ ] **Step 3: Full verification**

Run, from `cc-enrich-worker/`:

```bash
gofmt -l . && go vet ./... && go test ./... && go test -race ./cmd/cc-enrich-worker/
```

Expected: `gofmt -l` prints nothing; vet clean; all tests PASS twice (plain and race).

- [ ] **Step 4: Commit**

```bash
git add cmd/cc-enrich-worker/runrange.go README.md
git commit -m "feat(corpscout/cc-enrich-worker): report retries in range summary; document retry policy"
```
