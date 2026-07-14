# Part retry with backoff in the range-runner pool

**Date:** 2026-07-14
**Status:** approved
**Scope:** `cmd/cc-enrich-worker/runrange.go`, `rangestats.go`, and their tests. No changes to
`producePart`, markers, catalog, loader, or the worker package.

## Problem

Common Crawl S3 availability is bursty: individual WARC objects (whole parts) become effectively
unreachable for minutes-to-hours, then recover. The range runner treats every part failure as final
for the run and counts it toward a 5-consecutive-failure circuit breaker. Because shard coldness is
clustered across adjacent parts, a cold window routinely kills an otherwise healthy multi-hour run,
requiring a human restart. Observed recovery horizon: ~2 hours.

A full fetch/process decoupling with disk staging was designed and consciously deferred (see the
conversation record of 2026-07-14); this change is the small patch that captures most of the
operational benefit: failed parts are retried within the run instead of failing it.

## Design

### Retry policy

- A part whose produce attempt fails is **requeued**, not failed. Each part carries an attempt
  count; its next attempt becomes eligible after an exponential backoff:
  `1, 2, 4, 8, 16, 30, 30 minutes` (cap 30 min), for a maximum of **8 attempts** (~2 h span,
  matching the observed recovery horizon).
- A part counts as **Failed** (`rangeSummary.Failed`, `FailedParts`, non-zero exit) only when it
  exhausts all 8 attempts.
- Backoff schedule and attempt cap are package constants (`partBackoffBase`, capped schedule,
  `maxPartAttempts`), not flags. The backoff base is an injectable package variable so unit tests
  run in microseconds.

### Breaker semantics (two-phase)

- **Phase 1 — until the first part produces successfully:** unchanged from today. 5 consecutive
  attempt failures of any kind (`consecutiveFailureLimit`; first attempts or retries alike) trip
  the breaker and cancel the run. This keeps
  genuine systemic errors (corrupt catalog, bad output root) failing fast instead of spinning
  through backoffs. Credential errors already fail before the pool starts (`NewS3Getter` validates
  upfront). Skipped parts (existing `.produced` markers) do NOT count as a success for phase
  transition — only a produced part does.
- **Phase 2 — after any successful produce:** only **exhausted** parts count toward the consecutive
  counter. Transient failures requeue silently. Five consecutive exhausted parts (each having
  survived ~2 h of retries) still trip the breaker.

### Queue mechanics

The current feeder goroutine + closed `parts` channel cannot accept requeues. Replace it with a
**dispatcher goroutine** that owns all scheduling state (no shared mutation):

- State: a pending min-heap ordered by eligible-at time (initial parts eligible immediately, in
  order), plus an outstanding-attempts counter.
- Loop: `select` over worker result messages (success / failure-with-attempt), a timer armed for
  the next eligible part, and `ctx.Done()`. Eligible parts are sent to workers over the existing
  `parts` channel; the channel closes when the heap is empty and nothing is outstanding.
- Workers are unchanged except that they report the produce outcome to the dispatcher instead of
  mutating the shared tally under a mutex; the dispatcher owns `rangeSummary`.
- Marker skip (`.produced` exists) and stale-output-dir cleanup behave exactly as today; the
  existing stale-dir removal already cleans debris between attempts of the same part.

### What failure means across runs (unchanged)

Failure is never persisted. An exhausted part ends the run with no `.produced` marker, so the next
run over a range including it produces it from scratch. The summary's failed-part list and exit
code are informational only; `status` shows such parts as pending.

### Observability

- Per-event log: `range: part 137 failed attempt 2/8, retrying in 4m: <err>` (exhaustion logs
  `FAILED` as today).
- The cumulative stats line gains a waiting-for-retry gauge:
  `parts run=2 done=80/200 skip=10 fail=0 retrywait=3` (`poolProgress` gains a retry-wait counter).
- `rangeSummary` gains a total-retries counter, printed in the final summary.

## Testing

Unit tests against the injected fixture producer (existing style), with the backoff base shrunk to
microseconds:

- Part fails twice then succeeds → Produced, not Failed; attempt count and backoff ordering
  observed; no breaker trip.
- Part fails 8 times → Failed exactly once, listed in FailedParts, exit-code path unchanged.
- Phase 1 breaker: 5 consecutive first-attempt failures with zero successes → breaker trips,
  remaining parts never run.
- Phase 2 breaker: after one success, repeated transient failures never trip; 5 consecutive
  exhausted parts do.
- Requeued part's stale output dir is cleaned before the retry attempt.
- Context cancellation mid-backoff drains cleanly (no goroutine leak, no send on closed channel).

## Non-goals

- No disk staging, no segment-level resume, no fetch/process decoupling (deferred design).
- No new flags; constants only.
- No change to `maxFetchErrorRate`, markers, loader, or `status`.
