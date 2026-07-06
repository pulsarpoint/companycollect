# cc-dns-worker Per-Server Circuit Breaker — Design

**Date:** 2026-07-06
**Status:** Draft for review
**Extends:** `2026-07-05-commoncrawl-dns-scanner-design.md` (§3.2 scheduler, §6 error handling)

## 1. Problem

A domain whose authoritative NS *resolve* (discovery succeeds) but don't answer our direct UDP/53
queries becomes a worker hog: `Resolve` runs ~30 queries sequentially, each retrying across the
domain's (dead) NS IPs at the full `--query-timeout`, so one fully-unreachable domain burns
~`30 × len(NSIPs)×2 × timeout` (≈ 600s at defaults) on a single worker goroutine. At full-corpus
scale two things compound it: isolated dead domains each waste that budget, and — worse — many
domains **share** the same dead/firewalled small-provider NS IP, so without shared "this server is
dead" knowledge every one of them independently re-discovers the deadness one timeout at a time.

A per-server-IP circuit breaker turns deadness into cached, shared state: after N consecutive
transport failures to a server IP, its circuit opens and subsequent queries to that IP **fast-fail**
for a cooldown instead of timing out — for this domain's remaining queries and every other domain on
that IP.

## 2. Where it lives

Inside `internal/scheduler`. `Scheduler.Do(ctx, serverIP, fn)` is already the single per-server-IP
choke point every query passes through, and it already owns per-server state (`server{lim, slot}`).
The breaker is more per-server state + a check at the top of `Do` and an outcome-record after `fn`.
**Callers do not change:** `queryAuth` (query.go) and the discovery `query()` (discover.go) already
rotate to the next server on any error; a breaker fast-fail returns `ErrCircuitOpen`, which flows
through that same `err != nil → try next` path. Because both the discovery and authoritative
schedulers are the same `Scheduler` type, the breaker covers both tiers; on the discovery tier
(a few healthy recursive resolvers) it simply never trips.

## 3. What counts as a failure

The breaker keys on **`fn` returning an error**, which for this system is a **transport failure**
(UDP/TCP timeout or network error). It is NOT tripped by DNS-level answers: in `exchange.go`, a
`SERVFAIL`/`NXDOMAIN`/`NOERROR` response is a *successful* exchange (`fn` returns nil, the rcode is
on the response). This is deliberate — a server that answers `SERVFAIL` fast is alive-but-
misconfigured and must not be broken; a server that times out repeatedly is dead and is the one
wasting the timeout budget. Context-cancellation and slot/token-wait errors do not run `fn`, so they
never touch the breaker counter.

## 4. State machine (minimal)

Per server, add (guarded by a new per-server `sync.Mutex`, independent of the scheduler map mutex):
- `fails int` — **consecutive** transport failures since the last success.
- `openUntil time.Time` — zero when closed; a future time when open.

**`allow(now) bool`** (checked at the top of `Do`, before acquiring the slot/token — an open server
costs nothing):
- `openUntil` zero → **closed** → allow.
- `now` before `openUntil` → **open** → deny (`Do` returns `ErrCircuitOpen`).
- `now` at/after a non-zero `openUntil` → **half-open** (cooldown elapsed) → allow.

**`record(now, ok) `** (after `fn` runs):
- success → `fails = 0`, `openUntil = zero` (close).
- failure → `fails++`; if `fails >= threshold` → `openUntil = now + cooldown` (open / re-open).

**Half-open needs no separate "single-probe" flag:** the existing per-server in-flight cap
(`MaxInFlight`, default 3) already limits how many goroutines can be running `fn` against one server
at once, so at most ~3 probes hit a just-elapsed circuit before the first failure re-opens it. That
bound is free; we rely on it rather than adding a probe-claim flag (which would also introduce a
ctx-cancel cleanup hazard). A half-open failure keeps `fails >= threshold`, so a single failed probe
re-opens; a half-open success resets to closed.

## 5. `Do` flow (breaker enabled)
```
sv := forServer(serverIP)
if cfg.BreakerThreshold > 0 && !sv.allow(now()) {
    return ErrCircuitOpen            // fast-fail: no slot, no token, no fn
}
acquire slot (ctx-aware) ; defer release
lim.Wait(ctx)                        // ctx-aware
err := fn()
if cfg.BreakerThreshold > 0 {
    sv.record(now(), err == nil)
}
return err
```
When `BreakerThreshold <= 0` the breaker is **disabled** and `Do` behaves exactly as today — so the
existing scheduler tests (which never set breaker config) are unaffected.

## 6. Config, defaults, wiring
- `scheduler.Config` gains `BreakerThreshold int` (consecutive transport failures to open; **0 =
  disabled**) and `BreakerCooldown time.Duration`.
- `scheduler.New` leaves them as-passed (no default injected → default-off at the scheduler layer,
  preserving existing tests).
- `scan.go` enables it **on by default** with conservative values and exposes flags:
  `--breaker-threshold` (default `5`) and `--breaker-cooldown` (default `30s`), applied to BOTH the
  discovery and authoritative schedulers. `--breaker-threshold 0` disables it.
- `ErrCircuitOpen` — an exported sentinel (`errors.New("scheduler: circuit open")`); callers already
  treat it as a rotate-worthy error, no code change.

## 7. Testability
The token-bucket `rate.Limiter` uses real time internally, but breaker *timing* (open/cooldown) is
driven by an injectable clock: `Scheduler` gains an unexported `now func() time.Time` defaulting to
`time.Now`. Breaker unit tests (in-package) set `s.now` to a controllable fake and use a high
`PerServerQPS` so the limiter never interferes, making open→cooldown→half-open→close fully
deterministic with zero sleeps.

## 8. Testing
- **Trips after threshold:** with `BreakerThreshold=3`, a fake clock, and an `fn` that always errors,
  assert the 4th `Do` to the same IP returns `ErrCircuitOpen` **without** running `fn` (e.g. `fn`
  increments a counter; counter stays at 3).
- **Cooldown / half-open / close:** advance the fake clock past `openUntil` → next `Do` runs `fn`
  again (half-open); make that `fn` succeed → circuit closes (`fails` reset, subsequent `Do`s all run
  `fn`); make it fail → re-opens.
- **Consecutive-not-cumulative:** interleave a success before the threshold is reached → counter
  resets, circuit does not open.
- **Independence:** a tripped IP does not open a different IP's circuit.
- **Disabled by default:** with `BreakerThreshold=0`, `fn` runs every time regardless of failures
  (existing behavior); the existing `TestPerServerPacing`/`TestServersAreIndependent`/
  `TestMaxInFlightCap` remain unchanged and green.
- No new integration/e2e is required — the breaker is a scheduler-internal optimization proven by
  deterministic unit tests; a scan run with `--breaker-threshold 5` is a manual sanity check.

## 9. Key decisions
1. **Lives in `scheduler.Do`; callers unchanged** — `ErrCircuitOpen` rides the existing error-rotation path.
2. **Failure = transport error only** (`fn` errored); DNS rcodes (incl. SERVFAIL) do not trip it.
3. **Minimal state** (`fails` consecutive + `openUntil`); **no single-probe flag** — the existing
   `MaxInFlight` cap bounds the half-open burst for free.
4. **On by default** in `scan` (`--breaker-threshold 5`, `--breaker-cooldown 30s`); **disabled at the
   scheduler layer when threshold ≤ 0**, so existing tests are unaffected.
5. **Injectable clock** for deterministic, sleep-free breaker tests.

## 10. Non-goals
- Distinguishing failure *types* beyond transport-vs-response (no per-rcode policy).
- Exponential-backoff cooldown (fixed cooldown in v1; easy to add later).
- Persisting breaker state across runs (in-memory, like the limiters — a new run starts closed).
- Metrics/observability export (a `log` line on first open per server is optional, not required).
