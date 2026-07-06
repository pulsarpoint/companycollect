# cc-dns-worker SOCKS Proxy Pool — Design

**Date:** 2026-07-06
**Status:** Draft for review
**Extends:** `2026-07-05-commoncrawl-dns-scanner-design.md` (§3 transport/scheduler) and the circuit-breaker spec.

## 1. Problem

The scanner is already polite per target (per-NS-IP rate limit + circuit breaker), but a **single
source IP** querying authoritative servers continuously over a full-corpus run still gets throttled
or blocked: many servers rate-limit or blocklist per source IP, and even a gentle 10 r/s sustained
for hours trips some of them (and can get the scan box's IP flagged upstream as a scanner). The fix
is to spread the SOURCE footprint across **many SOCKS proxies** (distinct exit IPs), so each source
does a small fraction of the traffic to any one target while the per-target load stays unchanged.

This is a source-distribution feature for a **polite, read-only** DNS survey — it does NOT increase
load on any target. Per-target politeness (rate limit + breaker) is preserved exactly.

## 2. Decisions (settled)
- **Transport: DNS-over-TCP via SOCKS5 CONNECT.** Each proxied query opens a TCP tunnel through a
  proxy's SOCKS5 CONNECT to `<target-NS>:53` and does DNS-over-TCP (`dns.Client.ExchangeWithConn`).
  Works with any SOCKS5 proxy (CONNECT is universal) using `golang.org/x/net/proxy` (already in the
  dep tree). When NO proxies are configured, behavior is unchanged (direct UDP-first, TCP fallback).
- **Rate model: per-target rate + breaker stay GLOBAL and unchanged; the SOURCE rotates per query.**
  A target still sees ≤ its configured rate total; that rate is spread across N proxies, so each
  `(source → target)` pair does ~rate/N. The scheduler is **not** re-keyed — it stays per-target.
- **Applies to both tiers** (discovery recursive resolvers and authoritative NS) when a pool is
  configured; the source-block problem bites both.

## 3. Components

### 3.1 `internal/socks` — the proxy pool (new package)
Owns the proxy fleet, selection, and health. No DNS knowledge.
- `Proxy` — one entry: `Addr` (`host:port`), optional `User`/`Pass`, a lazily-built
  `proxy.Dialer` (from `x/net/proxy.SOCKS5`), and per-proxy breaker state (`fails`, `benchedUntil`).
- `Pool` — the fleet + round-robin cursor + injectable clock (for tests), guarded by a mutex.
  - `Load(entries []string) (*Pool, error)` — parse `host:port`, `user:pass@host:port`, or
    `socks5://user:pass@host:port` (one per line / comma item). Empty list → nil pool (direct mode).
  - `Next() *Proxy` — the next **healthy** proxy (round-robin, skipping benched ones); `nil` if all
    are benched.
  - `Dial(ctx, targetAddr string) (net.Conn, *Proxy, error)` — pick a proxy and SOCKS5-CONNECT-dial
    a TCP conn to `targetAddr`; on a dial failure mark that proxy failed and try the next healthy
    proxy, up to `MaxAttempts`. Returns `ErrNoProxy` when the pool is exhausted (all benched).
  - `markFail(p)` / `markOK(p)` — per-proxy consecutive-failure breaker: `FailThreshold` consecutive
    dial failures bench a proxy for `Cooldown`; a success resets it. (Same shape as the target
    breaker, but keyed on proxies and living in the pool.)
- `ErrNoProxy = errors.New("socks: no healthy proxy")`.

### 3.2 `internal/resolve/exchange.go` — proxy-aware transport
`NewExchanger` gains an optional `*socks.Pool`. When set, `Exchange` routes every query through the
pool as DNS-over-TCP; when nil, the current direct path is untouched. Both the discovery and
authoritative exchangers get the same pool.

## 4. Failure attribution (the subtle part)

A dead **proxy** must never trip a **target's** circuit breaker (and vice-versa). Structure `Exchange`
so proxy health and target health are recorded on the right axis:

```
Exchange(ctx, m, targetIP):
    sched.Do(ctx, targetIP, fn):                 // target rate + target breaker gate (unchanged)
        conn, p, err := pool.Dial(ctx, targetAddr)   // rotates proxies on dial failure; proxy health
        if err == ErrNoProxy:  return err            // pool exhausted (see §4.1)
        if err != nil:         return err            // K proxies all failed to reach target -> target-ish
        resp, xerr := dnsOverTCP(conn, m); conn.Close()
        return resp, xerr                            // post-connect outcome -> TARGET breaker
```

- **Proxy-dial failures** (can't reach a proxy, SOCKS handshake/auth fails) are handled INSIDE
  `pool.Dial`: they call `markFail(p)` and rotate to the next proxy — they never surface as the
  `fn` error, so the target breaker never sees them.
- Only the **post-connect exchange** result (`dnsOverTCP`) is returned as `fn`'s error, so the target
  breaker records a genuine target outcome.
- If `MaxAttempts` different proxies all fail to establish a tunnel to one target, that's most likely
  the target being down (K distinct sources couldn't reach it) → returned as `fn` error → target
  breaker counts it. Acceptable heuristic; avoids parsing SOCKS reply codes.

### 4.1 Pool exhaustion
`pool.Dial` returning `ErrNoProxy` (every proxy benched) must NOT poison every target's breaker and
must NOT silently mark every domain `error`. Two safeguards, keeping `scheduler` unaware of `socks`:
- `scheduler` gains an exported sentinel `ErrUnavailable` and `Do` skips breaker-recording when
  `errors.Is(fnErr, ErrUnavailable)` (the same "infrastructure, not the target" idea already applied
  to the ctx-cancel/limiter paths, generalized to one signal). The **resolve** Exchanger — which
  knows both packages — is what maps a `socks.ErrNoProxy` from `pool.Dial` into
  `fmt.Errorf("...: %w", scheduler.ErrUnavailable)` inside `fn`, so `scheduler` never imports `socks`.
  (A dial failure that is NOT `ErrNoProxy` — i.e. `MaxAttempts` distinct proxies each failed to reach
  the target — is returned as a plain error and DOES count against the target breaker, per §4.)
- `runScan` tracks consecutive `ErrNoProxy` outcomes; if the pool stays fully benched for a whole
  dispatch batch (systemic proxy outage / bad list), it **aborts the run with an error** rather than
  writing a batch of bogus `error` domains. (Transient benching self-heals via the per-proxy
  cooldown, so this only fires on a real outage.)

## 5. Config / flags
- `--socks <comma-list>` and/or `--socks-file <path>` (one proxy per line; `#` comments ignored).
  Neither set → direct mode (no proxies), current behavior.
- `--socks-max-attempts` (default `3`) — distinct proxies to try per query before giving up.
- `--socks-fail-threshold` (default `5`) — consecutive dial failures that bench a proxy.
- `--socks-cooldown` (default `60s`) — how long a benched proxy stays out.
- Proxy auth via `user:pass@host:port` entries. The pool applies to both schedulers' exchangers.

## 6. What does NOT change
- The per-target scheduler (rate limit + circuit breaker) — same keys, same politeness.
- The resolver's two-tier flow, `records.Plan`, store, load, resume, streaming dispatch.
- Direct (no-proxy) mode is byte-for-byte the current transport.

## 7. Testing
- **Pool unit tests:** parse the three entry formats (+ auth, + bad lines); `Next()` round-robins and
  skips benched proxies; `markFail` benches after threshold and `markOK`/cooldown restores; `Dial`
  rotates past a failing proxy and returns `ErrNoProxy` when all are benched. Deterministic via the
  injected clock.
- **Proxied exchange integration (in-process, no external network):** stand up a minimal in-process
  **SOCKS5 CONNECT** server and an in-process **TCP DNS** server (miekg/dns `Net:"tcp"`), point a
  `Pool` at the SOCKS server, and assert a query resolved through the proxy returns the crafted
  answer. A second test: a dead proxy in the pool is benched and the query still succeeds via the
  healthy one (rotation + attribution).
- **Failure attribution:** a proxy that refuses the tunnel does NOT increment the target's breaker
  (assert the target breaker stays closed after N proxy-only failures); a post-connect timeout DOES.
- **Direct-mode unchanged:** with no pool, existing resolve tests stay green; `-race` clean.

## 8. Non-goals (deferred)
- **UDP ASSOCIATE** (native-UDP through SOCKS) — needs UDP-capable proxies + a third-party lib.
- **Per-proxy rate limiting** — round-robin over many proxies keeps per-proxy load low; add a
  per-proxy token bucket later only if a proxy melts.
- **TCP connection pooling/keep-alive per (proxy,target)** — v1 is one tunnel per query (fine at
  ~10 r/s per target); pool later if connection churn matters.
- **Proxy auto-discovery / provider APIs** — the operator supplies the list.
- **DoT/DoH or TLS-to-proxy** — plain DNS over the SOCKS TCP tunnel.

## 9. Key decisions
1. **DNS-over-TCP via SOCKS5 CONNECT** (`x/net/proxy`), any SOCKS5 proxy; direct mode unchanged when no pool.
2. **Per-target rate + breaker unchanged; source rotates per query** — targets stay as polite as now.
3. **Failure attribution:** proxy failures live in the pool's per-proxy breaker; only post-connect
   outcomes reach the target breaker; `ErrNoProxy` is excluded from the target breaker and aborts the
   run if the pool stays fully benched for a batch.
4. **New `internal/socks` package** (pool + health + dial); `NewExchanger` takes an optional pool,
   applied to both tiers.
5. **In-process SOCKS5 + TCP-DNS test servers** for deterministic, network-free integration tests.
