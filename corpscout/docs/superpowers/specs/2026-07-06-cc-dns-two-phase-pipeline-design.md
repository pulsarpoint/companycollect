# cc-dns-worker Two-Phase Recurring Pipeline — Design

**Date:** 2026-07-06
**Status:** Draft for review
**Extends / partially supersedes:** the SOCKS-proxy spec (`2026-07-06-cc-dns-socks-proxies-design.md`) and the circuit-breaker/streaming specs. Builds on the core scanner spec (`2026-07-05-commoncrawl-dns-scanner-design.md`).

## 1. Problem

The scanner runs as a **recurring pipeline**: refresh each domain's DNS records roughly **every 2–3
days** (daily if capacity allows) to track SPF/DKIM/DMARC drift, NS migrations, DNSSEC adoption, etc.
The fused `scan` re-discovers each domain's authoritative nameservers from scratch **every run** —
which (a) re-triggers the recursive-resolver bottleneck (millions of NS/A/DS queries into a few
resolvers), and (b) is wasteful: **NS delegation is relatively stable, while records change often.**

Split the work to match how the data already stores (NS in `..._dns_scan`, records in
`..._dns_records`): **Phase 1 discovers NS (rare, cache-friendly, recursive-resolver-bound); Phase 2
resolves records (frequent, direct-to-authoritative, resolver-free, naturally spread).** Phase 2
**reuses** Phase 1's NS, so the expensive discovery happens rarely and the frequent record scans
never touch a recursive resolver.

## 2. Decisions (settled / proposed)
- **Two phases**, two subcommands: `discover` (domains → NS) and `resolve` (NS → records). The
  existing `scan` (fused) is retained for one-shot/bootstrap use; the pipeline uses the two new ones.
- **Freshness-driven selection** (proposed default — CONFIRM): a compact per-domain **state table**
  drives which domains each run processes. Phase 1 picks domains with missing/stale/failed NS
  (`last_ns_at` older than `--ns-ttl`, default 14d); Phase 2 picks domains with valid NS whose
  records are stale (`last_records_at` older than `--record-ttl`, default 2d), **oldest-due first**,
  capped by `--limit` — so a run does a bounded slice and the pipeline self-levels toward the target
  cadence. (Alternative considered: hash-bucket rotation — simpler, smooth load, but doesn't adapt to
  new/failed domains or tunable cadence. Flagged for review.)
- **Phase 1 uses a local recursive resolver** (`--resolvers 127.0.0.1:53`) — the ops fix for the
  overload — with its own high concurrency (see §6 `--discovery-inflight`). This makes Phase 1 light
  and independent of public resolvers.
- **SOCKS proxies deferred**: with Phase 1 on a local resolver and Phase 2 spread across
  authoritative servers, per-source blocking is unlikely to bind; keep the SOCKS spec on the shelf.

## 3. The state table — the pipeline's control plane

`corpscout.commoncrawl_domain_dns_state` — **one row per domain (latest)**, distinct from the
historical `..._dns_scan`/`..._dns_records` tables (which keep every `scan_id` for drift analysis).
`ReplacingMergeTree(updated_at)` `ORDER BY root_domain` (read with FINAL). ~33.6M rows; small and
cheap to scan for selection.
```sql
CREATE TABLE corpscout.commoncrawl_domain_dns_state
(
    root_domain     String,
    etld            LowCardinality(String),
    -- NS (Phase 1 output, reused by Phase 2)
    nameservers     Array(String),
    ns_ips          Array(String),
    dnssec_signed   UInt8,
    ds_present      UInt8,
    ns_status       LowCardinality(String),  -- ok | error | stale (needs re-discovery)
    last_ns_at      DateTime64(3,'UTC'),     -- when NS was last discovered (zero = never)
    ns_fails        UInt16,                  -- consecutive Phase-1 failures (backoff)
    -- records (Phase 2 output)
    records_status  LowCardinality(String),  -- ok | error
    last_records_at DateTime64(3,'UTC'),     -- when records were last resolved (zero = never)
    rec_fails       UInt16,
    updated_at      DateTime64(3,'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY root_domain;
```
Both phases upsert this table (insert a new row; FINAL keeps the latest `updated_at`). The historical
tables are still written for record-level history.

## 4. Phase 1 — `discover` (domains → NS)

**Select (due for NS):** domains in `commoncrawl_domains` that are **new** (no state row), **stale**
(`now - last_ns_at > --ns-ttl`), or **failed and past backoff** (`ns_status='error'` and
`now - last_ns_at > --ns-retry`). Streamed worklist (reuse `StreamClickHouse`), oldest-first, capped
by `--limit`.

**Process:** recursive NS discovery via `--resolvers` (a **local recursive resolver** in production),
using the existing `Discoverer`. This is the cache-heavy, high-concurrency step — needs
`--discovery-inflight` high (§6).

**Output:** upsert the **state** table (`nameservers`, `ns_ips`, `etld`, `dnssec_signed`,
`ds_present`, `ns_status`, `last_ns_at=now`, `ns_fails`) AND append to the historical `..._dns_scan`
table (NS/delegation drift over time). Resumable via the SQLite stage.

## 5. Phase 2 — `resolve` (NS → records)

**Select (due for records):** state rows with `ns_status='ok'` AND (`last_records_at` zero OR
`now - last_records_at > --record-ttl`), **`ORDER BY last_records_at ASC`** (oldest-due first),
capped by `--limit`. The state row carries `ns_ips` inline, so Phase 2 reads `(domain, ns_ips)`
directly — **no discovery, no recursive resolver.**

**Process:** the existing Tier-2 record queries (`Resolver.Resolve`) directly against `ns_ips`,
rate-limited per NS-IP (breaker unchanged). Optionally grouped by NS server for locality (deferred).

**Output:** append records to the historical `..._dns_records` table (with the run's `scan_id`) and
upsert the state (`records_status`, `last_records_at=now`, `rec_fails`). Resumable via the SQLite
stage.

**NS-staleness feedback loop:** if all of a domain's `ns_ips` fail (timeout/refused/NXDOMAIN across
every NS), Phase 2 marks the state `ns_status='stale'` (or `last_ns_at=0`) so Phase 1 re-discovers it
next run. This closes the loop when a domain re-delegates between phases.

## 6. CLI / config

New subcommands `discover` and `resolve`, plus the retained `scan`. Shared internals (resolve, store,
records, scheduler, load). Flags:
- Both: `--db`, `--scan-id`, `--limit`, `--workers`, `--dispatch-batch`, `--commit-batch`,
  `--per-server-qps`, `--per-server-inflight`, `--query-timeout`, breaker flags — as today.
- `discover`: `--resolvers` (default public; **set `127.0.0.1:53` in prod**), `--discovery-qps`
  (default 50; bump high for local resolver), **`--discovery-inflight`** (NEW, default 500 — the
  current shared `--per-server-inflight` of 3 throttles a local resolver to 3 concurrent; discovery
  needs its own high cap), `--ns-ttl` (default 14d), `--ns-retry` (default 1d).
- `resolve`: `--record-ttl` (default 2d).
- `load` extended to write the state table alongside the historical tables (or each phase auto-loads
  its SQLite stage on completion — implementer's choice, kept consistent with the current scan/load
  split).

## 7. What's reused vs new
- **Reused unchanged:** `internal/resolve` (Discoverer + Resolver), `internal/records`,
  `internal/scheduler` (+ breaker), `internal/store` (SQLite stage + resume), streaming
  (`StreamClickHouse`, `PendingBatch`), `internal/load`.
- **New:** the `commoncrawl_domain_dns_state` migration; `discover`/`resolve` subcommands (thin
  orchestration wrapping the reused internals with the phase-specific selection query + output); the
  `--discovery-inflight` split; state upserts in `load`.
- **Small refactor:** split the shared `--per-server-inflight` so discovery and authoritative
  schedulers can differ (Phase 1 wants high in-flight to the local resolver; Phase 2 low per NS-IP).

## 8. Error handling
- Phase 1 NS failure → state `ns_status='error'`, `ns_fails++`, retried after `--ns-retry` backoff.
- Phase 2 all-NS failure → state `ns_status='stale'` (re-discovered by Phase 1 next run) + record error.
- Both phases resumable (SQLite stage + status), idempotent, streamed (bounded memory).
- A run is a bounded slice (`--limit`); the pipeline converges to the target cadence over successive
  runs because selection is oldest-due-first.

## 9. Testing
- **State selection (unit/integration vs real CH):** seed a small `commoncrawl_domains` + state table;
  assert `discover` selects new/stale/failed-past-backoff domains and `resolve` selects ok-NS +
  records-stale, oldest-first, capped by `--limit`.
- **Phase reuse (integration, real CH + DNS):** `discover --limit N` populates state NS; then
  `resolve --limit N` resolves records using that NS WITHOUT any recursive-resolver query (assert the
  discovery scheduler saw zero queries in the resolve run); records land in CH; state timestamps
  advance. Re-running `resolve` immediately selects 0 (all fresh); after `--record-ttl` they're due
  again.
- **Staleness loop:** a domain whose NS all fail in `resolve` gets `ns_status='stale'`; the next
  `discover` re-selects it.
- Reuse the existing in-process DNS servers; the state-selection queries run against the real CH
  (`companycollect:9002`) with a throwaway `scan_id`/temp domains, cleaned up.

## 10. Non-goals (deferred)
- **Tiered record cadence** (A/MX/TXT daily; DKIM 10-selector brute-force + DNSSEC weekly) — a strong
  follow-up (DKIM is ~340M mostly-NXDOMAIN queries/full-run and changes rarely), but a refinement on
  top; v1 resolves the full record set at `--record-ttl`.
- **Hash-bucket selection** (if chosen over freshness at review, this whole selection layer changes).
- **SOCKS proxies** (shelved; Phase 1 local resolver + Phase 2 spread makes it unlikely to bind).
- **dagster asset** to schedule `discover` (e.g. daily, ns-ttl-gated) + `resolve` (daily) — the
  natural orchestration once the subcommands exist.
- **Grouping Phase 2 by NS server** for TCP/connection locality.

## 11. Key decisions
1. **Two phases** (`discover` NS, `resolve` records) reusing all existing internals; `scan` retained.
2. **Freshness-driven selection** via a compact per-domain **state table** (oldest-due-first + `--limit`
   → self-levelling to the target cadence). [CONFIRM vs hash-bucket.]
3. **NS reuse** is the payoff: discover rarely (`--ns-ttl`), resolve often (`--record-ttl`), records
   never touch a recursive resolver.
4. **Phase 1 → local recursive resolver** + **new `--discovery-inflight`** (unblocks the overload).
5. **State feedback loop:** Phase 2 marks NS stale on total failure → Phase 1 re-discovers.
6. Historical tables keep every scan for drift; the state table is the latest-only control plane.
