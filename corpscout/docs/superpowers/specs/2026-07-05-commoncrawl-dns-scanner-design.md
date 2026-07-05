# CommonCrawl DNS Scanner — Design

**Date:** 2026-07-05
**Status:** Draft for review
**Author:** brainstorming session (Goran + Claude)

## 1. Problem & Goal

We have a large set of domains in ClickHouse (`corpscout.commoncrawl_domains`). We want a
service that, for each domain, resolves a rich DNS record set **directly from the domain's
authoritative nameservers**, stores one row per domain per scan back in ClickHouse, and can be
re-run every few days to build **historical** DNS posture over time.

The hard part is not the DNS parsing — it is doing this **politely at scale**. A naive fan-out
would hammer shared infrastructure: millions of domains delegate to the same handful of TLD
servers (Verisign for `.com`) and the same big managed-DNS providers (Cloudflare, AWS Route 53,
GoDaddy). We must never send more than a few queries per second to any single server, while still
saturating our own egress by keeping a very large number of *distinct* servers in flight at once.

### Non-goals (v1)
- Full DNSSEC chain validation (we **capture** signing material; validation is derived later).
- HTTP-based mail checks (MTA-STS policy file over HTTPS, `.well-known`); we only read the DNS side.
- CT-log hostname discovery (designed for, but the hostname list is static/configurable in v1).
- Multi-node / distributed scanning (single node in v1; Redis is explicitly out).
- Parquet intermediate files (Parquet's multi-writer coordination is the problem we avoid; results
  stage in embedded SQLite instead — see §3.3, §4).

## 2. What we collect per domain

Per-domain query budget is ~30–50 DNS queries, matching the user's "~50 requests per domain."

**Hostnames (A/AAAA):** apex plus the 5 most common subdomains. Default list is configurable and
later replaced/augmented by CT-log discovery:
`www, mail, webmail, smtp, autodiscover` (apex is always resolved, stored under key `@`).

**Mail & policy records:**
- `MX` at apex
- `TXT` at apex — **all** TXT captured verbatim (SPF `v=spf1…` lives here)
- `DMARC` — `TXT _dmarc.<domain>`
- **DKIM brute force** — `TXT <selector>._domainkey.<domain>` for the top-10 selectors (configurable):
  `default, google, selector1, selector2, k1, dkim, s1, s2, mail, mandrill`
  (selector1/2 = Microsoft 365, google = Google Workspace, k1/mandrill = Mailchimp, …)
- `MTA-STS` — `TXT _mta-sts.<domain>`
- `TLS-RPT` — `TXT _smtp._tls.<domain>`
- `BIMI` — `TXT default._bimi.<domain>`

**Zone / infra records:**
- `NS` (authoritative set, from the domain itself) + resolved NS IPs
- `SOA` at apex
- `CAA` at apex
- **DNSSEC capture:** `DNSKEY` at apex, `DS` at parent (TLD) zone — store presence + records; do
  not validate the chain in v1.

Everything is stored **verbatim** (raw rdata strings, Maps, Arrays). SPF/DMARC/DKIM parsing,
hygiene scoring, and issue-template mapping are **derived later in SQL** — consistent with the
existing `commoncrawl_domain_security` / `commoncrawl_domain_page_meta` "capture broadly now,
analyze later" pattern.

## 3. The rate-limiting model (core of the design)

### 3.1 Two tiers of DNS work

To query authoritative servers "directly" we must first learn them. We split that into two tiers:

**Tier 1 — NS discovery (recursive).** For each domain we compute its public suffix (eTLD) via
`golang.org/x/net/publicsuffix`, then send **recursive** (RD=1) queries to a small set of configured
recursive resolvers — `NS` (authoritative nameserver names), `A`/`AAAA` per NS name (their IPs), and
`DS` (parent DNSSEC). The recursive resolver's cache absorbs the root/TLD load and transparently
resolves cross-TLD / glue-less nameservers, so **we never walk roots or hammer TLD servers.** Default
resolvers are public (1.1.1.1/8.8.8.8/9.9.9.9); for large runs point `--resolvers` at a local
`unbound` (`127.0.0.1:53`) to remove public-resolver dependence and get natural TLD caching.

> **Decision — why not iterative-from-roots.** Walking root→TLD→auth ourselves (the original idea)
> would let us pace the TLD tier directly, but it forces a hand-rolled iterative resolver (referrals,
> glue resolution for out-of-bailiwick NS, CNAME chains, per-run TLD caching) that is hard to get
> right and, done naively, hammers roots/TLDs once per domain. Delegating discovery to a recursive
> resolver eliminates that entire class of work and bugs. We keep "direct to authoritative" where it
> actually matters — the record queries (Tier 2).

**Tier 2 — record queries (authoritative-NS bound).** Once domain D's authoritative NS IPs are
known, all of D's record queries go **directly to those IPs** (RD=0). Many domains share Cloudflare
→ those IPs are paced **per authoritative-NS-server IP**; a small domain on its own single nameserver
is protected by the same per-server limiter. This is where per-server rate limiting earns its keep.

### 3.2 The unifying abstraction: one limiter per target server IP

**Every outbound query targets a specific server IP. There is exactly one token-bucket rate
limiter per server IP (default ~10 qps, small burst, configurable), plus a small per-server
in-flight cap (e.g. 2–3). A query may only be sent once its target server's limiter grants a
token.** Both tiers use the same mechanism keyed on different server IPs, via two `Scheduler`
instances: a **discovery** scheduler (the few recursive resolvers, a higher default qps — or very
high for a local unbound) and an **authoritative** scheduler (per-NS-IP, ~10 qps). The concurrency
that drives them is in §3.6.

Massive parallelism is *free* on the authoritative tier: there are hundreds of thousands of distinct
authoritative server IPs across the domain set, so total throughput = sum over all servers, while
each individual server stays gentle. We saturate egress by having many servers in flight, not by
hitting any one server hard. (Tier-1 throughput is bounded by the recursive resolvers, which is why
a local unbound is recommended at full scale.)

The user's mental model — "group domains that share an NS, queue their requests together" — is
exactly what falls out of this: work for a shared server naturally lands in that server's queue
behind its limiter.

### 3.3 Decision: single node, in-memory limiters, durable SQLite queue (no Redis)

Single process, no Redis. The rate limiters live **in memory** (`map[serverIP]*rate.Limiter`),
because the per-server guarantee is a *local* token bucket as long as one process owns all queries
to a given server — automatically true single-node. No distributed coordination, no Lua buckets, no
hot-path round-trips.

**Crash-resume is a requirement, so the work queue and per-domain status are durable in an embedded
SQLite database** (`scan.db`, WAL mode). SQLite doubles as the results stage — which also solves the
reason we avoid Parquet (see §3.6). The limiters' *token* state is deliberately **not** persisted:
on restart, pacing simply starts fresh, which is harmless. What must survive a restart is *which
domains are done* and *the records already resolved*, and those are in SQLite.

- On `scan` start: if `scan.db` has no rows for this `scan_id`, seed the `scan_domains` queue from
  the CH domain list with `status='pending'`. If rows exist, keep them.
- A domain is claimed, resolved, its records + summary written, and its status flipped to
  `done`/`error` **in one SQLite transaction**. A kill mid-domain leaves it `pending` (or
  `in_progress`, reset to `pending` on next start) → it is retried; a finished domain is never
  redone.
- `scan` is therefore restartable and idempotent: re-running resumes the remaining `pending` set.

**Scale-out path (documented, not built):** for multiple machines later, **shard by target server
IP** (consistent hash → owner machine); each server stays owned by one machine so its limiter stays
local with zero coordination — only the per-domain accumulator would need sharing (where Redis would
finally earn its place). The `Scheduler` interface keeps that swap contained.

### 3.4 Why SQLite, not Parquet, for staging

Parquet cannot be written by multiple writers concurrently, and coordinating a rolling set of files
is exactly the complexity that pushed toward Redis. An embedded **SQLite** stage removes both
problems at once: it accepts concurrent work through **one dedicated writer goroutine** doing
batched transactions (fast — tens of thousands of row-inserts/sec), it is a real queryable database
(so it holds the queue + status for resume), and it is a single local file with no external service.
The final `load` step bulk-copies SQLite → ClickHouse over the native protocol.

### 3.5 Resolver mechanics

Library: `github.com/miekg/dns` (already the org's DNS library, per pulsarprotectrunner2). The
monorepo also has `pulsarprotectrunner2/pkg/dns` (a recursive engine with health tracking, racing,
`GetAuthoritativeNameservers`); we model discovery on it but implement a lean in-module client rather
than import that module's heavy dependency tree (geoip, otel). If reuse is later preferred, extract
`pkg/dns` into a shared module first.

- **Tier-1 discovery:** recursive (RD=1) `NS`/`A`/`AAAA`/`DS` queries to the configured resolvers,
  rotating across them with 2 attempts each. Because recursive answers arrive in the ANSWER section,
  discovery is deterministically unit-testable against an in-process server.
- **Tier-2 records:** direct (RD=0) queries to the domain's NS IPs, rotating across them with retry.
- Every query: EDNS0 (bufsize 1232, DO bit set for DS/DNSKEY capture), UDP first with **TCP fallback
  on truncation (TC bit)**, per-query timeout. Retries are rotation-based (the per-server limiter
  spaces retries to the same server); an explicit backoff + per-server circuit breaker is deferred.
- Per-domain `queries_total`/`queries_ok` and each record's `rcode` capture outcome for observability.

### 3.6 Concurrency shape

- Seed the durable queue (§3.3) from CH, then a pool of **N resolver goroutines** (bounded, e.g.
  2k–10k) each claims a `pending` domain and runs its state machine: **discover NS (Tier 1) → fan
  out record queries (Tier 2) → emit a `DomainResult`**.
- Each `DomainResult` (summary + list of records) is sent on a channel to **one dedicated SQLite
  writer goroutine**, which commits results and flips domain status in **batched transactions**.
  One writer sidesteps SQLite's single-writer lock cleanly and keeps throughput high.
- Each individual query is a task that does `scheduler.Do(serverIP, send)` — it blocks on that
  server's limiter, which is the intended backpressure. Goroutines mostly wait on limiters.

## 4. Data flow & components

```
corpscout.commoncrawl_domains (CH)
        │  SELECT DISTINCT root_domain  (--query / --limit)
        ▼
  cc-dns-worker  scan  ──seed──▶  scan.db (SQLite): scan_domains queue (status)
        │
        │  resolver pool per domain:
        │    Tier-1 discovery → recursive resolvers (discovery scheduler; or local unbound)
        │    Tier-2 records   → domain's authoritative NS IPs directly (per-NS-IP scheduler)
        │                                 ─▶ DomainResult channel
        │                                         │
        │                          single writer goroutine (batched txns)
        ▼                                         ▼
  scan.db (SQLite): scan_domains(status,summary) + scan_records(one row per RR)
        │   ← resumable: restart continues the pending set
        │
  cc-dns-worker  load   (SQLite → ClickHouse, native-protocol batched INSERT)
        ▼
corpscout.commoncrawl_domain_dns_scan     (one row per domain per scan; summary/status)
corpscout.commoncrawl_domain_dns_records  (one row per DNS record; the normalized table)
        (both ReplacingMergeTree, historical by scan_id)
```

### Proposed layout (mirrors `cc-enrich-worker`)
```
commoncrawl/cc-dns-worker/
  cmd/cc-dns-worker/main.go     # subcommands: scan | load
  internal/input/               # read domain worklist from CH
  internal/resolve/             # recursive NS discovery + direct-authoritative record queries, DNSSEC
  internal/scheduler/           # per-server-IP token-bucket limiters + in-flight caps (interface)
  internal/records/             # query planning: hostnames, DKIM selectors, mail/policy qnames
  internal/model/               # DomainResult, DNSRecord, ScanSummary (ch tags for load)
  internal/store/               # SQLite: durable queue + status + staged records (single writer)
  internal/load/                # SQLite → CH native batch insert
```

## 5. Schema

Two normalized ClickHouse tables (the user's call: a proper `record_type`/`value` table over a wide
per-domain row). Both are `ReplacingMergeTree(resolved_at)` with `scan_id` in the ORDER BY key, so
re-scans every few days **coexist as history**; a re-run of the same domain in the same scan dedups
to the latest `resolved_at` (read with `FINAL`). Records are stored **verbatim**; SPF/DMARC/DKIM
parsing and scoring are derived later in SQL.

### 5.1 `commoncrawl_domain_dns_records` — the normalized record table (primary)
One row per resolved DNS record. Absence of a record = no row; observability of *attempts* lives in
the scan-summary table's status.
```sql
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records
(
    scan_id      LowCardinality(String),
    root_domain  String,
    name         String,                    -- qname queried (e.g. www.example.com, _dmarc.example.com)
    record_type  LowCardinality(String),    -- A, AAAA, MX, TXT, NS, SOA, CAA, DNSKEY, DS
    slot         LowCardinality(String),    -- semantic tag: "@", "www", "dmarc", "mta_sts", "bimi",
                                             --   "tls_rpt", DKIM selector, "" for infra records
    value        String,                    -- rdata verbatim (full TXT, IP, ...); MX = "<pref> <host>"
                                             --   so the sort key can't collapse two MX at diff prefs
    ttl          UInt32,
    priority     UInt16,                     -- MX preference (also embedded in value); 0 otherwise
    rcode        LowCardinality(String),     -- query rcode: NOERROR / NXDOMAIN / SERVFAIL / error
    source_run_id String,
    resolved_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, scan_id, record_type, name, value);
```

### 5.2 `commoncrawl_domain_dns_scan` — per-domain scan summary/status
One row per (domain, scan): what we learned about the zone as a whole, plus scan outcome. Small.
```sql
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_scan
(
    scan_id       LowCardinality(String),
    root_domain   String,
    etld          LowCardinality(String),
    nameservers   Array(String),            -- authoritative NS names
    ns_ips        Array(String),            -- resolved NS IPs (A/AAAA of the NS hostnames)
    dnssec_signed UInt8,                     -- DNSKEY present at apex
    ds_present    UInt8,                     -- DS present at parent
    status        LowCardinality(String),   -- done | error
    error         String,                    -- discovery/scan error, empty on success
    queries_total UInt16,
    queries_ok    UInt16,
    source_run_id String,
    resolved_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, scan_id);
```

### 5.3 SQLite stage (`scan.db`) — mirrors the two tables plus the queue
`scan_domains(scan_id, root_domain, status, etld, nameservers, ns_ips, dnssec_signed, ds_present,
queries_total, queries_ok, error, resolved_at, PRIMARY KEY(scan_id, root_domain))` is both the queue
(via `status`: `pending|done|error`) and the summary stage. `scan_records(scan_id, root_domain,
name, record_type, slot, value, ttl, priority, rcode, resolved_at)` stages the records. `load` reads
these and inserts into the two CH tables above.

## 6. Error handling
- Per-query: retry (2×) across alternate NS IPs, TCP on truncation, bounded timeout; on final
  failure the record simply produces no row and the query's `rcode` is recorded as `error` on any
  row it would have produced. Per-domain `queries_total`/`queries_ok` capture the aggregate.
- Tier-1 failure (cannot learn NS): the domain is written to `scan_domains` with `status='error'`
  and the reason in `error`, so a dead/parked domain is still recorded as scanned (and not retried
  endlessly within a scan).
- Server-level: a per-server circuit breaker (short-circuit a server IP after N consecutive timeouts)
  is **deferred**; v1 relies on the per-server limiter + bounded retries.
- **Resume:** because status transitions are one SQLite transaction per domain, a crash leaves the
  DB consistent; restarting `scan` reclaims `pending` (and resets any `in_progress`) and continues.

## 7. Testing strategy
- **Unit:** qname construction (DKIM selectors, `_dmarc`, `_mta-sts`, …), eTLD via publicsuffix,
  response→records mapping, per-server limiter pacing (assert ≤ configured qps).
- **SQLite store:** seed a queue, mark some domains done, reopen the DB and assert the pending set
  is exactly the not-done domains (the resume contract); assert batched record writes round-trip.
- **Deterministic discovery + records:** an in-process `miekg/dns` server on 127.0.0.1 serving a
  crafted zone doubles as a stand-in recursive resolver (answers in the ANSWER section), so both
  `DiscoverNS` (NS names, cross-TLD NS IPs, DS) **and** the record `collect` path are unit-tested with
  no external network — fully reproducible.
- **Real-DNS smoke (build-tagged):** resolve a few known-stable domains (e.g. `cloudflare.com` NS,
  `google.com` MX) to confirm real-world shape — network-permitting, per the "real integration
  tests" preference.
- **CH load:** stage rows in a temp `scan.db`, `load` into a real ClickHouse, read back (per the
  "integration tests must run against real infra" preference).
- Enforce `ch` tags on the load structs match the CH column names via a struct-tag test.

## 8. Orchestration / cadence
v1 deliverable is the Go worker + migrations + tests. The every-few-days cadence is added as a thin
follow-up **dagster asset** (partitioned by `scan_id`=date) that runs `scan` then `load`. Keeping
the scanner a standalone binary preserves the `cc-enrich-worker` operational pattern (run on a box,
stage locally, load to CH) while letting dagster own scheduling.

## 9. Key decisions summary
1. **Go worker** under `commoncrawl/cc-dns-worker`, mirroring `cc-enrich-worker` (scan + load).
2. **Recursive NS discovery** (Tier 1) to configured resolvers (public, or a local `unbound`) — no
   root walk — then **direct-to-authoritative record queries** (Tier 2) with `miekg/dns`. Lean
   in-module client modeled on `pulsarprotectrunner2/pkg/dns` (not imported — heavy deps).
3. **One token-bucket limiter per target server IP** (configurable) via two `Scheduler` instances: a
   discovery scheduler (recursive resolvers, higher qps) and an authoritative scheduler (~10 qps/NS-IP).
4. **Single node, no Redis.** Limiters in memory; **durable embedded SQLite** holds the work queue +
   status + staged records so a killed run **resumes** where it stopped. Scale-out (deferred) via
   shard-by-server-IP.
5. **SQLite stage, not Parquet** — avoids Parquet's multi-writer coordination; one dedicated writer
   goroutine batches into SQLite. `load` bulk-copies SQLite → ClickHouse.
6. **Two normalized CH tables**: `commoncrawl_domain_dns_records` (one row per record —
   `record_type`/`value`/`name`/`ttl`/`priority`/`rcode`) and `commoncrawl_domain_dns_scan`
   (per-domain summary/status). Both `ReplacingMergeTree(resolved_at)`, historical via `scan_id`.
7. Static configurable **hostname list (5)** and **DKIM selector list (10)**; CT-log discovery is a
   designed-for future input.
8. **DNSSEC captured, not validated** in v1.
```
