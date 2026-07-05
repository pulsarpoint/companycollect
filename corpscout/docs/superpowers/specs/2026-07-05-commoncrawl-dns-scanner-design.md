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
- One Parquet file per domain (we write **one row per domain** into rolling batched Parquet).

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

### 3.1 Two tiers of shared, rate-limited servers

To query authoritative servers "directly" we must first learn them, which is itself iterative
resolution. That produces two tiers of shared infrastructure that must each be paced:

**Tier 1 — NS discovery (parent/TLD-zone bound).** For each domain we compute its public suffix
(eTLD) via `golang.org/x/net/publicsuffix`, learn the TLD's nameservers **once per run** (cached),
then query a TLD nameserver for the domain's `NS` delegation + glue. Every `.com` domain's NS
delegation comes from the same ~13 gtld-servers → these queries are paced **per TLD-server IP**.

**Tier 2 — record queries (authoritative-NS bound).** Once domain D's authoritative NS set is
known (e.g. `ns1.cloudflare.com`, `ns2.cloudflare.com` → their IPs), all of D's record queries go
to those IPs. Many domains share Cloudflare → those IPs are paced **per authoritative-NS-server IP**.

### 3.2 The unifying abstraction: one limiter per target server IP

**Every outbound query targets a specific server IP. There is exactly one token-bucket rate
limiter per server IP (default ~10 qps, small burst, configurable), plus a small per-server
in-flight cap (e.g. 2–3). A query may only be sent once its target server's limiter grants a
token.** Tier 1 and Tier 2 are the same mechanism keyed on different server IPs.

Massive parallelism is *free*: there are hundreds of thousands of distinct authoritative server
IPs across the domain set, so total throughput = sum over all servers, while each individual
server stays gentle. We saturate egress by having many servers in flight, not by hitting any one
server hard.

The user's mental model — "group domains that share an NS, queue their requests together" — is
exactly what falls out of this: work for a shared server naturally lands in that server's queue
behind its limiter.

### 3.3 Decision: in-memory single process for v1, Redis deferred

The user proposed holding the whole NS→domains structure in Redis during the scan. **Recommended
v1: single-process, in-memory** (`map[serverIP]*rate.Limiter`, per-domain accumulator structs, a
bounded domain scheduler). Reasons:

- DNS queries are cheap (ms). One politely-paced box can process the full domain set over a run.
- The per-server rate guarantee is a *local* token bucket **as long as one process owns all
  queries to a given server** — which is automatically true in a single process. No distributed
  coordination, no Lua token buckets, no Redis round-trips on the hot path.
- Dramatically simpler and faster to ship and reason about.

Cost: no crash-resume (a killed run is re-run; the scan is idempotent, keyed by `scan_id`), no
horizontal scale.

**Scale-out path (documented, not built):** if we later need multiple machines, **shard by target
server IP** (consistent hash of server IP → owner machine). Each server is then owned by exactly
one machine, so its ≤10 qps stays a *local* limiter with **zero cross-machine coordination** — the
only thing that needs to be shared is the per-domain accumulator (results for one domain arrive on
different machines), which is where Redis would earn its place. We isolate this behind a
`Limiter`/`Scheduler` interface so the swap is contained.

> **Open decision for review:** accept in-memory v1, or require Redis-backed from the start?

### 3.4 Iterative resolver mechanics

Library: `github.com/miekg/dns` (already the org's DNS library, per pulsarprotectrunner2).

Per-run caches: static root hints; TLD nameserver sets + glue IPs (learned once per TLD);
per-domain authoritative NS. Per query: EDNS0 (bufsize 1232, DO bit set for DNSSEC capture),
UDP first with **TCP fallback on truncation (TC bit)**, 2 retries with backoff across the domain's
alternate NS IPs, per-query timeout. Every query's outcome (`NOERROR/NXDOMAIN/SERVFAIL/timeout`)
is recorded in a `query_status` map for observability.

### 3.5 Concurrency shape

- Load domain list from CH → schedule domains through a **bounded domain semaphore** (e.g. 5k–20k
  in flight) so memory stays flat regardless of total domain count.
- Each in-flight domain is a small state machine: **discover NS (Tier 1) → fan out record queries
  (Tier 2) → assemble row → hand to Parquet writer**.
- Each individual query is a task that does `limiters[serverIP].Wait(ctx)` then sends. Goroutines
  mostly block on limiters, which is the intended backpressure.

## 4. Data flow & components

```
corpscout.commoncrawl_domains (CH)
        │  SELECT DISTINCT root_domain  (or --worklist parquet, --limit)
        ▼
  cc-dns-worker  scan
        │  Tier-1 NS discovery (per-TLD-server limiters)
        │  Tier-2 record queries (per-auth-NS-server limiters)
        │  assemble one row per domain
        ▼
  dns.parquet  (rolling, one row per domain; parquet+ch tagged struct)
        │
  cc-dns-worker  load   (ClickHouse native protocol, batched INSERT)
        ▼
corpscout.commoncrawl_domain_dns   (ReplacingMergeTree, historical by scan_id)
```

### Proposed layout (mirrors `cc-enrich-worker`)
```
commoncrawl/cc-dns-worker/
  cmd/cc-dns-worker/main.go     # subcommands: scan | load
  internal/input/               # read domain worklist from CH / parquet
  internal/resolve/             # iterative resolver, root/TLD/NS caches, DNSSEC capture
  internal/scheduler/           # per-server-IP token-bucket limiters + in-flight caps (interface)
  internal/records/             # query planning: hostnames, DKIM selectors, mail/policy qnames
  internal/model/               # DomainDNSRow (parquet+ch tags)
  internal/output/              # rolling parquet writer
  internal/load/                # CH native batch insert (mirror cc-enrich-worker/internal/load)
```

## 5. ClickHouse schema

One wide row per (domain, scan), following the house `ReplacingMergeTree(resolved_at)` +
verbatim-capture style. `scan_id` is in the ORDER BY key so re-scans **coexist as history**;
within one scan, a re-run of a domain is deduped to the latest `resolved_at` (read with `FINAL`).

```sql
-- clickhouse/migrations/0000NN_corpscout_commoncrawl_domain_dns.up.sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns
(
    scan_id        LowCardinality(String),   -- scan batch identity, e.g. "2026-07-05"
    root_domain    String,
    etld           LowCardinality(String),   -- public suffix used for parent/DS queries

    nameservers    Array(String),            -- authoritative NS names
    ns_ips         Array(String),            -- resolved NS IPs (glue or A/AAAA)

    a              Map(LowCardinality(String), Array(String)),  -- hostname -> IPv4s, apex = "@"
    aaaa           Map(LowCardinality(String), Array(String)),  -- hostname -> IPv6s

    mx             Array(String),            -- "<pref> <host>" verbatim
    txt            Array(String),            -- ALL apex TXT verbatim (SPF included)
    dmarc          String,                   -- _dmarc TXT verbatim
    dkim           Map(LowCardinality(String), String),  -- selector -> TXT (answered selectors only)
    mta_sts        String,
    tls_rpt        String,
    bimi           String,

    caa            Array(String),
    soa            String,

    dnssec_signed  UInt8,                     -- DNSKEY present at apex
    ds_present     UInt8,                     -- DS present at parent
    dnskey         Array(String),
    ds             Array(String),

    query_status   Map(LowCardinality(String), String),  -- "qname/type" -> rcode/error
    resolver_path  LowCardinality(String),   -- "iterative"
    source_run_id  String,
    resolved_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (root_domain, scan_id);
```

> **Open decision for review:** also emit an optional long/raw companion table
> (`commoncrawl_domain_dns_records`: one row per `(scan_id, root_domain, qname, qtype, rcode, ttl,
> rdata)`) for full fidelity/debugging? Recommended **deferred** — the wide table + `query_status`
> covers v1; add the raw table only if we find we need per-record TTL/RRSIG detail.

## 6. Error handling
- Per-query: retry (2×) across alternate NS IPs, TCP on truncation, bounded timeout; on final
  failure the domain row still emits with that query's status recorded and its field empty.
- Tier-1 failure (cannot learn NS): emit a minimal row (`nameservers=[]`, statuses populated) so a
  dead/parked domain is still recorded as scanned.
- Server-level: repeated timeouts to a server IP trip a short circuit-breaker so we stop wasting
  budget on a dead server without starving others.
- The run is idempotent and re-runnable; partial output is safe (ReplacingMergeTree + `scan_id`).

## 7. Testing strategy
- **Unit:** qname construction (DKIM selectors, `_dmarc`, `_mta-sts`, …), eTLD via publicsuffix,
  response→row mapping, per-server limiter pacing (assert ≤ configured qps), NS/TLD cache reuse.
- **Deterministic integration:** spin an in-process `miekg/dns` authoritative server on 127.0.0.1
  serving a crafted zone; drive the full iterative resolver + scheduler against it and assert the
  assembled row + that per-server pacing held. (No external network; fully reproducible.)
- **Real-DNS smoke (build-tagged):** resolve a few known-stable domains (e.g. `cloudflare.com` NS,
  `google.com` MX) to confirm real-world shape — network-permitting, per the "real integration
  tests" preference.
- **CH load:** insert a sample `dns.parquet` into a real ClickHouse and read it back (per the
  "integration tests must run against real infra" preference).
- Enforce `parquet` tag == `ch` tag on `DomainDNSRow` with a struct-tag test, as `cc-enrich-worker`
  does.

## 8. Orchestration / cadence
v1 deliverable is the Go worker + migration + tests. The every-few-days cadence is added as a thin
follow-up **dagster asset** (partitioned by `scan_id`=date) that runs `scan` then `load`. Keeping
the scanner a standalone binary preserves the `cc-enrich-worker` operational pattern (run on a
box, produce Parquet, load to CH) while letting dagster own scheduling.

## 9. Key decisions summary
1. **Go worker** under `commoncrawl/cc-dns-worker`, mirroring `cc-enrich-worker` (scan + load).
2. **Iterative resolution** with `miekg/dns`, querying authoritative servers directly.
3. **One token-bucket limiter per target server IP** (~10 qps, configurable) is the single
   rate-limit primitive for both TLD (Tier 1) and authoritative-NS (Tier 2) tiers.
4. **In-memory, single process for v1**; Redis deferred; scale-out via shard-by-server-IP.
5. **One wide CH row per (domain, scan)**, `ReplacingMergeTree(resolved_at)`, historical via
   `scan_id` in the ORDER BY key; raw verbatim capture, analysis derived later in SQL.
6. Static configurable **hostname list (5)** and **DKIM selector list (10)**; CT-log discovery is a
   designed-for future input.
7. **DNSSEC captured, not validated** in v1.
```
