# cc-dns-worker

Resolves a **rich DNS record set directly from each domain's own authoritative nameservers** for
every domain in `corpscout.commoncrawl_domains`, stages the results in an embedded SQLite database
(resumable), and — as a separate step — loads that stage into two ClickHouse tables. Re-running it
every few days builds **historical DNS posture** per domain (SPF/DKIM/DMARC drift, nameserver
migrations, DNSSEC adoption, …), keyed by `scan_id`.

It is a subcommand CLI: `cc-dns-worker <scan|load> [flags]`. Like `cc-enrich-worker`, it does not
orchestrate — there's no worklist builder or scheduler package here, just the two stages:

| Stage | Command | Does | ClickHouse |
|---|---|---|---|
| **scan** | `scan` | pull domains from CH → resolve DNS directly from authoritative NS → stage in SQLite | only **reads** `commoncrawl_domains` |
| **load** | `load` | read the SQLite stage → bulk `INSERT` into `commoncrawl_domain_dns_*` | the **only** writer |

Splitting them means a `scan` can be inspected/re-run before it's loaded, a `load` can re-run without
re-resolving, and a crashed `scan` never half-writes ClickHouse. Both stages use the ClickHouse native
protocol, so `clickhouse-client` is never required.

> Full design rationale (rate-limiting model, why SQLite not Parquet, alternatives considered):
> [`../../docs/superpowers/specs/2026-07-05-commoncrawl-dns-scanner-design.md`](../../docs/superpowers/specs/2026-07-05-commoncrawl-dns-scanner-design.md).
> This doc is the worker's CLI + internals reference.

## Why "directly from authoritative nameservers"

Most DNS tooling asks a recursive resolver "what does this domain have?" That's fine for one lookup,
but at commoncrawl scale (tens of millions of domains) recursive resolvers become a shared,
rate-limited bottleneck you don't control. This worker instead learns each domain's own authoritative
nameserver IPs once, then queries **those IPs directly** (`RD=0`) for every record — spreading load
across the hundreds of thousands of distinct authoritative servers domains actually delegate to,
while still being polite to each one individually.

## The two-tier model

**Tier 1 — NS discovery (recursive).** For each domain, compute its public suffix (eTLD) via
`golang.org/x/net/publicsuffix`, then send **recursive** (`RD=1`) queries to a small set of
configured resolvers (`--resolvers`) for `NS` (authoritative nameserver names), `A`/`AAAA` per NS
name (their IPs), and `DS` (parent DNSSEC). The recursive resolver's cache absorbs the root/TLD load
and transparently resolves cross-TLD / glue-less nameservers — **the worker never walks roots or
hammers TLD servers itself.** Implemented in `internal/resolve/discover.go` (`Discoverer.DiscoverNS`).

- **`--resolvers` is REQUIRED — there is no public-resolver default.** Point it at a **local
  recursive resolver** (`unbound` / PowerDNS Recursor / knot-resolver) on the scan box:
  `--resolvers 127.0.0.1:53 --discovery-qps 2000`. Hammering public resolvers (1.1.1.1/8.8.8.8/…)
  with a full-corpus discovery run gets you throttled/blocked; a local resolver removes that
  dependence and gives natural TLD-level caching (a `.com` lookup is warm after the first few
  thousand domains). See **[Local recursive resolver (required)](#local-recursive-resolver-required)**
  below for the recommended topology, hardware sizing, and a tuned `unbound` config.

**Tier 2 — record queries (direct-to-authoritative).** Once a domain's authoritative NS IPs are
known, every record query for that domain goes **directly to those IPs** (`RD=0`), rotating across
them with retry. Implemented in `internal/resolve/query.go` (`Resolver.Resolve` / `queryAuth`). The
query plan (`internal/records/plan.go`) is per domain, no trailing dot needed on input:

- `A`/`AAAA` at apex (`@`) plus 5 default subdomains: `www, mail, webmail, smtp, autodiscover`
  (a subdomain that's a `CNAME` is captured verbatim rather than dropped).
- `MX`, `TXT` (verbatim — SPF *and* the service-verification tokens like `google-site-verification`,
  `MS=`, … live here in one query), `NS`, `SOA`, `CAA`, `DNSKEY` at apex.
- `HTTPS` (SVCB) at apex + `www` — ALPN, ECH, IP hints, CDN.
- Mail/policy: `_dmarc` (DMARC), `_mta-sts`, `_smtp._tls` (TLS-RPT), `default._bimi` — all `TXT`.
- **DKIM brute force**: `TXT <selector>._domainkey.<domain>` for 10 default selectors (`default,
  google, selector1, selector2, k1, dkim, s1, s2, mail, mandrill`).
- **SRV service discovery** (brute force, ~27 default `_service._proto` names): SIP/Teams, Exchange
  autodiscover, Active Directory (`_ldap`/`_kerberos`/`_gc`), Windows KMS, XMPP, cal/carddav, mail
  discovery, Matrix, STUN/TURN, game servers. Configurable via `records.Config.SRVServices`.
- `DS` at the parent zone, captured from Tier-1 discovery.

> The DKIM and SRV brute-force lists dominate query volume (~61 queries/domain by default, most of
> the SRV ones `NXDOMAIN`). Trim `SRVServices`/`DKIMSelectors` if you want less traffic — the
> `--stats-interval` line shows the live queries/domain — and they're the prime candidates for a
> slower re-scan cadence since they change rarely.

Everything is stored **verbatim** (raw rdata). SPF/DMARC/DKIM parsing and hygiene scoring are
**derived later in SQL**, not by this worker.

### Per-server rate limiting (the core mechanism)

Every outbound query targets one server IP. `internal/scheduler` hands out one token-bucket limiter
+ small in-flight semaphore **per server IP**, created lazily (`map[string]*server`, mutex-guarded).
Two independent `Scheduler` instances back the two tiers:

- **discovery scheduler** — the handful of recursive resolvers, `--discovery-qps` (default 50; bump
  much higher, e.g. 2000, for a local unbound).
- **authoritative scheduler** — one bucket per authoritative NS IP, `--per-server-qps` (default 10)
  and `--per-server-inflight` (default 3).

Because millions of domains share the same handful of nameserver providers (Cloudflare, Route 53,
GoDaddy, …), this per-IP keying is what keeps any single shared server from being hammered while
still saturating egress — total throughput is the *sum* over hundreds of thousands of distinct
server IPs in flight at once, each individually gentle. `--workers` (default 4000) bounds how many
domains are being resolved concurrently; most of those goroutines are simply blocked waiting on a
server's limiter.

Each per-server-IP entry in the scheduler also runs a **circuit breaker**: after `--breaker-threshold`
(default `5`) consecutive *transport* failures (timeouts) to one authoritative NS IP, that IP's
circuit opens and its queries fast-fail for `--breaker-cooldown` (default `30s`) instead of each
waiting out the full `--query-timeout`. This caps the cost of dead or firewalled nameservers,
especially when many domains share one. It counts transport failures only — a `SERVFAIL` response is
a normal (non-error) exchange and does not trip it. Set `--breaker-threshold 0` to disable.

## Local recursive resolver (required)

Tier-1 NS discovery **must** run against a recursive resolver you control (`--resolvers`) — there is
no public-resolver default, because pointing a full-corpus discovery run at 1.1.1.1/8.8.8.8/… gets you
throttled or blocked. A local `unbound` on a dedicated box removes that dependence and gives natural
TLD-level caching (the `.com` delegation is served from cache after the first lookup, so you never
re-walk roots/TLDs). Tier-2 record queries do **not** use it — they go directly to each domain's
authoritative servers.

### Topology

Two hosts is the clean setup:

- **Resolver host** — a plain LAN box running `unbound`; **no Tailscale** (keeps it clear of MagicDNS /
  `resolv.conf` management). Needs LAN reachability from the worker + internet egress for iterative
  resolution. Lock it down — an open recursive resolver is a DDoS-amplification vector.
- **Worker host** — runs `cc-dns-worker` with Tailscale (for ClickHouse over the tailnet) and broad
  outbound UDP/53 (Tier-2 record queries leave from here). Points at
  `--resolvers <resolver-lan-ip>:53 --discovery-qps 2000` (`--discovery-inflight`, default 500, keeps
  the resolver fed).

Co-location on one box works **only if** `unbound` binds `127.0.0.1:53` (not `0.0.0.0`, which would
collide with Tailscale's interface), but you lose cache longevity + resource isolation — prefer two
hosts.

### Rough hardware

Both loads are modest for a target of ~130–400 domains/s (33.6M every 2–3 days / daily). Start small
and scale whatever the stats say is the bottleneck.

| | Resolver | Worker |
|---|---|---|
| CPU | 4–8 cores | 8–16 cores |
| RAM | 16–32 GB (all cache) | 16–32 GB |
| Disk | 40 GB SSD | **200–500 GB NVMe** — the SQLite stage holds a full sweep's records (~20–50 GB/run) before `load`; use NVMe or the single writer bottlenecks |
| Net | 1 Gbps (bandwidth is tiny; watch packet-rate / conntrack) | 1 Gbps + OS tuning (below) — emits ~10k small UDP/53 packets/s to distinct IPs |

### `unbound` install + config (resolver host)

```bash
apt install unbound            # or: dnf install unbound
# put the config below in /etc/unbound/unbound.conf.d/scanner.conf
systemctl enable --now unbound
```

`/etc/unbound/unbound.conf.d/scanner.conf` — tuned for the iterative-heavy, single-client scan load:

```yaml
server:
    interface: 127.0.0.1
    interface: 10.0.0.5              # this resolver host's LAN IP
    port: 53
    # LOCK DOWN — open resolvers are abused for amplification DDoS.
    access-control: 127.0.0.0/8 allow
    access-control: 10.0.0.0/24 allow   # your LAN / the worker's subnet
    access-control: 0.0.0.0/0 refuse
    module-config: "iterator"       # no DNSSEC validation (the worker captures DS/DNSKEY itself)
    num-threads: 8                  # = cores
    so-reuseport: yes
    so-rcvbuf: 8m
    so-sndbuf: 8m
    msg-cache-size: 4g
    rrset-cache-size: 8g
    cache-min-ttl: 60
    cache-max-ttl: 86400
    prefetch: yes
    prefetch-key: yes
    outgoing-range: 4096            # per thread — needs file descriptors (see OS tuning)
    num-queries-per-thread: 2048
    infra-cache-numhosts: 1000000   # you hit a huge number of distinct authoritative servers
    qname-minimisation: yes
    extended-statistics: yes
    # do-ip6: no                    # uncomment if the host has no IPv6 egress
remote-control:
    control-enable: yes             # enables `unbound-control stats`
```

### OS tuning (both hosts, the worker especially)

DNS scanners are killed by network *state*, not CPU:

- **File descriptors** — `outgoing-range 4096 × 8 threads ≈ 33k` FDs for unbound alone; the worker
  churns a socket per in-flight UDP query. Raise limits (`systemctl edit unbound` →
  `[Service]` / `LimitNOFILE=131072`; worker side `ulimit -n 131072` and matching `fs.file-max`).
- **conntrack** — thousands of short UDP/53 flows to unique IPs blow out a stateful firewall/NAT's
  conntrack table (drops look like timeouts). Bump `net.netfilter.nf_conntrack_max`, shorten UDP
  timeouts, or avoid conntracking outbound `:53`.
- **Ephemeral ports** (worker) — widen `net.ipv4.ip_local_port_range` (e.g. `1024 65535`).

### Verify

```bash
dig @10.0.0.5 NS cloudflare.com +short                              # from the worker: does it answer?
unbound-control stats | grep -E 'num.(queries|cachehits|cachemiss)'  # cache hit rate
```

Watch `cachehits / (cachehits + cachemiss)` next to the worker's `--stats-interval` `queries/s` line
for the full resolver-side + worker-side traffic picture.

## The SQLite stage + resume contract

`scan` never writes ClickHouse directly. It stages everything in an embedded SQLite database
(`--db`, default `scan.db`, WAL mode) via `internal/store`:

- `scan_domains` is **both the work queue and the per-domain summary**: `(scan_id, root_domain)` is
  the primary key, `status` is `pending` until a domain finishes, then `done` or `error`.
- `scan_records` stages every resolved DNS record for a scan.

**Resume contract:** on `scan` start, the seed is **streamed** from ClickHouse (`input.StreamClickHouse`)
in `--seed-chunk` batches and `INSERT OR IGNORE`-ed into SQLite as each batch arrives — rows that
already exist (from a prior, possibly crashed run) are left alone, and the CH result set is never
materialized in full. Dispatch is streamed the same way: `Store.PendingBatch` returns up to
`--dispatch-batch` domains whose status is **not** `done`/`error`, ordered by `root_domain` starting
after a cursor; `runScan` resolves and commits that whole batch, advances the cursor to the batch's
last domain, and only then fetches the next `PendingBatch` (a commit barrier — no batch is dispatched
until the previous one is durably committed). A domain's records + summary are written and its status
flipped in **one SQLite transaction** (`Store.CommitBatch`) — a kill mid-domain simply leaves it
`pending`, so it's picked up and retried on the next `scan` invocation with the same
`--scan-id`/`--db`. A domain that already finished is never re-resolved. This makes `scan` **idempotent
and restartable**: `--limit 0` (full corpus) runs can be killed and re-run indefinitely, converging on
"0 pending" only when every domain has a terminal status.

Because seeding and dispatch are both chunked with each batch fully committed before the next is
fetched, peak memory is **bounded** to roughly one `--dispatch-batch` of in-flight results (default
20000 domains) — a full-corpus run (~33.6M domains) never holds the whole seed list or the whole
pending queue in memory at once.

> **Note on `--limit` reproducibility:** the default `--query` ends with `ORDER BY root_domain`, so
> `--limit N` returns the *same* N domains on every run. A re-run of `scan --limit N --scan-id X`
> therefore finds them already done and reports "0 pending" (deterministic resume). If you pass a
> *custom* `--query` together with `--limit`, add your own `ORDER BY` for the same guarantee — without
> a stable order ClickHouse may return a different N rows each run (harmless for a full `--limit 0`
> run, which seeds every domain regardless of order).

Only one goroutine ever calls `CommitBatch` (`db.SetMaxOpenConns(1)`), so SQLite's single-writer lock
is never contended: within each dispatch batch, `resolveBatch` fans the batch out across up to
`--workers` resolver goroutines, collects their `model.DomainResult`s off a channel, then that one
goroutine commits them in `--commit-batch` (default 200) domain transactions before `runScan` advances
the cursor and fetches the next `--dispatch-batch`.

## Build & run

Go **1.25+**. From `commoncrawl/cc-dns-worker/`:

```bash
go build -o bin/cc-dns-worker ./cmd/cc-dns-worker   # or: make build
make test    # go test ./...
make vet     # go vet ./...
make fmt     # go fmt ./...
```

A minimal scan-then-load round trip:
```bash
export CLICKHOUSE_ADDR=<host>:9002 CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=*** CLICKHOUSE_DB=corpscout
./bin/cc-dns-worker scan --db scan.db --scan-id 2026-07-06         # full corpus (--limit 0 default)
./bin/cc-dns-worker load --db scan.db --scan-id 2026-07-06
```

## Module layout

```
cc-dns-worker/
├── cmd/cc-dns-worker/
│   ├── main.go   # subcommand dispatch (scan | load)
│   ├── scan.go   # runScan: seed -> two schedulers -> resolver pool -> batched SQLite writer
│   ├── load.go   # runLoad: SQLite stage -> ClickHouse
│   └── ch.go     # chConn: CLICKHOUSE_* env -> native driver.Conn
├── internal/
│   ├── input/      # StreamClickHouse: batch-stream root_domain from CH (bounded-memory seed)
│   ├── resolve/     # Tier-1 Discoverer (discover.go), Tier-2 Resolver (query.go),
│   │                #   shared Exchanger transport (exchange.go: UDP+EDNS0, TCP fallback on truncation)
│   ├── scheduler/   # per-server-IP token-bucket limiter + in-flight cap
│   ├── records/     # Plan(): the fixed list of Tier-2 queries per domain
│   ├── model/       # DomainResult/DNSRecord (in-memory) + RecordRow/ScanRow (ch-tagged, for load)
│   ├── store/       # SQLite stage: Open/Seed/Pending/CommitBatch/StagedRecords/StagedDomains
│   └── load/        # FromStore: SQLite -> ClickHouse native batch INSERT
├── bin/            # build output (gitignored)
└── Makefile · go.mod
```

## Subcommands & flags

`-h` under either command prints its own flag set (`flag.NewFlagSet(..., flag.ExitOnError)`).

### `scan`

Seeds (or resumes) the SQLite queue from ClickHouse, then resolves every pending domain.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--scan-id` | string | today, UTC (`2006-01-02`) | scan batch id; ties `scan` and `load` together and becomes the CH `scan_id` column |
| `--run-id` | string | `= --scan-id` | source run id stamped on rows (`source_run_id`); override to distinguish multiple `scan` invocations that share one `--scan-id` |
| `--db` | string | `scan.db` | SQLite stage path |
| `--query` | string | `SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains ORDER BY root_domain` | ClickHouse query returning one `root_domain` column (keep an `ORDER BY` if you customize it and use `--limit` — see the reproducibility note above) |
| `--limit` | int | `0` (all) | cap on domains pulled from CH this invocation |
| `--resolvers` | string (CSV) | *(required, no default)* | recursive resolvers for Tier-1 NS discovery — point at a local resolver, e.g. `127.0.0.1:53` (unbound / PowerDNS Recursor); the run errors if unset |
| `--discovery-qps` | float | `50` | max queries/sec **per** recursive resolver; raise substantially (e.g. `2000`) for a local resolver |
| `--discovery-inflight` | int | `500` | max concurrent in-flight queries **per** recursive resolver — keep high so a local resolver isn't starved |
| `--per-server-qps` | float | `10` | max queries/sec **per** authoritative NS IP (Tier 2) |
| `--per-server-inflight` | int | `3` | max concurrent queries per **authoritative** NS IP (Tier-2 politeness; discovery uses `--discovery-inflight`) |
| `--workers` | int | `4000` | max domains resolved concurrently (semaphore-bounded goroutine pool) |
| `--commit-batch` | int | `200` | domains per SQLite commit transaction |
| `--seed-chunk` | int | `5000` | domains per SQLite seed transaction (only affects the initial `INSERT OR IGNORE` pass) |
| `--dispatch-batch` | int | `20000` | domains fetched from the queue and resolved per barrier iteration; bounds peak memory independently of corpus size |
| `--query-timeout` | duration | `5s` | per-DNS-query timeout (both tiers) |
| `--breaker-threshold` | int | `5` | consecutive transport failures before a server IP's circuit opens (`0` disables) |
| `--breaker-cooldown` | duration | `30s` | how long a server IP's circuit stays open before a half-open probe |
| `--stats-interval` | duration | `5s` | how often to print a live throughput/traffic stats line to **stdout** (`0` = off) |

While `scan` runs it prints a live stats line to **stdout** every `--stats-interval` (progress logs go
to stderr), so you can see throughput and how much DNS traffic is being generated — plus a final
summary line with run averages:
```
stats: elapsed=8s domains=13 (6/s, avg 2/s) queries=1316 (498/s, 101.2/domain) err: q=18.2% dom=0.0%
```
`queries` counts DNS queries actually sent (the traffic), `queries/domain` includes per-query NS-IP
retries, and `q`/`dom` are the query- and domain-level error rates.

### `load`

Bulk-copies one scan's SQLite stage into ClickHouse. Safe to re-run (target tables are
`ReplacingMergeTree`; read them with `FINAL`).

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--db` | string | `scan.db` | SQLite stage path (must match the `scan` that produced it) |
| `--scan-id` | string | today, UTC | which scan's staged rows to load |

## ClickHouse tables & history via `scan_id`

Two tables, both `ReplacingMergeTree(resolved_at)` with `scan_id` inside `ORDER BY` — so re-scanning
the same domains every few days **accumulates as history** rather than overwriting; a duplicate row
for the same key within one scan dedupes to the newest `resolved_at` when read with `FINAL`.
Migrations: `../../clickhouse/migrations/000101_corpscout_commoncrawl_domain_dns_records.up.sql` and
`000102_corpscout_commoncrawl_domain_dns_scan.up.sql`.

**`commoncrawl_domain_dns_records`** — one row per resolved record (the normalized table; absence of
a record = no row, not a null):

| Column | Type | Notes |
|---|---|---|
| `scan_id` | `LowCardinality(String)` | |
| `root_domain` | `String` | |
| `name` | `String` | qname queried, e.g. `www.example.com`, `_dmarc.example.com` |
| `record_type` | `LowCardinality(String)` | `A, AAAA, MX, TXT, NS, SOA, CAA, DNSKEY, DS` |
| `slot` | `LowCardinality(String)` | `@`, subdomain name, DKIM selector, `dmarc`/`mta_sts`/`tls_rpt`/`bimi`, or `""` for infra records |
| `value` | `String` | rdata verbatim; **MX is `"<pref> <host>"`** so the sort key (which excludes `priority`) can't collapse two MX at different preferences |
| `ttl` | `UInt32` | |
| `priority` | `UInt16` | MX preference (also embedded in `value`); `0` otherwise |
| `rcode` | `LowCardinality(String)` | rcode of the query that produced this record |
| `source_run_id` | `String` | |
| `resolved_at` | `DateTime64(3, 'UTC')` | ReplacingMergeTree version column |

`ORDER BY (root_domain, scan_id, record_type, name, value)`.

**`commoncrawl_domain_dns_scan`** — one row per (domain, scan): zone-level summary + outcome:

| Column | Type | Notes |
|---|---|---|
| `scan_id` | `LowCardinality(String)` | |
| `root_domain` | `String` | |
| `etld` | `LowCardinality(String)` | public suffix, via `publicsuffix` |
| `nameservers` | `Array(String)` | authoritative NS names |
| `ns_ips` | `Array(String)` | resolved NS IPs (A/AAAA of the NS names) |
| `dnssec_signed` | `UInt8` | DNSKEY present at apex |
| `ds_present` | `UInt8` | DS present at the parent zone |
| `status` | `LowCardinality(String)` | `done` \| `error` |
| `error` | `String` | discovery/scan error text; empty on success |
| `queries_total` / `queries_ok` | `UInt16` | Tier-2 query attempt/success counters |
| `source_run_id` | `String` | |
| `resolved_at` | `DateTime64(3, 'UTC')` | ReplacingMergeTree version column |

`ORDER BY (root_domain, scan_id)`.

The `load` column list is derived from each Go struct's `ch` tag (`internal/model`, `internal/load`
`chColumns[T]`), so a table may carry extra defaulted columns the worker doesn't populate.

## Environment variables

Read via `envOr()` in `cmd/cc-dns-worker/ch.go`. Used by both `scan` (reads `commoncrawl_domains`)
and `load` (writes the two `commoncrawl_domain_dns_*` tables) — always the **native** ClickHouse
protocol, never HTTP.

| Var | Default | Controls |
|---|---|---|
| `CLICKHOUSE_ADDR` | `localhost:9000` | ClickHouse native-protocol `host:port` |
| `CLICKHOUSE_DB` | `corpscout` | database |
| `CLICKHOUSE_USER` | `default` | user |
| `CLICKHOUSE_PASSWORD` | (empty) | password |

## Testing strategy

- **Unit** (`internal/resolve`, `internal/records`, `internal/scheduler`, `internal/store`): qname
  construction, an in-process `miekg/dns` server standing in for both a recursive resolver and an
  authoritative server so `DiscoverNS` and record `collect()` are deterministically tested with no
  external network; limiter pacing; SQLite seed/resume round-trip (`TestCommitBatchAndResume`).
- **Real-DNS smoke** (build-tagged, `internal/resolve/smoke_test.go`): resolves a few known-stable
  domains against real recursive/authoritative infrastructure.
- **CH load integration** (`internal/load/integration_test.go`): stages rows in a temp SQLite db,
  loads into a real ClickHouse, reads back, cleans up via `t.Cleanup` (removed even on failure).
- **This README's e2e** (manual, run against real infra, not part of `go test`): bounded `scan`,
  resume proof, `load`, and a ClickHouse count/verify — see the design spec §7 and the task-11 report
  for the actual captured run.

## Deferred (documented, not built in v1)

- **Deploying local `unbound` for discovery** — the worker already supports it (`--resolvers
  127.0.0.1:53 --discovery-qps 2000`); standing up `unbound` itself on the scan box is the deferred
  operational step, recommended for large/full-corpus runs to remove public-resolver dependence and
  get natural TLD caching.
- **Disk footprint for full-corpus scans** — seeding (`--seed-chunk`) and dispatch (`--dispatch-batch`)
  are now both streamed/batched, so peak **memory** is bounded to roughly one dispatch-batch regardless
  of corpus size (see the SQLite stage section above). What's still deferred is **disk**: `scan_records`
  stages every resolved record for the whole scan before `load` runs, so a full-corpus (~33.6M domain)
  `scan.db` can grow to the tens-of-GB range before it's loaded into ClickHouse and can be removed.
  Follow-ups: an incremental CH flush (load staged rows periodically during `scan` instead of only at
  the end) or partitioning full-corpus runs into several `--query`-scoped `scan_id`s that are each
  loaded and cleared before the next starts.
- **Redis-backed distributed scheduler**, shard-by-server-IP (design spec §3.3) — only needed once a
  single box can't keep up; each server IP would be owned by one machine (consistent hash) so its
  limiter stays local with zero coordination, and only the per-domain accumulator would need sharing.
- **CT-log hostname discovery** (design spec §2) feeding the hostname list, replacing the static 5
  (`www, mail, webmail, smtp, autodiscover`).
- **DNSSEC chain validation** — v1 only captures DNSKEY/DS *presence*; validating the actual signature
  chain is derived later, not by this worker.
- **A dagster asset** wrapping `scan` + `load` on an every-few-days schedule, partitioned by
  `scan_id` (design spec §8) — this repo ships the standalone binary only; scheduling is a follow-up.
