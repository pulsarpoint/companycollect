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

- Default resolvers are public: `1.1.1.1:53, 8.8.8.8:53, 9.9.9.9:53` (`resolve.DefaultResolvers`).
- **For large runs, run a local `unbound`** on the scan box and pass
  `--resolvers 127.0.0.1:53 --discovery-qps 2000`. This removes any dependence on public resolvers
  and gives natural TLD-level caching (a `.com` lookup is warm after the first few thousand domains).
  Deploying `unbound` itself is out of scope for this repo — the worker already supports pointing at
  one via flags.

**Tier 2 — record queries (direct-to-authoritative).** Once a domain's authoritative NS IPs are
known, every record query for that domain goes **directly to those IPs** (`RD=0`), rotating across
them with retry. Implemented in `internal/resolve/query.go` (`Resolver.Resolve` / `queryAuth`). The
query plan (`internal/records/plan.go`) is per domain, no trailing dot needed on input:

- `A`/`AAAA` at apex (`@`) plus 5 default subdomains: `www, mail, webmail, smtp, autodiscover`.
- `MX`, `TXT` (verbatim — SPF lives here), `NS`, `SOA`, `CAA`, `DNSKEY` at apex.
- Mail/policy: `_dmarc` (DMARC), `_mta-sts`, `_smtp._tls` (TLS-RPT), `default._bimi` — all `TXT`.
- **DKIM brute force**: `TXT <selector>._domainkey.<domain>` for 10 default selectors (`default,
  google, selector1, selector2, k1, dkim, s1, s2, mail, mandrill`).
- `DS` at the parent zone, captured from Tier-1 discovery.

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

## The SQLite stage + resume contract

`scan` never writes ClickHouse directly. It stages everything in an embedded SQLite database
(`--db`, default `scan.db`, WAL mode) via `internal/store`:

- `scan_domains` is **both the work queue and the per-domain summary**: `(scan_id, root_domain)` is
  the primary key, `status` is `pending` until a domain finishes, then `done` or `error`.
- `scan_records` stages every resolved DNS record for a scan.

**Resume contract:** on `scan` start, domains are (re-)seeded from the CH query with
`INSERT OR IGNORE` — rows that already exist (from a prior, possibly crashed run) are left alone.
`Pending()` then returns only domains whose status is **not** `done`/`error`. A domain's records +
summary are written and its status flipped in **one SQLite transaction**
(`Store.CommitBatch`) — a kill mid-domain simply leaves it `pending`, so it's picked up and retried
on the next `scan` invocation with the same `--scan-id`/`--db`. A domain that already finished is
never re-resolved. This makes `scan` **idempotent and restartable**: `--limit 0` (full corpus) runs
can be killed and re-run indefinitely, converging on "0 pending" only when every domain has a
terminal status.

> **Gotcha confirmed in e2e testing:** the default `--query` (`SELECT DISTINCT root_domain FROM
> corpscout.commoncrawl_domains`) has no `ORDER BY`. Combined with `--limit N`, ClickHouse does
> **not** guarantee the same N rows across repeated executions of the same query — so re-running
> `scan --limit 20 --scan-id X` twice can seed a *different* 20 domains the second time (each new to
> the stage, so each shows up as "N new; N pending" rather than "0 pending"), even though the resume
> mechanism itself (skip-if-done) is working correctly. This only matters when `--limit` is small; a
> full-corpus run (`--limit 0`, the production case) or any `--query` with an explicit `ORDER BY`
> does not have this issue. For a reproducible bounded smoke test, pass a query with
> `ORDER BY root_domain` explicitly.

Only one goroutine ever calls `CommitBatch` (`db.SetMaxOpenConns(1)`), so SQLite's single-writer lock
is never contended; the resolver pool sends `model.DomainResult` on a channel that this one writer
batches into transactions of `--commit-batch` (default 200) domains at a time.

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
│   ├── input/      # FromClickHouse: SELECT root_domain (+ optional LIMIT)
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
| `--query` | string | `SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains` | ClickHouse query returning one `root_domain` column (see the `ORDER BY` gotcha above if you also pass `--limit`) |
| `--limit` | int | `0` (all) | cap on domains pulled from CH this invocation |
| `--resolvers` | string (CSV) | `1.1.1.1:53,8.8.8.8:53,9.9.9.9:53` | recursive resolvers for Tier-1 NS discovery; use `127.0.0.1:53` for a local unbound |
| `--discovery-qps` | float | `50` | max queries/sec **per** recursive resolver; raise substantially (e.g. `2000`) for a local unbound |
| `--per-server-qps` | float | `10` | max queries/sec **per** authoritative NS IP (Tier 2) |
| `--per-server-inflight` | int | `3` | max concurrent queries per NS IP (either tier's scheduler) |
| `--workers` | int | `4000` | max domains resolved concurrently (semaphore-bounded goroutine pool) |
| `--commit-batch` | int | `200` | domains per SQLite commit transaction |
| `--seed-chunk` | int | `5000` | domains per SQLite seed transaction (only affects the initial `INSERT OR IGNORE` pass) |
| `--query-timeout` | duration | `5s` | per-DNS-query timeout (both tiers) |

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

- **Per-server circuit breaker** — after N consecutive timeouts to a server IP, short-circuit its
  queries so a dead nameserver stops burning per-query timeout budget. v1 relies on the per-server
  limiter + bounded rotation-based retries only.
- **Deploying local `unbound` for discovery** — the worker already supports it (`--resolvers
  127.0.0.1:53 --discovery-qps 2000`); standing up `unbound` itself on the scan box is the deferred
  operational step, recommended for large/full-corpus runs to remove public-resolver dependence and
  get natural TLD caching.
- **Streaming domain input** — `input.FromClickHouse` currently materializes the full `root_domain`
  result set before seeding. Seeding into SQLite is already chunked (`--seed-chunk`); the CH *read* is
  not — for full-corpus runs (~34M rows) switch to a streaming reader that seeds in chunks without
  holding the whole list in memory.
- **Redis-backed distributed scheduler**, shard-by-server-IP (design spec §3.3) — only needed once a
  single box can't keep up; each server IP would be owned by one machine (consistent hash) so its
  limiter stays local with zero coordination, and only the per-domain accumulator would need sharing.
- **CT-log hostname discovery** (design spec §2) feeding the hostname list, replacing the static 5
  (`www, mail, webmail, smtp, autodiscover`).
- **DNSSEC chain validation** — v1 only captures DNSKEY/DS *presence*; validating the actual signature
  chain is derived later, not by this worker.
- **A dagster asset** wrapping `scan` + `load` on an every-few-days schedule, partitioned by
  `scan_id` (design spec §8) — this repo ships the standalone binary only; scheduling is a follow-up.
