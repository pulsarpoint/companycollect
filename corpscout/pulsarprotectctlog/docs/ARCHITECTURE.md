# Architecture

Developer-facing overview of `pulsarprotectctlog`. For usage/config see the
[README](../README.md); for godoc, run `go doc ./internal/<pkg>`.

## What it does

`pulsarprotectctlog` downloads the leaf certificates of Certificate Transparency
(CT) logs, extracts per-certificate metadata and the hostnames (SANs), and
stores them in ClickHouse — to **track certificate expirations** and **collect
subdomains**. It processes one **ctlog** (one CT log = one append-only Merkle
tree, a temporal shard such as `sycamore-2025h2d`) at a time, draining its
leaves `0…head`. It supports both **tiled** (static-ct-api) and **RFC 6962**
logs, resumes from a persisted cursor, and deduplicates across logs.

## Data flow

```
                 log list (Chrome all_logs_list.json)
                         │  loglist.CTLogs(source)
                         ▼
   sources.json ──► [ ctlog: monitoring_url, interval, state ]
                         │
      ┌──────────────────┴───────────────────┐
      │ source.Source (per ctlog)            │
      │   tiled  → tileclient  (data tiles)  │   fetch + parse
      │   rfc6962 → ctclient   (get-entries) │   → []model.CertMeta
      └──────────────────┬───────────────────┘
                         │  ingest.Ingester.Run(WorkUnit{0..head})
                         ▼
      retention.KeepMetadata / HostnameRows   (classify per cert)
                         │
        ┌────────────────┴─────────────────┐
        ▼                                   ▼
  clickhouse.WriteCerts               clickhouse.WriteHostnames   control.SaveProgress
  (certs, kept only)                  (hostnames, always)         (SQLite cursor)
        │                                   │                          │
        ▼                                   ▼                          ▼
     ClickHouse `ctlogs.certs`      `ctlogs.hostnames`        `data/ctlog-control.db`
        └──────────── DATA PLANE ───────────┘                 └──── CONTROL PLANE ────┘
```

## Two planes

- **Data plane — ClickHouse** (`internal/store/clickhouse`). Two tables, whose
  DDL is mirrored by the golang-migrate scripts in `clickhouse/migrations/`
  (`EnsureSchema` creates them for a fresh dev DB; migrations own transitions —
  see the README):
  - `certs` — `ReplacingMergeTree`, per-certificate metadata deduped by
    `(issuer_ca_id, serial_number)` (collapses precert/final-cert pairs and
    cross-log copies). A **windowed** store: partitioned by expiry month and
    TTL'd to 90 days past `not_after`, so whole partitions drop as certs age
    out. Written only for certs the retention rule keeps. Serves cert-expiration
    tracking and on-demand cert lookups (the `sans Array(String)` column + bloom
    filter answers "which cert covered this fqdn" within the window).
  - `hostnames` — `AggregatingMergeTree`, the **permanent, distinct** query
    surface. One row per `(registered_domain, fqdn)`; repeated observations
    collapse under merge into `min(first_seen)`/`max(last_seen)` via
    `SimpleAggregateFunction` columns (idempotent under at-least-once re-drain).
    `max(last_ingested_at)` records the latest time the CT writer observed the
    hostname, independently of its SCT event time. Rows predating that watermark
    use the Unix epoch sentinel so downstream consumers can bootstrap them once.
    No per-cert identity (no serial), so it is bounded by the count of distinct
    hostnames rather than inflated by renewals. Partitioned by a stable hash of
    `registered_domain` so a name's observations co-locate (required for
    merge-dedup; a bonus for `registered_domain=` pruning). Reads fold unmerged
    parts with `GROUP BY` (or `FINAL`).
- **Control plane — local SQLite** (`internal/store/control`). One `work_units`
  row per ctlog (keyed by `source/ctlog-id`) holding the resume cursor
  (`next_index`), status, and counters. WAL + `busy_timeout` let multiple
  `process` runs share one file on a host. Deliberately not Postgres — see the
  README follow-ups for the multi-host story.

## Package map (`internal/`)

| Package | Responsibility |
|---|---|
| `config` | Load configuration from env (`caarlos0/env`) + auto-load `.env`. |
| `loglist` | Read the CT log list; enumerate a **source**'s ctlogs (`CTLog`, both `tiled_logs[]` and `logs[]`); probe reachability/head; `CTLogStatus` for `list`. |
| `model` | Core domain types: `CertMeta`, `HostnameRow`, `WorkUnit`, `EntryType`. |
| `tileclient` | static-ct-api transport: `/checkpoint` (head) and `/tile/data/<n>` (256-leaf data tiles), with the `tlog-tiles` path encoding. |
| `ctclient` | RFC 6962 transport: get-sth + get-entries with retry. |
| `parse` | Decode a leaf (tile `TileLeaf`/`DataTile` or RFC6962 `Entry`) → `CertMeta`. Tolerates unparseable-cert leaves; returns partial on a framing break. |
| `source` | The `Source` interface (`Name`, `TreeSize`, `FetchRange`) with two impls: `Tile` (parallel tile fetch) and `RFC6962`. Fetch+parse into `CertMeta`. |
| `retention` | `KeepMetadata` rule + `HostnameRows` derivation (eTLD+1 via publicsuffix; CN hostname hygiene). |
| `ingest` | `Ingester.Run` — the drain loop: `FetchRange` → classify → batched writes → cursor save; resume, cancel handling, `--limit`, dry-run. |
| `store/clickhouse` | Schema DDL + batched writers. |
| `store/control` | SQLite work-unit cursor store. |

`cmd/ctlog` is the CLI: subcommand dispatch (`list`, `process`), the
`process --source` orchestrator, source construction, and helpers
(`workUnitID`, `deriveCTLogID`, `tunedHTTPClient`).

## Key design decisions

- **Unit of work = one ctlog, drained `0…head`.** No date windows; the CT API
  is index-ordered, and each ctlog is a bounded, resumable job. Frozen ctlogs
  (window past) drain once and are marked done; active ctlogs are tailed
  (`--watch`) by re-reading only `[cursor, head)` each cycle until they freeze.
- **Idempotent resume.** The cursor is persisted only at flush, so a crash
  re-fetches from the last durable point; `ReplacingMergeTree` collapses any
  re-written rows. Writes across ClickHouse + SQLite are not atomic but are
  recovered by replay.
- **Dedup by `(issuer, serial)`** means pulling multiple logs (or both a
  precert and its final cert) never double-counts — the basis for multi-source
  coverage.
- **Resilience.** A single unparseable-cert leaf is skipped and counted (not
  fatal); only a TLS **framing** break stops a tile, and the source logs it and
  moves to the next tile — one bad leaf can't wedge a 150M-entry drain.
- **Parallelism where it pays.** Tile fetch is RTT-bound, so `source.Tile`
  fetches up to `FetchParallel` tiles per `FetchRange` concurrently; `list`
  probes ctlog heads concurrently. Parsing and ClickHouse writes are not the
  bottleneck.

## A drain, step by step (`process --source le-sycamore --ctlog sycamore-2025h2d`)

1. `config.Load` → `loglist.LoadSources` → `Find("le-sycamore")`.
2. `loglist.CTLogs` resolves the source's ctlogs; match `--ctlog` id.
3. `buildSource` → `source.NewTile(tileclient…)`.
4. Open ClickHouse (`EnsureSchema`) + control DB.
5. `drainCTLog`: `TreeSize` → head; `EnsureWorkUnit` (resume cursor); loop
   `FetchRange(cursor,head)` → `KeepMetadata`/`HostnameRows` → buffer → flush
   (`WriteCerts`/`WriteHostnames`/`SaveProgress`) at `WriteBatchSize`.
6. On completion `MarkDone` (frozen) or keep resumable (active/`--watch`).

## Extending

- **New source protocol:** implement `source.Source` (`Name`, `TreeSize`,
  `FetchRange`) and construct it in `cmd/ctlog/buildSource` based on the
  ctlog's `Type`.
- **New source (operator/log family):** add an entry to `sources.json` — no
  code change.
- **New stored field:** add to `model.CertMeta`, the ClickHouse DDL
  (`schema.go`) + writer column list (`writer.go`), and populate in
  `parse.BuildMeta`.
