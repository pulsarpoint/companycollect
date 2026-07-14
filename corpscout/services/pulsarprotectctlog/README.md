# pulsarprotectctlog

Standalone service that downloads Certificate Transparency (CT) log entries,
parses certificate metadata, and stores it in **ClickHouse**. Two goals:

1. **Track certificate expirations** — full metadata for relevant certs.
2. **Collect subdomains** harvested from cert SANs (the primary, cheap dataset).

- **Architecture / developer overview:** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- Design & decisions: [`docs/superpowers/plans/2026-06-26-shard-oriented-ct-processing.md`](docs/superpowers/plans/2026-06-26-shard-oriented-ct-processing.md)

## Vocabulary (native CT terms)

- **source** — a configured provider: one operator + a log-name prefix
  (`le-sycamore` = Let's Encrypt / Sycamore). Configured in `sources.json`.
- **ctlog** — one CT log = one append-only Merkle tree (a temporal shard, e.g.
  `sycamore-2025h2d`). The unit we enumerate, drain, and track. Identified by a
  **friendly id**; canonical **LogID** kept in metadata.
- **checkpoint** — a ctlog's signed tree head `(size, root)`; polled to read the
  current **head** (entry count). A ctlog emits many checkpoints over its life.
- **leaf** — one certificate (precert/cert) at index `0…head-1`. **Tiles** serve
  256 leaves each (static-ct-api).

## Source types

Two interchangeable backends behind `internal/source.Source`, chosen per source
by its `type`:

- **tiled** (preferred) — static-ct-api logs (Let's Encrypt Sunlight, Cloudflare
  Azul, …). Fetches **256-entry data tiles** over a CDN, in parallel; fast.
  Coverage starts ~mid-2025 (e.g. Sycamore `2025h2d` ≈ Aug 2025).
- **rfc6962** — classic `get-entries` logs (Google Argon/Xenon, …). Slow (~14
  entries/request) but covers windows older than tiled logs.

The parser, retention rule, ClickHouse writer, and control plane are shared.

## How it works

- **Unit of work = one ctlog**, drained `0…head` (no date windows). For an active
  ctlog you re-read only `[cursor, head)` each cycle.
- **Tiles**: leaves are fetched as 256-entry data tiles (immutable, CDN-cached);
  only the trailing partial tile is mutable, and ReplacingMergeTree absorbs that
  overlap on re-read.
- **Retention rule** (`internal/retention`): hostnames are always written; full
  cert metadata is stored unless the cert is **both expired and issued >1 year
  ago**.
- **Storage** (ClickHouse, `internal/store/clickhouse`): `certs` — a **windowed**
  per-cert table (deduped by `(issuer_ca_id, serial_number)`, TTL'd to 90 days
  past expiry) for expiration tracking + on-demand lookups; and `hostnames` — the
  **permanent, distinct** query surface (`AggregatingMergeTree`, one row per
  `(registered_domain, fqdn)` with `first_seen`/`last_seen`). Metadata-only — no
  raw DER.
- **Control plane** (`internal/store/control`): local SQLite in **WAL mode** with
  `busy_timeout`, per-ctlog cursor + status for idempotent resume. Multiple
  concurrent `process` runs on one host can safely share the same control DB file
  without `database is locked` errors. No Postgres.

## ctlog lifecycle (drain vs tail)

ctlogs are **temporally sharded by cert expiry** (`temporal_interval`):

- **frozen** (`now ≥ end_exclusive`): sealed, immutable, complete → **drain once**
  (`process`), mark done, never touch again.
- **active** (`now < end_exclusive`): still accepting submissions, grows
  continuously → **tail the delta** (`process --watch`): each cycle fetch the
  checkpoint head and ingest only `[cursor, head)`. Never re-pull the whole ctlog.
- **retired / unreachable** (checkpoint `AccessDenied`/404): a real ctlog whose
  tiles are no longer served (e.g. old sub-shards). `list` shows `reach=no`; it's
  a coverage gap recovered via other operators + dedup.

## sources.json

Lists the sources to process. Example (repo root):

```json
[
  {"name": "le-sycamore", "type": "tiled", "operator": "Let's Encrypt", "log_prefix": "Sycamore"},
  {"name": "le-willow",   "type": "tiled", "operator": "Let's Encrypt", "log_prefix": "Willow"}
]
```

`operator` matches the log-list operator exactly; `log_prefix` is a substring of
the ctlog description.

## Usage

```bash
make build                      # -> bin/ctlog   (see `make help` for all targets)

# LIST: a source's ctlogs with metadata + processing state (table or JSON)
bin/ctlog list --source le-sycamore
bin/ctlog list --source le-sycamore --json
bin/ctlog list                  # all sources

# PROCESS (orchestrator): drain all reachable frozen ctlogs in a source
# Skips unreachable / retired ctlogs; processes every other frozen ctlog once.
bin/ctlog process --source le-sycamore                                          # drain all reachable frozen ctlogs
bin/ctlog process --source le-sycamore --dry-run --limit 500                    # iterate + validate, no writes

# PROCESS (single ctlog): drain or tail one ctlog
bin/ctlog process --source le-sycamore --ctlog sycamore-2025h2d                # frozen: one pass
bin/ctlog process --source le-sycamore --ctlog sycamore-2026h2 --watch         # active: tail the delta;
                                                                                #   auto-finalizes and exits once
                                                                                #   the ctlog's expiry window passes
bin/ctlog process --source le-sycamore --ctlog sycamore-2025h2d --dry-run --limit 2000   # validate, no writes
```

`list --json` emits one object per ctlog with snake_case keys: `id`,
`description`, `log_id`, `type`, `source`, `monitoring_url`, `submission_url`,
`state`, `mmd`, `start`, `end`, `phase`, `reachable`, `head`, `tracked`,
`status`, `cursor`, `certs_written`, `sans_written`, `percent_done`.

## Configuration (env; `.env` in the working dir is auto-loaded)

| Var | Default | Purpose |
|---|---|---|
| `CTLOG_SOURCES_FILE` | `./sources.json` | sources definition |
| `CTLOG_SHARD_LIST_URL` | gstatic `all_logs_list.json` | log list (all states incl. frozen/retired) |
| `CTLOG_FETCH_PARALLEL` | 4 | concurrent data-tile fetches (`.env` sets 48) |
| `CTLOG_BATCH_SIZE` | 256 | rfc6962 get-entries request size |
| `CTLOG_HTTP_TIMEOUT` | 30s | HTTP client timeout |
| `CTLOG_MAX_RETRIES` | 5 | fetch retry attempts |
| `CTLOG_WRITE_BATCH_SIZE` | 5000 | ClickHouse flush threshold |
| `CTLOG_CONTROL_DB_PATH` | `./data/ctlog-control.db` | SQLite cursor store |
| `CTLOG_CLICKHOUSE_ADDR` | localhost:9000 | ClickHouse native address |
| `CTLOG_CLICKHOUSE_DATABASE` | ctlog | target database (`.env` sets `ctlogs`) |
| `CTLOG_CLICKHOUSE_USER` / `_PASSWORD` | default / – | credentials |
| `CTLOG_CLICKHOUSE_STORAGE_POLICY` | – | optional storage policy (disk isolation) |

## ClickHouse target

Uses the **same ClickHouse server as companycollect/corpscout**
(`companycollect:9002`, user `default`), in a **separate `ctlogs` database**.
Connection lives in `.env` (auto-loaded; gitignored). HTTP interface for ad-hoc
queries is `companycollect:8123`.

## Schema migrations

The ClickHouse schema is managed with **golang-migrate in Docker** (same
convention as corpscout), with versioned pairs in `clickhouse/migrations/`
(`NNNNNN_name.{up,down}.sql`). `EnsureSchema` in the app still creates the same
tables on first run for a fresh dev database, but migrations are the source of
truth for transitions on an existing deployment.

```bash
make clickhouse-migrate-up        # apply all pending migrations
make clickhouse-migrate-down      # roll back the last migration
make clickhouse-migrate-version   # show the current applied version
make clickhouse-migrate-force VERSION=1   # clear the dirty flag after a failed run
```

The connection URL is built from the `CTLOG_CLICKHOUSE_*` values in `.env`; set
`CLICKHOUSE_MIGRATE_URL` to override it directly (e.g. if the password needs
URL-encoding). The migrate container reaches the server via
`--add-host companycollect:$(COMPANYCOLLECT_HOST_IP)` (default the known lab IP).

Notes for authors:
- migrations run under `x-multi-statement=true`, which splits files on `;`
  **blind to comments** — keep semicolons out of comment text and string
  literals, and terminate every statement with `;`.
- `000001` doubles as the distinct-hostname transition: it collapses the old
  per-cert `cert_sans` firehose into the deduped `hostnames` store (backfilling
  `first_seen`/`last_seen`) and rebuilds `certs` with expiry-month partitions +
  a 90-day TTL. It is safe on both a fresh and an existing (pilot) database.
- `000002` adds `hostnames.last_ingested_at`. Historical rows use the Unix epoch
  sentinel; the writer assigns real UTC ingestion timestamps after deployment.

## Deploy & run as a service

Ansible is the only supported deployment path. It installs the versioned Linux
binary, shared state and configuration, and the systemd oneshot and timer for
each source. See [`ansible/README.md`](ansible/README.md) for configuration,
cutover, rollback, and verification instructions.

```bash
make build-linux
cd ansible
ansible-playbook site.yml --check --diff
ansible-playbook site.yml
```

Each `process --source` run is idempotent and resumable. The first run performs
the long backfill; later timer runs pull only the delta, and interrupted work
resumes from the persisted SQLite cursor.

```bash
# operate / observe on the server
journalctl -u ctlog@le-sycamore -f          # live logs
systemctl list-timers 'ctlog@*'             # next scheduled runs
systemctl start ctlog@le-sycamore.service   # trigger an out-of-band drain now
```

The control plane is local SQLite, so "what's processed" is authoritative per
host and survives versioned binary upgrades and rollbacks.

> Apply pending ClickHouse migrations (`make clickhouse-migrate-up`, run from a
> Docker-capable host against `companycollect`) **before** deploying or enabling
> the service so the writer and database schema remain compatible.

## Status

- ✅ Per-source, per-ctlog model verified live: `list` (table + JSON) enumerates
  a source's ctlogs with metadata + processing state; retired sub-shards show
  `reach=no`; `process --dry-run` drains correctly. Tiled + rfc6962 both work.
- ✅ Parallel tile fetch (~10–12k entries/s measured); one full month (~7M
  entries) ingested in ~12 min, 0 parse errors.
- ✅ Real data-tile fixture test in `internal/parse/testdata`.
- ✅ `process --source NAME` (no `--ctlog`) orchestrator mode: iterates all
  ctlogs in a source, drains every reachable frozen one, skips unreachable /
  retired ctlogs automatically.
- ✅ `--watch` auto-finalizes: once a ctlog's expiry window has passed, the
  watcher marks it done and exits cleanly instead of running forever.
- ✅ Control DB runs in WAL mode + `busy_timeout`; multiple concurrent `process`
  invocations on one host share one SQLite file without lock contention.

## Known follow-ups

- **Multi-worker control plane:** local SQLite is single-host. To run multiple
  workers draining ctlogs in parallel across **multiple hosts**, promote the
  control plane to a shared store (ClickHouse/Postgres with `FOR UPDATE SKIP
  LOCKED`) for ctlog claims + cursors.
- **`process --source --watch` concurrent orchestration:** tail all active
  ctlogs in a source concurrently and periodically re-enumerate the log list for
  new sub-shards.
- **Drain progress logging:** a single drain logs only on completion.

## TODO: "be sure we see every cert" mode (complete coverage)

A single source catches *most* certs but not *all*. To **guarantee** we see every
publicly-trusted, CT-logged certificate:

1. **Fetch the merged Chrome + Apple CT log lists** (authoritative ctlog set).
2. **Filter** to ctlogs whose `temporal_interval` overlaps the target window and
   whose state is usable / readonly / qualified.
3. **Process every ctlog in that set** (tiled where available, rfc6962 where not).
   Dedup by `(issuer, serial)` folds cross-log overlap into one row — "ingest
   everything" is safe and storage-bounded; only fetch time grows.
4. **Cross-check** unique-cert counts against a third party (crt.sh / Censys).

Caveats:
- **Hard limit:** CT only contains publicly-trusted, CT-submitted certs. Private
  / internal CAs are invisible to any CT-based approach.
- **Policy optimization:** every trusted cert carries SCTs from ≥2 distinct
  operators, so you may skip **at most one entire operator** and still be
  guaranteed coverage. Skipping two breaks the guarantee.
- **Tiled-only isn't enough today:** some operators (notably Google) are still
  rfc6962, so complete coverage means tiled + a few rfc6962 ctlogs (unless the
  one operator you skip is the rfc6962 one).

## Pilot measurement (ClickHouse)

```sql
SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disk, sum(rows) AS rows
FROM system.parts WHERE database='ctlogs' AND active GROUP BY table;

-- per-operator coverage contribution (which logs surfaced each hostname)
SELECT arrayJoin(groupUniqArrayArray(source_logs)) AS log, uniqExact(fqdn)
FROM ctlogs.hostnames GROUP BY log;

-- distinct subdomains for a domain (fold unmerged parts with GROUP BY)
SELECT fqdn, min(first_seen) AS first_seen, max(last_seen) AS last_seen
FROM ctlogs.hostnames WHERE registered_domain = 'example.com'
GROUP BY registered_domain, fqdn;

-- certs expiring in 30 days (within the TTL window)
SELECT count() FROM ctlogs.certs WHERE not_after BETWEEN now() AND now()+INTERVAL 30 DAY;
```
