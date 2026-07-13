# cc-enrich-worker

Processes selected pages directly from Common Crawl using the static WARC-oriented catalog created by
[`cc-warc-index-builder`](../cc-warc-index-builder/).

See the parent [`cc-processor` runbook](../README.md) for the complete catalog, migration, processing,
state, and deployment workflow. This README documents the worker boundary and its direct CLI.

## Input contract

Set `COMMONCRAWL_CATALOG_S3_BASE` to the RustFS bucket and catalog prefix. For example, with
`COMMONCRAWL_CATALOG_S3_BASE=s3://crawls/commoncrawl/catalogs` and selection `pages25`, the worker reads:

```text
s3://crawls/commoncrawl/catalogs/<crawl>/pages25/ready.json
s3://crawls/commoncrawl/catalogs/<crawl>/pages25/catalog.duckdb
```

RustFS is the authoritative catalog store. The worker fetches `ready.json` first and validates the
requested crawl and selection plus the committed catalog key, size, and SHA-256. It caches the complete
catalog once under the configured local base:

```text
<base>/<crawl>/warc-index/<selection>/catalog.duckdb
<base>/<crawl>/warc-index/<selection>/catalog.duckdb.sha256
```

A missing or different SHA sidecar causes a fresh download to a partial file, size and SHA verification,
and atomic cache replacement. A matching cache is reused by subsequent WARC runs. All runtime DuckDB
queries use this local read-only file; the worker does not attach directly to the RustFS object.

`--part N` means WARC index `N`. The worker loads that WARC's selected page coordinates and calculates
the compressed bytes required by the current processor:

- `tech` and `both` use every selected page;
- `industry` and `embed` use only pages whose catalog rank is `1`.

It HEADs a non-empty WARC to get its actual object size (to validate the selected ranges), then serves
each selected record with an exact object-range read. Range reads are the only fetch strategy: measured
across the real catalog every part selects ~3,500 pages / ~12% of the object's bytes, so a whole-object
download never wins. Nothing is buffered in memory and no temporary WARC file is written.

A WARC with no pages for the requested processor is a valid successful unit. After reading its catalog
entry, the worker does not initialize the Common Crawl WARC client or access the WARC object.

## Processing lifecycle

There is one lifecycle, split across two decoupled commands driven by on-disk markers:

1. **Produce + mark** — the range runner (`<cmd> --parts A-B`) produces each part's Parquet output via
   range reads and, on success, writes a `.produced` marker carrying the per-kind row counts.
2. **Load + mark** — `load --scan <root> [--watch]` sweeps that output root for `.produced` dirs that
   lack `.loaded`, inserts each into ClickHouse, verifies the row counts against the marker, and writes
   `.loaded`. The load side is independent of the producer and can run on a different host or schedule.

The `.produced`/`.loaded` marker pair is the authoritative resume + skip state: a part with `.produced`
is not reproduced, and a dir with `.loaded` is not reloaded. `embed` additionally uses its completed
vector file as an inner skip check.

A single `--part` run is retained only for **ad-hoc / debug produce**: it writes the same output layout
but does **not** write a `.produced` marker, so `load --scan` will not pick it up. Its output is for
inspection, not the load pipeline. (You could point `load --scan` at the parent dir to load it, but only
if a `.produced` marker exists — single-part runs never write one.)

See [`docs/superpowers/specs/2026-07-13-range-runner-design.md`](docs/superpowers/specs/2026-07-13-range-runner-design.md)
for the full design record.

## Range runner: process a part range

`--parts A-B` processes an inclusive WARC index range in one worker invocation.
Every part in the range with at least one selected page is produced via range reads; parts with zero
selected pages are **empty** and skipped. `industry`, `embed`, `tech`, and `both` all use the same
single strategy — there is no lane split and no `--mode`.

- At startup the runner reads the per-part catalog stats and prints `parts=<n> selected=<n> empty=<k>`.
- `--warc-parallel` (default 4) sets how many parts are produced concurrently; it also sizes the shared
  S3/HTTP transport so the parts genuinely contend for one connection budget rather than oversubscribing.
- Within a range, `.produced` markers make the run resumable: a part with an existing marker is
  skipped; an output directory with no marker (a crashed produce) is wiped and reproduced. A circuit
  breaker aborts the run after 5 CONSECUTIVE part failures (protects an unattended box from e.g. expired
  credentials silently burning hours); failed parts are logged, left unmarked, and retried on the next
  invocation. The run exits non-zero if any part failed.

Example — tech fingerprinting, parts 0-99:

```bash
./cc-enrich-worker/bin/cc-enrich-worker tech \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl-id CC-MAIN-2026-25 \
  --parts 0-99 \
  --warc-parallel 4
```

### Markers

Each part writes `.produced` as a SIBLING of its output directory once it finishes (not inside it):

```text
<base>/<crawl>/warc/<selection>/out_<cmd>_<part>/           # Parquet output
<base>/<crawl>/warc/<selection>/out_<cmd>_<part>.produced   # JSON: part, cmd, rows per kind,
                                                             # duration_s, source_run_id, finished_at
<base>/<crawl>/warc/<selection>/out_<cmd>_<part>.loaded     # written only by `load`, once loaded rows
                                                             # meet the counts recorded above
```

Both files are written via temp file + atomic rename. `.produced` is the range runner's skip/resume
unit; `.loaded` is the loader's skip unit. A part's row counts in `.produced` are keyed by the same
parquet kind names the loader maps to ClickHouse tables (`domains`, `industries`, `page_signals`,
`jsonld`, `contacts`, `tech`, `identifiers`, `security`, `page_meta`).

### `sync-db` — pre-warm the catalog cache

`sync-db` pulls the committed WARC catalog from S3/RustFS and caches it locally at
`<base>/<crawl-id>/warc-index/<selection>/catalog.duckdb`. Run it once on a freshly provisioned host to
download the (~17 GB) catalog before the first range run, so that run does not stall on the initial
sync:

```bash
./cc-enrich-worker/bin/cc-enrich-worker sync-db \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl-id CC-MAIN-2026-25
```

It reads `COMMONCRAWL_CATALOG_S3_BASE` and `CORPSCOUT_S3_*` (same config as a range run) and prints the
resolved local path once the catalog is ready. It is idempotent: the cached SHA is validated against
the commit, so a second run with an up-to-date cache re-verifies and returns without downloading. A
range run performs the same sync automatically, so `sync-db` is only needed when you want the download
to happen up front as an explicit step.

### `status` — read-only marker report

`status --root <dir>` walks a producer output root for `.produced`/`.loaded` markers and prints one
table of per-command counts:

```bash
./cc-enrich-worker/bin/cc-enrich-worker status \
  --root /opt/companycollect/corpscout/commoncrawl/data/CC-MAIN-2026-25/warc/pages25
```

```text
status: /opt/.../warc/pages25
  cmd        produced   loaded     pending    oldest pending age
  industry   120        118        2          14m3s
  tech       400        400        0          -
```

`produced` counts every `.produced` marker found for that command; `loaded` counts the subset with a
sibling `.loaded` marker; `pending` is `produced` minus `loaded` (produced but not yet loaded); "oldest
pending age" is how long the longest-waiting pending part has been sitting since it finished producing.
A bare output directory with no `.produced` marker (never produced, or a crashed produce not yet
retried) is not counted at all. `status` never writes anything and never connects to ClickHouse.

## Loader deployment

`load --scan <root> [--watch] [--parallel K]` is the decoupled counterpart to the range runner: it walks
`<root>` for `.produced` markers lacking a sibling `.loaded`, loads every fixed-name Parquet file in each
pending output directory into ClickHouse via the existing native-driver kind mapping, verifies the
loaded row counts against the counts recorded in `.produced`, and writes `.loaded` on success (temp file
+ atomic rename, so a crash mid-write never leaves a corrupt marker). A row-count shortfall or a
ClickHouse error leaves the dir pending; the next sweep retries it.

```bash
# One sweep, then exit — cron-friendly.
./cc-enrich-worker/bin/cc-enrich-worker load --scan /opt/.../warc/pages25 --parallel 4

# Stay running: sweep, then wait for a filesystem event under root or a 5-minute tick, and sweep again.
./cc-enrich-worker/bin/cc-enrich-worker load --scan /opt/.../warc/pages25 --parallel 4 --watch
```

`--watch` uses fsnotify (inotify/kqueue) on marker creation for near-instant pickup, PLUS an
unconditional 5-minute fallback sweep, because inotify events can be dropped on queue overflow and never
arrive for markers that appear via rsync/NFS from a separate producer machine — the periodic sweep is the
correctness mechanism, fsnotify is only the latency optimization. If the watcher cannot be set up (for
example the root does not support inotify) `load` logs one warning and degrades to pure 5-minute polling.

The loader never runs on the producer's critical path: producers write only Parquet and `.produced`
markers and have no ClickHouse dependency (`tech`/`embed` need none at all; `industry`/`both` read the
NACE reference once at startup as a fail-fast setup check, not per part). Run `load --scan --watch` as a
long-lived process wherever ClickHouse is reachable — it can lag behind the producers and catch up later,
and restarting it after a ClickHouse outage resumes cleanly from whatever is still pending.

## Operating model

There is one producer role — a range runner over `--parts` — and one loader role running
`load --scan --watch` wherever ClickHouse is reachable, decoupled from the producer (possibly colocated
with it or with ClickHouse itself). A second producer host running a different part sub-range is a
legitimate way to scale the aggregate Common Crawl S3 request budget across more source IPs.

Nothing in the code enforces any particular split. Every producer writes to the same `OUT_BASE_DIR`
layout and marker vocabulary (`out_<cmd>_<part>` plus its `.produced` / `.loaded` siblings), so
`load --scan` and `status` see one uniform tree regardless of which host produced a given part — point
them at the shared root (over a shared filesystem, or after rsync from each producer host) and they work
the same either way.

## JSON-LD output

Tech processing writes `jsonld.parquet`, loaded into `corpscout.commoncrawl_page_jsonld`. Every JSON-LD
object carrying `@type` or `@id` is a separate row, including nested publisher, author, provider, and
organization objects. Rows retain the page URL, WARC coordinates, zero-based script index, RFC 6901
entity path, sorted types, commonly queried fields, and canonical raw JSON.

No entity is selected as the page's organization and fields from different entities are never merged.
The source record coordinates plus script index and entity path make reprocessing idempotent. The former
single-profile `metadata.parquet` output is not produced or loaded.

## Build and run

From `commoncrawl/cc-processor/`:

```bash
make -C cc-enrich-worker
make -C cc-enrich-worker test
make -C cc-enrich-worker vet
```

The normal entry point is the range runner producing a part range, with `load --scan` loading it:

```bash
set -a; source .env; set +a

./cc-enrich-worker/bin/cc-enrich-worker tech \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl-id CC-MAIN-2026-25 \
  --selection pages25 \
  --parts 0-10 \
  --concurrency 32 \
  --chunk 16384 \
  --warc-parallel 8

./cc-enrich-worker/bin/cc-enrich-worker load \
  --scan /opt/companycollect/corpscout/commoncrawl/data/CC-MAIN-2026-25/warc/pages25 --parallel 4
```

See "Range runner: process a part range" and "Loader deployment" above for the `--parts` and
`load --scan --watch` forms (run the loader with `--watch` to load parts as they become available).

For ad-hoc / debug produce of a single part, swap `--parts 0-10` for `--part 0`. A single-part run
writes the same output layout but no `.produced` marker, so `load --scan` will not pick it up — its
output is for inspection only.

`.env` exists only at the processor root. When invoking the worker from its component directory,
source `../.env`.

Use `--s3-anonymous` off AWS to read through `https://data.commoncrawl.org/`. Signed S3 is the default
and is preferred on EC2.

## Produce flags

| Flag | Default | Meaning |
|---|---|---|
| `--base` | required | Local output and catalog-cache root — always passed explicitly (no environment fallback). |
| `--crawl-id` | required | Crawl identity, for example `CC-MAIN-2026-25`. |
| `--selection` | `pages25` | Catalog selection directory. |
| `--part` | required | Zero-based WARC index. |
| `--s3-anonymous` | `false` | Use anonymous HTTPS instead of signed S3. |
| `--out` | derived | Defaults to `<base>/<crawl>/warc/<selection>/out_<mode>_<part>`. |
| `--concurrency` | `32` | Industry/embed pages or tech/both domains in flight. |
| `--chunk` | `1024` | Catalog pages per tech/both processing chunk. |
| `--tech-engine` | `fast` | Technology matcher used by tech/both. |
| `--tech-max-bytes` | `0` | Cap on page bytes scanned for technologies; `0` scans the complete page. |

Industry/embed/both also accept `--embed-batch` and `--embed-concurrency`. Tech/both also accept
`--tech-engine` and `--tech-max-bytes`. Technology detection scans the complete page by default.

## Range runner flags

`--parts` is mutually exclusive with `--part` (and with `--out`, which only applies to a single-part
run); it activates the flags below. `--base`, `--crawl-id`, `--selection`, `--s3-anonymous`,
`--concurrency`, `--chunk`, `--tech-engine`, and `--tech-max-bytes` are shared with the single-part
flags above and behave identically per part.

| Flag | Default | Meaning |
|---|---|---|
| `--parts` | required for a range run | WARC index range `"A-B"` (inclusive) or a single `"N"`. |
| `--warc-parallel` | `4` | Parts produced concurrently (>=1); also sizes the shared transport budget. |

## Environment

| Variable | Meaning |
|---|---|
| `COMMONCRAWL_CATALOG_S3_BASE` | Required authoritative catalog location in `s3://bucket/prefix` form. |
| `CORPSCOUT_S3_ENDPOINT` | Required RustFS S3-compatible endpoint. |
| `CORPSCOUT_S3_REGION` | RustFS signing region; defaults to `us-east-1`. |
| `CORPSCOUT_S3_ACCESS_KEY`, `CORPSCOUT_S3_SECRET_KEY` | Required RustFS catalog credentials. |
| `AWS_REGION` | Signed Common Crawl S3 region; defaults to `us-east-1`. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Optional explicit credentials; EC2 instance roles are supported. |
| `CC_BASE_URL` | Anonymous HTTP base; defaults to `https://data.commoncrawl.org/`. |
| `COMMONCRAWL_EMBED_*` | Industry/embed/both endpoint and model settings. |
| `CLICKHOUSE_*` | Industry/both reference reads and `load` destination. |

The `WARC input ready` event reports the selected and object sizes, utilization, chosen mode, preparation
time, and whole-download throughput. Signed-S3 runs additionally report throttling and body-retry counters.
