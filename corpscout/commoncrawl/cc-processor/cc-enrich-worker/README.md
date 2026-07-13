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

It HEADs a non-empty WARC to get its actual object size. When
`selected_bytes / object_bytes * 100` is at least `--whole-warc-threshold`, the complete WARC is streamed
once to a temporary local file and indexed records are served with concurrent `ReadAt` calls. Below the
threshold, the existing exact object-range reader is used. The complete WARC is never buffered in memory and
the temporary file is removed when processing finishes or input preparation fails.

A WARC with no pages for the requested processor is a valid successful unit. After reading its catalog
entry, the worker does not initialize the Common Crawl WARC client or access the WARC object.

## Processing lifecycle

The `cc-crawl` produce/load lifecycle remains unchanged for `industry` and `tech`:

1. The processor produces local Parquet files.
2. `load --dir ...` inserts supported files into ClickHouse.
3. `cc-crawl` writes `out_<mode>_<warc-index>.loaded` only after loading succeeds.

The local `.loaded` file remains the authoritative skip check. Produce never marks a WARC as loaded.
`embed` uses its completed vector file, and direct-only `both` has no orchestrated marker.

`industry`, `tech`, `embed`, and `both` also support the range runner described below, which processes
a whole WARC part range in one worker process instead of one `--part` at a time under `cc-crawl`. Both
lifecycles write the same output layout and are interchangeable per part; see
[`docs/superpowers/specs/2026-07-13-range-runner-design.md`](docs/superpowers/specs/2026-07-13-range-runner-design.md)
for the full design record.

## Range runner: part ranges across two lanes

`--parts A-B --mode local|remote` processes an inclusive WARC index range in one worker invocation,
without `cc-crawl`. `--mode` is required — there is no default and no `all`; it selects both what the
runner claims from the catalog and how it fetches those parts:

- At startup the runner classifies parts `A`–`B` by selected page count against
  `--remote-max-pages X`: parts with `<= X` selected pages are **remote**-eligible (small enough that
  per-page S3 range reads beat downloading the whole object); parts with `> X` are **local** (large
  enough that one whole-WARC download followed by local reads is cheaper in requests). Parts with zero
  selected pages are **empty** and skipped.
- `tech` and `both` cover a full range by running BOTH lanes — normally as two separate processes, one
  per lane (see "Two-server operating model" below); each lane only takes the parts of its own class.
- `industry` and `embed` accept only `--mode remote`: their primary-pages-only selections are sparse,
  so there is no local lane for them. The remote runner takes every non-empty part in the range with no
  classification filtering; `--mode local` is rejected with an error.
- Within a range, `.produced` markers make the run resumable: a part with an existing marker is
  skipped; an output directory with no marker (a crashed produce) is wiped and reproduced. A circuit
  breaker aborts the run after 5 CONSECUTIVE part failures (protects an unattended box from e.g. expired
  credentials silently burning hours); failed parts are logged, left unmarked, and retried on the next
  invocation. The run exits non-zero if any part failed.

Example — remote lane, tech fingerprinting, parts 0-99:

```bash
./cc-enrich-worker/bin/cc-enrich-worker tech \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl-id CC-MAIN-2026-25 \
  --parts 0-99 --mode remote \
  --remote-max-pages 1000 \
  --warc-parallel 4
```

Example — local lane, same crawl, the large parts the remote lane above skipped:

```bash
./cc-enrich-worker/bin/cc-enrich-worker tech \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl-id CC-MAIN-2026-25 \
  --parts 0-99 --mode local \
  --remote-max-pages 1000 \
  --download-parallel 2 --process-parallel 2 --max-warc-files 5
```

Both lane runs must be given the SAME `--remote-max-pages` value — that is what makes the two classes a
disjoint, complete partition of the range. Each run prints its `X` and class counts at startup so a
mismatch between the two lane invocations is visible immediately.

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

### `plan` — read-only lane report

`plan` classifies a part range at a given `--remote-max-pages` threshold and reports the split, without
touching Parquet, markers, or ClickHouse — useful for picking a threshold before committing a range to
either lane:

```bash
./cc-enrich-worker/bin/cc-enrich-worker plan \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl-id CC-MAIN-2026-25 \
  --parts 0-99 \
  --remote-max-pages 1000
```

It prints local/remote/empty part counts, selected page and byte totals per lane, an estimated local-lane
download volume (parts x ~1 GiB), and a threshold sweep across `X` in `{100, 500, 1000, 2500, 5000,
10000}` (plus whatever `X` was passed) so the split can be tuned from the range's real page-count
distribution.

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

## Two-server operating model

The local and remote lanes are intended to run on DIFFERENT servers, and the loader on a third role
(possibly colocated with either, or with ClickHouse itself):

- **Remote-lane host**: S3-request-rate heavy, modest CPU and disk — runs `--mode remote` for the small
  parts.
- **Local-lane host**: disk-write and CPU heavy — runs `--mode local`, sustaining `--download-parallel`
  concurrent ~1 GiB downloads bounded by `--max-warc-files`, then processing them locally.
- **Loader host**: runs `load --scan --watch` wherever ClickHouse is reachable, decoupled from both
  producers.

Nothing in the code enforces this split — it is the documented operating model, chosen because it keeps
each box's bottleneck singular and the Common Crawl S3 request budget accountable per host. Both lanes
write to the same `OUT_BASE_DIR` layout and marker vocabulary (`out_<cmd>_<part>` plus its `.produced` /
`.loaded` siblings), so `load --scan` and `status` see one uniform tree regardless of which host produced
a given part — point them at the shared root (over a shared filesystem, or after rsync from each
producer host) and they work the same either way.

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

The normal entry point is `cc-crawl`:

```bash
./cc-crawl/bin/cc-crawl \
  -base /opt/companycollect/corpscout/commoncrawl/data \
  -crawl CC-MAIN-2026-25 \
  -mode tech \
  -parts 0-10 \
  -tech-conc 32 \
  -whole-warc-threshold 50
```

For a direct produce/load run:

```bash
set -a; source .env; set +a

./cc-enrich-worker/bin/cc-enrich-worker tech \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl-id CC-MAIN-2026-25 \
  --selection pages25 \
  --part 0 \
  --whole-warc-threshold 50 \
  --concurrency 32 \
  --chunk 16384

./cc-enrich-worker/bin/cc-enrich-worker load \
  --dir /opt/companycollect/corpscout/commoncrawl/data/CC-MAIN-2026-25/warc/pages25/out_tech_0
```

Or, to process a whole range natively (no `cc-crawl`) and load it as it becomes available, see
"Range runner: part ranges across two lanes" and "Loader deployment" above for the `--parts --mode
local|remote` and `load --scan --watch` forms.

`.env` exists only at the processor root. `cc-crawl` resolves it relative to its own binary, with the
working directory as a fallback, and passes it to the worker. If invoking the worker from its component
directory instead, source `../.env`.

Use `--s3-anonymous` off AWS to read through `https://data.commoncrawl.org/`. Signed S3 is the default
and is preferred on EC2.

## Produce flags

| Flag | Default | Meaning |
|---|---|---|
| `--base` | required | Local output and catalog-cache root — always passed explicitly (no environment fallback). |
| `--crawl-id` | required | Crawl identity, for example `CC-MAIN-2026-25`. |
| `--selection` | `pages25` | Catalog selection directory. |
| `--part` | required | Zero-based WARC index. |
| `--whole-warc-threshold` | `50` | Selected compressed-byte percentage that switches to one whole-object download. |
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
run); it activates the flags below. `--base`, `--crawl-id`, `--selection`, `--whole-warc-threshold`,
`--s3-anonymous`, `--concurrency`, `--chunk`, `--tech-engine`, and `--tech-max-bytes` are shared with the
single-part flags above and behave identically per part.

| Flag | Default | Meaning |
|---|---|---|
| `--parts` | required for a range run | WARC index range `"A-B"` (inclusive) or a single `"N"`. |
| `--mode` | required with `--parts` | `local` or `remote`; no default. `industry`/`embed` accept only `remote`. |
| `--remote-max-pages` | required (>=1) for tech/both | Split threshold `X`: parts with `<= X` selected pages are remote-eligible, `> X` are local. |
| `--warc-parallel` | `4` | Remote lane: parts produced concurrently. |
| `--download-parallel` | `2` | Local lane: whole-WARC downloads in flight; must be `<= --max-warc-files`. |
| `--process-parallel` | `2` | Local lane: downloaded WARCs processed concurrently. |
| `--max-warc-files` | required (>=1) for `--mode local`, no default (recommended 5) | Local lane: max whole WARC files on disk (in-flight downloads plus downloaded-but-unprocessed) at once — the disk-usage bound. |

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
