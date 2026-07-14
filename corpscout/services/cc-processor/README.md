# Common Crawl processor

`cc-processor` builds one selected-page catalog for a Common Crawl release and processes that catalog by
WARC object. The catalog builder, orchestrator, enrichment worker, and shared WARC reader live together so
their selection, storage, and processing contracts can be changed and tested as one system.

The normal production flow is:

```text
official Common Crawl Parquet URL index
    -> cc-warc-index-builder on wappalyzer
    -> catalog.duckdb + ready.json on RustFS
    -> cc-enrich-worker range runner on commoncrawl2 (range reads or one whole-WARC download)
    -> local Parquet + .produced marker
    -> cc-enrich-worker load --scan
    -> ClickHouse
    -> local .loaded marker
```

There is no runtime URL-part worklist and no raw-WARC staging service. RustFS stores the catalog, not the
Common Crawl WARC bodies. The worker reads WARC data directly from Common Crawl.

## Layout

| Path | Responsibility |
|---|---|
| `cc-warc-index-builder/` | Selects up to N pages per domain from the official URL-index Parquets, builds one WARC-oriented DuckDB catalog, and publishes it to RustFS. |
| `cc-enrich-worker/` | Runs an inclusive range of catalog WARC indexes (`--parts A-B`) via range reads or a complete WARC download, extracts data, writes Parquet + `.produced`, and loads ClickHouse via `load --scan` (which writes `.loaded`). Its `internal/fetch` package owns Common Crawl range-fetch and WARC/embedded-HTTP parsing. |
| `deploy/` | Builds and atomically deploys the `cc-enrich-worker` Linux binary. |
| `.env` | The single ignored environment file shared by the builder and worker in this checkout. |
| `.env.example` | Safe configuration template. |

Component details remain in
[`cc-warc-index-builder/README.md`](cc-warc-index-builder/README.md) and
[`cc-enrich-worker/README.md`](cc-enrich-worker/README.md).

## Prerequisites

Catalog building requires:

- Python 3.14;
- [`uv`](https://docs.astral.sh/uv/);
- local disk for candidate Parquets, the final DuckDB database, and DuckDB spill; and
- network access to the Common Crawl index and RustFS.

Runtime development and deployment require:

- Go 1.26.1 or newer;
- GNU Make;
- Docker with BuildKit support for the Linux/AMD64 CGO release build;
- Ansible Core and SSH access for deployment; and
- network access from the processor to RustFS, Common Crawl, ClickHouse, and the embedding endpoint when
  running industry or embed modes.

The DuckDB Go driver requires CGO. Use the provided release target and container toolchain for production
instead of copying a binary built directly on macOS.

## Shared environment

Create exactly one environment file at the processor root:

```bash
cd corpscout/commoncrawl/cc-processor
cp .env.example .env
chmod 0600 .env
```

At minimum, configure:

- `CORPSCOUT_S3_ENDPOINT`, `CORPSCOUT_S3_ACCESS_KEY`, `CORPSCOUT_S3_SECRET_KEY`, and
  `CORPSCOUT_S3_REGION` for RustFS;
- `COMMONCRAWL_CATALOG_S3_BASE` for the catalog bucket and base prefix (read by the worker's
  `sync-db` command — produce runs use the local cache it writes);
- `OUT_BASE_DIR` for local catalog-build or processing data;
- Common Crawl AWS credentials or an EC2 instance role for signed S3 access; and
- `CLICKHOUSE_*` before a produce/load run.

Industry and embed modes additionally require `COMMONCRAWL_EMBED_*`. Reference embeddings and processed
page embeddings must use the same model.

`make catalog` loads the root `.env`. A builder or worker invocation does not load the file itself;
export it first:

```bash
set -a
source .env
set +a
```

Values such as `OUT_BASE_DIR` are host-specific. On `wappalyzer` it should point to catalog build storage;
on `commoncrawl2` it should point to the processor's catalog cache, output, logs, and markers. Do not commit
or copy secrets into component directories.

## Build and test

Run the processor Makefile from this directory:

```bash
make
make test
make vet
```

The main targets are:

| Target | Result |
|---|---|
| `make` | Builds the Python builder entry point plus the Go worker binary. |
| `make catalog CRAWL=...` | Builds/resumes and publishes one catalog using `.env`. |
| `make test` | Runs builder and all Go runtime tests. |
| `make vet` | Vets the Go runtime modules. |
| `make release` | Produces the Linux runtime artifact under `dist/<os>-<arch>/`. |
| `make clean` | Removes generated component binaries and processor release artifacts. |

The Go runtime module can also be checked independently:

```bash
make -C cc-enrich-worker test
```

Build the production Linux/AMD64 runtime with:

```bash
make release TARGET_GOOS=linux TARGET_GOARCH=amd64
```

The release target tests and vets the Go runtime, then uses `deploy/runtime.Dockerfile` to write
`cc-enrich-worker` under `dist/linux-amd64/`. The catalog builder is a Python application
run on `wappalyzer`; it is not part of the processor-server binary release.

## Apply the ClickHouse schema

Apply ClickHouse migration `000127_corpscout_commoncrawl_page_jsonld` before deploying or running this
worker version:

```bash
cd corpscout
make clickhouse-migrate-up
```

The command uses the repository's configured `CLICKHOUSE_MIGRATE_URL`. Migration `000127` creates
`corpscout.commoncrawl_page_jsonld` and removes the old single-profile
`corpscout.commoncrawl_page_metadata` table. The worker writes every typed or identified JSON-LD entity as
an independent page-provenance row; it no longer selects one organization from a page.

The worker never creates ClickHouse tables. A missing-table load failure therefore means the migrations
must be brought current before retrying the WARC.

## Build and publish a catalog

The builder accepts any crawl ID for which Common Crawl publishes the URL-index and WARC manifests. The
crawl ID is an input, not a constant in the code.

On `wappalyzer`, keep the checkout, catalog data, spill, and logs on persistent storage:

```bash
ssh graovic@wappalyzer
cd /home/graovic/cc-processor
chmod 0600 .env

make catalog CRAWL=CC-MAIN-2026-25 PAGES_PER_DOMAIN=25
```

`make catalog` is the preferred entry point. It loads the shared `.env`, synchronizes the locked Python
environment, and runs the builder for the requested crawl. `CRAWL` is required so a command cannot silently
build the wrong release. `PAGES_PER_DOMAIN` defaults to `25`; pass additional builder flags through
`CATALOG_ARGS`, for example:

```bash
make catalog \
  CRAWL=CC-MAIN-2026-25 \
  PAGES_PER_DOMAIN=25 \
  CATALOG_ARGS='--attempts 5 --threads 16 --memory-limit 24GB'
```

For direct access to builder controls, run from the component directory and export the shared parent file:

```bash
cd /home/graovic/cc-processor/cc-warc-index-builder
set -a; source ../.env; set +a

uv sync --frozen
uv run --frozen cc-warc-index-builder \
  --base /home/graovic/cc-warc-index-data \
  --crawl CC-MAIN-2026-25 \
  --pages-per-domain 25 \
  --attempts 5 \
  --threads 16 \
  --memory-limit 24GB
```

The build performs one remote pass over the crawl's official Parquet URL-index shards. It writes resumable
candidate Parquets, selects the global top N pages per domain, maps them to stable zero-based WARC indexes,
and creates one local database:

```text
<OUT_BASE_DIR>/<crawl>/warc-index/pagesN/catalog.duckdb
```

That database contains:

- `metadata`, with crawl identity, policy version, manifest hashes, and totals;
- `warcs`, the complete manifest inventory and stable `warc_index -> warc_filename` mapping;
- `pages`, the selected URL, domain rank, WARC index, compressed offset, and compressed length;
- `warc_size_sample`, the deterministic object-size sample; and
- `warc_stats`, selected page/byte totals per WARC using the sampled average WARC size for an estimate.

Useful inspection queries include:

```sql
SELECT count(*) AS warcs, min(warc_index), max(warc_index) FROM warcs;

SELECT warc_index, warc_filename, selected_pages, selected_bytes,
       estimated_utilization_percent
FROM warc_stats
WHERE selected_pages > 0
ORDER BY selected_bytes DESC
LIMIT 20;

SELECT root_domain, url, domain_page_rank, warc_record_offset, warc_record_length
FROM pages
WHERE warc_index = 81565
ORDER BY warc_record_offset;
```

`warc_stats.estimated_utilization_percent` is for catalog inspection only. Runtime mode selection uses a
HEAD request for that specific WARC's actual object size.

If a remote shard returns `503` or another terminal error, rerun the identical command. Complete candidate
shards are validated and reused; only missing or invalid shards are queried again. If the final local
catalog is already healthy, rerunning skips index construction and republishes that database. Use
`--rebuild-catalog` only when intentionally replacing an existing catalog, and use `--cleanup-candidates`
only after accepting that a future rebuild will need to query the remote shards again.

### RustFS publication and key derivation

For:

```text
COMMONCRAWL_CATALOG_S3_BASE=s3://crawls/commoncrawl/catalogs
CRAWL=CC-MAIN-2026-25
pages-per-domain=25
```

the builder derives exactly:

```text
s3://crawls/commoncrawl/catalogs/CC-MAIN-2026-25/pages25/catalog.duckdb
s3://crawls/commoncrawl/catalogs/CC-MAIN-2026-25/pages25/ready.json
```

The crawl and `pagesN` selection are appended in code; do not include either in
`COMMONCRAWL_CATALOG_S3_BASE`. Publication removes the prior readiness marker, uploads the database with
its SHA-256 metadata, verifies its size and checksum metadata, and writes `ready.json` last. Consumers must
treat `ready.json` as the commit marker and must not use an uncommitted catalog object.

The worker's `sync-db` command — the explicit, only sync step; produce runs read just the local
cache — validates that marker and caches the complete catalog once at:

```text
<OUT_BASE_DIR>/<crawl>/warc-index/pagesN/catalog.duckdb
<OUT_BASE_DIR>/<crawl>/warc-index/pagesN/catalog.duckdb.sha256
```

The first WARC on a processing machine therefore pays the one-time catalog download. Later WARC runs reuse
the verified local cache while the remote committed checksum remains unchanged.

## Process WARC indexes

The examples run `cc-enrich-worker` from the processor root after sourcing the shared `.env`:

```bash
set -a; source .env; set +a

./cc-enrich-worker/bin/cc-enrich-worker tech \
  --crawl-id CC-MAIN-2026-25 \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --parts 0-1000 \
  --warc-parallel 8 \
  --concurrency 32
```

`--parts` means one stable WARC index or an inclusive WARC-index range from the catalog. It is not a
URL-index shard number. For example, `--parts 85-150` processes catalog WARC indexes 85 through 150,
inclusive. Machines can be assigned arbitrary non-overlapping ranges of different sizes. Each produced
part writes a `.produced` marker; load them into ClickHouse with `cc-enrich-worker load --scan <root>`
(add `--watch` to load parts as they land).

`--selection pagesN` selects the `pagesN` catalog and must match a catalog that has been built and committed.
Tech uses all selected pages. Industry and embed use only each domain's rank-1 page from the same catalog.

For each non-empty WARC, the worker obtains the actual Common Crawl object size and computes:

```text
selected compressed record bytes / complete WARC object bytes * 100
```

At or above the whole-WARC threshold, it downloads the WARC once to a temporary local file and serves the
selected gzip members with concurrent local reads. Below the threshold, it performs exact range reads for
the selected records. The temporary complete WARC is removed after processing. A catalog WARC with no pages
for the selected mode is a successful no-op and does not access the WARC object.

WARC reads always go through signed Common Crawl S3; off AWS, export explicit
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` credentials.

## Produce, load, and completion state

For `tech` and `industry`, the lifecycle is split across two decoupled commands, each idempotent through
its own on-disk marker:

```text
produce (cc-enrich-worker <mode> --parts A-B):
  .produced marker exists (or .loaded)   -> skip the part
  otherwise                              -> remove stale output directory
                                            -> produce Parquet
                                            -> require domains.parquet
                                            -> write .produced with per-kind row counts

load (cc-enrich-worker load --scan <root> [--watch]):
  sweep for .produced dirs lacking .loaded
    -> load all supported Parquet files into ClickHouse
    -> verify loaded row counts against the .produced marker
    -> write .loaded
```

Files live under:

```text
<OUT_BASE_DIR>/<crawl>/warc/pagesN/
├── logs/
├── out_tech_<warc-index>/
└── out_tech_<warc-index>.loaded
```

The marker is written only after the loader exits successfully. A fetch, extraction, Parquet, or ClickHouse
failure leaves no marker, so rerunning the same range retries that WARC. If loading succeeds but marker
creation fails, a retry can insert another physical version; the Common Crawl tables use replacement keys
and timestamps so current-result queries can deduplicate with normal merges or `FINAL` where immediate
deduplication is required.

Remove a `.loaded` marker only when intentionally reprocessing that WARC. The next run clears its old local
output directory and produces it again. The marker is local by design: operators assign ranges to processing
machines and retain their corresponding data directories.

Embed mode is file-based and does not load ClickHouse or create `.loaded`. It writes an atomic
`embeddings.parquet` beneath:

```text
<OUT_BASE_DIR>/<crawl>/embedding/warc/pagesN/out_industry_<warc-index>/
```

A valid existing embeddings file is its completion check.

## Deploy to `commoncrawl2`

The Ansible package builds on the control machine and deploys `cc-enrich-worker` as one
checksum-addressed release:

```bash
cd corpscout/commoncrawl/cc-processor/deploy

# Full build and remote preflight without changing the server.
ansible-playbook site.yml --limit commoncrawl2 --ask-become-pass --check --diff

# Deploy and atomically activate the binary.
ansible-playbook site.yml --limit commoncrawl2 --ask-become-pass
```

`--ask-become-pass` prompts for the remote `sudo` password; omit it when the inventory user has passwordless
sudo. The playbook does not deploy the catalog builder, catalogs, data, logs, output, or credentials. It
requires the protected processor-root `.env` or, on the first migration, copies the existing legacy file;
it then preserves the new file and only ensures the non-secret catalog base setting is present.

The binary is installed at its stable command path within the processor root:

```text
/opt/companycollect/corpscout/commoncrawl/cc-processor/cc-enrich-worker/bin/cc-enrich-worker
```

On the first deployment, if the new processor environment is absent, the playbook copies the regular legacy
`/opt/companycollect/corpscout/commoncrawl/.env` to
`/opt/companycollect/corpscout/commoncrawl/cc-processor/.env` with mode `0600`. Later deployments read only
the new processor-root file; the legacy file is not a second runtime configuration.

See [`deploy/README.md`](deploy/README.md) for the exact release layout, safety checks, and rollback.

## Roll back

The deployment records the previously active release in `previous`. Roll back both binaries together:

```bash
ssh -t graovic@commoncrawl2
cd /opt/companycollect/corpscout/commoncrawl/cc-processor
sudo ln -sfn "$(readlink previous)" .current-rollback
sudo mv -Tf .current-rollback current
```

Already-running crawls resolve the worker beside their versioned executable and remain pinned to their
original paired release. A rollback changes the binaries used by new commands; it does not change schemas,
catalogs, Parquet, or `.loaded` markers.

## Troubleshooting

### `ready.json` is missing or returns 404

Confirm that the crawl ID and `-max-pages` select the catalog that was built. A catalog is deliberately
unavailable while publication is in progress because the builder writes `ready.json` last. Rerun the builder
if an upload failed after removing the old marker.

### RustFS connection or authentication fails

Check `CORPSCOUT_S3_ENDPOINT` from the current host, then the access key, secret key, region, and
`COMMONCRAWL_CATALOG_S3_BASE`. The endpoint is an HTTP(S) service URL; the catalog base is an
`s3://bucket/prefix` URI. Do not put the crawl ID in the base URI.

### The local catalog cannot be opened

Ensure the processor has enough free disk for the full catalog and that both the database and SHA sidecar
are regular files. If the verified cache was externally damaged, stop active workers, remove only
`catalog.duckdb` and `catalog.duckdb.sha256` for that crawl/selection, and rerun; the worker downloads and
verifies the committed RustFS object again. Partial downloads are cleaned automatically.

### A requested WARC index is absent

The range exceeds the `warcs` inventory in the selected catalog or points at a different crawl. Inspect the
catalog's `warcs` table and choose indexes from that catalog; do not reuse WARC-index bounds from another
crawl.

### Common Crawl returns 429 or 503

The signed S3 reader reports HTTP attempts, SDK retries, throttling status, and body-read retries. Reduce the
relevant concurrency, verify the EC2 role or AWS credentials, and rerun the range. A failed WARC has no
`.loaded` marker.

### Produce succeeds but load fails

Check that migration `000127` and all earlier migrations are applied, then verify `CLICKHOUSE_*`. The marker
is not written after a failed load, so the same WARC can be retried after fixing ClickHouse.

### A WARC is skipped unexpectedly

The range runner skips `tech` and `industry` for a part whose output dir already has a `.produced` (or
`.loaded`) marker, and `load --scan` skips any dir already carrying `.loaded`. Inspect the corresponding
output and log before removing a marker. Removing `.produced` authorizes a produce retry; removing
`.loaded` authorizes a load retry for that WARC.

### The builder stops after remote shard failures

Rerun the identical crawl and pages-per-domain command. Do not delete `candidates/`: valid candidate files
are the resume state. `--attempts` controls bounded retries within one run; rerunning handles sources that
remained unavailable for the entire prior run.
