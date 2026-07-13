# cc-enrich-worker

Processes selected pages directly from Common Crawl using the static WARC-oriented catalog created by
[`cc-warc-index-builder`](../cc-warc-index-builder/).

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

## JSON-LD output

Tech processing writes `jsonld.parquet`, loaded into `corpscout.commoncrawl_page_jsonld`. Every JSON-LD
object carrying `@type` or `@id` is a separate row, including nested publisher, author, provider, and
organization objects. Rows retain the page URL, WARC coordinates, zero-based script index, RFC 6901
entity path, sorted types, commonly queried fields, and canonical raw JSON.

No entity is selected as the page's organization and fields from different entities are never merged.
The source record coordinates plus script index and entity path make reprocessing idempotent. The former
single-profile `metadata.parquet` output is not produced or loaded.

## Build and run

From `commoncrawl/`:

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

Use `--s3-anonymous` off AWS to read through `https://data.commoncrawl.org/`. Signed S3 is the default
and is preferred on EC2.

## Produce flags

| Flag | Default | Meaning |
|---|---|---|
| `--base` | `OUT_BASE_DIR` | Required local output and catalog-cache root. |
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

## Environment

| Variable | Meaning |
|---|---|
| `OUT_BASE_DIR` | Default `--base`. |
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
