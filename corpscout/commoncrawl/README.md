# Common Crawl enrichment

Builds a reusable WARC-oriented catalog for any `CC-MAIN-XXXX-YY` crawl, then processes WARC indexes
directly from Common Crawl. There is no runtime raw downloader or URL-index-part worklist.

## Components

| Directory | Role |
|---|---|
| `cc-warc-index-builder/` | Queries the official Common Crawl Parquet URL index once and publishes the selected page catalog. |
| `cc-crawl/` | Runs an inclusive WARC-index range and preserves local produce → load → `.loaded` orchestration. |
| `cc-enrich-worker/` | Chooses exact source ranges or one whole-WARC download, processes pages, writes Parquet, and loads ClickHouse. |
| `cc-raw/` | Shared direct Common Crawl fetch and WARC-record parser. |
| `reference-builder/` | Builds NACE and page-type reference embeddings for the industry processor. |

ClickHouse DDL lives in `../clickhouse/migrations/`. The worker never creates tables.

## 1. Build a crawl catalog

The builder uses Python 3.14 with `uv` and supports any crawl ID whose official Common Crawl manifests
and Parquet URL index are available:

```bash
cd cc-warc-index-builder
uv run --frozen cc-warc-index-builder \
  --base /data/commoncrawl \
  --crawl CC-MAIN-2026-25 \
  --pages-per-domain 25 \
  --threads 16 \
  --memory-limit 24GB
```

Remote index shards are retried and completed candidate shards are reused. The final runtime files are:

```text
/data/commoncrawl/CC-MAIN-2026-25/warc-index/pages25/warcs.parquet
/data/commoncrawl/CC-MAIN-2026-25/warc-index/pages25/pages.parquet
```

`warcs.parquet` assigns a stable zero-based index to every WARC object. `pages.parquet` contains only the
selected pages with their WARC index, compressed offset, compressed length, URL, domain, and domain page
rank. The DuckDB catalog is a builder artifact; processing machines need only the two Parquet files.

Copy those files to the same `<base>/<crawl>/warc-index/pagesN/` path on every processing machine.

## 2. Configure direct Common Crawl access

Copy `.env.example` to `.env`.

Signed S3 is the default and preferred path on EC2:

```bash
AWS_REGION=us-east-1
# Use an EC2 instance role, or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.
```

Off AWS, use the anonymous HTTPS endpoint:

```bash
S3_ANONYMOUS=true
CC_BASE_URL=https://data.commoncrawl.org/
```

Shared operator settings:

```bash
OUT_BASE_DIR=/data/commoncrawl
WHOLE_WARC_THRESHOLD=50
```

Industry/embed additionally use `COMMONCRAWL_EMBED_*`; industry and `load` use `CLICKHOUSE_*`.

## 3. Build and test the Go runtime

```bash
make -C cc-raw test
make -C cc-enrich-worker test
make -C cc-crawl test

make -C cc-enrich-worker build
make -C cc-crawl build
```

## 4. Process WARC indexes

The existing `-parts` operator flag is retained, but its values are now WARC indexes:

```bash
./cc-crawl/bin/cc-crawl \
  -base "$OUT_BASE_DIR" \
  -crawl CC-MAIN-2026-25 \
  -mode tech \
  -parts 0-1000 \
  -max-pages 25 \
  -whole-warc-threshold 50 \
  -tech-conc 32
```

Industry uses only catalog rank-1 pages:

```bash
./cc-crawl/bin/cc-crawl \
  -base "$OUT_BASE_DIR" \
  -crawl CC-MAIN-2026-25 \
  -mode industry \
  -parts 0-1000
```

For each WARC index, the worker:

1. reads its catalog pages;
2. filters to rank 1 for industry/embed;
3. HEADs a non-empty WARC for its actual size;
4. compares selected compressed bytes with the configured threshold;
5. either issues exact record ranges or streams the complete WARC to a temporary local file;
6. parses the same indexed gzip members through the existing processor;
7. removes a temporary complete WARC after processing; and
8. writes local Parquet output.

Whole-WARC mode uses local concurrent `ReadAt` calls and never holds the complete object in memory. A
WARC with no pages for the selected processor completes without AWS initialization or network traffic.

## 5. Loading and resumability

For `tech` and `industry`, `cc-crawl` preserves the existing state machine:

```text
out_<mode>_<warc-index>.loaded exists
    -> skip

otherwise
    -> produce Parquet
    -> require domains.parquet
    -> load Parquet into ClickHouse
    -> create .loaded
```

New output and markers live under `<base>/<crawl>/warc/pagesN/`, separate from the former URL-part
namespace and from other page-count selections.
The marker is written only after a successful ClickHouse load. Remove a specific marker to rerun that
WARC index. `embed` remains file-based under `<base>/<crawl>/embedding/warc/pagesN/` and does not load ClickHouse.

Technology and metadata evidence is stored per page in:

- `corpscout.commoncrawl_page_technologies`
- `corpscout.commoncrawl_page_metadata`

Both tables are partitioned by crawl ID. Technology rows remain normalized for reverse lookups by
technology and version.

See [`cc-warc-index-builder/README.md`](cc-warc-index-builder/README.md) and
[`cc-enrich-worker/README.md`](cc-enrich-worker/README.md) for component-level details.
