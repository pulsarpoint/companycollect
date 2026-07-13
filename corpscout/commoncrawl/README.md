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
| `cc-dns-scan/` | Resolves Common Crawl domains against authoritative DNS and persists delegation/record observations. |
| `cc-dns-axfr/` | Independently probes persisted authoritative endpoints for zone-transfer exposure. |
| `deploy/cc_dns_scan/` | Deploys the authoritative DNS scanner, Unbound, and DNS host tuning. |
| `deploy/cc_dns_axfr/` | Independently deploys the AXFR scanner. |
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

Remote index shards are retried and completed candidate shards are reused. After checkpointing and
validating the single DuckDB catalog, the builder uploads it to RustFS and writes `ready.json` last:

```text
s3://crawls/commoncrawl/catalogs/CC-MAIN-2026-25/pages25/catalog.duckdb
s3://crawls/commoncrawl/catalogs/CC-MAIN-2026-25/pages25/ready.json
```

`catalog.duckdb` assigns a stable zero-based index to every WARC object and contains the selected pages
with their WARC index, compressed offset, compressed length, URL, domain, and domain page rank. RustFS is
the authoritative catalog store. A processing machine downloads and verifies each committed catalog once,
then queries this local read-only cache:

```text
<base>/<crawl>/warc-index/<selection>/catalog.duckdb
<base>/<crawl>/warc-index/<selection>/catalog.duckdb.sha256
```

The SHA sidecar records the verified digest from `ready.json`. Workers reuse the cache while it matches the
committed digest; they do not attach to the DuckDB database remotely.

## 2. Configure the catalog and Common Crawl access

Copy `.env.example` to `.env`. Configure the RustFS catalog once; the builder and worker append the crawl
ID and `pagesN` selection themselves:

```bash
CORPSCOUT_S3_ENDPOINT=http://rustfs:9000
CORPSCOUT_S3_ACCESS_KEY=...
CORPSCOUT_S3_SECRET_KEY=...
COMMONCRAWL_CATALOG_S3_BASE=s3://crawls/commoncrawl/catalogs
```

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

Build all four runtime binaries from this directory:

```bash
make
make test
make vet
```

Individual modules remain directly runnable:

```bash
make -C cc-raw test
make -C cc-enrich-worker test
make -C cc-crawl test
make -C cc-dns-scan test
make -C cc-dns-axfr test

make -C cc-enrich-worker build
make -C cc-crawl build
make -C cc-dns-scan build
make -C cc-dns-axfr build
```

For production, deploy the WARC processor pair with `cc_processor` instead of copying local
`bin/` files. The control machine cross-compiles static Linux binaries and activates one paired release:

```bash
cd deploy/cc_processor
ansible-playbook site.yml --limit commoncrawl2 --ask-become-pass
```

See [`deploy/cc_processor/README.md`](deploy/cc_processor/README.md) for release layout, safety checks, and rollback.
DNS and AXFR have independent runbooks under [`deploy/cc_dns_scan`](deploy/cc_dns_scan/) and
[`deploy/cc_dns_axfr`](deploy/cc_dns_axfr/).

Deploy DNS first, then AXFR. Each playbook installs its unit but deliberately leaves it stopped and
disabled; start and enable it only after the playbook succeeds:

```bash
cd deploy/cc_dns_scan
ansible-playbook site.yml
ssh root@hetzner01 'systemctl enable --now cc-dns-scan'
ssh root@hetzner01 'journalctl -u cc-dns-scan -n 100 -f'

cd ../cc_dns_axfr
ansible-playbook site.yml
ssh root@hetzner01 'systemctl enable --now cc-dns-axfr'
ssh root@hetzner01 'journalctl -u cc-dns-axfr -n 100 -f'
```

`cc-axfr-scan` is the obsolete pre-split unit name. Do not use
`journalctl -u cc-axfr-scan`; the standalone scanner logs under `cc-dns-axfr`.

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

1. validates `ready.json`, then downloads `catalog.duckdb` only when the verified local cache is absent or
   its SHA sidecar differs;
2. queries the cached DuckDB for the WARC and its selected page coordinates;
3. filters to rank 1 for industry/embed;
4. HEADs a non-empty WARC for its actual size;
5. compares selected compressed bytes with the configured threshold;
6. either issues exact record ranges or streams the complete WARC to a temporary local file;
7. parses the same indexed gzip members through the existing processor;
8. removes a temporary complete WARC after processing; and
9. writes local Parquet output.

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

Technology and structured-data evidence is stored per page in:

- `corpscout.commoncrawl_page_technologies`
- `corpscout.commoncrawl_page_jsonld`

Both tables are partitioned by crawl ID. Technology rows remain normalized for reverse lookups by
technology and version. JSON-LD stores every typed or identified entity separately with its script
index, stable JSON pointer, canonical JSON, and page/WARC provenance; the worker does not select a
single organization profile.

See [`cc-warc-index-builder/README.md`](cc-warc-index-builder/README.md) and
[`cc-enrich-worker/README.md`](cc-enrich-worker/README.md) for component-level details.
