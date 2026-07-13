# cc-warc-index-builder

Builds a compact, WARC-oriented DuckDB catalog from the official Common Crawl URL-index Parquets.
The builder queries each index shard remotely, keeps only the pages that can still enter the requested
per-domain top N, performs the final ranking locally, and publishes the resulting DuckDB to RustFS.
It does not download complete URL-index Parquet shards or WARC bodies.

## Pipeline

1. Cache and validate `cc-index-table.paths.gz` and `warc.paths.gz` for the requested crawl.
2. Discover every `subset=warc` Parquet source in manifest order.
3. Query one remote Parquet source at a time with DuckDB and write a local candidate Parquet.
4. Reuse every complete candidate on restart and retry only missing or invalid candidates.
5. Take a deterministic, HEAD-only sample of WARC object sizes and cache it as JSON.
6. Read all candidates locally, calculate the global top N, join pages to stable `warc_index` values,
   aggregate WARC statistics, checkpoint, and atomically publish local `catalog.duckdb`.
7. Remove the previous remote `ready.json`, upload `catalog.duckdb` with SHA-256 metadata, verify its
   size and hash metadata with a HEAD request, and upload the new `ready.json` last.

### Why local top N is exact

Duplicate capture coordinates are collapsed first. For each root domain, a source shard then retains
its best N rows using the same complete ordering used by the global pass. A discarded row already has
at least N better rows for that domain in its own shard,
so it cannot enter the global top N after other shards are added. The local candidates are therefore a
sufficient input for the exact global row ranking.

Both passes use deterministic URL, WARC filename, record offset, and record length tie breakers. With
`--pages-per-domain 1`, the ordering selects the best main-site shallow page. With larger values it also
prioritizes home, company, legal, contact, privacy, and terms pages.

## Requirements and build

- Python 3.14
- [`uv`](https://docs.astral.sh/uv/)

```bash
cd corpscout/commoncrawl/cc-processor/cc-warc-index-builder
uv sync --frozen
uv run pytest -q
make build
```

`make build` creates `bin/cc-warc-index-builder` as a link to the locked uv environment.

## Run

RustFS publication is part of a successful build, including a run that reuses an existing local catalog.
The canonical entry point is the processor Makefile, which loads the one shared `cc-processor/.env`:

```bash
cd corpscout/commoncrawl/cc-processor
make catalog CRAWL=CC-MAIN-2026-25 PAGES_PER_DOMAIN=25
```

For a direct component invocation, export the parent environment file before starting:

```bash
cd cc-warc-index-builder
set -a; source ../.env; set +a
```

The region defaults to `us-east-1`; RustFS access always uses path-style S3 URLs. The base URI supplies
the bucket and optional object-key prefix. Credentials are read only from the environment.

```bash
./bin/cc-warc-index-builder \
  --base /home/graovic/cc-warc-index-data \
  --crawl CC-MAIN-2026-25 \
  --pages-per-domain 25 \
  --attempts 5
```

Useful controls:

- `--threads N` and `--memory-limit 24GB` configure DuckDB.
- `--rebuild-catalog` explicitly replaces the final catalog while still reusing identity-checked candidate Parquets.
- `--cleanup-candidates` removes candidates only after the catalog is ready.

The default is to retain candidates because they make interruption recovery and catalog rebuilds cheap.

## Output layout

For the command above:

```text
/home/graovic/cc-warc-index-data/CC-MAIN-2026-25/warc-index/
├── manifests/
│   ├── cc-index-table.paths.gz
│   └── warc.paths.gz
├── warc-size-sample-256.json
└── pages25/
    ├── candidates/
    │   └── v1-<index-manifest-sha256>/
    │       ├── part-00000.parquet
    │       └── ...
    └── catalog.duckdb
```

DuckDB spill directories and `catalog.duckdb.partial` are created beneath `pages25/` and removed after a
successful build. The completed catalog contains both the WARC inventory and selected page coordinates.

For the example configuration, the remote layout is:

```text
s3://crawls/commoncrawl/catalogs/CC-MAIN-2026-25/pages25/
├── catalog.duckdb
└── ready.json
```

Candidate Parquets remain local. A consumer must treat `ready.json` as the commit marker; the remote
DuckDB is not ready while that marker is absent. It contains:

```json
{
  "schema_version": 1,
  "crawl_id": "CC-MAIN-2026-25",
  "selection": "pages25",
  "catalog": {"key": ".../catalog.duckdb", "size_bytes": 123, "sha256": "..."}
}
```

### DuckDB tables

- `metadata`: crawl and selection identity/version, manifest hashes, counts, selected bytes, sample
  count, estimated average WARC size, and creation time.
- `warcs`: every manifest WARC with its stable zero-based `warc_index`.
- `warc_size_sample`: exact `Content-Length` values for the deterministic HEAD sample.
- `pages`: selected page URL, domain rank, language, WARC index, offset, and compressed record length.
- `warc_stats`: all WARC objects with selected page/byte counts plus average-size utilization estimates.

`estimated_utilization_percent` uses the crawl-wide sampled average; it is not an exact per-WARC
percentage. A downloader must obtain the specific object's size before making an exact threshold decision.

## Retry and resume

- Manifest and HEAD requests have bounded attempts with backoff and `Retry-After` support.
- Each candidate is first written as `.partial` and promoted only after a complete Parquet is present.
- A rerun validates candidate schema, policy version, source index, and index-manifest identity before
  reuse; mismatched or corrupt candidates are rebuilt.
- If any remote shard fails, the command exits nonzero after preserving all successful candidates. The
  per-shard files themselves are the resume state.
- The WARC-size JSON sample is reused only when its crawl and deterministic sampled objects match.
- The final DuckDB is built separately and atomically replaces the published catalog after checkpointing.
- Remote replacement first deletes the old readiness marker. Any upload or HEAD-verification failure exits
  nonzero and leaves the remote catalog uncommitted; rerunning uploads the existing healthy local DuckDB
  without rebuilding it.
- One build lock prevents two processes from sharing partial paths for the same crawl and selection.
- An existing catalog that fails identity or health checks is preserved unless `--rebuild-catalog` is
  supplied.

## Candidate cleanup

Prefer automatic cleanup on the successful run:

```bash
./bin/cc-warc-index-builder \
  --base /home/graovic/cc-warc-index-data \
  --crawl CC-MAIN-2026-25 \
  --cleanup-candidates
```

Or, after verifying that `catalog.duckdb` opens and its `metadata` row is correct, remove only:

```bash
rm -rf /home/graovic/cc-warc-index-data/CC-MAIN-2026-25/warc-index/pages25/candidates
```

Do not remove the manifests, WARC-size sample, or catalog. A later `--rebuild-catalog` must query remote
shards again if candidates were removed.

## Full run on wappalyzer

Keep the checkout, data, DuckDB spill, and logs under `/home/graovic`; never use `/tmp` for a full crawl.
`/tmp` may be capacity-limited or cleaned while the multi-hour run is active.

Recommended paths:

```text
/home/graovic/cc-processor                   # grouped checkout and shared .env
/home/graovic/cc-warc-index-data             # --base and DuckDB spill
/home/graovic/logs                            # persistent logs
```

After placing the grouped processor checkout on `graovic@wappalyzer` and creating the single protected
`/home/graovic/cc-processor/.env`:

```bash
ssh graovic@wappalyzer
mkdir -p /home/graovic/cc-warc-index-data /home/graovic/logs
cd /home/graovic/cc-processor
chmod 0600 .env
tmux new -s cc-warc-index

make catalog CRAWL=CC-MAIN-2026-25 PAGES_PER_DOMAIN=25 \
  2>&1 | tee /home/graovic/logs/cc-warc-index-CC-MAIN-2026-25.log
```

Use the direct command when overriding builder-specific controls:

```bash
cd /home/graovic/cc-processor/cc-warc-index-builder
set -a; source ../.env; set +a
uv sync --frozen

uv run --frozen cc-warc-index-builder \
  --base /home/graovic/cc-warc-index-data \
  --crawl CC-MAIN-2026-25 \
  --pages-per-domain 25 \
  --threads 16 \
  --memory-limit 24GB \
  2>&1 | tee /home/graovic/logs/cc-warc-index-CC-MAIN-2026-25.log
```

Detach from tmux with `Ctrl-b d`. Re-run the same command after any failure; completed candidate shards
are reused automatically.
