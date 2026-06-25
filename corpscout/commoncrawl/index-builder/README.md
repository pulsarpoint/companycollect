# cc-index-builder

Standalone builder of per-domain **worklists** from the CommonCrawl columnar URL index —
the small "what to fetch" list (one row per domain: WARC file + byte offset/length) that the
Go `cc-enrich-worker` consumes. **No dagster dependency** — only `duckdb` + `pyarrow`.

## Why it exists separately

Worklist generation is a DuckDB window query over the ~1 GB-per-part cc-index; DuckDB does it
far better than reimplementing in Go, and the step runs **once per shard, not per page**, so
it's not perf-critical. Keeping it as its own package means the processing boxes stay pure Go
and only the (one) worklist-generating box needs Python + two libraries.

## What it does

For one index part it picks, per registered domain, the most *representative* `fetch_status=200`
HTML page: the **main-site homepage** (apex or `www`, then shallowest URL path) over a functional
subdomain (`shop.`/`blog.`/`api.`…). Off-AWS it resolves exact part URLs from the published
`cc-index-table.paths.gz` manifest (anonymous S3 LIST is denied; anonymous GET is fine).

## Usage

```bash
# how many index parts the crawl has
python -m index_builder --crawl CC-MAIN-2026-25 --list

# build one shard (cached at data/commoncrawl/index/<crawl>/shard_0.parquet; skipped if present)
python -m index_builder --crawl CC-MAIN-2026-25 --part 0

# build a range, skipping any already cached
python -m index_builder --crawl CC-MAIN-2026-25 --parts 0-9

# explicit output path (single part), bypassing the cache
python -m index_builder --crawl CC-MAIN-2026-25 --part 0 --out shard0.parquet

# restrict the domain set (default: all domains)
python -m index_builder --crawl CC-MAIN-2026-25 --part 0 --where "content_languages like '%eng%'"
```

`cc-enrich-worker/run_crawl.sh` calls this (via `uv run python -m index_builder`) to produce each
shard before the Go pass.

## Install / test

```bash
uv run --with pytest pytest tests/        # or: pip install -e .[dev] && pytest
```

## Output schema

`root_domain, url, warc_filename, warc_record_offset, warc_record_length, content_languages`
