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

Two worklist shapes, because the two worker passes want different inputs (`--mode`):

- **`industry`** (default) — ONE most-representative `fetch_status=200` HTML page per registered
  domain: the **main-site homepage** (apex/`www`, then shallowest path) over a functional subdomain
  (`shop.`/`blog.`/`api.`…). That page is embedded → NACE classified.
- **`tech`** — MANY pages per domain (`--max-pages`, default 25; `0` = every HTML page). Wappalyzer
  + the contact/LEI/VAT/JSON-LD-profile extractors improve with coverage, and **LEI/VAT live on
  `/imprint`, `/legal`, `/contact` — never the homepage**. Ranked homepage-first, then legal/contact/
  about pages, then shallowest, so a per-domain cap keeps the identifier-bearing pages.

Off-AWS it resolves exact part URLs from the published `cc-index-table.paths.gz` manifest
(anonymous S3 LIST is denied; anonymous GET is fine).

## Usage

```bash
# how many index parts the crawl has
python -m index_builder --crawl CC-MAIN-2026-25 --list

# industry worklist (1 page/domain) -> data/commoncrawl/index/<crawl>/shard_industry_0.parquet
python -m index_builder --crawl CC-MAIN-2026-25 --part 0

# tech worklist (up to 25 pages/domain) -> shard_tech_0.parquet
python -m index_builder --crawl CC-MAIN-2026-25 --mode tech --part 0
python -m index_builder --crawl CC-MAIN-2026-25 --mode tech --max-pages 0 --part 0   # every HTML page

# range (skip cached), explicit out, or restrict the domain set
python -m index_builder --crawl CC-MAIN-2026-25 --mode tech --parts 0-9
python -m index_builder --crawl CC-MAIN-2026-25 --part 0 --out shard0.parquet
python -m index_builder --crawl CC-MAIN-2026-25 --part 0 --where "content_languages like '%eng%'"
```

> **Cost:** `tech` is much larger than `industry` (N pages × domains). The default cap of 25
> captures ~all tech + the legal/contact pages; `--max-pages 0` is the *whole crawl* (~3B pages).

`cc-enrich-worker/run_crawl.sh` calls this (via `uv run python -m index_builder`) to produce each
shard before the Go pass.

## Install / test

```bash
uv run --with pytest pytest tests/        # or: pip install -e .[dev] && pytest
```

## Output schema

`root_domain, url, warc_filename, warc_record_offset, warc_record_length, content_languages`
