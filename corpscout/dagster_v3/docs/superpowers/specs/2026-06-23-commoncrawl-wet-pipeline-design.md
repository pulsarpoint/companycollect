# CommonCrawl WET → Industry Pipeline (+ one-file benchmark) — Design

**Status:** design (plan-only). Scoped to the WET pipeline and a one-file benchmark.
The WARC/technology pipeline, sharded runner, ledger-driven durable loop, and Dagster
triggers are **separate follow-up specs** (this doc establishes the shape they plug into).

## Context

CommonCrawl publishes one crawl per month, id `CC-MAIN-YYYY-WW` (year + ISO week of the
crawl start; e.g. `CC-MAIN-2026-25` = June 2026). 125 crawls exist back to 2008. Each crawl
is **100,000 WET + 100,000 WARC + 100,000 WAT** files (1:1). WET ≈ 150 MB/file (~15 TB/crawl,
plaintext, no HTTP headers); WARC ≈ 1 GB/file (~100 TB/crawl, full HTTP response).

We process **one crawl per month** — keeping up with new releases is enough; historical
backfill is optional later (same machinery, enqueue an older crawl when idle).

The classification stack already exists and is tested: `nace_embed` (NACE reference matrix +
`classify_pages` with the margin/division-consensus gate + page-type prototypes),
`page_types` (keyword `match_signal`), `classifier.PageClassifier` (keyword → embedding
page-type/NACE → LLM tail), and `ingest.process_wet_to_clickhouse`. Reference data lives in
ClickHouse (`nace_category_embeddings`, `page_type_exemplars`, migrations 044/045) and as
local `.npz` snapshots. Output tables exist (migrations 046 `commoncrawl_domains`,
047 `commoncrawl_technologies`, 048 `commoncrawl_page_signals`).

## Goals

1. **A full one-WET-file flow**: download a WET file → local S3 → process with the
   `PageClassifier` → write **one Parquet per WET file** (column order = `commoncrawl_domains`)
   → the Parquet is directly loadable into ClickHouse.
2. **Benchmark it** to measure, on a real file: download time, process time, homepages/file,
   Parquet size (bytes/row and bytes/file), and project to a full crawl (×100,000):
   core-time, local Parquet storage, download volume. These numbers decide the worker pool
   size and storage budget for the monthly run.

Non-goals (separate specs): WARC pipeline, sharding/worker fleet, ledger, Temporal loop,
Dagster trigger/sensor, AWS backfill.

## Architecture (where this fits)

Two **independent** monthly pipelines — different volume, speed, and cadence, so never
coupled in one runner:

- **WET → industry** (this spec): homepages only → `commoncrawl_domains`. Light (~15 TB,
  homepage fraction). Classifier + embedder loaded once, reused across files.
- **WARC → technology** (future): all pages → `commoncrawl_technologies` +
  `commoncrawl_page_signals`. Heavy (~100 TB), sharded across workers, wappalyzer sidecar.

Both will share a future **ledger** keyed `(crawl_id, kind, file_index)` and a durable loop;
Dagster triggers + monitors. Not built in this spec.

### Decision: one Parquet per WET file

**Yes — 1:1 file→Parquet.** Rationale: a WET file is the atomic download+process unit, so
1:1 makes processing **resumable** (Parquet exists ⇒ file done), **parallelizable** (workers
never share a file), and maps 1:1 to the future ledger row. ClickHouse loads it with no custom
code (`INSERT INTO corpscout.commoncrawl_domains SELECT * FROM s3('…/CC-MAIN-…-NNNNN.parquet')`,
or glob a shard). The only risk is many small files (100k/crawl); the benchmark's bytes/file
number tells us whether to later batch N files into one segment-Parquet. **Start 1:1; revisit
only if files are wastefully small.**

## Components

1. **`ingest.build_wet_domain_rows(source, *, classifier, crawl_id, …) -> list[tuple]`**
   — extract the existing row-building out of `process_wet_to_clickhouse` (stream homepages →
   classify → build `commoncrawl_domains` tuples). `process_wet_to_clickhouse` becomes
   `build_wet_domain_rows(...)` + `_insert(...)`. No behaviour change; just a shared seam so
   the row-builder feeds both the ClickHouse sink and the Parquet sink.

2. **`ingest.DOMAINS_PARQUET_SCHEMA` + `ingest.write_domain_rows_parquet(rows, out_path)`**
   — pyarrow schema whose column order == `DOMAINS_COLUMNS` (so the Parquet is CH-loadable),
   list/timestamp types matching the migration. Writes one Parquet.

3. **`scripts/benchmark_wet.py`** — the one-file flow + measurement:
   - load `NaceReference`/`PrototypeSet` from local `.npz`, `EmbeddingClient.from_env()`,
     optional LLM (env); build `PageClassifier`.
   - resolve latest crawl + first WET url (`segment.latest_crawl` / `first_wet_url`).
   - download to a temp file; if S3 enabled (default), `put_object` then `download_file`
     back (mirrors the real download→local-S3→process path); `--no-s3` skips the round-trip.
   - `build_wet_domain_rows(...)` → `write_domain_rows_parquet(...)`.
   - print per-phase timings, homepages, Parquet size (bytes/row, bytes/file), industry /
     page-type / email counts, and the ×100,000 projection (core-days, storage, download TB).
   - flags: `--segment-index`, `--limit`, `--no-s3`, `--no-llm-tail`.

## Data flow

```
latest_crawl() ─▶ first_wet_url(segment_index)
       │
       ▼  download (requests, streamed)
   temp .warc.wet.gz ──(optional)──▶ local S3 put_object ──▶ download_file back
       │
       ▼  build_wet_domain_rows(classifier)   # homepages → keyword/embedding/LLM-tail
   commoncrawl_domains rows
       │
       ▼  write_domain_rows_parquet
   data/commoncrawl/benchmark/CC-MAIN-…-NNNNN.warc.wet.parquet   # 1 file, CH-loadable
       │
       ▼  (manual, to verify) INSERT INTO corpscout.commoncrawl_domains SELECT * FROM file(...)
```

## Error handling

- Streamed download with the existing `requests` retry session; a malformed record never
  crashes the loop (decode with `errors="replace"`, already the case).
- Empty file → zero rows → an empty Parquet with the correct schema (still CH-loadable).
- Embedding endpoint required; LLM tail optional (`--no-llm-tail`, or absent
  `COMMONCRAWL_LLM_BASE_URL` → embedding-only). Benchmark reports which path ran.

## Testing

- Unit (offline, existing fixtures): `build_wet_domain_rows` returns the same rows the
  current `process_wet_to_clickhouse` builds (refactor is behaviour-preserving — assert via the
  warcio WET fixture). `write_domain_rows_parquet` round-trips to a Parquet whose schema ==
  `DOMAINS_PARQUET_SCHEMA` and whose columns match `DOMAINS_COLUMNS` (read back, check a row).
- The benchmark script itself is run manually (live endpoints), not unit-tested.

## Open questions (resolved)

- **Parquet granularity:** 1:1 file→Parquet (above).
- **Benchmark classifier mode:** run embedding-only first (`--no-llm-tail`) for a clean core
  throughput number, then optionally with the LLM tail to measure its added cost; the LLM
  endpoint shares the GPU with embeddings, so it may be down when embeddings are up.
- **Storage location of Parquet:** local dir for the benchmark; the production pipeline will
  decide local-vs-S3 staging based on the measured bytes/file.
