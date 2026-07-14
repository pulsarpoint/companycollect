# slovakia_financials — design

## Source
Slovak RÚZ (Register účtovných závierok) open REST API, `https://www.registeruz.sk/cruz-public/api`
(free, no key; browser UA required — F5 WAF rejects bare UAs). No bulk file exists, and the
statement feed pages by ascending id (`pokracovat-za-id`), so this is the §4 "API-only" case —
but instead of date partitions we use a **single id cursor** (DuckDB
`slovakia_financials.ingest_cursor`): one forward sweep walks all history in bounded chunks
(default 2000 statements/run, ~3-4 API calls each) and then naturally picks up new filings,
with zero partition bookkeeping. Deviation from §4 recorded here: id-cursor instead of
MonthlyPartitions because the feed's natural window IS the id sequence.

## Pipeline shape (4 independent assets, daily 05:00 job)
1. `slovakia_financials_raw_statements_s3` — sweep after the cursor; store RAW JSON bundles
   (statement + entity + reports, exactly as returned, including deleted statements and
   non-public stubs) as one NDJSON batch per run in S3 bucket `source-slovakia-financials`
   under `slovakia_financials/raw_statements/batch-{after:012d}-{last:012d}/statements.ndjson`
   (+ `manifest.json`). Templates dedup to `slovakia_financials/templates/template-{id}.json`.
   Advances the cursor only after a successful batch write.
2. `slovakia_financials_metrics_duckdb` — lists S3 batches, anti-joins
   `slovakia_financials.processed_batches`, decodes new ones (statutory tables → canonical
   metrics via `TEMPLATE_METRIC_ROWS`) and appends into the accumulating
   `slovakia_financials.financial_metrics` DuckDB table. Per-batch delete+insert on
   `source_batch_key` → reprocessing is idempotent; wiping `processed_batches` forces a full
   re-decode from S3 without any API traffic.
3. `slovakia_financials_usd_duckdb` — standard §7 separate USD step (shared
   `ExchangeRateClient`, EUR keyed on `period_end_date`), set-based UPDATE.
4. `slovakia_financials_metrics_clickhouse` — atomic full replace of
   `corpscout.sk_financial_metrics` (migration 000043 owns DDL; export tuple
   `SK_FINANCIAL_METRICS_COLUMNS`; `source_batch_key` stays DuckDB-only; refuses empty input).

## Decisions / deviations
- **Raw-to-S3 before decode**: the API is the bottleneck (rate-limited per-statement calls);
  raw batches make decode changes (new template families, bug fixes) replayable without
  re-fetching. This replaces the pre-2026-07 monolith asset that fetched+decoded+converted+
  exported in one step.
- **Cursor in DuckDB, not S3**: continuity with the deployed cursor state; the batch keys in
  S3 additionally encode the id ranges if the cursor ever needs manual reconstruction.
- **ClickHouse export is full-replace** (was append+ReplacingMergeTree dedup): DuckDB now
  holds full decoded history, so the standard atomic snapshot replace applies. The
  ReplacingMergeTree engine from migration 000043 is kept (harmless under EXCHANGE).
- **dlt not used for the load boundary** (pre-existing deviation): the per-statement JSON API
  doesn't fit a dlt resource; HTTP retries come from `dlt.sources.helpers.requests` inside
  `RuzClient`.
- Fetch failures are counted (`fetch_failed`) and skipped permanently (later ids advance the
  cursor) — same semantics as the monolith; acceptable because RÚZ re-lists statements under
  `zmenene-od` when they change.
- Contact data / translation / NACE (§8, §8b, §8c): this module covers financials only; the
  register/contacts source for SK is tracked separately (see
  `docs/slovakia_rpo-financials-research.md`).

## Ops notes
- All four assets share pool `slovakia_financials_duckdb` (limit 1) — the raw asset holds the
  cursor in the same DuckDB file.
- To re-decode everything: `delete from slovakia_financials.processed_batches;` then
  materialize `slovakia_financials_metrics_duckdb` (S3-only, no API traffic).
- To re-sweep from scratch: reset the cursor
  (`update slovakia_financials.ingest_cursor set last_id = 0`) — existing S3 batch objects
  for the same id ranges are overwritten in place (idempotent keys).
- First post-cutover export only contains what the DuckDB table holds. If the deployed DuckDB
  file was ever wiped while ClickHouse accumulated history, the full-replace would shrink the
  ClickHouse table — the empty guard catches the zero case; for a partial case, re-decode from
  S3 (`processed_batches` wipe) first.
