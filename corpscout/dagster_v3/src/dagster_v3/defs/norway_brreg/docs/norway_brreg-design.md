# norway_brreg design doc

> Per `docs/source-design-doc-template.md` / `docs/data-source-guidelines.md`.
> The reference example for a **translation-gated** (LLM, Temporal-driven) source.

## 1. Source overview
- **Country / registry**: Norway — Brønnøysundregistrene (BRREG).
- **Module**: `defs/norway_brreg/` · DuckDB `data/norway_brreg_source.duckdb`
  (+ `data/norway_brreg_translation_queue.duckdb`) · pool `norway_brreg_duckdb`
- **ClickHouse**: `corpscout.companies` (000003), `financial_statements` (000004); provenance drop 000022.
- **Datasets** (free, no auth): Enhetsregisteret bulk entities `data.brreg.no/enhetsregisteret/api`
  (**gzipped JSON array**); per-company financials `…/regnskapsregisteret/regnskap/{org}` (**JSON API**).
- **Entity key**: `org_number` · **counts**: ~1.2 M companies.

## 2. Ingest mode — hybrid
- **Entities**: one **bulk gzipped-JSON** download, streamed with `ijson` (constant memory over the
  large array) → non-partitioned full-refresh. *Bulk file ⇒ full-refresh.*
- **Financials**: **per-company API fetches** (`regnskapsregisteret`) keyed off the entities org list
  — heavy (many requests); checkpoints into `financial_fetches`. (A per-record API, but driven by the
  entity list rather than a date window, so it's fetch-per-entity rather than partitioned.)

## 3. Loading
- Entities via dlt + `_stream_gzip_json_array` (ijson). Financials via the BRREG regnskap client into
  `financial_fetches` (with `fetch_status`/`fetched_at`), then normalized.
- Raw payloads + `source_payload_hash` kept in DuckDB only.

## 4. Transform
- `financial_statements_duckdb` normalizes the fetched JSON into the wide statements **and applies
  NOK→USD** in the normalize step (set-based SQL). Companies table is finalized after translation.

## 5. ClickHouse schema — deviations
- `companies` `ORDER BY (org_number)`; `financial_statements` `ORDER BY (org_number, …)`.
- Financial amounts carry `*_amount_original` (**NOK**) + `*_amount_usd`. `raw_*`/hash dropped (000022).

## 6. Translation — LLM (the distinguishing feature)
- Free-text fields are translated by the **LLM translation-service over Temporal**, not static maps:
  e.g. `articles_purpose_original`→`articles_purpose_en`, plus activity/description and NACE
  descriptions. The DDL carries the `_en` companions.
- **Orchestration** (the non-obvious part): `entities` → `translation_queue` *seeds the queue and
  starts-or-reuses* a serialized Temporal workflow (returns immediately; runs async) → the
  **completion sensor** (default RUNNING, polls every 60 s) fires `…_translation_completion_job` when
  the workflow is `COMPLETED` → `translations_applied` (writes EN columns into DuckDB) →
  `clickhouse_companies` (publishes). Legal form uses a static map.

## 7. Currency
- Native **NOK** → USD via the shared `ExchangeRateClient` (NOK is in the ECB set, so no legacy gap
  like Latvia's LVL). `_original`/`_usd` + fx columns on the statements.

## 8. Scheduling — coordinated monthly refresh (NOT the naive template)
- `norway_brreg_refresh_job` (monthly **7th**): loads `entities` once, **kicks off** the
  start-or-reuse translation workflow **and** runs the financials chain. Companies land via the
  completion sensor. Monthly because translation is long-running and the per-company fetch is heavy.
- Excludes `translations_applied`/`clickhouse_companies` (sensor's domain). Default STOPPED.

## 9. Issues found during processing
- **The automated loop never published companies**: the completion job selected only
  `translations_applied` (DuckDB EN columns), not `clickhouse_companies` (the actual export) →
  **added `clickhouse_companies` to the completion job** so the sensor-driven completion lands
  companies in ClickHouse.
- **A failed run can leak the `norway_brreg_duckdb` pool slot** (general Dagster behavior on ungraceful
  crash) → blocks later runs in QUEUED forever; `scripts/dagster-health-check.py --fix` detects/frees.
- Translation is slow/expensive (a single run was mid-flight at ~42 % during development) → kick off
  **monthly**, never daily; the start-or-reuse workflow makes re-triggering mid-flight safe.
- The shared `ExchangeRateClient` `UNION ALL`-per-pair query would have hit code 572 at Norway's
  statement scale too — fixed at the root (array params) during the Latvia/Estonia work.

## 10. Verification
- Tests `tests/test_norway_brreg_*.py` (incl. the refresh-schedule / completion-loop test). Live:
  resolved `no_companies` ~1.16 M. Confirm `_en` columns populate after the workflow completes.
