# finland (ytj + resolved + xbrl) design doc

> Per `docs/source-design-doc-template.md` / `docs/data-source-guidelines.md`.
> Finland spans **three modules**; this is the umbrella doc. The reference example for a
> **dbt resolve layer**, an **LLM-translated** register, and the **partitioned-API** financials shape.

## 1. Source overview
- **Country / registry**: Finland — PRH (Patentti- ja rekisterihallitus) open data.
- **Modules**:
  - `finland_ytj/` — bulk register load · DuckDB `data/finland_ytj.duckdb` · pool `finland_ytj_duckdb`
  - `finland_resolved/` — dbt over the ytj DuckDB → ClickHouse (shares the ytj DuckDB + pool)
  - `finland_xbrl/` — XBRL financials · own DuckDB + S3 object store `source-finland-prh-xbrl`
- **ClickHouse**: resolved `fi_companies`/`fi_names`/`fi_websites`/`fi_industries` (000005–000007,
  000010, 000014); XBRL raw tables `fi_xbrl_*` (000011) — **not exported yet** (see §9).
- **Datasets** (free, no auth): YTJ open-data API `avoindata.prh.fi/opendata-ytj-api/v3`
  (`finland_ytj_all_companies`); XBRL API `avoindata.prh.fi/opendata-xbrl-api/v3`.
- **Entity key**: `business_id`.

## 2. Ingest mode — two shapes in one source
- **Register (ytj)**: bulk all-companies download → DuckDB → **non-partitioned full-refresh**.
- **Financials (xbrl)**: **partitioned-API** — `MonthlyPartitionsDefinition` over the report
  registration window; downloads XBRL XML per window, stores it in S3, parses with **arelle**, then
  dbt. *This is the reference impl for the §4 "per-window API ⇒ partitioned incremental" rule.*

## 3. Loading
- ytj: dlt source pulls the all-companies dataset into `finland_ytj.duckdb`
  (`finland_ytj_all_companies_duckdb`, with an `all_companies_non_empty` asset check).
- xbrl: `…_financial_reports_duckdb` (report metadata) → `…_raw_xml_documents` → `…_xml_documents`
  (S3) → `…_parsed_tables` (arelle, **partitioned**) → dbt.

## 4. Transform
- **Resolved → tier 3 (dbt)**: `finland_resolved_dbt_assets` builds `fi_companies`/`fi_names`/
  `fi_websites`/`fi_industries` from the ytj DuckDB (a genuine multi-model DAG over nested YTJ data →
  dbt earns its keep here), then `finland_ytj_resolved_clickhouse` exports the resolved tables.
- **XBRL → tier 3 (dbt)** over the arelle-parsed facts → `fi_financial_metrics_long` (EAV).

## 5. ClickHouse schema — deviations
- Resolved tables are **normalized per relation** (companies/names/websites/industries) rather than
  one wide table — Finnish YTJ data is relational (multiple names/addresses per company).
- XBRL stores facts **long/EAV** (`fi_financial_metrics_long`) with `amount_original`/`amount_usd` +
  fx; raw XBRL columns (`raw_xml`/`raw_value`) live in the raw tables.

## 6. Translation — LLM
- The resolved tables carry `legal_form_description_en` + `…_translated_at`/`…_translation_provider`/
  `…_translation_model` (and industry `description_en` + provenance) → populated by the **LLM
  translation pipeline** (the `_provider`/`_model` columns are the tell). Mirrors `norway_brreg`'s
  approach. Static-mappable enums could move to static maps later for determinism.

## 7. Currency
- **EUR**. XBRL `fi_financial_metrics_long` carries `amount_original` + `amount_usd` +
  `fx_rate_to_usd`/`fx_rate_date` (conversion lands when the XBRL→ClickHouse export does).

## 8. Scheduling
- `finland_ytj_resolved_job` (register + dbt + export, via `.upstream()` across modules) — **daily
  04:45**, default STOPPED. The XBRL financials are **not scheduled** yet (see §9).

## 9. Issues / open items
- **finland_xbrl has no ClickHouse export asset** — its 5 assets stop at dbt/DuckDB, and
  `…_parsed_tables` is **partitioned** (arelle parse per registration-month). It needs its own design
  pass (export path + partition/cadence) before it gets a schedule; deliberately deferred.
- `finland_resolved` dbt reads `finland_ytj.duckdb` via `FINLAND_YTJ_DUCKDB_PATH`, set
  **unconditionally** from the resource default so a stale env var can't silently point dbt at a
  different file.
- Provenance (`source_payload_hash`) dropped from the resolved ClickHouse exports (000022) — kept in
  DuckDB staging only.

## 10. Verification
- Tests `tests/test_finland_resolved_*.py`, `tests/test_finland_ytj_*.py`, `tests/test_finland_xbrl_*.py`
  (incl. the resolved-schedule test). Live: resolved `fi_*` tables in ClickHouse; XBRL DuckDB-only for now.
