# estonia_ar design doc

> Worked example for `docs/source-design-doc-template.md`. Follows `docs/data-source-guidelines.md`.

## 1. Source overview
- **Country / registry**: Estonia — e-Business Register (Äriregister), publisher RIK.
- **Module**: `defs/estonia_ar/` · DuckDB `data/estonia_ar_source.duckdb` · pool `estonia_ar_duckdb`
- **ClickHouse tables**: `corpscout.ee_companies` (000024), `ee_financial_statements` (000025),
  `ee_financial_metrics` (000026)
- **Datasets** (all free bulk open data, **no credentials** — only the live API needs an agreement):
  | dataset | url (under `avaandmed.ariregister.rik.ee/sites/default/files`) | format | size | cadence |
  |---|---|---|---|---|
  | register | `/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip` | zipped CSV | 17 MB→95 MB | daily |
  | report-general | `/1.aruannete_yldandmed_kuni_<datestamp>_N.zip` | zipped CSV | 17 MB | monthly |
  | key indicators ×7 | `/4.<YEAR>_aruannete_elemendid_kuni_<datestamp>_N.zip` (2019–2025) | zipped CSV | ~22 MB ea | monthly |
- **Entity key**: `ariregistri_kood` (→ `reg_code`) · **counts**: 373,313 companies; ~1.5 M reports.

## 2. Ingest mode — bulk file, non-partitioned full-refresh
- **Why**: free full-snapshot bulk files exist → no reason to page the API (which also needs an
  agreement). 9 files total (register + report-general + 7 yearly), each a cumulative snapshot
  re-downloaded in full each run. *Not per-day* — "daily/monthly" is the source's refresh rate.
- **Format**: CSV, `;`-delimited, **UTF-8 BOM** on the register, dates **`DD.MM.YYYY`**, zipped
  (single CSV per zip). **Filenames**: register URL is stable; the financial filenames carry a
  **rotating monthly datestamp + `_N` suffix** → resolved from the dataset index at runtime
  (`resolve_financial_url`), not hardcoded.

## 3. Loading
- Register: a **narrow dlt row-resource** (`iter_estonia_ar_entity_rows`) — acceptable here because it
  applies the static-map translations and date parsing per row, over a 373 k register. Download→unzip
  via the streaming-retry helper + `zipfile`; read with `utf-8-sig` (BOM).
- Financials: **DuckDB `read_csv(all_varchar=true)`** after unzip (bulk, never row-by-row).
- Staging keeps `raw_entity`/`raw_financial_record` + `source_payload_hash` (DuckDB only).

## 4. Transform
- **Register → tier 1 (no transform): direct DuckDB→ClickHouse copy** of the `entities` table to
  `ee_companies`.
- **Financials → tier 2 (set-based SQL)**: union the 7 per-year EAV element tables, **conditional-
  aggregate pivot** (`max(case when elemendi_nimetus='…' …)`) keyed on `report_id`, joined to the
  report-general spine → wide `financial_statements`; then native metrics build + USD step.

## 5. ClickHouse schema — and DDL deviations
- Grain: `ee_companies` 1/company; `ee_financial_statements`/`ee_financial_metrics` 1/report.
  `ReplacingMergeTree`, `ORDER BY (reg_code)` / `(reg_code, report_id)`.
- **Deviations from the norm (with reasons):**
  - **`report_category_original/_en`** (report *size* category) instead of the plan's
    `statement_type_*` — Estonia has **no per-report statement type** (`tabel` is per-element, many
    per report); the size category (`valitud aruanne kategooria`) is the clean per-report enum.
  - **`gross_profit` column stays NULL** — no Estonian XBRL element maps to it.
  - **No `rounded_to_nearest`** — Estonian reports are in full EUR (unlike Latvia's scaling).
  - `first_entry_date Nullable(Date)` parsed from `DD.MM.YYYY`.
  - **No `raw_*`/`source_payload_hash` in the DDL** — fresh module, omitted per the 000021/022
    end-state; they stay in DuckDB staging and are dropped by `*_EXPORT_COLUMNS`.

## 6. Translation
| field | original | mechanism | source map |
|---|---|---|---|
| legal form | `legal_form_original` | static map | `EE_LEGAL_FORM_EN_BY_NAME` (~19) |
| legal-form subtype | `legal_form_subtype_original` | static map | `EE_LEGAL_FORM_SUBTYPE_EN_BY_NAME` (sparse) |
| status | `status_original` | static map | `EE_STATUS_EN_BY_CODE` (R/L/N/K) |
| report size category | `report_category_original` | static map | `EE_REPORT_CATEGORY_EN_BY_NAME` |
- **No LLM** in scope — `lihtandmed` has no free-text description; financial element names are already
  English XBRL. **Not translated**: company name, address (proper nouns).
- **Deferred (Phase 3)**: a company description/activity exists in the richer `…__yldandmed.json.zip`
  dataset → would be LLM-translated (`description_en` + `_translated_at/_provider/_model`) when added.

## 6b. Company contacts (§8b mandatory)
- **Table `ee_company_contacts`** (migration `000027`, `ReplacingMergeTree`,
  `ORDER BY (reg_code, contact_type, contact_value)`). One row per contact —
  `contact_type`/`contact_type_en` (`WWW`→Website, `EMAIL`→Email, `MOB`→Mobile, `TEL`→Phone,
  `FAX`→Fax, `MUU`→Other), `contact_value`, `is_current` (0 once `end_date` set), `source_url`.
- **Source = the richer `…__yldandmed.json.zip`** (the `lihtandmed` register CSV carries no
  contacts). Each company's `yldandmed.sidevahendid[]` array holds the contacts. **Website
  (`contact_type='WWW'`) is the domain-discovery signal** corpscout exists to capture.
- **Parse**: stream-download the zip → unzip → DuckDB `read_json(format='array', records=true,
  columns=…)` projecting **only** `ariregistri_kood` + `yldandmed.sidevahendid` out of the ~4.5 GB
  JSON → `unnest()` the array → one normalized row per contact (blank values dropped). Build SQL
  inlines literals (consistent with `financials.py`); refuses to replace on zero rows.
- Assets: `estonia_ar_general_data_duckdb` (single yldandmed download builds the contacts +
  industries DuckDB tables) → `estonia_ar_clickhouse_company_contacts` (`contacts.py` +
  `general_data.py` + `clickhouse.py`). No `raw_*`/hash columns; export == full tuple.

### Website/email → domain connection
- `ee_company_contacts` carries **`domain` + `domain_source`** (`'website'|'email'|''`), computed at
  build time: `root_domain(contact_value)` (shared `dagster_v3.domains` tldextract UDF) for WWW rows;
  for EMAIL rows the email suffix **only if it is unique to one company**. Counting *distinct
  companies* per suffix (migration `000028`) auto-drops every mail provider (gmail @190k companies)
  and shared accounting/formation-agent domain (kvatro.ee @286) — no magic threshold, just the
  uniqueness rule (`EMAIL_DOMAIN_MAX_COMPANIES`, default 1) + a small provider denylist backstop.
- **Why email matters**: only ~21k companies have a website but ~340k have an email on their own
  domain → email mining is the main domain-coverage source.
- **`ee_company_domains`** (migration `000029`) — deduped one row per `(reg_code, domain)` feeder
  (`company_domains.py`), website source preferred over email, exactly one `is_primary` per company;
  website rows carry `website_url`/`_normalized_url`/`_host`, email rows leave them empty.
- Flows into the **cross-source `company_website_domains`** graph: the `domains_clickhouse` asset adds
  an `ee_company_domains` UNION branch and a new **`domain_source`** column (migration `000030`;
  fi/no/wikidata branches backfilled `'website'`). `domains` rolls up per `root_domain` as before.

## 6c. Industry / NACE (EMTAK → unified NACE)
- **Table `ee_industries`** (migration `000031`, mirrors `no_industries`/`fi_industries`;
  `ReplacingMergeTree(resolved_at)`, `ORDER BY (reg_code, source_industry_code)`). One row per
  currently-declared activity.
- **Source**: `yldandmed.teatatud_tegevusalad` (sibling of `sidevahendid` in the same general-data
  JSON we already pull). Each entry has `emtak_kood` + `emtak_tekstina` (national EMTAK) **and
  `nace_kood` directly** (`"73.11"`) + `on_pohitegevusala` (primary flag) + `lopp_kpv` (validity).
- **Easiest of any source**: RIK supplies the NACE code, so `nace_mapping_method='source_provided'`,
  no fuzzy EMTAK→NACE mapping. `nace_revision` derived from EMTAK version (EMTAK 2025 → `NACE_REV_2_1`,
  earlier → `NACE_REV_2`); `nace_normalized_code` strips the dot. Activities with no `nace_kood` →
  `nace_mapping_status='unmapped'`. Ended activities (`lopp_kpv` set) are filtered out.
- **Unified id**: `nace_code`/`nace_revision` join `corpscout.nace_categories` (EU SPARQL reference,
  2,043 categories across Rev 2 + Rev 2.1). `description_en` is left null (EMTAK text is Estonian →
  future translation, columns already present).
- Assets: `estonia_ar_general_data_duckdb` builds the `ee_industries` DuckDB table (alongside
  contacts, from the **same single download**) → `estonia_ar_clickhouse_industries`
  (`industries.py` + `general_data.py` + `clickhouse.py`).

## 7. Currency
- All EUR (Estonia since 2011; reports 2019–2025) → **no legacy/unmapped-currency case**. Metrics
  carry `<metric>_amount_original` + `<metric>_amount_usd` + `fx_rate_to_usd/_date/_source`, keyed on
  `period_end_date`; conversion is the separate batched `apply_estonia_ar_usd_conversion` step.

## 8. Scheduling
- `estonia_ar_register_job` (entities + companies) — **daily 04:00**.
- `estonia_ar_financials_job` (full 13-asset chain via `.upstream()`) — **monthly, 5th 05:00**
  (after the new snapshot datestamp publishes). Crons staggered vs other sources; **default STOPPED**.
- `estonia_ar_general_data_job` (contacts + domains + industries from **one** yldandmed download) —
  **monthly, 8th 06:00**; the ~4.5 GB JSON is too heavy for daily, staggered after the financials run.
- `estonia_ar_full_refresh_job` (all 20 assets via group selection) — unscheduled, manual full run.

## 9. Issues found during processing
- **Literal `?` in report-general column names** (`"kas konsolideeritud?"`) collided with DuckDB's
  `?` positional-parameter markers → build SQL **inlines** the (escaped) values instead of `?` params.
- **`dg launch --assets "+leaf"` resolved only ONE hop** → the first financials run silently skipped
  the 8 downloads and failed (`Table financial_metrics does not exist`). Fixed by launching the
  **explicit 13-asset list**; the scheduled job uses **`AssetSelection.upstream()`** (full chain).
- **Competing manual runs** (UI + CLI) starved the single-writer `estonia_ar_duckdb` pool; a failed
  run can leak a pool slot (general risk) → `scripts/dagster-health-check.py --fix` detects/frees it.
- **Rotating monthly datestamp** in financial filenames → resolved from the index (§2).
- **USD step needed 2,641 distinct (currency, date) pairs in one call** — handled by the array-param
  `ExchangeRateClient` (the old `UNION ALL`-per-pair client would have hit ClickHouse code 572).

## 10. Verification
- Tests: `tests/test_estonia_ar_{resources,tables,assets,financials,financials_tables,metrics}.py`
  + migration registration; `uv run dg check defs`.
- Live (landed): `ee_companies` 373,313 (`legal_form_en`/`status_en` populated);
  `ee_financial_statements` & `ee_financial_metrics` 1,505,583 each; EUR rows USD-filled
  (sample `10128416/2025`: 3989 × 1.175 = 4687.08). Health-check clean afterward.
