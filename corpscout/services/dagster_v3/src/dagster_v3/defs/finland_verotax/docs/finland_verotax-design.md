# finland_verotax design doc

> Records *decisions*, not code. Follows `docs/data-source-guidelines.md`; deviations called out.

## 1. Source overview

- **Country / registry**: Finland — Finnish Tax Administration (Verohallinto), public corporate
  income tax data ("Yhteisöjen tuloverotuksen julkiset tiedot").
- **Module**: `defs/finland_verotax/` · DuckDB file `data/finland_verotax_source.duckdb` ·
  pool `finland_verotax_duckdb`
- **ClickHouse tables**: `corpscout.fi_tax_records` (migration `000144`)
- **Datasets used**:
  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | tax year 2024 | vero.fi `/contentassets/a7b04897…/yhteisö_tuloverotus_julk_2024.csv` | CSV | 27 MB | static snapshot | none |
  | tax years 2020–2023 | same contentassets folder, per-year filenames | CSV | 23–26 MB each | static snapshots | none |
  License **CC-BY-4.0** ("Source: Finnish Tax Administration" attribution). A new file appears
  each early November for the preceding tax year.
- **Entity key**: `business_id` (Y-tunnus, `1234567-8`) · **record count**: ~385k/year,
  ~1.9M rows across 2020–2024.
- **What this is (and is not)**: taxable income and assessed taxes per corporate taxpayer.
  It is a *profitability/size signal with universal coverage* — NOT a financial statement.
  `taxable_income` must never be mapped onto `profit_loss` (loss carryforwards and group
  contributions make them diverge). It complements `finland_xbrl`, which has full statements
  for only ~5% of companies until the 2027/2028 mandatory iXBRL filing.

## 2. Ingest mode (§2) — and why

- Chosen: **bulk file full-refresh, non-partitioned** (guidelines mode 1).
- Why: five static per-year CSV snapshots exist; nothing to page, nothing to partition.
  Files never change after publication (except rare corrections), so a full re-download of
  all five is cheap (~130 MB/year total) and atomic.
- Format quirks: **Latin-1 (ISO-8859-1)** encoding, `;` delimiter, **decimal comma**,
  bilingual FI|SV header row, no quoting observed.
- **Column shape varies by year**: 2020–2022 have 9 columns (include
  "Ennakot yhteensä" = total prepayments); 2023–2024 have 8 (prepayments dropped).
  The loader sniffs the header column count and applies the matching positional name list;
  `prepayments_total` is NULL for 8-column years.
- **Filenames rotate per year with no stable pattern** (three naming schemes across five
  years), so per §3 the year→URL map is **resolved at runtime from the vero.fi open-data
  page** (regex over `contentassets/*.csv` hrefs: must contain `yhteis` + `tuloverotus`,
  must not contain `muutos` which is the amendments file). A hardcoded fallback map covers
  the five known years if the page scrape fails; a newly published year is logged so
  `EXPECTED_YEARS` can be extended.

## 3. Loading (§3)

- Reader: DuckDB `read_csv(..., delim=';', header=false, skip=1, all_varchar=true,
  encoding='latin-1', names=[…])` — C++ reader, never row-by-row Python.
- Positional `names` (not the file header) because the header is bilingual free text with
  non-ASCII characters.
- Staging shape: one raw table per year (`raw_tax_records_<year>`), each a separate output
  of one multi-asset (per-file checkpoint per §3); `source_url` stamped at load time.
- Refuse-empty: a zero-row load raises `ValueError`.

## 4. Transform (§5)

- Mechanism: **set-based DuckDB SQL** (one union + cast statement) — no dbt (not a DAG).
- Shape: union the per-year raw tables → typed `tax_records`:
  - decimal comma → `try_cast(replace(replace(x,' ',''),',','.') as decimal(38,2))`
  - `municipality_raw` `"091 Helsinki"` → `municipality_code` + `municipality_name` via regex;
    rows without a leading numeric code keep code `''`.
  - `period_end_date = make_date(tax_year, 12, 31)` — synthetic FX key; the file carries no
    fiscal period dates (deviation from statement sources, documented here).
  - provenance: `raw_tax_record` JSON + `source_payload_hash` kept in DuckDB only.

## 5. ClickHouse schema — and DDL deviations

- Table + grain: `corpscout.fi_tax_records`, 1 row per `(business_id, tax_year)`.
- `ORDER BY (business_id, tax_year)` (both non-nullable) · ENGINE ReplacingMergeTree.
- Metric columns: `taxable_income`, `taxes_total`, `prepayments_total`, `tax_refund`,
  `residual_tax`, each as `_amount_original` + `_amount_usd` pair + the three fx columns.
- **Deviations**: no `period_start_date` (source has none); `prepayments_total_*` is NULL
  for tax years ≥2023 (column dropped by Vero); `municipality_name` is
  `LowCardinality(String)` (~310 distinct values).
- Export subset: `FI_TAX_RECORDS_EXPORT_COLUMNS` drops `raw_tax_record` /
  `source_payload_hash`.

## 6. Translation (§8)

- **No translated fields.** All text columns are proper nouns (taxpayer name, municipality
  name) which the standard excludes from translation. No loader asset.

## 6b. Contacts (§8b) — assessed

- **No contact data exists in this source** (columns: year, business id, name, municipality,
  four money amounts). No website/email/phone anywhere in the dataset.
- **Deviation**: the canonical `<src>_company_contacts`/`<src>_company_domains` pair is
  **not** created for this module. This is a supplement keyed to the existing Finland
  register — `finland_ytj` already writes the canonical `fi_company_contacts` /
  `fi_company_domains` pair for the same `business_id` space, so an empty duplicate pair
  would add tables without adding a source of truth.

## 6c. Industry / NACE (§8c) — assessed

- **No industry/activity data in this source.** `fi_industries` (from `finland_ytj` TOL2008)
  already covers the same business ids; joining is a consumer concern.

## 7. Currency (§7)

- Native currency: **EUR** for all rows (source publishes EUR only).
- USD conversion is the separate `finland_verotax_records_usd_duckdb` step via the shared
  `ExchangeRateClient`, keyed on the synthetic `period_end_date` (Dec 31 of the tax year),
  batched ≤50 requests/call; fills `fx_rate_to_usd`/`fx_rate_date`/`fx_source`.

## 8. Scheduling (§9)

- Job `finland_verotax_job` = `AssetSelection.assets("finland_verotax_records_clickhouse").upstream()`.
- Schedule: **yearly**, Nov 12 05:50 Europe/Belgrade (`50 5 12 11 *`) — Vero publishes in
  early November; a few days' margin. Minute staggered vs other sources. Default STOPPED
  until validated, per the standard.
- New tax year: runtime URL discovery picks the file up automatically once its year is added
  to `EXPECTED_YEARS` (one-line change; the discovery log announces unknown years).

## 9. Issues found during processing

- **Per-year schema drift**: 2020–2022 files carry a 9th column (prepayments) that
  2023–2024 dropped → header sniff + positional name lists; do not trust a fixed shape.
- **Three different filename conventions across five years** → runtime href discovery from
  the open-data page instead of hardcoded URLs (fallback map kept for resilience).
- **avoindata.suomi.fi CKAN dataset is stale** (only 2011–2014) — vero.fi is the canonical
  index for recent years, despite the portal existing.
- **Unquoted literal `"` in ~150 company names** (`UAB "Tokajus"`) breaks DuckDB's strict
  CSV sniffer in quoted mode → read with `quote='', escape=''` (the source never
  CSV-quotes fields; synthetic fixtures alone did not catch this — the real 2024 file did).
- **Finnish inflection in filenames**: 2020–2022 files say "tuloverotuk*sen*" (genitive),
  2023+ say "tuloverotus" → the discovery filter matches the stem `tuloverotu`.
- Latin-1 + decimal comma + bilingual headers (see §3/§4 handling).

## 10. Verification

- Tests: `tests/test_finland_verotax_tables.py` (column contracts),
  `tests/test_finland_verotax_records.py` (8/9-col fixtures through load+transform+usd),
  migration coverage in `tests/test_clickhouse_migrations.py`.
- Live: migrate → materialize the chain → spot-check `fi_tax_records` counts (~1.9M),
  `_usd` populated, a known row's `original × rate`.
