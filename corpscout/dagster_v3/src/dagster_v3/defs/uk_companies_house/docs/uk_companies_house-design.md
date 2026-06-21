# uk_companies_house design doc

Ingest the UK Companies House "Basic Company Data" register — companies + their
SIC-derived NACE industry + registered address — into DuckDB → ClickHouse,
mirroring `france_sirene`.

## 1. Source overview
- **Companies House** free monthly bulk: `BasicCompanyDataAsOneFile-YYYY-MM-01.zip`
  (~493 MB zip, ~5.6M companies, comma CSV). **No credentials.** Base
  `https://download.companieshouse.gov.uk/`; the dated filename is **resolved live** from
  the index page `en_output.html` (don't hardcode the month).
- Country `GB`. Already **English** (no translation needed).

## 2. Ingest mode — bulk file, non-partitioned full-refresh
- Single cumulative monthly snapshot → non-partitioned full refresh. One DuckDB file
  (`uk_companies_house_source.duckdb`, stem ≠ dataset `uk_companies_house`), pool
  `uk_companies_house_duckdb` on every writer.

## 3. Loading
- **DuckDB-native `read_csv` with `normalize_names=true`** — the header has leading spaces +
  dots (` CompanyNumber`, `RegAddress.PostTown`, `SICCode.SicText_1`); normalize_names yields clean
  `companynumber`, `regaddressposttown`, `siccodesictext_1`. Multithreaded, never row-by-row.

## 4. Transform (plain DuckDB SQL)
- **gb_companies**: company_number, name, company_category (legal form — English), company_status
  (English), is_active (status='Active'), incorporation_date / dissolution_date (`%d/%m/%Y`),
  address (AddressLine1+2), postal_code, city (PostTown), county, country (England/Scotland/…),
  country_of_origin.
- **gb_industries**: SIC is in `SICCode.SicText_1..4` as `"62012 - Description"`. Unpivot the 4
  columns → one row per non-empty SIC (is_primary = SicText_1). Parse code (leading digits) + text.

## 5. ClickHouse schema (migration-owned, ReplacingMergeTree)
- **`gb_companies`** `ORDER BY (company_number)` — provenance + company_number, name,
  company_category, company_status, is_active, incorporation_date, dissolution_date, address,
  address_line_2, postal_code, city, county, country, country_of_origin. No `raw_*`/hash in DDL.
- Non-nullable Strings coalesced to `''`.

## 6. Translation — none (UK source is English).

## 6b. Contacts (§8b) — ABSENT
- Companies House basic data has **no email/phone/website**. Documented absent; no contacts table.

## 6c. Industry / NACE
- **`gb_industries`** mirrors `fr_industries`/`ee_industries`: `source_industry_code` = 5-digit SIC,
  `source_industry_code_set`='UK_SIC_2007', `description_original`/`_en` = the SIC text (English),
  `nace_revision`='NACE_REV_2' (UK SIC 2007 ≈ NACE Rev 2), `nace_code` = first 4 digits `NN.NN`,
  `nace_normalized_code` = first 4 digits, `nace_mapping_method`='national_truncation'. Placeholder
  SIC (99999 Dormant → `9999`) → `unmapped`. Joins `corpscout.nace_categories`.

## 7. Currency — N/A (no monetary values in basic data).

## 8. Scheduling
- `uk_companies_house_register_job` (companies + industries) from the ONE monthly download (raw →
  both via `.upstream()`) → **monthly**, staggered cron; default STOPPED.

## 9. Deferred
- **Financials** — full accounts are iXBRL filings (Companies House accounts API / document bulk),
  a separate XBRL effort. **Contacts** — not in open data. Officers/PSC — separate datasets.

## 10. Verification
- `uv run pytest tests/test_uk_companies_house.py tests/test_clickhouse_migrations.py -q` +
  `uv run dg check defs`. Migrations apply. Materialize live; check `gb_companies` count +
  English category/status + address; `gb_industries` rows join `nace_categories`. TDD; commit by path.
