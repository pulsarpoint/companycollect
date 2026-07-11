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
- The raw ZIP is a durable asset in RustFS/S3, not a temporary implementation detail:
  `uk_companies_house_register_archive_s3` → `uk_companies_house_raw_duckdb`.
- Register objects live in bucket `source-uk-companies-house` under immutable,
  content-addressed keys:
  `raw/register/published_date=YYYY-MM-01/sha256=<digest>/<source-filename>.zip`.
  Each object has an immutable sibling `metadata.json` with source URL, publish date,
  size, SHA-256, object key, and sync timestamp. There is no mutable `latest` pointer.

## 3. Loading
- The S3 asset resolves and downloads the live Companies House URL with whole-stream retry,
  then uploads the verified ZIP. The DuckDB asset performs no Companies House HTTP request;
  it selects the latest stored register archive and downloads a temporary local copy from S3.
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
- `uk_companies_house_register_job` materializes the complete register chain
  (`register_archive_s3` → raw DuckDB → companies/industries → ClickHouse) via `.upstream()` →
  **monthly**, staggered cron; default STOPPED.

## 9. Financials — XBRL accounts (Phase 1: latest archive)
- Source: Companies House **Accounts Data Product** — daily iXBRL archives
  (`Accounts_Bulk_Data-YYYY-MM-DD.zip`, ~283 MB), URL resolved from the accounts index. Free.
- Parsed with the shared **`xbrl_common`** extractor. Per filing → company_number
  (from the iXBRL entity identifier) + reporting period end + a canonical metric set mapped from
  FRC core concepts (`UK_METRIC_CONCEPTS`). **`gb_financial_metrics`** (migration `000037`), native
  GBP + **GBP→USD** via the shared `ExchangeRateClient` (separate step, keyed on period_end_date).
- **Coverage caveat**: balance-sheet items (net assets, fixed/current assets, cash) are broadly
  tagged even by micro-entities; **turnover/profit only by companies that file a P&L** (medium+),
  so those are frequently NULL. **PDF-only filings carry no XBRL** → no metrics.
- Daily accounts archives use their own independent raw boundary:
  `uk_companies_house_accounts_archives_s3` → accounts consumers. They never depend on the
  monthly register DuckDB asset. Objects use
  `raw/accounts/published_date=YYYY-MM-DD/sha256=<digest>/<source-filename>.zip` in the same
  source bucket, with the same immutable metadata contract.
- **Four write paths, all → `gb_financial_metrics` (ReplacingMergeTree, no truncate — append +
  dedup by company):**
  1. **Latest archive** (`uk_companies_house_financials_job`) — parse the newest archive already
     persisted by `uk_companies_house_accounts_archives_s3` (proof / manual).
  2. **Forward-only incremental** (`uk_companies_house_accounts_incremental`, daily) — a cursor (in the
     source DuckDB) tracks the last successfully exported archive; each run parses stored S3 archives
     published since the cursor and appends them. The cursor advances only after ClickHouse succeeds,
     so a failed run reuses the same stored ZIPs on retry.
     Companies file annually, so over ~12 months this converges on the **latest annual report for every
     iXBRL filer** — free bulk, no API limits. `max_archives` bounds per-run runtime.
  3. **On-demand API** (`uk_companies_house_api_financials_job`, config `company_numbers`) — a
     four-asset chain for a specific company's latest accounts via the CH Filing History + Document
     API (needs the free `COMPANY_HOUSE` key; reads document metadata and skips PDF-only filings):
     `uk_companies_house_api_accounts_documents_s3` →
     `uk_companies_house_api_financial_metrics_duckdb` →
     `uk_companies_house_api_financial_metrics_usd_duckdb` →
     `uk_companies_house_api_financial_metrics` (ClickHouse append).
     The raw asset stores each iXBRL document at
     `raw/api_accounts/company_number=<number>/filing_date=YYYY-MM-DD/sha256=<digest>/accounts.xhtml`
     and writes an immutable `raw/api_accounts/batches/run_id=<run-id>/catalog.json`. The catalog is
     the downstream contract and records requested, stored, and missing companies plus source URLs,
     content type, hash, size, and object key. Parsing and FX assets never call Companies House.
     `CompaniesHouseResource` is the injected API boundary: it owns the `COMPANY_HOUSE`
     credential, base URL, timeout, retrying HTTP session, and Filing History/Document API calls.
  4. **PDF-only PoC** (`uk_companies_house_pdf_financial_metrics`, config `company_numbers`) — for
     companies whose latest accounts are **PDF-only** (often scanned images), OCR the PDF
     (`pdftoppm` + `tesseract`) and extract metrics with the platform LLM (`pdf_extract.py`, the
     `TRANSLATION_PROVIDER_LOCAL_*` OpenAI-compatible endpoint; captures currency + unit scale +
     confidence). Tagged `source_slug='uk_companies_house_accounts_pdf'` — **lower trust than XBRL**
     (OCR noise, layout variety, model judgement). On-demand only; not bulk. Validated on
     AstraZeneca's scanned 20-F: revenue $58,739M, operating profit $13,743M (USD, millions, conf 0.95).

## 10. Deferred
- **12-month backfill** to seed prior filings immediately (the incremental otherwise accrues forward).
- **Contacts** — not in open data. **Officers/PSC** — separate datasets.
- **Raw retention policy** — no raw register or accounts ZIP is deleted in the initial rollout.
  Add a separate downstream retention asset only after replay requirements and storage costs are measured.

## 10b. Replay / recovery
- If the local source DuckDB is lost, rematerialize the relevant DuckDB asset and its downstream
  chain from the persisted S3 archive; no source redownload is required.
- To retry accounts processing after a failed ClickHouse append, rerun the incremental job. Because
  the cursor was not advanced, it selects the same stored archive dates.
- Failed downstream steps in an on-demand API run reuse that run's immutable catalog and
  content-addressed iXBRL objects. A normal new API job run intentionally creates a new catalog
  after checking Companies House for each configured company's latest filing.
- The register and accounts streams intentionally share the source DuckDB resource and concurrency
  pool but have no data-lineage dependency on each other.

## 11. Verification
- `uv run pytest tests/test_uk_companies_house.py tests/test_clickhouse_migrations.py -q` +
  `uv run dg check defs`. Migrations apply. Materialize live; check `gb_companies` count +
  English category/status + address; `gb_industries` rows join `nace_categories`. TDD; commit by path.
