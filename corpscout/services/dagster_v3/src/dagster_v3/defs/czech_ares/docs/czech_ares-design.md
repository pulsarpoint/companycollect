# czech_ares design doc

Ingest the Czech business register — companies + NACE industry + registered address
— into DuckDB → ClickHouse, mirroring `france_sirene`/`uk_companies_house`.

## 1. Source overview
- The **bulk** source is the Czech Statistical Office **RES open data** (not the ARES
  REST API, which is per-company): **`res_data.csv`** at
  `https://opendata.csu.gov.cz/soubory/od/od_org03/res_data.csv` — ~540 MB, ~7M
  economic subjects, comma-delimited UTF-8, quoted. **Free, no credentials.**
  Stable filename (no date); refreshed **twice monthly** (15th + month end).
- Country `CZ`.

## 2. Ingest mode — bulk file, non-partitioned full-refresh
- One cumulative snapshot at a stable URL → non-partitioned full refresh. One DuckDB
  file (`czech_ares_source.duckdb`, stem ≠ dataset `czech_ares`); pool
  `czech_ares_duckdb` on every writer.

## 3. Loading
- **DuckDB-native `read_csv`** (all_varchar; the file is ~540 MB) — never row-by-row.
  Relational casts in downstream SQL.

## 4. Transform
- **cz_companies** (plain DuckDB SQL): `ICO` / `ico` is the Czech **IČO**
  (`Identifikační číslo osoby`) — the stable 8-digit company/economic-subject
  identifier and the primary join key for Czech companies. It maps from `ICO`;
  `FIRMA` (name), `FORMA` (legal-form code →
  static EN map `CZ_LEGAL_FORM_EN_BY_CODE`), status from `DDATZAN` (terminated date
  empty → active), `DDATVZN` (established) / `DDATZAN` (terminated) dates, address
  assembled from `ULICE_TEXT`+`CDOM`/`COR`, `PSC`, `OBEC_TEXT`, district `OKRESLAU`,
  `KATPO` (size category), `CISS2010` (institutional sector). **Address is in the same
  file** (no separate phase, unlike France).
- **cz_industries**: `NACE` (CZ-NACE 2008 → NACE Rev2) and `NACE2025` (→ NACE Rev2.1)
  — one main activity per company. Take the first 4 digits → `NN.NN` NACE code.
- **cz_company_contacts** (ClickHouse transform): RES/ARES do not expose structured
  contacts, but many `cz_companies.name` values contain embedded domains or email
  addresses. The contact asset scans company names for email/domain candidates,
  normalizes each to a root domain, validates the domain against
  `corpscout.commoncrawl_domains`, and falls back to DNS NS-record resolution when
  the domain is absent from Common Crawl. Common Crawl validation gets higher
  confidence (`0.95`) than DNS-only NS validation (`0.70`). Invalid/unresolvable
  candidates are dropped.

## 5. ClickHouse schema (migration-owned, ReplacingMergeTree)
- **`cz_companies`** `ORDER BY (ico)` — provenance + `ico`, `name`, `legal_form_code`,
  `legal_form_en`, `is_active`, `established_date`, `terminated_date`, `address`,
  `postal_code`, `city`, `district_code`, `size_category`, `institutional_sector`,
  `source_url`. No `raw_*`/hash in DDL; non-null Strings coalesced to `''`.
- **`cz_industries`** mirrors `fr_industries`/`gb_industries`.
- **`cz_company_contacts`** `ORDER BY (ico, contact_type, contact_value)` — one row
  per extracted valid contact candidate. Columns include `source_slug`,
  `source_record_id`, `ico`, `contact_type` (`domain` or `email`),
  `contact_value`, normalized root `domain`, `domain_source` (`commoncrawl` or
  `dns`), `confidence`, and `resolved_at`.

## 6. Translation
- **No LLM** — legal form via static EN map; NACE descriptions from `nace_categories`.
  Company names = proper nouns (not translated). Address parts are Czech place names.

## 6b. Contacts (§8b)
- RES and the ARES API have **no structured email/phone/website fields**. However,
  some company names embed domains or email addresses. Those inferred contacts are
  stored in `cz_company_contacts` and linked to `cz_companies` by `ico` (IČO).
- `domain_source='commoncrawl'` means the normalized domain exists in
  `commoncrawl_domains` and is treated as the stronger signal. `domain_source='dns'`
  means the domain was not in Common Crawl but returned DNS NS records.
- Phone numbers are still absent; this asset only extracts domains and emails from
  names.

## 6c. Industry / NACE
- Prefer `NACE2025` → `NACE_REV_2_1`; fall back to `NACE` → `NACE_REV_2`. First 4
  digits → `nace_code` `NN.NN`, `nace_normalized_code` = the 4 digits,
  `nace_mapping_method`='national_truncation'. Codes shorter than 4 digits
  (division/group level) → `unmapped`. Joins `corpscout.nace_categories`.

## 7. Currency — N/A (no monetary values in RES).

## 8. Scheduling
- `czech_ares_register_job` (companies + company contacts + industries from the one
  download/export chain via `.upstream()`) — twice-monthly cron (e.g. 17th + 2nd),
  default STOPPED.

## 9. Deferred
- **Financials** — Czech financial statements are in the Sbírka listin (justice.cz),
  often PDF; a separate effort. Structured contacts are not in open data beyond the
  name-embedded domain/email inference above. Multiple NACE per company are
  available via the ARES API (RES bulk has the main activity only).

## 10. Verification
- `uv run pytest tests/test_czech_ares.py tests/test_clickhouse_migrations.py -q` +
  `uv run dg check defs`. Migrations apply. Materialize live; `cz_companies` count +
  legal_form_en/address; `cz_company_contacts` contains only Common Crawl or
  DNS-NS-validated candidates; `cz_industries` join `nace_categories`. TDD; commit
  by path.
