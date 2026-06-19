# Latvia (UR Register of Enterprises) — source analysis & module roadmap

**Date:** 2026-06-19
**Author:** analysis pass for the next country pipeline (Sweden blocked on credentials)
**Source dossier:** `companycollect/companies/analysis/latvia/`

## Why Latvia

Picked as the next country because it needs **zero authentication** and is **best-in-class open**:

- **License: CC0-1.0 (public domain)** — no attribution, commercial reuse allowed.
- **No auth, no payment, no registration** for any dataset (`requires_authentication: false`,
  `requires_payment: false`, `credential_env_vars: []` in every handoff).
- Plain HTTP `GET` bulk CSV downloads from the national CKAN portal `data.gov.lv`.
- **Structured financials as CSV** (not PDF / not iXBRL) — so, unlike Finland, **no XBRL parse layer is needed**.
- Single, clean join key: **`regcode`** (11-digit registration number) across every dataset.

GDPR caveat: beneficial owners, officers, and members are **personal data** — handled as a separate, deferred
scope decision (see Risk R5).

## Sources (all CC0, all `;`-delimited UTF-8 CSV, verified live 2026-06-19)

| Source | Dataset | Rows (verified) | Join key | Notes |
|---|---|---|---|---|
| **Register spine** | `register.csv` | **485,134 entities** | `regcode` (PK) | identity, legal form, dates, address, ATVK, SEPA |
| **Financials — metadata** | `financial_statements.csv` | **1,970,094 reports** | `id` (PK), `legal_entity_registration_number` → regcode | year/period, employees, currency, `rounded_to_nearest` |
| **Financials — balance** | `balance_sheets.csv` | ~ per report | `statement_id` → financial_statements.id | total_assets, equity, current/non-current, … |
| **Financials — income** | `income_statements.csv` | ~ per report | `statement_id` → financial_statements.id | net_turnover, net_income, gross_profit (by_function/by_nature) |
| **Financials — cash flow** | `cash_flow_statements.csv` | ~ per report | `statement_id` → financial_statements.id | operating/investing/financing (optional 4th part) |
| Beneficial owners (PII) | `beneficial_owners.csv` | open CSV | `regcode` | **GDPR** — deferred |
| Officers / members / equity / events (PII) | ~35 UR datasets | open CSV | `regcode` | **GDPR** — deferred |

### Live-verified headers

`register.csv` (`;` delimiter):
```
regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;without_quotes;regtype;regtype_text;
type;type_text;registered;terminated;closed;address;index;addressid;region;city;atvk;reregistration_term
```

`financial_statements.csv`:
```
id;file_id;legal_entity_registration_number;source_schema;source_type;year;year_started_on;year_ended_on;
employees;rounded_to_nearest;currency;created_at
```

`balance_sheets.csv`:
```
statement_id;file_id;cash;marketable_securities;accounts_receivable;inventories;total_current_assets;
investments;fixed_assets;intangible_assets;total_non_current_assets;total_assets;
future_housing_repairs_payments;current_liabilities;non_current_liabilities;provisions;equity;total_equities
```

### Download URLs (from `sources.example.json`, live-verified)

```
register:             https://data.gov.lv/dati/dataset/4de9697f-850b-45ec-8bba-61fa09ce932f/resource/25e80bf3-f107-4ab4-89ef-251b5b9374e9/download/register.csv
financial_statements: https://data.gov.lv/dati/dataset/8d31b878-536a-44aa-a013-8bc6b669d477/resource/27fcc5ec-c63b-4bfd-bb08-01f073a52d04/download/financial_statements.csv
balance_sheets:       https://data.gov.lv/dati/dataset/8d31b878-536a-44aa-a013-8bc6b669d477/resource/50ef4f26-f410-4007-b296-22043ca3dc43/download/balance_sheets.csv
income_statements:    https://data.gov.lv/dati/dataset/8d31b878-536a-44aa-a013-8bc6b669d477/resource/d5fd17ef-d32e-40cb-8399-82b780095af0/download/income_statements.csv
beneficial_owners:    https://data.gov.lv/dati/dataset/b7848ab9-7886-4df0-8bc6-70052a8d9e1a/resource/20a9b26d-d056-4dbb-ae18-9ff23c87bdee/download/beneficial_owners.csv
```
(`cash_flow_statements.csv` lives under the same financials dataset `8d31b878-...`; resource id to confirm on
first download via the CKAN `package_show` action.)

## Existing repo pattern this mirrors (`norway_brreg`)

Latvia maps almost 1:1 onto the existing **`norway_brreg`** module, which is the closest analog (official
registry, full bulk download, structured financials):

1. **dlt streaming-download → per-source DuckDB file.** A `@dlt.resource` streams the bulk file (progress
   logging) and yields normalized rows; `@dlt_assets` loads it to `data/<module>.duckdb` with
   `write_disposition="replace"`, `primary_key`, behind a single-writer **concurrency pool**
   (`pool="latvia_ur_duckdb"`, instance limit 1) so concurrent materializations can't corrupt the DuckDB file.
2. **Downstream DuckDB-derived assets** (`@dg.asset` reading the same file behind the same pool).
3. **ClickHouse export** via `export_duckdb_table_to_clickhouse` (companies + financial_statements tables).
4. **A `_resolved` dbt-duckdb layer** (like `finland_resolved` / `norway_resolved`) for the joined/normalized
   model.

Wiring: `dg.load_from_defs_folder` auto-discovers any `defs/latvia_ur/` package; `dlt` and `clickhouse`
resources are already bound globally in `src/dagster_v3/definitions.py`; `LocalDuckDBResource` is instantiated
per module. A `definitions.py` is only needed if the module adds jobs/sensors (Norway has one; Finland YTJ
does not).

## Proposed module roadmap (incremental, like Finland)

- **Module 1 — `latvia_ur` register spine.** dlt streaming download of `register.csv` → DuckDB
  (`latvia_prur` dataset, `entities` table). Identity, legal form, status (derived from `terminated`/`closed`),
  address, ATVK, SEPA, derived `vat_id = "LV" + regcode`. **This is the first deliverable.**
- **Module 2 — financials.** Download the 3–4 financial CSVs, **preserve raw**, then pivot the parts per
  `statement_id` into one yearly statement row joined to `regcode`. This is the expensive/large step
  (4 × 200 MB+, ~2M reports) → needs the checkpoint discipline the user has asked for repeatedly.
- **Module 3 — `latvia_resolved`.** dbt-duckdb joining register + financials into the resolved company model;
  ClickHouse export.
- **Deferred — beneficial owners / officers / members (PII).** Gated on the GDPR decision (R5).

## Risks

- **R1 — Financials are large and multi-file (checkpoint discipline).** Four CSVs, 200 MB+ each, ~2M reports.
  Downloading all four and then pivoting in one shot risks a late failure forcing a full re-run — exactly the
  anti-pattern the user called out for Finland. Mitigation: preserve each raw CSV download (object store or
  on-disk staged table) as its own checkpoint, then pivot from staged tables; `handoff` already specifies
  `preserve_raw_download: true` and `preserve_raw_csvs_then_pivot_statement_parts_per_report`.
- **R2 — DuckDB single-writer.** Same as Finland/Norway: enforce `pool="latvia_ur_duckdb"` + instance limit 1.
- **R3 — `replace` write disposition wipes on empty download.** Norway/Finland guard against replacing the
  table when the source yields zero rows (`raise ValueError(...)`). Latvia register must do the same so a bad
  CKAN response can't blank 485k entities.
- **R4 — CSV quoting / embedded delimiters.** `register.csv` has quoted names containing `;` and embedded
  doubled quotes (`"IK ""KRASTNIEKI A I"""`). Must use a real CSV reader (DuckDB `read_csv` or Python `csv`),
  not naive `split(';')`.
- **R5 — GDPR (PII sources).** Beneficial owners, officers, members are personal data. CC0 covers IP reuse,
  not data protection. These need a lawful basis + retention policy before persistence and must not be used
  for direct marketing. **Deferred** — do not build until the data-protection decision is made.
- **R6 — Currency / units.** EUR (pre-2014 reports may be LVL); values must be scaled by `rounded_to_nearest`
  (e.g. `ONES`). Equity/net_income can be negative — keep signed.
- **R7 — Orphan statement parts.** A `statement_id` may appear in `balance_sheets`/`income_statements` but be
  missing from `financial_statements` metadata (handoff fixture calls this out). Join defensively; log + count
  orphans, don't drop silently (the user values not-silently-dropping-data).
- **R8 — `pipelines_dir` global singleton.** dlt's working dir is keyed only on `pipeline_name` and is shared
  across git checkouts/worktrees (the Finland `LoadPackageNotFound` incident). Use a per-checkout
  `pipelines_dir`, mirroring `finland_ytj_pipeline(database_path, *, pipelines_dir=None)`.

## Open scope decisions (for the user)

1. **Start with just the register spine (Module 1) first, then financials** — matches the incremental Finland
   approach and gives a working, testable deliverable fast. (Recommended.)
2. **PII sources (beneficial owners / officers / members):** in scope or deferred pending GDPR sign-off?
   (Recommended: defer.)
