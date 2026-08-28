# Sweden Ratsit JSON normalization schema proposal

Status: schema migration `000343_corpscout_se_ratsit_normalized_segments` and
the `se_ratsit_normalized` Dagster multi-asset are implemented and verified
locally, but have not been deployed or materialized against production data.

## Decision summary

Successful Ratsit report JSON should be normalized into eight source-specific
ClickHouse tables:

1. `corpscout.se_ratsit_company`
2. `corpscout.se_ratsit_company_industry_codes`
3. `corpscout.se_ratsit_company_summaries`
4. `corpscout.se_ratsit_responsible_people`
5. `corpscout.se_ratsit_establishments`
6. `corpscout.se_ratsit_financial_reports`
7. `corpscout.se_ratsit_financial_periods`
8. `corpscout.se_ratsit_people_at_address`

The existing `corpscout.se_company_ratsit` remains the scan history and S3
catalog. It is not replaced and no second scan table is introduced.

Normalized rows are content-addressed by the existing `result_sha256`, rather
than duplicated for every `scan_id`. If ten scans reuse the same S3 report, the
scan catalog contains ten observations while the normalized tables contain one
copy of that report for each normalizer version.

`address` and `coordinates` stay in `se_ratsit_company`; establishment address
and industry stay in `se_ratsit_establishments`; income statement, balance sheet, and
key-ratio fields stay in `se_ratsit_financial_periods`. These nested objects are
one-to-one with their parent row. Splitting them would add joins without creating
a useful new row grain.

## Source-to-table map

| JSON path | Target | Row grain |
| --- | --- | --- |
| envelope plus `report.company`, `report.coordinates`, `report.date_modified` | `se_ratsit_company` | one row per report content and normalizer version |
| `report.company.industry_codes[]` | `se_ratsit_company_industry_codes` | one row per source array item |
| `report.company.summary[]` | `se_ratsit_company_summaries` | one row per source paragraph |
| `report.responsible_people[]` | `se_ratsit_responsible_people` | one row per source array item |
| `report.workplaces[]` | `se_ratsit_establishments` | one row per source array item |
| `report.financials[]` | `se_ratsit_financial_reports` | one row per company/consolidated report block |
| `report.financials[].periods[]` | `se_ratsit_financial_periods` | one row per report block and fiscal period |
| `report.people_at_address[]` | `se_ratsit_people_at_address` | one row per source array item |

Only catalog rows with `outcome = 'success'` are normalized. Failure and
not-found JSON remain available through `se_company_ratsit` and S3, but they do
not create empty or synthetic rows in these tables.

## Shared identity and lineage

Every table has these four columns:

| Column | ClickHouse type | Meaning |
| --- | --- | --- |
| `company_id` | `String` | Ten- or twelve-digit canonical Swedish identifier used by the scan |
| `result_sha256` | `FixedString(64)` | Exact hash already stored in `se_company_ratsit` |
| `normalizer_version` | `LowCardinality(String)` | Version of the JSON-to-table mapping, for example `ratsit-normalizer-v1` |
| `normalized_at` | `DateTime64(6, 'UTC')` | ReplacingMergeTree version and audit time |

The common identity is `(company_id, result_sha256, normalizer_version)`. Child
tables append their source array indexes to that identity. Array indexes are
zero-based and preserve source order. They are preferable to names, URLs, or
establishment identifiers because those values can be null, duplicated, or changed
by Ratsit.

`scan_id` is deliberately absent from normalized tables. Scan observations,
fetch time, HTTP route, proxy name, and repeated observations belong to
`se_company_ratsit`. The exact normalized content is joined to a scan with:

```sql
scan.company_id = normalized.company_id
AND scan.result_sha256 = normalized.result_sha256
```

The base company row also retains the exact S3 bucket and object key so it can be
traced or replayed without first finding the original scan row.

## Proposed tables

### `se_ratsit_company`

This is both the one-row company segment and the completion marker for a fully
normalized report.

| Column | Type | Source or rule |
| --- | --- | --- |
| shared identity columns | as above | normalization lineage |
| `schema_version` | `UInt16` | envelope `schema_version` |
| `parser_version` | `LowCardinality(String)` | envelope `parser_version` |
| `requested_url` | `String` | envelope `requested_url` |
| `source_url` | `String` | envelope `source_url`; validate it equals `report.source_url` |
| `result_bucket` | `LowCardinality(String)` | `se_company_ratsit.result_bucket` |
| `result_object_key` | `String` | `se_company_ratsit.result_object_key` |
| `name` | `String` | `report.company.name`; required |
| `organization_number` | `String` | `report.company.organization_number`; must equal the rightmost ten digits of `company_id` |
| `legal_form` | `Nullable(String)` | `report.company.legal_form` |
| `status` | `Nullable(String)` | `report.company.status` |
| `address_street` | `Nullable(String)` | `report.company.address.street` |
| `address_postal_code` | `Nullable(String)` | `report.company.address.postal_code` |
| `address_locality` | `Nullable(String)` | `report.company.address.locality` |
| `address_county` | `Nullable(String)` | `report.company.address.county` |
| `business_description` | `Nullable(String)` | `report.company.business_description` |
| `latitude` | `Nullable(Float64)` | `report.coordinates.latitude` |
| `longitude` | `Nullable(Float64)` | `report.coordinates.longitude` |
| `source_date_modified` | `Nullable(Date32)` | `report.date_modified`; currently an ISO date |
| `industry_code_count` | `UInt16` | validated child-row count |
| `summary_count` | `UInt16` | validated child-row count |
| `responsible_people_count` | `UInt16` | validated child-row count |
| `establishment_count` | `UInt16` | validated child-row count |
| `financial_report_count` | `UInt16` | validated child-row count |
| `financial_period_count` | `UInt16` | validated child-row count across reports |
| `people_at_address_count` | `UInt16` | validated child-row count |

Engine and key:

```sql
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version)
```

The count columns make silent segment loss detectable and let this table act as
the commit marker without adding a redundant normalization-run table.

### `se_ratsit_company_industry_codes`

| Column | Type | Source or rule |
| --- | --- | --- |
| shared identity columns | as above | normalization lineage |
| `industry_index` | `UInt16` | position in `industry_codes[]` |
| `industry_code` | `Nullable(String)` | `code`; retain source formatting |
| `industry_description` | `Nullable(String)` | `description` |

```sql
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (
    company_id,
    result_sha256,
    normalizer_version,
    industry_index
)
```

### `se_ratsit_company_summaries`

| Column | Type | Source or rule |
| --- | --- | --- |
| shared identity columns | as above | normalization lineage |
| `summary_index` | `UInt16` | position in `summary[]` |
| `summary_text` | `String` | paragraph text |

```sql
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, summary_index)
```

### `se_ratsit_responsible_people`

| Column | Type | Source or rule |
| --- | --- | --- |
| shared identity columns | as above | normalization lineage |
| `person_index` | `UInt16` | position in `responsible_people[]` |
| `display_name` | `Nullable(String)` | source display name |
| `role` | `Nullable(String)` | source role text, not yet mapped to the canonical role vocabulary |
| `profile_url` | `Nullable(String)` | absolute Ratsit profile URL |

```sql
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, person_index)
```

This table is a source observation. It must not directly overwrite canonical
company-person relationships. Role mapping and person identity resolution are a
separate downstream step.

### `se_ratsit_establishments`

| Column | Type | Source or rule |
| --- | --- | --- |
| shared identity columns | as above | normalization lineage |
| `establishment_index` | `UInt16` | position in `workplaces[]` |
| `name` | `Nullable(String)` | establishment name |
| `identifier` | `Nullable(String)` | source workplace identifier (`Arbetsställenummer`) |
| `industry_code` | `Nullable(String)` | `industry.code` |
| `industry_description` | `Nullable(String)` | `industry.description` |
| `address_street` | `Nullable(String)` | `address.street` |
| `address_postal_code` | `Nullable(String)` | `address.postal_code` |
| `address_locality` | `Nullable(String)` | `address.locality` |
| `address_county` | `Nullable(String)` | `address.county` |
| `number_of_employees_raw` | `Nullable(String)` | exact parser value |
| `number_of_employees` | `Nullable(UInt32)` | populated only when the raw value is an unambiguous integer |

```sql
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, establishment_index)
```

Both employee columns are needed in version 1 because the current JSON parser
emits establishment employee counts as strings. A range or label must not be silently
coerced into an integer.

### `se_ratsit_financial_reports`

| Column | Type | Source or rule |
| --- | --- | --- |
| shared identity columns | as above | normalization lineage |
| `financial_report_index` | `UInt16` | position in `financials[]` |
| `scope` | `LowCardinality(String)` | normally `company`, `consolidated`, or parser fallback `report_N` |
| `monetary_unit` | `LowCardinality(Nullable(String))` | source unit such as `SEK`, `TSEK`, or `MSEK` |
| `period_count` | `UInt16` | number of child period rows |

```sql
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (
    company_id,
    result_sha256,
    normalizer_version,
    financial_report_index
)
```

Keeping this parent table preserves financial report blocks even when Ratsit
provides a scope and unit but no usable period rows.

### `se_ratsit_financial_periods`

| Column | Type | Source or rule |
| --- | --- | --- |
| shared identity columns | as above | normalization lineage |
| `financial_report_index` | `UInt16` | parent report position |
| `period_index` | `UInt16` | position in the parent `periods[]` array |
| `scope` | `LowCardinality(String)` | copied from the parent for direct queries |
| `monetary_unit` | `LowCardinality(Nullable(String))` | copied from the parent; no unit conversion |
| `fiscal_year` | `UInt16` | `fiscal_year` |
| `period_start` | `Nullable(Date32)` | `period_start` |
| `period_end` | `Nullable(Date32)` | `period_end` |
| `period_months` | `Nullable(UInt16)` | `period_months` |
| `revenue_amount` | `Nullable(Decimal(38, 6))` | `income_statement.revenue` |
| `operating_costs_amount` | `Nullable(Decimal(38, 6))` | `income_statement.operating_costs` |
| `operating_profit_amount` | `Nullable(Decimal(38, 6))` | `income_statement.operating_profit` |
| `profit_after_financial_items_amount` | `Nullable(Decimal(38, 6))` | `income_statement.profit_after_financial_items` |
| `net_income_amount` | `Nullable(Decimal(38, 6))` | `income_statement.net_income` |
| `current_assets_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.current_assets` |
| `fixed_assets_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.fixed_assets` |
| `share_capital_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.share_capital` |
| `equity_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.equity` |
| `untaxed_reserves_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.untaxed_reserves` |
| `provisions_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.provisions` |
| `long_term_liabilities_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.long_term_liabilities` |
| `current_liabilities_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.current_liabilities` |
| `liabilities_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.liabilities` |
| `total_assets_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.total_assets` |
| `balance_sheet_total_amount` | `Nullable(Decimal(38, 6))` | `balance_sheet.balance_sheet_total` |
| `cash_liquidity_percent` | `Nullable(Decimal(18, 6))` | `key_ratios.cash_liquidity_percent` |
| `equity_ratio_percent` | `Nullable(Decimal(18, 6))` | `key_ratios.equity_ratio_percent` |
| `net_profit_margin_percent` | `Nullable(Decimal(18, 6))` | `key_ratios.net_profit_margin_percent` |
| `ebitda_amount` | `Nullable(Decimal(38, 6))` | `key_ratios.ebitda` |
| `personnel_cost_per_employee_msek` | `Nullable(Decimal(38, 6))` | corresponding key ratio |
| `revenue_per_employee_msek` | `Nullable(Decimal(38, 6))` | corresponding key ratio |
| `revenue_change_percent` | `Nullable(Decimal(18, 6))` | corresponding key ratio |
| `average_salary` | `Nullable(Decimal(38, 6))` | source numeric value; do not infer a unit yet |
| `dividend_amount` | `Nullable(Decimal(38, 6))` | `dividend` in the report monetary unit |
| `employee_count` | `Nullable(UInt32)` | `employee_count` |

```sql
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (
    company_id,
    result_sha256,
    normalizer_version,
    financial_report_index,
    period_index
)
```

Amounts remain in `monetary_unit`. Version 1 must not rescale values to SEK or
map them into the shared canonical financial tables. That requires a separate,
tested semantic mapping. `average_salary` also remains a source numeric value
until its unit is verified across more company pages.

### `se_ratsit_people_at_address`

| Column | Type | Source or rule |
| --- | --- | --- |
| shared identity columns | as above | normalization lineage |
| `person_index` | `UInt16` | position in `people_at_address[]` |
| `name` | `String` | source display name |
| `age` | `Nullable(UInt16)` | age observed on the page |
| `profile_url` | `Nullable(String)` | absolute Ratsit profile URL |

```sql
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, person_index)
```

This is personal data about people sharing an address and not necessarily a
company relationship. The table should have restricted access and should not be
published through normal company APIs or fed into canonical company-person
relationships.

## Write and completeness semantics

One Dagster multi-asset should own all eight tables and parse each S3 JSON object
once. It should be non-subsettable because the outputs form one normalized
report.

For a configured `normalizer_version`, the asset should:

1. Read distinct successful `(company_id, result_sha256, bucket, object_key)`
   values from `se_company_ratsit FINAL`.
2. Exclude content already present in `se_ratsit_company FINAL` for that exact
   normalizer version.
3. Fetch objects by the catalog's exact S3 keys; do not list the company prefix.
4. Validate the envelope and build all eight typed row collections in memory.
5. Insert child tables first.
6. Insert `se_ratsit_company` last, after the actual child counts match its count
   columns.

The final company insert is the commit marker. A failed run can leave child rows,
but consumers only use report versions present in `se_ratsit_company`. A retry is
idempotent because every table uses the same immutable identity and
`ReplacingMergeTree(normalized_at)`. Queries that require immediate replacement
semantics use `FINAL`.

No DuckDB or dlt staging layer is needed for this path. The input is already a
small, normalized JSON document identified in ClickHouse and directly addressable
in S3. A typed Python mapper plus batched ClickHouse inserts has fewer moving
parts and reads every object only once.

The normalization asset should have its own manually runnable job so existing
S3 reports can be reparsed after a normalizer change without launching browsers.
It can also run immediately after `se_ratsit_scan_dispatch` in a combined job.

## Validation rules

- Envelope `company_id` must equal the catalog `company_id`.
- SHA-256 recomputed from the downloaded S3 object bytes must equal catalog
  `result_sha256` before inserting any rows.
- `schema_version` must be supported and `parser_version` must be non-empty.
- Envelope and nested report source URLs must be valid Ratsit URLs and equal.
- Company name must be non-empty and organization number must equal the rightmost
  ten digits of `company_id`.
- Empty strings become null for optional fields; required identity fields reject
  the object.
- Missing or empty arrays produce zero child rows, never placeholder rows.
- Dates and numbers are parsed strictly. An invalid value fails that report
  version instead of silently becoming zero.
- Unknown additional JSON fields are tolerated for forward compatibility and
  remain recoverable from S3. A changed meaning or type requires a new envelope
  `schema_version` or `normalizer_version`.
- Materialization metadata should expose selected objects, already-normalized
  objects, inserted rows by table, rejected objects, and count mismatches.

## Current-report query semantics

There should be no physical `_current` copy of these tables. The latest usable
Ratsit content is derived from the existing successful scan observations, so a
new failure or not-found observation does not hide the last success.

Conceptually:

```sql
WITH latest_success AS
(
    SELECT
        company_id,
        argMax(result_sha256, tuple(fetched_at, scan_id)) AS result_sha256
    FROM corpscout.se_company_ratsit FINAL
    WHERE outcome = 'success'
    GROUP BY company_id
)
SELECT company.*
FROM latest_success AS scan
INNER JOIN corpscout.se_ratsit_company FINAL AS company
    ON company.company_id = scan.company_id
   AND company.result_sha256 = scan.result_sha256
WHERE company.normalizer_version = 'ratsit-normalizer-v1';
```

Downstream serving assets can use the same latest-success relation when they are
ready to map Ratsit observations into canonical company, address, people, and
financial models.

## Implementation status

Completed locally:

1. One ClickHouse migration contains the eight tables and their down migration.
2. Typed JSON-envelope and segment models map the S3 report into ClickHouse
   rows.
3. The non-subsettable `se_ratsit_normalized` multi-asset and standalone
   `se_ratsit_normalize_job` are registered.
4. Tests cover every JSON segment, source/hash validation, latest-success
   selection, already-normalized skipping, and company-row-last insertion.
5. The saved Skanska JSON maps successfully to all eight tables, including two
   financial report blocks and ten financial periods.

Still pending:

1. Deploy migration `000343` and the Dagster definitions.
2. Materialize the existing 100-company pilot and reconcile table row counts
   against the source JSON arrays.
3. Add downstream canonical mappings only after the source-table results have
   been reviewed.
