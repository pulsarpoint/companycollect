# Resolved ClickHouse Finland Tables Design

## Goal

Define the target ClickHouse modeling pattern for `dagster_v3`: ClickHouse should store resolved analytical tables, not one-to-one copies of source API payloads or intermediate source tables.

Finland is the first concrete implementation target. The design should support Finland PRH/YTJ company data, PRH XBRL financial data, NACE industry references, source-specific descriptions, translations, and currency-normalized financial metrics.

## Core Decision

ClickHouse is the durable analytical contract. It should contain product-ready, country-resolved tables such as `fi_companies`, `fi_websites`, `fi_industries`, and `fi_financial_metrics`.

Raw evidence and intermediate complexity belong elsewhere:

- raw JSON, XML, XBRL, and file manifests live in object storage
- dlt and DuckDB load source listings and staging tables
- DuckDB performs joins, filtering, normalization, and source-specific resolution
- ClickHouse receives only resolved tables whose schema is owned by migrations

Dagster orchestrates extraction, DuckDB transforms, and ClickHouse exports. Dagster assets may verify required ClickHouse tables exist, but should not create durable ClickHouse schemas inline.

## Table Families

The first Finland resolved table family should use `fi_` prefixes:

- `fi_companies`
- `fi_websites`
- `fi_industries`
- `fi_addresses`
- `fi_registered_entries`
- `fi_legal_forms`
- `fi_financial_statements`
- `fi_financial_metrics`

These names describe the resolved country data product, not the source implementation. The source system remains captured in audit metadata columns.

## Shared References

NACE is shared reference data, not a Finland source table. The final model should keep NACE under a reference namespace such as `corpscout_reference`.

`fi_industries` should store resolved NACE keys:

- `nace_revision`
- `nace_code`
- `nace_normalized_code`
- `nace_mapping_method`
- `nace_mapping_status`

It should not duplicate NACE labels or descriptions as canonical columns. Queries and product projections can join to NACE reference tables for official English names and descriptions.

Official NACE English text should be preferred over machine translation whenever the field comes from NACE.

## Translation Columns

For source-specific text fields that require English, resolved ClickHouse tables should use a consistent translation pattern:

- `<field>_original`
- `<field>_language`
- `<field>_en`
- `<field>_translated_at`
- `<field>_translation_provider`
- `<field>_translation_model`

Examples:

- `description_original`
- `description_language`
- `description_en`
- `description_translated_at`
- `description_translation_provider`
- `description_translation_model`

This pattern should be used for descriptions and labels where the source has original-language text and no authoritative English reference value.

## Financial Amount Columns

Financial tables should preserve original reported currency and expose USD-normalized values side by side.

Use a consistent pattern:

- `<metric>_amount_original`
- `<metric>_currency_original`
- `<metric>_amount_usd`
- `<metric>_fx_rate_to_usd`
- `<metric>_fx_rate_date`
- `<metric>_fx_converted_at`

For generic metric tables, the same concept can be represented as:

- `amount_original`
- `currency_original`
- `amount_usd`
- `fx_rate_to_usd`
- `fx_rate_date`
- `fx_converted_at`

The `fx_rate_date` records the date of the exchange rate used. `fx_converted_at` records when the conversion was performed.

## Audit Metadata

Every resolved ClickHouse table should include audit columns:

- `source_system`
- `source_run_id`
- `source_record_id`
- `source_payload_hash`
- `resolved_at`

When a row is derived from multiple source records, the table should either:

- use the primary source record in these columns and expose extra lineage fields, or
- include a separate lineage table keyed by the resolved row identifier

The first implementation should keep the simple audit columns on every table and add separate lineage only when the transformation actually needs many-to-one traceability.

## Migration Ownership

ClickHouse schema is owned by migrations, not Dagster asset code.

The existing `corpscout/clickhouse/migrations` direction is the right precedent:

- DDL belongs in versioned SQL migrations
- migrations create databases, tables, views, cache tables, and materialized views
- migrations do not load large source data
- data loading happens through Dagster after migrations have been applied

The current source-copy migrations should be treated as historical design input, not automatically accepted as the final `dagster_v3` contract. New or revised migrations should define the resolved `fi_*` tables.

## Dagster And DuckDB Flow

The standard flow for Finland should be:

```text
external source
  -> dlt/raw download/object storage
  -> DuckDB staging tables
  -> DuckDB resolved tables
  -> ClickHouse export into migrated fi_* tables
```

Dagster responsibilities:

- run source extraction assets
- build DuckDB staging and resolved tables
- verify required ClickHouse tables exist
- truncate/replace or append into resolved ClickHouse tables according to each table's strategy
- emit materialization metadata with row counts and source run identifiers

Dagster should not:

- create durable ClickHouse schemas inline
- maintain ad hoc JSON pointers or manifests for table state
- write one-to-one raw API copies into ClickHouse unless a table is explicitly temporary/debug-only

## Existing Migration Assessment

The existing ClickHouse migrations correctly express the architectural direction:

- separate source and reference databases
- use migration-owned ClickHouse DDL
- model NACE as shared reference data
- model Finland company and XBRL concepts as ClickHouse tables

They do not yet represent the final desired `dagster_v3` resolved contract because several tables mirror source-specific normalized exports. Before implementation, create a specific resolved-table migration set for the `fi_*` tables and decide which old tables should be replaced, ignored, or retained as compatibility views.

## Next Design Decisions

This spec defines the modeling direction. Before implementation, write a follow-up table-contract spec or implementation plan that decides:

1. Exact columns and order keys for each `fi_*` table.
2. Whether resolved tables live in `corpscout_sources`, a new `corpscout_resolved`, or another database.
3. Whether compatibility views should expose old table names while the new resolved contract is introduced.
4. Whether ClickHouse exports are performed with native inserts, `INSERT INTO ... SELECT` from external files, or dlt where it fits the final schema.
5. Whether financial USD conversion should be computed in DuckDB before export or in a ClickHouse projection after exchange-rate tables are loaded.
