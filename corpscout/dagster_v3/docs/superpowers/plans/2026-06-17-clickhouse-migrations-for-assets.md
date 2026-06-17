# ClickHouse Migrations For Dagster Assets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit ClickHouse migration SQL files for every current Dagster v3 asset that writes final data to ClickHouse, and include the Finland financial destination tables required by the resolved country model.

**Architecture:** Keep migrations as plain ordered SQL files under `corpscout/clickhouse/migrations`. Do not add a new migration framework in this change. Each migration creates its target database and table with the schema currently expected by the Dagster export code. Dagster asset code can continue to call its existing table-preparation helpers for now; later cleanup can remove runtime DDL once migrations are wired into deployment.

**Tech Stack:** ClickHouse SQL, Dagster v3 asset schema constants, pytest.

---

## Current ClickHouse-Writing Assets

- `nace_categories_clickhouse` writes `reference.nace_categories`.
- `exchange_rates_current_clickhouse` and `exchange_rates_backfill_clickhouse` write `reference.exchange_rates`.
- `norway_brreg_clickhouse_companies` writes `norway_brreg.companies`.
- `norway_brreg_clickhouse_financial_statements` writes `norway_brreg.financial_statements`.
- `finland_ytj_resolved_clickhouse` writes:
  - `corpscout.fi_companies`
  - `corpscout.fi_websites`
  - `corpscout.fi_industries`
- Finland XBRL financial data must have destination tables:
  - `corpscout.fi_financial_statements`
  - `corpscout.fi_financial_metrics`

Note: the current Finland XBRL pipeline builds financial data in DuckDB but does not yet export those tables to ClickHouse. This migration work creates the destination contract first; wiring the exporter is a follow-up pipeline change.

---

## Migration Files

Create:

- `corpscout/clickhouse/migrations/0001_reference_nace_categories.sql`
- `corpscout/clickhouse/migrations/0002_reference_exchange_rates.sql`
- `corpscout/clickhouse/migrations/0003_norway_brreg_companies.sql`
- `corpscout/clickhouse/migrations/0004_norway_brreg_financial_statements.sql`
- `corpscout/clickhouse/migrations/0005_corpscout_fi_companies.sql`
- `corpscout/clickhouse/migrations/0006_corpscout_fi_websites.sql`
- `corpscout/clickhouse/migrations/0007_corpscout_fi_industries.sql`
- `corpscout/clickhouse/migrations/0008_corpscout_fi_financial_statements.sql`
- `corpscout/clickhouse/migrations/0009_corpscout_fi_financial_metrics.sql`

Each file should:

- Use `CREATE DATABASE IF NOT EXISTS`.
- Use `CREATE TABLE IF NOT EXISTS`.
- Avoid `TRUNCATE`; migrations define structure only.
- Use table engines matching idempotent final-table behavior:
  - Reference tables: `ReplacingMergeTree(pulled_at)` keyed by their natural reference key, so repeated pulls and corrected reference values keep the newest pulled row.
  - Current full-replace final tables: `ReplacingMergeTree` for source records where reruns can replace snapshots.

---

## Task 1: Add Migration Contract Tests

**Files:**

- Create or modify `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`

- [ ] Add tests that assert the migration directory contains the seven expected `.sql` files.
- [ ] Assert each migration contains `CREATE DATABASE IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS`.
- [ ] Assert the four tables that already have Python DDL constants match migration content:
  - `nace.tables.NACE_CATEGORIES_DDL`
  - `exchange_rates.tables.EXCHANGE_RATES_DDL`
  - `norway_brreg.tables.COMPANIES_DDL`
  - `norway_brreg.tables.FINANCIAL_STATEMENTS_DDL`
- [ ] Assert Finland company, website, and industry migration files include every column listed in `finland_resolved.tables.RESOLVED_TABLE_COLUMNS`.
- [ ] Assert Finland financial migrations include the expected statement and USD-ready metric columns.
- [ ] Run the focused test and confirm it fails before migrations exist:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py -q
```

Expected: FAIL because migration files are not present.

---

## Task 2: Add Reference Migrations

**Files:**

- Create `corpscout/clickhouse/migrations/0001_reference_nace_categories.sql`
- Create `corpscout/clickhouse/migrations/0002_reference_exchange_rates.sql`

- [ ] Copy the existing table definitions from the Dagster DDL constants.
- [ ] Add `CREATE DATABASE IF NOT EXISTS reference;` before each table definition.
- [ ] Keep dlt metadata columns (`_dlt_load_id`, `_dlt_id`) because dlt writes them.

---

## Task 3: Add Norway BRREG Migrations

**Files:**

- Create `corpscout/clickhouse/migrations/0003_norway_brreg_companies.sql`
- Create `corpscout/clickhouse/migrations/0004_norway_brreg_financial_statements.sql`

- [ ] Copy the current `COMPANIES_DDL` and `FINANCIAL_STATEMENTS_DDL` table definitions.
- [ ] Add `CREATE DATABASE IF NOT EXISTS norway_brreg;` before each table definition.
- [ ] Keep `ReplacingMergeTree` and current `ORDER BY` keys.
- [ ] Do not include `TRUNCATE`; the export path already handles replacement semantics.

---

## Task 4: Add Finland Resolved Migrations

**Files:**

- Create `corpscout/clickhouse/migrations/0005_corpscout_fi_companies.sql`
- Create `corpscout/clickhouse/migrations/0006_corpscout_fi_websites.sql`
- Create `corpscout/clickhouse/migrations/0007_corpscout_fi_industries.sql`

- [ ] Create `corpscout` database in each migration.
- [ ] Define columns in exactly the order from `finland_resolved.tables.RESOLVED_TABLE_COLUMNS`.
- [ ] Use ClickHouse types that match the DuckDB export values:
  - identifiers and descriptions: `String`
  - dates: `Nullable(Date)`
  - booleans: `UInt8`
  - translated timestamps and `resolved_at`: `Nullable(DateTime64(3, 'UTC'))` where nullable, otherwise `DateTime64(3, 'UTC')`
  - payload hash: `FixedString(64)`
- [ ] Use stable `ReplacingMergeTree` ordering:
  - `fi_companies`: `ORDER BY (business_id)`
  - `fi_websites`: `ORDER BY (business_id, website_normalized_url)`
  - `fi_industries`: `ORDER BY (business_id, source_record_id)`

---

## Task 5: Add Finland Financial Migrations

**Files:**

- Create `corpscout/clickhouse/migrations/0008_corpscout_fi_financial_statements.sql`
- Create `corpscout/clickhouse/migrations/0009_corpscout_fi_financial_metrics.sql`

- [ ] Create `corpscout` database in each migration.
- [ ] Model `fi_financial_statements` from the current Finland XBRL statement document contract, with source lineage and resolved audit columns.
- [ ] Model `fi_financial_metrics` with original metric amounts plus USD conversion fields:
  - `<metric>_amount_original`
  - `<metric>_amount_usd`
  - shared `currency_original`, `fx_rate_to_usd`, `fx_rate_date`, `fx_converted_at`
- [ ] Use `ReplacingMergeTree`:
  - `fi_financial_statements`: `ORDER BY (business_id, financial_date, statement_key)`
  - `fi_financial_metrics`: `ORDER BY (business_id, financial_date, statement_key)`

---

## Task 6: Verify

- [ ] Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py -q
```

- [ ] Run:

```bash
cd corpscout/dagster_v3
DAGSTER_HOME=$PWD DAGSTER_PG_URL="$(grep '^DAGSTER_PG_URL=' .env | cut -d= -f2-)" uv run dg check defs
```

- [ ] Inspect `git diff` for migration DDL accuracy.
- [ ] Commit the changes on `main`.
