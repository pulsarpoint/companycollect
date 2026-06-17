# Finland Output Coverage Migrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ClickHouse migrations so Finland final tables can preserve the main structures available from PRH YTJ and PRH XBRL sources.

**Architecture:** Keep prior committed migrations immutable. Add follow-up SQL migrations under `corpscout/clickhouse/migrations` for missing Finland YTJ child tables, additive company columns, XBRL raw-first tables, and additive statement columns. Keep this change schema-only; do not wire new Dagster exporters in this step.

**Tech Stack:** ClickHouse SQL, pytest migration contract tests.

---

## File Structure

- Modify `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`
  - Add migration file names `0010` and `0011`.
  - Add contract tests for new Finland YTJ and XBRL tables.
- Create `corpscout/clickhouse/migrations/0010_corpscout_finland_ytj_registry_tables.sql`
  - Add columns to `corpscout.fi_companies`.
  - Create `fi_names`, `fi_addresses`, `fi_legal_forms`, `fi_registered_entries`, `fi_tax_registrations`, `fi_company_situations`.
- Create `corpscout/clickhouse/migrations/0011_corpscout_finland_xbrl_raw_tables.sql`
  - Add columns to `corpscout.fi_financial_statements`.
  - Create `fi_xbrl_contexts`, `fi_xbrl_units`, `fi_xbrl_facts_raw`, `fi_xbrl_taxonomy_codes`, `fi_financial_metrics_long`.

---

## Task 1: Add Failing Migration Contract Tests

- [ ] Add new migration names to `EXPECTED_MIGRATIONS`:

```python
"0010_corpscout_finland_ytj_registry_tables.sql",
"0011_corpscout_finland_xbrl_raw_tables.sql",
```

- [ ] Add expected column constants for:
  - `fi_companies` additive columns.
  - YTJ child table columns.
  - XBRL statement additive columns.
  - XBRL raw-first table columns.
- [ ] Add tests that read the two migration SQL files and assert table names plus required columns.
- [ ] Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py -q
```

Expected: FAIL because migration files do not exist yet.

---

## Task 2: Add YTJ Registry Migration

- [ ] Create `0010_corpscout_finland_ytj_registry_tables.sql`.
- [ ] Use `CREATE DATABASE IF NOT EXISTS corpscout;`.
- [ ] Add `ALTER TABLE corpscout.fi_companies ADD COLUMN IF NOT EXISTS ...` for:
  - `business_id_registration_date`
  - `eu_id`
  - `vat_id`
  - `trade_register_status`
  - `raw_status_code`
  - `last_modified`
  - `is_vat_registered`
  - `is_employer_registered`
  - `is_prepayment_registered`
- [ ] Create YTJ child tables with `ReplacingMergeTree` and stable order keys:
  - `fi_names`
  - `fi_addresses`
  - `fi_legal_forms`
  - `fi_registered_entries`
  - `fi_tax_registrations`
  - `fi_company_situations`

---

## Task 3: Add XBRL Raw-First Migration

- [ ] Create `0011_corpscout_finland_xbrl_raw_tables.sql`.
- [ ] Use `CREATE DATABASE IF NOT EXISTS corpscout;`.
- [ ] Add `ALTER TABLE corpscout.fi_financial_statements ADD COLUMN IF NOT EXISTS ...` for:
  - `root_name`
  - `schema_refs`
  - `taxonomy_entrypoint`
  - `parsed_at`
- [ ] Create XBRL raw-first tables:
  - `fi_xbrl_contexts`
  - `fi_xbrl_units`
  - `fi_xbrl_facts_raw`
  - `fi_xbrl_taxonomy_codes`
  - `fi_financial_metrics_long`

---

## Task 4: Verify And Commit

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

- [ ] Inspect `git diff --stat`.
- [ ] Commit on `main`:

```bash
git add corpscout/clickhouse/migrations corpscout/dagster_v3/tests/test_clickhouse_migrations.py corpscout/dagster_v3/docs/superpowers/plans/2026-06-17-finland-output-coverage-migrations.md
git commit -m "feat: add finland coverage clickhouse migrations"
```

