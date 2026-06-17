# ClickHouse Migration Metadata Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `make clickhouse-migrate-up` after replacing old ClickHouse migrations by cleaning stale migration metadata and making the migration directory compatible with `golang-migrate`.

**Architecture:** First inspect the ClickHouse `schema_migrations` table used by `golang-migrate`. Then clean only migration metadata, not application data. Because historical migration names are no longer part of the desired ClickHouse migration set, rename the current new migrations to `golang-migrate` names starting at `000001` and add safe down migrations.

**Tech Stack:** ClickHouse, Docker `migrate/migrate:v4.17.0`, Makefile `clickhouse-migrate-up`, pytest migration contract tests.

---

## Task 1: Inspect Metadata State

- [ ] Query ClickHouse for `schema_migrations` in the database from `CLICKHOUSE_MIGRATE_URL`.
- [ ] Confirm whether it contains old version `12`.
- [ ] Confirm current migration files use invalid plain `.sql` names.

## Task 2: Fix Migration File Convention

- [ ] Rename current Dagster-created ClickHouse migrations from plain `.sql` files to `golang-migrate` pairs:
  - `000001_reference_nace_categories.up.sql`
  - `000002_reference_exchange_rates.up.sql`
  - `000003_norway_brreg_companies.up.sql`
  - `000004_norway_brreg_financial_statements.up.sql`
  - `000005_corpscout_fi_companies.up.sql`
  - `000006_corpscout_fi_websites.up.sql`
  - `000007_corpscout_fi_industries.up.sql`
  - `000008_corpscout_fi_financial_statements.up.sql`
  - `000009_corpscout_fi_financial_metrics.up.sql`
  - `000010_corpscout_finland_ytj_registry_tables.up.sql`
  - `000011_corpscout_finland_xbrl_raw_tables.up.sql`
- [ ] Add matching `.down.sql` files.
- [ ] Update `tests/test_clickhouse_migrations.py` to expect the `golang-migrate` names and validate both up/down files.
- [ ] Fix ClickHouse `ORDER BY` keys that use nullable columns because the target server has `allow_nullable_key` disabled.

## Task 3: Clean ClickHouse Migration Metadata

- [ ] Delete or reset only `schema_migrations` in the ClickHouse migration database.
- [ ] Do not drop application tables.
- [ ] Run `make clickhouse-migrate-up`.

## Task 4: Verify And Commit

- [ ] Run `uv run pytest tests/test_clickhouse_migrations.py -q`.
- [ ] Run `make clickhouse-migrate-up`.
- [ ] Commit migration-file fix and test updates.
