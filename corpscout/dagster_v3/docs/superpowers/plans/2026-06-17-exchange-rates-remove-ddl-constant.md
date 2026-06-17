# Exchange Rates Remove DDL Constant Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove exchange-rate ClickHouse DDL SQL from Dagster source code so migrations are the only owner of exchange-rate table creation.

**Architecture:** `dagster_v3.defs.exchange_rates.tables` should expose identifiers and column order needed by assets/tests, not `CREATE TABLE` SQL. The ClickHouse migration file remains the schema source of truth. Tests should verify the migration contains the expected exchange-rate table, engine, and columns without comparing against a duplicated Python DDL string.

**Tech Stack:** Python constants, pytest, ClickHouse SQL migration files, Dagster definition loading.

---

## File Structure

- Modify `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/tables.py`
  - Remove `EXCHANGE_RATES_DDL`.
  - Keep database/table/qualified-table constants and `EXCHANGE_RATES_COLUMNS`.
- Modify `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`
  - Replace assertions against `tables.EXCHANGE_RATES_DDL` with an assertion that the Dagster exchange-rate tables module does not expose DDL SQL.
- Modify `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`
  - Remove `exchange_rate_tables.EXCHANGE_RATES_DDL` from the generic Python-DDL-vs-migration comparison.
  - Add a direct migration test for `000002_reference_exchange_rates.up.sql` that asserts the migration owns table creation details.

### Task 1: Add Failing Tests For No Duplicated Exchange DDL

**Files:**
- Modify: `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`

- [ ] **Step 1: Update exchange-rate schema contract test**

Replace these assertions:

```python
assert "CREATE TABLE IF NOT EXISTS reference.exchange_rates" in tables.EXCHANGE_RATES_DDL
assert "ENGINE = ReplacingMergeTree(pulled_at)" in tables.EXCHANGE_RATES_DDL
assert "ORDER BY (quote_currency, base_currency, rate_date, source)" in (
    tables.EXCHANGE_RATES_DDL
)
```

with:

```python
assert not hasattr(tables, "EXCHANGE_RATES_DDL")
```

- [ ] **Step 2: Add a migration-owned schema test**

In `test_clickhouse_migrations.py`, add:

```python
def test_exchange_rate_migration_defines_reference_table_schema() -> None:
    sql = _migration_sql("000002_reference_exchange_rates.up.sql")

    assert "CREATE DATABASE IF NOT EXISTS reference" in sql
    assert "CREATE TABLE IF NOT EXISTS reference.exchange_rates" in sql
    assert "ENGINE = ReplacingMergeTree(pulled_at)" in sql
    assert "ORDER BY (quote_currency, base_currency, rate_date, source)" in sql
    for column in exchange_rate_tables.EXCHANGE_RATES_COLUMNS:
        assert column in sql
```

- [ ] **Step 3: Run tests to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_exchange_rates_clickhouse_schema_contract tests/test_clickhouse_migrations.py::test_exchange_rate_migration_defines_reference_table_schema -q
```

Expected: FAIL because `EXCHANGE_RATES_DDL` still exists in `tables.py`.

### Task 2: Remove Exchange-Rate DDL Constant

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/tables.py`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`

- [ ] **Step 1: Delete DDL constant**

Remove this full block from `tables.py`:

```python
EXCHANGE_RATES_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED_EXCHANGE_RATES_TABLE}
(
    rate_date Date,
    base_currency LowCardinality(String),
    quote_currency LowCardinality(String),
    rate Decimal(38, 12),
    source LowCardinality(String),
    source_url String,
    source_payload_hash FixedString(64),
    source_run_id String,
    pulled_at DateTime64(3, 'UTC'),
    _dlt_load_id String,
    _dlt_id String
)
ENGINE = ReplacingMergeTree(pulled_at)
ORDER BY (quote_currency, base_currency, rate_date, source)
"""
```

- [ ] **Step 2: Stop migration test from using deleted constant**

In `test_clickhouse_migrations_match_existing_python_ddl_constants`, remove this dictionary entry:

```python
"000002_reference_exchange_rates.up.sql": exchange_rate_tables.EXCHANGE_RATES_DDL,
```

- [ ] **Step 3: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py tests/test_clickhouse_migrations.py -q
```

Expected: PASS.

### Task 3: Validate Dagster Definitions And Commit

**Files:**
- No additional source changes.

- [ ] **Step 1: Validate definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: PASS.

- [ ] **Step 2: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/tables.py corpscout/dagster_v3/tests/test_exchange_rates_assets.py corpscout/dagster_v3/tests/test_clickhouse_migrations.py corpscout/dagster_v3/docs/superpowers/plans/2026-06-17-exchange-rates-remove-ddl-constant.md
git commit -m "fix: remove exchange rate ddl constant"
```

## Self-Review

Spec coverage:
- Removes the exchange-rate `CREATE TABLE IF NOT EXISTS` DDL from Dagster code.
- Keeps the migration as the schema source of truth.
- Keeps tests checking actual migration content and asset-facing constants.

Placeholder scan:
- No placeholders remain.

Type consistency:
- `exchange_rate_tables.EXCHANGE_RATES_COLUMNS` remains available and is used by migration tests.
