# SE company financial entity, slice 0: Ratsit USD conversion — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every monetary column of `corpscout.se_ratsit_financial_periods` a stored USD twin plus the three fx columns, filled by a re-runnable Dagster asset, so the financial entity (slices 1 to 4) reads ready-made pairs.

**Architecture:** Migration 000400 adds 20 `_usd` columns and `fx_rate_to_usd` / `fx_rate_date` / `fx_source` to the existing ReplacingMergeTree. A new module `sweden_ratsit/financial_usd.py` builds the SQL and runs the step: read the pending rate dates, resolve SEK→USD through the shared `ExchangeRateClient`, load the rates into a per-run `Join` table, run ONE `ALTER TABLE ... UPDATE` mutation over the pending rows with `joinGet`, wait for the mutation, drop the join table. The asset `se_ratsit_financial_periods_usd` in `sweden_ratsit/assets.py` wraps it with `execute` (preview by default).

**Tech Stack:** ClickHouse 26.5 (mutations, `Join` engine, `joinGet`, `multiplyDecimal`), Dagster (`dg.asset`, `dg.Config`, `ClickhouseResource` from `dagster_clickhouse`, clickhouse-driver native client), the in-repo `exchange_rates` package (`ExchangeRateClient`, `ExchangeRateRequest`, `UsdExchangeRate`), pytest with the clickhouse-local harness in `tests/clickhouse_local.py`, golang-migrate for migrations.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md`, section 3 (also section 12 slice 0 and section 13 names).

## Global Constraints

- Branch `se-financial-entity`, worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity`. Every path below is relative to `corpscout/services/dagster_v3` inside that worktree unless it starts with `corpscout/`. Never touch the main checkout at `/Users/graovic/pulsarpoint/ppoint/companycollect` (it carries the owner's uncommitted work).
- Migration number **000400** (prod ledger at 399 on 2026-09-11). Before merging, re-check `ls corpscout/clickhouse/migrations | tail -3` on main and `SELECT version FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1` on prod; renumber if either moved.
- Migration file rules (spec section 4): first line `CREATE DATABASE IF NOT EXISTS corpscout;`, no `;` inside comments, last line a statement; a `.down.sql` twin; both registered in `tests/test_clickhouse_migrations.py::EXPECTED_MIGRATIONS`.
- New columns exactly: `<column>_usd Nullable(Decimal(38, 6))` for the 18 `*_amount` columns (`balance_sheet_total_amount` included) and `personnel_cost_per_employee_usd`, `revenue_per_employee_usd` for the two `*_msek` columns; `fx_rate_to_usd Nullable(Decimal(38, 12))`, `fx_rate_date Nullable(Date32)`, `fx_source LowCardinality(String) DEFAULT ''`. No twin for `average_salary`, none for the `*_percent` ratios. Existing columns keep their names and values.
- USD = native value × unit scale × rate, in decimal arithmetic: scale `multiIf(monetary_unit = 'MSEK', 1000000, monetary_unit = 'TSEK', 1000, 1)` for the amount columns, a fixed `1000000` for the two `*_msek` columns. Rate date `ifNull(period_end, makeDate32(fiscal_year, 12, 31))`. Currency is always `SEK`.
- The join table name is always database-qualified (`corpscout._tmp_ratsit_fx_<run id without dashes>`): a mutation has no default database.
- Config `execute: bool = False`: a preview run reads and reports counts and writes nothing.
- Every write is an append or an in-place mutation of pending rows; nothing deletes data. The normalizer (`insert_normalized_ratsit_reports`) is not modified.
- Tests run from `corpscout/services/dagster_v3` with `uv run --env-file .env pytest ... -q` (pytest does not auto-load `.env`; `dg` does). clickhouse-local tests carry `pytestmark = pytest.mark.integration` and skip without a binary or Docker.
- Commits follow Conventional Commits and end with the attribution lines the session was given.
- Prod is owner-gated: nothing in Task 7 runs before the owner says "do it".

---

## File structure

| File | Responsibility |
|---|---|
| `corpscout/clickhouse/migrations/000400_corpscout_se_ratsit_financial_periods_usd.up.sql` / `.down.sql` | The 23 new columns and their removal |
| `src/dagster_v3/defs/sweden_ratsit/financial_usd.py` (new) | Column pairs, SQL builders (pending dates, join DDL, update, mutation status), rate loading, `convert_ratsit_financial_periods` |
| `src/dagster_v3/defs/sweden_ratsit/assets.py` (modify, end of file) | `RatsitFinancialUsdConfig`, asset `se_ratsit_financial_periods_usd`, job `se_ratsit_financial_usd_job`, registration in `defs` |
| `tests/test_clickhouse_migrations.py` (modify) | `EXPECTED_MIGRATIONS` entry and a DDL test for 000400 |
| `tests/test_sweden_ratsit_financial_usd.py` (new) | Unit tests: pairs, SQL text, preview vs execute, batching, mutation wait |
| `tests/test_sweden_ratsit_financial_usd_clickhouse_local.py` (new) | The mutation on a real engine: scaling, derived date, untouched rows, idempotence |
| `tests/test_sweden_ratsit_pilot.py` (modify) | Asset and job registration |

---

### Task 0: Worktree setup and baseline

**Files:**
- None in git. Creates the gitignored `.env` files in the worktree and its `.venv`.

- [ ] **Step 1: Copy the two gitignored env files from the main checkout**

```bash
cp /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/.env \
   /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity/corpscout/services/dagster_v3/.env
cp /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/.env \
   /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity/corpscout/.env
```

- [ ] **Step 2: Sync the environment**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity/corpscout/services/dagster_v3
uv sync --frozen
```

Expected: exits 0, `.venv` created.

- [ ] **Step 3: Run the Ratsit and migration suites for a baseline**

```bash
uv run --env-file .env pytest tests/test_sweden_ratsit_normalization.py tests/test_sweden_ratsit_pilot.py tests/test_clickhouse_migrations.py -q
```

Expected: all pass (the repo has four known-failing schedule-contract tests, but none in these files). Record the counts; the same files must still pass at the end of every task.

---

### Task 1: Migration 000400

**Files:**
- Create: `corpscout/clickhouse/migrations/000400_corpscout_se_ratsit_financial_periods_usd.up.sql`
- Create: `corpscout/clickhouse/migrations/000400_corpscout_se_ratsit_financial_periods_usd.down.sql`
- Modify: `tests/test_clickhouse_migrations.py` (the `EXPECTED_MIGRATIONS` tuple ending at line 416 with `"000399_corpscout_se_company_person_match",`; add a test after `test_ratsit_normalization_v2_migration_is_additive_and_nace_joinable`)

**Interfaces:**
- Produces: the 23 columns Task 2's SQL names. Column names are the constants of Task 2's `AMOUNT_COLUMNS` and `PER_EMPLOYEE_COLUMNS`.

- [ ] **Step 1: Write the failing migration test**

Add to `tests/test_clickhouse_migrations.py`, directly after `test_ratsit_normalization_v2_migration_is_additive_and_nace_joinable`:

```python
RATSIT_FINANCIAL_AMOUNT_COLUMNS = (
    "revenue_amount",
    "operating_costs_amount",
    "operating_profit_amount",
    "profit_after_financial_items_amount",
    "net_income_amount",
    "current_assets_amount",
    "fixed_assets_amount",
    "share_capital_amount",
    "equity_amount",
    "untaxed_reserves_amount",
    "provisions_amount",
    "long_term_liabilities_amount",
    "current_liabilities_amount",
    "liabilities_amount",
    "total_assets_amount",
    "balance_sheet_total_amount",
    "ebitda_amount",
    "dividend_amount",
)


def test_ratsit_financial_periods_usd_migration_adds_a_twin_per_monetary_column() -> None:
    up_sql = _migration_sql("000400_corpscout_se_ratsit_financial_periods_usd.up.sql")
    down_sql = _migration_sql("000400_corpscout_se_ratsit_financial_periods_usd.down.sql")

    assert up_sql.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    assert "ALTER TABLE corpscout.se_ratsit_financial_periods" in up_sql
    for column in RATSIT_FINANCIAL_AMOUNT_COLUMNS:
        assert (
            f"ADD COLUMN IF NOT EXISTS {column}_usd Nullable(Decimal(38, 6)) AFTER {column}"
            in up_sql
        ), column
        assert f"DROP COLUMN IF EXISTS {column}_usd" in down_sql, column
    for native, usd in (
        ("personnel_cost_per_employee_msek", "personnel_cost_per_employee_usd"),
        ("revenue_per_employee_msek", "revenue_per_employee_usd"),
    ):
        assert f"ADD COLUMN IF NOT EXISTS {usd} Nullable(Decimal(38, 6)) AFTER {native}" in up_sql
        assert f"DROP COLUMN IF EXISTS {usd}" in down_sql
    assert "ADD COLUMN IF NOT EXISTS fx_rate_to_usd Nullable(Decimal(38, 12)) AFTER employee_count" in up_sql
    assert "ADD COLUMN IF NOT EXISTS fx_rate_date Nullable(Date32) AFTER fx_rate_to_usd" in up_sql
    assert "ADD COLUMN IF NOT EXISTS fx_source LowCardinality(String) DEFAULT '' AFTER fx_rate_date" in up_sql
    # Unit unknown (Ratsit never states it) and ratios are not money: no twins.
    assert "average_salary_usd" not in up_sql
    assert "_percent_usd" not in up_sql
    assert up_sql.count("ADD COLUMN IF NOT EXISTS") == 23
    assert down_sql.count("DROP COLUMN IF EXISTS") == 23
```

And add `"000400_corpscout_se_ratsit_financial_periods_usd",` as the last entry of `EXPECTED_MIGRATIONS`, after `"000399_corpscout_se_company_person_match",`.

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run --env-file .env pytest tests/test_clickhouse_migrations.py -q -k "ratsit_financial_periods_usd or migration_files_are_explicit"
```

Expected: FAIL, `FileNotFoundError` for the `.up.sql` (and `migration_files_are_explicit` fails because the listed file does not exist).

- [ ] **Step 3: Write the up migration**

`corpscout/clickhouse/migrations/000400_corpscout_se_ratsit_financial_periods_usd.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- USD TWINS FOR RATSIT'S FINANCIAL PERIODS (financial entity spec 2026-09-11 section 3, slice 0).
--
-- The currency standard (data-source-guidelines section 7) stores every monetary figure a
-- source publishes WITH its USD twin in the source table, converted as a separate re-runnable
-- step. se_ratsit_financial_periods carried twenty monetary columns and no USD, so the figures
-- could only ever answer a single-country question. Each monetary column gets a twin right
-- after it: the eighteen *_amount lines (balance_sheet_total_amount included, for fidelity)
-- and the two per-employee figures Ratsit publishes in MSEK. One rate covers every figure of a
-- row, so fx_rate_to_usd / fx_rate_date / fx_source stay singular, after employee_count.
--
-- No twin for average_salary: Ratsit never states its unit (v2 proposal, field semantics), so
-- a conversion would guess. No twin for the *_percent ratios: they are not money.
--
-- The native columns keep their names and values: `revenue_amount` stays the published figure
-- in the row's monetary_unit (MSEK for every row today); the twin is the FULL-UNIT dollar value,
-- scale applied before FX by the asset se_ratsit_financial_periods_usd, which fills these
-- columns in place with one mutation over the rows whose fx_rate_to_usd is still NULL. The
-- normalizer never writes them (its INSERT lists its own columns), so they default to NULL on
-- every new report version and the asset converts them on its next run.

ALTER TABLE corpscout.se_ratsit_financial_periods
    ADD COLUMN IF NOT EXISTS revenue_amount_usd Nullable(Decimal(38, 6)) AFTER revenue_amount,
    ADD COLUMN IF NOT EXISTS operating_costs_amount_usd Nullable(Decimal(38, 6)) AFTER operating_costs_amount,
    ADD COLUMN IF NOT EXISTS operating_profit_amount_usd Nullable(Decimal(38, 6)) AFTER operating_profit_amount,
    ADD COLUMN IF NOT EXISTS profit_after_financial_items_amount_usd Nullable(Decimal(38, 6)) AFTER profit_after_financial_items_amount,
    ADD COLUMN IF NOT EXISTS net_income_amount_usd Nullable(Decimal(38, 6)) AFTER net_income_amount,
    ADD COLUMN IF NOT EXISTS current_assets_amount_usd Nullable(Decimal(38, 6)) AFTER current_assets_amount,
    ADD COLUMN IF NOT EXISTS fixed_assets_amount_usd Nullable(Decimal(38, 6)) AFTER fixed_assets_amount,
    ADD COLUMN IF NOT EXISTS share_capital_amount_usd Nullable(Decimal(38, 6)) AFTER share_capital_amount,
    ADD COLUMN IF NOT EXISTS equity_amount_usd Nullable(Decimal(38, 6)) AFTER equity_amount,
    ADD COLUMN IF NOT EXISTS untaxed_reserves_amount_usd Nullable(Decimal(38, 6)) AFTER untaxed_reserves_amount,
    ADD COLUMN IF NOT EXISTS provisions_amount_usd Nullable(Decimal(38, 6)) AFTER provisions_amount,
    ADD COLUMN IF NOT EXISTS long_term_liabilities_amount_usd Nullable(Decimal(38, 6)) AFTER long_term_liabilities_amount,
    ADD COLUMN IF NOT EXISTS current_liabilities_amount_usd Nullable(Decimal(38, 6)) AFTER current_liabilities_amount,
    ADD COLUMN IF NOT EXISTS liabilities_amount_usd Nullable(Decimal(38, 6)) AFTER liabilities_amount,
    ADD COLUMN IF NOT EXISTS total_assets_amount_usd Nullable(Decimal(38, 6)) AFTER total_assets_amount,
    ADD COLUMN IF NOT EXISTS balance_sheet_total_amount_usd Nullable(Decimal(38, 6)) AFTER balance_sheet_total_amount,
    ADD COLUMN IF NOT EXISTS ebitda_amount_usd Nullable(Decimal(38, 6)) AFTER ebitda_amount,
    ADD COLUMN IF NOT EXISTS personnel_cost_per_employee_usd Nullable(Decimal(38, 6)) AFTER personnel_cost_per_employee_msek,
    ADD COLUMN IF NOT EXISTS revenue_per_employee_usd Nullable(Decimal(38, 6)) AFTER revenue_per_employee_msek,
    ADD COLUMN IF NOT EXISTS dividend_amount_usd Nullable(Decimal(38, 6)) AFTER dividend_amount,
    ADD COLUMN IF NOT EXISTS fx_rate_to_usd Nullable(Decimal(38, 12)) AFTER employee_count,
    ADD COLUMN IF NOT EXISTS fx_rate_date Nullable(Date32) AFTER fx_rate_to_usd,
    ADD COLUMN IF NOT EXISTS fx_source LowCardinality(String) DEFAULT '' AFTER fx_rate_date;
```

- [ ] **Step 4: Write the down migration**

`corpscout/clickhouse/migrations/000400_corpscout_se_ratsit_financial_periods_usd.down.sql`:

```sql
ALTER TABLE corpscout.se_ratsit_financial_periods
    DROP COLUMN IF EXISTS fx_source,
    DROP COLUMN IF EXISTS fx_rate_date,
    DROP COLUMN IF EXISTS fx_rate_to_usd,
    DROP COLUMN IF EXISTS dividend_amount_usd,
    DROP COLUMN IF EXISTS revenue_per_employee_usd,
    DROP COLUMN IF EXISTS personnel_cost_per_employee_usd,
    DROP COLUMN IF EXISTS ebitda_amount_usd,
    DROP COLUMN IF EXISTS balance_sheet_total_amount_usd,
    DROP COLUMN IF EXISTS total_assets_amount_usd,
    DROP COLUMN IF EXISTS liabilities_amount_usd,
    DROP COLUMN IF EXISTS current_liabilities_amount_usd,
    DROP COLUMN IF EXISTS long_term_liabilities_amount_usd,
    DROP COLUMN IF EXISTS provisions_amount_usd,
    DROP COLUMN IF EXISTS untaxed_reserves_amount_usd,
    DROP COLUMN IF EXISTS equity_amount_usd,
    DROP COLUMN IF EXISTS share_capital_amount_usd,
    DROP COLUMN IF EXISTS fixed_assets_amount_usd,
    DROP COLUMN IF EXISTS current_assets_amount_usd,
    DROP COLUMN IF EXISTS net_income_amount_usd,
    DROP COLUMN IF EXISTS profit_after_financial_items_amount_usd,
    DROP COLUMN IF EXISTS operating_profit_amount_usd,
    DROP COLUMN IF EXISTS operating_costs_amount_usd,
    DROP COLUMN IF EXISTS revenue_amount_usd;
```

- [ ] **Step 5: Run the migration tests**

```bash
uv run --env-file .env pytest tests/test_clickhouse_migrations.py -q
```

Expected: PASS, including `test_clickhouse_migration_files_are_explicit`, `test_clickhouse_migrations_create_databases_and_tables` (the up file starts with the database statement and contains an `ALTER TABLE`; check the assertion in that test at line 810 to 848 accepts ALTER-only migrations, as 000196 and 000346 already are) and `test_clickhouse_migrations_have_down_files`.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/clickhouse/migrations/000400_corpscout_se_ratsit_financial_periods_usd.up.sql \
        corpscout/clickhouse/migrations/000400_corpscout_se_ratsit_financial_periods_usd.down.sql \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(ratsit): migration 000400 adds USD twins and fx columns to se_ratsit_financial_periods"
```

---

### Task 2: `financial_usd.py` — column pairs and SQL builders

**Files:**
- Create: `src/dagster_v3/defs/sweden_ratsit/financial_usd.py`
- Test: `tests/test_sweden_ratsit_financial_usd.py`

**Interfaces:**
- Consumes: `RATSIT_FINANCIAL_PERIODS_TABLE` from `dagster_v3.defs.sweden_ratsit.normalization` (`"se_ratsit_financial_periods"`).
- Produces (used by Tasks 3 to 5):
  - `DATABASE = "corpscout"`, `QUALIFIED_PERIODS_TABLE = "corpscout.se_ratsit_financial_periods"`
  - `RATSIT_FINANCIAL_CURRENCY = "SEK"`, `RATE_REQUEST_BATCH = 50`, `JOIN_TABLE_PREFIX = "corpscout._tmp_ratsit_fx_"`
  - `AMOUNT_COLUMNS: tuple[str, ...]` (18), `PER_EMPLOYEE_COLUMNS: tuple[tuple[str, str], ...]` (2), `USD_PAIRS: tuple[tuple[str, str, str], ...]` (20 of `(native, usd, scale_sql)`)
  - `RATE_DATE_SQL: str`, `MONETARY_PRESENT_SQL: str`
  - `pending_rate_dates_sql() -> str`, `join_table_name(run_id: str) -> str`, `join_table_ddl(join_table: str) -> str`, `join_insert_sql(join_table: str) -> str`, `usd_update_sql(join_table: str, *, mutations_sync: int = 0) -> str`, `mutation_status_sql() -> str`

- [ ] **Step 1: Write the failing tests**

`tests/test_sweden_ratsit_financial_usd.py`:

```python
"""Slice 0 of the financial entity (spec 2026-09-11 section 3): the USD twins of
se_ratsit_financial_periods. Column pairs, the SQL the asset runs, and the run function
against a fake client and fake rates."""

from dagster_v3.defs.sweden_ratsit import financial_usd
from dagster_v3.defs.sweden_ratsit.financial_usd import (
    AMOUNT_COLUMNS,
    JOIN_TABLE_PREFIX,
    MONETARY_PRESENT_SQL,
    PER_EMPLOYEE_COLUMNS,
    QUALIFIED_PERIODS_TABLE,
    RATE_DATE_SQL,
    USD_PAIRS,
    join_insert_sql,
    join_table_ddl,
    join_table_name,
    mutation_status_sql,
    pending_rate_dates_sql,
    usd_update_sql,
)


def test_usd_pairs_cover_the_twenty_monetary_columns_and_nothing_else() -> None:
    assert len(AMOUNT_COLUMNS) == 18
    assert AMOUNT_COLUMNS[0] == "revenue_amount"
    assert "balance_sheet_total_amount" in AMOUNT_COLUMNS
    assert PER_EMPLOYEE_COLUMNS == (
        ("personnel_cost_per_employee_msek", "personnel_cost_per_employee_usd"),
        ("revenue_per_employee_msek", "revenue_per_employee_usd"),
    )
    assert len(USD_PAIRS) == 20
    natives = [native for native, _, _ in USD_PAIRS]
    assert "average_salary" not in natives
    assert not any(native.endswith("_percent") for native in natives)
    for native, usd, scale_sql in USD_PAIRS:
        if native.endswith("_msek"):
            assert usd == native.removesuffix("_msek") + "_usd"
            assert scale_sql == "1000000"
        else:
            assert usd == f"{native}_usd"
            assert scale_sql == financial_usd.UNIT_SCALE_SQL
    assert financial_usd.UNIT_SCALE_SQL == (
        "multiIf(monetary_unit = 'MSEK', 1000000, monetary_unit = 'TSEK', 1000, 1)"
    )


def test_rate_date_and_pending_scan() -> None:
    assert RATE_DATE_SQL == "ifNull(period_end, makeDate32(fiscal_year, 12, 31))"
    assert MONETARY_PRESENT_SQL.startswith("(revenue_amount IS NOT NULL OR ")
    assert MONETARY_PRESENT_SQL.endswith("revenue_per_employee_msek IS NOT NULL)")
    sql = pending_rate_dates_sql()
    assert sql.startswith(f"SELECT {RATE_DATE_SQL} AS rate_date, count() AS rows")
    assert f"FROM {QUALIFIED_PERIODS_TABLE} FINAL" in sql
    assert f"WHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL}" in sql
    assert sql.endswith("GROUP BY rate_date\nORDER BY rate_date")


def test_join_table_is_qualified_run_scoped_and_a_join_engine() -> None:
    name = join_table_name("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    assert name == f"{JOIN_TABLE_PREFIX}a1b2c3d4e5f67890abcdef1234567890"
    assert name.startswith("corpscout.")
    ddl = join_table_ddl(name)
    assert ddl == (
        f"CREATE TABLE {name} (rate_date Date32, fx_rate Decimal(38, 12), "
        "fx_rate_date Date32, fx_source String) ENGINE = Join(ANY, LEFT, rate_date)"
    )
    assert join_insert_sql(name) == (
        f"INSERT INTO {name} (rate_date, fx_rate, fx_rate_date, fx_source) VALUES"
    )


def test_update_sql_names_every_pair_the_fx_columns_and_the_pending_guard() -> None:
    name = join_table_name("run-1")
    sql = usd_update_sql(name)
    assert sql.startswith(f"ALTER TABLE {QUALIFIED_PERIODS_TABLE} UPDATE\n")
    for native, usd, scale_sql in USD_PAIRS:
        assert (
            f"{usd} = multiplyDecimal({native} * {scale_sql}, "
            f"joinGet('{name}', 'fx_rate', {RATE_DATE_SQL}), 6)"
        ) in sql, usd
    assert f"fx_rate_to_usd = joinGet('{name}', 'fx_rate', {RATE_DATE_SQL})" in sql
    assert f"fx_rate_date = joinGet('{name}', 'fx_rate_date', {RATE_DATE_SQL})" in sql
    assert f"fx_source = joinGet('{name}', 'fx_source', {RATE_DATE_SQL})" in sql
    assert (
        f"WHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL} "
        f"AND {RATE_DATE_SQL} IN (SELECT rate_date FROM {name})"
    ) in sql
    assert sql.endswith("SETTINGS mutations_sync = 0")
    assert usd_update_sql(name, mutations_sync=2).endswith("SETTINGS mutations_sync = 2")
    # Every assignment is one line and the fx columns come last, so a reader can diff it.
    assert sql.count("\n") == 20 + 3 + 1


def test_mutation_status_sql_is_bound_by_table_and_join_name() -> None:
    sql = mutation_status_sql()
    assert "FROM system.mutations" in sql
    assert "database = %(database)s AND table = %(table)s AND command LIKE %(pattern)s" in sql
    assert "ORDER BY create_time DESC LIMIT 1" in sql
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run --env-file .env pytest tests/test_sweden_ratsit_financial_usd.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.sweden_ratsit.financial_usd'`.

- [ ] **Step 3: Write the module (builders only; Task 3 adds the run function)**

`src/dagster_v3/defs/sweden_ratsit/financial_usd.py`:

```python
"""USD twins for se_ratsit_financial_periods (financial entity spec 2026-09-11 section 3,
slice 0).

Ratsit publishes twenty monetary figures per period in MSEK (one decimal), TSEK or SEK and no
dollar value. The currency standard wants every monetary figure stored WITH its USD twin in the
source table, converted as a separate re-runnable step. This module is that step:

1. `pending_rate_dates_sql` finds the distinct rate dates of rows whose fx_rate_to_usd is
   still NULL (the rate date is the period end, or Dec 31 of the fiscal year for the 7,503
   undated rows).
2. The rates come from the shared ExchangeRateClient (SEK to USD through EUR, the latest ECB
   date at or before the requested one; for dates before the 2006 start of the rate table the
   earliest date after). They are written into a per-run Join table.
3. ONE `ALTER TABLE ... UPDATE` mutation fills the twins and the three fx columns of every
   pending row whose date got a rate, with `joinGet` against the join table. The join table
   name is always database-qualified: a mutation has no default database (proven 2026-09-11).
4. The mutation runs asynchronously (`mutations_sync = 0`) and `wait_for_mutation` polls
   system.mutations, because the first run rewrites 3.1M rows and a synchronous wait would sit
   on the driver's socket timeout. The join table is dropped only after the mutation is done.

`_amount` stays the published figure in the row's monetary_unit; `_usd` is the FULL-UNIT dollar
value: scale first (MSEK 1e6, TSEK 1e3, SEK 1; the two per-employee figures are always MSEK),
then FX, in decimal arithmetic (`multiplyDecimal(..., 6)` keeps the Decimal(38, 6) result).
A row is converted once (the guard is `fx_rate_to_usd IS NULL`); a row whose rate does not
exist yet is picked up on a later run; the normalizer only ever inserts NEW report versions, so
it never overwrites a converted row.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

from exchange_rates import ExchangeRateRequest

from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_FINANCIAL_PERIODS_TABLE

DATABASE = "corpscout"
QUALIFIED_PERIODS_TABLE = f"{DATABASE}.{RATSIT_FINANCIAL_PERIODS_TABLE}"
RATSIT_FINANCIAL_CURRENCY = "SEK"
RATE_REQUEST_BATCH = 50
JOIN_TABLE_PREFIX = f"{DATABASE}._tmp_ratsit_fx_"

# The eighteen *_amount columns of se_ratsit_financial_periods, in DDL order, every one in the
# row's monetary_unit. balance_sheet_total_amount is kept for fidelity even though Ratsit's own
# field rule makes total_assets_amount the canonical balance-sheet total.
AMOUNT_COLUMNS: tuple[str, ...] = (
    "revenue_amount",
    "operating_costs_amount",
    "operating_profit_amount",
    "profit_after_financial_items_amount",
    "net_income_amount",
    "current_assets_amount",
    "fixed_assets_amount",
    "share_capital_amount",
    "equity_amount",
    "untaxed_reserves_amount",
    "provisions_amount",
    "long_term_liabilities_amount",
    "current_liabilities_amount",
    "liabilities_amount",
    "total_assets_amount",
    "balance_sheet_total_amount",
    "ebitda_amount",
    "dividend_amount",
)
# The two per-employee figures Ratsit publishes in MSEK regardless of the row's unit.
PER_EMPLOYEE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("personnel_cost_per_employee_msek", "personnel_cost_per_employee_usd"),
    ("revenue_per_employee_msek", "revenue_per_employee_usd"),
)
UNIT_SCALE_SQL = "multiIf(monetary_unit = 'MSEK', 1000000, monetary_unit = 'TSEK', 1000, 1)"
MSEK_SCALE_SQL = "1000000"
# (native column, usd column, scale expression) for the twenty pairs.
USD_PAIRS: tuple[tuple[str, str, str], ...] = tuple(
    (native, f"{native}_usd", UNIT_SCALE_SQL) for native in AMOUNT_COLUMNS
) + tuple((native, usd, MSEK_SCALE_SQL) for native, usd in PER_EMPLOYEE_COLUMNS)

RATE_DATE_SQL = "ifNull(period_end, makeDate32(fiscal_year, 12, 31))"
MONETARY_PRESENT_SQL = "(" + " OR ".join(f"{native} IS NOT NULL" for native, _, _ in USD_PAIRS) + ")"
FX_COLUMNS: tuple[str, ...] = ("fx_rate_to_usd", "fx_rate_date", "fx_source")


def pending_rate_dates_sql() -> str:
    """The distinct rate dates of rows still without a rate, with their row counts."""
    return (
        f"SELECT {RATE_DATE_SQL} AS rate_date, count() AS rows\n"
        f"FROM {QUALIFIED_PERIODS_TABLE} FINAL\n"
        f"WHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL}\n"
        "GROUP BY rate_date\n"
        "ORDER BY rate_date"
    )


def join_table_name(run_id: str) -> str:
    """A database-qualified, run-scoped Join table name (only [0-9A-Za-z] of the run id)."""
    return f"{JOIN_TABLE_PREFIX}{re.sub(r'[^0-9A-Za-z]', '', run_id)}"


def join_table_ddl(join_table: str) -> str:
    return (
        f"CREATE TABLE {join_table} (rate_date Date32, fx_rate Decimal(38, 12), "
        "fx_rate_date Date32, fx_source String) ENGINE = Join(ANY, LEFT, rate_date)"
    )


def join_insert_sql(join_table: str) -> str:
    return f"INSERT INTO {join_table} (rate_date, fx_rate, fx_rate_date, fx_source) VALUES"


def usd_update_sql(join_table: str, *, mutations_sync: int = 0) -> str:
    """One mutation over the pending rows whose rate date is in the join table."""
    rate = f"joinGet('{join_table}', 'fx_rate', {RATE_DATE_SQL})"
    assignments = [
        f"{usd} = multiplyDecimal({native} * {scale_sql}, {rate}, 6)"
        for native, usd, scale_sql in USD_PAIRS
    ] + [
        f"fx_rate_to_usd = {rate}",
        f"fx_rate_date = joinGet('{join_table}', 'fx_rate_date', {RATE_DATE_SQL})",
        f"fx_source = joinGet('{join_table}', 'fx_source', {RATE_DATE_SQL})",
    ]
    return (
        f"ALTER TABLE {QUALIFIED_PERIODS_TABLE} UPDATE\n"
        + ",\n".join(f"    {assignment}" for assignment in assignments)
        + f"\nWHERE fx_rate_to_usd IS NULL AND {MONETARY_PRESENT_SQL} "
        f"AND {RATE_DATE_SQL} IN (SELECT rate_date FROM {join_table}) "
        f"SETTINGS mutations_sync = {mutations_sync}"
    )


def mutation_status_sql() -> str:
    """The newest mutation of the table whose command names the run's join table."""
    return (
        "SELECT is_done, latest_fail_reason FROM system.mutations "
        "WHERE database = %(database)s AND table = %(table)s AND command LIKE %(pattern)s "
        "ORDER BY create_time DESC LIMIT 1"
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run --env-file .env pytest tests/test_sweden_ratsit_financial_usd.py -q
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_ratsit/financial_usd.py \
        corpscout/services/dagster_v3/tests/test_sweden_ratsit_financial_usd.py
git commit -m "feat(ratsit): column pairs and SQL builders for the financial-periods USD step"
```

---

### Task 3: `convert_ratsit_financial_periods` — rates, join table, mutation, wait

**Files:**
- Modify: `src/dagster_v3/defs/sweden_ratsit/financial_usd.py` (append after `mutation_status_sql`)
- Test: `tests/test_sweden_ratsit_financial_usd.py` (append)

**Interfaces:**
- Consumes: Task 2's builders; `ExchangeRateRequest` and an object with `usd_rates(requests) -> dict[(currency, iso_date), UsdExchangeRate]` (the real `ExchangeRateClient`).
- Produces:
  - `@dataclass(frozen=True) UsdCounts(rows_pending: int, rate_dates_needed: int, rates_found: int, rows_convertible: int, rows_converted: int, rows_still_without_rate: int, executed: bool)` with `as_metadata() -> dict[str, int | bool]`
  - `load_usd_rates(exchange_rates, requests: Sequence[ExchangeRateRequest]) -> dict[tuple[str, str], Any]`
  - `wait_for_mutation(client, *, join_table: str, poll_seconds: float = 5.0, timeout_seconds: float = 7200.0, sleep: Callable[[float], None] = time.sleep) -> None`
  - `convert_ratsit_financial_periods(client, exchange_rates, *, run_id: str, execute: bool, log: Callable[..., object] | None = None, sleep: Callable[[float], None] = time.sleep) -> UsdCounts`

- [ ] **Step 1: Write the failing tests**

Add these imports to the top of `tests/test_sweden_ratsit_financial_usd.py` (merge the
`financial_usd` names into the existing `from ... import (...)` block, alphabetically):

```python
from datetime import date
from decimal import Decimal

import pytest
from exchange_rates import ExchangeRateRequest
from exchange_rates.models import UsdExchangeRate

from dagster_v3.defs.sweden_ratsit.financial_usd import (
    UsdCounts,
    convert_ratsit_financial_periods,
    load_usd_rates,
    wait_for_mutation,
)
```

Then append to the file:

```python
class FakeRates:
    """usd_rates over a dict of known ISO dates; a batch with one unknown date raises, as the
    real client does, so the loader must fall back to one request at a time."""

    def __init__(self, known: dict[str, Decimal]):
        self.known = known
        self.calls: list[list[ExchangeRateRequest]] = []

    def usd_rates(self, requests):
        requests = list(requests)
        self.calls.append(requests)
        out = {}
        for request in requests:
            if request.rate_date not in self.known:
                raise LookupError(request.rate_date)
            out[(request.currency, request.rate_date)] = UsdExchangeRate(
                currency=request.currency,
                requested_rate_date=request.rate_date,
                rate_date=request.rate_date,
                rate=self.known[request.rate_date],
                eur_to_usd=Decimal("1.1"),
                eur_to_currency=Decimal("11"),
                source="ecb",
                components=(),
            )
        return out


class FakeClient:
    """Answers the pending scan from `pending` (one list per call), the mutation poll from
    `statuses` (one row per call), and records every other statement."""

    def __init__(self, *, pending, statuses=((1, ""),)):
        self.pending = [list(p) for p in pending]
        self.statuses = list(statuses)
        self.statements: list[tuple[str, object]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params))
        if sql.startswith("SELECT ifNull(period_end"):
            return self.pending.pop(0)
        if "FROM system.mutations" in sql:
            row = self.statuses.pop(0)
            return [row] if row is not None else []
        if sql.startswith(("CREATE TABLE", "DROP TABLE", "INSERT INTO", "ALTER TABLE")):
            return []
        raise AssertionError(sql)


PENDING = [(date(2023, 12, 31), 3), (date(2021, 6, 30), 2), (date(2004, 12, 31), 1)]


def test_load_usd_rates_batches_at_fifty_and_skips_only_the_missing_date() -> None:
    known = {f"2023-01-{day:02d}": Decimal("0.1") for day in range(1, 32)}
    rates = FakeRates(known)
    requests = [
        ExchangeRateRequest(currency="SEK", rate_date=f"2023-01-{day:02d}") for day in range(1, 32)
    ] + [ExchangeRateRequest(currency="SEK", rate_date="1999-12-31")] + [
        ExchangeRateRequest(currency="SEK", rate_date=f"2023-01-{day:02d}") for day in range(1, 20)
    ]
    assert len(requests) == 51
    found = load_usd_rates(rates, requests)
    assert len(found) == 31
    assert ("SEK", "1999-12-31") not in found
    # First batch of 50 raised on the unknown date and was retried one by one (50 single
    # calls), the second batch of 1 succeeded whole: 1 + 50 + 1 calls.
    assert [len(call) for call in rates.calls] == [50] + [1] * 50 + [1]


def test_preview_reports_counts_and_writes_nothing() -> None:
    client = FakeClient(pending=[PENDING])
    rates = FakeRates({"2023-12-31": Decimal("0.099"), "2021-06-30": Decimal("0.117")})
    counts = convert_ratsit_financial_periods(client, rates, run_id="run-1", execute=False)
    assert counts == UsdCounts(
        rows_pending=6, rate_dates_needed=3, rates_found=2, rows_convertible=5,
        rows_converted=0, rows_still_without_rate=6, executed=False,
    )
    assert counts.as_metadata()["rows_convertible"] == 5
    assert [s for s, _ in client.statements] == [
        "SELECT ifNull(period_end, makeDate32(fiscal_year, 12, 31)) AS rate_date, count() AS rows\n"
        "FROM corpscout.se_ratsit_financial_periods FINAL\n"
        "WHERE fx_rate_to_usd IS NULL AND " + MONETARY_PRESENT_SQL + "\n"
        "GROUP BY rate_date\nORDER BY rate_date"
    ]


def test_execute_loads_the_join_table_runs_the_mutation_waits_and_drops() -> None:
    client = FakeClient(pending=[PENDING, [(date(2004, 12, 31), 1)]], statuses=[(0, ""), (1, "")])
    rates = FakeRates({"2023-12-31": Decimal("0.099"), "2021-06-30": Decimal("0.117")})
    slept: list[float] = []
    counts = convert_ratsit_financial_periods(
        client, rates, run_id="a1b2-c3", execute=True, sleep=slept.append,
    )
    assert counts == UsdCounts(
        rows_pending=6, rate_dates_needed=3, rates_found=2, rows_convertible=5,
        rows_converted=5, rows_still_without_rate=1, executed=True,
    )
    name = "corpscout._tmp_ratsit_fx_a1b2c3"
    statements = [s for s, _ in client.statements]
    assert statements[1] == join_table_ddl(name)
    assert statements[2] == join_insert_sql(name)
    rows = client.statements[2][1]
    assert rows == [
        (date(2023, 12, 31), Decimal("0.099"), date(2023, 12, 31), "ecb"),
        (date(2021, 6, 30), Decimal("0.117"), date(2021, 6, 30), "ecb"),
    ]
    assert statements[3] == usd_update_sql(name)
    assert "FROM system.mutations" in statements[4] and "FROM system.mutations" in statements[5]
    assert client.statements[4][1] == {
        "database": "corpscout", "table": "se_ratsit_financial_periods", "pattern": f"%{name}%",
    }
    assert slept == [5.0]  # one poll came back not done
    assert statements[6] == f"DROP TABLE IF EXISTS {name}"
    assert statements[7].startswith("SELECT ifNull(period_end")  # the after-count


def test_execute_with_no_rate_found_writes_nothing() -> None:
    client = FakeClient(pending=[PENDING])
    counts = convert_ratsit_financial_periods(client, FakeRates({}), run_id="r", execute=True)
    assert counts.rates_found == 0 and counts.rows_converted == 0 and counts.executed is True
    assert len(client.statements) == 1


def test_wait_for_mutation_raises_on_failure_and_on_timeout() -> None:
    failed = FakeClient(pending=[], statuses=[(0, "Code: 241. DB::Exception: memory")])
    with pytest.raises(RuntimeError, match="memory"):
        wait_for_mutation(failed, join_table="corpscout._tmp_ratsit_fx_x", sleep=lambda _: None)
    slow = FakeClient(pending=[], statuses=[(0, ""), (0, ""), (0, "")])
    with pytest.raises(TimeoutError):
        wait_for_mutation(
            slow, join_table="corpscout._tmp_ratsit_fx_x", poll_seconds=5.0,
            timeout_seconds=10.0, sleep=lambda _: None,
        )
    # A mutation row that has not appeared yet is not a failure: keep polling.
    late = FakeClient(pending=[], statuses=[None, (1, "")])
    wait_for_mutation(late, join_table="corpscout._tmp_ratsit_fx_x", sleep=lambda _: None)


def test_the_join_table_is_dropped_when_the_mutation_fails() -> None:
    client = FakeClient(pending=[PENDING], statuses=[(0, "boom")])
    rates = FakeRates({"2023-12-31": Decimal("0.099")})
    with pytest.raises(RuntimeError, match="boom"):
        convert_ratsit_financial_periods(client, rates, run_id="r", execute=True, sleep=lambda _: None)
    assert [s for s, _ in client.statements][-1] == "DROP TABLE IF EXISTS corpscout._tmp_ratsit_fx_r"
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run --env-file .env pytest tests/test_sweden_ratsit_financial_usd.py -q
```

Expected: FAIL at import, `ImportError: cannot import name 'UsdCounts'`.

- [ ] **Step 3: Append the run function to `financial_usd.py`**

```python
@dataclass(frozen=True)
class UsdCounts:
    rows_pending: int
    rate_dates_needed: int
    rates_found: int
    rows_convertible: int
    rows_converted: int
    rows_still_without_rate: int
    executed: bool

    def as_metadata(self) -> dict[str, int | bool]:
        return {
            "rows_pending": self.rows_pending,
            "rate_dates_needed": self.rate_dates_needed,
            "rates_found": self.rates_found,
            "rows_convertible": self.rows_convertible,
            "rows_converted": self.rows_converted,
            "rows_still_without_rate": self.rows_still_without_rate,
            "executed": self.executed,
        }


def load_usd_rates(
    exchange_rates: Any, requests: Sequence[ExchangeRateRequest]
) -> dict[tuple[str, str], Any]:
    """Batches of RATE_REQUEST_BATCH; a batch the client refuses (LookupError on any date)
    is retried one request at a time so only the missing dates are dropped. Same shape as
    sweden_financial.usd_conversion._load_rates."""
    rates: dict[tuple[str, str], Any] = {}
    for start in range(0, len(requests), RATE_REQUEST_BATCH):
        batch = list(requests[start : start + RATE_REQUEST_BATCH])
        try:
            rates.update(exchange_rates.usd_rates(batch))
        except LookupError:
            for request in batch:
                try:
                    rates.update(exchange_rates.usd_rates([request]))
                except LookupError:
                    continue
    return rates


def _pending(client: Any) -> list[tuple[date, int]]:
    return [(row[0], int(row[1])) for row in client.execute(pending_rate_dates_sql())]


def wait_for_mutation(
    client: Any,
    *,
    join_table: str,
    poll_seconds: float = 5.0,
    timeout_seconds: float = 7200.0,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Poll system.mutations for the mutation whose command names `join_table` until it is
    done. A failure reason raises RuntimeError; no completion within the timeout raises
    TimeoutError. A poll that finds no row yet keeps waiting (the row appears right after
    the ALTER returns, but a replica lag is possible)."""
    waited = 0.0
    params = {
        "database": DATABASE,
        "table": RATSIT_FINANCIAL_PERIODS_TABLE,
        "pattern": f"%{join_table}%",
    }
    while True:
        rows = client.execute(mutation_status_sql(), params)
        if rows:
            is_done, fail_reason = rows[0][0], rows[0][1]
            if fail_reason:
                raise RuntimeError(f"USD mutation failed: {fail_reason}")
            if int(is_done) == 1:
                return
        if waited >= timeout_seconds:
            raise TimeoutError(
                f"USD mutation still running after {timeout_seconds:.0f} s (join table {join_table})"
            )
        sleep(poll_seconds)
        waited += poll_seconds


def convert_ratsit_financial_periods(
    client: Any,
    exchange_rates: Any,
    *,
    run_id: str,
    execute: bool,
    log: Callable[..., object] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> UsdCounts:
    """Preview (execute=False) counts the pending rows and the rates that exist for them and
    writes nothing. Execute loads the rates into a run-scoped Join table, runs the mutation,
    waits for it, drops the join table, and re-counts what is still pending."""
    pending = _pending(client)
    rows_pending = sum(rows for _, rows in pending)
    requests = [
        ExchangeRateRequest(currency=RATSIT_FINANCIAL_CURRENCY, rate_date=rate_date.isoformat())
        for rate_date, _ in pending
    ]
    rates = load_usd_rates(exchange_rates, requests)
    rows_convertible = sum(
        rows for rate_date, rows in pending
        if (RATSIT_FINANCIAL_CURRENCY, rate_date.isoformat()) in rates
    )
    counts = UsdCounts(
        rows_pending=rows_pending,
        rate_dates_needed=len(pending),
        rates_found=len(rates),
        rows_convertible=rows_convertible,
        rows_converted=0,
        rows_still_without_rate=rows_pending,
        executed=execute,
    )
    if log is not None:
        log(
            "Ratsit financial USD %s: rows_pending=%s rate_dates_needed=%s rates_found=%s rows_convertible=%s",
            "execute" if execute else "preview",
            rows_pending, len(pending), len(rates), rows_convertible,
        )
    if not execute or not rates:
        return counts

    join_table = join_table_name(run_id)
    client.execute(join_table_ddl(join_table))
    try:
        client.execute(
            join_insert_sql(join_table),
            [
                (
                    date.fromisoformat(rate.requested_rate_date),
                    rate.rate,
                    date.fromisoformat(str(rate.rate_date)),
                    str(rate.source),
                )
                for rate in rates.values()
            ],
        )
        client.execute(usd_update_sql(join_table))
        wait_for_mutation(client, join_table=join_table, sleep=sleep)
    finally:
        client.execute(f"DROP TABLE IF EXISTS {join_table}")

    still = sum(rows for _, rows in _pending(client))
    counts = replace(counts, rows_converted=rows_pending - still, rows_still_without_rate=still)
    if log is not None:
        log(
            "Ratsit financial USD converted rows_converted=%s rows_still_without_rate=%s",
            counts.rows_converted, counts.rows_still_without_rate,
        )
    return counts
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run --env-file .env pytest tests/test_sweden_ratsit_financial_usd.py -q
```

Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_ratsit/financial_usd.py \
        corpscout/services/dagster_v3/tests/test_sweden_ratsit_financial_usd.py
git commit -m "feat(ratsit): convert_ratsit_financial_periods loads rates, mutates pending rows and waits"
```

---

### Task 4: The mutation on a real engine (clickhouse-local)

**Files:**
- Test: `tests/test_sweden_ratsit_financial_usd_clickhouse_local.py` (new)

**Interfaces:**
- Consumes: Task 2's builders; `tests/clickhouse_local.py::clickhouse_local_command`; migrations 000343 (creates the table), 000346 (adds `period_kind`), 000400.

- [ ] **Step 1: Write the test**

```python
"""The USD mutation of financial_usd.py on a real ClickHouse (spec 2026-09-11 section 3):
the table as migrations 000343 + 000346 + 000400 leave it, four kinds of row, the mutation
run twice. Needs clickhouse-local or Docker; skips otherwise."""

import subprocess
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pytest

from dagster_v3.defs.sweden_ratsit.financial_usd import (
    join_insert_sql,
    join_table_ddl,
    pending_rate_dates_sql,
    usd_update_sql,
)
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
TABLE = "corpscout.se_ratsit_financial_periods"
JOIN = "corpscout._tmp_ratsit_fx_test"


def _statements_for_periods_table() -> list[str]:
    """CREATE DATABASE, the periods CREATE TABLE from 000343, and every ALTER of the periods
    table from 000346 and 000400, in order; comments stripped."""
    out: list[str] = ["CREATE DATABASE IF NOT EXISTS corpscout"]
    for name in (
        "000343_corpscout_se_ratsit_normalized_segments.up.sql",
        "000346_corpscout_se_ratsit_normalization_v2.up.sql",
        "000400_corpscout_se_ratsit_financial_periods_usd.up.sql",
    ):
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.startswith(f"CREATE TABLE IF NOT EXISTS {TABLE}") or statement.startswith(
                f"ALTER TABLE {TABLE}"
            ):
                out.append(statement)
    assert sum(s.startswith("CREATE TABLE") for s in out) == 1
    return out


ROWS = """INSERT INTO corpscout.se_ratsit_financial_periods
(company_id, result_sha256, normalizer_version, financial_report_index, period_index, period_kind,
 scope, monetary_unit, fiscal_year, period_start, period_end, period_months,
 revenue_amount, equity_amount, personnel_cost_per_employee_msek, employee_count, normalized_at)
VALUES
('5567081699', repeat('a', 64), 'ratsit-normalizer-v2', 0, 0, 'financial_and_employment', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 57.1, 3.4, 0.9, 21, now64(6)),
('5567081699', repeat('a', 64), 'ratsit-normalizer-v2', 0, 1, 'financial_only', 'company', 'TSEK', 2022, NULL, NULL, NULL, 1234.5, NULL, NULL, NULL, now64(6)),
('5560000001', repeat('b', 64), 'ratsit-normalizer-v2', 0, 0, 'financial_only', 'company', 'MSEK', 2021, '2020-07-01', '2021-06-30', 12, 10, NULL, NULL, NULL, now64(6)),
('5560000002', repeat('c', 64), 'ratsit-normalizer-v2', 0, 0, 'employment_only', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, NULL, NULL, NULL, 7, now64(6))"""

RATES = f"""INSERT INTO {JOIN} VALUES
('2023-12-31', 0.099123456789, '2023-12-29', 'ecb'),
('2022-12-31', 0.095, '2022-12-30', 'ecb')"""

READ = f"""SELECT company_id, period_index, monetary_unit, revenue_amount, revenue_amount_usd,
       equity_amount_usd, personnel_cost_per_employee_usd, fx_rate_to_usd, fx_rate_date, fx_source
FROM {TABLE} FINAL ORDER BY company_id, period_index FORMAT TSV"""


def _run(statements: list[str]) -> list[str]:
    script = ";\n".join(statements) + ";\n"
    completed = subprocess.run(
        clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _cells(line: str) -> tuple:
    """A TSV line as a tuple: `\\N` is None, a numeric cell is a Decimal (ClickHouse trims a
    Decimal's trailing zeros, so 89211.111110 prints as 89211.11111), anything else a str."""
    out = []
    for cell in line.split("\t"):
        if cell == "\\N":
            out.append(None)
        else:
            try:
                out.append(Decimal(cell))
            except InvalidOperation:
                out.append(cell)
    return tuple(out)


def test_the_mutation_scales_derives_dates_and_leaves_unrated_rows_for_next_time() -> None:
    schema = _statements_for_periods_table()
    before = pending_rate_dates_sql() + " FORMAT TSV"
    lines = _run(
        schema
        + [ROWS, before, join_table_ddl(JOIN), RATES, usd_update_sql(JOIN, mutations_sync=2), READ]
        + [before]
    )
    # Pending before: three dates (the employment_only row has no money and is not pending).
    assert lines[:3] == ["2021-06-30\t1", "2022-12-31\t1", "2023-12-31\t1"]
    rows = [_cells(line) for line in lines[3:7]]
    D = Decimal
    # MSEK 57.1 at 0.099123456789: 57.1e6 * rate to six decimals; equity and the per-employee
    # figure (always MSEK) with the same rate; fx columns filled.
    assert rows[0] == (
        D("5567081699"), D(0), "MSEK", D("57.1"), D("5659949.382651"), D("337019.753083"),
        D("89211.111110"), D("0.099123456789"), "2023-12-29", "ecb",
    )
    # Undated TSEK row: rate date Dec 31 2022, 1234.5e3 * 0.095.
    assert rows[1] == (
        D("5567081699"), D(1), "TSEK", D("1234.5"), D("117277.5"), None, None,
        D("0.095"), "2022-12-30", "ecb",
    )
    # No rate for 2021-06-30 in the join table: untouched, still pending.
    assert rows[2] == (D("5560000001"), D(0), "MSEK", D(10), None, None, None, None, None, "")
    # Employees only: never money, never pending, never converted.
    assert rows[3] == (D("5560000002"), D(0), "MSEK", None, None, None, None, None, None, "")
    assert lines[7:] == ["2021-06-30\t1"]


def test_the_mutation_is_idempotent() -> None:
    schema = _statements_for_periods_table()
    once = _run(schema + [ROWS, join_table_ddl(JOIN), RATES, usd_update_sql(JOIN, mutations_sync=2), READ])
    twice = _run(
        schema
        + [ROWS, join_table_ddl(JOIN), RATES, usd_update_sql(JOIN, mutations_sync=2),
           usd_update_sql(JOIN, mutations_sync=2), READ]
    )
    assert once == twice
```

- [ ] **Step 2: Run it**

```bash
uv run --env-file .env pytest tests/test_sweden_ratsit_financial_usd_clickhouse_local.py -q -m integration
```

Expected: 2 passed (Docker pulls `clickhouse/clickhouse-server:26.5` on first use; the image is already present on this machine from the 2026-09-11 proof). If a Decimal differs only in its sixth decimal (round-half-even of the engine vs the expected string), correct the expected literal to the engine's value; any larger difference is a bug in the SQL.

- [ ] **Step 3: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/tests/test_sweden_ratsit_financial_usd_clickhouse_local.py
git commit -m "test(ratsit): the USD mutation on clickhouse-local, scaling, derived dates, idempotence"
```

---

### Task 5: Asset, config, job, registration

**Files:**
- Modify: `src/dagster_v3/defs/sweden_ratsit/assets.py` (imports at the top; the asset, config and job before `defs = dg.Definitions(`; the `defs` block)
- Modify: `tests/test_sweden_ratsit_pilot.py` (append a registration test after `test_ratsit_dispatch_and_normalized_table_assets_are_registered`)

**Interfaces:**
- Consumes: Task 3's `convert_ratsit_financial_periods`, `UsdCounts`; `ExchangeRateClient.from_env()` from the `exchange_rates` package; `assert_clickhouse_tables_exist` (already imported in assets.py).
- Produces: asset key `se_ratsit_financial_periods_usd` (unpartitioned, group `sweden_ratsit`, parents `se_ratsit_financial_periods` and `exchange_rates_v2_clickhouse`), job `se_ratsit_financial_usd_job`, config class `RatsitFinancialUsdConfig(execute: bool = False)`. Slice 2's Ratsit extractor depends on this asset key.

- [ ] **Step 1: Write the failing registration test**

Append to `tests/test_sweden_ratsit_pilot.py`:

```python
def test_ratsit_financial_usd_asset_and_job_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    node = repository.asset_graph.get(dg.AssetKey("se_ratsit_financial_periods_usd"))
    assert node.parent_keys == {
        dg.AssetKey("se_ratsit_financial_periods"),
        dg.AssetKey("exchange_rates_v2_clickhouse"),
    }
    assert node.partitions_def is None
    assert node.group_name == "sweden_ratsit"
    assert repository.has_job("se_ratsit_financial_usd_job")
```

- [ ] **Step 2: Run it to verify it fails**

```bash
uv run --env-file .env pytest tests/test_sweden_ratsit_pilot.py -q -k financial_usd
```

Expected: FAIL, the asset key is not in the graph (`KeyError` or an assertion on `get`).

- [ ] **Step 3: Add the asset, config and job to `assets.py`**

Imports, added to the existing import block (keep alphabetical groups):

```python
from exchange_rates import ExchangeRateClient

from dagster_v3.defs.sweden_ratsit.financial_usd import (
    QUALIFIED_PERIODS_TABLE,
    convert_ratsit_financial_periods,
)
```

Then, directly before `defs = dg.Definitions(`:

```python
class RatsitFinancialUsdConfig(dg.Config):
    # False previews: counts the pending rows and the rates that exist for them, writes nothing.
    # True loads the rates into a run-scoped Join table, runs ONE mutation over the pending rows
    # and waits for it. Same gate as the financial entity's extractors.
    execute: bool = False


@dg.asset(
    name="se_ratsit_financial_periods_usd",
    deps=[
        dg.AssetKey(RATSIT_FINANCIAL_PERIODS_TABLE),
        dg.AssetDep(
            dg.AssetKey("exchange_rates_v2_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
    ],
    group_name="sweden_ratsit",
    kinds={"python", "clickhouse", "fx", "ratsit"},
    tags={"country": "sweden", "source": "ratsit", "source_name": "sweden_ratsit", "layer": "normalized"},
    metadata={"table": QUALIFIED_PERIODS_TABLE},
    description=(
        "Fills the USD twins and the fx columns of se_ratsit_financial_periods in place for "
        "every row still without a rate (financial entity spec 2026-09-11, slice 0): SEK to "
        "USD at the period end (Dec 31 of the fiscal year for undated rows) through the "
        "shared exchange-rate client, scaled from the row's MSEK/TSEK unit first. "
        "Re-runnable: a row is converted once; a row whose rate does not exist yet waits "
        "for a later run. Preview by default (config execute)."
    ),
)
def se_ratsit_financial_periods_usd(
    context: dg.AssetExecutionContext,
    config: RatsitFinancialUsdConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_FINANCIAL_PERIODS_TABLE,),
    )
    with clickhouse.get_connection() as client:
        counts = convert_ratsit_financial_periods(
            client,
            ExchangeRateClient.from_env(),
            run_id=context.run_id,
            execute=config.execute,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "table": QUALIFIED_PERIODS_TABLE}
    )


se_ratsit_financial_usd_job = dg.define_asset_job(
    name="se_ratsit_financial_usd_job",
    selection=dg.AssetSelection.assets(se_ratsit_financial_periods_usd),
    description=(
        "Fill the USD twins of se_ratsit_financial_periods for every row still without a "
        "rate. Run with execute: true; the default previews."
    ),
)
```

And in `defs = dg.Definitions(...)`:

```python
    assets=[
        se_ratsit_scan_dispatch,
        se_ratsit_normalized,
        sweden_ratsit_translation_load,
        se_ratsit_financial_periods_usd,
    ],
    ...
    jobs=[se_ratsit_scan_dispatch_job, se_ratsit_normalize_job, se_ratsit_financial_usd_job],
```

- [ ] **Step 4: Run the registration test and the definitions check**

```bash
uv run --env-file .env pytest tests/test_sweden_ratsit_pilot.py -q -k "registered"
uv run dg check defs
```

Expected: both registration tests pass; `dg check defs` reports no errors (the known `company_domain_suggestions` adapter traceback is noise, not a failure).

- [ ] **Step 5: Run the full Ratsit and migration suites**

```bash
uv run --env-file .env pytest tests/test_sweden_ratsit_normalization.py tests/test_sweden_ratsit_pilot.py tests/test_sweden_ratsit_financial_usd.py tests/test_clickhouse_migrations.py -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_ratsit/assets.py \
        corpscout/services/dagster_v3/tests/test_sweden_ratsit_pilot.py
git commit -m "feat(ratsit): se_ratsit_financial_periods_usd asset and job, preview by default"
```

---

### Task 6: Docs and whole-slice check

**Files:**
- Modify: `src/dagster_v3/defs/sweden_ratsit/docs/sweden-ratsit-normalization-v2-proposal.md` (the "Field semantics" list under "## 7. Financial periods")
- Modify: `docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` (section 12, slice 0 line: add "Code complete <date>, branch se-financial-entity; prod pending")

- [ ] **Step 1: Document the twins where the field semantics live**

Append to the "Field semantics" bullet list of the v2 proposal:

```markdown
- Since migration 000400 (financial entity spec 2026-09-11, slice 0) every monetary column
  has a `_usd` twin filled in place by the asset `se_ratsit_financial_periods_usd`: scale by
  the row's `monetary_unit` (the two per-employee figures are always MSEK), then the SEK to
  USD rate at `period_end` (Dec 31 of `fiscal_year` when undated) from the shared
  exchange-rate client; `fx_rate_to_usd`, `fx_rate_date`, `fx_source` record the rate.
  `average_salary` has no twin until its unit is known; the ratios are not money.
```

- [ ] **Step 2: Record code completion in the spec**

In section 12, item 0, append: `Code complete 2026-09-11 on branch se-financial-entity (Tasks 1 to 5 of plan 2026-09-11-se-company-financial-0-ratsit-usd.md); prod rollout pending.`

- [ ] **Step 3: Run the wider suite once**

```bash
uv run --env-file .env pytest tests -q -x --ignore=tests/test_schedule_cron_contracts.py 2>&1 | tail -5
```

Expected: pass except the pre-existing baseline failures the repo carries (schedule-contract collisions, backfill-policy, duckdb-bulk, NACE, cron-uniqueness). A failure outside that set is this slice's to fix before Task 7.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_ratsit/docs/sweden-ratsit-normalization-v2-proposal.md \
        corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md
git commit -m "docs(ratsit): USD twins in the field semantics; spec records slice 0 code complete"
```

---

### Task 7: Prod rollout (owner-gated: wait for "do it")

**Files:** none. Runs against the companycollect host.

**Preconditions to verify before asking:**
- `git log --oneline main..se-financial-entity` shows the six commits above and the spec; the branch is reviewed.
- `ls /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations | tail -2` on main shows 000399 as the newest (else renumber and re-run the migration tests).
- `ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --query "SELECT version, dirty FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1"'` prints `399	0`.

- [ ] **Step 1: Merge to main from the main checkout, only if its tree is clean or the owner merges**

The main checkout carries the owner's uncommitted work; do not commit or stash it. Ask the owner to run, or run when clean:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git branch --show-current   # must print main
git merge --no-ff se-financial-entity -m "Merge branch 'se-financial-entity' (financial slice 0: Ratsit USD)"
```

- [ ] **Step 2: Apply migration 000400 (single step, from the worktree so no other uncommitted migration can ride along)**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity/corpscout
test -f .env || cp /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/.env .env
make clickhouse-migrate-version      # expect 399
make clickhouse-migrate-up-one       # applies 000400 only
make clickhouse-migrate-version      # expect 400
```

Verify the columns:

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --query "SELECT count() FROM system.columns WHERE database = '"'"'corpscout'"'"' AND table = '"'"'se_ratsit_financial_periods'"'"' AND (name LIKE '"'"'%_usd'"'"' OR name LIKE '"'"'fx_%'"'"')"'
```

Expected: `23`.

- [ ] **Step 3: Deploy dagster_v3 from a pristine worktree (the deploy recipe)**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity/corpscout/services/dagster_v3
test -f .env || cp /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/.env .env
git status --porcelain src | wc -l    # must be 0
uv sync --frozen
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/finland_ytj/dbt --profiles-dir src/dagster_v3/defs/finland_ytj/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/exchange_rates_v2/dbt --profiles-dir src/dagster_v3/defs/exchange_rates_v2/dbt
uv run --frozen --no-sync dg utils refresh-defs-state
uv run --frozen --no-sync dg check defs
cd ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml; echo "RC=$?"
```

Expected: `RC=0`. The deploy ships the worktree's tree, which must equal the merged main: after Step 1 run `git merge --ff-only main` on the branch in the worktree (main itself cannot be checked out there, the main checkout holds it) and confirm `git diff main --stat` prints nothing.

- [ ] **Step 4: Preview run**

In the Dagster UI (location `dagster_v3`), materialize `se_ratsit_financial_periods_usd` with the default config (`execute: false`). Record from the run metadata: `rows_pending` (expect about 3.1M), `rate_dates_needed`, `rates_found`, `rows_convertible`, `rows_still_without_rate`.

Expected: `rates_found` equals `rate_dates_needed` (the client answers every date with the nearest rate, before or after), so `rows_convertible == rows_pending`.

- [ ] **Step 5: Execute run (owner's "do it")**

Materialize again with config:

```yaml
ops:
  se_ratsit_financial_periods_usd:
    config:
      execute: true
```

The mutation rewrites every part of the table once (3.1M rows, a few GB); the asset polls every 5 s for up to two hours. Expected metadata: `rows_converted` about 3.1M, `rows_still_without_rate` 0.

- [ ] **Step 6: Verify on prod**

```bash
ssh -o ConnectTimeout=20 companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --multiquery --format PrettyCompactMonoBlock' <<'SQL'
SELECT monetary_unit, count() AS rows, countIf(fx_rate_to_usd IS NULL) AS without_rate, countIf(revenue_amount IS NOT NULL AND revenue_amount_usd IS NULL) AS revenue_unconverted FROM corpscout.se_ratsit_financial_periods FINAL GROUP BY monetary_unit;
SELECT fiscal_year, revenue_amount, revenue_amount_usd, fx_rate_to_usd, fx_rate_date, fx_source FROM corpscout.se_ratsit_financial_periods FINAL WHERE company_id = '5567081699' AND scope = 'company' ORDER BY fiscal_year DESC LIMIT 3;
SELECT 'ratsit usd vs bolagsverket usd, same fy, reported' AS what, count() AS pairs, countIf(abs(toFloat64(r.usd) - toFloat64(b.usd)) <= 0.02 * greatest(abs(toFloat64(b.usd)), 1)) AS agree_2pct FROM (SELECT company_id, fiscal_year, any(revenue_amount_usd) AS usd FROM corpscout.se_ratsit_financial_periods FINAL WHERE scope = 'company' AND revenue_amount_usd IS NOT NULL GROUP BY company_id, fiscal_year) r INNER JOIN (SELECT company_id, toUInt16(fiscal_year) AS fiscal_year, any(revenue_amount_usd) AS usd FROM corpscout.se_bolagsverket_financial_metrics FINAL WHERE observation_kind = 'reported' AND revenue_amount_usd IS NOT NULL AND fiscal_year IS NOT NULL GROUP BY company_id, fiscal_year) b USING (company_id, fiscal_year);
SELECT count() AS leftover_join_tables FROM system.tables WHERE database = 'corpscout' AND name LIKE '_tmp_ratsit_fx_%';
SQL
```

Expected: `without_rate` 0 for every unit; for 5567081699 fiscal 2023 `revenue_amount` 60.3 and `revenue_amount_usd` about 5.9M (60.3e6 times the ECB SEK→USD rate of 2023-12-29, about 0.099); the USD agreement with Bolagsverket within 2% on roughly half the pairs, the same share as the native comparison of 2026-09-11 (rounding, not rate); `leftover_join_tables` 0.

- [ ] **Step 7: Record the rollout**

In the spec section 12 item 0, append the prod record: date, migration 400 applied, run ids of the preview and execute runs, `rows_converted`, `rows_still_without_rate`, the three verification numbers. Commit on main (or on the branch and merge) with:

```bash
git commit -m "docs(se-financial): slice 0 shipped, prod record"
```

Then update the memory file `se-financial-entity.md` (slice 0 DONE, the numbers) and start slice 1's plan.

---

## Self-review

**Spec coverage (section 3):** table change → Task 1; the asset's four steps → Tasks 2, 3, 5 (pending scan, rate client, join table + mutation, counts and `execute`); the qualified join-table rule → Task 2 (`JOIN_TABLE_PREFIX` carries the database) and its test; idempotence and untouched unrated rows → Task 4; job → Task 5; prod preview, execute, spot check → Task 7; the "extract job selects it first" clause belongs to slice 2 (the extract job does not exist yet).

**Placeholder scan:** none; every code step carries its code; Task 4's "correct the expected strings" clause is bounded to a sixth-decimal rounding difference.

**Type consistency:** `UsdCounts` fields are the same seven in the dataclass, `as_metadata`, both tests and the asset metadata; `usd_update_sql(join_table, *, mutations_sync=0)` is called with the keyword in Task 4 and without in Task 3; `wait_for_mutation` takes `join_table` as a keyword everywhere; `FakeClient.statuses` rows are `(is_done, latest_fail_reason)` tuples or `None`, matching what `wait_for_mutation` reads (`rows[0][0]`, `rows[0][1]`, empty list for `None`).
