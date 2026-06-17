# Exchange Rates ClickHouse Final Step Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove runtime ClickHouse table creation from exchange-rate asset materialization and verify the last exchange-rate ClickHouse asset can materialize rows from a DuckDB-shaped fixture into ClickHouse.

**Architecture:** ClickHouse schema creation belongs to repository migrations, not Dagster asset execution. The exchange-rate assets will keep their existing dlt-to-ClickHouse load path, but `_run_exchange_rates_partition` will only delete the target date window and run dlt. The test will materialize `exchange_rates_backfill` with a fake dlt resource that reads a DuckDB fixture table matching the exchange-rate table schema and inserts those rows through a fake ClickHouse client, proving the asset issues no `CREATE` SQL and writes expected content.

**Tech Stack:** Dagster assets, dagster-dlt asset definitions, DuckDB test fixture, pytest, fake ClickHouse resource/client.

---

## File Structure

- Modify `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`
  - Remove the import and call to `prepare_exchange_rates_table`.
  - Keep deletion and dlt load logic unchanged.
- Delete `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/clickhouse.py`
  - The file only contains runtime DDL helper code that is no longer allowed in asset execution.
- Modify `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`
  - Remove tests that assert runtime `CREATE DATABASE` / `CREATE TABLE` behavior.
  - Add a Dagster materialization test for `exchange_rates_backfill_asset` using a DuckDB fixture table and fake ClickHouse resource.

### Task 1: Add Failing Materialization Test

**Files:**
- Modify: `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`

- [ ] **Step 1: Write the failing test**

Add imports:

```python
from pathlib import Path

import duckdb
```

Update `FakeClickHouseClient` so it records SQL and inserted rows:

```python
class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserted_rows: list[tuple] = []

    def execute(self, sql: str, params: list[tuple] | None = None) -> None:
        self.statements.append(sql)
        if params is not None:
            self.inserted_rows.extend(params)
```

Add helper classes and fixture helpers:

```python
class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self) -> Iterator[FakeClickHouseClient]:
        yield self.client


class DuckDbBackedDltResource:
    def __init__(self, duckdb_path: Path, clickhouse: FakeClickHouseResource) -> None:
        self.duckdb_path = duckdb_path
        self.clickhouse = clickhouse

    def run(self, **_: object) -> Iterator[dg.MaterializeResult]:
        with duckdb.connect(str(self.duckdb_path), read_only=True) as connection:
            rows = connection.execute(
                """
                select
                  rate_date,
                  base_currency,
                  quote_currency,
                  rate,
                  source,
                  source_url,
                  source_payload_hash,
                  source_run_id,
                  pulled_at,
                  _dlt_load_id,
                  _dlt_id
                from reference.exchange_rates
                order by rate_date, quote_currency
                """
            ).fetchall()
        with self.clickhouse.get_connection() as client:
            client.execute(
                "INSERT INTO reference.exchange_rates VALUES",
                rows,
            )
        yield dg.MaterializeResult(metadata={"inserted_rows": len(rows)})
```

Add DuckDB fixture creator:

```python
def _create_exchange_rates_duckdb_fixture(duckdb_path: Path) -> None:
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute("create schema reference")
        connection.execute(
            """
            create table reference.exchange_rates (
              rate_date date,
              base_currency varchar,
              quote_currency varchar,
              rate decimal(18, 8),
              source varchar,
              source_url varchar,
              source_payload_hash varchar,
              source_run_id varchar,
              pulled_at timestamp,
              _dlt_load_id varchar,
              _dlt_id varchar
            )
            """
        )
        connection.execute(
            """
            insert into reference.exchange_rates values
              ('2026-05-01', 'EUR', 'USD', 1.15800000, 'ECB EXR', 'https://ecb.example/USD', repeat('a', 64), 'run-test', '2026-05-01 12:00:00', 'load-1', 'row-1'),
              ('2026-05-01', 'EUR', 'EUR', 1.00000000, 'identity', '', repeat('0', 64), 'run-test', '2026-05-01 12:00:00', 'load-1', 'row-2')
            """
        )
```

Add the behavior test:

```python
def test_exchange_rates_backfill_materialization_loads_duckdb_rows_without_runtime_ddl(
    tmp_path: Path,
) -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    duckdb_path = tmp_path / "exchange_rates.duckdb"
    _create_exchange_rates_duckdb_fixture(duckdb_path)
    client = FakeClickHouseClient()
    clickhouse = FakeClickHouseResource(client)
    dlt_resource = DuckDbBackedDltResource(duckdb_path, clickhouse)

    result = dg.materialize(
        [fx_assets.exchange_rates_backfill_asset],
        partition_key="2026-05-01",
        resources={
            "clickhouse": clickhouse,
            "dlt": dlt_resource,
        },
        run_config={
            "ops": {
                "exchange_rates_backfill": {
                    "config": {
                        "currencies": ["USD"],
                    }
                }
            }
        },
    )

    assert result.success
    assert not any(statement.upper().startswith("CREATE ") for statement in client.statements)
    assert client.statements[0].startswith("ALTER TABLE reference.exchange_rates DELETE WHERE")
    assert client.statements[1] == "INSERT INTO reference.exchange_rates VALUES"
    assert [(row[0].isoformat(), row[1], row[2], str(row[3])) for row in client.inserted_rows] == [
        ("2026-05-01", "EUR", "EUR", "1.00000000"),
        ("2026-05-01", "EUR", "USD", "1.15800000"),
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_exchange_rates_backfill_materialization_loads_duckdb_rows_without_runtime_ddl -q
```

Expected: FAIL because the asset currently issues `CREATE DATABASE IF NOT EXISTS reference` before deletion/load, so the assertion against runtime DDL fails.

### Task 2: Remove Runtime Exchange-Rate DDL

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`
- Delete: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/clickhouse.py`
- Modify: `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`

- [ ] **Step 1: Remove production DDL call**

In `assets.py`, remove:

```python
from dagster_v3.defs.exchange_rates.clickhouse import prepare_exchange_rates_table
```

And remove this block from `_run_exchange_rates_partition`:

```python
context.log.info(
    "Preparing ClickHouse table %s for exchange-rate %s partition",
    tables.QUALIFIED_EXCHANGE_RATES_TABLE,
    asset_label,
)
prepare_exchange_rates_table(clickhouse)
```

- [ ] **Step 2: Remove obsolete helper tests and imports**

In `test_exchange_rates_assets.py`, remove:

```python
from typing import get_type_hints
from dagster_v3.defs.exchange_rates.clickhouse import prepare_exchange_rates_table
```

Delete these tests:

```python
def test_prepare_exchange_rates_table_is_typed_for_official_resource() -> None: ...
def test_prepare_exchange_rates_table_uses_reference_database(monkeypatch) -> None: ...
```

- [ ] **Step 3: Delete obsolete helper module**

Delete `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/clickhouse.py` because migrations now own ClickHouse table creation.

- [ ] **Step 4: Run focused exchange-rate tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py -q
```

Expected: PASS.

### Task 3: Validate Repository Definitions

**Files:**
- No source files changed.

- [ ] **Step 1: Run Dagster definition check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: PASS, confirming no stale import or definition load failure from removing `exchange_rates/clickhouse.py`.

### Task 4: Commit

**Files:**
- Modified: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`
- Deleted: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/clickhouse.py`
- Modified: `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`
- Created: `corpscout/dagster_v3/docs/superpowers/plans/2026-06-17-exchange-rates-clickhouse-final-step.md`

- [ ] **Step 1: Review diff**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git diff -- corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/clickhouse.py corpscout/dagster_v3/tests/test_exchange_rates_assets.py corpscout/dagster_v3/docs/superpowers/plans/2026-06-17-exchange-rates-clickhouse-final-step.md
```

- [ ] **Step 2: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/clickhouse.py corpscout/dagster_v3/tests/test_exchange_rates_assets.py corpscout/dagster_v3/docs/superpowers/plans/2026-06-17-exchange-rates-clickhouse-final-step.md
git commit -m "fix: remove exchange rate runtime clickhouse ddl"
```

## Self-Review

Spec coverage:
- Runtime `CREATE TABLE IF NOT EXISTS` removal is covered by Task 2.
- Exchange-rates section is scoped first, per request.
- Full materialization test is covered by Task 1 using `dg.materialize`.
- Dummy data lives in DuckDB with the same exchange-rate table schema used by the ClickHouse asset load path.
- Test checks inserted ClickHouse content and absence of runtime DDL.

Placeholder scan:
- No `TBD`, `TODO`, or incomplete task references remain.

Type consistency:
- Test helpers use `FakeClickHouseClient`, `FakeClickHouseResource`, and `DuckDbBackedDltResource` consistently.
- The materialized Dagster asset is `exchange_rates_backfill_asset`, whose public asset key remains `exchange_rates_backfill`.
