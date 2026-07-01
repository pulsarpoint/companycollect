# Direct DuckDB To ClickHouse Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Finland YTJ Dagster assets out of the unclear `resolved.py` module, make `finland_ytj_resolved_clickhouse` copy DuckDB tables directly to migrated ClickHouse tables, then remove the over-generic `dagster_v3.defs.clickhouse.resolved` export layer.

**Architecture:** Finland YTJ asset definitions live in `dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py`; table/column contracts remain in `resolved_tables.py`. ClickHouse table DDL is owned by migrations, not Dagster helper code. Each source export asset should open its source DuckDB connection, truncate the migrated ClickHouse target table, insert rows in batches using that source's explicit table/column contract, and return row-count metadata. No temporary stage tables, no exchange/rollback abstraction, no shared "resolved" module deciding source behavior.

**Tech Stack:** Dagster assets, `dagster-clickhouse`, `dagster-duckdb`, DuckDB SQL, ClickHouse native client `execute`/`insert_arrow`/`insert_rows`, pytest.

---

## File Structure

### Primary Finland YTJ Change

- Modify: `dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py`
  - Move the dbt assets, ClickHouse export asset, job, schedule, and definitions from `resolved.py` into this file.
  - Remove import from `dagster_v3.defs.clickhouse.resolved`.
  - Add local constants:
    - `CLICKHOUSE_DATABASE = "corpscout"`
    - `CLICKHOUSE_INSERT_BATCH_SIZE = 50_000`
  - Add small local quoting/copy helpers used only by Finland YTJ.
  - Update `finland_ytj_resolved_clickhouse` to directly truncate and insert `fi_companies`, `fi_names`, `fi_websites`, `fi_industries`.

- Delete after move: `dagster_v3/src/dagster_v3/defs/finland_ytj/resolved.py`
  - `resolved.py` is a bad module name. It hides that the file currently defines Dagster assets, dbt registration, ClickHouse export, job, schedule, and resources.

- Keep: `dagster_v3/src/dagster_v3/defs/finland_ytj/resolved_tables.py`
  - This remains the Finland YTJ source table contract.
  - Continue using `tables.FINLAND_YTJ_RESOLVED_TABLES`.
  - Continue using `tables.RESOLVED_EXPORT_COLUMNS` so `source_payload_hash` stays in DuckDB/dbt but is not exported to ClickHouse.

- Modify: `dagster_v3/tests/test_finland_ytj_resolved_assets.py`
  - Add focused tests that prove the asset performs direct copy behavior without calling `clickhouse.resolved`.

### Shared Module Removal

- Delete after all imports are gone: `dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py`
- Delete: `dagster_v3/tests/test_clickhouse_resolved.py`
- Keep: `dagster_v3/src/dagster_v3/defs/clickhouse/resources.py`
  - This is a ClickHouse resource adapter, not the bad resolved export abstraction.

### Importers That Must Be Migrated Before Deleting `clickhouse/resolved.py`

Run:

```bash
rg "defs\.clickhouse\.resolved|clickhouse.resolved|assert_clickhouse_tables_exist|replace_duckdb_connection_tables_in_clickhouse|export_duckdb_connection_table_to_clickhouse|RESOLVED_DATABASE" dagster_v3/src dagster_v3/tests -n
```

Current source importers to migrate:

- `dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py`
- `dagster_v3/src/dagster_v3/defs/norway_brreg/assets/entity_clickhouse.py`
- `dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_statements.py`
- `dagster_v3/src/dagster_v3/defs/latvia_ur/clickhouse.py`
- `dagster_v3/src/dagster_v3/defs/estonia_ar/clickhouse.py`
- `dagster_v3/src/dagster_v3/defs/brazil_rfb/clickhouse.py`
- `dagster_v3/src/dagster_v3/defs/brazil_cnae/assets.py`
- `dagster_v3/src/dagster_v3/defs/open_page_rank/assets.py`
- `dagster_v3/src/dagster_v3/defs/domains/assets.py`
- `dagster_v3/src/dagster_v3/defs/gleif/assets.py`
- `dagster_v3/src/dagster_v3/defs/wikidata/assets.py`
- `dagster_v3/src/dagster_v3/defs/nace/assets.py`
- `dagster_v3/src/dagster_v3/defs/exchange_rates_v2/assets.py`
- `dagster_v3/src/dagster_v3/defs/finland_xbrl/clickhouse.py`
- `dagster_v3/src/dagster_v3/defs/czech_ares/clickhouse.py`
- `dagster_v3/src/dagster_v3/defs/france_sirene/clickhouse.py`
- `dagster_v3/src/dagster_v3/defs/slovakia_rpo/clickhouse.py`
- `dagster_v3/src/dagster_v3/defs/slovakia_financials/clickhouse.py`
- `dagster_v3/src/dagster_v3/defs/uk_companies_house/clickhouse.py`

---

## Task 1: Move Finland YTJ Asset Definitions Into `assets.py`

**Files:**
- Modify: `dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py`
- Delete: `dagster_v3/src/dagster_v3/defs/finland_ytj/resolved.py`
- Modify: `dagster_v3/tests/test_finland_ytj_resolved_assets.py`

- [ ] **Step 1: Move imports from `resolved.py` to `assets.py`**

Move these imports from `dagster_v3/src/dagster_v3/defs/finland_ytj/resolved.py` into `dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py`, merging duplicates instead of adding a second import block:

```python
import os
from collections.abc import Mapping

from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
    get_asset_key_for_model,
)
```

`assets.py` already imports `Iterator`, `Path`, `Any`, `dagster as dg`, `DuckDBResource`, and `duckdb_resource`; do not duplicate those.

- [ ] **Step 2: Move constants and dbt project setup into `assets.py`**

Add these constants near the existing Finland YTJ constants:

```python
GROUP_NAME = "finland_ytj"
RESOLVED_DUCKDB_SCHEMA = "finland_resolved"
FINLAND_RESOLVED_DBT_PROJECT_DIR = Path(__file__).parent / "dbt"

_DEFAULT_DUCKDB_PATH = Path(DEFAULT_DUCKDB_PATH).expanduser()
if not _DEFAULT_DUCKDB_PATH.is_absolute():
    _DEFAULT_DUCKDB_PATH = _DEFAULT_DUCKDB_PATH.resolve()
os.environ["FINLAND_YTJ_DUCKDB_PATH"] = str(_DEFAULT_DUCKDB_PATH)

finland_resolved_dbt_project = DbtProject(
    project_dir=FINLAND_RESOLVED_DBT_PROJECT_DIR,
    profiles_dir=FINLAND_RESOLVED_DBT_PROJECT_DIR,
)
finland_resolved_dbt_project.prepare_if_dev()
```

Keep `DEFAULT_DUCKDB_PATH = "data/finland_ytj.duckdb"` as the one user-facing default in this module. `_DEFAULT_DUCKDB_PATH` is only the absolute path object used by dbt and the Dagster DuckDB resource.

- [ ] **Step 3: Move dbt assets and export asset shell into `assets.py`**

Move these definitions from `resolved.py` into `assets.py`, below `finland_ytj_all_companies_duckdb_asset` and above the module-level `defs`:

```python
class FinlandResolvedDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return super().get_asset_key(dbt_resource_props)
        return dg.AssetKey(f"finland_ytj_resolved_{dbt_resource_props['name']}")

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return GROUP_NAME


@dbt_assets(
    manifest=finland_resolved_dbt_project.manifest_path,
    project=finland_resolved_dbt_project,
    dagster_dbt_translator=FinlandResolvedDbtTranslator(),
    pool="finland_ytj_duckdb",
)
def finland_resolved_dbt_assets(
    context: AssetExecutionContext,
    finland_resolved_dbt: DbtCliResource,
) -> Iterator[Any]:
    yield from finland_resolved_dbt.cli(["build"], context=context).stream()


@dg.asset(
    deps=[
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_companies"),
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_names"),
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_websites"),
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_industries"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool="finland_ytj_duckdb",
    description="Exports resolved Finland YTJ DuckDB tables to migrated ClickHouse tables.",
)
def finland_ytj_resolved_clickhouse(
    clickhouse: ClickhouseResource,
    ytj_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    ...
```

The function body will be replaced in Task 3.

- [ ] **Step 4: Move job, schedule, and definitions into `assets.py`**

Replace the existing `defs = dg.Definitions(...)` in `assets.py` with a single combined definition:

```python
finland_ytj_resolved_job = dg.define_asset_job(
    "finland_ytj_resolved_job",
    selection=dg.AssetSelection.assets("finland_ytj_resolved_clickhouse").upstream(),
)
finland_ytj_resolved_schedule = dg.ScheduleDefinition(
    name="finland_ytj_resolved_schedule",
    job=finland_ytj_resolved_job,
    cron_schedule="45 4 * * *",
    execution_timezone="Europe/Belgrade",
)


defs = dg.Definitions(
    assets=[
        finland_ytj_all_companies_duckdb_asset,
        finland_resolved_dbt_assets,
        finland_ytj_resolved_clickhouse,
    ],
    asset_checks=[all_companies_non_empty],
    jobs=[finland_ytj_resolved_job],
    schedules=[finland_ytj_resolved_schedule],
    resources={
        "ytj_duckdb": duckdb_resource(_DEFAULT_DUCKDB_PATH),
        "ytj_api": ytj_resources.YtjApiResource(),
        "finland_resolved_dbt": DbtCliResource(
            project_dir=finland_resolved_dbt_project,
            profiles_dir=FINLAND_RESOLVED_DBT_PROJECT_DIR,
        ),
    },
)
```

- [ ] **Step 5: Delete `resolved.py`**

Delete:

```text
dagster_v3/src/dagster_v3/defs/finland_ytj/resolved.py
```

- [ ] **Step 6: Verify no code imports `finland_ytj.resolved`**

Run:

```bash
rg "finland_ytj\.resolved|defs\.finland_ytj import resolved" dagster_v3/src dagster_v3/tests -n
```

Expected: no matches.

- [ ] **Step 7: Run Finland YTJ definition tests**

Run:

```bash
cd dagster_v3
uv run pytest tests/test_finland_ytj_resolved_assets.py -q
```

Expected: existing tests still pass after the module move.

---

## Task 2: Add Finland YTJ Direct Export Tests

**Files:**
- Modify: `dagster_v3/tests/test_finland_ytj_resolved_assets.py`

- [ ] **Step 1: Add a fake ClickHouse client and direct export test**

Append this test support code to `dagster_v3/tests/test_finland_ytj_resolved_assets.py`:

```python
from contextlib import contextmanager
from typing import Any

import dagster as dg
import duckdb
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.insert_calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def execute(self, statement: str, rows: list[tuple[Any, ...]] | None = None) -> list[tuple[Any, ...]]:
        normalized = " ".join(statement.split())
        if rows is None:
            self.statements.append(normalized)
        else:
            self.insert_calls.append((normalized, rows))
        return []


def test_finland_ytj_resolved_clickhouse_copies_duckdb_tables_directly(
    monkeypatch,
    tmp_path,
) -> None:
    from dagster_v3.defs.finland_ytj import assets

    duckdb_path = tmp_path / "finland_ytj.duckdb"
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar,
                country_iso2 varchar,
                name varchar,
                name_normalized varchar,
                registration_date date,
                end_date date,
                lifecycle_status varchar,
                is_active boolean,
                legal_form_code varchar,
                legal_form_description_original varchar,
                legal_form_description_language varchar,
                legal_form_description_en varchar,
                legal_form_description_translated_at timestamp,
                legal_form_description_translation_provider varchar,
                legal_form_description_translation_model varchar,
                primary_website_url varchar,
                primary_website_host varchar,
                source_system varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                resolved_at timestamp
            )
            """
        )
        connection.execute(
            """
            insert into finland_resolved.fi_companies (
                business_id,
                country_iso2,
                name,
                name_normalized,
                registration_date,
                end_date,
                lifecycle_status,
                is_active,
                legal_form_code,
                legal_form_description_original,
                legal_form_description_language,
                legal_form_description_en,
                legal_form_description_translated_at,
                legal_form_description_translation_provider,
                legal_form_description_translation_model,
                primary_website_url,
                primary_website_host,
                source_system,
                source_run_id,
                source_record_id,
                source_payload_hash,
                resolved_at
            ) values (
                '1234567-8',
                'FI',
                'Example Oy',
                'example oy',
                '2024-01-01',
                null,
                'active',
                true,
                'OY',
                'Osakeyhtio',
                'fi',
                'Limited company',
                null,
                '',
                '',
                'https://example.fi',
                'example.fi',
                'finland_prh_ytj',
                'run-1',
                '1234567-8',
                'not-exported',
                '2026-07-01 00:00:00'
            )
            """
        )
        for table_name in ("fi_names", "fi_websites", "fi_industries"):
            connection.execute(
                f"""
                create table finland_resolved.{table_name}
                as select * from finland_resolved.fi_companies where false
                """
            )

    client = FakeClickHouseClient()

    @contextmanager
    def fake_clickhouse_connection(self: ClickhouseResource):
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_clickhouse_connection)

    result = dg.materialize(
        [assets.finland_ytj_resolved_clickhouse],
        resources={
            "clickhouse": ClickhouseResource(host="localhost"),
            "ytj_duckdb": DuckDBResource(database=str(duckdb_path)),
        },
    )

    assert result.success
    assert client.statements[:4] == [
        "TRUNCATE TABLE `corpscout`.`fi_companies`",
        "TRUNCATE TABLE `corpscout`.`fi_names`",
        "TRUNCATE TABLE `corpscout`.`fi_websites`",
        "TRUNCATE TABLE `corpscout`.`fi_industries`",
    ]
    assert len(client.insert_calls) == 1
    statement, rows = client.insert_calls[0]
    assert statement.startswith("INSERT INTO `corpscout`.`fi_companies`")
    assert "`source_payload_hash`" not in statement
    assert rows[0][0] == "1234567-8"
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cd dagster_v3
uv run pytest tests/test_finland_ytj_resolved_assets.py::test_finland_ytj_resolved_clickhouse_copies_duckdb_tables_directly -q
```

Expected: FAIL because `finland_ytj_resolved_clickhouse` still calls the shared `replace_duckdb_connection_tables_in_clickhouse` helper and does not directly issue `TRUNCATE TABLE`.

---

## Task 3: Make Finland YTJ Export Direct

**Files:**
- Modify: `dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py`

- [ ] **Step 1: Remove the shared resolved import**

Remove any import from:

```python
dagster_v3.defs.clickhouse.resolved
```

with no import from `dagster_v3.defs.clickhouse.resolved`.

- [ ] **Step 2: Add local Finland YTJ copy constants**

Below `RESOLVED_DUCKDB_SCHEMA = "finland_resolved"` in `assets.py`, add:

```python
CLICKHOUSE_DATABASE = "corpscout"
CLICKHOUSE_INSERT_BATCH_SIZE = 50_000
```

- [ ] **Step 3: Replace `finland_ytj_resolved_clickhouse` implementation**

Use this shape:

```python
def finland_ytj_resolved_clickhouse(
    clickhouse: ClickhouseResource,
    ytj_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with ytj_duckdb.get_connection() as duckdb_connection:
        with clickhouse.get_connection() as clickhouse_client:
            row_counts = _copy_finland_ytj_resolved_tables_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=clickhouse_client,
            )
    return dg.MaterializeResult(
        metadata={f"{table}_row_count": count for table, count in row_counts.items()}
    )
```

- [ ] **Step 4: Add local direct copy helpers in the same file**

Add these helpers below `finland_ytj_resolved_clickhouse`:

```python
def _copy_finland_ytj_resolved_tables_to_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
) -> dict[str, int]:
    row_counts: dict[str, int] = {}
    for table_name in tables.FINLAND_YTJ_RESOLVED_TABLES:
        row_counts[table_name] = _copy_finland_ytj_resolved_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            table_name=table_name,
            columns=tables.RESOLVED_EXPORT_COLUMNS[table_name],
        )
    return row_counts


def _copy_finland_ytj_resolved_table_to_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    table_name: str,
    columns: tuple[str, ...],
) -> int:
    qualified_clickhouse_table = _quote_clickhouse_qualified_table(
        CLICKHOUSE_DATABASE,
        table_name,
    )
    quoted_columns = ", ".join(_quote_clickhouse_identifier(column) for column in columns)
    duckdb_columns = ", ".join(_quote_duckdb_identifier(column) for column in columns)
    duckdb_table = (
        f"{_quote_duckdb_identifier(RESOLVED_DUCKDB_SCHEMA)}."
        f"{_quote_duckdb_identifier(table_name)}"
    )

    clickhouse_client.execute(f"TRUNCATE TABLE {qualified_clickhouse_table}")
    result = duckdb_connection.execute(f"select {duckdb_columns} from {duckdb_table}")

    row_count = 0
    while True:
        rows = result.fetchmany(CLICKHOUSE_INSERT_BATCH_SIZE)
        if not rows:
            return row_count
        clickhouse_client.execute(
            f"INSERT INTO {qualified_clickhouse_table} ({quoted_columns}) VALUES",
            rows,
        )
        row_count += len(rows)


def _quote_duckdb_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _quote_clickhouse_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _quote_clickhouse_qualified_table(database: str, table_name: str) -> str:
    return (
        f"{_quote_clickhouse_identifier(database)}."
        f"{_quote_clickhouse_identifier(table_name)}"
    )
```

This intentionally relies on migrated ClickHouse tables. If migrations are missing, `TRUNCATE TABLE` or `INSERT INTO` fails with the real ClickHouse error.

- [ ] **Step 5: Run the focused test**

Run:

```bash
cd dagster_v3
uv run pytest tests/test_finland_ytj_resolved_assets.py::test_finland_ytj_resolved_clickhouse_copies_duckdb_tables_directly -q
```

Expected: PASS.

- [ ] **Step 6: Run Finland YTJ tests**

Run:

```bash
cd dagster_v3
uv run pytest tests/test_finland_ytj_resolved_assets.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit Finland YTJ direct export**

```bash
git add dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py dagster_v3/src/dagster_v3/defs/finland_ytj/resolved.py dagster_v3/tests/test_finland_ytj_resolved_assets.py
git commit -m "refactor: copy Finland YTJ resolved tables directly to ClickHouse"
```

---

## Task 4: Migrate Remaining Sources Off `clickhouse.resolved`

**Files:**
- Modify every file listed in "Importers That Must Be Migrated Before Deleting `clickhouse/resolved.py`".
- Modify associated tests that monkeypatch `assert_clickhouse_tables_exist`, `export_duckdb_connection_table_to_clickhouse`, or `replace_duckdb_connection_tables_in_clickhouse`.

- [ ] **Step 1: For each source file, remove this import pattern**

Remove imports like:

```python
from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
    replace_duckdb_connection_tables_in_clickhouse,
)
```

- [ ] **Step 2: Put the ClickHouse database constant inside the source module**

For each migrated source module, add:

```python
CLICKHOUSE_DATABASE = "corpscout"
CLICKHOUSE_INSERT_BATCH_SIZE = 50_000
```

If the source already has a database constant such as `BRAZIL_CNAE_DATABASE`, keep that source-specific constant and do not add another one.

- [ ] **Step 3: Replace table existence assertion**

Remove calls like:

```python
assert_clickhouse_tables_exist(
    clickhouse,
    database=RESOLVED_DATABASE,
    tables=(table_name,),
)
```

Do not replace them with another preflight helper. The direct `TRUNCATE TABLE` / `INSERT INTO` calls are the validation. Missing migrations should fail with the database error.

- [ ] **Step 4: Replace single-table exports**

Replace calls like:

```python
row_count = export_duckdb_connection_table_to_clickhouse(
    duckdb_connection=connection,
    clickhouse_client=client,
    duckdb_schema=schema,
    duckdb_table=duckdb_table,
    clickhouse_database=RESOLVED_DATABASE,
    clickhouse_table=clickhouse_table,
    columns=columns,
    truncate=True,
)
```

with a source-local function call:

```python
row_count = _copy_duckdb_table_to_clickhouse(
    duckdb_connection=connection,
    clickhouse_client=client,
    duckdb_schema=schema,
    duckdb_table=duckdb_table,
    clickhouse_database=CLICKHOUSE_DATABASE,
    clickhouse_table=clickhouse_table,
    columns=columns,
)
```

Define `_copy_duckdb_table_to_clickhouse` in that source module or its source-local `clickhouse.py`:

```python
def _copy_duckdb_table_to_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_schema: str,
    duckdb_table: str,
    clickhouse_database: str,
    clickhouse_table: str,
    columns: tuple[str, ...],
) -> int:
    qualified_clickhouse_table = _quote_clickhouse_qualified_table(
        clickhouse_database,
        clickhouse_table,
    )
    clickhouse_columns = ", ".join(_quote_clickhouse_identifier(column) for column in columns)
    duckdb_columns = ", ".join(_quote_duckdb_identifier(column) for column in columns)
    qualified_duckdb_table = (
        f"{_quote_duckdb_identifier(duckdb_schema)}."
        f"{_quote_duckdb_identifier(duckdb_table)}"
    )

    clickhouse_client.execute(f"TRUNCATE TABLE {qualified_clickhouse_table}")
    result = duckdb_connection.execute(f"select {duckdb_columns} from {qualified_duckdb_table}")

    row_count = 0
    while True:
        rows = result.fetchmany(CLICKHOUSE_INSERT_BATCH_SIZE)
        if not rows:
            return row_count
        clickhouse_client.execute(
            f"INSERT INTO {qualified_clickhouse_table} ({clickhouse_columns}) VALUES",
            rows,
        )
        row_count += len(rows)
```

- [ ] **Step 5: Replace multi-table exports**

Replace calls like:

```python
row_counts = replace_duckdb_connection_tables_in_clickhouse(
    duckdb_connection=connection,
    clickhouse_client=client,
    duckdb_schema=schema,
    clickhouse_database=RESOLVED_DATABASE,
    tables=((table_name, columns), ...),
)
```

with a direct loop:

```python
row_counts = {
    table_name: _copy_duckdb_table_to_clickhouse(
        duckdb_connection=connection,
        clickhouse_client=client,
        duckdb_schema=schema,
        duckdb_table=table_name,
        clickhouse_database=CLICKHOUSE_DATABASE,
        clickhouse_table=table_name,
        columns=tuple(columns),
    )
    for table_name, columns in source_tables
}
```

- [ ] **Step 6: Keep source-specific transformations local**

If a source currently uses `column_expressions`, keep that logic in the source module instead of recreating generic `column_expressions` support. The source-local query should be explicit:

```python
duckdb_connection.execute(
    f"""
    select
        column_a,
        cast(column_b as varchar) as column_b
    from "{duckdb_schema}"."{duckdb_table}"
    """
)
```

- [ ] **Step 7: Update tests source by source**

For each source test that monkeypatches the shared helper, change it to assert source-owned behavior:

```python
assert "TRUNCATE TABLE `corpscout`.`target_table`" in fake_client.statements
assert fake_client.insert_calls
```

Do not keep tests that only prove forwarding into a helper. Those tests become obsolete when the helper is removed.

- [ ] **Step 8: Run importer scan**

Run:

```bash
rg "defs\.clickhouse\.resolved|clickhouse.resolved|assert_clickhouse_tables_exist|replace_duckdb_connection_tables_in_clickhouse|export_duckdb_connection_table_to_clickhouse|RESOLVED_DATABASE" dagster_v3/src dagster_v3/tests -n
```

Expected: no matches.

- [ ] **Step 9: Commit source migrations**

```bash
git add dagster_v3/src dagster_v3/tests
git commit -m "refactor: remove shared resolved ClickHouse exporter usage"
```

---

## Task 5: Delete `clickhouse.resolved`

**Files:**
- Delete: `dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py`
- Delete: `dagster_v3/tests/test_clickhouse_resolved.py`

- [ ] **Step 1: Delete the shared module and tests**

```bash
rm dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py
rm dagster_v3/tests/test_clickhouse_resolved.py
```

- [ ] **Step 2: Keep the resource module**

Do not delete:

```text
dagster_v3/src/dagster_v3/defs/clickhouse/resources.py
```

That file is still a resource boundary and not the resolved export abstraction.

- [ ] **Step 3: Verify no imports remain**

Run:

```bash
rg "clickhouse\.resolved|defs\.clickhouse\.resolved" dagster_v3/src dagster_v3/tests -n
```

Expected: no matches.

- [ ] **Step 4: Commit deletion**

```bash
git add -u dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py dagster_v3/tests/test_clickhouse_resolved.py
git commit -m "refactor: delete resolved ClickHouse export module"
```

---

## Task 6: Verification

**Files:**
- No code edits.

- [ ] **Step 1: Run targeted Finland YTJ tests**

```bash
cd dagster_v3
uv run pytest tests/test_finland_ytj_resolved_assets.py -q
```

Expected: PASS.

- [ ] **Step 2: Run tests for changed source modules**

Run the relevant test files for every migrated importer. At minimum:

```bash
cd dagster_v3
uv run pytest \
  tests/test_finland_ytj_resolved_assets.py \
  tests/test_brazil_cnae.py \
  tests/test_brazil_rfb_assets.py \
  tests/test_wikidata_assets.py \
  tests/test_domains_assets.py \
  -q
```

Expected: PASS.

- [ ] **Step 3: Run full Dagster test suite**

```bash
cd dagster_v3
uv run pytest -q
```

Expected: PASS.

- [ ] **Step 4: Validate Dagster definitions**

```bash
cd dagster_v3
uv run dg check
```

Expected: success with valid definitions.

- [ ] **Step 5: Final import scan**

```bash
rg "clickhouse\.resolved|defs\.clickhouse\.resolved|assert_clickhouse_tables_exist|replace_duckdb_connection_tables_in_clickhouse|export_duckdb_connection_table_to_clickhouse|RESOLVED_DATABASE" dagster_v3/src dagster_v3/tests -n
```

Expected: no matches.

---

## Design Notes

- The direct copy is intentionally `TRUNCATE TABLE` followed by batched `INSERT INTO`.
- ClickHouse migrations remain the only owner of table definitions.
- The asset should fail naturally if migrations were not applied.
- We are accepting the simpler failure model: if an insert fails after truncate, rerun the asset after fixing the issue. Do not reintroduce staging table exchange logic unless a concrete production requirement demands atomic table swap.
- Do not create a new generic `copy.py` helper under `defs/clickhouse` as a replacement for `resolved.py`. That would preserve the abstraction under a new name.

## Self-Review

- Spec coverage: Finland YTJ asset-module cleanup is covered by Task 1. Finland YTJ direct copy is covered by Tasks 2-3. Full removal of `clickhouse.resolved` is covered by Tasks 4-5. Verification is covered by Task 6.
- Placeholder scan: no TBD/TODO/fill-in placeholders. The only source-by-source work is explicit by file list and repeated direct-copy pattern.
- Type consistency: helper signatures use `Any`, `tuple[str, ...]`, source-local constants, and existing Dagster resource types consistently.
