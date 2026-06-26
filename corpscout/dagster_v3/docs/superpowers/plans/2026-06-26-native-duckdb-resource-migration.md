# Native DuckDB Resource Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace custom and raw production DuckDB connection ownership with Dagster's native `dagster_duckdb.DuckDBResource`.

**Architecture:** Dagster assets/checks own DuckDB connections through source-specific `DuckDBResource` instances. Helper modules receive `duckdb.DuckDBPyConnection` objects instead of opening paths directly. dlt and dbt remain path-based only at their required third-party API boundaries.

**Tech Stack:** Dagster 1.13.9, `dagster-duckdb`, DuckDB 1.5.3, dlt DuckDB destinations, dbt-duckdb, pytest, `dg check`.

---

## File Map

- Create: `corpscout/dagster_v3/src/dagster_v3/defs/common/duckdb_resources.py`
  - Native `DuckDBResource` factory, shared runtime connection config, database path extraction.
- Modify: `corpscout/dagster_v3/pyproject.toml`
  - Add `dagster-duckdb>=0.29.9`.
- Modify: `corpscout/dagster_v3/uv.lock`
  - Updated by `uv add`.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/common/resources.py`
  - Remove `LocalDuckDBResource`; keep `ObjectStoreResource`.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py`
  - Replace `LocalDuckDBResource` with native resource.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/finland_xbrl/assets.py`
  - Replace `LocalDuckDBResource`, path defaults, and read helpers.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/assets.py`
  - Replace resource typing and dbt path setup.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
  - Add `brazil_rfb_duckdb` native resource and pass connections into helpers.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/staging.py`
  - Accept caller-owned connection.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/transforms.py`
  - Accept caller-owned connection and remove Brazil runtime wrapper.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/contacts.py`
  - Accept caller-owned connection.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/clickhouse.py`
  - Export from caller-owned DuckDB connection.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py`
  - Add connection-oriented ClickHouse export helpers and remove internal raw connections.
- Modify production files found by:
  - `rg -n "duckdb\.connect" corpscout/dagster_v3/src/dagster_v3/defs -S`
  - Convert remaining direct production connections in source batches.
- Delete: `corpscout/dagster_v3/src/dagster_v3/defs/common/duckdb_runtime.py`
  - Removed after Brazil/native runtime migration.
- Modify tests:
  - `corpscout/dagster_v3/tests/test_common_resources.py`
  - `corpscout/dagster_v3/tests/test_brazil_rfb_transforms.py`
  - `corpscout/dagster_v3/tests/test_brazil_rfb_staging.py`
  - `corpscout/dagster_v3/tests/test_brazil_rfb_clickhouse.py`
  - `corpscout/dagster_v3/tests/test_finland_ytj_assets.py`
  - `corpscout/dagster_v3/tests/test_finland_xbrl_assets.py`
  - `corpscout/dagster_v3/tests/test_finland_xbrl_parsed_assets.py`
  - `corpscout/dagster_v3/tests/test_finland_resolved_assets.py`
  - `corpscout/dagster_v3/tests/test_clickhouse_resolved.py`

---

### Task 1: Add Native DuckDB Resource Factory

**Files:**
- Modify: `corpscout/dagster_v3/pyproject.toml`
- Modify: `corpscout/dagster_v3/uv.lock`
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/common/duckdb_resources.py`
- Modify: `corpscout/dagster_v3/tests/test_common_resources.py`

- [ ] **Step 1: Add the dependency**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv add "dagster-duckdb>=0.29.9"
```

Expected:

- `pyproject.toml` contains `dagster-duckdb>=0.29.9`.
- `uv.lock` contains `dagster-duckdb`.

- [ ] **Step 2: Write failing tests for common native DuckDB helpers**

Replace `test_shared_resources_live_in_common` in `corpscout/dagster_v3/tests/test_common_resources.py` with:

```python
import importlib
from pathlib import Path

from dagster_duckdb import DuckDBResource


def test_shared_resources_live_in_common() -> None:
    module = importlib.import_module("dagster_v3.defs.common.resources")
    assert hasattr(module, "ObjectStoreResource")


def test_duckdb_resource_factory_uses_generic_runtime_env(
    monkeypatch,
    tmp_path: Path,
) -> None:
    helpers = importlib.import_module("dagster_v3.defs.common.duckdb_resources")
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "8GiB")
    monkeypatch.setenv("DUCKDB_THREADS", "2")
    monkeypatch.setenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "150GiB")
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(tmp_path / "duckdb-temp"))

    resource = helpers.duckdb_resource(tmp_path / "source.duckdb")

    assert isinstance(resource, DuckDBResource)
    assert helpers.duckdb_database_path(resource) == tmp_path / "source.duckdb"
    assert resource.connection_config["memory_limit"] == "8GiB"
    assert resource.connection_config["threads"] == "2"
    assert resource.connection_config["max_temp_directory_size"] == "150GiB"
    assert resource.connection_config["temp_directory"] == str(tmp_path / "duckdb-temp")
    assert resource.connection_config["preserve_insertion_order"] is False
    assert (tmp_path / "duckdb-temp").is_dir()
```

- [ ] **Step 3: Run the new tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_common_resources.py -v
```

Expected: FAIL because `dagster_v3.defs.common.duckdb_resources` does not exist.

- [ ] **Step 4: Create the helper module**

Create `corpscout/dagster_v3/src/dagster_v3/defs/common/duckdb_resources.py`:

```python
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dagster_duckdb import DuckDBResource

DEFAULT_DUCKDB_THREADS = "4"
DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE = "100GiB"
DEFAULT_DUCKDB_TEMP_DIRECTORY = Path("data/duckdb_tmp")


def duckdb_resource(
    database: str | Path,
    *,
    default_temp_directory: str | Path | None = None,
) -> DuckDBResource:
    database_path = Path(database)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection_config = duckdb_connection_config(
        default_temp_directory=(
            default_temp_directory
            if default_temp_directory is not None
            else database_path.parent / "duckdb_tmp"
        )
    )
    return DuckDBResource(
        database=str(database_path),
        connection_config=connection_config,
    )


def duckdb_database_path(resource: DuckDBResource) -> Path:
    return Path(str(resource.database))


def duckdb_connection_config(
    *,
    default_temp_directory: str | Path = DEFAULT_DUCKDB_TEMP_DIRECTORY,
) -> dict[str, Any]:
    temp_directory = Path(_env_value("DUCKDB_TEMP_DIRECTORY") or default_temp_directory)
    temp_directory.mkdir(parents=True, exist_ok=True)

    config: dict[str, Any] = {
        "temp_directory": str(temp_directory),
        "max_temp_directory_size": (
            _env_value("DUCKDB_MAX_TEMP_DIRECTORY_SIZE")
            or DEFAULT_DUCKDB_MAX_TEMP_DIRECTORY_SIZE
        ),
        "threads": _env_value("DUCKDB_THREADS") or DEFAULT_DUCKDB_THREADS,
        "preserve_insertion_order": _env_bool(
            "DUCKDB_PRESERVE_INSERTION_ORDER",
            default=False,
        ),
    }
    memory_limit = _env_value("DUCKDB_MEMORY_LIMIT")
    if memory_limit is not None:
        config["memory_limit"] = memory_limit
    return config


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    clean_value = value.strip()
    return clean_value if clean_value else None


def _env_bool(name: str, *, default: bool) -> bool:
    value = _env_value(name)
    if value is None:
        return default
    normalized = value.lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of true/false, yes/no, on/off, or 1/0")
```

- [ ] **Step 5: Verify the helper API against installed `dagster-duckdb`**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run python - <<'PY'
from pathlib import Path
from dagster_v3.defs.common.duckdb_resources import duckdb_resource, duckdb_database_path

resource = duckdb_resource(Path("data/native_duckdb_resource_smoke.duckdb"))
print(type(resource).__name__)
print(duckdb_database_path(resource))
with resource.get_connection() as connection:
    print(connection.execute("select 1").fetchone()[0])
PY
```

Expected output contains:

```text
DuckDBResource
data/native_duckdb_resource_smoke.duckdb
1
```

If `resource.database` is not the public field name, inspect it with `uv run python -c "from dagster_duckdb import DuckDBResource; print(DuckDBResource.model_fields)"`, update `duckdb_database_path`, and keep the tests passing.

- [ ] **Step 6: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_common_resources.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add corpscout/dagster_v3/pyproject.toml corpscout/dagster_v3/uv.lock corpscout/dagster_v3/src/dagster_v3/defs/common/duckdb_resources.py corpscout/dagster_v3/tests/test_common_resources.py
git commit -m "feat: add native duckdb resource helper"
```

---

### Task 2: Migrate Finland YTJ From `LocalDuckDBResource`

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_finland_ytj_assets.py`

- [ ] **Step 1: Write the failing test update**

In `tests/test_finland_ytj_assets.py`, update `test_non_empty_check_reports_count` to create the native resource:

```python
from dagster_v3.defs.common.duckdb_resources import duckdb_resource


def test_non_empty_check_reports_count(tmp_path: Path) -> None:
    database_path = tmp_path / "finland_ytj.duckdb"
    ytj_assets.run_finland_ytj_dlt_pipeline(
        database_path=database_path,
        run_id="run-1",
        session=_session({"companies": [{"businessId": {"value": "a"}}]}),
    )
    resource = duckdb_resource(database_path)
    result = ytj_assets.all_companies_non_empty(resource)
    assert result.passed is True
    assert result.metadata["row_count"].value == 1
```

- [ ] **Step 2: Run the focused test and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_ytj_assets.py::test_non_empty_check_reports_count -v
```

Expected: FAIL because `all_companies_non_empty` still expects the custom resource API.

- [ ] **Step 3: Update Finland YTJ assets**

In `src/dagster_v3/defs/finland_ytj/assets.py`:

Replace:

```python
from dagster_v3.defs.common.resources import LocalDuckDBResource
```

with:

```python
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
)
```

Change the asset signature and path access:

```python
def finland_ytj_all_companies_duckdb_asset(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    ytj_duckdb: DuckDBResource,
) -> Iterator[Any]:
    """Load Finland PRH YTJ all-companies data to a local DuckDB database with dlt."""
    context.log.info("Materializing Finland YTJ dlt DuckDB table")
    yield from dlt.run(
        context=context,
        dlt_source=finland_ytj_source(run_id=context.run_id),
        dlt_pipeline=finland_ytj_pipeline(duckdb_database_path(ytj_duckdb)),
    )
```

Change the asset check:

```python
@dg.asset_check(asset="finland_ytj_all_companies_duckdb", name="all_companies_non_empty")
def all_companies_non_empty(ytj_duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with ytj_duckdb.get_connection() as connection:
        row_count = connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{DLT_COMPANIES_TABLE}"
        ).fetchone()[0]
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": int(row_count)},
    )
```

Change resource definitions:

```python
resources={
    "ytj_duckdb": duckdb_resource(DEFAULT_DUCKDB_PATH),
},
```

- [ ] **Step 4: Run Finland YTJ tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_ytj_assets.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/finland_ytj/assets.py corpscout/dagster_v3/tests/test_finland_ytj_assets.py
git commit -m "refactor: migrate finland ytj to native duckdb resource"
```

---

### Task 3: Migrate Finland XBRL And Resolved Assets

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/finland_xbrl/assets.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_finland_xbrl_assets.py`
- Modify: `corpscout/dagster_v3/tests/test_finland_xbrl_parsed_assets.py`
- Modify: `corpscout/dagster_v3/tests/test_finland_resolved_assets.py`

- [ ] **Step 1: Update tests that instantiate `LocalDuckDBResource`**

Replace all test imports of:

```python
from dagster_v3.defs.common.resources import LocalDuckDBResource, ObjectStoreResource
```

with:

```python
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
```

Replace test construction:

```python
LocalDuckDBResource(database_path=str(database_path))
```

with:

```python
duckdb_resource(database_path)
```

Replace direct path assertions that call `res.path()` with:

```python
from dagster_v3.defs.common.duckdb_resources import duckdb_database_path

duckdb_database_path(res)
```

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_parsed_assets.py tests/test_finland_resolved_assets.py -v
```

Expected: FAIL because production code still imports `LocalDuckDBResource`.

- [ ] **Step 3: Update Finland XBRL production imports and constants**

In `src/dagster_v3/defs/finland_xbrl/assets.py`, replace:

```python
from dagster_v3.defs.common.resources import LocalDuckDBResource, ObjectStoreResource
```

with:

```python
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
```

Replace:

```python
_XBRL_DUCKDB_PATH = Path(LocalDuckDBResource().database_path).expanduser()
```

with:

```python
_XBRL_DUCKDB_PATH = Path("data/finland_ytj.duckdb").expanduser()
```

Replace the dlt asset decorator path:

```python
dlt_pipeline=finland_xbrl_financial_reports_pipeline(_XBRL_DUCKDB_PATH),
```

Replace all `source_duckdb: LocalDuckDBResource` annotations with:

```python
source_duckdb: DuckDBResource
```

Replace `source_duckdb.path()` with:

```python
duckdb_database_path(source_duckdb)
```

Replace `source_duckdb.connect(read_only=True)` with:

```python
source_duckdb.get_connection()
```

Replace the resource definition:

```python
"source_duckdb": duckdb_resource(_XBRL_DUCKDB_PATH),
```

Replace `_ensure_parsed_duckdb_tables` with connection ownership at the call site. Add a new helper:

```python
def _ensure_parsed_duckdb_tables(connection: Any) -> None:
    connection.execute(f"create schema if not exists {XBRL_DLT_DATASET_NAME}")
    for table in (tables.STATEMENT_DOCUMENTS_TABLE, tables.FACTS_TABLE):
        column_definitions = ", ".join(
            f"{column} {_duckdb_column_type(column)}"
            for column in tables.TABLE_COLUMNS[table]
        )
        connection.execute(
            f"create table if not exists {XBRL_DLT_DATASET_NAME}.{table} ({column_definitions})"
        )
```

Where the old helper was called with a database path, use:

```python
with source_duckdb.get_connection() as connection:
    _ensure_parsed_duckdb_tables(connection)
```

- [ ] **Step 4: Update Finland resolved production code**

In `src/dagster_v3/defs/finland_resolved/assets.py`, replace:

```python
from dagster_v3.defs.common.resources import LocalDuckDBResource
```

with:

```python
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
)
```

Replace:

```python
_DEFAULT_DUCKDB_PATH = Path(LocalDuckDBResource().database_path).expanduser()
```

with:

```python
_DEFAULT_DUCKDB_PATH = Path("data/finland_ytj.duckdb").expanduser()
```

Change `finland_ytj_resolved_clickhouse` signature:

```python
def finland_ytj_resolved_clickhouse(
    clickhouse: ClickhouseResource,
    ytj_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
```

Replace `duckdb_path=ytj_duckdb.path()` with:

```python
duckdb_path=duckdb_database_path(ytj_duckdb)
```

Add the resource to definitions:

```python
"ytj_duckdb": duckdb_resource(_DEFAULT_DUCKDB_PATH),
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_parsed_assets.py tests/test_finland_resolved_assets.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/finland_xbrl/assets.py corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/assets.py corpscout/dagster_v3/tests/test_finland_xbrl_assets.py corpscout/dagster_v3/tests/test_finland_xbrl_parsed_assets.py corpscout/dagster_v3/tests/test_finland_resolved_assets.py
git commit -m "refactor: migrate finland duckdb assets to native resource"
```

---

### Task 4: Migrate Brazil RFB Connection Ownership

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/staging.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/transforms.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/contacts.py`
- Modify: `corpscout/dagster_v3/tests/test_brazil_rfb_staging.py`
- Modify: `corpscout/dagster_v3/tests/test_brazil_rfb_transforms.py`

- [ ] **Step 1: Update Brazil tests to pass connections into helpers**

In Brazil transform/staging/contact tests, replace production helper calls like:

```python
transforms.build_brazil_rfb_companies_and_establishments(
    database_path=database_path,
    source_run_id="run-1",
)
```

with:

```python
with duckdb.connect(str(database_path)) as connection:
    transforms.build_brazil_rfb_companies_and_establishments(
        connection=connection,
        source_run_id="run-1",
    )
```

Replace staging calls like:

```python
staging.load_all_raw_families_from_manifest(
    database_path=database_path,
    source_run_id="run-1",
)
```

with:

```python
with duckdb.connect(str(database_path)) as connection:
    staging.load_all_raw_families_from_manifest(
        connection=connection,
        source_run_id="run-1",
    )
```

Delete `test_duckdb_runtime_settings_are_applied_before_heavy_transforms`; runtime settings move to `test_common_resources.py`.

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_staging.py tests/test_brazil_rfb_transforms.py -v
```

Expected: FAIL because production helpers still accept `database_path`.

- [ ] **Step 3: Refactor `brazil_rfb/staging.py`**

Change:

```python
def load_raw_family_from_manifest(
    *,
    database_path: str | Path,
    family: str,
    source_run_id: str,
) -> int:
```

to:

```python
def load_raw_family_from_manifest(
    *,
    connection: duckdb.DuckDBPyConnection,
    family: str,
    source_run_id: str,
) -> int:
```

Remove the internal `with duckdb.connect` block and de-indent its body to use the passed `connection`.

Change:

```python
def load_all_raw_families_from_manifest(
    *,
    database_path: str | Path,
    source_run_id: str,
) -> dict[str, int]:
```

to:

```python
def load_all_raw_families_from_manifest(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
) -> dict[str, int]:
```

Call:

```python
load_raw_family_from_manifest(
    connection=connection,
    family=family,
    source_run_id=source_run_id,
)
```

- [ ] **Step 4: Refactor `brazil_rfb/transforms.py`**

Remove the Brazil-specific runtime helper block:

```python
from pathlib import Path
from dagster_v3.defs.common.duckdb_runtime import apply_duckdb_runtime_settings
DEFAULT_DUCKDB_TEMP_DIRECTORY = Path("data/brazil_rfb_duckdb_tmp")
def apply_brazil_rfb_duckdb_runtime_settings(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    apply_duckdb_runtime_settings(
        connection,
        default_temp_directory=DEFAULT_DUCKDB_TEMP_DIRECTORY,
    )
```

Change:

```python
def build_brazil_rfb_companies_and_establishments(
    *,
    database_path: str | Path,
    source_run_id: str,
) -> dict[str, int]:
```

to:

```python
def build_brazil_rfb_companies_and_establishments(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
) -> dict[str, int]:
```

Remove the internal `with duckdb.connect(str(database_path)) as connection:` and the runtime-setting call. De-indent the SQL body so it uses the caller-owned connection.

- [ ] **Step 5: Refactor `brazil_rfb/contacts.py`**

Change:

```python
def build_brazil_rfb_contact_info(
    *,
    database_path: str | Path,
    source_run_id: str,
    log: Callable | None = None,
) -> dict[str, int]:
```

to:

```python
def build_brazil_rfb_contact_info(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    log: Callable | None = None,
) -> dict[str, int]:
```

Change:

```python
def build_brazil_rfb_websites(
    *,
    database_path: str | Path,
    log: Callable | None = None,
) -> dict[str, int]:
```

to:

```python
def build_brazil_rfb_websites(
    *,
    connection: duckdb.DuckDBPyConnection,
    log: Callable | None = None,
) -> dict[str, int]:
```

Remove internal `duckdb.connect` blocks and use the passed connection. Keep `register_domain_udfs(connection)` inside `build_brazil_rfb_contact_info`.

Change `build_brazil_rfb_contact_info_and_websites` to accept a connection and pass it to both functions.

- [ ] **Step 6: Update Brazil assets to use native resource**

In `brazil_rfb/assets.py`, add imports:

```python
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
)
```

Add `brazil_rfb_duckdb: DuckDBResource` to non-dlt DuckDB asset signatures.

For `brazil_rfb_raw_files_duckdb`:

```python
def brazil_rfb_raw_files_duckdb(
    context: dg.AssetExecutionContext,
    brazil_rfb_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with brazil_rfb_duckdb.get_connection() as connection:
        counts = staging.load_all_raw_families_from_manifest(
            connection=connection,
            source_run_id=context.run_id,
        )
```

For `brazil_rfb_companies_duckdb`:

```python
def brazil_rfb_companies_duckdb(
    context: dg.AssetExecutionContext,
    brazil_rfb_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with brazil_rfb_duckdb.get_connection() as connection:
        counts = transforms.build_brazil_rfb_companies_and_establishments(
            connection=connection,
            source_run_id=context.run_id,
        )
```

For contact and website assets, use the same pattern with `contacts.build_brazil_rfb_contact_info` and `contacts.build_brazil_rfb_websites`.

In definitions, add:

```python
resources={
    "brazil_rfb_duckdb": duckdb_resource(BRAZIL_RFB_DUCKDB_PATH),
},
```

Keep the dlt pipeline path-based:

```python
dlt_pipeline=source.brazil_rfb_pipeline(BRAZIL_RFB_DUCKDB_PATH)
```

- [ ] **Step 7: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_rfb_staging.py tests/test_brazil_rfb_transforms.py tests/test_brazil_rfb_assets.py -v
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/staging.py corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/transforms.py corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/contacts.py corpscout/dagster_v3/tests/test_brazil_rfb_staging.py corpscout/dagster_v3/tests/test_brazil_rfb_transforms.py corpscout/dagster_v3/tests/test_brazil_rfb_assets.py
git commit -m "refactor: migrate brazil rfb to native duckdb resource"
```

---

### Task 5: Convert ClickHouse DuckDB Export Helpers To Connection-Based APIs

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/clickhouse.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_resolved.py`
- Modify: `corpscout/dagster_v3/tests/test_brazil_rfb_clickhouse.py`

- [ ] **Step 1: Add failing connection-oriented export tests**

In `tests/test_clickhouse_resolved.py`, add or update tests so they call the new API:

```python
with duckdb.connect(str(database_path)) as connection:
    rows = export_duckdb_connection_table_to_clickhouse(
        duckdb_connection=connection,
        clickhouse_client=client,
        duckdb_schema="src",
        duckdb_table="companies",
        clickhouse_database="corpscout",
        clickhouse_table="fi_companies",
        columns=("business_id", "name"),
        truncate=True,
    )
```

Expected helper name:

```python
export_duckdb_connection_table_to_clickhouse
replace_duckdb_connection_tables_in_clickhouse
```

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_resolved.py tests/test_brazil_rfb_clickhouse.py -v
```

Expected: FAIL because connection-oriented helpers do not exist.

- [ ] **Step 3: Refactor `clickhouse/resolved.py`**

Add:

```python
def export_duckdb_connection_table_to_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_schema: str,
    duckdb_table: str,
    clickhouse_database: str,
    clickhouse_table: str,
    columns: Sequence[str],
    truncate: bool,
    batch_size: int = DEFAULT_CLICKHOUSE_INSERT_BATCH_SIZE,
) -> int:
    _validate_batch_size(batch_size)
    clickhouse_columns = ", ".join(_quote_clickhouse_identifier(column) for column in columns)
    clickhouse_qualified_table = _quote_clickhouse_qualified_table(
        clickhouse_database,
        clickhouse_table,
    )
    if not truncate:
        return _insert_duckdb_rows_in_batches(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=duckdb_schema,
            duckdb_table=duckdb_table,
            clickhouse_qualified_table=clickhouse_qualified_table,
            clickhouse_columns=clickhouse_columns,
            columns=columns,
            batch_size=batch_size,
        )
    clickhouse_stage_table = _clickhouse_stage_table_name(clickhouse_table)
    clickhouse_qualified_stage_table = _quote_clickhouse_qualified_table(
        clickhouse_database,
        clickhouse_stage_table,
    )
    clickhouse_client.execute(
        f"CREATE TABLE {clickhouse_qualified_stage_table} AS {clickhouse_qualified_table}"
    )
    primary_error: Exception | None = None
    row_count = 0
    try:
        row_count = _insert_duckdb_rows_in_batches(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=duckdb_schema,
            duckdb_table=duckdb_table,
            clickhouse_qualified_table=clickhouse_qualified_stage_table,
            clickhouse_columns=clickhouse_columns,
            columns=columns,
            batch_size=batch_size,
        )
        clickhouse_client.execute(
            f"EXCHANGE TABLES {clickhouse_qualified_stage_table} AND {clickhouse_qualified_table}"
        )
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        _drop_clickhouse_stage_tables(
            clickhouse_client,
            (clickhouse_qualified_stage_table,),
            suppress_errors=primary_error is not None,
        )
    return row_count
```

Add a connection-based multi-table helper that keeps the current staging and rollback behavior while using the passed DuckDB connection:

```python
def replace_duckdb_connection_tables_in_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_schema: str,
    clickhouse_database: str,
    tables: Sequence[tuple[str, Sequence[str]]],
    batch_size: int = DEFAULT_CLICKHOUSE_INSERT_BATCH_SIZE,
) -> dict[str, int]:
    _validate_batch_size(batch_size)
    requested_tables = tuple(
        (clickhouse_table, tuple(columns)) for clickhouse_table, columns in tables
    )
    clickhouse_columns_by_table = {
        clickhouse_table: ", ".join(_quote_clickhouse_identifier(column) for column in columns)
        for clickhouse_table, columns in requested_tables
    }
    clickhouse_qualified_tables = {
        clickhouse_table: _quote_clickhouse_qualified_table(clickhouse_database, clickhouse_table)
        for clickhouse_table, _ in requested_tables
    }
    clickhouse_qualified_stage_tables: dict[str, str] = {}
    created_stage_tables: list[str] = []
    exchanged_tables: list[str] = []
    row_counts: dict[str, int] = {}
    primary_error: Exception | None = None

    try:
        for clickhouse_table, _ in requested_tables:
            clickhouse_stage_table = _clickhouse_stage_table_name(clickhouse_table)
            clickhouse_qualified_stage_table = _quote_clickhouse_qualified_table(
                clickhouse_database,
                clickhouse_stage_table,
            )
            clickhouse_qualified_stage_tables[clickhouse_table] = clickhouse_qualified_stage_table
            created_stage_tables.append(clickhouse_table)
            clickhouse_client.execute(
                f"CREATE TABLE {clickhouse_qualified_stage_table} AS "
                f"{clickhouse_qualified_tables[clickhouse_table]}"
            )

        for clickhouse_table, columns in requested_tables:
            row_counts[clickhouse_table] = _insert_duckdb_rows_in_batches(
                duckdb_connection=duckdb_connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=duckdb_schema,
                duckdb_table=clickhouse_table,
                clickhouse_qualified_table=clickhouse_qualified_stage_tables[clickhouse_table],
                clickhouse_columns=clickhouse_columns_by_table[clickhouse_table],
                columns=columns,
                batch_size=batch_size,
            )

        for clickhouse_table, _ in requested_tables:
            clickhouse_client.execute(
                f"EXCHANGE TABLES {clickhouse_qualified_stage_tables[clickhouse_table]} "
                f"AND {clickhouse_qualified_tables[clickhouse_table]}"
            )
            exchanged_tables.append(clickhouse_table)
    except Exception as exc:
        primary_error = exc
        rollback_failures: list[str] = []
        for clickhouse_table in reversed(exchanged_tables):
            try:
                clickhouse_client.execute(
                    f"EXCHANGE TABLES {clickhouse_qualified_stage_tables[clickhouse_table]} "
                    f"AND {clickhouse_qualified_tables[clickhouse_table]}"
                )
            except Exception:
                rollback_failures.append(
                    f"{clickhouse_table} "
                    f"({clickhouse_qualified_stage_tables[clickhouse_table]} <-> "
                    f"{clickhouse_qualified_tables[clickhouse_table]})"
                )
        if rollback_failures:
            raise RuntimeError(
                "Rollback failed after ClickHouse publish error; ClickHouse may be inconsistent. "
                "Failed rollback exchange(s): "
                + ", ".join(rollback_failures)
            ) from exc
        raise
    finally:
        _drop_clickhouse_stage_tables(
            clickhouse_client,
            tuple(
                clickhouse_qualified_stage_tables[clickhouse_table]
                for clickhouse_table in reversed(created_stage_tables)
            ),
            suppress_errors=primary_error is not None,
        )

    return row_counts
```

Keep the old path-based wrappers temporarily, but implement them by opening a connection and delegating to the new helpers. They will be removed after all production callers migrate.

- [ ] **Step 4: Update Brazil ClickHouse exports**

In `brazil_rfb/clickhouse.py`, change every export function from `database_path` to `duckdb_connection`:

```python
def export_brazil_rfb_clickhouse_companies(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable | None = None,
) -> int:
```

Call:

```python
rows = export_duckdb_connection_table_to_clickhouse(
    duckdb_connection=duckdb_connection,
    clickhouse_client=client,
    duckdb_schema=DLT_DATASET_NAME,
    duckdb_table=tables.COMPANIES_TABLE,
    clickhouse_database=tables.BRAZIL_RFB_DATABASE,
    clickhouse_table=tables.BR_COMPANIES_TABLE_CH,
    columns=tables.BR_COMPANIES_EXPORT_COLUMNS,
    truncate=True,
)
```

Apply the same pattern to establishments, contact info, and websites.

- [ ] **Step 5: Update Brazil and Finland resolved assets**

In Brazil ClickHouse assets, add `brazil_rfb_duckdb: DuckDBResource` and call:

```python
with brazil_rfb_duckdb.get_connection() as connection:
    rows = export_brazil_rfb_clickhouse_companies(
        duckdb_connection=connection,
        clickhouse=clickhouse,
        log=context.log.info,
    )
```

In `finland_resolved/assets.py`, replace:

```python
row_counts = replace_duckdb_tables_in_clickhouse(
    duckdb_path=duckdb_database_path(ytj_duckdb),
    clickhouse_client=client,
    duckdb_schema=RESOLVED_DUCKDB_SCHEMA,
    clickhouse_database=RESOLVED_DATABASE,
    tables=tuple(
        (table, tables.RESOLVED_EXPORT_COLUMNS[table])
        for table in tables.FINLAND_YTJ_RESOLVED_TABLES
    ),
)
```

with:

```python
with ytj_duckdb.get_connection() as connection:
    row_counts = replace_duckdb_connection_tables_in_clickhouse(
        duckdb_connection=connection,
        clickhouse_client=client,
        duckdb_schema=RESOLVED_DUCKDB_SCHEMA,
        clickhouse_database=RESOLVED_DATABASE,
        tables=tuple(
            (table, tables.RESOLVED_EXPORT_COLUMNS[table])
            for table in tables.FINLAND_YTJ_RESOLVED_TABLES
        ),
    )
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_resolved.py tests/test_brazil_rfb_clickhouse.py tests/test_finland_resolved_assets.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/clickhouse/resolved.py corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/clickhouse.py corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py corpscout/dagster_v3/src/dagster_v3/defs/finland_resolved/assets.py corpscout/dagster_v3/tests/test_clickhouse_resolved.py corpscout/dagster_v3/tests/test_brazil_rfb_clickhouse.py corpscout/dagster_v3/tests/test_finland_resolved_assets.py
git commit -m "refactor: export clickhouse tables from duckdb resources"
```

---

### Task 6: Remove Remaining Production `duckdb.connect` Calls

**Files:**
- Modify every production file reported by:
  - `rg -n "duckdb\.connect" corpscout/dagster_v3/src/dagster_v3/defs -S`

- [ ] **Step 1: Capture the production direct connection inventory**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "duckdb\.connect" corpscout/dagster_v3/src/dagster_v3/defs -S
```

Expected before this task: direct connection lines remain in source-specific modules such as Czech ARES, Estonia AR, Exchange Rates, France Sirene, GLEIF, Latvia UR, NACE, Norway BRREG, Open Page Rank, Slovakia, UK Companies House, and Wikidata.

- [ ] **Step 2: Apply the source-batch migration pattern**

For each remaining production direct connection:

1. Move `duckdb.connect` to the Dagster asset/check boundary.
2. Add a source-specific `DuckDBResource` to the source `defs`.
3. Pass `duckdb.DuckDBPyConnection` into helper functions.
4. Keep dlt/dbt `database_path` arguments only for APIs that require file paths.

Use this transformation for helper functions:

```python
# before
def build_source_table(*, database_path: str | Path, run_id: str) -> dict[str, int]:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema if not exists source_dataset")
        connection.execute("create or replace table source_dataset.normalized as select ? as run_id", [run_id])
        return {
            "rows": int(
                connection.execute("select count(*) from source_dataset.normalized").fetchone()[0]
            )
        }

# after
def build_source_table(
    *,
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
) -> dict[str, int]:
    connection.execute("create schema if not exists source_dataset")
    connection.execute("create or replace table source_dataset.normalized as select ? as run_id", [run_id])
    return {
        "rows": int(
            connection.execute("select count(*) from source_dataset.normalized").fetchone()[0]
        )
    }
```

Use this transformation for assets:

```python
from dagster_duckdb import DuckDBResource
from dagster_v3.defs.common.duckdb_resources import duckdb_resource


def source_asset(
    context: dg.AssetExecutionContext,
    source_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with source_duckdb.get_connection() as connection:
        counts = build_source_table(
            connection=connection,
            run_id=context.run_id,
        )
    return dg.MaterializeResult(metadata=counts)


defs = dg.Definitions(
    assets=[source_asset],
    resources={
        "source_duckdb": duckdb_resource(SOURCE_DUCKDB_PATH),
    },
)
```

Use this transformation for read-only checks/helpers:

```python
# before
with duckdb.connect(str(database_path), read_only=True) as connection:
    rows = connection.execute("select count(*) from source_dataset.normalized").fetchall()

# after
with source_duckdb.get_connection() as connection:
    rows = connection.execute("select count(*) from source_dataset.normalized").fetchall()
```

- [ ] **Step 3: Convert these source batches in separate commits**

Batch 1:

```text
corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/industries.py
corpscout/dagster_v3/src/dagster_v3/defs/czech_ares/resources.py
corpscout/dagster_v3/src/dagster_v3/defs/france_sirene/assets.py
corpscout/dagster_v3/src/dagster_v3/defs/france_sirene/industries.py
corpscout/dagster_v3/src/dagster_v3/defs/france_sirene/resources.py
corpscout/dagster_v3/src/dagster_v3/defs/slovakia_rpo/industries.py
corpscout/dagster_v3/src/dagster_v3/defs/slovakia_rpo/resources.py
corpscout/dagster_v3/src/dagster_v3/defs/uk_companies_house/financials.py
corpscout/dagster_v3/src/dagster_v3/defs/uk_companies_house/incremental.py
corpscout/dagster_v3/src/dagster_v3/defs/uk_companies_house/industries.py
corpscout/dagster_v3/src/dagster_v3/defs/uk_companies_house/resources.py
```

Run after Batch 1:

```bash
uv run pytest tests/test_czech_ares.py tests/test_france_sirene.py tests/test_slovakia_rpo.py tests/test_uk_companies_house.py -v
git add corpscout/dagster_v3/src/dagster_v3/defs/czech_ares corpscout/dagster_v3/src/dagster_v3/defs/france_sirene corpscout/dagster_v3/src/dagster_v3/defs/slovakia_rpo corpscout/dagster_v3/src/dagster_v3/defs/uk_companies_house corpscout/dagster_v3/tests/test_czech_ares.py corpscout/dagster_v3/tests/test_france_sirene.py corpscout/dagster_v3/tests/test_slovakia_rpo.py corpscout/dagster_v3/tests/test_uk_companies_house.py
git commit -m "refactor: migrate european registry duckdb helpers"
```

Batch 2:

```text
corpscout/dagster_v3/src/dagster_v3/defs/estonia_ar/assets.py
corpscout/dagster_v3/src/dagster_v3/defs/estonia_ar/company_domains.py
corpscout/dagster_v3/src/dagster_v3/defs/estonia_ar/contacts.py
corpscout/dagster_v3/src/dagster_v3/defs/estonia_ar/financials.py
corpscout/dagster_v3/src/dagster_v3/defs/estonia_ar/industries.py
corpscout/dagster_v3/src/dagster_v3/defs/estonia_ar/metrics.py
corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/assets.py
corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/financials.py
corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur/metrics.py
```

Run after Batch 2:

```bash
uv run pytest tests/test_estonia_ar_assets.py tests/test_estonia_ar_contacts.py tests/test_estonia_ar_financials.py tests/test_estonia_ar_industries.py tests/test_estonia_ar_metrics.py tests/test_latvia_ur_assets.py tests/test_latvia_ur_financials.py tests/test_latvia_ur_metrics.py -v
git add corpscout/dagster_v3/src/dagster_v3/defs/estonia_ar corpscout/dagster_v3/src/dagster_v3/defs/latvia_ur corpscout/dagster_v3/tests/test_estonia_ar_*.py corpscout/dagster_v3/tests/test_latvia_ur_*.py
git commit -m "refactor: migrate baltic duckdb helpers"
```

Batch 3:

```text
corpscout/dagster_v3/src/dagster_v3/defs/gleif/csv_transforms.py
corpscout/dagster_v3/src/dagster_v3/defs/gleif/dlt_csv.py
corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py
corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank/dlt_csv.py
corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank/transforms.py
corpscout/dagster_v3/src/dagster_v3/defs/wikidata/assets.py
```

Run after Batch 3:

```bash
uv run pytest tests/test_gleif_csv_transforms.py tests/test_gleif_dlt_csv.py tests/test_gleif_duckdb_state.py tests/test_open_page_rank_dlt_csv.py tests/test_open_page_rank_transforms.py tests/test_wikidata_assets.py -v
git add corpscout/dagster_v3/src/dagster_v3/defs/gleif corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank corpscout/dagster_v3/src/dagster_v3/defs/wikidata corpscout/dagster_v3/tests/test_gleif_*.py corpscout/dagster_v3/tests/test_open_page_rank_*.py corpscout/dagster_v3/tests/test_wikidata_assets.py
git commit -m "refactor: migrate enrichment duckdb helpers"
```

Batch 4:

```text
corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/assets.py
corpscout/dagster_v3/src/dagster_v3/defs/finland_financials/incremental.py
corpscout/dagster_v3/src/dagster_v3/defs/finland_financials/metrics.py
corpscout/dagster_v3/src/dagster_v3/defs/nace/assets.py
corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py
corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_fetches.py
corpscout/dagster_v3/src/dagster_v3/defs/slovakia_financials/incremental.py
corpscout/dagster_v3/src/dagster_v3/defs/slovakia_financials/metrics.py
```

Run after Batch 4:

```bash
uv run pytest tests/test_exchange_rates_v2_dbt.py tests/test_finland_financials.py tests/test_nace_categories.py tests/test_norway_brreg_assets.py tests/test_norway_brreg_financial_fetches.py tests/test_slovakia_financials.py -v
git add corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2 corpscout/dagster_v3/src/dagster_v3/defs/finland_financials corpscout/dagster_v3/src/dagster_v3/defs/nace corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg corpscout/dagster_v3/src/dagster_v3/defs/slovakia_financials corpscout/dagster_v3/tests/test_exchange_rates_v2_dbt.py corpscout/dagster_v3/tests/test_finland_financials.py corpscout/dagster_v3/tests/test_nace_categories.py corpscout/dagster_v3/tests/test_norway_brreg_*.py corpscout/dagster_v3/tests/test_slovakia_financials.py
git commit -m "refactor: migrate remaining production duckdb helpers"
```

- [ ] **Step 4: Verify no production direct connections remain**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "duckdb\.connect" corpscout/dagster_v3/src/dagster_v3/defs -S
```

Expected: no output.

---

### Task 7: Remove Custom DuckDB Resource And Obsolete Runtime Helper

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/common/resources.py`
- Delete: `corpscout/dagster_v3/src/dagster_v3/defs/common/duckdb_runtime.py`
- Modify: `corpscout/dagster_v3/tests/test_common_resources.py`

- [ ] **Step 1: Remove `LocalDuckDBResource` from common resources**

In `common/resources.py`, remove:

```python
from collections.abc import Iterator
from contextlib import contextmanager
import duckdb
```

Remove the full `LocalDuckDBResource` class.

Keep `ObjectStoreResource` and `_error_code`.

- [ ] **Step 2: Delete obsolete runtime helper**

Delete:

```text
corpscout/dagster_v3/src/dagster_v3/defs/common/duckdb_runtime.py
```

- [ ] **Step 3: Run cleanup audits**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "LocalDuckDBResource|duckdb_runtime|apply_duckdb_runtime_settings|apply_brazil_rfb_duckdb_runtime_settings" corpscout/dagster_v3/src corpscout/dagster_v3/tests -S
```

Expected: no output.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "duckdb\.connect" corpscout/dagster_v3/src/dagster_v3/defs -S
```

Expected: no output.

- [ ] **Step 4: Run common tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_common_resources.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/common/resources.py corpscout/dagster_v3/src/dagster_v3/defs/common/duckdb_resources.py corpscout/dagster_v3/tests/test_common_resources.py
git rm corpscout/dagster_v3/src/dagster_v3/defs/common/duckdb_runtime.py
git commit -m "refactor: remove custom duckdb resource"
```

---

### Task 8: Final Dagster And Test Verification

**Files:**
- Modify docs only if verification exposes config/documentation drift.

- [ ] **Step 1: Run static audits**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "LocalDuckDBResource|duckdb_runtime|apply_duckdb_runtime_settings|apply_brazil_rfb_duckdb_runtime_settings" corpscout/dagster_v3/src corpscout/dagster_v3/tests -S
rg -n "duckdb\.connect" corpscout/dagster_v3/src/dagster_v3/defs -S
```

Expected: both commands produce no output.

- [ ] **Step 2: Run Dagster definition validation**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check
```

Expected: success with no definition load errors.

- [ ] **Step 3: Run focused migration test suites**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest \
  tests/test_common_resources.py \
  tests/test_brazil_rfb_assets.py \
  tests/test_brazil_rfb_staging.py \
  tests/test_brazil_rfb_transforms.py \
  tests/test_brazil_rfb_clickhouse.py \
  tests/test_clickhouse_resolved.py \
  tests/test_finland_ytj_assets.py \
  tests/test_finland_xbrl_assets.py \
  tests/test_finland_xbrl_parsed_assets.py \
  tests/test_finland_resolved_assets.py \
  -v
```

Expected: PASS.

- [ ] **Step 4: Run the full Dagster v3 test suite**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests -v
```

Expected: PASS. If unrelated tests fail, capture exact failing test names and error summaries in the final handoff.

- [ ] **Step 5: Commit final verification/doc updates**

If verification required additional code or docs changes:

```bash
git add corpscout/dagster_v3
git commit -m "test: verify native duckdb resource migration"
```

If no files changed, skip this commit.

---

## Self-Review Checklist

- The plan adds `dagster-duckdb` before using `DuckDBResource`.
- The plan removes `LocalDuckDBResource` only after all consumers migrate.
- Brazil runtime settings move from source-specific post-connect SQL to native resource `connection_config`.
- dlt/dbt path-based APIs remain path-based.
- Production direct `duckdb.connect` calls are removed with an explicit `rg` audit.
- Tests are updated before implementation in each task.
- Commits are small enough to isolate failures.
