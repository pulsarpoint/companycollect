# Finland YTJ SCD2 + Incremental Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the full-`replace` Finland YTJ DuckDB load into an SCD2 (history-tracked) load whose change detection is driven by `source_payload_hash`, then normalize *only the companies that changed since the last run* into a child table.

**Architecture:** Two layers. **Layer 1** (`finland_ytj`) keeps one row per company with nested data already living in the `raw_company` JSON column, and switches the dlt write disposition to `merge`/`scd2` keyed on the existing `source_payload_hash` — dlt then adds `_dlt_valid_from`/`_dlt_valid_to` validity windows, retires disappeared companies, and inserts a new version when a company's payload hash changes. **Layer 2** reads the rows whose `_dlt_valid_from` is newer than a stored watermark (= new + updated this run), explodes their `raw_company.names[]` into a normalized `company_names` table via delete-then-insert, and deletes children of retired companies. A mandatory abort-on-empty guard prevents an empty download from mass-retiring the whole table.

**Tech Stack:** Python 3.10+, dlt `>=1.27.2` (SCD2 merge), DuckDB `>=1.5.3`, dagster-dlt, pytest `>=9.1.0`. All deps already present in `pyproject.toml`.

**Assumptions (adjust before executing if wrong):**
- Change detection uses the existing `source_payload_hash` column (hash of the raw company payload, excludes per-run provenance). A separate narrower `business_hash` is *not* introduced; if PRH's `lastModified` proves to bump without substance, that is a follow-up.
- Layer 2 produces a new, self-contained `finland_prhytj.company_names` table as the canonical "explode a nested array on changed rows" unit. Converting the existing `finland_resolved` companies/websites/industries tables to the same incremental pattern is explicitly out of scope (follow-up using the identical mechanism).
- Normalized tables hold current state; history lives only in the Layer 1 SCD2 table.

**Test command (all tasks):** `uv run pytest tests/test_finland_ytj_assets.py -v`

---

### Task 1: Switch Layer 1 to SCD2 with a hermetic pipelines dir

**Files:**
- Modify: `src/dagster_v3/defs/finland_ytj/assets.py:67-105` (resource disposition + pipeline factory)
- Test: `tests/test_finland_ytj_assets.py`

SCD2 state must not leak between test runs, so first make `finland_ytj_pipeline` accept an explicit dlt working directory, then change the write disposition.

- [ ] **Step 1: Write the failing test for SCD2 versioning across two loads**

Add to `tests/test_finland_ytj_assets.py` (reuses the existing `_session`, `_zip_json`, `FakeHttpSession` helpers already in that file):

```python
def _run(database_path, pipelines_dir, payload, run_id):
    return ytj_assets.run_finland_ytj_dlt_pipeline(
        database_path=database_path,
        run_id=run_id,
        session=_session(payload),
        pipelines_dir=pipelines_dir,
    )


def test_scd2_retires_changed_and_absent_companies(tmp_path: Path) -> None:
    database_path = tmp_path / "finland_ytj.duckdb"
    pipelines_dir = tmp_path / "dlt"

    _run(
        database_path,
        pipelines_dir,
        {"companies": [
            {"businessId": {"value": "a"}, "names": [{"name": "A Oy", "type": "1"}]},
            {"businessId": {"value": "b"}, "names": [{"name": "B Oy", "type": "1"}]},
        ]},
        "run-1",
    )
    # second load: 'a' changes its name, 'b' disappears
    _run(
        database_path,
        pipelines_dir,
        {"companies": [
            {"businessId": {"value": "a"}, "names": [{"name": "A Renamed Oy", "type": "1"}]},
        ]},
        "run-2",
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        active = connection.execute(
            f"""
            select business_id, primary_name
            from {DLT_DATASET_NAME}.{DLT_COMPANIES_TABLE}
            where _dlt_valid_to is null
            order by business_id
            """
        ).fetchall()
        a_versions = connection.execute(
            f"""
            select count(*) from {DLT_DATASET_NAME}.{DLT_COMPANIES_TABLE}
            where business_id = 'a'
            """
        ).fetchone()[0]

    assert active == [("a", "A Renamed Oy")]   # b retired, a shows new version only
    assert a_versions == 2                       # old + new version both retained
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_finland_ytj_assets.py::test_scd2_retires_changed_and_absent_companies -v`
Expected: FAIL — `run_finland_ytj_dlt_pipeline()` has no `pipelines_dir` argument (TypeError), and/or no `_dlt_valid_to` column exists.

- [ ] **Step 3: Thread `pipelines_dir` through the pipeline factory and runner**

In `src/dagster_v3/defs/finland_ytj/assets.py`, replace `run_finland_ytj_dlt_pipeline` (lines 86-94) and `finland_ytj_pipeline` (lines 97-105):

```python
def run_finland_ytj_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: HttpSession | None = None,
    pipelines_dir: str | Path | None = None,
) -> Any:
    return finland_ytj_pipeline(database_path, pipelines_dir=pipelines_dir).run(
        finland_ytj_source(run_id=run_id, session=session)
    )


def finland_ytj_pipeline(
    database_path: str | Path,
    *,
    pipelines_dir: str | Path | None = None,
) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name="finland_ytj_all_companies",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=DLT_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(pipelines_dir) if pipelines_dir is not None else None,
    )
```

- [ ] **Step 4: Switch the resource to SCD2 keyed on the payload hash**

In the same file, replace the `_all_companies_resource` decorator (line 67):

```python
@dlt.resource(
    name=DLT_COMPANIES_TABLE,
    write_disposition={
        "disposition": "merge",
        "strategy": "scd2",
        "row_version_column_name": "source_payload_hash",
    },
    max_table_nesting=0,
)
```

Rationale: `row_version_column_name="source_payload_hash"` makes dlt compare *only* that stable column (not the whole row, which carries per-run `source_run_id`/`source_line_number`). No `merge_key` is set, so a full extract retires companies absent from the new dump. `max_table_nesting=0` guarantees nested data stays in the `raw_company` JSON column rather than spawning child tables.

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_finland_ytj_assets.py::test_scd2_retires_changed_and_absent_companies -v`
Expected: PASS

- [ ] **Step 6: Run the existing suite to catch regressions from the disposition change**

Run: `uv run pytest tests/test_finland_ytj_assets.py -v`
Expected: PASS. Note: `test_dlt_duckdb_asset_loads_all_companies_table` still passes because querying without a `_dlt_valid_to` filter returns the single (active) version per company on a first load.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/finland_ytj/assets.py tests/test_finland_ytj_assets.py
git commit -m "feat(finland_ytj): load all_companies as SCD2 keyed on payload hash"
```

---

### Task 2: Mandatory abort-on-empty guard

**Files:**
- Modify: `src/dagster_v3/defs/finland_ytj/assets.py:67-84` (resource body)
- Test: `tests/test_finland_ytj_assets.py`

With SCD2, an empty extract retires **every** company. Guard against it before any row is yielded.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_finland_ytj_assets.py`:

```python
import pytest


def test_empty_download_raises_and_does_not_retire(tmp_path: Path) -> None:
    database_path = tmp_path / "finland_ytj.duckdb"
    pipelines_dir = tmp_path / "dlt"

    _run(database_path, pipelines_dir, {"companies": [{"businessId": {"value": "a"}}]}, "run-1")

    with pytest.raises(ValueError, match="no companies"):
        _run(database_path, pipelines_dir, {"companies": []}, "run-2")

    with duckdb.connect(str(database_path), read_only=True) as connection:
        active = connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{DLT_COMPANIES_TABLE} where _dlt_valid_to is null"
        ).fetchone()[0]
    assert active == 1   # 'a' survived; the empty load was refused
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_finland_ytj_assets.py::test_empty_download_raises_and_does_not_retire -v`
Expected: FAIL — no exception raised; `active == 0` because all rows were retired.

- [ ] **Step 3: Add the guard to the resource body**

In `src/dagster_v3/defs/finland_ytj/assets.py`, replace the body of `_all_companies_resource` (lines 76-83):

```python
    response_body = _download_all_companies(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        session=session,
    )
    payload = json.loads(_json_bytes_from_response(response_body))
    companies = _companies_from_payload(payload)
    if not companies:
        raise ValueError("PRH all_companies returned no companies; refusing to retire the table")
    yield from build_dlt_company_rows(companies, run_id=run_id)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_finland_ytj_assets.py::test_empty_download_raises_and_does_not_retire -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_ytj/assets.py tests/test_finland_ytj_assets.py
git commit -m "feat(finland_ytj): refuse empty SCD2 load to prevent mass retirement"
```

---

### Task 3: Watermark + changed/retired selection

**Files:**
- Create: `src/dagster_v3/defs/finland_ytj/changes.py`
- Test: `tests/test_finland_ytj_changes.py`

Select companies changed since the last normalization using a stored high-water mark over `_dlt_valid_from`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_finland_ytj_changes.py`:

```python
from pathlib import Path

import duckdb

import dagster_v3.defs.finland_ytj.assets as ytj_assets
import dagster_v3.defs.finland_ytj.changes as changes
from tests.test_finland_ytj_assets import _session

DLT_DATASET_NAME = "finland_prhytj"


def _run(database_path, pipelines_dir, payload, run_id):
    return ytj_assets.run_finland_ytj_dlt_pipeline(
        database_path=database_path,
        run_id=run_id,
        session=_session(payload),
        pipelines_dir=pipelines_dir,
    )


def test_changed_and_retired_since_watermark(tmp_path: Path) -> None:
    database_path = tmp_path / "finland_ytj.duckdb"
    pipelines_dir = tmp_path / "dlt"

    _run(database_path, pipelines_dir, {"companies": [
        {"businessId": {"value": "a"}, "names": [{"name": "A Oy", "type": "1"}]},
        {"businessId": {"value": "b"}, "names": [{"name": "B Oy", "type": "1"}]},
    ]}, "run-1")

    with duckdb.connect(str(database_path)) as connection:
        changes.ensure_watermark(connection)
        first_watermark = changes.current_watermark(connection)
        changed_1 = changes.changed_business_ids(connection, since=first_watermark)
        new_watermark = changes.max_valid_from(connection)

    assert sorted(changed_1) == ["a", "b"]

    # second load: 'a' changes, 'b' disappears, 'c' is new
    _run(database_path, pipelines_dir, {"companies": [
        {"businessId": {"value": "a"}, "names": [{"name": "A Renamed Oy", "type": "1"}]},
        {"businessId": {"value": "c"}, "names": [{"name": "C Oy", "type": "1"}]},
    ]}, "run-2")

    with duckdb.connect(str(database_path)) as connection:
        changed_2 = changes.changed_business_ids(connection, since=new_watermark)
        retired_2 = changes.retired_business_ids(connection, since=new_watermark)

    assert sorted(changed_2) == ["a", "c"]   # updated + new, not unchanged
    assert retired_2 == ["b"]                 # gone entirely, not merely re-versioned
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_finland_ytj_changes.py -v`
Expected: FAIL — `dagster_v3.defs.finland_ytj.changes` does not exist (ModuleNotFoundError).

- [ ] **Step 3: Implement the selection module**

Create `src/dagster_v3/defs/finland_ytj/changes.py`:

```python
"""Watermark-based change selection over the Finland YTJ SCD2 table."""

from __future__ import annotations

from typing import Any

DATASET = "finland_prhytj"
COMPANIES_TABLE = "all_companies"
STATE_TABLE = "normalization_state"
EPOCH = "1970-01-01 00:00:00"


def ensure_watermark(connection: Any) -> None:
    connection.execute(f"create schema if not exists {DATASET}")
    connection.execute(
        f"""
        create table if not exists {DATASET}.{STATE_TABLE} (
            id integer primary key,
            last_valid_from timestamp
        )
        """
    )
    connection.execute(
        f"""
        insert into {DATASET}.{STATE_TABLE} (id, last_valid_from)
        select 1, timestamp '{EPOCH}'
        where not exists (select 1 from {DATASET}.{STATE_TABLE} where id = 1)
        """
    )


def current_watermark(connection: Any) -> Any:
    return connection.execute(
        f"select last_valid_from from {DATASET}.{STATE_TABLE} where id = 1"
    ).fetchone()[0]


def max_valid_from(connection: Any) -> Any:
    return connection.execute(
        f"""
        select coalesce(max(_dlt_valid_from), timestamp '{EPOCH}')
        from {DATASET}.{COMPANIES_TABLE}
        """
    ).fetchone()[0]


def advance_watermark(connection: Any, value: Any) -> None:
    connection.execute(
        f"update {DATASET}.{STATE_TABLE} set last_valid_from = ? where id = 1",
        [value],
    )


def changed_business_ids(connection: Any, *, since: Any) -> list[str]:
    rows = connection.execute(
        f"""
        select distinct business_id
        from {DATASET}.{COMPANIES_TABLE}
        where _dlt_valid_to is null
          and _dlt_valid_from > ?
          and business_id is not null and business_id != ''
        order by business_id
        """,
        [since],
    ).fetchall()
    return [row[0] for row in rows]


def retired_business_ids(connection: Any, *, since: Any) -> list[str]:
    rows = connection.execute(
        f"""
        select distinct business_id
        from {DATASET}.{COMPANIES_TABLE} retired
        where retired._dlt_valid_to > ?
          and business_id is not null and business_id != ''
          and not exists (
            select 1 from {DATASET}.{COMPANIES_TABLE} active
            where active.business_id = retired.business_id
              and active._dlt_valid_to is null
          )
        order by business_id
        """,
        [since],
    ).fetchall()
    return [row[0] for row in rows]
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_finland_ytj_changes.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_ytj/changes.py tests/test_finland_ytj_changes.py
git commit -m "feat(finland_ytj): watermark-based changed/retired company selection"
```

---

### Task 4: Incremental `company_names` normalization

**Files:**
- Create: `src/dagster_v3/defs/finland_ytj/normalize.py`
- Test: `tests/test_finland_ytj_normalize.py`

Explode `raw_company.names[]` into `finland_prhytj.company_names`, refreshing only changed companies and removing retired ones.

- [ ] **Step 1: Write the failing test**

Create `tests/test_finland_ytj_normalize.py`:

```python
from pathlib import Path

import duckdb

import dagster_v3.defs.finland_ytj.assets as ytj_assets
import dagster_v3.defs.finland_ytj.normalize as normalize
from tests.test_finland_ytj_assets import _session

DLT_DATASET_NAME = "finland_prhytj"


def _run(database_path, pipelines_dir, payload, run_id):
    return ytj_assets.run_finland_ytj_dlt_pipeline(
        database_path=database_path,
        run_id=run_id,
        session=_session(payload),
        pipelines_dir=pipelines_dir,
    )


def test_incremental_company_names(tmp_path: Path) -> None:
    database_path = tmp_path / "finland_ytj.duckdb"
    pipelines_dir = tmp_path / "dlt"

    _run(database_path, pipelines_dir, {"companies": [
        {"businessId": {"value": "a"}, "names": [
            {"name": "A Oy", "type": "1"}, {"name": "A Aux", "type": "2"}]},
        {"businessId": {"value": "b"}, "names": [{"name": "B Oy", "type": "1"}]},
    ]}, "run-1")

    with duckdb.connect(str(database_path)) as connection:
        first = normalize.normalize_company_names(connection)

    assert first == {"changed": 2, "retired": 0}

    # 'a' drops its aux name, 'b' disappears, 'c' is new
    _run(database_path, pipelines_dir, {"companies": [
        {"businessId": {"value": "a"}, "names": [{"name": "A Oy", "type": "1"}]},
        {"businessId": {"value": "c"}, "names": [{"name": "C Oy", "type": "1"}]},
    ]}, "run-2")

    with duckdb.connect(str(database_path)) as connection:
        second = normalize.normalize_company_names(connection)
        rows = connection.execute(
            f"select business_id, name from {DLT_DATASET_NAME}.company_names order by business_id, name"
        ).fetchall()

    assert second == {"changed": 2, "retired": 1}   # a updated + c new ; b retired
    assert rows == [("a", "A Oy"), ("c", "C Oy")]    # a's aux name gone, b gone
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_finland_ytj_normalize.py -v`
Expected: FAIL — `dagster_v3.defs.finland_ytj.normalize` does not exist.

- [ ] **Step 3: Implement the normalization module**

Create `src/dagster_v3/defs/finland_ytj/normalize.py`:

```python
"""Incremental normalization of changed YTJ companies into child tables."""

from __future__ import annotations

from typing import Any

from dagster_v3.defs.finland_ytj import changes

NAMES_TABLE = "company_names"


def _ensure_names_table(connection: Any) -> None:
    connection.execute(f"create schema if not exists {changes.DATASET}")
    connection.execute(
        f"""
        create table if not exists {changes.DATASET}.{NAMES_TABLE} (
            business_id varchar,
            name varchar,
            name_type varchar,
            is_current boolean
        )
        """
    )


def normalize_company_names(connection: Any) -> dict[str, int]:
    changes.ensure_watermark(connection)
    _ensure_names_table(connection)

    watermark = changes.current_watermark(connection)
    next_watermark = changes.max_valid_from(connection)
    changed = changes.changed_business_ids(connection, since=watermark)
    retired = changes.retired_business_ids(connection, since=watermark)

    affected = changed + retired
    if affected:
        placeholders = ", ".join("?" for _ in affected)
        connection.execute(
            f"delete from {changes.DATASET}.{NAMES_TABLE} where business_id in ({placeholders})",
            affected,
        )

    if changed:
        placeholders = ", ".join("?" for _ in changed)
        connection.execute(
            f"""
            insert into {changes.DATASET}.{NAMES_TABLE}
            select
                c.business_id,
                json_extract_string(name_item.value, '$.name') as name,
                json_extract_string(name_item.value, '$.type') as name_type,
                coalesce(json_extract_string(name_item.value, '$.endDate'), '') = '' as is_current
            from {changes.DATASET}.{changes.COMPANIES_TABLE} c,
                 json_each(json_extract(c.raw_company, '$.names')) as name_item
            where c._dlt_valid_to is null
              and c.business_id in ({placeholders})
              and json_extract_string(name_item.value, '$.name') is not null
            """,
            changed,
        )

    changes.advance_watermark(connection, next_watermark)
    return {"changed": len(changed), "retired": len(retired)}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_finland_ytj_normalize.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_ytj/normalize.py tests/test_finland_ytj_normalize.py
git commit -m "feat(finland_ytj): incrementally normalize changed company names"
```

---

### Task 5: Dagster asset + non-empty asset check + wiring

**Files:**
- Modify: `src/dagster_v3/defs/finland_ytj/assets.py:128-135` (the `defs` block) and add the normalization asset + check
- Test: `tests/test_finland_ytj_assets.py`

Expose Layer 2 as a Dagster asset downstream of the SCD2 load, and add a non-empty asset check on Layer 1.

- [ ] **Step 1: Write the failing test for definitions wiring**

Add to `tests/test_finland_ytj_assets.py`:

```python
def test_changes_asset_and_check_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    assert "finland_ytj_company_names" in asset_keys

    check_keys = {check.name for check in repository.asset_graph.asset_checks_defs}
    assert "all_companies_non_empty" in check_keys
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_finland_ytj_assets.py::test_changes_asset_and_check_are_registered -v`
Expected: FAIL — asset key `finland_ytj_company_names` not found.

- [ ] **Step 3: Add the asset, the check, and register them**

In `src/dagster_v3/defs/finland_ytj/assets.py`, add an import near the top (after line 18):

```python
from dagster_v3.defs.finland_ytj.normalize import normalize_company_names
```

Add the asset and check above the `defs = dg.Definitions(...)` block (before line 128):

```python
@dg.asset(
    deps=["finland_ytj_all_companies_duckdb"],
    group_name="finland_ytj",
    kinds={"python", "duckdb", "sql"},
    description="Incrementally normalized Finland YTJ company names for companies changed since the last run.",
)
def finland_ytj_company_names(ytj_duckdb: LocalDuckDBResource) -> dg.MaterializeResult:
    with ytj_duckdb.connect() as connection:
        counts = normalize_company_names(connection)
    return dg.MaterializeResult(
        metadata={"changed_companies": counts["changed"], "retired_companies": counts["retired"]}
    )


@dg.asset_check(asset="finland_ytj_all_companies_duckdb", name="all_companies_non_empty")
def all_companies_non_empty(ytj_duckdb: LocalDuckDBResource) -> dg.AssetCheckResult:
    with ytj_duckdb.connect(read_only=True) as connection:
        active = connection.execute(
            f"""
            select count(*) from {DLT_DATASET_NAME}.{DLT_COMPANIES_TABLE}
            where _dlt_valid_to is null
            """
        ).fetchone()[0]
    return dg.AssetCheckResult(passed=active > 0, metadata={"active_companies": active})
```

Replace the `defs` block (lines 128-135):

```python
defs = dg.Definitions(
    assets=[
        finland_ytj_all_companies_duckdb_asset,
        finland_ytj_company_names,
    ],
    asset_checks=[all_companies_non_empty],
    resources={
        "ytj_duckdb": LocalDuckDBResource(),
    },
)
```

- [ ] **Step 4: Run the wiring test to verify it passes**

Run: `uv run pytest tests/test_finland_ytj_assets.py::test_changes_asset_and_check_are_registered -v`
Expected: PASS

- [ ] **Step 5: Validate the Dagster definitions load cleanly**

Run: `uv run dg check defs`
Expected: no errors — the new asset and check resolve, `finland_ytj_company_names` depends on `finland_ytj_all_companies_duckdb`.

- [ ] **Step 6: Run the full Finland YTJ test set**

Run: `uv run pytest tests/test_finland_ytj_assets.py tests/test_finland_ytj_changes.py tests/test_finland_ytj_normalize.py -v`
Expected: PASS

- [ ] **Step 7: Run the finland_resolved tests to confirm the SCD2 change did not break the downstream resolver**

Run: `uv run pytest tests/test_finland_resolved_assets.py -v`
Expected: PASS. `finland_resolved` reads `all_companies` without a `_dlt_valid_to` filter; if any test now sees multiple versions per company, add `where _dlt_valid_to is null` to the resolver SQL in `finland_resolved/assets.py` as a follow-up (note it, do not silently change behavior here).

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/finland_ytj/assets.py tests/test_finland_ytj_assets.py
git commit -m "feat(finland_ytj): add incremental company-names asset and non-empty check"
```

---

## Self-Review

**Spec coverage:**
- SCD2 load keyed on payload hash → Task 1.
- Keep nested data as JSON, no child tables → Task 1 (`max_table_nesting=0`).
- Empty-download protection (critical under SCD2) → Task 2.
- "Compare what we have vs what's new" / changed + retired selection → Task 3.
- "Normalization on the changed rows" → Task 4 (delete-then-insert per changed company, delete retired).
- Surface as Dagster asset + observability/guard → Task 5 (asset, check, metadata).

**Out of scope (stated):** converting `finland_resolved` companies/websites/industries to incremental merge; streaming/ijson download rewrite (R1/R3 from the analysis doc); partitioning/scheduling. Each is a clean follow-up plan reusing these primitives.

**Type consistency:** `changes.DATASET`/`COMPANIES_TABLE`/`STATE_TABLE` constants are referenced consistently by `normalize.py`; `normalize_company_names` returns `{"changed", "retired"}` matching both its test and the asset metadata; `run_finland_ytj_dlt_pipeline(..., pipelines_dir=...)` signature matches every test caller.

**Open risk to verify during execution:** dlt's exact `_dlt_valid_from`/`_dlt_valid_to` column names and the timestamp type are assumed from dlt SCD2 defaults — confirm with a one-off `DESCRIBE finland_prhytj.all_companies` after Task 1, Step 5, and adjust column names in `changes.py` if a configured naming convention differs.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-18-finland-ytj-scd2-incremental.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
