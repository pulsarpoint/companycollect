# Norway Table Schema Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `src/dagster_v3/defs/norway_brreg/tables.py` the single owner of Norway BRREG entity and normalized financial statement schema definitions.

**Architecture:** Move the dlt/DuckDB column schema dictionaries out of `assets.py` and into `tables.py`, next to the ClickHouse table column/DDL contracts. Keep short compatibility aliases in `assets.py` only if tests or callers already reference those names. Pass shallow copies of dlt column specs into dlt and DuckDB table creation so dlt cannot mutate shared table constants.

**Tech Stack:** Python 3.14, Dagster, dlt, DuckDB, pytest.

---

## File Structure

- Modify `src/dagster_v3/defs/norway_brreg/tables.py`
  - Add `BRREG_ENTITIES_COLUMNS`.
  - Add `BRREG_FINANCIAL_STATEMENTS_COLUMNS`.
  - Add `copy_dlt_columns(columns)` only if needed to protect shared schema constants from dlt mutation.
- Modify `src/dagster_v3/defs/norway_brreg/assets.py`
  - Remove large inline `BRREG_ENTITIES_COLUMNS` and `BRREG_FINANCIAL_STATEMENTS_COLUMNS` dictionaries.
  - Add aliases:
    ```python
    BRREG_ENTITIES_COLUMNS = tables.BRREG_ENTITIES_COLUMNS
    BRREG_FINANCIAL_STATEMENTS_COLUMNS = tables.BRREG_FINANCIAL_STATEMENTS_COLUMNS
    ```
  - Pass copied schemas to dlt and table replacement calls.
- Modify `tests/test_norway_brreg_assets.py`
  - Assert `brreg_assets.BRREG_ENTITIES_COLUMNS is brreg_tables.BRREG_ENTITIES_COLUMNS`.
  - Assert `brreg_assets.BRREG_FINANCIAL_STATEMENTS_COLUMNS is brreg_tables.BRREG_FINANCIAL_STATEMENTS_COLUMNS`.
  - Assert dlt pipeline execution does not mutate table constants.

---

### Task 1: Add Tests For Schema Ownership

**Files:**
- Modify: `tests/test_norway_brreg_assets.py`

- [x] **Step 1: Extend entity schema test**

In `test_entity_resource_declares_explicit_table_schema`, add:

```python
assert brreg_assets.BRREG_ENTITIES_COLUMNS is brreg_tables.BRREG_ENTITIES_COLUMNS
```

- [x] **Step 2: Extend financial statement schema test**

In `test_financial_statement_schema_matches_normalized_rows`, add:

```python
assert (
    brreg_assets.BRREG_FINANCIAL_STATEMENTS_COLUMNS
    is brreg_tables.BRREG_FINANCIAL_STATEMENTS_COLUMNS
)
```

- [x] **Step 3: Add mutation-protection test**

Add this test near the schema tests:

```python
def test_norway_table_schema_constants_are_not_mutated_by_dlt(tmp_path: Path) -> None:
    assert "name" not in brreg_tables.BRREG_ENTITIES_COLUMNS["org_number"]

    brreg_assets.run_norway_brreg_entities_dlt_pipeline(
        database_path=tmp_path / "norway.duckdb",
        run_id="entity-run",
        session=FakeHttpSession(_gzip_json_array([_entity_record()])),
    )

    assert "name" not in brreg_tables.BRREG_ENTITIES_COLUMNS["org_number"]
```

- [x] **Step 4: Run focused tests and verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_entity_resource_declares_explicit_table_schema tests/test_norway_brreg_assets.py::test_financial_statement_schema_matches_normalized_rows tests/test_norway_brreg_assets.py::test_norway_table_schema_constants_are_not_mutated_by_dlt -q
```

Expected: FAIL because the schema dictionaries are still defined in `assets.py`, not owned by `tables.py`.

---

### Task 2: Move Schema Dictionaries To `tables.py`

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/tables.py`
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`

- [x] **Step 1: Move `BRREG_ENTITIES_COLUMNS` to `tables.py`**

Cut the full `BRREG_ENTITIES_COLUMNS: dict[str, dict[str, Any]] = {...}` dictionary from `assets.py` and paste it into `tables.py` after the qualified table constants.

Add this import at the top of `tables.py`:

```python
from typing import Any
```

- [x] **Step 2: Move `BRREG_FINANCIAL_STATEMENTS_COLUMNS` to `tables.py`**

Cut the full `BRREG_FINANCIAL_STATEMENTS_COLUMNS: dict[str, dict[str, Any]] = {...}` dictionary from `assets.py` and paste it into `tables.py` after `BRREG_ENTITIES_COLUMNS`.

- [x] **Step 3: Add explicit copy helper in `tables.py`**

Add:

```python
def copy_dlt_columns(columns: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {name: dict(spec) for name, spec in columns.items()}
```

This helper names a real boundary: handing schema to dlt, which mutates column dictionaries.

- [x] **Step 4: Replace large dictionaries in `assets.py` with aliases**

In `assets.py`, replace both deleted dictionaries with:

```python
BRREG_ENTITIES_COLUMNS = tables.BRREG_ENTITIES_COLUMNS
BRREG_FINANCIAL_STATEMENTS_COLUMNS = tables.BRREG_FINANCIAL_STATEMENTS_COLUMNS
```

Keep these aliases near the other source constants so existing tests and imports remain stable.

---

### Task 3: Pass Schema Copies To Mutating Boundaries

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`

- [x] **Step 1: Copy entity columns before passing to dlt**

Change:

```python
columns=BRREG_ENTITIES_COLUMNS,
```

to:

```python
columns=tables.copy_dlt_columns(BRREG_ENTITIES_COLUMNS),
```

inside `run_norway_brreg_entities_dlt_pipeline`.

- [x] **Step 2: Copy financial statement columns before replacing DuckDB table**

Change:

```python
columns=BRREG_FINANCIAL_STATEMENTS_COLUMNS,
```

to:

```python
columns=tables.copy_dlt_columns(BRREG_FINANCIAL_STATEMENTS_COLUMNS),
```

inside `normalize_norway_brreg_financial_statements_duckdb`.

If other dlt calls pass the moved column constants directly, use the same copy call there.

---

### Task 4: Validate

**Files:**
- No new files.

- [x] **Step 1: Run focused Norway schema tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_entity_resource_declares_explicit_table_schema tests/test_norway_brreg_assets.py::test_financial_statement_schema_matches_normalized_rows tests/test_norway_brreg_assets.py::test_norway_table_schema_constants_are_not_mutated_by_dlt -q
```

Expected: PASS.

- [x] **Step 2: Validate Dagster definitions**

Run:

```bash
uv run dg check defs
```

Expected: `All definitions loaded successfully.`

- [x] **Step 3: Run relevant Norway tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
```

Expected: PASS.

---

## Self-Review

**Spec coverage:** The plan removes duplicated BRREG entity and financial statement schema dictionaries from `assets.py`, makes `tables.py` the schema owner, and protects shared constants from dlt mutation.

**Placeholder scan:** No placeholders remain.

**Type consistency:** The schema names remain `BRREG_ENTITIES_COLUMNS` and `BRREG_FINANCIAL_STATEMENTS_COLUMNS`; only ownership moves to `tables.py`.
