# Norway Resources Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Norway BRREG dlt/source resource code out of `assets.py` into `resources.py` so `assets.py` only defines Dagster assets, sensors, and orchestration glue.

**Architecture:** Create `src/dagster_v3/defs/norway_brreg/resources.py` as the owner of BRREG entity source iteration and dlt pipeline runner functions. `assets.py` imports those functions and uses them from assets. Keep compatibility aliases in `assets.py` for existing test/caller references, but tests should assert canonical ownership lives in `resources.py`.

**Tech Stack:** Python 3.14, Dagster, dlt, DuckDB, ijson, pytest.

---

## File Structure

- Create `src/dagster_v3/defs/norway_brreg/resources.py`
  - `HttpSession`
  - `BRREG_ENTITIES_COLUMNS`
  - `BRREG_FINANCIAL_STATEMENTS_COLUMNS`
  - `iter_brreg_entity_rows`
  - `build_entity_rows`
  - `run_norway_brreg_entities_dlt_pipeline`
  - `run_norway_brreg_financial_fetches_dlt_pipeline`
  - source parsing helpers used only by entity source/resource code.
- Modify `src/dagster_v3/defs/norway_brreg/assets.py`
  - Import resource functions from `.resources`.
  - Remove dlt source/resource implementations and entity parsing helpers.
  - Keep aliases for stable existing API:
    ```python
    BRREG_ENTITIES_COLUMNS = resources.BRREG_ENTITIES_COLUMNS
    build_entity_rows = resources.build_entity_rows
    ```
- Modify `tests/test_norway_brreg_assets.py`
  - Import `resources as brreg_resources`.
  - Assert canonical dlt/source functions live in `resources.py`.
  - Update direct source/resource tests to call `brreg_resources`.

---

### Task 1: Add Tests For Resource Ownership

**Files:**
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Import resources module in tests**

Add:

```python
from dagster_v3.defs.norway_brreg import resources as brreg_resources
```

near the existing Norway imports.

- [ ] **Step 2: Add ownership assertion test**

Add this test near the existing source/resource tests:

```python
def test_norway_brreg_dlt_resources_are_defined_in_resources_module() -> None:
    assert brreg_assets.run_norway_brreg_entities_dlt_pipeline is (
        brreg_resources.run_norway_brreg_entities_dlt_pipeline
    )
    assert brreg_assets.run_norway_brreg_financial_fetches_dlt_pipeline is (
        brreg_resources.run_norway_brreg_financial_fetches_dlt_pipeline
    )
    assert brreg_assets.iter_brreg_entity_rows is brreg_resources.iter_brreg_entity_rows
    assert brreg_assets.build_entity_rows is brreg_resources.build_entity_rows
```

- [ ] **Step 3: Update direct resource tests to call `brreg_resources`**

In tests that directly exercise source/resource behavior, replace:

```python
brreg_assets.build_entity_rows(...)
brreg_assets.iter_brreg_entity_rows(...)
brreg_assets.run_norway_brreg_entities_dlt_pipeline(...)
brreg_assets.run_norway_brreg_financial_fetches_dlt_pipeline(...)
```

with:

```python
brreg_resources.build_entity_rows(...)
brreg_resources.iter_brreg_entity_rows(...)
brreg_resources.run_norway_brreg_entities_dlt_pipeline(...)
brreg_resources.run_norway_brreg_financial_fetches_dlt_pipeline(...)
```

Keep asset orchestration tests using `brreg_assets` for actual Dagster assets and orchestration functions.

- [ ] **Step 4: Run focused tests and verify failure**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_brreg_dlt_resources_are_defined_in_resources_module tests/test_norway_brreg_assets.py::test_iter_brreg_entity_rows_downloads_gzip_and_yields_rows -q
```

Expected: FAIL because `dagster_v3.defs.norway_brreg.resources` does not exist yet.

---

### Task 2: Create `resources.py`

**Files:**
- Create: `src/dagster_v3/defs/norway_brreg/resources.py`
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`

- [ ] **Step 1: Move entity source/resource code to `resources.py`**

Move these from `assets.py` to `resources.py`:

```python
class HttpSession(Protocol): ...
BRREG_ENTITIES_COLUMNS = tables.BRREG_ENTITIES_COLUMNS
BRREG_FINANCIAL_STATEMENTS_COLUMNS = tables.BRREG_FINANCIAL_STATEMENTS_COLUMNS
def iter_brreg_entity_rows(...): ...
def build_entity_rows(...): ...
def run_norway_brreg_entities_dlt_pipeline(...): ...
def run_norway_brreg_financial_fetches_dlt_pipeline(...): ...
def source_payload_hash(...): ...
def _entity_row(...): ...
def _download_bytes(...): ...
def _stream_gzip_json_array(...): ...
def _entity_status(...): ...
def _source_url(...): ...
def _json_dumps(...): ...
def _json_default(...): ...
def _address_lines(...): ...
def _joined_text_lines(...): ...
def _dict(...): ...
def _list(...): ...
def _bool(...): ...
def _int_or_none(...): ...
def _string(...): ...
```

`resources.py` imports:

```python
from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Iterator
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol

import dlt
import ijson
import requests

from dagster_v3.defs.norway_brreg import tables
from dagster_v3.defs.norway_brreg.financial_fetches import (
    BRREG_FINANCIAL_FETCHES_COLUMNS,
    FINANCIAL_FETCHES_TABLE,
    iter_brreg_financial_statement_fetch_rows,
)
```

Define these constants in `resources.py`:

```python
COUNTRY = "NO"
DLT_DATASET_NAME = "norway_brreg"
ENTITIES_TABLE = "entities"
ENTITY_SOURCE_SLUG = "norway_brregenhet"
BRREG_BASE_URL = "https://data.brreg.no/enhetsregisteret/api"
BRREG_REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
BRREG_LEGAL_FORM_DESCRIPTION_EN_BY_CODE = {...}
```

- [ ] **Step 2: Replace moved definitions in `assets.py` with imports/aliases**

In `assets.py`, import:

```python
from dagster_v3.defs.norway_brreg import resources
from dagster_v3.defs.norway_brreg.resources import (
    BRREG_BASE_URL,
    BRREG_ENTITIES_COLUMNS,
    BRREG_FINANCIAL_STATEMENTS_COLUMNS,
    BRREG_REGNSKAP_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_USER_AGENT,
    ENTITIES_TABLE,
    HttpSession,
    build_entity_rows,
    iter_brreg_entity_rows,
    run_norway_brreg_entities_dlt_pipeline,
    run_norway_brreg_financial_fetches_dlt_pipeline,
)
```

Remove the moved code blocks from `assets.py`.

- [ ] **Step 3: Keep asset-only constants in `assets.py`**

Leave these in `assets.py`:

```python
GROUP_NAME
FINANCIAL_STATEMENTS_TABLE
FINANCIAL_SOURCE_SLUG
NORWAY_BRREG_TRANSLATION_SOURCE_SLUG
NORWAY_BRREG_TRANSLATION_WORKFLOW_ID
NORWAY_BRREG_DUCKDB_PATH
NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH
NORWAY_BRREG_LLM_TRANSLATION_FIELDS
NORWAY_BRREG_EN_FIELD_BY_ORIGINAL_FIELD
```

Do not move Dagster config classes or asset functions.

---

### Task 3: Remove Unused Imports And Fix Boundaries

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `src/dagster_v3/defs/norway_brreg/resources.py`

- [ ] **Step 1: Remove source-only imports from `assets.py`**

After moving source/resource functions, remove imports that are no longer used by `assets.py`, including any of:

```python
import gzip
import hashlib
from io import BytesIO
import ijson
import requests
```

Only remove imports after `rg` or test failures prove they are unused.

- [ ] **Step 2: Ensure no dlt resource construction remains in `assets.py`**

Run:

```bash
rg -n "dlt\\.resource|dlt\\.pipeline|iter_brreg_entity_rows|_download_bytes|_stream_gzip_json_array|_entity_row" src/dagster_v3/defs/norway_brreg/assets.py
```

Expected: only imported names or compatibility alias references remain; no function definitions for source/resource construction.

---

### Task 4: Validate

**Files:**
- No new files.

- [ ] **Step 1: Run focused Norway resource tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_brreg_dlt_resources_are_defined_in_resources_module tests/test_norway_brreg_assets.py::test_iter_brreg_entity_rows_downloads_gzip_and_yields_rows tests/test_norway_brreg_assets.py::test_financial_fetch_and_normalize_pipeline_loads_statements_table -q
```

Expected: PASS.

- [ ] **Step 2: Run full Norway asset tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
```

Expected: PASS.

- [ ] **Step 3: Validate Dagster definitions**

Run:

```bash
uv run dg check defs
```

Expected: `All definitions loaded successfully.`

---

## Self-Review

**Spec coverage:** The plan moves dlt/source resource code from `assets.py` into `resources.py`, while preserving Dagster asset definitions in `assets.py`.

**Placeholder scan:** No placeholders remain.

**Type consistency:** Source/resource functions are imported from `resources.py`; asset functions keep the same runtime behavior.
