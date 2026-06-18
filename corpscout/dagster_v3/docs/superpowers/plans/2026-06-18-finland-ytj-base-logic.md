# Finland YTJ Base-Logic Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Finland YTJ raw-ingestion asset correct and robust per the analysis (`docs/superpowers/analysis/2026-06-18-finland-ytj-assets.md`), addressing findings R1–R8, **before** any SCD2/incremental work.

**Architecture:** Keep the existing `@dlt_assets` → dlt source/pipeline → DuckDB shape and the `replace` write disposition. Change the *internals*: stream the bulk download to a temp file and parse it incrementally with `ijson` (flat memory), use dlt's retry-capable HTTP client, refuse empty downloads before they wipe the table, stop mutating the caller's HTTP session, replace PRH magic strings with named constants, remove import-time resource instantiation, surface row-count metadata via an asset check, and relocate the shared resources out of `finland_ytj` into `defs/common`.

**Tech Stack:** Python 3.10+, dlt `>=1.27.2` (`dlt.sources.helpers.requests`), `ijson>=3.4.0`, DuckDB, dagster-dlt, pytest `>=9.1.0`. All deps already in `pyproject.toml`.

**Decisions locked for this plan:**
- Scope = **all** findings R1–R8, including the resource move (R7).
- R1 ZIP strategy = **download to a temp file, then stream-parse the JSON member** (flat RAM, uses disk, no new dependency).
- Shared-resource home = `src/dagster_v3/defs/common/resources.py` (the analysis's suggested neutral location).
- Write disposition stays `replace`; SCD2 is a separate follow-up plan that rebases on this one.

**Sequencing note:** The previously drafted SCD2 plan (`2026-06-18-finland-ytj-scd2-incremental.md`) overlaps this one (its `pipelines_dir` threading and empty-guard). After this plan lands, re-derive the SCD2 plan on top of the new streaming resource.

**Test command (all tasks):** `uv run pytest tests/ -v` (scope to the touched files per task as noted).

---

### Task 1: Relocate shared resources to `defs/common` (R7)

**Files:**
- Create: `src/dagster_v3/defs/common/__init__.py`
- Create: `src/dagster_v3/defs/common/resources.py`
- Delete: `src/dagster_v3/defs/finland_ytj/resources.py`
- Modify: `src/dagster_v3/defs/finland_ytj/assets.py:18`, `src/dagster_v3/defs/finland_xbrl/assets.py:27`, `src/dagster_v3/defs/finland_resolved/assets.py:13`
- Modify (tests): `tests/test_finland_ytj_assets.py`, `tests/test_finland_resolved_assets.py:11`, `tests/test_finland_xbrl_assets.py:29`, `tests/test_finland_xbrl_parsed_assets.py:22`
- Test: `tests/test_common_resources.py`

- [ ] **Step 1: Write the failing test for the new location**

Create `tests/test_common_resources.py`:

```python
import importlib


def test_shared_resources_live_in_common() -> None:
    module = importlib.import_module("dagster_v3.defs.common.resources")
    assert hasattr(module, "LocalDuckDBResource")
    assert hasattr(module, "ObjectStoreResource")


def test_old_finland_ytj_resources_module_is_gone() -> None:
    try:
        importlib.import_module("dagster_v3.defs.finland_ytj.resources")
    except ModuleNotFoundError:
        return
    raise AssertionError("finland_ytj.resources should no longer exist")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_common_resources.py -v`
Expected: FAIL — `dagster_v3.defs.common.resources` does not exist.

- [ ] **Step 3: Create the common package and move the resources verbatim**

Create `src/dagster_v3/defs/common/__init__.py`:

```python
"""Shared, cross-domain Dagster resources."""
```

Create `src/dagster_v3/defs/common/resources.py` with the exact current contents of `finland_ytj/resources.py` (the `S3Client` Protocol, `ObjectStoreResource`, `LocalDuckDBResource`, and `_error_code`). Copy the file byte-for-byte from `src/dagster_v3/defs/finland_ytj/resources.py` (lines 1-106).

- [ ] **Step 4: Delete the old module**

```bash
git rm src/dagster_v3/defs/finland_ytj/resources.py
```

- [ ] **Step 5: Update all six import sites**

Replace in each file:

- `src/dagster_v3/defs/finland_ytj/assets.py:18`
  `from dagster_v3.defs.finland_ytj.resources import LocalDuckDBResource`
  → `from dagster_v3.defs.common.resources import LocalDuckDBResource`
- `src/dagster_v3/defs/finland_xbrl/assets.py:27`
  `from dagster_v3.defs.finland_ytj.resources import LocalDuckDBResource, ObjectStoreResource`
  → `from dagster_v3.defs.common.resources import LocalDuckDBResource, ObjectStoreResource`
- `src/dagster_v3/defs/finland_resolved/assets.py:13`
  `from dagster_v3.defs.finland_ytj.resources import LocalDuckDBResource`
  → `from dagster_v3.defs.common.resources import LocalDuckDBResource`
- `tests/test_finland_resolved_assets.py:11`
  → `from dagster_v3.defs.common.resources import LocalDuckDBResource`
- `tests/test_finland_xbrl_assets.py:29`
  → `from dagster_v3.defs.common.resources import LocalDuckDBResource, ObjectStoreResource`
- `tests/test_finland_xbrl_parsed_assets.py:22`
  → `from dagster_v3.defs.common.resources import LocalDuckDBResource, ObjectStoreResource`

- [ ] **Step 6: Run the new test plus all affected suites**

Run: `uv run pytest tests/test_common_resources.py tests/test_finland_ytj_assets.py tests/test_finland_resolved_assets.py tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_parsed_assets.py -v`
Expected: PASS (the xbrl suites may be slow; if any are marked `integration` and require services, run `-m "not integration"`).

- [ ] **Step 7: Validate definitions still load**

Run: `uv run dg check defs`
Expected: no errors.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: move shared duckdb/object-store resources to defs/common"
```

---

### Task 2: Named PRH constants + remove import-time resource instantiation (R4, R6)

**Files:**
- Modify: `src/dagster_v3/defs/finland_ytj/assets.py` (constants near top; `_primary_name`, `_lifecycle_status`; decorator path)
- Test: `tests/test_finland_ytj_assets.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_finland_ytj_assets.py`:

```python
def test_prh_code_constants_drive_classification() -> None:
    assert ytj_assets.PRH_NAME_TYPE_PRIMARY == "1"
    assert ytj_assets.PRH_TRADE_REGISTER_STATUS_CEASED == "3"
    assert ytj_assets.DEFAULT_DUCKDB_PATH == "data/finland_ytj.duckdb"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_finland_ytj_assets.py::test_prh_code_constants_drive_classification -v`
Expected: FAIL — `AttributeError: module ... has no attribute 'PRH_NAME_TYPE_PRIMARY'`.

- [ ] **Step 3: Add constants and use them**

In `src/dagster_v3/defs/finland_ytj/assets.py`, add after the existing module constants (after line 25):

```python
DEFAULT_DUCKDB_PATH = "data/finland_ytj.duckdb"
PRH_NAME_TYPE_PRIMARY = "1"             # PRH name "type": current primary trade name
PRH_TRADE_REGISTER_STATUS_CEASED = "3"  # PRH tradeRegisterStatus: removed from trade register
```

Replace the magic string in `_primary_name` (currently `_string(name.get("type")) == "1"`):

```python
        if _string(name.get("type")) == PRH_NAME_TYPE_PRIMARY and not _string(name.get("endDate"))
```

Replace the magic string in `_lifecycle_status` (currently `== "3"`):

```python
    if _string(company.get("endDate")) or _string(company.get("tradeRegisterStatus")) == PRH_TRADE_REGISTER_STATUS_CEASED:
```

- [ ] **Step 4: Remove import-time resource instantiation in the decorator (R6)**

`LocalDuckDBResource` should already be imported from `defs.common.resources` (Task 1). In the `@dlt_assets` decorator, replace `dlt_pipeline=finland_ytj_pipeline(LocalDuckDBResource().path())` with the constant:

```python
@dlt_assets(
    dlt_source=finland_ytj_source(),
    dlt_pipeline=finland_ytj_pipeline(DEFAULT_DUCKDB_PATH),  # spec-only; body re-creates with injected path
    name="finland_ytj_all_companies_duckdb",
    dagster_dlt_translator=FinlandYtjDltTranslator(),
)
```

And set the resource default to the same constant in `src/dagster_v3/defs/common/resources.py`:

```python
class LocalDuckDBResource(dg.ConfigurableResource):
    database_path: str = "data/finland_ytj.duckdb"
```

(Leave the literal default in `common/resources.py` as-is to avoid a cross-module import; the test only asserts `DEFAULT_DUCKDB_PATH` equals this value.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_finland_ytj_assets.py::test_prh_code_constants_drive_classification tests/test_finland_ytj_assets.py::test_dlt_company_rows_keep_raw_payload_and_extract_eligibility_fields -v`
Expected: PASS (the second test confirms classification still works through the constants).

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_ytj/assets.py tests/test_finland_ytj_assets.py
git commit -m "refactor(finland_ytj): name PRH codes and drop import-time resource construction"
```

---

### Task 3: Streaming temp-file download + retry + empty guard (R1, R2, R3, R8)

**Files:**
- Modify: `src/dagster_v3/defs/finland_ytj/assets.py` (imports, `HttpSession` Protocol, `_all_companies_resource`, `_download_all_companies`, add `_json_path_from_download`/`_iter_companies`/`_ijson_prefix`; remove `_json_bytes_from_response`)
- Modify (tests): `tests/test_finland_ytj_assets.py` (rewrite `FakeResponse`/`FakeHttpSession`/`_session`)
- Test: `tests/test_finland_ytj_assets.py`

This is the core change. The resource downloads to a temp dir, extracts the JSON member to a temp file, and `ijson`-streams companies one at a time, raising before yielding anything if the feed is empty.

- [ ] **Step 1: Rewrite the test fakes for streaming and write the new behavior tests**

In `tests/test_finland_ytj_assets.py`, replace the `FakeResponse`, `FakeHttpSession`, `_zip_json`, and `_session` helpers (current lines 15-45) with:

```python
class FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200) -> None:
        self._content = content
        self.status_code = status_code

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int = 1 << 20):
        for start in range(0, len(self._content), chunk_size):
            yield self._content[start : start + chunk_size]


class FakeHttpSession:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[tuple[str, dict | None]] = []

    def get(self, url: str, *, headers: dict | None = None, stream: bool = False, timeout: int = 120) -> FakeResponse:
        self.calls.append((url, headers))
        return FakeResponse(self.content)


def _zip_json(name: str, payload: dict) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr(name, json.dumps(payload))
    return buffer.getvalue()


def _session(payload: dict) -> FakeHttpSession:
    return FakeHttpSession(_zip_json("companies.json", payload))
```

Then add behavior tests:

```python
def test_source_streams_company_rows_from_zip() -> None:
    session = _session({"companies": [
        {"businessId": {"value": "old"}, "registrationDate": "2023-12-31"},
        {"businessId": {"value": "latest"}, "registrationDate": "2026-06-15"},
    ]})
    source = ytj_assets.finland_ytj_source(session=session, run_id="test-run")
    rows = list(source.resources[DLT_COMPANIES_TABLE])

    assert [row["business_id"] for row in rows] == ["old", "latest"]
    assert rows[0]["source_run_id"] == "test-run"
    assert session.calls == [
        ("https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies",
         {"User-Agent": "corpscout-dagster-v3-dev/0.1"})
    ]


def test_source_handles_bare_json_array_without_zip() -> None:
    raw = json.dumps([{"businessId": {"value": "x"}}]).encode("utf-8")
    session = FakeHttpSession(raw)
    source = ytj_assets.finland_ytj_source(session=session, run_id="r")
    rows = list(source.resources[DLT_COMPANIES_TABLE])
    assert [row["business_id"] for row in rows] == ["x"]


def test_source_does_not_mutate_caller_session() -> None:
    session = _session({"companies": [{"businessId": {"value": "x"}}]})
    list(ytj_assets.finland_ytj_source(session=session, run_id="r").resources[DLT_COMPANIES_TABLE])
    assert not hasattr(session, "headers")  # never wrote a User-Agent onto the session


def test_empty_feed_raises_before_yielding() -> None:
    session = _session({"companies": []})
    source = ytj_assets.finland_ytj_source(session=session, run_id="r")
    with pytest.raises(ValueError, match="no companies"):
        list(source.resources[DLT_COMPANIES_TABLE])
```

Add `import pytest` at the top of the test file if not present.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_finland_ytj_assets.py -k "stream or bare_json or mutate or empty_feed" -v`
Expected: FAIL — the new helpers reference streaming the resource still buffers; `session.calls` tuple shape differs; no empty guard.

- [ ] **Step 3: Update imports and the `HttpSession` Protocol**

In `src/dagster_v3/defs/finland_ytj/assets.py`, update the import block (lines 1-16). Remove `from io import BytesIO`; add `shutil`, `tempfile`, `ijson`, and the dlt requests helper:

```python
import hashlib
import json
import shutil
import tempfile
import urllib.parse
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol
from zipfile import ZipFile, is_zipfile

import dagster as dg
import dlt
import ijson
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import LocalDuckDBResource
```

Replace the `HttpSession` Protocol (lines 28-32):

```python
class HttpSession(Protocol):
    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        stream: bool = False,
        timeout: int = 120,
    ) -> Any:
        ...
```

- [ ] **Step 4: Rewrite the resource to stream, parse incrementally, and guard empty**

Replace `_all_companies_resource` (current lines 67-83):

```python
@dlt.resource(name=DLT_COMPANIES_TABLE, write_disposition="replace", primary_key="business_id")
def _all_companies_resource(
    *,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    session: HttpSession | None,
) -> Iterator[dict[str, Any]]:
    with tempfile.TemporaryDirectory(prefix="finland_ytj_") as tmpdir:
        work_dir = Path(tmpdir)
        download_path = _download_all_companies(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
            work_dir=work_dir,
        )
        json_path = _json_path_from_download(download_path, work_dir=work_dir)
        seen = False
        for index, company in enumerate(_iter_companies(json_path), start=1):
            seen = True
            yield _dlt_company_row(company, line_number=index, run_id=run_id)
        if not seen:
            raise ValueError(
                "PRH all_companies returned no companies; refusing to replace the table"
            )
```

- [ ] **Step 5: Rewrite the download helper (stream to disk, retry client, per-request header)**

Replace `_download_all_companies` (current lines 180-191):

```python
def _download_all_companies(
    *,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
    work_dir: Path,
) -> Path:
    http_session = session or dlt_requests.Session()  # dlt's client retries/backoff by default
    target = work_dir / "all_companies.download"
    with http_session.get(
        f"{base_url}/all_companies",
        headers={"User-Agent": user_agent},  # per-request: never mutates the session
        stream=True,
        timeout=timeout_seconds,
    ) as response:
        response.raise_for_status()
        with target.open("wb") as out:
            for chunk in response.iter_content(chunk_size=1 << 20):
                if chunk:
                    out.write(chunk)
    return target
```

- [ ] **Step 6: Replace `_json_bytes_from_response` with disk-based extraction + streaming iterators**

Remove `_json_bytes_from_response` (current lines 216-225) and add:

```python
def _json_path_from_download(download_path: Path, *, work_dir: Path) -> Path:
    if not is_zipfile(download_path):
        return download_path
    with ZipFile(download_path) as archive:
        json_names = [name for name in archive.namelist() if name.lower().endswith(".json")]
        if not json_names:
            raise ValueError("PRH all_companies zip did not contain a JSON file")
        target = work_dir / "all_companies.json"
        with archive.open(json_names[0]) as member, target.open("wb") as out:
            shutil.copyfileobj(member, out)
        return target


def _iter_companies(json_path: Path) -> Iterator[dict[str, Any]]:
    prefix = _ijson_prefix(json_path)
    with json_path.open("rb") as handle:
        for company in ijson.items(handle, prefix):
            if isinstance(company, dict):
                yield company


def _ijson_prefix(json_path: Path) -> str:
    with json_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1), b""):
            if chunk.isspace():
                continue
            return "item" if chunk == b"[" else "companies.item"
    return "item"
```

Note: `_companies_from_payload` (lines 228-234) is now unused by the resource. Leave it in place — it is still exercised by other call sites/tests; if `uv run pytest` reports it fully unused, remove it in this task and drop its test.

- [ ] **Step 7: Run the full finland_ytj suite**

Run: `uv run pytest tests/test_finland_ytj_assets.py -v`
Expected: PASS — streaming rows, bare-array handling, no session mutation, empty-feed guard, and the existing DuckDB load test all green.

- [ ] **Step 8: Commit**

```bash
git add src/dagster_v3/defs/finland_ytj/assets.py tests/test_finland_ytj_assets.py
git commit -m "feat(finland_ytj): stream bulk download to disk, retry, and guard empty feeds"
```

---

### Task 4: Surface load metadata via a non-empty asset check (R5)

**Files:**
- Modify: `src/dagster_v3/defs/finland_ytj/assets.py` (add asset check; register in `defs`)
- Test: `tests/test_finland_ytj_assets.py`

`@dlt_assets` makes per-materialization custom metadata awkward, so the row-count signal is delivered as an asset check that emits `active_companies` as check metadata (this also makes R2 visible in the UI).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_finland_ytj_assets.py`:

```python
def test_non_empty_check_reports_count(tmp_path: Path) -> None:
    database_path = tmp_path / "finland_ytj.duckdb"
    ytj_assets.run_finland_ytj_dlt_pipeline(
        database_path=database_path,
        run_id="run-1",
        session=_session({"companies": [{"businessId": {"value": "a"}}]}),
    )
    resource = ytj_assets.LocalDuckDBResource(database_path=str(database_path))
    result = ytj_assets.all_companies_non_empty(resource)
    assert result.passed is True
    assert result.metadata["row_count"].value == 1


def test_non_empty_check_and_asset_registered() -> None:
    repository = load_project_defs().get_repository_def()
    check_names = {check.name for check in repository.asset_graph.asset_checks_defs}
    assert "all_companies_non_empty" in check_names
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_finland_ytj_assets.py -k non_empty -v`
Expected: FAIL — `all_companies_non_empty` does not exist.

- [ ] **Step 3: Add the asset check and register it**

In `src/dagster_v3/defs/finland_ytj/assets.py`, add above the `defs = dg.Definitions(...)` block:

```python
@dg.asset_check(asset="finland_ytj_all_companies_duckdb", name="all_companies_non_empty")
def all_companies_non_empty(ytj_duckdb: LocalDuckDBResource) -> dg.AssetCheckResult:
    with ytj_duckdb.connect(read_only=True) as connection:
        row_count = connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{DLT_COMPANIES_TABLE}"
        ).fetchone()[0]
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": int(row_count)},
    )
```

Update the `defs` block to register the check:

```python
defs = dg.Definitions(
    assets=[
        finland_ytj_all_companies_duckdb_asset,
    ],
    asset_checks=[all_companies_non_empty],
    resources={
        "ytj_duckdb": LocalDuckDBResource(),
    },
)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_finland_ytj_assets.py -k non_empty -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_ytj/assets.py tests/test_finland_ytj_assets.py
git commit -m "feat(finland_ytj): add non-empty asset check surfacing row count"
```

---

### Task 5: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run every touched test suite**

Run: `uv run pytest tests/test_finland_ytj_assets.py tests/test_finland_resolved_assets.py tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_parsed_assets.py tests/test_common_resources.py -m "not integration" -v`
Expected: PASS

- [ ] **Step 2: Validate Dagster definitions load and the lineage is intact**

Run: `uv run dg list defs --json | python3 -c "import sys,json; d=json.load(sys.stdin); print([a['asset_key'] for a in d['assets'] if 'finland_ytj' in a['asset_key']])"`
Expected: includes `finland_ytj_all_companies_duckdb` (and the downstream `finland_ytj_resolved_*` keys are unchanged).

Run: `uv run dg check defs`
Expected: no errors.

- [ ] **Step 3: Sanity-check the streaming path against a real-shaped fixture (optional, no network)**

Run: `uv run pytest tests/test_finland_ytj_assets.py::test_dlt_duckdb_asset_loads_all_companies_table -v`
Expected: PASS — confirms the full dlt → DuckDB load works end-to-end through the new streaming resource.

- [ ] **Step 4: Final commit if anything was adjusted during verification**

```bash
git add -A
git commit -m "test(finland_ytj): verify base-logic hardening across suites" || echo "nothing to commit"
```

---

## Self-Review

**Finding coverage:**
- **R1** (whole-dataset-in-memory) → Task 3: temp-file download + `ijson` per-company streaming.
- **R2** (empty payload wipes table) → Task 3: `seen` guard raises `ValueError` before any row is yielded, aborting the `replace`.
- **R3** (no retry) → Task 3: `dlt_requests.Session()` default with built-in retry/backoff.
- **R4** (magic strings) → Task 2: `PRH_NAME_TYPE_PRIMARY` / `PRH_TRADE_REGISTER_STATUS_CEASED`.
- **R5** (no metadata) → Task 4: `all_companies_non_empty` check emits `row_count`.
- **R6** (import-time instantiation) → Task 2: `DEFAULT_DUCKDB_PATH` constant in the decorator.
- **R7** (resource coupling) → Task 1: move to `defs/common/resources.py`, update 6 importers, delete old module.
- **R8** (session mutation) → Task 3: per-request `headers={"User-Agent": ...}`, never writes to the session.

**Placeholder scan:** No `TBD`/"add error handling"/"similar to" — every code step is complete. The one judgement call (whether `_companies_from_payload` becomes fully unused) is flagged explicitly in Task 3 Step 6 with the action to take either way.

**Type consistency:** `_download_all_companies` returns `Path` and is consumed by `_json_path_from_download(...) -> Path` → `_iter_companies(Path) -> Iterator[dict]`; `work_dir` threads consistently; the `HttpSession.get` signature (keyword `headers`/`stream`/`timeout`) matches both `FakeHttpSession.get` and the `dlt_requests.Session().get` call. The `run_finland_ytj_dlt_pipeline` signature is unchanged in this plan (no `pipelines_dir` — that arrives in the SCD2 follow-up).

**Risk to watch during execution:** `ijson.items(handle, "companies.item")` assumes PRH's object-wrapped feed nests companies under the top-level key `companies`. If the real bulk file uses a different wrapper key, `_ijson_prefix` must map it — confirm against one real (or realistically-shaped) download before relying on production data. The bare-array path (`"item"`) is covered by `test_source_handles_bare_json_array_without_zip`.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-18-finland-ytj-base-logic.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
