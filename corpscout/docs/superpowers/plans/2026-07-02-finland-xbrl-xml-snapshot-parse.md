# Finland XBRL XML Snapshot Parse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `data_snapshot_xml_duckdb`, a monthly-partitioned asset that parses XML files downloaded by `data_snapshot_xml` into partition-scoped DuckDB tables.

**Architecture:** The new asset reads `manifest.jsonl` and `_SUCCESS.json` from the existing XML snapshot S3 folder, uses the existing `parse_statement_xml` parser, writes temporary parquet batches, then creates one DuckDB file per partition with `statement_documents` and `facts` tables. The asset is wired into `finland_xbrl_xml_snapshot_job` after `data_snapshot_xml`.

**Tech Stack:** Dagster assets and partitions, existing `ObjectStoreResource`, existing Finland XBRL lxml parser, Polars parquet writes, DuckDB table creation, pytest.

---

## File Structure

- Create `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/data_snapshot_xml_duckdb.py`
  - Owns S3 manifest parsing, partition output paths, temporary parquet writing, DuckDB merge, and the Dagster asset.
- Modify `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/__init__.py`
  - Exports the new asset and helper functions.
  - Registers the new asset in `dg.Definitions`.
- Modify `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/jobs.py`
  - Adds `data_snapshot_xml_duckdb` to `finland_xbrl_xml_snapshot_job`.
- Modify `dagster_v3/tests/test_finland_xbrl_assets.py`
  - Adds regression tests for path helpers, marker behavior, manifest parsing, DuckDB output, parser injection, cleanup, and job selection.
- Modify `dagster_v3/src/dagster_v3/defs/finland_xbrl/README.md`
  - Documents the parsed DuckDB artifact produced from XML snapshot partitions.

---

### Task 1: Add Failing Tests For XML Snapshot DuckDB Path And Job Wiring

**Files:**
- Modify: `dagster_v3/tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Add imports for the new symbols**

Add these imports to the existing `from dagster_v3.defs.finland_xbrl.assets import (...)` block:

```python
    FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH,
    data_snapshot_xml_duckdb,
    xml_snapshot_parse_duckdb_path,
    xml_snapshot_parse_temp_dir,
```

- [ ] **Step 2: Add failing path helper test**

Add this test near the existing XML snapshot helper tests:

```python
def test_xml_snapshot_parse_duckdb_path_helpers() -> None:
    assert str(FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH) == (
        "data/finland_xbrl/duckdb/xml_snapshot_parse"
    )
    assert xml_snapshot_parse_duckdb_path("2023-07-01") == (
        FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH
        / "partition_key=2023-07-01"
        / "data.duckdb"
    )
    assert xml_snapshot_parse_temp_dir("2023-07-01") == (
        Path("data/finland_xbrl/tmp/xml_snapshot_parse")
        / "partition_key=2023-07-01"
    )
```

- [ ] **Step 3: Update failing job selection test**

In `test_finland_xbrl_jobs_and_incremental_schedule_registered`, change:

```python
    assert xml_snapshot == {"data_snapshot_xml"}
```

to:

```python
    assert xml_snapshot == {"data_snapshot_xml", "data_snapshot_xml_duckdb"}
```

- [ ] **Step 4: Add failing asset partition test**

Add:

```python
def test_data_snapshot_xml_duckdb_uses_xml_snapshot_partitions() -> None:
    graph = load_project_defs().get_repository_def().asset_graph
    node = graph.get(AssetKey("data_snapshot_xml_duckdb"))

    assert node.partitions_def is XML_SNAPSHOT_PARTITIONS
```

- [ ] **Step 5: Run tests and confirm RED**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_duckdb_path_helpers tests/test_finland_xbrl_assets.py::test_finland_xbrl_jobs_and_incremental_schedule_registered tests/test_finland_xbrl_assets.py::test_data_snapshot_xml_duckdb_uses_xml_snapshot_partitions -q
```

Expected: FAIL because `FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH`, `data_snapshot_xml_duckdb`, `xml_snapshot_parse_duckdb_path`, and `xml_snapshot_parse_temp_dir` do not exist yet.

---

### Task 2: Add The New Asset File With Path Helpers Only

**Files:**
- Create: `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/data_snapshot_xml_duckdb.py`
- Modify: `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/__init__.py`
- Modify: `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/jobs.py`

- [ ] **Step 1: Create helper-only implementation**

Create `data_snapshot_xml_duckdb.py`:

```python
from pathlib import Path

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
    XML_SNAPSHOT_PARTITIONS,
    data_snapshot_xml,
)

FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH = Path(
    "data/finland_xbrl/duckdb/xml_snapshot_parse"
)
FINLAND_XBRL_XML_SNAPSHOT_PARSE_TEMP_PATH = Path(
    "data/finland_xbrl/tmp/xml_snapshot_parse"
)


def xml_snapshot_parse_duckdb_path(partition_key: str) -> Path:
    return (
        FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH
        / f"partition_key={partition_key}"
        / "data.duckdb"
    )


def xml_snapshot_parse_temp_dir(partition_key: str) -> Path:
    return FINLAND_XBRL_XML_SNAPSHOT_PARSE_TEMP_PATH / f"partition_key={partition_key}"


@dg.asset(
    name="data_snapshot_xml_duckdb",
    group_name="finland_xbrl",
    deps=[data_snapshot_xml],
    partitions_def=XML_SNAPSHOT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"python", "duckdb", "parquet", "xml"},
    description=(
        "Parses monthly historical Finland XBRL XML snapshot files from S3 "
        "into a partition-scoped DuckDB database."
    ),
)
def data_snapshot_xml_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    del object_store
    return dg.MaterializeResult(
        metadata={
            "partition": context.partition_key,
            "duckdb_path": str(xml_snapshot_parse_duckdb_path(context.partition_key)),
        }
    )
```

- [ ] **Step 2: Export symbols and register asset**

In `assets/__init__.py`, import:

```python
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH,
    data_snapshot_xml_duckdb,
    xml_snapshot_parse_duckdb_path,
    xml_snapshot_parse_temp_dir,
)
```

Add these names to `__all__`:

```python
    "FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH",
    "data_snapshot_xml_duckdb",
    "xml_snapshot_parse_duckdb_path",
    "xml_snapshot_parse_temp_dir",
```

Add `data_snapshot_xml_duckdb` to the `assets=[...]` list immediately after `data_snapshot_xml`.

- [ ] **Step 3: Wire job selection**

In `assets/jobs.py`, import the asset module symbol:

```python
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import data_snapshot_xml_duckdb
```

Change the XML snapshot job selection to:

```python
finland_xbrl_xml_snapshot_job = dg.define_asset_job(
    "finland_xbrl_xml_snapshot_job",
    selection=dg.AssetSelection.assets("data_snapshot_xml", "data_snapshot_xml_duckdb"),
    partitions_def=XML_SNAPSHOT_PARTITIONS,
)
```

- [ ] **Step 4: Run tests and confirm GREEN for wiring**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_duckdb_path_helpers tests/test_finland_xbrl_assets.py::test_finland_xbrl_jobs_and_incremental_schedule_registered tests/test_finland_xbrl_assets.py::test_data_snapshot_xml_duckdb_uses_xml_snapshot_partitions -q
```

Expected: PASS.

---

### Task 3: Add Failing Tests For Marker And Manifest Handling

**Files:**
- Modify: `dagster_v3/tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Add imports for helper under test**

Add these imports to the existing asset import block:

```python
    materialize_data_snapshot_xml_duckdb,
    read_xml_snapshot_manifest_rows,
```

- [ ] **Step 2: Add missing success marker test**

Add:

```python
def test_xml_snapshot_parse_requires_success_marker(tmp_path: Path) -> None:
    object_store, _s3_client = _object_store()

    with pytest.raises(FileNotFoundError, match="_SUCCESS.json"):
        materialize_data_snapshot_xml_duckdb(
            partition_key="2023-07-01",
            registered_date_start="2023-07-01",
            registered_date_end="2023-07-31",
            object_store=object_store,
            duckdb_path=tmp_path / "data.duckdb",
            temp_dir=tmp_path / "tmp",
            run_id="run-1",
        )

    assert not (tmp_path / "data.duckdb").exists()
```

- [ ] **Step 3: Add missing manifest test**

Add:

```python
def test_xml_snapshot_parse_requires_manifest(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    s3_client.objects[
        ("source-finland-prh-xbrl", xml_snapshot_success_key("2023-07-01", "2023-07-31"))
    ] = b"{}"

    with pytest.raises(FileNotFoundError, match="manifest.jsonl"):
        materialize_data_snapshot_xml_duckdb(
            partition_key="2023-07-01",
            registered_date_start="2023-07-01",
            registered_date_end="2023-07-31",
            object_store=object_store,
            duckdb_path=tmp_path / "data.duckdb",
            temp_dir=tmp_path / "tmp",
            run_id="run-1",
        )

    assert not (tmp_path / "data.duckdb").exists()
```

- [ ] **Step 4: Add manifest row validation test**

Add:

```python
def test_read_xml_snapshot_manifest_rows_validates_required_fields() -> None:
    object_store, s3_client = _object_store()
    manifest_key = xml_snapshot_manifest_key("2023-07-01", "2023-07-31")
    s3_client.objects[
        ("source-finland-prh-xbrl", manifest_key)
    ] = b'{"business_id":"0100123-2"}\n'

    with pytest.raises(ValueError, match="financial_date"):
        read_xml_snapshot_manifest_rows(
            object_store=object_store,
            manifest_key=manifest_key,
        )
```

- [ ] **Step 5: Run tests and confirm RED**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_requires_success_marker tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_requires_manifest tests/test_finland_xbrl_assets.py::test_read_xml_snapshot_manifest_rows_validates_required_fields -q
```

Expected: FAIL because `materialize_data_snapshot_xml_duckdb` and `read_xml_snapshot_manifest_rows` do not exist yet.

---

### Task 4: Implement Marker And Manifest Handling

**Files:**
- Modify: `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/data_snapshot_xml_duckdb.py`
- Modify: `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/__init__.py`

- [ ] **Step 1: Add manifest constants and reader**

Add to `data_snapshot_xml_duckdb.py`:

```python
import json
from collections.abc import Callable
from typing import Any

from dagster_v3.defs.finland_xbrl.assets.common import XBRL_BUCKET
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
    xml_snapshot_manifest_key,
    xml_snapshot_partition_prefix,
    xml_snapshot_success_key,
)

REQUIRED_XML_SNAPSHOT_MANIFEST_FIELDS = (
    "business_id",
    "financial_date",
    "registration_date",
    "source_url",
    "xml_object_key",
)


def read_xml_snapshot_manifest_rows(
    *,
    object_store: ObjectStoreResource,
    manifest_key: str,
) -> list[dict[str, str]]:
    body = object_store.read_bytes(manifest_key, bucket=XBRL_BUCKET)
    rows: list[dict[str, str]] = []
    for line_number, raw_line in enumerate(body.decode("utf-8").splitlines(), start=1):
        if not raw_line.strip():
            continue
        payload = json.loads(raw_line)
        if not isinstance(payload, dict):
            raise ValueError(f"Manifest line {line_number} is not an object: {manifest_key}")
        missing = [
            field
            for field in REQUIRED_XML_SNAPSHOT_MANIFEST_FIELDS
            if not str(payload.get(field) or "").strip()
        ]
        if missing:
            raise ValueError(
                f"Manifest line {line_number} is missing required fields {missing}: {manifest_key}"
            )
        rows.append(
            {
                field: str(payload[field]).strip()
                for field in REQUIRED_XML_SNAPSHOT_MANIFEST_FIELDS
            }
        )
    return rows
```

- [ ] **Step 2: Add a minimal materializer that enforces marker and manifest**

Add:

```python
def materialize_data_snapshot_xml_duckdb(
    *,
    partition_key: str,
    registered_date_start: str,
    registered_date_end: str,
    object_store: ObjectStoreResource,
    duckdb_path: Path,
    temp_dir: Path,
    run_id: str,
    log_info: Callable[[str], None] | None = None,
) -> dg.MaterializeResult:
    del temp_dir, run_id
    prefix = xml_snapshot_partition_prefix(registered_date_start, registered_date_end)
    manifest_key = xml_snapshot_manifest_key(registered_date_start, registered_date_end)
    success_key = xml_snapshot_success_key(registered_date_start, registered_date_end)
    if not object_store.exists(success_key, bucket=XBRL_BUCKET):
        raise FileNotFoundError(
            f"Finland XBRL XML snapshot success marker is missing: bucket={XBRL_BUCKET} key={success_key}"
        )
    if not object_store.exists(manifest_key, bucket=XBRL_BUCKET):
        raise FileNotFoundError(
            f"Finland XBRL XML snapshot manifest is missing: bucket={XBRL_BUCKET} key={manifest_key}"
        )
    rows = read_xml_snapshot_manifest_rows(
        object_store=object_store,
        manifest_key=manifest_key,
    )
    if log_info is not None:
        log_info(
            "Finland XBRL XML snapshot parse manifest loaded: "
            f"partition={partition_key} rows={len(rows)} manifest_key={manifest_key}"
        )
    return dg.MaterializeResult(
        metadata={
            "partition": partition_key,
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "s3_bucket": XBRL_BUCKET,
            "s3_prefix": prefix,
            "manifest_key": manifest_key,
            "success_key": success_key,
            "duckdb_path": str(duckdb_path),
            "documents_in_manifest": len(rows),
        }
    )
```

- [ ] **Step 3: Update asset function to call materializer**

Replace the stub asset body with:

```python
    window = context.partition_time_window
    start = window.start.date().isoformat()
    end = (window.end.date() - timedelta(days=1)).isoformat()
    return materialize_data_snapshot_xml_duckdb(
        partition_key=context.partition_key,
        registered_date_start=start,
        registered_date_end=end,
        object_store=object_store,
        duckdb_path=xml_snapshot_parse_duckdb_path(context.partition_key),
        temp_dir=xml_snapshot_parse_temp_dir(context.partition_key),
        run_id=context.run.run_id,
        log_info=context.log.info,
    )
```

Also add:

```python
from datetime import timedelta
```

- [ ] **Step 4: Export new helpers**

In `assets/__init__.py`, import and add to `__all__`:

```python
    materialize_data_snapshot_xml_duckdb,
    read_xml_snapshot_manifest_rows,
```

- [ ] **Step 5: Run tests and confirm GREEN**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_requires_success_marker tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_requires_manifest tests/test_finland_xbrl_assets.py::test_read_xml_snapshot_manifest_rows_validates_required_fields -q
```

Expected: PASS.

---

### Task 5: Add Failing Tests For Parsing, DuckDB Creation, And Cleanup

**Files:**
- Modify: `dagster_v3/tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Add imports used by tests**

At the top of `tests/test_finland_xbrl_assets.py`, ensure these imports already exist:

```python
import json
import duckdb
```

- [ ] **Step 2: Add fake parser class**

Add near existing fake parser helpers:

```python
class RecordingStatementParser:
    def __init__(self, *, fail_business_id: str | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fail_business_id = fail_business_id

    def __call__(self, **kwargs) -> ParsedStatement:
        self.calls.append(kwargs)
        if kwargs["business_id"] == self.fail_business_id:
            raise ValueError("bad xml")
        statement_key = f"statement-{kwargs['business_id']}"
        return ParsedStatement(
            statement_key=statement_key,
            rows_by_table={
                STATEMENT_DOCUMENTS_TABLE: [
                    {
                        **_statement_document_row(statement_key),
                        "business_id": kwargs["business_id"],
                        "financial_date": kwargs["financial_date"],
                        "registration_date": kwargs["registration_date"],
                        "source_url": kwargs["source_url"],
                        "xml_object_key": kwargs["xml_object_key"],
                        "source_run_id": kwargs["source_run_id"],
                    }
                ],
                FACTS_TABLE: [
                    {
                        **_fact_row(statement_key, fact_ordinal=1),
                        "business_id": kwargs["business_id"],
                        "financial_date": kwargs["financial_date"],
                    }
                ],
            },
            warnings=[],
        )
```

- [ ] **Step 3: Add helper for writing snapshot manifest fixtures**

Add:

```python
def _write_xml_snapshot_manifest_fixture(
    s3_client: FakeS3Client,
    *,
    start: str,
    end: str,
    rows: list[dict[str, Any]],
) -> None:
    s3_client.objects[("source-finland-prh-xbrl", xml_snapshot_success_key(start, end))] = b"{}"
    s3_client.objects[("source-finland-prh-xbrl", xml_snapshot_manifest_key(start, end))] = (
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows).encode("utf-8")
    )
```

- [ ] **Step 4: Add empty manifest DuckDB test**

Add:

```python
def test_xml_snapshot_parse_empty_manifest_creates_empty_duckdb(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    _write_xml_snapshot_manifest_fixture(
        s3_client,
        start="2023-07-01",
        end="2023-07-31",
        rows=[],
    )

    result = materialize_data_snapshot_xml_duckdb(
        partition_key="2023-07-01",
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
        object_store=object_store,
        duckdb_path=tmp_path / "data.duckdb",
        temp_dir=tmp_path / "tmp",
        run_id="run-1",
    )

    with duckdb.connect(str(tmp_path / "data.duckdb"), read_only=True) as connection:
        assert connection.execute("select count(*) from statement_documents").fetchone()[0] == 0
        assert connection.execute("select count(*) from facts").fetchone()[0] == 0
    assert result.metadata["documents_in_manifest"] == 0
    assert result.metadata["statement_documents_row_count"] == 0
    assert result.metadata["facts_row_count"] == 0
```

- [ ] **Step 5: Add valid XML parse and cleanup test**

Add:

```python
def test_xml_snapshot_parse_writes_partition_duckdb_and_removes_temp_parquet(
    tmp_path: Path,
) -> None:
    object_store, s3_client = _object_store()
    xml_key = xml_snapshot_document_key("2023-07-01", "2023-07-31", "0100123-2", "2022-12-31")
    s3_client.objects[("source-finland-prh-xbrl", xml_key)] = b"<xbrl />"
    _write_xml_snapshot_manifest_fixture(
        s3_client,
        start="2023-07-01",
        end="2023-07-31",
        rows=[
            {
                "business_id": "0100123-2",
                "financial_date": "2022-12-31",
                "registration_date": "2023-07-02",
                "source_url": "https://example.test/financial",
                "xml_object_key": xml_key,
            }
        ],
    )
    parser = RecordingStatementParser()
    temp_dir = tmp_path / "tmp"

    result = materialize_data_snapshot_xml_duckdb(
        partition_key="2023-07-01",
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
        object_store=object_store,
        duckdb_path=tmp_path / "data.duckdb",
        temp_dir=temp_dir,
        run_id="run-1",
        parser=parser,
    )

    assert len(parser.calls) == 1
    assert parser.calls[0]["business_id"] == "0100123-2"
    assert parser.calls[0]["body"] == b"<xbrl />"
    with duckdb.connect(str(tmp_path / "data.duckdb"), read_only=True) as connection:
        assert connection.execute("select business_id from statement_documents").fetchall() == [
            ("0100123-2",)
        ]
        assert connection.execute("select business_id from facts").fetchall() == [
            ("0100123-2",)
        ]
    assert result.metadata["documents_parsed_this_run"] == 1
    assert result.metadata["documents_failed_this_run"] == 0
    assert result.metadata["statement_documents_row_count"] == 1
    assert result.metadata["facts_row_count"] == 1
    assert result.metadata["temporary_directory_removed"] is True
    assert not temp_dir.exists()
```

- [ ] **Step 6: Add parser failure continuation test**

Add:

```python
def test_xml_snapshot_parse_records_parser_failures_and_continues(tmp_path: Path) -> None:
    object_store, s3_client = _object_store()
    good_key = xml_snapshot_document_key("2023-07-01", "2023-07-31", "0100123-2", "2022-12-31")
    bad_key = xml_snapshot_document_key("2023-07-01", "2023-07-31", "0202020-2", "2022-12-31")
    s3_client.objects[("source-finland-prh-xbrl", good_key)] = b"<xbrl />"
    s3_client.objects[("source-finland-prh-xbrl", bad_key)] = b"<xbrl />"
    _write_xml_snapshot_manifest_fixture(
        s3_client,
        start="2023-07-01",
        end="2023-07-31",
        rows=[
            {
                "business_id": "0202020-2",
                "financial_date": "2022-12-31",
                "registration_date": "2023-07-02",
                "source_url": "https://example.test/bad",
                "xml_object_key": bad_key,
            },
            {
                "business_id": "0100123-2",
                "financial_date": "2022-12-31",
                "registration_date": "2023-07-02",
                "source_url": "https://example.test/good",
                "xml_object_key": good_key,
            },
        ],
    )

    result = materialize_data_snapshot_xml_duckdb(
        partition_key="2023-07-01",
        registered_date_start="2023-07-01",
        registered_date_end="2023-07-31",
        object_store=object_store,
        duckdb_path=tmp_path / "data.duckdb",
        temp_dir=tmp_path / "tmp",
        run_id="run-1",
        parser=RecordingStatementParser(fail_business_id="0202020-2"),
    )

    with duckdb.connect(str(tmp_path / "data.duckdb"), read_only=True) as connection:
        assert connection.execute("select business_id from statement_documents").fetchall() == [
            ("0100123-2",)
        ]
    assert result.metadata["documents_in_manifest"] == 2
    assert result.metadata["documents_parsed_this_run"] == 1
    assert result.metadata["documents_failed_this_run"] == 1
```

- [ ] **Step 7: Run tests and confirm RED**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_empty_manifest_creates_empty_duckdb tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_writes_partition_duckdb_and_removes_temp_parquet tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_records_parser_failures_and_continues -q
```

Expected: FAIL because the materializer does not create DuckDB or accept `parser`.

---

### Task 6: Implement Parsing, Temporary Parquet, DuckDB Merge, And Cleanup

**Files:**
- Modify: `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/data_snapshot_xml_duckdb.py`

- [ ] **Step 1: Add imports**

Add:

```python
import shutil
from datetime import UTC, datetime

import duckdb
import polars as pl

from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.assets.parse import StatementParser
from dagster_v3.defs.finland_xbrl.parser import parse_statement_xml
```

- [ ] **Step 2: Add row normalization helpers**

Add:

```python
def _statement_document_row(row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.STATEMENT_DOCUMENTS_COLUMNS}


def _fact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {column: row.get(column) for column in tables.FACTS_COLUMNS}


def _write_partition_parquet(
    *,
    path: Path,
    rows: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, pl.DataType],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [{column: row.get(column) for column in columns} for row in rows],
        schema=schema,
    ).write_parquet(path)
```

- [ ] **Step 3: Add DuckDB table creation helper**

Add:

```python
def _create_duckdb_table_from_parquet(
    *,
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    parquet_dir: Path,
    columns: list[str],
    schema: dict[str, pl.DataType],
) -> int:
    parquet_files = sorted(parquet_dir.glob("*.parquet"))
    if parquet_files:
        files = [str(path) for path in parquet_files]
        connection.execute(
            f"create or replace table {table_name} as select * from read_parquet(?)",
            [files],
        )
    else:
        empty_path = parquet_dir / "_empty.parquet"
        _write_partition_parquet(
            path=empty_path,
            rows=[],
            columns=columns,
            schema=schema,
        )
        connection.execute(
            f"create or replace table {table_name} as select * from read_parquet(?)",
            [[str(empty_path)]],
        )
        empty_path.unlink()
    return connection.execute(f"select count(*) from {table_name}").fetchone()[0]
```

- [ ] **Step 4: Replace materializer with full implementation**

Update `materialize_data_snapshot_xml_duckdb` signature:

```python
    parser: StatementParser = parse_statement_xml,
```

Replace the body after manifest reading with:

```python
    parsed_at = datetime.now(UTC)
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    statement_dir = temp_dir / "statement_documents"
    facts_dir = temp_dir / "facts"
    statement_parquet_count = 0
    facts_parquet_count = 0
    documents_parsed = 0
    failed_rows: list[dict[str, str]] = []

    for index, row in enumerate(rows, start=1):
        try:
            body = object_store.read_bytes(row["xml_object_key"], bucket=XBRL_BUCKET)
            parsed = parser(
                business_id=row["business_id"],
                financial_date=row["financial_date"],
                registration_date=row["registration_date"],
                source_url=row["source_url"],
                xml_object_key=row["xml_object_key"],
                source_run_id=run_id,
                body=body,
                parsed_at=parsed_at,
            )
        except Exception as exc:  # noqa: BLE001 - record bad XML and continue
            failed_rows.append(
                {
                    "business_id": row["business_id"],
                    "financial_date": row["financial_date"],
                    "xml_object_key": row["xml_object_key"],
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            if log_info is not None:
                log_info(
                    "Finland XBRL XML snapshot parse failed: "
                    f"partition={partition_key} business_id={row['business_id']} "
                    f"financial_date={row['financial_date']} xml_key={row['xml_object_key']} "
                    f"error={type(exc).__name__}: {exc}"
                )
            continue

        statement_rows = [
            _statement_document_row(statement)
            for statement in parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE]
        ]
        fact_rows = [
            _fact_row(fact)
            for fact in parsed.rows_by_table[tables.FACTS_TABLE]
        ]
        if statement_rows:
            statement_parquet_count += 1
            _write_partition_parquet(
                path=statement_dir / f"part-{index:06d}.parquet",
                rows=statement_rows,
                columns=tables.STATEMENT_DOCUMENTS_COLUMNS,
                schema=tables.STATEMENT_DOCUMENTS_POLARS_SCHEMA,
            )
        if fact_rows:
            facts_parquet_count += 1
            _write_partition_parquet(
                path=facts_dir / f"part-{index:06d}.parquet",
                rows=fact_rows,
                columns=tables.FACTS_COLUMNS,
                schema=tables.FACTS_POLARS_SCHEMA,
            )
        documents_parsed += 1
        if log_info is not None and (
            index == 1 or index == len(rows) or index % 25 == 0
        ):
            log_info(
                "Finland XBRL XML snapshot parse progress: "
                f"partition={partition_key} {index}/{len(rows)} "
                f"business_id={row['business_id']} financial_date={row['financial_date']}"
            )

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(duckdb_path)) as connection:
        statement_count = _create_duckdb_table_from_parquet(
            connection=connection,
            table_name="statement_documents",
            parquet_dir=statement_dir,
            columns=tables.STATEMENT_DOCUMENTS_COLUMNS,
            schema=tables.STATEMENT_DOCUMENTS_POLARS_SCHEMA,
        )
        facts_count = _create_duckdb_table_from_parquet(
            connection=connection,
            table_name="facts",
            parquet_dir=facts_dir,
            columns=tables.FACTS_COLUMNS,
            schema=tables.FACTS_POLARS_SCHEMA,
        )

    temporary_directory_removed = False
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
        temporary_directory_removed = True
```

Update returned metadata to include:

```python
            "documents_parsed_this_run": documents_parsed,
            "documents_failed_this_run": len(failed_rows),
            "statement_documents_row_count": statement_count,
            "facts_row_count": facts_count,
            "temporary_statement_parquet_count": statement_parquet_count,
            "temporary_facts_parquet_count": facts_parquet_count,
            "temporary_directory_removed": temporary_directory_removed,
```

- [ ] **Step 5: Run tests and confirm GREEN**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_empty_manifest_creates_empty_duckdb tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_writes_partition_duckdb_and_removes_temp_parquet tests/test_finland_xbrl_assets.py::test_xml_snapshot_parse_records_parser_failures_and_continues -q
```

Expected: PASS.

---

### Task 7: Document The Parsed DuckDB Artifact

**Files:**
- Modify: `dagster_v3/src/dagster_v3/defs/finland_xbrl/README.md`

- [ ] **Step 1: Add README section**

Add a section after the XML snapshot layout section:

```markdown
### Historical XML Snapshot Parse

`data_snapshot_xml_duckdb` runs on the same monthly partitions as `data_snapshot_xml`.
It requires the XML snapshot `_SUCCESS.json` marker and reads that partition's
`manifest.jsonl`. Each listed XML object is parsed with the shared lxml parser.

The asset writes a local partition DuckDB:

```text
data/finland_xbrl/duckdb/xml_snapshot_parse/
  partition_key=<YYYY-MM-01>/
    data.duckdb
```

The DuckDB contains:

- `statement_documents`
- `facts`

Temporary parquet files are written while parsing and removed after the DuckDB
tables are created.
```

- [ ] **Step 2: Run markdown-safe diff check**

Run:

```bash
cd companycollect/corpscout
git diff --check
```

Expected: no output.

---

### Task 8: Run Full Verification

**Files:**
- All modified files from previous tasks.

- [ ] **Step 1: Run targeted test file**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py -q
```

Expected: all tests pass.

- [ ] **Step 2: Run ruff**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run ruff check src/dagster_v3/defs/finland_xbrl tests/test_finland_xbrl_assets.py
```

Expected: `All checks passed!`

- [ ] **Step 3: Run Dagster definitions check**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected:

```text
All component YAML validated successfully.
All definitions loaded successfully.
```

- [ ] **Step 4: Run whitespace check**

Run:

```bash
cd companycollect/corpscout
git diff --check
```

Expected: no output.

- [ ] **Step 5: Review final diff**

Run:

```bash
cd companycollect/corpscout
git diff --stat
git diff -- dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/data_snapshot_xml_duckdb.py
```

Expected: diff only includes the intended Finland XBRL asset, tests, README, and plan/design docs. Unrelated pre-existing untracked files must not be modified.

---

## Self-Review

- Spec coverage: The plan covers the new monthly asset, S3 marker/manifest requirements, parser reuse, temporary parquet, DuckDB merge, cleanup, job wiring, README, tests, and verification.
- Placeholder scan: No `TBD`, `TODO`, "similar to", or unspecified "add tests" instructions remain.
- Type consistency: Helper names match the design and are exported through `assets/__init__.py`. The parser argument uses the existing `StatementParser` type and defaults to `parse_statement_xml`.
