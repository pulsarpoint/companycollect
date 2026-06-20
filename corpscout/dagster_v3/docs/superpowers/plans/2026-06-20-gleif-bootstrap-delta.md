# GLEIF Bootstrap Delta Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a GLEIF source package to `dagster_v3` that supports one-time full Golden Copy bootstrap downloads, daily delta downloads, stateful DuckDB normalization, and ClickHouse publication into `corpscout.gleif_*` tables.

**Architecture:** Full and delta downloads are separate raw S3/RustFS assets because they persist different source artifacts. DuckDB and ClickHouse are single current-state assets because full and delta both update the same current GLEIF reference dataset. Raw file manifests and current-state metadata stay in object storage, not ClickHouse.

**Tech Stack:** Dagster assets/jobs/schedules, `requests`, `zipfile`, `ijson`, DuckDB, ClickHouse, existing `ObjectStoreResource`, existing `replace_duckdb_tables_in_clickhouse`, pytest.

---

## File Structure

- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000023_corpscout_gleif_reference_data.up.sql`
  - Creates `corpscout.gleif_*` tables only.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000023_corpscout_gleif_reference_data.down.sql`
  - Drops `corpscout.gleif_*` tables.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/__init__.py`
  - Empty package marker; `load_from_defs_folder` discovers definitions.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/tables.py`
  - Table names and ordered ClickHouse/DuckDB column tuples.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`
  - GLEIF endpoint constants, config classes, object-key helpers, state helpers, HTTP download client, manifest builders.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/parser.py`
  - Streaming ZIP/JSON parsing into normalized Python row dictionaries.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py`
  - Creates/replaces DuckDB state in full mode and applies delta changes in delta mode.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`
  - Dagster assets, jobs, and schedule.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_tables.py`
  - Table constant and migration-registration tests.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_source.py`
  - URL/key/state/manifest/download-helper tests.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_parser.py`
  - Small ZIP/JSON fixture parser tests.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_duckdb_state.py`
  - Full replace and delta upsert/delete tests.
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_assets.py`
  - Asset/job/schedule registration and raw asset materialization tests.
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_clickhouse_migrations.py`
  - Add migration `000023_corpscout_gleif_reference_data` to `EXPECTED_MIGRATIONS`.

## Naming Decisions

- Raw full asset: `gleif_full_raw_reference_files`
- Raw delta asset: `gleif_delta_raw_reference_files`
- Current DuckDB state asset: `gleif_reference_duckdb_state`
- ClickHouse export asset: `gleif_reference_clickhouse`
- Raw retention asset: `gleif_raw_retention`
- Manual bootstrap job: `gleif_reference_bootstrap_job`
- Daily delta job: `gleif_reference_delta_job`
- Daily delta schedule: `gleif_reference_delta_daily`
- Object-store bucket: `source-gleif-reference-data`
- S3 current state key: `gleif/state/current.json`
- DuckDB path: `data/gleif.duckdb`
- DuckDB schema: `gleif`

## Task 1: ClickHouse Schema and Table Constants

**Files:**
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000023_corpscout_gleif_reference_data.up.sql`
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse/migrations/000023_corpscout_gleif_reference_data.down.sql`
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/__init__.py`
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/tables.py`
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_tables.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_clickhouse_migrations.py`

- [ ] **Step 1: Write failing table and migration tests**

Create `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_tables.py`:

```python
from pathlib import Path

from dagster_v3.defs.gleif import tables


MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"


def test_gleif_table_names_are_current_reference_tables() -> None:
    assert tables.GLEIF_TABLES == (
        "gleif_lei_records",
        "gleif_lei_names",
        "gleif_lei_addresses",
        "gleif_lei_identifiers",
        "gleif_lei_relationships",
        "gleif_lei_relationship_periods",
        "gleif_lei_reporting_exceptions",
        "gleif_lei_issuers",
        "gleif_code_list_entries",
    )


def test_gleif_clickhouse_column_contracts_are_defined_for_all_tables() -> None:
    assert set(tables.GLEIF_TABLE_COLUMNS) == set(tables.GLEIF_TABLES)
    for table_name, columns in tables.GLEIF_TABLE_COLUMNS.items():
        assert columns
        assert len(columns) == len(set(columns)), table_name
        assert "source_run_id" in columns
        assert "resolved_at" in columns


def test_gleif_migration_does_not_create_raw_manifest_table() -> None:
    up_sql = (MIGRATIONS_DIR / "000023_corpscout_gleif_reference_data.up.sql").read_text()
    assert "CREATE DATABASE IF NOT EXISTS corpscout" in up_sql
    assert "corpscout.gleif_raw_file_manifest" not in up_sql
    for table_name in tables.GLEIF_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in up_sql
```

Modify `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_clickhouse_migrations.py`:

```python
EXPECTED_MIGRATIONS = (
    # keep existing entries
    "000022_corpscout_norway_finland_drop_provenance_columns",
    "000023_corpscout_gleif_reference_data",
)
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_tables.py tests/test_clickhouse_migrations.py -q
```

Expected: FAIL because `dagster_v3.defs.gleif` and migration `000023` do not exist.

- [ ] **Step 3: Create table constants**

Create empty `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/__init__.py`.

Create `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/tables.py` with constants for:

```python
GLEIF_LEI_RECORDS_TABLE = "gleif_lei_records"
GLEIF_LEI_NAMES_TABLE = "gleif_lei_names"
GLEIF_LEI_ADDRESSES_TABLE = "gleif_lei_addresses"
GLEIF_LEI_IDENTIFIERS_TABLE = "gleif_lei_identifiers"
GLEIF_LEI_RELATIONSHIPS_TABLE = "gleif_lei_relationships"
GLEIF_LEI_RELATIONSHIP_PERIODS_TABLE = "gleif_lei_relationship_periods"
GLEIF_LEI_REPORTING_EXCEPTIONS_TABLE = "gleif_lei_reporting_exceptions"
GLEIF_LEI_ISSUERS_TABLE = "gleif_lei_issuers"
GLEIF_CODE_LIST_ENTRIES_TABLE = "gleif_code_list_entries"

GLEIF_TABLES = (
    GLEIF_LEI_RECORDS_TABLE,
    GLEIF_LEI_NAMES_TABLE,
    GLEIF_LEI_ADDRESSES_TABLE,
    GLEIF_LEI_IDENTIFIERS_TABLE,
    GLEIF_LEI_RELATIONSHIPS_TABLE,
    GLEIF_LEI_RELATIONSHIP_PERIODS_TABLE,
    GLEIF_LEI_REPORTING_EXCEPTIONS_TABLE,
    GLEIF_LEI_ISSUERS_TABLE,
    GLEIF_CODE_LIST_ENTRIES_TABLE,
)
```

Add `GLEIF_TABLE_COLUMNS` with ordered column tuples exactly matching the DDL in the proposal document, excluding `gleif_raw_file_manifest`.

- [ ] **Step 4: Create migrations**

Create `000023_corpscout_gleif_reference_data.up.sql` from the DDL in:

`/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/docs/superpowers/analysis/2026-06-20-gleif-data-clickhouse-dagster-proposal.md`

Use only these tables:

```text
gleif_lei_records
gleif_lei_names
gleif_lei_addresses
gleif_lei_identifiers
gleif_lei_relationships
gleif_lei_relationship_periods
gleif_lei_reporting_exceptions
gleif_lei_issuers
gleif_code_list_entries
```

Create `000023_corpscout_gleif_reference_data.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.gleif_code_list_entries;
DROP TABLE IF EXISTS corpscout.gleif_lei_issuers;
DROP TABLE IF EXISTS corpscout.gleif_lei_reporting_exceptions;
DROP TABLE IF EXISTS corpscout.gleif_lei_relationship_periods;
DROP TABLE IF EXISTS corpscout.gleif_lei_relationships;
DROP TABLE IF EXISTS corpscout.gleif_lei_identifiers;
DROP TABLE IF EXISTS corpscout.gleif_lei_addresses;
DROP TABLE IF EXISTS corpscout.gleif_lei_names;
DROP TABLE IF EXISTS corpscout.gleif_lei_records;
```

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_tables.py tests/test_clickhouse_migrations.py -q
```

Expected: PASS.

## Task 2: GLEIF Source Helpers and Object-Store State

**Files:**
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_source.py`

- [ ] **Step 1: Write failing source-helper tests**

Create `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_source.py` with tests for URLs, keys, state, and manifest payloads:

```python
from datetime import UTC, datetime

from dagster_v3.defs.gleif import source


def test_golden_copy_url_supports_full_and_delta() -> None:
    assert source.golden_copy_url(file_kind="lei2", file_format="json", delta=None) == (
        "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.json"
    )
    assert source.golden_copy_url(
        file_kind="repex",
        file_format="json",
        delta="LastDay",
    ) == (
        "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/"
        "repex/latest.json?delta=LastDay"
    )


def test_raw_object_key_includes_load_mode_publish_date_and_run_id() -> None:
    key = source.raw_file_object_key(
        load_mode="delta",
        publish_date="2026-06-21T16:00:00+00:00",
        run_id="run-1",
        file_kind="lei_records",
        delta="LastDay",
        extension="json.zip",
    )

    assert key == (
        "gleif/raw/load_mode=delta/delta=LastDay/"
        "publish_date=2026-06-21T16-00-00Z/run_id=run-1/"
        "file_kind=lei_records/source.json.zip"
    )


def test_state_object_key_is_stable() -> None:
    assert source.GLEIF_STATE_OBJECT_KEY == "gleif/state/current.json"


def test_manifest_payload_has_operational_metadata_only() -> None:
    manifest = source.build_manifest(
        load_mode="full",
        delta=None,
        publish_date="2026-06-20T16:00:00+00:00",
        run_id="run-1",
        pulled_at=datetime(2026, 6, 20, 17, 0, tzinfo=UTC),
        files=[
            source.DownloadedFile(
                file_kind="lei_records",
                source_url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.json",
                s3_key="gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/run_id=run-1/file_kind=lei_records/source.json.zip",
                size_bytes=123,
                sha256="a" * 64,
                etag='"etag"',
                last_modified="Sat, 20 Jun 2026 17:29:16 GMT",
            )
        ],
    )

    assert manifest["source"] == "gleif"
    assert manifest["load_mode"] == "full"
    assert manifest["delta"] is None
    assert manifest["files"][0]["file_kind"] == "lei_records"
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_source.py -q
```

Expected: FAIL because `source.py` does not exist.

- [ ] **Step 3: Implement `source.py` helpers**

Create:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlencode

import dagster as dg
import requests
from pydantic import field_validator

GLEIF_RAW_BUCKET = "source-gleif-reference-data"
GLEIF_STATE_OBJECT_KEY = "gleif/state/current.json"
GLEIF_GOLDEN_COPY_BASE_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes"
GLEIF_FILE_KIND_TO_GOLDEN_COPY_KIND = {
    "lei_records": "lei2",
    "relationships": "rr",
    "reporting_exceptions": "repex",
}
GLEIF_GOLDEN_COPY_FILE_KINDS = ("lei_records", "relationships", "reporting_exceptions")


class GleifRawDownloadConfig(dg.Config):
    file_format: str = "json"
    request_timeout_seconds: int = 300

    @field_validator("file_format")
    @classmethod
    def validate_file_format(cls, value: str) -> str:
        if value not in {"json", "csv", "xml"}:
            raise ValueError("file_format must be json, csv, or xml")
        return value

    @field_validator("request_timeout_seconds")
    @classmethod
    def validate_timeout(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("request_timeout_seconds must be greater than zero")
        return value


@dataclass(frozen=True)
class DownloadedFile:
    file_kind: str
    source_url: str
    s3_key: str
    size_bytes: int
    sha256: str
    etag: str | None
    last_modified: str | None
```

Implement:

```python
def golden_copy_url(*, file_kind: str, file_format: str, delta: str | None) -> str:
    path = f"{GLEIF_GOLDEN_COPY_BASE_URL}/{file_kind}/latest.{file_format}"
    if delta is None:
        return path
    return f"{path}?{urlencode({'delta': delta})}"


def normalize_publish_date_for_key(publish_date: str) -> str:
    normalized = publish_date.replace("+00:00", "Z")
    return normalized.replace(":", "-")


def raw_file_object_key(
    *,
    load_mode: Literal["full", "delta", "mapping_refresh"],
    publish_date: str,
    run_id: str,
    file_kind: str,
    delta: str | None,
    extension: str,
) -> str:
    publish_key = normalize_publish_date_for_key(publish_date)
    parts = ["gleif/raw", f"load_mode={load_mode}"]
    if delta is not None:
        parts.append(f"delta={delta}")
    parts.extend(
        [
            f"publish_date={publish_key}",
            f"run_id={run_id}",
            f"file_kind={file_kind}",
            f"source.{extension}",
        ]
    )
    return "/".join(parts)
```

Add manifest/state helpers:

```python
def build_manifest(
    *,
    load_mode: str,
    delta: str | None,
    publish_date: str,
    run_id: str,
    pulled_at: datetime,
    files: list[DownloadedFile],
) -> dict[str, Any]:
    return {
        "source": "gleif",
        "load_mode": load_mode,
        "delta": delta,
        "publish_date": publish_date,
        "run_id": run_id,
        "pulled_at": pulled_at.isoformat(),
        "files": [
            {
                "file_kind": item.file_kind,
                "source_url": item.source_url,
                "s3_key": item.s3_key,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "etag": item.etag,
                "last_modified": item.last_modified,
            }
            for item in files
        ],
    }
```

Implement streaming download helper that writes bytes to `ObjectStoreResource.write_bytes()` after writing to a temporary file and hashing the bytes. Keep the HTTP client concrete: use `requests.Session`; tests can inject a fake session with `get()`.

- [ ] **Step 4: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_source.py -q
```

Expected: PASS.

## Task 3: Raw Full and Delta Assets

**Files:**
- Create/modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_assets.py`

- [ ] **Step 1: Write failing asset registration tests**

Create `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_assets.py`:

```python
from dagster import AssetKey

from dagster_v3.definitions import defs as load_project_defs


def test_gleif_assets_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_names = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}

    assert "gleif_full_raw_reference_files" in asset_names
    assert "gleif_delta_raw_reference_files" in asset_names
    assert "gleif_reference_duckdb_state" in asset_names
    assert "gleif_reference_clickhouse" in asset_names
    assert "gleif_raw_retention" in asset_names


def test_gleif_raw_assets_have_no_upstream_dependencies() -> None:
    repository = load_project_defs().get_repository_def()
    graph = repository.asset_graph

    assert graph.get(AssetKey(["gleif_full_raw_reference_files"])).parent_keys == set()
    assert graph.get(AssetKey(["gleif_delta_raw_reference_files"])).parent_keys == set()


def test_gleif_jobs_and_delta_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    schedule = next(
        item for item in repository.schedule_defs if item.name == "gleif_reference_delta_daily"
    )

    assert "gleif_reference_bootstrap_job" in set(repository.job_names)
    assert "gleif_reference_delta_job" in set(repository.job_names)
    assert schedule.job_name == "gleif_reference_delta_job"
    assert schedule.cron_schedule == "30 20 * * *"
    assert schedule.execution_timezone == "UTC"
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_assets.py -q
```

Expected: FAIL because `assets.py` does not exist.

- [ ] **Step 3: Implement raw asset functions**

Create `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.gleif import tables
from dagster_v3.defs.gleif.source import (
    GLEIF_RAW_BUCKET,
    GleifRawDownloadConfig,
    download_golden_copy_files,
)

GROUP_NAME = "gleif"
GLEIF_DUCKDB_SCHEMA = "gleif"
GLEIF_DUCKDB_PATH = Path("data/gleif.duckdb")
GLEIF_DUCKDB_POOL = "gleif_duckdb"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gleif"},
    description="Downloads full GLEIF Golden Copy ZIP files into object storage for bootstrap/recovery.",
)
def gleif_full_raw_reference_files(
    context: dg.AssetExecutionContext,
    config: GleifRawDownloadConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return download_golden_copy_files(
        context=context,
        object_store=object_store,
        config=config,
        load_mode="full",
        delta=None,
        run_id=context.run_id,
        pulled_at=datetime.now(UTC),
    )


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gleif"},
    description="Downloads LastDay GLEIF Golden Copy delta ZIP files into object storage.",
)
def gleif_delta_raw_reference_files(
    context: dg.AssetExecutionContext,
    config: GleifRawDownloadConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return download_golden_copy_files(
        context=context,
        object_store=object_store,
        config=config,
        load_mode="delta",
        delta="LastDay",
        run_id=context.run_id,
        pulled_at=datetime.now(UTC),
    )
```

Also add downstream asset definitions in this task so registration tests pass. Put imports for parser/state helpers inside asset functions so the module loads before those helpers are implemented:

```python
@dg.asset(
    deps=[dg.AssetKey("gleif_full_raw_reference_files"), dg.AssetKey("gleif_delta_raw_reference_files")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "gleif"},
    pool=GLEIF_DUCKDB_POOL,
)
def gleif_reference_duckdb_state() -> dg.MaterializeResult:
    from dagster_v3.defs.gleif.duckdb_state import refresh_gleif_duckdb_state

    return refresh_gleif_duckdb_state(GLEIF_DUCKDB_PATH)
```

Use broad `deps` on both raw assets for the current-state asset. Full and delta jobs select only the matching raw asset plus the shared current-state asset; the asset function reads the manifest for the active run to determine full versus delta mode.

- [ ] **Step 4: Define jobs and schedule**

Add at bottom of `assets.py`:

```python
gleif_reference_bootstrap_job = dg.define_asset_job(
    name="gleif_reference_bootstrap_job",
    selection=[
        "gleif_full_raw_reference_files",
        "gleif_reference_duckdb_state",
        "gleif_reference_clickhouse",
        "gleif_raw_retention",
    ],
)

gleif_reference_delta_job = dg.define_asset_job(
    name="gleif_reference_delta_job",
    selection=[
        "gleif_delta_raw_reference_files",
        "gleif_reference_duckdb_state",
        "gleif_reference_clickhouse",
        "gleif_raw_retention",
    ],
)

gleif_reference_delta_daily = dg.ScheduleDefinition(
    name="gleif_reference_delta_daily",
    job=gleif_reference_delta_job,
    cron_schedule="30 20 * * *",
    execution_timezone="UTC",
)
```

- [ ] **Step 5: Run registration tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_assets.py -q
```

Expected: PASS.

## Task 4: Golden Copy ZIP/JSON Parser

**Files:**
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/parser.py`
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_parser.py`

- [ ] **Step 1: Write parser fixture tests**

Create small in-memory ZIP fixtures in `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_parser.py`:

```python
import json
import zipfile
from io import BytesIO

from dagster_v3.defs.gleif import parser


def _zip_json(member_name: str, payload: dict) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, json.dumps(payload))
    return buffer.getvalue()


def test_parse_lei_records_zip_normalizes_entity_rows() -> None:
    payload = {
        "records": [
            {
                "LEI": "HWUPKR0MPOU8FGXBT394",
                "Entity": {
                    "LegalName": {"$": "Apple Inc.", "@lang": "en"},
                    "EntityStatus": "ACTIVE",
                    "LegalJurisdiction": "US-CA",
                    "EntityCategory": "GENERAL",
                    "LegalForm": {"EntityLegalFormCode": "XTIQ"},
                    "LegalAddress": {
                        "@lang": "en",
                        "FirstAddressLine": "One Apple Park Way",
                        "City": "Cupertino",
                        "Region": "US-CA",
                        "Country": "US",
                        "PostalCode": "95014",
                    },
                    "HeadquartersAddress": {
                        "@lang": "en",
                        "FirstAddressLine": "One Apple Park Way",
                        "City": "Cupertino",
                        "Region": "US-CA",
                        "Country": "US",
                        "PostalCode": "95014",
                    },
                },
                "Registration": {
                    "RegistrationStatus": "ISSUED",
                    "InitialRegistrationDate": "2012-06-06T15:53:00Z",
                    "LastUpdateDate": "2026-06-20T08:00:00Z",
                    "ManagingLOU": "EVK05KS7XY1DEII3R011",
                },
            }
        ]
    }

    rows = parser.parse_lei_records_zip(
        _zip_json("lei2.json", payload),
        source_run_id="run-1",
        retrieved_at="2026-06-20T17:00:00+00:00",
        resolved_at="2026-06-20T17:01:00+00:00",
        golden_copy_publish_date="2026-06-20T16:00:00+00:00",
    )

    assert rows.lei_records[0]["lei"] == "HWUPKR0MPOU8FGXBT394"
    assert rows.lei_records[0]["legal_name"] == "Apple Inc."
    assert rows.lei_records[0]["primary_country_iso2"] == "US"
    assert rows.lei_addresses[0]["address_role"] == "legal"
    assert rows.lei_addresses[1]["address_role"] == "headquarters"
```

Add relationship and reporting-exception tests:

```python
def test_parse_relationship_records_zip_normalizes_relationship_rows() -> None:
    payload = {
        "records": [
            {
                "RelationshipRecord": {
                    "Relationship": {
                        "StartNode": {"NodeID": "CHILDLEI12345678901", "NodeIDType": "LEI"},
                        "EndNode": {"NodeID": "PARENTLEI1234567890", "NodeIDType": "LEI"},
                        "RelationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                        "RelationshipStatus": "ACTIVE",
                        "RelationshipPeriods": {
                            "RelationshipPeriod": [
                                {
                                    "StartDate": "2025-01-01T00:00:00Z",
                                    "EndDate": "2025-12-31T00:00:00Z",
                                    "PeriodType": "ACCOUNTING_PERIOD",
                                }
                            ]
                        },
                    },
                    "Registration": {"RegistrationStatus": "PUBLISHED"},
                }
            }
        ]
    }

    rows = parser.parse_relationships_zip(
        _zip_json("rr.json", payload),
        source_run_id="run-1",
        retrieved_at="2026-06-20T17:00:00+00:00",
        resolved_at="2026-06-20T17:01:00+00:00",
    )

    assert rows.relationships[0]["relationship_type"] == "IS_DIRECTLY_CONSOLIDATED_BY"
    assert rows.relationship_periods[0]["period_type"] == "ACCOUNTING_PERIOD"


def test_parse_reporting_exceptions_zip_normalizes_exception_rows() -> None:
    payload = {
        "records": [
            {
                "Exception": {
                    "LEI": "CHILDLEI12345678901",
                    "ExceptionCategory": "NO_KNOWN_PERSON",
                    "ExceptionReason": "NO_LEI",
                    "ParentRelationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                },
                "Registration": {"RegistrationStatus": "PUBLISHED"},
            }
        ]
    }

    rows = parser.parse_reporting_exceptions_zip(
        _zip_json("repex.json", payload),
        source_run_id="run-1",
        retrieved_at="2026-06-20T17:00:00+00:00",
        resolved_at="2026-06-20T17:01:00+00:00",
    )

    assert rows.reporting_exceptions[0]["lei"] == "CHILDLEI12345678901"
    assert rows.reporting_exceptions[0]["exception_category"] == "NO_KNOWN_PERSON"
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_parser.py -q
```

Expected: FAIL because `parser.py` does not exist.

- [ ] **Step 3: Implement parser dataclasses and helpers**

Create `NormalizedGleifRows` dataclass:

```python
from dataclasses import dataclass, field


@dataclass
class NormalizedGleifRows:
    lei_records: list[dict] = field(default_factory=list)
    lei_names: list[dict] = field(default_factory=list)
    lei_addresses: list[dict] = field(default_factory=list)
    lei_identifiers: list[dict] = field(default_factory=list)
    relationships: list[dict] = field(default_factory=list)
    relationship_periods: list[dict] = field(default_factory=list)
    reporting_exceptions: list[dict] = field(default_factory=list)
```

Implement ZIP helpers:

```python
def _first_json_member(zip_bytes: bytes) -> dict:
    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        json_names = [name for name in archive.namelist() if name.endswith(".json")]
        if len(json_names) != 1:
            raise ValueError(f"expected exactly one JSON member, found {len(json_names)}")
        with archive.open(json_names[0]) as handle:
            return json.load(handle)
```

Implement field helpers that accept both likely Golden Copy JSON/CDF-style keys and API-style keys:

```python
def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("$", "name", "value"):
            item = value.get(key)
            if isinstance(item, str):
                return item
    return None
```

Implement parser functions to satisfy fixture tests. After the first real full download, inspect the actual JSON member and adjust only key aliases, not table shape.

- [ ] **Step 4: Run parser tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_parser.py -q
```

Expected: PASS.

## Task 5: Stateful DuckDB Full Replace and Delta Apply

**Files:**
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py`
- Create: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_duckdb_state.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`

- [ ] **Step 1: Write failing DuckDB state tests**

Create tests:

```python
from pathlib import Path

import duckdb

from dagster_v3.defs.gleif import duckdb_state


def test_full_refresh_replaces_current_state(tmp_path: Path) -> None:
    db_path = tmp_path / "gleif.duckdb"
    rows = duckdb_state.GleifStateRows(
        lei_records=[
            {
                "lei": "LEI0000000000000001",
                "legal_name": "Alpha Inc",
                "legal_name_language": "en",
                "entity_status": "ACTIVE",
                "registration_status": "ISSUED",
                "jurisdiction": "US",
                "category": "GENERAL",
                "subcategory": None,
                "legal_form_id": None,
                "legal_form_other": None,
                "registered_at_id": None,
                "registered_at_other": None,
                "registered_as": None,
                "associated_entity_lei": None,
                "associated_entity_name": None,
                "successor_entity_lei": None,
                "successor_entity_name": None,
                "creation_date": None,
                "expiration_date": None,
                "expiration_reason": None,
                "initial_registration_date": None,
                "last_update_date": None,
                "next_renewal_date": None,
                "managing_lou": None,
                "corroboration_level": None,
                "validated_at_id": None,
                "validated_at_other": None,
                "validated_as": None,
                "conformity_flag": None,
                "legal_address_country": "US",
                "headquarters_address_country": "US",
                "primary_country_iso2": "US",
                "golden_copy_publish_date": None,
                "source_system": "gleif",
                "source_run_id": "run-full",
                "retrieved_at": "2026-06-20T17:00:00+00:00",
                "resolved_at": "2026-06-20T17:01:00+00:00",
            }
        ]
    )

    counts = duckdb_state.replace_current_state(db_path, rows)

    assert counts["gleif_lei_records"] == 1
    with duckdb.connect(str(db_path), read_only=True) as connection:
        assert connection.execute("select legal_name from gleif.gleif_lei_records").fetchone()[0] == "Alpha Inc"


def test_delta_upserts_existing_lei_record(tmp_path: Path) -> None:
    db_path = tmp_path / "gleif.duckdb"
    base = duckdb_state.GleifStateRows(
        lei_records=[duckdb_state.minimal_lei_record("LEI0000000000000001", "Alpha Inc", "run-full")]
    )
    delta = duckdb_state.GleifStateRows(
        lei_records=[duckdb_state.minimal_lei_record("LEI0000000000000001", "Alpha Group Inc", "run-delta")]
    )

    duckdb_state.replace_current_state(db_path, base)
    duckdb_state.apply_delta_state(db_path, delta)

    with duckdb.connect(str(db_path), read_only=True) as connection:
        rows = connection.execute("select lei, legal_name from gleif.gleif_lei_records").fetchall()

    assert rows == [("LEI0000000000000001", "Alpha Group Inc")]
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_duckdb_state.py -q
```

Expected: FAIL because `duckdb_state.py` does not exist.

- [ ] **Step 3: Implement DuckDB state table creation**

Implement `GleifStateRows` dataclass with list fields corresponding to `GLEIF_TABLES`.

Implement:

```python
def replace_current_state(database_path: str | Path, rows: GleifStateRows) -> dict[str, int]:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema if not exists gleif")
        _replace_table(connection, "gleif_lei_records", tables.GLEIF_TABLE_COLUMNS["gleif_lei_records"], rows.lei_records)
        # repeat for each table
    return _row_counts(database_path)
```

Implement `_replace_table()` using parameterized `executemany`, not ad hoc value string construction:

```python
def _replace_table(connection, table_name: str, columns: tuple[str, ...], rows: list[dict]) -> None:
    column_sql = ", ".join(f'"{column}" varchar' for column in columns)
    connection.execute(f'create or replace table gleif."{table_name}" ({column_sql})')
    if not rows:
        return
    parameter_markers = ", ".join("?" for _ in columns)
    values = [[row.get(column) for column in columns] for row in rows]
    connection.executemany(
        f'insert into gleif."{table_name}" values ({parameter_markers})',
        values,
    )
```

Use string columns initially in DuckDB to keep ingestion reliable. ClickHouse insert conversion can be tightened after real Golden Copy fixtures are verified.

- [ ] **Step 4: Implement delta apply**

Implement `apply_delta_state()` by key:

```python
TABLE_KEYS = {
    "gleif_lei_records": ("lei",),
    "gleif_lei_names": ("lei", "name_type", "name_normalized", "sequence"),
    "gleif_lei_addresses": ("lei", "address_role"),
    "gleif_lei_identifiers": ("identifier_type", "identifier_value", "lei"),
    "gleif_lei_relationships": ("relationship_record_id",),
    "gleif_lei_relationship_periods": ("relationship_record_id", "period_type", "start_date"),
    "gleif_lei_reporting_exceptions": ("exception_record_id",),
    "gleif_lei_issuers": ("lei",),
    "gleif_code_list_entries": ("code_list", "code"),
}
```

For each table:

1. Create temporary delta table.
2. Delete matching keys from current table.
3. Insert delta rows.

This gives idempotent upserts for repeated delta files.

- [ ] **Step 5: Run DuckDB tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_duckdb_state.py -q
```

Expected: PASS.

## Task 6: ClickHouse Export Asset

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_assets.py`

- [ ] **Step 1: Add failing dependency/export tests**

Append to `test_gleif_assets.py`:

```python
def test_gleif_clickhouse_asset_depends_on_duckdb_state() -> None:
    repository = load_project_defs().get_repository_def()
    asset = repository.asset_graph.get(AssetKey(["gleif_reference_clickhouse"]))

    assert asset.parent_keys == {AssetKey(["gleif_reference_duckdb_state"])}
```

- [ ] **Step 2: Run tests and confirm failure if dependency is absent**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_assets.py -q
```

Expected: FAIL if `gleif_reference_clickhouse` is not implemented or has wrong dependencies.

- [ ] **Step 3: Implement ClickHouse export**

In `assets.py`, implement:

```python
from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    replace_duckdb_tables_in_clickhouse,
)


@dg.asset(
    deps=[dg.AssetKey("gleif_reference_duckdb_state")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "gleif"},
    pool=GLEIF_DUCKDB_POOL,
    description="Exports current GLEIF DuckDB state to ClickHouse corpscout.gleif_* tables.",
)
def gleif_reference_clickhouse(clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.GLEIF_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = replace_duckdb_tables_in_clickhouse(
            duckdb_path=GLEIF_DUCKDB_PATH,
            clickhouse_client=client,
            duckdb_schema=GLEIF_DUCKDB_SCHEMA,
            clickhouse_database=RESOLVED_DATABASE,
            tables=tuple(
                (table_name, tables.GLEIF_TABLE_COLUMNS[table_name])
                for table_name in tables.GLEIF_TABLES
            ),
        )
    return dg.MaterializeResult(
        metadata={f"{table_name}_row_count": count for table_name, count in row_counts.items()}
    )
```

- [ ] **Step 4: Run asset tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_assets.py -q
```

Expected: PASS.

## Task 7: Stateful Full/Delta Asset Wiring and Retention

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_assets.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_gleif_source.py`

- [ ] **Step 1: Add tests for bootstrap state guard**

Add to `test_gleif_source.py`:

```python
def test_delta_requires_existing_current_state() -> None:
    try:
        source.ensure_bootstrap_state_for_delta(None)
    except ValueError as exc:
        assert "full bootstrap" in str(exc)
    else:
        raise AssertionError("delta should fail before full bootstrap state exists")
```

- [ ] **Step 2: Implement state guard**

In `source.py`:

```python
def ensure_bootstrap_state_for_delta(state: dict[str, Any] | None) -> None:
    if not state or not state.get("last_full_publish_date"):
        raise ValueError("GLEIF delta cannot run before a successful full bootstrap")
```

- [ ] **Step 3: Wire `gleif_reference_duckdb_state` to process latest manifest**

Update `assets.py` so `gleif_reference_duckdb_state`:

1. Reads `gleif/state/current.json` if it exists.
2. Finds the current run's raw manifest from materialized raw asset metadata or by deterministic key from `run_id`.
3. Uses full mode when the selected raw manifest has `load_mode=full`.
4. Uses delta mode when the selected raw manifest has `load_mode=delta`.
5. Writes updated `gleif/state/current.json` only after DuckDB state refresh succeeds.

Use `dg.MaterializeResult` metadata:

```python
return dg.MaterializeResult(
    metadata={
        "load_mode": manifest["load_mode"],
        "publish_date": manifest["publish_date"],
        "duckdb_path": str(GLEIF_DUCKDB_PATH),
        **{f"{table_name}_row_count": count for table_name, count in row_counts.items()},
    }
)
```

- [ ] **Step 4: Implement retention asset**

In `assets.py`:

```python
@dg.asset(
    deps=[dg.AssetKey("gleif_reference_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gleif"},
)
def gleif_raw_retention(object_store: ObjectStoreResource) -> dg.MaterializeResult:
    keys = object_store.list_keys("gleif/raw/", bucket=GLEIF_RAW_BUCKET)
    # Keep all manifests and keep raw blobs for the newest publish_date per load_mode.
    keys_to_delete = select_gleif_raw_keys_for_deletion(keys)
    deleted_count = object_store.delete_keys(tuple(keys_to_delete), bucket=GLEIF_RAW_BUCKET)
    return dg.MaterializeResult(metadata={"deleted_key_count": deleted_count})
```

Implement `select_gleif_raw_keys_for_deletion(keys)` in `source.py` and unit test it with fake keys:

```python
def test_retention_keeps_manifests_and_newest_raw_snapshot() -> None:
    keys = [
        "gleif/raw/load_mode=full/publish_date=2026-06-19T16-00-00Z/run_id=old/file_kind=lei_records/source.json.zip",
        "gleif/raw/load_mode=full/publish_date=2026-06-19T16-00-00Z/run_id=old/manifest.json",
        "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/run_id=new/file_kind=lei_records/source.json.zip",
        "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/run_id=new/manifest.json",
    ]

    assert source.select_gleif_raw_keys_for_deletion(keys) == [
        "gleif/raw/load_mode=full/publish_date=2026-06-19T16-00-00Z/run_id=old/file_kind=lei_records/source.json.zip"
    ]
```

- [ ] **Step 5: Run source and asset tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_source.py tests/test_gleif_assets.py -q
```

Expected: PASS.

## Task 8: End-to-End Verification

- [ ] **Step 1: Run focused GLEIF tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest \
  tests/test_gleif_tables.py \
  tests/test_gleif_source.py \
  tests/test_gleif_parser.py \
  tests/test_gleif_duckdb_state.py \
  tests/test_gleif_assets.py \
  -q
```

Expected: PASS.

- [ ] **Step 2: Run migration tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py -q
```

Expected: PASS.

- [ ] **Step 3: Validate Dagster definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check
```

Expected: PASS with no definition loading errors.

- [ ] **Step 4: Apply ClickHouse migration locally**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: migration output applies `000023_corpscout_gleif_reference_data` or reports no change if already applied.

- [ ] **Step 5: Record the manual real-bootstrap command**

Do not download the 924 MB full file during automated tests. The first real bootstrap is an operator action after migrations and definition checks pass.

The first real bootstrap materialization should be run manually from Dagster UI or CLI:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg launch --job gleif_reference_bootstrap_job
```

Expected runtime behavior:

- three full `.json.zip` files appear under `source-gleif-reference-data/gleif/raw/load_mode=full/...`
- `gleif/state/current.json` exists after DuckDB state succeeds
- ClickHouse contains non-empty `corpscout.gleif_lei_records`

- [ ] **Step 6: Verify daily delta job remains scheduled, not bootstrap**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_assets.py::test_gleif_jobs_and_delta_schedule_are_registered -q
```

Expected: PASS, schedule targets `gleif_reference_delta_job`.

## Execution Notes

- Keep raw manifests out of ClickHouse.
- Do not create `gleif_full_*` and `gleif_delta_*` ClickHouse tables.
- Do not use Dagster partitions for `latest?delta=LastDay`.
- Do not use dlt for the full Golden Copy parser.
- Keep all new GLEIF tables in the `corpscout` ClickHouse database.
- Prefer concrete helpers over abstractions; only introduce protocols/fakes in tests where the existing `ObjectStoreResource` fake pattern is insufficient.

## Self-Review

- Spec coverage: schema, full raw bootstrap, delta raw schedule, DuckDB current state, ClickHouse export, S3 state, retention, tests, and jobs are covered.
- Red-flag scan: no empty "add tests" step, and each implementation task has file paths and commands.
- Type consistency: asset names, table names, job names, schedule name, bucket name, and state key match the current analysis proposal.
