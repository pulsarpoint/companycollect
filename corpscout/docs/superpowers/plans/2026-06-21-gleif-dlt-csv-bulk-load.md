# GLEIF dlt CSV Bulk Load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the slow Python JSON-to-DuckDB GLEIF bootstrap path with CSV ZIP raw files loaded into DuckDB through Dagster dlt assets, then normalized with DuckDB SQL.

**Architecture:** GLEIF raw assets download `latest.csv.zip` files to object storage. A Pythonic `@dlt_assets` definition materializes three raw DuckDB table assets from the run manifest, and `gleif_reference_duckdb_state` becomes a pure DuckDB SQL normalization/state asset. The final ClickHouse schema stays unchanged.

**Tech Stack:** Dagster assets, `dagster_dlt.DagsterDltResource`, `@dlt_assets`, dlt filesystem CSV source, dlt DuckDB destination, pandas for dlt CSV reading, DuckDB SQL, ClickHouse publication through the existing helper, pytest.

---

## Dagster And dlt Choice

The installed project exposes `dagster_dlt.DltLoadCollectionComponent`, and `uv run dg list components --json` confirms it. Do not use that component for this source because the GLEIF dlt source is runtime-driven by the raw S3 manifest and temporary extracted CSV paths. Use the repo's existing Pythonic pattern instead:

- `@dlt_assets`
- `DagsterDltResource`
- `DagsterDltTranslator`
- definition-time dlt source shape
- runtime `dlt.run(context=context, dlt_source=source, dlt_pipeline=pipeline)` with the manifest-selected source

This matches the existing NACE, Wikidata, Finland YTJ, Norway BRREG, Latvia UR, Estonia AR, exchange rates, and Finland XBRL assets.

## Target Asset Graph

One dlt asset definition named `gleif_raw_duckdb_dlt` should materialize these raw table assets:

- `gleif_raw_lei_records_duckdb`
- `gleif_raw_relationships_duckdb`
- `gleif_raw_reporting_exceptions_duckdb`

Full bootstrap job:

```text
gleif_full_raw_reference_files
  -> gleif_raw_lei_records_duckdb
  -> gleif_raw_relationships_duckdb
  -> gleif_raw_reporting_exceptions_duckdb
  -> gleif_reference_duckdb_state
  -> gleif_reference_clickhouse
  -> gleif_raw_retention
```

Daily delta job:

```text
gleif_delta_raw_reference_files
  -> gleif_raw_lei_records_duckdb
  -> gleif_raw_relationships_duckdb
  -> gleif_raw_reporting_exceptions_duckdb
  -> gleif_reference_duckdb_state
  -> gleif_reference_clickhouse
  -> gleif_raw_retention
```

The dlt raw assets should declare dependency edges to both raw S3 assets because a run can be full or delta. The jobs select only the raw S3 asset for their mode plus the shared downstream assets.

## File Structure

- Modify: `corpscout/dagster_v3/pyproject.toml`
  - Add `pandas>=3.0.0`, required by `dlt.sources.filesystem.read_csv`.
- Modify: `corpscout/dagster_v3/uv.lock`
  - Regenerate with `uv lock`.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`
  - Change default GLEIF raw format to CSV.
  - Add `file_format` to each manifest file entry.
  - Move or expose run manifest lookup as `manifest_for_run`.
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/dlt_csv.py`
  - Extract exactly one CSV member from each ZIP.
  - Define definition-time and runtime dlt CSV sources.
  - Define the dlt DuckDB pipeline for `data/gleif_reference.duckdb`.
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/csv_transforms.py`
  - Build normalized `gleif_staging.gleif_*` tables from dlt raw tables.
  - Replace or upsert current `gleif.gleif_*` tables after staging succeeds.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`
  - Add the `@dlt_assets` raw DuckDB definition.
  - Change the DuckDB path to `data/gleif_reference.duckdb`.
  - Update `gleif_reference_duckdb_state` dependencies to the three raw DuckDB assets.
  - Update bootstrap and delta jobs to include the three raw DuckDB assets.
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py`
  - Remove S3 raw-file loading from normalized state processing.
  - Delegate CSV raw-table normalization to `csv_transforms.py`.
  - Keep JSON row-list helpers only for legacy tests until they are removed in a follow-up.
- Modify: `corpscout/dagster_v3/tests/test_gleif_source.py`
  - Update raw format expectations and manifest tests.
- Create: `corpscout/dagster_v3/tests/test_gleif_dlt_csv.py`
  - Test ZIP CSV extraction, dlt source shape, and dlt DuckDB loading.
- Create: `corpscout/dagster_v3/tests/test_gleif_assets_dlt.py`
  - Test asset keys, dependencies, jobs, and dlt runtime source invocation.
- Create: `corpscout/dagster_v3/tests/test_gleif_csv_transforms.py`
  - Test normalized SQL transforms from dlt-normalized raw tables.
- Modify: `corpscout/dagster_v3/tests/test_gleif_duckdb_state.py`
  - Replace active manifest processing tests with dlt raw-table fixtures.

## Task 1: Source Defaults And Manifest Format

**Files:**
- Modify: `corpscout/dagster_v3/pyproject.toml`
- Modify: `corpscout/dagster_v3/uv.lock`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`
- Modify: `corpscout/dagster_v3/tests/test_gleif_source.py`

- [ ] **Step 1: Write failing tests for CSV default and manifest file format**

Add these tests to `corpscout/dagster_v3/tests/test_gleif_source.py`:

```python
from datetime import UTC, datetime

from dagster_v3.defs.gleif import source


def test_raw_download_config_defaults_to_csv() -> None:
    assert source.GleifRawDownloadConfig().file_format == "csv"


def test_manifest_payload_includes_file_format_per_file() -> None:
    manifest = source.build_manifest(
        load_mode="full",
        delta=None,
        publish_date="2026-06-20T16:00:00+00:00",
        run_id="run-1",
        pulled_at=datetime(2026, 6, 20, 17, 0, tzinfo=UTC),
        files=[
            source.DownloadedFile(
                file_kind="lei_records",
                file_format="csv",
                source_url=(
                    "https://goldencopy.gleif.org/api/v2/"
                    "golden-copies/publishes/lei2/latest.csv"
                ),
                s3_key=(
                    "gleif/raw/load_mode=full/publish_date=2026-06-20T16-00-00Z/"
                    "run_id=run-1/file_kind=lei_records/source.csv.zip"
                ),
                size_bytes=123,
                sha256="a" * 64,
                etag='"etag"',
                last_modified="Sat, 20 Jun 2026 17:29:16 GMT",
            )
        ],
    )

    assert manifest["files"][0]["file_format"] == "csv"
```

Update the existing Golden Copy URL test so the CSV URL is the default expectation:

```python
def test_golden_copy_url_supports_full_and_delta() -> None:
    assert source.golden_copy_url(file_kind="lei2", file_format="csv", delta=None) == (
        "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv"
    )
    assert source.golden_copy_url(
        file_kind="repex",
        file_format="csv",
        delta="LastDay",
    ) == (
        "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/"
        "repex/latest.csv?delta=LastDay"
    )
```

Update raw object key tests so they pass `extension="csv.zip"` and assert that the returned key ends with `/source.csv.zip`.

- [ ] **Step 2: Run source tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_source.py -q
```

Expected: failure because `GleifRawDownloadConfig.file_format` still defaults to `json`, `DownloadedFile` does not accept `file_format`, and manifest entries do not include `file_format`.

- [ ] **Step 3: Add pandas dependency**

Modify `corpscout/dagster_v3/pyproject.toml` dependencies:

```toml
"pandas>=3.0.0",
```

Place it next to the other data-processing dependencies:

```toml
"openai>=2.41.1",
"pandas>=3.0.0",
"polars[rtcompat]>=1.41.2",
```

Regenerate the lockfile:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv lock
```

- [ ] **Step 4: Implement CSV default and manifest file format**

In `corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py`, change the config default:

```python
class GleifRawDownloadConfig(dg.Config):
    file_format: str = "csv"
```

Change `DownloadedFile` to include the file format:

```python
@dataclass(frozen=True)
class DownloadedFile:
    file_kind: str
    file_format: str
    source_url: str
    s3_key: str
    size_bytes: int
    sha256: str
    etag: str | None
    last_modified: str | None
```

In `build_manifest`, add `file_format` to each file object:

```python
"files": [
    {
        "file_kind": item.file_kind,
        "file_format": item.file_format,
        "source_url": item.source_url,
        "s3_key": item.s3_key,
        "size_bytes": item.size_bytes,
        "sha256": item.sha256,
        "etag": item.etag,
        "last_modified": item.last_modified,
    }
    for item in files
],
```

In `_download_one_file`, set the dataclass field:

```python
downloaded_file = DownloadedFile(
    file_kind=file_kind,
    file_format=file_format,
    source_url=source_url,
    s3_key=s3_key,
    size_bytes=size_bytes,
    sha256=digest.hexdigest(),
    etag=response.headers.get("etag"),
    last_modified=response.headers.get("last-modified"),
)
```

- [ ] **Step 5: Expose manifest lookup for downstream assets**

Move the private run manifest lookup from `duckdb_state.py` to `source.py` as:

```python
def manifest_for_run(object_store: ObjectStoreResource, run_id: str) -> dict[str, Any]:
    manifest_keys = [
        key
        for key in object_store.list_keys("gleif/raw/", bucket=GLEIF_RAW_BUCKET)
        if key.endswith("/manifest.json") and f"/run_id={run_id}/" in key
    ]
    if not manifest_keys:
        raise ValueError(f"No GLEIF manifest found for run_id={run_id}")
    if len(manifest_keys) > 1:
        raise ValueError(f"Multiple GLEIF manifests found for run_id={run_id}: {manifest_keys}")
    return json.loads(object_store.read_bytes(manifest_keys[0], bucket=GLEIF_RAW_BUCKET))
```

Delete the duplicate private manifest function from `duckdb_state.py` after callers are moved in later tasks.

- [ ] **Step 6: Run source tests and verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_source.py -q
```

Expected: all `test_gleif_source.py` tests pass.

- [ ] **Step 7: Commit Task 1**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/pyproject.toml \
  corpscout/dagster_v3/uv.lock \
  corpscout/dagster_v3/src/dagster_v3/defs/gleif/source.py \
  corpscout/dagster_v3/tests/test_gleif_source.py
git commit -m "feat: default GLEIF raw downloads to CSV"
```

## Task 2: dlt CSV Source Helpers

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/dlt_csv.py`
- Create: `corpscout/dagster_v3/tests/test_gleif_dlt_csv.py`

- [ ] **Step 1: Write failing tests for ZIP validation**

Create `corpscout/dagster_v3/tests/test_gleif_dlt_csv.py`:

```python
from pathlib import Path
import zipfile

import pytest

from dagster_v3.defs.gleif.dlt_csv import extract_single_csv_member


def write_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)


def test_extract_single_csv_member_writes_csv(tmp_path: Path) -> None:
    archive_path = tmp_path / "source.csv.zip"
    write_zip(archive_path, {"lei2.csv": "LEI,Entity.LegalName\n123,Acme\n"})

    csv_path = extract_single_csv_member(
        zip_path=archive_path,
        output_dir=tmp_path / "out",
        file_kind="lei_records",
    )

    assert csv_path.name == "lei_records.csv"
    assert csv_path.read_text() == "LEI,Entity.LegalName\n123,Acme\n"


def test_extract_single_csv_member_rejects_empty_zip(tmp_path: Path) -> None:
    archive_path = tmp_path / "source.csv.zip"
    write_zip(archive_path, {"readme.txt": "not csv"})

    with pytest.raises(ValueError, match="contains no CSV members"):
        extract_single_csv_member(
            zip_path=archive_path,
            output_dir=tmp_path / "out",
            file_kind="lei_records",
        )


def test_extract_single_csv_member_rejects_multiple_csv_members(tmp_path: Path) -> None:
    archive_path = tmp_path / "source.csv.zip"
    write_zip(archive_path, {"a.csv": "a\n", "b.csv": "b\n"})

    with pytest.raises(ValueError, match="contains multiple CSV members"):
        extract_single_csv_member(
            zip_path=archive_path,
            output_dir=tmp_path / "out",
            file_kind="relationships",
        )
```

- [ ] **Step 2: Run dlt helper tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_dlt_csv.py -q
```

Expected: import failure because `dagster_v3.defs.gleif.dlt_csv` does not exist.

- [ ] **Step 3: Create CSV extraction and dlt source module**

Create `corpscout/dagster_v3/src/dagster_v3/defs/gleif/dlt_csv.py`:

```python
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
import zipfile

import dlt as dlt_lib
from dlt.sources.filesystem import filesystem, read_csv

GLEIF_DLT_PIPELINE_NAME = "gleif_raw_csv_duckdb"
GLEIF_DLT_RAW_DATASET_NAME = "gleif_raw"
GLEIF_RAW_LEI_RECORDS_TABLE = "gleif_raw_lei_records"
GLEIF_RAW_RELATIONSHIPS_TABLE = "gleif_raw_relationships"
GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE = "gleif_raw_reporting_exceptions"

FileKind = Literal["lei_records", "relationships", "reporting_exceptions"]

RAW_TABLE_BY_FILE_KIND: dict[FileKind, str] = {
    "lei_records": GLEIF_RAW_LEI_RECORDS_TABLE,
    "relationships": GLEIF_RAW_RELATIONSHIPS_TABLE,
    "reporting_exceptions": GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
}


@dataclass(frozen=True)
class ExtractedGleifCsv:
    file_kind: FileKind
    csv_path: Path
    source_url: str
    s3_key: str
    source_sha256: str
    publish_date: str
    load_mode: Literal["full", "delta"]
    run_id: str


def extract_single_csv_member(
    *,
    zip_path: str | Path,
    output_dir: str | Path,
    file_kind: str,
) -> Path:
    archive_path = Path(zip_path)
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(archive_path) as archive:
        csv_members = [
            info
            for info in archive.infolist()
            if not info.is_dir() and info.filename.lower().endswith(".csv")
        ]
        if not csv_members:
            raise ValueError(f"GLEIF ZIP {archive_path} contains no CSV members")
        if len(csv_members) > 1:
            names = [info.filename for info in csv_members]
            raise ValueError(f"GLEIF ZIP {archive_path} contains multiple CSV members: {names}")

        output_path = target_dir / f"{file_kind}.csv"
        with archive.open(csv_members[0]) as source, output_path.open("wb") as target:
            target.write(source.read())
        return output_path


def gleif_csv_dlt_pipeline(database_path: str | Path) -> Any:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    working_dir = database_file.parent / ".dlt" / "gleif"
    working_dir.mkdir(parents=True, exist_ok=True)
    return dlt_lib.pipeline(
        pipeline_name=GLEIF_DLT_PIPELINE_NAME,
        destination=dlt_lib.destinations.duckdb(str(database_file)),
        dataset_name=GLEIF_DLT_RAW_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(working_dir),
    )


def gleif_definition_time_csv_files() -> list[ExtractedGleifCsv]:
    base = Path("/tmp/gleif-dlt-definition-shape")
    return [
        ExtractedGleifCsv(
            file_kind=file_kind,
            csv_path=base / f"{file_kind}.csv",
            source_url=f"https://example.invalid/{file_kind}.csv",
            s3_key=f"definition-shape/{file_kind}/source.csv.zip",
            source_sha256="0" * 64,
            publish_date="1970-01-01T00:00:00+00:00",
            load_mode="full",
            run_id="definition-shape",
        )
        for file_kind in RAW_TABLE_BY_FILE_KIND
    ]


@dlt_lib.source(name="gleif_csv")
def gleif_csv_dlt_source(
    extracted_files: Iterable[ExtractedGleifCsv],
) -> list[Any]:
    resources: list[Any] = []
    for item in extracted_files:
        table_name = RAW_TABLE_BY_FILE_KIND[item.file_kind]
        resource = filesystem(
            bucket_url=str(item.csv_path.parent),
            file_glob=item.csv_path.name,
        ) | read_csv()
        resource = resource.with_name(table_name)
        resource.apply_hints(write_disposition="replace")
        resources.append(resource)
    return resources
```

- [ ] **Step 4: Write failing test for dlt source shape**

Add to `corpscout/dagster_v3/tests/test_gleif_dlt_csv.py`:

```python
from dagster_v3.defs.gleif.dlt_csv import (
    GLEIF_RAW_LEI_RECORDS_TABLE,
    GLEIF_RAW_RELATIONSHIPS_TABLE,
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
    gleif_csv_dlt_source,
    gleif_definition_time_csv_files,
)


def test_definition_time_source_has_three_resources() -> None:
    dlt_source = gleif_csv_dlt_source(gleif_definition_time_csv_files())

    assert set(dlt_source.resources.keys()) == {
        GLEIF_RAW_LEI_RECORDS_TABLE,
        GLEIF_RAW_RELATIONSHIPS_TABLE,
        GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
    }
```

- [ ] **Step 5: Run dlt helper tests and verify pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_dlt_csv.py -q
```

Expected: all `test_gleif_dlt_csv.py` tests pass.

- [ ] **Step 6: Commit Task 2**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/gleif/dlt_csv.py \
  corpscout/dagster_v3/tests/test_gleif_dlt_csv.py
git commit -m "feat: add GLEIF dlt CSV source helpers"
```

## Task 3: Dagster dlt Raw DuckDB Assets

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`
- Create: `corpscout/dagster_v3/tests/test_gleif_assets_dlt.py`

- [ ] **Step 1: Write failing asset graph tests**

Create `corpscout/dagster_v3/tests/test_gleif_assets_dlt.py`:

```python
import dagster as dg

from dagster_v3.defs.gleif import assets


def test_gleif_dlt_raw_asset_keys_are_registered() -> None:
    defs = assets.defs
    asset_keys = {key.to_user_string() for key in defs.get_asset_graph().get_all_asset_keys()}

    assert "gleif_raw_lei_records_duckdb" in asset_keys
    assert "gleif_raw_relationships_duckdb" in asset_keys
    assert "gleif_raw_reporting_exceptions_duckdb" in asset_keys


def test_gleif_reference_duckdb_state_depends_on_dlt_raw_assets() -> None:
    graph = assets.defs.get_asset_graph()
    deps = {
        key.to_user_string()
        for key in graph.get_parents(dg.AssetKey("gleif_reference_duckdb_state"))
    }

    assert deps == {
        "gleif_raw_lei_records_duckdb",
        "gleif_raw_relationships_duckdb",
        "gleif_raw_reporting_exceptions_duckdb",
    }


def test_gleif_jobs_select_dlt_raw_assets() -> None:
    bootstrap_selection = assets.gleif_reference_bootstrap_job.selection
    delta_selection = assets.gleif_reference_delta_job.selection

    assert "gleif_raw_lei_records_duckdb" in bootstrap_selection
    assert "gleif_raw_relationships_duckdb" in bootstrap_selection
    assert "gleif_raw_reporting_exceptions_duckdb" in bootstrap_selection
    assert "gleif_raw_lei_records_duckdb" in delta_selection
    assert "gleif_raw_relationships_duckdb" in delta_selection
    assert "gleif_raw_reporting_exceptions_duckdb" in delta_selection
```

- [ ] **Step 2: Run asset graph tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_assets_dlt.py -q
```

Expected: failure because the raw dlt assets are not registered and `gleif_reference_duckdb_state` still depends on raw S3 assets.

- [ ] **Step 3: Add dlt imports, constants, and translator**

Modify `corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py` imports:

```python
from collections.abc import Iterator
from typing import Any

import dlt as dlt_lib
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
```

Add imports from the new helper module:

```python
from dagster_v3.defs.gleif.dlt_csv import (
    GLEIF_DLT_RAW_DATASET_NAME,
    GLEIF_RAW_LEI_RECORDS_TABLE,
    GLEIF_RAW_RELATIONSHIPS_TABLE,
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
    ExtractedGleifCsv,
    extract_single_csv_member,
    gleif_csv_dlt_pipeline,
    gleif_csv_dlt_source,
    gleif_definition_time_csv_files,
)
from dagster_v3.defs.gleif.source import manifest_for_run
```

Change the DuckDB path constants:

```python
GLEIF_DUCKDB_PATH = Path("data/gleif_reference.duckdb")
GLEIF_DUCKDB_SCHEMA = f"{GLEIF_DUCKDB_PATH.stem}.gleif"
GLEIF_DUCKDB_RAW_ASSET_KEYS = (
    dg.AssetKey("gleif_raw_lei_records_duckdb"),
    dg.AssetKey("gleif_raw_relationships_duckdb"),
    dg.AssetKey("gleif_raw_reporting_exceptions_duckdb"),
)
```

Add translator code:

```python
GLEIF_DLT_ASSET_BY_TABLE = {
    GLEIF_RAW_LEI_RECORDS_TABLE: "gleif_raw_lei_records_duckdb",
    GLEIF_RAW_RELATIONSHIPS_TABLE: "gleif_raw_relationships_duckdb",
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE: "gleif_raw_reporting_exceptions_duckdb",
}


class GleifDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        asset_key = GLEIF_DLT_ASSET_BY_TABLE.get(data.resource.table_name)
        if asset_key is None:
            return spec
        return spec.replace_attributes(
            key=asset_key,
            deps=[
                dg.AssetKey("gleif_full_raw_reference_files"),
                dg.AssetKey("gleif_delta_raw_reference_files"),
            ],
            group_name=GROUP_NAME,
            description=f"Raw GLEIF CSV table `{data.resource.table_name}` loaded into DuckDB with dlt.",
            kinds={"python", "dlt", "duckdb", "gleif"},
        )
```

- [ ] **Step 4: Add manifest-to-extracted-files helper**

Add this helper to `assets.py`:

```python
def _extract_manifest_csv_files(
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
    temp_dir: Path,
) -> list[ExtractedGleifCsv]:
    extracted_files: list[ExtractedGleifCsv] = []
    for file_entry in manifest["files"]:
        if file_entry.get("file_format") != "csv":
            raise ValueError(
                "GLEIF dlt CSV loader only accepts manifest files with file_format=csv"
            )
        file_kind = file_entry["file_kind"]
        zip_path = temp_dir / file_kind / "source.csv.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(
            object_store.read_bytes(file_entry["s3_key"], bucket=GLEIF_RAW_BUCKET)
        )
        csv_path = extract_single_csv_member(
            zip_path=zip_path,
            output_dir=temp_dir / file_kind / "csv",
            file_kind=file_kind,
        )
        extracted_files.append(
            ExtractedGleifCsv(
                file_kind=file_kind,
                csv_path=csv_path,
                source_url=file_entry["source_url"],
                s3_key=file_entry["s3_key"],
                source_sha256=file_entry["sha256"],
                publish_date=manifest["publish_date"],
                load_mode=manifest["load_mode"],
                run_id=manifest["run_id"],
            )
        )
    return extracted_files
```

- [ ] **Step 5: Add the `@dlt_assets` raw DuckDB definition**

Add this asset definition after the raw S3 assets in `assets.py`:

```python
@dlt_assets(
    dlt_source=gleif_csv_dlt_source(gleif_definition_time_csv_files()),
    dlt_pipeline=gleif_csv_dlt_pipeline(GLEIF_DUCKDB_PATH),
    name="gleif_raw_duckdb_dlt",
    dagster_dlt_translator=GleifDltTranslator(),
    pool=GLEIF_DUCKDB_POOL,
)
def gleif_raw_duckdb_dlt_assets(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    object_store: ObjectStoreResource,
) -> Iterator[Any]:
    manifest = manifest_for_run(object_store, context.run_id)
    with tempfile.TemporaryDirectory(prefix="gleif-dlt-csv-") as temp_path:
        extracted_files = _extract_manifest_csv_files(
            object_store=object_store,
            manifest=manifest,
            temp_dir=Path(temp_path),
        )
        yield from dlt.run(
            context=context,
            dlt_source=gleif_csv_dlt_source(extracted_files),
            dlt_pipeline=gleif_csv_dlt_pipeline(GLEIF_DUCKDB_PATH),
        )
```

Add `import tempfile` to the imports.

- [ ] **Step 6: Rewire normalized state asset dependencies and jobs**

Change the normalized state asset decorator:

```python
@dg.asset(
    deps=GLEIF_DUCKDB_RAW_ASSET_KEYS,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "gleif"},
    pool=GLEIF_DUCKDB_POOL,
    description="Maintains current GLEIF normalized state in DuckDB from dlt-loaded raw CSV tables.",
)
def gleif_reference_duckdb_state(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
```

Update job selections:

```python
gleif_reference_bootstrap_job = dg.define_asset_job(
    name="gleif_reference_bootstrap_job",
    selection=[
        "gleif_full_raw_reference_files",
        "gleif_raw_lei_records_duckdb",
        "gleif_raw_relationships_duckdb",
        "gleif_raw_reporting_exceptions_duckdb",
        "gleif_reference_duckdb_state",
        "gleif_reference_clickhouse",
        "gleif_raw_retention",
    ],
)

gleif_reference_delta_job = dg.define_asset_job(
    name="gleif_reference_delta_job",
    selection=[
        "gleif_delta_raw_reference_files",
        "gleif_raw_lei_records_duckdb",
        "gleif_raw_relationships_duckdb",
        "gleif_raw_reporting_exceptions_duckdb",
        "gleif_reference_duckdb_state",
        "gleif_reference_clickhouse",
        "gleif_raw_retention",
    ],
)
```

Register the dlt assets:

```python
defs = dg.Definitions(
    assets=[
        gleif_full_raw_reference_files,
        gleif_delta_raw_reference_files,
        gleif_raw_duckdb_dlt_assets,
        gleif_reference_duckdb_state,
        gleif_reference_clickhouse,
        gleif_raw_retention,
    ],
    jobs=[gleif_reference_bootstrap_job, gleif_reference_delta_job],
    schedules=[gleif_reference_delta_daily],
)
```

- [ ] **Step 7: Run asset graph tests and Dagster definition check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_assets_dlt.py -q
uv run dg check defs
```

Expected: tests pass and `dg check defs` reports all definitions valid.

- [ ] **Step 8: Commit Task 3**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py \
  corpscout/dagster_v3/tests/test_gleif_assets_dlt.py
git commit -m "feat: add GLEIF dlt raw DuckDB assets"
```

## Task 4: Normalize From dlt Raw Tables

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/csv_transforms.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py`
- Create: `corpscout/dagster_v3/tests/test_gleif_csv_transforms.py`
- Modify: `corpscout/dagster_v3/tests/test_gleif_duckdb_state.py`

- [ ] **Step 1: Write failing transform tests**

Create `corpscout/dagster_v3/tests/test_gleif_csv_transforms.py` with a minimal DuckDB fixture:

```python
from pathlib import Path

import duckdb

from dagster_v3.defs.gleif.csv_transforms import replace_current_from_dlt_raw_tables
from dagster_v3.defs.gleif.dlt_csv import GLEIF_DLT_RAW_DATASET_NAME


def seed_raw_tables(database_path: Path) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {GLEIF_DLT_RAW_DATASET_NAME}")
        connection.execute(
            f"""
            create table {GLEIF_DLT_RAW_DATASET_NAME}.gleif_raw_lei_records (
              lei varchar,
              entity_legal_name varchar,
              entity_legal_name_xmllang varchar,
              entity_entity_status varchar,
              entity_legal_jurisdiction varchar,
              entity_entity_category varchar,
              entity_entity_sub_category varchar,
              entity_legal_form_entity_legal_form_code varchar,
              entity_legal_form_other_legal_form varchar,
              entity_registration_authority_registration_authority_id varchar,
              entity_registration_authority_other_registration_authority_id varchar,
              entity_registration_authority_registration_authority_entity_id varchar,
              entity_entity_creation_date varchar,
              entity_entity_expiration_date varchar,
              entity_entity_expiration_reason varchar,
              registration_initial_registration_date varchar,
              registration_last_update_date varchar,
              registration_registration_status varchar,
              registration_next_renewal_date varchar,
              registration_managing_lou varchar,
              registration_validation_sources varchar,
              registration_validation_authority_validation_authority_id varchar,
              registration_validation_authority_other_validation_authority_id varchar,
              registration_validation_authority_validation_authority_entity_id varchar,
              conformity_flag varchar,
              entity_legal_address_first_address_line varchar,
              entity_legal_address_city varchar,
              entity_legal_address_region varchar,
              entity_legal_address_country varchar,
              entity_legal_address_postal_code varchar,
              entity_headquarters_address_first_address_line varchar,
              entity_headquarters_address_city varchar,
              entity_headquarters_address_region varchar,
              entity_headquarters_address_country varchar,
              entity_headquarters_address_postal_code varchar,
              entity_other_entity_names_other_entity_name_1 varchar,
              entity_other_entity_names_other_entity_name_1_xmllang varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into {GLEIF_DLT_RAW_DATASET_NAME}.gleif_raw_lei_records values (
              '5493001KJTIIGC8Y1R12',
              'ACME PLC',
              'en',
              'ACTIVE',
              'GB',
              'GENERAL',
              null,
              'H0PO',
              null,
              'RA000585',
              null,
              '123456',
              '2020-01-01T00:00:00Z',
              null,
              null,
              '2020-01-02T00:00:00Z',
              '2026-06-20T00:00:00Z',
              'ISSUED',
              '2027-06-20T00:00:00Z',
              '213800WAVVOPS85N2205',
              'FULLY_CORROBORATED',
              null,
              null,
              null,
              'CONFORMING',
              '1 Market Street',
              'London',
              null,
              'GB',
              'EC1A 1AA',
              '2 HQ Street',
              'London',
              null,
              'GB',
              'EC1A 2BB',
              'ACME LIMITED',
              'en'
            )
            """
        )
        connection.execute(
            f"""
            create table {GLEIF_DLT_RAW_DATASET_NAME}.gleif_raw_relationships (
              relationship_start_node_node_id varchar,
              relationship_start_node_node_id_type varchar,
              relationship_end_node_node_id varchar,
              relationship_end_node_node_id_type varchar,
              relationship_relationship_type varchar,
              relationship_relationship_status varchar,
              relationship_period_1_start_date varchar,
              relationship_period_1_end_date varchar,
              relationship_period_1_period_type varchar,
              registration_initial_registration_date varchar,
              registration_last_update_date varchar,
              registration_registration_status varchar,
              registration_next_renewal_date varchar,
              registration_managing_lou varchar,
              registration_validation_sources varchar,
              registration_validation_documents varchar,
              registration_validation_reference varchar,
              deleted_at varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into {GLEIF_DLT_RAW_DATASET_NAME}.gleif_raw_relationships values (
              '5493001KJTIIGC8Y1R12',
              'LEI',
              '54930084UKLVMY22DS16',
              'LEI',
              'IS_DIRECTLY_CONSOLIDATED_BY',
              'ACTIVE',
              '2020-01-01',
              null,
              'ACCOUNTING_PERIOD',
              '2020-01-02T00:00:00Z',
              '2026-06-20T00:00:00Z',
              'PUBLISHED',
              '2027-06-20T00:00:00Z',
              '213800WAVVOPS85N2205',
              'FULLY_CORROBORATED',
              'SUPPORTING_DOCUMENTS',
              'annual-report',
              null
            )
            """
        )
        connection.execute(
            f"""
            create table {GLEIF_DLT_RAW_DATASET_NAME}.gleif_raw_reporting_exceptions (
              lei varchar,
              exception_category varchar,
              exception_reason_1 varchar,
              exception_reference_1 varchar,
              deleted_at varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into {GLEIF_DLT_RAW_DATASET_NAME}.gleif_raw_reporting_exceptions values (
              '5493001KJTIIGC8Y1R12',
              'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT',
              'NO_KNOWN_PERSON',
              'not-disclosed',
              null
            )
            """
        )


def test_replace_current_from_dlt_raw_tables_builds_normalized_tables(tmp_path: Path) -> None:
    database_path = tmp_path / "gleif_reference.duckdb"
    seed_raw_tables(database_path)

    row_counts = replace_current_from_dlt_raw_tables(
        database_path=database_path,
        load_mode="full",
        publish_date="2026-06-20T16:00:00+00:00",
        run_id="run-1",
    )

    assert row_counts["gleif_lei_records"] == 1
    assert row_counts["gleif_lei_names"] == 2
    assert row_counts["gleif_lei_addresses"] == 2
    assert row_counts["gleif_lei_relationships"] == 1
    assert row_counts["gleif_lei_relationship_periods"] == 1
    assert row_counts["gleif_lei_reporting_exceptions"] == 1
```

- [ ] **Step 2: Run transform tests and verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_csv_transforms.py -q
```

Expected: import failure because `csv_transforms.py` does not exist.

- [ ] **Step 3: Create transform module with schema constants**

Create `corpscout/dagster_v3/src/dagster_v3/defs/gleif/csv_transforms.py`:

```python
from __future__ import annotations

from pathlib import Path
from typing import Literal

import duckdb

from dagster_v3.defs.gleif import tables
from dagster_v3.defs.gleif.dlt_csv import (
    GLEIF_DLT_RAW_DATASET_NAME,
    GLEIF_RAW_LEI_RECORDS_TABLE,
    GLEIF_RAW_RELATIONSHIPS_TABLE,
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
)
from dagster_v3.defs.gleif.duckdb_state import (
    DUCKDB_SCHEMA,
    DUCKDB_STAGING_SCHEMA,
    TABLE_KEYS,
    _ensure_all_tables,
    _ensure_empty_tables,
    _ensure_schema,
    _qualified_table,
    _replace_current_tables_from_schema,
    _row_counts,
    _upsert_current_tables_from_schema,
)


def replace_current_from_dlt_raw_tables(
    *,
    database_path: str | Path,
    load_mode: Literal["full", "delta"],
    publish_date: str,
    run_id: str,
) -> dict[str, int]:
    database_file = Path(database_path)
    catalog_name = database_file.stem
    with duckdb.connect(str(database_file)) as connection:
        _ensure_required_raw_tables(connection)
        _ensure_schema(connection, catalog_name, schema_name=DUCKDB_SCHEMA)
        _ensure_schema(connection, catalog_name, schema_name=DUCKDB_STAGING_SCHEMA)
        if load_mode == "delta":
            _ensure_all_tables(connection, catalog_name=catalog_name, schema_name=DUCKDB_SCHEMA)
        _ensure_empty_tables(
            connection,
            catalog_name=catalog_name,
            schema_name=DUCKDB_STAGING_SCHEMA,
        )
        _build_staging_tables(
            connection,
            catalog_name=catalog_name,
            publish_date=publish_date,
            run_id=run_id,
        )
        if load_mode == "full":
            _replace_current_tables_from_schema(
                connection,
                catalog_name=catalog_name,
                source_schema_name=DUCKDB_STAGING_SCHEMA,
            )
        else:
            staged_counts = _staging_row_counts(connection, catalog_name=catalog_name)
            _upsert_current_tables_from_schema(
                connection,
                catalog_name=catalog_name,
                source_schema_name=DUCKDB_STAGING_SCHEMA,
                source_row_counts=staged_counts,
            )
    return _row_counts(database_file)
```

- [ ] **Step 4: Add raw table validation**

Add to `csv_transforms.py`:

```python
def _ensure_required_raw_tables(connection: duckdb.DuckDBPyConnection) -> None:
    required_tables = {
        GLEIF_RAW_LEI_RECORDS_TABLE,
        GLEIF_RAW_RELATIONSHIPS_TABLE,
        GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
    }
    existing_tables = {
        row[0]
        for row in connection.execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = ?
            """,
            [GLEIF_DLT_RAW_DATASET_NAME],
        ).fetchall()
    }
    missing_tables = sorted(required_tables - existing_tables)
    if missing_tables:
        raise ValueError(f"Missing GLEIF dlt raw tables: {missing_tables}")
```

- [ ] **Step 5: Add staging build SQL entrypoint**

Add to `csv_transforms.py`:

```python
def _build_staging_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    publish_date: str,
    run_id: str,
) -> None:
    _build_lei_records(connection, catalog_name=catalog_name, publish_date=publish_date, run_id=run_id)
    _build_lei_names(connection, catalog_name=catalog_name, publish_date=publish_date, run_id=run_id)
    _build_lei_addresses(connection, catalog_name=catalog_name, publish_date=publish_date, run_id=run_id)
    _build_lei_identifiers(connection, catalog_name=catalog_name, publish_date=publish_date, run_id=run_id)
    _build_relationships(connection, catalog_name=catalog_name, publish_date=publish_date, run_id=run_id)
    _build_relationship_periods(connection, catalog_name=catalog_name, publish_date=publish_date, run_id=run_id)
    _build_reporting_exceptions(connection, catalog_name=catalog_name, publish_date=publish_date, run_id=run_id)
    _empty_table(connection, catalog_name=catalog_name, table_name=tables.GLEIF_LEI_ISSUERS_TABLE)
    _empty_table(connection, catalog_name=catalog_name, table_name=tables.GLEIF_CODE_LIST_ENTRIES_TABLE)
```

Implement each `_build_*` function with `create or replace table` statements that target `_qualified_table(table_name, catalog_name=catalog_name, schema_name=DUCKDB_STAGING_SCHEMA)`. Use the dlt-normalized raw column names from the spec:

- `Entity.LegalName` becomes `entity_legal_name`
- `Entity.LegalName.xmllang` becomes `entity_legal_name_xmllang`
- `Relationship.Period.1.startDate` becomes `relationship_period_1_start_date`
- `Exception.Reason.1` becomes `exception_reason_1`

Use DuckDB casts such as `try_cast(entity_entity_creation_date as timestamp)` for timestamp columns and `try_cast(relationship_period_1_start_date as date)` for date columns.

- [ ] **Step 6: Wire duckdb_state to SQL transforms**

Modify `refresh_gleif_duckdb_state` in `duckdb_state.py` so the full branch calls:

```python
from dagster_v3.defs.gleif.csv_transforms import replace_current_from_dlt_raw_tables

row_counts = replace_current_from_dlt_raw_tables(
    database_path=database_path,
    load_mode="full",
    publish_date=manifest["publish_date"],
    run_id=manifest["run_id"],
)
```

Modify the delta branch so it calls:

```python
row_counts = replace_current_from_dlt_raw_tables(
    database_path=database_path,
    load_mode="delta",
    publish_date=manifest["publish_date"],
    run_id=manifest["run_id"],
)
```

Remove calls to `replace_current_state_from_manifest` and `apply_delta_state_from_manifest` from active asset execution.

- [ ] **Step 7: Run transform and state tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_csv_transforms.py tests/test_gleif_duckdb_state.py -q
```

Expected: tests pass after state tests are updated to seed dlt raw DuckDB tables.

- [ ] **Step 8: Commit Task 4**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/gleif/csv_transforms.py \
  corpscout/dagster_v3/src/dagster_v3/defs/gleif/duckdb_state.py \
  corpscout/dagster_v3/tests/test_gleif_csv_transforms.py \
  corpscout/dagster_v3/tests/test_gleif_duckdb_state.py
git commit -m "feat: normalize GLEIF dlt raw tables with DuckDB SQL"
```

## Task 5: End-to-End Jobs And ClickHouse Path

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_gleif_assets_dlt.py`

- [ ] **Step 1: Add ClickHouse log metadata test**

Add to `tests/test_gleif_assets_dlt.py`:

```python
def test_clickhouse_asset_uses_reference_duckdb_path() -> None:
    assert assets.GLEIF_DUCKDB_PATH.as_posix() == "data/gleif_reference.duckdb"
    assert assets.GLEIF_DUCKDB_SCHEMA == "gleif_reference.gleif"
```

- [ ] **Step 2: Update ClickHouse asset metadata logs**

In `gleif_reference_clickhouse`, keep `replace_duckdb_tables_in_clickhouse` and add a context parameter:

```python
def gleif_reference_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
```

After `row_counts` is returned, log per table:

```python
for table_name, row_count in row_counts.items():
    context.log.info(
        "published_gleif_clickhouse_table",
        extra={"table": table_name, "row_count": row_count},
    )
```

- [ ] **Step 3: Run Dagster definition and asset tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_assets_dlt.py -q
uv run dg check defs
```

Expected: asset tests pass and definitions validate.

- [ ] **Step 4: Commit Task 5**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/gleif/assets.py \
  corpscout/dagster_v3/tests/test_gleif_assets_dlt.py
git commit -m "feat: wire GLEIF dlt jobs to ClickHouse publication"
```

## Task 6: Full Verification And Rollout

**Files:**
- Modify only files changed by Tasks 1 through 5.

- [ ] **Step 1: Run focused test suite**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest \
  tests/test_gleif_source.py \
  tests/test_gleif_dlt_csv.py \
  tests/test_gleif_assets_dlt.py \
  tests/test_gleif_csv_transforms.py \
  tests/test_gleif_duckdb_state.py \
  -q
```

Expected: all listed tests pass.

- [ ] **Step 2: Run full Dagster validation**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
uv run dg list defs --assets --columns key,group,kinds,is_executable
```

Expected: definition check passes and the GLEIF raw dlt assets are listed as executable assets in group `gleif`.

- [ ] **Step 3: Run a small local materialization with fixture data**

Use the asset tests to build a fixture manifest and fake object store first. Then run only the dlt and DuckDB state path against fixture ZIPs:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_gleif_dlt_csv.py tests/test_gleif_csv_transforms.py -q
```

Expected: fixture CSV ZIPs load through dlt and normalized DuckDB tables are built.

- [ ] **Step 4: Run server bootstrap after merge**

On `companycollect`, after the branch is deployed and dependencies are synchronized:

```bash
cd ~/companycollect/corpscout/dagster_v3
uv sync
uv run dg check defs
uv run dg launch --job gleif_reference_bootstrap_job
```

Expected: the run downloads CSV ZIPs, loads three dlt raw DuckDB assets, builds normalized DuckDB state, publishes to ClickHouse, and applies raw retention.

- [ ] **Step 5: Verify server output**

Run:

```bash
cd ~/companycollect/corpscout
make clickhouse-client
```

Then execute:

```sql
select count() from corpscout.gleif_lei_records;
select count() from corpscout.gleif_lei_relationships;
select count() from corpscout.gleif_lei_reporting_exceptions;
```

Expected: counts are greater than zero after a successful full bootstrap.

- [ ] **Step 6: Commit final verification notes**

If Task 6 changes documentation or test fixtures, commit them:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
git add corpscout/dagster_v3/tests
git commit -m "test: verify GLEIF dlt CSV pipeline"
```

If Task 6 does not change files, do not create an empty commit.

## Self-Review

- The plan covers CSV raw download, manifest shape, dlt raw assets, DuckDB SQL normalization, ClickHouse publication, tests, and server rollout.
- The plan uses the existing `@dlt_assets` style instead of the dlt component because the runtime source depends on S3 manifest contents and extracted temporary file paths.
- The DuckDB database path is `data/gleif_reference.duckdb`, which gives catalog `gleif_reference` and normalized schema `gleif`.
- The dlt raw dataset is `gleif_raw`, and normalized staging stays in `gleif_staging`.
- The raw S3 download assets and the dlt raw DuckDB assets are separate, so retrying the dlt/normalization stage does not re-download Golden Copy files.
