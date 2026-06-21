# Open Page Rank Domains Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dagster source that downloads the DomCop/Open PageRank top 10M domains ZIP, stages it in S3 and DuckDB with dlt/Arrow, normalizes the current list, and atomically exports it to `corpscout.open_page_rank_domains`.

**Architecture:** Keep this as a source-specific module under `src/dagster_v3/defs/open_page_rank/`. The raw ZIP is streamed to object storage for audit and reprocessing, the CSV is extracted to a temp file and loaded into DuckDB through dlt `read_csv_duckdb(use_pyarrow=True)`, and a set-based DuckDB SQL transform produces the final current table. ClickHouse export uses the existing migration-owned schema and atomic stage-table swap helper.

**Tech Stack:** Dagster assets/jobs/schedules, `dagster_dlt`, dlt filesystem CSV source, PyArrow-backed CSV reading, DuckDB SQL, S3-compatible `ObjectStoreResource`, `dagster_clickhouse`, ClickHouse `ReplacingMergeTree`.

---

## File Structure

- Create: `src/dagster_v3/defs/open_page_rank/__init__.py`
- Create: `src/dagster_v3/defs/open_page_rank/source.py`
  - Download configuration, deterministic object keys, streaming download/upload helpers, manifest read/write, retention selection.
- Create: `src/dagster_v3/defs/open_page_rank/dlt_csv.py`
  - Extract one CSV from the ZIP, create the dlt DuckDB pipeline, load raw CSV through `read_csv_duckdb(use_pyarrow=True)`, and expose row-count helpers.
- Create: `src/dagster_v3/defs/open_page_rank/transforms.py`
  - Convert raw dlt CSV rows into the normalized current DuckDB table `open_page_rank.open_page_rank_domains`.
- Create: `src/dagster_v3/defs/open_page_rank/tables.py`
  - ClickHouse table name and export column constants matching migration `000040_corpscout_open_page_rank_domains`.
- Create: `src/dagster_v3/defs/open_page_rank/assets.py`
  - Dagster assets, job, schedule, and source-local `dg.Definitions`.
- Create: `src/dagster_v3/defs/open_page_rank/docs/open_page_rank-design.md`
  - Required source design doc following `docs/source-design-doc-template.md`.
- Modify: `src/dagster_v3/defs/common/resources.py`
  - Add streaming `upload_file` and `download_file` methods to avoid loading the 1GB ZIP into Python memory.
- Modify: `tests/test_clickhouse_migrations.py`
  - Keep the existing migration contract for `000040_corpscout_open_page_rank_domains`.
- Create: `tests/test_open_page_rank_source.py`
- Create: `tests/test_open_page_rank_dlt_csv.py`
- Create: `tests/test_open_page_rank_transforms.py`
- Create: `tests/test_open_page_rank_assets.py`

Do not modify the unrelated untracked Slovakia files unless the user explicitly asks for that work.

---

### Task 1: Add Streaming Object-Store File Methods

**Files:**
- Modify: `src/dagster_v3/defs/common/resources.py`
- Test: `tests/test_open_page_rank_source.py`

- [ ] **Step 1: Write the failing streaming-resource test**

Add this test double and assertion in `tests/test_open_page_rank_source.py`:

```python
from __future__ import annotations

from pathlib import Path
from typing import Any

from dagster_v3.defs.common.resources import ObjectStoreResource


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def create_bucket(self, Bucket: str) -> None:
        return None

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        self.objects[(Bucket, Key)] = Path(Filename).read_bytes()

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:
        Path(Filename).write_bytes(self.objects[(Bucket, Key)])

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if (Bucket, Key) not in self.objects:
            error = Exception("missing")
            setattr(error, "response", {"Error": {"Code": "404"}})
            raise error
        return {}

    def put_object(self, Bucket: str, Key: str, Body: Any) -> None:
        self.objects[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.read()

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        from io import BytesIO

        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def get_paginator(self, operation_name: str) -> Any:
        raise NotImplementedError(operation_name)

    def delete_objects(self, Bucket: str, Delete: dict[str, Any]) -> None:
        for item in Delete["Objects"]:
            self.objects.pop((Bucket, item["Key"]), None)


def test_object_store_uploads_and_downloads_files(tmp_path: Path) -> None:
    source = tmp_path / "source.zip"
    target = tmp_path / "target.zip"
    source.write_bytes(b"large-file-bytes")

    fake = FakeS3Client()
    object_store = ObjectStoreResource(s3_client=fake)
    object_store.upload_file("raw/source.zip", source, bucket="source-open-page-rank-domains")
    object_store.download_file("raw/source.zip", target, bucket="source-open-page-rank-domains")

    assert target.read_bytes() == b"large-file-bytes"
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
uv run pytest tests/test_open_page_rank_source.py::test_object_store_uploads_and_downloads_files -v
```

Expected: fail with `AttributeError: 'ObjectStoreResource' object has no attribute 'upload_file'`.

- [ ] **Step 3: Add concrete streaming methods**

Add these methods to `ObjectStoreResource`:

```python
    def upload_file(self, key: str, source_path: str | Path, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        self.client().upload_file(str(source_path), target_bucket, key)

    def download_file(self, key: str, target_path: str | Path, bucket: str | None = None) -> None:
        target_bucket = bucket or self.bucket
        self.client().download_file(target_bucket, key, str(target_path))
```

Extend `S3Client` with:

```python
    def upload_file(self, Filename: str, Bucket: str, Key: str) -> Any:
        ...

    def download_file(self, Bucket: str, Key: str, Filename: str) -> Any:
        ...
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
uv run pytest tests/test_open_page_rank_source.py::test_object_store_uploads_and_downloads_files -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/common/resources.py corpscout/dagster_v3/tests/test_open_page_rank_source.py
git commit -m "feat(dagster): add streaming object-store file helpers"
```

---

### Task 2: Add Open PageRank Source Download Logic

**Files:**
- Create: `src/dagster_v3/defs/open_page_rank/__init__.py`
- Create: `src/dagster_v3/defs/open_page_rank/source.py`
- Modify: `tests/test_open_page_rank_source.py`

- [ ] **Step 1: Write failing tests for keys, manifest, and retention**

Append these tests to `tests/test_open_page_rank_source.py`:

```python
from datetime import UTC, datetime

from dagster_v3.defs.open_page_rank.source import (
    OPEN_PAGE_RANK_RAW_BUCKET,
    OpenPageRankRawFile,
    build_manifest,
    manifest_object_key,
    raw_file_object_key,
    select_open_page_rank_raw_keys_for_deletion,
)


def test_open_page_rank_object_keys_are_run_scoped() -> None:
    assert raw_file_object_key(run_id="run-1", retrieved_date="2026-06-21") == (
        "raw/run_id=run-1/retrieved_date=2026-06-21/source.csv.zip"
    )
    assert manifest_object_key(run_id="run-1", retrieved_date="2026-06-21") == (
        "raw/run_id=run-1/retrieved_date=2026-06-21/manifest.json"
    )


def test_open_page_rank_manifest_records_source_file() -> None:
    manifest = build_manifest(
        run_id="run-1",
        retrieved_at=datetime(2026, 6, 21, 10, 30, tzinfo=UTC),
        file=OpenPageRankRawFile(
            source_url="https://www.domcop.com/files/top/top10milliondomains.csv.zip",
            s3_key="raw/run_id=run-1/retrieved_date=2026-06-21/source.csv.zip",
            size_bytes=123,
            sha256="a" * 64,
        ),
    )

    assert manifest["source"] == "open_page_rank"
    assert manifest["run_id"] == "run-1"
    assert manifest["retrieved_date"] == "2026-06-21"
    assert manifest["file"]["s3_key"].endswith("/source.csv.zip")


def test_open_page_rank_retention_keeps_newest_raw_file_and_manifests() -> None:
    keys = [
        "raw/run_id=old/retrieved_date=2026-06-14/source.csv.zip",
        "raw/run_id=old/retrieved_date=2026-06-14/manifest.json",
        "raw/run_id=new/retrieved_date=2026-06-21/source.csv.zip",
        "raw/run_id=new/retrieved_date=2026-06-21/manifest.json",
    ]

    assert select_open_page_rank_raw_keys_for_deletion(keys) == [
        "raw/run_id=old/retrieved_date=2026-06-14/source.csv.zip"
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_open_page_rank_source.py -v
```

Expected: fail because `dagster_v3.defs.open_page_rank.source` does not exist.

- [ ] **Step 3: Add the source module**

Create `src/dagster_v3/defs/open_page_rank/__init__.py` as an empty package marker.

Create `src/dagster_v3/defs/open_page_rank/source.py` with these public contracts:

```python
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from pydantic import field_validator

from dagster_v3.defs.common.resources import ObjectStoreResource

OPEN_PAGE_RANK_RAW_BUCKET = "source-open-page-rank-domains"
OPEN_PAGE_RANK_SOURCE_URL = "https://www.domcop.com/files/top/top10milliondomains.csv.zip"


class OpenPageRankDownloadConfig(dg.Config):
    source_url: str = OPEN_PAGE_RANK_SOURCE_URL
    request_timeout_seconds: int = 600

    @field_validator("request_timeout_seconds")
    @classmethod
    def validate_request_timeout_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("request_timeout_seconds must be greater than zero")
        return value


@dataclass(frozen=True)
class OpenPageRankRawFile:
    source_url: str
    s3_key: str
    size_bytes: int
    sha256: str


def raw_file_object_key(*, run_id: str, retrieved_date: str) -> str:
    return f"raw/run_id={run_id}/retrieved_date={retrieved_date}/source.csv.zip"


def manifest_object_key(*, run_id: str, retrieved_date: str) -> str:
    return f"raw/run_id={run_id}/retrieved_date={retrieved_date}/manifest.json"


def build_manifest(
    *,
    run_id: str,
    retrieved_at: datetime,
    file: OpenPageRankRawFile,
) -> dict[str, Any]:
    return {
        "source": "open_page_rank",
        "source_list_name": "domcop_top_10m_domains",
        "run_id": run_id,
        "retrieved_at": retrieved_at.isoformat(),
        "retrieved_date": retrieved_at.date().isoformat(),
        "file": {
            "source_url": file.source_url,
            "s3_key": file.s3_key,
            "size_bytes": file.size_bytes,
            "sha256": file.sha256,
        },
    }
```

Add the implementation functions in the same file:

```python
def download_raw_file(
    *,
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    config: OpenPageRankDownloadConfig,
    run_id: str,
    retrieved_at: datetime,
    session: requests.Session | None = None,
) -> dg.MaterializeResult:
    object_store.ensure_bucket(OPEN_PAGE_RANK_RAW_BUCKET)
    retrieved_date = retrieved_at.date().isoformat()
    s3_key = raw_file_object_key(run_id=run_id, retrieved_date=retrieved_date)
    manifest_key = manifest_object_key(run_id=run_id, retrieved_date=retrieved_date)
    temp_path = Path(context.run_id).with_suffix(".csv.zip")
    temp_path = Path("data/tmp/open_page_rank") / temp_path.name
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        size_bytes, digest = _stream_download_to_path(
            source_url=config.source_url,
            target_path=temp_path,
            timeout_seconds=config.request_timeout_seconds,
            session=session or requests.Session(),
        )
        object_store.upload_file(s3_key, temp_path, bucket=OPEN_PAGE_RANK_RAW_BUCKET)
    finally:
        temp_path.unlink(missing_ok=True)

    raw_file = OpenPageRankRawFile(
        source_url=config.source_url,
        s3_key=s3_key,
        size_bytes=size_bytes,
        sha256=digest,
    )
    manifest = build_manifest(run_id=run_id, retrieved_at=retrieved_at, file=raw_file)
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, sort_keys=True),
        bucket=OPEN_PAGE_RANK_RAW_BUCKET,
    )
    return dg.MaterializeResult(
        metadata={
            "s3_bucket": OPEN_PAGE_RANK_RAW_BUCKET,
            "s3_key": s3_key,
            "manifest_key": manifest_key,
            "size_bytes": size_bytes,
            "sha256": digest,
            "source_url": config.source_url,
        }
    )
```

Use a chunked helper:

```python
def _stream_download_to_path(
    *,
    source_url: str,
    target_path: Path,
    timeout_seconds: int,
    session: requests.Session,
) -> tuple[int, str]:
    response = session.get(source_url, stream=True, timeout=timeout_seconds)
    response.raise_for_status()
    digest = sha256()
    size_bytes = 0
    with target_path.open("wb") as target:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            if not chunk:
                continue
            digest.update(chunk)
            size_bytes += len(chunk)
            target.write(chunk)
    return size_bytes, digest.hexdigest()
```

Add manifest selection and retention:

```python
def manifest_for_run(object_store: ObjectStoreResource, run_id: str) -> dict[str, Any]:
    manifests = [
        json.loads(object_store.read_bytes(key, bucket=OPEN_PAGE_RANK_RAW_BUCKET))
        for key in object_store.list_keys("raw/", bucket=OPEN_PAGE_RANK_RAW_BUCKET)
        if key.endswith("/manifest.json") and f"/run_id={run_id}/" in key
    ]
    if not manifests:
        raise ValueError(f"No Open PageRank manifest found for Dagster run_id={run_id}")
    return max(manifests, key=lambda item: str(item["retrieved_at"]))


def select_open_page_rank_raw_keys_for_deletion(keys: list[str] | tuple[str, ...]) -> list[str]:
    raw_keys = [key for key in keys if key.endswith("/source.csv.zip")]
    newest_key = max(raw_keys) if raw_keys else ""
    return [key for key in raw_keys if key != newest_key]
```

- [ ] **Step 4: Run source tests**

Run:

```bash
uv run pytest tests/test_open_page_rank_source.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank/__init__.py corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank/source.py corpscout/dagster_v3/tests/test_open_page_rank_source.py
git commit -m "feat(open-page-rank): add raw archive download helpers"
```

---

### Task 3: Add dlt/Arrow CSV Loading Into DuckDB

**Files:**
- Create: `src/dagster_v3/defs/open_page_rank/dlt_csv.py`
- Test: `tests/test_open_page_rank_dlt_csv.py`

- [ ] **Step 1: Write a failing ZIP extraction and dlt load test**

Create `tests/test_open_page_rank_dlt_csv.py`:

```python
from __future__ import annotations

from pathlib import Path
import zipfile

from dagster_v3.defs.open_page_rank.dlt_csv import (
    ExtractedOpenPageRankCsv,
    OPEN_PAGE_RANK_RAW_TABLE,
    extract_single_csv_member,
    load_open_page_rank_raw_table,
    raw_table_row_count,
)


def test_extract_single_csv_member_rejects_zip_without_csv(tmp_path: Path) -> None:
    archive_path = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("notes.txt", "not csv")

    try:
        extract_single_csv_member(zip_path=archive_path, output_dir=tmp_path)
    except ValueError as exc:
        assert "contains no CSV members" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_dlt_loads_open_page_rank_csv_with_arrow(tmp_path: Path) -> None:
    csv_path = tmp_path / "top10milliondomains.csv"
    csv_path.write_text(
        "Rank,Domain,Open Page Rank,Extension\n"
        "1,google.com,10.00,com\n"
        "2,example.co.uk,7.50,uk\n"
    )
    database_path = tmp_path / "open_page_rank_source.duckdb"

    counts = load_open_page_rank_raw_table(
        database_path=database_path,
        extracted_file=ExtractedOpenPageRankCsv(csv_path=csv_path),
    )

    assert counts == {OPEN_PAGE_RANK_RAW_TABLE: 2}
    assert raw_table_row_count(database_path) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_open_page_rank_dlt_csv.py -v
```

Expected: fail because `dagster_v3.defs.open_page_rank.dlt_csv` does not exist.

- [ ] **Step 3: Add the dlt CSV loader**

Create `src/dagster_v3/defs/open_page_rank/dlt_csv.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import shutil
import zipfile

import dlt as dlt_lib
import duckdb
from dlt.sources.filesystem import filesystem, read_csv_duckdb

OPEN_PAGE_RANK_DLT_PIPELINE_NAME = "open_page_rank_raw_csv_duckdb"
OPEN_PAGE_RANK_DLT_DATASET_NAME = "open_page_rank_raw"
OPEN_PAGE_RANK_RAW_TABLE = "open_page_rank_raw_domains"


@dataclass(frozen=True)
class ExtractedOpenPageRankCsv:
    csv_path: Path


def extract_single_csv_member(*, zip_path: str | Path, output_dir: str | Path) -> Path:
    archive_path = Path(zip_path)
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path) as archive:
        csv_members = [
            info for info in archive.infolist()
            if not info.is_dir() and info.filename.lower().endswith(".csv")
        ]
        if not csv_members:
            raise ValueError(f"Open PageRank ZIP {archive_path} contains no CSV members")
        if len(csv_members) > 1:
            names = [info.filename for info in csv_members]
            raise ValueError(f"Open PageRank ZIP {archive_path} contains multiple CSV members: {names}")
        output_path = target_dir / "open_page_rank_domains.csv"
        with archive.open(csv_members[0]) as source, output_path.open("wb") as target:
            shutil.copyfileobj(source, target, length=8 * 1024 * 1024)
        return output_path


def open_page_rank_csv_dlt_pipeline(database_path: str | Path) -> Any:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    working_dir = database_file.parent / ".dlt" / "open_page_rank"
    working_dir.mkdir(parents=True, exist_ok=True)
    return dlt_lib.pipeline(
        pipeline_name=OPEN_PAGE_RANK_DLT_PIPELINE_NAME,
        destination=dlt_lib.destinations.duckdb(str(database_file)),
        dataset_name=OPEN_PAGE_RANK_DLT_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(working_dir),
    )
```

Add the source and loader:

```python
@dlt_lib.source(name="open_page_rank_csv")
def open_page_rank_csv_dlt_source(extracted_file: ExtractedOpenPageRankCsv) -> list[Any]:
    resource = filesystem(
        bucket_url=str(extracted_file.csv_path.parent),
        file_glob=extracted_file.csv_path.name,
    ) | read_csv_duckdb(use_pyarrow=True, header=True, all_varchar=True)
    resource = resource.with_name(OPEN_PAGE_RANK_RAW_TABLE)
    resource.apply_hints(write_disposition="replace")
    return [resource]


def load_open_page_rank_raw_table(
    *,
    database_path: str | Path,
    extracted_file: ExtractedOpenPageRankCsv,
) -> dict[str, int]:
    pipeline = open_page_rank_csv_dlt_pipeline(database_path)
    pipeline.drop_pending_packages()
    load_info = pipeline.run(open_page_rank_csv_dlt_source(extracted_file))
    load_info.raise_on_failed_jobs()
    return {OPEN_PAGE_RANK_RAW_TABLE: raw_table_row_count(database_path)}


def raw_table_row_count(database_path: str | Path) -> int:
    database_file = Path(database_path)
    if not database_file.exists():
        return 0
    with duckdb.connect(str(database_file), read_only=True) as connection:
        exists = connection.execute(
            """
            select 1
            from information_schema.tables
            where table_schema = ?
              and table_name = ?
            limit 1
            """,
            [OPEN_PAGE_RANK_DLT_DATASET_NAME, OPEN_PAGE_RANK_RAW_TABLE],
        ).fetchone()
        if exists is None:
            return 0
        return int(
            connection.execute(
                f'select count(*) from "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"'
            ).fetchone()[0]
        )
```

- [ ] **Step 4: Run dlt loader tests**

Run:

```bash
uv run pytest tests/test_open_page_rank_dlt_csv.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank/dlt_csv.py corpscout/dagster_v3/tests/test_open_page_rank_dlt_csv.py
git commit -m "feat(open-page-rank): load raw csv into duckdb with dlt"
```

---

### Task 4: Add DuckDB Normalization Transform

**Files:**
- Create: `src/dagster_v3/defs/open_page_rank/transforms.py`
- Create: `src/dagster_v3/defs/open_page_rank/tables.py`
- Test: `tests/test_open_page_rank_transforms.py`

- [ ] **Step 1: Write the failing transform test**

Create `tests/test_open_page_rank_transforms.py`:

```python
from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.open_page_rank.dlt_csv import (
    OPEN_PAGE_RANK_DLT_DATASET_NAME,
    OPEN_PAGE_RANK_RAW_TABLE,
)
from dagster_v3.defs.open_page_rank.tables import (
    OPEN_PAGE_RANK_DOMAINS_COLUMNS,
    OPEN_PAGE_RANK_DOMAINS_TABLE,
)
from dagster_v3.defs.open_page_rank.transforms import replace_current_open_page_rank_domains


def test_replace_current_open_page_rank_domains_normalizes_raw_rows(tmp_path: Path) -> None:
    database_path = tmp_path / "open_page_rank_source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f'create schema "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"')
        connection.execute(
            f'''
            create table "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"
            (
                rank varchar,
                domain varchar,
                open_page_rank varchar,
                extension varchar
            )
            '''
        )
        connection.execute(
            f'''
            insert into "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"
            values ('1', ' Google.COM ', '10.00', 'COM'), ('bad', '', 'x', '')
            '''
        )

    row_count = replace_current_open_page_rank_domains(
        database_path=database_path,
        source_url="https://www.domcop.com/files/top/top10milliondomains.csv.zip",
        source_run_id="run-1",
        retrieved_date="2026-06-21",
        retrieved_at="2026-06-21T10:30:00+00:00",
    )

    assert row_count == 1
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            f'''
            select {", ".join(OPEN_PAGE_RANK_DOMAINS_COLUMNS)}
            from open_page_rank.{OPEN_PAGE_RANK_DOMAINS_TABLE}
            '''
        ).fetchall()

    assert rows[0][0:9] == (
        "open_page_rank",
        "domcop_top_10m_domains",
        "run-1",
        "open_page_rank:1:google.com",
        1,
        "google.com",
        "google.com",
        "com",
        10.0,
    )
```

- [ ] **Step 2: Run transform test to verify it fails**

Run:

```bash
uv run pytest tests/test_open_page_rank_transforms.py -v
```

Expected: fail because `tables.py` and `transforms.py` do not exist.

- [ ] **Step 3: Add table constants**

Create `src/dagster_v3/defs/open_page_rank/tables.py`:

```python
OPEN_PAGE_RANK_DOMAINS_TABLE = "open_page_rank_domains"

OPEN_PAGE_RANK_TABLES = (OPEN_PAGE_RANK_DOMAINS_TABLE,)

OPEN_PAGE_RANK_DOMAINS_COLUMNS = (
    "source_system",
    "source_list_name",
    "source_run_id",
    "source_record_id",
    "source_rank",
    "domain",
    "root_domain",
    "domain_extension",
    "open_page_rank",
    "source_url",
    "retrieved_date",
    "retrieved_at",
    "resolved_at",
)

OPEN_PAGE_RANK_TABLE_COLUMNS = {
    OPEN_PAGE_RANK_DOMAINS_TABLE: OPEN_PAGE_RANK_DOMAINS_COLUMNS,
}
```

- [ ] **Step 4: Add set-based SQL transform**

Create `src/dagster_v3/defs/open_page_rank/transforms.py`:

```python
from __future__ import annotations

from pathlib import Path

import duckdb

from dagster_v3.defs.open_page_rank.dlt_csv import (
    OPEN_PAGE_RANK_DLT_DATASET_NAME,
    OPEN_PAGE_RANK_RAW_TABLE,
)
from dagster_v3.defs.open_page_rank.tables import OPEN_PAGE_RANK_DOMAINS_TABLE


OPEN_PAGE_RANK_DUCKDB_SCHEMA = "open_page_rank"


def replace_current_open_page_rank_domains(
    *,
    database_path: str | Path,
    source_url: str,
    source_run_id: str,
    retrieved_date: str,
    retrieved_at: str,
) -> int:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f'create schema if not exists "{OPEN_PAGE_RANK_DUCKDB_SCHEMA}"')
        connection.execute(
            f'''
            create or replace table "{OPEN_PAGE_RANK_DUCKDB_SCHEMA}"."{OPEN_PAGE_RANK_DOMAINS_TABLE}" as
            with raw as (
                select
                    try_cast(nullif(trim(rank), '') as uinteger) as source_rank,
                    lower(trim(domain)) as domain,
                    lower(trim(extension)) as domain_extension,
                    try_cast(nullif(trim(open_page_rank), '') as double) as open_page_rank
                from "{OPEN_PAGE_RANK_DLT_DATASET_NAME}"."{OPEN_PAGE_RANK_RAW_TABLE}"
            )
            select
                'open_page_rank' as source_system,
                'domcop_top_10m_domains' as source_list_name,
                ? as source_run_id,
                concat('open_page_rank:', cast(source_rank as varchar), ':', domain) as source_record_id,
                source_rank,
                domain,
                domain as root_domain,
                domain_extension,
                open_page_rank,
                ? as source_url,
                cast(? as date) as retrieved_date,
                cast(? as timestamp) as retrieved_at,
                now() as resolved_at
            from raw
            where source_rank is not null
              and domain != ''
            ''',
            [source_run_id, source_url, retrieved_date, retrieved_at],
        )
        return int(
            connection.execute(
                f'select count(*) from "{OPEN_PAGE_RANK_DUCKDB_SCHEMA}"."{OPEN_PAGE_RANK_DOMAINS_TABLE}"'
            ).fetchone()[0]
        )
```

- [ ] **Step 5: Run transform test**

Run:

```bash
uv run pytest tests/test_open_page_rank_transforms.py -v
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank/tables.py corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank/transforms.py corpscout/dagster_v3/tests/test_open_page_rank_transforms.py
git commit -m "feat(open-page-rank): normalize raw rankings in duckdb"
```

---

### Task 5: Add Dagster Assets, Job, and Schedule

**Files:**
- Create: `src/dagster_v3/defs/open_page_rank/assets.py`
- Test: `tests/test_open_page_rank_assets.py`

- [ ] **Step 1: Write failing asset-shape tests**

Create `tests/test_open_page_rank_assets.py`:

```python
from __future__ import annotations

import dagster as dg

from dagster_v3.defs.open_page_rank.assets import defs


def test_open_page_rank_defs_register_expected_assets_job_and_schedule() -> None:
    resolved = defs.get_repository_def()

    assert resolved.has_asset_key(dg.AssetKey("open_page_rank_raw_archive"))
    assert resolved.has_asset_key(dg.AssetKey("open_page_rank_raw_duckdb"))
    assert resolved.has_asset_key(dg.AssetKey("open_page_rank_domains_duckdb"))
    assert resolved.has_asset_key(dg.AssetKey("open_page_rank_domains_clickhouse"))
    assert resolved.has_asset_key(dg.AssetKey("open_page_rank_raw_retention"))
    assert resolved.has_job("open_page_rank_domains_refresh_job")
    assert resolved.get_schedule_def("open_page_rank_domains_weekly").job_name == (
        "open_page_rank_domains_refresh_job"
    )
```

- [ ] **Step 2: Run asset test to verify it fails**

Run:

```bash
uv run pytest tests/test_open_page_rank_assets.py -v
```

Expected: fail because `assets.py` does not exist.

- [ ] **Step 3: Add asset constants and raw archive asset**

Create `src/dagster_v3/defs/open_page_rank/assets.py` with:

```python
from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
import tempfile
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.open_page_rank import tables
from dagster_v3.defs.open_page_rank.dlt_csv import (
    ExtractedOpenPageRankCsv,
    extract_single_csv_member,
    load_open_page_rank_raw_table,
)
from dagster_v3.defs.open_page_rank.source import (
    OPEN_PAGE_RANK_RAW_BUCKET,
    OpenPageRankDownloadConfig,
    download_raw_file,
    manifest_for_run,
    select_open_page_rank_raw_keys_for_deletion,
)
from dagster_v3.defs.open_page_rank.transforms import (
    OPEN_PAGE_RANK_DUCKDB_SCHEMA,
    replace_current_open_page_rank_domains,
)

GROUP_NAME = "open_page_rank"
OPEN_PAGE_RANK_DUCKDB_PATH = Path("data/open_page_rank_source.duckdb")
OPEN_PAGE_RANK_DUCKDB_POOL = "open_page_rank_duckdb"
MIN_OPEN_PAGE_RANK_ROWS = 9_000_000
```

Add the raw archive asset:

```python
@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "open_page_rank"},
    description="Downloads the DomCop/Open PageRank top 10M domains ZIP into object storage.",
)
def open_page_rank_raw_archive(
    context: dg.AssetExecutionContext,
    config: OpenPageRankDownloadConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return download_raw_file(
        context=context,
        object_store=object_store,
        config=config,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
    )
```

- [ ] **Step 4: Add DuckDB raw and normalized assets**

Append:

```python
@dg.asset(
    deps=[dg.AssetKey("open_page_rank_raw_archive")],
    group_name=GROUP_NAME,
    kinds={"python", "dlt", "duckdb", "open_page_rank"},
    pool=OPEN_PAGE_RANK_DUCKDB_POOL,
    description="Loads the Open PageRank CSV from the raw ZIP into DuckDB using dlt and Arrow.",
)
def open_page_rank_raw_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = manifest_for_run(object_store, context.run_id)
    with tempfile.TemporaryDirectory(prefix="open-page-rank-") as temp_path:
        temp_dir = Path(temp_path)
        zip_path = temp_dir / "source.csv.zip"
        object_store.download_file(
            str(manifest["file"]["s3_key"]),
            zip_path,
            bucket=OPEN_PAGE_RANK_RAW_BUCKET,
        )
        csv_path = extract_single_csv_member(zip_path=zip_path, output_dir=temp_dir)
        row_counts = load_open_page_rank_raw_table(
            database_path=OPEN_PAGE_RANK_DUCKDB_PATH,
            extracted_file=ExtractedOpenPageRankCsv(csv_path=csv_path),
        )
    row_count = int(next(iter(row_counts.values())))
    if row_count < MIN_OPEN_PAGE_RANK_ROWS:
        raise ValueError(
            f"Open PageRank raw load produced too few rows: {row_count} < {MIN_OPEN_PAGE_RANK_ROWS}"
        )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "source_run_id": str(manifest["run_id"]),
            "source_url": str(manifest["file"]["source_url"]),
        }
    )


@dg.asset(
    deps=[dg.AssetKey("open_page_rank_raw_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql", "open_page_rank"},
    pool=OPEN_PAGE_RANK_DUCKDB_POOL,
    description="Normalizes the Open PageRank raw CSV table into the current DuckDB export table.",
)
def open_page_rank_domains_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = manifest_for_run(object_store, context.run_id)
    row_count = replace_current_open_page_rank_domains(
        database_path=OPEN_PAGE_RANK_DUCKDB_PATH,
        source_url=str(manifest["file"]["source_url"]),
        source_run_id=str(manifest["run_id"]),
        retrieved_date=str(manifest["retrieved_date"]),
        retrieved_at=str(manifest["retrieved_at"]),
    )
    return dg.MaterializeResult(metadata={"row_count": row_count})
```

- [ ] **Step 5: Add ClickHouse export and retention assets**

Append:

```python
@dg.asset(
    deps=[dg.AssetKey("open_page_rank_domains_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "open_page_rank"},
    pool=OPEN_PAGE_RANK_DUCKDB_POOL,
    description="Exports current Open PageRank domains from DuckDB to ClickHouse.",
)
def open_page_rank_domains_clickhouse(clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.OPEN_PAGE_RANK_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_count = export_duckdb_table_to_clickhouse(
            duckdb_path=OPEN_PAGE_RANK_DUCKDB_PATH,
            clickhouse_client=client,
            duckdb_schema=OPEN_PAGE_RANK_DUCKDB_SCHEMA,
            duckdb_table=tables.OPEN_PAGE_RANK_DOMAINS_TABLE,
            clickhouse_database=RESOLVED_DATABASE,
            clickhouse_table=tables.OPEN_PAGE_RANK_DOMAINS_TABLE,
            columns=tables.OPEN_PAGE_RANK_DOMAINS_COLUMNS,
            truncate=True,
        )
    return dg.MaterializeResult(metadata={"row_count": row_count})


@dg.asset(
    deps=[dg.AssetKey("open_page_rank_domains_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "open_page_rank"},
    description="Deletes old Open PageRank raw ZIP blobs while preserving manifests.",
)
def open_page_rank_raw_retention(object_store: ObjectStoreResource) -> dg.MaterializeResult:
    keys = object_store.list_keys("raw/", bucket=OPEN_PAGE_RANK_RAW_BUCKET)
    keys_to_delete = select_open_page_rank_raw_keys_for_deletion(keys)
    deleted_count = object_store.delete_keys(tuple(keys_to_delete), bucket=OPEN_PAGE_RANK_RAW_BUCKET)
    return dg.MaterializeResult(metadata={"deleted_key_count": deleted_count})
```

- [ ] **Step 6: Add job, stopped weekly schedule, and Definitions**

Append:

```python
open_page_rank_domains_refresh_job = dg.define_asset_job(
    name="open_page_rank_domains_refresh_job",
    selection=[
        "open_page_rank_raw_archive",
        "open_page_rank_raw_duckdb",
        "open_page_rank_domains_duckdb",
        "open_page_rank_domains_clickhouse",
        "open_page_rank_raw_retention",
    ],
)

open_page_rank_domains_weekly = dg.ScheduleDefinition(
    name="open_page_rank_domains_weekly",
    job=open_page_rank_domains_refresh_job,
    cron_schedule="45 2 * * 0",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        open_page_rank_raw_archive,
        open_page_rank_raw_duckdb,
        open_page_rank_domains_duckdb,
        open_page_rank_domains_clickhouse,
        open_page_rank_raw_retention,
    ],
    jobs=[open_page_rank_domains_refresh_job],
    schedules=[open_page_rank_domains_weekly],
)
```

- [ ] **Step 7: Run asset tests**

Run:

```bash
uv run pytest tests/test_open_page_rank_assets.py -v
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank/assets.py corpscout/dagster_v3/tests/test_open_page_rank_assets.py
git commit -m "feat(open-page-rank): add dagster assets and refresh job"
```

---

### Task 6: Add Source Design Documentation

**Files:**
- Create: `src/dagster_v3/defs/open_page_rank/docs/open_page_rank-design.md`

- [ ] **Step 1: Create the design doc**

Create `src/dagster_v3/defs/open_page_rank/docs/open_page_rank-design.md`:

```markdown
# Open PageRank domains design doc

## 1. Source overview
- **Country / registry**: Global domain rank list from DomCop/Open PageRank.
- **Module**: `defs/open_page_rank/` · DuckDB `data/open_page_rank_source.duckdb` · pool `open_page_rank_duckdb`
- **ClickHouse tables**: `corpscout.open_page_rank_domains` (`000040_corpscout_open_page_rank_domains`)
- **Datasets used**:
  | dataset | url | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | DomCop/Open PageRank top 10M domains | https://www.domcop.com/files/top/top10milliondomains.csv.zip | ZIP CSV | about 1GB zipped | source-managed; scheduled weekly in Corpscout | no |
- **Entity key**: `domain` with `source_rank`; expected record count about 10,000,000.

## 2. Ingest mode (§2 of guidelines) — and why
- Chosen: bulk file full-refresh.
- Why: the source publishes one full ZIP CSV, so partitions and API pagination add state without benefit.
- Format choice: CSV inside ZIP. The source page exposes columns `Rank`, `Domain`, `Open Page Rank`, and `Extension`.
- If partitioned: not partitioned; each run replaces the current list.

## 3. Loading (§3)
- Reader: dlt filesystem resource piped to `read_csv_duckdb(use_pyarrow=True, header=True, all_varchar=True)`.
- Why: avoids Python row loops and lets DuckDB/Arrow handle the large CSV.
- Staging shape: raw dlt table `open_page_rank_raw.open_page_rank_raw_domains`; values remain text until transform.
- Checkpoints / per-file split: one raw ZIP in S3 per run, one extracted CSV in a temporary directory.

## 4. Transform (§5)
- Mechanism: set-based DuckDB SQL.
- Shape: cast rank to `UInt32`, lower-case `domain` and `extension`, cast Open PageRank to `Float64`, attach run/source metadata.

## 5. ClickHouse schema — and DDL deviations
- Table + grain: `open_page_rank_domains`, one row per `(source_system, source_list_name, source_rank, domain)` in the current source snapshot.
- `ORDER BY`: `(root_domain, source_system, source_list_name, domain)` · engine: `ReplacingMergeTree(resolved_at)`.
- Deviation: no company key, contacts, country, industry, translation, or currency columns because this is a domain rank list, not a company registry.
- Export subset: all columns in `OPEN_PAGE_RANK_DOMAINS_COLUMNS`; no raw payload columns exported.

## 6. Translation (§8)
No translatable fields. Domain names and source labels are not translated.

## 6b. Contacts (§8b) — MANDATORY to assess
No company contact data is present. This source is a ranked domain universe only.

## 7. Currency (§7)
No monetary amounts.

## 8. Scheduling (§9)
- Job: `open_page_rank_domains_refresh_job`.
- Schedule: `open_page_rank_domains_weekly`, Sunday 02:45 UTC, default stopped until first live validation.

## 9. Issues found during processing
- The source ZIP is large, so raw download and S3 read/write must use file streaming methods, not `read_bytes` or `write_bytes`.

## 10. Verification
- Tests: `tests/test_open_page_rank_source.py`, `tests/test_open_page_rank_dlt_csv.py`, `tests/test_open_page_rank_transforms.py`, `tests/test_open_page_rank_assets.py`, `tests/test_clickhouse_migrations.py`.
- Live: apply ClickHouse migrations, materialize `open_page_rank_domains_refresh_job`, then verify ClickHouse row count is close to 10M and top ranks are populated.
```

- [ ] **Step 2: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank/docs/open_page_rank-design.md
git commit -m "docs(open-page-rank): document source design"
```

---

### Task 7: Verify Definitions, Migration Contract, and Live Launch Commands

**Files:**
- Modify only if tests expose a real bug.

- [ ] **Step 1: Run source-specific tests**

Run:

```bash
uv run pytest \
  tests/test_open_page_rank_source.py \
  tests/test_open_page_rank_dlt_csv.py \
  tests/test_open_page_rank_transforms.py \
  tests/test_open_page_rank_assets.py \
  tests/test_clickhouse_migrations.py \
  -v
```

Expected: all selected tests pass.

- [ ] **Step 2: Validate Dagster definitions**

Run:

```bash
uv run dg check defs
```

Expected: `All component YAML validated successfully.` and no import/definition errors.

- [ ] **Step 3: Confirm job is discoverable**

Run:

```bash
uv run dg list defs --jobs --json | rg "open_page_rank_domains_refresh_job"
```

Expected: output contains `open_page_rank_domains_refresh_job`.

- [ ] **Step 4: Apply ClickHouse migration**

Run from `corpscout/`:

```bash
make clickhouse-migrate-up
```

Expected: migration `000040_corpscout_open_page_rank_domains` is applied, or `no change` if it was already applied.

- [ ] **Step 5: Launch the refresh job**

Run from `corpscout/dagster_v3/` on the server with enough disk/network capacity:

```bash
uv run dg launch --job open_page_rank_domains_refresh_job
```

Expected: the run materializes all five source assets and does not exceed Python memory by buffering the ZIP.

- [ ] **Step 6: Verify ClickHouse row count and sample**

Run:

```bash
clickhouse-client --query "
select
  count() as rows,
  min(source_rank) as min_rank,
  max(source_rank) as max_rank
from corpscout.open_page_rank_domains
"

clickhouse-client --query "
select source_rank, domain, open_page_rank
from corpscout.open_page_rank_domains
order by source_rank
limit 10
"
```

Expected: row count is close to 10,000,000, `min_rank = 1`, and top rows have non-empty domains.

- [ ] **Step 7: Commit final verification adjustments**

If verification required code fixes, commit them:

```bash
git status --short
git add corpscout/dagster_v3/src/dagster_v3/defs/open_page_rank corpscout/dagster_v3/tests corpscout/dagster_v3/src/dagster_v3/defs/common/resources.py
git commit -m "test(open-page-rank): verify refresh pipeline"
```

---

## Self-Review

- Spec coverage: plan covers raw download to S3, dlt/Arrow CSV processing, DuckDB normalization, ClickHouse export, retention, schedule, docs, and tests.
- Placeholder scan: no unresolved placeholders are required to implement the source. The source URL is explicit: `https://www.domcop.com/files/top/top10milliondomains.csv.zip`.
- Type consistency: table name is consistently `open_page_rank_domains`; source module path is consistently `dagster_v3.defs.open_page_rank`; DuckDB schema is consistently `open_page_rank`; raw dlt dataset is consistently `open_page_rank_raw`.
