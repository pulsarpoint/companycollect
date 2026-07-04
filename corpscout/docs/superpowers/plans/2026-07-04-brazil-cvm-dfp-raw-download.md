# Brazil CVM DFP Raw Download Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first Brazil CVM asset: a year-partitioned Dagster asset that downloads all historical DFP yearly ZIP archives, including 2026, into S3/RustFS and skips the download when that year archive already exists.

**Architecture:** Create a new `dagster_v3.defs.brazil_cvm` source package. Keep the first asset raw-only: one static year partition maps to one CVM `dfp_cia_aberta_{year}.zip` source URL and one deterministic object key. Object existence is the idempotency boundary; if `year=<YYYY>/archive.zip` already exists, the asset returns reused metadata without downloading or overwriting.

**Tech Stack:** Dagster assets, `StaticPartitionsDefinition`, `ObjectStoreResource`, Python `requests`, S3-compatible RustFS, pytest.

---

## Scope

In scope:

- CVM DFP yearly ZIP download only.
- Historical years `2010` through `2026`, inclusive.
- One Dagster asset: `brazil_cvm_dfp_raw_archives_s3`.
- One job: `brazil_cvm_dfp_raw_backfill_job`.
- Deterministic object keys by year.
- Skip existing archive object before any HTTP request for that year.
- Tests for URL/key construction, download, skip behavior, asset partitions, and job selection.

Out of scope:

- ITR.
- Parsing DFP CSVs.
- DuckDB or ClickHouse tables.
- Weekly freshness checks for changed 2026 ZIPs.
- Versioned archive storage by `Last-Modified` or hash.
- Metrics extraction.

## File Map

Create:

- `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/__init__.py`
  - Exposes `defs` for Dagster auto-loading.
- `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/source.py`
  - CVM DFP constants, URL/key builders, HTTP streaming, raw archive sync resource.
- `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/assets.py`
  - Static year partitions, raw archive asset, and backfill job.
- `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_source.py`
  - Unit tests for source/resource behavior.
- `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_assets.py`
  - Definition tests for partitions, asset group, and job selection.

Modify:

- `companycollect/corpscout/docs/countries/brazil-financial-sources.md`
  - Add a short implementation-status note after this first asset exists.
- `companycollect/corpscout/docs/countries/brazil-todo.md`
  - Mark the first DFP raw archive asset as the immediate implementation task.

## Naming And Storage Contract

Constants:

```python
BRAZIL_CVM_GROUP_NAME = "brazil_cvm"
BRAZIL_CVM_RAW_BUCKET = "source-brazil-cvm"
BRAZIL_CVM_DFP_BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS"
BRAZIL_CVM_DFP_START_YEAR = 2010
BRAZIL_CVM_DFP_END_YEAR = 2026
```

Partition keys:

```text
2010
2011
...
2026
```

Source URL:

```text
https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_<year>.zip
```

Object keys:

```text
brazil_cvm/dfp/raw_archives/year=<year>/archive.zip
brazil_cvm/dfp/raw_archives/year=<year>/metadata.json
```

Skip rule:

```text
if object_store.exists("brazil_cvm/dfp/raw_archives/year=<year>/archive.zip", bucket="source-brazil-cvm"):
    skip HTTP download
    return MaterializeResult(metadata={"downloaded": False, "reused_existing_archive": True, ...})
```

## Task 1: Add Source Tests

**Files:**

- Create: `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_source.py`
- Later implementation: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/source.py`

- [ ] **Step 1: Create failing tests for URL/key helpers and raw archive download**

Create `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_source.py`:

```python
from pathlib import Path

from dagster_v3.defs.brazil_cvm.source import (
    BRAZIL_CVM_RAW_BUCKET,
    BrazilCvmDfpResource,
    dfp_archive_object_key,
    dfp_metadata_object_key,
    dfp_source_url,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_files: list[tuple[str, str]] = []
        self.written_json: list[tuple[str, str]] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.uploaded_files.append((bucket, key))
        self.objects[(bucket, key)] = Path(source_path).read_bytes()

    def write_json(self, key: str, body: str, bucket: str | None = None) -> None:
        assert bucket is not None
        self.written_json.append((bucket, key))
        self.objects[(bucket, key)] = body.encode("utf-8")


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        content_type: str = "application/zip",
        last_modified: str = "Sun, 28 Jun 2026 07:13:00 GMT",
    ) -> None:
        self._body = body
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": content_type,
            "Last-Modified": last_modified,
        }

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 0) -> list[bytes]:
        return [self._body[:3], self._body[3:]]


class FakeSession:
    def __init__(self, responses_by_url: dict[str, bytes]) -> None:
        self.responses_by_url = responses_by_url
        self.requested_urls: list[str] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.requested_urls.append(url)
        return FakeResponse(self.responses_by_url[url])


class NoDownloadSession:
    def __init__(self) -> None:
        self.requested_urls: list[str] = []

    def get(self, url: str, *, timeout: int, stream: bool = False) -> FakeResponse:
        self.requested_urls.append(url)
        raise AssertionError(f"unexpected HTTP request: {url}")


def test_dfp_source_url_and_object_keys_are_deterministic() -> None:
    assert (
        dfp_source_url("2026")
        == "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_2026.zip"
    )
    assert (
        dfp_archive_object_key("2026")
        == "brazil_cvm/dfp/raw_archives/year=2026/archive.zip"
    )
    assert (
        dfp_metadata_object_key("2026")
        == "brazil_cvm/dfp/raw_archives/year=2026/metadata.json"
    )


def test_brazil_cvm_dfp_resource_downloads_missing_year_archive() -> None:
    resource = BrazilCvmDfpResource()
    object_store = FakeObjectStore()
    url = dfp_source_url("2026")
    session = FakeSession({url: b"zip-body"})

    result = resource.sync_year_archive(
        year="2026",
        object_store=object_store,
        session=session,
    )

    archive_key = dfp_archive_object_key("2026")
    metadata_key = dfp_metadata_object_key("2026")
    assert object_store.created_buckets == [BRAZIL_CVM_RAW_BUCKET]
    assert session.requested_urls == [url]
    assert object_store.uploaded_files == [(BRAZIL_CVM_RAW_BUCKET, archive_key)]
    assert object_store.objects[(BRAZIL_CVM_RAW_BUCKET, archive_key)] == b"zip-body"
    assert object_store.written_json == [(BRAZIL_CVM_RAW_BUCKET, metadata_key)]
    assert result.downloaded is True
    assert result.reused_existing_archive is False
    assert result.year == "2026"
    assert result.source_url == url
    assert result.archive_key == archive_key
    assert result.size_bytes == len(b"zip-body")
    assert result.sha256


def test_brazil_cvm_dfp_resource_skips_existing_year_archive_without_http() -> None:
    resource = BrazilCvmDfpResource()
    object_store = FakeObjectStore()
    archive_key = dfp_archive_object_key("2026")
    object_store.objects[(BRAZIL_CVM_RAW_BUCKET, archive_key)] = b"already-there"
    session = NoDownloadSession()

    result = resource.sync_year_archive(
        year="2026",
        object_store=object_store,
        session=session,
    )

    assert session.requested_urls == []
    assert object_store.uploaded_files == []
    assert object_store.written_json == []
    assert result.downloaded is False
    assert result.reused_existing_archive is True
    assert result.year == "2026"
    assert result.archive_key == archive_key
    assert result.size_bytes is None
    assert result.sha256 is None
```

- [ ] **Step 2: Run the new tests and confirm they fail because the package does not exist**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cvm_source.py -q
```

Expected:

```text
ModuleNotFoundError: No module named 'dagster_v3.defs.brazil_cvm'
```

## Task 2: Implement `brazil_cvm.source`

**Files:**

- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/source.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_source.py`

- [ ] **Step 1: Add the source/resource implementation**

Create `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/source.py`:

```python
from __future__ import annotations

import json
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource

BRAZIL_CVM_RAW_BUCKET = "source-brazil-cvm"
BRAZIL_CVM_DFP_BASE_URL = (
    "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS"
)
DEFAULT_REQUEST_TIMEOUT_SECONDS = 1_800
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
DEFAULT_USER_AGENT = "corpscout-dagster-v3-brazil-cvm/0.1"

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


@dataclass(frozen=True)
class BrazilCvmDfpArchiveSyncResult:
    year: str
    source_url: str
    archive_key: str
    metadata_key: str
    downloaded: bool
    reused_existing_archive: bool
    size_bytes: int | None
    sha256: str | None
    content_type: str
    source_last_modified: str
    synced_at: str

    def metadata(self) -> dict[str, object]:
        return {
            "year": self.year,
            "source_url": self.source_url,
            "s3_bucket": BRAZIL_CVM_RAW_BUCKET,
            "archive_key": self.archive_key,
            "metadata_key": self.metadata_key,
            "downloaded": self.downloaded,
            "reused_existing_archive": self.reused_existing_archive,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256 or "",
            "content_type": self.content_type,
            "source_last_modified": self.source_last_modified,
            "synced_at": self.synced_at,
        }


def normalize_dfp_year(year: str | int) -> str:
    clean_year = str(year).strip()
    if len(clean_year) != 4 or not clean_year.isdigit():
        raise ValueError("Brazil CVM DFP year must use YYYY format")
    return clean_year


def dfp_archive_name(year: str | int) -> str:
    clean_year = normalize_dfp_year(year)
    return f"dfp_cia_aberta_{clean_year}.zip"


def dfp_source_url(year: str | int) -> str:
    return f"{BRAZIL_CVM_DFP_BASE_URL}/{dfp_archive_name(year)}"


def dfp_archive_object_key(year: str | int) -> str:
    clean_year = normalize_dfp_year(year)
    return f"brazil_cvm/dfp/raw_archives/year={clean_year}/archive.zip"


def dfp_metadata_object_key(year: str | int) -> str:
    clean_year = normalize_dfp_year(year)
    return f"brazil_cvm/dfp/raw_archives/year={clean_year}/metadata.json"


class BrazilCvmDfpResource(dg.ConfigurableResource):
    """Downloads CVM DFP yearly ZIP archives into object storage."""

    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    download_max_attempts: int = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    download_retry_base_seconds: float = DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS
    user_agent: str = DEFAULT_USER_AGENT

    def sync_year_archive(
        self,
        *,
        year: str,
        object_store: ObjectStoreResource,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> BrazilCvmDfpArchiveSyncResult:
        clean_year = normalize_dfp_year(year)
        source_url = dfp_source_url(clean_year)
        archive_key = dfp_archive_object_key(clean_year)
        metadata_key = dfp_metadata_object_key(clean_year)
        synced_at = datetime.now(UTC).isoformat()

        object_store.ensure_bucket(BRAZIL_CVM_RAW_BUCKET)
        if object_store.exists(archive_key, bucket=BRAZIL_CVM_RAW_BUCKET):
            if log_info is not None:
                log_info(
                    "Reusing existing Brazil CVM DFP archive: year=%s bucket=%s key=%s",
                    clean_year,
                    BRAZIL_CVM_RAW_BUCKET,
                    archive_key,
                )
            return BrazilCvmDfpArchiveSyncResult(
                year=clean_year,
                source_url=source_url,
                archive_key=archive_key,
                metadata_key=metadata_key,
                downloaded=False,
                reused_existing_archive=True,
                size_bytes=None,
                sha256=None,
                content_type="",
                source_last_modified="",
                synced_at=synced_at,
            )

        if log_info is not None:
            log_info(
                "Downloading Brazil CVM DFP archive: year=%s url=%s bucket=%s key=%s",
                clean_year,
                source_url,
                BRAZIL_CVM_RAW_BUCKET,
                archive_key,
            )

        http_session = session or self._session()
        with tempfile.TemporaryDirectory(prefix="brazil_cvm_dfp_") as tmpdir:
            target_path = Path(tmpdir) / dfp_archive_name(clean_year)
            size_bytes, digest, content_type, source_last_modified = (
                self._download_to_path(
                    url=source_url,
                    target_path=target_path,
                    session=http_session,
                    log_info=log_info,
                )
            )
            object_store.upload_file(
                archive_key,
                target_path,
                bucket=BRAZIL_CVM_RAW_BUCKET,
            )

        result = BrazilCvmDfpArchiveSyncResult(
            year=clean_year,
            source_url=source_url,
            archive_key=archive_key,
            metadata_key=metadata_key,
            downloaded=True,
            reused_existing_archive=False,
            size_bytes=size_bytes,
            sha256=digest,
            content_type=content_type,
            source_last_modified=source_last_modified,
            synced_at=synced_at,
        )
        object_store.write_json(
            metadata_key,
            json.dumps(asdict(result), sort_keys=True, indent=2),
            bucket=BRAZIL_CVM_RAW_BUCKET,
        )
        return result

    def _session(self) -> Any:
        session = dlt_requests.Session()
        session.headers.update({"User-Agent": self.user_agent})
        return session

    def _download_to_path(
        self,
        *,
        url: str,
        target_path: Path,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> tuple[int, str, str, str]:
        last_error: Exception | None = None
        for attempt in range(1, self.download_max_attempts + 1):
            try:
                return self._stream_download_to_path(
                    url=url,
                    target_path=target_path,
                    session=session,
                )
            except _DOWNLOAD_RETRYABLE_ERRORS as exc:
                last_error = exc
                target_path.unlink(missing_ok=True)
                if attempt >= self.download_max_attempts:
                    break
                wait_seconds = self.download_retry_base_seconds * attempt
                if log_info is not None:
                    log_info(
                        "Brazil CVM DFP archive download failed; retrying: "
                        "attempt=%s/%s wait_seconds=%s url=%s error=%s",
                        attempt,
                        self.download_max_attempts,
                        wait_seconds,
                        url,
                        exc,
                    )
                time.sleep(wait_seconds)
        assert last_error is not None
        raise last_error

    def _stream_download_to_path(
        self,
        *,
        url: str,
        target_path: Path,
        session: Any,
    ) -> tuple[int, str, str, str]:
        response = session.get(url, timeout=self.request_timeout_seconds, stream=True)
        response.raise_for_status()

        digest = sha256()
        size_bytes = 0
        with target_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue
                digest.update(chunk)
                size_bytes += len(chunk)
                handle.write(chunk)

        expected = response.headers.get("Content-Length")
        if expected is not None and int(expected) != size_bytes:
            raise ValueError(
                f"Brazil CVM DFP archive download size mismatch: "
                f"expected={expected} actual={size_bytes} url={url}"
            )

        return (
            size_bytes,
            digest.hexdigest(),
            response.headers.get("Content-Type", ""),
            response.headers.get("Last-Modified", ""),
        )
```

- [ ] **Step 2: Run the source tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cvm_source.py -q
```

Expected:

```text
3 passed
```

## Task 3: Add Asset Definition Tests

**Files:**

- Create: `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_assets.py`
- Later implementation: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/assets.py`
- Later implementation: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/__init__.py`

- [ ] **Step 1: Create failing definition tests**

Create `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_assets.py`:

```python
import dagster as dg


def test_brazil_cvm_dfp_raw_archive_asset_is_year_partitioned() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    node = repo.asset_graph.get(dg.AssetKey("brazil_cvm_dfp_raw_archives_s3"))

    assert node.group_name == "brazil_cvm"
    assert type(node.partitions_def).__name__ == "StaticPartitionsDefinition"
    assert node.partitions_def.get_partition_keys() == [
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
        "2026",
    ]


def test_brazil_cvm_dfp_raw_backfill_job_selects_only_raw_archive_asset() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "brazil_cvm_dfp_raw_backfill_job"
        ).asset_layer.executable_asset_keys
    }

    assert asset_keys == {"brazil_cvm_dfp_raw_archives_s3"}
```

- [ ] **Step 2: Run the new asset tests and confirm they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cvm_assets.py -q
```

Expected:

```text
dagster._core.errors.DagsterInvariantViolationError
```

or:

```text
KeyError: AssetKey(['brazil_cvm_dfp_raw_archives_s3'])
```

because the asset and job are not registered yet.

## Task 4: Implement The DFP Raw Archive Asset

**Files:**

- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/__init__.py`
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/assets.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_assets.py`

- [ ] **Step 1: Add the package export**

Create `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/__init__.py`:

```python
from dagster_v3.defs.brazil_cvm.assets import defs
```

- [ ] **Step 2: Add the asset and job**

Create `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/assets.py`:

```python
from __future__ import annotations

import dagster as dg

from dagster_v3.defs.brazil_cvm.source import (
    BrazilCvmDfpResource,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

GROUP_NAME = "brazil_cvm"
BRAZIL_CVM_DFP_START_YEAR = 2010
BRAZIL_CVM_DFP_END_YEAR = 2026
BRAZIL_CVM_DFP_RAW_ARCHIVE_ASSET_KEY = "brazil_cvm_dfp_raw_archives_s3"
BRAZIL_CVM_DFP_RAW_PARTITIONS = dg.StaticPartitionsDefinition(
    [str(year) for year in range(BRAZIL_CVM_DFP_START_YEAR, BRAZIL_CVM_DFP_END_YEAR + 1)]
)


@dg.asset(
    name=BRAZIL_CVM_DFP_RAW_ARCHIVE_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "cvm", "dfp"},
    partitions_def=BRAZIL_CVM_DFP_RAW_PARTITIONS,
    description=(
        "Downloads one Brazil CVM DFP yearly ZIP archive into S3/RustFS. "
        "The asset is year-partitioned from 2010 through 2026 and skips "
        "the HTTP download when that year archive already exists in object storage."
    ),
)
def brazil_cvm_dfp_raw_archives_s3(
    context: dg.AssetExecutionContext,
    brazil_cvm_dfp: BrazilCvmDfpResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    result = brazil_cvm_dfp.sync_year_archive(
        year=context.partition_key,
        object_store=object_store,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=result.metadata())


brazil_cvm_dfp_raw_backfill_job = dg.define_asset_job(
    "brazil_cvm_dfp_raw_backfill_job",
    selection=dg.AssetSelection.assets(BRAZIL_CVM_DFP_RAW_ARCHIVE_ASSET_KEY),
)


defs = dg.Definitions(
    assets=[brazil_cvm_dfp_raw_archives_s3],
    jobs=[brazil_cvm_dfp_raw_backfill_job],
    resources={"brazil_cvm_dfp": BrazilCvmDfpResource()},
)
```

- [ ] **Step 3: Run the asset definition tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cvm_assets.py -q
```

Expected:

```text
2 passed
```

## Task 5: Add Direct Asset Execution Tests

**Files:**

- Modify: `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_assets.py`
- Test target: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm/assets.py`

- [ ] **Step 1: Extend asset tests with direct materialization behavior**

Append to `companycollect/corpscout/dagster_v3/tests/test_brazil_cvm_assets.py`:

```python

class FakeBrazilCvmDfpResource:
    def __init__(self) -> None:
        self.requested_years: list[str] = []

    def sync_year_archive(
        self,
        *,
        year: str,
        object_store: object,
        log_info: object | None = None,
    ) -> object:
        from dagster_v3.defs.brazil_cvm.source import BrazilCvmDfpArchiveSyncResult

        self.requested_years.append(year)
        return BrazilCvmDfpArchiveSyncResult(
            year=year,
            source_url=f"https://example.test/dfp_cia_aberta_{year}.zip",
            archive_key=f"brazil_cvm/dfp/raw_archives/year={year}/archive.zip",
            metadata_key=f"brazil_cvm/dfp/raw_archives/year={year}/metadata.json",
            downloaded=False,
            reused_existing_archive=True,
            size_bytes=None,
            sha256=None,
            content_type="",
            source_last_modified="",
            synced_at="2026-07-04T00:00:00+00:00",
        )


def test_brazil_cvm_dfp_raw_archive_asset_uses_partition_year() -> None:
    from dagster_v3.defs.brazil_cvm.assets import brazil_cvm_dfp_raw_archives_s3

    fake_resource = FakeBrazilCvmDfpResource()
    result = brazil_cvm_dfp_raw_archives_s3(
        dg.build_asset_context(partition_key="2026"),
        brazil_cvm_dfp=fake_resource,
        object_store=object(),
    )

    assert fake_resource.requested_years == ["2026"]
    assert result.metadata["year"] == "2026"
    assert result.metadata["reused_existing_archive"] is True
    assert result.metadata["downloaded"] is False
```

- [ ] **Step 2: Run the asset tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cvm_assets.py -q
```

Expected:

```text
3 passed
```

## Task 6: Update Brazil Docs

**Files:**

- Modify: `companycollect/corpscout/docs/countries/brazil-financial-sources.md`
- Modify: `companycollect/corpscout/docs/countries/brazil-todo.md`

- [ ] **Step 1: Add implementation status to the financial source analysis**

In `companycollect/corpscout/docs/countries/brazil-financial-sources.md`, under `## 1. CVM DFP Annual Financial Statements`, add:

```markdown
### Implementation Status

First asset planned/implemented: `brazil_cvm_dfp_raw_archives_s3`.
It is partitioned by year from `2010` through `2026`, downloads the raw
`dfp_cia_aberta_<year>.zip` archive into `source-brazil-cvm`, and skips the
download when `brazil_cvm/dfp/raw_archives/year=<year>/archive.zip` already
exists. Parsing and metric extraction are separate follow-up assets.
```

- [ ] **Step 2: Update the todo next task**

In `companycollect/corpscout/docs/countries/brazil-todo.md`, replace the current `## Next Task` section with:

```markdown
## Next Task

Implement `brazil_cvm_dfp_raw_archives_s3`, the first Brazil CVM financial
asset. It should download annual DFP ZIP archives for partitions `2010` through
`2026` into S3/RustFS and skip a year when the raw archive key already exists.

After that lands, the next task is a DFP parser asset that reads the stored ZIPs
and loads the CSV family rows into DuckDB.
```

- [ ] **Step 3: Inspect the doc snippets**

Run:

```bash
sed -n '30,170p' /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/docs/countries/brazil-financial-sources.md
sed -n '1,80p' /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/docs/countries/brazil-todo.md
```

Expected:

```text
The new DFP implementation status and todo text are visible.
```

## Task 7: Run Verification

**Files:**

- All files from prior tasks.

- [ ] **Step 1: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_brazil_cvm_source.py tests/test_brazil_cvm_assets.py -q
```

Expected:

```text
6 passed
```

- [ ] **Step 2: Validate Dagster definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check
```

Expected:

```text
No definition loading errors.
```

- [ ] **Step 3: List the registered Brazil CVM definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg list defs --json | rg "brazil_cvm"
```

Expected:

```text
brazil_cvm_dfp_raw_archives_s3
brazil_cvm_dfp_raw_backfill_job
```

- [ ] **Step 4: Confirm dirty files are expected**

Run:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect status --short -- corpscout/dagster_v3/src/dagster_v3/defs/brazil_cvm corpscout/dagster_v3/tests/test_brazil_cvm_source.py corpscout/dagster_v3/tests/test_brazil_cvm_assets.py corpscout/docs/countries/brazil-financial-sources.md corpscout/docs/countries/brazil-todo.md
```

Expected:

```text
Only the new brazil_cvm package, new tests, and planned docs changes are listed.
```

## Follow-Up Plan After This Asset

After this raw archive asset is working, create a separate plan for parsing the stored DFP ZIPs:

- read each archive from `source-brazil-cvm`
- extract the 19 CSV files
- load document index rows and statement-family rows into DuckDB
- preserve `CNPJ_CIA`, `CD_CVM`, `DT_REFER`, `VERSAO`, statement family, consolidation type, account code, account description, value, currency, and scale
- derive metrics only after the raw row tables are stable

Do not add ITR to this first asset. The user explicitly asked for historical DFP per year only.
