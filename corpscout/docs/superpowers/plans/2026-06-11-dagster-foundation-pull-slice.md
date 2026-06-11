# Dagster Foundation + Finland PRH YTJ Pull Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `corpscout/dagster/` project and prove the execution model end-to-end with the Finland PRH YTJ pull asset streaming to RustFS with a run manifest.

**Architecture:** Per `docs/superpowers/specs/2026-06-11-finland-dagster-temporal-design.md`. This is Plan 1 of 4: foundation + pull vertical slice, ending at the spec's run-launcher pass criteria. Plan 2 (YTJ Python import + parity), Plan 3 (NACE/mapping/cache assets, slim Go worker, XBRL RustFS + sensor), and Plan 4 (cutover) follow. The pull asset mirrors the Go downloader contract exactly: paginated `GET {base}/companies?page=N` returning `{totalResults, companies[]}`, NDJSON one company per line, stop when records >= totalResults, or page empty, or (no totalResults and page < 100). Code lists are `GET /opendata-ytj-api/v3/description?code=X&lang=en`, body saved verbatim as `codelists/{CODE}.{lang}.tsv` (the endpoint returns TSV).

**Tech Stack:** Python 3.12, dagster, dagster-postgres, dagster-docker, boto3, requests; tests with pytest, moto (S3), responses (HTTP).

**Conventions for every task:**
- Working directory: `corpscout/dagster/` (create in Task 1)
- Run tests with: `.venv/bin/python -m pytest <path> -v`
- NDJSON note: Python re-serializes company JSON with `json.dumps(..., separators=(",", ":"), ensure_ascii=False)`. Bytes are not identical to the Go file; this is fine — parity in Plan 2 compares ClickHouse rows, and the bake-off feeds the same file to both importers.

---

### Task 1: Project scaffold

**Files:**
- Create: `corpscout/dagster/pyproject.toml`
- Create: `corpscout/dagster/.gitignore`
- Create: `corpscout/dagster/dagster_corpscout/__init__.py`
- Create: `corpscout/dagster/dagster_corpscout/definitions.py`
- Create: `corpscout/dagster/tests/__init__.py`
- Test: `corpscout/dagster/tests/test_definitions.py`

- [ ] **Step 1: Create project files**

`pyproject.toml`:

```toml
[project]
name = "dagster-corpscout"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "dagster>=1.10,<2",
    "dagster-postgres>=0.26",
    "dagster-docker>=0.26",
    "boto3>=1.34",
    "requests>=2.32",
]

[project.optional-dependencies]
dev = [
    "pytest>=8",
    "moto[s3]>=5",
    "responses>=0.25",
    "dagster-webserver>=1.10,<2",
]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["dagster_corpscout*"]

[tool.dagster]
module_name = "dagster_corpscout.definitions"
```

`.gitignore`:

```
.venv/
__pycache__/
*.egg-info/
.env
tmp_dagster_home*/
```

`dagster_corpscout/__init__.py`: empty file.

`dagster_corpscout/definitions.py`:

```python
import dagster as dg

defs = dg.Definitions()
```

`tests/__init__.py`: empty file.

- [ ] **Step 2: Write the test**

`tests/test_definitions.py`:

```python
def test_definitions_load():
    from dagster_corpscout.definitions import defs

    assert defs is not None
```

- [ ] **Step 3: Create venv, install, run test**

```bash
cd corpscout/dagster
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest tests/test_definitions.py -v
```

Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml .gitignore dagster_corpscout tests
git commit -m "feat(dagster): scaffold dagster_corpscout project"
```

---

### Task 2: Finland PRH YTJ source spec

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/__init__.py` (empty)
- Create: `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/__init__.py` (empty)
- Create: `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/spec.py`
- Test: `corpscout/dagster/tests/test_spec.py`

- [ ] **Step 1: Write the failing test**

```python
from dagster_corpscout.sources.finland_prhytj import spec


def test_constants():
    assert spec.SOURCE_NAME == "finland_prhytj"
    assert spec.BUCKET == "source-finland-prhytj"
    assert spec.BASE_URL.startswith("https://avoindata.prh.fi/")
    assert spec.PAGE_SIZE == 100


def test_code_lists_match_go_catalog():
    codes = [code for code, _ in spec.CODE_LISTS]
    assert codes == ["REK", "REK_KDI", "VIRANOM", "TLAJI", "YRMU", "STATUS3", "KIELI"]
    assert all(lang == "en" for _, lang in spec.CODE_LISTS)


def test_object_keys():
    assert spec.snapshot_object_key("20260611T120000Z") == "runs/20260611T120000Z/source.ndjson"
    assert (
        spec.code_list_object_key("20260611T120000Z", "REK", "en")
        == "runs/20260611T120000Z/codelists/REK.en.tsv"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_spec.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

`spec.py` (values mirror `scheduler/internal/companysources/sourcecatalog/sources/finland_prhytj.json` and `download.go`):

```python
"""Declarative source config for Finland PRH YTJ. Mirrors the Go source catalog."""

SOURCE_NAME = "finland_prhytj"
COUNTRY = "finland"
DISPLAY_NAME = "Finland PRH YTJ"
BUCKET = "source-finland-prhytj"
BASE_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
DESCRIPTION_PATH = "/opendata-ytj-api/v3/description"
PAGE_SIZE = 100

# (code, lang) — order matches the Go source catalog sort_order.
CODE_LISTS = [
    ("REK", "en"),
    ("REK_KDI", "en"),
    ("VIRANOM", "en"),
    ("TLAJI", "en"),
    ("YRMU", "en"),
    ("STATUS3", "en"),
    ("KIELI", "en"),
]


def snapshot_object_key(run_id: str) -> str:
    return f"runs/{run_id}/source.ndjson"


def code_list_object_key(run_id: str, code: str, lang: str) -> str:
    return f"runs/{run_id}/codelists/{code}.{lang}.tsv"


def manifest_object_key(run_id: str) -> str:
    return f"runs/{run_id}/manifest.json"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_spec.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add dagster_corpscout/sources tests/test_spec.py
git commit -m "feat(dagster): add finland prhytj source spec"
```

---

### Task 3: Streaming helpers (iterator-backed reader with hash/byte stats)

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/lib/__init__.py` (empty)
- Create: `corpscout/dagster/dagster_corpscout/lib/streaming.py`
- Test: `corpscout/dagster/tests/test_streaming.py`

These let a generator of bytes chunks be uploaded via `boto3.upload_fileobj`
(which requires a file-like `read()`), while hashing and counting exactly the
bytes that pass through — the spec's "no buffering of the payload" rule.

- [ ] **Step 1: Write the failing test**

```python
import hashlib

from dagster_corpscout.lib.streaming import IterableReader, StreamStats, observe_chunks


def test_iterable_reader_rejects_unbounded_read():
    reader = IterableReader(iter([b"abc"]))
    try:
        reader.read()
        raise AssertionError("expected ValueError for unbounded read")
    except ValueError:
        pass


def test_iterable_reader_reads_in_sizes():
    reader = IterableReader(iter([b"abc", b"def", b"gh"]))
    assert reader.read(2) == b"ab"
    assert reader.read(4) == b"cdef"
    assert reader.read(100) == b"gh"
    assert reader.read(10) == b""


def test_iterable_reader_does_not_drain_iterator():
    consumed = []

    def chunks():
        for chunk in [b"aaa", b"bbb", b"ccc", b"ddd"]:
            consumed.append(chunk)
            yield chunk

    reader = IterableReader(chunks())
    assert reader.read(4) == b"aaab"
    # Only the chunks needed to satisfy the bounded read were pulled.
    assert consumed == [b"aaa", b"bbb"]


def test_observe_chunks_counts_and_hashes_once():
    stats = StreamStats()
    reader = IterableReader(observe_chunks(iter([b"hello ", b"world"]), stats))
    data = b""
    while True:
        piece = reader.read(4)
        if not piece:
            break
        data += piece
    assert data == b"hello world"
    assert stats.bytes_read == 11
    assert stats.sha256_hex == hashlib.sha256(b"hello world").hexdigest()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_streaming.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

`streaming.py`:

```python
"""Iterator-to-file-like adapters for streaming uploads with hash/byte accounting."""

import hashlib
import io
from collections.abc import Iterator


class StreamStats:
    """Accumulates sha256, byte count, and record count for a streamed payload."""

    def __init__(self) -> None:
        self._hasher = hashlib.sha256()
        self.bytes_read = 0
        self.records = 0

    def update(self, chunk: bytes) -> None:
        self._hasher.update(chunk)
        self.bytes_read += len(chunk)

    @property
    def sha256_hex(self) -> str:
        return self._hasher.hexdigest()


def observe_chunks(chunks: Iterator[bytes], stats: StreamStats) -> Iterator[bytes]:
    """Yield chunks unchanged while updating stats. Each byte is observed exactly once."""
    for chunk in chunks:
        stats.update(chunk)
        yield chunk


class IterableReader(io.RawIOBase):
    """Read-only file-like object over an iterator of bytes chunks.

    Bounded reads only: an unbounded read would buffer the whole payload in
    memory, violating the streaming guarantee. boto3's transfer manager always
    reads in bounded chunks, so this restriction is safe for upload_fileobj.
    """

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = iter(chunks)
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            raise ValueError(
                "IterableReader only supports bounded reads; an unbounded read "
                "would buffer the whole payload in memory"
            )
        while len(self._buffer) < size:
            try:
                self._buffer += next(self._chunks)
            except StopIteration:
                break
        data, self._buffer = self._buffer[:size], self._buffer[size:]
        return data
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_streaming.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add dagster_corpscout/lib tests/test_streaming.py
git commit -m "feat(dagster): add streaming reader with hash and byte stats"
```

---

### Task 4: Run manifest builder

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/lib/manifest.py`
- Test: `corpscout/dagster/tests/test_manifest.py`

The manifest is the cross-plane contract from the spec ("Manifest contract"
section). Field names must match exactly — the Go XBRL workflow will write the
same shape in Plan 3 and the sensor will parse it.

- [ ] **Step 1: Write the failing test**

```python
from dagster_corpscout.lib.manifest import Artifact, build_manifest


def test_build_manifest_shape():
    manifest = build_manifest(
        run_id="20260611T120000Z",
        source="finland_prhytj",
        workflow_id="dagster-run-abc123",
        artifacts=[
            Artifact(
                key="source",
                object_key="runs/20260611T120000Z/source.ndjson",
                content_sha256="deadbeef",
                content_length_bytes=42,
                records_written=2,
            )
        ],
    )
    assert manifest == {
        "run_id": "20260611T120000Z",
        "source": "finland_prhytj",
        "workflow_id": "dagster-run-abc123",
        "artifacts": [
            {
                "key": "source",
                "object_key": "runs/20260611T120000Z/source.ndjson",
                "content_sha256": "deadbeef",
                "content_length_bytes": 42,
                "records_written": 2,
            }
        ],
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_manifest.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

`manifest.py`:

```python
"""Run manifest: the durable artifact ledger written to every run prefix.

Shape is the cross-plane contract from the design spec, shared with the Go
Temporal workflows. `workflow_id` carries the Temporal workflow id for
Temporal-produced runs and the Dagster run id for Dagster-produced runs.
"""

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Artifact:
    key: str
    object_key: str
    content_sha256: str
    content_length_bytes: int
    records_written: int


def build_manifest(
    run_id: str,
    source: str,
    workflow_id: str,
    artifacts: list[Artifact],
) -> dict:
    return {
        "run_id": run_id,
        "source": source,
        "workflow_id": workflow_id,
        "artifacts": [asdict(a) for a in artifacts],
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_manifest.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add dagster_corpscout/lib/manifest.py tests/test_manifest.py
git commit -m "feat(dagster): add run manifest builder"
```

---

### Task 5: RustFS resource

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/resources/__init__.py` (empty)
- Create: `corpscout/dagster/dagster_corpscout/resources/rustfs.py`
- Test: `corpscout/dagster/tests/test_rustfs.py`

- [ ] **Step 1: Write the failing test**

```python
import hashlib
import json

import boto3
from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource


def make_resource() -> RustFSResource:
    # Under moto, no endpoint_url: the mock intercepts default AWS endpoints.
    return RustFSResource(endpoint_url="", access_key="test", secret_key="test")


@mock_aws
def test_upload_stream_and_stats():
    resource = make_resource()
    resource.client().create_bucket(Bucket="bkt")

    stats = resource.upload_stream("bkt", "runs/x/source.ndjson", iter([b"line1\n", b"line2\n"]))

    body = boto3.client("s3").get_object(Bucket="bkt", Key="runs/x/source.ndjson")["Body"].read()
    assert body == b"line1\nline2\n"
    assert stats.bytes_read == 12
    assert stats.sha256_hex == hashlib.sha256(b"line1\nline2\n").hexdigest()


@mock_aws
def test_put_bytes_and_put_json():
    resource = make_resource()
    resource.client().create_bucket(Bucket="bkt")

    sha = resource.put_bytes("bkt", "codelists/REK.en.tsv", b"K\tV\n")
    assert sha == hashlib.sha256(b"K\tV\n").hexdigest()

    resource.put_json("bkt", "runs/x/manifest.json", {"run_id": "x"})
    body = boto3.client("s3").get_object(Bucket="bkt", Key="runs/x/manifest.json")["Body"].read()
    assert json.loads(body) == {"run_id": "x"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_rustfs.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write implementation**

`rustfs.py`:

```python
"""S3-compatible object storage resource for RustFS."""

import hashlib
import json

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from collections.abc import Iterator
from dagster import ConfigurableResource

from dagster_corpscout.lib.streaming import IterableReader, StreamStats, observe_chunks

# 64 MiB parts: a few-GB object uploads in tens of parts with bounded memory.
_TRANSFER_CONFIG = TransferConfig(multipart_chunksize=64 * 1024 * 1024)

# Path-style addressing is required for RustFS/MinIO (virtual-hosted style
# would resolve bucket.localhost). Mirrors UsePathStyle in the Go
# scheduler/internal/s3client/client.go.
_S3_CONFIG = Config(s3={"addressing_style": "path"})


class RustFSResource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    def client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url or None,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=_S3_CONFIG,
        )

    def upload_stream(self, bucket: str, key: str, chunks: Iterator[bytes]) -> StreamStats:
        """Stream chunks to an object via multipart upload; never buffers the payload."""
        stats = StreamStats()
        reader = IterableReader(observe_chunks(chunks, stats))
        self.client().upload_fileobj(reader, bucket, key, Config=_TRANSFER_CONFIG)
        return stats

    def put_bytes(self, bucket: str, key: str, body: bytes) -> str:
        """Put a small object; returns its sha256 hex."""
        self.client().put_object(Bucket=bucket, Key=key, Body=body)
        return hashlib.sha256(body).hexdigest()

    def put_json(self, bucket: str, key: str, payload: dict) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.client().put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/json"
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_rustfs.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add dagster_corpscout/resources tests/test_rustfs.py
git commit -m "feat(dagster): add rustfs streaming object storage resource"
```

---

### Task 6: PRH YTJ HTTP client

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/client.py`
- Test: `corpscout/dagster/tests/test_client.py`

Mirrors `scheduler/internal/companysources/finland/prhytj/download.go` exactly:
pagination starts at page 1; stop when records >= totalResults, or page has no
companies, or (totalResults never seen and page < PAGE_SIZE).

- [ ] **Step 1: Write the failing test**

```python
import responses

from dagster_corpscout.sources.finland_prhytj import spec
from dagster_corpscout.sources.finland_prhytj.client import (
    fetch_code_list,
    iter_companies,
    ndjson_chunks,
)
from dagster_corpscout.lib.streaming import StreamStats

BASE = spec.BASE_URL


def page_json(total, companies):
    return {"totalResults": total, "companies": companies}


@responses.activate
def test_iter_companies_stops_at_total_results():
    responses.get(f"{BASE}?page=1", json=page_json(3, [{"businessId": "1"}, {"businessId": "2"}]))
    responses.get(f"{BASE}?page=2", json=page_json(3, [{"businessId": "3"}]))

    companies = list(iter_companies(BASE))
    assert [c["businessId"] for c in companies] == ["1", "2", "3"]
    assert len(responses.calls) == 2


@responses.activate
def test_iter_companies_stops_on_empty_page():
    responses.get(f"{BASE}?page=1", json={"companies": [{"businessId": "1"}]})
    responses.get(f"{BASE}?page=2", json={"companies": []})

    # No totalResults and first page has 1 < PAGE_SIZE companies: stops after page 1.
    companies = list(iter_companies(BASE))
    assert len(companies) == 1
    assert len(responses.calls) == 1


@responses.activate
def test_iter_companies_raises_on_http_error():
    responses.get(f"{BASE}?page=1", status=503)
    try:
        list(iter_companies(BASE))
        raise AssertionError("expected an HTTP error")
    except Exception as exc:
        assert "503" in str(exc)


def test_ndjson_chunks_counts_records():
    stats = StreamStats()
    chunks = list(ndjson_chunks(iter([{"a": 1}, {"b": "ä"}]), stats))
    assert chunks == [b'{"a":1}\n', '{"b":"ä"}\n'.encode("utf-8")]
    assert stats.records == 2


@responses.activate
def test_fetch_code_list_returns_verbatim_body():
    responses.get(
        "https://avoindata.prh.fi/opendata-ytj-api/v3/description?code=REK&lang=en",
        body=b"1\tTrade register\n2\tFoundation register\n",
    )
    body = fetch_code_list(BASE, "REK", "en")
    assert body == b"1\tTrade register\n2\tFoundation register\n"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_client.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write implementation**

`client.py`:

```python
"""HTTP client for the PRH Open Data YTJ API v3.

Pagination contract mirrors the Go downloader
(scheduler/internal/companysources/finland/prhytj/download.go).
"""

import json
from collections.abc import Iterator
from urllib.parse import urlsplit, urlunsplit

import requests

from dagster_corpscout.lib.streaming import StreamStats
from dagster_corpscout.sources.finland_prhytj import spec

_TIMEOUT_SECONDS = 300


def iter_companies(base_url: str, page_size: int = spec.PAGE_SIZE) -> Iterator[dict]:
    """Yield company dicts across all pages, mirroring the Go stop conditions."""
    session = requests.Session()
    total_results: int | None = None
    records = 0
    page_number = 1
    while True:
        resp = session.get(base_url, params={"page": page_number}, timeout=_TIMEOUT_SECONDS)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("totalResults") is not None:
            total_results = int(payload["totalResults"])
        companies = payload.get("companies") or []
        for company in companies:
            records += 1
            yield company
        if total_results is not None and records >= total_results:
            return
        if not companies:
            return
        if total_results is None and len(companies) < page_size:
            return
        page_number += 1


def ndjson_chunks(companies: Iterator[dict], stats: StreamStats) -> Iterator[bytes]:
    """One canonical-JSON line per company; counts records on stats."""
    for company in companies:
        stats.records += 1
        line = json.dumps(company, separators=(",", ":"), ensure_ascii=False)
        yield line.encode("utf-8") + b"\n"


def fetch_code_list(base_url: str, code: str, lang: str) -> bytes:
    """Fetch a code list; the description endpoint returns TSV, stored verbatim."""
    parts = urlsplit(base_url)
    url = urlunsplit((parts.scheme, parts.netloc, spec.DESCRIPTION_PATH, "", ""))
    resp = requests.get(url, params={"code": code, "lang": lang}, timeout=_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.content
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_client.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add dagster_corpscout/sources/finland_prhytj/client.py tests/test_client.py
git commit -m "feat(dagster): add prh ytj http client mirroring go pagination"
```

---

### Task 7: raw_snapshot asset

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/assets.py`
- Test: `corpscout/dagster/tests/test_raw_snapshot.py`

- [ ] **Step 1: Write the failing test**

```python
import json

import boto3
import responses
from dagster import materialize
from moto import mock_aws

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland_prhytj import spec
from dagster_corpscout.sources.finland_prhytj.assets import raw_snapshot

BASE = spec.BASE_URL


def register_api_stubs():
    responses.get(
        f"{BASE}?page=1",
        json={"totalResults": 2, "companies": [{"businessId": "111"}, {"businessId": "222"}]},
    )
    for code, lang in spec.CODE_LISTS:
        responses.get(
            f"https://avoindata.prh.fi/opendata-ytj-api/v3/description?code={code}&lang={lang}",
            body=f"1\t{code} entry\n".encode("utf-8"),
        )


@mock_aws
@responses.activate
def test_raw_snapshot_writes_artifacts_and_manifest():
    register_api_stubs()
    rustfs = RustFSResource(endpoint_url="", access_key="t", secret_key="t")
    rustfs.client().create_bucket(Bucket=spec.BUCKET)

    result = materialize([raw_snapshot], resources={"rustfs": rustfs})
    assert result.success

    s3 = boto3.client("s3")
    keys = [
        o["Key"] for o in s3.list_objects_v2(Bucket=spec.BUCKET)["Contents"]
    ]
    run_id = sorted(keys)[0].split("/")[1]
    assert f"runs/{run_id}/source.ndjson" in keys
    assert f"runs/{run_id}/manifest.json" in keys
    for code, lang in spec.CODE_LISTS:
        assert f"runs/{run_id}/codelists/{code}.{lang}.tsv" in keys

    snapshot = s3.get_object(Bucket=spec.BUCKET, Key=f"runs/{run_id}/source.ndjson")["Body"].read()
    assert snapshot == b'{"businessId":"111"}\n{"businessId":"222"}\n'

    manifest = json.loads(
        s3.get_object(Bucket=spec.BUCKET, Key=f"runs/{run_id}/manifest.json")["Body"].read()
    )
    assert manifest["run_id"] == run_id
    assert manifest["source"] == spec.SOURCE_NAME
    # 1 snapshot + 7 code lists
    assert len(manifest["artifacts"]) == 8
    snap_artifact = manifest["artifacts"][0]
    assert snap_artifact["key"] == "source"
    assert snap_artifact["records_written"] == 2
    assert snap_artifact["content_length_bytes"] == len(snapshot)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_raw_snapshot.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write implementation**

`assets.py`:

```python
"""Finland PRH YTJ assets: the pull slice."""

from datetime import datetime, timezone

import dagster as dg

from dagster_corpscout.lib.manifest import Artifact, build_manifest
from dagster_corpscout.lib.streaming import StreamStats
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland_prhytj import spec
from dagster_corpscout.sources.finland_prhytj.client import (
    fetch_code_list,
    iter_companies,
    ndjson_chunks,
)


@dg.asset(
    key_prefix=[spec.SOURCE_NAME],
    name="raw_snapshot",
    group_name=spec.SOURCE_NAME,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": spec.SOURCE_NAME},
)
def raw_snapshot(context: dg.AssetExecutionContext, rustfs: RustFSResource) -> dg.MaterializeResult:
    """Pull the company snapshot + code lists from the PRH YTJ API into RustFS."""
    # Timestamp keeps prefixes human-sortable; the Dagster run-id suffix makes
    # the prefix unique even if two launches/retries start in the same second.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{timestamp}-{context.run_id[:8]}"
    artifacts: list[Artifact] = []

    snapshot_key = spec.snapshot_object_key(run_id)
    record_stats = StreamStats()
    chunks = ndjson_chunks(iter_companies(spec.BASE_URL), record_stats)
    upload_stats = rustfs.upload_stream(spec.BUCKET, snapshot_key, chunks)
    context.log.info(
        "snapshot uploaded: %d records, %d bytes", record_stats.records, upload_stats.bytes_read
    )
    artifacts.append(
        Artifact(
            key="source",
            object_key=snapshot_key,
            content_sha256=upload_stats.sha256_hex,
            content_length_bytes=upload_stats.bytes_read,
            records_written=record_stats.records,
        )
    )

    for code, lang in spec.CODE_LISTS:
        body = fetch_code_list(spec.BASE_URL, code, lang)
        key = spec.code_list_object_key(run_id, code, lang)
        sha = rustfs.put_bytes(spec.BUCKET, key, body)
        artifacts.append(
            Artifact(
                key=f"codelist_{code}_{lang}",
                object_key=key,
                content_sha256=sha,
                content_length_bytes=len(body),
                records_written=0,
            )
        )

    manifest = build_manifest(
        run_id=run_id,
        source=spec.SOURCE_NAME,
        workflow_id=f"dagster-run-{context.run_id}",
        artifacts=artifacts,
    )
    rustfs.put_json(spec.BUCKET, spec.manifest_object_key(run_id), manifest)

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "bucket": spec.BUCKET,
            "snapshot_object_key": snapshot_key,
            "records": record_stats.records,
            "snapshot_bytes": upload_stats.bytes_read,
            "snapshot_sha256": upload_stats.sha256_hex,
            "artifact_count": len(artifacts),
        }
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_raw_snapshot.py -v`
Expected: 1 passed

- [ ] **Step 5: Run the full suite**

Run: `.venv/bin/python -m pytest tests -v`
Expected: all passed

- [ ] **Step 6: Commit**

```bash
git add dagster_corpscout/sources/finland_prhytj/assets.py tests/test_raw_snapshot.py
git commit -m "feat(dagster): add finland prhytj raw_snapshot pull asset"
```

---

### Task 8: Schedule and Definitions wiring

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/finland_prhytj/schedules.py`
- Modify: `corpscout/dagster/dagster_corpscout/definitions.py`
- Test: `corpscout/dagster/tests/test_definitions.py` (extend)

The schedule starts STOPPED — per the spec's cutover plan, schedules are
enabled at cutover step 4, not on first deploy.

- [ ] **Step 1: Extend the test (failing)**

Replace `tests/test_definitions.py` with:

```python
import dagster as dg


def test_definitions_load():
    from dagster_corpscout.definitions import defs

    assets_def = defs.get_assets_def(dg.AssetKey(["finland_prhytj", "raw_snapshot"]))
    assert assets_def is not None


def test_pull_schedule_exists_and_is_stopped():
    from dagster_corpscout.definitions import defs

    schedule = defs.get_schedule_def("finland_prhytj_pull_schedule")
    assert schedule.cron_schedule == "0 3 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_definitions.py -v`
Expected: FAIL (asset not in defs / schedule missing)

- [ ] **Step 3: Write implementation**

`schedules.py`:

```python
import dagster as dg

from dagster_corpscout.sources.finland_prhytj.assets import raw_snapshot

pull_job = dg.define_asset_job(
    name="finland_prhytj_pull",
    selection=[raw_snapshot],
)

pull_schedule = dg.ScheduleDefinition(
    name="finland_prhytj_pull_schedule",
    job=pull_job,
    cron_schedule="0 3 * * 1",  # weekly, Monday 03:00 UTC
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

`definitions.py`:

```python
import dagster as dg

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland_prhytj.assets import raw_snapshot
from dagster_corpscout.sources.finland_prhytj.schedules import pull_job, pull_schedule

defs = dg.Definitions(
    assets=[raw_snapshot],
    jobs=[pull_job],
    schedules=[pull_schedule],
    resources={
        "rustfs": RustFSResource(
            endpoint_url=dg.EnvVar("CORPSCOUT_S3_ENDPOINT"),
            access_key=dg.EnvVar("CORPSCOUT_S3_ACCESS_KEY"),
            secret_key=dg.EnvVar("CORPSCOUT_S3_SECRET_KEY"),
        )
    },
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `CORPSCOUT_S3_ENDPOINT=http://x CORPSCOUT_S3_ACCESS_KEY=x CORPSCOUT_S3_SECRET_KEY=x .venv/bin/python -m pytest tests -v`
(EnvVar values resolve lazily, but set them so any eager resolution in future dagster versions cannot break the suite.)
Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add dagster_corpscout/definitions.py dagster_corpscout/sources/finland_prhytj/schedules.py tests/test_definitions.py
git commit -m "feat(dagster): wire raw_snapshot asset, weekly schedule, definitions"
```

---

### Task 9: Instance config, Docker image, compose stack, bucket script

**Files:**
- Create: `corpscout/dagster/dagster.yaml`
- Create: `corpscout/dagster/workspace.yaml`
- Create: `corpscout/dagster/Dockerfile`
- Create: `corpscout/dagster/docker-compose.yml`
- Create: `corpscout/dagster/.env.example`
- Create: `corpscout/dagster/scripts/create_buckets.py`

All containers use `network_mode: host`, matching the server convention for
the Temporal services (services reach each other via `localhost` on the
server). `DAGSTER_HOME=/opt/dagster/home` inside images.

- [ ] **Step 1: Write the config files**

`dagster.yaml`:

```yaml
storage:
  postgres:
    postgres_url:
      env: DAGSTER_PG_URL

run_launcher:
  module: dagster_docker
  class: DockerRunLauncher
  config:
    image:
      env: DAGSTER_RUN_IMAGE
    env_vars:
      - DAGSTER_PG_URL
      - CORPSCOUT_S3_ENDPOINT
      - CORPSCOUT_S3_ACCESS_KEY
      - CORPSCOUT_S3_SECRET_KEY
    container_kwargs:
      network_mode: host
      mem_limit: 2g

run_queue:
  max_concurrent_runs: 4
  tag_concurrency_limits:
    - key: dagster/concurrency_key
      limit: 1
      value:
        applyLimitPerUniqueValue: true

run_retries:
  enabled: true
```

`workspace.yaml`:

```yaml
load_from:
  - grpc_server:
      host: localhost
      port: 4266
      location_name: dagster_corpscout
```

`Dockerfile`:

```dockerfile
FROM python:3.12-slim
ENV DAGSTER_HOME=/opt/dagster/home
WORKDIR /opt/dagster/app
COPY pyproject.toml ./
COPY dagster_corpscout ./dagster_corpscout
RUN pip install --no-cache-dir . dagster-webserver
COPY dagster.yaml workspace.yaml /opt/dagster/home/
```

`docker-compose.yml`:

```yaml
services:
  dagster-code:
    build: .
    image: dagster-corpscout:latest
    restart: unless-stopped
    network_mode: host
    env_file: .env
    command: dagster api grpc -h 0.0.0.0 -p 4266 -m dagster_corpscout.definitions

  dagster-webserver:
    image: dagster-corpscout:latest
    restart: unless-stopped
    network_mode: host
    env_file: .env
    command: dagster-webserver -h 0.0.0.0 -p 3500 -w /opt/dagster/home/workspace.yaml
    depends_on: [dagster-code]

  dagster-daemon:
    image: dagster-corpscout:latest
    restart: unless-stopped
    network_mode: host
    env_file: .env
    # The daemon launches run containers via the host docker socket.
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    command: dagster-daemon run -w /opt/dagster/home/workspace.yaml
    depends_on: [dagster-code]
```

`.env.example`:

```bash
# Postgres database for Dagster's own storage (create once: CREATE DATABASE dagster)
DAGSTER_PG_URL=postgresql://corpscout:CHANGE_ME@localhost:5432/dagster
# Image used by DockerRunLauncher for run containers
DAGSTER_RUN_IMAGE=dagster-corpscout:latest
# RustFS (S3-compatible) — on the server, localhost; from the Mac, companycollect
CORPSCOUT_S3_ENDPOINT=http://localhost:9000
CORPSCOUT_S3_ACCESS_KEY=CHANGE_ME
CORPSCOUT_S3_SECRET_KEY=CHANGE_ME
```

`scripts/create_buckets.py`:

```python
"""One-time idempotent bucket creation for Finland source buckets."""

import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKETS = ["source-finland-prhytj", "source-finland-prh-xbrl"]


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        aws_access_key_id=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    for bucket in BUCKETS:
        try:
            client.create_bucket(Bucket=bucket)
            print(f"created {bucket}")
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                print(f"exists {bucket}")
            else:
                raise


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Validate compose and build locally**

```bash
cp .env.example .env   # placeholder values are fine for validation
docker compose config -q && echo COMPOSE_OK
docker build -t dagster-corpscout:latest .
```

Expected: `COMPOSE_OK`, image builds.

- [ ] **Step 3: Smoke-run definitions inside the image**

```bash
docker run --rm --env-file .env dagster-corpscout:latest \
  python -c "from dagster_corpscout.definitions import defs; print('DEFS_OK')"
```

Expected: `DEFS_OK`

- [ ] **Step 4: Commit**

```bash
git rm --cached .env 2>/dev/null; rm -f .env
git add dagster.yaml workspace.yaml Dockerfile docker-compose.yml .env.example scripts/create_buckets.py
git commit -m "feat(dagster): add instance config, docker image, compose stack, bucket script"
```

---

### Task 10: Server deploy and run-launcher validation (manual runbook)

This is the spec's "First Implementation Step" pass-criteria gate. No code —
exact commands and expected observations. Server: `companycollect`
(`100.85.212.113`), SSH `graovic@100.85.212.113`.

- [ ] **Step 1: Create the Dagster Postgres database (idempotent)**

```bash
printf '%s\n' "SELECT 'CREATE DATABASE dagster OWNER corpscout' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dagster')\gexec" | \
  docker run --rm -i postgres:16-alpine psql \
  "postgres://corpscout:<password>@100.85.212.113:5432/corpscout?sslmode=disable" -f -
```

Expected: `CREATE DATABASE` on first run; no output and exit 0 on re-run.

- [ ] **Step 2: Create buckets (once)**

```bash
cd corpscout/dagster
CORPSCOUT_S3_ENDPOINT=http://100.85.212.113:9000 \
CORPSCOUT_S3_ACCESS_KEY=<key> CORPSCOUT_S3_SECRET_KEY=<secret> \
.venv/bin/python scripts/create_buckets.py
```

Expected: `created source-finland-prhytj`, `created source-finland-prh-xbrl`.

- [ ] **Step 3: Deploy to server**

```bash
rsync -av --exclude='.env' --exclude='.venv' \
  corpscout/dagster/ graovic@100.85.212.113:/home/graovic/dagster/

ssh graovic@100.85.212.113 \
  "cd /home/graovic/dagster && cp -n .env.example .env"
# Then edit /home/graovic/dagster/.env on the server with real values
# (localhost endpoints — the server convention; never copy the Mac .env).

ssh graovic@100.85.212.113 \
  "cd /home/graovic/dagster && docker compose up -d --build"
```

Expected: three containers running (`docker compose ps`: dagster-code,
dagster-webserver, dagster-daemon all Up).

- [ ] **Step 4: Verify the UI and definitions**

Open `http://100.85.212.113:3500`. Expected: the asset
`finland_prhytj / raw_snapshot` appears in the asset catalog; the schedule
`finland_prhytj_pull_schedule` is listed and **Stopped**.

- [ ] **Step 5: Trigger the real pull and validate the pass criteria**

In the UI, materialize `finland_prhytj/raw_snapshot`. While it runs, on the
server:

```bash
ssh graovic@100.85.212.113 "docker ps | grep dagster-run"   # ephemeral run container exists
ssh graovic@100.85.212.113 "docker stats --no-stream \$(docker ps -q --filter name=dagster-run)"
```

Pass criteria (all from the spec, check each):

1. The run executes in an ephemeral `DockerRunLauncher` container, not in the
   code-location container.
2. Run container memory stays under the 2g limit while the multi-GB snapshot
   streams (expect tens of MB working set).
3. The run completes: materialization metadata shows `records` (~hundreds of
   thousands), `snapshot_bytes` (GB scale), `snapshot_sha256`, `run_id`.
4. Bucket contents are complete:

```bash
ssh graovic@100.85.212.113 "docker run --rm --network host -e AWS_ACCESS_KEY_ID=<key> -e AWS_SECRET_ACCESS_KEY=<secret> amazon/aws-cli --endpoint-url http://localhost:9000 s3 ls s3://source-finland-prhytj/ --recursive"
```

Expected keys: `runs/{run_id}/source.ndjson` (GB scale),
7 × `runs/{run_id}/codelists/*.en.tsv`, `runs/{run_id}/manifest.json`.

5. Re-materialize while the first run is still queued/running at most once:
   the `dagster/concurrency_key` tag limit holds (second run queues, does not
   execute concurrently).

- [ ] **Step 6: Record the validation**

Append results (run id, duration, record count, bytes, peak memory) to the
plan file under this task, commit:

```bash
git add docs/superpowers/plans/2026-06-11-dagster-foundation-pull-slice.md
git commit -m "docs: record dagster run-launcher validation results"
```

---

## Out of Scope for This Plan

- Python import port + bake-off parity (Plan 2)
- NACE / mapping / explorer-cache assets, asset checks, `data_sources`
  catalog sync from the code location (Plan 3)
- slim Go Temporal worker, XBRL RustFS + manifest changes, external asset +
  sensor (Plan 3)
- cutover (Plan 4)
