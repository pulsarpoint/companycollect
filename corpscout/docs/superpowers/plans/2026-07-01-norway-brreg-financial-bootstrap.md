# Norway BRREG Financial Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a one-time Temporal-backed Python bootstrap job that uploads missing Norway BRREG historical financial raw fetch parquet objects to S3, while Dagster converts those raw objects into historical statement parquet.

**Architecture:** Add a separate import package, `norway_financial_bootstrap`, inside the existing `dagster_v3` Python project. Temporal reads the current normalized `no_companies` parquet, skips every org/year that already has `norway_brreg/financial/raw_fetches/org=<org>/year=<year>/financial_fetch.parquet`, fetches only missing reports, and writes the same raw fetch parquet format. Dagster gets a historical financial resource that lists/reads those raw fetch objects and the snapshot statement asset converts them into `norway_brreg/financial/statements/snapshot/financial_statements.parquet`.

**Tech Stack:** Python 3.14, `uv`, `temporalio`, `boto3`, `requests`, `polars`, `pyarrow`, Dagster definitions, pytest.

---

## File Structure

- Create `corpscout/dagster_v3/norway_financial_bootstrap/__init__.py`  
  Package marker.
- Create `corpscout/dagster_v3/norway_financial_bootstrap/storage.py`  
  S3 key helpers, existing raw-fetch discovery, raw fetch parquet write/read.
- Create `corpscout/dagster_v3/norway_financial_bootstrap/candidates.py`  
  Reads `no_companies` parquet and builds deterministic org/year candidates.
- Create `corpscout/dagster_v3/norway_financial_bootstrap/brreg_client.py`  
  BRREG `GET /regnskap/{org_number}` client and response-to-fetch-row mapping.
- Create `corpscout/dagster_v3/norway_financial_bootstrap/activities.py`  
  Temporal activity that fetches a batch and writes one raw fetch parquet per missing org/year.
- Create `corpscout/dagster_v3/norway_financial_bootstrap/workflows.py`  
  `NorwayBrregInitialFinancialRawFetchWorkflow`.
- Create `corpscout/dagster_v3/norway_financial_bootstrap/cli.py`  
  CLI to start the workflow.
- Modify `corpscout/dagster_v3/pyproject.toml`  
  Include the new package and CLI script.
- Modify `corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_storage.py`  
  Add historical raw fetch listing/reading helpers for Dagster.
- Modify `corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_statements.py`  
  Make snapshot statements read historical raw fetches directly and write statement snapshot parquet.
- Modify `corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_fetches.py`  
  Remove or guard historical snapshot crawling so Dagster never starts the multi-day BRREG crawl.

## Task 1: Bootstrap Storage And Candidates

**Files:**
- Create: `corpscout/dagster_v3/norway_financial_bootstrap/__init__.py`
- Create: `corpscout/dagster_v3/norway_financial_bootstrap/storage.py`
- Create: `corpscout/dagster_v3/norway_financial_bootstrap/candidates.py`
- Test: `corpscout/dagster_v3/tests/test_norway_financial_bootstrap_storage.py`
- Test: `corpscout/dagster_v3/tests/test_norway_financial_bootstrap_candidates.py`

- [ ] **Step 1: Write failing storage tests**

Create `tests/test_norway_financial_bootstrap_storage.py`:

```python
from norway_financial_bootstrap.storage import (
    completed_key_from_raw_fetch_key,
    raw_fetch_key,
)


def test_raw_fetch_key_matches_existing_norway_financial_storage_contract() -> None:
    assert raw_fetch_key("811685852", "2024") == (
        "norway_brreg/financial/raw_fetches/org=811685852/"
        "year=2024/financial_fetch.parquet"
    )


def test_completed_key_from_raw_fetch_key_parses_existing_storage_path() -> None:
    assert completed_key_from_raw_fetch_key(
        "norway_brreg/financial/raw_fetches/org=811685852/"
        "year=2024/financial_fetch.parquet"
    ) == ("811685852", "2024")
```

- [ ] **Step 2: Write failing candidate tests**

Create `tests/test_norway_financial_bootstrap_candidates.py`:

```python
import polars as pl

from norway_financial_bootstrap.candidates import (
    FinancialCandidate,
    build_financial_candidates,
    missing_candidates,
)


def test_build_financial_candidates_filters_active_companies_with_accounts_year() -> None:
    frame = pl.DataFrame(
        [
            {
                "org_number": "200",
                "name": "B AS",
                "primary_website_url": "https://b.example",
                "is_active": True,
                "last_submitted_accounts_year": "2024",
            },
            {
                "org_number": "100",
                "name": "A AS",
                "primary_website_url": None,
                "is_active": True,
                "last_submitted_accounts_year": "2023",
            },
            {
                "org_number": "300",
                "name": "INACTIVE AS",
                "primary_website_url": "",
                "is_active": False,
                "last_submitted_accounts_year": "2024",
            },
        ]
    )

    assert build_financial_candidates(frame) == [
        FinancialCandidate("100", "A AS", "", "2023"),
        FinancialCandidate("200", "B AS", "https://b.example", "2024"),
    ]


def test_missing_candidates_skips_existing_raw_fetch_org_years() -> None:
    candidates = [
        FinancialCandidate("100", "A AS", "", "2023"),
        FinancialCandidate("200", "B AS", "", "2024"),
    ]

    assert missing_candidates(candidates, {("100", "2023")}) == [
        FinancialCandidate("200", "B AS", "", "2024")
    ]
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest \
  tests/test_norway_financial_bootstrap_storage.py \
  tests/test_norway_financial_bootstrap_candidates.py \
  -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'norway_financial_bootstrap'`.

- [ ] **Step 4: Implement storage and candidate modules**

Create `norway_financial_bootstrap/__init__.py`:

```python
"""One-time Norway BRREG financial raw-fetch bootstrap package."""
```

Create `norway_financial_bootstrap/storage.py`:

```python
from __future__ import annotations

import re
from dataclasses import dataclass
from io import BytesIO
from typing import Any

import boto3
import polars as pl
from botocore.config import Config

DEFAULT_BUCKET = "source-finland-prhytj"
RAW_FETCH_PREFIX = "norway_brreg/financial/raw_fetches/"
_RAW_FETCH_RE = re.compile(
    r"^norway_brreg/financial/raw_fetches/org=(?P<org_number>[^/]+)/"
    r"year=(?P<accounts_year>[^/]+)/financial_fetch[.]parquet$"
)


def raw_fetch_key(org_number: str, accounts_year: str) -> str:
    return (
        f"{RAW_FETCH_PREFIX}org={org_number}/year={accounts_year}/"
        "financial_fetch.parquet"
    )


def completed_key_from_raw_fetch_key(key: str) -> tuple[str, str] | None:
    match = _RAW_FETCH_RE.match(key)
    if match is None:
        return None
    return match.group("org_number"), match.group("accounts_year")


@dataclass
class NorwayFinancialBootstrapStorage:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str = DEFAULT_BUCKET
    region_name: str = "us-east-1"
    s3_client: Any | None = None

    def client(self) -> Any:
        if self.s3_client is None:
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region_name,
                config=Config(s3={"addressing_style": "path"}),
            )
        return self.s3_client

    def list_keys(self, prefix: str) -> list[str]:
        paginator = self.client().get_paginator("list_objects_v2")
        return [
            item["Key"]
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            for item in page.get("Contents", [])
        ]

    def existing_raw_fetch_org_years(self) -> set[tuple[str, str]]:
        return {
            parsed
            for key in self.list_keys(RAW_FETCH_PREFIX)
            if (parsed := completed_key_from_raw_fetch_key(key)) is not None
        }

    def read_parquet(self, key: str) -> pl.DataFrame:
        body = self.client().get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return pl.read_parquet(BytesIO(body))

    def write_raw_fetch(self, org_number: str, accounts_year: str, frame: pl.DataFrame) -> str:
        key = raw_fetch_key(org_number, accounts_year)
        buffer = BytesIO()
        frame.write_parquet(buffer)
        self.client().put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        return key
```

Create `norway_financial_bootstrap/candidates.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True, order=True)
class FinancialCandidate:
    org_number: str
    legal_name: str
    website: str
    last_submitted_accounts_year: str

    def as_org_mapping(self) -> dict[str, str]:
        return {
            "org_number": self.org_number,
            "legal_name": self.legal_name,
            "website": self.website,
            "last_submitted_accounts_year": self.last_submitted_accounts_year,
        }


def build_financial_candidates(frame: pl.DataFrame) -> list[FinancialCandidate]:
    if frame.is_empty():
        return []
    rows = (
        frame.filter(
            pl.col("is_active")
            & pl.col("last_submitted_accounts_year").is_not_null()
        )
        .select(
            [
                pl.col("org_number").cast(pl.Utf8),
                pl.col("name").fill_null("").cast(pl.Utf8),
                pl.col("primary_website_url").fill_null("").cast(pl.Utf8),
                pl.col("last_submitted_accounts_year").cast(pl.Utf8),
            ]
        )
        .sort("org_number")
        .to_dicts()
    )
    return [
        FinancialCandidate(
            org_number=str(row["org_number"]),
            legal_name=str(row["name"] or ""),
            website=str(row["primary_website_url"] or ""),
            last_submitted_accounts_year=str(row["last_submitted_accounts_year"] or ""),
        )
        for row in rows
    ]


def missing_candidates(
    candidates: list[FinancialCandidate],
    completed_org_years: set[tuple[str, str]],
) -> list[FinancialCandidate]:
    return [
        candidate
        for candidate in candidates
        if (candidate.org_number, candidate.last_submitted_accounts_year)
        not in completed_org_years
    ]
```

- [ ] **Step 5: Run tests and verify they pass**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest \
  tests/test_norway_financial_bootstrap_storage.py \
  tests/test_norway_financial_bootstrap_candidates.py \
  -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add corpscout/dagster_v3/norway_financial_bootstrap \
  corpscout/dagster_v3/tests/test_norway_financial_bootstrap_storage.py \
  corpscout/dagster_v3/tests/test_norway_financial_bootstrap_candidates.py
git commit -m "Add Norway financial bootstrap storage and candidates"
```

## Task 2: BRREG Client And Raw Fetch Parquet Rows

**Files:**
- Create: `corpscout/dagster_v3/norway_financial_bootstrap/brreg_client.py`
- Test: `corpscout/dagster_v3/tests/test_norway_financial_bootstrap_brreg_client.py`

- [ ] **Step 1: Write failing client tests**

Create `tests/test_norway_financial_bootstrap_brreg_client.py`:

```python
from typing import Any

from norway_financial_bootstrap.brreg_client import BrregFinancialClient
from norway_financial_bootstrap.candidates import FinancialCandidate


class FakeResponse:
    def __init__(self, status_code: int, payload: Any, text: str) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> Any:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse | Exception]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, timeout: int) -> FakeResponse:
        self.calls.append(url)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_brreg_client_maps_success_to_existing_fetch_row_contract() -> None:
    session = FakeSession(
        [FakeResponse(200, [{"id": 1}], '[{"id":1}]')]
    )
    client = BrregFinancialClient(session=session, sleep=lambda _seconds: None)

    row = client.fetch_candidate(
        FinancialCandidate("811685852", "SUCCESS AS", "", "2024"),
        source_run_id="run-1",
        source_line_number=1,
        fetched_at="2026-07-01T00:00:00.000Z",
    )

    assert session.calls == [
        "https://data.brreg.no/regnskapsregisteret/regnskap/811685852"
    ]
    assert row["fetch_status"] == "success"
    assert row["attempt_count"] == 1
    assert row["raw_response"] == '[{"id":1}]'


def test_brreg_client_does_not_retry_404() -> None:
    session = FakeSession([FakeResponse(404, {"message": "missing"}, "missing")])
    client = BrregFinancialClient(session=session, sleep=lambda _seconds: None)

    row = client.fetch_candidate(
        FinancialCandidate("811685852", "MISSING AS", "", "2024"),
        source_run_id="run-1",
        source_line_number=1,
        fetched_at="2026-07-01T00:00:00.000Z",
    )

    assert row["fetch_status"] == "not_found"
    assert row["attempt_count"] == 1


def test_brreg_client_retries_500_then_records_success_attempt_count() -> None:
    sleeps: list[float] = []
    session = FakeSession(
        [
            FakeResponse(500, {"message": "server"}, "server"),
            FakeResponse(200, [{"id": 1}], '[{"id":1}]'),
        ]
    )
    client = BrregFinancialClient(session=session, sleep=sleeps.append)

    row = client.fetch_candidate(
        FinancialCandidate("811685852", "SUCCESS AS", "", "2024"),
        source_run_id="run-1",
        source_line_number=1,
        fetched_at="2026-07-01T00:00:00.000Z",
    )

    assert row["fetch_status"] == "success"
    assert row["attempt_count"] == 2
    assert sleeps == [30.0]
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_norway_financial_bootstrap_brreg_client.py -q
```

Expected: FAIL with missing module.

- [ ] **Step 3: Implement BRREG client**

Create `norway_financial_bootstrap/brreg_client.py` using the existing fetch row contract:

```python
from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Callable

import requests

from norway_financial_bootstrap.candidates import FinancialCandidate

BRREG_REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-norway-financial-bootstrap/0.1"
RETRY_DELAYS_SECONDS = (30.0, 60.0, 120.0, 240.0)


class BrregFinancialClient:
    def __init__(
        self,
        *,
        base_url: str = BRREG_REGNSKAP_BASE_URL,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        user_agent: str = DEFAULT_USER_AGENT,
        session: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.sleep = sleep
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": user_agent})

    def fetch_candidate(
        self,
        candidate: FinancialCandidate,
        *,
        source_run_id: str,
        source_line_number: int,
        fetched_at: str | None = None,
    ) -> dict[str, Any]:
        timestamp = fetched_at or utc_now_iso()
        source_url = f"{self.base_url}/{candidate.org_number}"
        last_failure: dict[str, Any] | None = None
        for attempt_index in range(len(RETRY_DELAYS_SECONDS) + 1):
            attempt_count = attempt_index + 1
            try:
                response = self.session.get(source_url, timeout=self.timeout_seconds)
            except Exception as exc:
                last_failure = failure_row(
                    candidate,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    status_code=None,
                    fetch_status="network_error",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    fetched_at=timestamp,
                    attempt_count=attempt_count,
                    raw_response="",
                )
            else:
                row = row_from_response(
                    candidate,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    response=response,
                    fetched_at=timestamp,
                    attempt_count=attempt_count,
                )
                if row["fetch_status"] in {"success", "not_found", "gone", "empty"}:
                    return row
                last_failure = row
            if attempt_index < len(RETRY_DELAYS_SECONDS):
                self.sleep(RETRY_DELAYS_SECONDS[attempt_index])
        if last_failure is None:
            raise RuntimeError("BRREG fetch failed without a recorded failure row")
        return last_failure


def row_from_response(
    candidate: FinancialCandidate,
    *,
    source_url: str,
    source_run_id: str,
    source_line_number: int,
    response: Any,
    fetched_at: str,
    attempt_count: int,
) -> dict[str, Any]:
    if response.status_code == 404:
        return failure_row(
            candidate,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=404,
            fetch_status="not_found",
            error_type="HTTPStatusError",
            error_message="HTTP 404",
            fetched_at=fetched_at,
            attempt_count=attempt_count,
            raw_response=response_text(response),
        )
    if response.status_code >= 400:
        return failure_row(
            candidate,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status="server_error",
            error_type="HTTPStatusError",
            error_message=f"HTTP {response.status_code}",
            fetched_at=fetched_at,
            attempt_count=attempt_count,
            raw_response=response_text(response),
        )
    payload = response.json()
    if payload == []:
        return failure_row(
            candidate,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status="empty",
            error_type="",
            error_message="",
            fetched_at=fetched_at,
            attempt_count=attempt_count,
            raw_response=response_text(response),
        )
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        return failure_row(
            candidate,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status="invalid_payload",
            error_type="InvalidPayload",
            error_message="Expected BRREG financial response payload to be a list of objects",
            fetched_at=fetched_at,
            attempt_count=attempt_count,
            raw_response=response_text(response),
        )
    raw_response = json_dumps(payload)
    return base_row(
        candidate,
        source_url=source_url,
        source_run_id=source_run_id,
        source_line_number=source_line_number,
        source_payload_hash=hashlib.sha256(raw_response.encode("utf-8")).hexdigest(),
        fetch_status="success",
        http_status=response.status_code,
        error_type="",
        error_message="",
        attempt_count=attempt_count,
        fetched_at=fetched_at,
        raw_response=raw_response,
    )


def failure_row(
    candidate: FinancialCandidate,
    *,
    source_url: str,
    source_run_id: str,
    source_line_number: int,
    status_code: int | None,
    fetch_status: str,
    error_type: str,
    error_message: str,
    fetched_at: str,
    attempt_count: int,
    raw_response: str,
) -> dict[str, Any]:
    return base_row(
        candidate,
        source_url=source_url,
        source_run_id=source_run_id,
        source_line_number=source_line_number,
        source_payload_hash="0" * 64,
        fetch_status=fetch_status,
        http_status=status_code,
        error_type=error_type,
        error_message=error_message,
        attempt_count=attempt_count,
        fetched_at=fetched_at,
        raw_response=raw_response,
    )


def base_row(
    candidate: FinancialCandidate,
    *,
    source_url: str,
    source_run_id: str,
    source_line_number: int,
    source_payload_hash: str,
    fetch_status: str,
    http_status: int | None,
    error_type: str,
    error_message: str,
    attempt_count: int,
    fetched_at: str,
    raw_response: str,
) -> dict[str, Any]:
    return {
        "country_iso2": "NO",
        "source_slug": "norway_brregregnskap_fetch",
        "source_run_id": source_run_id,
        "source_line_number": source_line_number,
        "source_record_id": candidate.org_number,
        "source_payload_hash": source_payload_hash,
        "org_number": candidate.org_number,
        "legal_name": candidate.legal_name,
        "website": candidate.website,
        "last_submitted_accounts_year": candidate.last_submitted_accounts_year,
        "source_url": source_url,
        "fetch_status": fetch_status,
        "http_status": http_status,
        "error_type": error_type,
        "error_message": error_message,
        "attempt_count": attempt_count,
        "fetched_at": fetched_at,
        "raw_response": raw_response,
    }


def response_text(response: Any) -> str:
    return "" if getattr(response, "text", None) is None else str(response.text)


def json_dumps(payload: Any) -> str:
    return json.dumps(
        payload,
        default=json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
```

- [ ] **Step 4: Run tests and verify they pass**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_norway_financial_bootstrap_brreg_client.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/norway_financial_bootstrap/brreg_client.py \
  corpscout/dagster_v3/tests/test_norway_financial_bootstrap_brreg_client.py
git commit -m "Add Norway financial bootstrap BRREG client"
```

## Task 3: Temporal Fetch Activity And Workflow

**Files:**
- Create: `corpscout/dagster_v3/norway_financial_bootstrap/activities.py`
- Create: `corpscout/dagster_v3/norway_financial_bootstrap/workflows.py`
- Create: `corpscout/dagster_v3/norway_financial_bootstrap/cli.py`
- Modify: `corpscout/dagster_v3/pyproject.toml`
- Test: `corpscout/dagster_v3/tests/test_norway_financial_bootstrap_workflow.py`

- [ ] **Step 1: Write activity test**

Create `tests/test_norway_financial_bootstrap_workflow.py`:

```python
import polars as pl

from norway_financial_bootstrap.activities import FetchBatchInput, fetch_batch
from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.workflows import partition_batches


class FakeClient:
    def __init__(self) -> None:
        self.fetched: list[str] = []

    def fetch_candidate(self, candidate, *, source_run_id, source_line_number, fetched_at):
        self.fetched.append(candidate.org_number)
        return {
            "country_iso2": "NO",
            "source_slug": "norway_brregregnskap_fetch",
            "source_run_id": source_run_id,
            "source_line_number": source_line_number,
            "source_record_id": candidate.org_number,
            "source_payload_hash": "0" * 64,
            "org_number": candidate.org_number,
            "legal_name": candidate.legal_name,
            "website": candidate.website,
            "last_submitted_accounts_year": candidate.last_submitted_accounts_year,
            "source_url": f"https://example.test/{candidate.org_number}",
            "fetch_status": "success",
            "http_status": 200,
            "error_type": "",
            "error_message": "",
            "attempt_count": 1,
            "fetched_at": fetched_at,
            "raw_response": "[]",
        }


class FakeStorage:
    def __init__(self, completed: set[tuple[str, str]]) -> None:
        self.completed = completed
        self.writes: dict[tuple[str, str], pl.DataFrame] = {}

    def existing_raw_fetch_org_years(self) -> set[tuple[str, str]]:
        return set(self.completed)

    def write_raw_fetch(self, org_number: str, accounts_year: str, frame: pl.DataFrame) -> str:
        self.writes[(org_number, accounts_year)] = frame
        return f"raw/{org_number}/{accounts_year}.parquet"


def test_fetch_batch_skips_existing_raw_fetches_and_writes_one_parquet_per_missing_candidate() -> None:
    client = FakeClient()
    storage = FakeStorage(completed={("100", "2024")})

    result = fetch_batch(
        FetchBatchInput(
            source_run_id="run-1",
            fetched_at="2026-07-01T00:00:00.000Z",
            candidates=[
                FinancialCandidate("100", "EXISTING AS", "", "2024"),
                FinancialCandidate("200", "MISSING AS", "", "2024"),
            ],
        ),
        storage=storage,
        client=client,
    )

    assert client.fetched == ["200"]
    assert result.fetched_count == 1
    assert result.skipped_count == 1
    assert ("200", "2024") in storage.writes


def test_partition_batches_splits_candidates_deterministically() -> None:
    candidates = [
        FinancialCandidate("100", "A AS", "", "2024"),
        FinancialCandidate("200", "B AS", "", "2024"),
        FinancialCandidate("300", "C AS", "", "2024"),
    ]

    assert partition_batches(candidates, batch_size=2) == [
        [FinancialCandidate("100", "A AS", "", "2024"), FinancialCandidate("200", "B AS", "", "2024")],
        [FinancialCandidate("300", "C AS", "", "2024")],
    ]
```

- [ ] **Step 2: Run activity/workflow tests and verify they fail**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_norway_financial_bootstrap_workflow.py -q
```

Expected: FAIL with missing modules.

- [ ] **Step 3: Implement activities**

Create `norway_financial_bootstrap/activities.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl
from temporalio import activity

from norway_financial_bootstrap.brreg_client import BrregFinancialClient
from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.storage import NorwayFinancialBootstrapStorage


@dataclass
class FetchBatchInput:
    source_run_id: str
    fetched_at: str
    candidates: list[FinancialCandidate]


@dataclass
class FetchBatchResult:
    fetched_count: int
    skipped_count: int
    status_counts: dict[str, int]


@activity.defn
def fetch_batch(
    input: FetchBatchInput,
    *,
    storage: NorwayFinancialBootstrapStorage | None = None,
    client: BrregFinancialClient | None = None,
) -> FetchBatchResult:
    storage = storage or storage_from_env()
    client = client or BrregFinancialClient()
    completed = storage.existing_raw_fetch_org_years()
    fetched_count = 0
    skipped_count = 0
    status_counts: dict[str, int] = {}

    for index, candidate in enumerate(input.candidates, start=1):
        org_year = (candidate.org_number, candidate.last_submitted_accounts_year)
        if org_year in completed:
            skipped_count += 1
            continue

        row = client.fetch_candidate(
            candidate,
            source_run_id=input.source_run_id,
            source_line_number=index,
            fetched_at=input.fetched_at,
        )
        frame = pl.DataFrame([row])
        storage.write_raw_fetch(
            candidate.org_number,
            candidate.last_submitted_accounts_year,
            frame,
        )
        fetched_count += 1
        status = str(row.get("fetch_status") or "")
        status_counts[status] = status_counts.get(status, 0) + 1
        if index % 25 == 0:
            activity.heartbeat(
                {"processed": index, "fetched": fetched_count, "skipped": skipped_count}
            )

    return FetchBatchResult(
        fetched_count=fetched_count,
        skipped_count=skipped_count,
        status_counts=status_counts,
    )


def storage_from_env() -> NorwayFinancialBootstrapStorage:
    import os

    return NorwayFinancialBootstrapStorage(
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        access_key=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        secret_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
    )
```

- [ ] **Step 4: Implement workflow and CLI**

Create `norway_financial_bootstrap/workflows.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

from norway_financial_bootstrap.activities import (
    FetchBatchInput,
    FetchBatchResult,
    fetch_batch,
)
from norway_financial_bootstrap.candidates import FinancialCandidate


@dataclass
class BootstrapInput:
    candidates: list[FinancialCandidate]
    source_run_id: str
    fetched_at: str
    batch_size: int = 500


@dataclass
class BootstrapResult:
    candidate_count: int
    fetched_count: int
    skipped_count: int
    status_counts: dict[str, int]


def partition_batches(
    candidates: list[FinancialCandidate],
    *,
    batch_size: int,
) -> list[list[FinancialCandidate]]:
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero")
    return [
        candidates[index : index + batch_size]
        for index in range(0, len(candidates), batch_size)
    ]


@workflow.defn
class NorwayBrregInitialFinancialRawFetchWorkflow:
    @workflow.run
    async def run(self, input: BootstrapInput) -> BootstrapResult:
        results: list[FetchBatchResult] = []
        for batch in partition_batches(input.candidates, batch_size=input.batch_size):
            results.append(
                await workflow.execute_activity(
                    fetch_batch,
                    FetchBatchInput(
                        source_run_id=input.source_run_id,
                        fetched_at=input.fetched_at,
                        candidates=batch,
                    ),
                    start_to_close_timeout=timedelta(hours=6),
                    heartbeat_timeout=timedelta(minutes=3),
                )
            )
        status_counts: dict[str, int] = {}
        for result in results:
            for status, count in result.status_counts.items():
                status_counts[status] = status_counts.get(status, 0) + count
        return BootstrapResult(
            candidate_count=len(input.candidates),
            fetched_count=sum(result.fetched_count for result in results),
            skipped_count=sum(result.skipped_count for result in results),
            status_counts=status_counts,
        )
```

Create `norway_financial_bootstrap/cli.py`:

```python
from __future__ import annotations

import argparse
import asyncio
import os
from datetime import UTC, datetime

from temporalio.client import Client

from norway_financial_bootstrap.candidates import (
    build_financial_candidates,
    missing_candidates,
)
from norway_financial_bootstrap.storage import NorwayFinancialBootstrapStorage
from norway_financial_bootstrap.workflows import (
    BootstrapInput,
    NorwayBrregInitialFinancialRawFetchWorkflow,
)


async def start_bootstrap(args: argparse.Namespace) -> None:
    storage = storage_from_env()
    no_companies = storage.read_parquet(args.no_companies_key)
    all_candidates = build_financial_candidates(no_companies)
    candidates = missing_candidates(all_candidates, storage.existing_raw_fetch_org_years())
    workflow_id = f"norway-brreg-financial-raw-fetch-{args.snapshot_date}"
    temporal = await Client.connect(args.temporal_address)
    handle = await temporal.start_workflow(
        NorwayBrregInitialFinancialRawFetchWorkflow.run,
        BootstrapInput(
            candidates=candidates,
            source_run_id=workflow_id,
            fetched_at=datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            batch_size=args.batch_size,
        ),
        id=workflow_id,
        task_queue=args.task_queue,
    )
    print(handle.id)


def storage_from_env() -> NorwayFinancialBootstrapStorage:
    return NorwayFinancialBootstrapStorage(
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        access_key=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        secret_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument(
        "--no-companies-key",
        default="norway_brreg/entities/normalized/snapshot/no_companies.parquet",
    )
    parser.add_argument("--temporal-address", default=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))
    parser.add_argument("--task-queue", default="norway-financial-bootstrap")
    parser.add_argument("--batch-size", type=int, default=500)
    return parser


def main() -> None:
    asyncio.run(start_bootstrap(build_parser().parse_args()))
```

Update `pyproject.toml`:

```toml
[project.scripts]
translator-worker = "translator.worker:worker_main"
translator-import-legacy-queue = "translator.import_legacy:main"
norway-financial-bootstrap = "norway_financial_bootstrap.cli:main"

[tool.hatch.build.targets.wheel]
packages = ["src/dagster_v3", "exchange_rates", "translator", "norway_financial_bootstrap"]
force-include = { "pyproject.toml" = "pyproject.toml" }
```

- [ ] **Step 5: Run tests and import checks**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_norway_financial_bootstrap_workflow.py -q
uv run python -c "import norway_financial_bootstrap.cli; import norway_financial_bootstrap.workflows"
uv run norway-financial-bootstrap --help
```

Expected: PASS / help exits `0`.

- [ ] **Step 6: Commit**

```bash
git add corpscout/dagster_v3/norway_financial_bootstrap \
  corpscout/dagster_v3/pyproject.toml \
  corpscout/dagster_v3/tests/test_norway_financial_bootstrap_workflow.py
git commit -m "Add Norway financial raw fetch bootstrap workflow"
```

## Task 4: Dagster Historical Financial Resource

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_storage.py`
- Test: `corpscout/dagster_v3/tests/test_norway_brreg_financial_storage.py`

- [ ] **Step 1: Write failing resource/storage tests**

Add to `tests/test_norway_brreg_financial_storage.py`:

```python
from dagster_v3.defs.norway_brreg.financial_storage import (
    financial_raw_fetch_object_key,
)


def test_financial_storage_lists_historical_raw_fetch_keys() -> None:
    object_store = FakeObjectStore(
        keys=[
            financial_raw_fetch_object_key("100", "2024"),
            "norway_brreg/financial/raw_fetches/org=bad/readme.txt",
        ]
    )
    storage = NorwayBrregFinancialParquetStorageResource(object_store=object_store)

    assert storage.list_historical_raw_fetch_keys() == [
        financial_raw_fetch_object_key("100", "2024")
    ]
```

If the file lacks `FakeObjectStore`, add:

```python
class FakeObjectStore:
    def __init__(self, keys=None):
        self.keys = keys or []

    def list_keys(self, prefix, bucket=None):
        return [key for key in self.keys if key.startswith(prefix)]
```

- [ ] **Step 2: Run the storage test and verify it fails**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_storage.py::test_financial_storage_lists_historical_raw_fetch_keys -q
```

Expected: FAIL because `list_historical_raw_fetch_keys` does not exist.

- [ ] **Step 3: Add historical raw fetch helpers**

Modify `financial_storage.py`:

```python
RAW_FETCH_PREFIX = "norway_brreg/financial/raw_fetches/"


class NorwayBrregFinancialParquetStorageResource(dg.ConfigurableResource):
    ...

    def list_historical_raw_fetch_keys(self) -> list[str]:
        return sorted(
            key
            for key in self.object_store.list_keys(
                RAW_FETCH_PREFIX,
                bucket=NORWAY_BRREG_ENTITY_BUCKET,
            )
            if key.endswith("/financial_fetch.parquet")
        )

    def read_historical_raw_fetches(self) -> pl.DataFrame:
        frames = [
            self._read_frame(key)
            for key in self.list_historical_raw_fetch_keys()
        ]
        if not frames:
            return pl.DataFrame(schema=FINANCIAL_FETCHES_PARQUET_SCHEMA)
        return pl.concat(frames, how="vertical_relaxed")
```

Import `FINANCIAL_FETCHES_PARQUET_SCHEMA` from `assets.financial_fetches` only if it does not introduce
a cycle. If it creates a cycle, move the schema constant into `financial_fetches.py`'s non-asset helper
module or define a small local schema in `financial_storage.py` with the same columns.

- [ ] **Step 4: Run storage tests**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_storage.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_storage.py \
  corpscout/dagster_v3/tests/test_norway_brreg_financial_storage.py
git commit -m "Add Norway historical financial raw fetch storage"
```

## Task 5: Dagster Snapshot Statements From Historical Raw Fetches

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_statements.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_fetches.py`
- Test: `corpscout/dagster_v3/tests/test_norway_brreg_financial_statement_assets.py`
- Test: `corpscout/dagster_v3/tests/test_norway_brreg_financial_fetch_assets.py`

- [ ] **Step 1: Write failing statement snapshot test**

Add to `tests/test_norway_brreg_financial_statement_assets.py`:

```python
def test_snapshot_statement_asset_reads_historical_raw_fetches_without_fetching_brreg() -> None:
    storage = FakeFinancialStorage(
        historical_raw_fetches_frame=_financial_fetch_frame(
            [
                _success_fetch_row("811685852", "2024"),
            ]
        )
    )

    result = norway_brreg_financial_statements_snapshot_parquet(
        build_op_context(),
        norway_brreg_financial_storage=storage,
    )

    assert result.metadata["row_count"] == 1
    assert storage.written_snapshot_statements.height == 1
```

Extend the fake storage in that test file:

```python
def read_historical_raw_fetches(self) -> pl.DataFrame:
    return self.historical_raw_fetches_frame
```

- [ ] **Step 2: Run the statement test and verify it fails**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_statement_assets.py::test_snapshot_statement_asset_reads_historical_raw_fetches_without_fetching_brreg -q
```

Expected: FAIL because the asset currently reads `read_snapshot_fetches()`.

- [ ] **Step 3: Change snapshot statement asset to read historical raw fetches**

In `assets/financial_statements.py`, update the snapshot statement asset:

```python
fetches = norway_brreg_financial_storage.read_historical_raw_fetches()
statements = financial_normalize.normalize_financial_statement_rows(fetches)
output_key = norway_brreg_financial_storage.write_snapshot_statements(statements)
```

Keep update-partition behavior unchanged. Only the historical snapshot path should switch to historical
raw fetch discovery.

- [ ] **Step 4: Guard the old historical fetch crawler**

In `assets/financial_fetches.py`, make `norway_brreg_financial_fetches_snapshot_parquet` a metadata-only
historical raw fetch inventory asset or remove it from the historical job selection. It must not call
`fetch_financial_rows_for_orgs()` for the snapshot path.

Use this metadata-only shape if keeping the asset:

```python
def norway_brreg_financial_fetches_snapshot_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    frame = norway_brreg_financial_storage.read_historical_raw_fetches()
    status_counts = _status_counts(frame.to_dicts())
    context.log.info(
        "Loaded Norway historical financial raw fetches: rows=%d status_counts=%s",
        frame.height,
        status_counts,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": frame.height,
            "status_counts": status_counts,
        }
    )
```

- [ ] **Step 5: Run focused Dagster tests**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest \
  tests/test_norway_brreg_financial_fetch_assets.py \
  tests/test_norway_brreg_financial_statement_assets.py \
  -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_statements.py \
  corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_fetches.py \
  corpscout/dagster_v3/tests/test_norway_brreg_financial_statement_assets.py \
  corpscout/dagster_v3/tests/test_norway_brreg_financial_fetch_assets.py
git commit -m "Build Norway historical financial statements from raw fetches"
```

## Task 6: Verification

**Files:**
- No source edits unless verification exposes a bug in previous tasks.

- [ ] **Step 1: Run focused Norway financial suite**

Run:

```bash
cd corpscout/dagster_v3
uv run pytest \
  tests/test_norway_financial_bootstrap_storage.py \
  tests/test_norway_financial_bootstrap_candidates.py \
  tests/test_norway_financial_bootstrap_brreg_client.py \
  tests/test_norway_financial_bootstrap_workflow.py \
  tests/test_norway_brreg_financial_storage.py \
  tests/test_norway_brreg_financial_fetch_assets.py \
  tests/test_norway_brreg_financial_statement_assets.py \
  -q
```

Expected: PASS.

- [ ] **Step 2: Run Dagster definitions check**

Run:

```bash
cd corpscout/dagster_v3
uv run dg check defs
```

Expected: PASS.

- [ ] **Step 3: Run CLI import/help check**

Run:

```bash
cd corpscout/dagster_v3
uv run norway-financial-bootstrap --help
```

Expected: command prints usage and exits `0`.

- [ ] **Step 4: Commit verification fixes if needed**

If verification required changes:

```bash
git add <changed-files>
git commit -m "Fix Norway financial bootstrap verification issues"
```

If there were no changes, do not create an empty commit.

## Rollout Notes

1. Deploy the package and updated Dagster code.
2. Start the Temporal worker process for the `norway-financial-bootstrap` task queue.
3. Start the bootstrap:

```bash
cd /opt/companycollect/corpscout/dagster_v3
uv run norway-financial-bootstrap --snapshot-date 2026-07-01 --batch-size 500
```

4. Watch Temporal until `NorwayBrregInitialFinancialRawFetchWorkflow` completes.
5. Confirm raw fetch count is close to the historical candidate count:

```text
norway_brreg/financial/raw_fetches/org=<org_number>/year=<year>/financial_fetch.parquet
```

6. Materialize the Dagster historical statement path:

```text
norway_brreg_financial_statements_snapshot_parquet
norway_brreg_financial_statements_snapshot_usd_parquet
norway_brreg_financial_statements_snapshot_clickhouse
```

7. Leave daily update jobs as the recurring path.

## Self-Review Checklist

- Existing `financial_fetch.parquet` raw storage is preserved and skipped in Tasks 1, 3, and rollout.
- Temporal stops at raw fetch parquet upload in Tasks 2 and 3.
- Dagster builds `financial_statements.parquet` from historical raw fetches in Tasks 4 and 5.
- Daily update assets are unchanged.
- No generic multi-country framework is introduced.
- No ClickHouse checkpoint store is introduced.
