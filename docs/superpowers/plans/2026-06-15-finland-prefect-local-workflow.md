# Finland Prefect Local Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained local Prefect workflow in `companycollect/processor` that copies the Finland walkthrough logic, downloads bounded PRH samples, produces structured and canonical Parquet outputs, writes a portable manifest, and creates a Prefect summary artifact.

**Architecture:** Keep orchestration in one visible Prefect flow and put source-specific logic in small plain-Python modules under `processor/finland`. The flow writes local files under `processor/output/finland/<run_id>/`, while Prefect stores operational metadata through flow run names, parameters, tags, and a Markdown artifact.

**Tech Stack:** Python 3.12, Prefect 3, Polars, PyArrow, Requests, lxml, pytest.

---

## File Structure

- Create `companycollect/processor/finland/__init__.py`: package marker and public module documentation.
- Create `companycollect/processor/finland/config.py`: PRH URLs, defaults, metric map, and constants.
- Create `companycollect/processor/finland/schemas.py`: canonical schemas, key rules, and validation helpers.
- Create `companycollect/processor/finland/download.py`: retrying PRH HTTP client functions that write local raw files.
- Create `companycollect/processor/finland/structured.py`: raw YTJ/XBRL to structured Polars frames.
- Create `companycollect/processor/finland/canonical.py`: structured frames to canonical tables.
- Create `companycollect/processor/finland/io.py`: local Parquet and manifest writes.
- Create `companycollect/processor/finland_flow.py`: Prefect task definitions and flow entrypoint.
- Create `companycollect/processor/tests/`: focused tests for pure logic and flow assembly.
- Modify `companycollect/processor/pyproject.toml`: add runtime and dev dependencies.

The code must not import `companies/analysis/finland/notebook/conformance`. Logic is copied into `processor/finland` so the Prefect experiment is self-contained.

## Task 1: Package Scaffold And Dependencies

**Files:**
- Modify: `companycollect/processor/pyproject.toml`
- Create: `companycollect/processor/finland/__init__.py`
- Create: `companycollect/processor/tests/test_imports.py`

- [ ] **Step 1: Write the failing import test**

Create `companycollect/processor/tests/test_imports.py`:

```python
def test_finland_package_imports() -> None:
    import finland.config
    import finland.schemas

    assert finland.config.COUNTRY == "FI"
    assert "registrations" in finland.schemas.CANONICAL_SCHEMAS
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_imports.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'finland'`.

- [ ] **Step 3: Add dependencies**

Modify `companycollect/processor/pyproject.toml` so it contains:

```toml
[project]
name = "processor"
version = "0.1.0"
description = "Local Prefect experiments for company collection pipelines"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "fastapi<0.137",
    "lxml>=5.2",
    "polars>=1.0",
    "prefect>=3.7.4",
    "pyarrow>=17",
    "requests>=2.32",
]

[dependency-groups]
dev = [
    "pytest>=8",
]
```

- [ ] **Step 4: Create package marker**

Create `companycollect/processor/finland/__init__.py`:

```python
"""Local Finland PRH Prefect workflow modules."""
```

- [ ] **Step 5: Add minimal modules for import**

Create `companycollect/processor/finland/config.py`:

```python
COUNTRY = "FI"
```

Create `companycollect/processor/finland/schemas.py`:

```python
CANONICAL_SCHEMAS: dict[str, dict] = {"registrations": {}}
```

- [ ] **Step 6: Run test to verify it passes**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_imports.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add processor/pyproject.toml processor/finland processor/tests/test_imports.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "Add Finland Prefect package scaffold"
```

## Task 2: Config And Schema Validation

**Files:**
- Modify: `companycollect/processor/finland/config.py`
- Modify: `companycollect/processor/finland/schemas.py`
- Create: `companycollect/processor/tests/test_schemas.py`

- [ ] **Step 1: Write schema tests**

Create `companycollect/processor/tests/test_schemas.py`:

```python
import datetime as dt

import polars as pl
import pytest

from finland import schemas


def test_validate_table_accepts_matching_schema_and_unique_key() -> None:
    frame = pl.DataFrame(
        {
            "registration_uid": ["FI:1234567-8"],
            "company_uid": ["c:abc"],
            "country": ["FI"],
            "registration_number": ["1234567-8"],
            "registry_source": ["finland_prhytj"],
            "is_primary": [1],
            "entity_role": ["domestic"],
            "legal_name": ["Example Oy"],
            "legal_form_code": [None],
            "lifecycle_status": ["active"],
            "is_active": [1],
            "incorporation_date": [dt.date(2020, 1, 1)],
            "dissolution_date": [None],
            "addr_street": ["Testikatu 1"],
            "addr_post_code": ["00100"],
            "addr_city": ["Helsinki"],
            "addr_municipality_code": ["091"],
            "addr_country": ["FI"],
            "activity_code": ["62010"],
            "activity_scheme": ["TOL2008"],
            "vat_number": [None],
            "eu_id": [None],
            "lei": [None],
            "primary_website": ["https://example.fi"],
            "source_run_id": ["run-1"],
            "ingested_at": [dt.datetime(2026, 6, 15, 12, 0)],
            "updated_at": [dt.datetime(2026, 6, 15, 12, 0)],
        },
        schema=schemas.REGISTRATIONS,
    )

    schemas.validate_table(frame, schemas.REGISTRATIONS, unique_key="registration_uid")


def test_validate_table_rejects_missing_column() -> None:
    frame = pl.DataFrame({"registration_uid": ["FI:1234567-8"]})

    with pytest.raises(ValueError, match="missing columns"):
        schemas.validate_table(frame, schemas.REGISTRATIONS, unique_key="registration_uid")


def test_validate_table_rejects_duplicate_key() -> None:
    frame = pl.DataFrame(
        {
            "website_uid": ["w1", "w1"],
            "company_uid": ["c1", "c1"],
            "registration_uid": ["FI:1", "FI:1"],
            "country": ["FI", "FI"],
            "scope": ["registration", "registration"],
            "url": ["https://example.fi", "https://example.fi"],
            "normalized_url": ["https://example.fi", "https://example.fi"],
            "host": ["example.fi", "example.fi"],
            "is_primary": [1, 1],
            "source_kind": ["registry", "registry"],
            "discovery_method": ["registry_field", "registry_field"],
            "registry_source": ["finland_prhytj", "finland_prhytj"],
            "confidence": [1.0, 1.0],
            "is_live": [0, 0],
            "first_seen_at": [dt.datetime(2026, 6, 15), dt.datetime(2026, 6, 15)],
            "last_seen_at": [dt.datetime(2026, 6, 15), dt.datetime(2026, 6, 15)],
            "updated_at": [dt.datetime(2026, 6, 15), dt.datetime(2026, 6, 15)],
        },
        schema=schemas.COMPANY_WEBSITES,
    )

    with pytest.raises(ValueError, match="duplicate values"):
        schemas.validate_table(frame, schemas.COMPANY_WEBSITES, unique_key="website_uid")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_schemas.py -v
```

Expected: FAIL because `REGISTRATIONS`, `COMPANY_WEBSITES`, and `validate_table` are not defined.

- [ ] **Step 3: Implement config constants**

Replace `companycollect/processor/finland/config.py` with:

```python
from __future__ import annotations

import datetime as dt

COUNTRY = "FI"
DEFAULT_MAX_COMPANIES = 200
DEFAULT_XBRL_START = "2025-01-01"
DEFAULT_XBRL_END = "2025-01-03"
DEFAULT_OUTPUT_ROOT = "output/finland"
DEFAULT_NOW = dt.datetime(2026, 6, 15)

PRH_YTJ_COMPANIES_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
PRH_XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
USER_AGENT = "corpscout-prefect-local/0.1 (finland)"

METRIC_MAP: dict[tuple[str, str], str] = {
    ("fi_met:md103", "fi_MC:x673"): "revenue",
    ("fi_met:md103", "fi_MC:x689"): "operating_profit_loss",
    ("fi_met:md103", "fi_MC:x740"): "profit_loss",
    ("fi_met:mi53", "fi_MC:x360"): "total_assets",
    ("fi_met:mi53", "fi_MC:x376"): "equity",
    ("fi_met:mi53", "fi_MC:x424"): "liabilities",
    ("fi_met:mi53", "fi_MC:x399"): "cash_and_bank",
    ("fi_met:mi53", "fi_MC:x435"): "current_assets",
    ("fi_met:mi53", "fi_MC:x1768"): "current_receivables",
    ("fi_met:mi53", "fi_MC:x1811"): "current_liabilities",
    ("fi_met:md103", "fi_MC:x5"): "personnel_expenses",
    ("fi_met:md103", "fi_MC:x6"): "wages_and_salaries",
}

DURATION_METRICS = {
    "revenue",
    "operating_profit_loss",
    "profit_loss",
    "personnel_expenses",
    "wages_and_salaries",
}
```

- [ ] **Step 4: Implement schemas and validation**

Replace `companycollect/processor/finland/schemas.py` with:

```python
from __future__ import annotations

import polars as pl

REGISTRATIONS: dict[str, pl.DataType] = {
    "registration_uid": pl.Utf8,
    "company_uid": pl.Utf8,
    "country": pl.Utf8,
    "registration_number": pl.Utf8,
    "registry_source": pl.Utf8,
    "is_primary": pl.UInt8,
    "entity_role": pl.Utf8,
    "legal_name": pl.Utf8,
    "legal_form_code": pl.Utf8,
    "lifecycle_status": pl.Utf8,
    "is_active": pl.UInt8,
    "incorporation_date": pl.Date,
    "dissolution_date": pl.Date,
    "addr_street": pl.Utf8,
    "addr_post_code": pl.Utf8,
    "addr_city": pl.Utf8,
    "addr_municipality_code": pl.Utf8,
    "addr_country": pl.Utf8,
    "activity_code": pl.Utf8,
    "activity_scheme": pl.Utf8,
    "vat_number": pl.Utf8,
    "eu_id": pl.Utf8,
    "lei": pl.Utf8,
    "primary_website": pl.Utf8,
    "source_run_id": pl.Utf8,
    "ingested_at": pl.Datetime,
    "updated_at": pl.Datetime,
}

COMPANY: dict[str, pl.DataType] = {
    "company_uid": pl.Utf8,
    "uid_scheme": pl.Utf8,
    "lei": pl.Utf8,
    "primary_name": pl.Utf8,
    "status": pl.Utf8,
    "legal_form_code": pl.Utf8,
    "home_country": pl.Utf8,
    "incorporation_date": pl.Date,
    "dissolution_date": pl.Date,
    "registration_count": pl.UInt16,
    "operating_countries": pl.List(pl.Utf8),
    "primary_website": pl.Utf8,
    "sources": pl.List(pl.Utf8),
    "resolution_version": pl.Utf8,
    "first_seen_at": pl.Datetime,
    "updated_at": pl.Datetime,
}

FINANCIALS: dict[str, pl.DataType] = {
    "company_uid": pl.Utf8,
    "registration_uid": pl.Utf8,
    "country": pl.Utf8,
    "statement_id": pl.Utf8,
    "period_start": pl.Date,
    "period_end": pl.Date,
    "period_type": pl.Utf8,
    "period_reference": pl.Utf8,
    "basis": pl.Utf8,
    "currency": pl.Utf8,
    "metric_code": pl.Utf8,
    "value": pl.Float64,
    "source_metric_id": pl.Utf8,
    "registry_source": pl.Utf8,
    "mapping_version": pl.Utf8,
    "source_run_id": pl.Utf8,
    "ingested_at": pl.Datetime,
    "updated_at": pl.Datetime,
}

COMPANY_WEBSITES: dict[str, pl.DataType] = {
    "website_uid": pl.Utf8,
    "company_uid": pl.Utf8,
    "registration_uid": pl.Utf8,
    "country": pl.Utf8,
    "scope": pl.Utf8,
    "url": pl.Utf8,
    "normalized_url": pl.Utf8,
    "host": pl.Utf8,
    "is_primary": pl.UInt8,
    "source_kind": pl.Utf8,
    "discovery_method": pl.Utf8,
    "registry_source": pl.Utf8,
    "confidence": pl.Float32,
    "is_live": pl.UInt8,
    "first_seen_at": pl.Datetime,
    "last_seen_at": pl.Datetime,
    "updated_at": pl.Datetime,
}

CANONICAL_SCHEMAS = {
    "registrations": REGISTRATIONS,
    "company": COMPANY,
    "financials": FINANCIALS,
    "company_websites": COMPANY_WEBSITES,
}

UNIQUE_KEYS = {
    "registrations": "registration_uid",
    "company": "company_uid",
    "financials": None,
    "company_websites": "website_uid",
}


def validate_table(df: pl.DataFrame, schema: dict[str, pl.DataType], *, unique_key: str | None = None) -> None:
    missing = set(schema) - set(df.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")

    for name, dtype in schema.items():
        if df.schema[name] != dtype:
            raise ValueError(f"column {name!r} has dtype {df.schema[name]}, expected {dtype}")

    if unique_key is None:
        return

    non_null = df.filter(pl.col(unique_key).is_not_null())
    if non_null.height != non_null.select(unique_key).n_unique():
        raise ValueError(f"duplicate values in key column {unique_key!r}")
```

- [ ] **Step 5: Run schema tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_schemas.py tests/test_imports.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add processor/finland/config.py processor/finland/schemas.py processor/tests/test_schemas.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "Add Finland canonical schemas"
```

## Task 3: Local Raw Downloads

**Files:**
- Create: `companycollect/processor/finland/download.py`
- Create: `companycollect/processor/tests/test_download.py`

- [ ] **Step 1: Write download tests with a fake session**

Create `companycollect/processor/tests/test_download.py`:

```python
import json
from pathlib import Path

from finland import download


class FakeResponse:
    def __init__(self, payload: dict | None = None, content: bytes = b"", status_code: int = 200) -> None:
        self._payload = payload or {}
        self.content = content
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict, timeout: int) -> FakeResponse:
        self.calls.append((url, params))
        if url.endswith("/companies"):
            return FakeResponse(
                {
                    "totalResults": 2,
                    "companies": [
                        {"businessId": {"value": "1234567-8"}, "names": []},
                        {"businessId": {"value": "8765432-1"}, "names": []},
                    ],
                }
            )
        if url.endswith("/all_financial_statements"):
            return FakeResponse(
                {
                    "totalResults": 1,
                    "financials": [
                        {
                            "businessId": "1234567-8",
                            "financialDate": "2024-12-31",
                            "registrationDate": "2025-01-02",
                        }
                    ],
                }
            )
        if url.endswith("/financial"):
            return FakeResponse(content=b"<xbrl/>")
        raise AssertionError(f"unexpected URL {url}")


def test_download_ytj_companies_writes_ndjson(tmp_path: Path) -> None:
    result = download.download_ytj_companies(tmp_path, max_companies=1, session=FakeSession())

    assert result.path == tmp_path / "raw" / "prh_ytj_companies.ndjson"
    assert result.count == 1
    lines = result.path.read_text(encoding="utf-8").splitlines()
    assert json.loads(lines[0])["businessId"]["value"] == "1234567-8"


def test_download_xbrl_window_writes_listing_and_xml(tmp_path: Path) -> None:
    result = download.download_xbrl_window(
        tmp_path,
        registered_start="2025-01-01",
        registered_end="2025-01-03",
        session=FakeSession(),
    )

    assert result.listing_path == tmp_path / "raw" / "xbrl_listing.json"
    assert result.document_count == 1
    listing = json.loads(result.listing_path.read_text(encoding="utf-8"))
    assert listing["documents"][0]["business_id"] == "1234567-8"
    assert (tmp_path / "raw" / "xbrl" / "1234567-8_2024-12-31.xml").read_bytes() == b"<xbrl/>"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_download.py -v
```

Expected: FAIL because `finland.download` does not exist.

- [ ] **Step 3: Implement download module**

Create `companycollect/processor/finland/download.py`:

```python
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlencode

import requests

from finland.config import PRH_XBRL_BASE_URL, PRH_YTJ_COMPANIES_URL, USER_AGENT

TIMEOUT_SECONDS = 120
RETRY_DELAYS = (1.0, 2.0, 4.0)


class HttpSession(Protocol):
    headers: dict[str, str]

    def get(self, url: str, params: dict, timeout: int):
        ...


@dataclass(frozen=True)
class YtjDownloadResult:
    path: Path
    count: int


@dataclass(frozen=True)
class XbrlDownloadResult:
    listing_path: Path
    document_count: int


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def _get(session: HttpSession, url: str, params: dict):
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            response = session.get(url, params=params, timeout=TIMEOUT_SECONDS)
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < len(RETRY_DELAYS):
                    time.sleep(RETRY_DELAYS[attempt])
                    continue
            response.raise_for_status()
            return response
        except (requests.ConnectionError, requests.Timeout):
            if attempt == len(RETRY_DELAYS):
                raise
            time.sleep(RETRY_DELAYS[attempt])
    raise RuntimeError("request retry loop exited unexpectedly")


def download_ytj_companies(run_dir: Path, *, max_companies: int | None, session: HttpSession | None = None) -> YtjDownloadResult:
    raw_dir = run_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_path = raw_dir / "prh_ytj_companies.ndjson"
    http = session or make_session()

    count = 0
    page = 1
    total: int | None = None
    with out_path.open("wb") as handle:
        while True:
            payload = _get(http, PRH_YTJ_COMPANIES_URL, {"page": page}).json()
            if payload.get("totalResults") is not None:
                total = int(payload["totalResults"])

            companies = payload.get("companies") or []
            if not companies:
                break

            for company in companies:
                handle.write(json.dumps(company, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                handle.write(b"\n")
                count += 1
                if max_companies is not None and count >= max_companies:
                    return YtjDownloadResult(path=out_path, count=count)

            if total is not None and count >= total:
                break
            if len(companies) < 100:
                break
            page += 1

    return YtjDownloadResult(path=out_path, count=count)


def _financial_source_url(business_id: str, financial_date: str) -> str:
    return f"{PRH_XBRL_BASE_URL}/financial?" + urlencode(
        {"businessId": business_id, "financialDate": financial_date}
    )


def download_xbrl_window(
    run_dir: Path,
    *,
    registered_start: str,
    registered_end: str,
    session: HttpSession | None = None,
) -> XbrlDownloadResult:
    raw_dir = run_dir / "raw"
    xbrl_dir = raw_dir / "xbrl"
    xbrl_dir.mkdir(parents=True, exist_ok=True)
    listing_path = raw_dir / "xbrl_listing.json"
    http = session or make_session()

    documents: list[dict] = []
    page = 1
    while True:
        payload = _get(
            http,
            f"{PRH_XBRL_BASE_URL}/all_financial_statements",
            {"registeredDateStart": registered_start, "registeredDateEnd": registered_end, "page": page},
        ).json()
        items = payload.get("financials") or []
        if not items:
            break

        for item in items:
            business_id = str(item.get("businessId") or "").strip()
            financial_date = str(item.get("financialDate") or "").strip()
            if not business_id or not financial_date:
                continue

            body = _get(
                http,
                f"{PRH_XBRL_BASE_URL}/financial",
                {"businessId": business_id, "financialDate": financial_date},
            ).content
            xml_path = xbrl_dir / f"{business_id}_{financial_date}.xml"
            xml_path.write_bytes(body)
            documents.append(
                {
                    "business_id": business_id,
                    "financial_date": financial_date,
                    "registration_date": item.get("registrationDate"),
                    "path": str(xml_path.relative_to(run_dir)),
                    "source_url": _financial_source_url(business_id, financial_date),
                }
            )

        total = int(payload.get("totalResults") or 0)
        if total and page * 100 >= total:
            break
        if len(items) < 100:
            break
        page += 1

    listing_path.write_text(json.dumps({"documents": documents, "parse_failures": []}, indent=2), encoding="utf-8")
    return XbrlDownloadResult(listing_path=listing_path, document_count=len(documents))
```

- [ ] **Step 4: Run download tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_download.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add processor/finland/download.py processor/tests/test_download.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "Add Finland local download helpers"
```

## Task 4: YTJ Structured Transform

**Files:**
- Create or modify: `companycollect/processor/finland/structured.py`
- Create: `companycollect/processor/tests/test_structured_ytj.py`

- [ ] **Step 1: Write YTJ transform tests**

Create `companycollect/processor/tests/test_structured_ytj.py`:

```python
import json

from finland.structured import ytj_structured_from_ndjson


def test_ytj_structured_from_ndjson_builds_expected_frames() -> None:
    raw = {
        "businessId": {"value": "1234567-8"},
        "tradeRegisterStatus": "2",
        "registrationDate": "2020-01-01",
        "endDate": None,
        "names": [{"name": "Example Oy", "type": "1", "endDate": None}],
        "website": {"url": "example.fi"},
        "addresses": [
            {
                "type": "1",
                "street": "Testikatu 1",
                "postCode": "00100",
                "postOffices": [{"city": "Helsinki", "municipalityCode": "091"}],
            }
        ],
        "mainBusinessLine": {"type": "62010", "typeCodeSet": "TOL2008"},
    }
    ndjson = (json.dumps(raw) + "\n").encode("utf-8")

    frames = ytj_structured_from_ndjson(ndjson)

    assert frames["fi_prhytj_statuses"].row(0, named=True)["lifecycle_status"] == "active"
    assert frames["fi_prhytj_names"].row(0, named=True)["name"] == "Example Oy"
    assert frames["fi_prhytj_websites"].row(0, named=True)["normalized_url"] == "https://example.fi"
    assert frames["fi_prhytj_websites"].row(0, named=True)["host"] == "example.fi"
    assert frames["fi_prhytj_addresses"].row(0, named=True)["city"] == "Helsinki"
    assert frames["fi_prhytj_business_lines"].row(0, named=True)["business_line_type"] == "62010"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_structured_ytj.py -v
```

Expected: FAIL because `ytj_structured_from_ndjson` is not defined.

- [ ] **Step 3: Implement YTJ structured transform**

Create `companycollect/processor/finland/structured.py` with the YTJ transform:

```python
from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
from lxml import etree


def ytj_structured_from_ndjson(ndjson: bytes) -> dict[str, pl.DataFrame]:
    df = pl.read_ndjson(ndjson, infer_schema_length=None).with_columns(
        pl.col("businessId").struct.field("value").alias("business_id")
    )

    statuses = (
        df.select(
            "business_id",
            pl.col("tradeRegisterStatus").alias("trade_register_status"),
            pl.col("registrationDate").alias("registration_date"),
            pl.col("endDate").fill_null("").alias("end_date"),
        )
        .with_columns(
            pl.when((pl.col("end_date") != "") | (pl.col("trade_register_status") == "3"))
            .then(pl.lit("ceased"))
            .otherwise(pl.lit("active"))
            .alias("lifecycle_status")
        )
        .with_columns((pl.col("lifecycle_status") == "active").alias("is_active"))
    )

    names = (
        df.select("business_id", "names")
        .explode("names")
        .drop_nulls("names")
        .unnest("names")
        .select(
            "business_id",
            "name",
            pl.col("type").alias("name_type_code"),
            pl.col("endDate").is_null().alias("is_current"),
            (pl.col("type") == "1").alias("is_primary"),
        )
    )

    if "website" not in df.columns:
        websites = pl.DataFrame(
            schema={
                "business_id": pl.Utf8,
                "url": pl.Utf8,
                "normalized_url": pl.Utf8,
                "host": pl.Utf8,
                "is_current": pl.Boolean,
                "is_primary": pl.Boolean,
            }
        )
    else:
        websites = (
            df.select("business_id", "website")
            .with_columns(
                pl.when(pl.col("website").is_not_null())
                .then(pl.col("website").struct.field("url"))
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("url")
            )
            .filter(pl.col("url").is_not_null() & (pl.col("url") != ""))
            .with_columns(
                pl.when(pl.col("url").str.contains("://"))
                .then(pl.col("url"))
                .otherwise(pl.concat_str([pl.lit("https://"), pl.col("url")]))
                .alias("normalized_url")
            )
            .with_columns(
                pl.col("normalized_url")
                .str.replace(r"^https?://", "")
                .str.split("/")
                .list.first()
                .str.to_lowercase()
                .alias("host"),
                pl.lit(True).alias("is_current"),
                pl.lit(True).alias("is_primary"),
            )
            .select("business_id", "url", "normalized_url", "host", "is_current", "is_primary")
        )

    address_source = (
        df.select("business_id", "addresses")
        .explode("addresses")
        .drop_nulls("addresses")
        .unnest("addresses")
        .with_columns(
            pl.col("postOffices").list.eval(pl.element().struct.field("city")).list.first().alias("city"),
            pl.col("postOffices")
            .list.eval(pl.element().struct.field("municipalityCode"))
            .list.first()
            .alias("municipality_code"),
        )
    )
    country_expr = pl.col("country") if "country" in address_source.columns else pl.lit("FI")
    addresses = address_source.select(
        "business_id",
        pl.col("type").alias("address_type_code"),
        "street",
        pl.col("postCode").alias("post_code"),
        "city",
        "municipality_code",
        country_expr.alias("country"),
    )

    business_lines = (
        df.select("business_id", "mainBusinessLine")
        .with_columns(
            pl.col("mainBusinessLine").struct.field("type").alias("business_line_type"),
            pl.col("mainBusinessLine").struct.field("typeCodeSet").alias("business_line_code_set"),
        )
        .filter(pl.col("business_line_type").is_not_null())
        .select("business_id", "business_line_type", "business_line_code_set")
    )

    return {
        "fi_prhytj_statuses": statuses,
        "fi_prhytj_names": names,
        "fi_prhytj_websites": websites,
        "fi_prhytj_addresses": addresses,
        "fi_prhytj_business_lines": business_lines,
    }
```

- [ ] **Step 4: Run YTJ structured test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_structured_ytj.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add processor/finland/structured.py processor/tests/test_structured_ytj.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "Add Finland YTJ structured transform"
```

## Task 5: XBRL Structured Facts

**Files:**
- Modify: `companycollect/processor/finland/structured.py`
- Create: `companycollect/processor/tests/test_structured_xbrl.py`

- [ ] **Step 1: Write XBRL parser test**

Create `companycollect/processor/tests/test_structured_xbrl.py`:

```python
from pathlib import Path

from finland.structured import xbrl_facts_from_listing


XBRL_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl
  xmlns:xbrli="http://www.xbrl.org/2003/instance"
  xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
  xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met"
  xmlns:fi_dim="http://www.suomi.fi/xbrl/crr/dict/dim"
  xmlns:fi_MC="http://www.suomi.fi/xbrl/crr/dict/dom/MC">
  <xbrli:context id="c1">
    <xbrli:entity><xbrli:identifier scheme="test">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2024-12-31</xbrli:instant></xbrli:period>
    <xbrli:scenario>
      <xbrldi:explicitMember dimension="fi_dim:MCY">fi_MC:x673</xbrldi:explicitMember>
    </xbrli:scenario>
  </xbrli:context>
  <fi_met:md103 contextRef="c1">12345.67</fi_met:md103>
</xbrli:xbrl>
"""


def test_xbrl_facts_from_listing_extracts_numeric_fact(tmp_path: Path) -> None:
    xml_path = tmp_path / "raw" / "xbrl" / "1234567-8_2024-12-31.xml"
    xml_path.parent.mkdir(parents=True)
    xml_path.write_bytes(XBRL_XML)
    listing = {
        "documents": [
            {
                "business_id": "1234567-8",
                "financial_date": "2024-12-31",
                "registration_date": "2025-01-02",
                "path": "raw/xbrl/1234567-8_2024-12-31.xml",
                "source_url": "https://example.test/financial",
            }
        ]
    }

    facts, failures = xbrl_facts_from_listing(tmp_path, listing)

    assert failures == []
    row = facts.row(0, named=True)
    assert row["business_id"] == "1234567-8"
    assert row["financial_date"] == "2024-12-31"
    assert row["concept_qname"] == "fi_met:md103"
    assert row["mcy_member_code"] == "fi_MC:x673"
    assert row["numeric_value"] == 12345.67
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_structured_xbrl.py -v
```

Expected: FAIL because `xbrl_facts_from_listing` is not defined.

- [ ] **Step 3: Add XBRL parsing functions**

Append to `companycollect/processor/finland/structured.py`:

```python
XBRLI_NS = "http://www.xbrl.org/2003/instance"
XBRLDI_NS = "http://xbrl.org/2006/xbrldi"
MET_NS = "http://www.suomi.fi/xbrl/crr/dict/met"
CANONICAL_PREFIXES = {
    "http://www.suomi.fi/xbrl/crr/dict/met": "fi_met",
    "http://www.suomi.fi/xbrl/crr/dict/dim": "fi_dim",
    "http://www.suomi.fi/xbrl/crr/dict/dom/MC": "fi_MC",
    "http://www.suomi.fi/xbrl/crr/dict/dom/RF": "fi_RF",
}


def _canonical_qname(text: str | None, nsmap: dict) -> str | None:
    if not text or ":" not in text:
        return text
    prefix, local = text.split(":", 1)
    return f"{CANONICAL_PREFIXES.get(nsmap.get(prefix), prefix)}:{local}"


def _parse_xbrl_facts(body: bytes, *, business_id: str, financial_date: str) -> list[dict]:
    root = etree.fromstring(body, parser=etree.XMLParser(resolve_entities=False))
    contexts: dict[str, tuple[str | None, str | None]] = {}
    for context in root.findall(f"{{{XBRLI_NS}}}context"):
        mcy = None
        ref = None
        for member in context.findall(f".//{{{XBRLDI_NS}}}explicitMember"):
            dimension = _canonical_qname(member.get("dimension", ""), member.nsmap)
            value = _canonical_qname((member.text or "").strip(), member.nsmap)
            if dimension and dimension.endswith("MCY"):
                mcy = value
            elif dimension and dimension.endswith("REF"):
                ref = value
        context_id = context.get("id")
        if context_id:
            contexts[context_id] = (mcy, ref)

    statement_key = hashlib.sha256(
        f"{business_id}:{financial_date}:{hashlib.sha256(body).hexdigest()}".encode()
    ).hexdigest()
    rows: list[dict] = []
    for element in root.iter():
        if not isinstance(element.tag, str) or element.get("contextRef") is None:
            continue
        qname = etree.QName(element)
        if qname.namespace != MET_NS:
            continue
        try:
            numeric_value = float((element.text or "").strip())
        except ValueError:
            continue
        mcy, ref = contexts.get(element.get("contextRef"), (None, None))
        rows.append(
            {
                "statement_key": statement_key,
                "business_id": business_id,
                "financial_date": financial_date,
                "concept_qname": f"fi_met:{qname.localname}",
                "mcy_member_code": mcy,
                "ref_member_code": ref,
                "numeric_value": numeric_value,
            }
        )
    return rows


def xbrl_facts_from_listing(run_dir: Path, listing: dict) -> tuple[pl.DataFrame, list[dict]]:
    rows: list[dict] = []
    failures: list[dict] = []
    for document in listing.get("documents", []):
        try:
            body = (run_dir / document["path"]).read_bytes()
            rows.extend(
                _parse_xbrl_facts(
                    body,
                    business_id=document["business_id"],
                    financial_date=document["financial_date"],
                )
            )
        except Exception as exc:
            failures.append(
                {
                    "business_id": document.get("business_id"),
                    "financial_date": document.get("financial_date"),
                    "path": document.get("path"),
                    "error": str(exc),
                }
            )

    schema = {
        "statement_key": pl.Utf8,
        "business_id": pl.Utf8,
        "financial_date": pl.Utf8,
        "concept_qname": pl.Utf8,
        "mcy_member_code": pl.Utf8,
        "ref_member_code": pl.Utf8,
        "numeric_value": pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema), failures
```

- [ ] **Step 4: Run XBRL tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_structured_xbrl.py tests/test_structured_ytj.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add processor/finland/structured.py processor/tests/test_structured_xbrl.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "Add Finland XBRL fact parser"
```

## Task 6: Canonical Builders

**Files:**
- Create: `companycollect/processor/finland/canonical.py`
- Create: `companycollect/processor/tests/test_canonical.py`

- [ ] **Step 1: Write canonical tests**

Create `companycollect/processor/tests/test_canonical.py`:

```python
import datetime as dt

import polars as pl

from finland.canonical import build_canonical_tables, company_uid, registration_uid


def _structured_frames() -> dict[str, pl.DataFrame]:
    return {
        "fi_prhytj_statuses": pl.DataFrame(
            {
                "business_id": ["1234567-8"],
                "trade_register_status": ["2"],
                "registration_date": ["2020-01-01"],
                "end_date": [""],
                "lifecycle_status": ["active"],
                "is_active": [True],
            }
        ),
        "fi_prhytj_names": pl.DataFrame(
            {
                "business_id": ["1234567-8"],
                "name": ["Example Oy"],
                "name_type_code": ["1"],
                "is_current": [True],
                "is_primary": [True],
            }
        ),
        "fi_prhytj_websites": pl.DataFrame(
            {
                "business_id": ["1234567-8"],
                "url": ["example.fi"],
                "normalized_url": ["https://example.fi"],
                "host": ["example.fi"],
                "is_current": [True],
                "is_primary": [True],
            }
        ),
        "fi_prhytj_addresses": pl.DataFrame(
            {
                "business_id": ["1234567-8"],
                "address_type_code": ["1"],
                "street": ["Testikatu 1"],
                "post_code": ["00100"],
                "city": ["Helsinki"],
                "municipality_code": ["091"],
                "country": ["FI"],
            }
        ),
        "fi_prhytj_business_lines": pl.DataFrame(
            {
                "business_id": ["1234567-8"],
                "business_line_type": ["62010"],
                "business_line_code_set": ["TOL2008"],
            }
        ),
        "fi_prh_xbrl_facts": pl.DataFrame(
            {
                "statement_key": ["statement-1"],
                "business_id": ["1234567-8"],
                "financial_date": ["2024-12-31"],
                "concept_qname": ["fi_met:md103"],
                "mcy_member_code": ["fi_MC:x673"],
                "ref_member_code": [None],
                "numeric_value": [12345.67],
            }
        ),
    }


def test_uid_helpers_match_contract() -> None:
    assert registration_uid("1234567-8") == "FI:1234567-8"
    assert company_uid("1234567-8").startswith("c:")
    assert len(company_uid("1234567-8")) == 42


def test_build_canonical_tables() -> None:
    now = dt.datetime(2026, 6, 15, 12, 0)
    tables = build_canonical_tables(_structured_frames(), run_id="run-1", now=now)

    assert tables["registrations"].row(0, named=True)["legal_name"] == "Example Oy"
    assert tables["company"].row(0, named=True)["primary_name"] == "Example Oy"
    assert tables["financials"].row(0, named=True)["metric_code"] == "revenue"
    assert tables["company_websites"].row(0, named=True)["host"] == "example.fi"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_canonical.py -v
```

Expected: FAIL because `finland.canonical` does not exist.

- [ ] **Step 3: Implement canonical builders**

Create `companycollect/processor/finland/canonical.py`:

```python
from __future__ import annotations

import datetime as dt
import hashlib

import polars as pl

from finland.config import COUNTRY, DURATION_METRICS, METRIC_MAP
from finland import schemas

RESOLUTION_VERSION = "finland-v1"
FINANCIAL_MAPPING_VERSION = "finland-fin-v1"


def registration_uid(business_id: str) -> str:
    return f"{COUNTRY}:{business_id}"


def company_uid(business_id: str) -> str:
    return "c:" + hashlib.sha1(f"{COUNTRY}:{business_id}".encode()).hexdigest()


def _date(value) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _first(df: pl.DataFrame, business_id: str, column: str):
    if df.is_empty():
        return None
    subset = df.filter(pl.col("business_id") == business_id)
    if subset.is_empty():
        return None
    return subset[column].to_list()[0]


def _first_row(df: pl.DataFrame, business_id: str) -> dict:
    if df.is_empty():
        return {}
    subset = df.filter(pl.col("business_id") == business_id)
    if subset.is_empty():
        return {}
    return subset.row(0, named=True)


def build_registrations(structured: dict[str, pl.DataFrame], *, run_id: str, now: dt.datetime) -> pl.DataFrame:
    statuses = structured["fi_prhytj_statuses"]
    names = structured["fi_prhytj_names"]
    websites = structured["fi_prhytj_websites"]
    addresses = structured["fi_prhytj_addresses"]
    lines = structured["fi_prhytj_business_lines"]

    rows: list[dict] = []
    for status in statuses.iter_rows(named=True):
        business_id = status["business_id"]
        primary_names = names.filter((pl.col("business_id") == business_id) & (pl.col("name_type_code") == "1"))
        legal_name = primary_names["name"].to_list()[0] if not primary_names.is_empty() else _first(names, business_id, "name")
        address = _first_row(addresses, business_id)
        line = _first_row(lines, business_id)
        end_date = status.get("end_date")
        rows.append(
            {
                "registration_uid": registration_uid(business_id),
                "company_uid": company_uid(business_id),
                "country": COUNTRY,
                "registration_number": business_id,
                "registry_source": "finland_prhytj",
                "is_primary": 1,
                "entity_role": "domestic",
                "legal_name": legal_name,
                "legal_form_code": None,
                "lifecycle_status": status.get("lifecycle_status") or "unknown",
                "is_active": 1 if status.get("is_active") else 0,
                "incorporation_date": _date(status.get("registration_date")),
                "dissolution_date": _date(end_date),
                "addr_street": address.get("street"),
                "addr_post_code": address.get("post_code"),
                "addr_city": address.get("city"),
                "addr_municipality_code": address.get("municipality_code"),
                "addr_country": address.get("country") or COUNTRY,
                "activity_code": line.get("business_line_type"),
                "activity_scheme": line.get("business_line_code_set"),
                "vat_number": None,
                "eu_id": None,
                "lei": None,
                "primary_website": _first(websites, business_id, "normalized_url"),
                "source_run_id": run_id,
                "ingested_at": now,
                "updated_at": now,
            }
        )
    return pl.DataFrame(rows, schema=schemas.REGISTRATIONS)


def build_company(registrations: pl.DataFrame, *, now: dt.datetime) -> pl.DataFrame:
    rows: list[dict] = []
    for uid in registrations["company_uid"].unique().to_list():
        group = registrations.filter(pl.col("company_uid") == uid)
        first = group.row(0, named=True)
        rows.append(
            {
                "company_uid": uid,
                "uid_scheme": "surrogate",
                "lei": None,
                "primary_name": first["legal_name"],
                "status": "active" if group["is_active"].max() == 1 else "inactive",
                "legal_form_code": first["legal_form_code"],
                "home_country": COUNTRY,
                "incorporation_date": first["incorporation_date"],
                "dissolution_date": first["dissolution_date"],
                "registration_count": group.height,
                "operating_countries": sorted(group["country"].unique().to_list()),
                "primary_website": first["primary_website"],
                "sources": ["finland_prhytj"],
                "resolution_version": RESOLUTION_VERSION,
                "first_seen_at": now,
                "updated_at": now,
            }
        )
    return pl.DataFrame(rows, schema=schemas.COMPANY)


def build_financials(facts: pl.DataFrame, *, run_id: str, now: dt.datetime) -> pl.DataFrame:
    rows: list[dict] = []
    for fact in facts.iter_rows(named=True):
        metric = METRIC_MAP.get((fact.get("concept_qname"), fact.get("mcy_member_code")))
        if metric is None:
            continue
        business_id = fact["business_id"]
        rows.append(
            {
                "company_uid": company_uid(business_id),
                "registration_uid": registration_uid(business_id),
                "country": COUNTRY,
                "statement_id": fact["statement_key"],
                "period_start": None,
                "period_end": _date(fact["financial_date"]),
                "period_type": "duration" if metric in DURATION_METRICS else "instant",
                "period_reference": "prior" if fact.get("ref_member_code") else "current",
                "basis": "individual",
                "currency": "EUR",
                "metric_code": metric,
                "value": float(fact["numeric_value"]),
                "source_metric_id": f"{fact.get('concept_qname')}/{fact.get('mcy_member_code')}",
                "registry_source": "finland_prh_xbrl",
                "mapping_version": FINANCIAL_MAPPING_VERSION,
                "source_run_id": run_id,
                "ingested_at": now,
                "updated_at": now,
            }
        )
    return pl.DataFrame(rows, schema=schemas.FINANCIALS)


def build_company_websites(websites: pl.DataFrame, *, now: dt.datetime) -> pl.DataFrame:
    rows: list[dict] = []
    for website in websites.iter_rows(named=True):
        business_id = website["business_id"]
        normalized_url = website.get("normalized_url") or website.get("url") or ""
        if not normalized_url:
            continue
        reg_uid = registration_uid(business_id)
        rows.append(
            {
                "website_uid": hashlib.sha1(f"registration:{reg_uid}:{normalized_url}".encode()).hexdigest(),
                "company_uid": company_uid(business_id),
                "registration_uid": reg_uid,
                "country": COUNTRY,
                "scope": "registration",
                "url": website.get("url") or normalized_url,
                "normalized_url": normalized_url,
                "host": website.get("host") or "",
                "is_primary": 1 if website.get("is_primary") else 0,
                "source_kind": "registry",
                "discovery_method": "registry_field",
                "registry_source": "finland_prhytj",
                "confidence": 1.0,
                "is_live": 0,
                "first_seen_at": now,
                "last_seen_at": now,
                "updated_at": now,
            }
        )
    return pl.DataFrame(rows, schema=schemas.COMPANY_WEBSITES)


def build_canonical_tables(structured: dict[str, pl.DataFrame], *, run_id: str, now: dt.datetime) -> dict[str, pl.DataFrame]:
    registrations = build_registrations(structured, run_id=run_id, now=now)
    company = build_company(registrations, now=now)
    financials = build_financials(structured["fi_prh_xbrl_facts"], run_id=run_id, now=now)
    websites = build_company_websites(structured["fi_prhytj_websites"], now=now)
    return {
        "registrations": registrations,
        "company": company,
        "financials": financials,
        "company_websites": websites,
    }
```

- [ ] **Step 4: Run canonical tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_canonical.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add processor/finland/canonical.py processor/tests/test_canonical.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "Add Finland canonical builders"
```

## Task 7: Local Parquet And Manifest IO

**Files:**
- Create: `companycollect/processor/finland/io.py`
- Create: `companycollect/processor/tests/test_io.py`

- [ ] **Step 1: Write IO tests**

Create `companycollect/processor/tests/test_io.py`:

```python
import json
from pathlib import Path

import polars as pl

from finland.io import write_manifest, write_tables


def test_write_tables_writes_parquet(tmp_path: Path) -> None:
    paths = write_tables(tmp_path, "structured", {"sample": pl.DataFrame({"value": [1]})})

    assert paths["sample"] == tmp_path / "structured" / "sample.parquet"
    assert pl.read_parquet(paths["sample"])["value"].to_list() == [1]


def test_write_manifest_writes_json(tmp_path: Path) -> None:
    path = write_manifest(
        tmp_path,
        run_id="run-1",
        parameters={"max_companies": 5},
        raw_counts={"ytj_companies": 5, "xbrl_documents": 1},
        structured_shapes={"sample": (1, 1)},
        canonical_shapes={"company": (1, 2)},
        output_paths={"company": tmp_path / "canonical" / "company.parquet"},
        parse_failures=[],
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "run-1"
    assert payload["parameters"]["max_companies"] == 5
    assert payload["canonical_shapes"]["company"] == [1, 2]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_io.py -v
```

Expected: FAIL because `finland.io` does not exist.

- [ ] **Step 3: Implement IO module**

Create `companycollect/processor/finland/io.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

import polars as pl


def write_tables(run_dir: Path, layer: str, tables: dict[str, pl.DataFrame]) -> dict[str, Path]:
    layer_dir = run_dir / layer
    layer_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for name, frame in tables.items():
        path = layer_dir / f"{name}.parquet"
        frame.write_parquet(path)
        paths[name] = path
    return paths


def _shape_map(shapes: dict[str, tuple[int, int]]) -> dict[str, list[int]]:
    return {name: [rows, columns] for name, (rows, columns) in shapes.items()}


def _path_map(paths: dict[str, Path]) -> dict[str, str]:
    return {name: str(path) for name, path in paths.items()}


def write_manifest(
    run_dir: Path,
    *,
    run_id: str,
    parameters: dict,
    raw_counts: dict[str, int],
    structured_shapes: dict[str, tuple[int, int]],
    canonical_shapes: dict[str, tuple[int, int]],
    output_paths: dict[str, Path],
    parse_failures: list[dict],
) -> Path:
    manifest = {
        "run_id": run_id,
        "parameters": parameters,
        "raw_counts": raw_counts,
        "structured_shapes": _shape_map(structured_shapes),
        "canonical_shapes": _shape_map(canonical_shapes),
        "output_paths": _path_map(output_paths),
        "parse_failures": parse_failures,
    }
    path = run_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return path
```

- [ ] **Step 4: Run IO tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_io.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add processor/finland/io.py processor/tests/test_io.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "Add Finland local output writers"
```

## Task 8: Prefect Flow Assembly

**Files:**
- Create: `companycollect/processor/finland_flow.py`
- Create: `companycollect/processor/tests/test_finland_flow.py`

- [ ] **Step 1: Write flow helper tests**

Create `companycollect/processor/tests/test_finland_flow.py`:

```python
import re

from finland_flow import build_run_id, summary_markdown


def test_build_run_id_uses_finland_prefix() -> None:
    run_id = build_run_id()

    assert re.match(r"finland-\d{8}T\d{6}Z", run_id)


def test_summary_markdown_contains_counts_and_path() -> None:
    markdown = summary_markdown(
        run_id="finland-20260615T120000Z",
        parameters={"max_companies": 5},
        raw_counts={"ytj_companies": 5, "xbrl_documents": 1},
        canonical_shapes={"company": (5, 16)},
        manifest_path="/tmp/manifest.json",
    )

    assert "finland-20260615T120000Z" in markdown
    assert "ytj_companies" in markdown
    assert "/tmp/manifest.json" in markdown
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_finland_flow.py -v
```

Expected: FAIL because `finland_flow.py` does not exist.

- [ ] **Step 3: Implement Prefect flow**

Create `companycollect/processor/finland_flow.py`:

```python
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import polars as pl
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from finland import canonical, download, io, schemas, structured
from finland.config import DEFAULT_MAX_COMPANIES, DEFAULT_OUTPUT_ROOT, DEFAULT_XBRL_END, DEFAULT_XBRL_START


def build_run_id(now: dt.datetime | None = None) -> str:
    value = now or dt.datetime.now(dt.UTC)
    return "finland-" + value.strftime("%Y%m%dT%H%M%SZ")


def _shapes(tables: dict[str, pl.DataFrame]) -> dict[str, tuple[int, int]]:
    return {name: frame.shape for name, frame in tables.items()}


def summary_markdown(
    *,
    run_id: str,
    parameters: dict,
    raw_counts: dict[str, int],
    canonical_shapes: dict[str, tuple[int, int]],
    manifest_path: str,
) -> str:
    rows = "\n".join(f"- `{name}`: `{shape[0]}` rows, `{shape[1]}` columns" for name, shape in canonical_shapes.items())
    return (
        f"# Finland local Prefect run\n\n"
        f"- Run ID: `{run_id}`\n"
        f"- Parameters: `{json.dumps(parameters, sort_keys=True)}`\n"
        f"- Raw counts: `{json.dumps(raw_counts, sort_keys=True)}`\n"
        f"- Manifest: `{manifest_path}`\n\n"
        f"## Canonical Tables\n\n{rows}\n"
    )


@task(retries=3, retry_delay_seconds=[1, 2, 4])
def download_raw_task(run_dir: str, max_companies: int, xbrl_start: str, xbrl_end: str) -> dict:
    logger = get_run_logger()
    root = Path(run_dir)
    ytj = download.download_ytj_companies(root, max_companies=max_companies)
    xbrl = download.download_xbrl_window(root, registered_start=xbrl_start, registered_end=xbrl_end)
    logger.info("Downloaded %s YTJ companies and %s XBRL documents", ytj.count, xbrl.document_count)
    return {
        "ytj_path": str(ytj.path),
        "ytj_count": ytj.count,
        "xbrl_listing_path": str(xbrl.listing_path),
        "xbrl_document_count": xbrl.document_count,
    }


@task
def build_structured_task(run_dir: str, raw: dict) -> tuple[dict[str, pl.DataFrame], list[dict]]:
    root = Path(run_dir)
    ytj_bytes = Path(raw["ytj_path"]).read_bytes()
    tables = structured.ytj_structured_from_ndjson(ytj_bytes)
    listing = json.loads(Path(raw["xbrl_listing_path"]).read_text(encoding="utf-8"))
    facts, failures = structured.xbrl_facts_from_listing(root, listing)
    tables["fi_prh_xbrl_facts"] = facts
    return tables, failures


@task
def write_structured_task(run_dir: str, tables: dict[str, pl.DataFrame]) -> dict[str, str]:
    paths = io.write_tables(Path(run_dir), "structured", tables)
    return {name: str(path) for name, path in paths.items()}


@task
def build_canonical_task(tables: dict[str, pl.DataFrame], run_id: str) -> dict[str, pl.DataFrame]:
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    canonical_tables = canonical.build_canonical_tables(tables, run_id=run_id, now=now)
    for name, frame in canonical_tables.items():
        schemas.validate_table(frame, schemas.CANONICAL_SCHEMAS[name], unique_key=schemas.UNIQUE_KEYS[name])
    return canonical_tables


@task
def write_outputs_task(
    run_dir: str,
    run_id: str,
    parameters: dict,
    raw_counts: dict[str, int],
    structured_tables: dict[str, pl.DataFrame],
    canonical_tables: dict[str, pl.DataFrame],
    parse_failures: list[dict],
) -> dict:
    root = Path(run_dir)
    canonical_paths = io.write_tables(root, "canonical", canonical_tables)
    manifest_path = io.write_manifest(
        root,
        run_id=run_id,
        parameters=parameters,
        raw_counts=raw_counts,
        structured_shapes=_shapes(structured_tables),
        canonical_shapes=_shapes(canonical_tables),
        output_paths=canonical_paths,
        parse_failures=parse_failures,
    )
    return {
        "manifest_path": str(manifest_path),
        "canonical_shapes": _shapes(canonical_tables),
        "canonical_paths": {name: str(path) for name, path in canonical_paths.items()},
    }


@task
def create_summary_artifact_task(run_id: str, parameters: dict, raw_counts: dict[str, int], output: dict) -> None:
    create_markdown_artifact(
        key=f"finland-local-{run_id}",
        markdown=summary_markdown(
            run_id=run_id,
            parameters=parameters,
            raw_counts=raw_counts,
            canonical_shapes=output["canonical_shapes"],
            manifest_path=output["manifest_path"],
        ),
        description="Finland local Prefect run summary",
    )


@task
def fail_on_parse_failures_task(parse_failures: list[dict]) -> None:
    if parse_failures:
        raise ValueError(f"XBRL parse failures recorded in manifest: {parse_failures}")


@flow(name="finland-local-workflow", flow_run_name="finland-local-{run_id}", log_prints=True)
def finland_local_flow(
    run_id: str | None = None,
    max_companies: int = DEFAULT_MAX_COMPANIES,
    xbrl_start: str = DEFAULT_XBRL_START,
    xbrl_end: str = DEFAULT_XBRL_END,
    output_root: str = DEFAULT_OUTPUT_ROOT,
) -> dict:
    resolved_run_id = run_id or build_run_id()
    run_dir = str(Path(output_root) / resolved_run_id)
    parameters = {
        "run_id": resolved_run_id,
        "max_companies": max_companies,
        "xbrl_start": xbrl_start,
        "xbrl_end": xbrl_end,
        "output_root": output_root,
    }
    raw = download_raw_task(run_dir, max_companies, xbrl_start, xbrl_end)
    structured_tables, parse_failures = build_structured_task(run_dir, raw)
    write_structured_task(run_dir, structured_tables)
    canonical_tables = build_canonical_task(structured_tables, resolved_run_id)
    raw_counts = {"ytj_companies": raw["ytj_count"], "xbrl_documents": raw["xbrl_document_count"]}
    output = write_outputs_task(
        run_dir,
        resolved_run_id,
        parameters,
        raw_counts,
        structured_tables,
        canonical_tables,
        parse_failures,
    )
    create_summary_artifact_task(resolved_run_id, parameters, raw_counts, output)
    fail_on_parse_failures_task(parse_failures)
    return output


if __name__ == "__main__":
    finland_local_flow()
```

- [ ] **Step 4: Run flow helper tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests/test_finland_flow.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add processor/finland_flow.py processor/tests/test_finland_flow.py
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "Add Finland Prefect flow"
```

## Task 9: End-To-End Local Verification

**Files:**
- Modify: `companycollect/processor/README.md`

- [ ] **Step 1: Add usage documentation**

Replace `companycollect/processor/README.md` if it is empty, or append this content if it already has useful text:

```markdown
# Processor

Local Prefect experiments for company collection pipelines.

## Finland local workflow

Run a small bounded Finland PRH sample:

```bash
uv run python finland_flow.py
```

Run with explicit parameters:

```bash
uv run python - <<'PY'
from finland_flow import finland_local_flow

finland_local_flow(
    run_id="finland-dev",
    max_companies=5,
    xbrl_start="2025-01-01",
    xbrl_end="2025-01-01",
    output_root="output/finland",
)
PY
```

Outputs are written under:

```text
output/finland/<run_id>/
  raw/
  structured/
  canonical/
  manifest.json
```

`manifest.json` is the portable dataset record. Prefect stores run state, parameters,
logs, and a Markdown summary artifact for operational inspection.
```

- [ ] **Step 2: Run full test suite**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests -v
```

Expected: PASS.

- [ ] **Step 3: Run tiny live smoke**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run python - <<'PY'
from finland_flow import finland_local_flow

result = finland_local_flow(
    run_id="finland-smoke",
    max_companies=5,
    xbrl_start="2025-01-01",
    xbrl_end="2025-01-01",
    output_root="output/finland",
)
print(result["manifest_path"])
PY
```

Expected: command exits 0 and prints `output/finland/finland-smoke/manifest.json`.

- [ ] **Step 4: Verify generated files**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
test -f output/finland/finland-smoke/manifest.json
test -f output/finland/finland-smoke/structured/fi_prhytj_statuses.parquet
test -f output/finland/finland-smoke/structured/fi_prh_xbrl_facts.parquet
test -f output/finland/finland-smoke/canonical/registrations.parquet
test -f output/finland/finland-smoke/canonical/company.parquet
test -f output/finland/finland-smoke/canonical/financials.parquet
test -f output/finland/finland-smoke/canonical/company_websites.parquet
```

Expected: all commands exit 0.

- [ ] **Step 5: Inspect manifest and row counts**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run python - <<'PY'
import json
from pathlib import Path

import polars as pl

manifest = json.loads(Path("output/finland/finland-smoke/manifest.json").read_text())
print(manifest["run_id"])
for name in ["registrations", "company", "financials", "company_websites"]:
    frame = pl.read_parquet(f"output/finland/finland-smoke/canonical/{name}.parquet")
    print(name, frame.shape)
PY
```

Expected: prints `finland-smoke` and one shape line for each canonical table.

- [ ] **Step 6: Commit**

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect add processor/README.md
git -C /Users/graovic/pulsarpoint/ppoint/companycollect commit -m "Document Finland Prefect local workflow"
```

## Task 10: Final Review

**Files:**
- Review: all files changed under `companycollect/processor`

- [ ] **Step 1: Show changed files**

Run:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect status --short
```

Expected: only intentional untracked generated output files may remain under `processor/output/`. Do not commit generated output.

- [ ] **Step 2: Run final tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run pytest tests -v
```

Expected: PASS.

- [ ] **Step 3: Check imports**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/processor
uv run python - <<'PY'
from finland_flow import finland_local_flow
from finland import canonical, config, download, io, schemas, structured

print(finland_local_flow.name)
print(config.PRH_YTJ_COMPANIES_URL)
print(bool(canonical.company_uid("1234567-8")))
print(bool(download.make_session))
print(bool(io.write_manifest))
print(bool(schemas.CANONICAL_SCHEMAS))
print(bool(structured.ytj_structured_from_ndjson))
PY
```

Expected: prints the flow name, PRH URL, and six `True` values.

- [ ] **Step 4: Remove generated smoke output from git tracking**

Run:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect status --short -- processor/output
```

Expected: if generated output is listed as untracked, leave it uncommitted. If a generated file was staged by mistake, unstage only generated output:

```bash
git -C /Users/graovic/pulsarpoint/ppoint/companycollect restore --staged processor/output
```

- [ ] **Step 5: Report completion**

Summarize:

- files created;
- test command results;
- smoke run result;
- output manifest path;
- any generated output intentionally left uncommitted.

## Self-Review

Spec coverage:

- Self-contained local Prefect flow: Task 8.
- Copied processor-local Finland logic: Tasks 2 through 8.
- Bounded YTJ and XBRL downloads: Task 3.
- Structured Parquet: Tasks 4, 5, and 7.
- Canonical Parquet: Tasks 6 and 7.
- Manifest without `latest.json`: Task 7.
- Prefect Markdown artifact: Task 8.
- Validation and smoke tests: Tasks 2, 6, 8, 9, and 10.
- No production persistence, schedules, workers, or dbt project: all tasks stay under `processor` and local output.

Placeholder scan:

- The plan contains no open-ended implementation markers.
- Each code-bearing task includes concrete file content or concrete function content.
- Commands include expected outcomes.

Type consistency:

- `run_id`, `max_companies`, `xbrl_start`, `xbrl_end`, and `output_root` are consistently named in docs, manifest, and flow.
- Structured table names match canonical builder inputs.
- Canonical table names match schema and output filenames.
