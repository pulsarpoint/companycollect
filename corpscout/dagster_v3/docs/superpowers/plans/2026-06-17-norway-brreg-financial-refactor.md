# Norway Brreg Financial Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor Norway BRREG financial data ingestion so fetch attempts are durable, failures are observable, normalization is pure, and exchange-rate conversion is batched.

**Architecture:** Split the current `norway_brreg_financial_statements_duckdb` behavior into three durable stages: candidate selection, raw financial fetches with per-org status rows, and normalized financial statements from successful fetch payloads. Use a plain Dagster asset for the single BRREG financial fetch table and run one dlt resource directly inside that asset; avoid a fake one-resource `@dlt.source` wrapper. Use DuckDB SQL for candidate/filtering transforms and keep ClickHouse export downstream of normalized DuckDB tables.

**Tech Stack:** Dagster assets, dagster-dlt for existing multi-resource/listing assets, dlt DuckDB destination, DuckDB SQL, BRREG REST API, `dlt.sources.helpers.requests.Client`, existing `dagster_v3.exchange_rates.ExchangeRateClient`, pytest.

---

## File Structure

- Create `src/dagster_v3/defs/norway_brreg/financial_fetches.py`
  - Owns financial fetch statuses, fetch table schema, HTTP client construction, fetch row construction, candidate selection, and the row iterator for raw BRREG financial endpoint fetches.
- Create `src/dagster_v3/defs/norway_brreg/financial_normalize.py`
  - Owns converting successful fetch payloads into normalized financial statement rows and resolving exchange rates in batches.
- Modify `src/dagster_v3/defs/norway_brreg/assets.py`
  - Keep Dagster asset wiring, translators, and final asset metadata here.
  - Add a plain Dagster asset for `norway_brreg_financial_fetches_duckdb` that runs one dlt resource directly.
  - Remove raw financial HTTP behavior from `_financial_statements_resource`.
- Modify `src/dagster_v3/defs/norway_brreg/tables.py`
  - Add ClickHouse/DuckDB column constants for `financial_fetches` only if the final ClickHouse export should also expose fetch status. For this plan, fetch statuses stay in DuckDB staging only.
- Modify `src/dagster_v3/exchange_rates/client.py`
  - Add a true batch API for multiple `(currency, rate_date)` requests.
- Modify `tests/test_norway_brreg_assets.py`
  - Add asset graph tests and integration tests for fetch and normalization stages.
- Create `tests/test_norway_brreg_financial_fetches.py`
  - Focused unit tests for fetch status modeling and retry client configuration.
- Create `tests/test_norway_brreg_financial_normalize.py`
  - Focused unit tests for normalization and batched FX usage.

---

## Task 1: Add Financial Fetch Status Model And DuckDB Schema

**Files:**
- Create: `src/dagster_v3/defs/norway_brreg/financial_fetches.py`
- Create: `tests/test_norway_brreg_financial_fetches.py`

- [ ] **Step 1: Write failing schema/status tests**

Add `tests/test_norway_brreg_financial_fetches.py`:

```python
import json

from dagster_v3.defs.norway_brreg import financial_fetches


def test_financial_fetch_status_values_are_explicit() -> None:
    assert financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS == "success"
    assert financial_fetches.FINANCIAL_FETCH_STATUS_NOT_FOUND == "not_found"
    assert financial_fetches.FINANCIAL_FETCH_STATUS_SERVER_ERROR == "server_error"
    assert financial_fetches.FINANCIAL_FETCH_STATUS_NETWORK_ERROR == "network_error"
    assert financial_fetches.FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD == "invalid_payload"


def test_financial_fetches_schema_matches_emitted_success_row() -> None:
    row = financial_fetches.financial_fetch_success_row(
        org={
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "www.equinor.com",
            "last_submitted_accounts_year": "2024",
        },
        source_url="https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
        payload=[{"id": 1}],
        source_run_id="run-1",
        source_line_number=1,
        status_code=200,
        fetched_at="2026-06-17T00:00:00.000Z",
        attempt_count=1,
    )

    assert set(financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS) == set(row)
    assert row["fetch_status"] == "success"
    assert row["http_status"] == 200
    assert row["source_record_id"] == "923609016"
    assert json.loads(row["raw_response"]) == [{"id": 1}]
    assert len(row["source_payload_hash"]) == 64


def test_financial_fetches_schema_matches_emitted_failure_row() -> None:
    row = financial_fetches.financial_fetch_failure_row(
        org={
            "org_number": "814115232",
            "legal_name": "BROKEN AS",
            "website": "https://example.test",
            "last_submitted_accounts_year": "2024",
        },
        source_url="https://data.brreg.no/regnskapsregisteret/regnskap/814115232",
        source_run_id="run-1",
        source_line_number=2,
        status_code=500,
        fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_SERVER_ERROR,
        error_type="HTTPStatusError",
        error_message="HTTP 500",
        fetched_at="2026-06-17T00:00:00.000Z",
        attempt_count=3,
        raw_response="",
    )

    assert set(financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS) == set(row)
    assert row["fetch_status"] == "server_error"
    assert row["http_status"] == 500
    assert row["raw_response"] == ""
    assert row["source_payload_hash"] == "0" * 64
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_financial_fetches.py -q
```

Expected: fail with `ModuleNotFoundError` or missing `financial_fetches` attributes.

- [ ] **Step 3: Implement schema and row builders**

Create `src/dagster_v3/defs/norway_brreg/financial_fetches.py`:

```python
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

FINANCIAL_FETCHES_TABLE = "financial_fetches"

FINANCIAL_FETCH_STATUS_SUCCESS = "success"
FINANCIAL_FETCH_STATUS_NOT_FOUND = "not_found"
FINANCIAL_FETCH_STATUS_SERVER_ERROR = "server_error"
FINANCIAL_FETCH_STATUS_NETWORK_ERROR = "network_error"
FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD = "invalid_payload"

BRREG_FINANCIAL_FETCHES_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
    "org_number": {"data_type": "text", "nullable": False},
    "legal_name": {"data_type": "text"},
    "website": {"data_type": "text"},
    "last_submitted_accounts_year": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "fetch_status": {"data_type": "text"},
    "http_status": {"data_type": "bigint"},
    "error_type": {"data_type": "text"},
    "error_message": {"data_type": "text"},
    "attempt_count": {"data_type": "bigint"},
    "fetched_at": {"data_type": "timestamp"},
    "raw_response": {"data_type": "text"},
}


def financial_fetch_success_row(
    *,
    org: Mapping[str, Any],
    source_url: str,
    payload: list[dict[str, Any]],
    source_run_id: str,
    source_line_number: int,
    status_code: int,
    fetched_at: str,
    attempt_count: int,
) -> dict[str, Any]:
    raw_response = _json_dumps(payload)
    return _base_financial_fetch_row(
        org=org,
        source_url=source_url,
        source_run_id=source_run_id,
        source_line_number=source_line_number,
        source_payload_hash=hashlib.sha256(raw_response.encode("utf-8")).hexdigest(),
        fetch_status=FINANCIAL_FETCH_STATUS_SUCCESS,
        http_status=status_code,
        error_type="",
        error_message="",
        attempt_count=attempt_count,
        fetched_at=fetched_at,
        raw_response=raw_response,
    )


def financial_fetch_failure_row(
    *,
    org: Mapping[str, Any],
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
    return _base_financial_fetch_row(
        org=org,
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


def _base_financial_fetch_row(
    *,
    org: Mapping[str, Any],
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
        "source_record_id": _string(org.get("org_number")),
        "source_payload_hash": source_payload_hash,
        "org_number": _string(org.get("org_number")),
        "legal_name": _string(org.get("legal_name")),
        "website": _string(org.get("website")),
        "last_submitted_accounts_year": _string(org.get("last_submitted_accounts_year")),
        "source_url": source_url,
        "fetch_status": fetch_status,
        "http_status": http_status,
        "error_type": error_type,
        "error_message": error_message,
        "attempt_count": attempt_count,
        "fetched_at": fetched_at,
        "raw_response": raw_response,
    }


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _string(value: Any) -> str:
    return "" if value is None else str(value)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_norway_brreg_financial_fetches.py -q
```

Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/financial_fetches.py tests/test_norway_brreg_financial_fetches.py
git commit -m "feat: add norway brreg financial fetch schema"
```

---

## Task 2: Implement Durable Financial Fetch Row Iterator

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/financial_fetches.py`
- Modify: `tests/test_norway_brreg_financial_fetches.py`

- [ ] **Step 1: Write failing tests for fetch outcomes**

Append to `tests/test_norway_brreg_financial_fetches.py`:

```python
from pathlib import Path
from typing import Any

import duckdb


class FakeResponse:
    def __init__(self, *, status_code: int, payload: Any = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> Any:
        return self._payload


class FakeDltRequestsClient:
    def __init__(self, responses: dict[str, FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, int]] = []

    def get(self, url: str, *, timeout: int) -> FakeResponse:
        self.calls.append((url, timeout))
        if url == "https://data.brreg.no/regnskapsregisteret/regnskap/network":
            raise OSError("network down")
        return self.responses[url]


def _seed_entities(database_path: Path) -> None:
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema if not exists norway_brreg")
        connection.execute(
            """
            create or replace table norway_brreg.entities as
            select *
            from (
                values
                    ('923609016', 'EQUINOR ASA', 'www.equinor.com', '2024', true),
                    ('811685852', 'MISSING AS', 'www.missing.test', '2024', true),
                    ('814115232', 'SERVER AS', 'www.server.test', '2024', true),
                    ('network', 'NETWORK AS', 'www.network.test', '2024', true)
            ) as t(org_number, legal_name, website, last_submitted_accounts_year, is_active)
            """
        )


def test_financial_fetches_iterator_emits_success_and_failure_rows(tmp_path: Path) -> None:
    database_path = tmp_path / "norway.duckdb"
    _seed_entities(database_path)
    client = FakeDltRequestsClient(
        {
            "https://data.brreg.no/regnskapsregisteret/regnskap/923609016": FakeResponse(
                status_code=200,
                payload=[{"id": 1}],
                text='[{"id":1}]',
            ),
            "https://data.brreg.no/regnskapsregisteret/regnskap/811685852": FakeResponse(
                status_code=404,
                payload={"message": "not found"},
                text='{"message":"not found"}',
            ),
            "https://data.brreg.no/regnskapsregisteret/regnskap/814115232": FakeResponse(
                status_code=500,
                payload={"message": "server error"},
                text='{"message":"server error"}',
            ),
        }
    )

    rows = list(
        financial_fetches.iter_brreg_financial_statement_fetch_rows(
            database_path=database_path,
            base_url="https://data.brreg.no/regnskapsregisteret/regnskap",
            source_run_id="run-1",
            timeout_seconds=120,
            user_agent="test-agent",
            fetched_at="2026-06-17T00:00:00.000Z",
            client=client,
        )
    )

    assert [(row["org_number"], row["fetch_status"], row["http_status"]) for row in rows] == [
        ("811685852", "not_found", 404),
        ("814115232", "server_error", 500),
        ("923609016", "success", 200),
        ("network", "network_error", None),
    ]
    assert len(client.calls) == 4


def test_financial_fetches_table_schema_is_explicit() -> None:
    assert financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS["org_number"] == {
        "data_type": "text",
        "nullable": False,
    }
    assert financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS["raw_response"] == {
        "data_type": "text"
    }
    assert financial_fetches.FINANCIAL_FETCHES_TABLE == "financial_fetches"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_financial_fetches.py -q
```

Expected: fail with missing `iter_brreg_financial_statement_fetch_rows`.

- [ ] **Step 3: Implement BRREG-specific fetch iterator**

Add to `src/dagster_v3/defs/norway_brreg/financial_fetches.py`:

```python
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from dlt.sources.helpers.requests import Client as DltRequestsClient

BRREG_REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"


def iter_brreg_financial_statement_fetch_rows(
    *,
    database_path: str | Path,
    source_run_id: str,
    base_url: str = BRREG_REGNSKAP_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    fetched_at: str | None = None,
    client: Any | None = None,
) -> Iterator[dict[str, Any]]:
    http_client = client or _default_financial_fetch_http_client(
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
    )
    fetch_timestamp = fetched_at or _utc_now_iso()
    for source_line_number, org in enumerate(_financial_fetch_candidates(database_path), start=1):
        source_url = f"{base_url}/{org['org_number']}"
        yield _fetch_brreg_financial_statement(
            client=http_client,
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            timeout_seconds=timeout_seconds,
            fetched_at=fetch_timestamp,
        )


def _fetch_brreg_financial_statement(
    *,
    client: Any,
    org: Mapping[str, Any],
    source_url: str,
    source_run_id: str,
    source_line_number: int,
    timeout_seconds: int,
    fetched_at: str,
) -> dict[str, Any]:
    try:
        response = client.get(source_url, timeout=timeout_seconds)
    except Exception as exc:
        return financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=None,
            fetch_status=FINANCIAL_FETCH_STATUS_NETWORK_ERROR,
            error_type=type(exc).__name__,
            error_message=str(exc),
            fetched_at=fetched_at,
            attempt_count=1,
            raw_response="",
        )

    if response.status_code == 404:
        return financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status=FINANCIAL_FETCH_STATUS_NOT_FOUND,
            error_type="HTTPStatusError",
            error_message="HTTP 404",
            fetched_at=fetched_at,
            attempt_count=1,
            raw_response=response.text,
        )

    if response.status_code >= 500:
        return financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status=FINANCIAL_FETCH_STATUS_SERVER_ERROR,
            error_type="HTTPStatusError",
            error_message=f"HTTP {response.status_code}",
            fetched_at=fetched_at,
            attempt_count=1,
            raw_response=response.text,
        )

    payload = response.json()
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        return financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status=FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD,
            error_type="InvalidPayload",
            error_message="Expected BRREG financial response payload to be a list of objects",
            fetched_at=fetched_at,
            attempt_count=1,
            raw_response=response.text,
        )

    return financial_fetch_success_row(
        org=org,
        source_url=source_url,
        payload=payload,
        source_run_id=source_run_id,
        source_line_number=source_line_number,
        status_code=response.status_code,
        fetched_at=fetched_at,
        attempt_count=1,
    )


def _financial_fetch_candidates(database_path: str | Path) -> list[dict[str, Any]]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            """
            select org_number, legal_name, website, last_submitted_accounts_year
            from norway_brreg.entities
            where is_active = true
              and nullif(trim(website), '') is not null
              and nullif(trim(last_submitted_accounts_year), '') is not null
            order by org_number
            """
        ).fetchall()
    return [
        {
            "org_number": _string(org_number),
            "legal_name": _string(legal_name),
            "website": _string(website),
            "last_submitted_accounts_year": _string(last_submitted_accounts_year),
        }
        for org_number, legal_name, website, last_submitted_accounts_year in rows
    ]


def _default_financial_fetch_http_client(
    *,
    timeout_seconds: int,
    user_agent: str,
) -> DltRequestsClient:
    return DltRequestsClient(
        request_timeout=timeout_seconds,
        max_connections=8,
        raise_for_status=False,
        request_max_attempts=5,
        request_backoff_factor=2.0,
        request_max_retry_delay=120.0,
        respect_retry_after_header=True,
        session_attrs={"headers": {"User-Agent": user_agent}},
    )


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_norway_brreg_financial_fetches.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/financial_fetches.py tests/test_norway_brreg_financial_fetches.py
git commit -m "feat: persist norway brreg financial fetch outcomes"
```

---

## Task 3: Add Pure Financial Normalization From Successful Fetches

**Files:**
- Create: `src/dagster_v3/defs/norway_brreg/financial_normalize.py`
- Create: `tests/test_norway_brreg_financial_normalize.py`
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`

- [ ] **Step 1: Write failing normalization tests**

Create `tests/test_norway_brreg_financial_normalize.py`:

```python
import json
from decimal import Decimal

from dagster_v3.defs.norway_brreg import financial_normalize


class FakeUsdRate:
    rate = Decimal("0.10")
    rate_date = "2024-12-31"
    source = "test-fx"

    def convert(self, amount: Decimal) -> Decimal:
        return amount * self.rate


class FakeExchangeRates:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []

    def usd_rates(self, requests):
        self.requests.extend((request.currency, request.rate_date) for request in requests)
        return {
            (request.currency, request.rate_date): FakeUsdRate()
            for request in requests
        }


def _financial_record() -> dict:
    return {
        "id": 5667197,
        "journalnr": "2025428073",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "923609016",
            "organisasjonsform": "ASA",
            "morselskap": True,
        },
        "regnskapsperiode": {"fraDato": "2024-01-01", "tilDato": "2024-12-31"},
        "valuta": "NOK",
        "avviklingsregnskap": False,
        "oppstillingsplan": "store",
        "revisjon": {"ikkeRevidertAarsregnskap": False, "fravalgRevisjon": False},
        "regnkapsprinsipper": {"smaaForetak": False, "regnskapsregler": "forenkletAnvendelseIFRS"},
        "egenkapitalGjeld": {
            "egenkapital": {"sumEgenkapital": 41090000000},
            "gjeldOversikt": {
                "sumGjeld": 68060000000,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 42024000000},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 26036000000},
            },
        },
        "eiendeler": {
            "sumEiendeler": 109150000000,
            "omloepsmidler": {"sumOmloepsmidler": 50000000000},
            "anleggsmidler": {"sumAnleggsmidler": 59150000000},
        },
        "resultatregnskapResultat": {
            "driftsresultat": {
                "driftsinntekter": {"sumDriftsinntekter": 72543000000},
                "driftskostnad": {"sumDriftskostnad": 61000000000},
                "driftsresultat": 11543000000,
            },
            "finansresultat": {"nettoFinans": -500000000},
            "ordinaertResultatFoerSkattekostnad": 11043000000,
            "aarsresultat": 8500000000,
        },
    }


def test_build_financial_statement_rows_from_fetch_rows_uses_batched_fx() -> None:
    exchange_rates = FakeExchangeRates()
    fetch_rows = [
        {
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "www.equinor.com",
            "last_submitted_accounts_year": "2024",
            "source_run_id": "run-1",
            "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
            "fetch_status": "success",
            "raw_response": json.dumps([_financial_record()]),
        }
    ]

    rows = financial_normalize.build_financial_statement_rows_from_fetch_rows(
        fetch_rows,
        exchange_rates=exchange_rates,
    )

    assert len(rows) == 1
    assert rows[0]["org_number"] == "923609016"
    assert rows[0]["period_end_date"] == "2024-12-31"
    assert rows[0]["currency"] == "NOK"
    assert rows[0]["operating_revenue_amount_usd"] == Decimal("7254300000.0")
    assert exchange_rates.requests == [("NOK", "2024-12-31")]


def test_build_financial_statement_rows_from_fetch_rows_skips_unsuccessful_fetches() -> None:
    rows = financial_normalize.build_financial_statement_rows_from_fetch_rows(
        [
            {
                "org_number": "811685852",
                "legal_name": "MISSING AS",
                "website": "www.missing.test",
                "last_submitted_accounts_year": "2024",
                "source_run_id": "run-1",
                "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/811685852",
                "fetch_status": "not_found",
                "raw_response": "",
            }
        ],
        exchange_rates=FakeExchangeRates(),
    )

    assert rows == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_financial_normalize.py -q
```

Expected: fail with missing `financial_normalize` module.

- [ ] **Step 3: Move normalization logic into new module**

Create `src/dagster_v3/defs/norway_brreg/financial_normalize.py` by moving the existing functions from `assets.py`:

```python
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any, Protocol

from dagster_v3.exchange_rates import ExchangeRateRequest

COUNTRY = "NO"
FINANCIAL_SOURCE_SLUG = "norway_brregregnskap"


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[ExchangeRateRequest]) -> dict[tuple[str, str], Any]: ...


def build_financial_statement_rows_from_fetch_rows(
    fetch_rows: list[dict[str, Any]],
    *,
    exchange_rates: ExchangeRates,
) -> list[dict[str, Any]]:
    successful_records: list[tuple[dict[str, Any], dict[str, Any], int]] = []
    rate_requests_by_key: dict[tuple[str, str], ExchangeRateRequest] = {}
    for fetch_row in fetch_rows:
        if fetch_row.get("fetch_status") != "success":
            continue
        payload = json.loads(_string(fetch_row.get("raw_response")) or "[]")
        if not isinstance(payload, list):
            continue
        for line_number, record in enumerate(payload, start=1):
            if not isinstance(record, dict):
                continue
            currency = _string(record.get("valuta")).upper()
            period = _dict(record.get("regnskapsperiode"))
            period_end_date = _string(period.get("tilDato"))
            rate_requests_by_key[(currency, period_end_date)] = ExchangeRateRequest(
                currency=currency,
                rate_date=period_end_date,
            )
            successful_records.append((fetch_row, record, line_number))

    rates = exchange_rates.usd_rates(list(rate_requests_by_key.values()))
    return [
        _financial_statement_row(
            record,
            org=fetch_row,
            line_number=line_number,
            fx_rate=rates[
                (
                    _string(record.get("valuta")).upper(),
                    _string(_dict(record.get("regnskapsperiode")).get("tilDato")),
                )
            ],
            run_id=_string(fetch_row.get("source_run_id")),
            source_url=_string(fetch_row.get("source_url")),
        )
        for fetch_row, record, line_number in successful_records
    ]
```

Then copy the existing `_financial_statement_row`, `source_payload_hash`, `_json_dumps`, `_json_default`, `_dict`, `_bool`, `_int_or_none`, `_decimal_or_none`, `_fiscal_year`, and `_string` helpers from `assets.py` into `financial_normalize.py`, changing `_financial_statement_row` to accept `fx_rate` directly instead of `exchange_rates`.

Use this signature:

```python
def _financial_statement_row(
    record: dict[str, Any],
    *,
    org: dict[str, Any],
    line_number: int,
    fx_rate: Any,
    run_id: str,
    source_url: str,
) -> dict[str, Any]:
```

Inside the row builder, remove this line:

```python
fx_rate = exchange_rates.usd_rate(currency=currency, rate_date=period_end_date)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_norway_brreg_financial_normalize.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/financial_normalize.py tests/test_norway_brreg_financial_normalize.py
git commit -m "feat: normalize norway brreg financial fetch rows"
```

---

## Task 4: Rewire Dagster Assets Into Fetch And Normalize Stages

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Write failing asset graph tests**

Modify `tests/test_norway_brreg_assets.py` asset registration test to assert the new asset exists and dependencies are explicit:

```python
def test_norway_entity_asset_is_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}

    assert "norway_brreg_entities_duckdb" in asset_keys
    assert "norway_brreg_financial_fetches_duckdb" in asset_keys
    assert "norway_brreg_financial_statements_duckdb" in asset_keys
    assert "norway_brreg_clickhouse_tables" in asset_keys
```

Add this focused dependency assertion:

```python
def test_norway_financial_asset_dependencies_are_split() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph

    deps_by_asset = {
        key.path[-1]: {dep.path[-1] for dep in asset_graph.get(key).parent_keys}
        for key in asset_graph.get_all_asset_keys()
    }

    assert deps_by_asset["norway_brreg_financial_fetches_duckdb"] == {
        "norway_brreg_entities_duckdb"
    }
    assert deps_by_asset["norway_brreg_financial_statements_duckdb"] == {
        "norway_brreg_financial_fetches_duckdb"
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_entity_asset_is_registered tests/test_norway_brreg_assets.py::test_norway_financial_asset_dependencies_are_split -q
```

Expected: fail because `norway_brreg_financial_fetches_duckdb` does not exist.

- [ ] **Step 3: Add fetch asset that runs one dlt resource directly**

In `src/dagster_v3/defs/norway_brreg/assets.py`, import:

```python
import dlt

from dagster_v3.defs.norway_brreg.financial_fetches import (
    BRREG_FINANCIAL_FETCHES_COLUMNS,
    BRREG_REGNSKAP_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_USER_AGENT,
    FINANCIAL_FETCHES_TABLE,
    iter_brreg_financial_statement_fetch_rows,
)
from dagster_v3.defs.norway_brreg.financial_normalize import (
    build_financial_statement_rows_from_fetch_rows,
)
```

Do not add `FINANCIAL_FETCHES_TABLE` to `NorwayBrregDltTranslator.get_asset_spec`. This fetch table is a single dlt resource run inside a plain Dagster asset, so the Dagster asset metadata belongs on `@dg.asset`.

Add a new plain Dagster asset:

```python
@dg.asset(
    deps=[dg.AssetKey("norway_brreg_entities_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "dlt", "duckdb"},
    description="Norway Brreg annual-account fetch outcomes loaded to local DuckDB with dlt.",
)
def norway_brreg_financial_fetches_duckdb_asset(
    context: AssetExecutionContext,
    config: NorwayBrregFinancialFetchConfig,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Norway Brreg financial fetch load: duckdb_path=%s, input_table=%s.%s, output_table=%s.%s",
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
        DLT_DATASET_NAME,
        FINANCIAL_FETCHES_TABLE,
    )

    resource = dlt.resource(
        iter_brreg_financial_statement_fetch_rows(
            database_path=NORWAY_BRREG_DUCKDB_PATH,
            source_run_id=context.run_id,
            base_url=config.base_url,
            timeout_seconds=config.timeout_seconds,
            user_agent=config.user_agent,
        ),
        name=FINANCIAL_FETCHES_TABLE,
        write_disposition="replace",
        primary_key=["org_number", "source_run_id"],
        columns=BRREG_FINANCIAL_FETCHES_COLUMNS,
    )
    load_info = norway_brreg_pipeline(
        NORWAY_BRREG_DUCKDB_PATH,
        pipeline_name="norway_brreg_financial_fetches",
    ).run(resource)
    row_count = _duckdb_table_count(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
        table_name=f"{DLT_DATASET_NAME}.{FINANCIAL_FETCHES_TABLE}",
    )
    status_counts = _duckdb_fetch_status_counts(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
        table_name=f"{DLT_DATASET_NAME}.{FINANCIAL_FETCHES_TABLE}",
    )

    context.log.info(
        "Completed Norway Brreg financial fetch load: duckdb_path=%s, table=%s.%s, rows=%s, statuses=%s",
        NORWAY_BRREG_DUCKDB_PATH,
        DLT_DATASET_NAME,
        FINANCIAL_FETCHES_TABLE,
        row_count,
        status_counts,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "status_counts": status_counts,
            "load_info": str(load_info),
        }
    )
```

Add the config and count helpers in `assets.py`:

```python
class NorwayBrregFinancialFetchConfig(dg.Config):
    base_url: str = BRREG_REGNSKAP_BASE_URL
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    user_agent: str = DEFAULT_USER_AGENT


def _duckdb_table_count(*, database_path: str | Path, table_name: str) -> int:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        value = connection.execute(f"select count(*) from {table_name}").fetchone()[0]
    return int(value)


def _duckdb_fetch_status_counts(
    *,
    database_path: str | Path,
    table_name: str,
) -> dict[str, int]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        rows = connection.execute(
            f"""
            select fetch_status, count(*) as row_count
            from {table_name}
            group by fetch_status
            order by fetch_status
            """
        ).fetchall()
    return {str(status): int(row_count) for status, row_count in rows}
```

- [ ] **Step 4: Replace financial statements dlt asset with pure DuckDB transform asset**

Remove `@dlt_assets` from `norway_brreg_financial_statements_duckdb_asset` and replace it with:

```python
@dg.asset(
    deps=[dg.AssetKey("norway_brreg_financial_fetches_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    description="Norway Brreg normalized annual-account rows derived from successful fetch outcomes.",
)
def norway_brreg_financial_statements_duckdb_asset(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    counts = normalize_norway_brreg_financial_statements_duckdb(
        database_path=NORWAY_BRREG_DUCKDB_PATH,
        exchange_rates=ExchangeRateClient.from_env(),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)
```

Add this helper in `assets.py`:

```python
def normalize_norway_brreg_financial_statements_duckdb(
    *,
    database_path: str | Path,
    exchange_rates: ExchangeRates,
    log: Callable[..., None] | None = None,
) -> dict[str, int]:
    with duckdb.connect(str(database_path)) as connection:
        fetch_rows = _fetch_duckdb_dicts(
            connection,
            dataset=DLT_DATASET_NAME,
            table=FINANCIAL_FETCHES_TABLE,
            columns=tuple(BRREG_FINANCIAL_FETCHES_COLUMNS),
        )
        rows = build_financial_statement_rows_from_fetch_rows(
            fetch_rows,
            exchange_rates=exchange_rates,
        )
        connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        connection.execute(f"drop table if exists {DLT_DATASET_NAME}.{FINANCIAL_STATEMENTS_TABLE}")
        if rows:
            connection.register("financial_statement_rows", rows)
            connection.execute(
                f"create table {DLT_DATASET_NAME}.{FINANCIAL_STATEMENTS_TABLE} as select * from financial_statement_rows"
            )
        else:
            column_defs = ", ".join(
                f"{column_name} varchar"
                for column_name in BRREG_FINANCIAL_STATEMENTS_COLUMNS
            )
            connection.execute(
                f"create table {DLT_DATASET_NAME}.{FINANCIAL_STATEMENTS_TABLE} ({column_defs})"
            )
    _log(log, "Normalized Norway Brreg financial statements: fetches=%s, rows=%s", len(fetch_rows), len(rows))
    return {
        "financial_fetches": len(fetch_rows),
        "financial_statements": len(rows),
        "successful_fetches": sum(1 for row in fetch_rows if row.get("fetch_status") == "success"),
        "failed_fetches": sum(1 for row in fetch_rows if row.get("fetch_status") != "success"),
    }
```

Then replace references in `defs = dg.Definitions(...)` or project definitions so `norway_brreg_financial_fetches_duckdb_asset` is included.

- [ ] **Step 5: Run asset tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py -q
uv run dg check defs
```

Expected:

```text
All definitions loaded successfully.
```

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "feat: split norway brreg financial fetch and normalize assets"
```

---

## Task 5: Remove Old HTTP Transformer Path And Compatibility Helpers

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Write failing removal test**

Add to `tests/test_norway_brreg_assets.py`:

```python
def test_norway_brreg_assets_no_longer_exposes_financial_http_transformer() -> None:
    assert not hasattr(brreg_assets, "_financial_statements_resource")
    assert not hasattr(brreg_assets, "norway_brreg_financial_orgs_resource")
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_norway_brreg_assets_no_longer_exposes_financial_http_transformer -q
```

Expected: fail because old functions still exist.

- [ ] **Step 3: Remove old source/transformer functions**

Delete these symbols from `src/dagster_v3/defs/norway_brreg/assets.py`:

```text
norway_brreg_financial_statements_source
_http_session_factory
norway_brreg_financial_orgs_resource
_financial_statements_resource
run_norway_brreg_financial_statements_dlt_pipeline
```

Move any tests that still need old behavior to use:

```python
iter_brreg_financial_statement_fetch_rows(...)
normalize_norway_brreg_financial_statements_duckdb(...)
```

- [ ] **Step 4: Run BRREG tests**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py tests/test_norway_brreg_financial_fetches.py tests/test_norway_brreg_financial_normalize.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "refactor: remove norway brreg financial http transformer"
```

---

## Task 6: Add True Batch Exchange-Rate API

**Files:**
- Modify: `src/dagster_v3/exchange_rates/client.py`
- Modify: `tests/test_exchange_rate_client.py`

- [ ] **Step 1: Write failing batch query test**

Add to `tests/test_exchange_rate_client.py`:

```python
def test_exchange_rate_client_batches_unique_currency_date_requests() -> None:
    clickhouse = FakeNativeClickHouseClient(
        rows=[
            ("2024-12-31", "USD", "1.0389", "ECB EXR", "usd-1231", "hash-usd-1231", "2026-06-16"),
            ("2024-12-31", "NOK", "11.7950", "ECB EXR", "nok-1231", "hash-nok-1231", "2026-06-16"),
            ("2023-01-02", "USD", "1.0666", "ECB EXR", "usd-2023", "hash-usd-2023", "2026-06-16"),
            ("2023-01-02", "NOK", "10.5138", "ECB EXR", "nok-2023", "hash-nok-2023", "2026-06-16"),
        ]
    )
    client = ExchangeRateClient(clickhouse)

    rates = client.usd_rates(
        [
            ExchangeRateRequest(currency="NOK", rate_date="2024-12-31"),
            ExchangeRateRequest(currency="NOK", rate_date="2024-12-31"),
            ExchangeRateRequest(currency="NOK", rate_date="2022-12-31"),
        ]
    )

    assert rates[("NOK", "2024-12-31")].rate_date == "2024-12-31"
    assert rates[("NOK", "2022-12-31")].rate_date == "2023-01-02"
    assert len(clickhouse.queries) == 1
    assert "requested_rates" in clickhouse.queries[0].sql
    assert "selected_dates" in clickhouse.queries[0].sql
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_exchange_rate_client.py::test_exchange_rate_client_batches_unique_currency_date_requests -q
```

Expected: fail because current implementation issues multiple bounded queries.

- [ ] **Step 3: Implement one-query batch lookup**

In `src/dagster_v3/exchange_rates/client.py`, replace the per-load loop in `usd_rates` with a single `_load_components_for_requests` method.

Use this API:

```python
def _load_components_for_requests(
    self,
    requests: list[ExchangeRateRequest],
) -> dict[tuple[str, str, str], ExchangeRateComponent]:
```

Return components keyed by `(request_currency, requested_rate_date, quote_currency)` so two requested dates can resolve to different selected FX dates without overwriting each other.

Add a helper that expands the request set into one row per required quote currency:

```python
def _requested_components_sql(requests: list[ExchangeRateRequest]) -> str:
    rows: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    for request in requests:
        request_currency = _sql_currency(request.currency)
        requested_rate_date = _sql_date(request.rate_date)
        for quote_currency in sorted(_required_currencies(request_currency)):
            key = (request_currency, requested_rate_date, quote_currency)
            if key in seen:
                continue
            rows.append(
                "SELECT "
                f"'{request_currency}' AS request_currency, "
                f"toDate('{requested_rate_date}') AS requested_rate_date, "
                f"'{quote_currency}' AS quote_currency"
            )
            seen.add(key)
    return "\nUNION ALL\n".join(rows)


def _sql_currency(currency: str) -> str:
    value = currency.upper()
    if not value.isalpha() or len(value) != 3:
        raise ValueError(f"Invalid currency code: {currency}")
    return value


def _sql_date(rate_date: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", rate_date):
        raise ValueError(f"Invalid rate date: {rate_date}")
    return rate_date
```

The SQL should use that inline table and choose the latest date on or before the requested date, falling back to the first available date after it:

```sql
WITH requested_rates AS (
    SELECT 'NOK' AS request_currency, toDate('2024-12-31') AS requested_rate_date, 'NOK' AS quote_currency
    UNION ALL
    SELECT 'NOK' AS request_currency, toDate('2024-12-31') AS requested_rate_date, 'USD' AS quote_currency
),
requested_counts AS (
    SELECT
        request_currency,
        requested_rate_date,
        countDistinct(quote_currency) AS required_currency_count
    FROM requested_rates
    GROUP BY request_currency, requested_rate_date
),
available_dates AS (
    SELECT
        requested_rates.request_currency,
        requested_rates.requested_rate_date,
        exchange_rates.rate_date,
        if(exchange_rates.rate_date <= requested_rates.requested_rate_date, 0, 1) AS priority
    FROM requested_rates
    INNER JOIN reference.exchange_rates AS exchange_rates
        ON exchange_rates.base_currency = 'EUR'
       AND exchange_rates.quote_currency = requested_rates.quote_currency
    INNER JOIN requested_counts
        ON requested_counts.request_currency = requested_rates.request_currency
       AND requested_counts.requested_rate_date = requested_rates.requested_rate_date
    GROUP BY
        requested_rates.request_currency,
        requested_rates.requested_rate_date,
        exchange_rates.rate_date,
        priority,
        requested_counts.required_currency_count
    HAVING countDistinct(exchange_rates.quote_currency) = requested_counts.required_currency_count
),
selected_dates AS (
    SELECT
        request_currency,
        requested_rate_date,
        if(
            countIf(priority = 0) > 0,
            maxIf(rate_date, priority = 0),
            minIf(rate_date, priority = 1)
        ) AS selected_rate_date
    FROM available_dates
    GROUP BY request_currency, requested_rate_date
)
SELECT
    selected_dates.request_currency,
    toString(selected_dates.requested_rate_date),
    toString(exchange_rates.rate_date),
    exchange_rates.quote_currency,
    toString(exchange_rates.rate),
    exchange_rates.source,
    exchange_rates.source_url,
    exchange_rates.source_payload_hash,
    toString(exchange_rates.pulled_at)
FROM selected_dates
INNER JOIN requested_rates
    ON requested_rates.request_currency = selected_dates.request_currency
   AND requested_rates.requested_rate_date = selected_dates.requested_rate_date
INNER JOIN reference.exchange_rates AS exchange_rates
    ON exchange_rates.base_currency = 'EUR'
   AND exchange_rates.quote_currency = requested_rates.quote_currency
   AND exchange_rates.rate_date = selected_dates.selected_rate_date
ORDER BY selected_dates.request_currency, selected_dates.requested_rate_date, exchange_rates.quote_currency
```

In the implementation, replace `reference.exchange_rates` in the snippet with `{self._table}` only. Do not interpolate any unvalidated request values into SQL; pass every request through `_sql_currency` and `_sql_date`.

Parse rows as:

```python
return {
    (str(row[0]).upper(), str(row[1]), str(row[3]).upper()): ExchangeRateComponent(
        rate_date=str(row[2]),
        base_currency="EUR",
        quote_currency=str(row[3]).upper(),
        rate=Decimal(str(row[4])),
        source=str(row[5]),
        source_url=str(row[6]),
        source_payload_hash=str(row[7]),
        pulled_at=str(row[8]),
    )
    for row in rows
}
```

Update `_resolve_usd_rate` to read components by `(request.currency, request.rate_date, quote_currency)`. The error message should keep the requested date in the text, but it can mention “on, before, or after” because the fallback now intentionally uses the first available future date when the request predates stored rates.

- [ ] **Step 4: Run exchange-rate tests**

Run:

```bash
uv run pytest tests/test_exchange_rate_client.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/exchange_rates/client.py tests/test_exchange_rate_client.py
git commit -m "perf: batch exchange rate lookups"
```

---

## Task 7: Add Observability Metadata And Verification

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Add metadata test for financial normalization**

Add:

```python
def test_normalize_norway_brreg_financial_statements_reports_fetch_counts(tmp_path: Path) -> None:
    database_path = tmp_path / "norway.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema if not exists norway_brreg")
        connection.execute(
            """
            create or replace table norway_brreg.financial_fetches as
            select *
            from (
                values
                    ('NO', 'norway_brregregnskap_fetch', 'run-1', 1, '923609016', repeat('a', 64), '923609016', 'EQUINOR ASA', 'www.equinor.com', '2024', 'url-1', 'success', 200, '', '', 1, timestamp '2026-06-17 00:00:00', '[]'),
                    ('NO', 'norway_brregregnskap_fetch', 'run-1', 2, '811685852', repeat('0', 64), '811685852', 'MISSING AS', 'www.missing.test', '2024', 'url-2', 'not_found', 404, 'HTTPStatusError', 'HTTP 404', 1, timestamp '2026-06-17 00:00:00', '')
            ) as t(country_iso2, source_slug, source_run_id, source_line_number, source_record_id, source_payload_hash, org_number, legal_name, website, last_submitted_accounts_year, source_url, fetch_status, http_status, error_type, error_message, attempt_count, fetched_at, raw_response)
            """
        )

    counts = brreg_assets.normalize_norway_brreg_financial_statements_duckdb(
        database_path=database_path,
        exchange_rates=FakeExchangeRates(),
    )

    assert counts == {
        "financial_fetches": 2,
        "financial_statements": 0,
        "successful_fetches": 1,
        "failed_fetches": 1,
    }
```

- [ ] **Step 2: Run test**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_normalize_norway_brreg_financial_statements_reports_fetch_counts -q
```

Expected: pass.

- [ ] **Step 3: Run full verification**

Run:

```bash
uv run pytest -q
uv run dg check defs
```

Expected:

```text
All definitions loaded successfully.
```

- [ ] **Step 4: Commit**

```bash
git add src/dagster_v3/defs/norway_brreg/assets.py tests/test_norway_brreg_assets.py
git commit -m "test: report norway brreg financial fetch observability"
```

---

## Self-Review

- Spec coverage:
  - Durable fetch outcomes are implemented in Tasks 1 and 2.
  - Failure observability replaces silent `return` behavior in Tasks 1, 2, and 7.
  - Financial normalization is separated from HTTP extraction in Tasks 3 and 4.
  - Batched exchange-rate usage is implemented in Tasks 3 and 6.
  - Dagster asset graph is split into explicit durable stages in Task 4.
- Placeholder scan:
  - The plan intentionally avoids `TBD`, `TODO`, and open-ended “handle errors” instructions.
  - Each task includes exact file paths, code snippets, commands, and expected results.
- Type consistency:
  - Fetch status rows use `fetch_status`, `http_status`, `raw_response`, and `source_run_id` consistently across fetch and normalize tasks.
  - Normalization consumes rows from `FINANCIAL_FETCHES_TABLE` and produces rows for `FINANCIAL_STATEMENTS_TABLE`.
  - Dagster asset keys are consistently named `norway_brreg_financial_fetches_duckdb` and `norway_brreg_financial_statements_duckdb`.
