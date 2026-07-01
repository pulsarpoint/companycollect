# Norway Financial Bootstrap Slot Markers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Norway financial bootstrap S3 candidate batch shards with deterministic ClickHouse candidate slots, Temporal continue-as-new slot workflows, and S3 raw-report status markers.

**Architecture:** Startup code creates or reuses a frozen ClickHouse candidate table keyed by `run_id + slot + slot_index`; this is not a Temporal workflow or activity. After startup preparation, the service starts one Temporal workflow chain per slot. Each workflow run gets one candidate, skips it if S3 status markers exist, otherwise fetches BRREG structured financial data for that org, writes raw reports and a colocated done/failed marker, then continues as new with `slot_index + 1`.

**Tech Stack:** Python 3.14, Temporal Python SDK, ClickHouse via `clickhouse_connect`, boto3 S3-compatible storage, pytest, ruff.

---

## Current State

The package currently lives under:

```text
corpscout/norway_financial_bootstrap/
```

It still has uncommitted code from the previous batch-based experiment. Before implementing this plan, inspect the current working tree and decide whether to keep or discard each uncommitted change. Do not blindly revert user work.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
```

Expected current dirty files include:

```text
corpscout/norway_financial_bootstrap/norway_financial_bootstrap/activities.py
corpscout/norway_financial_bootstrap/norway_financial_bootstrap/cli.py
corpscout/norway_financial_bootstrap/norway_financial_bootstrap/storage.py
corpscout/norway_financial_bootstrap/norway_financial_bootstrap/workflows.py
corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py
corpscout/norway_financial_bootstrap/norway_financial_bootstrap/paths.py
```

Keep useful fixes that are still valid, such as:

- Temporal activity wrappers having exactly one payload argument.
- Heartbeating during retry sleeps.
- Comments explaining Temporal argument decoding.

Remove or replace batch-shard concepts:

- `candidate_batch_key`
- `write_candidate_batches`
- `read_candidate_batch`
- `BootstrapInput.batch_count`
- `BootstrapInput.batch_keys`
- `FetchBatchInput.candidate_batch_key`
- `existing_raw_report_ids` from the hot path.

## Target File Structure

Modify:

- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/candidates.py`
  - Candidate dataclass should contain only `org_number`.
- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/clickhouse.py`
  - Source candidate query.
  - Candidate table DDL.
  - Deterministic slot insertion.
  - Candidate lookup by `run_id + slot + slot_index`.
  - Plain startup function for candidate table preparation; this must not be a Temporal activity.
- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/paths.py`
  - Raw report key.
  - Org status marker keys.
  - Remove candidate batch path helpers.
- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/storage.py`
  - S3 exact object existence.
  - Raw report writes.
  - Done/failed marker reads/writes.
  - Remove candidate parquet batch storage.
- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/brreg_client.py`
  - Fetch by `org_number` only, no `år` query parameter.
- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/fetch_status.py`
  - Remove candidate fields not used by bootstrap markers.
- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/activities.py`
  - Replace batch activity with focused slot activities.
- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/workflows.py`
  - Replace batch workflow with slot workflow.
- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/cli.py`
  - Prepare candidate table during service startup and start slot workflows.
- `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/worker.py`
  - Register the new workflow and activities.
- `corpscout/norway_financial_bootstrap/README.md`
  - Update operation instructions.

Tests:

- `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_candidates.py`
- `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_clickhouse.py`
- `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_storage.py`
- `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_brreg_client.py`
- `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py`
- `corpscout/norway_financial_bootstrap/tests/test_package_independence.py`

---

### Task 1: Simplify Candidate Model To Organization Only

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/candidates.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_candidates.py`

- [ ] **Step 1: Write the failing candidate tests**

Replace the candidate test file content with tests that assert only `org_number` is carried:

```python
import polars as pl

from norway_financial_bootstrap.candidates import (
    FinancialCandidate,
    build_financial_candidates,
)


def test_build_financial_candidates_filters_active_companies_with_accounts_year() -> None:
    frame = pl.DataFrame(
        {
            "org_number": ["300", "100", "200", "400"],
            "name": ["ignored", "ignored", "ignored", "ignored"],
            "primary_website_url": ["https://x.test", None, "", ""],
            "is_active": [True, True, False, True],
            "last_submitted_accounts_year": ["2024", "2023", "2024", None],
        }
    )

    assert build_financial_candidates(frame) == [
        FinancialCandidate("100"),
        FinancialCandidate("300"),
    ]


def test_financial_candidate_mapping_contains_only_org_number() -> None:
    assert FinancialCandidate("923609016").as_org_mapping() == {
        "org_number": "923609016"
    }
```

- [ ] **Step 2: Run candidate tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_candidates.py -q
```

Expected: FAIL because the dataclass still has `legal_name`, `website`, and `last_submitted_accounts_year`.

- [ ] **Step 3: Implement the candidate model**

Update `candidates.py` to:

```python
from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True, order=True)
class FinancialCandidate:
    org_number: str

    def as_org_mapping(self) -> dict[str, str]:
        return {"org_number": self.org_number}


def build_financial_candidates(frame: pl.DataFrame) -> list[FinancialCandidate]:
    rows = (
        frame.filter(
            pl.col("is_active").eq(True)
            & pl.col("last_submitted_accounts_year").is_not_null()
        )
        .select(pl.col("org_number").cast(pl.String))
        .unique()
        .sort("org_number")
        .to_dicts()
    )
    return [FinancialCandidate(row["org_number"]) for row in rows]
```

- [ ] **Step 4: Run candidate tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_candidates.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/norway_financial_bootstrap/candidates.py \
  corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_candidates.py
git commit -m "refactor: simplify norway financial candidates"
```

---

### Task 2: Add ClickHouse Frozen Candidate Table API

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/clickhouse.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_clickhouse.py`

- [ ] **Step 1: Write failing ClickHouse tests**

Replace the clickhouse test file content with:

```python
from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.clickhouse import (
    CANDIDATE_TABLE,
    CREATE_CANDIDATE_TABLE_SQL,
    INSERT_CANDIDATES_SQL,
    NO_COMPANIES_QUERY,
    candidate_table_has_run,
    financial_candidates_from_clickhouse,
    get_candidate_from_clickhouse,
    prepare_candidate_table,
)


class FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClickHouseClient:
    def __init__(self, query_results=None):
        self.query_results = list(query_results or [])
        self.queries = []
        self.commands = []

    def query(self, sql, parameters=None):
        self.queries.append((sql, parameters))
        return self.query_results.pop(0)

    def command(self, sql, parameters=None):
        self.commands.append((sql, parameters))


def test_financial_candidates_from_clickhouse_reads_only_org_number() -> None:
    client = FakeClickHouseClient([FakeQueryResult([("923609016",), ("811685852",)])])

    assert financial_candidates_from_clickhouse(client) == [
        FinancialCandidate("923609016"),
        FinancialCandidate("811685852"),
    ]

    assert "toString(org_number) as org_number" in NO_COMPANIES_QUERY
    assert "last_submitted_accounts_year is not null" in NO_COMPANIES_QUERY.lower()
    assert "name" not in NO_COMPANIES_QUERY.lower()
    assert "website" not in NO_COMPANIES_QUERY.lower()


def test_candidate_table_sql_uses_deterministic_round_robin_slots() -> None:
    assert CANDIDATE_TABLE == "norway_financial_bootstrap_candidates"
    assert "ENGINE = MergeTree" in CREATE_CANDIDATE_TABLE_SQL
    assert "ORDER BY (run_id, slot, slot_index)" in CREATE_CANDIDATE_TABLE_SQL
    assert "rn % {slot_count:UInt8}" in INSERT_CANDIDATES_SQL
    assert "intDiv(rn, {slot_count:UInt8})" in INSERT_CANDIDATES_SQL
    assert "row_number() OVER (ORDER BY org_number) - 1 AS rn" in INSERT_CANDIDATES_SQL


def test_prepare_candidate_table_inserts_only_when_run_is_missing() -> None:
    client = FakeClickHouseClient([FakeQueryResult([(0,)])])

    prepared = prepare_candidate_table(client, run_id="run-1", slot_count=4)

    assert prepared.inserted is True
    assert prepared.existing_count == 0
    assert client.commands[0][0] == CREATE_CANDIDATE_TABLE_SQL
    assert client.commands[1][0] == INSERT_CANDIDATES_SQL
    assert client.commands[1][1] == {"run_id": "run-1", "slot_count": 4}


def test_prepare_candidate_table_reuses_existing_run() -> None:
    client = FakeClickHouseClient([FakeQueryResult([(12,)])])

    prepared = prepare_candidate_table(client, run_id="run-1", slot_count=4)

    assert prepared.inserted is False
    assert prepared.existing_count == 12
    assert client.commands == [(CREATE_CANDIDATE_TABLE_SQL, None)]


def test_get_candidate_from_clickhouse_reads_slot_index() -> None:
    client = FakeClickHouseClient([FakeQueryResult([("923609016",)])])

    candidate = get_candidate_from_clickhouse(
        client, run_id="run-1", slot_id=2, slot_index=9
    )

    assert candidate == FinancialCandidate("923609016")
    sql, params = client.queries[0]
    assert "where run_id = {run_id:String}" in sql.lower()
    assert "slot = {slot:UInt8}" in sql
    assert "slot_index = {slot_index:UInt64}" in sql
    assert params == {"run_id": "run-1", "slot": 2, "slot_index": 9}


def test_get_candidate_from_clickhouse_returns_none_when_missing() -> None:
    client = FakeClickHouseClient([FakeQueryResult([])])

    assert (
        get_candidate_from_clickhouse(client, run_id="run-1", slot_id=0, slot_index=99)
        is None
    )


def test_candidate_table_has_run_checks_existing_count() -> None:
    client = FakeClickHouseClient([FakeQueryResult([(3,)])])

    assert candidate_table_has_run(client, run_id="run-1") == 3
```

- [ ] **Step 2: Run ClickHouse tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_clickhouse.py -q
```

Expected: FAIL because candidate table functions do not exist yet.

- [ ] **Step 3: Implement ClickHouse API**

Update `clickhouse.py` to include:

```python
from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

import clickhouse_connect

from norway_financial_bootstrap.candidates import FinancialCandidate

CLICKHOUSE_DATABASE = "corpscout"
NO_COMPANIES_TABLE = "no_companies"
CANDIDATE_TABLE = "norway_financial_bootstrap_candidates"

NO_COMPANIES_QUERY = f"""
select
    toString(org_number) as org_number
from {CLICKHOUSE_DATABASE}.{NO_COMPANIES_TABLE}
where is_active = true
  and last_submitted_accounts_year is not null
order by org_number
"""

CREATE_CANDIDATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
(
  run_id String,
  slot UInt8,
  slot_index UInt64,
  org_number String,
  created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (run_id, slot, slot_index)
"""

INSERT_CANDIDATES_SQL = f"""
INSERT INTO {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
  (run_id, slot, slot_index, org_number)
SELECT
  {{run_id:String}} AS run_id,
  rn % {{slot_count:UInt8}} AS slot,
  intDiv(rn, {{slot_count:UInt8}}) AS slot_index,
  org_number
FROM (
  SELECT
    row_number() OVER (ORDER BY org_number) - 1 AS rn,
    toString(org_number) AS org_number
  FROM {CLICKHOUSE_DATABASE}.{NO_COMPANIES_TABLE}
  WHERE is_active = true
    AND last_submitted_accounts_year IS NOT NULL
)
ORDER BY slot, slot_index
"""

COUNT_CANDIDATES_FOR_RUN_SQL = f"""
SELECT count()
FROM {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
WHERE run_id = {{run_id:String}}
"""

GET_CANDIDATE_SQL = f"""
SELECT org_number
FROM {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
WHERE run_id = {{run_id:String}}
  AND slot = {{slot:UInt8}}
  AND slot_index = {{slot_index:UInt64}}
LIMIT 1
"""


@dataclass(frozen=True)
class CandidateTablePreparation:
    run_id: str
    slot_count: int
    existing_count: int
    inserted: bool


def clickhouse_from_env() -> Any:
    return clickhouse_connect.get_client(
        host=_required_env("CLICKHOUSE_HOST"),
        port=_int_env("CLICKHOUSE_HTTP_PORT", 8123),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=CLICKHOUSE_DATABASE,
        secure=_bool_env("CLICKHOUSE_SECURE", False),
    )


def financial_candidates_from_clickhouse(client: Any) -> list[FinancialCandidate]:
    result = client.query(NO_COMPANIES_QUERY)
    return [FinancialCandidate(org_number=str(row[0])) for row in result.result_rows]


def prepare_candidate_table(
    client: Any, *, run_id: str, slot_count: int
) -> CandidateTablePreparation:
    if slot_count < 1 or slot_count > 255:
        raise ValueError("slot_count must be between 1 and 255")
    client.command(CREATE_CANDIDATE_TABLE_SQL)
    existing_count = candidate_table_has_run(client, run_id=run_id)
    if existing_count > 0:
        return CandidateTablePreparation(
            run_id=run_id,
            slot_count=slot_count,
            existing_count=existing_count,
            inserted=False,
        )
    client.command(
        INSERT_CANDIDATES_SQL,
        parameters={"run_id": run_id, "slot_count": slot_count},
    )
    return CandidateTablePreparation(
        run_id=run_id,
        slot_count=slot_count,
        existing_count=0,
        inserted=True,
    )


def candidate_table_has_run(client: Any, *, run_id: str) -> int:
    result = client.query(COUNT_CANDIDATES_FOR_RUN_SQL, parameters={"run_id": run_id})
    if not result.result_rows:
        return 0
    return int(result.result_rows[0][0])


def get_candidate_from_clickhouse(
    client: Any, *, run_id: str, slot_id: int, slot_index: int
) -> FinancialCandidate | None:
    result = client.query(
        GET_CANDIDATE_SQL,
        parameters={"run_id": run_id, "slot": slot_id, "slot_index": slot_index},
    )
    if not result.result_rows:
        return None
    return FinancialCandidate(org_number=str(result.result_rows[0][0]))


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
```

- [ ] **Step 4: Run ClickHouse tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_clickhouse.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/norway_financial_bootstrap/clickhouse.py \
  corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_clickhouse.py
git commit -m "feat: add norway financial candidate slots"
```

---

### Task 3: Replace S3 Batch Paths With Raw Report And Status Marker Paths

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/paths.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_storage.py`

- [ ] **Step 1: Write failing path/storage tests**

In `test_norway_financial_bootstrap_storage.py`, replace old candidate-batch path tests with:

```python
from norway_financial_bootstrap.paths import (
    DEFAULT_BUCKET,
    RAW_REPORT_PREFIX,
    done_marker_key,
    failed_marker_key,
    raw_report_key,
    report_year_from_report,
)


def test_raw_report_key_matches_finance_storage_contract() -> None:
    assert DEFAULT_BUCKET == "source-norway-brreg"
    assert RAW_REPORT_PREFIX == "norway_brreg/finance/raw_reports/"
    assert raw_report_key("811685852", "2024", "SELSKAP", "6697842") == (
        "norway_brreg/finance/raw_reports/org=811685852/"
        "year=2024/type=SELSKAP/id=6697842.json"
    )


def test_status_marker_keys_are_colocated_under_org_raw_report_prefix() -> None:
    assert done_marker_key("811685852") == (
        "norway_brreg/finance/raw_reports/org=811685852/status/done.json"
    )
    assert failed_marker_key("811685852") == (
        "norway_brreg/finance/raw_reports/org=811685852/status/failed.json"
    )


def test_report_year_from_report_uses_regnskapsperiode_til_dato() -> None:
    assert (
        report_year_from_report(
            {"regnskapsperiode": {"fraDato": "2024-01-01", "tilDato": "2024-12-31"}}
        )
        == "2024"
    )


def test_report_year_from_report_falls_back_to_fra_dato() -> None:
    assert report_year_from_report({"regnskapsperiode": {"fraDato": "2023-01-01"}}) == "2023"
```

- [ ] **Step 2: Run storage tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_storage.py -q
```

Expected: FAIL because marker helpers and `report_year_from_report` do not exist.

- [ ] **Step 3: Implement path helpers**

Update `paths.py` to:

```python
from __future__ import annotations

from typing import Any

DEFAULT_BUCKET = "source-norway-brreg"
RAW_REPORT_PREFIX = "norway_brreg/finance/raw_reports/"


def raw_report_key(
    org_number: str, report_year: str, report_type: str, report_id: str
) -> str:
    return (
        f"{RAW_REPORT_PREFIX}org={org_number}/"
        f"year={report_year}/type={report_type}/id={report_id}.json"
    )


def done_marker_key(org_number: str) -> str:
    return f"{RAW_REPORT_PREFIX}org={org_number}/status/done.json"


def failed_marker_key(org_number: str) -> str:
    return f"{RAW_REPORT_PREFIX}org={org_number}/status/failed.json"


def report_year_from_report(report: dict[str, Any]) -> str:
    period = report.get("regnskapsperiode")
    if not isinstance(period, dict):
        raise RuntimeError("BRREG financial report is missing regnskapsperiode")
    date_value = period.get("tilDato") or period.get("fraDato")
    if date_value is None or str(date_value).strip() == "":
        raise RuntimeError("BRREG financial report is missing regnskapsperiode date")
    return str(date_value)[:4]
```

- [ ] **Step 4: Run storage path tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_storage.py -q
```

Expected: existing storage tests may still fail because `storage.py` still imports removed batch helpers. Continue to Task 4 before expecting full storage tests to pass.

- [ ] **Step 5: Commit**

Commit only if the path tests pass or if the only failures are from old storage imports that Task 4 explicitly removes:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/norway_financial_bootstrap/paths.py \
  corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_storage.py
git commit -m "refactor: add norway financial raw report marker paths"
```

---

### Task 4: Implement S3 Raw Report And Marker Storage

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/storage.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_storage.py`

- [ ] **Step 1: Add failing storage behavior tests**

Append these tests to `test_norway_financial_bootstrap_storage.py`:

```python
import json
from botocore.exceptions import ClientError

from norway_financial_bootstrap.storage import (
    CandidateMarkerStatus,
    NorwayFinancialBootstrapStorage,
)


class FakeS3Client:
    def __init__(self):
        self.objects = {}
        self.puts = []

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body
        self.puts.append((Bucket, Key, Body))

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def get_object(self, Bucket, Key):
        from io import BytesIO

        return {"Body": BytesIO(self.objects[(Bucket, Key)])}


def test_storage_checks_exact_object_existence_with_head_object() -> None:
    client = FakeS3Client()
    storage = NorwayFinancialBootstrapStorage(s3_client=client)

    assert storage.client_object_exists("missing.json") is False
    client.put_object(Bucket=storage.bucket, Key="exists.json", Body=b"{}")
    assert storage.client_object_exists("exists.json") is True


def test_storage_writes_raw_report_to_response_year_key() -> None:
    client = FakeS3Client()
    storage = NorwayFinancialBootstrapStorage(s3_client=client)

    key = storage.write_raw_report(
        org_number="923609016",
        report={
            "id": 5667197,
            "regnskapstype": "SELSKAP",
            "regnskapsperiode": {"tilDato": "2024-12-31"},
        },
    )

    assert key == (
        "norway_brreg/finance/raw_reports/org=923609016/"
        "year=2024/type=SELSKAP/id=5667197.json"
    )
    body = json.loads(client.objects[(storage.bucket, key)].decode("utf-8"))
    assert body["id"] == 5667197


def test_storage_reads_missing_done_failed_marker_status() -> None:
    storage = NorwayFinancialBootstrapStorage(s3_client=FakeS3Client())

    assert storage.candidate_marker_status("923609016") == CandidateMarkerStatus.MISSING


def test_storage_reads_done_marker_before_failed_marker() -> None:
    client = FakeS3Client()
    storage = NorwayFinancialBootstrapStorage(s3_client=client)
    storage.write_done_marker(
        org_number="923609016",
        fetch_status="success",
        report_count=1,
        raw_report_keys=["report.json"],
        completed_at="2026-07-01T18:00:00Z",
    )

    assert storage.candidate_marker_status("923609016") == CandidateMarkerStatus.DONE


def test_storage_writes_failed_marker_with_error_details() -> None:
    client = FakeS3Client()
    storage = NorwayFinancialBootstrapStorage(s3_client=client)

    key = storage.write_failed_marker(
        org_number="923609016",
        error_type="RuntimeError",
        error_message="failed after retries",
        failed_at="2026-07-01T18:00:00Z",
    )

    assert key == "norway_brreg/finance/raw_reports/org=923609016/status/failed.json"
    body = json.loads(client.objects[(storage.bucket, key)].decode("utf-8"))
    assert body["org_number"] == "923609016"
    assert body["error_type"] == "RuntimeError"
```

- [ ] **Step 2: Run storage tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_storage.py -q
```

Expected: FAIL because storage marker APIs do not exist.

- [ ] **Step 3: Implement storage APIs**

Update `storage.py` to remove parquet candidate batch methods and include:

```python
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import boto3
from botocore.exceptions import ClientError

from norway_financial_bootstrap.paths import (
    DEFAULT_BUCKET,
    done_marker_key,
    failed_marker_key,
    raw_report_key,
    report_year_from_report,
)


class CandidateMarkerStatus(StrEnum):
    DONE = "done"
    FAILED = "failed"
    MISSING = "missing"


@dataclass
class NorwayFinancialBootstrapStorage:
    endpoint_url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    bucket: str = field(init=False, default=DEFAULT_BUCKET)
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
            )
        return self.s3_client

    def client_object_exists(self, key: str) -> bool:
        try:
            self.client().head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def candidate_marker_status(self, org_number: str) -> CandidateMarkerStatus:
        if self.client_object_exists(done_marker_key(org_number)):
            return CandidateMarkerStatus.DONE
        if self.client_object_exists(failed_marker_key(org_number)):
            return CandidateMarkerStatus.FAILED
        return CandidateMarkerStatus.MISSING

    def write_raw_report(self, *, org_number: str, report: dict[str, Any]) -> str:
        report_id = _required_report_value(report, "id")
        report_type = _required_report_value(report, "regnskapstype")
        report_year = report_year_from_report(report)
        key = raw_report_key(org_number, report_year, report_type, report_id)
        self.client().put_object(Bucket=self.bucket, Key=key, Body=_json_bytes(report))
        return key

    def write_done_marker(
        self,
        *,
        org_number: str,
        fetch_status: str,
        report_count: int,
        raw_report_keys: list[str],
        completed_at: str,
    ) -> str:
        key = done_marker_key(org_number)
        self.client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=_json_bytes(
                {
                    "org_number": org_number,
                    "fetch_status": fetch_status,
                    "report_count": report_count,
                    "raw_report_keys": raw_report_keys,
                    "completed_at": completed_at,
                }
            ),
        )
        return key

    def write_failed_marker(
        self,
        *,
        org_number: str,
        error_type: str,
        error_message: str,
        failed_at: str,
    ) -> str:
        key = failed_marker_key(org_number)
        self.client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=_json_bytes(
                {
                    "org_number": org_number,
                    "error_type": error_type,
                    "error_message": error_message,
                    "failed_at": failed_at,
                }
            ),
        )
        return key


def _required_report_value(report: dict[str, Any], key: str) -> str:
    value = report.get(key)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"BRREG financial report is missing required field {key!r}")
    return str(value)


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
```

- [ ] **Step 4: Run storage tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_storage.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/norway_financial_bootstrap/storage.py \
  corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_storage.py
git commit -m "feat: add norway financial s3 markers"
```

---

### Task 5: Fetch BRREG Financial Data By Organization Only

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/brreg_client.py`
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/fetch_status.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_brreg_client.py`

- [ ] **Step 1: Write failing BRREG client tests**

Update relevant tests so the client receives `FinancialCandidate("923609016")` and asserts URL has no `år` query parameter:

```python
from norway_financial_bootstrap.brreg_client import BrregFinancialClient
from norway_financial_bootstrap.candidates import FinancialCandidate


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        return self.response


def test_fetch_candidate_calls_org_endpoint_without_year_query() -> None:
    session = FakeSession(
        FakeResponse(
            payload=[
                {
                    "id": 5667197,
                    "regnskapstype": "SELSKAP",
                    "regnskapsperiode": {"tilDato": "2024-12-31"},
                }
            ]
        )
    )
    client = BrregFinancialClient(session=session, sleep=lambda _: None)

    row = client.fetch_candidate(
        FinancialCandidate("923609016"),
        source_run_id="run-1",
        source_line_number=1,
        fetched_at="2026-07-01T18:00:00Z",
    )

    assert session.calls[0][0].endswith("/923609016")
    assert "år=" not in session.calls[0][0]
    assert row["org_number"] == "923609016"
    assert row["fetch_status"] == "success"
```

- [ ] **Step 2: Run BRREG client tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_brreg_client.py -q
```

Expected: FAIL because client still builds `?år=`.

- [ ] **Step 3: Update fetch status base row**

In `fetch_status.py`, make `_base_financial_fetch_row` emit only fields needed by marker/fetch logic:

```python
return {
    "country_iso2": "NO",
    "source_slug": "norway_brregregnskap_fetch",
    "source_run_id": source_run_id,
    "source_line_number": source_line_number,
    "source_record_id": _string(org.get("org_number")),
    "source_payload_hash": source_payload_hash,
    "org_number": _string(org.get("org_number")),
    "source_url": source_url,
    "fetch_status": fetch_status,
    "http_status": http_status,
    "error_type": error_type,
    "error_message": error_message,
    "attempt_count": attempt_count,
    "fetched_at": fetched_at,
    "raw_response": raw_response,
}
```

- [ ] **Step 4: Update BRREG client URL construction**

In `brreg_client.py`:

- Remove `urlencode({"år": ...})`.
- Build source URL as:

```python
source_url = f"{self.base_url}/{candidate.org_number}"
```

- Call:

```python
response = self.session().get(source_url, timeout=self.timeout_seconds)
```

- [ ] **Step 5: Run BRREG client tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_brreg_client.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/norway_financial_bootstrap/brreg_client.py \
  corpscout/norway_financial_bootstrap/norway_financial_bootstrap/fetch_status.py \
  corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_brreg_client.py
git commit -m "refactor: fetch norway financials by organization"
```

---

### Task 6: Replace Batch Activities With Slot Activities

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/activities.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py`

- [ ] **Step 1: Write failing activity tests**

In `test_norway_financial_bootstrap_workflow.py`, replace batch activity tests with tests for these functions:

```python
from norway_financial_bootstrap.activities import (
    CandidateMarkerStatusInput,
    FetchAndStoreCandidateInput,
    GetCandidateInput,
    MarkerWriteInput,
    get_candidate,
    get_candidate_with_dependencies,
    candidate_marker_status_with_dependencies,
    fetch_and_store_candidate_with_dependencies,
    write_done_marker_with_dependencies,
    write_failed_marker_with_dependencies,
)
from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.storage import CandidateMarkerStatus


class FakeClickHouseGateway:
    def __init__(self, candidate=None):
        self.candidate = candidate
        self.calls = []

    def get_candidate(self, run_id, slot_id, slot_index):
        self.calls.append((run_id, slot_id, slot_index))
        return self.candidate


class FakeStorage:
    def __init__(self):
        self.status = CandidateMarkerStatus.MISSING
        self.raw_writes = []
        self.done_markers = []
        self.failed_markers = []

    def candidate_marker_status(self, org_number):
        return self.status

    def write_raw_report(self, *, org_number, report):
        key = f"raw/{org_number}/{report['id']}.json"
        self.raw_writes.append((org_number, report))
        return key

    def write_done_marker(self, **kwargs):
        self.done_markers.append(kwargs)
        return f"raw/{kwargs['org_number']}/status/done.json"

    def write_failed_marker(self, **kwargs):
        self.failed_markers.append(kwargs)
        return f"raw/{kwargs['org_number']}/status/failed.json"


class FakeBrregClient:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def fetch_candidate(self, candidate, source_run_id, source_line_number, fetched_at):
        self.calls.append((candidate, source_run_id, source_line_number, fetched_at))
        return self.row


def test_get_candidate_activity_registers_single_temporal_input_argument() -> None:
    activity_definition = get_candidate.__temporal_activity_definition
    assert len(activity_definition.arg_types) == 1


def test_get_candidate_reads_candidate_by_slot_index() -> None:
    gateway = FakeClickHouseGateway(FinancialCandidate("923609016"))

    candidate = get_candidate_with_dependencies(
        GetCandidateInput(run_id="run-1", slot_id=2, slot_index=9),
        clickhouse=gateway,
    )

    assert candidate == FinancialCandidate("923609016")
    assert gateway.calls == [("run-1", 2, 9)]


def test_marker_status_reads_s3_status() -> None:
    storage = FakeStorage()
    storage.status = CandidateMarkerStatus.DONE

    assert (
        candidate_marker_status_with_dependencies(
            CandidateMarkerStatusInput(org_number="923609016"), storage=storage
        )
        == CandidateMarkerStatus.DONE
    )


def test_fetch_and_store_candidate_writes_raw_reports_and_returns_result() -> None:
    row = {
        "org_number": "923609016",
        "fetch_status": "success",
        "fetched_at": "2026-07-01T18:00:00Z",
        "raw_response": (
            '[{"id":5667197,"regnskapstype":"SELSKAP",'
            '"regnskapsperiode":{"tilDato":"2024-12-31"}}]'
        ),
    }
    storage = FakeStorage()
    client = FakeBrregClient(row)

    result = fetch_and_store_candidate_with_dependencies(
        FetchAndStoreCandidateInput(
            run_id="run-1",
            org_number="923609016",
            fetched_at="2026-07-01T18:00:00Z",
        ),
        storage=storage,
        client=client,
    )

    assert result.org_number == "923609016"
    assert result.fetch_status == "success"
    assert result.report_count == 1
    assert result.raw_report_keys == ["raw/923609016/5667197.json"]
    assert client.calls[0][0] == FinancialCandidate("923609016")


def test_write_done_marker_persists_result() -> None:
    storage = FakeStorage()

    key = write_done_marker_with_dependencies(
        MarkerWriteInput(
            org_number="923609016",
            fetch_status="success",
            report_count=1,
            raw_report_keys=["raw.json"],
            timestamp="2026-07-01T18:00:00Z",
            error_type="",
            error_message="",
        ),
        storage=storage,
    )

    assert key.endswith("/status/done.json")
    assert storage.done_markers[0]["report_count"] == 1


def test_write_failed_marker_persists_error() -> None:
    storage = FakeStorage()

    key = write_failed_marker_with_dependencies(
        MarkerWriteInput(
            org_number="923609016",
            fetch_status="",
            report_count=0,
            raw_report_keys=[],
            timestamp="2026-07-01T18:00:00Z",
            error_type="RuntimeError",
            error_message="failed",
        ),
        storage=storage,
    )

    assert key.endswith("/status/failed.json")
    assert storage.failed_markers[0]["error_type"] == "RuntimeError"
```

- [ ] **Step 2: Run workflow tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_workflow.py -q
```

Expected: FAIL because new activity types/functions do not exist.

- [ ] **Step 3: Implement slot activities**

In `activities.py`, replace batch activity implementation with:

```python
from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from temporalio import activity

from norway_financial_bootstrap import fetch_status as financial_fetches
from norway_financial_bootstrap.brreg_client import BrregFinancialClient
from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.clickhouse import clickhouse_from_env, get_candidate_from_clickhouse
from norway_financial_bootstrap.storage import (
    CandidateMarkerStatus,
    NorwayFinancialBootstrapStorage,
)

HEARTBEAT_SLEEP_INTERVAL_SECONDS = 60


@dataclass(frozen=True)
class GetCandidateInput:
    run_id: str
    slot_id: int
    slot_index: int


@dataclass(frozen=True)
class CandidateMarkerStatusInput:
    org_number: str


@dataclass(frozen=True)
class FetchAndStoreCandidateInput:
    run_id: str
    org_number: str
    fetched_at: str


@dataclass(frozen=True)
class FetchAndStoreCandidateResult:
    org_number: str
    fetch_status: str
    report_count: int
    raw_report_keys: list[str]
    fetched_at: str


@dataclass(frozen=True)
class MarkerWriteInput:
    org_number: str
    fetch_status: str
    report_count: int
    raw_report_keys: list[str]
    timestamp: str
    error_type: str
    error_message: str


def storage_from_env(*, endpoint_url: str | None = None) -> NorwayFinancialBootstrapStorage:
    return NorwayFinancialBootstrapStorage(
        endpoint_url=endpoint_url or os.environ.get("CORPSCOUT_S3_ENDPOINT"),
        access_key=os.environ.get("CORPSCOUT_S3_ACCESS_KEY"),
        secret_key=os.environ.get("CORPSCOUT_S3_SECRET_KEY"),
    )


@activity.defn
def get_candidate(input: GetCandidateInput) -> FinancialCandidate | None:
    return get_candidate_with_dependencies(input, clickhouse=clickhouse_from_env())


def get_candidate_with_dependencies(input: GetCandidateInput, *, clickhouse: Any):
    return get_candidate_from_clickhouse(
        clickhouse,
        run_id=input.run_id,
        slot_id=input.slot_id,
        slot_index=input.slot_index,
    )


@activity.defn
def candidate_marker_status(input: CandidateMarkerStatusInput) -> CandidateMarkerStatus:
    return candidate_marker_status_with_dependencies(input, storage=storage_from_env())


def candidate_marker_status_with_dependencies(
    input: CandidateMarkerStatusInput, *, storage: Any
) -> CandidateMarkerStatus:
    return storage.candidate_marker_status(input.org_number)


@activity.defn
def fetch_and_store_candidate(
    input: FetchAndStoreCandidateInput,
) -> FetchAndStoreCandidateResult:
    return fetch_and_store_candidate_with_dependencies(
        input,
        storage=storage_from_env(),
        client=BrregFinancialClient(sleep=_heartbeating_sleep),
    )


def fetch_and_store_candidate_with_dependencies(
    input: FetchAndStoreCandidateInput,
    *,
    storage: Any,
    client: BrregFinancialClient,
) -> FetchAndStoreCandidateResult:
    candidate = FinancialCandidate(input.org_number)
    row = client.fetch_candidate(
        candidate,
        source_run_id=input.run_id,
        source_line_number=1,
        fetched_at=input.fetched_at,
    )
    status = str(row.get("fetch_status") or "unknown")
    if financial_fetches.financial_fetch_status_requires_failure(status):
        raise RuntimeError(f"BRREG financial fetch failed for {input.org_number}: {status}")

    raw_report_keys: list[str] = []
    if status == financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS:
        for report in _reports_from_success_row(row):
            raw_report_keys.append(
                storage.write_raw_report(org_number=input.org_number, report=report)
            )

    return FetchAndStoreCandidateResult(
        org_number=input.org_number,
        fetch_status=status,
        report_count=len(raw_report_keys),
        raw_report_keys=raw_report_keys,
        fetched_at=input.fetched_at,
    )


@activity.defn
def write_done_marker(input: MarkerWriteInput) -> str:
    return write_done_marker_with_dependencies(input, storage=storage_from_env())


def write_done_marker_with_dependencies(input: MarkerWriteInput, *, storage: Any) -> str:
    return storage.write_done_marker(
        org_number=input.org_number,
        fetch_status=input.fetch_status,
        report_count=input.report_count,
        raw_report_keys=input.raw_report_keys,
        completed_at=input.timestamp,
    )


@activity.defn
def write_failed_marker(input: MarkerWriteInput) -> str:
    return write_failed_marker_with_dependencies(input, storage=storage_from_env())


def write_failed_marker_with_dependencies(input: MarkerWriteInput, *, storage: Any) -> str:
    return storage.write_failed_marker(
        org_number=input.org_number,
        error_type=input.error_type,
        error_message=input.error_message,
        failed_at=input.timestamp,
    )


def _reports_from_success_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    payload = json.loads(str(row.get("raw_response") or "[]"))
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise RuntimeError("BRREG financial success row does not contain a list of reports")
    return payload


def _heartbeating_sleep(
    total_seconds: float,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    remaining_seconds = max(float(total_seconds), 0.0)
    _safe_heartbeat({"retry_sleep_seconds_remaining": remaining_seconds})
    while remaining_seconds > 0:
        sleep_seconds = min(HEARTBEAT_SLEEP_INTERVAL_SECONDS, remaining_seconds)
        sleep(sleep_seconds)
        remaining_seconds = max(remaining_seconds - sleep_seconds, 0.0)
        _safe_heartbeat({"retry_sleep_seconds_remaining": remaining_seconds})


def _safe_heartbeat(details: Any) -> None:
    try:
        activity.heartbeat(details)
    except RuntimeError:
        pass
```

- [ ] **Step 4: Run activity tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_workflow.py -q
```

Expected: Some workflow-specific tests may still fail because `workflows.py` still uses batch workflow. Continue to Task 7 before expecting full file pass.

- [ ] **Step 5: Commit**

Commit only if activity tests pass or remaining failures are explicitly workflow registration/continue-as-new failures addressed in Task 7:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/norway_financial_bootstrap/activities.py \
  corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py
git commit -m "refactor: add norway financial slot activities"
```

---

### Task 7: Implement Temporal Slot Workflow

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/workflows.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py`

- [ ] **Step 1: Add workflow unit tests**

Add tests that use monkeypatching to simulate workflow execution logic without external services. Required assertions:

```python
from norway_financial_bootstrap.workflows import (
    DEFAULT_SLOT_COUNT,
    BootstrapSlotInput,
    NorwayBrregFinancialBootstrapSlotWorkflow,
    slot_workflow_id,
)


def test_slot_workflow_id_is_stable_per_slot() -> None:
    assert slot_workflow_id("norway-brreg-finance-historical-bootstrap", 2) == (
        "norway-brreg-finance-historical-bootstrap-slot-2"
    )


def test_default_slot_count_is_four() -> None:
    assert DEFAULT_SLOT_COUNT == 4


def test_bootstrap_slot_input_starts_at_index_zero() -> None:
    assert BootstrapSlotInput(
        run_id="run-1",
        slot_id=0,
        slot_index=0,
        slot_count=4,
        fetched_at="2026-07-01T18:00:00Z",
    ).slot_index == 0
```

For deeper Temporal behavior, use the existing Temporal workflow test environment if present in the file. The test should verify:

- candidate missing completes without continue-as-new.
- done marker causes continue-as-new with `slot_index + 1`.
- missing marker causes fetch, done marker write, then continue-as-new.
- fetch activity failure after retries writes failed marker, then continue-as-new.

- [ ] **Step 2: Run workflow tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_workflow.py -q
```

Expected: FAIL because slot workflow classes do not exist.

- [ ] **Step 3: Implement workflow**

Replace `workflows.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

DEFAULT_TEMPORAL_ADDRESS = "companycollect:7233"
DEFAULT_TASK_QUEUE = "norway-financial-bootstrap"
DEFAULT_SLOT_COUNT = 4

ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=10)
FETCH_START_TO_CLOSE_TIMEOUT = timedelta(hours=12)
FETCH_HEARTBEAT_TIMEOUT = timedelta(minutes=5)
FETCH_RETRY_POLICY = RetryPolicy(maximum_attempts=3)

with workflow.unsafe.imports_passed_through():
    from norway_financial_bootstrap.activities import (
        CandidateMarkerStatusInput,
        FetchAndStoreCandidateInput,
        GetCandidateInput,
        MarkerWriteInput,
        candidate_marker_status,
        fetch_and_store_candidate,
        get_candidate,
        write_done_marker,
        write_failed_marker,
    )
    from norway_financial_bootstrap.storage import CandidateMarkerStatus


@dataclass(frozen=True)
class BootstrapSlotInput:
    run_id: str
    slot_id: int
    slot_index: int
    slot_count: int
    fetched_at: str


@dataclass(frozen=True)
class BootstrapSlotResult:
    run_id: str
    slot_id: int
    slot_index: int
    completed: bool
    org_number: str | None


def slot_workflow_id(run_id: str, slot_id: int) -> str:
    return f"{run_id}-slot-{slot_id}"


@workflow.defn
class NorwayBrregFinancialBootstrapSlotWorkflow:
    @workflow.run
    async def run(self, input: BootstrapSlotInput) -> BootstrapSlotResult:
        candidate = await workflow.execute_activity(
            get_candidate,
            GetCandidateInput(
                run_id=input.run_id,
                slot_id=input.slot_id,
                slot_index=input.slot_index,
            ),
            start_to_close_timeout=ACTIVITY_START_TO_CLOSE_TIMEOUT,
        )
        if candidate is None:
            return BootstrapSlotResult(
                run_id=input.run_id,
                slot_id=input.slot_id,
                slot_index=input.slot_index,
                completed=True,
                org_number=None,
            )

        marker_status = await workflow.execute_activity(
            candidate_marker_status,
            CandidateMarkerStatusInput(org_number=candidate.org_number),
            start_to_close_timeout=ACTIVITY_START_TO_CLOSE_TIMEOUT,
        )
        if marker_status in {CandidateMarkerStatus.DONE, CandidateMarkerStatus.FAILED}:
            workflow.continue_as_new(_next_input(input))

        try:
            result = await workflow.execute_activity(
                fetch_and_store_candidate,
                FetchAndStoreCandidateInput(
                    run_id=input.run_id,
                    org_number=candidate.org_number,
                    fetched_at=input.fetched_at,
                ),
                heartbeat_timeout=FETCH_HEARTBEAT_TIMEOUT,
                start_to_close_timeout=FETCH_START_TO_CLOSE_TIMEOUT,
                retry_policy=FETCH_RETRY_POLICY,
            )
        except ActivityError as exc:
            await workflow.execute_activity(
                write_failed_marker,
                MarkerWriteInput(
                    org_number=candidate.org_number,
                    fetch_status="",
                    report_count=0,
                    raw_report_keys=[],
                    timestamp=input.fetched_at,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                ),
                start_to_close_timeout=ACTIVITY_START_TO_CLOSE_TIMEOUT,
            )
            workflow.continue_as_new(_next_input(input))

        await workflow.execute_activity(
            write_done_marker,
            MarkerWriteInput(
                org_number=result.org_number,
                fetch_status=result.fetch_status,
                report_count=result.report_count,
                raw_report_keys=result.raw_report_keys,
                timestamp=result.fetched_at,
                error_type="",
                error_message="",
            ),
            start_to_close_timeout=ACTIVITY_START_TO_CLOSE_TIMEOUT,
        )
        workflow.continue_as_new(_next_input(input))


def _next_input(input: BootstrapSlotInput) -> BootstrapSlotInput:
    return BootstrapSlotInput(
        run_id=input.run_id,
        slot_id=input.slot_id,
        slot_index=input.slot_index + 1,
        slot_count=input.slot_count,
        fetched_at=input.fetched_at,
    )
```

- [ ] **Step 4: Run workflow tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_workflow.py -q
```

Expected: PASS after old batch workflow tests are removed/replaced.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/norway_financial_bootstrap/workflows.py \
  corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py
git commit -m "feat: add norway financial slot workflow"
```

---

### Task 8: Update CLI To Prepare Candidate Table And Start Slot Workflows

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/cli.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py`

- [ ] **Step 1: Write failing CLI tests**

Add tests asserting:

```python
from norway_financial_bootstrap.cli import FIXED_WORKFLOW_ID, start_slot_workflows
from norway_financial_bootstrap.workflows import DEFAULT_SLOT_COUNT


class FakeTemporalClient:
    def __init__(self):
        self.started = []

    async def start_workflow(self, workflow, input, id, task_queue):
        self.started.append((workflow, input, id, task_queue))
        class Handle:
            pass
        handle = Handle()
        handle.id = id
        return handle


def test_start_slot_workflows_starts_one_workflow_per_slot() -> None:
    client = FakeTemporalClient()

    import asyncio

    ids = asyncio.run(
        start_slot_workflows(
            client=client,
            run_id=FIXED_WORKFLOW_ID,
            fetched_at="2026-07-01T18:00:00Z",
            slot_count=DEFAULT_SLOT_COUNT,
            task_queue="test-queue",
        )
    )

    assert ids == [
        "norway-brreg-finance-historical-bootstrap-slot-0",
        "norway-brreg-finance-historical-bootstrap-slot-1",
        "norway-brreg-finance-historical-bootstrap-slot-2",
        "norway-brreg-finance-historical-bootstrap-slot-3",
    ]
    assert [call[1].slot_index for call in client.started] == [0, 0, 0, 0]
```

- [ ] **Step 2: Run CLI tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_workflow.py -q
```

Expected: FAIL because `start_slot_workflows` does not exist.

- [ ] **Step 3: Implement CLI starter**

Update `cli.py` to:

- Remove `write_candidate_batches`.
- Remove loading all candidates into Python.
- Call `prepare_candidate_table(clickhouse_from_env(), run_id=FIXED_WORKFLOW_ID, slot_count=DEFAULT_SLOT_COUNT)` as normal startup code before any Temporal workflow is started.
- Do not register or execute candidate table preparation as a Temporal activity.
- Connect Temporal once.
- Start slot workflows with ids from `slot_workflow_id`.

Core helper:

```python
async def start_slot_workflows(
    *,
    client: Client,
    run_id: str,
    fetched_at: str,
    slot_count: int,
    task_queue: str,
) -> list[str]:
    workflow_ids: list[str] = []
    for slot_id in range(slot_count):
        workflow_id = slot_workflow_id(run_id, slot_id)
        handle = await client.start_workflow(
            NorwayBrregFinancialBootstrapSlotWorkflow.run,
            BootstrapSlotInput(
                run_id=run_id,
                slot_id=slot_id,
                slot_index=0,
                slot_count=slot_count,
                fetched_at=fetched_at,
            ),
            id=workflow_id,
            task_queue=task_queue,
        )
        workflow_ids.append(handle.id)
    return workflow_ids
```

In `main`, print one workflow id per line.

- [ ] **Step 4: Run CLI/workflow tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_workflow.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/norway_financial_bootstrap/cli.py \
  corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py
git commit -m "refactor: start norway financial slot workflows"
```

---

### Task 9: Update Worker Registration

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/worker.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py`
- Modify: `corpscout/norway_financial_bootstrap/tests/test_package_independence.py`

- [ ] **Step 1: Write failing worker registration test**

Update the existing worker registration test to assert:

```python
from norway_financial_bootstrap.activities import (
    candidate_marker_status,
    fetch_and_store_candidate,
    get_candidate,
    write_done_marker,
    write_failed_marker,
)
from norway_financial_bootstrap.workflows import NorwayBrregFinancialBootstrapSlotWorkflow


def test_worker_registers_slot_workflow_and_activities(monkeypatch) -> None:
    import norway_financial_bootstrap.worker as worker

    record = {}

    class FakeWorker:
        def __init__(self, client, task_queue, workflows, activities, max_concurrent_activities):
            record["task_queue"] = task_queue
            record["workflows"] = workflows
            record["activities"] = activities
            record["max_concurrent_activities"] = max_concurrent_activities

    monkeypatch.setattr(worker, "Worker", FakeWorker)

    worker.build_worker(object(), task_queue="norway-financial-test", max_workers=3)

    assert record["workflows"] == [NorwayBrregFinancialBootstrapSlotWorkflow]
    assert set(record["activities"]) == {
        get_candidate,
        candidate_marker_status,
        fetch_and_store_candidate,
        write_done_marker,
        write_failed_marker,
    }
    assert record["max_concurrent_activities"] == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_workflow.py tests/test_package_independence.py -q
```

Expected: FAIL because worker still registers batch workflow/activity names.

- [ ] **Step 3: Update worker registration**

In `worker.py`, register:

```python
from norway_financial_bootstrap.activities import (
    candidate_marker_status,
    fetch_and_store_candidate,
    get_candidate,
    write_done_marker,
    write_failed_marker,
)
from norway_financial_bootstrap.workflows import (
    DEFAULT_TASK_QUEUE,
    DEFAULT_TEMPORAL_ADDRESS,
    NorwayBrregFinancialBootstrapSlotWorkflow,
)
```

and:

```python
Worker(
    client,
    task_queue=task_queue,
    workflows=[NorwayBrregFinancialBootstrapSlotWorkflow],
    activities=[
        get_candidate,
        candidate_marker_status,
        fetch_and_store_candidate,
        write_done_marker,
        write_failed_marker,
    ],
    max_concurrent_activities=max_workers,
)
```

- [ ] **Step 4: Run worker tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_norway_financial_bootstrap_workflow.py tests/test_package_independence.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/norway_financial_bootstrap/worker.py \
  corpscout/norway_financial_bootstrap/tests/test_norway_financial_bootstrap_workflow.py \
  corpscout/norway_financial_bootstrap/tests/test_package_independence.py
git commit -m "refactor: register norway financial slot worker"
```

---

### Task 10: Update README And Remove Batch Runtime Language

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/README.md`

- [ ] **Step 1: Update README behavior description**

Replace any batch/parquet candidate language with:

```markdown
## Runtime Model

The starter creates or reuses a frozen ClickHouse candidate table:

```text
corpscout.norway_financial_bootstrap_candidates
```

Candidates are deterministic:

```text
run_id + slot + slot_index -> org_number
```

The starter launches four Temporal slot workflows by default:

```text
norway-brreg-finance-historical-bootstrap-slot-0
norway-brreg-finance-historical-bootstrap-slot-1
norway-brreg-finance-historical-bootstrap-slot-2
norway-brreg-finance-historical-bootstrap-slot-3
```

Each workflow run processes one organization, then continues as new with the next slot index. Restarting from index `0` is safe because the workflow checks S3 status markers before calling BRREG.
```

Add marker docs:

```markdown
## S3 Layout

Raw reports:

```text
source-norway-brreg/norway_brreg/finance/raw_reports/org=<org>/year=<year>/type=<type>/id=<id>.json
```

Status markers:

```text
source-norway-brreg/norway_brreg/finance/raw_reports/org=<org>/status/done.json
source-norway-brreg/norway_brreg/finance/raw_reports/org=<org>/status/failed.json
```
```

- [ ] **Step 2: Commit README**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap/README.md
git commit -m "docs: document norway financial slot bootstrap"
```

---

### Task 11: Full Verification And Cleanup

**Files:**
- Review all files under `corpscout/norway_financial_bootstrap/`

- [ ] **Step 1: Remove stale batch symbols**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
rg -n "batch|candidate_batch|batch_keys|batch_count|read_candidate_batch|write_candidate_batch|existing_raw_report_ids|accounts_year|last_submitted_accounts_year" norway_financial_bootstrap tests README.md
```

Expected allowed hits:

- `last_submitted_accounts_year` only in ClickHouse source filtering tests/query.
- `batch` only if referring to unrelated pytest wording; prefer removing all bootstrap batch language.
- No `candidate_batch`, `batch_keys`, `batch_count`, `read_candidate_batch`, `write_candidate_batch`, or `existing_raw_report_ids`.

- [ ] **Step 2: Run all package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest -q
```

Expected: PASS.

- [ ] **Step 3: Run lint**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run ruff check .
```

Expected: PASS.

- [ ] **Step 4: Check package import independence**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/norway_financial_bootstrap
uv run pytest tests/test_package_independence.py -q
```

Expected: PASS and no Dagster imports.

- [ ] **Step 5: Check git status**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
```

Expected: no uncommitted implementation changes except ignored local files such as `.env`, `.venv`, `.pytest_cache`, `.ruff_cache`, or intentionally untracked logs.

- [ ] **Step 6: Final commit if needed**

If verification fixes were made:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/norway_financial_bootstrap
git commit -m "test: verify norway financial slot bootstrap"
```

---

## Cleanup Command For Old Candidate Shards

After the new code is deployed and confirmed, delete old candidate batch leftovers only. Do not delete raw reports.

For MinIO/RustFS via AWS CLI-compatible endpoint:

```bash
aws --endpoint-url "$CORPSCOUT_S3_ENDPOINT" s3 rm \
  s3://source-norway-brreg/norway_brreg/finance/bootstrap_runs/run=norway-brreg-finance-historical-bootstrap/ \
  --recursive
```

Do not run this until the new slot-marker workflow is ready.

---

## Important Boundary

Candidate table preparation is startup work, not Temporal work.

The service startup sequence is:

```text
start process
  -> load environment
  -> connect ClickHouse
  -> create/reuse corpscout.norway_financial_bootstrap_candidates
  -> connect Temporal
  -> start slot workflows from index 0
```

Temporal workflow history should only contain per-organization processing:

```text
get_candidate
candidate_marker_status
fetch_and_store_candidate
write_done_marker or write_failed_marker
continue-as-new
```

No workflow or activity should perform full candidate table bootstrap.

---

## Plan Self-Review

Spec coverage:

- Frozen ClickHouse candidate table: Task 2 and Task 8 startup call.
- Org-only candidate source: Tasks 1 and 2.
- No S3 candidate shards: Tasks 3, 8, 11.
- S3 raw report + colocated markers: Tasks 3 and 4.
- Temporal slot workflow with continue-as-new: Tasks 7 and 8.
- Worker registration: Task 9.
- BRREG fetch without `år`: Task 5.
- Tests and verification: Tasks 1-11.

Placeholder scan: no `TBD`, `TODO`, or unspecified implementation steps remain.

Type consistency:

- `FinancialCandidate(org_number)` is used throughout.
- Workflow input uses `run_id`, `slot_id`, `slot_index`, `slot_count`, `fetched_at`.
- S3 status APIs use `done_marker_key`, `failed_marker_key`, and `candidate_marker_status`.
