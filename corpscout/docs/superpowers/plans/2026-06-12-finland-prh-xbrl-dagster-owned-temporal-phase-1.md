# Finland PRH XBRL Dagster-Owned Temporal Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first Dagster-owned Temporal workflow for Finland PRH XBRL financial statements: pull XML files for one company/date window to RustFS, then let Dagster process completed Temporal runs into ClickHouse.

**Architecture:** The new Temporal workflow, activities, and worker live inside `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl`, because this is a data-pipeline source responsibility, not a scheduler/UI source-action responsibility. Temporal stores workflow state and retries; RustFS stores raw XML; Dagster starts workflows and sensors completed Temporal runs; dlt parses S3 XML objects and writes the existing ClickHouse XBRL document/content tables. There is no Postgres source-run/file-run ledger and no manifest file in this phase.

**Tech Stack:** Python 3.12, Dagster, Temporal Python SDK (`temporalio`), boto3/RustFS, requests, lxml, dlt ClickHouse destination, ClickHouse.

---

## File Structure

Cleanup from the interrupted Go attempt:

- Delete if present: `corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull.go`
- Delete if present: `corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull_test.go`
- Delete if present: `corpscout/scheduler/internal/temporal/workflow/companysources/prh_xbrl_company_pull.go`
- Revert only PRH XBRL company-pull hunks from:
  - `corpscout/scheduler/internal/app/temporal.go`
  - `corpscout/scheduler/internal/companysources/finland/prhxbrl/client.go`
  - `corpscout/scheduler/internal/httpapi/workflow_triggers_test.go`
  - `corpscout/scheduler/internal/temporal/actions/companysources/actions.go`
  - `corpscout/scheduler/internal/temporal/workflow/companysources/workflow_test.go`

Dagster package files:

- Modify: `corpscout/dagster/pyproject.toml`
- Modify: `corpscout/dagster/.env.example`
- Modify: `corpscout/dagster/docker-compose.yml`
- Modify: `corpscout/dagster/dagster_corpscout/definitions.py`
- Modify: `corpscout/dagster/dagster_corpscout/registry.py`
- Modify: `corpscout/dagster/dagster_corpscout/source_bundle.py`
- Create: `corpscout/dagster/dagster_corpscout/resources/temporal.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/__init__.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/spec.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/client.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/storage.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_models.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_activities.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_workflows.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_worker.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/dlt_pipeline.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/xbrl_parser.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/__init__.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/external.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/jobs.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/schedules.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/sensors.py`

Tests:

- Create: `corpscout/dagster/tests/test_finland_prh_xbrl_models.py`
- Create: `corpscout/dagster/tests/test_finland_prh_xbrl_client.py`
- Create: `corpscout/dagster/tests/test_finland_prh_xbrl_storage.py`
- Create: `corpscout/dagster/tests/test_finland_prh_xbrl_temporal_workflow.py`
- Create: `corpscout/dagster/tests/test_finland_prh_xbrl_dlt_pipeline.py`
- Create: `corpscout/dagster/tests/test_finland_prh_xbrl_sensor.py`
- Modify: `corpscout/dagster/tests/test_definitions.py`
- Modify: `corpscout/dagster/tests/test_source_conventions.py`

---

### Task 0: Remove the Aborted Go Workflow Attempt

**Files:**
- Delete: `corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull.go`
- Delete: `corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull_test.go`
- Delete: `corpscout/scheduler/internal/temporal/workflow/companysources/prh_xbrl_company_pull.go`
- Modify: Go files listed in the File Structure cleanup section

- [ ] **Step 1: Confirm the unwanted Go symbols exist before cleanup**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n 'FinlandPRHXBRLCompanyPull|PullCompanyStatementsToS3|buildCompanyFinancialsURL' corpscout/scheduler/internal
```

Expected before cleanup: matches exist only from the interrupted implementation. If there are no matches, skip to Step 4.

- [ ] **Step 2: Delete the untracked Go files from the aborted implementation**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rm -f corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull.go
rm -f corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull_test.go
rm -f corpscout/scheduler/internal/temporal/workflow/companysources/prh_xbrl_company_pull.go
```

Expected: the three files no longer exist.

- [ ] **Step 3: Remove the interrupted Go hunks**

Edit these files and remove only the PRH XBRL company-pull additions:

```text
corpscout/scheduler/internal/app/temporal.go
corpscout/scheduler/internal/companysources/finland/prhxbrl/client.go
corpscout/scheduler/internal/httpapi/workflow_triggers_test.go
corpscout/scheduler/internal/temporal/actions/companysources/actions.go
corpscout/scheduler/internal/temporal/workflow/companysources/workflow_test.go
```

Concrete removals:

```text
- `FinlandPRHXBRLCompanyPull` workflow registration
- `FinlandPRHXBRLCompanyPullActivity` activity registration
- `s3 *s3client.Client` field added to existing Go company-source `Actions`
- `FinlandPRHXBRLCompanyPullActivity` method
- `buildCompanyFinancialsURL`
- `downloadCompanyFinancialsPage`
- HTTP API test for `/api/v1/workflows/finland/prh-xbrl/company-pull`
- workflow test `TestFinlandPRHXBRLCompanyPullRunsActivityWithWorkflowIdentity`
```

- [ ] **Step 4: Verify the cleanup**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n 'FinlandPRHXBRLCompanyPull|PullCompanyStatementsToS3|buildCompanyFinancialsURL' corpscout/scheduler/internal
```

Expected: no output.

- [ ] **Step 5: Run focused Go tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/temporal/workflow/companysources ./internal/temporal/actions/companysources ./internal/app ./internal/httpapi -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit cleanup only if the aborted Go files were present**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/app/temporal.go \
  corpscout/scheduler/internal/companysources/finland/prhxbrl/client.go \
  corpscout/scheduler/internal/httpapi/workflow_triggers_test.go \
  corpscout/scheduler/internal/temporal/actions/companysources/actions.go \
  corpscout/scheduler/internal/temporal/workflow/companysources/workflow_test.go
git add -u corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull.go \
  corpscout/scheduler/internal/companysources/finland/prhxbrl/company_pull_test.go \
  corpscout/scheduler/internal/temporal/workflow/companysources/prh_xbrl_company_pull.go
git commit -m "Remove aborted Go PRH XBRL workflow attempt"
```

Expected: only the cleanup is committed. Existing Dagster grouping edits and `.env.bak` are not staged by this task.

---

### Task 1: Add Dagster Source Package Skeleton and Dependencies

**Files:**
- Modify: `corpscout/dagster/pyproject.toml`
- Modify: `corpscout/dagster/.env.example`
- Modify: `corpscout/dagster/docker-compose.yml`
- Modify: `corpscout/dagster/dagster_corpscout/source_bundle.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/spec.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/__init__.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/__init__.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/external.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/jobs.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/schedules.py`
- Modify: `corpscout/dagster/dagster_corpscout/registry.py`
- Test: `corpscout/dagster/tests/test_source_conventions.py`
- Test: `corpscout/dagster/tests/test_definitions.py`

- [ ] **Step 1: Write failing source registration tests**

Append to `corpscout/dagster/tests/test_definitions.py`:

```python
def prh_xbrl_key(name: str) -> dg.AssetKey:
    return dg.AssetKey(["sources", "finland", "prh_xbrl", name])


def test_definitions_include_finland_prh_xbrl_assets():
    from dagster_corpscout.definitions import defs

    assert defs.get_assets_def(prh_xbrl_key("source_system")) is not None
    assert defs.get_assets_def(prh_xbrl_key("raw_xml_documents")) is not None
```

Expected: this fails until the source package is registered.

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_definitions.py::test_definitions_include_finland_prh_xbrl_assets -v
```

Expected: FAIL because `raw_xml_documents` is not registered.

- [ ] **Step 3: Add Python dependencies**

In `corpscout/dagster/pyproject.toml`, add these dependencies to `[project].dependencies`:

```toml
    "temporalio>=1.11",
    "lxml>=5.2",
    "dlt[clickhouse]>=1.0",
```

Keep existing dependencies.

- [ ] **Step 4: Add environment variables**

In `corpscout/dagster/.env.example`, add:

```dotenv
TEMPORAL_TARGET_HOST=companycollect:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE_PRH_XBRL=finland-prh-xbrl
FINLAND_PRH_XBRL_BUCKET=source-finland-prh-xbrl
FINLAND_PRH_XBRL_BASE_URL=https://avoindata.prh.fi/opendata-xbrl-api/v3
```

- [ ] **Step 5: Add source metadata**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/spec.py`:

```python
"""Declarative source config for Finland PRH XBRL financial statements."""

SOURCE_NAME = "finland_prh_xbrl"
COUNTRY = "finland"
SOURCE_SLUG = "prh_xbrl"
DISPLAY_NAME = "Finland PRH XBRL"
ASSET_KEY_PREFIX = ["sources", COUNTRY, SOURCE_SLUG]
GROUP_NAME = f"source_{COUNTRY}_{SOURCE_SLUG}"
TAGS = {
    "country": COUNTRY,
    "source": SOURCE_SLUG,
    "source_name": SOURCE_NAME,
}

BUCKET = "source-finland-prh-xbrl"
BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
TASK_QUEUE = "finland-prh-xbrl"
WORKFLOW_NAME = "FinlandPRHXBRLCompanyPullWorkflow"
```

- [ ] **Step 6: Add placeholder assets for registration**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/external.py`:

```python
import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl import spec

source_system = dg.AssetSpec(
    key=[*spec.ASSET_KEY_PREFIX, "source_system"],
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "external"},
    description="Finland PRH XBRL financial statement API.",
)
```

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py`:

```python
import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl import spec


@dg.asset(
    key=[*spec.ASSET_KEY_PREFIX, "raw_xml_documents"],
    deps=[dg.AssetKey([*spec.ASSET_KEY_PREFIX, "source_system"])],
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "raw"},
)
def raw_xml_documents() -> dg.MaterializeResult:
    return dg.MaterializeResult(metadata={"status": "materialized by sensor after Temporal completion"})
```

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/__init__.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl.assets.external import source_system
from dagster_corpscout.sources.finland.prh_xbrl.assets.raw import raw_xml_documents

__all__ = ["source_system", "raw_xml_documents"]
```

- [ ] **Step 7: Add jobs and schedules stubs**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/jobs.py`:

```python
import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl.assets import raw_xml_documents

process_completed_pull_job = dg.define_asset_job(
    name="finland_prh_xbrl_process_completed_pull",
    selection=[raw_xml_documents],
)
```

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/schedules.py`:

```python
schedules = ()
```

- [ ] **Step 8: Register the source bundle**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/__init__.py`:

```python
from dagster_corpscout.source_bundle import SourceBundle
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets import raw_xml_documents, source_system
from dagster_corpscout.sources.finland.prh_xbrl.jobs import process_completed_pull_job
from dagster_corpscout.sources.finland.prh_xbrl.schedules import schedules

source_bundle = SourceBundle(
    source_name=spec.SOURCE_NAME,
    asset_key_prefix=tuple(spec.ASSET_KEY_PREFIX),
    assets=(source_system, raw_xml_documents),
    jobs=(process_completed_pull_job,),
    schedules=schedules,
)

__all__ = ["source_bundle"]
```

Modify `corpscout/dagster/dagster_corpscout/registry.py`:

```python
from dagster_corpscout.sources.finland.prh_ytj import source_bundle as finland_prh_ytj
from dagster_corpscout.sources.finland.prh_xbrl import source_bundle as finland_prh_xbrl

source_modules = (
    "dagster_corpscout.sources.finland.prh_ytj",
    "dagster_corpscout.sources.finland.prh_xbrl",
)
source_bundles = [finland_prh_ytj, finland_prh_xbrl]
```

Keep the existing `all_assets`, `all_asset_checks`, `all_jobs`, and `all_schedules` comprehensions.

- [ ] **Step 9: Extend `SourceBundle` with sensors**

Modify `corpscout/dagster/dagster_corpscout/source_bundle.py`:

```python
@dataclass(frozen=True)
class SourceBundle:
    source_name: str
    asset_key_prefix: tuple[str, ...]
    assets: tuple[Any, ...]
    asset_checks: tuple[Any, ...] = ()
    jobs: tuple[Any, ...] = ()
    schedules: tuple[Any, ...] = ()
    sensors: tuple[Any, ...] = ()
```

Modify `corpscout/dagster/dagster_corpscout/registry.py`:

```python
all_sensors = [sensor for bundle in source_bundles for sensor in bundle.sensors]
```

Modify `corpscout/dagster/dagster_corpscout/definitions.py`:

```python
from dagster_corpscout.registry import all_asset_checks, all_assets, all_jobs, all_schedules, all_sensors
from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource

defs = dg.Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
    resources={
        "rustfs": RustFSResource(
            endpoint_url=dg.EnvVar("CORPSCOUT_S3_ENDPOINT"),
            access_key=dg.EnvVar("CORPSCOUT_S3_ACCESS_KEY"),
            secret_key=dg.EnvVar("CORPSCOUT_S3_SECRET_KEY"),
        ),
        "clickhouse": ClickHouseResource(
            host=dg.EnvVar("CLICKHOUSE_HOST"),
            port=dg.EnvVar("CLICKHOUSE_PORT"),
            username=dg.EnvVar("CLICKHOUSE_USER"),
            password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
            database=dg.EnvVar("CLICKHOUSE_DATABASE"),
            secure=dg.EnvVar("CLICKHOUSE_SECURE"),
        ),
    },
)
```

- [ ] **Step 10: Add a Temporal worker service**

Modify `corpscout/dagster/docker-compose.yml`:

```yaml
  finland-prh-xbrl-temporal-worker:
    image: dagster-corpscout:latest
    restart: unless-stopped
    env_file: .env
    extra_hosts:
      - "companycollect:100.85.212.113"
    command: python -m dagster_corpscout.sources.finland.prh_xbrl.temporal_worker
    depends_on: [dagster-code]
    networks: [dagster-corpscout]
```

- [ ] **Step 11: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_definitions.py tests/test_source_conventions.py -v
```

Expected: PASS.

- [ ] **Step 12: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/pyproject.toml \
  corpscout/dagster/.env.example \
  corpscout/dagster/docker-compose.yml \
  corpscout/dagster/dagster_corpscout/source_bundle.py \
  corpscout/dagster/dagster_corpscout/definitions.py \
  corpscout/dagster/dagster_corpscout/registry.py \
  corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl \
  corpscout/dagster/tests/test_definitions.py \
  corpscout/dagster/tests/test_source_conventions.py
git commit -m "Add Finland PRH XBRL Dagster source package"
```

---

### Task 2: Add Pull Models and Object Key Rules

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_models.py`
- Test: `corpscout/dagster/tests/test_finland_prh_xbrl_models.py`

- [ ] **Step 1: Write failing model tests**

Create `corpscout/dagster/tests/test_finland_prh_xbrl_models.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl.temporal_models import (
    CompanyPullInput,
    object_prefix_for_pull,
)


def test_object_prefix_hashes_company_and_date_window_without_exposing_business_id():
    prefix = object_prefix_for_pull(
        business_id="0176460-0",
        financial_date_start="2021-01-01",
        financial_date_end="2025-12-31",
        temporal_run_id="run-abc",
    )

    assert prefix.startswith("companies/")
    assert "0176460-0" not in prefix
    assert "financial_date_start=2021-01-01" in prefix
    assert "financial_date_end=2025-12-31" in prefix
    assert prefix.endswith("/run=run-abc")


def test_company_pull_input_requires_date_window_order():
    try:
        CompanyPullInput(
            business_id="0176460-0",
            financial_date_start="2025-12-31",
            financial_date_end="2021-01-01",
        ).validated()
    except ValueError as exc:
        assert "financial_date_start must be on or before financial_date_end" in str(exc)
    else:
        raise AssertionError("expected validation error")
```

- [ ] **Step 2: Run failing tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_models.py -v
```

Expected: FAIL because `temporal_models.py` does not exist.

- [ ] **Step 3: Implement models**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_models.py`:

```python
from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone

from dagster_corpscout.sources.finland.prh_xbrl import spec


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def object_prefix_for_pull(
    *,
    business_id: str,
    financial_date_start: str,
    financial_date_end: str,
    temporal_run_id: str,
) -> str:
    digest = hashlib.sha256(
        f"{business_id}|{financial_date_start}|{financial_date_end}".encode("utf-8")
    ).hexdigest()[:24]
    return (
        f"companies/{digest}/"
        f"financial_date_start={financial_date_start}/"
        f"financial_date_end={financial_date_end}/"
        f"run={temporal_run_id}"
    )


@dataclass(frozen=True)
class CompanyPullInput:
    business_id: str
    financial_date_start: str
    financial_date_end: str
    max_documents: int = 50
    source_url: str = spec.BASE_URL
    bucket: str = spec.BUCKET

    def validated(self) -> "CompanyPullInput":
        business_id = self.business_id.strip()
        if not business_id:
            raise ValueError("business_id is required")
        start = _parse_date(self.financial_date_start, "financial_date_start")
        end = _parse_date(self.financial_date_end, "financial_date_end")
        if start > end:
            raise ValueError("financial_date_start must be on or before financial_date_end")
        if self.max_documents <= 0 or self.max_documents > 1000:
            raise ValueError("max_documents must be between 1 and 1000")
        return CompanyPullInput(
            business_id=business_id,
            financial_date_start=start.isoformat(),
            financial_date_end=end.isoformat(),
            max_documents=self.max_documents,
            source_url=self.source_url.strip() or spec.BASE_URL,
            bucket=self.bucket.strip() or spec.BUCKET,
        )

    def as_payload(self) -> dict:
        return asdict(self.validated())


@dataclass(frozen=True)
class PulledDocument:
    business_id: str
    financial_date: str
    registration_date: str | None
    bucket: str
    object_key: str
    source_url: str
    xml_sha256: str
    xml_size_bytes: int


@dataclass(frozen=True)
class CompanyPullResult:
    business_id: str
    financial_date_start: str
    financial_date_end: str
    bucket: str
    prefix: str
    temporal_workflow_id: str
    temporal_run_id: str
    documents_discovered: int
    documents_downloaded: int
    documents_skipped: int
    documents_failed: int
    documents: list[PulledDocument] = field(default_factory=list)
    completed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def as_payload(self) -> dict:
        return asdict(self)
```

- [ ] **Step 4: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_models.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_models.py \
  corpscout/dagster/tests/test_finland_prh_xbrl_models.py
git commit -m "Add PRH XBRL Temporal pull models"
```

---

### Task 3: Add PRH XBRL API Client

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/client.py`
- Test: `corpscout/dagster/tests/test_finland_prh_xbrl_client.py`

- [ ] **Step 1: Write failing client tests**

Create `corpscout/dagster/tests/test_finland_prh_xbrl_client.py`:

```python
import responses

from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient


def test_list_company_financials_uses_business_id_and_page():
    client = PRHXBRLClient(base_url="https://example.test/opendata-xbrl-api/v3")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://example.test/opendata-xbrl-api/v3/financials",
            json={
                "totalResults": 1,
                "financials": [
                    {
                        "businessId": "0176460-0",
                        "financialDate": "2022-12-31",
                        "registrationDate": "2023-04-01",
                    }
                ],
            },
            status=200,
        )

        page = client.list_company_financials("0176460-0", page=1)

    assert rsps.calls[0].request.url.endswith("/financials?businessId=0176460-0&page=1")
    assert page.total_results == 1
    assert page.financials[0].financial_date == "2022-12-31"


def test_download_financial_xml_returns_bytes_and_source_url():
    client = PRHXBRLClient(base_url="https://example.test/opendata-xbrl-api/v3")

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://example.test/opendata-xbrl-api/v3/financial",
            body=b"<xbrl />",
            status=200,
        )

        body, source_url = client.download_financial_xml("0176460-0", "2022-12-31")

    assert body == b"<xbrl />"
    assert source_url.endswith("/financial?businessId=0176460-0&financialDate=2022-12-31")
```

- [ ] **Step 2: Run failing tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_client.py -v
```

Expected: FAIL because `client.py` does not exist.

- [ ] **Step 3: Implement client**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/client.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlencode

import requests


@dataclass(frozen=True)
class FinancialStatementRef:
    business_id: str
    financial_date: str
    registration_date: str | None = None


@dataclass(frozen=True)
class FinancialsPage:
    total_results: int
    financials: list[FinancialStatementRef]


class PRHXBRLClient:
    def __init__(self, *, base_url: str, timeout_seconds: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def list_company_financials(self, business_id: str, *, page: int = 1) -> FinancialsPage:
        response = requests.get(
            f"{self.base_url}/financials",
            params={"businessId": business_id, "page": page},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        return FinancialsPage(
            total_results=int(payload.get("totalResults") or 0),
            financials=[
                FinancialStatementRef(
                    business_id=str(item.get("businessId") or "").strip(),
                    financial_date=str(item.get("financialDate") or "").strip(),
                    registration_date=(str(item.get("registrationDate")).strip() if item.get("registrationDate") else None),
                )
                for item in payload.get("financials", [])
            ],
        )

    def download_financial_xml(self, business_id: str, financial_date: str) -> tuple[bytes, str]:
        query = urlencode({"businessId": business_id, "financialDate": financial_date})
        source_url = f"{self.base_url}/financial?{query}"
        response = requests.get(source_url, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.content, source_url
```

- [ ] **Step 4: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_client.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/client.py \
  corpscout/dagster/tests/test_finland_prh_xbrl_client.py
git commit -m "Add PRH XBRL API client"
```

---

### Task 4: Add RustFS XML Storage Helpers

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/storage.py`
- Test: `corpscout/dagster/tests/test_finland_prh_xbrl_storage.py`

- [ ] **Step 1: Write failing storage tests**

Create `corpscout/dagster/tests/test_finland_prh_xbrl_storage.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl.storage import document_object_key, sha256_hex


def test_document_object_key_places_xml_under_prefix():
    key = document_object_key(
        prefix="companies/hash/financial_date_start=2021-01-01/financial_date_end=2025-12-31/run=run-1",
        business_id="0176460-0",
        financial_date="2022-12-31",
    )

    assert key == (
        "companies/hash/financial_date_start=2021-01-01/"
        "financial_date_end=2025-12-31/run=run-1/documents/0176460-0/2022-12-31.xml"
    )


def test_sha256_hex_is_stable():
    assert sha256_hex(b"<xbrl />") == "f59baff5d27f43e577c4583571cabf1bfccc2e926db113dff6196b5937c50e9c"
```

- [ ] **Step 2: Run failing tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_storage.py -v
```

Expected: FAIL because `storage.py` does not exist.

- [ ] **Step 3: Implement storage helpers**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/storage.py`:

```python
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


_S3_CONFIG = Config(s3={"addressing_style": "path"})


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def document_object_key(*, prefix: str, business_id: str, financial_date: str) -> str:
    return f"{prefix.rstrip('/')}/documents/{business_id}/{financial_date}.xml"


@dataclass(frozen=True)
class S3Settings:
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    @staticmethod
    def from_env() -> "S3Settings":
        return S3Settings(
            endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
            access_key=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
            secret_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
        )

    def client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=_S3_CONFIG,
        )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.create_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def put_xml(client, *, bucket: str, key: str, body: bytes) -> str:
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/xml")
    return sha256_hex(body)
```

- [ ] **Step 4: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_storage.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/storage.py \
  corpscout/dagster/tests/test_finland_prh_xbrl_storage.py
git commit -m "Add PRH XBRL RustFS storage helpers"
```

---

### Task 5: Add Temporal Activity, Workflow, and Worker

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_activities.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_workflows.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_worker.py`
- Test: `corpscout/dagster/tests/test_finland_prh_xbrl_temporal_workflow.py`

- [ ] **Step 1: Write failing workflow test**

Create `corpscout/dagster/tests/test_finland_prh_xbrl_temporal_workflow.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl.temporal_models import CompanyPullInput


def test_company_pull_input_payload_is_workflow_safe():
    payload = CompanyPullInput(
        business_id="0176460-0",
        financial_date_start="2021-01-01",
        financial_date_end="2025-12-31",
        max_documents=10,
    ).as_payload()

    assert payload == {
        "business_id": "0176460-0",
        "financial_date_start": "2021-01-01",
        "financial_date_end": "2025-12-31",
        "max_documents": 10,
        "source_url": "https://avoindata.prh.fi/opendata-xbrl-api/v3",
        "bucket": "source-finland-prh-xbrl",
    }
```

- [ ] **Step 2: Add the activity implementation**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_activities.py`:

```python
from __future__ import annotations

import logging
from datetime import date

from temporalio import activity

from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient
from dagster_corpscout.sources.finland.prh_xbrl.storage import (
    S3Settings,
    document_object_key,
    ensure_bucket,
    put_xml,
)
from dagster_corpscout.sources.finland.prh_xbrl.temporal_models import (
    CompanyPullInput,
    CompanyPullResult,
    PulledDocument,
    object_prefix_for_pull,
)

logger = logging.getLogger(__name__)


def _date_in_window(value: str, start: str, end: str) -> bool:
    current = date.fromisoformat(value)
    return date.fromisoformat(start) <= current <= date.fromisoformat(end)


@activity.defn(name="finland_prh_xbrl_pull_company_documents")
async def pull_company_documents(payload: dict) -> dict:
    workflow_info = activity.info()
    pull_input = CompanyPullInput(**payload).validated()
    prefix = object_prefix_for_pull(
        business_id=pull_input.business_id,
        financial_date_start=pull_input.financial_date_start,
        financial_date_end=pull_input.financial_date_end,
        temporal_run_id=workflow_info.workflow_run_id,
    )

    s3 = S3Settings.from_env().client()
    ensure_bucket(s3, pull_input.bucket)
    client = PRHXBRLClient(base_url=pull_input.source_url)

    documents: list[PulledDocument] = []
    discovered = 0
    skipped = 0
    failed = 0
    seen = 0
    page_number = 1
    total_results = 0

    while True:
        page = client.list_company_financials(pull_input.business_id, page=page_number)
        if page.total_results:
            total_results = page.total_results
        if not page.financials:
            break

        for statement in page.financials:
            seen += 1
            discovered += 1
            if statement.business_id != pull_input.business_id:
                skipped += 1
                continue
            if not _date_in_window(statement.financial_date, pull_input.financial_date_start, pull_input.financial_date_end):
                skipped += 1
                continue
            if len(documents) + failed >= pull_input.max_documents:
                skipped += 1
                continue

            try:
                body, source_url = client.download_financial_xml(
                    statement.business_id,
                    statement.financial_date,
                )
                key = document_object_key(
                    prefix=prefix,
                    business_id=statement.business_id,
                    financial_date=statement.financial_date,
                )
                digest = put_xml(s3, bucket=pull_input.bucket, key=key, body=body)
                documents.append(
                    PulledDocument(
                        business_id=statement.business_id,
                        financial_date=statement.financial_date,
                        registration_date=statement.registration_date,
                        bucket=pull_input.bucket,
                        object_key=key,
                        source_url=source_url,
                        xml_sha256=digest,
                        xml_size_bytes=len(body),
                    )
                )
                logger.info("downloaded PRH XBRL document", extra={"business_id": statement.business_id, "financial_date": statement.financial_date, "object_key": key})
            except Exception:
                failed += 1
                logger.exception("failed to download PRH XBRL document", extra={"business_id": statement.business_id, "financial_date": statement.financial_date})

        if total_results and seen >= total_results:
            break
        if len(documents) + failed >= pull_input.max_documents:
            break
        page_number += 1

    result = CompanyPullResult(
        business_id=pull_input.business_id,
        financial_date_start=pull_input.financial_date_start,
        financial_date_end=pull_input.financial_date_end,
        bucket=pull_input.bucket,
        prefix=prefix,
        temporal_workflow_id=workflow_info.workflow_id,
        temporal_run_id=workflow_info.workflow_run_id,
        documents_discovered=discovered,
        documents_downloaded=len(documents),
        documents_skipped=skipped,
        documents_failed=failed,
        documents=documents,
    )
    logger.info("completed PRH XBRL company pull", extra=result.as_payload())
    return result.as_payload()
```

- [ ] **Step 3: Add workflow implementation**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_workflows.py`:

```python
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from dagster_corpscout.sources.finland.prh_xbrl.temporal_activities import pull_company_documents


@workflow.defn(name="FinlandPRHXBRLCompanyPullWorkflow")
class FinlandPRHXBRLCompanyPullWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        return await workflow.execute_activity(
            pull_company_documents,
            payload,
            start_to_close_timeout=timedelta(minutes=60),
        )
```

- [ ] **Step 4: Add worker entrypoint**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_worker.py`:

```python
import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.temporal_activities import pull_company_documents
from dagster_corpscout.sources.finland.prh_xbrl.temporal_workflows import FinlandPRHXBRLCompanyPullWorkflow


async def main() -> None:
    target_host = os.getenv("TEMPORAL_TARGET_HOST", "companycollect:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    task_queue = os.getenv("TEMPORAL_TASK_QUEUE_PRH_XBRL", spec.TASK_QUEUE)
    client = await Client.connect(target_host, namespace=namespace)
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[FinlandPRHXBRLCompanyPullWorkflow],
        activities=[pull_company_documents],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_temporal_workflow.py tests/test_finland_prh_xbrl_models.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_activities.py \
  corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_workflows.py \
  corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/temporal_worker.py \
  corpscout/dagster/tests/test_finland_prh_xbrl_temporal_workflow.py
git commit -m "Add PRH XBRL Temporal worker"
```

---

### Task 6: Add Dagster Temporal Resource and Launcher Job

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/resources/temporal.py`
- Modify: `corpscout/dagster/dagster_corpscout/definitions.py`
- Modify: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/jobs.py`
- Test: `corpscout/dagster/tests/test_finland_prh_xbrl_temporal_workflow.py`

- [ ] **Step 1: Add launcher test**

Append to `corpscout/dagster/tests/test_finland_prh_xbrl_temporal_workflow.py`:

```python
def test_launcher_workflow_id_is_deterministic_for_request():
    from dagster_corpscout.sources.finland.prh_xbrl.jobs import workflow_id_for_company_pull

    workflow_id = workflow_id_for_company_pull(
        business_id="0176460-0",
        financial_date_start="2021-01-01",
        financial_date_end="2025-12-31",
    )

    assert workflow_id.startswith("finland-prh-xbrl-company-")
    assert "0176460-0" not in workflow_id
    assert workflow_id.endswith("-2021-01-01-2025-12-31")
```

- [ ] **Step 2: Add Temporal resource**

Create `corpscout/dagster/dagster_corpscout/resources/temporal.py`:

```python
from __future__ import annotations

import asyncio

from dagster import ConfigurableResource
from temporalio.client import Client


class TemporalResource(ConfigurableResource):
    target_host: str
    namespace: str = "default"

    async def get_client(self) -> Client:
        return await Client.connect(self.target_host, namespace=self.namespace)

    def run_async(self, coro):
        return asyncio.run(coro)
```

Modify `corpscout/dagster/dagster_corpscout/definitions.py` resource block:

```python
from dagster_corpscout.resources.temporal import TemporalResource

"temporal": TemporalResource(
    target_host=dg.EnvVar("TEMPORAL_TARGET_HOST"),
    namespace=dg.EnvVar("TEMPORAL_NAMESPACE"),
),
```

- [ ] **Step 3: Add launcher op and job**

Modify `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/jobs.py`:

```python
import hashlib

import dagster as dg

from dagster_corpscout.resources.temporal import TemporalResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets import raw_xml_documents
from dagster_corpscout.sources.finland.prh_xbrl.temporal_models import CompanyPullInput
from dagster_corpscout.sources.finland.prh_xbrl.temporal_workflows import FinlandPRHXBRLCompanyPullWorkflow


class CompanyPullConfig(dg.Config):
    business_id: str
    financial_date_start: str
    financial_date_end: str
    max_documents: int = 50
    source_url: str = spec.BASE_URL
    bucket: str = spec.BUCKET


def workflow_id_for_company_pull(*, business_id: str, financial_date_start: str, financial_date_end: str) -> str:
    digest = hashlib.sha256(f"{business_id}|{financial_date_start}|{financial_date_end}".encode("utf-8")).hexdigest()[:16]
    return f"finland-prh-xbrl-company-{digest}-{financial_date_start}-{financial_date_end}"


@dg.op(required_resource_keys={"temporal"})
def start_company_pull(context: dg.OpExecutionContext, config: CompanyPullConfig) -> dict:
    payload = CompanyPullInput(**config.model_dump()).as_payload()
    workflow_id = workflow_id_for_company_pull(
        business_id=payload["business_id"],
        financial_date_start=payload["financial_date_start"],
        financial_date_end=payload["financial_date_end"],
    )

    async def _start() -> dict:
        client = await context.resources.temporal.get_client()
        handle = await client.start_workflow(
            FinlandPRHXBRLCompanyPullWorkflow.run,
            payload,
            id=workflow_id,
            task_queue=spec.TASK_QUEUE,
        )
        return {"workflow_id": handle.id, "run_id": handle.result_run_id}

    result = context.resources.temporal.run_async(_start())
    context.log.info("started Finland PRH XBRL Temporal workflow", extra=result)
    return result


@dg.job(name="finland_prh_xbrl_start_company_pull")
def start_company_pull_job():
    start_company_pull()


process_completed_pull_job = dg.define_asset_job(
    name="finland_prh_xbrl_process_completed_pull",
    selection=[raw_xml_documents],
)
```

- [ ] **Step 4: Register both jobs**

Modify `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/__init__.py`:

```python
from dagster_corpscout.source_bundle import SourceBundle
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets import raw_xml_documents, source_system
from dagster_corpscout.sources.finland.prh_xbrl.jobs import (
    process_completed_pull_job,
    start_company_pull_job,
)

source_bundle = SourceBundle(
    source_name=spec.SOURCE_NAME,
    asset_key_prefix=tuple(spec.ASSET_KEY_PREFIX),
    assets=(source_system, raw_xml_documents),
    jobs=(start_company_pull_job, process_completed_pull_job),
    schedules=(),
)

__all__ = ["source_bundle"]
```

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_temporal_workflow.py tests/test_definitions.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/dagster_corpscout/resources/temporal.py \
  corpscout/dagster/dagster_corpscout/definitions.py \
  corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/jobs.py \
  corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/__init__.py \
  corpscout/dagster/tests/test_finland_prh_xbrl_temporal_workflow.py
git commit -m "Add Dagster launcher for PRH XBRL Temporal pulls"
```

---

### Task 7: Add XBRL Parser and dlt ClickHouse Pipeline

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/xbrl_parser.py`
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/dlt_pipeline.py`
- Modify: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py`
- Test: `corpscout/dagster/tests/test_finland_prh_xbrl_dlt_pipeline.py`

- [ ] **Step 1: Write failing parser test**

Create `corpscout/dagster/tests/test_finland_prh_xbrl_dlt_pipeline.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl.xbrl_parser import parse_xbrl_document


def test_parse_xbrl_document_extracts_context_units_and_facts():
    xml = b'''<?xml version="1.0" encoding="UTF-8"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance" xmlns:fi-met="http://example.test/fi-met">
  <context id="C1"><entity><identifier scheme="ytunnus">0176460-0</identifier></entity><period><instant>2022-12-31</instant></period></context>
  <unit id="EUR"><measure>iso4217:EUR</measure></unit>
  <fi-met:md103 contextRef="C1" unitRef="EUR" decimals="0">12345</fi-met:md103>
</xbrl>'''

    parsed = parse_xbrl_document(
        business_id="0176460-0",
        financial_date="2022-12-31",
        bucket="source-finland-prh-xbrl",
        object_key="companies/hash/documents/0176460-0/2022-12-31.xml",
        xml_sha256="abc",
        xml_size_bytes=len(xml),
        body=xml,
    )

    assert parsed.document["business_id"] == "0176460-0"
    assert parsed.contexts[0]["context_id"] == "C1"
    assert parsed.units[0]["unit_id"] == "EUR"
    assert parsed.facts[0]["concept_local_name"] == "md103"
    assert parsed.facts[0]["value_text"] == "12345"
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_dlt_pipeline.py -v
```

Expected: FAIL because `xbrl_parser.py` does not exist.

- [ ] **Step 3: Implement parser**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/xbrl_parser.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

from lxml import etree

XBRL_NS = "http://www.xbrl.org/2003/instance"


@dataclass(frozen=True)
class ParsedXBRLDocument:
    document: dict
    contexts: list[dict]
    units: list[dict]
    facts: list[dict]


def _qname_parts(element) -> tuple[str, str]:
    qname = etree.QName(element)
    return qname.namespace or "", qname.localname


def parse_xbrl_document(
    *,
    business_id: str,
    financial_date: str,
    bucket: str,
    object_key: str,
    xml_sha256: str,
    xml_size_bytes: int,
    body: bytes,
) -> ParsedXBRLDocument:
    root = etree.fromstring(body)
    document_id = f"{business_id}|{financial_date}|{xml_sha256}"
    contexts = []
    for context in root.findall(f"{{{XBRL_NS}}}context"):
        context_id = context.get("id", "")
        period = context.find(f".//{{{XBRL_NS}}}period")
        contexts.append(
            {
                "document_id": document_id,
                "business_id": business_id,
                "financial_date": financial_date,
                "context_id": context_id,
                "period_xml": etree.tostring(period, encoding="unicode") if period is not None else "",
                "context_xml": etree.tostring(context, encoding="unicode"),
            }
        )

    units = []
    for unit in root.findall(f"{{{XBRL_NS}}}unit"):
        unit_id = unit.get("id", "")
        units.append(
            {
                "document_id": document_id,
                "business_id": business_id,
                "financial_date": financial_date,
                "unit_id": unit_id,
                "unit_xml": etree.tostring(unit, encoding="unicode"),
            }
        )

    facts = []
    for element in root.iter():
        namespace, local_name = _qname_parts(element)
        if namespace == XBRL_NS:
            continue
        context_ref = element.get("contextRef")
        if not context_ref:
            continue
        facts.append(
            {
                "document_id": document_id,
                "business_id": business_id,
                "financial_date": financial_date,
                "concept_namespace": namespace,
                "concept_local_name": local_name,
                "context_id": context_ref,
                "unit_id": element.get("unitRef", ""),
                "decimals": element.get("decimals", ""),
                "value_text": (element.text or "").strip(),
            }
        )

    return ParsedXBRLDocument(
        document={
            "document_id": document_id,
            "business_id": business_id,
            "financial_date": financial_date,
            "bucket": bucket,
            "object_key": object_key,
            "xml_sha256": xml_sha256,
            "xml_size_bytes": xml_size_bytes,
        },
        contexts=contexts,
        units=units,
        facts=facts,
    )
```

- [ ] **Step 4: Implement dlt pipeline wrapper**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/dlt_pipeline.py`:

```python
from __future__ import annotations

import os
from collections.abc import Iterable

import boto3
import dlt
from botocore.config import Config

from dagster_corpscout.sources.finland.prh_xbrl.storage import sha256_hex
from dagster_corpscout.sources.finland.prh_xbrl.xbrl_parser import parse_xbrl_document

_S3_CONFIG = Config(s3={"addressing_style": "path"})


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        aws_access_key_id=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=_S3_CONFIG,
    )


def iter_xml_objects(*, bucket: str, prefix: str) -> Iterable[dict]:
    client = _s3_client()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix.rstrip('/')}/documents/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.endswith(".xml"):
                body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
                parts = key.rsplit("/", 2)
                yield {
                    "bucket": bucket,
                    "object_key": key,
                    "business_id": parts[-2],
                    "financial_date": parts[-1].removesuffix(".xml"),
                    "body": body,
                    "xml_sha256": sha256_hex(body),
                    "xml_size_bytes": len(body),
                }


def parsed_rows(*, bucket: str, prefix: str) -> dict[str, list[dict]]:
    rows = {
        "fi_prh_xbrl_statement_documents": [],
        "fi_prh_xbrl_contexts": [],
        "fi_prh_xbrl_units": [],
        "fi_prh_xbrl_facts_raw": [],
    }
    for item in iter_xml_objects(bucket=bucket, prefix=prefix):
        parsed = parse_xbrl_document(**item)
        rows["fi_prh_xbrl_statement_documents"].append(parsed.document)
        rows["fi_prh_xbrl_contexts"].extend(parsed.contexts)
        rows["fi_prh_xbrl_units"].extend(parsed.units)
        rows["fi_prh_xbrl_facts_raw"].extend(parsed.facts)
    return rows


def run_pipeline(*, bucket: str, prefix: str) -> dict:
    rows_by_table = parsed_rows(bucket=bucket, prefix=prefix)

    @dlt.source
    def prh_xbrl_source():
        for table_name, rows in rows_by_table.items():
            yield dlt.resource(rows, name=table_name, write_disposition="append")

    pipeline = dlt.pipeline(
        pipeline_name="finland_prh_xbrl",
        destination="clickhouse",
        dataset_name=os.getenv("CLICKHOUSE_DATABASE", "corpscout_sources"),
    )
    load_info = pipeline.run(prh_xbrl_source())
    return {"tables": {name: len(rows) for name, rows in rows_by_table.items()}, "load_info": str(load_info)}
```

- [ ] **Step 5: Update raw asset to process a completed pull**

Modify `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py`:

```python
import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.dlt_pipeline import run_pipeline


class CompletedPullConfig(dg.Config):
    bucket: str
    prefix: str
    temporal_workflow_id: str
    temporal_run_id: str


@dg.asset(
    key=[*spec.ASSET_KEY_PREFIX, "raw_xml_documents"],
    deps=[dg.AssetKey([*spec.ASSET_KEY_PREFIX, "source_system"])],
    group_name=spec.GROUP_NAME,
    tags={**spec.TAGS, "layer": "raw"},
)
def raw_xml_documents(context: dg.AssetExecutionContext, config: CompletedPullConfig) -> dg.MaterializeResult:
    result = run_pipeline(bucket=config.bucket, prefix=config.prefix)
    return dg.MaterializeResult(
        metadata={
            "bucket": config.bucket,
            "prefix": config.prefix,
            "temporal_workflow_id": config.temporal_workflow_id,
            "temporal_run_id": config.temporal_run_id,
            "tables": result["tables"],
        }
    )
```

- [ ] **Step 6: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_dlt_pipeline.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/xbrl_parser.py \
  corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/dlt_pipeline.py \
  corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/assets/raw.py \
  corpscout/dagster/tests/test_finland_prh_xbrl_dlt_pipeline.py
git commit -m "Add PRH XBRL dlt ClickHouse pipeline"
```

---

### Task 8: Add Temporal Completion Sensor

**Files:**
- Create: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/sensors.py`
- Modify: `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/__init__.py`
- Test: `corpscout/dagster/tests/test_finland_prh_xbrl_sensor.py`

- [ ] **Step 1: Write failing sensor payload test**

Create `corpscout/dagster/tests/test_finland_prh_xbrl_sensor.py`:

```python
from dagster_corpscout.sources.finland.prh_xbrl.sensors import run_config_for_completed_pull


def test_run_config_for_completed_pull_targets_raw_asset_config():
    result = {
        "bucket": "source-finland-prh-xbrl",
        "prefix": "companies/hash/run=run-1",
        "temporal_workflow_id": "workflow-1",
        "temporal_run_id": "run-1",
    }

    run_config = run_config_for_completed_pull(result)

    assert run_config["ops"]["raw_xml_documents"]["config"] == result
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_sensor.py -v
```

Expected: FAIL because `sensors.py` does not exist.

- [ ] **Step 3: Implement sensor helper and sensor**

Create `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/sensors.py`:

```python
from __future__ import annotations

import json

import dagster as dg

from dagster_corpscout.resources.temporal import TemporalResource
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.jobs import process_completed_pull_job


def run_config_for_completed_pull(result: dict) -> dict:
    return {
        "ops": {
            "raw_xml_documents": {
                "config": {
                    "bucket": result["bucket"],
                    "prefix": result["prefix"],
                    "temporal_workflow_id": result["temporal_workflow_id"],
                    "temporal_run_id": result["temporal_run_id"],
                }
            }
        }
    }


def _cursor_seen(cursor: str | None) -> set[str]:
    if not cursor:
        return set()
    return set(json.loads(cursor))


@dg.sensor(
    name="finland_prh_xbrl_temporal_completion_sensor",
    job=process_completed_pull_job,
    default_status=dg.DefaultSensorStatus.STOPPED,
    required_resource_keys={"temporal"},
)
def temporal_completion_sensor(context: dg.SensorEvaluationContext):
    temporal: TemporalResource = context.resources.temporal
    seen = _cursor_seen(context.cursor)

    async def _completed_results() -> list[tuple[str, dict]]:
        client = await temporal.get_client()
        query = f"WorkflowType = '{spec.WORKFLOW_NAME}' AND ExecutionStatus = 'Completed'"
        out: list[tuple[str, dict]] = []
        async for execution in client.list_workflows(query=query):
            run_id = execution.run_id
            if run_id in seen:
                continue
            handle = client.get_workflow_handle(execution.id, run_id=run_id)
            out.append((run_id, await handle.result()))
        return out

    completed = temporal.run_async(_completed_results())
    new_seen = set(seen)
    for run_id, result in completed:
        new_seen.add(run_id)
        yield dg.RunRequest(
            run_key=run_id,
            run_config=run_config_for_completed_pull(result),
            tags={
                "temporal_workflow_id": result["temporal_workflow_id"],
                "temporal_run_id": result["temporal_run_id"],
                "source_name": spec.SOURCE_NAME,
            },
        )
    context.update_cursor(json.dumps(sorted(new_seen)[-500:]))
```

- [ ] **Step 4: Register sensor in source bundle**

Modify `corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/__init__.py`:

```python
from dagster_corpscout.source_bundle import SourceBundle
from dagster_corpscout.sources.finland.prh_xbrl import spec
from dagster_corpscout.sources.finland.prh_xbrl.assets import raw_xml_documents, source_system
from dagster_corpscout.sources.finland.prh_xbrl.jobs import (
    process_completed_pull_job,
    start_company_pull_job,
)
from dagster_corpscout.sources.finland.prh_xbrl.sensors import temporal_completion_sensor

source_bundle = SourceBundle(
    source_name=spec.SOURCE_NAME,
    asset_key_prefix=tuple(spec.ASSET_KEY_PREFIX),
    assets=(source_system, raw_xml_documents),
    jobs=(start_company_pull_job, process_completed_pull_job),
    schedules=(),
    sensors=(temporal_completion_sensor,),
)

__all__ = ["source_bundle"]
```

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest tests/test_finland_prh_xbrl_sensor.py tests/test_definitions.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/sensors.py \
  corpscout/dagster/dagster_corpscout/sources/finland/prh_xbrl/__init__.py \
  corpscout/dagster/tests/test_finland_prh_xbrl_sensor.py
git commit -m "Add PRH XBRL Temporal completion sensor"
```

---

### Task 9: Manual End-to-End Smoke Test

**Files:**
- Modify: `corpscout/dagster/README.md`

- [ ] **Step 1: Rebuild Dagster image**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
docker compose build
docker compose up -d
```

Expected: services include `finland-prh-xbrl-temporal-worker`.

- [ ] **Step 2: Start a manual pull from Dagster**

In Dagster UI at `http://localhost:3500`, run job `finland_prh_xbrl_start_company_pull` with config:

```yaml
ops:
  start_company_pull:
    config:
      business_id: "0176460-0"
      financial_date_start: "2021-01-01"
      financial_date_end: "2025-12-31"
      max_documents: 10
```

Expected: job logs contain a Temporal workflow ID and run ID.

- [ ] **Step 3: Confirm Temporal completed**

Run:

```bash
docker compose logs finland-prh-xbrl-temporal-worker --tail=200
```

Expected: logs include `completed PRH XBRL company pull`.

- [ ] **Step 4: Confirm XML objects exist in RustFS**

Run on `companycollect`:

```bash
find /opt/rustfs/data/source-finland-prh-xbrl -type f | head -20
```

Expected: XML files exist under a path shaped like `companies/<hash>/financial_date_start=2021-01-01/financial_date_end=2025-12-31/run=<temporal-run-id>/documents/<business-id>/<financial-date>.xml`.

- [ ] **Step 5: Enable or tick the sensor**

In Dagster UI, evaluate `finland_prh_xbrl_temporal_completion_sensor`.

Expected: it launches `finland_prh_xbrl_process_completed_pull`.

- [ ] **Step 6: Confirm ClickHouse rows**

Run:

```bash
docker exec -i companyindex-dev-clickhouse-1 clickhouse-client --query "
SELECT count() FROM corpscout_sources.fi_prh_xbrl_statement_documents;
SELECT count() FROM corpscout_sources.fi_prh_xbrl_facts_raw;
"
```

Expected: both counts are greater than zero after processing.

- [ ] **Step 7: Document the runbook**

Add to `corpscout/dagster/README.md`:

```markdown
## Finland PRH XBRL Temporal Pull

Run `finland_prh_xbrl_start_company_pull` with `business_id`, `financial_date_start`, and `financial_date_end`.
The Temporal worker stores raw XML in `source-finland-prh-xbrl` under a hashed company/date-window prefix.
The completion sensor starts `finland_prh_xbrl_process_completed_pull`, which parses XML from RustFS and writes ClickHouse XBRL tables.
No Postgres source-run or manifest file is used for this flow.
```

- [ ] **Step 8: Run full Dagster tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster
./.venv/bin/python -m pytest -v
```

Expected: PASS.

- [ ] **Step 9: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster/README.md
git commit -m "Document PRH XBRL Temporal pull runbook"
```

---

## Self-Review

- Spec coverage: The plan implements Dagster-owned Temporal workflow code, a separate worker process, PRH API pulls, RustFS XML storage, Temporal completion sensing, and dlt-to-ClickHouse processing.
- Explicit non-goals: No Postgres source action/file runs, no Go scheduler workflow, no manifest files.
- Type consistency: `CompanyPullInput`, `CompanyPullResult`, `PulledDocument`, `bucket`, `prefix`, `temporal_workflow_id`, and `temporal_run_id` are used consistently across workflow, sensor, and asset config.
- Operational handoff: The source bucket is `source-finland-prh-xbrl`; per-run isolation is done with hashed company/date-window prefixes, not one bucket per run.
