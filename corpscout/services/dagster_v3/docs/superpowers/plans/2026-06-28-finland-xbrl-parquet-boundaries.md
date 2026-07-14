# Finland XBRL Parquet Boundaries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the first Finland XBRL pipeline slice so each Dagster asset materializes its own parquet artifact instead of exchanging data through shared DuckDB tables.

**Architecture:** The pipeline will use S3/object-store parquet objects as asset boundaries. Partitioned assets write one parquet object per partition, and non-partitioned seed assets write one stable parquet object. DuckDB remains available for existing parsed/dbt stages until a later migration slice, but the raw XML download path will consume parquet, not DuckDB.

**Tech Stack:** Dagster assets, `ObjectStoreResource`, PyArrow tables/datasets/compute, parquet object bytes in S3/RustFS-compatible storage, existing fake S3 tests.

---

## Scope

This plan implements only the first slice:

- Add parquet IO helpers for Finland XBRL.
- Add a YTJ all-companies parquet asset for XBRL consumption.
- Replace the Finland XBRL company seed DuckDB asset with a parquet seed asset.
- Change financial report discovery assets to write partition parquet.
- Add a Python/PyArrow eligible financial reports asset that joins parquet inputs.
- Change raw XML download assets to read eligible financial reports parquet.

This plan does not migrate parsed XBRL tables, metric mapping, ClickHouse export, or dbt-derived metrics. Those remain for the second slice.

## File Structure

- Create `src/dagster_v3/defs/finland_xbrl/assets/parquet_io.py`
  - Owns object keys and small read/write helpers for parquet bytes.
  - Keeps S3/parquet boundary details out of asset code.

- Modify `src/dagster_v3/defs/finland_ytj/assets.py`
  - Adds `finland_ytj_all_companies_parquet` as a downstream asset from `finland_ytj_all_companies_duckdb`.
  - Writes `ytj/all_companies.parquet`.

- Replace `src/dagster_v3/defs/finland_xbrl/assets/company_seed_duckdb.py`
  - Rename to `company_seed.py`.
  - Remove `ytj_duckdb` and `source_duckdb` use.
  - Read `ytj/all_companies.parquet` and write `xbrl/company_seed/eligible_companies.parquet`.

- Modify `src/dagster_v3/defs/finland_xbrl/assets/financial_reports.py`
  - Discovery still uses PRH API/dlt-shaped rows, but writes partition parquet instead of DuckDB for backfill/incremental outputs.
  - Remove the existing financial reports DuckDB marker from first-slice jobs and downstream deps. Leave old DuckDB helper functions only when later parsed/dbt tests still import them directly.

- Create `src/dagster_v3/defs/finland_xbrl/assets/eligible_financial_reports.py`
  - Reads financial reports partition parquet plus company seed parquet.
  - Joins/filter by `business_id`.
  - Writes one eligible reports parquet per partition.

- Modify `src/dagster_v3/defs/finland_xbrl/assets/raw_xml_documents.py`
  - Read eligible financial reports partition parquet instead of querying DuckDB.
  - Keep XML object writes and XML document manifest parquet behavior.

- Modify `src/dagster_v3/defs/finland_xbrl/assets/jobs.py`
  - Select parquet assets by their new names.

- Modify `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`
  - Export/register the new assets and helpers.

- Modify `tests/test_finland_xbrl_assets.py` and `tests/test_finland_ytj_assets.py`
  - Assert object keys, parquet rows, dependencies, and no DuckDB dependency in raw XML input path.

---

### Task 1: Shared Parquet IO Helpers

**Files:**
- Create: `src/dagster_v3/defs/finland_xbrl/assets/parquet_io.py`
- Test: `tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Write failing tests for object keys and round-trip parquet bytes**

Add these imports to `tests/test_finland_xbrl_assets.py`:

```python
import pyarrow as pa
```

Add this test:

```python
def test_finland_xbrl_parquet_io_round_trips_table() -> None:
    object_store, s3_client = _object_store()
    table = pa.table(
        {
            "business_id": ["active-web"],
            "financial_date": ["2026-05-31"],
        }
    )

    xbrl_assets.write_xbrl_parquet(
        object_store,
        key="xbrl/test/table.parquet",
        table=table,
    )
    loaded = xbrl_assets.read_xbrl_parquet(
        object_store,
        key="xbrl/test/table.parquet",
    )

    assert ("source-finland-prh-xbrl", "xbrl/test/table.parquet") in s3_client.objects
    assert loaded.to_pydict() == table.to_pydict()
```

Add this test:

```python
def test_finland_xbrl_partition_parquet_key_uses_asset_and_partition() -> None:
    assert xbrl_assets.partition_parquet_key(
        "financial_reports",
        "2026-06-01",
    ) == "xbrl/financial_reports/partition=2026-06-01.parquet"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_finland_xbrl_parquet_io_round_trips_table tests/test_finland_xbrl_assets.py::test_finland_xbrl_partition_parquet_key_uses_asset_and_partition -q
```

Expected: fail because `write_xbrl_parquet`, `read_xbrl_parquet`, and `partition_parquet_key` are not exported.

- [ ] **Step 3: Implement parquet helper module**

Create `src/dagster_v3/defs/finland_xbrl/assets/parquet_io.py`:

```python
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import XBRL_BUCKET


def partition_parquet_key(asset_name: str, partition_key: str) -> str:
    return f"xbrl/{asset_name}/partition={partition_key}.parquet"


def stable_parquet_key(asset_name: str, file_name: str) -> str:
    return f"xbrl/{asset_name}/{file_name}.parquet"


def write_xbrl_parquet(
    object_store: ObjectStoreResource,
    *,
    key: str,
    table: pa.Table,
) -> None:
    output = BytesIO()
    pq.write_table(table, output)
    object_store.write_bytes(key, output.getvalue(), bucket=XBRL_BUCKET)


def read_xbrl_parquet(
    object_store: ObjectStoreResource,
    *,
    key: str,
) -> pa.Table:
    return pq.read_table(BytesIO(object_store.read_bytes(key, bucket=XBRL_BUCKET)))
```

- [ ] **Step 4: Export helpers**

Modify `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`:

```python
from dagster_v3.defs.finland_xbrl.assets.parquet_io import (
    partition_parquet_key,
    read_xbrl_parquet,
    stable_parquet_key,
    write_xbrl_parquet,
)
```

Add to `__all__`:

```python
"partition_parquet_key",
"read_xbrl_parquet",
"stable_parquet_key",
"write_xbrl_parquet",
```

- [ ] **Step 5: Run tests**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_finland_xbrl_parquet_io_round_trips_table tests/test_finland_xbrl_assets.py::test_finland_xbrl_partition_parquet_key_uses_asset_and_partition -q
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets/parquet_io.py src/dagster_v3/defs/finland_xbrl/assets/__init__.py tests/test_finland_xbrl_assets.py
git commit -m "feat: add Finland XBRL parquet IO helpers"
```

---

### Task 2: YTJ All Companies Parquet Asset

**Files:**
- Modify: `src/dagster_v3/defs/finland_ytj/assets.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`
- Test: `tests/test_finland_ytj_assets.py`

- [ ] **Step 1: Write failing test for YTJ parquet asset**

In `tests/test_finland_ytj_assets.py`, add a fake object store helper if one is not already present:

```python
class FakeS3Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def create_bucket(self, Bucket: str) -> None:
        return None

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket: str, Key: str) -> dict:
        return {"Body": FakeS3Body(self.objects[(Bucket, Key)])}
```

Add this test:

```python
def test_ytj_all_companies_parquet_asset_writes_parquet(tmp_path: Path) -> None:
    import pyarrow.parquet as pq
    from io import BytesIO
    from dagster_v3.defs.common.resources import ObjectStoreResource

    database_path = tmp_path / "finland_ytj.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_prhytj")
        connection.execute(
            """
            create table finland_prhytj.all_companies (
                business_id varchar,
                primary_name varchar,
                is_active boolean,
                website_normalized_url varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_prhytj.all_companies values
            ('active-web', 'Active Oy', true, 'https://active.example')
            """
        )

    s3_client = FakeS3Client()
    object_store = ObjectStoreResource(bucket="source-finland-prh-xbrl", s3_client=s3_client)
    result = ytj_assets.finland_ytj_all_companies_parquet(
        ytj_duckdb=duckdb_resource(database_path),
        object_store=object_store,
    )

    key = "ytj/all_companies.parquet"
    table = pq.read_table(BytesIO(s3_client.objects[("source-finland-prh-xbrl", key)]))
    assert table.to_pydict()["business_id"] == ["active-web"]
    assert result.metadata["object_key"] == key
    assert result.metadata["row_count"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_finland_ytj_assets.py::test_ytj_all_companies_parquet_asset_writes_parquet -q
```

Expected: fail because `finland_ytj_all_companies_parquet` does not exist.

- [ ] **Step 3: Implement YTJ parquet asset**

Modify `src/dagster_v3/defs/finland_ytj/assets.py`:

```python
from io import BytesIO

import pyarrow.parquet as pq

from dagster_v3.defs.common.resources import ObjectStoreResource
```

Add constants:

```python
YTJ_PARQUET_BUCKET = "source-finland-prh-xbrl"
YTJ_ALL_COMPANIES_PARQUET_KEY = "ytj/all_companies.parquet"
```

Add asset:

```python
@dg.asset(
    name="finland_ytj_all_companies_parquet",
    group_name="finland_ytj",
    deps=["finland_ytj_all_companies_duckdb"],
    kinds={"python", "parquet", "s3"},
    pool="finland_ytj_duckdb",
)
def finland_ytj_all_companies_parquet(
    ytj_duckdb: DuckDBResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(ytj_duckdb) as connection:
        table = connection.execute(
            """
            select
                business_id,
                primary_name,
                is_active,
                website_normalized_url
            from finland_prhytj.all_companies
            """
        ).to_arrow_table()

    output = BytesIO()
    pq.write_table(table, output)
    object_store.write_bytes(
        YTJ_ALL_COMPANIES_PARQUET_KEY,
        output.getvalue(),
        bucket=YTJ_PARQUET_BUCKET,
    )
    return dg.MaterializeResult(
        metadata={
            "bucket": YTJ_PARQUET_BUCKET,
            "object_key": YTJ_ALL_COMPANIES_PARQUET_KEY,
            "row_count": table.num_rows,
        }
    )
```

Register asset in `defs = dg.Definitions(...)` assets list.

- [ ] **Step 4: Run test**

Run:

```bash
uv run pytest tests/test_finland_ytj_assets.py::test_ytj_all_companies_parquet_asset_writes_parquet -q
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_ytj/assets.py tests/test_finland_ytj_assets.py
git commit -m "feat: add Finland YTJ all companies parquet asset"
```

---

### Task 3: Company Seed Parquet Asset

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/company_seed_duckdb.py`
- Rename: `src/dagster_v3/defs/finland_xbrl/assets/company_seed_duckdb.py` to `src/dagster_v3/defs/finland_xbrl/assets/company_seed.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`
- Modify: `tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Write failing test for company seed parquet**

Replace `test_build_finland_xbrl_company_seed_duckdb_copies_active_companies_with_websites` with:

```python
def test_build_finland_xbrl_company_seed_parquet_filters_active_companies_with_websites() -> None:
    object_store, s3_client = _object_store()
    input_key = "ytj/all_companies.parquet"
    output_key = "xbrl/company_seed/eligible_companies.parquet"
    xbrl_assets.write_xbrl_parquet(
        object_store,
        key=input_key,
        table=pa.table(
            {
                "business_id": ["active-web", "inactive-web", "active-missing-web"],
                "primary_name": ["Active Oy", "Inactive Oy", "No Web Oy"],
                "is_active": [True, False, True],
                "website_normalized_url": [
                    "https://active.example",
                    "https://inactive.example",
                    "",
                ],
            }
        ),
    )
    log_messages: list[str] = []

    result = xbrl_assets.build_finland_xbrl_company_seed_parquet(
        object_store=object_store,
        input_key=input_key,
        output_key=output_key,
        log_info=log_messages.append,
    )
    output = xbrl_assets.read_xbrl_parquet(object_store, key=output_key)

    assert output.to_pydict() == {
        "business_id": ["active-web"],
        "primary_name": ["Active Oy"],
        "website_normalized_url": ["https://active.example"],
    }
    assert result.metadata["object_key"] == output_key
    assert result.metadata["row_count"] == 1
    assert log_messages == [
        f"Building Finland XBRL company seed parquet from {input_key}",
        "Finland XBRL company seed parquet built: row_count=1",
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_build_finland_xbrl_company_seed_parquet_filters_active_companies_with_websites -q
```

Expected: fail because `build_finland_xbrl_company_seed_parquet` does not exist.

- [ ] **Step 3: Implement company seed parquet**

Rename the file:

```bash
git mv src/dagster_v3/defs/finland_xbrl/assets/company_seed_duckdb.py src/dagster_v3/defs/finland_xbrl/assets/company_seed.py
```

Replace its contents with:

```python
from typing import Callable

import dagster as dg
import pyarrow as pa
import pyarrow.compute as pc
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import FINLAND_XBRL_DUCKDB_POOL
from dagster_v3.defs.finland_xbrl.assets.parquet_io import read_xbrl_parquet, write_xbrl_parquet

YTJ_ALL_COMPANIES_PARQUET_KEY = "ytj/all_companies.parquet"
COMPANY_SEED_PARQUET_KEY = "xbrl/company_seed/eligible_companies.parquet"


def build_finland_xbrl_company_seed_parquet(
    *,
    object_store: ObjectStoreResource,
    input_key: str = YTJ_ALL_COMPANIES_PARQUET_KEY,
    output_key: str = COMPANY_SEED_PARQUET_KEY,
    log_info: Callable[[str], None] | None = None,
) -> dg.MaterializeResult:
    if log_info is not None:
        log_info(f"Building Finland XBRL company seed parquet from {input_key}")
    companies = read_xbrl_parquet(object_store, key=input_key)
    active = pc.equal(companies["is_active"], pa.scalar(True))
    has_website = pc.not_equal(
        pc.fill_null(companies["website_normalized_url"], ""),
        "",
    )
    filtered = companies.filter(pc.and_(active, has_website)).select(
        ["business_id", "primary_name", "website_normalized_url"]
    )
    write_xbrl_parquet(object_store, key=output_key, table=filtered)
    if log_info is not None:
        log_info(f"Finland XBRL company seed parquet built: row_count={filtered.num_rows}")
    return dg.MaterializeResult(
        metadata={
            "object_key": output_key,
            "row_count": filtered.num_rows,
        }
    )


@dg.asset(
    name="finland_xbrl_company_seed",
    group_name="finland_xbrl",
    deps=["finland_ytj_all_companies_parquet"],
    kinds={"python", "parquet", "s3"},
    pool=FINLAND_XBRL_DUCKDB_POOL,
)
def finland_xbrl_company_seed(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return build_finland_xbrl_company_seed_parquet(
        object_store=object_store,
        log_info=context.log.info,
    )
```

- [ ] **Step 4: Update exports and deps**

Modify `src/dagster_v3/defs/finland_xbrl/assets/__init__.py` imports/exports:

```python
from dagster_v3.defs.finland_xbrl.assets.company_seed import (
    COMPANY_SEED_PARQUET_KEY,
    YTJ_ALL_COMPANIES_PARQUET_KEY,
    build_finland_xbrl_company_seed_parquet,
    finland_xbrl_company_seed,
)
```

Remove `finland_xbrl_company_seed_duckdb` and `build_finland_xbrl_company_seed_duckdb` from imports, `__all__`, and `defs.assets`.

Add:

```python
"COMPANY_SEED_PARQUET_KEY",
"YTJ_ALL_COMPANIES_PARQUET_KEY",
"build_finland_xbrl_company_seed_parquet",
"finland_xbrl_company_seed",
```

Register `finland_xbrl_company_seed` in `defs.assets`.

- [ ] **Step 5: Run test**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_build_finland_xbrl_company_seed_parquet_filters_active_companies_with_websites -q
```

Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets/company_seed.py src/dagster_v3/defs/finland_xbrl/assets/__init__.py tests/test_finland_xbrl_assets.py
git add -u src/dagster_v3/defs/finland_xbrl/assets/company_seed_duckdb.py
git commit -m "refactor: write Finland XBRL company seed as parquet"
```

---

### Task 4: Financial Reports Partition Parquet

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/financial_reports.py`
- Modify: `tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Write failing test for financial report parquet output**

Add test:

```python
def test_financial_reports_window_writes_partition_parquet(tmp_path: Path) -> None:
    object_store, _s3_client = _object_store()
    session = FakePagedFinancialReportsSession()
    result = xbrl_assets.materialize_financial_reports_partition_parquet(
        object_store=object_store,
        registered_date_start="2026-06-01",
        registered_date_end="2026-06-30",
        partition_key="2026-06-01",
        run_id="test-run",
        session=session,
        request_delay_seconds=0,
    )

    table = xbrl_assets.read_xbrl_parquet(
        object_store,
        key="xbrl/financial_reports/partition=2026-06-01.parquet",
    )
    assert table.to_pydict()["business_id"] == ["active-web", "second-active-web"]
    assert result.metadata["object_key"] == "xbrl/financial_reports/partition=2026-06-01.parquet"
    assert result.metadata["row_count"] == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_financial_reports_window_writes_partition_parquet -q
```

Expected: fail because `materialize_financial_reports_partition_parquet` does not exist.

- [ ] **Step 3: Implement financial reports parquet builder**

In `src/dagster_v3/defs/finland_xbrl/assets/financial_reports.py`, add:

```python
import pyarrow as pa

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.parquet_io import partition_parquet_key, write_xbrl_parquet
```

Add helper:

```python
def materialize_financial_reports_partition_parquet(
    *,
    object_store: ObjectStoreResource,
    registered_date_start: str,
    registered_date_end: str,
    partition_key: str,
    run_id: str,
    session: HttpSession | None = None,
    request_delay_seconds: float = DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    max_retries: int = DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
    retry_initial_delay_seconds: float = DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
    retry_max_delay_seconds: float = DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
    log_info: Callable[[str], None] | None = None,
) -> dg.MaterializeResult:
    source = finland_xbrl_financial_reports_source(
        registered_date_start=registered_date_start,
        registered_date_end=registered_date_end,
        run_id=run_id,
        session=session,
        request_delay_seconds=request_delay_seconds,
        max_retries=max_retries,
        retry_initial_delay_seconds=retry_initial_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        log_info=log_info,
    )
    rows = list(source.resources[XBRL_DLT_FINANCIAL_REPORTS_TABLE])
    table = pa.Table.from_pylist(rows) if rows else pa.table(
        {
            "business_id": pa.array([], pa.string()),
            "financial_date": pa.array([], pa.string()),
            "registration_date": pa.array([], pa.string()),
            "discovery_registered_date_start": pa.array([], pa.string()),
            "discovery_registered_date_end": pa.array([], pa.string()),
            "source_run_id": pa.array([], pa.string()),
            "source_page_number": pa.array([], pa.int64()),
            "source_page_record_number": pa.array([], pa.int64()),
            "source_record_number": pa.array([], pa.int64()),
            "source_payload_sha256": pa.array([], pa.string()),
        }
    )
    key = partition_parquet_key("financial_reports", partition_key)
    write_xbrl_parquet(object_store, key=key, table=table)
    return dg.MaterializeResult(
        metadata={
            "partition": partition_key,
            "registered_date_start": registered_date_start,
            "registered_date_end": registered_date_end,
            "object_key": key,
            "row_count": table.num_rows,
        }
    )
```

- [ ] **Step 4: Update Dagster partitioned assets**

Change `finland_xbrl_financial_reports_backfill_duckdb` and `finland_xbrl_financial_reports_incremental_duckdb` into parquet assets:

```python
name="finland_xbrl_financial_reports_backfill"
kinds={"python", "parquet", "s3"}
```

and:

```python
name="finland_xbrl_financial_reports_incremental"
kinds={"python", "parquet", "s3"}
```

Asset bodies should call `materialize_financial_reports_partition_parquet` with `object_store` and `context.partition_key`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_financial_reports_window_writes_partition_parquet tests/test_finland_xbrl_assets.py::test_xbrl_dlt_source_pages_financial_report_listing -q
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets/financial_reports.py tests/test_finland_xbrl_assets.py
git commit -m "refactor: write Finland XBRL financial reports as partition parquet"
```

---

### Task 5: Eligible Financial Reports PyArrow Asset

**Files:**
- Create: `src/dagster_v3/defs/finland_xbrl/assets/eligible_financial_reports.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`
- Test: `tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Write failing test for eligible report parquet join**

Add:

```python
def test_eligible_financial_reports_parquet_joins_reports_to_company_seed() -> None:
    object_store, _s3_client = _object_store()
    xbrl_assets.write_xbrl_parquet(
        object_store,
        key="xbrl/financial_reports/partition=2026-06-01.parquet",
        table=pa.table(
            {
                "business_id": ["active-web", "missing-web"],
                "financial_date": ["2026-05-31", "2026-05-31"],
                "registration_date": ["2026-06-01", "2026-06-01"],
                "discovery_registered_date_start": ["2026-06-01", "2026-06-01"],
                "discovery_registered_date_end": ["2026-06-30", "2026-06-30"],
            }
        ),
    )
    xbrl_assets.write_xbrl_parquet(
        object_store,
        key=xbrl_assets.COMPANY_SEED_PARQUET_KEY,
        table=pa.table(
            {
                "business_id": ["active-web"],
                "primary_name": ["Active Oy"],
                "website_normalized_url": ["https://active.example"],
            }
        ),
    )

    result = xbrl_assets.build_eligible_financial_reports_parquet(
        object_store=object_store,
        partition_key="2026-06-01",
    )
    output = xbrl_assets.read_xbrl_parquet(
        object_store,
        key="xbrl/eligible_financial_reports/partition=2026-06-01.parquet",
    )

    assert output.to_pydict()["business_id"] == ["active-web"]
    assert output.to_pydict()["primary_name"] == ["Active Oy"]
    assert result.metadata["row_count"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_eligible_financial_reports_parquet_joins_reports_to_company_seed -q
```

Expected: fail because `build_eligible_financial_reports_parquet` does not exist.

- [ ] **Step 3: Implement eligible reports asset**

Create `src/dagster_v3/defs/finland_xbrl/assets/eligible_financial_reports.py`:

```python
import dagster as dg
import pyarrow.compute as pc

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl.assets.common import DAILY_PARTITIONS, BACKFILL_PARTITIONS
from dagster_v3.defs.finland_xbrl.assets.company_seed import COMPANY_SEED_PARQUET_KEY, finland_xbrl_company_seed
from dagster_v3.defs.finland_xbrl.assets.financial_reports import (
    finland_xbrl_financial_reports_backfill,
    finland_xbrl_financial_reports_incremental,
)
from dagster_v3.defs.finland_xbrl.assets.parquet_io import (
    partition_parquet_key,
    read_xbrl_parquet,
    write_xbrl_parquet,
)


def build_eligible_financial_reports_parquet(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
) -> dg.MaterializeResult:
    reports_key = partition_parquet_key("financial_reports", partition_key)
    output_key = partition_parquet_key("eligible_financial_reports", partition_key)
    reports = read_xbrl_parquet(object_store, key=reports_key)
    companies = read_xbrl_parquet(object_store, key=COMPANY_SEED_PARQUET_KEY)
    joined = reports.join(
        companies,
        keys="business_id",
        join_type="inner",
    )
    sorted_joined = joined.sort_by([("registration_date", "ascending"), ("financial_date", "ascending"), ("business_id", "ascending")])
    write_xbrl_parquet(object_store, key=output_key, table=sorted_joined)
    return dg.MaterializeResult(
        metadata={
            "partition": partition_key,
            "reports_object_key": reports_key,
            "company_seed_object_key": COMPANY_SEED_PARQUET_KEY,
            "object_key": output_key,
            "row_count": sorted_joined.num_rows,
        }
    )
```

Add assets:

```python
@dg.asset(
    name="finland_xbrl_eligible_financial_reports_backfill",
    group_name="finland_xbrl",
    deps=[finland_xbrl_financial_reports_backfill, finland_xbrl_company_seed],
    partitions_def=BACKFILL_PARTITIONS,
    kinds={"python", "pyarrow", "parquet", "s3"},
)
def finland_xbrl_eligible_financial_reports_backfill(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    context.log.info("Building eligible Finland XBRL financial reports for %s", context.partition_key)
    return build_eligible_financial_reports_parquet(
        object_store=object_store,
        partition_key=context.partition_key,
    )


@dg.asset(
    name="finland_xbrl_eligible_financial_reports_incremental",
    group_name="finland_xbrl",
    deps=[finland_xbrl_financial_reports_incremental, finland_xbrl_company_seed],
    partitions_def=DAILY_PARTITIONS,
    kinds={"python", "pyarrow", "parquet", "s3"},
)
def finland_xbrl_eligible_financial_reports_incremental(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    context.log.info("Building eligible Finland XBRL financial reports for %s", context.partition_key)
    return build_eligible_financial_reports_parquet(
        object_store=object_store,
        partition_key=context.partition_key,
    )
```

- [ ] **Step 4: Export/register assets**

Modify `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`:

```python
from dagster_v3.defs.finland_xbrl.assets.eligible_financial_reports import (
    build_eligible_financial_reports_parquet,
    finland_xbrl_eligible_financial_reports_backfill,
    finland_xbrl_eligible_financial_reports_incremental,
)
```

Add all three names to `__all__` and both assets to `defs.assets`.

- [ ] **Step 5: Run test**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_eligible_financial_reports_parquet_joins_reports_to_company_seed -q
```

Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets/eligible_financial_reports.py src/dagster_v3/defs/finland_xbrl/assets/__init__.py tests/test_finland_xbrl_assets.py
git commit -m "feat: build Finland XBRL eligible reports with PyArrow"
```

---

### Task 6: Raw XML Reads Eligible Reports Parquet

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/raw_xml_documents.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/jobs.py`
- Modify: `tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Write failing test for raw XML parquet input**

Replace `test_load_eligible_financial_report_rows_filters_by_registration_window_only` with:

```python
def test_load_eligible_financial_report_rows_reads_partition_parquet() -> None:
    object_store, _s3_client = _object_store()
    xbrl_assets.write_xbrl_parquet(
        object_store,
        key="xbrl/eligible_financial_reports/partition=2026-06-01.parquet",
        table=pa.table(
            {
                "business_id": ["active-web"],
                "financial_date": ["2026-05-31"],
                "registration_date": ["2026-06-01"],
                "discovery_registered_date_start": ["2026-06-01"],
                "discovery_registered_date_end": ["2026-06-30"],
            }
        ),
    )

    rows = xbrl_assets.load_eligible_financial_report_rows(
        object_store=object_store,
        partition_key="2026-06-01",
    )

    assert rows == [
        {
            "business_id": "active-web",
            "financial_date": "2026-05-31",
            "registration_date": "2026-06-01",
            "discovery_registered_date_start": "2026-06-01",
            "discovery_registered_date_end": "2026-06-30",
        }
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_load_eligible_financial_report_rows_reads_partition_parquet -q
```

Expected: fail because `load_eligible_financial_report_rows` still expects DuckDB/window arguments.

- [ ] **Step 3: Update raw XML loader**

In `src/dagster_v3/defs/finland_xbrl/assets/raw_xml_documents.py`, replace the DuckDB loader with:

```python
def load_eligible_financial_report_rows(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
) -> list[dict[str, Any]]:
    table = read_xbrl_parquet(
        object_store,
        key=partition_parquet_key("eligible_financial_reports", partition_key),
    )
    return table.to_pylist()
```

Update imports:

```python
from dagster_v3.defs.finland_xbrl.assets.parquet_io import partition_parquet_key, read_xbrl_parquet
from dagster_v3.defs.finland_xbrl.assets.eligible_financial_reports import (
    finland_xbrl_eligible_financial_reports_backfill,
    finland_xbrl_eligible_financial_reports_incremental,
)
```

Update `_materialize_raw_xml_window` to call:

```python
financial_reports = load_eligible_financial_report_rows(
    object_store=object_store,
    partition_key=context.partition_key,
)
```

Remove `source_duckdb` from `_materialize_raw_xml_window`, `finland_xbrl_raw_xml_documents_backfill`, and `finland_xbrl_raw_xml_documents_incremental`.

- [ ] **Step 4: Update raw XML deps**

Change backfill raw XML deps to:

```python
deps=[finland_xbrl_eligible_financial_reports_backfill]
```

Change incremental raw XML deps to:

```python
deps=[finland_xbrl_eligible_financial_reports_incremental]
```

- [ ] **Step 5: Update job selection**

Modify `src/dagster_v3/defs/finland_xbrl/assets/jobs.py`:

```python
selection=dg.AssetSelection.assets(
    "finland_xbrl_company_seed",
    "finland_xbrl_financial_reports_backfill",
    "finland_xbrl_eligible_financial_reports_backfill",
    "finland_xbrl_raw_xml_documents_backfill",
    "finland_xbrl_parse_backfill",
)
```

and incremental:

```python
selection=dg.AssetSelection.assets(
    "finland_xbrl_company_seed",
    "finland_xbrl_financial_reports_incremental",
    "finland_xbrl_eligible_financial_reports_incremental",
    "finland_xbrl_raw_xml_documents_incremental",
    "finland_xbrl_parse_incremental",
)
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_load_eligible_financial_report_rows_reads_partition_parquet tests/test_finland_xbrl_assets.py::test_xbrl_asset_graph_models_xml_documents_catalog_as_bridge tests/test_finland_xbrl_assets.py::test_finland_xbrl_jobs_and_incremental_schedule_registered -q
```

Expected: 3 passed.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets/raw_xml_documents.py src/dagster_v3/defs/finland_xbrl/assets/jobs.py tests/test_finland_xbrl_assets.py
git commit -m "refactor: feed Finland XBRL raw XML from parquet reports"
```

---

### Task 7: Remove First-Slice dbt/DuckDB Edges From Active Graph

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/dbt/models/sources.yml`
- Modify: `tests/test_finland_xbrl_assets.py`

- [ ] **Step 1: Write failing graph test**

Update `test_xbrl_transforms_are_dbt_assets` to assert the first-slice eligible reports asset is no longer dbt-backed:

```python
def test_xbrl_first_slice_uses_python_parquet_assets_not_dbt_sources():
    graph = load_project_defs().get_repository_def().asset_graph
    keys = {k.path[-1] for k in graph.get_all_asset_keys()}

    assert "finland_xbrl_eligible_financial_reports_backfill" in keys
    assert "finland_xbrl_eligible_financial_reports_incremental" in keys
    assert "finland_xbrl_eligible_financial_reports" not in keys
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_xbrl_first_slice_uses_python_parquet_assets_not_dbt_sources -q
```

Expected: fail because dbt still registers `finland_xbrl_eligible_financial_reports`.

- [ ] **Step 3: Remove eligible model from dbt registration for first slice**

Delete `src/dagster_v3/defs/finland_xbrl/dbt/models/eligible_financial_reports.sql`.

Remove this model test block from `sources.yml`:

```yaml
  - name: eligible_financial_reports
    columns:
      - name: business_id
        data_tests: [not_null]
```

Keep source table metadata for financial reports and parsed XBRL tables only if later dbt metric assets still need them.

- [ ] **Step 4: Regenerate dbt manifest**

Run:

```bash
uv run dbt parse --project-dir src/dagster_v3/defs/finland_xbrl/dbt --profiles-dir src/dagster_v3/defs/finland_xbrl/dbt
```

Expected: command exits 0.

- [ ] **Step 5: Run graph test**

Run:

```bash
uv run pytest tests/test_finland_xbrl_assets.py::test_xbrl_first_slice_uses_python_parquet_assets_not_dbt_sources -q
```

Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/dbt/models/sources.yml tests/test_finland_xbrl_assets.py
git add -u src/dagster_v3/defs/finland_xbrl/dbt/models/eligible_financial_reports.sql
git commit -m "refactor: remove Finland XBRL eligible reports from dbt"
```

---

### Task 8: Full Verification

**Files:**
- No source edits unless verification exposes a bug.

- [ ] **Step 1: Run Finland-focused tests**

Run:

```bash
uv run pytest tests/test_finland_ytj_assets.py tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_dbt.py tests/test_finland_xbrl_parsed_assets.py -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run lint**

Run:

```bash
uv run ruff check src/dagster_v3/defs/finland_ytj src/dagster_v3/defs/finland_xbrl tests/test_finland_ytj_assets.py tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_parsed_assets.py
```

Expected: `All checks passed!`

- [ ] **Step 3: Run Dagster definition check**

Run:

```bash
uv run dg check defs
```

Expected:

```text
All component YAML validated successfully.
All definitions loaded successfully.
```

- [ ] **Step 4: Inspect graph for removed DuckDB exchange**

Run:

```bash
rg -n "finland_xbrl_company_seed_duckdb|finland_xbrl_eligible_companies|load_eligible_financial_report_rows\\(.*source_duckdb|XBRL_ELIGIBLE_COMPANIES_TABLE" src/dagster_v3/defs/finland_xbrl
```

Expected:
- no `finland_xbrl_company_seed_duckdb`
- no `finland_xbrl_eligible_companies`
- no raw XML loader call requiring `source_duckdb`
- no `XBRL_ELIGIBLE_COMPANIES_TABLE`

- [ ] **Step 5: Final commit if verification fixes were needed**

If verification required fixes:

```bash
git add src/dagster_v3/defs/finland_ytj src/dagster_v3/defs/finland_xbrl tests/test_finland_ytj_assets.py tests/test_finland_xbrl_assets.py tests/test_finland_xbrl_parsed_assets.py
git commit -m "test: verify Finland XBRL parquet boundary migration"
```

If no fixes were needed, do not create an empty commit.

---

## Self-Review

- Spec coverage: The plan covers the approved first slice: shared parquet IO, YTJ parquet source, XBRL company seed parquet, financial reports parquet, eligible financial reports parquet, raw XML parquet input, dbt/DuckDB edge removal, and verification.
- Placeholder scan: The plan uses concrete file paths, commands, function names, object keys, and expected outputs throughout.
- Type consistency: Helper names use `*_parquet`; asset keys drop `_duckdb` where the output is parquet; partition object keys consistently use `partition_parquet_key(asset_name, partition_key)`.
- Scope control: Parsed XBRL, metrics conversion, ClickHouse export, and final DuckDB/inspection assets are intentionally outside this first implementation slice.
