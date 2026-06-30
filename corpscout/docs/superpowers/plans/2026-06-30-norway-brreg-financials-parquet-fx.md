# Norway Brreg Financials Parquet And FX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy Norway Brreg financial DuckDB path with explicit parquet assets for raw fetches, original-currency normalized statements, USD conversion, and ClickHouse publish.

**Architecture:** The full snapshot path runs once from the normalized Brreg entity snapshot and writes durable financial parquet objects. The daily path only processes org numbers affected by that day's entity update partition. ClickHouse is the only full current database view; parquet assets remain partition-scoped source and transform outputs.

**Tech Stack:** Dagster assets/jobs/schedules, Polars/PyArrow parquet, S3-compatible `ObjectStoreResource`, `NorwayBrregApiResource`, shared `ExchangeRateClient`, `dagster_clickhouse.ClickhouseResource`, ClickHouse `corpscout.no_financial_statements`.

---

## File Structure

- Create `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_storage.py`
  - Owns S3 object keys and read/write methods for Norway financial parquet assets.
- Modify `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_fetches.py`
  - Keep row builders and HTTP status handling, add financial-update candidate handling, and remove DuckDB candidate/state behavior from the new path.
- Modify `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py`
  - Split original-currency statement normalization from USD conversion.
- Modify `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/entity_updates.py`
  - Ensure daily entity update downloads include Brreg change details so financial candidates can be derived without a second entity update API call.
- Create `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_fetches.py`
  - Adds raw financial fetch snapshot/update parquet assets.
- Create `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_statements.py`
  - Adds original-currency and USD statement parquet assets.
- Create `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_clickhouse.py`
  - Adds snapshot replace and daily update publish assets for `corpscout.no_financial_statements`.
- Modify `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py`
  - Exports new assets/jobs and stops exporting old financial DuckDB assets after migration.
- Modify `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py`
  - Registers the new financial storage resource, assets, and jobs.
- Modify `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/translation.py`
  - Adds explicit financial jobs or moves job definitions to a clearer `assets/jobs.py` if the file becomes too mixed.
- Modify tests:
  - `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_fetches.py`
  - `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py`
  - `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py`
  - `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_workflows.py`
  - Add `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_storage.py`
  - Add `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_clickhouse.py`

## Asset Contract

New asset names:

- `norway_brreg_financial_fetches_snapshot_parquet`
- `norway_brreg_financial_update_candidates_parquet`
- `norway_brreg_financial_fetches_updates_parquet`
- `norway_brreg_financial_statements_snapshot_parquet`
- `norway_brreg_financial_statements_updates_parquet`
- `norway_brreg_financial_statements_snapshot_usd_parquet`
- `norway_brreg_financial_statements_updates_usd_parquet`
- `norway_brreg_financial_statements_snapshot_clickhouse`
- `norway_brreg_financial_statements_updates_clickhouse`

New jobs:

- `norway_brreg_financials_full_snapshot_job`
- `norway_brreg_daily_update_job`

Selection rules:

- Snapshot financial fetch candidates come from `norway_brreg_entities_snapshot_no_companies_parquet`.
- Snapshot candidates require `is_active = true`.
- Snapshot candidates require `last_submitted_accounts_year` because the initial financial seed is per org/year, not a public bulk financial download.
- Snapshot candidates do not require website.
- Daily financial update candidates come from `norway_brreg_financial_update_candidates_parquet`, which reads the raw entity update parquet produced with `includeChanges=true` and keeps only updates where `endringer[].path == "/sisteInnsendteAarsregnskap"`.
- Company update assets and financial update assets run in one combined daily job, and both branches share the same raw entity update download. Do not make a second Brreg entity update API call for financials.
- Removed companies from the entity update branch still cause entity table deletes, but they do not drive financial fetches.
- Financial fetch uses no year filter, so Brreg can return all available annual accounts for the org.
- Initial financial seed is resumable at `(org_number, last_submitted_accounts_year)`: if raw fetch data for that key already exists in S3, skip that org/year instead of updating existing raw data.
- Daily financial update candidates are refreshes: if `/sisteInnsendteAarsregnskap` changes for an org, fetch it and overwrite/update the S3 raw fetch for that org/year.
- The daily financial path has no "skip existing raw fetch" guard. The Brreg update feed is the signal that the source changed, so the raw fetch object for that `(org_number, last_submitted_accounts_year)` key is replaced.

Error rules:

- HTTP 404/410 and valid empty-list responses are source outcomes and stay as fetch status rows.
- Persistent 429/5xx/network/invalid payload statuses are written to raw fetch parquet, then downstream normalization fails with counts and sample org numbers. This preserves audit data without silently publishing partial financial data.
- Missing FX rates fail the USD asset, matching the Finland XBRL standard.

---

### Task 1: Financial Storage Resource

**Files:**
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_storage.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_storage.py`

- [ ] **Step 1: Write storage path tests**

```python
from dagster_v3.defs.norway_brreg.financial_storage import (
    financial_fetches_snapshot_object_key,
    financial_fetches_update_object_key,
    financial_raw_fetch_object_key,
    financial_update_candidates_object_key,
    financial_statements_snapshot_object_key,
    financial_statements_update_object_key,
    financial_statements_usd_snapshot_object_key,
    financial_statements_usd_update_object_key,
)


def test_norway_financial_storage_object_keys_are_stable() -> None:
    assert financial_fetches_snapshot_object_key() == (
        "norway_brreg/financial/fetches/snapshot/financial_fetches.parquet"
    )
    assert financial_fetches_update_object_key("2026-06-30") == (
        "norway_brreg/financial/fetches/updates/date=2026-06-30/financial_fetches.parquet"
    )
    assert financial_raw_fetch_object_key("923609016", "2025") == (
        "norway_brreg/financial/raw_fetches/org=923609016/year=2025/financial_fetch.parquet"
    )
    assert financial_update_candidates_object_key("2026-06-30") == (
        "norway_brreg/financial/update_candidates/date=2026-06-30/financial_update_candidates.parquet"
    )
    assert financial_statements_snapshot_object_key() == (
        "norway_brreg/financial/statements/snapshot/financial_statements.parquet"
    )
    assert financial_statements_update_object_key("2026-06-30") == (
        "norway_brreg/financial/statements/updates/date=2026-06-30/financial_statements.parquet"
    )
    assert financial_statements_usd_snapshot_object_key() == (
        "norway_brreg/financial/statements_usd/snapshot/financial_statements.parquet"
    )
    assert financial_statements_usd_update_object_key("2026-06-30") == (
        "norway_brreg/financial/statements_usd/updates/date=2026-06-30/financial_statements.parquet"
    )
```

- [ ] **Step 2: Run the failing storage test**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_storage.py -q
```

Expected: fails because `financial_storage.py` does not exist.

- [ ] **Step 3: Implement the storage resource**

Add `NorwayBrregFinancialParquetStorageResource` with methods:

```python
class NorwayBrregFinancialParquetStorageResource(dg.ConfigurableResource):
    _object_store: Any = PrivateAttr()

    def __init__(self, object_store: object | None = None, **data: object) -> None:
        super().__init__(**data)
        self._object_store = object_store or ObjectStoreResource()

    def raw_fetch_exists(self, org_number: str, accounts_year: str) -> bool:
        return self._object_store.exists(
            financial_raw_fetch_object_key(org_number, accounts_year),
            bucket=NORWAY_BRREG_ENTITY_BUCKET,
        )

    def write_raw_fetch(
        self,
        org_number: str,
        accounts_year: str,
        frame: pl.DataFrame,
        *,
        overwrite: bool,
    ) -> str:
        key = financial_raw_fetch_object_key(org_number, accounts_year)
        if not overwrite and self._object_store.exists(key, bucket=NORWAY_BRREG_ENTITY_BUCKET):
            return key
        return self._write_frame(key, frame)

    def write_snapshot_fetches(self, frame: pl.DataFrame) -> str:
        return self._write_frame(financial_fetches_snapshot_object_key(), frame)

    def write_update_fetches(self, partition_date: str, frame: pl.DataFrame) -> str:
        return self._write_frame(financial_fetches_update_object_key(partition_date), frame)

    def read_snapshot_fetches(self) -> pl.DataFrame:
        return self._read_frame(financial_fetches_snapshot_object_key())

    def read_update_fetches(self, partition_date: str) -> pl.DataFrame:
        return self._read_frame(financial_fetches_update_object_key(partition_date))

    def write_update_financial_candidates(self, partition_date: str, frame: pl.DataFrame) -> str:
        return self._write_frame(financial_update_candidates_object_key(partition_date), frame)

    def read_update_financial_candidates(self, partition_date: str) -> pl.DataFrame:
        return self._read_frame(financial_update_candidates_object_key(partition_date))
```

Also add equivalent read/write methods for original statements and USD statements.

- [ ] **Step 4: Run storage tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_storage.py -q
```

Expected: pass.

- [ ] **Step 5: Commit storage resource**

```bash
git add companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_storage.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_storage.py
git commit -m "Add Norway financial parquet storage resource"
```

### Task 2: Raw Financial Fetch Rows Without DuckDB State

**Files:**
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_fetches.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_fetches.py`

- [ ] **Step 1: Add tests for candidate selection and statuses**

Add tests proving:

```python
def test_snapshot_financial_candidates_use_active_companies_without_website_filter() -> None:
    frame = pl.DataFrame(
        [
            {"org_number": "1", "name": "Active No Website", "is_active": True, "primary_website_url": None, "last_submitted_accounts_year": "2025"},
            {"org_number": "2", "name": "Active Website", "is_active": True, "primary_website_url": "https://x.no", "last_submitted_accounts_year": "2025"},
            {"org_number": "3", "name": "Inactive", "is_active": False, "primary_website_url": "https://y.no", "last_submitted_accounts_year": "2025"},
            {"org_number": "4", "name": "No Accounts", "is_active": True, "primary_website_url": None, "last_submitted_accounts_year": None},
        ]
    )

    rows = financial_fetch_candidates_from_no_companies(frame)

    assert [row["org_number"] for row in rows] == ["1", "2"]


def test_transport_failure_status_is_retryable_failure_for_downstream_guard() -> None:
    row = financial_fetch_failure_row(
        org={"org_number": "999", "legal_name": "Broken AS"},
        source_url="https://data.brreg.no/regnskapsregisteret/regnskap/999",
        source_run_id="run-1",
        source_line_number=1,
        status_code=500,
        fetch_status=FINANCIAL_FETCH_STATUS_SERVER_ERROR,
        error_type="HTTPStatusError",
        error_message="HTTP 500",
        fetched_at="2026-06-30T00:00:00.000Z",
        attempt_count=5,
        raw_response="server error",
    )

    assert row["fetch_status"] == "server_error"
    assert financial_fetch_status_requires_failure(row["fetch_status"]) is True
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_fetches.py -q
```

Expected: new helper functions are missing.

- [ ] **Step 3: Implement helpers**

Add:

```python
SOURCE_OUTCOME_FETCH_STATUSES = {
    FINANCIAL_FETCH_STATUS_SUCCESS,
    FINANCIAL_FETCH_STATUS_NOT_FOUND,
    "gone",
    "empty",
}


def financial_fetch_status_requires_failure(fetch_status: str) -> bool:
    return fetch_status not in SOURCE_OUTCOME_FETCH_STATUSES


def financial_fetch_candidates_from_no_companies(frame: pl.DataFrame) -> list[dict[str, Any]]:
    if frame.is_empty():
        return []
    return [
        {
            "org_number": str(row["org_number"]),
            "legal_name": str(row["name"] or ""),
            "website": str(row["primary_website_url"] or ""),
            "last_submitted_accounts_year": str(row["last_submitted_accounts_year"] or ""),
        }
        for row in frame
        .filter((pl.col("is_active") == True) & pl.col("last_submitted_accounts_year").is_not_null())
        .sort("org_number")
        .to_dicts()
    ]
```

Keep `_fetch_brreg_financial_statement` behavior reusable, but make the public fetch function accept an iterable of org dicts instead of reading and mutating DuckDB.

- [ ] **Step 4: Run fetch tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_fetches.py -q
```

Expected: pass.

- [ ] **Step 5: Commit fetch row helpers**

```bash
git add companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_fetches.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_fetches.py
git commit -m "Refactor Norway financial fetch rows for parquet assets"
```

### Task 3: Financial Update Candidates And Raw Financial Fetch Parquet Assets

**Files:**
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/entity_updates.py`
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_fetches.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_entity_snapshot_asset.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_workflows.py`

- [ ] **Step 1: Add tests for entity update change details and new raw fetch assets**

In `test_norway_brreg_entity_snapshot_asset.py`, extend `test_entity_updates_asset_writes_changed_records_as_daily_parquet_to_s3`:

```python
assert api.kwargs is not None
assert api.kwargs["include_changes"] is True
assert callable(api.kwargs["log"])
```

This proves the existing entity update parquet contains `raw_update.endringer`, which is the only trigger source for daily financial fetches.

In `test_norway_brreg_definitions.py`, add assertions that all raw financial update assets are registered and have the expected parents:

```python
assert "norway_brreg_financial_fetches_snapshot_parquet" in asset_names
assert "norway_brreg_financial_update_candidates_parquet" in asset_names
assert "norway_brreg_financial_fetches_updates_parquet" in asset_names
```

Expected parent sets:

```python
snapshot parents == {"norway_brreg_entities_snapshot_no_companies_parquet"}
financial_update_candidates parents == {"norway_brreg_entity_updates_s3"}
financial_fetch_updates parents == {"norway_brreg_financial_update_candidates_parquet"}
```

- [ ] **Step 2: Run graph tests and verify failure**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_entity_snapshot_asset.py tests/test_norway_brreg_definitions.py -q
```

Expected: fails because the entity update asset is not passing `include_changes=True`, and financial assets are not registered.

- [ ] **Step 3: Make daily entity updates include change details**

Update `norway_brreg_entity_updates_s3` so the one shared daily entity update API call fetches Brreg change details:

```python
records = list(
    norway_brreg_api.iter_updated_entities(
        start=updated_at_start,
        end=updated_at_end,
        include_changes=True,
        log=context.log.info,
    )
)
```

Do not add a second entity update API call in the financial pipeline.

- [ ] **Step 4: Implement snapshot fetch asset**

Core behavior:

```python
@dg.asset(
    name="norway_brreg_financial_fetches_snapshot_parquet",
    deps=[dg.AssetKey("norway_brreg_entities_snapshot_no_companies_parquet")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
)
def norway_brreg_financial_fetches_snapshot_parquet(
    context,
    norway_brreg_api: NorwayBrregApiResource,
    norway_brreg_entity_storage: NorwayBrregEntityParquetStorageResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    companies = norway_brreg_entity_storage.read_normalized_snapshot_table("no_companies")
    candidates = financial_fetch_candidates_from_no_companies(companies)
    rows = []
    skipped_existing = 0
    downloaded = 0
    for candidate in candidates:
        org_number = candidate["org_number"]
        accounts_year = candidate["last_submitted_accounts_year"]
        if norway_brreg_financial_storage.raw_fetch_exists(org_number, accounts_year):
            skipped_existing += 1
            rows.append(reused_financial_fetch_catalog_row(candidate, source_run_id=context.run_id))
            continue
        fetch_rows = fetch_financial_rows_for_orgs(
            orgs=[candidate],
            api=norway_brreg_api,
            source_run_id=context.run_id,
            log=context.log.info,
        )
        fetch_frame = financial_fetch_rows_frame(fetch_rows)
        norway_brreg_financial_storage.write_raw_fetch(
            org_number,
            accounts_year,
            fetch_frame,
            overwrite=False,
        )
        rows.extend(fetch_rows)
        downloaded += 1
    frame = financial_fetch_rows_frame(rows)
    key = norway_brreg_financial_storage.write_snapshot_fetches(frame)
    return dg.MaterializeResult(
        metadata={
            "candidate_count": len(candidates),
            "downloaded_count": downloaded,
            "skipped_existing_count": skipped_existing,
            "row_count": frame.height,
            "s3_key": key,
        }
    )
```

The asset must log candidate count, downloaded count, skipped-existing count, status counts, and S3 key. This is a one-time seed that can be re-run safely until all `(org_number, last_submitted_accounts_year)` raw fetches exist.

- [ ] **Step 5: Implement financial update candidate asset**

Core behavior:

```python
@dg.asset(
    name="norway_brreg_financial_update_candidates_parquet",
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
    deps=[dg.AssetKey("norway_brreg_entity_updates_s3")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
)
def norway_brreg_financial_update_candidates_parquet(
    context,
    norway_brreg_entity_storage: NorwayBrregEntityParquetStorageResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    raw_updates = norway_brreg_entity_storage.read_raw_update_frame(partition_date)
    frame = financial_update_candidates_frame(raw_updates, source_run_id=context.run_id)
    key = norway_brreg_financial_storage.write_update_financial_candidates(partition_date, frame)
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "row_count": frame.height,
            "s3_key": key,
        }
    )
```

The source update asset must call `NorwayBrregApiResource.iter_updated_entities` with `include_changes=True` so `raw_update.endringer` is present in the parquet. This candidate asset extracts one row per org where the raw update contains `/sisteInnsendteAarsregnskap`, carrying `org_number`, `last_submitted_accounts_year`, source update id/date, and source run id. The candidate asset should be empty, not failing, when there are no financial-account changes in the partition.

- [ ] **Step 6: Implement update fetch asset**

Core behavior:

```python
@dg.asset(
    name="norway_brreg_financial_fetches_updates_parquet",
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
    deps=[dg.AssetKey("norway_brreg_financial_update_candidates_parquet")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
)
def norway_brreg_financial_fetches_updates_parquet(
    context,
    norway_brreg_api: NorwayBrregApiResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    candidates = norway_brreg_financial_storage.read_update_financial_candidates(partition_date)
    rows = []
    downloaded = 0
    for candidate in candidates.to_dicts():
        org_number = candidate["org_number"]
        accounts_year = candidate["last_submitted_accounts_year"]
        fetch_rows = fetch_financial_rows_for_orgs(
            orgs=[candidate],
            api=norway_brreg_api,
            source_run_id=context.run_id,
            log=context.log.info,
        )
        fetch_frame = financial_fetch_rows_frame(fetch_rows)
        norway_brreg_financial_storage.write_raw_fetch(
            org_number,
            accounts_year,
            fetch_frame,
            overwrite=True,
        )
        rows.extend(fetch_rows)
        downloaded += 1
    frame = financial_fetch_rows_frame(rows)
    key = norway_brreg_financial_storage.write_update_fetches(partition_date, frame)
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "candidate_count": candidates.height,
            "downloaded_count": downloaded,
            "row_count": frame.height,
            "s3_key": key,
        }
    )
```

This asset must not read `norway_brreg_entity_updates_affected_orgs_parquet`; only `/sisteInnsendteAarsregnskap` changes are financial fetch candidates. Unlike the initial seed, this daily asset intentionally overwrites the raw fetch object for each candidate `(org_number, last_submitted_accounts_year)` because Brreg has signaled that the accounts year changed.

- [ ] **Step 7: Register resource and assets**

In `definitions.py`, add:

```python
"norway_brreg_financial_storage": NorwayBrregFinancialParquetStorageResource(),
```

Register the financial update candidate asset and both raw fetch assets in `defs.assets`.

- [ ] **Step 8: Run graph tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_entity_snapshot_asset.py tests/test_norway_brreg_definitions.py tests/test_norway_brreg_workflows.py -q
```

Expected: pass for new raw fetch graph, with old financial DuckDB tests still passing until removal task.

- [ ] **Step 9: Commit raw fetch assets**

```bash
git add companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/entity_updates.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_fetches.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_entity_snapshot_asset.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_workflows.py
git commit -m "Add Norway financial fetch parquet assets"
```

### Task 4: Original-Currency Statement Parquet Assets

**Files:**
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py`
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_statements.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py`

- [ ] **Step 1: Add tests that original normalization has no USD dependency**

```python
def test_build_original_financial_statement_rows_does_not_require_exchange_rates() -> None:
    fetch_rows = [successful_financial_fetch_row_fixture()]

    rows = build_original_financial_statement_rows_from_fetch_rows(fetch_rows)

    assert rows[0]["operating_revenue_amount_original"] == Decimal("1000")
    assert "operating_revenue_amount_usd" not in rows[0]
    assert "fx_rate_to_usd" not in rows[0]
```

- [ ] **Step 2: Add tests for hard-failing retryable fetch statuses**

```python
def test_original_statement_normalization_fails_on_retryable_fetch_status() -> None:
    fetch_rows = [
        {"org_number": "999", "fetch_status": "server_error", "error_message": "HTTP 500"}
    ]

    with pytest.raises(ValueError, match="server_error"):
        build_original_financial_statement_rows_from_fetch_rows(fetch_rows)
```

- [ ] **Step 3: Run failing normalization tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_normalize.py -q
```

Expected: new functions are missing.

- [ ] **Step 4: Implement original normalization**

Split existing `_financial_statement_row` into:

```python
def build_original_financial_statement_rows_from_fetch_rows(
    fetch_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    failed_status_rows = [
        row for row in fetch_rows
        if financial_fetch_status_requires_failure(_string(row.get("fetch_status")))
    ]
    if failed_status_rows:
        sample = ", ".join(_string(row.get("org_number")) for row in failed_status_rows[:10])
        raise ValueError(
            f"Norway financial fetches contain retryable failures: "
            f"count={len(failed_status_rows)} sample_org_numbers={sample}"
        )
    rows: list[dict[str, Any]] = []
    for fetch_row in fetch_rows:
        if _string(fetch_row.get("fetch_status")) != FINANCIAL_FETCH_STATUS_SUCCESS:
            continue
        payload = json.loads(_string(fetch_row.get("raw_response")) or "[]")
        if not isinstance(payload, list):
            continue
        for line_number, record in enumerate(payload, start=1):
            if isinstance(record, dict):
                rows.append(
                    _original_financial_statement_row(
                        record,
                        org=fetch_row,
                        line_number=line_number,
                        run_id=_string(fetch_row.get("source_run_id")),
                        source_url=_string(fetch_row.get("source_url")),
                    )
                )
    return rows
```

Original rows must include source/audit fields, identity fields, period fields, currency, and `*_amount_original` fields. They must not include `*_amount_usd`, `fx_rate_to_usd`, `fx_rate_date`, or `fx_source`.

- [ ] **Step 5: Implement original statement assets**

Add snapshot/update assets that:

- read raw fetch parquet from `NorwayBrregFinancialParquetStorageResource`
- call `build_original_financial_statement_rows_from_fetch_rows`
- write original statement parquet
- log fetch rows, successful fetches, source outcome rows, output rows, min/max fiscal year

- [ ] **Step 6: Run normalization and graph tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_normalize.py tests/test_norway_brreg_definitions.py -q
```

Expected: pass.

- [ ] **Step 7: Commit original statement assets**

```bash
git add companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_statements.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py
git commit -m "Add Norway original financial statement parquet assets"
```

### Task 5: USD FX Statement Parquet Assets

**Files:**
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_statements.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py`

- [ ] **Step 1: Add USD conversion tests**

```python
def test_build_usd_financial_statement_rows_uses_period_end_date() -> None:
    exchange_rates = FakeExchangeRates(rate=Decimal("0.10"), source="test")
    original_rows = [original_statement_row_fixture(currency="NOK", period_end_date="2024-12-31")]

    rows = build_usd_financial_statement_rows(
        original_rows,
        exchange_rates=exchange_rates,
        converted_at="2026-06-30T00:00:00.000Z",
    )

    assert exchange_rates.requests == [("NOK", "2024-12-31")]
    assert rows[0]["operating_revenue_amount_original"] == Decimal("1000")
    assert rows[0]["operating_revenue_amount_usd"] == Decimal("100.00")
    assert rows[0]["fx_rate_to_usd"] == Decimal("0.10")
    assert rows[0]["fx_rate_date"] == "2024-12-31"
    assert rows[0]["fx_converted_at"] == "2026-06-30T00:00:00.000Z"
```

```python
def test_build_usd_financial_statement_rows_fails_on_missing_rate() -> None:
    with pytest.raises(LookupError, match="Missing NOK/USD exchange rates"):
        build_usd_financial_statement_rows(
            [original_statement_row_fixture(currency="NOK", period_end_date="2024-12-31")],
            exchange_rates=FakeExchangeRatesWithMissing(),
            converted_at="2026-06-30T00:00:00.000Z",
        )
```

- [ ] **Step 2: Run failing FX tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_normalize.py -q
```

Expected: new USD function is missing.

- [ ] **Step 3: Implement USD conversion**

Use the same policy as Finland:

```python
def build_usd_financial_statement_rows(
    original_rows: list[dict[str, Any]],
    *,
    exchange_rates: ExchangeRates,
    converted_at: str,
) -> list[dict[str, Any]]:
    requests = _usd_rate_requests(original_rows)
    rates = _load_required_usd_rates(exchange_rates, requests)
    return [
        _financial_statement_usd_row(row, rates, converted_at=converted_at)
        for row in original_rows
    ]
```

`_load_required_usd_rates` must raise `LookupError` for any missing `(currency, period_end_date)` pair.

- [ ] **Step 4: Implement USD parquet assets**

Snapshot asset depends on `norway_brreg_financial_statements_snapshot_parquet`.

Update asset depends on `norway_brreg_financial_statements_updates_parquet` and uses `NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS`.

Both assets:

- read original-currency parquet
- call `ExchangeRateClient.from_env()`
- write USD parquet
- return metadata: row count, distinct currency count, distinct FX rate date count, S3 key

- [ ] **Step 5: Run FX tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_normalize.py tests/test_norway_brreg_definitions.py -q
```

Expected: pass.

- [ ] **Step 6: Commit USD assets**

```bash
git add companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_statements.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py
git commit -m "Add Norway financial statement USD parquet assets"
```

### Task 6: ClickHouse Publish Assets

**Files:**
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_clickhouse.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_clickhouse.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py`

- [ ] **Step 1: Add ClickHouse publish tests**

Cover:

- snapshot publish calls replace for `corpscout.no_financial_statements`
- update publish deletes orgs present in the financial USD update parquet, then inserts replacement USD rows
- empty financial USD update parquet skips ClickHouse mutation

Use the existing patterns from `test_norway_brreg_entity_clickhouse.py`.

- [ ] **Step 2: Run failing ClickHouse tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_clickhouse.py -q
```

Expected: missing module/assets.

- [ ] **Step 3: Implement snapshot publish**

Snapshot asset:

```python
@dg.asset(
    name="norway_brreg_financial_statements_snapshot_clickhouse",
    deps=[dg.AssetKey("norway_brreg_financial_statements_snapshot_usd_parquet")],
    group_name=GROUP_NAME,
    kinds={"python", "parquet", "duckdb", "clickhouse", "brreg"},
)
def norway_brreg_financial_statements_snapshot_clickhouse(
    clickhouse: ClickhouseResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=RESOLVED_DATABASE, tables=("no_financial_statements",))
    frame = norway_brreg_financial_storage.read_snapshot_statements_usd()
    rows = replace_financial_snapshot_parquet_in_clickhouse(
        clickhouse=clickhouse,
        frame=frame,
        database=RESOLVED_DATABASE,
        table="no_financial_statements",
    )
    return dg.MaterializeResult(metadata={"row_count": rows, "clickhouse_table": "no_financial_statements"})
```

- [ ] **Step 4: Implement update publish**

Update asset:

```python
@dg.asset(
    name="norway_brreg_financial_statements_updates_clickhouse",
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
    deps=[dg.AssetKey("norway_brreg_financial_statements_updates_usd_parquet")],
    group_name=GROUP_NAME,
    kinds={"python", "parquet", "duckdb", "clickhouse", "brreg"},
)
def norway_brreg_financial_statements_updates_clickhouse(
    context,
    clickhouse: ClickhouseResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    usd_frame = norway_brreg_financial_storage.read_update_statements_usd(partition_date)
    row_count = apply_financial_update_parquet_to_clickhouse(
        clickhouse=clickhouse,
        frame=usd_frame,
        database=RESOLVED_DATABASE,
        table="no_financial_statements",
    )
    return dg.MaterializeResult(metadata={"partition_date": partition_date, "row_count": row_count})
```

Use the distinct `org_number` values from `usd_frame` as the financial affected-org set.
Use the existing `insert_rows` staging-table pattern from entity updates, not a nullable-date SQL literal builder.

- [ ] **Step 5: Run ClickHouse and graph tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_clickhouse.py tests/test_norway_brreg_definitions.py -q
```

Expected: pass.

- [ ] **Step 6: Commit ClickHouse publish assets**

```bash
git add companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_clickhouse.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_clickhouse.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py
git commit -m "Publish Norway financial statement parquet to ClickHouse"
```

### Task 7: Jobs And Old DuckDB Financial Removal

**Files:**
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/translation.py` or create `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/jobs.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py`
- Modify: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py`

- [ ] **Step 1: Add job membership tests**

Full financial snapshot job must contain:

```python
{
    "norway_brreg_entities_snapshot_s3",
    "norway_brreg_entities_snapshot_no_companies_parquet",
    "norway_brreg_financial_fetches_snapshot_parquet",
    "norway_brreg_financial_statements_snapshot_parquet",
    "norway_brreg_financial_statements_snapshot_usd_parquet",
    "norway_brreg_financial_statements_snapshot_clickhouse",
}
```

Combined daily update job must contain:

```python
{
    "norway_brreg_entity_updates_s3",
    "norway_brreg_entity_updates_no_companies_parquet",
    "norway_brreg_entity_updates_affected_orgs_parquet",
    "norway_brreg_entity_updates_removed_orgs_parquet",
    "norway_brreg_entity_updates_clickhouse",
    "norway_brreg_financial_update_candidates_parquet",
    "norway_brreg_financial_fetches_updates_parquet",
    "norway_brreg_financial_statements_updates_parquet",
    "norway_brreg_financial_statements_updates_usd_parquet",
    "norway_brreg_financial_statements_updates_clickhouse",
}
```

Old assets must be absent from definitions:

```python
assert "norway_brreg_financial_fetches_duckdb" not in asset_names
assert "norway_brreg_financial_statements_duckdb" not in asset_names
```

The old entity-only daily job and schedule must be absent after the combined job is wired:

```python
assert "norway_brreg_entity_updates_job" not in repo.job_names
assert "norway_brreg_entity_updates_schedule" not in schedule_names
```

- [ ] **Step 2: Run failing job tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_definitions.py -q
```

Expected: new jobs missing and old financial DuckDB assets still registered.

- [ ] **Step 3: Define explicit jobs**

Use `AssetSelection.assets("asset_name").upstream()` or explicit asset lists. Keep names literal:

```python
norway_brreg_financials_full_snapshot_job = dg.define_asset_job(
    "norway_brreg_financials_full_snapshot_job",
    selection=dg.AssetSelection.assets("norway_brreg_financial_statements_snapshot_clickhouse").upstream(),
)

norway_brreg_daily_update_job = dg.define_asset_job(
    "norway_brreg_daily_update_job",
    selection=dg.AssetSelection.assets("norway_brreg_financial_statements_updates_clickhouse").upstream(),
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
)
```

Add one daily schedule for `norway_brreg_daily_update_job`. Do not keep `norway_brreg_entity_updates_job`
or `norway_brreg_entity_updates_schedule`; the combined daily job is the only scheduled Brreg update
path so the API is not called twice for the same partition.
Do not add a schedule for the full snapshot job.

- [ ] **Step 4: Remove old financial DuckDB assets from active definitions**

Remove from `definitions.py` assets:

- `norway_brreg_entities_duckdb_asset`, if no active asset depends on it after this task
- `norway_brreg_financial_fetches_duckdb_asset`
- `norway_brreg_financial_statements_duckdb_asset`

Keep legacy helper files only if tests still import reusable functions. The active Dagster graph must not expose old financial DuckDB assets.

- [ ] **Step 5: Run job tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_definitions.py tests/test_norway_brreg_workflows.py -q
```

Expected: pass.

- [ ] **Step 6: Commit job wiring and old asset removal**

```bash
git add companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/translation.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_workflows.py
git commit -m "Wire Norway financial parquet jobs"
```

### Task 8: Final Validation And Cleanup

**Files:**
- Modify tests touched above
- Modify stale imports in `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py`
- Modify stale tests in `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py`

- [ ] **Step 1: Remove or update tests that assert old DuckDB financial behavior**

Replace old expectations with parquet pipeline expectations. Keep unit tests for row builders if they are still used.

- [ ] **Step 2: Run focused Norway tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest \
  tests/test_norway_brreg_financial_storage.py \
  tests/test_norway_brreg_financial_fetches.py \
  tests/test_norway_brreg_financial_normalize.py \
  tests/test_norway_brreg_financial_clickhouse.py \
  tests/test_norway_brreg_definitions.py \
  tests/test_norway_brreg_workflows.py \
  tests/test_norway_brreg_entity_clickhouse.py \
  -q
```

Expected: pass.

- [ ] **Step 3: Run definition validation**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: definitions load successfully.

- [ ] **Step 4: Run lint for changed Python files**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run ruff check src/dagster_v3/defs/norway_brreg tests/test_norway_brreg_financial_storage.py tests/test_norway_brreg_financial_fetches.py tests/test_norway_brreg_financial_normalize.py tests/test_norway_brreg_financial_clickhouse.py tests/test_norway_brreg_definitions.py tests/test_norway_brreg_workflows.py
```

Expected: pass.

- [ ] **Step 5: Commit final cleanup**

```bash
git add companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_storage.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_fetches.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_clickhouse.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_workflows.py \
  companycollect/corpscout/dagster_v3/tests/test_norway_brreg_assets.py
git commit -m "Clean up Norway financial parquet pipeline tests"
```

## Manual Dagster Checks After Deployment

1. Materialize `norway_brreg_financial_fetches_snapshot_parquet`.
2. Re-materialize it once and confirm logs show `skipped_existing_count` increasing for org/year raw fetches already present in S3.
3. Materialize `norway_brreg_financial_statements_snapshot_parquet`.
4. Materialize `norway_brreg_financial_statements_snapshot_usd_parquet`.
5. Materialize `norway_brreg_financial_statements_snapshot_clickhouse`.
6. For a daily partition, materialize `norway_brreg_daily_update_job`.
7. Query ClickHouse:

```sql
select count(*) from corpscout.no_financial_statements;
select currency, count(*) from corpscout.no_financial_statements group by currency order by count() desc;
select count(*) from corpscout.no_financial_statements where fx_rate_to_usd is null and currency != '';
```

The third query should be zero for rows with financial amounts that require conversion.

## Self-Review

- Spec coverage: covers source fetch, snapshot/update split, original-currency parquet, USD FX asset, ClickHouse publish, old DuckDB removal, tests, and Dagster validation.
- Placeholder scan: no placeholder tasks remain.
- Type consistency: asset names, resource name, storage method names, and job names are consistent across tasks.
