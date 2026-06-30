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
  - Keep row builders and HTTP status handling, but remove DuckDB candidate/state behavior from the new path.
- Modify `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py`
  - Split original-currency statement normalization from USD conversion.
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
- `norway_brreg_financial_fetches_updates_parquet`
- `norway_brreg_financial_statements_snapshot_parquet`
- `norway_brreg_financial_statements_updates_parquet`
- `norway_brreg_financial_statements_snapshot_usd_parquet`
- `norway_brreg_financial_statements_updates_usd_parquet`
- `norway_brreg_financial_statements_snapshot_clickhouse`
- `norway_brreg_financial_statements_updates_clickhouse`

New jobs:

- `norway_brreg_financials_full_snapshot_job`
- `norway_brreg_financials_daily_update_job`

Selection rules:

- Snapshot financial fetch candidates come from `norway_brreg_entities_snapshot_no_companies_parquet`.
- Snapshot candidates require `is_active = true`.
- Snapshot candidates do not require website and do not require `last_submitted_accounts_year`.
- Daily update candidates come from `norway_brreg_entity_updates_affected_orgs_parquet` and `norway_brreg_entity_updates_no_companies_parquet`.
- Daily update fetches only affected orgs with replacement company rows.
- Removed orgs are not fetched; the ClickHouse update asset still deletes their old financial rows through `affected_orgs`.
- Financial fetch uses no year filter, so Brreg can return all available annual accounts for the org.
- Full snapshot raw fetch parquet is reused if it already exists. Re-running the job must not redownload all financial accounts by default.

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

    def snapshot_fetches_exist(self) -> bool:
        return self._object_store.exists(
            financial_fetches_snapshot_object_key(),
            bucket=NORWAY_BRREG_ENTITY_BUCKET,
        )

    def write_snapshot_fetches(self, frame: pl.DataFrame) -> str:
        return self._write_frame(financial_fetches_snapshot_object_key(), frame)

    def write_update_fetches(self, partition_date: str, frame: pl.DataFrame) -> str:
        return self._write_frame(financial_fetches_update_object_key(partition_date), frame)

    def read_snapshot_fetches(self) -> pl.DataFrame:
        return self._read_frame(financial_fetches_snapshot_object_key())

    def read_update_fetches(self, partition_date: str) -> pl.DataFrame:
        return self._read_frame(financial_fetches_update_object_key(partition_date))
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
            {"org_number": "1", "name": "Active No Website", "is_active": True, "primary_website_url": None},
            {"org_number": "2", "name": "Active Website", "is_active": True, "primary_website_url": "https://x.no"},
            {"org_number": "3", "name": "Inactive", "is_active": False, "primary_website_url": "https://y.no"},
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
            "last_submitted_accounts_year": "",
        }
        for row in frame.filter(pl.col("is_active") == True).sort("org_number").to_dicts()
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

### Task 3: Raw Financial Fetch Parquet Assets

**Files:**
- Create: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_fetches.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py`
- Modify: `companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_definitions.py`
- Test: `companycollect/corpscout/dagster_v3/tests/test_norway_brreg_workflows.py`

- [ ] **Step 1: Add graph tests for new raw fetch assets**

Add assertions that both assets are registered and have the expected parents:

```python
assert "norway_brreg_financial_fetches_snapshot_parquet" in asset_names
assert "norway_brreg_financial_fetches_updates_parquet" in asset_names
```

Expected parent sets:

```python
snapshot parents == {"norway_brreg_entities_snapshot_no_companies_parquet"}
updates parents == {
    "norway_brreg_entity_updates_no_companies_parquet",
    "norway_brreg_entity_updates_affected_orgs_parquet",
}
```

- [ ] **Step 2: Run graph tests and verify failure**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_definitions.py -q
```

Expected: fails because assets are not registered.

- [ ] **Step 3: Implement snapshot fetch asset**

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
    if norway_brreg_financial_storage.snapshot_fetches_exist():
        context.log.info("Reusing existing Norway Brreg financial fetch snapshot parquet")
        frame = norway_brreg_financial_storage.read_snapshot_fetches()
        return dg.MaterializeResult(metadata={"row_count": frame.height, "downloaded": False})

    companies = norway_brreg_entity_storage.read_normalized_snapshot_table("no_companies")
    candidates = financial_fetch_candidates_from_no_companies(companies)
    rows = fetch_financial_rows_for_orgs(
        orgs=candidates,
        api=norway_brreg_api,
        source_run_id=context.run_id,
        log=context.log.info,
    )
    frame = financial_fetch_rows_frame(rows)
    key = norway_brreg_financial_storage.write_snapshot_fetches(frame)
    return dg.MaterializeResult(metadata={"row_count": frame.height, "s3_key": key, "downloaded": True})
```

The asset must log candidate count, fetched count, status counts, and S3 key.

- [ ] **Step 4: Implement update fetch asset**

Core behavior:

```python
@dg.asset(
    name="norway_brreg_financial_fetches_updates_parquet",
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
    deps=[
        dg.AssetKey("norway_brreg_entity_updates_no_companies_parquet"),
        dg.AssetKey("norway_brreg_entity_updates_affected_orgs_parquet"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
)
def norway_brreg_financial_fetches_updates_parquet(...):
    partition_date = context.partition_key
    affected = norway_brreg_entity_storage.read_normalized_update_table(partition_date, "affected_orgs")
    companies = norway_brreg_entity_storage.read_normalized_update_table(partition_date, "no_companies")
    candidates = financial_update_candidates_from_frames(affected, companies)
    rows = fetch_financial_rows_for_orgs(...)
    frame = financial_fetch_rows_frame(rows)
    key = norway_brreg_financial_storage.write_update_fetches(partition_date, frame)
    return dg.MaterializeResult(metadata={"partition_date": partition_date, "row_count": frame.height, "s3_key": key})
```

Removed orgs appear in affected orgs but not in `no_companies`; they are intentionally not fetched.

- [ ] **Step 5: Register resource and assets**

In `definitions.py`, add:

```python
"norway_brreg_financial_storage": NorwayBrregFinancialParquetStorageResource(),
```

Register both raw fetch assets in `defs.assets`.

- [ ] **Step 6: Run graph tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_definitions.py tests/test_norway_brreg_workflows.py -q
```

Expected: pass for new raw fetch graph, with old financial DuckDB tests still passing until removal task.

- [ ] **Step 7: Commit raw fetch assets**

```bash
git add companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/financial_fetches.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets/__init__.py \
  companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py \
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
    ...
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
- update publish deletes all affected orgs, including removed orgs, then inserts replacement USD rows
- empty affected-org partition skips ClickHouse mutation

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
def norway_brreg_financial_statements_snapshot_clickhouse(...):
    assert_clickhouse_tables_exist(clickhouse, database=RESOLVED_DATABASE, tables=("no_financial_statements",))
    frame = norway_brreg_financial_storage.read_snapshot_statements_usd()
    rows = replace_financial_snapshot_parquet_in_clickhouse(...)
    return dg.MaterializeResult(metadata={"row_count": rows, "clickhouse_table": "no_financial_statements"})
```

- [ ] **Step 4: Implement update publish**

Update asset:

```python
@dg.asset(
    name="norway_brreg_financial_statements_updates_clickhouse",
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
    deps=[
        dg.AssetKey("norway_brreg_financial_statements_updates_usd_parquet"),
        dg.AssetKey("norway_brreg_entity_updates_affected_orgs_parquet"),
        dg.AssetKey("norway_brreg_entity_updates_removed_orgs_parquet"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "parquet", "duckdb", "clickhouse", "brreg"},
)
def norway_brreg_financial_statements_updates_clickhouse(...):
    affected_orgs = norway_brreg_entity_storage.read_normalized_update_table(partition_date, "affected_orgs")
    usd_frame = norway_brreg_financial_storage.read_update_statements_usd(partition_date)
    row_count = apply_financial_update_parquet_to_clickhouse(...)
    return dg.MaterializeResult(metadata={"partition_date": partition_date, "row_count": row_count})
```

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

Daily financial update job must contain:

```python
{
    "norway_brreg_entity_updates_s3",
    "norway_brreg_entity_updates_no_companies_parquet",
    "norway_brreg_entity_updates_affected_orgs_parquet",
    "norway_brreg_entity_updates_removed_orgs_parquet",
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

- [ ] **Step 2: Run failing job tests**

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_definitions.py -q
```

Expected: new jobs missing and old financial DuckDB assets still registered.

- [ ] **Step 3: Define explicit jobs**

Use `AssetSelection.assets(...).upstream()` or explicit asset lists. Keep names literal:

```python
norway_brreg_financials_full_snapshot_job = dg.define_asset_job(
    "norway_brreg_financials_full_snapshot_job",
    selection=dg.AssetSelection.assets("norway_brreg_financial_statements_snapshot_clickhouse").upstream(),
)

norway_brreg_financials_daily_update_job = dg.define_asset_job(
    "norway_brreg_financials_daily_update_job",
    selection=dg.AssetSelection.assets("norway_brreg_financial_statements_updates_clickhouse").upstream(),
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
)
```

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
2. Re-materialize it once and confirm logs say existing snapshot parquet was reused.
3. Materialize `norway_brreg_financial_statements_snapshot_parquet`.
4. Materialize `norway_brreg_financial_statements_snapshot_usd_parquet`.
5. Materialize `norway_brreg_financial_statements_snapshot_clickhouse`.
6. For a daily partition, materialize `norway_brreg_financials_daily_update_job`.
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
