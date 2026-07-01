# Standardize Dagster ClickHouse Resource

> **For Grao / Codex:** Use this plan to remove the project-owned ClickHouse resource wrapper from `dagster_v3` and use `dagster_clickhouse.ClickhouseResource` everywhere.

**Goal:** Replace `dagster_v3.defs.clickhouse.resources.ClickHouseConnectResource` and `clickhouse_resource_from_env()` with the official `dagster_clickhouse.ClickhouseResource`, update all assets/tests to use the official resource, and delete the custom implementation.

**Architecture:** Keep one central Dagster resource named `clickhouse` in `dagster_v3/src/dagster_v3/definitions.py`. Assets receive `clickhouse: ClickhouseResource` through normal Dagster resource injection. Source packages must not define or re-export ClickHouse resource factories. The project should have no custom ClickHouse resource class.

**Tech Stack:** Dagster, `dagster-clickhouse`, `clickhouse-driver` native protocol, pytest, `dg check`, uv.

## Current State

The repo already depends on `dagster-clickhouse>=0.29.9`, and `dagster_clickhouse.ClickhouseResource` is available.

The current custom implementation lives in:

- `dagster_v3/src/dagster_v3/defs/clickhouse/resources.py`

It wraps `clickhouse_connect`, uses the HTTP port default `8123`, and exposes helper methods like `insert_arrow()` and `insert_rows()`.

Current usages that must be removed:

- `dagster_v3/src/dagster_v3/definitions.py`
- `dagster_v3/src/dagster_v3/defs/nace/assets.py`
- `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/financial_metrics.py`
- `dagster_v3/tests/test_common_resources.py`
- `dagster_v3/tests/test_nace_categories.py`
- `dagster_v3/tests/test_exchange_rates_v2_dbt.py`
- `dagster_v3/tests/test_wikidata_assets.py`

Important compatibility detail: `dagster_clickhouse.ClickhouseResource.get_connection()` yields a `clickhouse_driver.Client`. That client does not have `clickhouse_connect`'s `insert_arrow()` helper. Code that currently calls `insert_arrow()` must be converted to normal native-client inserts.

## Desired File Changes

### 1. Centralize Official Resource In `definitions.py`

Update `dagster_v3/src/dagster_v3/definitions.py`:

- Import `ClickhouseResource` from `dagster_clickhouse`.
- Remove the import of `clickhouse_resource_from_env`.
- Build the `clickhouse` resource directly in this file.
- Use native protocol naming: `CLICKHOUSE_NATIVE_PORT`, default `9000`.
- Keep credentials as Dagster env vars where possible.

Target shape:

```python
import os
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource


def _clickhouse_resource_from_env() -> ClickhouseResource:
    return ClickhouseResource(
        host=dg.EnvVar("CLICKHOUSE_HOST"),
        port=_int_env("CLICKHOUSE_NATIVE_PORT", 9000),
        user=dg.EnvVar("CLICKHOUSE_USER"),
        password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
        database=dg.EnvVar("CLICKHOUSE_DATABASE"),
        secure=_bool_env("CLICKHOUSE_SECURE", False),
    )


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
```

This keeps the env parsing at the project entry point, where resources are assembled, without creating another wrapper package.

### 2. Remove Source-Level Resource Factory Leakage

Update `dagster_v3/src/dagster_v3/defs/nace/assets.py`:

- Remove `from dagster_v3.defs.clickhouse.resources import clickhouse_resource_from_env`.
- Keep `from dagster_clickhouse import ClickhouseResource`.
- Keep asset signatures using `clickhouse: ClickhouseResource`.

No source package should provide `clickhouse_resource_from_env()`.

### 3. Update Finland XBRL Type Annotation

Update `dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/financial_metrics.py`:

- Replace `ClickHouseConnectResource` import with `ClickhouseResource`.
- Change the `finland_xbrl_financial_metrics_clickhouse` parameter annotation to `clickhouse: ClickhouseResource`.

### 4. Convert Arrow Insert To Native Client Insert

Update `dagster_v3/src/dagster_v3/defs/finland_xbrl/clickhouse.py`.

Current code calls:

```python
clickhouse_client.insert_arrow(stage_table, arrow_table, database=database)
```

Replace this with a table-specific SQL template and native `clickhouse_driver.Client.execute()` insert. Do not build the insert SQL by joining `FINANCIAL_METRICS_CLICKHOUSE_COLUMNS`; that hides the actual write contract.

```python
def _financial_metrics_insert_sql(qualified_table: str) -> str:
    return f"""
        INSERT INTO {qualified_table} (
            statement_key,
            business_id,
            financial_date,
            registration_date,
            period_start,
            period_end,
            reported_company_name,
            source_url,
            xml_object_key,
            xml_sha256,
            xml_size_bytes,
            currency_original,
            revenue_amount_original,
            revenue_amount_usd,
            operating_profit_loss_amount_original,
            operating_profit_loss_amount_usd,
            profit_loss_amount_original,
            profit_loss_amount_usd,
            total_assets_amount_original,
            total_assets_amount_usd,
            equity_amount_original,
            equity_amount_usd,
            liabilities_amount_original,
            liabilities_amount_usd,
            cash_and_bank_amount_original,
            cash_and_bank_amount_usd,
            current_assets_amount_original,
            current_assets_amount_usd,
            current_receivables_amount_original,
            current_receivables_amount_usd,
            current_liabilities_amount_original,
            current_liabilities_amount_usd,
            personnel_expenses_amount_original,
            personnel_expenses_amount_usd,
            wages_and_salaries_amount_original,
            wages_and_salaries_amount_usd,
            employees,
            source_fact_count,
            mapped_fact_count,
            unmapped_numeric_fact_count,
            metric_warnings,
            mapping_version,
            fx_rate_to_usd,
            fx_rate_date,
            fx_converted_at,
            source_system,
            source_run_id,
            source_record_id,
            source_payload_hash,
            resolved_at
        ) VALUES
    """


def _financial_metrics_insert_rows(arrow_table: pa.Table) -> list[tuple[object, ...]]:
    return [
        (
            row["statement_key"],
            row["business_id"],
            row["financial_date"],
            row["registration_date"],
            row["period_start"],
            row["period_end"],
            row["reported_company_name"],
            row["source_url"],
            row["xml_object_key"],
            row["xml_sha256"],
            row["xml_size_bytes"],
            row["currency_original"],
            row["revenue_amount_original"],
            row["revenue_amount_usd"],
            row["operating_profit_loss_amount_original"],
            row["operating_profit_loss_amount_usd"],
            row["profit_loss_amount_original"],
            row["profit_loss_amount_usd"],
            row["total_assets_amount_original"],
            row["total_assets_amount_usd"],
            row["equity_amount_original"],
            row["equity_amount_usd"],
            row["liabilities_amount_original"],
            row["liabilities_amount_usd"],
            row["cash_and_bank_amount_original"],
            row["cash_and_bank_amount_usd"],
            row["current_assets_amount_original"],
            row["current_assets_amount_usd"],
            row["current_receivables_amount_original"],
            row["current_receivables_amount_usd"],
            row["current_liabilities_amount_original"],
            row["current_liabilities_amount_usd"],
            row["personnel_expenses_amount_original"],
            row["personnel_expenses_amount_usd"],
            row["wages_and_salaries_amount_original"],
            row["wages_and_salaries_amount_usd"],
            row["employees"],
            row["source_fact_count"],
            row["mapped_fact_count"],
            row["unmapped_numeric_fact_count"],
            row["metric_warnings"],
            row["mapping_version"],
            row["fx_rate_to_usd"],
            row["fx_rate_date"],
            row["fx_converted_at"],
            row["source_system"],
            row["source_run_id"],
            row["source_record_id"],
            row["source_payload_hash"],
            row["resolved_at"],
        )
        for row in arrow_table.to_pylist()
    ]


rows = _financial_metrics_insert_rows(arrow_table)
if rows:
    clickhouse_client.execute(_financial_metrics_insert_sql(qualified_stage_table), rows)
```

Keep the existing staging-table swap behavior for this resource migration. The larger question of direct DuckDB-to-ClickHouse export is covered by `docs/superpowers/plans/2026-07-01-direct-duckdb-clickhouse-export.md`.

### 5. Delete Custom Resource Module

Delete:

- `dagster_v3/src/dagster_v3/defs/clickhouse/resources.py`

Keep `dagster_v3/src/dagster_v3/defs/clickhouse/__init__.py` and other files for now if they are still imported by existing ClickHouse table helpers. Do not remove the whole `clickhouse` package as part of this small migration unless no imports remain.

### 6. Remove `clickhouse-connect` Dependency If Unused

After imports are updated, run:

```bash
rg "clickhouse_connect|clickhouse-connect" dagster_v3/src dagster_v3/tests dagster_v3/pyproject.toml -n
```

If only `dagster_v3/pyproject.toml` still references `clickhouse-connect`, remove it and update `dagster_v3/uv.lock` with `uv lock`.

Do not remove `clickhouse-connect` if unrelated source packages still use it directly.

## Test Plan

Follow test-first order so the resource migration is controlled.

### 1. Update Resource Tests First

Change tests that assert the custom class to assert the official class:

- `dagster_v3/tests/test_common_resources.py`
- `dagster_v3/tests/test_nace_categories.py`
- `dagster_v3/tests/test_exchange_rates_v2_dbt.py`
- `dagster_v3/tests/test_wikidata_assets.py`

Expected assertion pattern:

```python
from dagster_clickhouse import ClickhouseResource

assert (
    repository.get_top_level_resources()["clickhouse"].configurable_resource_cls
    is ClickhouseResource
)
```

For env behavior, test the central project definitions:

```python
monkeypatch.setenv("CLICKHOUSE_NATIVE_PORT", "9440")
monkeypatch.setenv("CLICKHOUSE_SECURE", "on")

repository = load_project_defs().get_repository_def()
resource = repository.get_top_level_resources()["clickhouse"].resource_fn.__self__

assert isinstance(resource, ClickhouseResource)
assert resource.port == 9440
assert resource.secure is True
```

Remove tests that instantiate or monkeypatch `dagster_v3.defs.clickhouse.resources`.

### 2. Add/Update Finland XBRL Insert Test

Add a focused test around `_replace_clickhouse_table_with_arrow()` or update the existing Finland XBRL ClickHouse export test.

The fake client should expose only `execute()`, not `insert_arrow()`. This catches accidental continued dependency on `clickhouse_connect`.

Assert that:

- Stage table is created.
- Empty Arrow tables do not issue an insert.
- Non-empty Arrow tables call `execute("INSERT INTO ... VALUES", rows)`.
- Stage table is exchanged with the real table.
- Stage table is dropped in `finally`.

### 3. Run Focused Tests

```bash
cd dagster_v3
uv run pytest \
  tests/test_common_resources.py \
  tests/test_nace_categories.py \
  tests/test_exchange_rates_v2_dbt.py \
  tests/test_wikidata_assets.py \
  tests/test_finland_xbrl_assets.py \
  -q
```

### 4. Scan For Stale Custom Resource References

```bash
rg "ClickHouseConnectResource|clickhouse_resource_from_env|dagster_v3\.defs\.clickhouse\.resources|CLICKHOUSE_HTTP_PORT|clickhouse_connect" \
  dagster_v3/src dagster_v3/tests dagster_v3/pyproject.toml -n
```

Expected result after this migration:

- No custom resource references.
- No `CLICKHOUSE_HTTP_PORT`.
- No `clickhouse_connect` usage unless it belongs to an unrelated package that still truly needs it.

### 5. Run Dagster Validation

```bash
cd dagster_v3
uv run dg check
```

### 6. Run Full Test Suite

```bash
cd dagster_v3
uv run pytest -q
```

## Acceptance Criteria

- All Dagster code imports `ClickhouseResource` from `dagster_clickhouse`.
- `dagster_v3/src/dagster_v3/defs/clickhouse/resources.py` is deleted.
- No source package exposes a ClickHouse resource factory.
- `CLICKHOUSE_NATIVE_PORT` replaces `CLICKHOUSE_HTTP_PORT`.
- Code that writes Finland XBRL financial metrics no longer calls `insert_arrow()`.
- Focused tests, `dg check`, and the full pytest suite pass.
- `rg` finds no stale custom resource references.

## Implementation Order

1. Update tests to expect `dagster_clickhouse.ClickhouseResource`.
2. Update `definitions.py` to create the official resource centrally.
3. Remove the unused NACE import and update Finland XBRL annotation.
4. Convert Finland XBRL Arrow insert to native `execute(..., rows)`.
5. Delete `defs/clickhouse/resources.py`.
6. Remove `clickhouse-connect` from dependencies if no direct usage remains.
7. Run focused tests, stale-reference scan, `dg check`, and full pytest.
