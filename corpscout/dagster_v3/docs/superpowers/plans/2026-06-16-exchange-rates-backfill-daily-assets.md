# Exchange Rates Backfill And Daily Assets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single ad-hoc exchange-rate asset with two explicit Dagster assets: a monthly historical backfill asset and a daily update asset that both populate `reference.exchange_rates`.

**Architecture:** Dagster owns exchange-rate population as a first-class upstream data task. `exchange_rates_backfill` uses monthly partitions and bulk ECB date ranges to populate roughly three years of history with about 36 API calls. `exchange_rates_daily` uses daily partitions and a schedule for ongoing updates. Both assets call shared source/write helpers and write idempotently to the same ClickHouse table by deleting the target date window before inserting fresh ECB rows.

**Tech Stack:** Dagster assets and partitions, Dagster schedules, dlt REST API source, ECB EXR SDMX JSON API, ClickHouse, pytest.

---

## Scope

This plan covers exchange-rate ingestion only. It does not change BRREG financial normalization. After this plan, BRREG should be run after exchange-rate backfill/daily assets have populated `reference.exchange_rates`.

The earlier `2026-06-16-exchange-rate-ensure-api.md` plan is superseded for now. We are not fetching ECB rows inside BRREG.

## File Structure

- Modify `src/dagster_v3/defs/exchange_rates/source.py`
  - Add range-based ECB source config.
  - Parse SDMX JSON observations into dated rows.
  - Generate EUR identity rows for actual rate dates in each requested window.
- Modify `src/dagster_v3/defs/exchange_rates/assets.py`
  - Replace single `exchange_rates_asset` registration with:
    - `exchange_rates_backfill_asset` using `MonthlyPartitionsDefinition`
    - `exchange_rates_daily_asset` using `DailyPartitionsDefinition`
  - Add partition-window helpers.
  - Delete existing ClickHouse rows for the asset partition window before dlt insert.
  - Add `exchange_rates_daily_schedule`.
- Modify `tests/test_exchange_rates_assets.py`
  - Update source tests from exact-date resource generation to range resource generation.
  - Add partition-window tests.
  - Add registration tests for both assets and the daily schedule.
  - Add tests that partitioned assets delete only their own target windows before inserting.

## Task 1: ECB Range Source

**Files:**
- Modify: `tests/test_exchange_rates_assets.py`
- Modify: `src/dagster_v3/defs/exchange_rates/source.py`

- [ ] **Step 1: Add failing range config test**

Add this test to `tests/test_exchange_rates_assets.py`:

```python
def test_exchange_rate_range_rest_api_config_models_bulk_ecb_endpoint() -> None:
    config = fx_source.exchange_rate_range_rest_api_config(
        start_date="2024-12-01",
        end_date="2024-12-31",
        currencies=["USD", "NOK"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    resources = {resource["name"]: resource for resource in config["resources"]}
    resource = resources["exchange_rates_ecb_2024_12_01_2024_12_31"]

    assert resource["table_name"] == "exchange_rates"
    assert resource["endpoint"] == {
        "path": "D.NOK+USD.EUR.SP00.A",
        "params": {
            "format": "jsondata",
            "startPeriod": "2024-12-01",
            "endPeriod": "2024-12-31",
        },
        "paginator": "single_page",
    }
    assert resource["processing_steps"]
```

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py::test_exchange_rate_range_rest_api_config_models_bulk_ecb_endpoint -q
```

Expected: fail because `exchange_rate_range_rest_api_config` does not exist.

- [ ] **Step 3: Implement range config**

In `src/dagster_v3/defs/exchange_rates/source.py`, add:

```python
def exchange_rate_range_rest_api_config(
    *,
    start_date: str,
    end_date: str,
    currencies: list[str],
    source_run_id: str,
    pulled_at: str,
    user_agent: str = DEFAULT_USER_AGENT,
) -> RESTAPIConfig:
    quote_currencies = [
        currency
        for currency in sorted({currency.upper() for currency in currencies} | {"USD", "NOK"})
        if currency != "EUR"
    ]
    currency_key = "+".join(quote_currencies)
    source_url = f"{ECB_EXR_BASE_URL}/D.{currency_key}.EUR.SP00.A"
    resource_name = (
        f"exchange_rates_ecb_{start_date.replace('-', '_')}_{end_date.replace('-', '_')}"
    )
    return {
        "client": {
            "base_url": f"{ECB_EXR_BASE_URL}/",
            "headers": {"User-Agent": user_agent},
        },
        "resources": [
            {
                "name": resource_name,
                "table_name": EXCHANGE_RATES_DLT_TABLE,
                "write_disposition": "append",
                "primary_key": ["rate_date", "base_currency", "quote_currency", "source"],
                "endpoint": {
                    "path": f"D.{currency_key}.EUR.SP00.A",
                    "params": {
                        "format": "jsondata",
                        "startPeriod": start_date,
                        "endPeriod": end_date,
                    },
                    "paginator": "single_page",
                },
                "processing_steps": [
                    {
                        "yield_map": _ecb_range_mapper(
                            quote_currencies=quote_currencies,
                            source_url=source_url,
                            source_run_id=source_run_id,
                            pulled_at=pulled_at,
                        )
                    }
                ],
            }
        ],
    }
```

Add `_ecb_range_mapper` as a placeholder that calls a new `ecb_rate_rows_from_range_payload` function added in Task 2.

- [ ] **Step 4: Run green test**

Run the focused test and expect pass.

## Task 2: Range Payload Parsing And Identity Rows

**Files:**
- Modify: `tests/test_exchange_rates_assets.py`
- Modify: `src/dagster_v3/defs/exchange_rates/source.py`

- [ ] **Step 1: Add failing range parser test**

Add this helper and test:

```python
def _ecb_range_payload() -> dict:
    return {
        "structure": {
            "dimensions": {
                "series": [
                    {"values": [{"id": "NOK"}, {"id": "USD"}]},
                    {"values": [{"id": "EUR"}]},
                    {"values": [{"id": "SP00"}]},
                    {"values": [{"id": "A"}]},
                ],
                "observation": [
                    {
                        "values": [
                            {"id": "2024-12-30"},
                            {"id": "2024-12-31"},
                        ]
                    }
                ],
            }
        },
        "dataSets": [
            {
                "series": {
                    "0:0:0:0": {"observations": {"0": [11.8], "1": [11.79]}},
                    "1:0:0:0": {"observations": {"0": [1.04], "1": [1.0389]}},
                }
            }
        ],
    }


def test_ecb_rate_rows_from_range_payload_returns_dated_rows() -> None:
    rows = fx_source.ecb_rate_rows_from_range_payload(
        _ecb_range_payload(),
        quote_currencies=["NOK", "USD"],
        source_url="https://data-api.ecb.europa.eu/service/data/EXR/D.NOK+USD.EUR.SP00.A",
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert [
        (row["rate_date"], row["quote_currency"], row["rate"])
        for row in rows
    ] == [
        ("2024-12-30", "NOK", "11.8"),
        ("2024-12-31", "NOK", "11.79"),
        ("2024-12-30", "USD", "1.04"),
        ("2024-12-31", "USD", "1.0389"),
    ]
```

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py::test_ecb_rate_rows_from_range_payload_returns_dated_rows -q
```

Expected: fail because the parser does not exist.

- [ ] **Step 3: Implement parser**

Implement `ecb_rate_rows_from_range_payload` so it:

- reads currency ids from `payload["structure"]["dimensions"]["series"][0]["values"]`
- reads date ids from `payload["structure"]["dimensions"]["observation"][0]["values"]`
- iterates `payload["dataSets"][0]["series"]`
- maps first series key component to quote currency index
- maps observation keys to date indexes
- returns rows with `rate_date`, `base_currency="EUR"`, `quote_currency`, `rate`, `source`, `source_url`, `source_payload_hash`, `source_run_id`, and `pulled_at`

- [ ] **Step 4: Add identity row test**

Add:

```python
def test_identity_exchange_rates_for_dates_resource_yields_actual_rate_dates() -> None:
    resource = fx_source.identity_exchange_rates_for_dates_resource(
        rate_dates=["2024-12-30", "2024-12-31"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert [row["rate_date"] for row in list(resource)] == ["2024-12-30", "2024-12-31"]
```

- [ ] **Step 5: Implement identity date resource**

Create `identity_exchange_rates_for_dates_resource` as a clearer alias/replacement for `identity_exchange_rates_resource` when callers already know actual dates. Keep the old function as a wrapper for compatibility.

- [ ] **Step 6: Run green tests**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py::test_ecb_rate_rows_from_range_payload_returns_dated_rows tests/test_exchange_rates_assets.py::test_identity_exchange_rates_for_dates_resource_yields_actual_rate_dates -q
```

Expected: both pass.

## Task 3: Range Source Wrapper

**Files:**
- Modify: `tests/test_exchange_rates_assets.py`
- Modify: `src/dagster_v3/defs/exchange_rates/source.py`

- [ ] **Step 1: Add failing source test**

Add:

```python
def test_exchange_rates_range_source_exposes_bulk_resource_and_identity_resource() -> None:
    source = fx_source.exchange_rates_range_source(
        start_date="2024-12-01",
        end_date="2024-12-31",
        currencies=["USD"],
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert "exchange_rates_ecb_2024_12_01_2024_12_31" in source.resources.keys()
    assert "exchange_rates_identity" in source.resources.keys()
```

- [ ] **Step 2: Run red test**

Run the focused test and expect failure because `exchange_rates_range_source` does not exist.

- [ ] **Step 3: Implement range source**

Add:

```python
@dlt.source(name="exchange_rates")
def exchange_rates_range_source(
    *,
    start_date: str,
    end_date: str,
    currencies: list[str],
    source_run_id: str = "",
    pulled_at: str | None = None,
) -> list[DltResource]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    ecb_source = rest_api_source(
        config=exchange_rate_range_rest_api_config(
            start_date=start_date,
            end_date=end_date,
            currencies=currencies,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        ),
        name="exchange_rates_ecb",
    )
    return [
        *ecb_source.resources.values(),
        identity_exchange_rates_for_range_resource(
            start_date=start_date,
            end_date=end_date,
            source_run_id=source_run_id,
            pulled_at=effective_pulled_at,
        ),
    ]
```

The identity range resource should emit one EUR identity row per calendar date in the window. This may include weekends; that is acceptable because it only supports EUR requests and does not affect NOK/USD cross-rate resolution.

- [ ] **Step 4: Run green test**

Run the focused source test and expect pass.

## Task 4: Partition Window Helpers

**Files:**
- Modify: `tests/test_exchange_rates_assets.py`
- Modify: `src/dagster_v3/defs/exchange_rates/assets.py`

- [ ] **Step 1: Add failing partition helper tests**

Add:

```python
def test_exchange_rate_month_partition_window() -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    assert fx_assets.month_partition_window("2024-02") == ("2024-02-01", "2024-02-29")


def test_exchange_rate_day_partition_window() -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    assert fx_assets.day_partition_window("2024-12-31") == ("2024-12-31", "2024-12-31")
```

- [ ] **Step 2: Run red tests**

Run the focused tests and expect failure because helper functions do not exist.

- [ ] **Step 3: Implement helpers**

In `assets.py`, add:

```python
def month_partition_window(partition_key: str) -> tuple[str, str]:
    start = date.fromisoformat(f"{partition_key}-01")
    end = date(start.year + (start.month // 12), (start.month % 12) + 1, 1) - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def day_partition_window(partition_key: str) -> tuple[str, str]:
    day = date.fromisoformat(partition_key)
    return day.isoformat(), day.isoformat()
```

- [ ] **Step 4: Run green tests**

Run focused helper tests and expect pass.

## Task 5: Idempotent Delete Window

**Files:**
- Modify: `tests/test_exchange_rates_assets.py`
- Modify: `src/dagster_v3/defs/exchange_rates/assets.py`

- [ ] **Step 1: Add failing delete test**

Add:

```python
def test_delete_exchange_rates_window_targets_source_dates_and_currencies() -> None:
    from dagster_v3.defs.exchange_rates import assets as fx_assets

    client = FakeClickHouseClient()
    fx_assets.delete_exchange_rates_window(
        client,
        start_date="2024-12-01",
        end_date="2024-12-31",
        currencies=["NOK", "USD", "EUR"],
    )

    assert client.statements == [
        (
            "ALTER TABLE reference.exchange_rates DELETE WHERE "
            "source IN ('ECB EXR', 'identity') "
            "AND rate_date >= '2024-12-01' "
            "AND rate_date <= '2024-12-31' "
            "AND quote_currency IN ('EUR', 'NOK', 'USD')"
        )
    ]
```

- [ ] **Step 2: Run red test**

Run the focused test and expect failure because the helper does not exist.

- [ ] **Step 3: Implement delete helper**

Implement `delete_exchange_rates_window(client, *, start_date, end_date, currencies)` in `assets.py`. It should normalize currencies to include `EUR`, `NOK`, and `USD`, sort them, SQL-quote single quotes defensively, and call `client.execute(sql)`.

- [ ] **Step 4: Run green test**

Run focused delete test and expect pass.

## Task 6: Two Partitioned Assets And Schedule

**Files:**
- Modify: `tests/test_exchange_rates_assets.py`
- Modify: `src/dagster_v3/defs/exchange_rates/assets.py`

- [ ] **Step 1: Add failing asset registration test**

Replace the old single-asset registration expectation with:

```python
def test_exchange_rates_assets_and_daily_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    schedule_names = {schedule.name for schedule in repository.schedule_defs}

    assert "exchange_rates_backfill" in asset_keys
    assert "exchange_rates_daily" in asset_keys
    assert "exchange_rates" not in asset_keys
    assert "exchange_rates_daily_schedule" in schedule_names
```

- [ ] **Step 2: Run red test**

Run the focused registration test and expect failure because assets do not exist yet.

- [ ] **Step 3: Implement assets**

In `assets.py`:

- Replace `exchange_rates_asset` with `exchange_rates_backfill_asset` and `exchange_rates_daily_asset`.
- Keep `ExchangeRatesConfig` with default currencies.
- Add:

```python
EXCHANGE_RATES_BACKFILL_PARTITIONS = dg.MonthlyPartitionsDefinition(start_date="2023-01-01")
EXCHANGE_RATES_DAILY_PARTITIONS = dg.DailyPartitionsDefinition(start_date=date.today().isoformat())
```

- For each asset:
  - compute `start_date, end_date` from `context.partition_key`
  - call `prepare_exchange_rates_table(clickhouse)`
  - open `clickhouse.get_connection()` and call `delete_exchange_rates_window(...)`
  - run dlt with `exchange_rates_range_source(start_date=..., end_date=..., currencies=config.currencies, source_run_id=context.run_id)`
  - log start/end with partition window and currencies

- Define:

```python
exchange_rates_daily_job = dg.define_asset_job(
    "exchange_rates_daily_job",
    selection=[dg.AssetKey("exchange_rates_daily")],
)
exchange_rates_daily_schedule = dg.build_schedule_from_partitioned_job(
    exchange_rates_daily_job,
)
```

If Dagster derives a different schedule name, explicitly use `dg.ScheduleDefinition` for `exchange_rates_daily_schedule` with daily cron.

- Update `defs = dg.Definitions(...)` to include both assets and the schedule.

- [ ] **Step 4: Run green registration test**

Run the focused registration test and expect pass.

## Task 7: Existing Tests And Compatibility

**Files:**
- Modify: `tests/test_exchange_rates_assets.py`
- Modify: `src/dagster_v3/defs/exchange_rates/source.py`
- Modify: `src/dagster_v3/defs/exchange_rates/assets.py`

- [ ] **Step 1: Update old source tests**

Keep existing exact-date source functions if needed for compatibility, but update tests so new range source behavior is the primary contract. Do not remove existing low-level functions if other tests still use them.

- [ ] **Step 2: Run focused exchange-rate tests**

Run:

```bash
uv run pytest tests/test_exchange_rates_assets.py tests/test_exchange_rate_client.py -q
```

Expected: pass.

- [ ] **Step 3: Run Dagster checks**

Run:

```bash
uv run dg check defs
uv run dg check toml
uv run dg check yaml
```

Expected: pass.

- [ ] **Step 4: Run full tests**

Run:

```bash
uv run pytest -q
```

Expected: pass with real Temporal/LLM integration skipped unless explicitly enabled.

## Self-Review

- Spec coverage: covers monthly backfill, daily update, shared physical table, bulk ECB range API, idempotent delete-before-insert, daily schedule, and tests.
- Placeholder scan: no TBD/TODO/undefined behavior. Each task has concrete function names, file paths, and commands.
- Type consistency: asset names are `exchange_rates_backfill` and `exchange_rates_daily`; table name remains `reference.exchange_rates`; source functions live in `source.py`.
