# Exchange Rates Direct dlt Resources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace exchange-rate REST config builders with direct dlt resources that call ECB once and yield final ClickHouse-compatible rows.

**Architecture:** `exchange_rates_range_source` and `exchange_rates_source` will return explicit dlt resources. The ECB resources will perform the HTTP request and call existing pure parser functions. The Dagster dlt translator remains only for asset metadata mapping.

**Tech Stack:** Python, dlt resources, requests, pytest, Dagster definition loading.

---

## File Structure

- Modify `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/source.py`
  - Remove `rest_api_source`, `RESTAPIConfig`, config-builder functions, and mapper closures.
  - Add direct ECB dlt resources.
  - Keep pure payload parsers and ClickHouse pipeline wiring.
- Modify `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`
  - Replace config-shape tests with behavior tests for direct ECB resources.

### Task 1: Write Failing Direct Resource Tests

**Files:**
- Modify: `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`

- [ ] **Step 1: Remove config-shape tests**

Delete:

```python
def test_exchange_rate_rest_api_config_models_ecb_endpoint() -> None: ...
def test_exchange_rate_range_rest_api_config_models_bulk_ecb_endpoint() -> None: ...
```

- [ ] **Step 2: Add fake response helper**

Add:

```python
class FakeHttpResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self.payload
```

- [ ] **Step 3: Add range resource behavior test**

Add:

```python
def test_ecb_exchange_rates_range_resource_fetches_endpoint_and_yields_rows(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_get(url: str, *, params: dict, headers: dict, timeout: int) -> FakeHttpResponse:
        calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return FakeHttpResponse(_ecb_range_payload())

    monkeypatch.setattr(fx_source.requests, "get", fake_get)

    rows = list(
        fx_source.ecb_exchange_rates_range_resource(
            start_date="2024-12-01",
            end_date="2024-12-31",
            currencies=["USD", "NOK"],
            source_run_id="run-1",
            pulled_at="2026-06-16T00:00:00.000Z",
        )
    )

    assert calls == [
        {
            "url": "https://data-api.ecb.europa.eu/service/data/EXR/D.NOK+USD.EUR.SP00.A",
            "params": {
                "format": "jsondata",
                "startPeriod": "2024-12-01",
                "endPeriod": "2024-12-31",
            },
            "headers": {"User-Agent": "corpscout-dagster-v3-dev/0.1"},
            "timeout": 30,
        }
    ]
    assert [(row["rate_date"], row["quote_currency"], row["rate"]) for row in rows] == [
        ("2024-12-30", "NOK", "11.8"),
        ("2024-12-31", "NOK", "11.79"),
        ("2024-12-30", "USD", "1.04"),
        ("2024-12-31", "USD", "1.0389"),
    ]
```

- [ ] **Step 4: Update source resource-name tests**

Change the single-date source test to expect direct ECB resource names:

```python
assert {
    "exchange_rates_ecb_usd_2024_12_31",
    "exchange_rates_ecb_nok_2024_12_31",
    "exchange_rates_identity",
}.issubset(source.resources.keys())
```

- [ ] **Step 5: Run tests to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py::test_ecb_exchange_rates_range_resource_fetches_endpoint_and_yields_rows tests/test_exchange_rates_assets.py::test_exchange_rates_source_exposes_ecb_and_identity_resources -q
```

Expected: FAIL because `ecb_exchange_rates_range_resource` does not exist yet.

### Task 2: Implement Direct ECB dlt Resources

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/source.py`

- [ ] **Step 1: Replace imports and constants**

Remove:

```python
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig
```

Add:

```python
import requests
```

Add:

```python
DEFAULT_ECB_TIMEOUT_SECONDS = 30
```

- [ ] **Step 2: Add shared table hints**

Add:

```python
EXCHANGE_RATES_PRIMARY_KEY = ["rate_date", "base_currency", "quote_currency", "source"]
```

- [ ] **Step 3: Replace `exchange_rates_source`**

Use direct ECB resources:

```python
@dlt.source(name="exchange_rates")
def exchange_rates_source(... ) -> list[DltResource]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    return [
        *[
            ecb_exchange_rates_resource(...)
            for rate_date in sorted(set(rate_dates))
            for currency in _quote_currencies(currencies)
        ],
        identity_exchange_rates_resource(...),
    ]
```

- [ ] **Step 4: Replace `exchange_rates_range_source`**

Use one direct range resource plus identity:

```python
@dlt.source(name="exchange_rates")
def exchange_rates_range_source(... ) -> list[DltResource]:
    effective_pulled_at = pulled_at or _utc_now_iso()
    return [
        ecb_exchange_rates_range_resource(...),
        identity_exchange_rates_for_range_resource(...),
    ]
```

- [ ] **Step 5: Add direct ECB resources**

Add:

```python
def ecb_exchange_rates_resource(...) -> DltResource: ...
def ecb_exchange_rates_range_resource(...) -> DltResource: ...
```

Each function should:

- build the exact ECB URL used today,
- call `requests.get(..., params={...}, headers={...}, timeout=30)`,
- call `response.raise_for_status()`,
- parse `response.json()` with the existing parser,
- return `dlt.resource(..., table_name="exchange_rates", write_disposition="append", primary_key=EXCHANGE_RATES_PRIMARY_KEY)`.

- [ ] **Step 6: Remove old indirection**

Delete:

```python
exchange_rate_rest_api_config
exchange_rate_range_rest_api_config
_ecb_mapper
_ecb_range_mapper
```

- [ ] **Step 7: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py -q
```

Expected: PASS.

### Task 3: Validate Definitions And Migration Tests

**Files:**
- No additional source files.

- [ ] **Step 1: Run migration tests touched by exchange-rate table behavior**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_clickhouse_migrations.py -q
```

Expected: PASS.

- [ ] **Step 2: Validate Dagster definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: PASS.

### Task 4: Commit

**Files:**
- Modified: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/source.py`
- Modified: `corpscout/dagster_v3/tests/test_exchange_rates_assets.py`
- Created: `corpscout/dagster_v3/docs/superpowers/plans/2026-06-17-exchange-rates-direct-dlt-resources.md`

- [ ] **Step 1: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/source.py corpscout/dagster_v3/tests/test_exchange_rates_assets.py corpscout/dagster_v3/docs/superpowers/plans/2026-06-17-exchange-rates-direct-dlt-resources.md
git commit -m "refactor: simplify exchange rate dlt resources"
```

## Self-Review

Spec coverage:
- Removes REST config builders and mapper factories.
- Keeps a single dlt source that directly yields loadable resources.
- Keeps data transformation in the source resource, not in `dagster_dlt_translator`.
- Preserves final ClickHouse table and asset behavior.

Placeholder scan:
- No placeholders remain.

Type consistency:
- Tests reference `ecb_exchange_rates_range_resource`, which Task 2 adds.
- Existing parser functions remain available for tests and source resources.
