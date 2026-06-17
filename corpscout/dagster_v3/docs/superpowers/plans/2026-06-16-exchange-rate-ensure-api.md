# Exchange Rate Ensure API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reusable exchange-rate API that batches missing ECB rates into ClickHouse before country pipelines convert financial amounts to USD.

**Architecture:** The shared `dagster_v3.exchange_rates` package owns rate lookup, missing-rate fetch, ClickHouse batch insert, and USD cross-rate resolution. BRREG calls this package once per financial payload batch and uses the returned in-memory rate map while building rows. Dagster assets remain orchestration only; source-specific code does not fetch or insert exchange-rate rows directly.

**Tech Stack:** Python, ClickHouse native client, ECB EXR JSON API, pytest, Dagster/dlt BRREG assets.

---

## File Structure

- Modify `src/dagster_v3/exchange_rates/client.py`: add `ensure_usd_rates`, ECB fetch hooks, batched ClickHouse insert, and lookback-date logic.
- Modify `src/dagster_v3/exchange_rates/__init__.py`: export any public model or helper needed by callers.
- Modify `src/dagster_v3/defs/norway_brreg/assets.py`: use batch rate resolution when normalizing financial statements.
- Modify `tests/test_exchange_rate_client.py`: add unit tests for no-op local cache hits, missing-rate fetch/insert, lookback windows, and deduplication.
- Modify `tests/test_norway_brreg_assets.py`: add test proving BRREG requests rates once for unique financial dates and reuses returned rates.

## Task 1: Shared `ensure_usd_rates` Contract

**Files:**
- Modify: `tests/test_exchange_rate_client.py`
- Modify: `src/dagster_v3/exchange_rates/client.py`

- [ ] **Step 1: Write failing no-op cache-hit test**

Add a test where ClickHouse already contains EUR/USD and EUR/NOK rows for `2024-12-31`. Call:

```python
rates = client.ensure_usd_rates([
    ExchangeRateRequest(currency="NOK", rate_date="2024-12-31"),
])
```

Assert:

```python
assert rates[("NOK", "2024-12-31")].rate == Decimal("0.08807969478592623993217465028")
assert clickhouse.inserts == []
```

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_exchange_rate_client.py::test_exchange_rate_client_ensure_usd_rates_returns_existing_rates_without_insert -q
```

Expected: failure because `ExchangeRateClient.ensure_usd_rates` does not exist.

- [ ] **Step 3: Implement minimal no-op method**

Add `ensure_usd_rates(self, requests, *, lookback_days=7, source_run_id="", pulled_at=None)` that initially delegates to `self.usd_rates(requests)`.

- [ ] **Step 4: Run green test**

Run the same focused test and expect it to pass.

## Task 2: Missing ECB Components And Batch Insert

**Files:**
- Modify: `tests/test_exchange_rate_client.py`
- Modify: `src/dagster_v3/exchange_rates/client.py`

- [ ] **Step 1: Write failing missing-rate test**

Add a fake ECB provider returning component rows for `USD` and `NOK` on `2024-12-31`. Use an empty fake ClickHouse client. Call `ensure_usd_rates` once for duplicate NOK requests. Assert:

```python
assert len(clickhouse.inserts) == 1
assert inserted_table == "reference.exchange_rates"
assert inserted_columns == (
    "rate_date",
    "base_currency",
    "quote_currency",
    "rate",
    "source",
    "source_url",
    "source_payload_hash",
    "source_run_id",
    "pulled_at",
)
assert inserted_rows contain one USD row and one NOK row
assert ecb_provider.calls are deduplicated
```

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_exchange_rate_client.py::test_exchange_rate_client_ensure_usd_rates_fetches_and_inserts_missing_components_once -q
```

Expected: failure because `ensure_usd_rates` does not fetch or insert.

- [ ] **Step 3: Implement component detection and insert**

Implementation rules:

- Normalize requests by `(currency.upper(), rate_date)`.
- Required component currencies are `USD` plus requested non-USD currencies; include `EUR` identity rows for EUR requests.
- Load existing components once using `_load_components`.
- For every requested date, inspect candidate dates from requested date back `lookback_days`.
- Fetch only missing `(quote_currency, rate_date)` components.
- Insert all fetched rows using `clickhouse_client.insert(table, rows, column_names=...)`.
- Merge inserted rows into the in-memory component map before resolving rates.

- [ ] **Step 4: Run green test**

Run the focused test and expect it to pass.

## Task 3: Lookback Resolution

**Files:**
- Modify: `tests/test_exchange_rate_client.py`
- Modify: `src/dagster_v3/exchange_rates/client.py`

- [ ] **Step 1: Write failing lookback test**

Simulate request `NOK` on `2024-12-31`, with ECB provider returning no rows on `2024-12-31` and rows on `2024-12-30`. Assert returned rate has:

```python
assert rate.requested_rate_date == "2024-12-31"
assert rate.rate_date == "2024-12-30"
```

- [ ] **Step 2: Run red test**

Run the focused lookback test and expect failure.

- [ ] **Step 3: Implement date-window fetch**

Generate ISO date strings from requested date backward through `lookback_days`, using `datetime.date`. Try the requested date first, then previous dates. Resolve using the latest date with all required components.

- [ ] **Step 4: Run green test**

Run the focused lookback test and expect pass.

## Task 4: BRREG Batch Rate Usage

**Files:**
- Modify: `tests/test_norway_brreg_assets.py`
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`

- [ ] **Step 1: Write failing BRREG batch test**

Add a fake exchange-rate provider with:

```python
def ensure_usd_rates(self, requests): ...
```

Build two financial records with the same `NOK`/`2024-12-31` date. Assert the provider saw one unique request and that both rows used the returned rate.

- [ ] **Step 2: Run red test**

Run:

```bash
uv run pytest tests/test_norway_brreg_assets.py::test_financial_rows_resolve_exchange_rates_once_per_unique_currency_date -q
```

Expected: failure because BRREG still calls `usd_rate` per row.

- [ ] **Step 3: Implement batch map usage**

In `build_financial_statement_rows`, collect unique `ExchangeRateRequest(currency, period_end_date)` from valid records. If the exchange-rate object has `ensure_usd_rates`, call it once. Pass the returned map to `_financial_statement_row`. Keep `usd_rate` fallback for simple tests/fakes that only implement the old protocol.

- [ ] **Step 4: Run green test**

Run the focused BRREG test and expect pass.

## Task 5: Verification

**Files:**
- All files above.

- [ ] **Step 1: Run focused tests**

Run:

```bash
uv run pytest tests/test_exchange_rate_client.py tests/test_norway_brreg_assets.py -q
```

Expected: all pass.

- [ ] **Step 2: Run full test suite**

Run:

```bash
uv run pytest -q
```

Expected: all non-integration tests pass; real Temporal/LLM integration remains skipped unless explicitly enabled.

- [ ] **Step 3: Validate Dagster definitions**

Run:

```bash
uv run dg check defs
uv run dg check toml
uv run dg check yaml
```

Expected: all checks pass.

## Self-Review

- Spec coverage: covers shared ensure API, batched ClickHouse writes, lookback behavior, and BRREG batch usage.
- Placeholder scan: no `TODO`, `TBD`, or unspecified implementation steps.
- Type consistency: `ExchangeRateRequest`, `UsdExchangeRate`, and `ExchangeRateComponent` match existing package models.
