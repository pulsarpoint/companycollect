# Exchange Rate Client and Missing FX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify the exchange-rate client table reference and keep Norway financial normalization from failing when Brreg returns a currency that has no ECB reference rate.

**Architecture:** The exchange-rate package should read from the single migrated `reference.exchange_rates` table directly. Norway financial normalization should batch FX lookups for normal operation, fall back to individual lookups only when the batch contains a missing rate, and still emit the original financial row with null USD conversion fields when no FX rate exists.

**Tech Stack:** Python, ClickHouse, DuckDB, Dagster assets, pytest.

---

### Task 1: Prove Missing FX Should Not Drop Financial Rows

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py`

- [ ] **Step 1: Add a fake FX client with one missing currency**

```python
class FakeExchangeRatesWithMissing:
    def __init__(self) -> None:
        self.requests: list[list[tuple[str, str]]] = []

    def usd_rates(self, requests):
        request_keys = [(request.currency, request.rate_date) for request in requests]
        self.requests.append(request_keys)
        if any(currency == "USN" for currency, _ in request_keys):
            raise LookupError("No USD exchange rate for USN on, before, or after 2024-12-31")
        return {
            (request.currency, request.rate_date): FakeUsdRate()
            for request in requests
        }
```

- [ ] **Step 2: Add the failing test**

```python
def test_build_financial_statement_rows_keeps_rows_without_fx_rate() -> None:
    exchange_rates = FakeExchangeRatesWithMissing()
    unsupported_record = _financial_record()
    unsupported_record["id"] = 5667198
    unsupported_record["valuta"] = "USN"

    rows = financial_normalize.build_financial_statement_rows_from_fetch_rows(
        [
            {
                "org_number": "923609016",
                "legal_name": "EQUINOR ASA",
                "website": "www.equinor.com",
                "last_submitted_accounts_year": "2024",
                "source_run_id": "run-1",
                "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
                "fetch_status": "success",
                "raw_response": json.dumps([_financial_record(), unsupported_record]),
            }
        ],
        exchange_rates=exchange_rates,
    )

    assert [row["currency"] for row in rows] == ["NOK", "USN"]
    assert rows[0]["operating_revenue_amount_usd"] == Decimal("7254300000.00")
    assert rows[1]["operating_revenue_amount_original"] == Decimal("72543000000")
    assert rows[1]["operating_revenue_amount_usd"] is None
    assert rows[1]["fx_rate_to_usd"] is None
    assert rows[1]["fx_rate_date"] == ""
    assert rows[1]["fx_source"] == ""
```

- [ ] **Step 3: Run the test red**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_normalize.py::test_build_financial_statement_rows_keeps_rows_without_fx_rate -q
```

Expected: FAIL because the current batch FX lookup raises `LookupError` and aborts row construction.

### Task 2: Preserve Rows When FX Is Missing

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py`

- [ ] **Step 1: Route FX lookup through a helper**

Replace:

```python
    rates = exchange_rates.usd_rates(list(rate_requests_by_key.values()))
```

with:

```python
    rates = _load_available_usd_rates(exchange_rates, list(rate_requests_by_key.values()))
```

- [ ] **Step 2: Use optional FX rates per row**

Replace:

```python
                fx_rate=rates[(currency, period_end_date)],
```

with:

```python
                fx_rate=rates.get((currency, period_end_date)),
```

- [ ] **Step 3: Add fallback FX lookup helper**

```python
def _load_available_usd_rates(
    exchange_rates: ExchangeRates,
    requests: list[ExchangeRateRequest],
) -> dict[tuple[str, str], Any]:
    if not requests:
        return {}
    try:
        return exchange_rates.usd_rates(requests)
    except LookupError:
        rates: dict[tuple[str, str], Any] = {}
        for request in requests:
            try:
                rates.update(exchange_rates.usd_rates([request]))
            except LookupError:
                continue
        return rates
```

- [ ] **Step 4: Make `_financial_statement_row` accept missing FX**

Change the `fx_rate` handling so `fx_rate_to_usd`, `fx_rate_date`, and `fx_source` become `None`, `""`, and `""` when no rate exists, and every `*_amount_usd` field becomes `None`.

- [ ] **Step 5: Run the red test green**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_norway_brreg_financial_normalize.py::test_build_financial_statement_rows_keeps_rows_without_fx_rate -q
```

Expected: PASS.

### Task 3: Remove the Unused Exchange-Rate Table Constructor Parameter

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/exchange_rates/client.py`
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_exchange_rate_client.py`

- [ ] **Step 1: Add a regression assertion**

Add to an existing exchange-rate client SQL test:

```python
    assert "FROM requested_rates" in sql
    assert "INNER JOIN reference.exchange_rates AS exchange_rates" in sql
```

- [ ] **Step 2: Simplify the constructor**

Change:

```python
    def __init__(
        self,
        clickhouse_client: Any,
        *,
        table: str = DEFAULT_EXCHANGE_RATES_TABLE,
    ) -> None:
        self._clickhouse_client = clickhouse_client
        self._table = table
```

to:

```python
    def __init__(self, clickhouse_client: Any) -> None:
        self._clickhouse_client = clickhouse_client
```

- [ ] **Step 3: Use the constant directly in SQL**

Replace both `{self._table}` interpolations with `{DEFAULT_EXCHANGE_RATES_TABLE}`.

- [ ] **Step 4: Run exchange-rate client tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rate_client.py -q
```

Expected: PASS.

### Task 4: Verify and Commit

**Files:**
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/exchange_rates/client.py`
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py`
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_exchange_rate_client.py`
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py`

- [ ] **Step 1: Run focused tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rate_client.py tests/test_norway_brreg_financial_normalize.py -q
```

Expected: PASS.

- [ ] **Step 2: Validate Dagster definitions**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: PASS.

- [ ] **Step 3: Verify against local real data**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
set -a && source .env && set +a
uv run python - <<'PY'
import json
import duckdb
from dagster_v3.defs.norway_brreg.financial_normalize import build_financial_statement_rows_from_fetch_rows
from exchange_rates import ExchangeRateClient

with duckdb.connect("data/norway_brreg_source.duckdb", read_only=True) as connection:
    fetch_rows = [
        dict(zip(["fetch_status", "raw_response", "org_number", "legal_name", "website", "last_submitted_accounts_year", "source_run_id", "source_url"], row))
        for row in connection.execute(
            """
            select fetch_status, raw_response, org_number, legal_name, website,
                   last_submitted_accounts_year, source_run_id, source_url
            from norway_brreg.financial_fetches
            where fetch_status = 'success'
            limit 50
            """
        ).fetchall()
    ]
rows = build_financial_statement_rows_from_fetch_rows(fetch_rows, exchange_rates=ExchangeRateClient.from_env())
print(len(rows), sum(1 for row in rows if row["fx_rate_to_usd"] is None))
PY
```

Expected: script exits 0 and prints row counts.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git diff --check
rm -rf corpscout/dagster_v3/storage
git add docs/superpowers/plans/2026-06-18-fix-exchange-rate-client-and-missing-fx.md \
  corpscout/dagster_v3/exchange_rates/client.py \
  corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/financial_normalize.py \
  corpscout/dagster_v3/tests/test_exchange_rate_client.py \
  corpscout/dagster_v3/tests/test_norway_brreg_financial_normalize.py
git commit -m "fix: tolerate missing norway financial fx rates"
```

Expected: Commit succeeds on `main`.

## Self-Review

Spec coverage: the plan explains why `_table` exists, removes the unused indirection, and fixes the reproducible Norway financial normalization failure caused by unsupported FX currencies.

Placeholder scan: no placeholders remain.

Type consistency: `fx_rate` becomes optional only inside row construction, and the public exchange-rate client still exposes the same `usd_rate`, `usd_rates`, and `convert_to_usd` methods.
