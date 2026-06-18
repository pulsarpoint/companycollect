# Fix Exchange Rate ClickHouse Aliases Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `norway_brreg_financial_statements_duckdb` failing during USD conversion because ClickHouse cannot resolve `request_currency` in the exchange-rate lookup CTE.

**Architecture:** Keep the existing `ExchangeRateClient` query shape. Add explicit column aliases in the `available_dates` CTE so downstream CTEs can reliably reference `request_currency`, `requested_rate_date`, and `rate_date` on ClickHouse.

**Tech Stack:** Python, ClickHouse SQL via `clickhouse_connect`, pytest, Dagster asset validation.

---

### Task 1: Alias Exchange Rate CTE Output Columns

**Files:**
- Modify: `corpscout/dagster_v3/exchange_rates/client.py`
- Modify: `corpscout/dagster_v3/tests/test_exchange_rate_client.py`

- [x] **Step 1: Add a SQL regression test**

Add a test asserting the generated SQL aliases the `available_dates` CTE output:

```python
def test_exchange_rate_client_aliases_available_date_columns_for_clickhouse() -> None:
    clickhouse = FakeNativeClickHouseClient(rows=[])
    client = ExchangeRateClient(clickhouse)

    try:
        client.usd_rate(currency="NOK", rate_date="2024-12-31")
    except LookupError:
        pass

    sql = clickhouse.queries[0].sql
    assert "requested_rates.request_currency AS request_currency" in sql
    assert "requested_rates.requested_rate_date AS requested_rate_date" in sql
    assert "exchange_rates.rate_date AS rate_date" in sql
```

- [x] **Step 2: Patch the SQL**

In `ExchangeRateClient._load_components_for_requests`, change:

```sql
requested_rates.request_currency,
requested_rates.requested_rate_date,
exchange_rates.rate_date,
```

to:

```sql
requested_rates.request_currency AS request_currency,
requested_rates.requested_rate_date AS requested_rate_date,
exchange_rates.rate_date AS rate_date,
```

- [x] **Step 3: Run focused tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rate_client.py tests/test_norway_brreg_financial_normalize.py -q
```

Expected: all tests pass.

- [x] **Step 4: Validate Dagster definitions**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: definitions load successfully.

- [x] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add docs/superpowers/plans/2026-06-18-fix-exchange-rate-clickhouse-aliases.md corpscout/dagster_v3/exchange_rates/client.py corpscout/dagster_v3/tests/test_exchange_rate_client.py
git commit -m "fix: alias exchange rate clickhouse cte columns"
```
