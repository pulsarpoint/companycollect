# Latvia Resolved — Financial Metrics + EUR→USD (Module 3, Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or executing-plans. Checkbox steps track progress.

**Goal:** Produce a canonical `lv_financial_metrics` table: the headline financial figures distilled from the wide `latvia_ur.financial_statements`, scaled by `rounded_to_nearest`, with each metric in both native EUR (`*_original`) and USD (`*_usd`) plus FX provenance — mirroring Norway's `financial_statements` original/USD pattern.

**Why this scope:** The "resolved layer" is three independently-sized pieces. This plan builds **Phase 1 (financial metrics + currency conversion)** — the directly-useful answer to "do we need currency conversion." Deferred to their own plans: **Phase 2** a canonical resolved company table (mostly duplicates Module 1's `lv_companies` until websites exist), and **Phase 3** website discovery (a crawl-service/Temporal integration that lives outside `dagster_v3`).

**Architecture:** A Python normalize asset (`latvia_ur_financial_metrics_duckdb`) reads the wide EUR statements from `data/latvia_ur_source.duckdb`, selects the headline metrics, applies `rounded_to_nearest` scaling, converts EUR→USD per report `period_end_date` via the shared `ExchangeRateClient` (ECB, EUR-based — so EUR→USD is a direct lookup), and writes `latvia_ur.financial_metrics`. A ClickHouse export asset replaces `corpscout.lv_financial_metrics` (schema owned by a new migration `000018`). Single-writer `latvia_ur_duckdb` pool throughout.

**Tech Stack:** Python, duckdb, `from exchange_rates import ExchangeRateClient, ExchangeRateRequest`, dagster, dagster-clickhouse. Run `dg`/`pytest` via `uv run`.

**Reference templates (read to match style):**
- `src/dagster_v3/defs/norway_brreg/financial_normalize.py` — the original→USD conversion (rate batching, fallback, `*_amount_usd`/`fx_rate_to_usd`/`fx_rate_date`/`fx_source`).
- `src/dagster_v3/defs/latvia_ur/clickhouse.py` + `assets.py` — the assert-exists + atomic-replace export and asset wiring already built for Latvia.
- `clickhouse/migrations/000016_corpscout_lv_financial_statements.up.sql` — DDL conventions.

**Exchange-rate API (verified):**
```python
from exchange_rates import ExchangeRateClient, ExchangeRateRequest
client = ExchangeRateClient.from_env()
rates = client.usd_rates([ExchangeRateRequest(currency="EUR", rate_date="2016-12-31")])
rate = rates.get(("EUR", "2016-12-31"))      # UsdExchangeRate | absent
rate.rate        # Decimal EUR->USD rate
rate.rate_date   # str (the rate's effective date)
rate.source      # str (e.g. "ECB EXR")
rate.convert(amount)   # Decimal | None
```

**Headline metric mapping (Latvia wide col → canonical metric):**
| canonical metric | latvia_ur.financial_statements column |
|---|---|
| revenue | net_turnover |
| gross_profit | by_function_gross_profit |
| pretax_result | income_before_income_taxes |
| net_result | net_income |
| total_assets | total_assets |
| current_assets | total_current_assets |
| non_current_assets | total_non_current_assets |
| equity | equity |
| current_liabilities | current_liabilities |
| non_current_liabilities | non_current_liabilities |

**`rounded_to_nearest` scaling:** `{"ONES": 1, "THOUSANDS": 1000, "MILLIONS": 1_000_000}`; unknown/empty → factor 1 **and log a warning** (don't silently mis-scale). Verified value in data: `ONES`. Scaling is applied to the EUR `*_original` value BEFORE FX conversion.

---

### Task 1: ClickHouse migration `000018_corpscout_lv_financial_metrics`

**Files:**
- Create: `clickhouse/migrations/000018_corpscout_lv_financial_metrics.up.sql`
- Create: `clickhouse/migrations/000018_corpscout_lv_financial_metrics.down.sql`
- Modify: `tests/test_clickhouse_migrations.py` (append to `EXPECTED_MIGRATIONS`)

(Note: `000017_corpscout_wikidata_company_country` already exists in `EXPECTED_MIGRATIONS` per the current test; this adds `000018`. If a different next number is free, use that and keep the tuple contiguous.)

- [ ] **Step 1: Write the up migration**

`clickhouse/migrations/000018_corpscout_lv_financial_metrics.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.lv_financial_metrics
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    statement_id String,
    regcode String,
    fiscal_year Nullable(Int32),
    period_start_date Nullable(Date),
    period_end_date Nullable(Date),
    employees Nullable(Int64),
    currency LowCardinality(String),
    rounded_to_nearest LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 2)),
    revenue_amount_usd Nullable(Decimal(38, 2)),
    gross_profit_amount_original Nullable(Decimal(38, 2)),
    gross_profit_amount_usd Nullable(Decimal(38, 2)),
    pretax_result_amount_original Nullable(Decimal(38, 2)),
    pretax_result_amount_usd Nullable(Decimal(38, 2)),
    net_result_amount_original Nullable(Decimal(38, 2)),
    net_result_amount_usd Nullable(Decimal(38, 2)),
    total_assets_amount_original Nullable(Decimal(38, 2)),
    total_assets_amount_usd Nullable(Decimal(38, 2)),
    current_assets_amount_original Nullable(Decimal(38, 2)),
    current_assets_amount_usd Nullable(Decimal(38, 2)),
    non_current_assets_amount_original Nullable(Decimal(38, 2)),
    non_current_assets_amount_usd Nullable(Decimal(38, 2)),
    equity_amount_original Nullable(Decimal(38, 2)),
    equity_amount_usd Nullable(Decimal(38, 2)),
    current_liabilities_amount_original Nullable(Decimal(38, 2)),
    current_liabilities_amount_usd Nullable(Decimal(38, 2)),
    non_current_liabilities_amount_original Nullable(Decimal(38, 2)),
    non_current_liabilities_amount_usd Nullable(Decimal(38, 2)),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (regcode, statement_id);
```

- [ ] **Step 2: Write the down migration**

`clickhouse/migrations/000018_corpscout_lv_financial_metrics.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.lv_financial_metrics;
```

- [ ] **Step 3: Append to EXPECTED_MIGRATIONS**

In `tests/test_clickhouse_migrations.py`, add after the last entry:

```python
    "000018_corpscout_lv_financial_metrics",
```

- [ ] **Step 4: Run migration tests**

Run: `uv run pytest tests/test_clickhouse_migrations.py -q`
Expected: PASS (count increments by 1).

- [ ] **Step 5: Commit**

```bash
git add clickhouse/migrations/000018_corpscout_lv_financial_metrics.up.sql clickhouse/migrations/000018_corpscout_lv_financial_metrics.down.sql dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): add corpscout.lv_financial_metrics migration"
```

---

### Task 2: metric schema + columns in `tables.py`

**Files:**
- Modify: `src/dagster_v3/defs/latvia_ur/tables.py`
- Test: `tests/test_latvia_ur_financials_tables.py` (extend)

- [ ] **Step 1: Add metric constants**

Append to `tables.py`:

```python
# --- Financial metrics (Module 3, Phase 1) ---------------------------------

FINANCIAL_METRICS_WIDE_TABLE = "financial_metrics"
LV_FINANCIAL_METRICS_TABLE = "lv_financial_metrics"
QUALIFIED_LV_FINANCIAL_METRICS_TABLE = (
    f"{LATVIA_UR_DATABASE}.{LV_FINANCIAL_METRICS_TABLE}"
)

# canonical metric name -> source column in FINANCIAL_STATEMENTS_WIDE_TABLE
FINANCIAL_METRIC_SOURCE_COLUMNS = {
    "revenue": "net_turnover",
    "gross_profit": "by_function_gross_profit",
    "pretax_result": "income_before_income_taxes",
    "net_result": "net_income",
    "total_assets": "total_assets",
    "current_assets": "total_current_assets",
    "non_current_assets": "total_non_current_assets",
    "equity": "equity",
    "current_liabilities": "current_liabilities",
    "non_current_liabilities": "non_current_liabilities",
}
FINANCIAL_METRIC_NAMES = tuple(FINANCIAL_METRIC_SOURCE_COLUMNS)

ROUNDED_TO_NEAREST_FACTORS = {"ONES": 1, "THOUSANDS": 1000, "MILLIONS": 1_000_000}

_METRIC_AMOUNT_COLUMNS = tuple(
    col
    for metric in FINANCIAL_METRIC_NAMES
    for col in (f"{metric}_amount_original", f"{metric}_amount_usd")
)

LV_FINANCIAL_METRICS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "statement_id",
    "regcode",
    "fiscal_year",
    "period_start_date",
    "period_end_date",
    "employees",
    "currency",
    "rounded_to_nearest",
    *_METRIC_AMOUNT_COLUMNS,
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "resolved_at",
)
```

- [ ] **Step 2: Write the contract test**

Append to `tests/test_latvia_ur_financials_tables.py`:

```python
METRICS_MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "clickhouse"
    / "migrations"
    / "000018_corpscout_lv_financial_metrics.up.sql"
).read_text()


def test_metric_columns_match_migration():
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_LV_FINANCIAL_METRICS_TABLE}"
        in METRICS_MIGRATION
    )
    for column in tables.LV_FINANCIAL_METRICS_COLUMNS:
        assert f"    {column} " in METRICS_MIGRATION, f"missing {column}"


def test_each_metric_source_column_exists_in_wide_schema():
    for source_col in tables.FINANCIAL_METRIC_SOURCE_COLUMNS.values():
        assert source_col in tables.LV_FINANCIAL_STATEMENTS_COLUMNS
```

- [ ] **Step 3: Run**

Run: `uv run pytest tests/test_latvia_ur_financials_tables.py -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add dagster_v3/src/dagster_v3/defs/latvia_ur/tables.py dagster_v3/tests/test_latvia_ur_financials_tables.py
git commit -m "feat(latvia_ur): add financial-metrics column schema"
```

---

### Task 3: EUR→USD metric builder (`metrics.py`)

**Files:**
- Create: `src/dagster_v3/defs/latvia_ur/metrics.py`
- Test: `tests/test_latvia_ur_metrics.py`

This adapts Norway's `financial_normalize.py`: read the wide statements, scale by `rounded_to_nearest`, batch one `ExchangeRateRequest(currency, period_end_date)` per distinct pair, convert each metric, write `latvia_ur.financial_metrics`.

- [ ] **Step 1: Write the failing test (with a stub ExchangeRates)**

`tests/test_latvia_ur_metrics.py`:

```python
from decimal import Decimal
from pathlib import Path

import duckdb

from dagster_v3.defs.latvia_ur import metrics, tables
from dagster_v3.defs.latvia_ur.financials import build_latvia_ur_financial_statements
from tests.test_latvia_ur_financials import _seed_raw  # reuse the raw seed helper


class _StubRate:
    def __init__(self, rate: Decimal, rate_date: str) -> None:
        self.rate = rate
        self.rate_date = rate_date
        self.source = "TEST"

    def convert(self, amount: Decimal):
        return None if amount is None else (amount * self.rate)


class _StubExchangeRates:
    def usd_rates(self, requests):
        # EUR -> USD at a flat 1.10 for any requested (currency, date)
        return {
            (r.currency, r.rate_date): _StubRate(Decimal("1.10"), r.rate_date)
            for r in requests
        }


def test_metrics_scale_and_convert(tmp_path: Path):
    db_path = tmp_path / "latvia_ur_source.duckdb"
    _seed_raw(db_path)
    build_latvia_ur_financial_statements(database_path=db_path, source_run_id="run-1")

    counts = metrics.build_latvia_ur_financial_metrics(
        database_path=db_path,
        source_run_id="run-1",
        exchange_rates=_StubExchangeRates(),
    )
    assert counts["metrics"] == 2

    wide = f"{tables.DLT_DATASET_NAME}.{tables.FINANCIAL_METRICS_WIDE_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            f"select revenue_amount_original, revenue_amount_usd, "
            f"net_result_amount_original, net_result_amount_usd, fx_rate_to_usd, fx_source "
            f"from {wide} where statement_id = '709390'"
        ).fetchone()
        cols = [r[0] for r in conn.execute(f"describe {wide}").fetchall()]
    # ONES factor -> revenue 135 EUR; USD = 135 * 1.10 = 148.5
    assert row[0] == Decimal("135.00")
    assert row[1] == Decimal("148.50")
    assert row[2] == Decimal("-3860.00")     # net_income, signed
    assert row[3] == Decimal("-4246.00")     # -3860 * 1.10
    assert row[4] == Decimal("1.100000000000")
    assert row[5] == "TEST"
    assert set(cols) == set(tables.LV_FINANCIAL_METRICS_COLUMNS)
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_latvia_ur_metrics.py -q`
Expected: FAIL (`metrics` module missing).

- [ ] **Step 3: Implement `metrics.py`**

`src/dagster_v3/defs/latvia_ur/metrics.py`:

```python
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

import duckdb

from dagster_v3.defs.latvia_ur import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
WIDE_STATEMENTS = tables.FINANCIAL_STATEMENTS_WIDE_TABLE
METRICS_TABLE = tables.FINANCIAL_METRICS_WIDE_TABLE
SOURCE_SLUG = "latvia_ur_financials"


class ExchangeRateRequestLike:
    def __init__(self, currency: str, rate_date: str) -> None:
        self.currency = currency
        self.rate_date = rate_date


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], Any]: ...


def _request(currency: str, rate_date: str) -> Any:
    # Import here so tests can inject a stub without the real client/env.
    from exchange_rates import ExchangeRateRequest

    return ExchangeRateRequest(currency=currency, rate_date=rate_date)


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def build_latvia_ur_financial_metrics(
    *,
    database_path: str | Path,
    source_run_id: str,
    exchange_rates: ExchangeRates,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    metric_cols = ", ".join(
        f"{src} as {name}"
        for name, src in tables.FINANCIAL_METRIC_SOURCE_COLUMNS.items()
    )
    select_sql = f"""
        select
            statement_id, regcode, fiscal_year, period_start_date, period_end_date,
            employees, currency, rounded_to_nearest, source_payload_hash,
            {metric_cols}
        from {DLT_DATASET_NAME}.{WIDE_STATEMENTS}
    """
    with duckdb.connect(str(database_path)) as connection:
        cursor = connection.execute(select_sql)
        names = [d[0] for d in cursor.description]
        records = [dict(zip(names, r, strict=True)) for r in cursor.fetchall()]

    # one rate request per (currency, period_end_date)
    requests: dict[tuple[str, str], Any] = {}
    for rec in records:
        currency = str(rec["currency"] or "").upper()
        end = "" if rec["period_end_date"] is None else str(rec["period_end_date"])
        if currency and end:
            requests[(currency, end)] = _request(currency, end)
    rates = _load_rates(exchange_rates, list(requests.values()))

    rows = [_metric_row(rec, rates=rates, source_run_id=source_run_id) for rec in records]
    _write_metrics_table(database_path, rows)

    counts = {"metrics": len(rows), "rate_pairs": len(requests)}
    if log is not None:
        log("Built Latvia UR financial metrics: metrics=%s, rate_pairs=%s",
            counts["metrics"], counts["rate_pairs"])
    return counts


def _load_rates(exchange_rates: ExchangeRates, requests: list[Any]) -> dict[tuple[str, str], Any]:
    if not requests:
        return {}
    try:
        return exchange_rates.usd_rates(requests)
    except Exception:
        rates: dict[tuple[str, str], Any] = {}
        for request in requests:
            try:
                rates.update(exchange_rates.usd_rates([request]))
            except Exception:
                continue
        return rates


def _metric_row(rec: dict[str, Any], *, rates: dict[tuple[str, str], Any], source_run_id: str) -> dict[str, Any]:
    currency = str(rec["currency"] or "").upper()
    end = "" if rec["period_end_date"] is None else str(rec["period_end_date"])
    factor = tables.ROUNDED_TO_NEAREST_FACTORS.get(str(rec["rounded_to_nearest"] or "").upper(), 1)
    fx_rate = rates.get((currency, end))

    row: dict[str, Any] = {
        "country_iso2": "LV",
        "source_slug": SOURCE_SLUG,
        "source_run_id": source_run_id,
        "source_record_id": rec["statement_id"],
        "source_payload_hash": rec["source_payload_hash"],
        "statement_id": rec["statement_id"],
        "regcode": rec["regcode"],
        "fiscal_year": rec["fiscal_year"],
        "period_start_date": rec["period_start_date"],
        "period_end_date": rec["period_end_date"],
        "employees": rec["employees"],
        "currency": currency,
        "rounded_to_nearest": rec["rounded_to_nearest"],
        "fx_rate_to_usd": None if fx_rate is None else fx_rate.rate,
        "fx_rate_date": None if fx_rate is None else fx_rate.rate_date,
        "fx_source": "" if fx_rate is None else fx_rate.source,
        "resolved_at": datetime.now(timezone.utc),
    }
    for metric in tables.FINANCIAL_METRIC_NAMES:
        raw = _decimal(rec[metric])
        scaled = None if raw is None else (raw * factor)
        row[f"{metric}_amount_original"] = scaled
        row[f"{metric}_amount_usd"] = (
            None if scaled is None or fx_rate is None else fx_rate.convert(scaled)
        )
    return row


def _write_metrics_table(database_path: str | Path, rows: list[dict[str, Any]]) -> None:
    columns = tables.LV_FINANCIAL_METRICS_COLUMNS
    decimal_amount_cols = {
        f"{m}_amount_original" for m in tables.FINANCIAL_METRIC_NAMES
    } | {f"{m}_amount_usd" for m in tables.FINANCIAL_METRIC_NAMES}

    def ddl_type(col: str) -> str:
        if col in decimal_amount_cols:
            return "decimal(38, 2)"
        if col == "fx_rate_to_usd":
            return "decimal(38, 12)"
        if col in {"period_start_date", "period_end_date", "fx_rate_date"}:
            return "date"
        if col == "fiscal_year":
            return "integer"
        if col == "employees":
            return "bigint"
        if col == "resolved_at":
            return "timestamp"
        return "varchar"

    col_defs = ", ".join(f"{c} {ddl_type(c)}" for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    qualified = f"{DLT_DATASET_NAME}.{METRICS_TABLE}"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        connection.execute(f"drop table if exists {qualified}")
        connection.execute(f"create table {qualified} ({col_defs})")
        if rows:
            connection.executemany(
                f"insert into {qualified} ({', '.join(columns)}) values ({placeholders})",
                [tuple(r.get(c) for c in columns) for r in rows],
            )
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_latvia_ur_metrics.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dagster_v3/src/dagster_v3/defs/latvia_ur/metrics.py dagster_v3/tests/test_latvia_ur_metrics.py
git commit -m "feat(latvia_ur): build EUR/USD financial metrics from wide statements"
```

---

### Task 4: metric asset + ClickHouse export

**Files:**
- Modify: `src/dagster_v3/defs/latvia_ur/clickhouse.py` (add `export_latvia_ur_clickhouse_financial_metrics`)
- Modify: `src/dagster_v3/defs/latvia_ur/assets.py` (add `latvia_ur_financial_metrics_duckdb` + `latvia_ur_clickhouse_financial_metrics`)
- Test: `tests/test_latvia_ur_metrics.py` (extend with an export-wiring test)

- [ ] **Step 1: Add the export function** to `clickhouse.py` (mirror `export_latvia_ur_clickhouse_financial_statements`, swapping table/columns to `tables.LV_FINANCIAL_METRICS_TABLE`, `tables.FINANCIAL_METRICS_WIDE_TABLE`, `tables.LV_FINANCIAL_METRICS_COLUMNS`).

- [ ] **Step 2: Add two assets** to `assets.py`:

```python
@dg.asset(
    name="latvia_ur_financial_metrics_duckdb",
    deps=[dg.AssetKey("latvia_ur_financial_statements_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="Latvia UR headline financial metrics (EUR + USD) from the wide statements.",
)
def latvia_ur_financial_metrics_duckdb(context: AssetExecutionContext) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient
    from dagster_v3.defs.latvia_ur.metrics import build_latvia_ur_financial_metrics

    counts = build_latvia_ur_financial_metrics(
        database_path=LATVIA_UR_DUCKDB_PATH,
        source_run_id=context.run_id,
        exchange_rates=ExchangeRateClient.from_env(),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_financial_metrics_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=LATVIA_UR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_LV_FINANCIAL_METRICS_TABLE},
    description="Latvia UR financial metrics exported to ClickHouse corpscout.lv_financial_metrics.",
)
def latvia_ur_clickhouse_financial_metrics(
    context: AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    from dagster_v3.defs.latvia_ur.clickhouse import (
        export_latvia_ur_clickhouse_financial_metrics,
    )

    rows = export_latvia_ur_clickhouse_financial_metrics(
        database_path=LATVIA_UR_DUCKDB_PATH, clickhouse=clickhouse, log=context.log.info
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_LV_FINANCIAL_METRICS_TABLE}
    )
```

- [ ] **Step 3: Export-wiring test** (extend `tests/test_latvia_ur_metrics.py`): build metrics with the stub, monkeypatch `ClickhouseResource.get_connection` to a fake whose `system.tables` returns `[(tables.LV_FINANCIAL_METRICS_TABLE,)]`, call `export_latvia_ur_clickhouse_financial_metrics`, assert `rows == 2`, an `EXCHANGE TABLES` statement, and inserted column count `== len(tables.LV_FINANCIAL_METRICS_COLUMNS)`.

- [ ] **Step 4: Verify**

Run: `uv run pytest tests/test_latvia_ur_metrics.py -q` → PASS.
Run: `uv run dg check defs` → all definitions load.
Run: `uv run dg list defs | grep latvia` → confirm `latvia_ur_financial_metrics_duckdb` and `latvia_ur_clickhouse_financial_metrics` appear.

- [ ] **Step 5: Commit**

```bash
git add dagster_v3/src/dagster_v3/defs/latvia_ur/clickhouse.py dagster_v3/src/dagster_v3/defs/latvia_ur/assets.py dagster_v3/tests/test_latvia_ur_metrics.py
git commit -m "feat(latvia_ur): metrics asset + ClickHouse export"
```

---

## Self-Review

- **Coverage:** migration (T1), schema+contract (T2), scale+EUR→USD builder (T3), asset+export (T4). EUR→USD via the shared `ExchangeRateClient` keyed on `period_end_date`; `rounded_to_nearest` scaling applied before FX; signed metrics preserved; FX provenance columns populated; `*_usd` NULL when no rate.
- **Type consistency:** `LV_FINANCIAL_METRICS_COLUMNS` is the single source of order for the DuckDB table, the export, and the migration contract test. `build_latvia_ur_financial_metrics` and `export_latvia_ur_clickhouse_financial_metrics` signatures match their call sites.
- **No placeholders:** Tasks 1–3 carry full code; Task 4 reuses the already-built export pattern (one analogous function + two assets) and references exact identifiers.

## Out of scope (separate future plans)
- **Phase 2 — resolved canonical company table** (`name_normalized`, `primary_website_*`): mostly duplicates Module 1's `lv_companies` until websites exist; low marginal value now.
- **Phase 3 — website discovery** (`lv_websites` + the `domains` UNION-ALL block): requires the crawl-service (scheduler/Temporal), which is outside `dagster_v3`. Needs its own integration plan; Wikidata gives free partial coverage in the meantime.
- **LVL legacy handling:** only if `SELECT DISTINCT currency` on the full load shows non-EUR rows; convert at the fixed peg (1 EUR = 0.702804 LVL) before USD.
