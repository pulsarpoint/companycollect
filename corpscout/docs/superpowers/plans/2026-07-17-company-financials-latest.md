# Per-Country Latest-Financials Tables + /companies Revenue Column Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build per-country `xx_company_financials_latest` ClickHouse tables (one row per company: latest fiscal year's headline stats, USD-normalized) via Dagster CH→CH assets that rebuild whenever that country's financials update — then surface a sortable **Revenue (USD)** column and a **Has financials** filter on the backoffice `/companies` page.

**Architecture:** dagster_v3 gets one new cross-source module `defs/company_financials_latest/` mirroring the existing `defs/domains` precedent (pure ClickHouse→ClickHouse assets with cross-module `deps=[dg.AssetKey(...)]`, stage-table + `EXCHANGE TABLES`). One asset per country (8 countries: no, fi, se, ee, lv, gb, br, sk), each with `automation_condition=dg.AutomationCondition.eager()` (rebuild when the upstream export lands) plus one daily fallback schedule. The backoffice registry gains a per-country `financialsLatest` config; the unified UNION query LEFT-JOINs the tiny summary table per branch for the revenue column and uses a semi-join for the Has-financials filter. "Has financials" = the join row exists — no separate presence table.

**Tech Stack:** dagster_v3 (Python, clickhouse-driver via `ClickhouseResource`, golang-migrate migrations, pytest, `uv run`), backoffice (React Router 8, TypeScript, vitest live-ClickHouse tests).

## Global Constraints

- dagster_v3 conventions bind (its CLAUDE.md): `uv run` everywhere; NO `from __future__ import annotations` in asset modules; migration owns the CH schema (no DDL in Python beyond the stage `CREATE TABLE … AS` + `EXCHANGE TABLES` pattern); refuse to replace on empty input; commit by explicit path (tree carries heavy unrelated WIP); `uv run dg check defs` before done.
- **Migration number is 000137** (`000137_corpscout_company_financials_latest`). Migrations 000134–136 exist on disk but are UNCOMMITTED WIP from another session — do not touch them, do not commit them. The `EXPECTED_MIGRATIONS` tuple in `tests/test_clickhouse_migrations.py` carries their uncommitted lines too: to append the 000137 line without committing WIP, use the proven stash dance (`git stash push <file>` → edit → commit → `git stash pop` → verify stash empty; if the pop conflicts, resolve by keeping BOTH the WIP lines and the new line in order and report it).
- Uniform summary schema (all 8 tables, exact): `company_id String, fiscal_year Nullable(Int32), period_end_date Nullable(Date), currency LowCardinality(String), revenue_amount_original Nullable(Float64), revenue_amount_usd Nullable(Float64), net_result_amount_original Nullable(Float64), net_result_amount_usd Nullable(Float64), total_assets_amount_original Nullable(Float64), total_assets_amount_usd Nullable(Float64), equity_amount_original Nullable(Float64), equity_amount_usd Nullable(Float64), employees Nullable(Float64), years_count UInt32, resolved_at DateTime64(3, 'UTC')` — `ENGINE = MergeTree ORDER BY company_id` (sort key non-nullable). `currency` coalesces to `''` (non-nullable String rule).
- **USD derivation is pure SQL**: `coalesce(<usd column>, <original column> * toFloat64(fx_rate_to_usd))` — verified live: `no_financial_statements` has `fx_rate_to_usd` on 425,372/425,380 rows while stored USD covers only 57,764. Apply the coalesce uniformly in every country's insert SQL (harmless where stored USD is dense). No `ExchangeRateClient` calls needed.
- Latest-year selection must mirror each country's existing backoffice `detail.financialsQuery` ORDER BY semantics (they encode post-bugfix restatement tiebreakers): order `fiscal_year DESC` + the country's tiebreaker, `LIMIT 1 BY company_id`. ClickHouse sorts NULLs last by default, so NULL fiscal years never win.
- The empty-stage guard: after INSERT, `SELECT count()` on the stage and `raise ValueError` on 0 rows before EXCHANGE (all 8 tables have data today — smallest is sk with 1 company).
- Backoffice rules: identifiers only from the registry; user values only via CH named params; UNION branch type alignment (every branch must emit identically-typed `revenue_usd Nullable(Float64)` and `fiscal_year Nullable(Int32)`, using `CAST(NULL AS Nullable(Float64))` etc. for countries without a summary table); dev server 5183 is USER-OWNED — never touch it; tests run live against ClickHouse.
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

## Ground truth (verified live, 2026-07-17)

- Distinct companies with financial data: SE 525,494 · NO 425,368 · EE 321,384 · LV 253,928 · GB 24,115 · FI 21,315 · BR 1,218 · SK 1 → ≈1.57M summary rows total.
- Source tables/id columns: `no_financial_statements` (`org_number`; revenue = `operating_revenue_amount_*`; fiscal_year Nullable(Int64); tiebreak `resolved_at DESC`), `fi_financial_metrics` (`business_id`; net result = `profit_loss_amount_*`; has `employees`), `se_financial_metrics` (`company_id`; net result = `profit_loss_amount_*`; has `employees`; period col `report_period_end`), `ee_financial_metrics` (`reg_code`; `net_result_amount_*`; NO employees col), `lv_financial_metrics` (`regcode`; `net_result_amount_*`; has `employees`; tiebreak `resolved_at DESC`), `gb_financial_metrics` (`company_number`; `net_result_amount_*`; NO employees), `sk_financial_metrics` (`ico`; wide harmonized shape — implementer verifies exact net-result column name via DESCRIBE), `br_cvm_financial_metrics` (**a CH VIEW**, LONG format: one row per `(cnpj_basico, period_end_date, metric_name, …)` with `amount_original/amount_usd/currency/period_type/consolidation_type/reference_date/version`; metric names `revenue`, `net_income`, `total_assets`, `equity`; the pivot pattern lives verbatim in backoffice `countries.ts` br `financialsQuery` lines ~583-600: filter `period_type='annual'`, order `consolidation_type='consolidated' DESC, reference_date DESC, version DESC`, `LIMIT 1 BY fy, metric`, then `anyIf` pivot per fy).
- Producing (upstream) asset keys for deps/automation: no → `norway_brreg_financial_statements_snapshot_clickhouse` + `norway_brreg_financial_statements_updates_clickhouse`; fi → `fi_financial_metrics_ch`; se → `sweden_financial_metrics_clickhouse`; ee → `estonia_ar_clickhouse_financial_metrics`; lv → `latvia_financial_metrics_clickhouse`; gb → `uk_companies_house_clickhouse_financial_metrics` + `uk_companies_house_pdf_financial_metrics` + `uk_companies_house_accounts_incremental`; br → `brazil_fin_cvm_dfp_statement_rows_clickhouse` + `brazil_fin_cvm_itr_statement_rows_clickhouse`; sk → `slovakia_financials_metrics_clickhouse`.
- CH→CH asset precedent: `defs/domains/assets.py:34` (`domains_clickhouse` — cross-module deps, `CREATE TABLE stage AS target` → `INSERT INTO … SELECT` → `EXCHANGE TABLES` → `DROP … stage` in finally). `ClickhouseResource` injected as asset param; `with clickhouse.get_connection() as client:`. `assert_clickhouse_tables_exist(clickhouse, database=…, tables=…)` from `defs/clickhouse/resolved.py:25`. `RESOLVED_DATABASE = "corpscout"`. These pure-CH assets carry NO pool.
- `dg.AutomationCondition` is unused in the repo so far — `eager()` is additive (needs the daemon's automation sensor; the daily fallback schedule guarantees rebuilds regardless). Schedule pattern precedent: `sweden_financial/assets.py:428-443` (`dg.define_asset_job` + `dg.ScheduleDefinition`).
- Backoffice unified layer: `branchSql` at `app/lib/unified.server.ts:118-126` (per-branch over-fetch `LIMIT page*pageSize`, outer merge sort + `LIMIT/OFFSET`); `UNION_SORTS = new Set(["country","name"])` at :33; `canAnswer` special-cases `industry` at :35-38; `branchWhere` at :53-77; count branch at :102-109 (no join — use a semi-join for the filter). Empties-last idiom: `coalesce(toString(expr), '') = '' ASC, expr DIR, id`.
- Registry: `se_companies` joins financials on `company_id` (NOT its `idColumn` `registration_number`) — same split as its `industryJoinKeyExpr`; `br_companies` joins on `cnpj_basico`. All other countries join on their `idColumn`.
- Filters: `UNIFIED_FACET_KEYS = ["country", ...COLUMN_FACET_KEYS, "industry"]` (`app/lib/filters.ts:38`); `country` is whitelist-special-cased in `parseUnifiedFilters` (:51-63) — `has_financials` follows the same shape whitelisted to `["true"]`.
- No compact number formatter exists in the app yet; idiom = module-level `const nf = new Intl.NumberFormat(…)`.
- FilterSidebar (`app/components/data-table/filter-sidebar.tsx:132-158`) maps facet keys to `FacetCombobox`; no boolean toggle exists — new small `FacetToggle` using existing `toggleFilterValue` (`url.ts:34-46`) with sentinel value `"true"`; `app/components/ui/checkbox.tsx` exists unused.
- Live tests: `tests/unified.server.test.ts` (live CH, 30-60s timeouts, e.g. "country facet lists all 10").

## Out of scope (logged)

- Numeric range filters (revenue min/max) — needs new filter UI; sort + boolean filter only in this pass.
- fr/cz (no financial source) — branches emit NULL revenue, excluded from the filter via `canAnswer`.
- SK pipeline only has 1 metrics row (pre-existing gap); the summary table is built generically and will fill when that pipeline is fixed.
- Per-country detail pages already show full statements — unchanged.

---

### Task 1: ClickHouse migration 000137 + contract scaffolding

**Files:**
- Create: `corpscout/clickhouse/migrations/000137_corpscout_company_financials_latest.up.sql`
- Create: `corpscout/clickhouse/migrations/000137_corpscout_company_financials_latest.down.sql`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/__init__.py` (empty)
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/tables.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` — stash dance, see Global Constraints)
- Test: `corpscout/services/dagster_v3/tests/test_company_financials_latest.py` (contract part)

**Interfaces:**
- Produces: CH tables `corpscout.{no,fi,se,ee,lv,gb,br,sk}_company_financials_latest`; Python constants `COMPANY_FINANCIALS_LATEST_TABLES: tuple[str, ...]` and `COMPANY_FINANCIALS_LATEST_COLUMNS: tuple[str, ...]` in `tables.py` (Tasks 2–3 consume both).

- [ ] **Step 1: Write `tables.py`**

```python
COMPANY_FINANCIALS_LATEST_COUNTRIES = ("no", "fi", "se", "ee", "lv", "gb", "br", "sk")

COMPANY_FINANCIALS_LATEST_TABLES = tuple(
    f"{code}_company_financials_latest" for code in COMPANY_FINANCIALS_LATEST_COUNTRIES
)

COMPANY_FINANCIALS_LATEST_COLUMNS = (
    "company_id",
    "fiscal_year",
    "period_end_date",
    "currency",
    "revenue_amount_original",
    "revenue_amount_usd",
    "net_result_amount_original",
    "net_result_amount_usd",
    "total_assets_amount_original",
    "total_assets_amount_usd",
    "equity_amount_original",
    "equity_amount_usd",
    "employees",
    "years_count",
    "resolved_at",
)
```

- [ ] **Step 2: Write the migration**

`000137_…up.sql` — `CREATE DATABASE IF NOT EXISTS corpscout;` then eight identical `CREATE TABLE IF NOT EXISTS corpscout.<name>` blocks with EXACTLY the Global-Constraints schema (4-space-indented column lines like the sibling migrations so the space-delimited contract match works), `ENGINE = MergeTree ORDER BY company_id`. Example block (repeat for all 8 names):

```sql
CREATE TABLE IF NOT EXISTS corpscout.no_company_financials_latest
(
    company_id String,
    fiscal_year Nullable(Int32),
    period_end_date Nullable(Date),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Float64),
    revenue_amount_usd Nullable(Float64),
    net_result_amount_original Nullable(Float64),
    net_result_amount_usd Nullable(Float64),
    total_assets_amount_original Nullable(Float64),
    total_assets_amount_usd Nullable(Float64),
    equity_amount_original Nullable(Float64),
    equity_amount_usd Nullable(Float64),
    employees Nullable(Float64),
    years_count UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY company_id;
```

`down.sql`: eight `DROP TABLE IF EXISTS corpscout.<name>;` lines.

- [ ] **Step 3: Contract test (write failing first)**

In `tests/test_company_financials_latest.py`:

```python
from pathlib import Path

from dagster_v3.defs.company_financials_latest.tables import (
    COMPANY_FINANCIALS_LATEST_COLUMNS,
    COMPANY_FINANCIALS_LATEST_TABLES,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def _migration_sql() -> str:
    return (MIGRATIONS_DIR / "000137_corpscout_company_financials_latest.up.sql").read_text()


def test_migration_creates_every_summary_table_with_full_schema() -> None:
    sql = _migration_sql()
    assert len(COMPANY_FINANCIALS_LATEST_TABLES) == 8
    for table in COMPANY_FINANCIALS_LATEST_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in sql
    for column in COMPANY_FINANCIALS_LATEST_COLUMNS:
        assert f"    {column} " in sql
```

(Adjust `parents[3]` if the migrations dir resolves differently — mirror how `test_clickhouse_migrations.py` computes `MIGRATIONS_DIR` and reuse that exact approach.)

Run: `uv run pytest tests/test_company_financials_latest.py -q` → FAIL (no migration yet) → create the files → PASS.

- [ ] **Step 4: Append to EXPECTED_MIGRATIONS (stash dance) and verify**

`git stash push corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` → append `"000137_corpscout_company_financials_latest",` after the last committed entry → run `uv run pytest tests/test_clickhouse_migrations.py -q` (NOTE: with the WIP stashed, the exact-file-match test sees disk files 000134-136 that the committed tuple lacks — if it fails for exactly those three, that is pre-existing WIP inconsistency, not yours; confirm your 000137 entries pass by asserting the specific tests you can, then proceed) → commit → `git stash pop` → verify `git stash list` empty and the WIP lines are back. Re-run `uv run pytest tests/test_clickhouse_migrations.py -q` with the tree restored → all pass.

- [ ] **Step 5: Apply the migration to ClickHouse**

Apply per the project's migration runbook (golang-migrate against corpscout DB; if the migrate CLI isn't available locally, applying the up.sql via `curl` to the CH HTTP endpoint statement-by-statement is acceptable for the dev CH and MUST be reported). Verify: `SELECT count() FROM system.tables WHERE database='corpscout' AND name LIKE '%_company_financials_latest'` → 8.

- [ ] **Step 6: Commit**

```bash
git add corpscout/clickhouse/migrations/000137_corpscout_company_financials_latest.up.sql corpscout/clickhouse/migrations/000137_corpscout_company_financials_latest.down.sql corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/__init__.py corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/tables.py corpscout/services/dagster_v3/tests/test_company_financials_latest.py corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(dagster): company financials latest summary tables migration"
```

---

### Task 2: Dagster assets — per-country insert SQL + module

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/sql.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/assets.py`
- Test: extend `corpscout/services/dagster_v3/tests/test_company_financials_latest.py`

**Interfaces:**
- Consumes: `tables.py` constants (Task 1); upstream CH tables (Ground truth).
- Produces: assets named `{code}_company_financials_latest_clickhouse` for the 8 codes; `build_latest_insert_sql(code: str) -> str` in `sql.py` returning `INSERT INTO corpscout.<stage> …` -ready SELECT (the asset wraps it); module-level `defs = dg.Definitions(...)` with the job + schedule.

- [ ] **Step 1: Write `sql.py`**

One `SOURCES` spec dict + a builder. The SELECT body per country (exact SQL; `{stage}` is formatted in by the asset):

```python
"""Per-country SELECTs producing the uniform latest-financials row set.

Latest year per company mirrors each country's backoffice detail-query
tiebreakers. USD falls back to original * fx_rate_to_usd where stored USD
is null (Norway's stored USD covers ~14% of rows but fx_rate ~100%).
"""

_WIDE_TEMPLATE = """
SELECT
  toString({id}) AS company_id,
  toInt32(fiscal_year) AS fiscal_year,
  {period_end} AS period_end_date,
  coalesce({currency}, '') AS currency,
  toFloat64({rev}_amount_original) AS revenue_amount_original,
  toFloat64(coalesce({rev}_amount_usd, {rev}_amount_original * toFloat64(fx_rate_to_usd))) AS revenue_amount_usd,
  toFloat64({net}_amount_original) AS net_result_amount_original,
  toFloat64(coalesce({net}_amount_usd, {net}_amount_original * toFloat64(fx_rate_to_usd))) AS net_result_amount_usd,
  toFloat64(total_assets_amount_original) AS total_assets_amount_original,
  toFloat64(coalesce(total_assets_amount_usd, total_assets_amount_original * toFloat64(fx_rate_to_usd))) AS total_assets_amount_usd,
  toFloat64(equity_amount_original) AS equity_amount_original,
  toFloat64(coalesce(equity_amount_usd, equity_amount_original * toFloat64(fx_rate_to_usd))) AS equity_amount_usd,
  {employees} AS employees,
  toUInt32(count() OVER (PARTITION BY {id})) AS years_count,
  now64(3) AS resolved_at
FROM corpscout.{table}
ORDER BY fiscal_year DESC NULLS LAST, {tiebreak}
LIMIT 1 BY {id}
"""
```

NOTE: `count() OVER (PARTITION BY …)` combined with `LIMIT 1 BY` — the implementer MUST verify window-then-limit ordering works in this CH version with a quick manual query; if the window function is awkward, the equally correct fallback is a join against `(SELECT {id}, toUInt32(uniqExact(fiscal_year)) AS years_count FROM corpscout.{table} GROUP BY {id})` — pick whichever reads cleaner after testing, report which.

`SOURCES` per country (fill `_WIDE_TEMPLATE`):

| code | table | id | currency | rev | net | employees | period_end | tiebreak |
|---|---|---|---|---|---|---|---|---|
| no | no_financial_statements | org_number | currency | operating_revenue | net_result | CAST(NULL AS Nullable(Float64)) | period_end_date | resolved_at DESC |
| fi | fi_financial_metrics | business_id | currency_original | revenue | profit_loss | toFloat64(employees) | toDate(period_end) | resolved_at DESC |
| se | se_financial_metrics | company_id | currency | revenue | profit_loss | toFloat64(employees) | toDate(report_period_end) | resolved_at DESC |
| ee | ee_financial_metrics | reg_code | currency | revenue | net_result | CAST(NULL AS Nullable(Float64)) | period_end_date | resolved_at DESC |
| lv | lv_financial_metrics | regcode | currency | revenue | net_result | toFloat64(employees) | period_end_date | resolved_at DESC |
| gb | gb_financial_metrics | company_number | currency | revenue | net_result | CAST(NULL AS Nullable(Float64)) | period_end_date | resolved_at DESC |
| sk | sk_financial_metrics | ico | currency_original | revenue | net_result | CAST(NULL AS Nullable(Float64)) | period_end_date | resolved_at DESC |

All seven wide tables carry `fx_rate_to_usd`, `resolved_at`, and the listed period/currency columns — verified live via `system.columns` (2026-07-17). Only remaining type unknowns: fi `period_end` and se `report_period_end` may be DateTime rather than Date — hence the `toDate(...)` wrappers; if either is already Date, drop the wrapper.

**BR is special** (long-format view; no fx_rate; pivot mirrors the backoffice br `financialsQuery`):

```sql
SELECT
  toString(cnpj_basico) AS company_id,
  toInt32(fy) AS fiscal_year,
  max(ped) AS period_end_date,
  coalesce(any(cur), '') AS currency,
  anyIf(orig, metric = 'revenue') AS revenue_amount_original,
  anyIf(usd, metric = 'revenue') AS revenue_amount_usd,
  anyIf(orig, metric = 'net_income') AS net_result_amount_original,
  anyIf(usd, metric = 'net_income') AS net_result_amount_usd,
  anyIf(orig, metric = 'total_assets') AS total_assets_amount_original,
  anyIf(usd, metric = 'total_assets') AS total_assets_amount_usd,
  anyIf(orig, metric = 'equity') AS equity_amount_original,
  anyIf(usd, metric = 'equity') AS equity_amount_usd,
  CAST(NULL AS Nullable(Float64)) AS employees,
  toUInt32(uniqExact(fy) OVER (PARTITION BY cnpj_basico)) AS years_count,
  now64(3) AS resolved_at
FROM (
  SELECT cnpj_basico, toYear(period_end_date) AS fy, max(period_end_date) OVER (PARTITION BY cnpj_basico, toYear(period_end_date)) AS ped,
    metric_name AS metric, toFloat64(amount_original) AS orig, toFloat64(amount_usd) AS usd, currency AS cur
  FROM corpscout.br_cvm_financial_metrics
  WHERE period_type = 'annual'
  ORDER BY consolidation_type = 'consolidated' DESC, reference_date DESC, version DESC
  LIMIT 1 BY cnpj_basico, fy, metric
)
GROUP BY cnpj_basico, fy
ORDER BY fy DESC
LIMIT 1 BY cnpj_basico
```

(The BR years_count-over-groups interplay is the trickiest SQL in this plan — the implementer verifies it read-only against live CH and may restructure with an explicit years-count subquery join if the window over grouped rows misbehaves; the CONTRACT is: latest annual fy per company, consolidated preferred, plus the distinct-annual-years count.)

`build_latest_insert_sql(code)` returns the SELECT; assets format `INSERT INTO corpscout.{stage_table} ({", ".join(COMPANY_FINANCIALS_LATEST_COLUMNS)}) {select}` — explicit column list so SELECT order and stage order can never misalign.

- [ ] **Step 2: Write `assets.py`** (mirror `defs/domains/assets.py`; NO pool — pure CH):

```python
import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE, assert_clickhouse_tables_exist
from dagster_v3.defs.company_financials_latest.sql import SOURCES, build_latest_insert_sql
from dagster_v3.defs.company_financials_latest.tables import (
    COMPANY_FINANCIALS_LATEST_COLUMNS,
    COMPANY_FINANCIALS_LATEST_TABLES,
)

UPSTREAM_KEYS = {
    "no": ["norway_brreg_financial_statements_snapshot_clickhouse", "norway_brreg_financial_statements_updates_clickhouse"],
    "fi": ["fi_financial_metrics_ch"],
    "se": ["sweden_financial_metrics_clickhouse"],
    "ee": ["estonia_ar_clickhouse_financial_metrics"],
    "lv": ["latvia_financial_metrics_clickhouse"],
    "gb": ["uk_companies_house_clickhouse_financial_metrics", "uk_companies_house_pdf_financial_metrics", "uk_companies_house_accounts_incremental"],
    "br": ["brazil_fin_cvm_dfp_statement_rows_clickhouse", "brazil_fin_cvm_itr_statement_rows_clickhouse"],
    "sk": ["slovakia_financials_metrics_clickhouse"],
}


def _replace_summary_table(client, *, code: str, log) -> int:
    target = f"{code}_company_financials_latest"
    stage = f"{target}__stage"
    client.execute(f"DROP TABLE IF EXISTS {RESOLVED_DATABASE}.{stage}")
    client.execute(
        f"CREATE TABLE {RESOLVED_DATABASE}.{stage} AS {RESOLVED_DATABASE}.{target}"
    )
    try:
        columns = ", ".join(COMPANY_FINANCIALS_LATEST_COLUMNS)
        client.execute(
            f"INSERT INTO {RESOLVED_DATABASE}.{stage} ({columns}) {build_latest_insert_sql(code)}"
        )
        [(row_count,)] = client.execute(
            f"SELECT count() FROM {RESOLVED_DATABASE}.{stage}"
        )
        if row_count == 0:
            raise ValueError(
                f"{stage} has 0 rows; refusing to replace {target}"
            )
        client.execute(
            f"EXCHANGE TABLES {RESOLVED_DATABASE}.{stage} AND {RESOLVED_DATABASE}.{target}"
        )
        log.info("replaced %s with %s rows", target, row_count)
        return int(row_count)
    finally:
        client.execute(f"DROP TABLE IF EXISTS {RESOLVED_DATABASE}.{stage}")


def _build_asset(code: str):
    @dg.asset(
        name=f"{code}_company_financials_latest_clickhouse",
        group_name="company_financials_latest",
        deps=[dg.AssetKey(key) for key in UPSTREAM_KEYS[code]],
        automation_condition=dg.AutomationCondition.eager(),
        kinds={"clickhouse"},
    )
    def _asset(context: dg.AssetExecutionContext, clickhouse: ClickhouseResource) -> dg.MaterializeResult:
        assert_clickhouse_tables_exist(
            clickhouse,
            database=RESOLVED_DATABASE,
            tables=[f"{code}_company_financials_latest", SOURCES[code]["table"]],
        )
        with clickhouse.get_connection() as client:
            row_count = _replace_summary_table(client, code=code, log=context.log)
        return dg.MaterializeResult(metadata={"row_count": row_count})

    return _asset


company_financials_latest_assets = [_build_asset(code) for code in UPSTREAM_KEYS]

company_financials_latest_job = dg.define_asset_job(
    "company_financials_latest_job",
    selection=dg.AssetSelection.assets(
        *[f"{code}_company_financials_latest_clickhouse" for code in UPSTREAM_KEYS]
    ),
)

company_financials_latest_schedule = dg.ScheduleDefinition(
    job=company_financials_latest_job,
    cron_schedule="30 6 * * *",
    execution_timezone="Europe/Oslo",
)

defs = dg.Definitions(
    assets=company_financials_latest_assets,
    jobs=[company_financials_latest_job],
    schedules=[company_financials_latest_schedule],
)
```

Adapt mechanical details to what `defs/domains/assets.py` and `sweden_financial/assets.py` actually do (resource registration is global in `definitions.py`; kinds/group naming; whether `dg.Definitions` per-module or bare module-level defs are the discovery convention — MATCH the domains module exactly). The BR asset's `SOURCES["br"]["table"]` existence check uses the view name `br_cvm_financial_metrics` (`assert_clickhouse_tables_exist` must work for views — verify; if it checks `system.tables` it does, since views appear there).

- [ ] **Step 3: Unit tests** (extend `tests/test_company_financials_latest.py`): for each code assert `build_latest_insert_sql(code)` mentions the right source table, the right id column aliased to `company_id`, and every one of `COMPANY_FINANCIALS_LATEST_COLUMNS[:-1]` appears as an alias; assert `UPSTREAM_KEYS` covers exactly the 8 codes and `COMPANY_FINANCIALS_LATEST_TABLES` matches. Run with `uv run pytest tests/test_company_financials_latest.py -q` → PASS. `uv run dg check defs` → clean (this also validates the cross-module AssetKeys resolve).

- [ ] **Step 4: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/sql.py corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/assets.py corpscout/services/dagster_v3/tests/test_company_financials_latest.py
git commit -m "feat(dagster): per-country latest financials summary assets"
```

---

### Task 3: Materialize all 8 + ClickHouse verification (operational)

**Files:** none (report carries evidence)

- [ ] **Step 1:** Start dev instance (`./scripts/dagster-dev.sh`, background). Launch all 8 explicitly (they are leaves; their CH upstreams already exist — do NOT pull upstream chains): `uv run dg launch --assets no_company_financials_latest_clickhouse,fi_company_financials_latest_clickhouse,se_company_financials_latest_clickhouse,ee_company_financials_latest_clickhouse,lv_company_financials_latest_clickhouse,gb_company_financials_latest_clickhouse,br_company_financials_latest_clickhouse,sk_company_financials_latest_clickhouse`. Pure-CH aggregations over ≤2M-row tables — minutes, run foreground with generous timeout.
- [ ] **Step 2:** Verify counts match the live uniqExact expectations (± small drift): no≈425,368 · fi≈21,315 · se≈525,494 · ee≈321,384 · lv≈253,928 · gb≈24,115 · br≈1,218 · sk=1. Per table: `SELECT count(), countIf(revenue_amount_usd IS NOT NULL), max(fiscal_year) FROM corpscout.<t>`. Spot-checks: NO company with derived USD (`revenue_amount_usd IS NOT NULL AND fiscal_year >= 2023` on a row whose statements row had NULL stored usd); one row per company (`SELECT count() = uniqExact(company_id)`). Sanity: latest fiscal years cluster 2023-2025.
- [ ] **Step 3:** Clean up the dev instance you started. No commits.

---

### Task 4: Backoffice — registry + unified query (join, sort, filter)

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/countries.ts` (type + 8 country entries)
- Modify: `corpscout/services/backoffice/app/lib/unified.server.ts`
- Modify: `corpscout/services/backoffice/app/lib/filters.ts`
- Test: `corpscout/services/backoffice/tests/unified.server.test.ts` (+ registry live sweep in `tests/queries.server.test.ts` if that's where table-existence sweeps live)

**Interfaces:**
- Produces: `CountryConfig.financialsLatest?: { table: string; companyKeyExpr: string }`; `UnifiedRow` gains `revenue_usd: number | null; fiscal_year: number | null`; sort key `"revenue"`; filter key `"has_financials"` (value whitelist `["true"]`). Task 5 consumes all of these.

- [ ] **Step 1: Registry.** Add to the `CountryConfig` type: `/** Latest-financials summary table (one row per company). companyKeyExpr is the expression on companiesTable matching summary.company_id. */ financialsLatest?: { table: string; companyKeyExpr: string };` Then per country: no `{ table: "no_company_financials_latest", companyKeyExpr: "org_number" }`; fi `business_id`; se `{ table: "se_company_financials_latest", companyKeyExpr: "company_id" }` (NOT registration_number); ee `reg_code`; lv `regcode`; gb `company_number`; br `{ table: "br_company_financials_latest", companyKeyExpr: "cnpj_basico" }`; sk `ico`. fr/cz: omitted.

- [ ] **Step 2: filters.ts.** Add `"has_financials"` to `UNIFIED_FACET_KEYS` (after `"country"`), `has_financials: "Has financials"` to `UNIFIED_FACET_LABELS`, and in `parseUnifiedFilters` whitelist it: `if (key === "has_financials") values = values.filter((v) => v === "true");`.

- [ ] **Step 3: unified.server.ts.**
  - `UNION_SORTS` gains `"revenue"`.
  - `canAnswer`: `if (key === "has_financials") return Boolean(c.financialsLatest);`
  - `branchWhere`: when `filters.has_financials` is active and `c.financialsLatest` exists, push `` `${c.idColumn ...}` `` — exactly: `` conds.push(`${c.financialsLatest.companyKeyExpr} IN (SELECT company_id FROM ${c.financialsLatest.table})`) `` (semi-join → works identically in the count branch, which stays join-free).
  - `branchSql`: when `c.financialsLatest` exists, LEFT JOIN and select typed columns; when not, NULL literals of matching types:

```ts
const fin = c.financialsLatest;
const finSelect = fin
  ? `toNullable(fin.revenue_amount_usd) AS revenue_usd, toNullable(fin.fiscal_year) AS fiscal_year`
  : `CAST(NULL AS Nullable(Float64)) AS revenue_usd, CAST(NULL AS Nullable(Int32)) AS fiscal_year`;
const finJoin = fin
  ? `LEFT JOIN ${fin.table} AS fin ON fin.company_id = toString(${fin.companyKeyExpr})`
  : "";
```

  (`toString(...)` on the companies side because summary `company_id` is String while e.g. some idColumns may be numeric-typed — the live idColumn-is-String test sweep pins them as String today, so `toString` is a no-op safety.) Sort expr for `sort === "revenue"`: branch `ORDER BY isNull(revenue_usd) ASC, revenue_usd ${dirSql}, ${c.idColumn}` — mirror in the outer sort: `isNull(revenue_usd) ASC, revenue_usd ${dirSql}, country_code, id`. IMPORTANT: for `sort === "revenue"`, branches WITHOUT `financialsLatest` can only contribute NULL-revenue rows — keep them (they fill later pages), the isNull-first ordering handles it. Default `dir` for revenue should behave sensibly: `nextSortDir` in url.ts starts new sorts ascending — Task 5 will pass an explicit default of `desc` for revenue (see Task 5 Step 2); server accepts both.
  - The outer SELECT list gains `revenue_usd, fiscal_year`, and `UnifiedRow` the two fields.

- [ ] **Step 4: Live tests (write first, watch fail, implement, pass).** In `tests/unified.server.test.ts`:

```ts
it("revenue sort surfaces real USD revenues descending, empties last", async () => {
  const result = await searchUnifiedCompanies({ sort: "revenue", dir: "desc", pageSize: 25 });
  expect(result.rows.length).toBe(25);
  const revs = result.rows.map((r) => r.revenue_usd);
  expect(revs[0]).toBeGreaterThan(1_000_000);
  for (let i = 1; i < revs.length; i++) {
    if (revs[i] != null && revs[i - 1] != null) expect(revs[i - 1]! >= revs[i]!).toBe(true);
  }
}, 60_000);

it("has_financials filter restricts to companies with summary rows", async () => {
  const result = await searchUnifiedCompanies({ filters: { has_financials: ["true"], country: ["no"] } });
  expect(result.total).toBeGreaterThan(400_000);
  expect(result.total).toBeLessThan(500_000);
}, 30_000);

it("has_financials excludes countries without a summary table", async () => {
  const result = await searchUnifiedCompanies({ filters: { has_financials: ["true"], country: ["fr"] } });
  expect(result.total).toBe(0);
});

it("default sort still returns revenue fields on rows", async () => {
  const result = await searchUnifiedCompanies({ pageSize: 25 });
  for (const row of result.rows) {
    expect(row).toHaveProperty("revenue_usd");
  }
}, 30_000);
```

Run `pnpm vitest run tests/unified.server.test.ts` → all pass (old + new). Measure and REPORT the revenue-sort latency (expect seconds on the deep join-sort — same acceptance as name sort; if it exceeds ~15s, report, don't hide).

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/backoffice/app/lib/countries.ts corpscout/services/backoffice/app/lib/unified.server.ts corpscout/services/backoffice/app/lib/filters.ts corpscout/services/backoffice/tests/unified.server.test.ts
git commit -m "feat(backoffice): unified revenue sort and has-financials filter"
```

---

### Task 5: Backoffice UI — revenue column + toggle + gate

**Files:**
- Modify: `corpscout/services/backoffice/app/components/data-table/unified-columns.tsx`
- Modify: `corpscout/services/backoffice/app/components/data-table/column-header.tsx` (only if a default-dir override is needed; see Step 2)
- Modify: `corpscout/services/backoffice/app/components/data-table/filter-sidebar.tsx`
- Create: none (formatter lives in unified-columns.tsx per existing module-level idiom)
- Test: `corpscout/services/backoffice/app/components/data-table/unified-columns.test.ts` (formatter), plus typecheck/test gate

- [ ] **Step 1: Revenue column.** In `unified-columns.tsx`, module-level `const compactUsd = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });` and a small exported helper:

```ts
export function formatRevenueUsd(value: number | null | undefined, fiscalYear: number | null | undefined): string {
  if (value == null) return "—";
  return `$${compactUsd.format(value)}${fiscalYear != null ? ` (${fiscalYear})` : ""}`;
}
```

New column between `industry` and `country`: header `<div className="text-right"><DataTableColumnHeader label="Revenue (USD)" sortKey="revenue" currentSort={sort} currentDir={dir} /></div>`, cell `<div className="text-right tabular-nums text-sm">{formatRevenueUsd(row.original.revenue_usd, row.original.fiscal_year)}</div>`.

- [ ] **Step 2: Sensible first-click direction.** `nextSortDir` (url.ts) starts new sort keys ascending; for revenue, descending-first is the useful default. Smallest change: in `nextSortDir`, add an optional descending-first set — `const DESC_FIRST = new Set(["revenue"]);` and when `sortKey !== currentSort`, return `DESC_FIRST.has(sortKey) ? "desc" : "asc"`. Keep it in url.ts (pure, already unit-tested there if url tests exist — extend them).

- [ ] **Step 3: Has-financials toggle.** In `filter-sidebar.tsx`, a `FacetToggle` sibling above the combobox list:

```tsx
function FacetToggle({ label, active, onToggle }: { label: string; active: boolean; onToggle: () => void }) {
  return (
    <button
      type="button"
      onClick={onToggle}
      className="flex w-full items-center justify-between rounded-md border px-3 py-2 text-sm hover:bg-accent"
    >
      <span>{label}</span>
      <Checkbox checked={active} className="pointer-events-none" />
    </button>
  );
}
```

Wire in `FilterSidebar`: render `<FacetToggle label="Has financials" active={filters.has_financials?.includes("true") ?? false} onToggle={() => navigate(toggleFilterValue(searchParams, "has_financials", "true"))} />` before the `UNIFIED_FACET_KEYS.map(...)` loop, and EXCLUDE `has_financials` from that map loop (it must not render as a combobox — filter the key out). Reuse the sidebar's existing navigate/searchParams plumbing (`useEffectiveSearchParams`, `useNavigate` — match whatever FacetCombobox uses). Active-filter badges: verify the badge row renders `has_financials: true` sanely — give it the label "Has financials" via `UNIFIED_FACET_LABELS` (already added in Task 4) and if the raw value "true" shows, map it to "yes" in the badge text for this key.

- [ ] **Step 4: Formatter unit tests** (`unified-columns.test.ts`): `formatRevenueUsd(1_234_567, 2024)` → `"$1.2M (2024)"`; `formatRevenueUsd(null, null)` → `"—"`; `formatRevenueUsd(950, 2023)` → `"$950 (2023)"`. Run → pass.

- [ ] **Step 5: Gate.** `pnpm typecheck` clean; `pnpm test` all green. Throwaway dev server (NOT 5183 — start with `pnpm dev`, note it may auto-pick a port like 5186; kill only what you started): `/companies?sort=revenue&dir=desc` shows large-revenue companies first with compact amounts; toggling "Has financials" filters and shows a badge; a country facet + has_financials combined works. Screenshot-level verification via curl + rg on the SSR HTML is sufficient.

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/backoffice/app/components/data-table/unified-columns.tsx corpscout/services/backoffice/app/components/data-table/unified-columns.test.ts corpscout/services/backoffice/app/components/data-table/filter-sidebar.tsx corpscout/services/backoffice/app/components/data-table/url.ts
git commit -m "feat(backoffice): revenue column and has-financials toggle"
```

---

## Deployment note (for the final report, not a task)

The summary tables build from CH tables that exist on the shared ClickHouse, so local materialization (Task 3) populates production-visible data immediately. The Dagster module itself (schedule + eager automation) only takes effect on the remote prod host after the user pushes + deploys — until then the summary tables are a one-shot snapshot that goes stale as country financials update. Same deploy batch as the pending Finland legal-form commits.
