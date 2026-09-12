# SE company financial entity, slice 1: tables and precedence — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the five tables of the SE company financial entity (migration 000401), the package `dagster_v3.defs.se_company.financial` with its table and precedence catalogues and the precedence export asset, pinned by DDL contract tests, and apply them on prod.

**Architecture:** One migration creates the period-keyed suggestion, main, history, precedence and rule tables (spec section 4). `tables.py` is the single place the column order lives and is pinned against the migration DDL through `tests/se_company_ddl.py`; `precedence.py` holds the spec's section-5 map (Ratsit first) with an import-time key check; `assets.py` exports the map as global rows with the idempotent shape the person entity uses. Nothing reads the tables yet: the extractors (slice 2), the fold (slice 3) and the backoffice (slice 4) follow.

**Tech Stack:** ClickHouse 26.5 (golang-migrate migrations), Dagster (`dg.asset`, `ClickhouseResource`, autoloaded through `load_from_defs_folder`), pytest with the repo's DDL helpers in `tests/se_company_ddl.py`.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` — sections 4 (tables), 5 (precedence), 8 (Dagster), 11 (testing), 12 item 1, 13 (names).

## Global Constraints

- Branch `se-financial-entity`, worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity` (at a5ee02a5b, equal to main; `.env` files and `.venv` are already in place from slice 0). Every path below is relative to `corpscout/services/dagster_v3` inside that worktree unless it starts with `corpscout/`. Never touch the main checkout at `/Users/graovic/pulsarpoint/ppoint/companycollect` (the owner's uncommitted work lives there). Do not use `git stash`.
- Migration number **000401** (prod ledger at 400 since 2026-09-12, main's newest file is 000400). Re-check both at merge time.
- Migration file rules: first line `CREATE DATABASE IF NOT EXISTS corpscout;`, no `;` inside SQL comments, last line a statement, a `.down.sql` twin, both registered as the last entry of `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`.
- Table names exactly `se_company_financial_suggestion`, `se_company_financial`, `se_company_financial_history`, `se_company_financial_precedence`, `se_company_financial_rule`; the twenty monetary fields in this order: revenue, operating_costs, operating_result, result_after_financial_items, net_result, ebitda, total_assets, fixed_assets, current_assets, cash_and_bank, equity, share_capital, untaxed_reserves, provisions, liabilities, long_term_liabilities, current_liabilities, personnel_expenses, wages_and_salaries, dividend; each `<field>_amount_original` / `<field>_amount_usd` is `Nullable(Decimal(38, 6))`; `period_key = '<scope>:<period_end>'` pinned by a CHECK; `scope IN ('standalone', 'consolidated')`; `amount_scale IN (1, 1000, 1000000)`.
- One deliberate refinement of spec 4.1: `suggestion_id` is `FixedString(64)` (a sha256 hex, as the address and person entities mint), not a bare `String`; Task 5 records that in the spec.
- Precedence numbers (spec section 5, Ratsit first): reviewer 20000, ratsit 1000, bolagsverket 900, esef 900, bolagsverket_comparative 800, per field exactly as `precedence.py` below lists; `reviewer_draft` appears in no map; a company rule carries 10000.
- The export writes only `company_id = ''`, `period_key = ''` rows, is idempotent (no insert when the stored rows already equal the dictionary), and never touches a company-scoped row.
- Tests run from `corpscout/services/dagster_v3` with `uv run --env-file .env pytest ... -q`; defs-loading tests need the `.env` (it is present). `uv run ruff check <files>` must be clean.
- Commit messages follow Conventional Commits and end with EXACTLY these two lines, pasted verbatim:
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_013oGzirJgExBzVuy9GQBYHz
- Task 6 (prod) runs after the branch's final review and merge, on the owner's standing "do it" for this slice; it creates empty tables and 82 precedence rows, nothing destructive.

---

## File structure

| File | Responsibility |
|---|---|
| `corpscout/clickhouse/migrations/000401_corpscout_se_company_financial_entity.up.sql` / `.down.sql` | The five tables and their removal |
| `src/dagster_v3/defs/se_company/financial/__init__.py` (new) | Package docstring |
| `src/dagster_v3/defs/se_company/financial/tables.py` (new) | Table names, field lists, column tuples in DDL order, key helpers |
| `src/dagster_v3/defs/se_company/financial/precedence.py` (new) | The per-field map, `precedence_for`, `precedence_rows` |
| `src/dagster_v3/defs/se_company/financial/assets.py` (new) | `export_precedence`, the export asset |
| `src/dagster_v3/defs/se_company/financial/docs/financial-design.md` (new) | The package's design note and runbook |
| `tests/test_clickhouse_migrations.py` (modify) | `EXPECTED_MIGRATIONS` entry and the 000401 shape test |
| `tests/test_se_company_financial_tables.py` (new) | DDL contract: column tuples equal the deployed DDL |
| `tests/test_se_company_financial_precedence.py` (new) | The map's invariants and numbers, `precedence_rows` |
| `tests/test_se_company_financial_assets.py` (new) | `export_precedence` against a fake client; asset registration |

---

### Task 1: Migration 000401

**Files:**
- Create: `corpscout/clickhouse/migrations/000401_corpscout_se_company_financial_entity.up.sql`
- Create: `corpscout/clickhouse/migrations/000401_corpscout_se_company_financial_entity.down.sql`
- Modify: `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` ends with `"000400_corpscout_se_ratsit_financial_periods_usd",` — add the new entry after it; add the test after `test_ratsit_financial_periods_usd_migration_adds_a_twin_per_monetary_column`)

**Interfaces:**
- Produces: the five tables whose column order Task 2's `tables.py` mirrors exactly (the DDL test compares them line by line).

- [ ] **Step 1: Write the failing migration test**

Add to `tests/test_clickhouse_migrations.py`:

```python
FINANCIAL_ENTITY_TABLES = (
    "se_company_financial_suggestion",
    "se_company_financial",
    "se_company_financial_history",
    "se_company_financial_precedence",
    "se_company_financial_rule",
)


def test_se_company_financial_entity_migration_declares_five_period_keyed_tables() -> None:
    up_sql = _migration_sql("000401_corpscout_se_company_financial_entity.up.sql")
    down_sql = _migration_sql("000401_corpscout_se_company_financial_entity.down.sql")

    assert up_sql.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    for table in FINANCIAL_ENTITY_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n" in up_sql, table
        assert f"DROP TABLE IF EXISTS corpscout.{table};" in down_sql, table
    assert up_sql.count("CREATE TABLE IF NOT EXISTS") == 5
    assert up_sql.count("ORDER BY (company_id, source, period_key)") == 1
    assert up_sql.count("ORDER BY (company_id, scope, period_end)") == 1
    assert up_sql.count("ORDER BY (company_id, scope, period_end, changed_at)") == 1
    assert up_sql.count("ORDER BY (company_id, period_key, field, source)") == 1
    assert up_sql.count("ORDER BY (company_id, period_key, action)") == 1
    # The period key is derived from scope and period end on both tables that carry it.
    assert up_sql.count(
        "CONSTRAINT valid_period_key CHECK period_key = concat(scope, ':', toString(period_end))"
    ) == 2
    assert "CONSTRAINT valid_amount_scale CHECK amount_scale IN (1, 1000, 1000000)" in up_sql
    assert "CONSTRAINT valid_action CHECK action IN ('hide')" in up_sql
    assert "CONSTRAINT valid_global_scope CHECK company_id != '' OR period_key = ''" in up_sql
    # Twenty USD twins on the suggestion row and on each of main and history.
    assert up_sql.count("_amount_usd Nullable(Decimal(38, 6))") == 60
    # No reader moves in slice 1: no serving view is touched.
    assert "SYSTEM STOP VIEW" not in up_sql and "MODIFY QUERY" not in up_sql
```

And add `"000401_corpscout_se_company_financial_entity",` as the last entry of `EXPECTED_MIGRATIONS`, after `"000400_corpscout_se_ratsit_financial_periods_usd",`.

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run --env-file .env pytest tests/test_clickhouse_migrations.py -q -k "financial_entity_migration or migration_files_are_explicit"
```

Expected: FAIL with `FileNotFoundError` for the `.up.sql` (and `migration_files_are_explicit` fails because the listed file does not exist).

- [ ] **Step 3: Write the up migration**

`corpscout/clickhouse/migrations/000401_corpscout_se_company_financial_entity.up.sql`, exactly:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- THE SE COMPANY FINANCIAL ENTITY (spec 2026-09-11 section 4, slice 1). Five tables on the
-- basic-info shape keyed by PERIOD: per-source suggestions, the folded main table with its
-- history, and the reviewer's precedence rules and hide rules. No normalized layer: the
-- sources deliver typed numbers, so unit scaling and field mapping happen in the extractors
-- (slice 2). Nothing reads these tables until the fold (slice 3) and the backoffice (slice 4).
--
-- ONE ROW PER COMPANY, ACCOUNTING SCOPE AND PERIOD END. scope is standalone (legal-entity
-- accounts: Bolagsverket, Ratsit's company reports) or consolidated (group accounts: ESEF,
-- Ratsit's consolidated reports); a group figure never merges with a legal-entity figure.
-- period_key is the text '<scope>:<period_end>' and is what suggestions, rules and the
-- backoffice key on; the CHECK pins it to the two columns it is derived from.
--
-- TWENTY MONETARY FIELDS, EACH A PAIR. <field>_amount_original is the figure in the
-- source's currency at FULL units (Ratsit's 57.1 MSEK lands as 57100000 with amount_scale
-- 1000000 saying it was published in millions); <field>_amount_usd is the source's own
-- conversion, copied, never recomputed here. NULL means "no opinion", never zero. The main
-- row carries a _source beside every value and no row-level fx columns: each figure's USD
-- twin is its own winner's conversion, so one rate per row would be wrong the moment two
-- sources share a row.
--
-- A TOMBSTONE is a suggestion row whose twenty figures and employees are all NULL: an
-- extractor writes one per period a source stops delivering. The reviewer sources use the
-- same period keys as the pipeline sources.

-- Per-source suggestions (spec 4.1): one current row per company, source and period.
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial_suggestion
(
    company_id String,
    source LowCardinality(String),
    period_key String,
    suggestion_id FixedString(64),
    suggested_at DateTime64(3, 'UTC'),
    source_record_uid String,
    scope LowCardinality(String),
    period_end Date32,
    period_end_derived UInt8 DEFAULT 0,
    period_start Nullable(Date32),
    fiscal_year Nullable(UInt16),
    period_months Nullable(UInt16),
    filing_fiscal_year Nullable(UInt16),
    currency LowCardinality(Nullable(String)),
    amount_scale UInt32 DEFAULT 1,
    revenue_amount_original Nullable(Decimal(38, 6)),
    revenue_amount_usd Nullable(Decimal(38, 6)),
    operating_costs_amount_original Nullable(Decimal(38, 6)),
    operating_costs_amount_usd Nullable(Decimal(38, 6)),
    operating_result_amount_original Nullable(Decimal(38, 6)),
    operating_result_amount_usd Nullable(Decimal(38, 6)),
    result_after_financial_items_amount_original Nullable(Decimal(38, 6)),
    result_after_financial_items_amount_usd Nullable(Decimal(38, 6)),
    net_result_amount_original Nullable(Decimal(38, 6)),
    net_result_amount_usd Nullable(Decimal(38, 6)),
    ebitda_amount_original Nullable(Decimal(38, 6)),
    ebitda_amount_usd Nullable(Decimal(38, 6)),
    total_assets_amount_original Nullable(Decimal(38, 6)),
    total_assets_amount_usd Nullable(Decimal(38, 6)),
    fixed_assets_amount_original Nullable(Decimal(38, 6)),
    fixed_assets_amount_usd Nullable(Decimal(38, 6)),
    current_assets_amount_original Nullable(Decimal(38, 6)),
    current_assets_amount_usd Nullable(Decimal(38, 6)),
    cash_and_bank_amount_original Nullable(Decimal(38, 6)),
    cash_and_bank_amount_usd Nullable(Decimal(38, 6)),
    equity_amount_original Nullable(Decimal(38, 6)),
    equity_amount_usd Nullable(Decimal(38, 6)),
    share_capital_amount_original Nullable(Decimal(38, 6)),
    share_capital_amount_usd Nullable(Decimal(38, 6)),
    untaxed_reserves_amount_original Nullable(Decimal(38, 6)),
    untaxed_reserves_amount_usd Nullable(Decimal(38, 6)),
    provisions_amount_original Nullable(Decimal(38, 6)),
    provisions_amount_usd Nullable(Decimal(38, 6)),
    liabilities_amount_original Nullable(Decimal(38, 6)),
    liabilities_amount_usd Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_original Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_usd Nullable(Decimal(38, 6)),
    current_liabilities_amount_original Nullable(Decimal(38, 6)),
    current_liabilities_amount_usd Nullable(Decimal(38, 6)),
    personnel_expenses_amount_original Nullable(Decimal(38, 6)),
    personnel_expenses_amount_usd Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_original Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_usd Nullable(Decimal(38, 6)),
    dividend_amount_original Nullable(Decimal(38, 6)),
    dividend_amount_usd Nullable(Decimal(38, 6)),
    employees Nullable(UInt64),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date32),
    fx_source LowCardinality(String) DEFAULT '',
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    source_run_id String,
    extractor_version LowCardinality(String),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_scope CHECK scope IN ('standalone', 'consolidated'),
    CONSTRAINT valid_period_key CHECK period_key = concat(scope, ':', toString(period_end)),
    CONSTRAINT valid_amount_scale CHECK amount_scale IN (1, 1000, 1000000)
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source, period_key);

-- Published periods (spec 4.2): one row per company, scope and period end, written by the
-- fold. currency is decided first and gates which rows may supply money (spec 6); a
-- _source column is '' when the field has no value.
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial
(
    company_id String,
    scope LowCardinality(String),
    period_end Date32,
    period_key String,
    period_start Nullable(Date32),
    period_start_source LowCardinality(String),
    fiscal_year Nullable(UInt16),
    fiscal_year_source LowCardinality(String),
    period_months Nullable(UInt16),
    period_months_source LowCardinality(String),
    currency LowCardinality(String),
    currency_source LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 6)),
    revenue_amount_usd Nullable(Decimal(38, 6)),
    revenue_source LowCardinality(String),
    operating_costs_amount_original Nullable(Decimal(38, 6)),
    operating_costs_amount_usd Nullable(Decimal(38, 6)),
    operating_costs_source LowCardinality(String),
    operating_result_amount_original Nullable(Decimal(38, 6)),
    operating_result_amount_usd Nullable(Decimal(38, 6)),
    operating_result_source LowCardinality(String),
    result_after_financial_items_amount_original Nullable(Decimal(38, 6)),
    result_after_financial_items_amount_usd Nullable(Decimal(38, 6)),
    result_after_financial_items_source LowCardinality(String),
    net_result_amount_original Nullable(Decimal(38, 6)),
    net_result_amount_usd Nullable(Decimal(38, 6)),
    net_result_source LowCardinality(String),
    ebitda_amount_original Nullable(Decimal(38, 6)),
    ebitda_amount_usd Nullable(Decimal(38, 6)),
    ebitda_source LowCardinality(String),
    total_assets_amount_original Nullable(Decimal(38, 6)),
    total_assets_amount_usd Nullable(Decimal(38, 6)),
    total_assets_source LowCardinality(String),
    fixed_assets_amount_original Nullable(Decimal(38, 6)),
    fixed_assets_amount_usd Nullable(Decimal(38, 6)),
    fixed_assets_source LowCardinality(String),
    current_assets_amount_original Nullable(Decimal(38, 6)),
    current_assets_amount_usd Nullable(Decimal(38, 6)),
    current_assets_source LowCardinality(String),
    cash_and_bank_amount_original Nullable(Decimal(38, 6)),
    cash_and_bank_amount_usd Nullable(Decimal(38, 6)),
    cash_and_bank_source LowCardinality(String),
    equity_amount_original Nullable(Decimal(38, 6)),
    equity_amount_usd Nullable(Decimal(38, 6)),
    equity_source LowCardinality(String),
    share_capital_amount_original Nullable(Decimal(38, 6)),
    share_capital_amount_usd Nullable(Decimal(38, 6)),
    share_capital_source LowCardinality(String),
    untaxed_reserves_amount_original Nullable(Decimal(38, 6)),
    untaxed_reserves_amount_usd Nullable(Decimal(38, 6)),
    untaxed_reserves_source LowCardinality(String),
    provisions_amount_original Nullable(Decimal(38, 6)),
    provisions_amount_usd Nullable(Decimal(38, 6)),
    provisions_source LowCardinality(String),
    liabilities_amount_original Nullable(Decimal(38, 6)),
    liabilities_amount_usd Nullable(Decimal(38, 6)),
    liabilities_source LowCardinality(String),
    long_term_liabilities_amount_original Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_usd Nullable(Decimal(38, 6)),
    long_term_liabilities_source LowCardinality(String),
    current_liabilities_amount_original Nullable(Decimal(38, 6)),
    current_liabilities_amount_usd Nullable(Decimal(38, 6)),
    current_liabilities_source LowCardinality(String),
    personnel_expenses_amount_original Nullable(Decimal(38, 6)),
    personnel_expenses_amount_usd Nullable(Decimal(38, 6)),
    personnel_expenses_source LowCardinality(String),
    wages_and_salaries_amount_original Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_usd Nullable(Decimal(38, 6)),
    wages_and_salaries_source LowCardinality(String),
    dividend_amount_original Nullable(Decimal(38, 6)),
    dividend_amount_usd Nullable(Decimal(38, 6)),
    dividend_source LowCardinality(String),
    employees Nullable(UInt64),
    employees_source LowCardinality(String),
    sources Array(LowCardinality(String)),
    active UInt8,
    inactive_reason LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_scope CHECK scope IN ('standalone', 'consolidated'),
    CONSTRAINT valid_period_key CHECK period_key = concat(scope, ':', toString(period_end))
)
ENGINE = ReplacingMergeTree(folded_at)
ORDER BY (company_id, scope, period_end);

-- Period history (spec 4.3): every main column plus the change block, appended by the fold
-- only when a period's values, sources or activity changed, the first publish included.
-- Append-only, no constraints (the main table validated the row).
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial_history
(
    company_id String,
    scope LowCardinality(String),
    period_end Date32,
    period_key String,
    period_start Nullable(Date32),
    period_start_source LowCardinality(String),
    fiscal_year Nullable(UInt16),
    fiscal_year_source LowCardinality(String),
    period_months Nullable(UInt16),
    period_months_source LowCardinality(String),
    currency LowCardinality(String),
    currency_source LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 6)),
    revenue_amount_usd Nullable(Decimal(38, 6)),
    revenue_source LowCardinality(String),
    operating_costs_amount_original Nullable(Decimal(38, 6)),
    operating_costs_amount_usd Nullable(Decimal(38, 6)),
    operating_costs_source LowCardinality(String),
    operating_result_amount_original Nullable(Decimal(38, 6)),
    operating_result_amount_usd Nullable(Decimal(38, 6)),
    operating_result_source LowCardinality(String),
    result_after_financial_items_amount_original Nullable(Decimal(38, 6)),
    result_after_financial_items_amount_usd Nullable(Decimal(38, 6)),
    result_after_financial_items_source LowCardinality(String),
    net_result_amount_original Nullable(Decimal(38, 6)),
    net_result_amount_usd Nullable(Decimal(38, 6)),
    net_result_source LowCardinality(String),
    ebitda_amount_original Nullable(Decimal(38, 6)),
    ebitda_amount_usd Nullable(Decimal(38, 6)),
    ebitda_source LowCardinality(String),
    total_assets_amount_original Nullable(Decimal(38, 6)),
    total_assets_amount_usd Nullable(Decimal(38, 6)),
    total_assets_source LowCardinality(String),
    fixed_assets_amount_original Nullable(Decimal(38, 6)),
    fixed_assets_amount_usd Nullable(Decimal(38, 6)),
    fixed_assets_source LowCardinality(String),
    current_assets_amount_original Nullable(Decimal(38, 6)),
    current_assets_amount_usd Nullable(Decimal(38, 6)),
    current_assets_source LowCardinality(String),
    cash_and_bank_amount_original Nullable(Decimal(38, 6)),
    cash_and_bank_amount_usd Nullable(Decimal(38, 6)),
    cash_and_bank_source LowCardinality(String),
    equity_amount_original Nullable(Decimal(38, 6)),
    equity_amount_usd Nullable(Decimal(38, 6)),
    equity_source LowCardinality(String),
    share_capital_amount_original Nullable(Decimal(38, 6)),
    share_capital_amount_usd Nullable(Decimal(38, 6)),
    share_capital_source LowCardinality(String),
    untaxed_reserves_amount_original Nullable(Decimal(38, 6)),
    untaxed_reserves_amount_usd Nullable(Decimal(38, 6)),
    untaxed_reserves_source LowCardinality(String),
    provisions_amount_original Nullable(Decimal(38, 6)),
    provisions_amount_usd Nullable(Decimal(38, 6)),
    provisions_source LowCardinality(String),
    liabilities_amount_original Nullable(Decimal(38, 6)),
    liabilities_amount_usd Nullable(Decimal(38, 6)),
    liabilities_source LowCardinality(String),
    long_term_liabilities_amount_original Nullable(Decimal(38, 6)),
    long_term_liabilities_amount_usd Nullable(Decimal(38, 6)),
    long_term_liabilities_source LowCardinality(String),
    current_liabilities_amount_original Nullable(Decimal(38, 6)),
    current_liabilities_amount_usd Nullable(Decimal(38, 6)),
    current_liabilities_source LowCardinality(String),
    personnel_expenses_amount_original Nullable(Decimal(38, 6)),
    personnel_expenses_amount_usd Nullable(Decimal(38, 6)),
    personnel_expenses_source LowCardinality(String),
    wages_and_salaries_amount_original Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_usd Nullable(Decimal(38, 6)),
    wages_and_salaries_source LowCardinality(String),
    dividend_amount_original Nullable(Decimal(38, 6)),
    dividend_amount_usd Nullable(Decimal(38, 6)),
    dividend_source LowCardinality(String),
    employees Nullable(UInt64),
    employees_source LowCardinality(String),
    sources Array(LowCardinality(String)),
    active UInt8,
    inactive_reason LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    changed_fields Array(String),
    changed_at DateTime64(3, 'UTC'),
    change_kind LowCardinality(String),
    fold_run_id String
)
ENGINE = MergeTree
ORDER BY (company_id, scope, period_end, changed_at);

-- Precedence (spec 4.4): the basic-info shape with a period scope. company_id '' rows are
-- the global order exported from precedence.py; a company row is a reviewer decision for
-- one period (period_key = the suggestion key) or for every period of the company
-- (period_key ''). The export never touches a company row; a release is a new version with
-- removed = 1, never a delete.
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial_precedence
(
    company_id String,
    period_key String,
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_global_scope CHECK company_id != '' OR period_key = ''
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, period_key, field, source);

-- Reviewer rules (spec 4.5): the address shape, one action -- hide a period. removed = 1
-- releases it (the tab's Unhide); a rule is never edited in place.
CREATE TABLE IF NOT EXISTS corpscout.se_company_financial_rule
(
    company_id String,
    period_key String,
    action LowCardinality(String),
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_action CHECK action IN ('hide')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, period_key, action);
```

- [ ] **Step 4: Write the down migration**

`corpscout/clickhouse/migrations/000401_corpscout_se_company_financial_entity.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.se_company_financial_rule;
DROP TABLE IF EXISTS corpscout.se_company_financial_precedence;
DROP TABLE IF EXISTS corpscout.se_company_financial_history;
DROP TABLE IF EXISTS corpscout.se_company_financial;
DROP TABLE IF EXISTS corpscout.se_company_financial_suggestion;
```

- [ ] **Step 5: Run the migration tests**

```bash
uv run --env-file .env pytest tests/test_clickhouse_migrations.py -q
```

Expected: PASS (the shape test, `test_clickhouse_migration_files_are_explicit`, `test_clickhouse_migrations_create_databases_and_tables`, `test_clickhouse_migrations_have_down_files`, `test_clickhouse_migration_line_comments_do_not_contain_semicolons`).

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/clickhouse/migrations/000401_corpscout_se_company_financial_entity.up.sql \
        corpscout/clickhouse/migrations/000401_corpscout_se_company_financial_entity.down.sql \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(se-financial): migration 000401 creates the five financial entity tables"
```

---

### Task 2: The package and `tables.py`, pinned to the DDL

**Files:**
- Create: `src/dagster_v3/defs/se_company/financial/__init__.py`
- Create: `src/dagster_v3/defs/se_company/financial/tables.py`
- Test: `tests/test_se_company_financial_tables.py`

**Interfaces:**
- Consumes: the migration of Task 1 (through `tests/se_company_ddl.py::table_block` and `declared_columns`, which read the migration files).
- Produces (used by Tasks 3 and 4 and every later slice): `DATABASE`, the five `*_TABLE` names and `QUALIFIED_*` twins, `SCRATCH_SCOPE_PREFIX`, `SOURCES`, `SCOPES`, `AMOUNT_SCALES`, `RULE_ACTIONS`, `INACTIVE_REASONS`, `CHANGE_KINDS`, `MONETARY_FIELDS` (20), `PERIOD_FIELDS` (3), `FOLDED_FIELDS` (25), `original_column(field)`, `usd_column(field)`, `source_column(field)`, `period_key(scope, period_end: date) -> str`, `MONETARY_SUGGESTION_COLUMNS`, `MONETARY_MAIN_COLUMNS`, `SUGGESTION_COLUMNS` (63), `SUGGESTION_VALUE_COLUMNS` (41), `MAIN_COLUMNS` (80), `HISTORY_COLUMNS` (84), `PRECEDENCE_COLUMNS` (9), `RULE_COLUMNS` (7).

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_financial_tables.py`:

```python
"""The five financial-entity tables (spec 2026-09-11 section 4), pinned against the migration
DDL through tests/se_company_ddl.py so tables.py and the deployed schema cannot drift."""

from datetime import date

from dagster_v3.defs.se_company.financial import tables
from tests.se_company_ddl import declared_columns, table_block

COMPANY_ID_CHECK = "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')"
SCOPE_CHECK = "CONSTRAINT valid_scope CHECK scope IN ('standalone', 'consolidated')"
PERIOD_KEY_CHECK = "CONSTRAINT valid_period_key CHECK period_key = concat(scope, ':', toString(period_end))"


def test_the_twenty_monetary_fields_and_the_folded_order() -> None:
    assert len(tables.MONETARY_FIELDS) == 20
    assert tables.MONETARY_FIELDS[0] == "revenue" and tables.MONETARY_FIELDS[-1] == "dividend"
    assert len(set(tables.MONETARY_FIELDS)) == 20
    assert tables.PERIOD_FIELDS == ("period_start", "fiscal_year", "period_months")
    assert tables.FOLDED_FIELDS == (
        *tables.PERIOD_FIELDS, "currency", *tables.MONETARY_FIELDS, "employees",
    )
    assert len(tables.FOLDED_FIELDS) == 25
    assert tables.original_column("revenue") == "revenue_amount_original"
    assert tables.usd_column("revenue") == "revenue_amount_usd"
    assert tables.source_column("employees") == "employees_source"
    assert tables.period_key("standalone", date(2023, 12, 31)) == "standalone:2023-12-31"
    assert tables.SOURCES == (
        "bolagsverket", "bolagsverket_comparative", "esef", "ratsit", "reviewer", "reviewer_draft",
    )
    assert tables.SCOPES == ("standalone", "consolidated")
    assert tables.AMOUNT_SCALES == (1, 1000, 1000000)
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_financial"
    assert tables.SCRATCH_SCOPE_PREFIX == "corpscout._tmp_financial_scope_"


def test_suggestion_table_is_one_current_row_per_company_source_and_period() -> None:
    block = table_block(tables.SUGGESTION_TABLE)
    assert declared_columns(tables.SUGGESTION_TABLE) == list(tables.SUGGESTION_COLUMNS)
    assert len(tables.SUGGESTION_COLUMNS) == 63
    assert "ENGINE = ReplacingMergeTree(suggested_at)" in block
    assert "ORDER BY (company_id, source, period_key)" in block
    for check in (COMPANY_ID_CHECK, SCOPE_CHECK, PERIOD_KEY_CHECK):
        assert check in block, check
    assert "CONSTRAINT valid_amount_scale CHECK amount_scale IN (1, 1000, 1000000)" in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    period_end Date32," in block
    assert "    period_end_derived UInt8 DEFAULT 0," in block
    assert "    filing_fiscal_year Nullable(UInt16)," in block
    assert "    currency LowCardinality(Nullable(String))," in block
    assert "    amount_scale UInt32 DEFAULT 1," in block
    assert "    employees Nullable(UInt64)," in block
    assert "    fx_rate_to_usd Nullable(Decimal(38, 12))," in block
    assert "    fx_source LowCardinality(String) DEFAULT ''," in block
    for field in tables.MONETARY_FIELDS:
        assert f"    {tables.original_column(field)} Nullable(Decimal(38, 6))," in block, field
        assert f"    {tables.usd_column(field)} Nullable(Decimal(38, 6))," in block, field
    assert tables.SUGGESTION_VALUE_COLUMNS == (*tables.MONETARY_SUGGESTION_COLUMNS, "employees")
    assert len(tables.SUGGESTION_VALUE_COLUMNS) == 41
    assert "MATERIALIZED" not in block


def test_main_table_is_one_row_per_company_scope_and_period_end() -> None:
    block = table_block(tables.MAIN_TABLE)
    assert declared_columns(tables.MAIN_TABLE) == list(tables.MAIN_COLUMNS)
    assert len(tables.MAIN_COLUMNS) == 80
    assert "ENGINE = ReplacingMergeTree(folded_at)" in block
    assert "ORDER BY (company_id, scope, period_end)" in block
    for check in (COMPANY_ID_CHECK, SCOPE_CHECK, PERIOD_KEY_CHECK):
        assert check in block, check
    # The row's currency is decided first and is never NULL ('' when no source names one).
    assert "    currency LowCardinality(String)," in block
    for field in tables.FOLDED_FIELDS:
        assert tables.source_column(field) in tables.MAIN_COLUMNS, field
        assert f"    {tables.source_column(field)} LowCardinality(String)," in block, field
    assert "    sources Array(LowCardinality(String))," in block
    assert "    active UInt8," in block
    assert "    inactive_reason LowCardinality(String)," in block
    # No row-level fx columns: each figure's USD twin is its own winner's conversion.
    assert "fx_rate_to_usd" not in block and "amount_scale" not in block


def test_history_is_the_main_row_plus_the_change_block() -> None:
    block = table_block(tables.HISTORY_TABLE)
    assert declared_columns(tables.HISTORY_TABLE) == list(tables.HISTORY_COLUMNS)
    assert tables.HISTORY_COLUMNS == (
        *tables.MAIN_COLUMNS, "changed_fields", "changed_at", "change_kind", "fold_run_id",
    )
    # Append-only, written only by the fold from rows the main table already validated.
    assert "CONSTRAINT" not in block
    assert "ENGINE = MergeTree" in block
    assert "ORDER BY (company_id, scope, period_end, changed_at)" in block
    assert "    changed_fields Array(String)," in block
    assert "    change_kind LowCardinality(String)," in block


def test_precedence_table_is_global_or_company_scoped_with_a_period() -> None:
    block = table_block(tables.PRECEDENCE_TABLE)
    assert declared_columns(tables.PRECEDENCE_TABLE) == list(tables.PRECEDENCE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, period_key, field, source)" in block
    assert (
        "CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')"
        in block
    )
    # A global row (company_id '') never carries a period: only a company decision may.
    assert "CONSTRAINT valid_global_scope CHECK company_id != '' OR period_key = ''" in block
    assert "    removed UInt8 DEFAULT 0," in block
    assert "    decided_by LowCardinality(String) DEFAULT ''," in block


def test_rule_table_hides_a_period() -> None:
    block = table_block(tables.RULE_TABLE)
    assert declared_columns(tables.RULE_TABLE) == list(tables.RULE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, period_key, action)" in block
    assert COMPANY_ID_CHECK in block
    assert "CONSTRAINT valid_action CHECK action IN ('hide')" in block
    assert tables.RULE_ACTIONS == ("hide",)
    assert tables.INACTIVE_REASONS == ("", "hidden", "withdrawn")
    assert tables.CHANGE_KINDS == ("created", "updated", "hidden", "withdrawn", "reactivated")
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_tables.py -q
```

Expected: FAIL at import, `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.financial'`.

- [ ] **Step 3: Write the package**

`src/dagster_v3/defs/se_company/financial/__init__.py`:

```python
"""The SE company financial entity on the basic-info shape.

Spec: docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md. Per-source
suggestions keyed by company, accounting scope and period end are folded per company into
published periods with a source beside every value and a history, ranked by a Ratsit-first
precedence with per-period reviewer rules, and reviewed in the backoffice. Sweden only.
"""
```

`src/dagster_v3/defs/se_company/financial/tables.py`, exactly:

```python
"""Table names and column tuples of the financial entity, pinned against migration 000401
through tests/test_se_company_financial_tables.py so this module and the deployed schema
cannot drift. Fourth entity on the basic-info shape (spec 2026-09-11 section 4): keyed by
company, accounting scope and period end; no normalized layer."""

from datetime import date

DATABASE = "corpscout"
SUGGESTION_TABLE = "se_company_financial_suggestion"
MAIN_TABLE = "se_company_financial"
HISTORY_TABLE = "se_company_financial_history"
PRECEDENCE_TABLE = "se_company_financial_precedence"
RULE_TABLE = "se_company_financial_rule"

QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"
QUALIFIED_MAIN_TABLE = f"{DATABASE}.{MAIN_TABLE}"
QUALIFIED_HISTORY_TABLE = f"{DATABASE}.{HISTORY_TABLE}"
QUALIFIED_PRECEDENCE_TABLE = f"{DATABASE}.{PRECEDENCE_TABLE}"
QUALIFIED_RULE_TABLE = f"{DATABASE}.{RULE_TABLE}"

# This entity's own scratch-table prefix (basic_info/extract.py:scope_pages), so a financial
# scan's scratch table can never collide with another entity's.
SCRATCH_SCOPE_PREFIX = "corpscout._tmp_financial_scope_"

SOURCES: tuple[str, ...] = (
    "bolagsverket", "bolagsverket_comparative", "esef", "ratsit", "reviewer", "reviewer_draft",
)
SCOPES: tuple[str, ...] = ("standalone", "consolidated")
AMOUNT_SCALES: tuple[int, ...] = (1, 1000, 1000000)
RULE_ACTIONS: tuple[str, ...] = ("hide",)
INACTIVE_REASONS: tuple[str, ...] = ("", "hidden", "withdrawn")
CHANGE_KINDS: tuple[str, ...] = ("created", "updated", "hidden", "withdrawn", "reactivated")

# The twenty monetary fields (spec section 4), in DDL order. Each is a pair of columns on
# the suggestion row and a triple (original, usd, source) on the main row.
MONETARY_FIELDS: tuple[str, ...] = (
    "revenue",
    "operating_costs",
    "operating_result",
    "result_after_financial_items",
    "net_result",
    "ebitda",
    "total_assets",
    "fixed_assets",
    "current_assets",
    "cash_and_bank",
    "equity",
    "share_capital",
    "untaxed_reserves",
    "provisions",
    "liabilities",
    "long_term_liabilities",
    "current_liabilities",
    "personnel_expenses",
    "wages_and_salaries",
    "dividend",
)
# The period attributes the fold decides with the same precedence machinery as the money.
PERIOD_FIELDS: tuple[str, ...] = ("period_start", "fiscal_year", "period_months")
# Every field with a precedence map, in the order the main row lays them out: the period
# attributes, the currency (decided first, spec 6), the money, the employees.
FOLDED_FIELDS: tuple[str, ...] = (*PERIOD_FIELDS, "currency", *MONETARY_FIELDS, "employees")


def original_column(field: str) -> str:
    """The native-currency column of a monetary field."""
    return f"{field}_amount_original"


def usd_column(field: str) -> str:
    """The USD twin of a monetary field."""
    return f"{field}_amount_usd"


def source_column(field: str) -> str:
    """The main row's `_source` column of any folded field."""
    return f"{field}_source"


def period_key(scope: str, period_end: date) -> str:
    """The suggestion key of a period: '<scope>:<period_end>' (the DDL's CHECK)."""
    return f"{scope}:{period_end.isoformat()}"


MONETARY_SUGGESTION_COLUMNS: tuple[str, ...] = tuple(
    column for field in MONETARY_FIELDS for column in (original_column(field), usd_column(field))
)
MONETARY_MAIN_COLUMNS: tuple[str, ...] = tuple(
    column
    for field in MONETARY_FIELDS
    for column in (original_column(field), usd_column(field), source_column(field))
)

# What an extractor (or the backoffice) inserts: every column of the table, in DDL order.
SUGGESTION_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "period_key", "suggestion_id", "suggested_at", "source_record_uid",
    "scope", "period_end", "period_end_derived", "period_start", "fiscal_year", "period_months",
    "filing_fiscal_year", "currency", "amount_scale",
    *MONETARY_SUGGESTION_COLUMNS,
    "employees", "fx_rate_to_usd", "fx_rate_date", "fx_source",
    "decided_by", "note", "source_run_id", "extractor_version",
)
# The value columns a live suggestion row may carry; a tombstone has every one NULL.
SUGGESTION_VALUE_COLUMNS: tuple[str, ...] = (*MONETARY_SUGGESTION_COLUMNS, "employees")

# The main row, in DDL order: each folded field followed by its _source (the money as
# original, usd, source).
MAIN_COLUMNS: tuple[str, ...] = (
    "company_id", "scope", "period_end", "period_key",
    "period_start", "period_start_source",
    "fiscal_year", "fiscal_year_source",
    "period_months", "period_months_source",
    "currency", "currency_source",
    *MONETARY_MAIN_COLUMNS,
    "employees", "employees_source",
    "sources", "active", "inactive_reason", "folded_at", "fold_version", "source_run_id",
)
HISTORY_COLUMNS: tuple[str, ...] = (
    *MAIN_COLUMNS, "changed_fields", "changed_at", "change_kind", "fold_run_id",
)
PRECEDENCE_COLUMNS: tuple[str, ...] = (
    "company_id", "period_key", "field", "source", "precedence", "removed", "decided_by",
    "note", "decided_at",
)
RULE_COLUMNS: tuple[str, ...] = (
    "company_id", "period_key", "action", "removed", "decided_by", "note", "decided_at",
)
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_tables.py -q
uv run ruff check src/dagster_v3/defs/se_company/financial tests/test_se_company_financial_tables.py
```

Expected: 6 passed; ruff clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/__init__.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/tables.py \
        corpscout/services/dagster_v3/tests/test_se_company_financial_tables.py
git commit -m "feat(se-financial): the financial entity package and its table catalogue, pinned to 000401"
```

---

### Task 3: `precedence.py`

**Files:**
- Create: `src/dagster_v3/defs/se_company/financial/precedence.py`
- Test: `tests/test_se_company_financial_precedence.py`

**Interfaces:**
- Consumes: `tables.FOLDED_FIELDS`, `tables.SOURCES` (Task 2).
- Produces: `RULE_PRECEDENCE = 10000`, `REVIEWER_PRECEDENCE = 20000`, `FINANCIAL_PRECEDENCE: dict[str, dict[str, int]]`, `precedence_for(field, source) -> int | None`, `precedence_rows() -> list[tuple[str, str, int]]` (82 rows, fields in fold order, highest first, ties by source name).

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_financial_precedence.py`:

```python
"""Spec section 5: Ratsit first, the registers at 900 (they never share a scope), the
restated column last; the reviewer on top of everything including company rules."""

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.precedence import (
    FINANCIAL_PRECEDENCE,
    REVIEWER_PRECEDENCE,
    RULE_PRECEDENCE,
    precedence_for,
    precedence_rows,
)

FULL = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "esef": 900, "bolagsverket_comparative": 800}
THREE = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "esef": 900}


def test_every_folded_field_has_a_map_and_the_reviewer_tops_each() -> None:
    assert set(FINANCIAL_PRECEDENCE) == set(tables.FOLDED_FIELDS)
    for field, by_source in FINANCIAL_PRECEDENCE.items():
        assert by_source["reviewer"] == REVIEWER_PRECEDENCE == 20000, field
        assert max(by_source.values()) == 20000, field
        assert set(by_source) <= set(tables.SOURCES), field
        assert "reviewer_draft" not in by_source, field
        assert precedence_for(field, "reviewer_draft") is None, field
    assert REVIEWER_PRECEDENCE > RULE_PRECEDENCE == 10000


def test_ratsit_leads_every_field_it_supplies() -> None:
    for field, by_source in FINANCIAL_PRECEDENCE.items():
        if "ratsit" in by_source:
            assert by_source["ratsit"] == 1000, field
            for source, precedence in by_source.items():
                if source not in ("reviewer", "ratsit"):
                    assert precedence < 1000, (field, source)


def test_bolagsverket_and_esef_tie_because_they_never_share_a_scope() -> None:
    both = [f for f, m in FINANCIAL_PRECEDENCE.items() if "bolagsverket" in m and "esef" in m]
    assert len(both) == 13
    for field in both:
        assert FINANCIAL_PRECEDENCE[field]["bolagsverket"] == FINANCIAL_PRECEDENCE[field]["esef"] == 900


def test_the_numbers_of_the_spec() -> None:
    for field in ("period_start", "fiscal_year", "period_months", "currency", "revenue", "total_assets"):
        assert FINANCIAL_PRECEDENCE[field] == FULL, field
    for field in ("operating_result", "net_result", "equity", "liabilities", "employees"):
        assert FINANCIAL_PRECEDENCE[field] == THREE, field
    for field in ("cash_and_bank", "personnel_expenses"):
        assert FINANCIAL_PRECEDENCE[field] == {"reviewer": 20000, "bolagsverket": 900, "esef": 900}, field
    for field in ("current_assets", "current_liabilities"):
        assert FINANCIAL_PRECEDENCE[field] == {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900}, field
    assert FINANCIAL_PRECEDENCE["wages_and_salaries"] == {"reviewer": 20000, "bolagsverket": 900}
    for field in (
        "operating_costs", "result_after_financial_items", "ebitda", "fixed_assets",
        "share_capital", "untaxed_reserves", "provisions", "long_term_liabilities", "dividend",
    ):
        assert FINANCIAL_PRECEDENCE[field] == {"reviewer": 20000, "ratsit": 1000}, field


def test_the_comparative_source_appears_only_where_bolagsverket_restates() -> None:
    restated = {f for f, m in FINANCIAL_PRECEDENCE.items() if "bolagsverket_comparative" in m}
    assert restated == {"period_start", "fiscal_year", "period_months", "currency", "revenue", "total_assets"}
    assert precedence_for("revenue", "bolagsverket_comparative") == 800
    assert precedence_for("net_result", "bolagsverket_comparative") is None
    assert precedence_for("no_such_field", "ratsit") is None


def test_rows_are_fields_in_fold_order_highest_first_ties_by_source_name() -> None:
    rows = precedence_rows()
    assert len(rows) == 82
    assert rows[:5] == [
        ("period_start", "reviewer", 20000),
        ("period_start", "ratsit", 1000),
        ("period_start", "bolagsverket", 900),
        ("period_start", "esef", 900),
        ("period_start", "bolagsverket_comparative", 800),
    ]
    assert rows[-4:] == [
        ("employees", "reviewer", 20000), ("employees", "ratsit", 1000),
        ("employees", "bolagsverket", 900), ("employees", "esef", 900),
    ]
    assert tuple(dict.fromkeys(field for field, _, _ in rows)) == tables.FOLDED_FIELDS
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_precedence.py -q
```

Expected: FAIL at import, `ModuleNotFoundError: ... financial.precedence`.

- [ ] **Step 3: Write the module**

`src/dagster_v3/defs/se_company/financial/precedence.py`, exactly:

```python
"""Per-field, per-source precedence of the financial fold (spec 2026-09-11 section 5).

One map per field, scope-agnostic: Bolagsverket and ESEF never meet, because Bolagsverket
only ever supplies the standalone scope and ESEF only the consolidated one, so both can sit
at 900. RATSIT FIRST (owner decision 2026-09-11, "I think ratsit information is pretty
accurate"): the measured disagreement with Bolagsverket is rounding to a tenth of a million,
not error, and the figures come from the same filed reports. Precedence only decides when
several sources have an opinion on the same cell; for the 1.09M company-years only Ratsit
covers and the nine figures only Ratsit carries, the order never matters. The comparative
source (a later filing's restated prior-year column) appears only where Bolagsverket
restates a figure, revenue and total assets. The reviewer is a source like the others,
ranked above every automated one and above company rules (RULE_PRECEDENCE); `reviewer_draft`
appears in no map and can never win a fold.

A source absent from a field's map cannot supply that field; a company rule (slice 4) can
rank it in for one company or one period. A change to these numbers is a full re-fold
(`changed_only: false` over all 64 buckets) after the export.
"""

from dagster_v3.defs.se_company.financial import tables

# The number a reviewer's "Use this" rule carries, and the reviewer source's own rank
# (above it, so an activated typed value outranks any preference rule).
RULE_PRECEDENCE = 10000
REVIEWER_PRECEDENCE = 20000

_FULL = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "esef": 900, "bolagsverket_comparative": 800}
_THREE_SOURCES = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "esef": 900}
_REGISTERS_ONLY = {"reviewer": 20000, "bolagsverket": 900, "esef": 900}
_RATSIT_AND_BOLAGSVERKET = {"reviewer": 20000, "ratsit": 1000, "bolagsverket": 900}
_BOLAGSVERKET_ONLY = {"reviewer": 20000, "bolagsverket": 900}
_RATSIT_ONLY = {"reviewer": 20000, "ratsit": 1000}

FINANCIAL_PRECEDENCE: dict[str, dict[str, int]] = {
    # Period attributes and the currency: every source that delivers a period has them.
    "period_start": dict(_FULL),
    "fiscal_year": dict(_FULL),
    "period_months": dict(_FULL),
    "currency": dict(_FULL),
    # The two figures a later Bolagsverket filing restates.
    "revenue": dict(_FULL),
    "total_assets": dict(_FULL),
    # Carried by Ratsit and both registers.
    "operating_result": dict(_THREE_SOURCES),
    "net_result": dict(_THREE_SOURCES),
    "equity": dict(_THREE_SOURCES),
    "liabilities": dict(_THREE_SOURCES),
    "employees": dict(_THREE_SOURCES),
    # Registers only: Ratsit publishes neither.
    "cash_and_bank": dict(_REGISTERS_ONLY),
    "personnel_expenses": dict(_REGISTERS_ONLY),
    # Ratsit and Bolagsverket (ESEF's mapping has no current-asset split).
    "current_assets": dict(_RATSIT_AND_BOLAGSVERKET),
    "current_liabilities": dict(_RATSIT_AND_BOLAGSVERKET),
    # Bolagsverket alone.
    "wages_and_salaries": dict(_BOLAGSVERKET_ONLY),
    # Ratsit alone: the nine lines only its reports carry.
    "operating_costs": dict(_RATSIT_ONLY),
    "result_after_financial_items": dict(_RATSIT_ONLY),
    "ebitda": dict(_RATSIT_ONLY),
    "fixed_assets": dict(_RATSIT_ONLY),
    "share_capital": dict(_RATSIT_ONLY),
    "untaxed_reserves": dict(_RATSIT_ONLY),
    "provisions": dict(_RATSIT_ONLY),
    "long_term_liabilities": dict(_RATSIT_ONLY),
    "dividend": dict(_RATSIT_ONLY),
}

# Not an assert: this runs at import time under load_from_defs_folder, and `python -O`
# would strip it. A raise names the offending tuples in the code location's error.
if set(FINANCIAL_PRECEDENCE) != set(tables.FOLDED_FIELDS):
    raise ValueError(
        f"FINANCIAL_PRECEDENCE keys {sorted(FINANCIAL_PRECEDENCE)} "
        f"must equal FOLDED_FIELDS {sorted(tables.FOLDED_FIELDS)}"
    )


def precedence_for(field: str, source: str) -> int | None:
    """The global precedence of `source` for `field`, or None when it cannot supply it."""
    return FINANCIAL_PRECEDENCE.get(field, {}).get(source)


def precedence_rows() -> list[tuple[str, str, int]]:
    """Every (field, source, precedence) pair, fields in fold order, highest first, ties by
    source name -- the rows the export asset writes as company_id '' / period_key ''."""
    rows: list[tuple[str, str, int]] = []
    for field in tables.FOLDED_FIELDS:
        by_source = FINANCIAL_PRECEDENCE[field]
        for source, precedence in sorted(by_source.items(), key=lambda item: (-item[1], item[0])):
            rows.append((field, source, precedence))
    return rows
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_precedence.py -q
uv run ruff check src/dagster_v3/defs/se_company/financial tests/test_se_company_financial_precedence.py
```

Expected: 6 passed; ruff clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/precedence.py \
        corpscout/services/dagster_v3/tests/test_se_company_financial_precedence.py
git commit -m "feat(se-financial): the Ratsit-first precedence map of the financial fold"
```

---

### Task 4: The precedence export asset

**Files:**
- Create: `src/dagster_v3/defs/se_company/financial/assets.py`
- Test: `tests/test_se_company_financial_assets.py`

**Interfaces:**
- Consumes: `tables.QUALIFIED_PRECEDENCE_TABLE`, `tables.PRECEDENCE_TABLE`, `tables.PRECEDENCE_COLUMNS`, `tables.DATABASE` (Task 2); `precedence_rows()` (Task 3); `assert_clickhouse_tables_exist` from `dagster_v3.defs.clickhouse.resolved`.
- Produces: `GROUP_NAME = "se_company_financial"`, `export_precedence(client, exported_at) -> tuple[int, int]`, asset `se_company_financial_precedence_clickhouse` (unpartitioned, no deps), autoloaded by `dagster_v3.definitions` through `load_from_defs_folder` (no registration step).

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_financial_assets.py`:

```python
"""The precedence export (spec section 8): global rows only, idempotent, and registered
without dependencies."""

from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.assets import GROUP_NAME, export_precedence
from dagster_v3.defs.se_company.financial.precedence import precedence_rows
from dagster_v3.definitions import defs as load_project_defs

_SELECT_STORED_SQL = (
    f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
    "FINAL WHERE company_id = '' AND period_key = '' AND removed = 0"
)
_INSERT_SQL = (
    f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
    f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
)


class FakeClient:
    """Dispatches on the whole statement text against the table's own qualified name."""

    def __init__(self, stored: list[tuple] = (), stale: int = 0) -> None:
        self.calls: list[tuple[str, object]] = []
        self.stored = list(stored)
        self.stale = stale

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params))
        if sql == _SELECT_STORED_SQL:
            return list(self.stored)
        if sql.startswith(f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE}"):
            return []
        if sql.startswith(f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE}"):
            return [(self.stale,)]
        raise AssertionError(f"unexpected statement: {sql!r}")


def test_export_inserts_the_82_global_rows_and_counts_stale_pairs() -> None:
    client = FakeClient(stale=3)
    exported_at = datetime(2026, 9, 12, 8, 0, 0, 123000, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (82, 3)
    select_sql, _ = client.calls[0]
    assert select_sql == _SELECT_STORED_SQL
    insert_sql, rows = client.calls[1]
    assert insert_sql == _INSERT_SQL
    assert rows[0] == ("", "", "period_start", "reviewer", 20000, 0, "code", "", exported_at)
    assert all(row[0] == "" and row[1] == "" for row in rows)
    assert [row[2:5] for row in rows] == precedence_rows()
    count_sql, params = client.calls[2]
    assert "company_id = ''" in count_sql and "period_key = ''" in count_sql
    assert "removed = 0" in count_sql and "FINAL" in count_sql
    assert params == {"exported_at": "2026-09-12 08:00:00.123"}


def test_export_against_a_changed_dictionary_inserts_every_row() -> None:
    stored = list(precedence_rows())
    stored[-1] = (stored[-1][0], stored[-1][1], 999)  # one number differs from the dictionary
    client = FakeClient(stored=stored, stale=1)
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 9, 0, tzinfo=UTC))
    assert (pairs, stale) == (82, 1)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "INSERT", "SELECT"]


def test_export_against_matching_stored_rows_inserts_nothing() -> None:
    client = FakeClient(stored=list(precedence_rows()), stale=0)
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 10, 0, tzinfo=UTC))
    assert (pairs, stale) == (0, 0)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "SELECT"]


def test_the_export_asset_is_registered_without_dependencies() -> None:
    repository = load_project_defs().get_repository_def()
    node = repository.asset_graph.get(dg.AssetKey("se_company_financial_precedence_clickhouse"))
    assert node.parent_keys == set()
    assert node.partitions_def is None
    assert node.group_name == GROUP_NAME == "se_company_financial"
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_assets.py -q
```

Expected: FAIL at import, `ModuleNotFoundError: ... financial.assets`.

- [ ] **Step 3: Write the module**

`src/dagster_v3/defs/se_company/financial/assets.py`, exactly:

```python
"""Dagster assets of the financial entity. Slice 1 ships the precedence export; the
extractors and the extract job (slice 2), the fold (slice 3) follow in their own modules."""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.precedence import precedence_rows

GROUP_NAME = "se_company_financial"


def _precedence_export_timestamp(exported_at: datetime) -> str:
    """``exported_at`` as a UTC ``%Y-%m-%d %H:%M:%S.mmm`` string for ``toDateTime64(..., 3,
    'UTC')``: a bare tz-aware datetime parameter would let the stale-pairs comparison depend
    on the server's default timezone and drop sub-second precision (the three older entities
    do the same)."""
    return exported_at.strftime("%Y-%m-%d %H:%M:%S.") + f"{exported_at.microsecond // 1000:03d}"


def export_precedence(client: Any, exported_at: datetime) -> tuple[int, int]:
    """Insert every (field, source, precedence) pair as a global rule (company_id '',
    period_key '', decided_by 'code') and count global rules exported before this run that
    the dictionary no longer names. Returns (pairs inserted, stale pairs remaining). Never
    touches a company-scoped row.

    `decided_at` is one of the fold's selection watermarks (slice 3), so writing a fresh one
    when nothing changed would re-fold every company on an idle re-materialisation. Read the
    stored global rows first: when they already equal `precedence_rows()`, insert nothing
    and report 0 pairs (the caller reads that as `unchanged`); otherwise insert every pair."""
    wanted = precedence_rows()
    stored = {
        tuple(row)
        for row in client.execute(
            f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
            "FINAL WHERE company_id = '' AND period_key = '' AND removed = 0"
        )
    }
    pairs = 0
    if stored != set(wanted):
        rows = [
            ("", "", field, source, precedence, 0, "code", "", exported_at)
            for field, source, precedence in wanted
        ]
        client.execute(
            f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
            f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
            rows,
        )
        pairs = len(rows)
    stale = int(
        client.execute(
            f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL "
            "WHERE company_id = '' AND period_key = '' AND removed = 0 "
            "AND decided_at < toDateTime64(%(exported_at)s, 3, 'UTC')",
            {"exported_at": _precedence_export_timestamp(exported_at)},
        )[0][0]
    )
    return pairs, stale


@dg.asset(
    name="se_company_financial_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports FINANCIAL_PRECEDENCE to se_company_financial_precedence as global rules "
        "(company_id '', period_key '') for the fold and the backoffice to read. The Python "
        "dictionary is the only source for these rows; re-run after changing it, then "
        "re-fold every bucket with changed_only false (a precedence change is not a "
        "per-company change). Idle re-materialisation is a no-op: when the stored rows "
        "already match the dictionary, nothing is written and the watermark does not move. "
        "Never touches a company-scoped row."
    ),
)
def se_company_financial_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,)
    )
    exported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        pairs, stale = export_precedence(client, exported_at)
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; "
            "they stay until removed by hand",
            stale,
        )
    return dg.MaterializeResult(
        metadata={
            "pairs": pairs, "stale_pairs": stale, "unchanged": pairs == 0,
            "table": tables.QUALIFIED_PRECEDENCE_TABLE,
        }
    )
```

- [ ] **Step 4: Run the tests, the definitions check and ruff**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_assets.py -q
uv run dg check defs
uv run ruff check src/dagster_v3/defs/se_company/financial tests/test_se_company_financial_assets.py
```

Expected: 4 passed (the registration test loads every definition, about a minute); `dg check defs` reports no errors (the known `company_domain_suggestions` adapter traceback is noise); ruff clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/assets.py \
        corpscout/services/dagster_v3/tests/test_se_company_financial_assets.py
git commit -m "feat(se-financial): the precedence export asset, idempotent like the person entity's"
```

---

### Task 5: Design note, spec record, whole-slice check

**Files:**
- Create: `src/dagster_v3/defs/se_company/financial/docs/financial-design.md`
- Modify: `docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` (section 4.1's `suggestion_id` line; section 12 item 1)

- [ ] **Step 1: Write the design note**

`src/dagster_v3/defs/se_company/financial/docs/financial-design.md`:

```markdown
# SE company financial entity — package notes

Spec: `docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` (the binding
document; this note is the package's map and runbook).

## Shape (slice 1)

Five tables, migration 000401, keyed by company, accounting scope (`standalone` |
`consolidated`) and period end; `period_key` is `'<scope>:<period_end>'` and is what
suggestions, rules and the backoffice key on.

| Table | Grain | Written by |
|---|---|---|
| `se_company_financial_suggestion` | one current row per company, source and period | the extractors (slice 2), the backoffice's reviewer rows (slice 4) |
| `se_company_financial` | one row per company, scope and period end | the fold (slice 3) |
| `se_company_financial_history` | one row per change of a published period | the fold |
| `se_company_financial_precedence` | global rows (`company_id ''`, `period_key ''`) and company rules | the export asset; the backoffice |
| `se_company_financial_rule` | hide rules per company and period | the backoffice |

`tables.py` is the single place the column order lives and is pinned to the DDL by
`tests/test_se_company_financial_tables.py`. The twenty monetary fields are pairs
(`<field>_amount_original` in the source's currency at full units, `<field>_amount_usd` the
source's own conversion) with a `<field>_source` on the main row; `amount_scale` on the
suggestion row records the unit the source published in (Ratsit: 1000000).

## Precedence

`precedence.py` holds the spec's section-5 map: Ratsit 1000 first, the registers 900 (they
never share a scope), the restated Bolagsverket column 800, the reviewer 20000 above a
company rule's 10000. A source absent from a field's map cannot supply it.

Runbook: after changing the numbers, materialize `se_company_financial_precedence_clickhouse`
(it writes only when the stored global rows differ from the dictionary, so an idle re-run
moves no watermark) and then re-fold every bucket with `changed_only: false` (slice 3): a
precedence change is not a per-company change and the fold's per-company watermarks will
not notice it on their own.
```

- [ ] **Step 2: Record the refinement and the slice in the spec**

In section 4.1's code block, change the line `suggestion_id       String                  -- lineage id, minted by the extractor from one clock read` to `suggestion_id       FixedString(64)         -- lineage id (sha256 hex), minted by the extractor from one clock read`.

In section 12, item 1, append (wrapped like its neighbours): `Code complete 2026-09-12 on branch se-financial-entity (plan 2026-09-12-se-company-financial-1-tables-precedence.md); prod apply and export pending.`

- [ ] **Step 3: Run the slice's suites and the wider suite once**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_tables.py tests/test_se_company_financial_precedence.py tests/test_se_company_financial_assets.py tests/test_clickhouse_migrations.py -q
set -a; source .env; set +a; uv run pytest tests -q -p no:cacheprovider --ignore=tests/test_schedule_cron_contracts.py --deselect tests/test_backfill_policy_contracts.py::test_every_partitioned_asset_uses_multi_run_backfill_policy --deselect tests/test_duckdb_bulk_loading_contract.py::test_production_has_only_the_explicit_ted_executemany_debt --deselect tests/test_nace_categories.py::test_nace_assets_are_registered_as_staged_flow --deselect tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values --deselect tests/test_technology_aliases_clickhouse.py::test_catalog_asset_publishes_aliases_clears_them_and_rejects_bad_input 2>&1 | tail -3
```

Expected: the first command all green; the second `N passed, 3 skipped, 5 deselected` with no failures (the five deselected are the known baseline and environment failures, recorded on 2026-09-12). Any failure is this slice's to report.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/docs/financial-design.md \
        corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md
git commit -m "docs(se-financial): package notes for the financial entity; spec records slice 1 code complete"
```

---

### Task 6: Prod apply and export (after the final review and the merge)

**Files:** none. Runs against the companycollect and dagster hosts.

**Preconditions:** the branch's whole-branch review is clean and it is merged into main (from the main checkout, `git merge --no-ff se-financial-entity`, the owner's dirty files never overlap the branch's); `git merge --ff-only main` in the worktree so it equals main; `ls corpscout/clickhouse/migrations | tail -1` on main is 000401 and prod's ledger prints `400	0`.

- [ ] **Step 1: Apply migration 000401 (single step, from the worktree)**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity/corpscout
make clickhouse-migrate-version      # expect 400
make clickhouse-migrate-up-one       # applies 000401 only
make clickhouse-migrate-version      # expect 401
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --query "SELECT name, engine FROM system.tables WHERE database = '"'"'corpscout'"'"' AND name LIKE '"'"'se_company_financial%'"'"' ORDER BY name FORMAT PrettyCompactMonoBlock"'
```

Expected: the five tables, `ReplacingMergeTree` for four and `MergeTree` for the history; ledger `401	0`.

- [ ] **Step 2: Deploy dagster_v3 from the worktree (the deploy recipe)**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity/corpscout/services/dagster_v3
test -z "$(git diff main --stat)" && test -f .env && test -z "$(git status --porcelain src)"
uv sync --frozen
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/finland_ytj/dbt --profiles-dir src/dagster_v3/defs/finland_ytj/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/exchange_rates_v2/dbt --profiles-dir src/dagster_v3/defs/exchange_rates_v2/dbt
uv run --frozen --no-sync dg utils refresh-defs-state
uv run --frozen --no-sync dg check defs
cd ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml > /tmp/light_sync_slice1.log 2>&1; echo "RC=$?"; tail -3 /tmp/light_sync_slice1.log
```

Expected: `RC=0`, `failed=0`. Confirm the asset is live: `curl -s http://dagster:3000/graphql -H 'Content-Type: application/json' -d '{"query":"{ assetNodes(assetKeys: [{path: [\"se_company_financial_precedence_clickhouse\"]}]) { assetKey { path } groupName } }"}'` prints the key with group `se_company_financial`.

- [ ] **Step 3: Run the export**

Launch through GraphQL (`launchRun(executionParams: {selector: {repositoryLocationName: "dagster_v3", repositoryName: "__repository__", jobName: "__ASSET_JOB", assetSelection: [{path: ["se_company_financial_precedence_clickhouse"]}]}, runConfigData: {}, executionMetadata: {tags: []}})`). If the run stays QUEUED for more than five minutes (the run queue was starved by thirty ESEF refresh runs on 2026-09-12), cancel it and run in-process on the host instead:

```bash
ssh dagster 'cd /opt/companycollect/corpscout/dagster_v3 && sudo -n bash -c "set -a; source .env; set +a; export HOME=/root DAGSTER_HOME=/opt/companycollect/corpscout/dagster_v3 DAGSTER_DISABLE_TELEMETRY=1 PYTHONDONTWRITEBYTECODE=1 VIRTUAL_ENV=/opt/companycollect/corpscout/dagster_v3/.venv PATH=/root/.local/bin:/opt/companycollect/corpscout/dagster_v3/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin TERM=dumb; nohup .venv/bin/dagster asset materialize -m dagster_v3.definitions -a defs --select se_company_financial_precedence_clickhouse > /tmp/financial_precedence.log 2>&1 & echo started"'
```

Then read the outcome with one-shot greps of the log (`grep -E "RUN_SUCCESS|RUN_FAILURE|Traceback" /tmp/financial_precedence.log`); a `tail -f` watch attached over ssh did not emit events on 2026-09-12.

- [ ] **Step 4: Verify**

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --multiquery --format PrettyCompactMonoBlock' <<'SQL'
SELECT count() AS global_rows, uniqExact(field) AS fields, min(decided_at) AS exported_at FROM corpscout.se_company_financial_precedence FINAL WHERE company_id = '' AND period_key = '' AND removed = 0;
SELECT field, source, precedence FROM corpscout.se_company_financial_precedence FINAL WHERE company_id = '' AND field = 'revenue' ORDER BY precedence DESC;
SELECT count() FROM corpscout.se_company_financial_suggestion;
SQL
```

Expected: 82 global rows over 25 fields; revenue reads reviewer 20000, ratsit 1000, bolagsverket 900, esef 900, bolagsverket_comparative 800; the suggestion table is empty. Materializing the export a second time reports `pairs 0, unchanged true`.

- [ ] **Step 5: Record**

Append to spec section 12 item 1: `Prod 2026-09-12: 000401 applied (ledger 401), deployed, export run <id> wrote 82 global rows.` Commit on main as `docs(se-financial): slice 1 shipped, prod record`, fast-forward the worktree, update the memory file `se-financial-entity.md`, then write slice 2's plan.

---

## Self-review

**Spec coverage.** Section 4.1 to 4.5 → Task 1 (DDL) and Task 2 (`tables.py` pinned to it; the `suggestion_id` type refinement recorded in Task 5). Section 5 → Task 3 (map, `RULE_PRECEDENCE`, rules resolution belongs to slice 3's fold). Section 8's `se_company_financial_precedence_clickhouse` → Task 4; the group name `se_company_financial` → Task 4. Section 11's DDL contract test → Task 2; the migrations registry → Task 1. Section 12 item 1 → Tasks 1 to 6. Section 13 names → the package files. Not in this slice, by the spec: extractors, jobs, the fold, the backoffice.

**Placeholder scan.** None: every file is given in full; the prod steps carry their commands and expected values.

**Type consistency.** `export_precedence(client, exported_at) -> tuple[int, int]` is the name Task 4's asset and tests use; `precedence_rows()` returns `(field, source, precedence)` triples and the export prefixes `("", "")` to match `PRECEDENCE_COLUMNS` (`company_id, period_key, field, source, precedence, removed, decided_by, note, decided_at`); `tables.FOLDED_FIELDS` is the key set `precedence.py` checks at import and the order `precedence_rows()` walks; the 82-row count is 6×5 + 5×4 + 2×3 + 2×3 + 1×2 + 9×2.
