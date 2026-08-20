# Sweden Financial Source Views and Shared Financial UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give each Swedish company a source-first Financials tab where every
available source (currently Bolagsverket and ESEF) can be selected and rendered
through the same financial-overview UI, without merging source values.

**Architecture:** First rename the misleading Bolagsverket-derived table from
`se_financial_metrics` to `se_bolagsverket_financial_metrics` and update every
runtime consumer. Keep source-owned tables and source semantics separate. Then
create one explicit canonical metric map in Python and expose two ClickHouse
serving views with the same column contract:
`se_financials_bolagsverket_current` and `se_financials_esef_current`. The
backoffice loads both independently, lists only sources that have data, and
renders the selected source through one shared component. Adding a third source
later means adding its mapping, its same-shape view, and one source definition;
it does not require a multi-source merge algorithm.

**Tech Stack:** Python 3.12, Dagster, ClickHouse SQL, React Router 8, React 19,
TypeScript, shadcn/ui Tabs, Vitest.

## Implementation status — 2026-08-19

- [x] Renamed the physical Bolagsverket table and every runtime consumer.
- [x] Added the code-owned canonical source mapping and ESEF mapping v2.
- [x] Added and applied the two independent same-shape ClickHouse views.
- [x] Changed the Sweden Financials loader to query each source independently.
- [x] Added the shared source selector, locale-controlled overview, and
  source-specific facts links.
- [x] Updated data-design documentation and verified one-source and two-source
  companies in the local browser.

Local ClickHouse is at migration 286. `esef_financial_metrics_clickhouse` was
materialized after the mapping change (25,060 rows), including Sagax 2024
rental income as revenue under `esef-ifrs-v2`. The focused Dagster suite,
Dagster definition check, backoffice unit/live integration tests, TypeScript
typecheck, and production build pass. Task commits remain intentionally
separate from the pre-existing dirty financial worktree.

---

## Scope decisions

1. **Rename the ambiguous table first.** `se_financial_metrics` becomes
   `se_bolagsverket_financial_metrics`, making its source ownership explicit.
   ESEF remains in `esef_financial_metrics`.
2. **Do not merge values across sources.** A company/year can have one value in
   each source. The selected source decides which dataset the page displays.
3. **Do not create a canonical cross-source winner.** There is no source
   priority, equality comparison, conflict array, or coalescing layer in this
   plan.
4. **Use one canonical display contract.** Both serving views expose identical
   aliases, null-filling metrics a source cannot currently provide.
5. **Preserve accounting scope.** Bolagsverket is `standalone`; ESEF is
   `consolidated_ifrs`. The scope must be visible beside the source name.
6. **Keep complete source facts reachable.** Bolagsverket rows link to the
   existing year facts page. ESEF rows link to the existing filing facts page by
   `fxo_id`.
7. **First mapping increment is conservative.** Add standard IFRS concepts that
   map cleanly to existing metric columns. Do not treat Sagax's issuer extension
   `ProfitFromPropertyManagement` as generic operating profit, and do not parse
   employee counts from HTML note blocks in this work.

The resulting source-owned chains are:

```text
se_financial_facts
  -> se_bolagsverket_financial_observations
  -> se_bolagsverket_financial_metrics
  -> se_financials_bolagsverket_current

esef_facts
  -> esef_financial_metrics
  -> se_financials_esef_current
```

## UI contract

**Visual thesis:** A calm, source-first financial workspace: the source choice
is always obvious, while charts, ratios, tables, language, and SEK/USD formatting
remain stable when the source changes.

**Content plan:** Source selector → selected-source identity and scope → latest
KPIs → trend → ratios → income statement → balance sheet → source facts and
evidence.

**Interaction thesis:**

- Source tabs use a restrained shared-layout transition and expose the
  available year range.
- Switching sources replaces the dataset in place; it does not stack two long
  financial pages.
- Language selection remains unchanged while switching sources.

## Canonical source-view contract

Both ClickHouse views must expose these columns with compatible types:

```text
source_id
accounting_scope
company_id
source_document_id
fiscal_year
report_period_start
report_period_end
currency
revenue_amount_original
revenue_amount_usd
operating_result_amount_original
operating_result_amount_usd
net_result_amount_original
net_result_amount_usd
total_assets_amount_original
total_assets_amount_usd
equity_amount_original
equity_amount_usd
liabilities_amount_original
liabilities_amount_usd
cash_and_bank_amount_original
cash_and_bank_amount_usd
current_assets_amount_original
current_assets_amount_usd
current_liabilities_amount_original
current_liabilities_amount_usd
personnel_expenses_amount_original
personnel_expenses_amount_usd
wages_and_salaries_amount_original
wages_and_salaries_amount_usd
employees
source_fact_count
mapped_fact_count
mapping_version
fx_rate_to_usd
fx_rate_date
fx_source
observation
source_fiscal_year
source_record_uids
source_url
viewer_url
```

Source-specific rules:

- Bolagsverket uses `statement_key` as `source_document_id`, wraps its single
  `source_record_uid` in an array, emits empty `source_url`/`viewer_url` where
  document URLs are assembled elsewhere, and preserves `filed` versus
  `comparative`.
- ESEF uses the selected `primary_fxo_id` as `source_document_id`, preserves the
  current amendment-composition logic and all contributing source-record UIDs,
  and always emits `observation = 'filed'`.
- Metrics absent from the current ESEF schema (`current_assets`,
  `current_liabilities`, personnel expenses, wages and salaries) are typed NULLs
  in the ESEF view. The shared UI already suppresses rows and ratios that cannot
  be calculated.

---

## CRITICAL: repository and git rules

The `companycollect` worktree already contains extensive uncommitted financial
changes, including the untracked migration
`000284_corpscout_se_financial_metrics_unified_years.*`.

- Treat the current worktree as the baseline. Do not revert, rewrite, or format
  unrelated files.
- Before implementation, confirm migration 284 and its related tests are a
  coherent baseline. The rename migration in Task 1 depends on those columns.
- Re-check the highest migration number immediately before Task 1. Use 285 for
  the rename only if it is still free. The source-view migration uses the next
  number (286 if 285 remains the rename).
- Use `git add` only for the files named in the current task. Never use
  `git add .`, `git add -A`, or `git add -u`.
- Use plain `git commit`; never amend a commit that may belong to concurrent
  work.
- Run all commands from
  `/Users/graovic/pulsarpoint/ppoint/companycollect` unless a step says
  otherwise.

---

## Task 1 — Rename the Bolagsverket metrics table and every runtime consumer

This is the mandatory first implementation task. The source-specific name must
be established before adding another mapping or serving view.

**Files:**

- Create:
  `corpscout/clickhouse/migrations/000285_corpscout_se_bolagsverket_financial_metrics_rename.up.sql`
- Create:
  `corpscout/clickhouse/migrations/000285_corpscout_se_bolagsverket_financial_metrics_rename.down.sql`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/metrics.py`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/assets.py`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/__init__.py`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/assets.py`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/sql.py`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/sources.yml`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_section_item_source_links_build.sql`
- Modify: `corpscout/services/backoffice/app/lib/countries.ts`
- Modify: `corpscout/services/backoffice/app/lib/queries.server.ts`
- Create:
  `corpscout/services/dagster_v3/tests/test_sweden_financial_table_rename_migration.py`
- Modify:
  `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`
- Modify:
  `corpscout/services/dagster_v3/tests/test_sweden_financial_metrics.py`
- Modify:
  `corpscout/services/dagster_v3/tests/test_sweden_financial_assets.py`
- Modify:
  `corpscout/services/dagster_v3/tests/test_clickhouse_leaf_checks.py`
- Modify:
  `corpscout/services/dagster_v3/tests/test_company_financials_latest.py`
- Modify: `corpscout/services/backoffice/app/lib/countries.test.ts`
- Modify:
  `corpscout/services/backoffice/app/lib/queries-financials.test.ts`

- [ ] **Step 1: Write a failing rename migration test**

Create `test_sweden_financial_table_rename_migration.py`. Assert that the new up
migration contains exactly the forward rename:

```sql
RENAME TABLE corpscout.se_financial_metrics
TO corpscout.se_bolagsverket_financial_metrics;
```

Assert that the down migration reverses that rename. Also assert that neither
migration copies rows, truncates data, drops the table, or creates a second
physical table.

Add the migration name to `EXPECTED_MIGRATIONS` in
`test_clickhouse_migrations.py`.

- [ ] **Step 2: Run the migration tests and confirm the new test fails**

```bash
cd corpscout/services/dagster_v3
uv run pytest \
  tests/test_sweden_financial_table_rename_migration.py \
  tests/test_clickhouse_migrations.py -q
```

Expected: missing migration files and migration-registry entry.

- [ ] **Step 3: Add the metadata-only ClickHouse rename migration**

The up migration starts with `CREATE DATABASE IF NOT EXISTS corpscout;` and
uses `RENAME TABLE`; it does not rebuild or duplicate the table. The down
migration performs the exact inverse rename.

Do not edit historical migrations 090, 134, 244, or 284. They correctly describe
the table name at the time those migrations ran.

- [ ] **Step 4: Rename the Dagster table, function, and asset symbols**

Use these explicit names:

```text
SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE
QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE
SE_BOLAGSVERKET_FINANCIAL_METRICS_COLUMNS
replace_se_bolagsverket_financial_metrics_clickhouse
se_bolagsverket_financial_metrics_clickhouse
```

Update the asset metadata, checks, schedules, sensor/check attachments,
`__all__`, and test expectations. The asset continues to depend on
`se_bolagsverket_financial_observations_clickhouse`; only its ambiguous name and
physical target change.

Do not rename generic metric names such as `revenue`, `profit_loss`, or
`MONEY_METRIC_NAMES`.

- [ ] **Step 5: Update all downstream Dagster and dbt consumers**

Change the Sweden entry in `company_financials_latest/sql.py`, its asset
dependency, ClickHouse leaf checks, the company-serving dbt source declaration,
and the financial evidence-link model to read
`se_bolagsverket_financial_metrics`.

After the changes, this command must return no runtime references outside
historical migrations and the rename migration itself:

```bash
rg -n "se_financial_metrics" \
  corpscout/services/dagster_v3/src \
  corpscout/services/backoffice/app \
  -g '!**/docs/**'
```

- [ ] **Step 6: Update the current backoffice queries before introducing views**

Change every Sweden runtime query and schema-readiness check in `countries.ts`
and `queries.server.ts` from `se_financial_metrics` to
`se_bolagsverket_financial_metrics`, including:

- the main yearly financial query;
- source-facts statement selection;
- facts-document selection;
- the `financialsByYear` configuration;
- provenance-column readiness checks;
- the pre-migration compatible query.

This keeps the current Financials page operational after the table rename and
before the source-view tasks land.

- [ ] **Step 7: Run the focused rename suites**

```bash
cd corpscout/services/dagster_v3
uv run pytest \
  tests/test_sweden_financial_table_rename_migration.py \
  tests/test_clickhouse_migrations.py \
  tests/test_sweden_financial_metrics.py \
  tests/test_sweden_financial_assets.py \
  tests/test_clickhouse_leaf_checks.py \
  tests/test_company_financials_latest.py -q

cd ../backoffice
npm test -- \
  app/lib/countries.test.ts \
  app/lib/queries-financials.test.ts
npm run typecheck
```

Expected: all pass, and runtime source searches use only the new table name.

- [ ] **Step 8: Validate Dagster definitions after the asset rename**

```bash
cd corpscout/services/dagster_v3
uv run dg check defs
```

Expected: definitions load and all renamed asset dependencies resolve.

- [ ] **Step 9: Commit Task 1 only**

```bash
git add \
  corpscout/clickhouse/migrations/000285_corpscout_se_bolagsverket_financial_metrics_rename.up.sql \
  corpscout/clickhouse/migrations/000285_corpscout_se_bolagsverket_financial_metrics_rename.down.sql \
  corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/metrics.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/assets.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/__init__.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/assets.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/company_financials_latest/sql.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/sources.yml \
  corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_section_item_source_links_build.sql \
  corpscout/services/backoffice/app/lib/countries.ts \
  corpscout/services/backoffice/app/lib/queries.server.ts \
  corpscout/services/dagster_v3/tests/test_sweden_financial_table_rename_migration.py \
  corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
  corpscout/services/dagster_v3/tests/test_sweden_financial_metrics.py \
  corpscout/services/dagster_v3/tests/test_sweden_financial_assets.py \
  corpscout/services/dagster_v3/tests/test_clickhouse_leaf_checks.py \
  corpscout/services/dagster_v3/tests/test_company_financials_latest.py \
  corpscout/services/backoffice/app/lib/countries.test.ts \
  corpscout/services/backoffice/app/lib/queries-financials.test.ts
git commit -m "Rename Sweden Bolagsverket financial metrics"
```

---

## Task 2 — Establish the code-owned canonical metric map

**Files:**

- Create:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/common/financial_metric_mappings.py`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/observations.py`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/metrics.py`
- Create:
  `corpscout/services/dagster_v3/tests/test_financial_metric_mappings.py`
- Modify:
  `corpscout/services/dagster_v3/tests/test_sweden_financial_observations.py`
- Modify:
  `corpscout/services/dagster_v3/tests/test_esef_filings_metrics.py`

- [ ] **Step 1: Write a failing mapping-contract test**

Create `test_financial_metric_mappings.py` and assert that the public mapping is
canonical-key first and source second:

```python
from dagster_v3.defs.common.financial_metric_mappings import (
    FINANCIAL_METRIC_MAPPINGS,
)


def test_metric_mapping_is_canonical_key_then_source() -> None:
    assert list(FINANCIAL_METRIC_MAPPINGS) == [
        "revenue",
        "operating_result",
        "net_result",
        "total_assets",
        "equity",
        "liabilities",
        "cash_and_bank",
        "current_assets",
        "current_liabilities",
        "personnel_expenses",
        "wages_and_salaries",
        "employees",
    ]
    assert FINANCIAL_METRIC_MAPPINGS["revenue"]["bolagsverket"][0] == (
        "Nettoomsattning"
    )
    assert "ifrs-full:Revenue" in FINANCIAL_METRIC_MAPPINGS["revenue"]["esef"]
    assert (
        "ifrs-full:RentalIncomeFromInvestmentProperty"
        in FINANCIAL_METRIC_MAPPINGS["revenue"]["esef"]
    )
```

Also assert that every mapping entry is a non-empty tuple and only the known
source keys `bolagsverket` and `esef` are present.

- [ ] **Step 2: Run the new test and confirm it fails**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_financial_metric_mappings.py -q
```

Expected: import failure because `financial_metric_mappings.py` does not exist.

- [ ] **Step 3: Add the direct nested mapping**

Create `financial_metric_mappings.py` with one exported constant and no class,
registry object, factory, or interface:

```python
FINANCIAL_METRIC_MAPPINGS: dict[
    str, dict[str, tuple[str, ...]]
] = {
    "revenue": {
        "bolagsverket": ("Nettoomsattning",),
        "esef": (
            "ifrs-full:Revenue",
            "ifrs-full:RevenueFromContractsWithCustomers",
            "ifrs-full:RevenueAndOperatingIncome",
            "ifrs-full:RevenueFromSaleOfGoods",
            "ifrs-full:RevenueFromRenderingOfServices",
            "ifrs-full:RentalIncomeFromInvestmentProperty",
        ),
    },
    "operating_result": {
        "bolagsverket": ("Rorelseresultat",),
        "esef": ("ifrs-full:ProfitLossFromOperatingActivities",),
    },
    "net_result": {
        "bolagsverket": ("AretsResultat",),
        "esef": ("ifrs-full:ProfitLoss",),
    },
    "total_assets": {
        "bolagsverket": ("Tillgangar", "Balansomslutning"),
        "esef": ("ifrs-full:Assets",),
    },
    "equity": {
        "bolagsverket": ("EgetKapital",),
        "esef": ("ifrs-full:Equity",),
    },
    "liabilities": {"esef": ("ifrs-full:Liabilities",)},
    "cash_and_bank": {
        "bolagsverket": ("KassaBank", "KassaBankExklRedovisningsmedel"),
        "esef": ("ifrs-full:CashAndCashEquivalents",),
    },
    "current_assets": {"bolagsverket": ("Omsattningstillgangar",)},
    "current_liabilities": {"bolagsverket": ("KortfristigaSkulder",)},
    "personnel_expenses": {"bolagsverket": ("Personalkostnader",)},
    "wages_and_salaries": {"bolagsverket": ("LonerAndraErsattningar",)},
    "employees": {
        "bolagsverket": ("MedelantaletAnstallda",),
        "esef": ("ifrs-full:AverageNumberOfEmployees",),
    },
}
```

Document in the module that tuple order is source fallback priority.

- [ ] **Step 4: Make both existing pipelines derive their source maps**

In `sweden_financial/observations.py`, derive the canonical part of
`BOLAGSVERKET_FINANCIAL_CONCEPTS` from the new map, then append the existing
Bolagsverket-only supporting concepts (`result_after_financial_items`,
`solidity`, `equity_liabilities`, and `current_receivables`) directly. Preserve
all existing metric codes and SQL behavior.

In `esef_filings/metrics.py`, keep the storage-column names required by
`esef_financial_metrics`, but derive `IFRS_METRIC_CONCEPTS` through this explicit
alias map:

```python
_ESEF_STORAGE_KEY_BY_CANONICAL_KEY = {
    "revenue": "revenue",
    "operating_result": "operating_profit",
    "net_result": "profit_loss",
    "total_assets": "total_assets",
    "equity": "equity",
    "liabilities": "liabilities",
    "cash_and_bank": "cash",
    "employees": "employees",
}
```

Do not rename existing ESEF table columns in this task.

- [ ] **Step 5: Update focused source-pipeline tests**

Update the existing tests so they prove:

- Bolagsverket concept names still resolve to the same metric codes.
- Supporting Bolagsverket concepts were not lost.
- `IFRS_METRIC_CONCEPTS` is derived from the common mapping.
- `RentalIncomeFromInvestmentProperty` is the last revenue fallback.
- `ProfitFromPropertyManagement` remains absent from operating result.
- The ESEF mapping version increments from `esef-ifrs-v1` to
  `esef-ifrs-v2`.

- [ ] **Step 6: Run the mapping and pipeline tests**

```bash
cd corpscout/services/dagster_v3
uv run pytest \
  tests/test_financial_metric_mappings.py \
  tests/test_sweden_financial_observations.py \
  tests/test_esef_filings_metrics.py -q
```

Expected: all pass.

- [ ] **Step 7: Commit Task 2 only**

```bash
git add \
  corpscout/services/dagster_v3/src/dagster_v3/defs/common/financial_metric_mappings.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/observations.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/metrics.py \
  corpscout/services/dagster_v3/tests/test_financial_metric_mappings.py \
  corpscout/services/dagster_v3/tests/test_sweden_financial_observations.py \
  corpscout/services/dagster_v3/tests/test_esef_filings_metrics.py
git commit -m "Add canonical financial source metric mappings"
```

---

## Task 3 — Add separate same-shape ClickHouse source views

**Files:**

- Create:
  `corpscout/clickhouse/migrations/000286_corpscout_se_financial_source_views.up.sql`
- Create:
  `corpscout/clickhouse/migrations/000286_corpscout_se_financial_source_views.down.sql`
- Create:
  `corpscout/services/dagster_v3/tests/test_sweden_financial_source_views_migration.py`
- Modify:
  `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`

- [ ] **Step 1: Write failing migration-contract tests**

Test that the up migration creates exactly these views:

```text
corpscout.se_financials_bolagsverket_current
corpscout.se_financials_esef_current
```

Assert that both view SELECTs expose every column in the canonical source-view
contract above. Also assert:

- Bolagsverket reads `corpscout.se_bolagsverket_financial_metrics`.
- ESEF reads `corpscout.esef_financial_metrics`, joins
  `corpscout.esef_filings`, and resolves `company_id` through
  `corpscout.company_identifier` with `country_code = 'SE'`.
- Bolagsverket emits `accounting_scope = 'standalone'`.
- ESEF emits `accounting_scope = 'consolidated_ifrs'`.
- The ESEF view retains `argMaxIf` amendment composition and
  `composed_from_amendment` does not need to be exposed to the generic metric
  row.
- The down migration drops only these two views.

- [ ] **Step 2: Run and confirm the migration test fails**

```bash
cd corpscout/services/dagster_v3
uv run pytest tests/test_sweden_financial_source_views_migration.py -q
```

Expected: missing migration files.

- [ ] **Step 3: Create the Bolagsverket view**

Use `CREATE OR REPLACE VIEW`. Move the existing selection and tiebreak rules
from the Sweden `financialsQuery` into the view:

```text
fiscal_year DESC
observation_kind = 'reported' DESC
isNull(revenue_amount_original) ASC
source_fiscal_year DESC
source_record_id DESC
```

Use a grouped `argMax(tuple(...), rank_tuple)` projection keyed by company and
year so the view returns one selected Bolagsverket observation per company/year
while an outer `company_id` predicate can still push into the 2M-row source
table. Preserve comparative-year provenance. Do not use `LIMIT BY` inside the
view: live verification showed that it blocks predicate pushdown and turns a
single-company request into a full-table scan.

- [ ] **Step 4: Create the ESEF view**

Move the current `ESEF_FILINGS_QUERY` version-composition CTE into the view and
project the canonical aliases:

```text
operating_profit_* -> operating_result_*
profit_loss_*      -> net_result_*
cash_*             -> cash_and_bank_*
```

Use typed NULL expressions for unsupported fields. Do not join ESEF and
Bolagsverket values.

- [ ] **Step 5: Add the down migration and register migration expectations**

The down migration contains only:

```sql
DROP VIEW IF EXISTS corpscout.se_financials_esef_current;
DROP VIEW IF EXISTS corpscout.se_financials_bolagsverket_current;
```

Add migration 286 to the repository's migration-name expectations in
`test_clickhouse_migrations.py`.

- [ ] **Step 6: Run migration tests**

```bash
cd corpscout/services/dagster_v3
uv run pytest \
  tests/test_sweden_financial_source_views_migration.py \
  tests/test_clickhouse_migrations.py -q
```

Expected: all pass.

- [ ] **Step 7: Apply locally and smoke-query the two views**

From the repository root, apply the pending migrations:

```bash
cd corpscout
make clickhouse-migrate-version
make clickhouse-migrate-up
```

Return to the repository root, then run the following queries with the local
ClickHouse client or the existing ClickHouse query helper:

```sql
SELECT source_id, accounting_scope, company_id, fiscal_year,
       revenue_amount_original, source_document_id
FROM corpscout.se_financials_bolagsverket_current
WHERE company_id = '5567081699'
ORDER BY fiscal_year DESC;

SELECT source_id, accounting_scope, company_id, fiscal_year,
       revenue_amount_original, source_document_id
FROM corpscout.se_financials_esef_current
WHERE company_id IN ('5567081699', '5565200028')
ORDER BY company_id, fiscal_year DESC;
```

Expected:

- Nordic Legal Entity Identifier AB (`5567081699`) has independently queryable
  rows from both views.
- Sagax (`5565200028`) has ESEF rows even when Bolagsverket standardized values
  are absent.
- No row contains values coalesced from both sources.

- [ ] **Step 8: Commit Task 3 only**

```bash
git add \
  corpscout/clickhouse/migrations/000286_corpscout_se_financial_source_views.up.sql \
  corpscout/clickhouse/migrations/000286_corpscout_se_financial_source_views.down.sql \
  corpscout/services/dagster_v3/tests/test_sweden_financial_source_views_migration.py \
  corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "Add separate Sweden financial source views"
```

---

## Task 4 — Load each Sweden source through the canonical view contract

**Files:**

- Modify: `corpscout/services/backoffice/app/lib/countries.ts`
- Modify: `corpscout/services/backoffice/app/lib/queries.server.ts`
- Modify: `corpscout/services/backoffice/app/lib/countries.test.ts`
- Modify: `corpscout/services/backoffice/app/lib/queries-financials.test.ts`
- Modify: `corpscout/services/backoffice/tests/esef-financial-reports.server.test.ts`

- [ ] **Step 1: Write failing source-loader tests**

Extend `queries-financials.test.ts` to assert that Sweden's financial-detail
loader independently queries:

```text
se_financials_bolagsverket_current
se_financials_esef_current
```

Test three responses:

1. only Bolagsverket rows → one available source;
2. only ESEF rows → one available source;
3. rows from both → two available sources in configured order.

Assert there is no SQL `UNION`, `coalesce` between source views, or shared
`LIMIT 1 BY fiscal_year` after the two result sets are returned.

- [ ] **Step 2: Run the tests and confirm they fail**

```bash
cd corpscout/services/backoffice
npm test -- app/lib/queries-financials.test.ts
```

Expected: current loader still reads the source-native tables/query paths.

- [ ] **Step 3: Add a source-aware financial row type**

Keep `FinancialYearRow` unchanged for other countries. Add
`FinancialSourceYearRow extends FinancialYearRow` with these required fields:

```typescript
source_id: "bolagsverket" | "esef";
accounting_scope: "standalone" | "consolidated_ifrs";
source_document_id: string;
source_record_uids: string[];
source_url: string;
viewer_url: string;
```

Change `CompanyFinancialSource.financials` to use
`FinancialSourceYearRow[]`. Keep the existing optional provenance fields on the
base row. Do not add a second ESEF-only display row type for the Financials tab.

- [ ] **Step 4: Point Sweden's configured source queries at the views**

Keep source metadata in `financialSources`, but give each definition an
explicit query identifier or view query. Do the smallest direct change that
lets `getCompanyFinancialDetail` execute one query per configured source.

For Sweden:

```text
bolagsverket-annual-accounts -> se_financials_bolagsverket_current
esef                         -> se_financials_esef_current
```

Do not convert every country's financial configuration in this task. Preserve
the existing generic path for Finland, Norway, and other countries.

- [ ] **Step 5: Normalize evidence attachment for both row sets**

Use `source_record_uids` for every source row. Bolagsverket supplies a
one-element array; ESEF may supply multiple UIDs after amendment composition.
Attach evidence without a source-kind branch.

- [ ] **Step 6: Keep the existing ESEF report-detail query operational**

`getEsefFinancialReport` and the full company overview still use
`EsefFilingRow`. Do not delete that type or its query as part of this task. Only
the dedicated Financials tab should consume the new canonical ESEF source view.

- [ ] **Step 7: Run loader and integration tests**

```bash
cd corpscout/services/backoffice
npm test -- \
  app/lib/countries.test.ts \
  app/lib/queries-financials.test.ts \
  tests/esef-financial-reports.server.test.ts
```

Expected: all pass against migrated local ClickHouse.

- [ ] **Step 8: Commit Task 4 only**

```bash
git add \
  corpscout/services/backoffice/app/lib/countries.ts \
  corpscout/services/backoffice/app/lib/queries.server.ts \
  corpscout/services/backoffice/app/lib/countries.test.ts \
  corpscout/services/backoffice/app/lib/queries-financials.test.ts \
  corpscout/services/backoffice/tests/esef-financial-reports.server.test.ts
git commit -m "Load Sweden financial sources independently"
```

---

## Task 5 — Render every source through one selectable financial overview

**Files:**

- Create:
  `corpscout/services/backoffice/app/components/financials/financial-source-switcher.tsx`
- Rename/Modify:
  `corpscout/services/backoffice/app/components/financials/sweden-financial-overview.tsx`
  to
  `corpscout/services/backoffice/app/components/financials/financial-source-overview.tsx`
- Modify:
  `corpscout/services/backoffice/app/components/financials/copy.ts`
- Modify:
  `corpscout/services/backoffice/app/components/financials/metrics.ts`
- Modify:
  `corpscout/services/backoffice/app/components/financials/metrics.test.ts`
- Modify: `corpscout/services/backoffice/app/routes/company-financials.tsx`
- Delete after route migration:
  `corpscout/services/backoffice/app/components/detail/esef-section.tsx`

- [ ] **Step 1: Write failing source-selection helper tests**

Add pure tests in `metrics.test.ts` for a small helper that:

- preserves configured source order;
- omits sources with no metric rows and no source documents;
- chooses the first available source initially;
- retains the selected source if it is still available;
- falls back to the first source if the selected source disappears.

Keep selection behavior pure so it can be tested without adding a browser test
library.

- [ ] **Step 2: Run the helper tests and confirm they fail**

```bash
cd corpscout/services/backoffice
npm test -- app/components/financials/metrics.test.ts
```

- [ ] **Step 3: Make the overview source-neutral and locale-controlled**

Rename `SwedenFinancialOverview` to `FinancialSourceOverview` and change its
props to accept:

```typescript
source: CompanyFinancialSource;
locale: FinancialLocale;
onLocaleChange: (locale: FinancialLocale) => void;
factsHref?: (row: FinancialSourceYearRow) => string;
filingStatus: CompanyFinancialFilingStatus | null;
children?: ReactNode;
```

Remove hardcoded Bolagsverket/standalone language from the component. Read
source label, source description, scope label, filing-note wording, and facts
action wording from static mappings in `copy.ts` keyed by locale and source ID.

The common sections—KPI strip, chart, ratios, income statement, balance sheet,
SEK/USD values, evidence, and unavailable-year handling—remain unchanged.

- [ ] **Step 4: Add static source copy for English and Swedish**

Add `financialSourceCopy` with entries for:

```text
bolagsverket-annual-accounts
esef
```

Each entry must define localized:

- source label;
- short source description;
- accounting-scope label;
- latest-filing label;
- source-facts action;
- evidence/source note.

Do not duplicate the common financial table labels in this mapping.

- [ ] **Step 5: Build the source switcher with existing shadcn Tabs**

Use the installed `Tabs`, `TabsList`, and `TabsTrigger` components. The switcher
owns `selectedSourceId` and `locale`, so language stays stable while the source
changes.

Each trigger shows:

- localized source name;
- scope badge;
- available year range.

Render one `FinancialSourceOverview` for the active source. If only one source
is available, keep the source identity visible; the single trigger may remain
disabled or render as a non-interactive selected tab. Do not stack multiple
complete overview components.

- [ ] **Step 6: Route facts links by source row**

In `company-financials.tsx`:

- Bolagsverket →
  `/company/se/:id/facts/:fiscalYear`
- ESEF →
  `/company/se/:id/financials/esef/:sourceDocumentId`

Pass filing status and `FinancialReportDocuments` only to the Bolagsverket
source. ESEF should show its source URL/viewer link and its existing All Facts
page, but not Bolagsverket's filing-status wording.

- [ ] **Step 7: Remove the old ESEF-only presentation path**

After both sources render through `FinancialSourceOverview`, remove
`EsefSection` and its route import. Confirm `rg "EsefSection"` returns no
matches.

- [ ] **Step 8: Run UI tests and typecheck**

```bash
cd corpscout/services/backoffice
npm test -- \
  app/components/financials/metrics.test.ts \
  app/lib/countries.test.ts \
  app/lib/queries-financials.test.ts
npm run typecheck
```

Expected: all tests and TypeScript checks pass.

- [ ] **Step 9: Commit Task 5 only**

```bash
git add \
  corpscout/services/backoffice/app/components/financials/financial-source-switcher.tsx \
  corpscout/services/backoffice/app/components/financials/financial-source-overview.tsx \
  corpscout/services/backoffice/app/components/financials/copy.ts \
  corpscout/services/backoffice/app/components/financials/metrics.ts \
  corpscout/services/backoffice/app/components/financials/metrics.test.ts \
  corpscout/services/backoffice/app/routes/company-financials.tsx \
  corpscout/services/backoffice/app/components/detail/esef-section.tsx
git commit -m "Add selectable financial source overview"
```

---

## Task 6 — End-to-end verification and documentation

**Files:**

- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/docs/sweden_financial-design.md`
- Modify:
  `corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/docs/esef_filings-design.md`
- Create:
  `corpscout/services/backoffice/tests/sweden-financial-source-views.server.test.ts`

- [ ] **Step 1: Add a live-data integration test**

The test should load:

- Nordic Legal Entity Identifier AB (`5567081699`), expected to expose both
  sources;
- Sagax (`5565200028`), expected to expose ESEF even when Bolagsverket has no
  standardized numeric metrics.

Assert source order, source scope, year ordering, non-empty source document IDs,
and source-specific facts links.

- [ ] **Step 2: Update the two data-design documents**

Document:

- the canonical-key-first mapping shape;
- source-owned tables versus source-specific serving views;
- the identical serving-view contract;
- why values are selected by source rather than merged;
- how a third source is added;
- why property-management result and HTML disclosure extraction remain outside
  the first version.

- [ ] **Step 3: Validate Dagster definitions**

```bash
cd corpscout/services/dagster_v3
uv run dg check defs
```

Expected: definitions load successfully.

- [ ] **Step 4: Run the focused data and application suites**

```bash
cd corpscout/services/dagster_v3
uv run pytest \
  tests/test_financial_metric_mappings.py \
  tests/test_sweden_financial_observations.py \
  tests/test_sweden_financial_metrics.py \
  tests/test_esef_filings_metrics.py \
  tests/test_sweden_financial_source_views_migration.py \
  tests/test_clickhouse_migrations.py -q

cd ../backoffice
npm test -- \
  app/components/financials/metrics.test.ts \
  app/lib/countries.test.ts \
  app/lib/queries-financials.test.ts \
  tests/esef-financial-reports.server.test.ts \
  tests/sweden-financial-source-views.server.test.ts
npm run typecheck
npm run build
```

Expected: all commands pass.

- [ ] **Step 5: Verify the browser behavior**

Open these pages in the local backoffice:

```text
http://localhost:5183/company/se/5567081699/financials
http://localhost:5183/company/se/5565200028/financials
```

For Nordic Legal Entity Identifier AB verify:

- both Bolagsverket and ESEF appear in the source selector;
- clicking either source keeps the same page structure and changes the values;
- source and accounting scope are unambiguous;
- Swedish/English selection persists across source changes;
- every monetary amount shows source currency and USD;
- Bolagsverket and ESEF All Facts actions open their correct detail pages.

For Sagax verify:

- ESEF is visible and selected even if it is the only metric source;
- rental income appears as revenue after the `esef-ifrs-v2` metrics rebuild;
- missing operating result or employee values render as unavailable, not zero;
- no empty Bolagsverket overview is shown above the ESEF data.

Check desktop and narrow/mobile widths. Confirm keyboard navigation across the
source tabs and visible focus states.

- [ ] **Step 6: Commit Task 6 only**

```bash
git add \
  corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_financial/docs/sweden_financial-design.md \
  corpscout/services/dagster_v3/src/dagster_v3/defs/esef_filings/docs/esef_filings-design.md \
  corpscout/services/backoffice/tests/sweden-financial-source-views.server.test.ts
git commit -m "Document and verify Sweden financial source views"
```

---

## Rollout order

1. Land and apply migration 284 if it is not already deployed.
2. Land Task 1. Pause the affected Sweden metrics materialization, apply rename
   migration 285, deploy the same revision of Dagster and backoffice code, then
   resume the renamed `se_bolagsverket_financial_metrics_clickhouse` asset.
   Validate that row count, company count, and year range are unchanged by the
   metadata-only rename.
3. Land Task 2 and rebuild `esef_financial_metrics` so
   `RentalIncomeFromInvestmentProperty` is available through `esef-ifrs-v2`.
4. Apply source-view migration 286 and validate both views directly.
5. Deploy Task 4's backoffice source loaders.
6. Deploy Task 5's source-switching UI.
7. Complete Task 6 and verify the two-source company and Sagax before treating the feature as
   complete.

The table rename is a coordinated release because old code reads the old name
and new code reads the new name. Do not resume the Dagster writer between the
migration and code deployment. The source-view migration must precede the new
backoffice source-loader deployment.

## Completion criteria

- The physical Bolagsverket table is named
  `se_bolagsverket_financial_metrics`; the old `se_financial_metrics` name
  remains only in historical migrations and the explicit rename migration.
- The Dagster asset, health checks, latest-financial summary, evidence model,
  and backoffice runtime queries all use the source-specific name.
- One canonical metric mapping is organized as
  `canonical metric -> source -> ordered source concepts`.
- Bolagsverket and ESEF remain distinct source datasets.
- Two ClickHouse views expose the same source-view contract.
- The Financials tab lists every available source and displays one selected
  source at a time through the same overview component.
- Language and SEK/USD behavior are consistent across sources.
- Every displayed value retains a route to its source facts and evidence.
- Sagax no longer appears financially empty merely because its data is ESEF.
- No cross-source value merge, priority rule, or conflict model is introduced.
