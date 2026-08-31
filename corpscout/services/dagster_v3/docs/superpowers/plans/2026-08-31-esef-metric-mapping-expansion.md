# ESEF Metric Mapping Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the audited gaps in the ESEF→metrics mapping: operating result for financial institutions (fallback `ifrs-full:ProfitLossBeforeTax`), a new `personnel_expenses` metric from `ifrs-full:EmployeeBenefitsExpense`, and a documented no-change decision on employees.

**Architecture:** Candidate concepts live in `src/dagster_v3/defs/common/financial_metric_mappings.py`; `esef_filings/metrics.py` compiles them at import time into `argMaxIf`+`coalesce` SQL and rebuilds `corpscout.esef_financial_metrics` atomically (stage → shrink-guard → `EXCHANGE TABLES`). Phase A (operating-result fallback) is pure mapping+tests — no schema change. Phase B (personnel expenses) adds two `Nullable(Decimal128(2))` columns to the metrics table, threads the new metric through `metrics.py`, `tables.py`, and the `se_financials_esef_current` view (whose `personnel_expenses_*` outputs are today hardcoded NULL). `MAPPING_VERSION` bumps `esef-ifrs-v2` → `esef-ifrs-v3` (pure provenance stamp — nothing keys off it, but tests pin it).

**Tech Stack:** Python 3.14, Dagster, ClickHouse (golang-migrate ledger), pytest.

**Spec:** Data-driven audit approach chosen by owner 2026-08-31. Audit evidence (prod ClickHouse, 6,620 parsed filings with facts):
- `operating_profit` NULL for 969 filings; **919 of them tag `ifrs-full:ProfitLossBeforeTax`** (banks/insurers report "rörelseresultat" as profit before tax; they have no `ProfitLossFromOperatingActivities`).
- `ifrs-full:EmployeeBenefitsExpense` appears in **2,792 filings**; the `personnel_expenses` metric has no ESEF mapping at all.
- `employees` maps for only 18/6,620 (`ifrs-full:AverageNumberOfEmployees` is essentially never tagged; the audit found no viable substitute headcount concept — candidates like `NoncurrentProvisionsForEmployeeBenefits` are monetary, not headcounts). **Decision: leave the employees mapping unchanged; document.**
- Reference filing: Svenska Handelsbanken FY2023 (`NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0`) — has `ProfitLossBeforeTax` and `EmployeeBenefitsExpense` (13,642,000,000 SEK), currently shows Operating result "—".
- **Task 1 re-measurement (2026-08-31, execution time):** operating_profit non-null 5,651/6,620; employees 18/6,620; `ProfitLossBeforeTax` present in **921** of the 969 filings missing operating result (audit gate passed); `EmployeeBenefitsExpense` in 2,792 filings. The 919 above was the plan-time estimate; 921 is the executed figure used in code comments.

## Global Constraints

- Run tests from `corpscout/services/dagster_v3`: `uv run pytest tests/<file> -q` (repo convention; check `Makefile`/`pyproject` for the exact runner if `uv` is not configured).
- The ordered tuple contract: **within a metric, earlier concepts win** (`financial_metric_mappings.py` docstring; `metrics.py:117`). `ProfitLossBeforeTax` must come AFTER `ProfitLossFromOperatingActivities`.
- Deterministic tiebreak rule (house rule, `metrics.py:120-124`): every `argMaxIf` uses `(coalesce(decimals, -1000), fact_id)` — the SQL builder does this automatically for new candidates; never hand-write a bare `argMax`.
- Migration ledger: new migrations must be appended to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py:32-374`; the next free number is **000364** unless the uncommitted working-tree `000363_corpscout_se_jobtech_links_jobs` has grown — always take `max(existing)+1` at execution time. Both `.up.sql` and `.down.sql` are mandatory; no `;` inside `--` comments; up must not TRUNCATE.
- Heavy materializations run on prod Dagster (companycollect), launched from its UI — never locally.
- **Sequencing:** do not rebuild metrics mid-backfill for the final numbers; the rebuild in Task 5 can run any time (it is atomic), but the acceptance counts below assume the 2026-08-31 corpus (6,620 filings). Re-measure whatever the corpus is at execution time.
- Conventional Commits; do not commit the pre-existing unrelated working-tree changes.

## File Structure

- Modify: `src/dagster_v3/defs/common/financial_metric_mappings.py`
- Modify: `src/dagster_v3/defs/esef_filings/metrics.py` (Phase B only: new metric column wiring)
- Modify: `src/dagster_v3/defs/esef_filings/tables.py` (Phase B: column constants pinned against migration DDL)
- Create: `corpscout/clickhouse/migrations/000364_corpscout_esef_personnel_expenses.up.sql` / `.down.sql` (Phase B)
- Modify: `tests/test_financial_metric_mappings.py`, `tests/test_esef_filings_metrics.py`, `tests/test_clickhouse_migrations.py`, `tests/test_esef_filings_client.py` (column-order test)

---

### Task 1: Re-run and record the audit

- [ ] **Step 1:** Run the audit queries against prod ClickHouse (`ssh companycollect`, `docker exec clickhouse-clickhouse-1 clickhouse-client`):

```sql
-- Baseline coverage
SELECT countIf(operating_profit_amount_original IS NOT NULL) AS op,
       countIf(employees IS NOT NULL) AS emp,
       count() AS filings
FROM corpscout.esef_financial_metrics WHERE source_fact_count > 0;

-- Operating-result candidates among filings missing it
WITH missing AS (
  SELECT fxo_id FROM corpscout.esef_financial_metrics
  WHERE source_fact_count > 0 AND operating_profit_amount_original IS NULL)
SELECT concept_qname, uniqExact(fxo_id) AS filings
FROM corpscout.esef_facts
WHERE fxo_id IN (SELECT fxo_id FROM missing) AND value_kind = 'monetary'
  AND (concept_local_name ILIKE '%profit%' OR concept_local_name ILIKE '%operating%')
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Personnel-expense candidate
SELECT uniqExact(fxo_id) FROM corpscout.esef_facts
WHERE concept_qname = 'ifrs-full:EmployeeBenefitsExpense';
```

- [ ] **Step 2:** Append the numbers + date to this plan's "Spec" section (or a sibling `docs/analysis/` note). If `ProfitLossBeforeTax` no longer dominates the missing set (<50%), STOP and re-decide with the owner.

### Task 2 (Phase A): operating-result fallback

**Files:**
- Modify: `src/dagster_v3/defs/common/financial_metric_mappings.py` (operating_result esef tuple)
- Modify: `tests/test_esef_filings_metrics.py:116-156`, `tests/test_financial_metric_mappings.py`

**Interfaces:**
- Produces: `FINANCIAL_METRIC_MAPPINGS["operating_result"]["esef"] == ("ifrs-full:ProfitLossFromOperatingActivities", "ifrs-full:ProfitLossBeforeTax")`; `MAPPING_VERSION == "esef-ifrs-v3"`.

- [ ] **Step 1: Update the tests first.** In `tests/test_esef_filings_metrics.py`:
  - `test_ifrs_metric_concepts_shape_and_order` (`:116`): the per-metric "exactly 1 concept" loop (`:145-155`) must now special-case `operating_profit` — assert its full ordered tuple:

```python
    assert IFRS_METRIC_CONCEPTS["operating_profit"] == (
        "ifrs-full:ProfitLossFromOperatingActivities",
        "ifrs-full:ProfitLossBeforeTax",
    )
```

  - `:156`: `assert MAPPING_VERSION == "esef-ifrs-v3"`.
  - `test_liabilities_fallback_uses_coalesced_candidates` (`:205`) references `liabilities_c0`/`total_assets_c0`/`equity_c0` — those metrics are unchanged, so it still passes; verify, don't edit.

  In `tests/test_financial_metric_mappings.py` add:

```python
def test_esef_operating_result_falls_back_to_profit_before_tax():
    concepts = FINANCIAL_METRIC_MAPPINGS["operating_result"]["esef"]
    assert concepts == (
        "ifrs-full:ProfitLossFromOperatingActivities",
        "ifrs-full:ProfitLossBeforeTax",
    )
```

- [ ] **Step 2: Run — FAIL** (`uv run pytest tests/test_esef_filings_metrics.py tests/test_financial_metric_mappings.py -q`).

- [ ] **Step 3: Implement.** In `financial_metric_mappings.py`:

```python
    "operating_result": {
        "bolagsverket": ("Rorelseresultat",),
        "esef": (
            "ifrs-full:ProfitLossFromOperatingActivities",
            # Financial institutions (banks, insurers) have no operating-
            # activities subtotal; their reported operating result is profit
            # before tax. Audit 2026-08-31: present in 919 of the 969 parsed
            # filings that the primary concept misses.
            "ifrs-full:ProfitLossBeforeTax",
        ),
    },
```

In `metrics.py:110`: `MAPPING_VERSION = "esef-ifrs-v3"`.

- [ ] **Step 4: Run — PASS**, including `test_build_select_contains_all_eight_concept_groups` (`:166`, auto-adapts to new candidates). Run the whole ESEF test set: `uv run pytest tests/test_esef_filings_metrics.py tests/test_financial_metric_mappings.py tests/test_esef_filings_assets.py -q`.

- [ ] **Step 5: Commit** — `git commit -m "feat(esef): map ProfitLossBeforeTax as operating-result fallback (esef-ifrs-v3)"`.

### Task 3 (Phase B): personnel-expenses migration

**Files:**
- Create: `corpscout/clickhouse/migrations/000364_corpscout_esef_personnel_expenses.up.sql` / `.down.sql`
- Modify: `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS` tuple + a content test)

- [ ] **Step 1: Write the ledger test additions first.** Append `"000364_corpscout_esef_personnel_expenses"` to `EXPECTED_MIGRATIONS` (keep sorted position) and add a content test near the other per-migration tests:

```python
def test_esef_personnel_expenses_migration_is_additive_and_reversible() -> None:
    up = (MIGRATIONS_DIR / "000364_corpscout_esef_personnel_expenses.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000364_corpscout_esef_personnel_expenses.down.sql").read_text()
    assert "ADD COLUMN IF NOT EXISTS personnel_expenses_amount_original Nullable(Decimal128(2))" in up
    assert "ADD COLUMN IF NOT EXISTS personnel_expenses_amount_usd Nullable(Decimal128(2))" in up
    assert "CREATE OR REPLACE VIEW corpscout.se_financials_esef_current" in up
    assert "DROP COLUMN IF EXISTS personnel_expenses_amount_original" in down
    assert "TRUNCATE" not in up
```

- [ ] **Step 2: Run — FAIL** (`uv run pytest tests/test_clickhouse_migrations.py -q`, the explicit-listing test fails first).

- [ ] **Step 3: Write the migration.** `000364_...up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Additive: banks and most industrials tag ifrs-full:EmployeeBenefitsExpense
-- (2,792 of 6,620 parsed filings, audit 2026-08-31); the serving view's
-- personnel_expenses outputs were hardcoded NULL until now.
ALTER TABLE corpscout.esef_financial_metrics
    ADD COLUMN IF NOT EXISTS personnel_expenses_amount_original Nullable(Decimal128(2)) AFTER cash_amount_usd,
    ADD COLUMN IF NOT EXISTS personnel_expenses_amount_usd Nullable(Decimal128(2)) AFTER personnel_expenses_amount_original;

CREATE OR REPLACE VIEW corpscout.se_financials_esef_current AS
-- Full view body copied from 000286_corpscout_se_financial_source_views.up.sql:102-210
-- with exactly one change: the two personnel_expenses output columns switch from
-- CAST(NULL ...) to argMax over the new metric columns, mirroring how
-- operating_result maps operating_profit_amount_original/usd in that body.
...;
```

The `...` above is NOT a placeholder to leave — at execution time copy the 108-line view body from `000286_corpscout_se_financial_source_views.up.sql:102-210` verbatim and swap the `personnel_expenses_amount_original` / `personnel_expenses_amount_usd` NULL casts (in the `:178-185` block) for the same `argMax(v.personnel_expenses_amount_original, v.version)`-style expressions the sibling columns use. Down migration: `ALTER TABLE ... DROP COLUMN IF EXISTS` both columns and `CREATE OR REPLACE VIEW` restoring the exact 000286 body.

- [ ] **Step 4: Run ledger tests — PASS.** Commit: `git commit -m "feat(clickhouse): personnel_expenses columns on esef_financial_metrics + serving view"`.

### Task 4 (Phase B): thread the metric through the export

**Files:**
- Modify: `src/dagster_v3/defs/common/financial_metric_mappings.py` (add `"esef": ("ifrs-full:EmployeeBenefitsExpense",)` to `personnel_expenses`)
- Modify: `src/dagster_v3/defs/esef_filings/metrics.py` (`:183-190` coalesce list, the stage-SELECT projection and USD-conversion block, the INSERT column list at `:511-515`)
- Modify: `src/dagster_v3/defs/esef_filings/tables.py` (metrics column constants — pinned by `tests/test_esef_filings_client.py::test_export_columns_match_migration_000149_column_order` and now 000364)
- Modify: `tests/test_esef_filings_metrics.py` (`test_ifrs_metric_concepts_shape_and_order` — key count 8→9 and key order; `test_esef_financial_metrics_export_columns_match_migration_order` `:307`)

- [ ] **Step 1: Tests first.** `IFRS_METRIC_CONCEPTS` gains key `personnel_expenses` (no storage-key rename needed — the canonical key IS the column stem). Update the pinned key tuple and add:

```python
    assert IFRS_METRIC_CONCEPTS["personnel_expenses"] == (
        "ifrs-full:EmployeeBenefitsExpense",
    )
```

Update the export-column-order test fixture to include the two new columns in migration order (after `cash_amount_usd`).

- [ ] **Step 2: Run — FAIL.**

- [ ] **Step 3: Implement.** Mapping: add the esef tuple to `personnel_expenses`. In `metrics.py`, mirror the `cash` metric end-to-end: a `personnel_expenses_sql = _coalesce_candidates_sql("personnel_expenses")` line beside `:183-190`, projection + USD conversion columns beside the cash ones, and the two column names in the INSERT list — grep `cash_amount_original` in `metrics.py` and replicate every occurrence for `personnel_expenses_amount_original`. In `tables.py`, add both columns to the metrics column constant in migration order.

- [ ] **Step 4: Run the full dagster test suite for the module — PASS**: `uv run pytest tests/test_esef_filings_metrics.py tests/test_esef_filings_client.py tests/test_financial_metric_mappings.py tests/test_clickhouse_migrations.py -q`.

- [ ] **Step 5: Commit** — `git commit -m "feat(esef): export personnel_expenses metric from EmployeeBenefitsExpense"`.

### Task 5: Apply, rebuild, verify (prod)

- [ ] **Step 1:** Apply migration: `make clickhouse-migrate-up` from `corpscout/` with the prod `CLICKHOUSE_MIGRATE_URL` (deployment recipe: see the worktree deploy runbook if the tree is dirty).
- [ ] **Step 2:** Deploy dagster_v3 code to prod (pristine-worktree recipe; dbt-state refresh if applicable).
- [ ] **Step 3:** Materialize `esef_financial_metrics_clickhouse` from the prod Dagster UI (it belongs to no job/schedule — manual only; `allow_shrink` stays False, growth expected).
- [ ] **Step 4:** Verify:

```sql
SELECT countIf(operating_profit_amount_original IS NOT NULL) AS op,
       countIf(personnel_expenses_amount_original IS NOT NULL) AS pers,
       count() AS filings, any(mapping_version)
FROM corpscout.esef_financial_metrics WHERE source_fact_count > 0;
-- Expect (2026-08-31 corpus): op ≈ 6,570 (was 5,651), pers ≈ 2,792, version esef-ifrs-v3
SELECT operating_profit_amount_original, personnel_expenses_amount_original
FROM corpscout.esef_financial_metrics
WHERE fxo_id = 'NHBDILHZTYCNBV5UYZ31-2023-12-31-ESEF-SE-0';
-- Expect non-NULL operating result (ProfitLossBeforeTax) and 13,642,000,000 personnel expenses
```

- [ ] **Step 5:** Check the backoffice: Handelsbanken Financial tab now shows Operating result and Personnel expenses for 2023 (`se_financials_esef_current` is a view — no extra refresh; `se_companies_serving` MV auto-refreshes within 15 min).
- [ ] **Step 6:** Record before/after coverage in the session summary.

## Self-Review

- Both audited wins covered (Tasks 2, 3-4); employees decision documented in Spec, no code change. ✔
- Ordered-tuple semantics preserved (fallback second). ✔
- Every test pin the exploration found is addressed: `:116/:145-155/:156` shape test, `:205` liabilities (verified-unchanged), `:307` column order, client column-order test, migration ledger listing + content test. ✔
- The one deliberate not-fully-inline block (view body copy) names its exact source lines and the exact substitution — executor needs no design judgment. ✔
