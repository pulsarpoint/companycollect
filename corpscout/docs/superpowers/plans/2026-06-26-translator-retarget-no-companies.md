# Retarget Translation to `no_companies` + Drop Orphaned Raw Exports — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Point the translator at the *consumed* resolved table `corpscout.no_companies` instead of the orphaned raw `corpscout.companies`, and drop the orphaned raw Norway exports (`corpscout.companies` + `corpscout.financial_statements`) along with their export assets.

**Architecture:** Carry the 3 free-text `*_original` columns through `norway_resolved`'s `no_companies` dbt model + ClickHouse schema; retarget the translator registry + view + trigger to `no_companies` (the `text_translations` cache + join-view design is unchanged — `no_companies` is wipe-and-replaced via EXCHANGE TABLES, so a cache that survives the wipe is still correct); then remove the raw export assets and drop the two orphaned tables + the old view via forward migrations.

**Tech Stack:** dbt-duckdb, ClickHouse SQL (golang-migrate), Dagster, Temporal, pytest.

## Global Constraints

- Migrations are forward-only under `corpscout/clickhouse/migrations/`; next numbers are `000059`, `000060`, `000061`. Append each to `EXPECTED_MIGRATIONS` in `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`.
- Every up migration carries `CREATE DATABASE IF NOT EXISTS corpscout;` (project suite requirement). Views use `CREATE OR REPLACE VIEW`.
- `ORDER BY` must stay non-nullable. `no_companies` is `ReplacingMergeTree(resolved_at) ORDER BY (org_number)`.
- The 3 free-text fields: `company_description` / `articles_purpose` / `activity_text` (columns `*_original`). Source language `no`. The translator owns ONLY these; `legal_form_description` (reference translation) is out of scope.
- Keep `000003`/`000004` + `COMPANIES_DDL`/`FINANCIAL_STATEMENTS` constants (forward-only ledger + the `test_clickhouse_migrations.py:438` DDL pin); we drop the live tables via a new migration, not by rewinding.
- Sequencing rule: the graph + migrations must stay valid at every committed step — **add columns + new view + retarget trigger BEFORE dropping `companies` and its exports.**
- Run `uv run dg check defs` + `uv run pytest` (from `corpscout/dagster_v3/`) before finishing each task. Commit by explicit path; never `git add -A` (the tree carries unrelated scheduler/ui/compose deletions in flight).

## File Structure

| File | Change |
|------|--------|
| `…/norway_resolved/dbt/models/no_companies.sql` | SELECT the 3 `*_original` columns from the entities source |
| `…/norway_resolved/tables.py` | Add the 3 to `RESOLVED_TABLE_COLUMNS['no_companies']` |
| `clickhouse/migrations/000059_*` | `ALTER no_companies ADD COLUMN` the 3 `*_original` |
| `clickhouse/migrations/000060_*` | `CREATE VIEW corpscout.no_companies_translated` |
| `translator/registry.py` | `ch_table` → `corpscout.no_companies` |
| `tests/test_translator_scan.py` | update expected `FROM corpscout.no_companies` |
| `…/norway_brreg/assets.py` | trigger deps → `norway_resolved_clickhouse`; remove the 2 raw export assets + export fns; rewrite refresh job |
| `…/norway_brreg/definitions.py` | drop the 2 export assets from the lists |
| `…/norway_brreg/tables.py` | remove `*_EXPORT_COLUMNS` usage for the dropped exports (keep `COMPANIES_DDL`/`FINANCIAL_STATEMENTS_DDL`) |
| `clickhouse/migrations/000061_*` | drop view `norway_companies_translated`; `DROP TABLE corpscout.companies`, `corpscout.financial_statements` |
| tests | migration contract + norway asset graph tests updated |

Paths under `corpscout/dagster_v3/` unless noted (migrations under `corpscout/clickhouse/`).

---

## Task 1 — Carry free-text `*_original` into `no_companies` (dbt + resolved schema)

**Files:** `…/norway_resolved/dbt/models/no_companies.sql`, `…/norway_resolved/tables.py`, `clickhouse/migrations/000059_corpscout_no_companies_free_text_columns.{up,down}.sql`, `tests/test_clickhouse_migrations.py`, `tests/test_norway_resolved_dbt.py` (or a new schema test)

**Produces:** `corpscout.no_companies` carries `company_description_original`, `articles_purpose_original`, `activity_text_original` (Nullable(String)); the dbt model + RESOLVED_TABLE_COLUMNS + the export include them.

- [ ] **Step 1: dbt model** — in `no_companies.sql`, add to the SELECT (from `{{ source('norway_brreg','entities') }}`):
```sql
  nullif(company_description_original, '') as company_description_original,
  nullif(articles_purpose_original, '') as articles_purpose_original,
  nullif(activity_text_original, '') as activity_text_original,
```
- [ ] **Step 2: resolved table contract** — add the 3 names to `RESOLVED_TABLE_COLUMNS['no_companies']` in `norway_resolved/tables.py` (they flow into `RESOLVED_EXPORT_COLUMNS` via the existing `_export_columns` helper). Place them adjacent to the `legal_form_description_*` block.
- [ ] **Step 3: migration 000059 up** — `CREATE DATABASE IF NOT EXISTS corpscout;` then:
```sql
ALTER TABLE corpscout.no_companies ADD COLUMN IF NOT EXISTS company_description_original Nullable(String);
ALTER TABLE corpscout.no_companies ADD COLUMN IF NOT EXISTS articles_purpose_original Nullable(String);
ALTER TABLE corpscout.no_companies ADD COLUMN IF NOT EXISTS activity_text_original Nullable(String);
```
- [ ] **Step 4: migration 000059 down** — `ALTER TABLE corpscout.no_companies DROP COLUMN IF EXISTS …` for the 3.
- [ ] **Step 5:** append `"000059_corpscout_no_companies_free_text_columns"` to `EXPECTED_MIGRATIONS`; add a contract test asserting the 3 ADD COLUMNs.
- [ ] **Step 6:** `uv run pytest tests/test_clickhouse_migrations.py tests/test_norway_resolved_dbt.py -q` green; `uv run dg check defs` passes. Commit.

> Note: `no_companies` is wipe-and-replaced (`CREATE stage AS no_companies` copies the live schema, then inserts `RESOLVED_EXPORT_COLUMNS`). The migration owns the schema; the export now fills the 3 columns.

---

## Task 2 — `no_companies_translated` view + retarget the translator registry

**Files:** `clickhouse/migrations/000060_corpscout_no_companies_translated_view.{up,down}.sql`, `translator/registry.py`, `tests/test_translator_scan.py`, `tests/test_text_translations_schema.py`, `tests/test_clickhouse_migrations.py`

**Produces:** view `corpscout.no_companies_translated` (no_companies + the 3 `_en` from `text_translations`); the translator scans/flushes against `corpscout.no_companies`.

- [ ] **Step 1: migration 000060 up** — `CREATE DATABASE IF NOT EXISTS corpscout;` then a `CREATE OR REPLACE VIEW corpscout.no_companies_translated AS SELECT c.*, ifNull(ap.translated_text,'') AS articles_purpose_en, ifNull(act.translated_text,'') AS activity_text_en, ifNull(cd.translated_text,'') AS company_description_en FROM corpscout.no_companies AS c LEFT JOIN (… argMax over text_translations WHERE source_slug='norway_brreg' AND field='articles_purpose' …) ap ON ap.source_text_hash = cityHash64(c.articles_purpose_original)` and likewise for `activity_text`/`company_description`. No `EXCEPT` (no_companies never had these `_en`).
- [ ] **Step 2: migration 000060 down** — `DROP VIEW IF EXISTS corpscout.no_companies_translated;`
- [ ] **Step 3:** append the migration name to `EXPECTED_MIGRATIONS`; add a contract test (view references `corpscout.no_companies`, the 3 fields, `cityHash64(c.*_original)`, `argMax`).
- [ ] **Step 4: registry** — in `translator/registry.py`, change `norway_brreg`'s `ch_table` from `"corpscout.companies"` to `"corpscout.no_companies"`. Fields unchanged.
- [ ] **Step 5: scan test** — in `tests/test_translator_scan.py`, update the expected `FROM corpscout.companies AS c` assertions to `FROM corpscout.no_companies AS c`.
- [ ] **Step 6:** `uv run pytest tests/test_translator_scan.py tests/test_text_translations_schema.py tests/test_clickhouse_migrations.py -q` green; `uv run dg check defs`. Commit.

---

## Task 3 — Retarget the translation trigger to the resolved export + rewire the job

**Files:** `…/norway_brreg/assets.py`, `…/norway_brreg/definitions.py`, `tests/test_norway_brreg_definitions.py`

**Produces:** `norway_brreg_translation_trigger` depends on `norway_resolved_clickhouse` (so it fires after `no_companies` lands); the refresh job drives the full chain through the resolved export + trigger.

- [ ] **Step 1: trigger dep** — change the `norway_brreg_translation_trigger` asset's `deps` from `dg.AssetKey("norway_brreg_clickhouse_companies")` to `dg.AssetKey("norway_resolved_clickhouse")` (cross-module AssetKey; valid since definitions are merged globally).
- [ ] **Step 2: refresh job** — rewrite `norway_brreg_refresh_job` selection to drive the whole chain through the trigger:
```python
norway_brreg_refresh_job = dg.define_asset_job(
    "norway_brreg_refresh_job",
    selection=dg.AssetSelection.assets("norway_brreg_translation_trigger").upstream(),
)
```
`.upstream()` from the trigger pulls `norway_resolved_clickhouse` → the dbt `no_*` models → `norway_brreg_entities_duckdb` (+ financials feeding the dbt source). Confirm with `dg` that the planned set includes the dbt assets + entities. (If cross-module upstream resolution is incomplete, list the resolved-layer asset keys explicitly.)
- [ ] **Step 3: graph test** — update `tests/test_norway_brreg_definitions.py`: trigger parent is now `norway_resolved_clickhouse`; refresh-job membership reflects the resolved chain.
- [ ] **Step 4:** `uv run dg check defs` passes (no dangling keys); `uv run pytest tests/test_norway_brreg_definitions.py tests/test_translator_trigger.py -q` green. Commit.

> Open verify: confirm `norway_resolved` assets are discovered in the same code location (they are, via `load_from_defs_folder`) so the cross-module dep + job selection resolve. If `norway_resolved` needs its own schedule instead, split into a `norway_resolved` job + schedule and have the trigger live there — decide during execution after seeing the `dg` planned set.

---

## Task 4 — Remove the raw export assets + drop the orphaned tables

**Files:** `…/norway_brreg/assets.py`, `…/norway_brreg/definitions.py`, `…/norway_brreg/tables.py`, `clickhouse/migrations/000061_corpscout_drop_raw_norway_exports.{up,down}.sql`, `tests/test_clickhouse_migrations.py`, `tests/test_norway_brreg_*`

**Produces:** the `norway_brreg_clickhouse_companies` + `norway_brreg_clickhouse_financial_statements` assets and their export functions are gone; `corpscout.companies` + `corpscout.financial_statements` + the `norway_companies_translated` view are dropped.

- [ ] **Step 1:** remove the two export assets (`norway_brreg_clickhouse_companies`, `norway_brreg_clickhouse_financial_statements`) and their helper export functions from `assets.py`; remove them from `definitions.py`'s `assets=[…]`. Remove now-unused `export_norway_brreg_clickhouse_*` and the `*_EXPORT_COLUMNS` references (keep `COMPANIES_DDL`/`FINANCIAL_STATEMENTS` constants + `COMPANIES_COLUMNS` — pinned by 000003/000004 + the migrations DDL test).
- [ ] **Step 2:** delete the now-obsolete tests that exercised the raw companies/financials export (the export-row tests in `tests/test_norway_brreg_assets.py` that build rows from `COMPANIES_EXPORT_COLUMNS`/`FINANCIAL_STATEMENTS_EXPORT_COLUMNS`). Keep the entities/financials DuckDB + DDL-contract tests.
- [ ] **Step 3: migration 000061 up** — `CREATE DATABASE IF NOT EXISTS corpscout;` then:
```sql
DROP VIEW IF EXISTS corpscout.norway_companies_translated;
DROP TABLE IF EXISTS corpscout.companies;
DROP TABLE IF EXISTS corpscout.financial_statements;
```
- [ ] **Step 4: migration 000061 down** — recreate the tables (mirror `000003`/`000004` CREATEs, post-`000058` schema = `companies` minus the 3 free-text `_en`) and the `norway_companies_translated` view (best-effort inverse; forward-only in prod). Keep it correct enough for local down-migration.
- [ ] **Step 5:** append `"000061_corpscout_drop_raw_norway_exports"` to `EXPECTED_MIGRATIONS`; add a contract test asserting the DROPs. Update the `000057`/`000058` view assertions only if the down-migration text changed.
- [ ] **Step 6: full suite** — `uv run pytest -q` green; `uv run dg check defs`; `rg -n "corpscout\\.companies|QUALIFIED_COMPANIES_TABLE|norway_brreg_clickhouse_companies|norway_companies_translated" src clickhouse` shows only the historical `000003`/`000057`/`000058` migrations + `COMPANIES_DDL` mirror (no live asset/view references). Commit.

---

## Self-Review

**Spec coverage:** free-text carried into `no_companies` (Task 1); translator + view retargeted (Task 2); trigger + job rewired (Task 3); raw exports + tables dropped (Task 4). The `text_translations` cache is reused unchanged (no_companies is wipe-replaced → cache+view is correct).

**Sequencing safety:** columns + view + trigger land BEFORE the drop; `dg check` + tests gate every task, so the asset graph and migration ledger stay valid at each commit.

**Out of scope (flagged):** `legal_form_description` reference translation on `no_companies` (its NULL `_en`/provider/model columns) is a separate reference-data concern; `no_industries`/`no_financial_statements` translation untouched.

**Open during execution:** the cross-module refresh-job `.upstream()` resolution (Task 3) — verify the `dg` planned set pulls the resolved chain; if not, enumerate the resolved asset keys or give `norway_resolved` its own job+schedule with the trigger attached.
