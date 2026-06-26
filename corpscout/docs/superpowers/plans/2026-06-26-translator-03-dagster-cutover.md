# Translator Service — Plan 03: Dagster Cutover

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove translation from the Dagster asset graph (assets, sensor, job), replace it with a fire-and-forget Temporal trigger after the companies export, drop the 3 free-text `_en` columns from the ClickHouse `companies` table, and delete the three now-unused translation folders.

**Architecture:** `norway_brreg_clickhouse_companies` is repointed from `translations_applied` to `norway_brreg_entities_duckdb`, so companies land in the normal flow. A new `norway_brreg_translation_trigger` asset (downstream of companies) fires `TranslateSourceWorkflow` via Temporal `USE_EXISTING` and returns immediately. The 3 free-text `_en` columns leave both the ClickHouse export and the base table (migration 000058); they are supplied by the `norway_companies_translated` view (now without the `EXCEPT`). Finally `translations/`, `temporal/translations/`, and `src/dagster_v3/defs/translations/` are deleted, along with their pyproject scripts and packages.

**Tech Stack:** Dagster, Temporal (`temporalio`), ClickHouse SQL, `pytest`.

## Global Constraints

- Depends on **Plan 01** (ClickHouse table + view) and **Plan 02** (the `translator` package + worker).
- The migration ledger is forward-only; new migration is `000058`. Add it to `EXPECTED_MIGRATIONS`.
- **Do NOT edit** `COMPANIES_DDL` or `COMPANIES_COLUMNS` in `norway_brreg/tables.py` — `tests/test_clickhouse_migrations.py:438` pins `COMPANIES_DDL` to the existing `000003_norway_brreg_companies.up.sql`. The 3 free-text `_en` columns are removed from the **export** only (`COMPANIES_EXPORT_COLUMNS`) and from the **live** ClickHouse table via an `ALTER … DROP COLUMN` migration.
- The 3 free-text `_en` columns: `articles_purpose_en`, `activity_text_en`, `company_description_en`. The 4 reference `_en` columns (`legal_form_description_en`, `nace1/2/3_description_en`) stay everywhere.
- Trigger is fire-and-forget: start the workflow, return its run id, never `.result()`. Workflow id `translate-norway_brreg`, conflict policy `USE_EXISTING`, task queue `translation-local-llm`.
- Run tests with `uv run pytest` and validate definitions with `uv run dg check defs`, both from `corpscout/dagster_v3/`.
- **DuckDB resource pattern (already adopted):** Norway assets receive `norway_brreg_duckdb: DuckDBResource` and use `duckdb_database_path(...)` / `read_only_duckdb_connection(...)` / `export_norway_brreg_clickhouse_companies(duckdb_connection=...)` (helpers from `dagster_v3.defs.common.duckdb_resources` and `…clickhouse.resolved`). `definitions.py` wires a `resources={...}` dict via `duckdb_resource(<path>)`. This plan must **preserve** the `norway_brreg_duckdb` resource and only remove the translation-specific `norway_brreg_translation_queue_duckdb` resource + its `NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH` constant. The standalone translator worker (Plan 02) does **not** use this resource — it runs outside Dagster on plain `duckdb.connect`.
- Asset line numbers below are approximate (the DuckDB-resource refactor shifted them); act on the named symbol and confirm with the given `rg`.
- Commit by explicit path; never `git add -A`.

---

## File Structure

| File | Change |
|------|--------|
| `src/dagster_v3/defs/norway_brreg/assets.py` | Repoint companies dep; add trigger asset; remove 3 translation assets + completion job + seed/apply/config/describe helpers + their imports; rewrite refresh job |
| `src/dagster_v3/defs/norway_brreg/sensors.py` | Delete the completion sensor (file may become empty → delete) |
| `src/dagster_v3/defs/norway_brreg/definitions.py` | Update assets/jobs/sensors/imports |
| `src/dagster_v3/defs/norway_brreg/tables.py` | Exclude 3 free-text `_en` from `COMPANIES_EXPORT_COLUMNS` |
| `clickhouse/migrations/000058_corpscout_companies_drop_free_text_en.{up,down}.sql` | Drop columns + recreate view without `EXCEPT` |
| `tests/test_clickhouse_migrations.py` | Append `000058…` to `EXPECTED_MIGRATIONS` |
| `tests/test_text_translations_schema.py` | Add 000058 assertions |
| `tests/test_norway_brreg_assets.py` | Remove translation tests; fix export-row tests |
| `tests/test_translator_trigger.py` (create) | Trigger asset test |
| `translations/`, `temporal/translations/`, `src/dagster_v3/defs/translations/` | **Delete** |
| `pyproject.toml` | Remove old scripts + `translations`/`temporal` packages |

All paths under `corpscout/dagster_v3/` unless noted (migrations are under `corpscout/clickhouse/`).

---

## Task 1: Stop exporting the 3 free-text `_en`; repoint companies dep

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/tables.py`, `src/dagster_v3/defs/norway_brreg/assets.py:227`
- Test: `tests/test_norway_brreg_tables_export.py` (create), `tests/test_norway_brreg_assets.py` (fix existing)

**Interfaces:**
- Produces: `COMPANIES_EXPORT_COLUMNS` no longer contains `articles_purpose_en`, `activity_text_en`, `company_description_en` (but still contains the 4 reference `_en`). `norway_brreg_clickhouse_companies` now depends on `norway_brreg_entities_duckdb`.

- [ ] **Step 1: Write the failing export-columns test**

Create `tests/test_norway_brreg_tables_export.py`:

```python
from dagster_v3.defs.norway_brreg import tables


def test_free_text_en_columns_are_not_exported():
    for column in ("articles_purpose_en", "activity_text_en", "company_description_en"):
        assert column not in tables.COMPANIES_EXPORT_COLUMNS


def test_reference_en_columns_are_still_exported():
    for column in (
        "legal_form_description_en",
        "nace1_description_en",
        "nace2_description_en",
        "nace3_description_en",
    ):
        assert column in tables.COMPANIES_EXPORT_COLUMNS


def test_original_free_text_columns_are_still_exported():
    for column in (
        "articles_purpose_original",
        "activity_text_original",
        "company_description_original",
    ):
        assert column in tables.COMPANIES_EXPORT_COLUMNS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_norway_brreg_tables_export.py -q`
Expected: FAIL — `test_free_text_en_columns_are_not_exported` fails (they are currently exported).

- [ ] **Step 3: Add the export exclusion in `tables.py`**

In `src/dagster_v3/defs/norway_brreg/tables.py`, immediately after the `CLICKHOUSE_EXCLUDED_COLUMNS` definition (around line 244), add:

```python
# Free-text _en columns are supplied by the translator service via the
# corpscout.norway_companies_translated view (migration 000058 drops them from the
# base companies table). They are no longer exported from DuckDB. The 4 reference
# _en columns (legal_form/nace) stay — they are populated by reference-data joins.
COMPANIES_TRANSLATED_EN_COLUMNS = frozenset(
    {"articles_purpose_en", "activity_text_en", "company_description_en"}
)
```

Then change the `COMPANIES_EXPORT_COLUMNS` definition (line 251) from:

```python
COMPANIES_EXPORT_COLUMNS = _export_columns(COMPANIES_COLUMNS)
```

to:

```python
COMPANIES_EXPORT_COLUMNS = tuple(
    column
    for column in COMPANIES_COLUMNS
    if column not in (CLICKHOUSE_EXCLUDED_COLUMNS | COMPANIES_TRANSLATED_EN_COLUMNS)
)
```

(Leave `FINANCIAL_STATEMENTS_EXPORT_COLUMNS = _export_columns(...)` unchanged.)

- [ ] **Step 4: Repoint the companies export dependency**

In `src/dagster_v3/defs/norway_brreg/assets.py`, find the `@dg.asset` decorator for `norway_brreg_clickhouse_companies` (≈line 240, its `deps` currently `norway_brreg_translations_applied`) and change that one line:

```python
    deps=[dg.AssetKey("norway_brreg_translations_applied")],
```

to:

```python
    deps=[dg.AssetKey("norway_brreg_entities_duckdb")],
```

Leave the asset body unchanged — it already reads via the DuckDB resource (`read_only_duckdb_connection(norway_brreg_duckdb)` + `export_norway_brreg_clickhouse_companies(duckdb_connection=...)`), and the entities asset writes the table it reads, so the dep is satisfied. Confirm exactly one site changed:

Run: `rg -n 'deps=\[dg.AssetKey\("norway_brreg_translations_applied"\)\]' src/dagster_v3/defs/norway_brreg/assets.py`
Expected: no matches after the edit.

- [ ] **Step 5: Fix the existing export-row assertions**

In `tests/test_norway_brreg_assets.py`, the two export tests build `company_row = dict(zip(brreg_tables.COMPANIES_EXPORT_COLUMNS, ...))` and then assert on `company_row["company_description_en"]` (lines ~1175 and ~1227). Those keys no longer exist. Remove the now-invalid free-text `_en` assertions in both tests:

Delete any line of the form (in both export tests):

```python
    assert company_row["company_description_en"] == ""
    assert company_row["articles_purpose_en"] == ""
    assert company_row["activity_text_en"] == ""
```

Verify none remain:

Run: `rg -n 'company_row\["(articles_purpose_en|activity_text_en|company_description_en)"\]' tests/test_norway_brreg_assets.py`
Expected: no matches.

- [ ] **Step 6: Run the relevant tests to verify they pass**

Run: `uv run pytest tests/test_norway_brreg_tables_export.py tests/test_norway_brreg_assets.py -q`
Expected: PASS. (If a test in `test_norway_brreg_assets.py` exercises the removed translation seed/apply, it is removed in Task 4 — if it fails here on `seed_norway_brreg_translation_queue`, defer that file's full green to Task 4 and only confirm the export tests here with `-k export`.)

- [ ] **Step 7: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/tables.py \
        corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py \
        corpscout/dagster_v3/tests/test_norway_brreg_tables_export.py \
        corpscout/dagster_v3/tests/test_norway_brreg_assets.py
git commit -m "feat(norway): stop exporting free-text _en; companies depends on entities"
```

---

## Task 2: Migration 000058 — drop free-text `_en`, recreate the view without `EXCEPT`

**Files:**
- Create: `clickhouse/migrations/000058_corpscout_companies_drop_free_text_en.up.sql` + `.down.sql`
- Modify: `tests/test_clickhouse_migrations.py`, `tests/test_text_translations_schema.py`

**Interfaces:**
- Produces: ClickHouse `corpscout.companies` without the 3 free-text `_en` columns; `corpscout.norway_companies_translated` redefined to select `c.*` plus the 3 joined `_en` (no `EXCEPT`, since the base columns are gone).

- [ ] **Step 1: Add the failing migration assertions**

Append to `tests/test_text_translations_schema.py`:

```python
DROP_EN_UP = MIGRATIONS_DIR / "000058_corpscout_companies_drop_free_text_en.up.sql"
DROP_EN_DOWN = MIGRATIONS_DIR / "000058_corpscout_companies_drop_free_text_en.down.sql"


def test_drop_free_text_en_up_drops_three_columns_and_rebuilds_view():
    sql = DROP_EN_UP.read_text(encoding="utf-8")
    for col in ("articles_purpose_en", "activity_text_en", "company_description_en"):
        assert f"DROP COLUMN IF EXISTS {col}" in sql
    # View is recreated WITHOUT the EXCEPT clause (base no longer has those columns).
    assert "CREATE OR REPLACE VIEW corpscout.norway_companies_translated" in sql
    assert "EXCEPT (" not in sql
    assert "cityHash64(c.company_description_original)" in sql


def test_drop_free_text_en_down_readds_columns():
    sql = DROP_EN_DOWN.read_text(encoding="utf-8")
    for col in ("articles_purpose_en", "activity_text_en", "company_description_en"):
        assert f"ADD COLUMN IF NOT EXISTS {col} String" in sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_text_translations_schema.py -q -k drop_free_text`
Expected: FAIL — `FileNotFoundError` for `000058_*.up.sql`.

- [ ] **Step 3: Create the up migration**

Create `clickhouse/migrations/000058_corpscout_companies_drop_free_text_en.up.sql`:

```sql
CREATE OR REPLACE VIEW corpscout.norway_companies_translated AS
SELECT
    c.*,
    ifNull(ap.translated_text, '') AS articles_purpose_en,
    ifNull(act.translated_text, '') AS activity_text_en,
    ifNull(cd.translated_text, '') AS company_description_en
FROM corpscout.companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'articles_purpose'
    GROUP BY source_text_hash
) AS ap ON ap.source_text_hash = cityHash64(c.articles_purpose_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'activity_text'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(c.activity_text_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'company_description'
    GROUP BY source_text_hash
) AS cd ON cd.source_text_hash = cityHash64(c.company_description_original);

ALTER TABLE corpscout.companies DROP COLUMN IF EXISTS articles_purpose_en;
ALTER TABLE corpscout.companies DROP COLUMN IF EXISTS activity_text_en;
ALTER TABLE corpscout.companies DROP COLUMN IF EXISTS company_description_en;
```

(The view is recreated *before* dropping the columns so it never references columns that no longer exist.)

- [ ] **Step 4: Create the down migration**

Create `clickhouse/migrations/000058_corpscout_companies_drop_free_text_en.down.sql`:

```sql
ALTER TABLE corpscout.companies ADD COLUMN IF NOT EXISTS articles_purpose_en String;
ALTER TABLE corpscout.companies ADD COLUMN IF NOT EXISTS activity_text_en String;
ALTER TABLE corpscout.companies ADD COLUMN IF NOT EXISTS company_description_en String;

CREATE OR REPLACE VIEW corpscout.norway_companies_translated AS
SELECT
    c.* EXCEPT (articles_purpose_en, activity_text_en, company_description_en),
    ifNull(ap.translated_text, '') AS articles_purpose_en,
    ifNull(act.translated_text, '') AS activity_text_en,
    ifNull(cd.translated_text, '') AS company_description_en
FROM corpscout.companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'articles_purpose'
    GROUP BY source_text_hash
) AS ap ON ap.source_text_hash = cityHash64(c.articles_purpose_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'activity_text'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(c.activity_text_original)
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_slug = 'norway_brreg' AND field = 'company_description'
    GROUP BY source_text_hash
) AS cd ON cd.source_text_hash = cityHash64(c.company_description_original);
```

- [ ] **Step 5: Append to `EXPECTED_MIGRATIONS`**

In `tests/test_clickhouse_migrations.py`, add after the `000057` entry:

```python
    "000057_corpscout_norway_companies_translated_view",
    "000058_corpscout_companies_drop_free_text_en",
```

- [ ] **Step 6: Run migration tests to verify they pass**

Run: `uv run pytest tests/test_text_translations_schema.py tests/test_clickhouse_migrations.py -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add corpscout/clickhouse/migrations/000058_corpscout_companies_drop_free_text_en.up.sql \
        corpscout/clickhouse/migrations/000058_corpscout_companies_drop_free_text_en.down.sql \
        corpscout/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/dagster_v3/tests/test_text_translations_schema.py
git commit -m "feat(clickhouse): drop free-text _en from companies, simplify view (000058)"
```

---

## Task 3: Fire-and-forget translation trigger asset

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py` (add the trigger asset)
- Test: `tests/test_translator_trigger.py`

**Interfaces:**
- Consumes: `TranslateSourceWorkflow`, `TranslateSourceWorkflowInput` (`translator.workflow`); `LOCAL_LLM_TRANSLATION_TASK_QUEUE` (`translator.activities`).
- Produces: `NORWAY_BRREG_TRANSLATE_WORKFLOW_ID = "translate-norway_brreg"`; `build_norway_brreg_translate_input() -> TranslateSourceWorkflowInput`; asset `norway_brreg_translation_trigger` (deps `norway_brreg_clickhouse_companies`).

- [ ] **Step 1: Write the failing test**

Create `tests/test_translator_trigger.py`:

```python
from dagster_v3.defs.norway_brreg.assets import (
    NORWAY_BRREG_TRANSLATE_WORKFLOW_ID,
    build_norway_brreg_translate_input,
)


def test_translate_workflow_id_is_stable():
    assert NORWAY_BRREG_TRANSLATE_WORKFLOW_ID == "translate-norway_brreg"


def test_build_translate_input_targets_norway_brreg():
    params = build_norway_brreg_translate_input()
    assert params.source_slug == "norway_brreg"
    assert params.batch_size == 50
    assert params.max_batch_failures == 0
    assert params.queue_dir == "data/translator"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_translator_trigger.py -q`
Expected: FAIL — `ImportError: cannot import name 'NORWAY_BRREG_TRANSLATE_WORKFLOW_ID'`.

- [ ] **Step 3: Add the trigger asset to `assets.py`**

Add near the top imports of `src/dagster_v3/defs/norway_brreg/assets.py`:

```python
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from translator.activities import LOCAL_LLM_TRANSLATION_TASK_QUEUE
from translator.workflow import TranslateSourceWorkflow, TranslateSourceWorkflowInput
```

Add this block (place it just before the `norway_brreg_refresh_job` definition):

```python
NORWAY_BRREG_TRANSLATE_WORKFLOW_ID = "translate-norway_brreg"


def build_norway_brreg_translate_input() -> TranslateSourceWorkflowInput:
    return TranslateSourceWorkflowInput(
        source_slug="norway_brreg",
        queue_dir="data/translator",
        batch_size=50,
        timeout_seconds=120,
        max_batch_failures=0,
        worker_id="translation-temporal-worker",
        max_tokens=2048,
        extra_body_json='{"chat_template_kwargs": {"enable_thinking": false}}',
        initialize_timeout_seconds=60,
        batch_timeout_buffer_seconds=30,
        summarize_timeout_seconds=60,
        activity_maximum_attempts=3,
        lease_timeout_seconds=600,
        scan_timeout_seconds=300,
        flush_timeout_seconds=300,
    )


async def _start_norway_brreg_translation(temporal_address: str) -> str:
    client = await Client.connect(temporal_address)
    handle = await client.start_workflow(
        TranslateSourceWorkflow.run,
        build_norway_brreg_translate_input(),
        id=NORWAY_BRREG_TRANSLATE_WORKFLOW_ID,
        task_queue=LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return handle.result_run_id


@dg.asset(
    deps=[dg.AssetKey("norway_brreg_clickhouse_companies")],
    group_name=GROUP_NAME,
    kinds={"python", "temporal"},
    description=(
        "Fire-and-forget: start (or reuse) the standalone translator Temporal "
        "workflow after companies land in ClickHouse. Does not wait for completion."
    ),
)
def norway_brreg_translation_trigger(context: AssetExecutionContext) -> dg.MaterializeResult:
    import asyncio
    import os

    address = os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    run_id = asyncio.run(_start_norway_brreg_translation(address))
    context.log.info(
        "Started Norway Brreg translation workflow: workflow_id=%s run_id=%s",
        NORWAY_BRREG_TRANSLATE_WORKFLOW_ID,
        run_id,
    )
    return dg.MaterializeResult(
        metadata={
            "workflow_id": NORWAY_BRREG_TRANSLATE_WORKFLOW_ID,
            "workflow_run_id": run_id,
            "task_queue": LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        }
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_translator_trigger.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py \
        corpscout/dagster_v3/tests/test_translator_trigger.py
git commit -m "feat(norway): fire-and-forget translation trigger asset"
```

---

## Task 4: Remove the in-graph translation assets, sensor, and job

**Files:**
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`, `sensors.py`, `definitions.py`
- Modify: `tests/test_norway_brreg_assets.py` (remove translation tests)

**Interfaces:**
- Removes: assets `norway_brreg_translation_queue`, `norway_brreg_translation_workflow_status`, `norway_brreg_translations_applied`; `norway_brreg_translation_completion_job`; `norway_brreg_translation_completion_sensor`; helpers `seed_norway_brreg_translation_queue`, `apply_norway_brreg_translation_queue_results`, `describe_norway_brreg_translation_workflow` (+ `_describe…`), `norway_brreg_translation_queue_status_metadata`, `norway_brreg_translation_workflow_status_metadata`, `log_norway_brreg_translation_workflow_status`, `NorwayBrregTranslationConfig`, the `NORWAY_BRREG_TRANSLATION_*` constants (including `NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH`), the `NORWAY_BRREG_LLM_TRANSLATION_FIELDS` / `NORWAY_BRREG_EN_FIELD_BY_ORIGINAL_FIELD` constants, and the **`norway_brreg_translation_queue_duckdb` resource** wiring in `definitions.py`. Removes the `from dagster_v3.defs.translations.assets import …`, `from temporal.translations.queue import …`, and `from translations.* import …` imports in `assets.py`. The `norway_brreg_translation_queue` asset was the only consumer of the `norway_brreg_translation_queue_duckdb` resource — so that resource and its path constant are now dead.
- Produces: `norway_brreg_refresh_job` selection covering financials + companies + trigger.

- [ ] **Step 1: Delete the translation tests first (so the suite reflects the new surface)**

In `tests/test_norway_brreg_assets.py`, delete every test that exercises translation seeding/applying/workflow-status. Identify them:

Run: `rg -n "def test_.*translation|seed_norway_brreg_translation_queue|apply_norway_brreg_translation_queue_results|translations_applied|translation_queue|translation_workflow_status" tests/test_norway_brreg_assets.py`

Delete each matching `def test_…(): …` function body in full (the ones referencing the removed symbols). Keep tests that only touch `COMPANIES_COLUMNS`/`COMPANIES_DDL`/entities/financials/export.

- [ ] **Step 2: Remove the assets, job, helpers, and imports from `assets.py`**

Delete from `src/dagster_v3/defs/norway_brreg/assets.py`:
- the asset functions `norway_brreg_translation_queue` (≈305–399), `norway_brreg_translation_workflow_status` (≈402–435), `norway_brreg_translations_applied` (≈438–483);
- `norway_brreg_translation_completion_job` (≈486–493);
- the helpers `describe_norway_brreg_translation_workflow` / `_describe_norway_brreg_translation_workflow`, `seed_norway_brreg_translation_queue`, `apply_norway_brreg_translation_queue_results`, `norway_brreg_translation_queue_status_metadata`, `norway_brreg_translation_workflow_status_metadata`, `log_norway_brreg_translation_workflow_status`, the `NorwayBrregTranslationConfig` config class, and the `NORWAY_BRREG_TRANSLATION_*` / `NORWAY_BRREG_LLM_TRANSLATION_FIELDS` / `NORWAY_BRREG_EN_FIELD_BY_ORIGINAL_FIELD` constants;
- the imports `from dagster_v3.defs.translations.assets import (TemporalClient, start_translation_workflow)` and `from temporal.translations.queue import (...)` and any `from translations.* import (...)` used only by the removed code.

Then rewrite the refresh job (≈502–505) so companies + trigger run in the normal flow:

```python
norway_brreg_refresh_job = dg.define_asset_job(
    "norway_brreg_refresh_job",
    selection=dg.AssetSelection.assets(
        "norway_brreg_clickhouse_financial_statements",
        "norway_brreg_translation_trigger",
    ).upstream(),
)
```

(`norway_brreg_translation_trigger.upstream()` pulls the trigger → companies → entities; the financials leaf pulls the financials chain. Their union is the whole graph.)

- [ ] **Step 3: Delete the completion sensor**

`src/dagster_v3/defs/norway_brreg/sensors.py` exists only for the completion sensor. Delete the file:

```bash
git rm corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/sensors.py
```

- [ ] **Step 4: Update `definitions.py`**

Replace `src/dagster_v3/defs/norway_brreg/definitions.py` with the version below. **Note the `resources` dict is preserved** — only the `norway_brreg_translation_queue_duckdb` resource and the `NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH` import are dropped; `norway_brreg_duckdb` and the `duckdb_resource` import stay (every Norway asset still needs that resource):

```python
import dagster as dg

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.norway_brreg.assets import (
    NORWAY_BRREG_DUCKDB_PATH,
    norway_brreg_clickhouse_companies,
    norway_brreg_clickhouse_financial_statements,
    norway_brreg_entities_duckdb_asset,
    norway_brreg_financial_fetches_duckdb_asset,
    norway_brreg_financial_statements_duckdb_asset,
    norway_brreg_refresh_job,
    norway_brreg_refresh_schedule,
    norway_brreg_translation_trigger,
)


defs = dg.Definitions(
    assets=[
        norway_brreg_entities_duckdb_asset,
        norway_brreg_financial_fetches_duckdb_asset,
        norway_brreg_financial_statements_duckdb_asset,
        norway_brreg_clickhouse_companies,
        norway_brreg_clickhouse_financial_statements,
        norway_brreg_translation_trigger,
    ],
    jobs=[norway_brreg_refresh_job],
    schedules=[norway_brreg_refresh_schedule],
    resources={
        "norway_brreg_duckdb": duckdb_resource(NORWAY_BRREG_DUCKDB_PATH),
    },
)
```

- [ ] **Step 5: Verify no dangling references and definitions load**

Run: `rg -ni "translation_queue|translations_applied|translation_workflow_status|translation_completion|start_translation_workflow|NorwayBrregTranslationConfig" src/dagster_v3/defs/norway_brreg/`
Expected: no matches (the `-i` also catches `NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH` and the `norway_brreg_translation_queue_duckdb` resource).

Run: `uv run dg check defs`
Expected: PASS (definitions load; no missing asset keys).

- [ ] **Step 6: Run the Norway + trigger tests**

Run: `uv run pytest tests/test_norway_brreg_assets.py tests/test_translator_trigger.py tests/test_norway_brreg_tables_export.py -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/assets.py \
        corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/definitions.py \
        corpscout/dagster_v3/src/dagster_v3/defs/norway_brreg/sensors.py \
        corpscout/dagster_v3/tests/test_norway_brreg_assets.py
git commit -m "refactor(norway): remove in-graph translation assets/sensor/job"
```

---

## Task 5: Delete the old translation folders + pyproject cleanup

**Files:**
- Delete: `translations/`, `temporal/translations/`, `src/dagster_v3/defs/translations/`
- Modify: `corpscout/dagster_v3/pyproject.toml`
- Delete: any tests that import the deleted top-level packages

**Interfaces:**
- Produces: a tree where translation lives only in `translator/`.

- [ ] **Step 1: Find every remaining importer of the old packages**

Run: `rg -n "^(from|import) (translations|temporal\.translations)\b|dagster_v3\.defs\.translations" --glob '!translator/**'`
Expected: the only hits are test files that test the old smoke/queue modules (e.g. `tests/test_translation_*`, `tests/test_*queue*`) — note them; they get deleted with the modules they covered (the equivalents are re-covered under `translator/` in Plan 02).

- [ ] **Step 2: Delete the old packages and their now-orphaned tests**

```bash
git rm -r corpscout/dagster_v3/translations \
          corpscout/dagster_v3/temporal/translations \
          corpscout/dagster_v3/src/dagster_v3/defs/translations
# delete tests that import the removed top-level modules (from Step 1's list), e.g.:
git rm corpscout/dagster_v3/tests/test_translation_queue.py 2>/dev/null || true
```

If `temporal/` is now empty (no other subpackages), remove it too:

```bash
rmdir corpscout/dagster_v3/temporal 2>/dev/null || true
```

- [ ] **Step 3: Clean up `pyproject.toml`**

In `corpscout/dagster_v3/pyproject.toml`:
- Remove the old console scripts under `[project.scripts]`: `translation-provider-smoke`, `translation-queue-smoke`, `translation-temporal-worker`, `translation-temporal-start`, `translation-temporal-status`. Keep `translator-worker`.
- Update the wheel packages, removing `translations` and `temporal` (keep `translator`):

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/dagster_v3", "exchange_rates", "translator"]
force-include = { "pyproject.toml" = "pyproject.toml" }
```

- [ ] **Step 4: Verify nothing references the removed packages and everything loads**

Run: `rg -n "^(from|import) (translations|temporal\.translations)\b|dagster_v3\.defs\.translations" || echo CLEAN`
Expected: `CLEAN`.

Run: `uv run dg check defs`
Expected: PASS.

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest -q`
Expected: PASS (all green; translation coverage now lives under `translator/`).

- [ ] **Step 6: Commit**

```bash
git add -u corpscout/dagster_v3
git add corpscout/dagster_v3/pyproject.toml
git commit -m "chore(translator): delete legacy translations/ + temporal/translations/ packages"
```

> Note: `git add -u corpscout/dagster_v3` here stages only tracked deletions/modifications under that path (the `git rm` removals). Confirm `git status` shows nothing unrelated before committing.

---

## Self-Review

**Spec coverage (this plan's slice):** removals (assets/sensor/job/module — Decision: "Changes to Dagster / Remove") ✓ Task 4; fire-and-forget trigger by Temporal `USE_EXISTING` (build #4, Decisions 4,5) ✓ Task 3; drop free-text `_en` from export + base, view supplies them (Decision 9 + scoping note) ✓ Tasks 1,2; delete the three legacy folders (consolidation choice) ✓ Task 5. End-to-end: ingest → companies export (no free-text `_en`) → trigger → translator worker fills `text_translations` → `norway_companies_translated` view surfaces `_en`.

**Placeholder scan:** the removal steps (Task 4 Steps 1–2, Task 5 Steps 1–2) are deletions verified by an exact `rg` returning no matches + `uv run dg check defs` + `uv run pytest` passing — not placeholders (you cannot reproduce "delete these functions" as code, and the verification is concrete). Every additive step (trigger asset, migration, tables exclusion, definitions.py) is full code.

**Type consistency:** the trigger imports `TranslateSourceWorkflow`/`TranslateSourceWorkflowInput` from `translator.workflow` and `LOCAL_LLM_TRANSLATION_TASK_QUEUE` from `translator.activities` — the exact names produced by Plan 02. `build_norway_brreg_translate_input()` fills every field of `TranslateSourceWorkflowInput` defined in Plan 02 Task 6 (15 fields), in order. `COMPANIES_TRANSLATED_EN_COLUMNS` is referenced only within `tables.py`. The 000058 view column aliases (`articles_purpose_en`, `activity_text_en`, `company_description_en`) match the originals joined on (`*_original`).

**Cross-plan ordering guard:** Task 1 repoints companies→entities and Task 3 adds the trigger *before* Task 4 deletes `translations_applied`, so the asset graph is always resolvable; `uv run dg check defs` is run in Tasks 4 and 5 to catch any dangling key.
