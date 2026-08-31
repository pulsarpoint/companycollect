# ESEF Enrichment Serving Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote `customer_markets_json`, `operating_geographies_json`, and `material_group_relationships_json` from the extraction table `esef_document_company_information` into the SE serving table `se_company_info_esef` and onto the admin SE Info tab, following the existing products/segments pattern.

**Architecture:** The serving table is filled by asset `se_company_info_esef_clickhouse` (`src/dagster_v3/defs/se_company/esef.py:77-118`) via an append-only, `new_versions_only` publish (LEFT ANTI JOIN on the `(company_id, source_record_uid)` ordering keys). The backoffice column list is **single-sourced**: adding a key to `ARTIFACT_PAYLOAD_FIELDS.esef` in `app/lib/se-company-info-payload.ts` automatically extends both the generated SQL (`payloadMapSql`, `se-company-info.server.ts:177-182`) and the review-page rendering (`PayloadValue` json-list branch). Three legs: (1) additive ClickHouse migration + one-off backfill for already-published rows (the anti-join will never re-emit them), (2) dagster column/SQL extension, (3) backoffice field-list extension. All three source columns already exist upstream and are populated by the LLM enrichment (`esef_filings/llm_enrichment_assets.py:934-939`).

**Tech Stack:** ClickHouse (golang-migrate ledger), Python/Dagster, TypeScript/React Router, pytest + vitest.

**Spec:** Owner decision 2026-08-31: "Promote all three." Evidence: for company 5020077862, `esef_document_company_information` carries customer markets (Corporate customers, Private individuals), operating geographies (SE/UK/NO/NL/FI), and group relationships — none of which reach `se_company_info_esef` (only description + products + segments, `000297_corpscout_se_company_info.up.sql:36-61`).

## Global Constraints

- **Migration number:** two other 2026-08-31 plans also allocate migrations. Always take `max(existing in corpscout/clickhouse/migrations)+1` at execution time; this plan calls it `0003XX_corpscout_se_company_info_esef_enrichment` below — substitute the real number everywhere.
- **evidence_hash stays unchanged.** It is `MATERIALIZED` over a `'se-company-info-esef-v1'`-prefixed concat (`000297:41-48`) that does not include the new columns. Changing a MATERIALIZED expression rewrites semantics for every existing row and would break stored-hash comparisons; the new fields ride outside the hash. If the owner later wants them hashed, that is a deliberate `-v2` follow-up, not this plan.
- Dagster tests: `tests/test_se_company_esef.py` derives the expected column list by **parsing the real migration DDL** (`tests/se_company_ddl.py::declared_columns`). Verify that helper folds `ALTER TABLE ... ADD COLUMN` statements in; if it only reads CREATE TABLE, extend it as part of Task 2.
- Backoffice tests: `tests/se-company-info.server.test.ts:50-60` keeps `DDL_PAYLOAD_COLUMNS` as a deliberate hand-copy of the DDL and asserts display-list ≡ DDL-list plus SQL fragments per column.
- Conventional Commits; do not commit the pre-existing unrelated working-tree changes.

## File Structure

- Create: `corpscout/clickhouse/migrations/0003XX_corpscout_se_company_info_esef_enrichment.up.sql` / `.down.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (ledger + content test)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/esef.py` (columns + SQL)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/info.py:129` (`UNREAD_ARTIFACT_COLUMNS`)
- Modify: `corpscout/services/backoffice/app/lib/se-company-info-payload.ts:85-107`
- Modify: `corpscout/services/backoffice/tests/se-company-info.server.test.ts`, `tests/admin-se-company-info.test.tsx`

---

### Task 0: Sample the group-relationships JSON shape

- [ ] **Step 1:** On prod ClickHouse:

```sql
SELECT material_group_relationships_json
FROM corpscout.esef_document_company_information
WHERE material_group_relationships_json NOT IN ('', '[]')
LIMIT 3;
```

- [ ] **Step 2:** `parseJsonList` (`se-company-info-payload.ts:194-215`) titles items from `ITEM_NAME_KEYS = ["name","label","title"]` (`:162`). If the sampled items key their display name differently (e.g. `related_company_name`), add that key to `ITEM_NAME_KEYS` in Task 4 and pin it in the test. Record the observed shape here before proceeding.

### Task 1: Migration + backfill of already-published rows

**Files:**
- Create: `corpscout/clickhouse/migrations/0003XX_corpscout_se_company_info_esef_enrichment.up.sql` / `.down.sql`
- Modify: `tests/test_clickhouse_migrations.py`

- [ ] **Step 1: Ledger test first** — append the name to `EXPECTED_MIGRATIONS` and add:

```python
def test_se_company_info_esef_enrichment_migration_is_additive() -> None:
    up = (MIGRATIONS_DIR / "0003XX_corpscout_se_company_info_esef_enrichment.up.sql").read_text()
    down = (MIGRATIONS_DIR / "0003XX_corpscout_se_company_info_esef_enrichment.down.sql").read_text()
    for column in (
        "customer_markets_json",
        "operating_geographies_json",
        "material_group_relationships_json",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column} String DEFAULT ''" in up
        assert f"DROP COLUMN IF EXISTS {column}" in down
    assert "MATERIALIZED" not in up  # evidence_hash untouched
    assert "TRUNCATE" not in up
```

Run: FAIL (explicit-listing test), then write the files.

- [ ] **Step 2: `up.sql`:**

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Additive serving columns; deliberately OUTSIDE the v1 evidence_hash concat
-- (changing a MATERIALIZED expression would rewrite provenance for all rows).
ALTER TABLE corpscout.se_company_info_esef
    ADD COLUMN IF NOT EXISTS customer_markets_json String DEFAULT '' AFTER products_and_services_json,
    ADD COLUMN IF NOT EXISTS operating_geographies_json String DEFAULT '' AFTER customer_markets_json,
    ADD COLUMN IF NOT EXISTS material_group_relationships_json String DEFAULT '' AFTER business_segments_json;
```

`down.sql`: the three `DROP COLUMN IF EXISTS` in one ALTER.

- [ ] **Step 3: One-off backfill (operations doc, not the ledger).** The publish is `new_versions_only` — already-exported `(company_id, source_record_uid)` pairs are anti-joined away, so their new columns would stay `''` forever. Add `corpscout/clickhouse/operations/2026-08-31-se-company-info-esef-enrichment-backfill.sql`:

```sql
-- Run once after the 0003XX migration and the dagster deploy. Inserts a
-- fresh version of every already-served row with the three new fields
-- populated; ReplacingMergeTree(observed_at) with equal versions keeps the
-- last-inserted row, and all serving reads use FINAL.
INSERT INTO corpscout.se_company_info_esef
    (company_id, source_record_uid, observed_at, source_run_id,
     source_document_id, lei, entity_name, fiscal_year, company_description,
     description_language, description_confidence,
     products_and_services_json, customer_markets_json,
     operating_geographies_json, business_segments_json,
     material_group_relationships_json)
SELECT
    served.company_id, served.source_record_uid, served.observed_at,
    served.source_run_id, served.source_document_id, served.lei,
    served.entity_name, served.fiscal_year, served.company_description,
    served.description_language, served.description_confidence,
    served.products_and_services_json,
    info.customer_markets_json,
    info.operating_geographies_json,
    served.business_segments_json,
    info.material_group_relationships_json
FROM corpscout.se_company_info_esef AS served FINAL
INNER JOIN corpscout.esef_document_company_information AS info FINAL
    ON info.company_id = served.company_id
   AND info.source_record_uid = served.source_record_uid
WHERE served.customer_markets_json = ''
  AND served.operating_geographies_json = ''
  AND served.material_group_relationships_json = '';
```

(Note the INSERT column order must interleave exactly as the table's post-ALTER physical order; verify with `DESCRIBE` before running.)

- [ ] **Step 4:** `uv run pytest tests/test_clickhouse_migrations.py -q` → PASS. Commit: `git commit -m "feat(clickhouse): promote esef enrichment fields to se_company_info_esef"`.

### Task 2: Dagster export extension

**Files:**
- Modify: `src/dagster_v3/defs/se_company/esef.py:30-74`
- Modify: `src/dagster_v3/defs/se_company/info.py:129`
- Modify: `tests/test_se_company_esef.py` (only if `declared_columns` needs the ALTER-folding extension; the column assertions themselves auto-adapt)

**Interfaces:**
- Produces: `SE_COMPANY_INFO_ESEF_COLUMNS` gains, in physical table order, `customer_markets_json` (after `products_and_services_json`), `operating_geographies_json`, then `business_segments_json`, then `material_group_relationships_json` last.

- [ ] **Step 1:** Run `uv run pytest tests/test_se_company_esef.py -q` — after Task 1 the DDL-derived expectation should FAIL against the unchanged code (if it still passes, `declared_columns` ignores ALTERs: extend `tests/se_company_ddl.py` to fold `ALTER TABLE ... ADD COLUMN` into the declared set first, and add a regression test for the helper).
- [ ] **Step 2:** In `esef.py`: insert the three names into `SE_COMPANY_INFO_ESEF_COLUMNS` (`:30-44`) at the positions matching the post-ALTER physical order, and add the matching projections to BOTH the `candidates` CTE (`:59-60` region — `info.customer_markets_json`, `info.operating_geographies_json`, `info.material_group_relationships_json`) and the outer SELECT (`:72-73` region). The invalid-condition and `LIMIT 1 BY` clauses are untouched.
- [ ] **Step 3:** In `info.py:129`, extend `UNREAD_ARTIFACT_COLUMNS["esef"]` with the three columns (they stay out of the LLM read contract like the existing JSON blobs).
- [ ] **Step 4:** `uv run pytest tests/test_se_company_esef.py tests/test_se_company_info.py tests/test_se_company_layout.py -q` → PASS. Commit: `git commit -m "feat(se): export esef customer markets, geographies, group relationships to serving"`.

### Task 3: Backoffice field list

**Files:**
- Modify: `corpscout/services/backoffice/app/lib/se-company-info-payload.ts:85-107` (+ `:162` if Task 0 requires)
- Modify: `tests/se-company-info.server.test.ts:50-60`, `tests/admin-se-company-info.test.tsx:110-128,418-439`

- [ ] **Step 1: Tests first** (backoffice dir, `npx vitest run`):
  - Add the three columns to `DDL_PAYLOAD_COLUMNS.esef` (order as in the table).
  - Extend the admin-info fixture payload with e.g. `customer_markets_json: '[{"name":"Corporate customers","confidence":0.9,"evidence_ids":["E0007"]}]'` (same shape for the other two) and assert the rendered labels `"Customer markets"`, `"Operating geographies"`, `"Group relationships"` and an `<li>Corporate customers` item.
  - Run → FAIL (display list ≠ DDL list).
- [ ] **Step 2:** Append to `ARTIFACT_PAYLOAD_FIELDS.esef`:

```ts
  {
    key: "customer_markets_json",
    label: "Customer markets",
    kind: "json-list",
  },
  {
    key: "operating_geographies_json",
    label: "Operating geographies",
    kind: "json-list",
  },
  {
    key: "material_group_relationships_json",
    label: "Group relationships",
    kind: "json-list",
  },
```

(plus the `ITEM_NAME_KEYS` addition if Task 0 found a non-`name` key). No SQL or component edits — both derive from this list.

- [ ] **Step 3:** `npx vitest run tests/se-company-info.server.test.ts tests/admin-se-company-info.test.tsx && npm run typecheck` → PASS/clean. Commit: `git commit -m "feat(se): show esef customer markets, geographies, group relationships on the info tab"`.

### Task 4: Deploy + verify

- [ ] **Step 1:** Apply the migration (`make clickhouse-migrate-up` from `corpscout/` against prod), deploy dagster_v3 (pristine-worktree recipe if the tree is dirty), run the Task 1 Step 3 backfill INSERT, then `OPTIMIZE TABLE corpscout.se_company_info_esef FINAL` is NOT required (reads use FINAL) but is acceptable to compact.
- [ ] **Step 2:** Verify data:

```sql
SELECT company_id, customer_markets_json, operating_geographies_json
FROM corpscout.se_company_info_esef FINAL
WHERE company_id = '5020077862';
-- Expect non-empty markets + geographies for Handelsbanken
SELECT countIf(customer_markets_json != '') AS with_markets, count() AS rows
FROM corpscout.se_company_info_esef FINAL;
```

- [ ] **Step 3:** Verify UI: `http://localhost:5183/admin/se/company/5020077862/info` → ESEF source card shows the three new sections. Future documents flow through the extended asset automatically (`se_company_info_job`, weekly schedule currently STOPPED — materialize manually or per the backfill-completion plan's steady-state task).
- [ ] **Step 4:** Report before/after in the session summary.

## Self-Review

- All three fields promoted end-to-end: DDL → backfill for anti-joined rows → asset SQL → payload field list → UI (generic renderer). ✔
- evidence_hash decision made explicit and conservative. ✔
- The `new_versions_only` re-publish trap is handled by the operations backfill, with the ReplacingMergeTree equal-version caveat documented. ✔
- Migration-number collision across the three same-day plans handled by the `max+1` rule. ✔
