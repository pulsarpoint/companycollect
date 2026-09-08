# SE Basic Info Slice 6: `economic_activity` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the three-valued `economic_activity` field (SCB Företagsstatus 1/0/9 -> active/never/ceased) to the basic-info entity end to end: DDL, column tuples, fold, precedence, the SCB extractor, the Info tab, the edit sheet and the public SE shell.

**Architecture:** The entity's per-field pattern, nothing new: one ALTER-only migration, the column tuples in `tables.py` drive the fold, the loaders and the LLM extractor; the SQL extractors add one aliased column each; the backoffice adds one field entry, one enum and its select. `NON_NULLABLE_FIELDS` generalises the fold's `status` special case.

**Tech Stack:** Python 3.14 / Dagster / pytest (`uv run pytest`), clickhouse-local (docker) for the extractor integration test, TypeScript / React Router / vitest (`npx vitest run`, `npm run typecheck`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-08-se-basic-info-6-economic-activity-design.md`

## Global Constraints

- **Repo root:** `/Users/graovic/pulsarpoint/ppoint/companycollect`. Dagster commands from `corpscout/services/dagster_v3` with `uv run`; backoffice commands from `corpscout/services/backoffice`. Always `cd` with absolute paths (the shell's cwd drifts).
- **Branch:** `se-basic-info-6-economic-activity` from main. Commit by explicit path only.
- **Definitions-loading tests** need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`.
- **Migration number:** 000394 (000393 is the address rename, committed, unapplied). Register it in `EXPECTED_MIGRATIONS`. Comments in migration files must not contain `;`.
- **ALTER format** (replayed by `tests/se_company_ddl.py`): one clause per line, `ADD COLUMN <name> <type> AFTER <column>,` with `AFTER` last on the line.
- **Values (verbatim):** `active`, `never`, `ceased`; unknown `''` on main/history, NULL on suggestions.
- **Commit footer** on every commit:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01UJnba4eXZta4f9KaKJxhY9
  ```

---

## File map

| File | Change |
|---|---|
| `corpscout/clickhouse/migrations/000394_corpscout_se_company_basic_info_economic_activity.{up,down}.sql` (new) | Three ALTERs / three DROP COLUMNs |
| `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` | `EXPECTED_MIGRATIONS` gains 000394 |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/tables.py` | `VALUE_COLUMNS`, `MAIN_COLUMNS`, `NON_NULLABLE_FIELDS`, `ECONOMIC_ACTIVITIES` |
| `.../basic_info/fold.py` | `Suggestion.economic_activity`, `BasicInfoRow.economic_activity(_source)`, `NON_NULLABLE_FIELDS` in `_value_and_source` and `fold_company` |
| `.../basic_info/precedence.py` | the `economic_activity` map |
| `.../basic_info/scb.py`, `bolagsverket.py`, `esef.py`, `ratsit.py`, `wikidata.py` | the aliased column; `scb-v2` |
| `.../basic_info/docs/basic_info-design.md`, `docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md` | amendments |
| `corpscout/services/dagster_v3/tests/test_se_company_basic_info_tables.py`, `_precedence.py`, `_fold.py`, `_extractors_sql.py`, `_extractors_clickhouse_local.py` | re-pinned; the local harness replays `ALTER TABLE` statements and adds 000394 to its migration list |
| `corpscout/services/backoffice/app/lib/se-basic-info-fields.ts`, `se-basic-info.server.ts`, `app/components/admin/se-basic-info-edit-sheet.tsx`, `se-basic-info-workspace.tsx`, `app/lib/countries.ts` | the field end to end |
| `corpscout/services/backoffice/tests/se-basic-info-fields.test.ts`, `se-basic-info.server.test.ts`, `se-basic-info-edit-sheet.test.tsx`, `admin-se-company-basic-info.test.tsx`, `app/lib/countries.test.ts` | re-pinned |

---

## Task 1: DDL and column tuples

- [x] **Step 1:** Re-pin `test_se_company_basic_info_tables.py` (`VALUE_COLUMNS` with `economic_activity` after `status`; main block asserts `economic_activity LowCardinality(String)` via `declared_columns`, not the CREATE block) and `test_clickhouse_migrations.py`. Run: expect failures.
- [x] **Step 2:** Write 000394 up/down; edit `tables.py`. Run the two tests: PASS.
- [x] **Step 3:** Commit `feat(clickhouse): 000394 adds economic_activity to the basic-info entity`.

## Task 2: Fold and precedence

- [x] **Step 1:** Re-pin `test_se_company_basic_info_precedence.py` (the new map, rows order) and `test_se_company_basic_info_fold.py` (a company whose SCB row says `never` folds `economic_activity = 'never'`, source `scb`; no SCB row -> `''`/`''`; `changed_fields` names it; `''` compares as no value). Run: expect failures.
- [x] **Step 2:** Edit `fold.py` and `precedence.py`. Run the fold, precedence, batch and assets tests: PASS.
- [x] **Step 3:** Commit `feat(dagster): the basic-info fold carries economic_activity`.

## Task 3: Extractors

- [x] **Step 1:** Re-pin `test_se_company_basic_info_extractors_sql.py` (the SCB `multiIf`, `scb-v2`, the NULL alias in the other four) and the clickhouse-local test (the harness replays `ALTER TABLE` statements of the listed migrations; 000394 listed; an SCB fixture row with code `9` yields `ceased`). Run: expect failures.
- [x] **Step 2:** Edit the five extractors and the harness. Run both tests (docker): PASS.
- [x] **Step 3:** Commit `feat(dagster): the SCB extractor emits economic_activity (scb-v2)`.

## Task 4: Docs

- [x] **Step 1:** Amend `basic_info-design.md` (extractors, fields) and the 2026-09-03 spec section 4 (the map, the "later slice" sentence). Commit `docs(dagster): basic-info design carries economic_activity`.

## Task 5: Backoffice

- [x] **Step 1:** Re-pin the five tests (field list, enum, validator, SQL builders carrying the column and its source, the select's three options, the workspace label, the shell query). Run: expect failures.
- [x] **Step 2:** Edit the five files. `npm run typecheck` and the five test files: PASS.
- [x] **Step 3:** Commit `feat(backoffice): economic_activity on the Info tab, edit sheet and SE shell`.

## Task 6: Verify and merge

- [x] **Step 1:** Dagster unit suite (`-m "not integration"`, the cron-collision test deselected): only the four failures already on main.
- [x] **Step 2:** `uv run dg check defs`; backoffice full vitest: only the live-DB timeouts and the ESEF tab test already on main.
- [x] **Step 3:** Merge `--no-ff` into main (footer) -- AFTER Task 7 step 1: the backoffice dev server runs main and the SE shell selects `i.economic_activity`, so a merge before the migration breaks every SE page (verified: the live SE tests fail with `Identifier 'i.economic_activity' cannot be resolved` until 000394 is applied).

## Task 7: Rollout (owner-run steps marked)

- [x] **Step 1 (owner):** apply 000393 (other session's) then 000394: `make clickhouse-migrate-up-one` twice from `corpscout`.
- [ ] **Step 2 (owner):** dbt-state refresh + light_sync deploy.
- [ ] **Step 3:** materialise `se_company_basic_info_precedence_clickhouse`.
- [ ] **Step 4:** launch `se_basic_info_suggestions_scb` with `execute: true, since: "2000-01-01T00:00:00Z"`.
- [ ] **Step 5:** backfill all 64 partitions of `se_company_basic_info_fold`.
- [ ] **Step 6:** smoke `http://localhost:5183/admin/se/company/5020077862/info?field=economic_activity` and `http://localhost:5183/company/se/5020077862`; count the three values on the main table.

## Verification record (2026-09-08)

- Dagster: 3286 passed, 4 failed (the pre-existing four), 1 skipped; `dg check defs` clean; the clickhouse-local extractor test replays 000394 (17 passed).
- Backoffice: typecheck clean; the six field tests green (129 tests); the full suite's only new failures are the live SE queries selecting the unapplied column (expected until Task 7 step 1), the rest are the known timeouts and the ESEF tab test.
