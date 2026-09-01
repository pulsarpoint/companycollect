# SE Company-Info Field Values Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SE company-info correction ledger with a latest-row-wins field-value table, drop the old table, and rebuild the review UI around "use this / edit / release".

**Architecture:** See the spec. One new ClickHouse table; Dagster's rules collapse to "latest row per field overrides the computed default"; the backoffice writes rows and shows history. Address and person ledgers untouched.

**Tech Stack:** ClickHouse (golang-migrate ledger), Python 3.14 / Dagster / pytest (`uv run pytest`), TypeScript / React Router / vitest (`npx vitest run`, `npm run typecheck`).

**Spec:** docs/superpowers/specs/2026-09-01-se-company-info-field-values-design.md — binding authority; every value below is copied from it.

## Global Constraints

- Dagster tests from `corpscout/services/dagster_v3` (`uv run pytest tests/<file> -q`); backoffice from `corpscout/services/backoffice` (`npx vitest run <file>`; `npm run typecheck` clean before every commit). tests/test_esef_filings_assets.py and Definitions-loading tests need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix` in the env.
- **Do not touch** address-ledger code (`address.py`, `address_rules.py`, `se-address-*.ts`, `se-company-address*.tsx`, `se_company_address_correction`) or person-ledger code. Shared symbols the address side imports MUST keep working: `common.LedgerRow/build_ledger_sql/effective_ledger/ledger_sensor`, `info_rules.evidence_set_hash_for/ArtifactRow/_text`, backoffice `se-info-corrections.ts` enums (`SE_INFO_CORRECTION_KINDS/STATUSES` + types), `parseCorrectionFilters`, `SeCompanyInfoCorrectionsFilterSheet/FilterFields`, `EMPTY_CORRECTION_FILTERS`, `correctionFilterChips`, `correctionsListSearch`.
- Table/column names, constraints, sources, fields, provenance values, sensor name, function names: exactly as the spec. Migration number = `max(existing)+1` at execution (ledger `EXPECTED_MIGRATIONS` + a content test; both up/down; no `;` after `--`; up must not TRUNCATE).
- The published-row scan keeps the alias `latest_correction_at` and reason `ledger_pending`; the published table's `correction_ids` column keeps its name (now "applied field-value ids").
- Conventional Commits; trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` + `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`; stage by explicit path.

### Task 1: migration — new table, grant, gated drop

Files: create `corpscout/clickhouse/migrations/0003NN_corpscout_se_company_info_field_value.{up,down}.sql`; modify `tests/test_clickhouse_migrations.py` (EXPECTED_MIGRATIONS + content test), `tests/test_se_company_layout.py` (retarget the four old-table tests: 84 → new table columns + `ORDER BY (company_id, field, created_at, value_id)`; 98 → new GRANT/REVOKE; 108/145 keep as historical assertions on 000299/000304 text — they still hold), `tests/se_company_ddl.py` if it hard-codes the old table.
- [ ] Ledger/content tests first (RED). Up = spec DDL + GRANT (no DROP: the old ledger's retirement is a deploy-time step, see Task 9 -- revised 2026-09-02). Down = `DROP TABLE IF EXISTS corpscout.se_company_info_field_value`, `REVOKE INSERT ... FROM corpscout_person_correction_writer`. GREEN; commit `feat(clickhouse): se_company_info_field_value replaces the info correction ledger`.

### Task 2: shared sensor `id_column` (additive)

Files: `src/dagster_v3/defs/se_company/common.py` (`build_ledger_cursor_sql`, `build_touched_companies_sql`, `ledger_sensor`, `ledger_cursor`, `touched_company_ids_since` gain `id_column: str = "correction_id"`), `tests/test_se_company_common.py` (+ one test: a sensor built with `id_column="value_id"` emits SQL using `value_id`; existing tests unchanged).
- [ ] RED → implement → GREEN → commit `feat(se): ledger_sensor accepts the id column name`.

### Task 3: rules — `apply_field_values`

Files: `src/dagster_v3/defs/se_company/info_rules.py`, `tests/test_se_company_info_rules.py`.
- [ ] Delete `INFO_KIND_ORDER`, `ZERO_HASH`, `apply_info_ledger`, `InfoOutcome.stale_correction_ids`; add `FieldValueRow` + `apply_field_values` exactly per spec (latest by `(created_at, value_id)`; NULL releases; per-field independence; llm provenance from `stored` by `suggestion_id == UUID(source_ref)`; other sources deterministic `field-value:<source>`; `correction_ids` = applied live row ids sorted; unknown field/source → skipped + counted in a new `InfoOutcome.invalid_value_count: int = 0`). Rewrite the 24 ledger tests listed in the spec's Testing section as field-value cases; keep `test_evidence_set_hash_for_is_order_independent` and all merge tests. Commit `feat(se): field values replace the info correction rules`.

### Task 4: info.py wiring

Files: `src/dagster_v3/defs/se_company/info.py`, `tests/test_se_company_info.py`, `tests/test_se_company_info_clickhouse_local.py`, `tests/test_se_company_layout.py` (if not done in T1).
- [ ] `SE_COMPANY_INFO_FIELD_VALUE`; `build_field_values_sql(table)` (`SELECT value_id, company_id, field, value, source, source_ref, created_at FROM corpscout.<t> WHERE company_id IN %(company_ids)s ORDER BY company_id, field, created_at, value_id`) + row mapper; group by company; call `apply_field_values(outcome, rows, stored=item.stored)` where `apply_info_ledger` was; remove the approval-keeping input_hash recomputation (658-669) after confirming `reuse_or_call` has no other dependency on it; scan `ledger` CTE → new table; metrics `invalid_value_count` replaces `stale_correction_count`; `assert_clickhouse_tables_exist` lists the new table; sensor `se_company_info_field_value_sensor = ledger_sensor(name=..., table=SE_COMPANY_INFO_FIELD_VALUE, id_column="value_id", job=se_company_info_review_job, asset_names=("se_company_info_clickhouse",), extra_config=AUTOMATED_RUN_CONFIG)`; Definitions + docstring updated. Tests: scan-SQL assertions, "a field value survives a model-off run" (replaces the approval round-trip), sensor name + STOPPED, EXISTING_TABLES/NEEDED_TABLES, preview metadata keys. `WEBTECH_*` env for Definitions tests. Commit `feat(se): publish field values, watch the field-value table`.

### Task 5: backoffice lib + server

Files: create `app/lib/se-info-field-values.ts` + `tests/se-info-field-values.test.ts`; modify `app/lib/clickhouse.server.ts` (`chInsertSeCompanyInfoFieldValues`, delete the correction wrapper), `app/lib/se-company-info.server.ts` (`FIELD_VALUES_SQL`, `appendSeCompanyInfoFieldValues`, `SeCompanyInfoFieldValueRow`, `SeCompanyInfoDetail.fieldValues`; delete `CORRECTIONS_SQL`, `appendSeCompanyInfoCorrection`, `SeCompanyInfoCorrectionRow`), slim `app/lib/se-info-corrections.ts` to the shared enums/types (delete `validateSeInfoCorrection`, `liveOverrideCorrectionId`, `SeInfoCorrectionInput/Draft`, the `ZERO_EVIDENCE_HASH` re-export; fix importers), tests `se-company-info.server.test.ts`, `clickhouse-writer.server.test.ts`; delete `tests/se-info-corrections.test.ts`.
- [ ] `FIELD_VALUES_SQL`:
```sql
SELECT toString(v.value_id) AS value_id, v.field AS field, v.value AS value,
  toString(v.source) AS source, v.source_ref AS source_ref, toString(v.source_at) AS source_at,
  v.decided_by AS decided_by, v.note AS note, toString(v.created_at) AS created_at,
  toUInt8(v.value_id = live.value_id) AS is_live
FROM corpscout.se_company_info_field_value AS v
LEFT JOIN (
  SELECT field, argMax(value_id, (created_at, value_id)) AS value_id
  FROM corpscout.se_company_info_field_value WHERE company_id = {companyId:String} GROUP BY field
) AS live ON live.field = v.field
WHERE v.company_id = {companyId:String}
ORDER BY v.created_at DESC, v.value_id DESC
LIMIT 200
```
`appendSeCompanyInfoFieldValues(inputs)` validates every input, checks the company is published (`SELECT 1 FROM corpscout.se_company_info FINAL WHERE company_id = ... LIMIT 1`, error "This company is not published."), inserts `{value_id: randomUUID(), company_id, field, value, source, source_ref, source_at, decided_by: "backoffice", note, created_at}` rows, returns `{valueIds}`. Commit `feat(backoffice): field-value store for SE company info`.

### Task 6: form builder + route action

Files: create `app/lib/se-info-field-value-form.ts` + `tests/se-info-field-value-form.test.ts`; delete `app/lib/se-info-review-form.ts` + `tests/se-info-review-form.test.ts`; modify `app/routes/admin-se-company-info.tsx` (+ the action tests in `tests/admin-se-company-info.test.tsx`).
- [ ] `buildFieldValueInputs(form, {companyId}, detail)` by `intent`: `use-source` (fields `field`, `value`, `source`, `source_ref`, `source_at`), `use-suggestion` (`suggestion_id` → look the suggestion up in `detail.suggestions`, parse its JSON; rows for `description` and, when the sv half is a non-empty string, `description_sv`; source `llm`, source_ref = suggestion_id, source_at = its created_at), `edit` (fields `description`, `description_sv`, `clear_description`, `clear_description_sv`, `original_description`, `original_description_sv`, `note`: a ticked clear box → NULL row; changed text → reviewer row; unchanged → no row; zero rows → `{ok:false, error:"Nothing changed."}`), `release` (`field` → NULL row, source reviewer). Route action: `appendSeCompanyInfoFieldValues(inputs)` → `{ok:true, valueIds}`; validation errors → `{ok:false, error}`. Commit `feat(backoffice): field-value intents on the SE info page`.

### Task 7: workspace UI

Files: `app/components/admin/se-company-info-review-workspace.tsx`, `app/components/admin/company-description-card.tsx` (accept optional per-proposal action slot), `tests/admin-se-company-info.test.tsx`, `tests/company-description-card.test.tsx`.
- [ ] Per spec §Backoffice: **Use this** on each source option (form intent `use-source`; field = `description` when the en block is shown, `description_sv` when the original block is shown and its language is `sv`; wikidata → `description`; hidden `source`, `source_ref` = the artifact's `source_record_uid`, `source_at` = its `observed_at`) — this requires `DescriptionProposal` to carry `sourceRecordUid` and `observedAt` (extend `descriptionProposals`); **Edit** on Final → inline form (intent `edit`); model-suggestions card → one **Use this suggestion** form per suggestion (intent `use-suggestion`); Corrections section → **Value history** card (rows per spec, live badge, **Release to pipeline** button per field, intent `release`); delete `HiddenCommon`, `liveOverrideRefusal` import, undo forms, stale/applied badges, "Correction ids" row; result alert copy "Saved — published on the next rebuild". Rewrite/delete the listed tests. Commit `feat(backoffice): use-this / edit / release review flow on the SE info page`.

### Task 8: remove the corrections page; pipeline + dagster constants

Files: delete `app/routes/admin-se-company-info-corrections.tsx`, `app/components/admin/se-company-info-corrections-table.tsx`, `tests/admin-se-company-info-corrections.test.tsx` (FIRST move its `SeCompanyInfoCorrectionsFilterFields` + `parseCorrectionFilters` describes into new `tests/se-company-correction-filters.test.tsx`); modify `app/routes.ts` (route line), `app/components/admin/se-company-header.tsx` (drop the "Corrections ledger" link), `app/routes/admin-se-companies-info.tsx` (drop "Info corrections" link), `app/routes/admin-layout.tsx` (breadcrumb branch), `app/lib/se-company-info-lists.server.ts` (delete the ledger half from `:651`, keep `resetSeCompanyInfoFilterOptionsCache`) + `tests/se-company-info-lists.server.test.ts` (delete ledger cases, trim the sort test), `app/lib/se-company-info-pipeline.server.ts` (ledger CTE → new table) + its test, `app/lib/dagster.server.ts` (`SE_COMPANY_INFO_SENSOR = "se_company_info_field_value_sensor"`) + `dagster.server.test.ts`, `tests/admin-se-company-area.test.tsx` (drop the link assertion), `tests/admin-se-company-info-pipeline-sheet.test.tsx` (sensor fixture name). Typecheck must be clean. Commit `refactor(backoffice): retire the info corrections page and ledger references`.

### Task 9: deploy checkpoint (owner-gated; the retirement DROP is destructive)

Window hazards (whole-branch review 2026-09-02): new backoffice before 000371 -> every Info page loader/action and the Pipeline sheet 500 (`FIELD_VALUES_SQL`, ledger CTE); new dagster before 000371 -> `assert_clickhouse_tables_exist` fails every info run; old code after the retirement DROP -> Info page / Pipeline sheet / info runs fail on the old table. Order:

- [ ] 1. Merge. Stop `se_company_info_correction_sensor` and pause `se_company_info_weekly` if RUNNING.
- [ ] 2. Apply 000371 (creates the new table only; ledger must be at 000370). Harmless to old code.
- [ ] 3. Deploy dagster_v3 (pristine worktree recipe incl. dbt-state refresh); confirm `se_company_info_field_value_sensor` is listed.
- [ ] 4. Restart the backoffice on the merged code; open one company's Info tab and the Pipeline sheet (executes `FIELD_VALUES_SQL` and the new ledger CTE).
- [ ] 5. Start `se_company_info_field_value_sensor`; re-enable the weekly schedule if it was running.
- [ ] 6. Smoke: Use-this on one company -> tick <= 60 s -> review-job run scoped to that id -> published row carries the value and `correction_ids=[value_id]`; Release -> next run restores the default with `correction_ids=[]`.
- [ ] 7. Retirement (owner go): re-verify `SELECT countIf(length(correction_ids) > 0) FROM corpscout.se_company_info` = 0 and note the row count of `corpscout.se_company_info_correction` (4 on 2026-09-01); `DROP TABLE corpscout.se_company_info_correction` as direct SQL; write + apply the retirement migration (`DROP TABLE IF EXISTS`; down = 000297+000299 DDL empty) with the then-next-free number. UNDROP window ~480 s.

## Self-Review
Spec coverage: table+drop (T1), sensor param (T2), rules (T3), wiring/scan/sensor (T4), store (T5), intents (T6), UI (T7), page removal + pipeline/constants (T8), deploy (T9). Names consistent with the spec throughout.
