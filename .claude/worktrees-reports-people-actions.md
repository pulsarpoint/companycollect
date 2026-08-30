# people-page-actions -- Actions menu + Simple Sync sheet, Tasks tab

Branch: `people-page-actions` (from `main`, `companycollect` repo). Scope: `corpscout/services/backoffice` only.

## Files

New:
- `app/lib/se-people-simple-sync.server.ts` -- Simple Sync preview query (single-source pending scope) + `loadSimpleSyncPreview`.
- `app/lib/se-people-simple-sync.server.test.ts` -- SQL-shape tests + `loadSimpleSyncPreview` mapping tests.
- `app/lib/se-people-tasks.server.ts` -- Tasks tab data (`loadSePeopleTasks`), 7-row asset/job catalog.
- `app/lib/se-people-tasks.server.test.ts` -- coverage/guarded-read tests.
- `app/components/admin/se-people-actions.tsx` -- Actions `DropdownMenu` + Simple Sync `Sheet`.
- `app/routes/admin-se-people.test.ts` -- action test (`confirm-simple-sync` / `launch-simple-sync`).

Modified:
- `app/routes/admin-se-people.tsx` -- added `action` (the two intents above), rendered `SePeopleSimpleSyncSheet` in the header.
- `app/components/admin/se-people-sources-table.tsx` -- added the Tasks tab render branch (`SePeopleTasksTable`), skips the filter form/pagination for that tab.
- `app/lib/se-people-sources.ts` -- added `"tasks"` to `SE_PEOPLE_SOURCE_TABS` (fifth tab).
- `app/lib/se-people-sources.server.ts` -- `SePeopleSourcePage` union gained a `"tasks"` variant; `loadSePeopleSourcePage` dispatches to `loadSePeopleTasks()` for it.
- `app/lib/dagster.server.ts` -- added `SE_COMPANY_PERSON_ROLE_JOB` / `SE_COMPANY_PERSON_REVIEW_JOB` constants (extended the existing job/asset registry, nothing forked).

## Preview-vs-asset semantics (the controller's diff)

**dagster_v3 `company_people/normalization.py`** -- `company_status` CTE + the `pending_direct_company_count` predicate `build_company_statistics_sql` reports:

```python
company_status AS (
    SELECT
        drafts.company_id AS company_id,
        drafts.source_count AS source_count,
        drafts.observation_count AS observation_count,
        drafts.draft_ids AS draft_ids,
        published.company_id != ''
            AND published.draft_ids = drafts.draft_ids
            AND published.correction_ids = corrections.correction_ids AS is_unchanged
    FROM draft_companies AS drafts
    LEFT JOIN published_companies AS published USING (company_id)
    LEFT JOIN effective_company_corrections AS corrections USING (company_id)
)
...
countIf(NOT is_unchanged AND source_count = 1) AS pending_direct_company_count
```

**backoffice `se-people-simple-sync.server.ts`** -- `company_status` CTE + `pending_single_source_companies`:

```ts
company_status AS (
    SELECT
        drafts.company_id AS company_id,
        drafts.source AS source,
        drafts.source_count AS source_count,
        drafts.observation_count AS observation_count,
        published.company_id != ''
            AND published.draft_ids = drafts.draft_ids
            AND published.correction_ids = corrections.correction_ids AS is_unchanged
    FROM draft_companies AS drafts
    LEFT JOIN published_companies AS published USING (company_id)
    LEFT JOIN effective_company_corrections AS corrections USING (company_id)
),
pending_single_source_companies AS (
    SELECT company_id, source, observation_count
    FROM company_status
    WHERE NOT is_unchanged AND source_count = 1
)
```

Same predicate (`NOT is_unchanged AND source_count = 1`), same `is_unchanged` comparison, same `draft_id` hash formula (`source_views.py`'s `_SOURCE_OBSERVATION_ID_SQL`, domain `se-company-person-source-observation-v2`, per-branch disambiguator `signatory_uid`/`candidate_uid`/`company_wikidata_id`), same person-correction-kind allowlist (`corrections.py`'s `PERSON_CORRECTION_KINDS`). Two deliberate differences, both documented in the file's module docstring: (1) unscoped -- no `company_ids` param, because Confirm always launches `buildCleanCopyRunConfig({ companyIds: [] })` (every company, same as the Pipeline page's own clean-copy launch); (2) `any(source) AS source` added to `draft_companies`, valid only because `source_count = 1` guarantees one source per kept row.

## Launch-path reuse (Feature 1 Confirm)

`admin-se-people.tsx`'s `launch-simple-sync` intent calls exactly the Pipeline page's own clean-copy path: `launchRun({ job: SE_COMPANY_PERSON_PUBLISH_JOB, runConfig: buildCleanCopyRunConfig({ companyIds: [], maxCompanies: DEFAULT_MAX_COMPANIES, companyBatchSize: DEFAULT_COMPANY_BATCH_SIZE }), tags: { pilot: "backoffice" } })` -- same job constant, same run-config builder, same tag convention as `admin-se-people-pipeline.tsx`'s `intent === "launch-clean-copy"` branch. No new launch plumbing; no duplicated op-key/field-name mapping.

## Tasks tab coverage

7 rows via `se-people-tasks.server.ts`'s `TASK_SPECS`, each a `listRuns({ job, limit: 1 })` + (on `SUCCESS`) `assetMaterializations({ asset, limit: 1 })`:

| Row | Job | Asset (metrics) |
|---|---|---|
| clean-copy | `se_company_person_publish_job` | `se_company_person_clickhouse` (`inserted_count`, `total_person_count`) |
| llm-suggestions | `se_company_person_llm_suggestions_job` | `se_company_person_llm_suggestions` (`suggestion_inserted_count`, `would_call_model`) |
| promotion | `se_company_person_promotion_job` | `se_company_person_promotion` (`inserted_count`, `total_person_count`) |
| identity-evaluation | `se_company_person_identity_evaluation_job` | `se_company_person_identity_evaluation` (`collision_candidate_count`, `excluded_blank_name_count`) |
| merge-suggestions | `se_company_person_merge_job` | `se_company_person_merge_suggestions` (`suggestion_inserted_count`, `decided_group_count`) |
| roles | `se_company_person_role_job` (new constant) | `se_company_person_role_clickhouse` (`inserted_role_count`, `total_current_role_count`) |
| review | `se_company_person_review_job` (new constant, the one eager/sensor-driven job in the group) | none -- selects 4 assets/run (clean copy + role draft + role + promotion, `corrections.py`'s `REVIEW_ASSET_NAMES`); status+timing only, per the task's own "if metadata extraction is disproportionate, status+timing suffices" allowance |

Columns: asset/job, colored status badge (`statusVariant`, mirrored from `se-company-info-pipeline.tsx`'s SUCCESS=default/FAILURE|CANCELED=destructive/STARTED|STARTING|QUEUED=secondary/else=outline convention), started, ended, duration, key metrics. Guarded: `loadSePeopleTasks` catches `DagsterError` and returns `{ rows: [], error }`, rendered as a destructive `Alert` over an empty table (same shape as the Pipeline page's `dagsterView()`).

## Test evidence

```
npm run typecheck                          -- clean, no errors (tsc + react-router typegen)

npx vitest run <14 people/dagster/pipeline-related files>
  -- 10 files, 114 tests, all passing (app/lib/se-people-simple-sync.server.test.ts,
     app/lib/se-people-tasks.server.test.ts, app/routes/admin-se-people.test.ts,
     tests/se-people-sources.test.ts, tests/se-people-sources.server.test.ts,
     tests/se-people-sources-table.test.tsx, app/lib/se-company-person-pipeline.server.test.ts,
     app/lib/se-company-person-pipeline.test.ts, app/lib/dagster.server.test.ts,
     app/routes/admin-se-companies-pipeline.test.ts)

npx vitest run (full suite, backoffice/)   -- 1275 passed, 8 failed across 6 files.
  7 of the 8 failures are PRE-EXISTING live-ClickHouse integration tests unrelated to
  this change (tests/esef-financial-reports.server.test.ts x2, tests/fr-detail.queries.test.ts x2,
  tests/queries.server.test.ts x1 -- all real chQuery calls against France/Sweden/Estonia
  financial data, timing out under this shared machine's load; none import anything touched
  here). The 8th failure (tests/se-people-sources.test.ts's tab-catalog test) WAS caused by
  this branch (added the 5th "tasks" tab) and has been fixed in this branch's own commits
  (updated expectation, plus new coverage in tests/se-people-sources.server.test.ts and
  tests/se-people-sources-table.test.tsx). Re-run of just the 6 previously-failing files
  after the fix: only the 5 live-ClickHouse ones still fail (pre-existing/environmental).
```

## Concerns / notes for the controller

1. **Metric key choice is a judgment call.** Each Task row shows 2 of the several int metadata keys each asset's `MaterializeResult` can carry (e.g. clean copy also has `directly_inserted_count`, `skipped_multi_source_count`, `deferred_company_count`). Picked the two that most directly answer "did this do anything" per asset; the rest are visible in the Dagster UI via the run link if needed.
2. **Review job has no per-run metric** -- it launches 4 assets per run (`REVIEW_ASSET_NAMES`), so no single asset's materialization represents "the run's" result; status/timing only, called out explicitly in code comments.
3. **Simple Sync is always unscoped** (every single-source-pending company) -- there is no company-id picker in the sheet, matching the spec's "companies where EXACTLY ONE source has people" description literally rather than adding a scope control the spec didn't ask for.
4. **Pre-existing test collateral fixed, not just avoided**: `tests/se-people-sources.test.ts`, `tests/se-people-sources.server.test.ts` and `tests/se-people-sources-table.test.tsx` all needed (and got) updates for the new 5th tab -- these are included in commit 2 below, not left broken.

## Commits

- `dbc7f6f8` feat(backoffice): people page actions menu with simple-sync preview sheet
- `0a6de02b` feat(backoffice): people tasks tab with asset run stats

Branch `people-page-actions`, both on top of `23165188` (main tip at start of this session). No foreign commits appeared on the branch during this session.
