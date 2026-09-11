# Ratsit slice 1 — basic info from every Ratsit company, and the dead inspector page

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the ~864k companies Ratsit normalized on 2026-09-09 a basic-info suggestion row and fold it, and delete the backoffice Ratsit request inspector, which has read a dropped table since migration 000334.

**Architecture:** No new code and no schema change. Task 1 is a pure deletion in the backoffice (`corpscout/services/backoffice`): seven files and four edits remove the `/admin/se/companies/ratsit` route, its component, its two libs, its three test files, the "Ratsit" tab and the two prose mentions of it. Task 2 is a controller runbook on prod: re-run the existing `se_basic_info_suggestions_ratsit` extractor (unchanged at `ratsit-v2`, already deployed since 2026-09-08) over the whole Ratsit universe, then back-fill `se_company_basic_info_fold` over its 64 static partitions and read out how many companies gained a description.

**Tech Stack:** React Router v7 (`react-router` 8.0.0), TypeScript 5.9, vitest 4.1, pnpm for the backoffice; Dagster 1.13.9 and its GraphQL API on the prod host for the run; ClickHouse 26.5 (`corpscout` database) for the readouts.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md` — this plan is section 8 item 1, and its content is section 3 (3.1 the dead inspector, 3.2 the prod run) resting on the facts of section 2, the rulings of section 6 and the names of section 7.

## Global Constraints

- Work only in the worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info` on branch `se-ratsit-source`. Never `git stash`. Never `git add -A` or `git add .` — stage by explicit path, every time.
- Task 1 touches nothing outside `corpscout/services/backoffice`. Task 2 touches nothing but the spec and this plan.
- Live readers of `source = 'ratsit'` rows in the entity tables (basic info, addresses, people) are **untouched**. Ratsit stays a source; only the crawl-result inspector goes.
- Do not add tests for deleted code. Do not "fix" the pre-existing failing tests (`tests/queries.server.test.ts`, `tests/admin-se-company-esef.test.tsx`) — they fail on `main` today and are out of scope.
- No new dependencies, in either project.
- No extractor change: `basic_info/ratsit.py` stays at `ratsit-v2`. No migration. No Dagster deploy — the host already carries `ratsit-v2` (deployed 2026-09-08 with the translated source views).
- The schedule `se_company_basic_info_weekly` stays **STOPPED** (owner decision 2026-09-08). The run is launched by hand.
- Commit messages end with the two trailers, in this order:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
  ```

## File Structure

| file | what happens |
| --- | --- |
| `corpscout/services/backoffice/app/routes/admin-se-companies-ratsit.tsx` | deleted — the route module (loader + component) of the inspector |
| `corpscout/services/backoffice/app/components/admin/se-ratsit-request-browser.tsx` | deleted — `SeRatsitRequestList` and `SeRatsitRequestInspector`, its only importer was the route |
| `corpscout/services/backoffice/app/lib/se-ratsit-results.server.ts` | deleted — the ClickHouse reads against `corpscout.se_company_ratsit_crawl_results` (table dropped by 000334) |
| `corpscout/services/backoffice/app/lib/se-ratsit-results.ts` | deleted — the client-safe URL helpers of the inspector |
| `corpscout/services/backoffice/tests/admin-se-companies-ratsit.test.ts` | deleted — 3 tests of the route loader |
| `corpscout/services/backoffice/app/lib/se-ratsit-results.test.ts` | deleted — 3 tests of the URL helpers and the tab entry |
| `corpscout/services/backoffice/app/lib/se-ratsit-results.server.test.ts` | deleted — 6 tests of the server lib |
| `corpscout/services/backoffice/app/routes.ts:177` | edited — the `route("ratsit", …)` line under `se/companies` goes |
| `corpscout/services/backoffice/app/lib/se-companies-tabs.ts:18` | edited — the `{ value: "ratsit", label: "Ratsit" }` tab goes; the helpers stay generic |
| `corpscout/services/backoffice/app/routes/admin-se-companies-layout.tsx:31-35` | edited — the header prose no longer says "inspect Ratsit captures" |
| `corpscout/services/backoffice/app/components/admin/admin-sidebar.tsx:45-46` | edited — the comment listing the tabs drops Ratsit |
| `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md` | edited in Task 2 — the Shipped record under section 8 item 1 |
| this plan | ticked in Task 2 |

Nothing else imports the cluster: `rg` finds `se-ratsit-results` and `se-ratsit-request-browser` only inside the seven files and `admin-se-companies-ratsit` only in `app/routes.ts` beside them. The generated `.react-router/types/app/routes/+types/admin-se-companies-ratsit.ts` is gitignored and regenerated by `react-router typegen`, but a **stale** copy on disk still fails `tsc` (tsconfig includes `.react-router/types/**/*`), so Task 1 deletes it by hand.

---

### Task 1: Delete the dead Ratsit inspector cluster (backoffice)

**Files:**
- Delete: `corpscout/services/backoffice/app/routes/admin-se-companies-ratsit.tsx`
- Delete: `corpscout/services/backoffice/app/components/admin/se-ratsit-request-browser.tsx`
- Delete: `corpscout/services/backoffice/app/lib/se-ratsit-results.server.ts`
- Delete: `corpscout/services/backoffice/app/lib/se-ratsit-results.ts`
- Delete: `corpscout/services/backoffice/tests/admin-se-companies-ratsit.test.ts`
- Delete: `corpscout/services/backoffice/app/lib/se-ratsit-results.test.ts`
- Delete: `corpscout/services/backoffice/app/lib/se-ratsit-results.server.test.ts`
- Modify: `corpscout/services/backoffice/app/routes.ts:177`
- Modify: `corpscout/services/backoffice/app/lib/se-companies-tabs.ts:18`
- Modify: `corpscout/services/backoffice/app/routes/admin-se-companies-layout.tsx:31-35`
- Modify: `corpscout/services/backoffice/app/components/admin/admin-sidebar.tsx:45-46`
- Test: none added. The suite loses 3 files / 12 tests and gains nothing.

**Interfaces:**
- Consumes: nothing from another task.
- Produces: for Task 2, one commit on `se-ratsit-source` whose tree has no `/admin/se/companies/ratsit` route and a three-entry `SE_COMPANIES_TABS` (`info`, `geocoding`, `financial`). `seCompaniesTabFromPath`, `seCompaniesTabLabel` and `seCompaniesTabPath` keep their signatures (`(pathname: string) => SeCompaniesTab`, `(tab: SeCompaniesTab) => string`, `(tab: SeCompaniesTab) => string`); only the union `SeCompaniesTab` narrows from `"info" | "geocoding" | "financial" | "ratsit"` to `"info" | "geocoding" | "financial"`.

- [x] **Step 1: Record the BEFORE baseline of the suite and the typecheck**

The suite has pre-existing failures and some of them are load-flaky, so the baseline is a *record*, not a promise. Run both from the backoffice directory and paste the tail of each into the task notes:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
git status --short                     # must be empty: never commit someone else's work
pnpm typecheck 2>&1 | tail -5
CI=1 pnpm exec vitest run --reporter=dot 2>&1 | tail -30
```

Expected on 2026-09-11 (measured three times on this worktree):

- `pnpm typecheck` prints only three `The \`envFile\` option is deprecated` lines and exits 0.
- `Test Files  2 failed | 126 passed (128)` and `Tests  5 failed | 1347 passed (1352)` on a quiet machine; the two failing files are `tests/queries.server.test.ts` (4 address assertions) and `tests/admin-se-company-esef.test.tsx` (1 rendering assertion). Under load one run gave `4 failed | 124 passed (128)` / `8 failed | 1344 passed (1352)` — extra failures are timeouts, not regressions.
- **The numbers that matter and are NOT flaky: 128 test files, 1352 tests.** Write them down; Step 8 asserts 125 and 1340.

- [x] **Step 2: Delete the seven files**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
git rm -q \
  app/routes/admin-se-companies-ratsit.tsx \
  app/components/admin/se-ratsit-request-browser.tsx \
  app/lib/se-ratsit-results.server.ts \
  app/lib/se-ratsit-results.ts \
  app/lib/se-ratsit-results.test.ts \
  app/lib/se-ratsit-results.server.test.ts \
  tests/admin-se-companies-ratsit.test.ts
git status --short
```

Expected: exactly seven `D ` lines and nothing else. `git rm` stages the deletions, which is what Step 9 commits.

- [x] **Step 3: Edit `app/routes.ts` — drop the route**

Remove the `ratsit` line from the `se/companies` block. Before (lines 173-178):

```ts
    route("se/companies", "routes/admin-se-companies-layout.tsx", [
      index("routes/admin-se-companies-info.tsx"),
      route("geocoding", "routes/admin-se-companies-geocoding.tsx"),
      route("financial", "routes/admin-se-companies-financial.tsx"),
      route("ratsit", "routes/admin-se-companies-ratsit.tsx"),
    ]),
```

After:

```ts
    route("se/companies", "routes/admin-se-companies-layout.tsx", [
      index("routes/admin-se-companies-info.tsx"),
      route("geocoding", "routes/admin-se-companies-geocoding.tsx"),
      route("financial", "routes/admin-se-companies-financial.tsx"),
    ]),
```

- [x] **Step 4: Edit `app/lib/se-companies-tabs.ts` — drop the tab**

Before (lines 14-19):

```ts
export const SE_COMPANIES_TABS = [
  { value: "info", label: "Info" },
  { value: "geocoding", label: "Geocoding" },
  { value: "financial", label: "Financial" },
  { value: "ratsit", label: "Ratsit" },
] as const;
```

After:

```ts
export const SE_COMPANIES_TABS = [
  { value: "info", label: "Info" },
  { value: "geocoding", label: "Geocoding" },
  { value: "financial", label: "Financial" },
] as const;
```

Change nothing else in the file. `seCompaniesTabFromPath`, `seCompaniesTabLabel` and `seCompaniesTabPath` are already generic over `SE_COMPANIES_TABS`, so a stale `/admin/se/companies/ratsit` bookmark now falls back to `"info"` in the breadcrumb and 404s on the route — the spec's intent.

- [x] **Step 5: Edit `app/routes/admin-se-companies-layout.tsx` — the header prose**

Before (lines 31-35):

```tsx
        <p className="text-sm text-muted-foreground">
          Every Swedish company Dagster publishes, one register read at a time:
          browse the info list, review geocoding, inspect Ratsit captures, and
          (soon) financials.
        </p>
```

After:

```tsx
        <p className="text-sm text-muted-foreground">
          Every Swedish company Dagster publishes, one register read at a time:
          browse the info list, review geocoding, and (soon) financials.
        </p>
```

- [x] **Step 6: Edit `app/components/admin/admin-sidebar.tsx` — the tab comment**

Before (lines 44-50, inside `COUNTRY_NAVIGATION[0].items`):

```tsx
      {
        // One entry for the whole tabbed list area (Info · Geocoding ·
        // Financial · Ratsit). exact:false so it stays active on every tab.
        title: "Companies",
```

After:

```tsx
      {
        // One entry for the whole tabbed list area (Info · Geocoding ·
        // Financial). exact:false so it stays active on every tab.
        title: "Companies",
```

Nothing else in the sidebar changes: there was never a Ratsit nav entry, only this comment.

- [x] **Step 7: Drop the stale generated route types, then typecheck**

`react-router typegen` writes the tree but does not reliably prune a `+types` file whose route is gone, and `tsconfig.json` includes `.react-router/types/**/*` — the stale module still says `typeof import("../admin-se-companies-ratsit.js")`, which `tsc` cannot resolve. Delete it first:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
rm -f '.react-router/types/app/routes/+types/admin-se-companies-ratsit.ts'
pnpm typecheck
echo "typecheck exit: $?"
```

Expected: the same three `envFile` deprecation lines as the baseline, no TypeScript diagnostics, `typecheck exit: 0`. If `tsc` still complains about `admin-se-companies-ratsit`, the generated tree is stale elsewhere: `rm -rf .react-router` and re-run (the directory is gitignored and fully regenerated).

- [x] **Step 8: Run the suite and compare with the baseline**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
CI=1 pnpm exec vitest run --reporter=dot 2>&1 | tail -30
```

Expected, against the Step 1 record:

- `Test Files … (125)` — exactly 3 fewer than 128.
- `Tests … (1340)` — exactly 12 fewer than 1352 (3 + 3 + 6 tests in the deleted files).
- The failing file names are a subset of the baseline's: `tests/queries.server.test.ts` and `tests/admin-se-company-esef.test.tsx`. **No failure may name a deleted file or mention Ratsit.**

If a *new* file fails, stop and read the failure before touching anything: the only legitimate change to this suite is the disappearance of the three deleted files.

- [x] **Step 9: Prove no reference survives**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
rg -n -e 'se-ratsit-results' -e 'se-ratsit-request-browser' \
      -e 'admin-se-companies-ratsit' -e 'se_company_ratsit_crawl_results' app tests
echo "exit: $?"
```

Expected: no output and `exit: 1` (ripgrep's "no matches"). Then confirm the remaining `ratsit` mentions are all the *source* rows the constraints protect, plus one unrelated marketing link:

```bash
rg -n -i 'ratsit' app tests | rg -v '"ratsit"' | rg -v "'ratsit'"
```

Expected: a few dozen lines, every one a source label (`ratsit: "Ratsit"` in the basic-info, person and address field catalogues), a `source = 'ratsit'` value or comment in the entity libs and their tests, or the `ratsit.se` href in `app/routes/financial-demo.tsx` — none names the deleted cluster. Leave all of them alone.

- [x] **Step 10: Commit, by explicit paths**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
git add -- \
  app/routes.ts \
  app/lib/se-companies-tabs.ts \
  app/routes/admin-se-companies-layout.tsx \
  app/components/admin/admin-sidebar.tsx \
  app/routes/admin-se-companies-ratsit.tsx \
  app/components/admin/se-ratsit-request-browser.tsx \
  app/lib/se-ratsit-results.server.ts \
  app/lib/se-ratsit-results.ts \
  app/lib/se-ratsit-results.test.ts \
  app/lib/se-ratsit-results.server.test.ts \
  tests/admin-se-companies-ratsit.test.ts
git status --short
```

Expected: four `M ` lines and seven `D ` lines, nothing unstaged, nothing untracked. Then write the message to a file inside the git directory (never inside the worktree, so it cannot be committed by accident) and commit with `-F`:

```bash
MSGFILE="$(git rev-parse --git-dir)/RATSIT_SLICE1_MSG"
cat > "$MSGFILE" <<'EOF'
refactor(backoffice): delete the dead Ratsit request inspector

/admin/se/companies/ratsit read corpscout.se_company_ratsit_crawl_results,
dropped by migration 000334, so the page has been a guaranteed error since.
Removes the route, its browser component, the two libs and their three test
files, the Ratsit tab, and the two prose mentions of it in the companies
layout header and the sidebar comment.

Ratsit stays a source of the SE company entities: the source = 'ratsit' rows
in basic info, addresses and people are untouched.

Spec: dagster_v3 docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md
section 3.1.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
git log --oneline -1
git show --stat --oneline HEAD | tail -15
```

Expected: one commit on `se-ratsit-source` touching exactly 11 files, `4 files changed` worth of edits plus 7 deletions, and `git branch --show-current` still `se-ratsit-source`.

---

### Task 2: Prod run and readout (controller)

**The controller runs this task with the owner; a task subagent never touches prod.** There is no code in it: every step is a merge, a Dagster run launched over GraphQL, or a read-only `SELECT`. No migration and **no Dagster deploy** — the host has carried `ratsit-v2` since the translated-source-views rollout on 2026-09-08, and Task 1 changed only the backoffice, which runs locally from the main checkout (so its "deploy" is the merge).

**Files:**
- Modify: `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md` (Step 11, the Shipped record under section 8 item 1)
- Modify: `corpscout/services/dagster_v3/docs/superpowers/plans/2026-09-11-se-ratsit-1-info-and-inspector.md` (Step 11, tick the boxes)
- Test: none — the deliverable is prod state, evidenced by the readouts.

**Interfaces:**
- Consumes: Task 1's commit on `se-ratsit-source` (merged in Step 1).
- Produces: the numbers the spec asks for in section 3.2 — `description_source = 'ratsit'` and `description_sv_source = 'ratsit'` counts and the empty-description count, before and after — written into the spec as the Shipped record.

**Two access recipes used throughout.** Both are one line each; the auto-mode classifier may refuse them, in which case post the exact command and have the owner run it as `! <command>` and paste the output back.

ClickHouse (prod, database `corpscout`), multi-line SQL on stdin:

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
SELECT 1;
SQL
```

If the container name has moved: `ssh companycollect "docker ps --format '{{.Names}}'" | rg clickhouse`.

Dagster GraphQL (prod host, webserver on :3000), payload on stdin:

```bash
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/payload.json | python3 -m json.tool
```

Every launch below targets code location `dagster_v3`, repository `__repository__`, job `__ASSET_JOB`, with the asset named in `assetSelection` and its config under `ops.<asset name>.config`.

- [x] **Step 1: Whole-branch review, then merge to main**

1. Review the branch end to end: `git -C /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info diff main...se-ratsit-source`. Expect the spec commit plus Task 1's single deletion commit, and nothing outside `corpscout/services/backoffice` and `corpscout/services/dagster_v3/docs`.
2. The controller merges. If the main checkout is on `main`: `git -C /Users/graovic/pulsarpoint/ppoint/companycollect merge se-ratsit-source`. If it is on another session's branch, merge through a deploy worktree that checks `main` out (memory `se-worktree-deploy-recipe`) — never `git checkout main` in the shared checkout.
3. Verify: `git -C /Users/graovic/pulsarpoint/ppoint/companycollect log --oneline -3` shows the merge, and `git -C … status --short` is clean.
4. Backoffice smoke on the owner's local dev server (`npm run dev`, http://localhost:5183 — memory `backoffice-runs-locally`):

   ```bash
   curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:5183/admin/se/companies'
   curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:5183/admin/se/companies/geocoding'
   curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:5183/admin/se/companies/ratsit'
   ```

   Expected: `200`, `200`, `404`. The tab bar on the first two shows three tabs — Info, Geocoding, Financial.

- [x] **Step 2: Confirm what prod is about to run**

The schedule must be off and the deployed extractor must be `ratsit-v2` — a version that is not what this plan assumes means someone deployed something in between.

```bash
cat > /tmp/instigators.json <<'JSON'
{"query":"query Instigators($repositorySelector: RepositorySelector!) { schedulesOrError(repositorySelector: $repositorySelector) { __typename ... on Schedules { results { name cronSchedule scheduleState { status } } } } }","variables":{"repositorySelector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__"}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/instigators.json \
  | python3 -c "import json,sys; [print(s['name'], s['cronSchedule'], s['scheduleState']['status']) for s in json.load(sys.stdin)['data']['schedulesOrError']['results'] if 'basic_info' in s['name']]"
```

Expected: `se_company_basic_info_weekly 40 6 * * 1 STOPPED`. **If it says RUNNING, stop and tell the owner** — the weeklies stay stopped (spec section 6).

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
SELECT extractor_version, count() AS rows, max(suggested_at) AS last_write
FROM corpscout.se_company_basic_info_suggestion FINAL
WHERE source = 'ratsit'
GROUP BY extractor_version
ORDER BY extractor_version;
SQL
```

Expected: one row, `ratsit-v2`, 83,696 rows, `last_write` on 2026-09-08 (spec section 2).

- [x] **Step 3: Record the BEFORE numbers**

These are the baseline half of the spec's readout. Run all three and paste the output into the task notes.

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
-- (a) the entity, before
SELECT
    count()                                                        AS companies,
    countIf(description_source = 'ratsit')                         AS description_ratsit,
    countIf(description_sv_source = 'ratsit')                      AS description_sv_ratsit,
    countIf(description_source = '')                               AS no_description_source,
    countIf(description IS NULL OR description = '')               AS empty_description,
    countIf(description_sv IS NULL OR description_sv = '')         AS empty_description_sv,
    countIf(legal_name_source = 'ratsit')                          AS legal_name_ratsit,
    countIf(status_source = 'ratsit')                              AS status_ratsit
FROM corpscout.se_company_basic_info FINAL;

-- (b) the ratsit suggestions, before
SELECT
    count()                                                        AS rows,
    uniqExact(company_id)                                          AS companies,
    countIf(description IS NOT NULL AND description != '')         AS with_description,
    countIf(description_sv IS NOT NULL AND description_sv != '')   AS with_description_sv,
    max(observed_at)                                               AS newest_observed_at
FROM corpscout.se_company_basic_info_suggestion FINAL
WHERE source = 'ratsit';

-- (c) the Ratsit universe the scan will walk
SELECT
    uniqExact(company_id)                                          AS companies_with_a_report,
    countIf(business_description IS NOT NULL AND business_description != '') AS reports_with_a_description
FROM corpscout.se_ratsit_company FINAL;
SQL
```

Expected (spec section 2, prod 2026-09-10/11): (a) ~3,523,558 companies, `description_ratsit` **75**, `no_description_source` **667,794**; (b) 83,696 rows over 83,696 companies; (c) 947,200 companies, 878,596 reports with a description. Small drift is fine — record what prod actually says, because the AFTER step subtracts from *these* numbers, not from the spec's.

- [x] **Step 4: Preview the extract**

The asset's default is a preview (`execute: false`), which walks the same change scan and counts without writing. It is the cheap proof that the scan finds the ~864k companies and not, say, 3 or 3 million.

```bash
cat > /tmp/ratsit-preview.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { pipelineName errors { message path reason } } ... on PythonError { message } ... on InvalidSubsetError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_basic_info_suggestions_ratsit"]}]},"runConfigData":{"ops":{"se_basic_info_suggestions_ratsit":{"config":{"execute":false,"page_size":10000}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/ratsit-preview.json | python3 -m json.tool
```

Expected: `"__typename": "LaunchRunSuccess"` and a `runId`. Poll it:

```bash
cat > /tmp/run.json <<'JSON'
{"query":"query Run($runId: ID!) { runOrError(runId: $runId) { __typename ... on Run { runId status startTime endTime } } }","variables":{"runId":"REPLACE_WITH_RUN_ID"}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/run.json | python3 -m json.tool
```

Poll every 60 s until `status` is `SUCCESS` (one-shot polls, not a long-lived loop — memory `se-person-entity`: local pollers get OOM-killed during long runs). Then read the materialization metadata:

```bash
cat > /tmp/mat.json <<'JSON'
{"query":"query Mat($assetKeys: [AssetKeyInput!]!, $limit: Int!) { assetNodes(assetKeys: $assetKeys) { id assetMaterializations(limit: $limit) { runId timestamp metadataEntries { label __typename ... on IntMetadataEntry { intValue } ... on TextMetadataEntry { text } ... on BoolMetadataEntry { boolValue } } } } }","variables":{"assetKeys":[{"path":["se_basic_info_suggestions_ratsit"]}],"limit":1}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/mat.json | python3 -m json.tool
```

Expected metadata: `execute` false, `companies` ≈ 863,500, `pages` 87 (863,504 / 10,000, ceiling), `candidates` ≈ 863,500, `inserted` **0**, `stopped_at_cap` false. The spec's number is 863,504 companies normalized on 09-09 that never reached any entity, plus any company whose report is newer than its current suggestion. **If `candidates` is under 800,000 or over 1,000,000, stop and reconcile against Step 3(c) before writing anything.**

- [x] **Step 5: Execute the extract**

Same launch with the gate open. The only change from Step 4 is `"execute": true`.

```bash
cat > /tmp/ratsit-execute.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { pipelineName errors { message path reason } } ... on PythonError { message } ... on InvalidSubsetError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_basic_info_suggestions_ratsit"]}]},"runConfigData":{"ops":{"se_basic_info_suggestions_ratsit":{"config":{"execute":true,"page_size":10000}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/ratsit-execute.json | python3 -m json.tool
```

Poll with `/tmp/run.json` as in Step 4 until `SUCCESS`, then re-read the materialization metadata.

Expected: `execute` true, `companies` and `candidates` the same order as the preview, `inserted` **equal to `candidates`**, `stopped_at_cap` false (the default cap is 5,000,000 companies, far above this scan). Record the wall time; the 83,696-row run took 41 s at page_size 20,000 on 2026-09-08, so an order of ten minutes here is unremarkable.

- [x] **Step 6: Read out the suggestions, and prove convergence**

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
SELECT
    count()                                                        AS rows,
    uniqExact(company_id)                                          AS companies,
    countIf(description IS NOT NULL AND description != '')         AS with_description,
    countIf(description_sv IS NOT NULL AND description_sv != '')   AS with_description_sv,
    countIf(description_language = 'en')                           AS description_en,
    countIf(legal_name IS NOT NULL AND legal_name != '')           AS with_legal_name,
    countIf(status = 'active')                                     AS status_active,
    countIf(status = 'inactive')                                   AS status_inactive
FROM corpscout.se_company_basic_info_suggestion FINAL
WHERE source = 'ratsit';
SQL
```

Expected: `companies` ≈ 947,200 (Step 3(c)'s universe, i.e. 83,696 + ~863.5k), `with_description` ≈ 878,596, `rows = companies` (the suggestion table is `ReplacingMergeTree` ordered by `(company_id, source)`, so `FINAL` collapses to one row per company per source — any excess means `FINAL` was omitted somewhere).

Then re-run the **preview** of Step 4 once more. Expected: `candidates` **0** — the change scan is stamp-based, so a converged extractor selects nothing. A non-zero count here means a stamp is moving on its own; find out which before folding.

- [x] **Step 7: Back-fill the fold over all 64 buckets**

`se_company_basic_info_fold` is a static-partitioned asset (`bucket_00`..`bucket_63`) with `BackfillPolicy.multi_run(max_partitions_per_run=1)` and pool `se_company_basic_info_fold`, so a backfill produces one run per partition and the pool serializes them. Default config: `changed_only: true`, `page_size: 20000` — a backfill carries no run config, which is exactly what the default wants (the fold's watermark selection sees the newer suggestions and folds only those companies).

Build the payload (64 partition names, generated rather than typed):

```bash
python3 - <<'PY' > /tmp/fold-backfill.json
import json
query = (
    "mutation LaunchBackfill($backfillParams: LaunchBackfillParams!) {"
    " launchPartitionBackfill(backfillParams: $backfillParams) { __typename"
    " ... on LaunchBackfillSuccess { backfillId }"
    " ... on PartitionSetNotFoundError { message }"
    " ... on PythonError { message } } }"
)
params = {
    "partitionNames": [f"bucket_{i:02d}" for i in range(64)],
    "assetSelection": [{"path": ["se_company_basic_info_fold"]}],
    "fromFailure": False,
    "tags": [],
}
print(json.dumps({"query": query, "variables": {"backfillParams": params}}))
PY
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/fold-backfill.json | python3 -m json.tool
```

Expected: `"__typename": "LaunchBackfillSuccess"` and a `backfillId`. Record it.

Poll the backfill's runs by tag (one-shot per tick, every 2-5 minutes):

```bash
cat > /tmp/backfill-runs.json <<'JSON'
{"query":"query BackfillRuns($backfillId: String!) { runsOrError(filter: {tags: [{key: \"dagster/backfill\", value: $backfillId}]}) { __typename ... on Runs { results { runId status tags { key value } } } } }","variables":{"backfillId":"REPLACE_WITH_BACKFILL_ID"}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/backfill-runs.json \
  | python3 -c "
import collections, json, sys
runs = json.load(sys.stdin)['data']['runsOrError']['results']
by_status = collections.Counter(r['status'] for r in runs)
print(len(runs), 'runs:', dict(by_status))
for r in runs:
    if r['status'] not in ('SUCCESS', 'STARTED', 'STARTING', 'QUEUED'):
        print('  !', r['runId'], r['status'], {t['key']: t['value'] for t in r['tags'] if t['key'] == 'dagster/partition'})
"
```

**Read the first finished run's metadata before the rest complete** (the pool runs them one at a time, so there is a natural checkpoint):

```bash
cat > /tmp/fold-mat.json <<'JSON'
{"query":"query Mat($assetKeys: [AssetKeyInput!]!, $limit: Int!) { assetNodes(assetKeys: $assetKeys) { id assetMaterializations(limit: $limit) { runId timestamp metadataEntries { label __typename ... on IntMetadataEntry { intValue } ... on TextMetadataEntry { text } ... on BoolMetadataEntry { boolValue } } } } }","variables":{"assetKeys":[{"path":["se_company_basic_info_fold"]}],"limit":1}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/fold-mat.json | python3 -m json.tool
```

Expected per bucket: `bucket` the partition's number, `changed_only` true, `page_size` 20000, `considered` ≈ 864,000 / 64 ≈ 13,500, `folded ≈ considered`, `changed` close to `folded` (a company gaining a description changes), `unchanged` small, `unpublished` 0 or near it. **If `considered` comes back near 55,000 (the whole bucket) instead of ~13,500, the run is re-folding everything — stop, because `changed_only` was not honoured.**

Expect 64/64 `SUCCESS`. The slice-2 backfill of 2026-09-04 took about a minute per bucket behind this same limit-1 pool; a wider fold is slower, so budget an hour or two and record the real total.

- [x] **Step 8: Read out the entity (the spec's numbers)**

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
-- (a) the same shape as Step 3(a): compare line by line
SELECT
    count()                                                        AS companies,
    countIf(description_source = 'ratsit')                         AS description_ratsit,
    countIf(description_sv_source = 'ratsit')                      AS description_sv_ratsit,
    countIf(description_source = '')                               AS no_description_source,
    countIf(description IS NULL OR description = '')               AS empty_description,
    countIf(description_sv IS NULL OR description_sv = '')         AS empty_description_sv,
    countIf(legal_name_source = 'ratsit')                          AS legal_name_ratsit,
    countIf(status_source = 'ratsit')                              AS status_ratsit
FROM corpscout.se_company_basic_info FINAL;

-- (b) who supplies the description now
SELECT description_source, count() AS companies
FROM corpscout.se_company_basic_info FINAL
GROUP BY description_source
ORDER BY companies DESC;

-- (c) and the Swedish one
SELECT description_sv_source, count() AS companies
FROM corpscout.se_company_basic_info FINAL
GROUP BY description_sv_source
ORDER BY companies DESC;

-- (d) ten Ratsit-described companies, for the hand spot-check
SELECT company_id, legal_name, legal_name_source, status, status_source,
       description_language, substring(ifNull(description, ''), 1, 120) AS description_head
FROM corpscout.se_company_basic_info FINAL
WHERE description_source = 'ratsit'
ORDER BY cityHash64(company_id)
LIMIT 10;
SQL
```

Expected against Step 3(a):

- `description_ratsit` rises from 75 into the **several hundred thousand** — Ratsit's 300 loses to Bolagsverket's 400 wherever Bolagsverket has a text, so the gain is the companies that had no description at all (667,794 of them) and that Ratsit describes (878,596 reports carry one).
- `empty_description` and `no_description_source` fall by the same amount `description_ratsit` rose (allow a handful of drift from other sources' rows written in between).
- `description_sv_ratsit` rises similarly; Ratsit's Swedish original is supplied whenever the report has a description.
- `legal_name_ratsit` and `status_ratsit` stay near zero: SCB (1000) and Bolagsverket (900/1000) outrank Ratsit (300) for both fields wherever the register knows the company. They move only for companies Ratsit knows and the register does not.
- `companies` (the row count) may rise above the Step 3(a) figure for exactly those register-unknown companies. Record the delta; do not assume it is zero.
- Spot-check (d) by hand against the source: for one of the ten, `SELECT name, status, business_description FROM corpscout.se_ratsit_company FINAL WHERE company_id = '<id>' ORDER BY normalized_at DESC LIMIT 1` must match what the entity published.

- [x] **Step 9: Let the serving view refresh, then check it**

`corpscout.se_companies_serving` is a refreshable MV on the hour at :45 (13-15 minutes). Nothing needs launching (spec 3.2 item 3); just confirm the next refresh succeeded.

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';
SQL
```

Expected: `status` `Scheduled`, `last_success_time` after the backfill finished, `exception` empty. A failed refresh here is the gate that catches a fold that wrote something the view cannot read.

- [x] **Step 10: Confirm nothing was restarted**

```bash
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/instigators.json \
  | python3 -c "import json,sys; [print(s['name'], s['scheduleState']['status']) for s in json.load(sys.stdin)['data']['schedulesOrError']['results'] if 'se_company' in s['name']]"
```

Expected: `se_company_basic_info_weekly STOPPED`, and the address and person weeklies STOPPED as well. This run must not have turned anything on.

- [x] **Step 11: Record the shipped slice and tick the plan**

1. Append a Shipped record to spec section 8 item 1, in the house style (the person spec's section 9 records are the model): the plan file, the merge commit, what shipped, and the prod numbers from Steps 3, 5, 6, 7 and 8 — `description_source = 'ratsit'` before → after, `description_sv_source = 'ratsit'` before → after, the empty-description count before → after, the suggestion companies before → after, the extract's `candidates`/`inserted` and wall time, the backfill id and its total wall time, and any ruling made on the way. Section 8 item 1 reads:

   ```
   1. Basic info from every Ratsit company (prod run) and the dead inspector page (backoffice).
   ```

   and the record goes directly under it, indented like the person spec's, beginning `Shipped 2026-09-11 (plan \`2026-09-11-se-ratsit-1-info-and-inspector.md\`, main <merge commit>): …`.

2. Tick every `- [ ]` in this plan that was done.

3. Commit both files, by explicit path, from the main checkout (the branch is merged by now):

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
MSGFILE="$(git rev-parse --git-dir)/RATSIT_SLICE1_SHIPPED_MSG"
cat > "$MSGFILE" <<'EOF'
docs(spec): record Ratsit slice 1 on prod; plan ticked

The Ratsit basic-info extract now covers every normalized Ratsit company and
the fold has run over all 64 buckets. Numbers in spec section 8 item 1.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
git add -- \
  corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md \
  corpscout/services/dagster_v3/docs/superpowers/plans/2026-09-11-se-ratsit-1-info-and-inspector.md
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
git log --oneline -1
```

4. Update the memory note for this project (`se-basic-info-design.md`, or a new `se-ratsit-source.md`) with: slice 1 is live, the after-numbers, and that slices 2 (the person extractor) and 3 (addresses) are next and unstarted.

---

## Self-review

**1. Spec coverage (section 3, plus the section 2 / 6 / 7 material it rests on)**

| spec | where |
| --- | --- |
| 3 "No extractor change: `basic_info/ratsit.py` stays at ratsit-v2" | Global Constraints; Task 2 Step 2 asserts `ratsit-v2` on prod |
| 3.1 delete the seven files (route, component, two libs, three tests) | Task 1 Step 2, all seven named verbatim |
| 3.1 edit `app/routes.ts` — remove `route("ratsit", …)` under `se/companies` | Task 1 Step 3, with the block before and after |
| 3.1 edit `app/lib/se-companies-tabs.ts` — remove the tab, helpers stay generic | Task 1 Step 4; the Interfaces block records that only the `SeCompaniesTab` union narrows |
| 3.1 edit `admin-se-companies-layout.tsx` — header no longer says "inspect Ratsit captures" | Task 1 Step 5, lines 31-35 quoted |
| 3.1 edit `admin-sidebar.tsx` — the tab comment drops Ratsit | Task 1 Step 6, lines 44-50 quoted |
| 3.1 "nothing else imports the cluster; no e2e test references the route" | File Structure paragraph; Task 1 Step 9's four-string `rg` proof over `app/` and `tests/` (the backoffice has no e2e directory — `tests/` is the whole suite) |
| 3.1 "the generated `+types` regenerates itself" | Task 1 Step 7 — and it deletes the stale file first, because `tsconfig.json` includes `.react-router/types/**/*` and `typegen` does not prune |
| 3.1 "`pnpm typecheck` and the vitest suite must stay at their pre-existing counts" | Task 1 Steps 1 (baseline: 128 files / 1352 tests, 2 known failing files) and 8 (125 / 1340, same failing files) |
| 3.1 "live readers of `source = 'ratsit'` rows are untouched" | Global Constraints; Task 1 Step 9's second `rg` enumerates the mentions that must survive |
| 3.2 item 1 — `se_basic_info_suggestions_ratsit`, `execute: true, page_size: 10000`, ~86 pages, ~864k candidates | Task 2 Steps 4 (preview) and 5 (execute), with the exact GraphQL payloads and the expected metadata |
| 3.2 item 2 — fold over all 64 buckets, changed-only default | Task 2 Step 7, `launchPartitionBackfill` over `bucket_00`..`bucket_63`, no run config |
| 3.2 item 2 readout — `description_source = 'ratsit'`, `description_sv_source = 'ratsit'` with FINAL, empty description before vs after | Task 2 Steps 3 (before) and 8 (after), the same query shape both times |
| 3.2 item 3 — "the hourly serving refresh follows on its own" | Task 2 Step 9 checks `system.view_refreshes` rather than launching anything |
| 3.2 "record the counts in section 8" | Task 2 Step 11 |
| 6 — Ratsit's 300 loses to Bolagsverket's 400; the gain is the ~667k with no description | Task 2 Step 8's expectations spell out why the number lands where it lands |
| 6 — weeklies stay STOPPED, runs launched by hand | Global Constraints; Task 2 Steps 2 and 10 assert it before and after |
| 7 — names: asset `se_basic_info_suggestions_ratsit`, version `ratsit-v2`, deleted backoffice files in 3.1 | used throughout; the seven deleted paths match 3.1 one for one |

Out of scope by the spec itself and therefore absent here: the person extractor (slice 2), the addresses (slice 3), scheduling the Ratsit scan, Ratsit financials / industry codes / summaries / legal form.

**2. Placeholder scan**

No `TBD`, `TODO`, "implement later", "add appropriate error handling", "similar to Task N" or bare "write tests for the above". Every edit shows the before and after text; every command is runnable as written. The only fill-ins are `REPLACE_WITH_RUN_ID` and `REPLACE_WITH_BACKFILL_ID` in the two poll payloads, which are values prod hands back at run time and cannot be known in advance.

**3. Name consistency**

Checked against the code on this branch: asset `se_basic_info_suggestions_ratsit` (`extract.py` `asset_prefix="se_basic_info_suggestions_"` + source `ratsit`), asset `se_company_basic_info_fold` with `StaticPartitionsDefinition(["bucket_00"…"bucket_63"])` and `BasicInfoFoldConfig(changed_only=True, page_size=20000)`, `ExtractConfig(execute=False, page_size=5000, max_companies=5_000_000)` with `page_size ≤ 20000` (so `10000` validates), schedule `se_company_basic_info_weekly` (`40 6 * * 1`, STOPPED), tables `corpscout.se_company_basic_info`, `corpscout.se_company_basic_info_suggestion`, `corpscout.se_ratsit_company`, `corpscout.se_companies_serving`, dropped table `corpscout.se_company_ratsit_crawl_results`, columns `description_source` / `description_sv_source` / `legal_name_source` / `status_source` (migration 000377), extractor version `ratsit-v2`. Backoffice symbols: `SE_COMPANIES_TABS`, `SeCompaniesTab`, `seCompaniesTabFromPath`, `seCompaniesTabLabel`, `seCompaniesTabPath`, `SeRatsitRequestList`, `SeRatsitRequestInspector`. Fold metadata labels used in the expectations (`companies`, `considered`, `folded`, `changed`, `unchanged`, `unpublished`, `bucket`, `changed_only`, `page_size`) are `FoldCounts.as_metadata` plus the asset's own; extract metadata labels (`companies`, `pages`, `candidates`, `inserted`, `execute`, `stopped_at_cap`) are `ExtractCounts.as_metadata`.

**Choices made where the spec left room**

- The spec says "run the extractor with `execute: true`"; the asset's default is a preview and the change scan is the expensive half either way, so Task 2 previews first (Step 4) and executes second (Step 5), with a stop rule if the candidate count is not of the expected order. Same asset, same config but the gate.
- The spec says "fold over all 64 buckets". Task 2 launches one backfill over all 64 rather than a bucket-at-a-time sequence, and inserts a read of the first finished run's metadata as the checkpoint — the limit-1 pool serializes the runs anyway, so this costs nothing and still catches a `changed_only` that is not biting.
- The spec's readout list is descriptions only. Task 2 also records `count()`, `legal_name_source = 'ratsit'` and `status_source = 'ratsit'`, because Ratsit ranks for those two fields as well (precedence 300) and companies Ratsit knows that the register does not will publish from Ratsit alone — a row-count change the spec does not predict.
