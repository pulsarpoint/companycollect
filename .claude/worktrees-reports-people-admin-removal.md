# Legacy people-curation removal + tabbed source/final browser

Branch: `remove-old-people-admin` (from `main` @ `41c6de26`), directory
`corpscout/services/backoffice`.

Two owner amendments landed mid-task, in order:
1. Keep `/admin/se/people` registered but render it as a literally empty
   page (no loader, no workspace UI) instead of deleting the route.
2. (Third amendment, supersedes the second) `/admin/se/people` instead
   becomes a tabbed, server-paged data browser over the three SE person
   source views plus the resolved `se_company_person` table.

Both are reflected below and in the two commits.

## Commit 1 -- `refactor(backoffice): remove legacy people curation workspace and worker` (`84a140ff`)

### Removed

**Routes** (file + `routes.ts` entry):
- `admin-se-people-sources.tsx` (`/admin/se/people/sources`)
- `admin-se-people-source.tsx` (`/admin/se/people/sources/:sourceName`)
- `admin-se-people-llm-input.tsx` (`/admin/se/people/llm-input/:draftTwoId`) --
  not named in the original dispatch, but 100% Draft-2/LLM-profile machinery
  (`sweden-people-draft-two.server`, `sweden-person-profile-responses.server`,
  `person-profile-llm.server`); its only entry point was the removed Draft
  workspace's computed links.
- `admin-se-people-draft-job.ts`, `admin-se-people-draft-two-job.ts`,
  `admin-se-people-llm-bulk-job.ts` -- resource routes polled by the removed
  workspace, not named in the dispatch but found via `rg` (each imports a
  removed `.server` lib and had no other consumer).

**Server libs**: `sweden-people-draft.server.ts`, `sweden-people-draft-two.server.ts`,
`sweden-people-draft-sources.server.ts`, `sweden-people-temporal-client.server.ts`,
`sweden-person-profile-responses.server.ts`, `sweden-person-profile-bulk.server.ts`,
`sweden-role-mappings.server.ts`, `people-sources.server.ts` (all named in the
dispatch), plus three found by dependency sweep with zero consumers outside the
removed set: `sweden-people-processing-jobs.server.ts`,
`sweden-person-profile-temporal.server.ts`, `person-profile-llm.server.ts`,
`llm-availability.server.ts`, and the client-safe `people-sources.ts`.

**Components** (verified single-consumer, all inside the removed set):
`people-curation-workspace.tsx`, `people-draft-tables.tsx`,
`people-draft-initializer.tsx`, `people-draft-two-builder.tsx`,
`people-profile-bulk-enhancer.tsx`, `people-source-table.tsx`,
`person-profile-suggestion-card.tsx`, `source-role-mappings-sheet.tsx`.

**Temporal worker stack**: all 8 files under `app/temporal/` (the whole
directory was dedicated to this feature), `Dockerfile.people-worker`, the
`temporal:people-worker` package.json script, and the `@temporalio/*`
(5 packages) + `@duckdb/node-api` dependencies (confirmed zero other
consumers via `rg`; also dropped from `vite.config.ts`'s `optimizeDeps.exclude`
and `pnpm-lock.yaml` regenerated via `pnpm install`).

**Judgment calls beyond the literal dispatch** (flagging per "classify
anything unexpected"):
- `scripts/sync-sweden-role-mappings.py` + its `sync:sweden-role-mappings`
  package.json script: existed solely to populate the role-mapping SQLite the
  removed sources/editor page read. Removed as orphaned tooling.
- `content/sweden/people/role_mappings.sqlite` + its `README.md`: **this file
  IS git-tracked** (fails the dispatch's literal "gitignored + untracked"
  test for local-file auto-delete), but it is 100% orphaned once
  `sweden-role-mappings.server.ts` and the sync script are gone (verified via
  `rg`: no other reader). Removed via `git rm` as source cleanup, not as a
  "local data file" deletion. Flagging explicitly in case the owner wants it
  restored.
- Main `Dockerfile`: dropped the now-empty `COPY --from=build-env /app/content
  /app/content` line -- with `content/sweden/people` gone, `content/` has zero
  files, and an empty/absent source directory would break that `COPY` in a
  fresh clone. Nothing else in `app/` reads `content/` at runtime.
- `.env.example` and `README.md`: dropped the `TEMPORAL_*` block and the
  "Running the Sweden people Temporal worker" section (all vars/prose were
  exclusive to the removed worker).
- 13 test files for the removed modules also removed.

### Local data files (dispatch's explicit list)

- `data/sweden/people-draft.duckdb`, `data/sweden/people-curation.sqlite`:
  confirmed untracked (`git ls-files` empty, `git check-ignore` matches
  `corpscout/.gitignore`) -- deleted from disk with plain `rm`, not `git rm`.
  `data/sweden/` is now gone (was only these two files); `data/settings/`
  (unrelated -- LLM settings SQLite) untouched.
- `content/sweden/people/role_mappings.sqlite`: tracked (see above) -- handled
  as source removal in the commit, not as a local-file delete.

### KEEP verification

`admin-se-people-person.tsx`, `admin-se-people-pipeline.tsx`,
`admin-se-people-stale-corrections.tsx`, `admin-se-company-people.tsx`,
`admin-general-roles.tsx` -- checked every import in each: zero imports from
anything in the removed set.

### Nav / breadcrumb rewiring

- `admin-sidebar.tsx`: dropped the "Sources" sub-item (and its now-unused
  `Rows3Icon` import). "People" (`/admin/se/people`), "Stale corrections" and
  "People pipeline" entries were untouched -- per the owner's amendment,
  keeping `/admin/se/people` registered already solves the nav-parent
  question, since it stayed the sidebar's "People" link and the header's
  logo target (`admin-sidebar.tsx`'s `SidebarHeader` button already pointed
  at `/admin/se/people`, unchanged).
- `admin-layout.tsx` (`AdminBreadcrumbs`): every admin page's "Admin" crumb
  links to `/admin/se/people` (not `/admin`) -- this route is the de facto
  breadcrumb anchor for the whole admin area, so keeping it registered was
  load-bearing, not cosmetic. Removed the dead `onPeopleLlmInputPage` branch
  and the `onSourcesPage`/source-lookup branch (with its `people-sources`
  import), since both target pages are gone. The fallback branch (used by
  pipeline, stale-corrections, person-review, and `/admin/se/people` itself)
  now always renders a plain "Admin / Sweden / People" crumb.
- `admin-index.tsx` (bare `/admin`) redirects to `/admin/se/people` --
  unchanged, still valid since the route survives.

### Entry-point verification (person review page)

`rg` for `se/people/person` confirms both surviving entry points still link
to it: `se-stale-corrections-table.tsx` and `se-company-people.tsx` (the
company People tab). The removed `people-draft-tables.tsx` also linked to it,
but that's moot since it's deleted with the workspace.

### Stray import cleanup

`se-company-info-filter-sheet.tsx` had a doc-comment citing
`source-role-mappings-sheet.tsx` (not a real import) -- reworded to cite
`data-table/contract-filter-sheet.tsx` instead, its other real precedent.

## Commit 2 -- `feat(backoffice): tabbed source and final people browser` (`23165188`)

Per the third owner amendment, `/admin/se/people`'s (now-empty) body was
replaced with a tabbed, server-paged browser:

- **Bolagsverket** tab -> `corpscout.se_company_person_bolagsverket` (view,
  no FINAL needed -- source is a non-versioned MergeTree).
- **ESEF** tab -> `corpscout.se_company_person_esef` (view; FINAL already
  applied inside the view definition).
- **Wikidata** tab -> `corpscout.se_company_person_wikidata` (view; FINAL
  already applied inside the view's joins).
- **People (final)** tab -> `corpscout.se_company_person FINAL` (live
  ReplacingMergeTree) -- columns: company_id, person_id, name, description,
  model_provider, model_name, updated_at (the provenance columns judged
  useful; `draft_ids`/`correction_ids`/hash columns omitted as not
  reader-facing).

Column sets and the exact view/table shapes were read from
`dagster_v3/src/dagster_v3/defs/company_people/source_views.py` and the
`000291_corpscout_se_company_person` migration test
(`test_clickhouse_migrations.py`), not guessed.

Shape mirrors the repo's established list pages
(`se-company-info-lists.server.ts` / `se-company-geocoding-list.server.ts`):
named-param `company_id = {companyId:String}` exact match, `<name column>
ILIKE {name:String}` contains, `LIMIT {limit:UInt32} OFFSET {offset:UInt32}`
(reusing the shared `PAGE_LIMIT_OFFSET_SQL`), page size 50 default clamped to
[10,200] (`paging.ts`'s existing clamps -- unchanged). Tab, filters and paging
all live in `?tab=&companyId=&name=&page=&pageSize=` on this one route, so
every view deep-links; the loader queries **only the active tab's table**
(one page query + one `count()`, never all four). Tabs use shadcn
`Tabs`/`TabsList`/`TabsTrigger` as pure navigation (`render={<Link/>}`),
mirroring `admin-se-companies-layout.tsx`'s tab-bar pattern -- not
`TabsContent`, since only one tab's data is ever fetched/mounted at a time.

New files: `app/lib/se-people-sources.ts` (client-safe tab/filter parsing +
`sePeopleSourcesSearch` URL builder), `app/lib/se-people-sources.server.ts`
(the four list/count query pairs + `loadSePeopleSourcePage` dispatcher),
`app/components/admin/se-people-sources-table.tsx` (tab nav, filter form,
one `DataTable` per tab). `admin-se-people.tsx` rewritten to a thin
loader + this component.

Row affordances: `company_id` links to `/company/SE/:id` as a cell-level
link on every tab; the People (final) tab's **whole row** links to
`/admin/se/people/person/:companyId/:personId` (via `DataTable`'s
`rowHref`), matching the amendment's asymmetric spec. The final tab's empty
state reads "No resolved people yet -- run the pipeline's clean-copy step
from se/people/pipeline" rather than an error.

### Tests added (mirroring sibling conventions)

- `tests/se-people-sources.test.ts` -- tab/filter/view parsing + search-string
  builder (mirrors `se-company-geocoding-filters.test.ts`).
- `tests/se-people-sources.server.test.ts` -- SQL-shape tests (`chQuery`
  mocked, per-tab SELECT/FROM/FINAL assertions, WHERE building, LIMIT/OFFSET
  clamping, `loadSePeopleSourcePage` dispatch -- mirrors
  `se-company-geocoding-list.server.test.ts`).
- `tests/se-people-sources-table.test.tsx` -- component render test
  (`createMemoryRouter` + `renderToStaticMarkup`, mirrors
  `admin-se-stale-corrections.test.tsx`): tab links, company_id links, the
  final tab's row-level person-review link, the empty-state copy, and the
  filter form's hidden fields / Clear visibility.
- `tests/admin-se-people.test.ts` -- loader wiring test (URL -> dispatcher
  call args), mirrors `admin-se-people-pipeline.test.ts`'s module-mock style.

## Verification

- `npm run typecheck` (`react-router typegen && tsc`): clean, exit 0 -- run
  three times (after commit 1, after writing the new feature, after adding
  tests).
- `npx vitest run tests/admin-se-people-person.test.tsx
  tests/admin-se-people-pipeline.test.ts tests/admin-se-stale-corrections.test.tsx
  tests/se-company-person.server.test.ts
  tests/admin-se-company-info-pipeline-sheet.test.tsx
  tests/admin-llm-settings.test.tsx`: 102/102 passed (kept-adjacent pages).
- New tests: `se-people-sources.test.ts` / `.server.test.ts` /
  `-table.test.tsx` / `admin-se-people.test.ts`: 49/49 passed.
- Full suite (`npx vitest run`), run twice (once per commit): both runs
  showed **1259-1263 passed, 4 failed**, but the specific failing tests
  differed between the two runs (`address-quality.test.ts`,
  `esef-financial-reports.server.test.ts` failed both times;
  `fr-detail.queries.test.ts` failed once, `queries.server.test.ts` failed
  once) -- all are live-ClickHouse integration tests unrelated to anything
  touched here (France procurement, Sweden address geocoding composition,
  ESEF financial metrics), all failing via network `Error: Test timed out`,
  not assertion mismatches tied to this change. Pre-existing flakiness, not
  a regression.
- Final sweep: `rg -n 'people-draft|people_draft|sweden-people-worker|
  people-curation|role_mappings' app/` (and separately across the whole
  `corpscout/services/backoffice` tree excluding `node_modules`/lockfile) ->
  zero hits.

## Concern: a foreign commit landed on this branch mid-task

This repo checkout is a **single shared working tree/HEAD** across concurrent
agent sessions (not isolated git worktrees) -- confirmed via `git reflog`.
Between my commit 1 (`84a140ff`) and commit 2 (`23165188`), another
concurrent session (working on `crawler_ratsit`, per the dispatch's own
"crawler_ratsit dirt belongs to another session" note) committed directly
onto whatever branch HEAD happened to point at, which was `remove-old-people-
admin` at that moment: `155d1624 feat(ratsit): add safe local inspection
workflow` (20 files, all under `corpscout/services/crawler_ratsit/`, zero
overlap with my files).

I did not stage or author that commit -- verified `git status`/`git diff`
before each of my two commits, both times showing only my own explicit paths
staged. To avoid data loss I first preserved it on a new pointer,
`rescue/ratsit-safe-local-inspection-155d1624` (points at `155d1624`,
untouched otherwise), then attempted `git rebase --onto 84a140ff 155d1624
remove-old-people-admin` to cleanly excise it from this branch. **That rebase
was blocked by the auto-mode permission classifier** (history-rewrite
requires explicit approval), so I stopped rather than attempt a workaround.

Current state: `remove-old-people-admin` still contains all three commits in
order (`84a140ff` mine, `155d1624` foreign, `23165188` mine) -- nothing lost,
nothing conflicting, but the branch is not "my two commits only." The foreign
commit is also safely preserved on `rescue/ratsit-safe-local-inspection-
155d1624` for the other session. Recommend either: (a) approve a rebase to
clean `remove-old-people-admin` down to just my two commits, or (b) leave it,
since the foreign commit doesn't touch any file this task cares about and
will presumably be picked up by its own session's branch/PR anyway.
