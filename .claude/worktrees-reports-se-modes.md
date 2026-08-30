# SE person resolution restructuring — report

Branch: `se-people-resolution-modes` (from `main`). Scope: dagster_v3
`company_people/normalization.py` + `corrections.py`, backoffice People
pipeline page and its libs.

Commits: `129147f7 feat(se-people): split resolution into clean-copy and
gated LLM modes` (4 files, dagster_v3), `41c6de26 feat(backoffice): separate
clean-copy and LLM resolution launches` (8 files, backoffice — 2 new test
files). Note: an intermediate stash-recovery step (see the incident section
below) briefly staged both projects' files together; caught before pushing
anything, `git reset --soft` + explicit re-staging split it back into the
two commits above with no file ending up in the wrong one.

## Design evolution (read this first)

The owner's dispatch asked for a **mode flag** (`mode: 'copy'|'llm'`) on the
existing `se_company_person_clickhouse` asset. Before any code was written for
that shape, two mid-task amendments arrived from the coordinator:

1. LLM output must never write straight to `se_company_person` — it must land
   as a **suggestion** (with confidence) in
   `se_company_person_enrichment_observation`, promoted later by a separate,
   deterministic, model-free step.
2. That LLM path must be a **separate asset**, not a mode flag — three
   distinct assets, three distinct thin jobs, no `mode` config anywhere.

Because no mode-flag code had been committed yet (only planning), there was
nothing to unwind — implementation went directly to the final three-asset
shape below.

## What changed — dagster_v3

### Three assets, one still under its old key

1. **`se_company_person_clickhouse`** (asset key **unchanged**, no churn) —
   now **clean copy only**. Deterministic, single-source companies straight
   to `se_company_person`. A multi-source company in the scanned batch is
   skipped and counted (`skipped_multi_source_count`), never partially
   processed. `SECompanyPersonConfig` now has exactly three fields
   (`company_ids`, `max_companies`, `company_batch_size`) — no LLM fields at
   all, stripped. No preview gate: always writes when launched (deterministic
   and free, mirroring `se_company/info.py`'s copy-mode-needs-no-gate
   precedent).

2. **`se_company_person_llm_suggestions`** (new asset) — multi-source
   companies only. Builds the LLM client from a **named profile**
   (`PERSON_LLM_PROFILES` / `DEFAULT_PERSON_LLM_PROFILE_NAME`, a *separate*
   hand-mirrored registry from `merge.py`'s `MERGE_LLM_PROFILES` — same
   values today, kept apart because the two Dagster-side dicts may diverge
   independently, e.g. `max_tokens`), never `deepseek_settings()`/env-direct.
   `execute: bool = False` gates the model call exactly like `merge.py`/
   `info.py`: a bare Materialize previews (counts `would_call_model`, calls
   nothing, writes nothing). **Writes suggestions ONLY**, to
   `se_company_person_enrichment_observation` — `se_company_person` is never
   touched by this asset. Reuses the *existing* batching/K3-group/LLM-call
   machinery (`_normalize_multi_source_company`) almost verbatim via a new
   thin wrapper, `suggest_multi_source_companies`, which keeps only the
   `suggestion_writes` and discards the profile/finalize side of the old
   combined path.

3. **`se_company_person_promotion`** (new asset) — deterministic, model-free.
   Reads suggestion rows via a new `build_promotion_candidate_companies_sql`
   (distinct companies with ≥1 stored suggestion, paged), layers the newest
   **live** suggestion per person (`promote_company_from_suggestions`) onto
   previously-published profiles, then applies the correction ledger on top
   via the *unchanged* `_finalize_company_profiles`/`apply_person_corrections`
   — exactly the same layering the old combined asset always did. Config:
   `min_confidence: float = 0.0` (promote all by default), plus company
   scoping/`max_companies`/`company_batch_size`. No execute gate — like clean
   copy, it's free and calls no model.

### The confidence contract

- `LlmCompanyPersonSuggestion` (pydantic response model) gained
  `confidence: float = Field(default=1.0, ge=0, le=1)`. Default keeps every
  existing in-process construction (many tests, the reuse path) working
  unchanged. The system prompt in `build_company_people_request` now asks for
  it explicitly.
- `StoredSuggestion` (corrections.py) gained the same field, parsed from the
  suggestion JSON with `1.0` fallback for a pre-existing row written before
  this change — no ClickHouse migration needed: `suggestion` is already a
  `String` column with `CHECK isValidJSON(suggestion)`, confidence just rides
  inside that JSON now.

### Promotion's staleness proxy (documented, not hidden)

Promotion never rebuilds the original LLM request (deliberately model-free),
so it cannot recompute `input_hash` the way the suggestion asset's own reuse
check does. Its staleness test is coarser: a suggestion is "live" only if
every one of its `draft_ids` is still present in the company's current
observations. This mirrors the same coarse-grained notion
`apply_person_corrections._evidence_is_current` already uses for a
correction's own `evidence_hash`. `current_input_hashes` (needed by
`approve_suggestion`/`reject_suggestion`) is built from the *live* set, not
confidence-filtered — a low-confidence suggestion can still be explicitly
approved by a human on an already-published person's page.

### Shared skip/count helper

Extracted `split_pending_statuses_by_source_count(statuses, *,
keep_multi_source)` — used by both the clean-copy and LLM-suggestions
materialize loops so the "skip the other kind, count it, never partially
process" rule is pinned once and independently unit-tested, not duplicated.

### corrections.py — one deliberate addition outside the original file list

`REVIEW_ASSET_NAMES` (feeds `se_company_person_review_job` and the
correction-sensor's `review_run_request`) gained `se_company_person_promotion`.
Reasoning: since `se_company_person_clickhouse` is now clean-copy-only, a
correction against an *already-promoted* multi-source person would never be
re-applied by the sensor-triggered job without this — the sensor exists
precisely to keep published rows in sync with new ledger corrections, and
that guarantee would have silently regressed for every multi-source person
otherwise. `review_run_request`'s config-building loop is generic over
`REVIEW_ASSET_NAMES` and needed no other change; `se_company_person_promotion`
takes `company_ids` like every other review asset.

### Jobs

- `se_company_person_job` (role_draft + person + role, 3-asset) — **unchanged**,
  still valid (person op is just narrower now).
- `se_company_person_publish_job` (existing, `se_company_person_clickhouse`
  only) — reused as-is for the backoffice's clean-copy launch.
- `se_company_person_llm_suggestions_job` (new, single-asset).
- `se_company_person_promotion_job` (new, single-asset).

## Job-shape decision

Chose **three single-asset jobs**, matching the second amendment's explicit
"three launches, one per job, 1:1" and `merge.py`'s own precedent (its asset
doesn't cascade into roles either). None of the three new/reused jobs pulls
in `se_company_person_role_draft_clickhouse`/`se_company_person_role_clickhouse`
— see Concerns below for the trade-off this creates.

## What changed — backoffice

- `app/lib/dagster.server.ts`: added `SE_COMPANY_PERSON_PUBLISH_JOB`,
  `SE_COMPANY_PERSON_LLM_SUGGESTIONS_JOB`/`_ASSET`,
  `SE_COMPANY_PERSON_PROMOTION_JOB`/`_ASSET`. `SE_COMPANY_PERSON_JOB` stays
  exported (doc comment: no longer used by the Pipeline page's own launches,
  kept for the full-cascade use case).
- `app/lib/se-company-person-pipeline.server.ts`: `buildResolutionRunConfig`
  replaced by `buildCleanCopyRunConfig` / `buildLlmSuggestionsRunConfig` /
  `buildPromotionRunConfig`, each targeting exactly one op key.
- `app/lib/se-company-person-pipeline.ts`: added `PERSON_LLM_PROFILES` /
  `DEFAULT_PERSON_LLM_PROFILE_NAME` / `personLlmProfile()` (separate
  hand-mirrored list from `MERGE_LLM_PROFILES`, same "keep in sync by hand"
  deferred-minor as the existing merge list) and `MIN_CONFIDENCE`/
  `MAX_CONFIDENCE`/`DEFAULT_MIN_CONFIDENCE`/`clampMinConfidence` (fractional
  clamp, doesn't truncate like the integer `clamp()`).
- `app/routes/admin-se-people-pipeline.tsx`: loader now lists runs for five
  jobs (identity, clean-copy, llm-suggestions, promotion, merge); action
  handles `confirm/launch-clean-copy`, `-llm-suggestions`, `-promotion` as
  three independent branches (was one `-resolution` branch); the
  DagsterError→section fallback in the catch block updated to disambiguate
  the three new intents before falling through to the generic `merge` check.
- `app/components/admin/se-people-pipeline.tsx`: `PeoplePipelineSection`
  gained `"clean-copy" | "llm-suggestions" | "promotion"` (replacing
  `"resolution"`); `ResolutionSection` split into `CleanCopySection` (company
  scope + max_companies + batch size, **no LLM controls**, confirm text says
  "no LLM involved"), `LlmSuggestionsSection` (profile Select + execute
  confirm + "writes suggestions only ... nothing goes live until promotion"
  text), `PromotionSection` (company scope + batch size + `min_confidence`,
  free/deterministic framing). `ProfileSelect` generalized to take a
  `profiles`/`defaultValue` prop pair so both the merge and LLM-suggestions
  pickers reuse it. Both new resolution sections keep the 10,000 default
  `max_companies` (`DEFAULT_MAX_COMPANIES`, unchanged).

## Job-shape / roles-cascade trade-off — please confirm

The old single "Resolution" launch always cascaded into
`se_company_person_role_draft_clickhouse` + `se_company_person_role_clickhouse`
after publishing people. The three new single-asset launches do **not** —
"Roles job untouched" (original spec) plus the "1:1" job-shape steer pointed
away from wiring a new cross-asset dependency into `roles.py`. Practical
effect: after a clean-copy or promotion run, roles for the touched companies
go stale until something else runs `se_company_person_role_clickhouse` (the
existing combined `se_company_person_job`, or the correction-sensor's
`se_company_person_review_job`, which now includes promotion — see above).
Flagging explicitly since it's a real behavior change from the previous
one-launch design; happy to add a roles cascade to the copy/promotion jobs
if that's not what's wanted.

## Other concerns

- **Review-page pre-promotion visibility gap.** `getSeCompanyPerson`
  (`se-company-person.server.ts`) requires an existing row in
  `se_company_person` before it will show anything, including suggestions —
  it returns `null` outright if the person isn't published yet. A suggestion
  for a **brand-new** (never-before-published) multi-source person is
  therefore invisible on the review page until a promotion run creates the
  row. This matches the design (promotion, not manual review, is what
  populates the final table) but means a reviewer can't preview an
  unpromoted suggestion for a first-time person. Suggestion JSON itself is
  rendered generically (`<pre>{suggestion.suggestion}</pre>`, no
  field-specific parsing other than the merge-suggestion special case), so
  `confidence` shows up automatically once a person row exists — no
  incompatibility there, just this timing gap. Not fixed; flagging per the
  "check compatibility, say NEEDS_CONTEXT if it doesn't fit" instruction —
  this one DOES fit, just with the caveat above.
- **`ruff format --check` pre-existing non-compliance.** Confirmed via
  `git stash` that `normalization.py`/`corrections.py`/`merge.py` already
  failed `ruff format --check` before this branch's changes (long-line
  docstring style is the repo's existing convention here). `ruff check`
  (lint) is clean on every touched file; I did not run `ruff format` to
  avoid an unrelated-diff reformat of pre-existing lines.
- **Confidence promotion gate is coarse.** `min_confidence` compares against
  the *newest* stored suggestion's confidence per person; it does not average
  or otherwise combine confidence across a company's multiple LLM batches
  (large companies chunked by role bucket) — each batch's suggestion is
  independently gated, which is consistent with how the rest of the pipeline
  already treats a batch's `SuggestionWrite` as one atomic unit per person.

## Test evidence

Dagster (`uv run pytest`):

```
tests/test_se_company_person_normalization.py .......... (56 passed)
tests/test_se_company_person_corrections.py ............ (30 passed, incl.
    updated REVIEW_ASSET_NAMES/review_run_request pins)
tests/test_se_company_person_merge.py ................. (32 passed)
tests/test_se_company_person_roles.py ..................
tests/test_se_company_person_identity_eval.py ..........
tests/test_se_company_person_source_observations.py .....
tests/test_se_company_person_views.py ....................
    -> 172 passed total (combined run, before adding the new tests below)
    -> 113 passed re-running normalization+corrections+merge alone after
       adding the new tests (was 102 before)
tests/test_esef_llm_enrichment.py + test_se_company_person_normalization_live.py
    -> 32 passed, 2 skipped (unaffected by removing normalization.py's
       deepseek_settings() import; that test file imports it directly)
```

Backoffice **pipeline-page test file**: initial `find`/`grep` passes missed
`corpscout/services/backoffice/tests/admin-se-people-pipeline.test.ts` (it
lives under the top-level `tests/` directory, not `app/lib/`) — caught only
when the full `npx vitest run` surfaced its 2 failures against the old
`"resolution"` intent/section names. Extended it properly: the `describe("resolution", ...)`
block became three (`"clean copy"`, `"llm suggestions"`, `"promotion"`),
each asserting the exact job name + op config crossing the Dagster boundary
(mirroring the existing style — real run-config builders, faked
`launchRun`/`listRuns`/`loadSeCompanyPersonPipelineStats`); the loader test
updated from 3 to 5 expected `listRuns` calls in job order. All 22 tests in
that file pass.

New tests added (`test_se_company_person_normalization.py`): confidence
default/bounds validation; `split_pending_statuses_by_source_count` for both
directions; `suggest_multi_source_companies` rejects a single-source company
and writes suggestions only (never finalizes); `promote_company_from_suggestions`
gates by confidence, skips a suggestion whose evidence moved (stale), applies
corrections on top, and is idempotent on rerun (no duplicate write); job-wiring
pins for the two new single-asset jobs; `SECompanyPersonConfig` has exactly
the three non-LLM fields.

`corrections.py` test file: updated the two `REVIEW_ASSET_NAMES`/
`review_run_request` assertions to include `se_company_person_promotion`.

```
$ DAGSTER_PG_URL="postgresql://dagster_local:dagster_local@127.0.0.1:55432/dagster_local" uv run dg check defs
All component YAML validated successfully.
All definitions loaded successfully.

$ uv run ruff check src/dagster_v3/defs/company_people/normalization.py \
    src/dagster_v3/defs/company_people/corrections.py \
    tests/test_se_company_person_normalization.py \
    tests/test_se_company_person_corrections.py
All checks passed!
```

Backoffice:

```
$ npm run typecheck
> react-router typegen && tsc
(exit 0, no errors)

$ npx vitest run tests/admin-se-people-pipeline.test.ts \
    app/lib/se-company-person-pipeline.server.test.ts \
    app/lib/se-company-person-pipeline.test.ts
 Test Files  3 passed (3)
      Tests  37 passed (37)
```

A full-suite `npx vitest run` (1245 tests) was also run for broader
confidence: 7 files / 12 tests failed, all pre-existing and unrelated —
6 are live-ClickHouse-dependent tests (esef/Norway/France/Estonia financial
report queries) timing out against whatever ClickHouse this host reaches,
and the 7th was exactly `admin-se-people-pipeline.test.ts` before it got
fixed above (2 of its assertions referenced the retired `"resolution"`
shape). After the fix, targeted reruns of every SE-person-pipeline file are
green; I did not chase the unrelated ClickHouse-timeout failures (out of
scope, not touched by this change).

### Incident: a stash comparison step clobbered the working tree mid-task

While checking whether `ruff format`'s complaints on `merge.py` (a file I
never touched) were pre-existing, I ran `git stash && <check> ; git stash
pop` twice. The second time, the inner command (a broad `npx vitest run`
across 5 unrelated files) exceeded the tool's 2-minute timeout and was
killed — `git stash pop` after the `;` never ran. Between the `stash` and
my next `git status`, **a concurrent session had modified
`corpscout/services/crawler_ratsit/*`** (the shared tree this branch's
instructions warned about), so a plain `git stash pop` then refused
outright (merge conflict) rather than silently overwriting anything.
Recovered with `git checkout stash@{0} -- <this task's 10 files>` — restoring
exactly the files this task touched from the stash, leaving the other
session's `crawler_ratsit` changes on disk completely untouched — then
re-ran the full dagster + backoffice verification (`dg check defs`, `ruff
check`, the full pytest/vitest sets above) to confirm nothing was lost, and
dropped the now-redundant stash. The other session's own stash
(`stash@{0}`: "sweden incremental-exports WIP") was never touched. Flagging
this in full because it's exactly the kind of shared-tree hazard the task
brief called out — no data was lost, but it was close.

## Files touched (all absolute paths)

- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/normalization.py`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/src/dagster_v3/defs/company_people/corrections.py`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/tests/test_se_company_person_normalization.py`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3/tests/test_se_company_person_corrections.py`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/lib/dagster.server.ts`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/lib/se-company-person-pipeline.server.ts`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/lib/se-company-person-pipeline.ts`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/routes/admin-se-people-pipeline.tsx`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/components/admin/se-people-pipeline.tsx`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/tests/admin-se-people-pipeline.test.ts`
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/lib/se-company-person-pipeline.server.test.ts` (new)
- `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/backoffice/app/lib/se-company-person-pipeline.test.ts` (new)
