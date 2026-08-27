# SE People Experiment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Sweden people model per the approved spec: three source views, K3 identity with a measured evaluation, backoffice-triggered LLM merging with a parameterized model, view-fed normalization/roles, People-tab switch, and the in-work retirement of the `se_company_person_draft` inbox.

**Architecture:** History and evidence stay in the ORIGINAL source tables (000289 semantic hashes are the evidence keys). Three plain CH views give Sweden a uniform person-observation shape. `person_id` moves to hash domain `se-company-person-v2` keyed by the K3 identity rule (full normalized name + deterministic middle-name/QID reconciliation); ambiguous merges become collision candidates resolved by a backoffice-triggered, LLM-parameterized merge asset writing suggestions through the existing correction ledger. The draft inbox retires; provenance arrays on the final carry source_record_uids + observed hashes.

**Tech Stack:** dagster_v3 (`defs/company_people/` evolves in place; move to `defs/se_company/people.py` is NOT this plan), ClickHouse migrations (next free number was **000329** at writing — re-verify `ls corpscout/clickhouse/migrations | tail -1` before creating files and shift if taken), backoffice (React Router), clickhouse-local test harness.

**Spec:** `docs/superpowers/specs/2026-08-27-se-people-experiment-design.md` (binding, incl. §6 owner decisions). Patterns to copy, not re-derive: view drift-pin = `sweden_company/geocode_serving_overlay.py` + its migration/test pair; LLM run parameterization + `execute` gate = `se_company/info.py` (`AUTOMATED_RUN_CONFIG`, named profiles, `reuse_or_call`); backoffice pipeline-trigger page = `/admin/se/company-info/pipeline`; drop-migration gates = 000328.

## Global Constraints

- `uv run` everything from `corpscout/services/dagster_v3`; `uv run dg check defs` + relevant pytest + `uv run ruff check` before each commit. `DAGSTER_PG_URL="postgresql://dagster_local:dagster_local@127.0.0.1:55432/dagster_local"` for dg in fresh worktrees.
- Migrations: `CREATE DATABASE IF NOT EXISTS corpscout;` first line both files; down twins; no `;` in `--` comments; register in `EXPECTED_MIGRATIONS`; drop migrations carry inline gates (zero-reader proof incl. pre-rename/qualified-constant sweep, row-count snapshot, code-deployed-before-apply, ~480s UNDROP note). **Faithful recreates must include pre-rename ALTERs** (the 000328 lesson).
- Every SQL constant executed on clickhouse-local 26.5 under `join_use_nulls` 0 AND 1 with identical answers. Named params `%(name)s`; project columns explicitly; non-null String gets `''`.
- No `from __future__ import annotations` in asset modules. Commits by explicit path, Conventional Commits, trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- TDD: failing test first, run to see the stated failure, then implement.
- LLM asset rules (owner): model is a RUN PARAMETER via named profiles; `execute: true` gate (UI Materialize = preview that writes nothing); backoffice-triggered; never scheduled/eager.
- Production steps (deploy, migration applies, first resolution run, drop applies) are controller-only, marked **(controller)**.
- Other sessions share the tree: touch nothing outside listed files.

## Task 1: Migration 000329 — the three SE person views

**Files:** Create `corpscout/clickhouse/migrations/000329_corpscout_se_company_person_views.{up,down}.sql`; Create `src/dagster_v3/defs/company_people/source_views.py` (the Python SQL builders — single source of truth the migration embeds); Modify `tests/test_clickhouse_migrations.py` (register); Create `tests/test_se_company_person_views.py`.

**Interfaces produced:** `build_se_company_person_bolagsverket_view_sql()`, `..._esef_...`, `..._wikidata_...` returning the exact `CREATE OR REPLACE VIEW corpscout.<name> AS SELECT ...` bodies; view names as constants `SE_COMPANY_PERSON_BOLAGSVERKET_VIEW` etc.

- [ ] **Step 1 (failing tests):** drift-pin tests comparing each migration-embedded SELECT (normalized whitespace) against the builder output (the `test_se_address_geocodes_served_view.py` pattern); clickhouse-local tests creating the three upstream tables from their real migrations' DDL, inserting fixture rows (incl. a Wikidata row bridging via LEI, an invalid-orgnr row that must be filtered, an ESEF row with `country_code='DK'` that must be excluded), and asserting each view's projected shape per spec §3.1 — under both join_use_nulls settings.
- [ ] **Step 2:** run → FAIL (builders/views missing).
- [ ] **Step 3:** implement builders + migration. Column shapes exactly as spec §3.1's table (uniform: company_id, source_record_uid, person_profile_hash, person_role_hash, full_name, plus per-source typed extras; bolagsverket also first_name/last_name and `full_name = trim(concat(first_name,' ',last_name))`). Wikidata view: bridge join via `wikidata_company_identifiers` on `se_orgnr` OR LEI (copy the join shape from `company_people/draft.py`'s wikidata read), company_id normalized digits-only and filtered to `match(company_id,'^[0-9]{10}([0-9]{2})?$')`. down.sql drops the three views.
- [ ] **Step 4:** full run + `uv run pytest tests/test_clickhouse_migrations.py -q` → PASS.
- [ ] **Step 5:** Commit `feat(se-people): SE person source views over the originals`.

## Task 2: K-rule evaluation asset (one-off analysis)

**Files:** Create `src/dagster_v3/defs/company_people/identity_eval.py`; Test `tests/test_se_company_person_identity_eval.py`.

**Interfaces produced:** pure functions `identity_key_k1(name) -> str` (today's first|last, imported/aliased from normalization for parity), `identity_key_k2(name) -> str` (all tokens, casefold, whitespace-normalized, diacritics PRESERVED), `k3_merge_groups(company_rows) -> list[MergeDecision]` (K2 keys merged iff first+last equal AND one token-set ⊇ other, or shared `person_wikidata_id`; everything else that K1 would merge → `collision_candidate`). Asset `se_company_person_identity_evaluation` (manual job only) reading the three views, computing per-rule counts + the K1-collision classification, emitting metadata (persons per rule, merges, splits, collision_candidate count, 20 sampled candidate groups) and writing the candidate groups to a scratch CH table `se_company_person_collision_candidate` (created in migration 000329 alongside the views: MergeTree ORDER BY (company_id, candidate_group_id); this is experiment state, not serving).
- [ ] **Step 1 (failing tests):** unit tests for k1/k2/k3 on the documented cases: "Anna Maria Svensson" vs "Anna Svensson" (K1 merges, K3 merges via superset), "Anna Svensson" vs "Anna B Svensson" vs "Anna C Svensson" (K3 → collision candidates), diacritics ("Åsa Öberg" ≠ "Asa Oberg" under K2/K3), QID-link merge.
- [ ] **Steps 2-4:** RED → implement → GREEN incl. an executed clickhouse-local test of the asset body over fixture views.
- [ ] **Step 5:** Commit `feat(se-people): identity-rule evaluation (K1/K2/K3) with collision candidates`.

## Task 3: Normalization reads views; person_id v2

**Files:** Modify `src/dagster_v3/defs/company_people/normalization.py` (source reads: draft table → the three views; `person_id_for` → domain `se-company-person-v2` keyed by the K3 deterministic key; provenance: `draft_ids` semantics replaced by source_record_uids + observed 000289 hashes — check migration 000291's columns and add a small ALTER migration 000330 ONLY if a provenance column must be renamed/added, else reuse existing arrays), Modify `roles.py` (role-draft source CTE reads the views; role hashes keep their v2 domain), Modify `corrections.py` only if staleness inputs change shape. Backoffice `se-company-person.server.ts` re-implements the person-id hash in TS — update it to the v2 domain + K3 key IN THE SAME change (grep `se-company-person-v1`).
- [ ] **Step 1:** failing tests pinning: v2 hash domain, K3 key production ("Anna Maria Svensson" and "Anna Svensson" same person_id within a company), views as the only sources (fake-CH dispatch keys), provenance arrays carry view uids+hashes.
- [ ] **Steps 2-4:** RED → implement → GREEN; run `uv run pytest tests/test_se_company_person_views.py tests/test_se_company_person_identity_eval.py tests/test_se_company_person_normalization_live.py tests/test_se_company_person_clickhouse_local.py -q` (NOTE: `-k company_people` does NOT match the `test_se_company_person_*` files — name tests explicitly by path) + the clickhouse-local person harness.
- [ ] **Step 5:** Commit `feat(se-people): view-fed normalization with K3 person identity (v2 hash domain)`.

## Task 4: Backoffice-triggered LLM merge asset

**Files:** Create `src/dagster_v3/defs/company_people/merge.py`; Modify `enrichment` config plumbing only by COPYING `se_company/info.py`'s profile pattern (do not import across packages if info.py's helpers are module-local — replicate the small config class); Test `tests/test_se_company_person_merge.py`.

**Interfaces produced:** asset `se_company_person_merge_suggestions` + job `se_company_person_merge_job`. Run config: `execute: bool = False` (False → preview: log counts, write NOTHING), `llm_profile: str` (named profiles resolving provider/model/key-env exactly like `DEFAULT_LLM_PROFILE` in info.py), `company_ids: list[str] | None`, `max_groups: int | None`. Behavior: read `se_company_person_collision_candidate` groups (minus groups already decided in the ledger), send each group's observations (names, roles, fiscal years, sources) to the LLM asking merge/keep-separate with confidence + rationale, write to `se_company_person_enrichment_observation` (reused by `input_hash` — the group's evidence hashes), NEVER auto-apply: a human approves via the existing correction review page, and an approved merge lands in `se_company_person_correction` with a `merge_persons` payload kind that Task 3's correction application honors (extend `corrections.py`'s kinds: `merge_persons {into_person_id, from_person_ids[]}` + `split_person` as its undo).
- [ ] **Step 1:** failing tests: preview writes nothing; execute+profile writes suggestions; input_hash reuse skips already-suggested unchanged groups; `merge_persons` correction application re-keys roles + tombstones the merged-from profile.
- [ ] **Steps 2-4:** RED → implement → GREEN.
- [ ] **Step 5:** Commit `feat(se-people): backoffice-triggered LLM merge suggestions with parameterized model`.

## Task 5: Backoffice — trigger page + People tab switch + review wiring

**Files (backoffice):** Extend the pipeline-trigger pattern (`/admin/se/company-info/pipeline` copy) to a people pipeline page exposing: identity evaluation run, resolution run, merge run (profile Select, execute confirm); People tab (`se-company-people.server.ts`) — verify it reads `se_company_person`/`se_company_person_role` only (it does) and remove any draft-table reads (`DRAFTS_SQL` in `se-company-person.server.ts:157` must switch to reading the three VIEWS for the person review page's evidence panel); collision-candidate review: surface `se_company_person_collision_candidate` + merge suggestions on the person review page with approve→`merge_persons` correction write.
- [ ] **Steps:** typecheck-driven; component tests where the existing pages have them; `npm run typecheck` clean.
- [ ] Commit `feat(backoffice): people pipeline triggers, view-fed evidence, merge review`.

## Task 6: Retire the draft inbox (owner: in this work, no parallel run)

**Files:** Delete `src/dagster_v3/defs/company_people/draft.py` + its jobs; migration **000331** (or next free) dropping `corpscout.se_company_person_draft` with the full inline gates (snapshot the live row count at apply time — 5,560,060 at spec time; zero-reader grep AFTER Tasks 3/5 land incl. backoffice TS; pre-rename-ALTER sweep for the faithful down recreate — check 000289/000290 ALTERs against THIS table name); delete `tests/test_se_company_person_draft.py` assertions that exercise the collector (keep pure migration-text pins); EXPECTED_MIGRATIONS.
- [ ] **Steps:** zero-reader proof first (rg src + backoffice app), then code removal commit, then migration commit. `dg check defs` green.
- [ ] Commits: `refactor(se-people): remove draft inbox collector` + `feat(clickhouse): retire se_company_person_draft`.

## Task 7 (controller): deploy, apply, first resolution, switch verification

> Amended after the final whole-branch review (which found the original list stale
> vs the ledger rulings). Migration numbers as merged: 000330 (views + candidate
> table), 000331 (view widening — disambiguators/role_label/evidence fields),
> 000332 (draft-table drop). Execute strictly in order.

- [ ] **1. Deploy code** (pristine-worktree light_sync) BEFORE applying any migration.
- [ ] **2. Targeted migration apply to 000331 ONLY.** Check prod `schema_migrations`
      position first (the concurrent ratsit 000329 may or may not be applied). A bare
      `make clickhouse-migrate-up` would ALSO apply 000332 and drop the draft table
      early — use `make clickhouse-migrate-up-one` the required number of times,
      verifying the ledger lands at 331/clean.
- [ ] **3. C1 duplicate-count settle queries** (ruled at Task 3's review; run before any
      resolution): for EACH of the three views —
      `SELECT count() - uniqExact((company_id, source, source_record_uid, person_profile_hash, person_role_hash)) FROM corpscout.se_company_person_<source>`
      (small nonzero allowed — those are exactly what the disambiguators separate),
      and the loader invariant:
      `SELECT count() = uniqExact(draft_id)` over the union source-observations CTE
      → MUST be 1; if not, STOP: a duplicate draft_id survived the disambiguators.
- [ ] **4. Full-corpus identity evaluation** (`se_company_person_identity_evaluation`,
      `write_candidates=true`); **owner reviews the K1/K2/K3 numbers** (with the
      QID-rule caveat the asset description carries) **and confirms K3** — STOP on
      surprise.
- [ ] **5. Baseline capture, THEN cutover TRUNCATE** (ledger ruling I2): snapshot the
      v1 baseline first — `se_company_person` count and a full `se_company_person_role`
      snapshot (needed for the deferred §3.2 role-stability metric) — then TRUNCATE
      `se_company_person`, `se_company_person_role_draft`, `se_company_person_role`.
      The correction ledger + suggestion table are NOT touched; v1-era review
      decisions become stale by design — the owner has been told.
- [ ] **6. First full resolution** via the backoffice pipeline page (10,000-company
      prefill is deliberate; full corpus requires typing the override — paid LLM
      path for multi-source companies, no preview exists).
- [ ] **7. Verify**: person counts vs the step-5 baseline; role-assignment stability
      vs the step-5 role snapshot (the deferred §3.2 metric); spot-check collision
      candidates; People tab renders v2 people; correction staleness behaves.
      Interpretation note: the wikidata view has no registered-company scope, so a
      small legitimate uptick from orgnr-shaped unregistered bridged companies is
      expected.
- [ ] **8. Apply 000332** (the draft drop) only AFTER step 7 verifies; re-snapshot the
      live row count at apply time into the ledger; ~480s UNDROP window.
- [ ] **9. Record outcomes** in this plan + ledger + memory.

**Deferred (explicitly not this plan):** moving people into `defs/se_company/people.py`; retiring the DuckDB/SQLite/Temporal admin machinery; Phase B (`country_person*` + public pages + dbt management switch) — next plan, unblocked by first resolution.
