# Country-People Retirement (Phase B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the `country_people` group entirely — Dagster assets/job/schedule/sensor, the five+corrections ClickHouse tables, the dbt join feeding public person ids, and every backoffice surface reading these tables. Owner-ordered 2026-08-27, superseding the earlier "wait for serving replacement" gate: the public people pages disappear and the Management section loses resolved person ids until the Sweden model takes over serving (a later plan).

**Sweep (verified 2026-08-27):** dagster `defs/company_people/identity.py` + freshness leaves in `defs/common/clickhouse_checks.py`; dbt `company_management_current_build.sql`, `company_section_item_source_links_build.sql`, `sources.yml`; backoffice `people.server.ts`, `clickhouse.server.ts` (helpers), `countries.ts`, plus the routes importing them (`/people`, `/person/:name`, `/country/:country/person/:id`, `/country/:country/person-targets`) and `company-sections.server.ts`'s profile resolution; migrations 000239 (tables), 000240 (corrections), 000241 (writer role — **SHARED with se_company_person corrections via 000296: the ROLE must survive**).

## Global Constraints

Same as the SE People Experiment plan (uv run, migration rules incl. the 000328 pre-rename-ALTER lesson, clickhouse-local under both join_use_nulls where SQL changes, explicit-path commits, Conventional Commits + Co-Authored-By trailer, TDD where behavior changes, controller-only prod steps). Code deployed BEFORE drops. Live table snapshot at decision time: country_person 1,534,160 / _observation 5,559,541 / _match 5,559,541 / _review_candidate 268,785 (re-snapshot at apply).

## Task 1: Dagster removal

Delete `defs/company_people/identity.py` (multi-asset → 4 keys, job, daily 08:00 schedule, correction sensor) and its Definitions wiring; remove the `country_person_observation`/`country_person_match`/`country_person` freshness/row-count leaves from `defs/common/clickhouse_checks.py`; delete identity tests. Zero-reader sweep for `country_person` across dagster src (classify every hit; the dbt files are Task 2's). Verify: migrations suite + `-k` targeted tests + ruff + dg check defs. Commit: `refactor(country-people): remove the country person identity pipeline`.

## Task 2: dbt serving models

`company_management_current_build.sql`: drop the `country_person_match` join; `person_id` output column becomes `''` (keep the column — downstream contract intact, links die gracefully). `company_section_item_source_links_build.sql`: remove its `country_person` reference the same way (verify what it uses it for first; preserve row output shape). `sources.yml`: remove the country_person source entries. Keep dbt tests green (`dbt parse` + the company_serving test suite); executed-SQL checks where the harness covers these models. Commit: `refactor(company-serving): management serving no longer reads country_person`.

## Task 3: Backoffice removal

Delete the public people routes + their entries in `app/routes.ts` (`/people`, `/person/:name`, `/country/:country/person/:id`, `/country/:country/person-targets`), `people.server.ts`, the country-person helpers in `clickhouse.server.ts` (incl. `chInsertPersonCorrections` if country-only), country-person usage in `countries.ts` and `company-sections.server.ts` (profile-link resolution: Management section renders without person profile links), and all nav/sidebar links to those pages. `npm run typecheck` + touched vitest files; zero `country_person` reads after (rg). Commit: `refactor(backoffice): remove country-person pages and reads`.

## Task 4: Drop migration

Next-free number (re-verify; expect 000333): drop `country_person`, `country_person_observation`, `country_person_identifier`, `country_person_match`, `country_person_review_candidate` (verify which migration created review_candidate — include it in the ALTER sweep), and `country_person_correction` (000240). **DO NOT drop or revoke the `corpscout_person_correction_writer` role (000241) — 000296 grants it on the se_company_person ledger; only remove country-table grants if separable statements exist.** Full inline gates; faithful down recreates with the complete ALTER sweep under all historical names; EXPECTED_MIGRATIONS. Commit: `feat(clickhouse): retire the country_person tables`.

## Task 5 (controller): deploy → apply → verify

Deploy (pristine worktree light_sync) → re-snapshot row counts → targeted apply of the drop migration ONLY if 000332 status is handled deliberately (check ledger position: 000332, the draft drop, may still be pending its own gate — NEVER let it ride along; single-step applies with position checks) → verify tables gone, `dg check defs` healthy on host, public pages return the app's standard not-found, Management section renders without person ids → record outcomes + memory.
