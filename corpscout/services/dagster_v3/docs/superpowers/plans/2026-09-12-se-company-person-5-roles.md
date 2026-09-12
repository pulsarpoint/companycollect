# SE Company Person Slice 5: Roles as Rows — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish every role a published person ever held as its own ClickHouse row — `corpscout.se_company_person_role`, a refreshable materialized view derived from the person entity's two existing tables — and read it in the People tab's Roles panel, so "what did person Y do at company X over time" is one plain `SELECT`.

**Architecture:** One migration, one derived object, no writer. `se_company_person_role` is a refreshable materialized view in the shape the repo's four other refreshable views use (`000326`, `000335`, `000391`, `000392`): the engine is declared **inside** the view (`ENGINE = MergeTree ORDER BY (...)`), so the name IS the table readers query and ClickHouse rebuilds it whole on every refresh. Its SELECT `ARRAY JOIN`s the main row's `normalized_ids` back to the normalized rows the fold built the person from, keeps the rows that carry a role, and stamps `is_current` from the person row's `current_roles` — deterministic, derived, and never written by the fold, the normalizer, the rules or the reviewer. The SELECT is machine-rendered by `person/tables.py::build_se_company_person_role_sql()` and the migration body is drift-pinned against it exactly as `se_companies_serving` is. The view is created `EMPTY` (the migrate client's `read_timeout` is 300 s and the first build reads 5.6M normalized rows), so the controller forces the first refresh by hand. The backoffice adds one read — an eighth entry in `loadSePersonDetail`'s `Promise.all` — and the person panel's Roles section lists the view's rows, falling back to the person row's own arrays when the view has not rebuilt since the last fold.

**Tech Stack:** Python 3.14 / Dagster 1.13.9 (`uv run --frozen --no-sync`, tests with `pytest`), ClickHouse 26.5 (golang-migrate, `make -s -C corpscout clickhouse-migrate-up-one`), React Router v7 backoffice (TypeScript, vitest, `npm run typecheck`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md` — **section 11** ("Roles as rows", owner decision 2026-09-12: the view, its SELECT, its ORDER BY, the prod readouts and the backoffice half), section 3.2 and 3.3 (the two tables the view reads, column by column), section 5.4 (what the person row's role arrays mean), section 9 item 5 (this slice's one-line scope) and section 10 (names). Section 9 items 0 to 4 carry the shipped records of everything this slice builds on; read item 2's fold record for why `role_year` can be missing and item 3's for the backoffice shape. Predecessor plans in this directory: `2026-09-09-se-company-person-0-tables-normalizer-retirement.md`, `2026-09-09-se-company-person-1-extractors.md`, `2026-09-10-se-company-person-2-fold.md`, `2026-09-10-se-company-person-3-backoffice.md`, `2026-09-10-se-company-person-4-rename.md`.

## Global Constraints

- **Worktree and branch.** All work happens in `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info` on branch `se-person-roles` (already checked out, clean, one commit ahead of `main`: the spec's section 11).
- **Never `git stash`.** Commit by explicit path only — never `git add -A`, never `git add .`. Write the message to a file and use `git commit -F <file>`; the trailers below must be the last two lines, each on its own line:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
  ```
- **Dagster commands** run from `corpscout/services/dagster_v3`. Export the env first for anything that loads the definitions tree: `set -a && . ./.env && set +a`, then `uv run --frozen --no-sync pytest ...`. `uv run --frozen --no-sync dg check defs` before any commit that touches `src/`.
- **Backoffice commands** run from `corpscout/services/backoffice`: `npx vitest run <files>`, `npm run typecheck`, `npm run dev`. The full suite takes about 3 minutes.
- **The fold, the normalizer, the rules and the precedence are untouched.** The view is DERIVED: no asset writes it, no fold reads it, and `fold.py`, `batch.py`, `normalize*.py`, `precedence.py` and `match.py` are not edited by this slice. The person row's `role_codes` / `role_years` / `role_sources` / `current_roles` arrays stay exactly as the fold writes them, and everything that reads them keeps reading them.
- **No new dependencies** — no package is added to `pyproject.toml`, `uv.lock`, `package.json` or `pnpm-lock.yaml`.
- **No semicolon may appear inside a `--` comment in a migration file** (`tests/test_clickhouse_migrations.py::test_clickhouse_migration_line_comments_do_not_contain_semicolons` and `::test_no_semicolon_inside_sql_comments`): golang-migrate splits the file on `;` without stripping comments, so a `;` in a comment becomes an empty query (code 62). The file must also END with a statement, not prose (`::test_every_migration_ends_with_a_statement_not_a_comment`).
- **`ORDER BY` cannot contain `Nullable` columns** (`allow_nullable_key` is off — `corpscout/services/dagster_v3/CLAUDE.md`). The view's sort key therefore takes `role_code` and `role_year` through `assumeNotNull` / `ifNull` (Task 1 step 1 explains both).
- **`EMPTY` plus a manual refresh, never `SYSTEM WAIT VIEW`.** The migrate client's `read_timeout` is 300 s (memory `se-companies-serving-view`); a first build over 1.1M persons and 5.6M normalized rows can outlive it, and a dropped client leaves the ledger dirty. The migration creates the view empty and returns in milliseconds; the controller runs `SYSTEM REFRESH VIEW` and polls `system.view_refreshes`.
- **The name is REUSED.** `corpscout.se_company_person_role` was the 2026-08-19 model's role table, dropped by hand in person slice 0 on 2026-09-09 — exactly the situation `se_company_person` was in before migration 000398 took that freed name. Two test guards name it today and BOTH must move in Task 1 (`tests/test_clickhouse_migrations.py::SLICE_0_DROPPED_OBJECTS` forbids any up migration from declaring it; `tests/test_se_person_retirement_drops.py` asserts the spent drop script names no kept object). The drop script `corpscout/clickhouse/operations/se_person_retirement_drops.sql` stays spent and must never run again.
- **Whole-name matching, always.** `corpscout.se_company_person` is a prefix of `_suggestion`, `_normalized`, `_history`, `_rule`, `_precedence`, `_match`, `_match_state` and now `_role`. Every string match on a table name — a test assertion, a fake client's dispatch branch, an `rg` check — must carry the alias or the token that follows it (`corpscout.se_company_person AS m FINAL`, `corpscout.se_company_person_role AS r`), never the bare name.
- **Migration numbers collide.** Other sessions merge to `main` daily. The highest migration here is `000401_corpscout_se_company_financial_entity`, so this one is `000402_corpscout_se_company_person_role`. If `main` has taken 000402 by merge time, renumber BEFORE merging: both migration files, `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`, `MIGRATION` in `tests/test_se_company_person_role_view.py`, and every "000402" in `person-design.md` and the spec.
- **Do not fix pre-existing failing tests.** Backoffice baseline at the branch point: 126 files / 1,349 tests with two files already red — `tests/queries.server.test.ts` and `tests/admin-se-company-esef.test.tsx`. Leave them alone and say so when reporting.
- Never `from __future__ import annotations` in a module that defines Dagster assets (`tables.py` defines none, but the rule holds for the package).

## Baseline, verified at the branch point (2026-09-12)

- `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_se_company_person_tables.py tests/test_se_person_retirement_drops.py tests/test_se_companies_serving_mv.py -q` → **154 passed** in 2.4 s.
- No `clickhouse-local` / `clickhouse` binary on this machine; Docker IS running, so `tests/clickhouse_local.py` falls back to `clickhouse/clickhouse-server:26.5` and the integration test in Task 1 really runs. If it ever skips, say it skipped — never report it as a pass.
- `git status` is clean; `main..HEAD` is one commit (the spec's section 11).
- **The view's SELECT and DDL were run against ClickHouse 26.5 while this plan was written** (clickhouse-local under Docker, both `join_use_nulls` settings, a fixture of three persons and six normalized rows). Proven, not assumed: `ARRAY JOIN p.normalized_ids AS member_id` followed by `INNER JOIN ... ON ...` parses and joins; `CREATE TABLE ... ENGINE = MergeTree ORDER BY (company_id, person_key, role_year, role_code, source, slot) AS <the SELECT>` is accepted; and the inferred columns come back as
  `company_id String, person_key FixedString(64), display_name String, birth_year Nullable(UInt16), role_code String, role_year UInt16, role_from Nullable(Date), role_to Nullable(Date), source LowCardinality(String), slot String, normalized_id FixedString(64), is_current UInt8, folded_at DateTime64(3, 'UTC')`
  — the two unwrapped key columns non-nullable, everything else as declared. A roleless observation, an observation no person names and an inactive person's observation all produced no row, and a dateless role came back under `role_year` 0. Task 1 step 11 turns that run into the committed test.

## Prod facts (spec section 11, measured 2026-09-12)

- The view's SELECT run live yields **3,837,006 rows** over **1,113,485 persons with a role**; 160,279 active persons carry no role and get no row.
- The person rows hold 3,149,261 distinct `(role_code, role_year)` pairs and the parallel arrays are never ragged.
- `corpscout.se_company_person` holds 1,273,764 active persons (about 1.29M rows including the inactive ones) over 578,289 companies; `corpscout.se_company_person_normalized` about 5.57M rows.
- Readout person for the smoke: Swedbank's **Erik Bo Bengtsson** — board member 2021 and 2022 (ESEF, the seat ending 2023-01-18), executive 2023 and 2024, legal representative 2026 (Ratsit).
- `corpscout.se_companies_serving` refreshes hourly at **:45** and takes 13-15 minutes; `se_address_geocodes_current` refreshes at **:00**. The new view takes **:20**, which is why spec section 11 chose that offset.

## Already shipped — do not rebuild: the People list's `year` filter

Spec section 11's second backoffice sentence ("the People list gains a `year` filter, `has(p.role_years, {year})` on the person row") **is already live** — slice 3 shipped six filters, not five (`app/lib/se-people-filters.ts` and `app/lib/se-people-list.server.ts`, commit `3ed65049d`, on `main`). Verified at the branch point:

| piece | where | state |
| --- | --- | --- |
| `SePeopleFilters.year: string` | `app/lib/se-people-filters.ts:24` | present |
| `parseSePeopleFilters` — `YEAR_PATTERN = /^[0-9]{4}$/`, anything else drops to `""` | `app/lib/se-people-filters.ts:20,54,61` | present |
| `sePeopleHref` carries `year` | `app/lib/se-people-filters.ts:76` | present |
| `buildSePeopleFilter` → `has(p.role_years, {year:UInt16})`, `params.year = Number(year)` | `app/lib/se-people-list.server.ts:160-164` | present |
| the `Year` control (`<Input id="people-year" name="year" placeholder="YYYY">`) | `app/components/admin/se-people-table.tsx:161-162` | present |
| tests | `tests/admin-se-people.test.tsx:34-41`, `tests/se-people-list.server.test.ts:54-55` | present |

So this plan has **no list task**. Task 3 step 6 re-checks the filter on prod data as part of the smoke, and the self-review records the finding. Do not add a second year filter, and do not re-point the existing one at the new view: section 11 says the list filter stays on the person row's arrays.

---

## File map

**Task 1 — the view, its builder and its pins (one commit):**

| file | responsibility after this task |
| --- | --- |
| `corpscout/clickhouse/migrations/000402_corpscout_se_company_person_role.up.sql` (create) | two statements: `CREATE DATABASE IF NOT EXISTS corpscout` and the refreshable `CREATE MATERIALIZED VIEW corpscout.se_company_person_role ... EMPTY AS <render>` |
| `corpscout/clickhouse/migrations/000402_corpscout_se_company_person_role.down.sql` (create) | `CREATE DATABASE IF NOT EXISTS corpscout` and `DROP VIEW IF EXISTS corpscout.se_company_person_role` (which takes the view's inner table with it) |
| `src/dagster_v3/defs/se_company/person/tables.py` (modify: constants after line 38, tuples after line 89) | `ROLE_VIEW`, `QUALIFIED_ROLE_VIEW`, `ROLE_VIEW_COLUMNS`, `ROLE_VIEW_ORDER_BY` and `build_se_company_person_role_sql()` — the one place the SELECT exists |
| `tests/test_se_company_person_role_view.py` (create) | the drift pin (migration body == a fresh builder render), the anti-vacuous check, the structural contract of both migration files, and the non-nullable-sort-key proof |
| `tests/test_se_company_person_role_view_clickhouse_local.py` (create) | the behavioural proof against a real ClickHouse: the DDL's sort key accepts the SELECT's inferred types, and the SELECT produces one row per person per role observation under both `join_use_nulls` settings |
| `tests/test_se_company_person_tables.py` (modify: after `test_the_entity_name_is_a_prefix_of_five_siblings`) | the new constants agree with each other and with the migration's ORDER BY |
| `tests/test_clickhouse_migrations.py` (modify: `EXPECTED_MIGRATIONS` line 417, `SLICE_0_DROPPED_OBJECTS`/`SLICE_0_KEPT_OBJECTS` lines 4362-4400) | 000402 is in the ledger contract; `se_company_person_role` moves from "dropped, never re-declared" to "declared by 000402", with the reuse spelled out |
| `tests/test_se_person_retirement_drops.py` (modify: lines 1-15, 55-90) | the never-droppable list carries the reused name; the guard keeps whole-name matching with TWO reused names |
| `tests/test_se_companies_serving_mv.py` (modify: comment on `RETIRED_ROLE_TABLE` only) | the comment says the name is live again and still must not appear in the serving view's body — the assertion is unchanged |
| `src/dagster_v3/defs/se_company/person/docs/person-design.md` (modify: title line 1, module table, a new "Roles as rows" section before "Known limits") | the package doc names the view, its cadence, its lag and the reused name |

**Task 2 — the People tab's Roles panel (one commit):**

| file | responsibility after this task |
| --- | --- |
| `app/lib/se-person-tables.ts` (modify) | `SE_COMPANY_PERSON_ROLE_TABLE`, beside the main-table constant, with the prefix warning |
| `app/lib/se-company-person-entity.server.ts` (modify: type block near line 222, SQL block after line 394, `loadSePersonDetail` lines 698-726) | `SePersonRoleRow`, `PERSON_ROLE_SQL`, the eighth read and `SePersonPublished.roleRows` |
| `app/components/admin/se-person-workspace.tsx` (modify: the Roles `<section>` at lines 1275-1300) | the panel lists the view's rows (role, year, from/to, source, current), falling back to the row's arrays with a note |
| `tests/se-company-person-entity.server.test.ts` (modify: imports, the `answer(sql)` stub near line 233, the detail assertions) | the fake client answers the eighth read; the loader groups the rows by person and pins the SQL |
| `tests/admin-se-company-person.test.tsx` (modify: the `published` fixture near line 67, the workspace render test at line 223) | the panel renders the view's rows, and the fallback when it has none |

**Task 3 — prod run (controller). No repo change except the shipped record and the plan ticks.**

## Interfaces

- **Task 1 produces**, and Task 2 and Task 3 consume:
  - migration name `000402_corpscout_se_company_person_role`;
  - `tables.ROLE_VIEW == "se_company_person_role"`, `tables.QUALIFIED_ROLE_VIEW == "corpscout.se_company_person_role"`;
  - `tables.ROLE_VIEW_COLUMNS == ("company_id", "person_key", "display_name", "birth_year", "role_code", "role_year", "role_from", "role_to", "source", "slot", "normalized_id", "is_current", "folded_at")` — the view's columns in DDL order;
  - `tables.ROLE_VIEW_ORDER_BY == ("company_id", "person_key", "role_year", "role_code", "source", "slot")`;
  - `build_se_company_person_role_sql() -> str` — the SELECT, with no trailing semicolon, interpolating `QUALIFIED_MAIN_TABLE` and `QUALIFIED_NORMALIZED_TABLE`.
- **Task 2 produces** (TypeScript, consumed by the component and its tests):
  - `SE_COMPANY_PERSON_ROLE_TABLE === "corpscout.se_company_person_role"` in `~/lib/se-person-tables`;
  - `interface SePersonRoleRow { company_id: string; person_key: string; role_code: string; role_year: number; role_from: string; role_to: string; source: string; slot: string; normalized_id: string; is_current: number }`;
  - `PERSON_ROLE_SQL: string`;
  - `SePersonPublished.roleRows: SePersonRoleRow[]` — the rows of THIS person, in the view's order, `[]` when the view has none.
- **Unchanged and relied upon:** `SePersonPublished.roles: SePersonRoleYear[]` (the array-derived triple, still built and still the fallback), `SePersonRoleYear { code: string; year: number; sources: string[] }`, `rolesByYear(roles)` in the workspace, `roleLabel(code, options)` and `personSourceLabel(source)` from `~/lib/se-person-fields`, `EMPTY_VALUE` from `~/components/admin/definition-list`, `tests/se_company_ddl.py::table_block(table)` / `declared_columns(table)`, `tests/clickhouse_local.py::clickhouse_local_command()`.
- **Deliberately NOT produced:** no Dagster asset, job, schedule, pool or sensor; no dbt model; no change to `build_se_companies_serving_sql()`; no second `year` filter.

---

### Task 1: Migration 000402 creates the roles view, `tables.py` owns its SELECT, four test files pin it

**Files:**
- Create: `corpscout/clickhouse/migrations/000402_corpscout_se_company_person_role.up.sql`
- Create: `corpscout/clickhouse/migrations/000402_corpscout_se_company_person_role.down.sql`
- Create: `tests/test_se_company_person_role_view.py`
- Create: `tests/test_se_company_person_role_view_clickhouse_local.py`
- Modify: `src/dagster_v3/defs/se_company/person/tables.py` (constants after line 38, tuples after line 89, the builder at the end)
- Modify: `tests/test_se_company_person_tables.py` (one new test after `test_the_entity_name_is_a_prefix_of_five_siblings`)
- Modify: `tests/test_clickhouse_migrations.py:417` (`EXPECTED_MIGRATIONS`) and `:4356-4400` (`SLICE_0_DROPPED_OBJECTS`, `SLICE_0_KEPT_OBJECTS` and their comment)
- Modify: `tests/test_se_person_retirement_drops.py:10-15,55-90` (`KEPT`, `REUSED_NAME`, the guard)
- Modify: `tests/test_se_companies_serving_mv.py:31-33` (the `RETIRED_ROLE_TABLE` comment only)
- Modify: `src/dagster_v3/defs/se_company/person/docs/person-design.md`

**Interfaces:**
- Consumes: `tables.QUALIFIED_MAIN_TABLE` (`"corpscout.se_company_person"`), `tables.QUALIFIED_NORMALIZED_TABLE` (`"corpscout.se_company_person_normalized"`), `tests/se_company_ddl.py::table_block`, `tests/clickhouse_local.py::clickhouse_local_command`.
- Produces: `tables.ROLE_VIEW`, `tables.QUALIFIED_ROLE_VIEW`, `tables.ROLE_VIEW_COLUMNS`, `tables.ROLE_VIEW_ORDER_BY`, `tables.build_se_company_person_role_sql() -> str`, and the migration pair `000402_corpscout_se_company_person_role`.

- [ ] **Step 1: Write the failing pin test**

Create `tests/test_se_company_person_role_view.py`:

```python
"""Migration 000402: roles as rows, and the pin that keeps its body machine-rendered.

`corpscout.se_company_person_role` (spec 2026-09-09 section 11, person slice 5) is a
REFRESHABLE materialized view over the two tables the entity already has: one row per
published ACTIVE person and per role-carrying normalized observation they were folded
from. Nothing writes it -- the fold, the normalizer, the rules and the precedence never
see it -- so the only thing that can drift is the SELECT itself. This file couples the
two halves: the migration's body must be `build_se_company_person_role_sql()`'s render,
and the DDL around it must be the refreshable form this repo uses (engine INSIDE the
view, `EMPTY`, hourly at :20), the same coupling tests/test_se_companies_serving_mv.py
keeps for the serving view.

THE NAME IS REUSED. corpscout.se_company_person_role was the 2026-08-19 model's role
table, dropped by hand in person slice 0 on 2026-09-09 -- the freed-name story migration
000398 already played out for se_company_person. The two guards that know the name are
tests/test_clickhouse_migrations.py (no up migration may DECLARE a slice-0 dropped
object) and tests/test_se_person_retirement_drops.py (the spent drop script names no
kept object); both move in this task.
"""

from pathlib import Path

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.tables import build_se_company_person_role_sql

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000402_corpscout_se_company_person_role"
VIEW = "corpscout.se_company_person_role"
MAIN = "corpscout.se_company_person"
NORMALIZED = "corpscout.se_company_person_normalized"


def _sql(suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{MIGRATION}.{suffix}.sql").read_text(encoding="utf-8")


def _statements(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _body(statement: str) -> str:
    """The statement without the comment lines that precede it."""
    lines = statement.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    body = "\n".join(lines).strip()
    assert body, f"no statement left after stripping comments: {statement[:80]!r}"
    return body


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _executable(sql: str) -> str:
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


def _view_body(sql: str) -> str:
    """The SELECT the CREATE MATERIALIZED VIEW installs, without its semicolon."""
    [statement] = [s for s in _statements(sql) if "CREATE MATERIALIZED VIEW" in s]
    marker = "\nEMPTY\nAS "
    return statement[statement.index(marker) + len(marker) :]


def test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it() -> None:
    assert _normalized(_view_body(_sql("up"))) == _normalized(
        build_se_company_person_role_sql()
    )


def test_the_pin_is_not_vacuous() -> None:
    body = _view_body(_sql("up"))
    assert body.startswith("SELECT\n")
    assert f"FROM {MAIN} AS p FINAL" in body
    assert "ARRAY JOIN p.normalized_ids AS member_id" in body
    assert f"INNER JOIN {NORMALIZED} AS n FINAL" in body
    assert "ON n.company_id = p.company_id AND n.normalized_id = member_id" in body
    assert "WHERE p.active = 1 AND n.role_code IS NOT NULL" in body
    assert "has(p.current_roles, assumeNotNull(n.role_code)) AS is_current" in body
    # Whole-name matching: MAIN prefixes NORMALIZED and the view itself, so the main
    # table is counted with the alias that follows it.
    assert body.count(f"{MAIN} AS p FINAL") == 1
    assert VIEW not in body  # the view never reads itself
    for column in tables.ROLE_VIEW_COLUMNS:
        assert f" AS {column}" in body, column


def test_the_up_migration_creates_one_refreshable_view_created_empty() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 2
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    create = _body(statements[1])
    assert create.startswith(f"CREATE MATERIALIZED VIEW {VIEW}\n")
    assert "\nREFRESH EVERY 1 HOUR OFFSET 20 MINUTE\n" in create
    # The engine lives INSIDE the view (000326/000391's form), so the view IS the table
    # every reader queries and there is no second object to keep in step.
    assert "\nENGINE = MergeTree\n" in create
    assert f"\nORDER BY ({', '.join(tables.ROLE_VIEW_ORDER_BY)})\n" in create
    # EMPTY: the CREATE returns at once and the first build is an explicit SYSTEM
    # REFRESH VIEW in the runbook -- never a SYSTEM WAIT VIEW, which outlives the
    # migrate client's read_timeout of 300 s and leaves the ledger dirty.
    assert "\nEMPTY\nAS SELECT\n" in create
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    # Nothing else moves on the way up: no table, no ALTER, no drop.
    assert "CREATE TABLE" not in _sql("up")
    assert "ALTER TABLE" not in _sql("up")
    assert "DROP" not in _executable(_sql("up")).upper()


def test_the_down_migration_drops_the_view_and_with_it_its_inner_table() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 2
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"DROP VIEW IF EXISTS {VIEW}"
    # One object in, one object out: the inline engine means the view owns its MergeTree
    # and DROP VIEW takes the data with it.
    assert "DROP TABLE" not in _sql("down")
    assert "CREATE MATERIALIZED VIEW" not in _sql("down")


def test_the_sort_key_carries_no_nullable_column() -> None:
    """`allow_nullable_key` is off (dagster_v3/CLAUDE.md). `role_code` and `role_year`
    are Nullable on the normalized row and BOTH are in the sort key, so the SELECT
    unwraps them -- and only them: the other four key columns are already non-nullable,
    and the three Nullable columns outside the key stay Nullable on purpose (a missing
    birth year or an open span must read as NULL, not as a zero)."""
    body = _view_body(_sql("up"))

    assert "assumeNotNull(n.role_code) AS role_code" in body
    assert "ifNull(n.role_year, 0) AS role_year" in body
    assert "n.role_code AS role_code" not in body
    assert "n.role_year AS role_year" not in body
    assert "p.birth_year AS birth_year" in body
    assert "n.role_from AS role_from" in body
    assert "n.role_to AS role_to" in body
```

- [ ] **Step 2: Run it to watch it fail on the missing builder**

```bash
cd corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run --frozen --no-sync pytest tests/test_se_company_person_role_view.py -q
```

Expected: collection error — `ImportError: cannot import name 'build_se_company_person_role_sql' from 'dagster_v3.defs.se_company.person.tables'`.

- [ ] **Step 3: Add the constants and the builder to `tables.py`**

In `src/dagster_v3/defs/se_company/person/tables.py`, after `MATCH_STATE_TABLE` (line 23) add the view name:

```python
# Slice 5 (spec section 11): roles as ROWS. A refreshable materialized view over the main
# table and the normalized rows -- one row per published person and role observation --
# rebuilt hourly at :20. It is DERIVED: nothing in this package writes it, and the fold
# never reads it. THE NAME IS REUSED: corpscout.se_company_person_role was the 2026-08-19
# model's role table, dropped by hand in slice 0 on 2026-09-09, the same freed-name story
# migration 000398 played out for the main table.
ROLE_VIEW = "se_company_person_role"
```

after `QUALIFIED_MATCH_STATE_TABLE` (line 38):

```python
QUALIFIED_ROLE_VIEW = f"{DATABASE}.{ROLE_VIEW}"
```

and after `MATCH_STATE_COLUMNS` (the end of the module, line 89):

```python
# The view's columns in DDL order, and its sort key. `is_current` is the only derived
# column; every other one is a main-table or a normalized-table column carried through.
ROLE_VIEW_COLUMNS: tuple[str, ...] = (
    "company_id", "person_key", "display_name", "birth_year", "role_code", "role_year",
    "role_from", "role_to", "source", "slot", "normalized_id", "is_current", "folded_at",
)
# ORDER BY holds no Nullable column (allow_nullable_key is off), which is why the SELECT
# below unwraps role_code and role_year and nothing else.
ROLE_VIEW_ORDER_BY: tuple[str, ...] = (
    "company_id", "person_key", "role_year", "role_code", "source", "slot",
)


def build_se_company_person_role_sql() -> str:
    """The SELECT behind `corpscout.se_company_person_role` (spec section 11).

    One row per published ACTIVE person and per role-carrying observation the fold built
    them from: `ARRAY JOIN` over the main row's `normalized_ids` -- the normalized
    versions the CURRENT published row was folded from -- back to the normalized table,
    keeping the rows that carry a role code. A person with no role at all (160,279 of
    them on prod) gets no row; an observation re-normalized since the last fold drops out
    until the next one, because its `normalized_id` is no longer the one the person row
    names.

    Two expressions differ from the spec's prose SELECT, and both are forced by the sort
    key: `role_code` and `role_year` are Nullable on the normalized row and `ORDER BY`
    cannot hold a Nullable column. `assumeNotNull(n.role_code)` is exact -- the WHERE has
    already dropped every NULL -- and `ifNull(n.role_year, 0)` publishes a dateless role
    under year 0, which is how this view says "no year at all" (the person row's
    `role_years` array says it differently: the fold takes a dateless role as held now).

    THE VIEW IS DERIVED AND NOTHING WRITES IT. It lags a fold by at most an hour; the
    person row's role arrays remain the fold's own summary.
    """
    return f"""SELECT
  p.company_id AS company_id,
  p.person_key AS person_key,
  p.display_name AS display_name,
  p.birth_year AS birth_year,
  assumeNotNull(n.role_code) AS role_code,
  ifNull(n.role_year, 0) AS role_year,
  n.role_from AS role_from,
  n.role_to AS role_to,
  n.source AS source,
  n.slot AS slot,
  n.normalized_id AS normalized_id,
  has(p.current_roles, assumeNotNull(n.role_code)) AS is_current,
  p.folded_at AS folded_at
FROM {QUALIFIED_MAIN_TABLE} AS p FINAL
ARRAY JOIN p.normalized_ids AS member_id
INNER JOIN {QUALIFIED_NORMALIZED_TABLE} AS n FINAL
  ON n.company_id = p.company_id AND n.normalized_id = member_id
WHERE p.active = 1 AND n.role_code IS NOT NULL"""
```

Also extend the module docstring's first paragraph with one sentence: `Slice 5 added the derived role view (ROLE_VIEW, build_se_company_person_role_sql), created by migration 000402.`

- [ ] **Step 4: Run the pin test again to watch it fail on the missing migration**

```bash
uv run --frozen --no-sync pytest tests/test_se_company_person_role_view.py -q
```

Expected: every test errors with `FileNotFoundError: ... 000402_corpscout_se_company_person_role.up.sql`.

- [ ] **Step 5: Write the migration**

Create `corpscout/clickhouse/migrations/000402_corpscout_se_company_person_role.up.sql`. **No `;` anywhere inside a `--` comment**, and the file ends with the statement:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- ROLES AS ROWS (spec 2026-09-09 section 11, person slice 5). The person row keeps its
-- index-parallel role block (role_codes, role_years, role_sources, current_roles), which
-- answers "who are the people of company X" and not "what did person Y do at company X
-- over time". This view answers the second question in plain SQL: one row per published
-- ACTIVE person and per role-carrying normalized observation the fold built them from.
--
-- DERIVED, NOT WRITTEN. Nothing inserts into it. The fold, the normalizer, the reviewer
-- rules and the precedence are untouched by this migration, and the arrays on the person
-- row stay exactly as the fold writes them. The view is recomputed whole on every
-- refresh, so it cannot drift from the two tables it reads -- it can only LAG them, by
-- at most one hour.
--
-- THE REFRESHABLE FORM THIS REPO USES (000326, 000335, 000391, 000392): the engine is
-- declared INSIDE the view, so corpscout.se_company_person_role IS the MergeTree readers
-- query and there is no separate target table to keep in step. The down file's DROP VIEW
-- takes that inner table with it.
--
-- REFRESH AT :20. se_companies_serving refreshes at :45 and takes 13 to 15 minutes,
-- se_address_geocodes_current at :00 -- :20 is the gap between them.
--
-- CREATED EMPTY, FIRST BUILD BY HAND. A refreshable view's first build over 1.1M persons
-- and 5.6M normalized rows is minutes of work, and the migrate client's read_timeout is
-- 300 seconds. EMPTY skips the initial refresh, so this CREATE returns in milliseconds
-- and the ledger can never be left dirty by a dropped client. The controller then runs
-- SYSTEM REFRESH VIEW corpscout.se_company_person_role and polls system.view_refreshes.
-- Until that lands the view answers with zero rows, which the backoffice panel handles
-- by falling back to the person row's own arrays.
--
-- THE SORT KEY HOLDS NO NULLABLE COLUMN (allow_nullable_key is off). role_code and
-- role_year are Nullable on the normalized row, so the SELECT unwraps both:
-- assumeNotNull is exact under the WHERE, and a dateless role lands under year 0.
--
-- THE NAME IS REUSED. corpscout.se_company_person_role was the 2026-08-19 model's role
-- table, dropped by hand in person slice 0 on 2026-09-09 -- exactly what 000398 did with
-- se_company_person. The operations script that dropped it is SPENT and must never run
-- again.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- person/tables.py::build_se_company_person_role_sql(), drift-pinned by dagster_v3
-- tests/test_se_company_person_role_view.py.

CREATE MATERIALIZED VIEW corpscout.se_company_person_role
REFRESH EVERY 1 HOUR OFFSET 20 MINUTE
ENGINE = MergeTree
ORDER BY (company_id, person_key, role_year, role_code, source, slot)
EMPTY
AS SELECT
  p.company_id AS company_id,
  p.person_key AS person_key,
  p.display_name AS display_name,
  p.birth_year AS birth_year,
  assumeNotNull(n.role_code) AS role_code,
  ifNull(n.role_year, 0) AS role_year,
  n.role_from AS role_from,
  n.role_to AS role_to,
  n.source AS source,
  n.slot AS slot,
  n.normalized_id AS normalized_id,
  has(p.current_roles, assumeNotNull(n.role_code)) AS is_current,
  p.folded_at AS folded_at
FROM corpscout.se_company_person AS p FINAL
ARRAY JOIN p.normalized_ids AS member_id
INNER JOIN corpscout.se_company_person_normalized AS n FINAL
  ON n.company_id = p.company_id AND n.normalized_id = member_id
WHERE p.active = 1 AND n.role_code IS NOT NULL;
```

Create `corpscout/clickhouse/migrations/000402_corpscout_se_company_person_role.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000402. The view owns its MergeTree (the engine is declared inside it), so one
-- DROP VIEW removes the definition and the data together and nothing is left behind.
-- Nothing else has to come back: the view is derived, so no row it held was ever the
-- only copy of anything, and the two tables it read are not this migration's business.
-- The deployed backoffice reads this name through PERSON_ROLE_SQL, and after a rollback
-- its Roles panel falls back to the person row's arrays -- the same thing it does in the
-- window between the CREATE and the first refresh.

DROP VIEW IF EXISTS corpscout.se_company_person_role;
```

- [ ] **Step 6: Run the pin test to green**

```bash
uv run --frozen --no-sync pytest tests/test_se_company_person_role_view.py -q
```

Expected: 5 passed. If `test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it` fails, the migration body and the builder differ — fix the MIGRATION, never the builder's render, by pasting the builder's output.

- [ ] **Step 7: Run the ledger suites to watch the reused name break them**

```bash
uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_se_person_retirement_drops.py -q
```

Expected: two failures, both about the name 000402 just brought back —
`test_clickhouse_migration_files_are_explicit` (the two new files are not in `EXPECTED_MIGRATIONS`) and
`test_no_up_migration_declares_a_slice_0_dropped_object` (`000402_corpscout_se_company_person_role.up.sql declares se_company_person_role`).

- [ ] **Step 8: Register 000402 and move the reused name in both guards**

In `tests/test_clickhouse_migrations.py`, add the entry at the end of `EXPECTED_MIGRATIONS` (after `"000401_corpscout_se_company_financial_entity",`, line 417):

```python
    "000402_corpscout_se_company_person_role",
```

Then, in the same file, move `"se_company_person_role"` from `SLICE_0_DROPPED_OBJECTS` (line ~4372) to `SLICE_0_KEPT_OBJECTS` and extend the comment above the first tuple. The comment currently ends "…the entity's DDL still lives in 000396 under se_company_person_v2, which is why that name, not this one, is the kept object asserted below." Append:

```python
# se_company_person_role IS ALSO LIVE AGAIN, and unlike the main table it is DECLARED by a
# migration: 000402 creates the slice-5 roles view under the name slice 0 freed, so the
# name moves to the kept list below. The guard keeps its meaning -- no up migration may
# declare a name whose object is gone -- and this one is no longer gone.
```

Remove this line from `SLICE_0_DROPPED_OBJECTS`:

```python
    "se_company_person_role",
```

and add this line to `SLICE_0_KEPT_OBJECTS` (after `"se_company_person_precedence",`):

```python
    "se_company_person_role",
```

In `tests/test_se_person_retirement_drops.py`, replace the `REUSED_NAME` block (lines ~77-83) with:

```python
# The TWO names on both lists, and the reason this file exists. corpscout.se_company_person
# was the 2026-08-19 table this script dropped on 2026-09-09, and migration 000398 gave the
# freed name to the entity's main table a day later. corpscout.se_company_person_role was
# that same model's role table, dropped in the same run, and migration 000402 gave ITS name
# to the slice-5 roles view. THE SCRIPT IS SPENT -- running it again today would destroy
# 1.1M published persons and the view built over them. It stays in the repo as history
# under the ledger policy and must never be run a second time.
REUSED_NAMES = frozenset({"se_company_person", "se_company_person_role"})
```

add the name to `KEPT`, right after `"se_company_person_precedence",`:

```python
    # The slice-5 roles view (migration 000402), living under the name this script's
    # tenth DROP took away on 2026-09-09.
    "se_company_person_role",
```

and replace `test_the_drop_script_names_no_kept_object` with:

```python
def test_the_drop_script_names_no_kept_object() -> None:
    """Whole-name comparison. A substring check would call se_company_person_role a hit on
    the entity's se_company_person_rule, or -- written the other way round -- would call
    company_person_role_type unsafe because company_person_role is being dropped.

    REUSED_NAMES is excluded rather than the check being weakened: two of this script's
    DROPs and two LIVE objects spell the same name for different objects -- dropped on
    2026-09-09, recreated by 000398's rename a day later and by 000402's view on
    2026-09-12."""
    dropped = {name for _, name in _statements(DROPS)}
    assert dropped.isdisjoint(set(KEPT) - REUSED_NAMES)
    assert len(dropped) == len(DROP_ORDER)
    assert "se_company_person_v2" not in dropped
    assert dropped & set(KEPT) == REUSED_NAMES
```

Finally update that file's module docstring: `BOTH SCRIPTS ARE SPENT: they ran on prod on 2026-09-09, and migrations 000398 and 000402 have since given TWO of their DROP names -- corpscout.se_company_person and corpscout.se_company_person_role -- to live objects. Re-running se_person_retirement_drops.sql would destroy them. See REUSED_NAMES below.`

- [ ] **Step 9: Run both suites to green**

```bash
uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_se_person_retirement_drops.py -q
```

Expected: all green (149 + the migrations file's own count, the same total as the baseline plus nothing new).

- [ ] **Step 10: Pin the constants beside the other DDL pins**

Add to `tests/test_se_company_person_tables.py`, immediately after `test_the_entity_name_is_a_prefix_of_five_siblings`:

```python
def test_the_role_view_constants_describe_the_slice_5_view() -> None:
    """Migration 000402's derived view (spec section 11). It is the SIXTH name with the
    entity's prefix and the first that is not a table, so whole-name matching applies to
    it exactly as it does to the five sibling tables above."""
    assert tables.ROLE_VIEW == "se_company_person_role"
    assert tables.QUALIFIED_ROLE_VIEW == "corpscout.se_company_person_role"
    assert tables.ROLE_VIEW.startswith(f"{tables.MAIN_TABLE}_")
    assert tables.ROLE_VIEW != tables.MAIN_TABLE
    assert tables.ROLE_VIEW_COLUMNS == (
        "company_id", "person_key", "display_name", "birth_year", "role_code", "role_year",
        "role_from", "role_to", "source", "slot", "normalized_id", "is_current", "folded_at",
    )
    assert tables.ROLE_VIEW_ORDER_BY == (
        "company_id", "person_key", "role_year", "role_code", "source", "slot",
    )
    # Every key column is one the view publishes, and every column it publishes comes from
    # one of the two tables it reads -- except is_current, which the SELECT derives.
    for column in tables.ROLE_VIEW_ORDER_BY:
        assert column in tables.ROLE_VIEW_COLUMNS, column
    for column in tables.ROLE_VIEW_COLUMNS:
        assert (
            column in tables.MAIN_COLUMNS
            or column in tables.NORMALIZED_COLUMNS
            or column == "is_current"
        ), column
```

This file's other tests read a `CREATE TABLE` block through `table_block`; the view has none — it is not a table in the ledger, its shape is the SELECT's, and `tests/test_se_company_person_role_view.py` owns that half. No import changes are needed here: `tables` is already imported at the top of the file.

Run it:

```bash
uv run --frozen --no-sync pytest tests/test_se_company_person_tables.py -q
```

Expected: the file's existing tests plus the new one, all green.

- [ ] **Step 11: Write the clickhouse-local proof**

Create `tests/test_se_company_person_role_view_clickhouse_local.py`:

```python
"""The roles-as-rows SELECT against a real ClickHouse (clickhouse-local).

Two claims a text pin cannot settle (spec 2026-09-09 section 11):

1. THE DDL IS LEGAL. The view declares its engine inline and sorts on
   (company_id, person_key, role_year, role_code, source, slot), while role_code and
   role_year are Nullable on the normalized row and allow_nullable_key is off. This file
   builds the view's inner table the way ClickHouse builds it -- CREATE TABLE ... ENGINE =
   MergeTree ORDER BY (...) AS <the SELECT> -- so a Nullable column reaching the sort key
   fails HERE, not on the prod CREATE, and the column types are read back out of
   system.columns.
2. THE SELECT SAYS WHAT IT MEANS. One row per published ACTIVE person and per
   role-carrying observation the fold built them from: a roleless observation contributes
   nothing, an observation no published person was folded from contributes nothing, an
   inactive person contributes nothing, a dateless role lands under year 0, and
   is_current is 1 exactly for the codes on the person's current_roles.

Both join_use_nulls settings run, and here that is not a formality: this SELECT makes a
real INNER JOIN, which is the statement the setting changes.

The tables come from migration 000396's own DDL, with 000398's rename replayed over the
main table (the ledger policy leaves 000396 declaring se_company_person_v2), exactly as
tests/test_se_company_person_fold_clickhouse_local.py does it.
"""

import json
import subprocess

import pytest

from dagster_v3.defs.se_company.person import tables
from tests.clickhouse_local import clickhouse_local_command
from tests.se_company_ddl import table_block

pytestmark = pytest.mark.integration

COMPANY = "5560000001"
ANNA = "a" * 64          # active, four members, three of them with a role
CARL = "b" * 64          # active, one dateless Wikidata role
ERIK = "c" * 64          # INACTIVE (hidden): contributes no row at all
N_BOARD = "1" * 64       # Anna, bolagsverket, board_member 2025 -- a current role
N_CHAIR = "2" * 64       # Anna, esef, board_chair 2025 -- a current role
N_AUDIT = "3" * 64       # Anna, bolagsverket, auditor 2019 -- held once, not current
N_NOROLE = "4" * 64      # Anna, bolagsverket, role_code NULL -- 2.0M such rows on prod
N_CEO = "5" * 64         # Carl, wikidata, no year at all, an open span from 2019-05-01
N_HIDDEN = "6" * 64      # Erik's only observation
N_ORPHAN = "7" * 64      # a normalized row no published person was folded from
FOLDED_AT = "2026-09-11 09:00:00.000"

MAIN_INSERT_COLUMNS = (
    "company_id, person_key, display_name, first_name, last_name, birth_year, "
    "sources, slots, normalized_ids, current_roles, active, inactive_reason, "
    "text_source, folded_at, fold_version, source_run_id"
)
NORMALIZED_INSERT_COLUMNS = (
    "company_id, source, slot, suggestion_id, normalized_id, normalizer_version, "
    "parse_status, display_first, display_last, display_name, birth_year, role_code, "
    "role_key, role_year, role_from, role_to, normalized_at"
)


def _main_row(
    person_key: str,
    display_name: str,
    birth_year: str,
    slots: str,
    normalized_ids: str,
    current_roles: str,
    active: int,
    inactive_reason: str,
) -> str:
    return (
        f"('{COMPANY}', '{person_key}', '{display_name}', 'Anna', 'Svensson', {birth_year}, "
        f"['bolagsverket'], {slots}, {normalized_ids}, {current_roles}, {active}, "
        f"'{inactive_reason}', 'bolagsverket', toDateTime64('{FOLDED_AT}', 3, 'UTC'), "
        "'se-person-fold-v1', 'run-fold')"
    )


def _normalized_row(
    source: str,
    slot: str,
    normalized_id: str,
    display_name: str,
    role_code: str,
    role_year: str,
    role_from: str = "NULL",
    role_to: str = "NULL",
) -> str:
    return (
        f"('{COMPANY}', '{source}', '{slot}', '{normalized_id}', '{normalized_id}', "
        f"'se-person-normalizer-v1', 'ok', 'Anna', 'Svensson', '{display_name}', NULL, "
        f"{role_code}, NULL, {role_year}, {role_from}, {role_to}, "
        "toDateTime64('2026-09-11 08:00:00.000', 3, 'UTC'))"
    )


def _script(join_use_nulls: int) -> str:
    parts = [
        f"SET join_use_nulls = {join_use_nulls};",
        "CREATE DATABASE IF NOT EXISTS corpscout;",
        # 000396 declares the main table under its build name; 000398 renames the DEPLOYED
        # table and never edits that file, so the local schema replays the rename.
        table_block("se_company_person_v2").replace(
            "corpscout.se_company_person_v2", tables.QUALIFIED_MAIN_TABLE
        ),
        table_block("se_company_person_normalized"),
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({MAIN_INSERT_COLUMNS}) VALUES\n"
        + ",\n".join(
            (
                _main_row(
                    ANNA, "Anna Svensson", "1975",
                    "['uid-1:sig-1', 'doc-9:cand-1', 'uid-1:sig-9', 'uid-1:sig-7']",
                    f"['{N_BOARD}', '{N_CHAIR}', '{N_AUDIT}', '{N_NOROLE}']",
                    "['board_chair', 'board_member']", 1, "",
                ),
                _main_row(
                    CARL, "Carl von Essen", "NULL", "['Q1:P169:Q7']",
                    f"['{N_CEO}']", "['chief_executive_officer']", 1, "",
                ),
                _main_row(
                    ERIK, "Erik Larsson", "NULL", "['uid-2:sig-1']",
                    f"['{N_HIDDEN}']", "['auditor']", 0, "hidden",
                ),
            )
        )
        + ";",
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} ({NORMALIZED_INSERT_COLUMNS}) VALUES\n"
        + ",\n".join(
            (
                _normalized_row("bolagsverket", "uid-1:sig-1", N_BOARD, "Anna Svensson", "'board_member'", "2025"),
                _normalized_row("esef", "doc-9:cand-1", N_CHAIR, "Anna Maria Svensson", "'board_chair'", "2025"),
                _normalized_row("bolagsverket", "uid-1:sig-9", N_AUDIT, "Anna Svensson", "'auditor'", "2019"),
                _normalized_row("bolagsverket", "uid-1:sig-7", N_NOROLE, "Anna Svensson", "NULL", "2024"),
                _normalized_row(
                    "wikidata", "Q1:P169:Q7", N_CEO, "Carl von Essen",
                    "'chief_executive_officer'", "NULL", "toDate('2019-05-01')",
                ),
                _normalized_row("bolagsverket", "uid-2:sig-1", N_HIDDEN, "Erik Larsson", "'auditor'", "2022"),
                _normalized_row("bolagsverket", "uid-9:sig-9", N_ORPHAN, "Orphan Row", "'board_member'", "2025"),
            )
        )
        + ";",
        # The view's inner table, built exactly as an engine-in-view refreshable MV builds
        # it. This is the statement that fails if a Nullable column reaches the sort key.
        f"CREATE TABLE {tables.QUALIFIED_ROLE_VIEW}\nENGINE = MergeTree\n"
        f"ORDER BY ({', '.join(tables.ROLE_VIEW_ORDER_BY)})\n"
        f"AS {tables.build_se_company_person_role_sql()};",
        # The ORDER BY has to sit in a subquery: `position` is not an aggregate and
        # ClickHouse refuses it beside groupArray (code 215, NOT_AN_AGGREGATE).
        "SELECT groupArray(concat(name, ' ', type)) AS columns FROM (SELECT name, type "
        f"FROM system.columns WHERE database = 'corpscout' AND table = '{tables.ROLE_VIEW}' "
        "ORDER BY position) FORMAT JSONEachRow;",
        f"SELECT * FROM {tables.QUALIFIED_ROLE_VIEW} "
        "ORDER BY person_key, role_year, role_code FORMAT JSONEachRow;",
    ]
    return "\n".join(parts) + "\n"


@pytest.fixture(
    scope="module",
    params=(0, 1),
    ids=("join_use_nulls_off", "join_use_nulls_on"),
)
def view(request: pytest.FixtureRequest) -> tuple[list[str], list[dict]]:
    command = clickhouse_local_command()
    try:
        completed = subprocess.run(
            command,
            input=_script(request.param),
            capture_output=True,
            text=True,
            timeout=900,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    columns: list[str] = []
    rows: list[dict] = []
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        parsed = json.loads(line)
        if "columns" in parsed:
            columns = parsed["columns"]
        else:
            rows.append(parsed)
    assert columns, completed.stdout
    return columns, rows


def test_the_sort_key_columns_are_not_nullable_and_the_rest_are_as_declared(
    view: tuple[list[str], list[dict]],
) -> None:
    columns, _ = view
    types = dict(column.split(" ", 1) for column in columns)

    assert list(types) == list(tables.ROLE_VIEW_COLUMNS)
    for key_column in tables.ROLE_VIEW_ORDER_BY:
        assert not types[key_column].startswith("Nullable"), key_column
    # The two the SELECT unwraps, exactly: a String role code and a UInt16 year.
    assert types["role_code"] == "String"
    assert types["role_year"] == "UInt16"
    # Outside the key, Nullable survives on purpose -- a missing birth year and an open
    # span must read as NULL, never as a zero or an empty date.
    assert types["birth_year"] == "Nullable(UInt16)"
    assert types["role_from"] == "Nullable(Date)"
    assert types["role_to"] == "Nullable(Date)"
    assert types["is_current"] == "UInt8"


def test_one_row_per_active_person_and_role_carrying_observation(
    view: tuple[list[str], list[dict]],
) -> None:
    _, rows = view

    assert [
        (row["person_key"], row["role_code"], row["role_year"], row["source"], row["is_current"])
        for row in rows
    ] == [
        (ANNA, "auditor", 2019, "bolagsverket", 0),
        (ANNA, "board_chair", 2025, "esef", 1),
        (ANNA, "board_member", 2025, "bolagsverket", 1),
        # A role with no year at all lands under 0 -- the view's way of saying "dateless",
        # where the person row's role_years array says "held now" instead.
        (CARL, "chief_executive_officer", 0, "wikidata", 1),
    ]
    anna = rows[0]
    assert anna["company_id"] == COMPANY
    assert anna["display_name"] == "Anna Svensson"
    assert anna["birth_year"] == 1975
    assert anna["normalized_id"] == N_AUDIT
    assert anna["slot"] == "uid-1:sig-9"
    assert anna["folded_at"].startswith("2026-09-11 09:00:00")
    carl = rows[3]
    assert carl["birth_year"] is None
    assert carl["role_from"] == "2019-05-01"
    assert carl["role_to"] is None


def test_roleless_unfolded_and_inactive_observations_contribute_nothing(
    view: tuple[list[str], list[dict]],
) -> None:
    _, rows = view
    ids = {row["normalized_id"] for row in rows}

    assert N_NOROLE not in ids      # role_code IS NULL -- the WHERE drops it
    assert N_ORPHAN not in ids      # no person names it in normalized_ids -- the JOIN drops it
    assert N_HIDDEN not in ids      # its person is inactive -- the WHERE drops it
    assert all(row["person_key"] != ERIK for row in rows)
```

- [ ] **Step 12: Run the clickhouse-local proof**

```bash
uv run --frozen --no-sync pytest tests/test_se_company_person_role_view_clickhouse_local.py -q
```

Expected: 6 passed (3 tests × 2 `join_use_nulls` settings). Docker must be running — the machine has no `clickhouse-local` binary, so `tests/clickhouse_local.py` pulls `clickhouse/clickhouse-server:26.5`. If the module SKIPS, say it skipped; never report a skip as a pass. If ClickHouse rejects the `ON` clause after `ARRAY JOIN`, or rejects the sort key, fix the BUILDER and re-copy its render into the migration — the pin test in step 6 will tell you if the two halves diverge.

- [ ] **Step 13: Record the view in the package doc**

In `src/dagster_v3/defs/se_company/person/docs/person-design.md`:

1. Title line 1: `# se_company.person (slices 0-5)`.
2. The `tables.py` row of the module table: append to its Responsibility cell `; also the slice-5 role view's name and SELECT (ROLE_VIEW, build_se_company_person_role_sql, migration 000402)`.
3. Insert this section immediately before `## Known limits`:

```markdown
## Roles as rows (`se_company_person_role`, slice 5)

`corpscout.se_company_person_role` (migration 000402, spec section 11) is a REFRESHABLE
materialized view: `REFRESH EVERY 1 HOUR OFFSET 20 MINUTE`, `ENGINE = MergeTree ORDER BY
(company_id, person_key, role_year, role_code, source, slot)`, created `EMPTY` so the
migration returns at once and the first build is an explicit `SYSTEM REFRESH VIEW`. The
engine is declared inside the view, so the name IS the table readers query. Its SELECT
lives in `tables.py::build_se_company_person_role_sql()` and the migration body is pinned
against a fresh render by `tests/test_se_company_person_role_view.py`, exactly as the
serving view is pinned.

One row per published ACTIVE person and per role-carrying normalized observation the fold
built them from: `ARRAY JOIN` over the main row's `normalized_ids`, `INNER JOIN` back to
`se_company_person_normalized`, `WHERE p.active = 1 AND n.role_code IS NOT NULL`. Prod
2026-09-12: 3,837,006 rows over 1,113,485 persons; 160,279 active persons hold no role and
get no row. Nothing writes it -- the fold, the normalizer, the rules and the precedence do
not know it exists -- and the person row's role arrays stay the fold's own summary.

Four things to know before reading it:

- **It lags a fold by up to an hour.** The view rebuilds at :20, so a person folded at :25
  keeps their previous rows until the next :20 and a brand-new person has none at all. The
  backoffice panel falls back to the person row's arrays when the view has no row for a
  person. If the lag ever stops being acceptable the same SELECT moves into the fold.
- **It follows the FOLD's members, not today's normalized rows.** `normalized_ids` names
  the versions the CURRENT published row was folded from, so an observation re-normalized
  since then (the tab's re-fold-pending badge) drops out until the next fold.
- **`role_year` 0 means "no year at all".** The column is `UInt16` because the year is in
  the sort key and `allow_nullable_key` is off. A dateless role (290 of Wikidata's 466 on
  prod) lands under 0 here, while the fold's `role_years` array puts it under the current
  year -- "taken as held now" is the fold's ruling, not this view's.
- **The name is reused.** It was the 2026-08-19 model's role table, dropped by hand in
  slice 0. The spent script `clickhouse/operations/se_person_retirement_drops.sql` still
  names it and must never be run again.
```

- [ ] **Step 14: Run everything this task touches, then commit**

```bash
uv run --frozen --no-sync dg check defs
uv run --frozen --no-sync pytest tests/test_se_company_person_role_view.py \
  tests/test_se_company_person_role_view_clickhouse_local.py \
  tests/test_se_company_person_tables.py tests/test_clickhouse_migrations.py \
  tests/test_se_person_retirement_drops.py tests/test_se_companies_serving_mv.py \
  tests/test_se_company_person_fold.py tests/test_se_company_person_batch.py -q
```

Expected: `dg check defs` green (no definition changed, but the package imports), and every suite green — 154 from the baseline plus the new file's 5 and the local proof's 6.

```bash
cat > /tmp/person-roles-task1.txt <<'MSG'
feat(se-person): roles as rows — migration 000402 and its drift pin

corpscout.se_company_person_role is a refreshable materialized view over the
person entity's main and normalized tables: one row per published active person
and per role-carrying observation the fold built them from, rebuilt hourly at :20
and created EMPTY so the first build is an explicit SYSTEM REFRESH VIEW.

The SELECT lives in person/tables.py::build_se_company_person_role_sql() and the
migration body is pinned against a fresh render, as the serving view is. role_code
and role_year are unwrapped for the sort key (allow_nullable_key is off), so a
dateless role publishes under year 0. A clickhouse-local proof builds the inner
table from the render and checks the rows and the column types under both
join_use_nulls settings.

The view takes back the name slice 0 freed on 2026-09-09, so both guards that knew
that name move: the ledger's slice-0 dropped list and the spent drop script's
kept list, which now carries two reused names.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/clickhouse/migrations/000402_corpscout_se_company_person_role.up.sql \
  corpscout/clickhouse/migrations/000402_corpscout_se_company_person_role.down.sql \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/tables.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/docs/person-design.md \
  corpscout/services/dagster_v3/tests/test_se_company_person_role_view.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_role_view_clickhouse_local.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_tables.py \
  corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
  corpscout/services/dagster_v3/tests/test_se_person_retirement_drops.py \
  corpscout/services/dagster_v3/tests/test_se_companies_serving_mv.py
git commit -F /tmp/person-roles-task1.txt
```

(Run the two `git` commands from the worktree root, not from `dagster_v3`.)

---

### Task 2: The People tab reads the roles view

The company's People tab (`/admin/se/company/:companyId/people`) loads its whole state in one `Promise.all` of seven reads and renders each published person in `PersonPanel`. This task adds the eighth read and replaces the Roles section's source of truth, keeping the array-derived block as the fallback for the window in which the view has not rebuilt.

**Files:**
- Modify: `app/lib/se-person-tables.ts` (one constant)
- Modify: `app/lib/se-company-person-entity.server.ts` (`SePersonRoleRow` near line 222, `PERSON_ROLE_SQL` after line 394, `SePersonPublished.roleRows` at line 231, `loadSePersonDetail` lines 698-726)
- Modify: `app/components/admin/se-person-workspace.tsx` (the Roles `<section>`, lines 1275-1300)
- Modify: `tests/se-company-person-entity.server.test.ts` (imports, the `ROLE_ROWS` fixture, `answer(sql)`, two assertions, one new test)
- Modify: `tests/admin-se-company-person.test.tsx` (the `published` fixture, one new test)

**Interfaces:**
- Consumes from Task 1: the view's name and column list — `corpscout.se_company_person_role` with `company_id, person_key, display_name, birth_year, role_code, role_year, role_from, role_to, source, slot, normalized_id, is_current, folded_at`, `role_year = 0` meaning "no year at all".
- Produces: `SE_COMPANY_PERSON_ROLE_TABLE`, `SePersonRoleRow`, `PERSON_ROLE_SQL`, `SePersonPublished.roleRows: SePersonRoleRow[]`.

- [ ] **Step 1: Write the failing loader tests**

In `tests/se-company-person-entity.server.test.ts`, add `PERSON_ROLE_SQL` to the import list (alphabetical, between `PERSON_RAW_SQL` and `PERSON_RULES_SQL`) and `type SePersonRoleRow` to the type imports. Add the fixture right after `CALL_NAME_MATCH`:

```ts
/** The roles view's rows for this company (spec section 11), in the view's own order:
 * the merged person's two current roles and one she no longer holds, then the Wikidata
 * person's DATELESS role, which the view publishes under year 0. */
const ROLE_ROWS: SePersonRoleRow[] = [
  {
    company_id: COMPANY, person_key: MERGED_KEY, role_code: "board_chair", role_year: 2025,
    role_from: "", role_to: "", source: "esef", slot: "doc-9:cand-1", normalized_id: "n2",
    is_current: 1,
  },
  {
    company_id: COMPANY, person_key: MERGED_KEY, role_code: "board_member", role_year: 2025,
    role_from: "", role_to: "", source: "bolagsverket", slot: "uid-1:sig-1",
    normalized_id: "n1", is_current: 1,
  },
  {
    company_id: COMPANY, person_key: MERGED_KEY, role_code: "auditor", role_year: 2019,
    role_from: "", role_to: "", source: "bolagsverket", slot: "uid-1:sig-4",
    normalized_id: "n5", is_current: 0,
  },
  {
    company_id: COMPANY, person_key: WIKI_KEY, role_code: "chief_executive_officer",
    role_year: 0, role_from: "2019-05-01", role_to: "", source: "wikidata",
    slot: "Q1:P169:Q7", normalized_id: "n3", is_current: 1,
  },
];
```

Add the dispatch branch to `answer(sql)`, directly under the main-row branch — the view's name has the main table's as a PREFIX, which is why every branch here carries the alias that follows the name:

```ts
  if (sql.includes("FROM corpscout.se_company_person_role AS r")) return ROLE_ROWS;
```

In `it("pins every read to the entity's tables, …")`, add at the end:

```ts
    // The eighth read (spec section 11): the roles view. NO FINAL -- every refresh
    // rebuilds a plain MergeTree whole, so it holds exactly one version of every row.
    expect(PERSON_ROLE_SQL).toContain("FROM corpscout.se_company_person_role AS r");
    expect(PERSON_ROLE_SQL).not.toContain("FINAL");
    expect(PERSON_ROLE_SQL).toContain("toString(r.person_key) AS person_key");
    expect(PERSON_ROLE_SQL).toContain("toUInt16(r.role_year) AS role_year");
    expect(PERSON_ROLE_SQL).toContain("ifNull(toString(r.role_from), '') AS role_from");
    expect(PERSON_ROLE_SQL).toContain("ifNull(toString(r.role_to), '') AS role_to");
    expect(PERSON_ROLE_SQL).toContain("toUInt8(r.is_current) AS is_current");
    expect(PERSON_ROLE_SQL).toContain("WHERE r.company_id = {companyId:String}");
    expect(PERSON_ROLE_SQL).toContain(
      "ORDER BY r.person_key, r.role_year DESC, r.role_code, r.source",
    );
```

Add `PERSON_ROLE_SQL` to the two `for (const sql of [...])` loops that already list the seven reads: the `{companyId:String}` loop at the end of this pins test, and the parameter loop at the end of `it("assembles the published persons, …")`.

In `it("assembles the published persons, their members, roles, rules and the drafts", …)`, after the existing `merged?.roles` assertion:

```ts
    // The view's own rows, grouped by person and left in the view's order. The
    // array-derived `roles` block above is untouched: it stays the fold's summary and
    // the panel's fallback.
    expect(merged?.roleRows).toEqual(ROLE_ROWS.slice(0, 3));
    expect(detail?.published[1]?.roleRows).toEqual([ROLE_ROWS[3]]);
```

And add a new test right after it:

```ts
  it("leaves roleRows empty when the view has not rebuilt since the fold", async () => {
    // The view refreshes at :20 and a fold can land at any minute, so a freshly folded
    // person legitimately has no row for up to an hour -- and a brand-new person has
    // none at all. That is a fallback, never an error.
    clickhouse.query.mockImplementation(async (sql: string) =>
      sql === PERSON_ROLE_SQL ? [] : answer(sql),
    );

    const detail = await loadSePersonDetail(COMPANY);

    expect(detail?.published.map((entry) => entry.roleRows)).toEqual([[], []]);
    expect(detail?.published[0]?.roles).toEqual([
      { code: "board_chair", year: 2025, sources: ["esef"] },
      { code: "board_member", year: 2025, sources: ["bolagsverket"] },
    ]);
  });
```

- [ ] **Step 2: Run them to watch them fail**

```bash
cd corpscout/services/backoffice
npx vitest run tests/se-company-person-entity.server.test.ts
```

Expected: the file fails to typecheck/import — `PERSON_ROLE_SQL` and `SePersonRoleRow` are not exported by `~/lib/se-company-person-entity.server`.

- [ ] **Step 3: Add the table constant**

In `app/lib/se-person-tables.ts`, after `SE_COMPANY_PERSON_TABLE`:

```ts
/**
 * The roles-as-rows view (migration 000402, spec section 11): one row per published
 * ACTIVE person and per role-carrying observation the fold built them from, rebuilt
 * hourly at :20 by a refreshable materialized view.
 *
 * READ IT WITHOUT `FINAL`. It is a plain MergeTree that each refresh rebuilds whole, so
 * it never holds two versions of a row -- and it is NOT a `ReplacingMergeTree`, so
 * `FINAL` on it would be a plain error.
 *
 * MIND THE PREFIX: this name has `SE_COMPANY_PERSON_TABLE` as a prefix, so a match on
 * either one has to carry the alias that follows it.
 */
export const SE_COMPANY_PERSON_ROLE_TABLE = "corpscout.se_company_person_role";
```

- [ ] **Step 4: Add the row type, the SQL and the eighth read**

In `app/lib/se-company-person-entity.server.ts`:

1. Extend the import from `~/lib/se-person-tables`:

```ts
import {
  SE_COMPANY_PERSON_ROLE_TABLE,
  SE_COMPANY_PERSON_TABLE,
} from "~/lib/se-person-tables";
```

2. Add the row type immediately after `SePersonRoleYear` (line 222-226):

```ts
/** One row of `corpscout.se_company_person_role` (spec section 11): one role
 * observation of one published person, with the observation that carried it. Unlike
 * `SePersonRoleYear` -- the fold's summary, zipped out of the person row's parallel
 * arrays -- this is a stored row per (role, year, source, slot), so it also carries the
 * span dates and the slot the reviewer can look up. `role_year` is 0 when the
 * observation carried no year at all (the view's `ifNull(role_year, 0)`), and
 * `is_current` is 1 when the code is on the person's `current_roles`. */
export interface SePersonRoleRow {
  company_id: string;
  person_key: string;
  role_code: string;
  role_year: number;
  role_from: string;
  role_to: string;
  source: string;
  slot: string;
  normalized_id: string;
  is_current: number;
}
```

3. Add `roleRows` to `SePersonPublished`, right after `roles` (line 231):

```ts
  /** The same roles as stored ROWS, from the refreshable view (spec section 11). Empty
   * when the view has not rebuilt since this person was folded -- the panel then falls
   * back to `roles`. */
  roleRows: SePersonRoleRow[];
```

4. Add the read after `PERSON_MATCH_SQL` (line 394):

```ts
/**
 * The eighth read (spec section 11): this company's role rows, from the refreshable
 * view migration 000402 creates. NO `FINAL` -- the view's storage is a plain MergeTree
 * that every refresh rebuilds whole, so it holds exactly one version of every row and
 * `FINAL` would be an error, not a precaution.
 *
 * The view lags a fold by at most an hour (it rebuilds at :20), so a company folded
 * moments ago can answer with nothing. That is why the panel keeps the person row's own
 * role arrays as a fallback rather than showing an empty Roles section.
 */
export const PERSON_ROLE_SQL = `SELECT
  r.company_id AS company_id, toString(r.person_key) AS person_key,
  r.role_code AS role_code, toUInt16(r.role_year) AS role_year,
  ifNull(toString(r.role_from), '') AS role_from, ifNull(toString(r.role_to), '') AS role_to,
  toString(r.source) AS source, r.slot AS slot,
  toString(r.normalized_id) AS normalized_id, toUInt8(r.is_current) AS is_current
FROM ${SE_COMPANY_PERSON_ROLE_TABLE} AS r
WHERE r.company_id = {companyId:String}
ORDER BY r.person_key, r.role_year DESC, r.role_code, r.source`;
```

5. In `loadSePersonDetail` (line 698), take the eighth read and group it:

```ts
  const [mainRows, history, normalizedRows, rawRows, rules, precedence, matchRows, roleRows] =
    await Promise.all([
      chQuery<SePersonRow>(PERSON_MAIN_SQL, { companyId }),
      chQuery<SePersonHistoryRow>(PERSON_HISTORY_SQL, { companyId }),
      chQuery<SePersonNormalizedRow>(PERSON_NORMALIZED_SQL, { companyId }),
      chQuery<SePersonRawRow>(PERSON_RAW_SQL, { companyId }),
      chQuery<SePersonRuleRow>(PERSON_RULES_SQL, { companyId }),
      chQuery<SePersonPrecedenceRow>(PERSON_PRECEDENCE_SQL, { companyId }),
      chQuery<SePersonMatchRow>(PERSON_MATCH_SQL, { companyId }),
      chQuery<SePersonRoleRow>(PERSON_ROLE_SQL, { companyId }),
    ]);
```

then, beside the two existing `Map`s (`normalizedBySlot`, `rawBySlot`):

```ts
  // One pass over the company's role rows, keyed by person: the view returns them in
  // (person, year desc, code, source) order and the panel renders them in that order.
  const roleRowsByKey = new Map<string, SePersonRoleRow[]>();
  for (const role of roleRows) {
    const rows = roleRowsByKey.get(role.person_key);
    if (rows === undefined) roleRowsByKey.set(role.person_key, [role]);
    else rows.push(role);
  }
```

and add one line to the object the `mainRows.map` callback returns, right after the `roles:` block:

```ts
      roleRows: roleRowsByKey.get(row.person_key) ?? [],
```

- [ ] **Step 5: Run the loader tests to green**

```bash
npx vitest run tests/se-company-person-entity.server.test.ts
```

Expected: the whole file green, including the two new assertions and the fallback test.

- [ ] **Step 6: Write the failing panel test**

In `tests/admin-se-company-person.test.tsx`, add `roleRows` to the `published` fixture, right after its `roles` line:

```ts
  roleRows: [
    {
      company_id: COMPANY, person_key: KEY, role_code: "board_member", role_year: 2025,
      role_from: "", role_to: "", source: "bolagsverket", slot: "uid-1:sig-1",
      normalized_id: "n1", is_current: 1,
    },
    {
      company_id: COMPANY, person_key: KEY, role_code: "auditor", role_year: 0,
      role_from: "2019-05-01", role_to: "2021-03-31", source: "wikidata",
      slot: "Q1:P169:Q7", normalized_id: "n3", is_current: 0,
    },
  ],
```

and add this test after `it("renders the workspace: the persons, their sources, roles and the fold state", …)`:

```ts
  it("lists the roles view's rows in the panel, and falls back to the row's arrays when the view has none", () => {
    const html = render(
      <SePersonWorkspace companyId={COMPANY} detail={detail} roleOptions={ROLE_OPTIONS} selectedKey={KEY} result={null} />,
    );
    // A stored row per (role, year, source, slot): the catalog label, the year, the
    // span when the observation carried one, and the source that saw it.
    expect(html).toContain("Board member");
    expect(html).toContain("2025");
    expect(html).toContain("2019-05-01");
    expect(html).toContain("2021-03-31");
    expect(html).toContain("Wikidata");
    expect(html).not.toContain("the roles view has not rebuilt");

    // No row yet -- a person folded since the view's last :20 rebuild. The panel shows
    // the person row's own arrays and says so, instead of claiming the person has no
    // role at all.
    const fallback = render(
      <SePersonWorkspace
        companyId={COMPANY}
        detail={{ ...detail, published: [{ ...published, roleRows: [] }] }}
        roleOptions={ROLE_OPTIONS}
        selectedKey={KEY}
        result={null}
      />,
    );
    expect(fallback).toContain("Board member");
    expect(fallback).toContain("the roles view has not rebuilt");
  });
```

- [ ] **Step 7: Run it to watch it fail**

```bash
npx vitest run tests/admin-se-company-person.test.tsx
```

Expected: the new test fails on `expect(html).toContain("2019-05-01")` (the panel still renders only the array-derived block, which has no dates) and on the missing fallback sentence.

- [ ] **Step 8: Render the view's rows in `PersonPanel`**

In `app/components/admin/se-person-workspace.tsx`, replace the whole Roles `<section>` (lines 1275-1300, the one whose heading is `Roles`) with:

```tsx
          <section>
            <h3 className="text-muted-foreground text-xs uppercase tracking-wide">Roles</h3>
            {entry.roleRows.length > 0 ? (
              <ul className="mt-2 flex flex-col gap-1 text-sm">
                {entry.roleRows.map((role) => (
                  <li
                    key={`${role.source}|${role.slot}|${role.role_code}|${role.role_year}`}
                    className="grid gap-x-3 sm:grid-cols-[4rem_1fr]"
                  >
                    <span className="text-muted-foreground font-mono text-xs">
                      {role.role_year === 0 ? EMPTY_VALUE : role.role_year}
                    </span>
                    <span className="flex flex-wrap items-center gap-2">
                      <Badge variant={role.is_current === 1 ? "default" : "outline"}>
                        {roleLabel(role.role_code, roleOptions)}
                      </Badge>
                      {role.role_from === "" && role.role_to === "" ? null : (
                        <span className="text-muted-foreground font-mono text-xs">
                          {role.role_from === "" ? "?" : role.role_from}
                          {" – "}
                          {role.role_to === "" ? "" : role.role_to}
                        </span>
                      )}
                      <span className="text-muted-foreground text-xs">
                        {personSourceLabel(role.source)}
                      </span>
                    </span>
                  </li>
                ))}
              </ul>
            ) : entry.roles.length === 0 ? (
              <p className="mt-1 text-sm">none</p>
            ) : (
              <>
                <ul className="mt-2 flex flex-col gap-1 text-sm">
                  {rolesByYear(entry.roles).map(([year, entries]) => (
                    <li key={year} className="grid gap-x-3 sm:grid-cols-[4rem_1fr]">
                      <span className="text-muted-foreground font-mono text-xs">
                        {year === 0 ? EMPTY_VALUE : year}
                      </span>
                      <span className="flex flex-wrap items-center gap-2">
                        {entries.map((role) => (
                          <span key={role.code} className="flex items-center gap-1">
                            <Badge variant="outline">{roleLabel(role.code, roleOptions)}</Badge>
                            <span className="text-muted-foreground text-xs">
                              {role.sources.map(personSourceLabel).join(", ")}
                            </span>
                          </span>
                        ))}
                      </span>
                    </li>
                  ))}
                </ul>
                <p className="text-muted-foreground mt-1 text-xs">
                  From the person row's own arrays — the roles view has not rebuilt since
                  this fold.
                </p>
              </>
            )}
          </section>
```

Nothing else in the file changes: `rolesByYear`, `roleLabel`, `personSourceLabel`, `EMPTY_VALUE` and `Badge` are already imported and the fallback branch is the block that was there before, verbatim.

- [ ] **Step 9: Run both suites, typecheck, commit**

```bash
npx vitest run tests/admin-se-company-person.test.tsx tests/se-company-person-entity.server.test.ts \
  tests/se-person-edit-sheet.test.tsx tests/admin-se-people.test.tsx tests/se-people-list.server.test.ts
npm run typecheck
```

Expected: all five suites green and `npm run typecheck` clean. (The two files that are red at the branch point — `tests/queries.server.test.ts` and `tests/admin-se-company-esef.test.tsx` — are not in this list and stay untouched.)

```bash
cat > /tmp/person-roles-task2.txt <<'MSG'
feat(backoffice): the People tab reads the roles view

loadSePersonDetail takes an eighth read -- PERSON_ROLE_SQL over
corpscout.se_company_person_role, no FINAL, one round trip with the other
seven -- and groups its rows onto each published person as `roleRows`.

The person panel's Roles section lists those rows: role, year (0 shown as an
em dash), the span when the observation carried one, and the source, with the
badge filled for a role the person currently holds. When the view has no row for
a person -- the hour between a fold and the next :20 rebuild -- the section falls
back to the person row's own arrays and says so.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/services/backoffice/app/lib/se-person-tables.ts \
  corpscout/services/backoffice/app/lib/se-company-person-entity.server.ts \
  corpscout/services/backoffice/app/components/admin/se-person-workspace.tsx \
  corpscout/services/backoffice/tests/se-company-person-entity.server.test.ts \
  corpscout/services/backoffice/tests/admin-se-company-person.test.tsx
git commit -F /tmp/person-roles-task2.txt
```

---

### Task 3: Prod run (controller)

No new code. **No dagster deploy is required by this slice** — nothing in the definitions tree reads the view (`ROLE_VIEW` and the builder are used by the migration and the tests only), and no asset, job, schedule or pool changes. The next deploy of `main` for any other reason picks the constants up. The backoffice runs locally from the owner's main checkout (memory `backoffice-runs-locally`), so its "deploy" is the merge.

The order is: **migrate first, then merge** (final-review ruling: the merge is the backoffice deploy — the owner's dev server serves `main` — and the loader's eighth read would hit UNKNOWN_TABLE in the window; the read now degrades to the fallback, but the migration still goes first: run `make -s -C <worktree>/corpscout clickhouse-migrate-up-one` from this branch's checkout, check the ledger, then merge), force the first refresh, read it out, smoke the tab.

1. [ ] **Whole-branch review, then merge to `main`.** If `main` has taken 000402 in the meantime, renumber FIRST (both migration files, `EXPECTED_MIGRATIONS`, `MIGRATION` in `tests/test_se_company_person_role_view.py`, the `000402` mentions in `person-design.md` and in the spec's section 11), then re-run:

   ```bash
   cd corpscout/services/dagster_v3
   set -a && . ./.env && set +a
   uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py \
     tests/test_se_company_person_role_view.py tests/test_se_company_person_tables.py \
     tests/test_se_person_retirement_drops.py -q
   ```

   Merge from the main checkout when it is on `main`; if it is not, merge through the deploy worktree (`git -C <scratch>/deploy-worktree checkout main && git -C <scratch>/deploy-worktree merge se-person-roles`) and leave the owner's checkout alone.

2. [ ] **Apply the migration from the merged `main` checkout.** No refresh window to dodge: this migration touches no existing view, and `se_companies_serving` (:45) is not named anywhere in it.

   ```bash
   make -s -C corpscout clickhouse-migrate-up-one
   make -s -C corpscout clickhouse-migrate-version
   ```

   Expected: the first returns in **seconds** (the view is created `EMPTY`, there is no `SYSTEM WAIT VIEW`), and the version reads `402` with no `(dirty)`. If the client ever does drop, check `SELECT name FROM system.tables WHERE database = 'corpscout' AND name = 'se_company_person_role'`: if the view is there, `make -s -C corpscout clickhouse-migrate-force VERSION=402`; if it is not, re-run the up-one.

3. [ ] **Force the first build and watch it land.** The view is empty until this runs (or until the next :20 tick, whichever comes first):

   ```bash
   ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SYSTEM REFRESH VIEW corpscout.se_company_person_role\""
   ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT view, status, toString(last_success_time), toString(last_refresh_time), toString(next_refresh_time), exception FROM system.view_refreshes WHERE database = 'corpscout' AND view = 'se_company_person_role'\""
   ```

   Poll the second command until `status` is `Scheduled` with an empty `exception` and `last_success_time` set (a failed attempt sets only `last_refresh_time`). **Do not use `SYSTEM WAIT VIEW`** — it blocks for the whole build and is exactly the statement that outlived a client at 000391. If `exception` is `MEMORY_LIMIT_EXCEEDED` or a join error, the fix is a follow-up migration adding `SETTINGS join_algorithm = 'grace_hash,hash'` to the SELECT (the serving view's shape), not an edit to the applied one — and the owner decides.

4. [ ] **Read it out.** Four numbers, all of them checkable against spec section 11:

   ```sql
   -- a) the view's own size
   SELECT count() AS rows,
          uniqExact(person_key) AS persons,
          uniqExact(company_id) AS companies,
          countIf(role_year = 0) AS dateless,
          countIf(is_current = 1) AS current_rows
   FROM corpscout.se_company_person_role;

   -- b) the same thing recomputed LIVE from the two tables, the view's own SELECT
   SELECT count() AS rows, uniqExact(p.person_key) AS persons
   FROM corpscout.se_company_person AS p FINAL
   ARRAY JOIN p.normalized_ids AS member_id
   INNER JOIN corpscout.se_company_person_normalized AS n FINAL
       ON n.company_id = p.company_id AND n.normalized_id = member_id
   WHERE p.active = 1 AND n.role_code IS NOT NULL;

   -- c) active persons with no role at all -- they get no row, by design
   SELECT countIf(active = 1) AS active_persons FROM corpscout.se_company_person FINAL;
   ```

   Expected: (a) `rows` ≈ **3,837,006** and `persons` ≈ **1,113,485**; (b) equal to (a) to the rows any fold since 2026-09-12 has changed — a DIFFERENCE here means the stored view is not the SELECT, which is the one thing the refresh exists to guarantee; (c) `active_persons` − `persons` ≈ **160,279**. Record all of them.

5. [ ] **Spot-check one person against the spec's own readout.** Swedbank's Erik Bo Bengtsson:

   ```sql
   SELECT person_key, display_name, role_code, role_year, role_from, role_to, source, slot, is_current
   FROM corpscout.se_company_person_role
   WHERE company_id = (SELECT company_id FROM corpscout.se_companies_serving WHERE legal_name ILIKE 'Swedbank AB%' LIMIT 1)
     AND display_name ILIKE '%Bengtsson%'
   ORDER BY role_year, role_code;
   ```

   Expected (spec section 11): board member 2021 and 2022 from ESEF with the seat ending 2023-01-18, executive 2023 and 2024, legal representative 2026 from Ratsit. Compare each row against the person's own tab in the next step; a mismatch is a finding, not a rounding error.

6. [ ] **Smoke the backoffice on the owner's dev server.** `http://localhost:5183` when the owner's main checkout is on `main` (the merge is the deploy); otherwise run one from this worktree on 5199 (`npm run dev -- --port 5199`) and use that. Check, in order:
   - the People tab of the Swedbank company id from step 5: the panel's **Roles** section lists the view's rows — role label, year, the ESEF seat's end date, the source labels — and does NOT show "the roles view has not rebuilt";
   - a person folded during the smoke (use **Fold now** on any company): immediately after the fold the panel falls back to the arrays and says so, and after the next :20 refresh it lists rows again;
   - the People list at `/admin/se/people?year=2025` — the pre-existing `year` filter (shipped in slice 3, `has(p.role_years, {year:UInt16})`) still answers, counts strip and all. Nothing in this slice changed it; this is the check that nothing broke it.

   ```bash
   curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:5183/admin/se/people?year=2025'
   curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:5183/admin/se/company/<swedbank-id>/people'
   ```

   Expected: both `200`.

7. [ ] **Record and archive.**
   - Append the slice-5 shipped record to spec **section 9 item 5**: migration 000402 (the number it actually got), the engine-in-view refreshable form created `EMPTY`, the builder + drift pin, the clickhouse-local proof, the two reused-name guards that moved, the eighth backoffice read and the panel's fallback, and every readout from steps 3 to 6 (row count, persons, dateless rows, the live recomputation, Erik Bo Bengtsson's rows, the tab and the list).
   - Record the two rulings this plan made (they are in the self-review below and belong in the record): the engine-in-view form over a separate target table, and `role_year UInt16` with 0 for "no year" so the spec's ORDER BY survives `allow_nullable_key` being off.
   - Note in the record that the spec's list-filter sentence was ALREADY satisfied by slice 3, so this slice shipped no list change.
   - Tick this plan's boxes and archive the ledger under `.superpowers/sdd/person-roles/`.
   - Update memory `se-person-entity.md`: slice 5 live, `corpscout.se_company_person_role` is the roles-as-rows view (hourly at :20, derived, lags a fold by up to an hour), and the name is reused from the 2026-08-19 model.

---

## Self-review

**1. Spec coverage (section 11, sentence by sentence).**

| spec section 11 says | where it lands |
| --- | --- |
| the person row keeps its role arrays; nothing that reads them changes | Global Constraints ("the fold … untouched"); Task 2 keeps `roles` and only adds `roleRows` |
| `se_company_person_role` is a refreshable MV, `REFRESH EVERY 1 HOUR OFFSET 20 MINUTE`, created `EMPTY`, first build by `SYSTEM REFRESH VIEW` | Task 1 steps 3 and 5 (the DDL), step 1's `test_the_up_migration_creates_one_refreshable_view_created_empty`, Task 3 step 3 |
| the exact SELECT (columns, `ARRAY JOIN`, `INNER JOIN`, `WHERE`) | Task 1 step 3's builder and step 5's migration, pinned by `test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it` and proved by the clickhouse-local test |
| `ENGINE = MergeTree ORDER BY (company_id, person_key, role_year, role_code, source, slot)` | Task 1 step 3 (`ROLE_VIEW_ORDER_BY`), step 5 (the DDL), step 11 (the sort key really accepts the SELECT's types) |
| rebuilt deterministically from the two tables that exist | Task 1 step 5's header comment; Task 3 step 4's live recomputation is the check |
| prod numbers (3,837,006 rows / 1,113,485 persons / 160,279 roleless) | "Prod facts" section and Task 3 step 4 |
| the per-year summary is a `GROUP BY` — no second object | nothing built; stated in Task 1 step 13's doc note ("the person row's role arrays stay the fold's own summary") |
| the view lags a fold by at most an hour; if that matters the SELECT moves into the fold | Task 1 step 13 (doc), Task 2 steps 4 and 8 (the fallback), Task 2 step 1's fallback test |
| the People tab's panel lists role, year, from/to, source from the table | Task 2 steps 6 and 8 |
| the People list gains a `year` filter | **already shipped in slice 3** — see "Already shipped" above; Task 3 step 6 verifies it, nothing is built |
| names: `tables.ROLE_VIEW`, migration `000402_corpscout_se_company_person_role`, the drift pin `build_se_company_person_role_sql()` | Task 1 steps 3, 5 and 1 |
| slice 5 of section 9: migration and pin, panel and filter; prod migrate, refresh, readouts, the tab | Tasks 1, 2 and 3 |

**2. Placeholder scan.** No TBD, no "implement later", no "similar to Task N", no "add appropriate error handling". Every step carries the literal SQL, Python or TSX it installs, and every test body is written out. The only deliberately unwritten text is the shipped record in Task 3 step 7, whose content is enumerated.

**3. Name consistency.** `ROLE_VIEW` / `QUALIFIED_ROLE_VIEW` / `ROLE_VIEW_COLUMNS` / `ROLE_VIEW_ORDER_BY` / `build_se_company_person_role_sql` are spelled the same in Task 1 steps 1, 3, 10, 11 and 14. `SE_COMPANY_PERSON_ROLE_TABLE`, `SePersonRoleRow`, `PERSON_ROLE_SQL` and `roleRows` are spelled the same in Task 2 steps 1, 3, 4, 6 and 8 and in the Interfaces block. The view's column names are one list, repeated identically in `ROLE_VIEW_COLUMNS`, the builder's aliases, the TypeScript row type and the clickhouse-local type assertions. `SePersonRoleYear` (the old, array-derived type) is untouched and never confused with `SePersonRoleRow`.

**4. Choices this plan made where the spec left room** — all four belong in the shipped record:

- **The refreshable form: engine-in-view, ONE object, not `TO <target table>`.** Spec section 11 gives both a view and a "Target `ENGINE = MergeTree ORDER BY (...)`" and does not say which form. Every refreshable view in this repo (000320, 000326, 000335, 000391, 000392) declares the engine inside the view, and two things in the spec settle it: the runbook's `SYSTEM REFRESH VIEW corpscout.se_company_person_role` names the VIEW by that name, and `tables.ROLE_VIEW` is a single constant. A `TO` form would need a second name for the target and a second constant. Consequence recorded in the migration and the tests: the view owns its inner table, so the down file's single `DROP VIEW` removes the definition and the data together — the brief's "drop view and table" is one statement here, not two.
- **`role_year UInt16`, 0 for "no year", and the spec's ORDER BY kept whole.** `n.role_year` is `Nullable(UInt16)` and `ORDER BY` cannot hold a Nullable column (`allow_nullable_key` is off), so the alternatives were `ifNull(role_year, 0)` in the key or dropping `role_year` out of the key. The key is what makes "one person's roles over time" a range read, and the spec wrote it with `role_year` in third position, so the key stays and the column is unwrapped. 0 means "the observation carried no year at all" — mostly Wikidata's 290 dateless roles. Note the deliberate asymmetry with the fold, recorded in `person-design.md` and in the panel: the fold takes a dateless role as *held now* and puts it under the current year in `role_years`, while the view says 0.
- **`role_code` through `assumeNotNull`.** Same reason (it is in the key), and exact: the `WHERE` has already dropped every NULL. Spec section 11's prose SELECT writes `n.role_code` and `has(p.current_roles, n.role_code)` bare; both became `assumeNotNull(n.role_code)` and the difference is typing only. This is the one place the migration's body is not literally the spec's text.
- **The panel falls back to the arrays instead of showing an empty section.** The spec says the panel "lists the roles from the table" and separately that the view lags a fold by up to an hour; it does not say what the tab shows in that hour. Showing "none" for a person who demonstrably holds roles would be a lie a reviewer acts on, so the fallback renders the fold's own arrays with a one-line note. Rejected alternative: reading the view with a freshness check and hiding the section — more machinery, less information.

**5. Where the brief or the spec disagreed with the code, and what this plan does.**

- **The list's `year` filter is already shipped.** The brief asked for "Task 3: Backoffice list: the `year` filter (parse, SQL, control, tests)" and the spec asks for it too, but slice 3 built all four pieces (`app/lib/se-people-filters.ts:24,54,61,76`, `app/lib/se-people-list.server.ts:160-164`, `app/components/admin/se-people-table.tsx:161-162`, `tests/admin-se-people.test.tsx:34-41`, `tests/se-people-list.server.test.ts:54-55`). Building it again would duplicate a live filter. The plan drops that task, states the evidence in "Already shipped", and verifies the filter on prod in Task 3 step 6. There is also no `tests/se-people-filters.test.ts` — the filter helpers are tested inside `tests/admin-se-people.test.tsx`, which is where any further assertion belongs.
- **Reusing the name breaks two guards, which the brief did not name.** `tests/test_clickhouse_migrations.py::test_no_up_migration_declares_a_slice_0_dropped_object` turns red the moment 000402 exists (`se_company_person_role` is on `SLICE_0_DROPPED_OBJECTS`), so Task 1 step 8 moves the name to `SLICE_0_KEPT_OBJECTS`. `tests/test_se_person_retirement_drops.py` does not fail on its own, but its `KEPT`/`REUSED_NAME` pair is the repo's record of which dropped names came back, and a spent `DROP TABLE corpscout.se_company_person_role` now names a live object — so the same step adds the name to `KEPT` and makes `REUSED_NAMES` a pair. `tests/test_se_companies_serving_mv.py::RETIRED_ROLE_TABLE` keeps its assertion (the serving view must still never name the roles object) and only its comment changes.
- **No dagster deploy, and the brief's runbook did not say either way.** Nothing in the definitions tree reads the view, so Task 3 says so explicitly rather than carrying a deploy the slice does not need.
