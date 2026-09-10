# SE Company Person Slice 4: Rename and Docs — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the person entity its final name — `corpscout.se_company_person_v2` becomes `corpscout.se_company_person` — in one migration that also re-points the serving view, switch the three code constants that spell the name, and record the rename in the package doc and the spec.

**Architecture:** The proven address recipe (migration 000393, prod 2026-09-08), one statement shorter because there is nothing to park: slice 0 already dropped the 2026-08-19 table that held the name, so this is `SYSTEM STOP VIEW`, one `RENAME TABLE` with a single pair, one `ALTER TABLE corpscout.se_companies_serving MODIFY QUERY` carrying the SELECT exactly as `companies_current.build_se_companies_serving_sql()` renders it with the new constant, then `SYSTEM START VIEW`. `MODIFY QUERY` changes a refreshable view's definition in place — the view keeps the rows it is already serving and its next scheduled refresh (hourly at :45, migration 000366) runs the new query — so there is no `_next` view to build and no `SYSTEM WAIT VIEW` to sit through. The code half is three constants (`companies_current.COMPANY_PERSON_TABLE`, `person/tables.py::MAIN_TABLE`, the backoffice's `SE_COMPANY_PERSON_TABLE`) plus the tests that pin the literal. Nothing is dropped, no asset or schedule is renamed, and the backoffice UI does not change.

**Tech Stack:** Python 3.14 / Dagster 1.13.9 (`uv run --frozen --no-sync`), ClickHouse 26.5 (migrations via golang-migrate, `make -s -C corpscout clickhouse-migrate-up-one`), React Router v7 backoffice (vitest, `npm run typecheck`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md` — section 3.3 (the main table, "built as `_v2` … the rename is a `RENAME TABLE` plus `MODIFY QUERY` on the serving view, the proven address recipe"), section 8 (the readers after the cutover: the serving view's `has_people`, `people_bolagsverket` and `people_esef` through the builder constant, plus the backoffice tab and list — nothing else), section 9 item 4 ("Rename and docs: `se_company_person_v2` to `se_company_person` with the serving re-point, the design doc under the package, memory") and section 10 (names). Predecessor plans: `2026-09-09-se-company-person-0-tables-normalizer-retirement.md` (migration 000396 created the six tables and re-pointed the view at `_v2`), `2026-09-09-se-company-person-1-extractors.md`, `2026-09-10-se-company-person-2-fold.md` (the fold that filled the table) and `2026-09-10-se-company-person-3-backoffice.md` (the tab and the list that read it). The address twin to mirror: `2026-09-08-se-company-address-4b-retire.md` Tasks 1, 2, 6 and 8.

## Global Constraints

- Dagster commands run from `corpscout/services/dagster_v3` with `uv run --frozen --no-sync ...` and `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`; `uv run --frozen --no-sync dg check defs` before every commit that touches `src/`.
- Backoffice commands run from `corpscout/services/backoffice`: `npx vitest run <files>` and `npm run typecheck`.
- **The new name is a prefix of five sibling tables.** `corpscout.se_company_person` prefixes `corpscout.se_company_person_suggestion`, `_normalized`, `_history`, `_rule` and `_precedence`. Every string match on the main table — a test assertion, a fake ClickHouse client's dispatch branch, an `rg` check — must match the WHOLE qualified name: carry the alias or the token that follows it (`corpscout.se_company_person AS m FINAL`, `corpscout.se_company_person AS p FINAL`, `corpscout.se_company_person FINAL`), never the bare name. A dispatch branch written as `sql.includes("corpscout.se_company_person")` would answer the normalized or the suggestion query with the published persons.
- **Nothing in this slice drops a ClickHouse object and no historical migration file is edited** (dev-phase ledger policy, memory `clickhouse-ledger-squash-planned`). Migration 000396 keeps declaring the main table under its build name `se_company_person_v2`; 000398 renames the DEPLOYED table. Every test that reads the DDL out of 000396 therefore keeps naming `se_company_person_v2` as the *DDL* name and replays the rename itself — `tests/test_se_company_address_tables.py::MAIN_DDL_TABLE` is the pattern to copy.
- **Migration numbers collide.** Other sessions merge to main daily (the ESEF slices 2 and 3 are in flight and each carries a migration). Number this one 000398; if main has taken it at merge time, renumber before merging: both migration files, `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`, `MIGRATION` in `tests/test_se_companies_serving_mv.py`, the `migrate force 398` line in the up file's header, and every "000398" in the package doc and the spec.
- **No semicolon may appear inside a `--` comment in any migration file** (`test_clickhouse_migration_line_comments_do_not_contain_semicolons`), and the file must end with a statement, not prose (`test_every_migration_ends_with_a_statement_not_a_comment`). Write the recovery note as "run SYSTEM START VIEW corpscout.se_companies_serving by hand" with no trailing semicolon.
- **The MODIFY QUERY body is machine-rendered, never hand-written.** `tests/test_se_companies_serving_mv.py` compares the migration's body against a fresh `build_se_companies_serving_sql()` render, whitespace-collapsed. The up file's body must be byte-for-byte the render produced AFTER the constant edit; the down file's body must be byte-for-byte 000396's body.
- Never `from __future__ import annotations` in a module that defines Dagster assets.
- Commit by explicit path only; never `git add -A`. Trailers, in this order, each on its own line at the end of every commit message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- **Pre-existing failure, not a regression of this slice:** `tests/test_schedule_cron_contracts.py::test_every_schedule_fires_on_a_unique_minute_hour_pair` (verified red on `se-person-entity` at d0e62ca6 before any edit). Everything else named in this plan is green at the branch point: `tests/test_se_companies_serving_mv.py`, `tests/test_clickhouse_migrations.py`, `tests/test_se_company_person_tables.py`, `tests/test_se_person_retirement_drops.py` (149 passed) and the seven person unit suites (100 passed).
- The clickhouse-local suites need either a `clickhouse-local`/`clickhouse` binary on PATH or a running Docker (`tests/clickhouse_local.py` falls back to `clickhouse/clickhouse-server:26.5`); they SKIP otherwise. If they skip, say so — never report a pass.

## Prod facts at the branch point (2026-09-10 19:05 UTC)

- `corpscout.se_company_person` does NOT exist — person slice 0 dropped the 2026-08-19 table by hand on 2026-09-09, so the target name is free and the `RENAME TABLE` needs one pair, not two.
- `corpscout.se_company_person_v2` is a `ReplacingMergeTree` holding 1,126,408 rows (1,126,402 persons plus the un-merged versions slice 3's smoke left on company 5592501521).
- The ledger is clean at 397.
- `corpscout.se_companies_serving` refreshes hourly at :45 and takes about 13 to 15 minutes; `has_people` is 1 for 578,289 companies.
- No Dagster asset, job, schedule, pool or group name changes: none of them carries `_v2` (the asset group is already `se_company_person`).

## The rename, in one table

| what | before slice 4 | after slice 4 |
| --- | --- | --- |
| the entity's main table on prod | `corpscout.se_company_person_v2` | `corpscout.se_company_person` |
| its DDL in the ledger (untouched) | `000396` declares `se_company_person_v2` | unchanged — 000398 renames the deployed table |
| `sweden_company/companies_current.py` | `COMPANY_PERSON_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_person_v2"` | `… .se_company_person` |
| `se_company/person/tables.py` | `MAIN_TABLE = "se_company_person_v2"` | `MAIN_TABLE = "se_company_person"` |
| backoffice `app/lib/se-person-tables.ts` | `SE_COMPANY_PERSON_TABLE = "corpscout.se_company_person_v2"` | `… "corpscout.se_company_person"` |
| the serving drift pin | `tests/test_se_companies_serving_mv.py` at 000396 | at 000398 |

---

## File map

**Task 1 — Dagster and the migration (one commit):**

| file | responsibility after this task |
| --- | --- |
| `corpscout/clickhouse/migrations/000398_corpscout_se_company_person_rename.up.sql` (create) | five statements: `CREATE DATABASE`, `SYSTEM STOP VIEW`, `RENAME TABLE`, `ALTER TABLE … MODIFY QUERY` with the new render, `SYSTEM START VIEW` |
| `corpscout/clickhouse/migrations/000398_corpscout_se_company_person_rename.down.sql` (create) | the same five, reversed: the rename runs BEFORE the query that needs the `_v2` name, and the body is 000396's |
| `src/dagster_v3/defs/sweden_company/companies_current.py` (modify, line 84 and its comment) | `COMPANY_PERSON_TABLE` is the one place the serving view names the entity |
| `src/dagster_v3/defs/se_company/person/tables.py` (modify, lines 1-10) | `MAIN_TABLE` is the one place the package names it; the module docstring records the rename |
| `src/dagster_v3/defs/se_company/person/assets.py` (modify, line 277) | the fold asset's description text says `se_company_person` |
| `tests/test_se_companies_serving_mv.py` (rewrite) | the drift pin and the structural contract of 000398 |
| `tests/test_se_company_person_tables.py` (modify) | `MAIN_DDL_TABLE` replays the rename over 000396's DDL; the qualified-name and prefix assertions carry the new name |
| `tests/test_se_companies_serving_sql.py` (modify, lines 406-410, 423) | the clickhouse-local fixture's person stub is created and seeded under the new name |
| `tests/test_se_company_person_fold_clickhouse_local.py` (modify, `_schema_statements`) | replays the rename over 000396's CREATE so `batch.py`'s SQL finds the table it names |
| `tests/test_clickhouse_migrations.py` (modify, `EXPECTED_MIGRATIONS` and the slice-0 comments) | 000398 is in the ledger contract; the comments say why `se_company_person` is on the dropped list AND is the live entity |
| `tests/test_se_person_retirement_drops.py` (modify, `KEPT` and its guard) | the never-droppable list carries the live name; the one reused name is called out explicitly |

**Task 2 — Backoffice (one commit):**

| file | responsibility after this task |
| --- | --- |
| `app/lib/se-person-tables.ts` (modify) | the single constant every backoffice read goes through |
| `app/lib/se-company-person-entity.server.ts` (modify, line 61 comment only) | the doc comment names the live table |
| `app/lib/se-people-list.server.ts` (modify, line 3 comment only) | the same |
| `tests/se-company-person-entity.server.test.ts` (modify, lines 200 and 227) | the fake client's dispatch and the SQL pin match the whole qualified name |
| `tests/se-people-list.server.test.ts` (modify, lines 32, 97, 103, 113) | the same |

**Task 3 — Docs (one commit):**

| file | responsibility after this task |
| --- | --- |
| `src/dagster_v3/defs/se_company/person/docs/person-design.md` (modify) | the main table is `se_company_person`; one sentence of `_v2` history; the 000398 interrupted-rename runbook beside 000396's |
| `docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md` (modify, sections 3.3 and 10) | 3.3 carries the plain name and the history sentence; 10 lists the plain name with no parenthetical |

**Task 4 — Prod run (controller, no repo change except the shipped record).**

## Interfaces

- **Task 1 produces:** migration name `000398_corpscout_se_company_person_rename`; `companies_current.COMPANY_PERSON_TABLE == "corpscout.se_company_person"`; `person.tables.MAIN_TABLE == "se_company_person"` and therefore `person.tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_person"`. Every `batch.py` builder (`main_insert_sql`, `current_main_rows_sql`, `main_watermarks_sql`) and both fold assets' `table` metadata follow that constant with no edit of their own.
- **Task 2 consumes** the same string as a TypeScript literal: `SE_COMPANY_PERSON_TABLE === "corpscout.se_company_person"`, imported by `se-company-person-entity.server.ts` and `se-people-list.server.ts` and by nothing else.
- **Task 3 consumes** the migration number from Task 1 (`000398`) and the constant names from Tasks 1 and 2.
- **Task 4 consumes** all of the above and the deploy artefacts; it produces the shipped record in spec section 9 item 4.
- **Unchanged and relied upon:** `build_se_companies_serving_sql() -> str` (signature identical; its render changes because the constant it interpolates changes), `tests/se_company_ddl.py::table_block(table: str) -> str` and `declared_columns(table: str) -> list[str]` (both keyed on the CREATING migration's name, i.e. still `se_company_person_v2`).

---

### Task 1: Migration 000398 renames the entity, the two Python constants follow, every dagster pin moves

**Files:**
- Create: `corpscout/clickhouse/migrations/000398_corpscout_se_company_person_rename.up.sql`, `…down.sql`
- Modify: `src/dagster_v3/defs/sweden_company/companies_current.py:79-84` (`COMPANY_PERSON_TABLE` and its comment)
- Modify: `src/dagster_v3/defs/se_company/person/tables.py:1-10` (module docstring and `MAIN_TABLE`)
- Modify: `src/dagster_v3/defs/se_company/person/assets.py:277` (the fold asset's description text)
- Rewrite: `tests/test_se_companies_serving_mv.py`
- Modify: `tests/test_se_company_person_tables.py:55-56,112,127-138`
- Modify: `tests/test_se_companies_serving_sql.py:406-410,423`
- Modify: `tests/test_se_company_person_fold_clickhouse_local.py:100-112`
- Modify: `tests/test_clickhouse_migrations.py:413,4261-4292`
- Modify: `tests/test_se_person_retirement_drops.py:1-11,47-64,77-85`

**Interfaces:**
- Consumes: `companies_current.build_se_companies_serving_sql() -> str`, `tests/se_company_ddl.py::table_block`/`declared_columns`.
- Produces: `000398_corpscout_se_company_person_rename`; `COMPANY_PERSON_TABLE == "corpscout.se_company_person"`; `tables.MAIN_TABLE == "se_company_person"`, `tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_person"` — the string Task 2's TypeScript constant must equal.

- [x] **Step 1: Point the serving builder at the final name**

In `src/dagster_v3/defs/sweden_company/companies_current.py`, replace the `COMPANY_PERSON_TABLE` constant and the four comment lines above it (they currently end "It is se_company_person_v2 for slices 0 to 3; slice 4 renames it and edits this one line."):

```python
# The SE person entity (migration 000396, renamed to its final name by 000398). This
# constant is the one place the view names it; the three people flags below read the
# table's ACTIVE rows under FINAL, so a hidden or withdrawn person does not keep a flag
# lit (spec section 8).
COMPANY_PERSON_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_person"
```

`PEOPLE_SET`, `PEOPLE_BOLAGSVERKET_SET` and `PEOPLE_ESEF_SET` (lines 216-224) interpolate that constant and need no edit.

- [x] **Step 2: Render both MODIFY QUERY bodies into scratch files**

The new body — run this AFTER Step 1's edit, so the render already carries the new name:

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync python -c \
  "from dagster_v3.defs.sweden_company.companies_current import build_se_companies_serving_sql; print(build_se_companies_serving_sql())" \
  > /tmp/serving_new.sql
```

The old body, for the down file — it is 000396's `MODIFY QUERY` body, verbatim:

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync python -c "
from pathlib import Path
sql = Path('../../clickhouse/migrations/000396_corpscout_se_company_person_entity.up.sql').read_text()
[statement] = [s for s in sql.split(';') if 'MODIFY QUERY' in s]
marker = 'MODIFY QUERY' + chr(10)
print(statement[statement.index(marker) + len(marker):].strip())
" > /tmp/serving_old.sql
```

Check both before pasting them (these exact counts were verified on the branch point):

```bash
wc -l /tmp/serving_new.sql /tmp/serving_old.sql          # 171 lines each
grep -c 'corpscout\.se_company_person FINAL' /tmp/serving_new.sql   # 3
grep -c 'se_company_person_v2 FINAL' /tmp/serving_new.sql           # 0
grep -c 'se_company_person_v2 FINAL' /tmp/serving_old.sql           # 3
grep -c 'corpscout\.se_company_person FINAL' /tmp/serving_old.sql   # 0
diff /tmp/serving_new.sql /tmp/serving_old.sql            # exactly 3 changed lines, 122-124
head -1 /tmp/serving_new.sql   # WITH company_addresses AS (
tail -1 /tmp/serving_new.sql   #     max_memory_usage = 12884901888
```

The three lines that differ are the whole difference between the two files:

```sql
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1)) AS has_people,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1 AND has(sources, 'bolagsverket'))) AS people_bolagsverket,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1 AND has(sources, 'esef'))) AS people_esef,
```

The trailing `SETTINGS join_algorithm = 'grace_hash,hash', … max_memory_usage = 12884901888` block is PART of the body and must be pasted with it: a `MODIFY QUERY` that dropped it would leave the hourly refresh running without the grace-hash join and the external-sort budget, which is what OOM'd the server in August (memory `se-companies-serving-view`).

- [x] **Step 3: Write the up migration**

`corpscout/clickhouse/migrations/000398_corpscout_se_company_person_rename.up.sql` — exactly five statements. Paste `/tmp/serving_new.sql` where the body is marked, byte for byte; do not hand-edit a character of it.

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- THE PERSON ENTITY TAKES ITS FINAL NAME (spec 2026-09-09 section 3.3, slice 4).
-- corpscout.se_company_person_v2 -- built by 000396 beside the 2026-08-19 model, filled by
-- slice 2's fold (1.13M persons over 578,289 companies) and read by slice 3's backoffice --
-- becomes corpscout.se_company_person. ONE PAIR IN THE RENAME, not two: person slice 0
-- dropped the old table of that name by hand on 2026-09-09, so the target name is free and
-- there is nothing to park under _legacy the way the address rename (000393) had to.
--
-- NO STAGED _next SWAP, and that is the difference from 000391 and 000392. Those two
-- REPLACED the serving view's definition, which meant building a second view and swapping
-- names. This migration changes only the TABLE NAME the same definition reads, and ALTER
-- TABLE's MODIFY-QUERY clause does that in place: the refreshable view keeps the rows it is
-- already serving and its next scheduled refresh (hourly at :45, migration 000366) runs the
-- new query. Proven on prod 2026-09-08 by 000393 and again on 2026-09-09 by 000396. So
-- there is no _next to populate and no refresh wait to sit through.
--
-- THE VIEW IS STOPPED FIRST because between the RENAME and the MODIFY-QUERY step its stored
-- query names a table that no longer exists, so a refresh landing in that window would
-- fail -- a stopped view cannot refresh at all.
--
-- APPLY THIS OUTSIDE THE :45 REFRESH WINDOW. The refresh takes 13 to 15 minutes, so start
-- the migration just after one finishes (about :00) and the STOP cannot interrupt a run.
--
-- IF THE MIGRATE CLIENT DROPS between the STOP and the START, the view is left stopped and
-- serving its last contents at full speed with nothing raising anywhere. Recovery is by
-- hand: check corpscout.se_companies_serving in system.view_refreshes, run SYSTEM START VIEW
-- corpscout.se_companies_serving, then migrate force 398 so the ledger records where the
-- database actually is. The person design doc's runbook section has the full sequence.
--
-- THE LEDGER KEEPS 000396's DDL UNDER THE OLD NAME. Under the dev-phase ledger policy a
-- historical file is history: 000396 still declares se_company_person_v2, and the tests that
-- read that DDL replay this rename themselves.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

SYSTEM STOP VIEW corpscout.se_companies_serving;

RENAME TABLE corpscout.se_company_person_v2 TO corpscout.se_company_person;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
<<< the contents of /tmp/serving_new.sql, verbatim >>>;

SYSTEM START VIEW corpscout.se_companies_serving;
```

- [x] **Step 4: Write the down migration**

`corpscout/clickhouse/migrations/000398_corpscout_se_company_person_rename.down.sql` — the same shape reversed, so the RENAME lands before the query that needs the `_v2` name. Paste `/tmp/serving_old.sql` verbatim.

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000398: the entity goes back to corpscout.se_company_person_v2 and the serving
-- view is re-pointed at the _v2 render 000396 deployed. The rename runs BEFORE the
-- MODIFY-QUERY step here for the same reason it runs after it in the up file: the query
-- must never be set to a name that does not exist yet. Nothing else comes back -- the
-- 2026-08-19 table that used to hold the name was dropped by hand in slice 0 and is not
-- this migration's business.

SYSTEM STOP VIEW corpscout.se_companies_serving;

RENAME TABLE corpscout.se_company_person TO corpscout.se_company_person_v2;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
<<< the contents of /tmp/serving_old.sql, verbatim >>>;

SYSTEM START VIEW corpscout.se_companies_serving;
```

- [x] **Step 5: Rewrite the drift pin**

`tests/test_se_companies_serving_mv.py` — keep `MIGRATIONS_DIR`, `_sql_of`, `_sql`, `_statements`, `_body`, `_normalized`, `_executable` and `_modify_query_body` exactly as they are; replace the module docstring, the constants and the four tests below them. `PREVIOUS_MIGRATION` is how the pin finds the render the down file must restore: 000396 installed its body with the SAME `ALTER TABLE … MODIFY QUERY` shape, so `_modify_query_body` reads both files and no `_previous_view_body` helper is needed (000393 needed one only because 000392 embedded its SELECT in a `CREATE MATERIALIZED VIEW`).

```python
"""Migration 000398: the SE person entity takes its final name and the serving view follows.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags, the address JSON + primary
geocode summary, and (since 000338) the registered-activity translation, status-reason label
and spine fields absorbed from the retired `se_companies_translated` view.

WHAT 000398 CHANGES (person slice 4). `corpscout.se_company_person_v2` -- created by 000396,
filled by slice 2's fold and read by slice 3's backoffice -- is renamed
`corpscout.se_company_person`, and `has_people`, `people_bolagsverket` and `people_esef` read
it under the new name. ONE pair in the RENAME: slice 0 dropped the 2026-08-19 table that held
that name, so nothing has to be parked. The definition is otherwise UNCHANGED, so this is the
in-place `ALTER TABLE ... MODIFY QUERY` of 000393 and 000396, not the staged swap of
000391/000392: no `_next`, no `SYSTEM WAIT VIEW`, no drop.

The drift pin couples the migration's MODIFY QUERY body to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
"""

from pathlib import Path

from dagster_v3.defs.sweden_company.companies_current import (
    build_se_companies_serving_sql,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000398_corpscout_se_company_person_rename"
PREVIOUS_MIGRATION = "000396_corpscout_se_company_person_entity"
VIEW = "corpscout.se_companies_serving"
ENTITY = "corpscout.se_company_person"
ENTITY_V2 = "corpscout.se_company_person_v2"
# The 2026-08-19 model's role table, dropped in slice 0. It must never come back into the
# view's body, and its name is a prefix trap of its own.
RETIRED_ROLE_TABLE = "corpscout.se_company_person_role"
# The five tables ENTITY is a PREFIX of. No whole-name match on ENTITY may hit one of them.
SIBLING_TABLES = (
    "corpscout.se_company_person_suggestion",
    "corpscout.se_company_person_normalized",
    "corpscout.se_company_person_history",
    "corpscout.se_company_person_rule",
    "corpscout.se_company_person_precedence",
)
```

Then the tests (the helpers between the constants and these stay untouched):

```python
def test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it() -> None:
    assert _normalized(_modify_query_body(_sql("up"))) == _normalized(
        build_se_companies_serving_sql()
    )


def test_the_pin_is_not_vacuous() -> None:
    body = _modify_query_body(_sql("up"))
    assert len(body) > 2000
    assert "groupArray" in body
    assert "primary_geocode_class" in body
    assert "corpscout.se_company_address AS a FINAL" in body
    # Whole-name matching: ENTITY prefixes all five siblings, so the count is taken on the
    # name PLUS the token that follows it in the three people subqueries.
    assert body.count(f"{ENTITY} FINAL") == 3
    assert "has(sources, 'bolagsverket')" in body and "has(sources, 'esef')" in body
    assert ENTITY_V2 not in body
    assert f"{RETIRED_ROLE_TABLE} " not in body and f"{RETIRED_ROLE_TABLE}\n" not in body
    for sibling in SIBLING_TABLES:
        assert sibling not in body, sibling
    assert "SETTINGS join_algorithm = 'grace_hash,hash'" in body
    assert "max_memory_usage = 12884901888" in body


def test_the_up_migration_stops_renames_repoints_and_starts() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[2]) == f"RENAME TABLE {ENTITY_V2} TO {ENTITY}"
    assert _body(statements[3]).startswith(f"ALTER TABLE {VIEW}\nMODIFY QUERY\n")
    assert _body(statements[4]) == f"SYSTEM START VIEW {VIEW}"
    # A rename, nothing else: no staged swap, no new table, no drop on either side.
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    assert "CREATE MATERIALIZED VIEW" not in _sql("up")
    assert "CREATE TABLE" not in _sql("up")
    for suffix in ("up", "down"):
        assert "DROP" not in _executable(_sql(suffix)).upper(), suffix


def test_the_down_migration_renames_back_and_restores_000396s_render() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[2]) == f"RENAME TABLE {ENTITY} TO {ENTITY_V2}"
    assert _body(statements[4]) == f"SYSTEM START VIEW {VIEW}"
    # The restored query is 000396's, modulo whitespace (_normalized collapses runs of
    # whitespace before comparing, so this is not a character-for-character check).
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _modify_query_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )


def test_the_up_migration_documents_the_interrupted_repoint_recovery() -> None:
    up = _sql("up")
    assert "SYSTEM START VIEW" in up
    assert "migrate force 398" in up
```

- [x] **Step 6: Run the drift pin and watch it fail for the right reason**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_companies_serving_mv.py -q
```

Expected at this point: PASS if Steps 1-4 pasted the bodies correctly. If `test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it` fails, the up file's body is not the render — re-paste it, never patch it by hand. If `test_the_down_migration_renames_back_and_restores_000396s_render` fails, the down body is not 000396's — re-run the extraction command in Step 2.

- [x] **Step 7: Point the package's own constant at the final name**

`src/dagster_v3/defs/se_company/person/tables.py` — the docstring's second paragraph and `MAIN_TABLE`:

```python
"""Table names and column tuples of the person entity, pinned against migration 000396.

The main table is se_company_person since migration 000398 (slice 4). It was BUILT as
se_company_person_v2, because the 2026-08-19 table held the final name until slice 0 dropped
it, and 000396's DDL still declares it under that build name -- the rename is a RENAME TABLE
on the deployed database, and MAIN_TABLE is the one place this package spells it.
"""
```

```python
MAIN_TABLE = "se_company_person"
```

Nothing else in the package changes: `batch.py` and `assets.py` read `tables.QUALIFIED_MAIN_TABLE`.

- [x] **Step 8: Fix the one description string that spells the table by hand**

`src/dagster_v3/defs/se_company/person/assets.py:277`, inside the `se_company_person_fold` asset's `description`:

```python
        "into se_company_person: observations of one person merge into one row with every "
```

Confirm nothing else in `src/` still spells the old name:

```bash
cd corpscout/services/dagster_v3
rg -n "se_company_person_v2" src/
```

Expected: no output.

- [x] **Step 9: Replay the rename in the two DDL-reading tests**

`tests/test_se_company_person_tables.py` — the DDL still lives under the build name, exactly as `tests/test_se_company_address_tables.py` does it. Add the constant under `DATA_CHECK`:

```python
# Migration 000396 declares the main table under the build name it was created with; 000398
# renames the DEPLOYED table and, under the ledger policy, does not touch that file.
MAIN_DDL_TABLE = "se_company_person_v2"
```

In `test_main_table_is_one_row_per_company_and_person`, the first two lines become:

```python
    block = table_block(MAIN_DDL_TABLE)
    assert declared_columns(MAIN_DDL_TABLE) == list(tables.MAIN_COLUMNS)
```

In `test_column_tuples_agree_with_each_other`, the main-table assertion and a prefix guard:

```python
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_person"
    # The five sibling tables keep names the main one is a PREFIX of, which is why every
    # string match on it elsewhere carries the alias or the token that follows it.
    assert tables.QUALIFIED_SUGGESTION_TABLE.startswith(tables.QUALIFIED_MAIN_TABLE)
```

And `test_the_entity_name_is_a_prefix_of_five_siblings` loses its future tense:

```python
def test_the_entity_name_is_a_prefix_of_five_siblings() -> None:
    """Whole-name matching, everywhere. Since migration 000398 the main table is
    se_company_person, and that name prefixes all five of the tables below."""
    siblings = (
        tables.SUGGESTION_TABLE, tables.NORMALIZED_TABLE, tables.HISTORY_TABLE,
        tables.RULE_TABLE, tables.PRECEDENCE_TABLE,
    )
    for name in siblings:
        assert name.startswith(f"{tables.MAIN_TABLE}_")
        assert name != tables.MAIN_TABLE
```

`tests/test_se_company_person_fold_clickhouse_local.py` — its `_schema_statements()` builds the fixture schema out of 000396's CREATEs, so without a replay it would create `se_company_person_v2` while `batch.py`'s SQL reads `corpscout.se_company_person`, and every fold read would fail with UNKNOWN_TABLE. The filter must run BEFORE the replacement (the filter matches the `se_company_person_` prefix, which the new name does not carry):

```python
def _schema_statements() -> list[str]:
    """CREATE DATABASE plus the six CREATE TABLEs of 000396 -- never its SYSTEM STOP/START
    VIEW or ALTER TABLE ... MODIFY QUERY, which name se_companies_serving, a view this
    fixture does not build. 000396 declares the main table under its build name and 000398
    renames the DEPLOYED table without touching that file, so the rename is replayed here:
    batch.py reads tables.QUALIFIED_MAIN_TABLE, which is the renamed name."""
    text = (MIGRATIONS_DIR / MIGRATION_FILE).read_text(encoding="utf-8")
    statements: list[str] = []
    for raw in text.split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement.upper().startswith("CREATE DATABASE") or (
            "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_" in statement
        ):
            statements.append(
                statement.replace(
                    "corpscout.se_company_person_v2", tables.QUALIFIED_MAIN_TABLE
                )
            )
    return statements
```

`tables` is already imported in that module (`from dagster_v3.defs.se_company.person import batch, tables`).

- [x] **Step 10: Rename the person stub in the executable serving suite**

`tests/test_se_companies_serving_sql.py` — the stub is hand-written (not `table_block`), so it is a straight rename of the literal in two places. The comment above the CREATE (line ~405) and the CREATE itself:

```python
        # The person entity's main table (migration 000396, renamed by 000398) -- read
        # FINAL, active rows only. Only the three columns the serving SELECT's IN-subqueries
        # touch. ReplacingMergeTree (not plain MergeTree, which this ClickHouse rejects with
        # ILLEGAL_FINAL) so the stub accepts the same FINAL modifier the real table's engine
        # does.
        "CREATE TABLE corpscout.se_company_person (company_id String, sources Array(String), active UInt8) ENGINE = ReplacingMergeTree ORDER BY company_id;",
```

and the seed (line ~423):

```python
        f"INSERT INTO corpscout.se_company_person VALUES ('{PRECISE}', ['esef'], 1);",
```

- [x] **Step 11: Register 000398 in the ledger contract and correct the slice-0 comments**

`tests/test_clickhouse_migrations.py` — append to `EXPECTED_MIGRATIONS`, after `"000397_corpscout_esef_document_people_extraction",`:

```python
    "000398_corpscout_se_company_person_rename",
```

The comment above `SLICE_0_DROPPED_OBJECTS` (line ~4261) gains the name-reuse paragraph — the list is a guard on DECLARATIONS, and 000398 does not declare anything:

```python
# Every name whose DDL leaves the ledger in person slice 0: the twelve the owner-run script
# drops, plus se_company_person_draft, se_company_person_draft_legacy and company_person_role,
# dropped by migrations back in August and only losing their DDL now. WHOLE-NAME matching
# only: se_company_person prefixes the five sibling tables the entity KEEPS, and
# company_person_role prefixes company_person_role_type, the catalog that stays.
#
# se_company_person IS ALSO THE LIVE ENTITY SINCE 000398, which renamed
# se_company_person_v2 into the name slice 0 freed. The guard below is unaffected: it forbids
# a CREATE or an ALTER that DECLARES one of these names, and a RENAME TABLE declares nothing.
# The entity's DDL still lives in 000396 under se_company_person_v2, which is why that name,
# not this one, is the kept object asserted below.
```

`SLICE_0_KEPT_OBJECTS` keeps `"se_company_person_v2"` — it is the name 000396 declares, and `test_no_up_migration_declares_a_slice_0_dropped_object` asserts each kept object IS declared by some up file. Add one comment line above the tuple:

```python
# The names the ledger DECLARES for the entity. se_company_person_v2 is 000396's build name;
# the deployed table is se_company_person since 000398, which renames rather than declares.
```

- [x] **Step 12: Move the never-droppable list onto the live name**

`tests/test_se_person_retirement_drops.py` — the drop script is spent history (it ran on prod 2026-09-09) and `DROP_ORDER` is unchanged. What changes is the never-droppable set and the one name that is on BOTH lists:

```python
# Never droppable, at the names the entity carries TODAY: migration 000398 renamed
# se_company_person_v2 to se_company_person, so the main table joins its five siblings here
# under the live name. Also the role catalog Serbia shares, the raw sources every extractor
# reads, and the serving view.
KEPT = (
    "se_company_person",
    "se_company_person_suggestion",
    "se_company_person_normalized",
    "se_company_person_history",
    "se_company_person_rule",
    "se_company_person_precedence",
    "company_person_role_type",
    "se_financial_report_signatories",
    "esef_document_people",
    "wikidata_company_people",
    "wikidata_persons",
    "wikidata_company_identifiers",
    "se_companies_serving",
)

# The one name on both lists, and the reason this file exists. corpscout.se_company_person
# was the 2026-08-19 table this script dropped on 2026-09-09; migration 000398 then gave the
# freed name to the entity's main table. THE SCRIPT IS SPENT -- running it again today would
# destroy 1.1M published persons. It stays in the repo as history under the ledger policy and
# must never be run a second time.
REUSED_NAME = "se_company_person"
```

and the guard that used to say "`se_company_person_v2` is not dropped":

```python
def test_the_drop_script_names_no_kept_object() -> None:
    """Whole-name comparison. A substring check would call se_company_person_role a hit on
    the entity's se_company_person_rule, or -- written the other way round -- would call
    company_person_role_type unsafe because company_person_role is being dropped.

    REUSED_NAME is excluded rather than the check being weakened: the script's last DROP and
    the entity's main table spell the same name for different objects, one dropped on
    2026-09-09 and one created by 000398's rename a day later."""
    dropped = {name for _, name in _statements(DROPS)}
    assert dropped.isdisjoint(set(KEPT) - {REUSED_NAME})
    assert len(dropped) == len(DROP_ORDER)
    assert "se_company_person_v2" not in dropped
    assert dropped & set(KEPT) == {REUSED_NAME}
```

The module docstring's second paragraph gains one sentence:

```python
"""The person slice-0 drop scripts say exactly what the spec's retirement list says, in
dependency order.

The scripts live in corpscout/clickhouse/operations/ beside the ledger they retire from, and
they are owner-run: nothing in this repo executes them, so this file is the only thing
standing between a typo and a dropped production table. It reads the SQL, parses the object
names out, and compares them as WHOLE names -- corpscout.se_company_person is a prefix of
five entries here AND of the six tables the entity keeps, and company_person_role prefixes
the role catalog that stays.

BOTH SCRIPTS ARE SPENT: they ran on prod on 2026-09-09, and migration 000398 has since given
the name of their last DROP -- corpscout.se_company_person -- to the entity's live main
table. Re-running se_person_retirement_drops.sql would destroy it. See REUSED_NAME below.
"""
```

`test_the_precheck_gates_on_the_serving_view_being_repointed` keeps its `se_company_person_v2` assertion — the precheck is spent history that gated on 000396's re-point, and 000398 is not its business — but its docstring says so:

```python
def test_the_precheck_gates_on_the_serving_view_being_repointed() -> None:
    """The one reader that had to be off the old tables before they went. Migration 000396
    did that, and this gate proves it landed. The literal stays se_company_person_v2 on
    purpose: the precheck ran on 2026-09-09, before 000398 renamed the entity, and a spent
    operations script is history like a migration file."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "se_company_person_v2" in sql
    assert "system.view_refreshes" in sql
```

- [x] **Step 13: Run every dagster suite this task touches**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_companies_serving_mv.py tests/test_clickhouse_migrations.py \
         tests/test_se_company_person_tables.py tests/test_se_person_retirement_drops.py \
         tests/test_se_company_person_batch.py tests/test_se_company_person_fold.py \
         tests/test_se_company_person_assets.py tests/test_se_company_person_extractors_sql.py \
         tests/test_se_company_person_jobs.py tests/test_se_company_person_precedence.py \
         tests/test_se_company_person_normalize.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_companies_serving_sql.py tests/test_se_company_person_fold_clickhouse_local.py \
         tests/test_se_company_person_normalize_clickhouse_local.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
```

Expected: all pass (149 of them were green in the first four files at the branch point, and the seven person unit suites added 100 more; the second command needs `clickhouse-local` or Docker and SKIPS otherwise — report a skip as a skip). `dg check defs` must be green: no asset, job, schedule or pool name changed, so a failure here means something else broke.

Then confirm the whole repo is clean of the old name apart from the ledger and the historical plans:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "se_company_person_v2" corpscout/services/dagster_v3/src corpscout/services/dagster_v3/tests
```

Expected hits, and only these: `tests/test_se_company_person_tables.py` (`MAIN_DDL_TABLE` and its comment), `tests/test_se_company_person_fold_clickhouse_local.py` (the replay), `tests/test_se_companies_serving_mv.py` (`ENTITY_V2`), `tests/test_clickhouse_migrations.py` (`SLICE_0_KEPT_OBJECTS` and its comment), `tests/test_se_person_retirement_drops.py` (the two guards above). No hit in `src/`.

- [x] **Step 14: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000398_corpscout_se_company_person_rename.up.sql \
        corpscout/clickhouse/migrations/000398_corpscout_se_company_person_rename.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/companies_current.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/tables.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/assets.py \
        corpscout/services/dagster_v3/tests/test_se_companies_serving_mv.py \
        corpscout/services/dagster_v3/tests/test_se_company_person_tables.py \
        corpscout/services/dagster_v3/tests/test_se_companies_serving_sql.py \
        corpscout/services/dagster_v3/tests/test_se_company_person_fold_clickhouse_local.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_se_person_retirement_drops.py
git commit -m "feat(clickhouse): 000398 renames the person entity to se_company_person

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 2: The backoffice names the entity through its one constant

**Files (all paths from `corpscout/services/backoffice`):**
- Modify: `app/lib/se-person-tables.ts:1-12` (the doc comment and the constant)
- Modify: `app/lib/se-company-person-entity.server.ts:61` (a doc comment naming the table)
- Modify: `app/lib/se-people-list.server.ts:3` (a doc comment naming the table)
- Modify: `tests/se-company-person-entity.server.test.ts:200,227`
- Modify: `tests/se-people-list.server.test.ts:32,97,103,113`

**Interfaces:**
- Consumes: the string Task 1 produced — `corpscout.se_company_person`.
- Produces: `SE_COMPANY_PERSON_TABLE === "corpscout.se_company_person"` from `~/lib/se-person-tables`, imported by `se-company-person-entity.server.ts` and `se-people-list.server.ts`. No other backoffice module may spell the table.

**The prefix trap, restated because it bites here twice.** `corpscout.se_company_person` is a prefix of `corpscout.se_company_person_suggestion`, `_normalized`, `_history`, `_rule` and `_precedence`. `tests/se-company-person-entity.server.test.ts:200` dispatches a fake ClickHouse client on `sql.includes("FROM corpscout.se_company_person_v2 AS m FINAL")` and falls through to five more branches keyed on those sibling tables — written as the bare new name, the FIRST branch would answer every one of the six reads with the published persons. `tests/se-people-list.server.test.ts` dispatches on the bare `se_company_person_v2` in three places; those become the aliased form too, even though the list module happens to query no sibling table today.

- [x] **Step 1: Write the failing test edits**

`tests/se-people-list.server.test.ts` — the SQL pin in `it("reads the main table through FINAL, sorted by company then name, paged by parameter")` (line 32) gains the new name and a never-contains half:

```ts
    expect(PEOPLE_LIST_SELECT_SQL).toContain("FROM corpscout.se_company_person AS p FINAL");
    expect(PEOPLE_LIST_SELECT_SQL).not.toContain("se_company_person_v2");
    expect(PEOPLE_COUNTS_SQL).toContain("FROM corpscout.se_company_person AS p FINAL");
```

and the three dispatch/lookup predicates in `it("pages the persons under the resolved ids and names every company of the page in one lookup")` (lines 97, 103 and 113) match the whole qualified name plus the alias:

```ts
      if (String(sql).includes("corpscout.se_company_person AS p FINAL")) return [ROW];
```

```ts
    const listCall = clickhouse.query.mock.calls.find(([sql]) =>
      String(sql).includes("corpscout.se_company_person AS p FINAL"),
    );
```

```ts
    clickhouse.query.mockImplementation(async (sql: string) =>
      String(sql).includes("corpscout.se_company_person AS p FINAL") ? [ROW] : [],
    );
```

`tests/se-company-person-entity.server.test.ts` — the fake client's first dispatch branch (line 200) and the SQL pin (line 227):

```ts
  if (sql.includes("FROM corpscout.se_company_person AS m FINAL")) return [MERGED_ROW, WIKI_ROW];
```

```ts
    expect(PERSON_MAIN_SQL).toContain("FROM corpscout.se_company_person AS m FINAL");
    expect(PERSON_MAIN_SQL).not.toContain("se_company_person_v2");
```

- [x] **Step 2: Run them and watch them fail**

```bash
cd corpscout/services/backoffice
npx vitest run tests/se-people-list.server.test.ts tests/se-company-person-entity.server.test.ts
```

Expected: FAIL. `se-people-list.server.test.ts` fails on the `toContain` pin (the module still renders `se_company_person_v2`); `se-company-person-entity.server.test.ts` fails in the fake client with `unexpected SQL: SELECT …` because no branch matches the main read any more.

- [x] **Step 3: Point the constant at the final name**

`app/lib/se-person-tables.ts`, whole file:

```ts
/**
 * The published SE person entity (spec 2026-09-09, section 3.3), under the name migration
 * 000398 gave it. It was BUILT as `corpscout.se_company_person_v2` -- the 2026-08-19 table
 * held the final name until person slice 0 dropped it -- and renamed in slice 4. Every
 * backoffice read of the published persons names it through this constant.
 *
 * MIND THE PREFIX: `corpscout.se_company_person_suggestion`, `_normalized`, `_history`,
 * `_rule` and `_precedence` all start with this string, so a match on the table -- in a
 * test, or in a fake client's dispatch -- has to carry the alias that follows it.
 */
export const SE_COMPANY_PERSON_TABLE = "corpscout.se_company_person";
```

- [x] **Step 4: Fix the two doc comments that spell the table by hand**

`app/lib/se-company-person-entity.server.ts:61` — the interface comment above `SePersonRow`:

```ts
/** One published person (`se_company_person`, the 29 columns of
```

`app/lib/se-people-list.server.ts:3` — the module header:

```ts
 * The `/admin/se/people` list (person spec section 7): one row per published person of
 * `se_company_person`, read through FINAL, filtered, counted and paged server-side.
```

- [x] **Step 5: Run the tests and the type check**

```bash
cd corpscout/services/backoffice
npx vitest run tests/se-people-list.server.test.ts tests/se-company-person-entity.server.test.ts
npm run typecheck
```

Expected: both suites pass; `typecheck` clean.

- [x] **Step 6: Prove no other backoffice module spells the table**

```bash
cd corpscout/services/backoffice
rg -n "se_company_person_v2" app tests
rg -n "corpscout\.se_company_person([^_a-zA-Z0-9]|$)" app
```

Expected: the first command prints nothing at all. The second prints exactly one line, `app/lib/se-person-tables.ts:12` (the constant) — every other module interpolates `${SE_COMPANY_PERSON_TABLE}`.

- [x] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/backoffice/app/lib/se-person-tables.ts \
        corpscout/services/backoffice/app/lib/se-company-person-entity.server.ts \
        corpscout/services/backoffice/app/lib/se-people-list.server.ts \
        corpscout/services/backoffice/tests/se-company-person-entity.server.test.ts \
        corpscout/services/backoffice/tests/se-people-list.server.test.ts
git commit -m "refactor(backoffice): read the SE person entity under its final name

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 3: The package doc and the spec say the entity's real name

**Files:**
- Modify: `src/dagster_v3/defs/se_company/person/docs/person-design.md:1,9` and the `## Interrupted-migration runbook (000396)` section
- Modify: `docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md:117-121` (section 3.3) and `:561-563` (section 10)

**Interfaces:**
- Consumes: the migration number `000398` and the constant names from Tasks 1 and 2. Nothing consumes this task; Task 4 appends the shipped record to section 9 item 4 of the same spec file.

There is no test to write here — these are two Markdown files. The verification is a grep and a read-through, and the two spec edits must leave the file's own conventions intact (section 3.3 is a table definition with a fenced column block; section 10 is one running paragraph of names).

- [x] **Step 1: Retitle the package doc and correct the `tables.py` row**

`src/dagster_v3/defs/se_company/person/docs/person-design.md`, line 1:

```markdown
# se_company.person (slices 0-4)
```

line 9, the `tables.py` row of the module table — one sentence of `_v2` history, as spec section 9 item 4 asks:

```markdown
| `tables.py` | Table names/column tuples, pinned against migration 000396; main table `se_company_person` since migration 000398 (built as `se_company_person_v2`, because the 2026-08-19 table held the final name until slice 0 dropped it) |
```

- [x] **Step 2: Extend the runbook section to cover 000398**

In the same file, replace the `## Interrupted-migration runbook (000396)` heading and add the rename's own paragraph under it. The section's existing 000396 paragraph stays exactly as it is; this appends after it:

```markdown
## Interrupted-migration runbook (000396 and 000398)
```

```markdown
000398 is the same shape with a rename in the middle and no CREATEs: `SYSTEM STOP VIEW`,
`RENAME TABLE corpscout.se_company_person_v2 TO corpscout.se_company_person`, `ALTER TABLE
... MODIFY QUERY`, `SYSTEM START VIEW`. Between the RENAME and the ALTER the view's stored
query names a table that no longer exists, which is why the view is stopped first. Recovery
by hand, if the migrate client drops in that window:

1. `SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE
   database = 'corpscout' AND view = 'se_companies_serving'` -- a stopped view still lists here.
2. `SELECT name FROM system.tables WHERE database = 'corpscout' AND name LIKE
   'se_company_person%'` says whether the RENAME landed.
3. If it landed but the ALTER did not, run the `ALTER TABLE ... MODIFY QUERY` statement
   verbatim from `000398_corpscout_se_company_person_rename.up.sql`; if neither landed,
   re-running the whole up file is safe once the view has been started again.
4. `SYSTEM START VIEW corpscout.se_companies_serving`.
5. `migrate force 398`.

Apply it OUTSIDE the :45 refresh window -- the refresh takes 13 to 15 minutes, so start just
after one finishes. Neither 000396 nor 000398 uses `SYSTEM WAIT VIEW`, so neither can hit the
staged-swap failure mode that left the ledger dirty at 391: for that one see
`se_company/address/docs/address-design.md`, section "If a serving swap is interrupted" and
"If the 000393 rename is interrupted", and memory `se-companies-serving-view` (the migrate
client's `read_timeout=300` against a 27-minute `SYSTEM WAIT VIEW`).
```

- [x] **Step 3: Give spec section 3.3 the plain name and keep the history in one sentence**

`docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md`, line 117 (the heading) and lines 119-121 (the paragraph under it):

```markdown
### 3.3 `se_company_person` (main)
```

```markdown
One row per company and person. BUILT as `se_company_person_v2` because the 2026-08-19 table
held the final name until slice 0 dropped it, and renamed by migration 000398 in slice 4 with
a `RENAME TABLE` plus `MODIFY QUERY` on the serving view -- the proven address recipe
(000393). 000396's DDL still declares the build name, under the ledger policy that a
historical migration file is history.
```

The fenced column block below it does not change.

- [x] **Step 4: Give spec section 10 the plain name**

Lines 561-563 of the same file lose the parenthetical:

```markdown
Tables `se_company_person_suggestion`, `se_company_person_normalized`, `se_company_person`,
`se_company_person_history`, `se_company_person_rule`, `se_company_person_precedence`;
catalog `company_person_role_type` (kept). Package
```

The rest of the paragraph (the package, asset, job, schedule and backoffice names) is unchanged: none of them carries `_v2`.

- [x] **Step 5: Verify the docs**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
rg -n "se_company_person_v2" corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/docs/person-design.md \
       corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md
```

Expected: exactly three hits, all of them deliberate history — the `tables.py` row of the package doc, the runbook's `RENAME TABLE` line, and section 3.3's "BUILT as" sentence. Section 10, section 8 and the section 9 records must not mention `_v2` as a current name. Read section 9's items 0 to 3 once: they are shipped records and their `_v2` mentions are historical statements of what those slices did, so they stay.

- [x] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/docs/person-design.md \
        corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md
git commit -m "docs(se-person): the entity's final name in the package doc and the spec

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 4: Prod run (controller)

No new code. The migration lands right after the merge and outside the :45 refresh window, and the dagster deploy lands AFTER the migration (controller ruling 2026-09-10 over this plan's first draft: the deployed code then never names a table that does not exist, and the owner's backoffice on the main checkout reads the new name only in the minutes between the merge and the migration). The backoffice runs locally from the main checkout (memory `backoffice-runs-locally`), so its "deploy" is the merge.

1. [x] **Whole-branch review, then merge to main.** The main checkout is on `main` today. If main has taken 000398 in the meantime, renumber FIRST — both migration files, `EXPECTED_MIGRATIONS`, `MIGRATION` in `tests/test_se_companies_serving_mv.py`, the `migrate force 398` line in the up file's header, and the `000398` mentions in `person-design.md` and spec sections 3.3 and 10 — then re-run:

   ```bash
   cd corpscout/services/dagster_v3
   WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
     pytest tests/test_clickhouse_migrations.py tests/test_se_companies_serving_mv.py -q
   ```

2. [x] **Apply the migration, outside the refresh window.** Move the deploy worktree to the merge commit first (`git -C <scratch>/deploy-worktree checkout --detach <merge-commit>`) so the Makefile runs the merged ledger. Wait for a :45 refresh to finish (13-15 min, so start about :00 and no later than :30):

   ```sql
   SELECT view, status, last_success_time, exception
   FROM system.view_refreshes
   WHERE database = 'corpscout' AND view = 'se_companies_serving';
   ```

   Proceed only when `status` is `Scheduled` and `last_success_time` is this hour's :45 run. Then:

   ```bash
   make -s -C <scratch>/deploy-worktree/corpscout clickhouse-migrate-up-one
   ```

   This should return in seconds: there is no `SYSTEM WAIT VIEW` here, which is the statement that outlived the migrate client's `read_timeout=300` at 000391 (memory `se-companies-serving-view`). **If the client drops anyway**, follow the runbook Task 3 put in `person-design.md`: read `system.view_refreshes`; check `SELECT name FROM system.tables WHERE database='corpscout' AND name LIKE 'se_company_person%'` to see whether the RENAME landed; run the `ALTER TABLE ... MODIFY QUERY` from the up file if it did not; `SYSTEM START VIEW corpscout.se_companies_serving`; then `make -s -C <scratch>/deploy-worktree/corpscout clickhouse-migrate-force VERSION=398`.

3. [x] **Deploy the dagster host from a pristine deploy worktree at the merge commit** (memory `se-worktree-deploy-recipe`; the constant changes what `build_se_companies_serving_sql` renders and what `batch.py` writes to, so the host must carry it before the table moves):

   ```bash
   git worktree add <scratch>/deploy-worktree <merge-commit>
   cp corpscout/services/dagster_v3/.env <scratch>/deploy-worktree/corpscout/services/dagster_v3/.env
   cd <scratch>/deploy-worktree/corpscout/services/dagster_v3
   uv sync --frozen
   uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/finland_ytj/dbt --profiles-dir src/dagster_v3/defs/finland_ytj/dbt
   uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/exchange_rates_v2/dbt --profiles-dir src/dagster_v3/defs/exchange_rates_v2/dbt
   uv run --frozen --no-sync dg utils refresh-defs-state
   uv run --frozen --no-sync dg check defs
   cd ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml
   ```

   Capture the playbook's exit code explicitly; never trust `ansible-playbook | tail`. Expect `failed=0`.

   **This step runs after the migration.** Between the merge (step 1) and the migration (step 2) the owner's backoffice on the main checkout names `corpscout.se_company_person` while the database still holds `_v2`; keep that window to minutes and do not open the People tab in it. Nothing scheduled reads the main table: `se_company_person_weekly` is STOPPED and both fold assets are manual.

4. [x] **Read out the rename.** Record the row count from BEFORE the migration (1,126,408 at 2026-09-10 19:05 UTC; take a fresh one just before step 3 in case the slice-3 smoke company was re-folded) and require it unchanged:

   ```sql
   SELECT name, engine, total_rows FROM system.tables
   WHERE database = 'corpscout' AND name LIKE 'se_company_person%' ORDER BY name;

   EXISTS TABLE corpscout.se_company_person_v2;   -- 0
   EXISTS TABLE corpscout.se_company_person;      -- 1

   SELECT count() AS rows, countIf(active = 1) AS active_rows,
          uniqExact(company_id) AS companies
   FROM corpscout.se_company_person;
   ```

   Expected: six `se_company_person*` tables, all under their final names, no `_v2`; `rows` equal to the pre-migration count; `companies` 578,289. A `RENAME TABLE` moves the parts on disk without touching them, so any difference here means something else wrote to the table.

5. [x] **Read out the view.** The stored query must name the new table and nothing else:

   ```sql
   SELECT position(create_table_query, 'corpscout.se_company_person FINAL') AS repointed,
          position(create_table_query, 'se_company_person_v2') AS stale
   FROM system.tables
   WHERE database = 'corpscout' AND name = 'se_companies_serving';

   SELECT view, status, last_success_time, exception
   FROM system.view_refreshes
   WHERE database = 'corpscout' AND view = 'se_companies_serving';
   ```

   Expected: `repointed` > 0, `stale` = 0, `status` `Scheduled` (the view was restarted), no exception. Then WAIT for the next :45 refresh and check it succeeded and the flags are unchanged:

   ```sql
   SELECT countIf(has_people = 1) AS has_people,
          countIf(source_bolagsverket = 1) AS source_bolagsverket,
          countIf(source_esef = 1) AS source_esef,
          count() AS rows
   FROM corpscout.se_companies_serving;
   ```

   Expected: `has_people` 578,289 (± whatever the slice-3 smoke on 5592501521 left, i.e. at most one company), `rows` around 3.5M, and `system.view_refreshes` showing a fresh `last_success_time` with an empty `exception`. **This is the gate:** a re-point that named a table wrongly shows up here as a failed refresh, not as an error at migrate time.

6. [x] **Backoffice smoke on the main checkout's dev server** (`npm run dev`, `http://localhost:5183`). The reads go through `SE_COMPANY_PERSON_TABLE`, so the proof is that the pages answer and that the pinned literal is the new name:

   ```bash
   cd corpscout/services/backoffice
   npx vitest run tests/se-people-list.server.test.ts tests/se-company-person-entity.server.test.ts
   curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:5183/admin/se/people?status=active'
   curl -s -o /dev/null -w '%{http_code}\n' 'http://localhost:5183/admin/se/company/5592501521/people'
   ```

   Expected: both suites green (they pin `FROM corpscout.se_company_person AS p FINAL` and `… AS m FINAL`), both curls `200`. Then open both pages in a browser: the People list's counts strip must read the same `persons` / `companies` numbers step 4 measured, and the company's People tab must render its persons, members, roles, `data`, history and raw evidence.

7. [x] **Prove the write path.** Launch `se_company_person_fold_companies` from the Dagster UI with config

   ```yaml
   ops:
     se_company_person_fold_companies:
       config:
         company_ids: ["5592501521"]
   ```

   (`changed_only` defaults to `false` for the targeted fold, so it re-folds unconditionally.) Expected in the run's materialization metadata: `table` = `corpscout.se_company_person`, `history_table` = `corpscout.se_company_person_history`, `considered` 1, `unchanged` equal to the company's person count, and `created`/`updated`/`hidden`/`withdrawn`/`reactivated` all 0 — the fold rewrote the rows and nothing changed, which is exactly what a rename should look like from the writer's side. Confirm on the server:

   ```sql
   SELECT person_key, display_name, sources, active, folded_at
   FROM corpscout.se_company_person FINAL
   WHERE company_id = '5592501521' ORDER BY display_name;

   SELECT count() FROM corpscout.se_company_person_history
   WHERE company_id = '5592501521' AND changed_at > now() - INTERVAL 1 HOUR;
   ```

   Expected: the persons back with a fresh `folded_at`, and 0 new history rows.

8. [x] **Record and archive.**
   - Append the slice-4 shipped record to spec section 9 item 4: migration 000398 (the number it actually got), the one-pair `RENAME TABLE`, the in-place `MODIFY QUERY`, the three constants, the tests that replay the rename over 000396's DDL, the readouts of steps 4 to 7, and the ruling that `se_company_person` now appears BOTH on slice 0's dropped list and as the live entity (the drop scripts are spent and must never be re-run).
   - Note in the ledger that `tests/test_se_companies_serving_mv.py` now pins **000398** as the migration carrying the serving definition — the next reader of that view must re-point it again.
   - Archive the ledger under `.superpowers/sdd/person-entity/`.
   - Update memory `se-address-entity.md`'s PEOPLE entity line: slice 4 live, the entity is `se_company_person`, the person entity is complete; and memory `se-companies-serving-view.md`: the drift pin is at 000398.

---

## Self-review

**1. Spec coverage.**

- **Section 3.3** ("built as `_v2` … the rename is a `RENAME TABLE` plus `MODIFY QUERY` on the serving view, the proven address recipe"): Task 1 Steps 3 and 4 write exactly that migration — `SYSTEM STOP VIEW`, one `RENAME TABLE` pair, `ALTER TABLE … MODIFY QUERY` with a machine-rendered body, `SYSTEM START VIEW` — and Task 3 Step 3 rewrites the section itself onto the plain name with one sentence of history.
- **Section 8** (the readers after the cutover: the serving view's `has_people`, `people_bolagsverket` and `people_esef` through the builder constant; the backoffice tab and list; nothing else): Task 1 Step 1 moves the builder constant and Task 1 Step 5's `test_the_pin_is_not_vacuous` asserts the three flag subqueries read the new name three times. Task 2 moves the backoffice constant and its two pins. Task 1 Step 13's `rg` over `src/` and Task 2 Step 6's `rg` over `app` prove no third reader exists.
- **Section 9 item 4** ("Rename and docs … the design doc under the package, memory"): Tasks 1 and 2 are the rename, Task 3 the design doc, Task 4 step 8 the shipped record and the memory updates.
- **Section 10** (names): Task 3 Step 4. Every other name in that paragraph — package modules, `se_company_person_suggestions_<source>`, `se_company_person_normalize`, `se_company_person_fold`, `se_company_person_fold_companies`, `se_company_person_precedence_clickhouse`, `se_company_person_extract_job`, `se_company_person_weekly`, the eleven backoffice files and two routes — carries no `_v2` and is untouched, which is why `dg check defs` is expected green rather than merely passing.
- **Not in this slice, by ruling R6:** no asset, job, schedule or pool rename; no backoffice UI change; no drop of any ClickHouse object; no edit to a historical migration file or to the spent `corpscout/clickhouse/operations/se_person_retirement_*.sql` scripts.

**2. Placeholder scan.** Every code step carries its code: both migration headers and all five statements per file, the drift pin's constants and five tests, the four dagster test replays, the two backoffice test edits and the constant, the two doc comments, the four Markdown edits, and every prod query with its expected value. The ONE thing not inlined is the 171-line rendered `MODIFY QUERY` body, and that is deliberate for the same reason the address plan gave: inlining it would guarantee drift from the builder the pin compares it against. Task 1 Step 2 gives the exact command that prints it, the six checks that prove the output is the right one, and the three lines that are the entire difference between the up and down bodies.

**3. Type consistency.** `person.tables.MAIN_TABLE = "se_company_person"` (Task 1 Step 7) makes `tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_person"`, which is the same string as `companies_current.COMPANY_PERSON_TABLE` (Task 1 Step 1) and as the backoffice's `SE_COMPANY_PERSON_TABLE` (Task 2 Step 3). `MAIN_DDL_TABLE = "se_company_person_v2"` (Task 1 Step 9) is deliberately the OTHER string — it names the DDL in 000396, never the deployed table — and the same distinction drives the `_schema_statements()` replay in the fold clickhouse-local fixture and `SLICE_0_KEPT_OBJECTS` in the migrations test. `MIGRATION`/`PREVIOUS_MIGRATION` in the drift pin are `000398_corpscout_se_company_person_rename` and `000396_corpscout_se_company_person_entity`, matching the file names Task 1 Step 3 creates and the file 000396 already is.

**4. The whole-name hazard, task by task.** `corpscout.se_company_person` prefixes five sibling tables, and this plan touches six places where a bare substring match would be wrong:
- the drift pin counts `f"{ENTITY} FINAL"`, not `ENTITY`, and asserts each of the five siblings is absent from the view body (Task 1 Step 5);
- `test_the_entity_name_is_a_prefix_of_five_siblings` asserts `name.startswith(f"{tables.MAIN_TABLE}_")` and `name != tables.MAIN_TABLE` (Task 1 Step 9);
- `_schema_statements()` filters on the `se_company_person_` prefix BEFORE replacing, because the new name does not carry the trailing underscore the filter matches (Task 1 Step 9);
- `test_the_drop_script_names_no_kept_object` compares whole names and excludes exactly one reused name rather than weakening the check (Task 1 Step 12);
- the entity server test's fake client dispatches on `FROM corpscout.se_company_person AS m FINAL`, ahead of five sibling branches (Task 2 Step 1);
- the people-list test dispatches on `corpscout.se_company_person AS p FINAL` in three places (Task 2 Step 1).

**5. The name collision this slice creates, stated once.** `corpscout.se_company_person` is the name person slice 0 dropped on 2026-09-09 (the 2026-08-19 model's table) and the name 000398 gives the live entity a day later. Three artefacts now carry both meanings and each is handled explicitly: `SLICE_0_DROPPED_OBJECTS` in `tests/test_clickhouse_migrations.py` (a guard on DECLARATIONS — a `RENAME TABLE` declares nothing, comment added in Task 1 Step 11); `DROP_ORDER` versus `KEPT` in `tests/test_se_person_retirement_drops.py` (`REUSED_NAME`, Task 1 Step 12); and `corpscout/clickhouse/operations/se_person_retirement_drops.sql` itself, which is left byte-for-byte as it ran but is now capable of destroying 1.1M published persons if anyone re-runs it. Task 1 Step 12 puts that warning in the test module's docstring and Task 3 Step 2 in the package doc's runbook section, because R6 forbids editing anything else in this slice. **Open for the controller:** whether the spent script should also carry a `-- SPENT` banner in the SQL file itself, where an operator would actually see it. That is a comment-only edit to a file nothing executes; it is not in this plan.
