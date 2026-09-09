# SE company person entity, slice 0: tables, normalizer, retirement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the six person-entity tables and re-point the serving view's people flags at them in one migration, build the `se_company/person` package with the Swedish person normalizer, its golden corpus and the normalize asset, and delete every trace of the 2026-08-19 people chain — the `company_people` Dagster package, the backoffice People area, the SE Management section and its two dbt models — down to owner-run drop scripts and the migration-file edits.

**Architecture:** The third entity on the basic-info shape, built exactly like `se_company/address`: migrations own the schema, `tables.py` pins names and column tuples against the DDL, a pure per-country normalizer with a JSONL golden corpus, and one paged Dagster asset that turns raw suggestion rows into normalized rows. Nothing extracts (slice 1) and nothing folds (slice 2), so the new tables stay empty on prod and the serving flags read empty until slice 2. The retirement is four deletions in dependency order — the Dagster package with its nine jobs and its sensor, the backoffice admin People area and company People tab, the SE Management section with its dbt models and `company_serving` contract (the component and the legacy detail mode other countries render stay), and the shared `clickhouse-local` test helper that has to move before its host file dies — followed by three owner-run SQL scripts and the ledger cleanup. No numbered migration drops anything.

**Tech Stack:** ClickHouse 26.5 (golang-migrate), clickhouse-driver 0.2.10 through `dagster_clickhouse.ClickhouseResource`, Dagster 1.13.9 (`uv run --frozen --no-sync`), Python 3.14, pytest, `clickhouse-local` for the integration test, dbt (`company_serving`), React Router v7 backoffice (vitest).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md` — slice 0 is section 9 item 0; its content is sections 3, 4 and 8, with section 10 for the names.

## Global Constraints

- Dagster commands run from `corpscout/services/dagster_v3` as `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/<file> -q`; `uv run --frozen --no-sync dg check defs` must pass before every commit that touches `src/`, and **after every module deletion inside a task**, not only at its end.
- Backoffice commands run from `corpscout/services/backoffice`: `npx vitest run <files>` and `npm run typecheck`. dbt parses run as `uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/company_serving/dbt --profiles-dir src/dagster_v3/defs/company_serving/dbt` from `corpscout/services/dagster_v3`.
- **The migration number is `000395` at write time and the controller renumbers it at merge.** The highest migration on main at the branch point is `000394`; another session's ESEF branch holds its own `000395` and will merge first or second. Renumbering means four edits: both migration file names, the entry in `EXPECTED_MIGRATIONS` (`tests/test_clickhouse_migrations.py`), `MIGRATION` in `tests/test_se_companies_serving_mv.py`, and the `migrate force 395` line in the migration's header comment.
- **`se_company_person` is a prefix of eleven other names**: `se_company_person_suggestion`, `_normalized`, `_v2`, `_history`, `_rule`, `_precedence` (the new six) and `_role`, `_role_draft`, `_correction`, `_enrichment_observation`, `_collision_candidate` (five of the dropped ones). So is `company_person_role`, which prefixes the KEPT `company_person_role_type`. Every string match on a table name — a test assertion, a fake client's dispatch branch, an `rg` check, the drop-script guard — must compare **whole names** (split the statement and compare tokens, or anchor with `([^_a-zA-Z0-9]|$)`), never a bare `in` on the qualified string. A guard written as `"company_person_role" not in script` fires on the role catalog that stays.
- **`data` is a `String` holding a JSON object at every layer** — suggestion, normalized, main and history — never ClickHouse's native `JSON` type (owner ruling 2026-09-09; the spec is amended the same way). Every table that carries one also carries `CONSTRAINT valid_data CHECK JSONType(data) = 'Object'`, pinned by the DDL test, and the normalize SQL coerces anything that is not a JSON object to `'{}'` before the row is read, so the constraint can only fire on a hand-written row. Verified 2026-09-09 against `clickhouse/clickhouse-server:26.5`: the constraint accepts `{}` and `{"x":1}` and rejects `[1,2]` with `Code: 469 VIOLATED_CONSTRAINT`. Reads and writes therefore stay on clickhouse-driver's ordinary block path, exactly as the address entity's do — there is no text-INSERT machinery anywhere in this entity.
- **`role_key` is a real `Nullable(String)` column** on `se_company_person_suggestion` and `se_company_person_normalized` (owner ruling 2026-09-09; the spec is amended the same way): the source's own role code, passed through untouched, beside the human label in `role_original`. The three per-source role maps are keyed on it, not on the label. Extractors fill it in slice 1; the normalizer reads the column, never `data`.
- Column names and order in `tables.py` equal the DDL exactly (`tests/se_company_ddl.py::declared_columns` is the judge). The suggestion, normalized, main and rule tables each carry `CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')`; the precedence table's is `CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')`; the history table has none. (The spec's DDL listing omits the constraints; both predecessor entities carry them and `tests/se_company_ddl.py` reads the same shape.)
- The main table is `se_company_person_v2` for the whole of slices 0 to 3; slice 4 renames it to `se_company_person`. Nothing in this plan renames it.
- `NORMALIZER_VERSION = "se-person-normalizer-v1"`. Parse statuses are exactly `ok`, `partial`, `no_person`. `normalized_id = sha256(f"{suggestion_id}\n{normalizer_version}")` (spec 3.2 — **not** the address entity's stamp-based id, so re-running the normalizer on an unchanged suggestion writes the identical id).
- Every `.up.sql` starts with `CREATE DATABASE IF NOT EXISTS corpscout;`, has **no `;` inside a `--` comment**, and ends with a statement rather than prose (`tests/test_clickhouse_migrations.py`). Every emptied file's only statement line is exactly `CREATE DATABASE IF NOT EXISTS corpscout;`.
- **Nothing in this plan executes DDL against a server, and no numbered migration drops anything.** The drops are hand-run by the owner from a committed script under the dev-phase ledger policy (memory `clickhouse-ledger-squash-planned`; owner ruling 2026-08-25). `EXPECTED_MIGRATIONS` grows by exactly one name (the new migration); only `EMPTIED_MIGRATIONS` grows for the retirement.
- **Editing a historical migration file is only legal when the object is gone from the server**, so on prod the order is: merge → deploy dagster → apply the migration → precheck → owner runs the drops. The repo may sit briefly with the DDL removed and the objects still present; nothing replays the ledger on prod (forward-only).
- No `from __future__ import annotations` in any module that defines a `@dg.asset`.
- Delete a Python or TypeScript module only when `rg` shows no importer outside the deleted set; otherwise move the still-imported symbol to a keeper and then delete. Two symbols in this slice are exactly that case and are handled explicitly: the three per-source role maps (Task 2 moves them, Task 4 deletes their old modules) and `tests/test_se_company_person_clickhouse_local.py`'s `_clickhouse_local_command` / `_literal`, imported by eight other test files (Task 4 moves them to `tests/clickhouse_local.py`).
- **Another session merges to main daily.** Merge through a worktree that checks main out (memory `se-worktree-deploy-recipe`); the main checkout may sit on another branch.
- Commit by explicit path after every task; never `git add -A`. Trailers, contiguous at the end of every commit message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- Pre-existing unrelated failures, not regressions of this slice: `tests/test_se_company_address_extractors_clickhouse_local.py` (broken since main's 000390 — the fixture lacks `se_ratsit_company_translated`), `tests/test_schedule_cron_contracts.py` (four `(minute, hour)` collisions), `tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values`; backoffice `admin-se-company-esef.test.tsx` and the live-ClickHouse timeouts under the full suite.

---

## File structure

| file | change |
| --- | --- |
| `corpscout/clickhouse/migrations/000396_corpscout_se_company_person_entity.{up,down}.sql` | **create** — the six tables and the serving re-point |
| `src/dagster_v3/defs/se_company/person/__init__.py` | **create** — package docstring pointing at the spec |
| `.../person/tables.py` | **create** — names, qualified names, column tuples, sources, parse statuses |
| `.../person/roles.py` | **create** — the three per-source maps, moved, plus `role_code_for` |
| `.../person/normalize_se.py` | **create** — `RawPerson`, `NormalizedPerson`, `normalize_se_person`, `NORMALIZER_VERSION`. Pure |
| `.../person/normalize.py` | **create** — the scan/read/insert SQL, row shaping, the batch functions |
| `.../person/assets.py` | **create** — `GROUP_NAME`, `NORMALIZE_POOL`, `PersonNormalizeConfig`, `se_company_person_normalize` |
| `.../person/jobs.py` | **not created** — the extract job and the stopped weekly arrive with the extractors in slice 1 |
| `.../person/docs/person-design.md` | **create** — module map |
| `src/dagster_v3/defs/sweden_company/companies_current.py` | modify — `COMPANY_PERSON_TABLE` and the three people IN-sets |
| `src/dagster_v3/defs/company_people/` (6 modules) | **delete** |
| `src/dagster_v3/defs/{sweden_financial,esef_filings,wikidata}/roles.py` | **delete** (moved into the person package) |
| `src/dagster_v3/defs/company_serving/{tables.py,publish.py}` | modify — the MANAGEMENT contract goes |
| `.../company_serving/dbt/models/company_management_current_build.sql` | **delete** |
| `.../company_serving/dbt/models/{company_section_presence_current_build,company_section_item_source_links_build}.sql`, `{schema,sources}.yml` | modify |
| `corpscout/services/backoffice/app/...` (routes, libs, components, nav, breadcrumbs) | **delete/modify** — Task 5 and Task 6 list every path; `management-section.tsx` and the French loader chain are untouched |
| `corpscout/clickhouse/operations/se_person_retirement_{precheck,drops,postcheck}.sql` | **create** |
| `tests/clickhouse_local.py` | **create** — the shared `clickhouse-local` helper, moved off the dying person test |
| `tests/fixtures/se_persons/golden.jsonl` | **create** — the golden corpus |
| `tests/test_se_company_person_tables.py`, `_normalize_se.py`, `_normalize.py`, `_normalize_clickhouse_local.py`, `tests/test_se_person_retirement_drops.py` | **create** |
| `tests/test_se_company_person*.py` (the nine old ones) | **delete** |

---

### Task 1: Migration 000395 — the six tables and the serving re-point

**Files:**
- Create: `corpscout/clickhouse/migrations/000396_corpscout_se_company_person_entity.up.sql`, `...down.sql`
- Create: `src/dagster_v3/defs/se_company/person/__init__.py`, `src/dagster_v3/defs/se_company/person/tables.py`
- Modify: `src/dagster_v3/defs/sweden_company/companies_current.py` (the `COMPANY_PERSON_TABLE` constant and `PEOPLE_SET` / `PEOPLE_BOLAGSVERKET_SET` / `PEOPLE_ESEF_SET`)
- Modify: `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`, one name after `"000394_corpscout_se_company_basic_info_economic_activity"`)
- Modify: `tests/test_se_companies_serving_mv.py` (the drift pin moves to 000395)
- Modify: `tests/test_se_companies_serving_sql.py:409-410,423-424` (the clickhouse-local fixture's people stubs)
- Test: `tests/test_se_company_person_tables.py`

**Interfaces:**
- Produces: `tables.DATABASE`, `SUGGESTION_TABLE`, `NORMALIZED_TABLE`, `MAIN_TABLE`, `HISTORY_TABLE`, `RULE_TABLE`, `PRECEDENCE_TABLE` and their `QUALIFIED_*` twins; `SUGGESTION_COLUMNS`, `NORMALIZED_COLUMNS`, `MAIN_COLUMNS`, `HISTORY_COLUMNS`, `RULE_COLUMNS`, `PRECEDENCE_COLUMNS`, `MEMBER_COLUMNS`, `ROLE_BLOCK_COLUMNS`, `SOURCES`, `PARSE_STATUSES`, `RULE_KINDS`, `INACTIVE_REASONS`, `CHANGE_KINDS` (Tasks 2, 3 and slices 1 to 4 use them); `companies_current.COMPANY_PERSON_TABLE == "corpscout.se_company_person_v2"`.
- Consumes: `tests/se_company_ddl.py::declared_columns` and `table_block`; `companies_current.build_se_companies_serving_sql()`.

- [ ] **Step 1: Write the failing test**

`tests/test_se_company_person_tables.py`:

```python
"""The six person-entity tables (spec 2026-09-09 section 3), pinned against the migration
DDL through tests/se_company_ddl.py so tables.py and the deployed schema cannot drift."""

from dagster_v3.defs.se_company.person import tables
from tests.se_company_ddl import declared_columns, table_block

COMPANY_ID_CHECK = "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')"
# `data` is a String holding a JSON object, not the native JSON type (spec 3, amended
# 2026-09-09): clickhouse-driver reads and writes it like any other String, and this
# constraint is what stops a hand-written row from putting an array or a scalar in it.
DATA_CHECK = "CONSTRAINT valid_data CHECK JSONType(data) = 'Object'"


def test_suggestion_table_is_one_current_row_per_company_source_and_slot() -> None:
    block = table_block("se_company_person_suggestion")
    assert declared_columns("se_company_person_suggestion") == list(tables.SUGGESTION_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(suggested_at)" in block
    assert "ORDER BY (company_id, source, slot)" in block
    assert COMPANY_ID_CHECK in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    full_name Nullable(String)," in block
    assert "    first_name Nullable(String)," in block
    assert "    last_name Nullable(String)," in block
    assert "    birth_year Nullable(UInt16)," in block
    assert "    role_original Nullable(String)," in block
    assert "    role_key Nullable(String)," in block
    assert "    role_from Nullable(Date)," in block
    assert "    data String," in block
    assert DATA_CHECK in block
    assert "MATERIALIZED" not in block


def test_normalized_table_has_the_same_key_and_its_own_version() -> None:
    block = table_block("se_company_person_normalized")
    assert declared_columns("se_company_person_normalized") == list(tables.NORMALIZED_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(normalized_at)" in block
    assert "ORDER BY (company_id, source, slot)" in block
    assert COMPANY_ID_CHECK in block
    assert "    normalized_id FixedString(64)," in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    normalizer_version LowCardinality(String)," in block
    assert "    parse_status LowCardinality(String)," in block
    assert "    parse_notes Array(String)," in block
    for column in ("first_tokens", "middle_tokens", "last_tokens"):
        assert f"    {column} Array(String)," in block, column
    assert "    role_key Nullable(String)," in block
    assert "    data String," in block
    assert DATA_CHECK in block
    # The normalized row carries no suggested_at: suggestion_id already names the raw
    # version it was computed from, and that is what the change scan compares.
    assert "suggested_at" not in block


def test_main_table_is_one_row_per_company_and_person() -> None:
    block = table_block("se_company_person_v2")
    assert declared_columns("se_company_person_v2") == list(tables.MAIN_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(folded_at)" in block
    assert "ORDER BY (company_id, person_key)" in block
    assert COMPANY_ID_CHECK in block
    assert "    person_key FixedString(64)," in block
    assert "    sources Array(LowCardinality(String))," in block
    assert "    normalized_ids Array(FixedString(64))," in block
    assert "    member_birth_years Array(Nullable(UInt16))," in block
    assert "    member_data Array(String)," in block
    assert "    role_sources Array(Array(String))," in block
    assert "    active UInt8," in block
    assert "    data String," in block
    assert DATA_CHECK in block
    for column in tables.MEMBER_COLUMNS:
        assert f"    {column} Array(" in block, column


def test_history_is_the_main_row_plus_the_change_block() -> None:
    block = table_block("se_company_person_history")
    assert declared_columns("se_company_person_history") == list(tables.HISTORY_COLUMNS)
    assert tables.HISTORY_COLUMNS == (*tables.MAIN_COLUMNS, "changed_at", "change_kind", "fold_run_id")
    # Append-only, written only by the fold from rows the main table already validated:
    # neither constraint is repeated here.
    assert "CONSTRAINT" not in block
    assert "    data String," in block
    assert "ENGINE = MergeTree" in block
    assert "ORDER BY (company_id, person_key, changed_at)" in block
    assert "    change_kind LowCardinality(String)," in block


def test_rule_table_is_a_per_company_reviewer_decision() -> None:
    block = table_block("se_company_person_rule")
    assert declared_columns("se_company_person_rule") == list(tables.RULE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(created_at)" in block
    assert "ORDER BY (company_id, rule_id)" in block
    assert COMPANY_ID_CHECK in block
    assert "    kind LowCardinality(String)," in block
    assert "    person_keys Array(FixedString(64))," in block
    assert "    slots Array(String)," in block
    assert "    active UInt8," in block


def test_precedence_table_has_the_basic_info_shape() -> None:
    block = table_block("se_company_person_precedence")
    assert declared_columns("se_company_person_precedence") == list(tables.PRECEDENCE_COLUMNS)
    assert tables.PRECEDENCE_COLUMNS == (
        "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
    )
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, field, source)" in block
    assert "CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')" in block


def test_column_tuples_agree_with_each_other() -> None:
    assert tables.QUALIFIED_SUGGESTION_TABLE == "corpscout.se_company_person_suggestion"
    assert tables.QUALIFIED_NORMALIZED_TABLE == "corpscout.se_company_person_normalized"
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_person_v2"
    assert tables.QUALIFIED_HISTORY_TABLE == "corpscout.se_company_person_history"
    assert tables.QUALIFIED_RULE_TABLE == "corpscout.se_company_person_rule"
    assert tables.QUALIFIED_PRECEDENCE_TABLE == "corpscout.se_company_person_precedence"
    assert tables.SOURCES == ("bolagsverket", "esef", "wikidata", "ratsit", "reviewer", "reviewer_draft")
    assert tables.PARSE_STATUSES == ("ok", "partial", "no_person")
    assert tables.RULE_KINDS == ("hide", "merge", "split")
    assert tables.INACTIVE_REASONS == ("", "hidden", "withdrawn")
    assert tables.CHANGE_KINDS == ("created", "updated", "hidden", "withdrawn", "reactivated")
    assert tables.ROLE_TYPE_TABLE == "company_person_role_type"
    for column in tables.MEMBER_COLUMNS:
        assert column in tables.MAIN_COLUMNS and column.startswith("member_")
    for column in tables.ROLE_BLOCK_COLUMNS:
        assert column in tables.MAIN_COLUMNS


def test_the_entity_name_is_a_prefix_of_five_siblings() -> None:
    """Whole-name matching, everywhere. corpscout.se_company_person_v2 is the entity now and
    se_company_person is the name it takes in slice 4 -- and that name prefixes all five of
    the tables below, plus every table this slice drops."""
    siblings = (
        tables.SUGGESTION_TABLE, tables.NORMALIZED_TABLE, tables.HISTORY_TABLE,
        tables.RULE_TABLE, tables.PRECEDENCE_TABLE,
    )
    for name in siblings:
        assert name.startswith("se_company_person_")
        assert name != "se_company_person"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_tables.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.person'`.

- [ ] **Step 3: Point the serving builder at the new table**

In `src/dagster_v3/defs/sweden_company/companies_current.py`, add the constant beside `COMPANY_ADDRESS_TABLE` (line 81):

```python
# The SE person entity (migration 000395). This constant is the one place the view names
# it. The table is empty until slice 2's first fold, so the three people flags below read
# 0 for every company between this migration and that fold -- deliberately (spec section
# 8: "the flags read empty tables until the first fold"). It is se_company_person_v2 for
# slices 0 to 3; slice 4 renames it and edits this one line.
COMPANY_PERSON_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_person_v2"
```

and replace the three people sets (today at lines 206-214) with:

```python
# Active published persons only, read FINAL (ReplacingMergeTree(folded_at)): a person the
# reviewer hid or whose observations all became tombstones must not keep the flag lit.
PEOPLE_SET = f"SELECT company_id FROM {COMPANY_PERSON_TABLE} FINAL WHERE active = 1"
PEOPLE_BOLAGSVERKET_SET = (
    f"SELECT company_id FROM {COMPANY_PERSON_TABLE} FINAL "
    "WHERE active = 1 AND has(sources, 'bolagsverket')"
)
PEOPLE_ESEF_SET = (
    f"SELECT company_id FROM {COMPANY_PERSON_TABLE} FINAL "
    "WHERE active = 1 AND has(sources, 'esef')"
)
```

- [ ] **Step 4: Render the new and the old view bodies into scratch files**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync python -c \
  "from dagster_v3.defs.sweden_company.companies_current import build_se_companies_serving_sql; print(build_se_companies_serving_sql())" \
  > /tmp/serving_person_new.sql
uv run --frozen --no-sync python -c "
from pathlib import Path
sql = Path('../../clickhouse/migrations/000393_corpscout_se_company_address_rename.up.sql').read_text()
statement = [s for s in sql.split(';') if 'MODIFY QUERY' in s][0]
marker = 'MODIFY QUERY' + chr(10)
print(statement[statement.index(marker) + len(marker):])
" > /tmp/serving_person_old.sql
grep -c "se_company_person_v2 FINAL" /tmp/serving_person_new.sql   # 3
grep -c "se_company_person_role" /tmp/serving_person_old.sql       # 2
```

`/tmp/serving_person_new.sql` must contain `corpscout.se_company_person_v2 FINAL` three times and no `corpscout.se_company_person_role`; `/tmp/serving_person_old.sql` the reverse. Both end with the `SETTINGS join_algorithm = 'grace_hash,hash', ... max_memory_usage = 12884901888` block — that block is part of the body and travels with the `MODIFY QUERY`.

- [ ] **Step 5: Write the up migration**

`corpscout/clickhouse/migrations/000396_corpscout_se_company_person_entity.up.sql` — the header comment below, then exactly ten statements: `CREATE DATABASE`, the six `CREATE TABLE`s in spec order, `SYSTEM STOP VIEW`, `ALTER TABLE ... MODIFY QUERY` (paste `/tmp/serving_person_new.sql` where marked, do not hand-edit it), `SYSTEM START VIEW`.

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- THE SE COMPANY PERSON ENTITY (spec 2026-09-09 sections 3 and 8, slice 0). Six tables on
-- the basic-info shape -- raw suggestions, a stored normalized layer with its own version,
-- the folded main table with history, and the reviewer's rules and precedence -- plus the
-- serving view's people flags moved onto the new main table in the same file.
--
-- WHY THE SERVING RE-POINT RIDES WITH THE CREATES. The old chain
-- (corpscout.se_company_person and se_company_person_role, 000291/000292/000293) is dropped
-- by hand in this same slice, and the serving view is its last reader. Re-pointing here
-- means the view never names a table that is about to disappear. The new table is EMPTY
-- until slice 2's first fold, so has_people, people_bolagsverket and people_esef read 0 for
-- every company in between. That is deliberate and owner-agreed -- the admin companies list
-- keeps its filters, they simply match nothing until the fold runs.
--
-- NO STAGED _next SWAP. 000391 and 000392 REPLACED the view's definition, which meant
-- building a second view and swapping names. This migration changes which tables the same
-- definition reads, and ALTER TABLE's MODIFY-QUERY clause does that in place -- the proven
-- 000393 recipe: the refreshable view keeps the rows it is already serving and its next
-- scheduled refresh (hourly at :45, migration 000366) runs the new query. No _next to
-- populate and no refresh wait to sit through.
--
-- THE VIEW IS STOPPED FIRST so no refresh can land between the CREATEs and the new query.
--
-- IF THE MIGRATE CLIENT DROPS between the STOP and the START, the view is left stopped and
-- serving its last contents at full speed with nothing raising anywhere. Recovery is by
-- hand: check corpscout.se_companies_serving in system.view_refreshes, run SYSTEM START VIEW
-- corpscout.se_companies_serving, then migrate force 395 so the ledger records where the
-- database actually is. The person design doc's runbook section has the full sequence.
--
-- data IS A String HOLDING A JSON OBJECT, not the native JSON type (owner ruling
-- 2026-09-09). It reads and writes like any other String, so nothing about this entity's
-- assets differs from the address entity's, and the CONSTRAINT valid_data on each table
-- carrying one is what keeps an array or a scalar out. The normalize step coerces anything
-- else to the empty object before it inserts, so the constraint only ever fires on a
-- hand-written row. See se_company/person/docs/person-design.md.
--
-- THE SELECT AT THE END IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering
-- of companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

-- Raw person suggestions (spec 3.1): what each source delivered, one current row per
-- company, source and slot, never normalized. A source that stops delivering a slot writes
-- a row with every person column NULL (tombstone). role_original is the human label and
-- role_key the source's own code beside it (Bolagsverket's role kind, Wikidata's property
-- id, ESEF's role category) -- the per-source maps are keyed on the code.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_suggestion
(
    company_id String,
    source LowCardinality(String),
    slot String,
    suggestion_id FixedString(64),
    suggested_at DateTime64(3, 'UTC'),
    source_record_id String,
    full_name Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    birth_year Nullable(UInt16),
    wikidata_id Nullable(String),
    role_original Nullable(String),
    role_key Nullable(String),
    fiscal_year Nullable(UInt16),
    role_from Nullable(Date),
    role_to Nullable(Date),
    document_ref Nullable(String),
    data String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_data CHECK JSONType(data) = 'Object'
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source, slot);

-- Normalized person suggestions (spec 3.2): the same key and row count as the raw table,
-- written only by the normalize asset. normalized_id names this version and suggestion_id
-- the raw version it was computed from -- there is no suggested_at here, because a changed
-- observation always changes suggestion_id and that is what the change scan compares.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_normalized
(
    company_id String,
    source LowCardinality(String),
    slot String,
    suggestion_id FixedString(64),
    normalized_id FixedString(64),
    normalizer_version LowCardinality(String),
    parse_status LowCardinality(String),
    parse_notes Array(String),
    first_tokens Array(String),
    middle_tokens Array(String),
    last_tokens Array(String),
    display_first String,
    display_last String,
    display_name String,
    birth_year Nullable(UInt16),
    wikidata_id Nullable(String),
    role_key Nullable(String),
    role_code Nullable(String),
    role_year Nullable(UInt16),
    role_from Nullable(Date),
    role_to Nullable(Date),
    data String,
    normalized_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_data CHECK JSONType(data) = 'Object'
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, source, slot);

-- Published persons (spec 3.3): one row per company and person, written by the fold. Built
-- as se_company_person_v2 because the 2026-08-19 table holds the name until this slice
-- drops it -- slice 4 renames this one into its place.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_v2
(
    company_id String,
    person_key FixedString(64),
    display_name String,
    first_name String,
    last_name String,
    birth_year Nullable(UInt16),
    wikidata_id Nullable(String),
    sources Array(LowCardinality(String)),
    slots Array(String),
    normalized_ids Array(FixedString(64)),
    member_sources Array(LowCardinality(String)),
    member_slots Array(String),
    member_names Array(String),
    member_birth_years Array(Nullable(UInt16)),
    member_wikidata_ids Array(String),
    member_data Array(String),
    role_codes Array(String),
    role_years Array(UInt16),
    role_sources Array(Array(String)),
    current_roles Array(String),
    first_year Nullable(UInt16),
    last_year Nullable(UInt16),
    text_source LowCardinality(String),
    data String,
    active UInt8,
    inactive_reason LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_data CHECK JSONType(data) = 'Object'
)
ENGINE = ReplacingMergeTree(folded_at)
ORDER BY (company_id, person_key);

-- Person history (spec 3.4): the previous published row, appended before the main write
-- whenever a fold changes it. Append-only, no company_id constraint (the main table
-- enforces it), and three columns the main table does not carry.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_history
(
    company_id String,
    person_key FixedString(64),
    display_name String,
    first_name String,
    last_name String,
    birth_year Nullable(UInt16),
    wikidata_id Nullable(String),
    sources Array(LowCardinality(String)),
    slots Array(String),
    normalized_ids Array(FixedString(64)),
    member_sources Array(LowCardinality(String)),
    member_slots Array(String),
    member_names Array(String),
    member_birth_years Array(Nullable(UInt16)),
    member_wikidata_ids Array(String),
    member_data Array(String),
    role_codes Array(String),
    role_years Array(UInt16),
    role_sources Array(Array(String)),
    current_roles Array(String),
    first_year Nullable(UInt16),
    last_year Nullable(UInt16),
    text_source LowCardinality(String),
    data String,
    active UInt8,
    inactive_reason LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    changed_at DateTime64(3, 'UTC'),
    change_kind LowCardinality(String),
    fold_run_id String
)
ENGINE = MergeTree
ORDER BY (company_id, person_key, changed_at);

-- Reviewer rules (spec 3.5): hide, merge and split, keyed by the fold's output. active = 0
-- undoes a rule (the tab's Reset); a rule is never edited in place.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_rule
(
    company_id String,
    rule_id FixedString(64),
    kind LowCardinality(String),
    person_keys Array(FixedString(64)),
    slots Array(String),
    active UInt8,
    note String,
    created_at DateTime64(3, 'UTC'),
    created_by String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (company_id, rule_id);

-- Person source precedence (spec 3.6): the basic-info and address shape. company_id ''
-- rows are the global order exported from precedence.py; the only field with a precedence
-- is `name`, and it decides the displayed spelling only -- never which persons publish.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_precedence
(
    company_id String,
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, field, source);

SYSTEM STOP VIEW corpscout.se_companies_serving;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
<<< paste /tmp/serving_person_new.sql here, verbatim, then a semicolon >>>

SYSTEM START VIEW corpscout.se_companies_serving;
```

- [ ] **Step 6: Write the down migration**

`000396_corpscout_se_company_person_entity.down.sql` — `CREATE DATABASE`, `SYSTEM STOP VIEW`, `ALTER TABLE ... MODIFY QUERY` with `/tmp/serving_person_old.sql` (000393's body, character for character), `SYSTEM START VIEW`, then the six `DROP TABLE IF EXISTS` in reverse creation order:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Reverses 000395. The serving view goes back to 000393's render (the old
-- se_company_person / se_company_person_role reads) BEFORE the tables go, so the view never
-- names a table that no longer exists. Rolling back does not restore the old people tables:
-- they are dropped by hand in this same slice and nothing recreates them.

SYSTEM STOP VIEW corpscout.se_companies_serving;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
<<< paste /tmp/serving_person_old.sql here, verbatim, then a semicolon >>>

SYSTEM START VIEW corpscout.se_companies_serving;

DROP TABLE IF EXISTS corpscout.se_company_person_precedence;
DROP TABLE IF EXISTS corpscout.se_company_person_rule;
DROP TABLE IF EXISTS corpscout.se_company_person_history;
DROP TABLE IF EXISTS corpscout.se_company_person_v2;
DROP TABLE IF EXISTS corpscout.se_company_person_normalized;
DROP TABLE IF EXISTS corpscout.se_company_person_suggestion;
```

- [ ] **Step 7: Write `tables.py` and the package init**

`src/dagster_v3/defs/se_company/person/__init__.py`:

```python
"""The SE company person entity on the basic-info shape.

Spec: docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md. Raw suggestions
(what sources deliver) are normalized into a stored layer with its own version, folded per
company into published persons with members, roles and history, and reviewed in the
backoffice. Sweden only: there is no cross-company person identity.
"""
```

`src/dagster_v3/defs/se_company/person/tables.py`:

```python
"""Table names and column tuples of the person entity, pinned against migration 000395.

The main table is se_company_person_v2 for slices 0 to 3; slice 4 renames it to
se_company_person, which is why MAIN_TABLE is the one place the name appears.
"""

DATABASE = "corpscout"
SUGGESTION_TABLE = "se_company_person_suggestion"
NORMALIZED_TABLE = "se_company_person_normalized"
MAIN_TABLE = "se_company_person_v2"
HISTORY_TABLE = "se_company_person_history"
RULE_TABLE = "se_company_person_rule"
PRECEDENCE_TABLE = "se_company_person_precedence"
# Kept from the retired model: the 25-code role catalog the normalizer maps into, seeded for
# Sweden (000290/000294) and Serbia (000319).
ROLE_TYPE_TABLE = "company_person_role_type"

QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"
QUALIFIED_NORMALIZED_TABLE = f"{DATABASE}.{NORMALIZED_TABLE}"
QUALIFIED_MAIN_TABLE = f"{DATABASE}.{MAIN_TABLE}"
QUALIFIED_HISTORY_TABLE = f"{DATABASE}.{HISTORY_TABLE}"
QUALIFIED_RULE_TABLE = f"{DATABASE}.{RULE_TABLE}"
QUALIFIED_PRECEDENCE_TABLE = f"{DATABASE}.{PRECEDENCE_TABLE}"
QUALIFIED_ROLE_TYPE_TABLE = f"{DATABASE}.{ROLE_TYPE_TABLE}"

SOURCES: tuple[str, ...] = ("bolagsverket", "esef", "wikidata", "ratsit", "reviewer", "reviewer_draft")
PARSE_STATUSES: tuple[str, ...] = ("ok", "partial", "no_person")
RULE_KINDS: tuple[str, ...] = ("hide", "merge", "split")
INACTIVE_REASONS: tuple[str, ...] = ("", "hidden", "withdrawn")
CHANGE_KINDS: tuple[str, ...] = ("created", "updated", "hidden", "withdrawn", "reactivated")

SUGGESTION_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "suggested_at", "source_record_id",
    "full_name", "first_name", "last_name", "birth_year", "wikidata_id", "role_original",
    "role_key", "fiscal_year", "role_from", "role_to", "document_ref", "data",
)
NORMALIZED_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "normalized_id", "normalizer_version",
    "parse_status", "parse_notes", "first_tokens", "middle_tokens", "last_tokens",
    "display_first", "display_last", "display_name", "birth_year", "wikidata_id",
    "role_key", "role_code", "role_year", "role_from", "role_to", "data", "normalized_at",
)
MEMBER_COLUMNS: tuple[str, ...] = (
    "member_sources", "member_slots", "member_names", "member_birth_years",
    "member_wikidata_ids", "member_data",
)
ROLE_BLOCK_COLUMNS: tuple[str, ...] = (
    "role_codes", "role_years", "role_sources", "current_roles", "first_year", "last_year",
)
MAIN_COLUMNS: tuple[str, ...] = (
    "company_id", "person_key", "display_name", "first_name", "last_name", "birth_year",
    "wikidata_id", "sources", "slots", "normalized_ids",
    *MEMBER_COLUMNS,
    *ROLE_BLOCK_COLUMNS,
    "text_source", "data", "active", "inactive_reason", "folded_at", "fold_version",
    "source_run_id",
)
HISTORY_COLUMNS: tuple[str, ...] = (*MAIN_COLUMNS, "changed_at", "change_kind", "fold_run_id")
RULE_COLUMNS: tuple[str, ...] = (
    "company_id", "rule_id", "kind", "person_keys", "slots", "active", "note",
    "created_at", "created_by",
)
PRECEDENCE_COLUMNS: tuple[str, ...] = (
    "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
)
```

- [ ] **Step 8: Move the drift pin onto 000395**

Rewrite `tests/test_se_companies_serving_mv.py`. Keep every helper (`_sql_of`, `_sql`, `_statements`, `_body`, `_normalized`, `_executable`, `_modify_query_body`) exactly as it is, delete `_previous_view_body` (000393 has no `CREATE MATERIALIZED VIEW` to read; its body is a `MODIFY QUERY` like this one's), and replace the module docstring, the constants and the five tests with:

```python
"""Migration 000395: the SE person entity is created and the serving view's people flags
move onto it.

`corpscout.se_companies_serving` is the ONE wide per-company row every admin companies list
page reads: the info-list columns, the presence and source flags, the address JSON + primary
geocode summary, and (since 000338) the registered-activity translation, status-reason label
and spine fields absorbed from the retired `se_companies_translated` view.

WHAT 000395 CHANGES (person slice 0). `has_people`, `people_bolagsverket` and `people_esef`
stop reading `corpscout.se_company_person` and `corpscout.se_company_person_role` -- the
2026-08-19 model, dropped by hand in this same slice -- and read the active rows of the new
`corpscout.se_company_person_v2` instead. The definition is otherwise UNCHANGED, so this is
the in-place `ALTER TABLE ... MODIFY QUERY` of 000393, not the staged swap of 000391/000392.
The new table is empty until slice 2's first fold, so all three flags read 0 in between.

The drift pin couples the migration's MODIFY QUERY body to a fresh render of
companies_current.build_se_companies_serving_sql -- editing either half alone turns this red.
"""

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000396_corpscout_se_company_person_entity"
PREVIOUS_MIGRATION = "000393_corpscout_se_company_address_rename"
VIEW = "corpscout.se_companies_serving"
ENTITY = "corpscout.se_company_person_v2"
RETIRED_PERSON_TABLE = "corpscout.se_company_person"
RETIRED_ROLE_TABLE = "corpscout.se_company_person_role"
NEW_TABLES = (
    "corpscout.se_company_person_suggestion",
    "corpscout.se_company_person_normalized",
    "corpscout.se_company_person_v2",
    "corpscout.se_company_person_history",
    "corpscout.se_company_person_rule",
    "corpscout.se_company_person_precedence",
)


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
    assert body.count(f"{ENTITY} FINAL") == 3
    assert "has(sources, 'bolagsverket')" in body and "has(sources, 'esef')" in body
    # Whole-name matching: se_company_person prefixes the entity and both retired tables.
    assert f"{RETIRED_ROLE_TABLE} " not in body and f"{RETIRED_ROLE_TABLE}\n" not in body
    assert f"{RETIRED_PERSON_TABLE} " not in body and f"{RETIRED_PERSON_TABLE}\n" not in body
    assert "SETTINGS join_algorithm = 'grace_hash,hash'" in body
    assert "max_memory_usage = 12884901888" in body


def test_the_up_migration_creates_six_tables_then_stops_repoints_and_starts() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 10
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    for index, table in enumerate(NEW_TABLES, start=1):
        assert _body(statements[index]).startswith(f"CREATE TABLE IF NOT EXISTS {table}\n"), table
    assert _body(statements[7]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[8]).startswith(f"ALTER TABLE {VIEW}\nMODIFY QUERY\n")
    assert _body(statements[9]) == f"SYSTEM START VIEW {VIEW}"
    # No staged swap and no drop: this migration only adds tables and re-points a query.
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    assert "CREATE MATERIALIZED VIEW" not in _sql("up")
    assert "DROP" not in _executable(_sql("up")).upper()


def test_the_down_migration_restores_000393s_render_then_drops_the_six_tables() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 10
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    assert _body(statements[3]) == f"SYSTEM START VIEW {VIEW}"
    # The restored query is 000393's, character for character.
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _modify_query_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )
    # Reverse creation order, so nothing is dropped while something still names it.
    dropped = [_body(statement) for statement in statements[4:]]
    assert dropped == [f"DROP TABLE IF EXISTS {table}" for table in reversed(NEW_TABLES)]


def test_the_up_migration_documents_the_interrupted_repoint_recovery() -> None:
    up = _sql("up")
    assert "SYSTEM START VIEW" in up
    assert "migrate force 395" in up
```

- [ ] **Step 9: Fix the executable serving suite's people stubs**

In `tests/test_se_companies_serving_sql.py`, replace the two stub `CREATE TABLE` lines (409-410) with one, and the two seed lines (423-424) with one:

```python
        # The person entity's main table (migration 000395) -- read FINAL, active rows only.
        # Only the three columns the serving SELECT's IN-subqueries touch.
        "CREATE TABLE corpscout.se_company_person_v2 (company_id String, sources Array(String), active UInt8) ENGINE = MergeTree ORDER BY company_id;",
```

```python
        f"INSERT INTO corpscout.se_company_person_v2 VALUES ('{PRECISE}', ['esef'], 1);",
```

`PRECISE` therefore keeps `has_people = 1`, `people_esef = 1` and `people_bolagsverket = 0`, which is what the file's existing assertions already expect. Check with `rg -n "people_esef|has_people|people_bolagsverket" tests/test_se_companies_serving_sql.py` that no assertion needs a new value; if one does, change the seed's `sources` array, never the assertion.

- [ ] **Step 10: Add the migration to the ledger contract**

In `tests/test_clickhouse_migrations.py`, append to `EXPECTED_MIGRATIONS`, right after `"000394_corpscout_se_company_basic_info_economic_activity",`:

```python
    "000396_corpscout_se_company_person_entity",
```

- [ ] **Step 11: Run the tests**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_person_tables.py tests/test_clickhouse_migrations.py \
         tests/test_se_companies_serving_mv.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
```

Expected: all PASS, `All definitions loaded successfully.` `tests/test_se_companies_serving_sql.py` is an integration file — run it if `clickhouse-local` or docker is available (`-m integration`), otherwise note the skip and let the controller run it.

- [ ] **Step 12: Commit**

```bash
git add corpscout/clickhouse/migrations/000396_corpscout_se_company_person_entity.up.sql \
  corpscout/clickhouse/migrations/000396_corpscout_se_company_person_entity.down.sql \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/__init__.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/tables.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/companies_current.py \
  corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
  corpscout/services/dagster_v3/tests/test_se_companies_serving_mv.py \
  corpscout/services/dagster_v3/tests/test_se_companies_serving_sql.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_tables.py
git commit -m "$(cat <<'EOF'
feat(clickhouse): SE company person entity tables and the serving re-point (000395)

Six tables on the basic-info shape and, in the same file, the 000393
MODIFY-QUERY recipe moving has_people / people_bolagsverket / people_esef onto
se_company_person_v2's active rows. The new table is empty until slice 2's
first fold, so the flags read 0 in between -- deliberate, spec section 8.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 2: The role maps move in, the Swedish person normalizer and its golden corpus

**Files:**
- Create: `src/dagster_v3/defs/se_company/person/roles.py` (the three per-source maps, moved verbatim from `sweden_financial/roles.py`, `esef_filings/roles.py`, `wikidata/roles.py`; those three modules are deleted in Task 4, when their last other importer goes)
- Create: `src/dagster_v3/defs/se_company/person/normalize_se.py`
- Create: `tests/fixtures/se_persons/golden.jsonl`
- Test: `tests/test_se_company_person_normalize_se.py`

**Interfaces:**
- Produces: `roles.SOURCE_ROLE_MAPPINGS`, `roles.SOURCE_ROLELESS_CODES`, `roles.role_code_for(source, *, role_original=None, role_key=None) -> str | None`; `normalize_se.NORMALIZER_VERSION`, `PARSE_STATUSES`, `RawPerson(source, full_name, first_name, last_name, birth_year, wikidata_id, role_original, role_key)` (all optional, `source: str = ""`), `NormalizedPerson(parse_status, parse_notes, first_tokens, middle_tokens, last_tokens, display_first, display_last, display_name, role_code)` (tuples for the four sequence fields), `normalize_se_person(raw: RawPerson) -> NormalizedPerson`. Task 3 uses all of them.
- Consumes: nothing from Task 1 (the normalizer is pure and does not import `tables`).

- [ ] **Step 1: Write the failing test**

`tests/test_se_company_person_normalize_se.py`:

```python
"""The Swedish person normalizer (spec 2026-09-09 section 4): a golden corpus of real and
synthetic register rows, and the role mapping it carries."""

import json
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.person.normalize_se import (
    NORMALIZER_VERSION,
    PARSE_STATUSES,
    NormalizedPerson,
    RawPerson,
    normalize_se_person,
)
from dagster_v3.defs.se_company.person.roles import role_code_for

CORPUS = Path(__file__).resolve().parent / "fixtures" / "se_persons" / "golden.jsonl"

SEQUENCE_FIELDS = ("parse_notes", "first_tokens", "middle_tokens", "last_tokens")


def corpus() -> list[dict]:
    return [json.loads(line) for line in CORPUS.read_text(encoding="utf-8").splitlines() if line.strip()]


CASES = corpus()


@pytest.mark.parametrize(
    "case",
    CASES,
    ids=[f"{c['source']}:{json.dumps(c['raw'], ensure_ascii=False)[:60]}" for c in CASES],
)
def test_golden_corpus(case: dict) -> None:
    result = normalize_se_person(RawPerson(source=case["source"], **case["raw"]))
    for field_name, value in case["expected"].items():
        actual = getattr(result, field_name)
        expected = tuple(value) if field_name in SEQUENCE_FIELDS else value
        assert actual == expected, field_name


def test_the_corpus_covers_every_status_and_every_source() -> None:
    statuses = {c["expected"]["parse_status"] for c in CASES}
    assert statuses == set(PARSE_STATUSES)
    assert {c["source"] for c in CASES} >= {"bolagsverket", "esef", "wikidata", "reviewer"}
    assert len(CASES) >= 40


def test_identity_folds_diacritics_hyphens_and_initial_periods() -> None:
    """Håkan and Hakan meet, Ö and O meet, Sven-Erik is two tokens and S.E. is two tokens."""
    accented = normalize_se_person(RawPerson(source="bolagsverket", first_name="Håkan", last_name="Öberg"))
    plain = normalize_se_person(RawPerson(source="bolagsverket", first_name="Hakan", last_name="Oberg"))
    assert accented.first_tokens == plain.first_tokens == ("hakan",)
    assert accented.last_tokens == plain.last_tokens == ("oberg",)
    # The display spelling keeps what the source delivered; only the tokens fold.
    assert accented.display_name == "Håkan Öberg" and plain.display_name == "Hakan Oberg"

    hyphen = normalize_se_person(RawPerson(source="esef", full_name="Sven-Erik Andersson"))
    spaced = normalize_se_person(RawPerson(source="esef", full_name="Sven Erik Andersson"))
    assert hyphen.first_tokens == spaced.first_tokens == ("sven",)
    assert hyphen.middle_tokens == spaced.middle_tokens == ("erik",)
    assert hyphen.display_name == "Sven-Erik Andersson"


def test_particles_glue_to_the_last_name() -> None:
    result = normalize_se_person(RawPerson(source="esef", full_name="Carl von Essen"))
    assert result.display_first == "Carl" and result.display_last == "von Essen"
    assert result.last_tokens == ("von", "essen")
    assert result.parse_status == "ok"


def test_role_code_prefers_the_key_then_the_label_then_the_label_itself() -> None:
    # The map is keyed on the source's own code, which the extractor puts in role_key.
    assert role_code_for("bolagsverket", role_original="Ordförande", role_key="chairman") == "board_chair"
    # Bolagsverket's ORIGINAL_ROLE map is keyed on the Swedish label instead.
    assert (
        role_code_for("bolagsverket", role_original="Arbetstagarrepresentant", role_key="other")
        == "employee_board_representative"
    )
    # Nothing maps: the delivered label publishes as itself, lowercased and trimmed.
    assert role_code_for("bolagsverket", role_original="  Firmatecknare ", role_key="other") == "firmatecknare"
    assert role_code_for("reviewer", role_original="Ordförande") == "ordförande"
    assert role_code_for("wikidata", role_original="board member", role_key="P3320") == "board_member"
    assert role_code_for("esef", role_original="VD", role_key="chief_executive") == "chief_executive_officer"
    assert role_code_for("bolagsverket") is None


def test_roleless_source_codes_publish_no_role() -> None:
    """A Bolagsverket signatory with an unknown role kind is person evidence, not a role
    observation -- the one roleless entry the three moved maps carry."""
    assert role_code_for("bolagsverket", role_original="Styrelseledamot", role_key="unknown") is None


def test_version_constant_and_dataclass_shape() -> None:
    assert NORMALIZER_VERSION == "se-person-normalizer-v1"
    assert PARSE_STATUSES == ("ok", "partial", "no_person")
    assert NormalizedPerson.__slots__  # frozen dataclass with slots: hashable fold inputs
    assert RawPerson().source == ""
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_normalize_se.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.person.normalize_se'`.

- [ ] **Step 3: Write `roles.py`**

`src/dagster_v3/defs/se_company/person/roles.py`, exactly:

```python
"""Per-source role mappings for the SE person entity (spec 2026-09-09 section 4.3).

Moved here verbatim from sweden_financial/roles.py, esef_filings/roles.py and
wikidata/roles.py -- the three modules the retired company_people package spliced together
and whose only other importer went with it. A label neither the source's map nor its
roleless set knows is published as itself, lowercased and trimmed (owner ruling 2026-08-28:
never dropped, never bucketed). Every mapped value is a role_code in
corpscout.company_person_role_type, the 25-code catalog that outlived the old model.
"""

from collections.abc import Mapping

# Bolagsverket annual-report signatories. The kind map is keyed on the source parser's
# role_kind; the original-role map refines values the parser groups under `other`, and is
# curated separately so one recognized value never accepts every `other` observation.
BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "auditor": "auditor",
    "board_member": "board_member",
    "ceo": "chief_executive_officer",
    "chairman": "board_chair",
    "deputy_board_member": "deputy_board_member",
    "liquidator": "liquidator",
}
BOLAGSVERKET_ORIGINAL_ROLE_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "Arbetstagarrepresentant": "employee_board_representative",
    "Vice VD": "deputy_chief_executive_officer",
}
# A signatory with an unknown role is still person evidence, but it is not a role
# observation. `other` is deliberately absent: it is a native role to classify, not a hole.
BOLAGSVERKET_ROLELESS_ROLE_KINDS: frozenset[str] = frozenset({"unknown"})

# ESEF LLM extraction, keyed on the extraction's role_category.
ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "audit_partner": "audit_partner",
    "auditor": "auditor",
    "board_chair": "board_chair",
    "board_member": "board_member",
    "chief_executive": "chief_executive_officer",
    "chief_financial_officer": "chief_financial_officer",
    "executive": "executive",
}
ESEF_ROLELESS_ROLE_CATEGORIES: frozenset[str] = frozenset()

# Wikidata, keyed on the property id that linked the person to the company.
WIKIDATA_ROLE_PROPERTY_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "P112": "founder",
    "P127": "owner",
    "P169": "chief_executive_officer",
    "P3320": "board_member",
    "P488": "board_chair",
}
WIKIDATA_ROLELESS_PROPERTIES: frozenset[str] = frozenset()

SOURCE_ROLE_MAPPINGS: Mapping[str, Mapping[str, str]] = {
    "bolagsverket": {
        **BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE,
        **BOLAGSVERKET_ORIGINAL_ROLE_TO_CANONICAL_ROLE,
    },
    "esef": ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE,
    "wikidata": WIKIDATA_ROLE_PROPERTY_TO_CANONICAL_ROLE,
}
SOURCE_ROLELESS_CODES: Mapping[str, frozenset[str]] = {
    "bolagsverket": BOLAGSVERKET_ROLELESS_ROLE_KINDS,
    "esef": ESEF_ROLELESS_ROLE_CATEGORIES,
    "wikidata": WIKIDATA_ROLELESS_PROPERTIES,
}

_LOOKUP: Mapping[str, Mapping[str, str]] = {
    source: {key.strip().lower(): value for key, value in mapping.items()}
    for source, mapping in SOURCE_ROLE_MAPPINGS.items()
}
_ROLELESS: Mapping[str, frozenset[str]] = {
    source: frozenset(code.strip().lower() for code in codes)
    for source, codes in SOURCE_ROLELESS_CODES.items()
}


def _clean_label(label: str | None) -> str:
    return " ".join(label.split()) if label else ""


def role_code_for(
    source: str, *, role_original: str | None = None, role_key: str | None = None
) -> str | None:
    """The catalog code for one delivered role, or the delivered label itself.

    `role_key` is the source's own mapping key when it delivers one beside the human label
    -- Bolagsverket's role_kind, Wikidata's property id, ESEF's role category -- which the
    extractor lifts out of the suggestion row's `data`. The key is tried first because the
    maps are keyed on it, the label second (Bolagsverket's original-role map is keyed on the
    Swedish label), and what comes back when neither maps is the delivered label, lowercased
    and trimmed. A key in the source's roleless set means "person evidence, no role" and
    returns None. Sources with no map at all -- reviewer, reviewer_draft, ratsit -- always
    take the passthrough.
    """
    lookup = _LOOKUP.get(source, {})
    roleless = _ROLELESS.get(source, frozenset())
    for candidate in (role_key, role_original):
        cleaned = _clean_label(candidate)
        if not cleaned:
            continue
        if cleaned.lower() in roleless:
            return None
        mapped = lookup.get(cleaned.lower())
        if mapped is not None:
            return mapped
    fallback = _clean_label(role_original) or _clean_label(role_key)
    return fallback.lower() if fallback else None
```

- [ ] **Step 4: Write the normalizer**

`src/dagster_v3/defs/se_company/person/normalize_se.py`, exactly:

```python
"""The Swedish person normalizer (spec 2026-09-09 section 4).

Pure: one source's delivered name and role fields in, a display spelling, identity tokens,
a role code and a parse status out. It splits, folds and classifies; it never invents a
name, never guesses a missing half and never drops a role it cannot map. Every behaviour
change bumps NORMALIZER_VERSION, which is what re-normalizes stored rows.

WHAT THE STATUSES MEAN (spec 4.4). `ok`: a first and a last token exist -- foldable into a
person. `partial`: one name word, or nothing but initials -- stored with its notes, never
folded into a person. `no_person`: the name field holds a role word, a number or a date, a
company suffix, or nothing at all. The August 2026 audit found roles in the name field and
dates in the role field, which is why the no_person rules read the NAME field and the role
mapping never touches it.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from dagster_v3.defs.se_company.person.roles import role_code_for

NORMALIZER_VERSION = "se-person-normalizer-v1"
PARSE_STATUSES: tuple[str, ...] = ("ok", "partial", "no_person")

_WHITESPACE = re.compile(r"\s+")
_DIGIT = re.compile(r"\d")
_SUBTOKEN_SPLIT = re.compile(r"[.\-]+")
_NON_TOKEN = re.compile(r"[^a-z0-9]+")

# Particles glue to the last name, in the display spelling and in the tokens (spec 4.1, 4.2).
_PARTICLES = frozenset({"von", "af", "de", "van", "der", "la", "le"})
# Honorifics dropped from the display spelling. Matched on the WHOLE folded word before any
# hyphen split, so the Swedish given name Ing-Marie is never mistaken for a title.
_TITLE_WORDS = frozenset({"dr", "prof", "professor", "doktor", "herr", "fru", "froken", "mr", "mrs", "ms"})
_TITLE_PHRASES = (
    ("jur", "kand"), ("civ", "ing"), ("civ", "ekon"),
    ("ekon", "dr"), ("fil", "dr"), ("med", "dr"), ("jur", "dr"),
)
# A role word in the NAME field means the row is not a person. Whole tokens, in order.
_ROLE_PHRASES = (
    ("styrelseledamot",), ("styrelseordforande",), ("styrelsesuppleant",), ("ordforande",),
    ("suppleant",), ("ledamot",), ("revisor",), ("likvidator",), ("firmatecknare",),
    ("arbetstagarrepresentant",), ("vd",), ("verkstallande", "direktor"), ("vice", "vd"),
    ("huvudansvarig", "revisor"), ("auktoriserad", "revisor"),
    ("board", "member"), ("board", "chair"), ("chairman",), ("auditor",), ("liquidator",),
    ("chief", "executive", "officer"), ("director",), ("founder",), ("owner",),
)
# Whole tokens only. `ek` is deliberately absent: Ek is a common Swedish surname.
_COMPANY_TOKENS = frozenset({"ab", "hb", "kb", "aktiebolag", "handelsbolag", "kommanditbolag"})


@dataclass(frozen=True, slots=True)
class RawPerson:
    """One suggestion row's person and role fields, as the source delivered them."""

    source: str = ""
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    birth_year: int | None = None
    wikidata_id: str | None = None
    role_original: str | None = None
    # The source's own mapping key beside the label, lifted out of the row's `data` by the
    # extractor: Bolagsverket's role_kind, Wikidata's property id, ESEF's role category.
    role_key: str | None = None


@dataclass(frozen=True, slots=True)
class NormalizedPerson:
    parse_status: str
    parse_notes: tuple[str, ...]
    first_tokens: tuple[str, ...]
    middle_tokens: tuple[str, ...]
    last_tokens: tuple[str, ...]
    display_first: str
    display_last: str
    display_name: str
    role_code: str | None


def _clean(text: str | None) -> str:
    return _WHITESPACE.sub(" ", text.strip()) if text else ""


def _fold(text: str) -> str:
    """Case-folded and diacritic-free: Håkan and Hakan meet, Ö and O meet (spec 4.2)."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(char for char in decomposed if not unicodedata.combining(char)).casefold()


def _fold_word(word: str) -> str:
    """One whole word folded, periods removed, hyphens KEPT (title and particle matching)."""
    return _fold(word).replace(".", "")


def _subtokens(word: str) -> list[str]:
    """The identity tokens of one word: folded, split on hyphens and periods, letters and
    digits only. Sven-Erik gives sven, erik; S.E. gives s, e."""
    pieces = _SUBTOKEN_SPLIT.split(_fold(word))
    return [token for token in (_NON_TOKEN.sub("", piece) for piece in pieces) if token]


def _drop_titles(words: list[str]) -> tuple[list[str], list[str]]:
    """(kept words, dropped titles) -- honorific phrases first, then single honorifics."""
    folded = [_fold_word(word) for word in words]
    kept: list[str] = []
    dropped: list[str] = []
    index = 0
    while index < len(words):
        phrase = next(
            (p for p in _TITLE_PHRASES if tuple(folded[index : index + len(p)]) == p), None
        )
        if phrase is not None:
            dropped.append(" ".join(phrase))
            index += len(phrase)
            continue
        if folded[index] in _TITLE_WORDS:
            dropped.append(folded[index])
            index += 1
            continue
        kept.append(words[index])
        index += 1
    return kept, dropped


def _has_role_phrase(tokens: list[str]) -> bool:
    return any(
        tuple(tokens[start : start + len(phrase)]) == phrase
        for phrase in _ROLE_PHRASES
        for start in range(len(tokens) - len(phrase) + 1)
    )


def _split_full_name(full: str) -> tuple[list[str], list[str], list[str], bool]:
    """(first words, last words, dropped titles, comma form) for a one-string name.

    A comma form is "Last, First". Otherwise the last word is the last name, preceded by any
    run of particles: "Carl von Essen" is Carl / von Essen, "von Essen" is / von Essen.
    """
    if "," in full:
        last_part, _, first_part = full.partition(",")
        last_words, dropped_last = _drop_titles(_clean(last_part).split(" ")) if _clean(last_part) else ([], [])
        first_words, dropped_first = _drop_titles(_clean(first_part).split(" ")) if _clean(first_part) else ([], [])
        return first_words, last_words, dropped_first + dropped_last, True
    words, dropped = _drop_titles(full.split(" "))
    index = max(len(words) - 1, 0)
    while index > 0 and _fold_word(words[index - 1]) in _PARTICLES:
        index -= 1
    return words[:index], words[index:], dropped, False


def normalize_se_person(raw: RawPerson) -> NormalizedPerson:
    """The whole of spec section 4 for one delivered row."""
    role_code = role_code_for(raw.source, role_original=raw.role_original, role_key=raw.role_key)
    first_in, last_in, full = _clean(raw.first_name), _clean(raw.last_name), _clean(raw.full_name)
    split_delivered = bool(first_in or last_in)
    source_text = " ".join(part for part in (first_in, last_in) if part) if split_delivered else full

    def _no_person(note: str) -> NormalizedPerson:
        # The delivered text stays in display_name so the reviewer can see what was rejected.
        return NormalizedPerson(
            parse_status="no_person", parse_notes=(note,),
            first_tokens=(), middle_tokens=(), last_tokens=(),
            display_first="", display_last="", display_name=source_text, role_code=role_code,
        )

    if not source_text:
        return _no_person("empty name")
    tokens = [token for word in source_text.split(" ") for token in _subtokens(word)]
    if _DIGIT.search(source_text):
        return _no_person("digits in the name field")
    if any(token in _COMPANY_TOKENS for token in tokens):
        return _no_person("company suffix in the name field")
    if _has_role_phrase(tokens):
        return _no_person("role word in the name field")

    if split_delivered:
        first_words, dropped_first = _drop_titles(first_in.split(" ")) if first_in else ([], [])
        last_words, dropped_last = _drop_titles(last_in.split(" ")) if last_in else ([], [])
        dropped, comma_form = dropped_first + dropped_last, False
    else:
        first_words, last_words, dropped, comma_form = _split_full_name(full)

    notes: list[str] = []
    if dropped:
        notes.append("removed title " + ", ".join(dropped))
    if comma_form:
        notes.append("comma form")

    given_tokens = [token for word in first_words for token in _subtokens(word)]
    last_tokens = tuple(token for word in last_words for token in _subtokens(word))
    first_tokens = tuple(given_tokens[:1])
    middle_tokens = tuple(given_tokens[1:])
    display_first = " ".join(first_words)
    display_last = " ".join(last_words)

    if not first_tokens or not last_tokens:
        status = "partial"
        notes.append("only one name word")
    elif all(len(token) == 1 for token in (*first_tokens, *middle_tokens)):
        status = "partial"
        notes.append("initials only")
    else:
        status = "ok"

    return NormalizedPerson(
        parse_status=status,
        parse_notes=tuple(notes),
        first_tokens=first_tokens,
        middle_tokens=middle_tokens,
        last_tokens=last_tokens,
        display_first=display_first,
        display_last=display_last,
        display_name=" ".join(part for part in (display_first, display_last) if part),
        role_code=role_code,
    )
```

- [ ] **Step 5: Write the golden corpus**

Create `tests/fixtures/se_persons/golden.jsonl` with exactly these fifty lines. Each line is
`{"source": ..., "raw": {<the RawPerson fields the source delivers, minus source>}, "expected": {<every NormalizedPerson field>}}`.
Every expectation below was computed by hand against the implementation in Step 4; if one
fails, the implementation drifted from this plan — fix the implementation, never the
expectation, and report the diff.

```jsonl
{"source": "bolagsverket", "raw": {"first_name": "Anna", "last_name": "Svensson", "role_original": "Styrelseledamot", "role_key": "board_member"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["anna"], "middle_tokens": [], "last_tokens": ["svensson"], "display_first": "Anna", "display_last": "Svensson", "display_name": "Anna Svensson", "role_code": "board_member"}}
{"source": "bolagsverket", "raw": {"first_name": "Sven-Erik", "last_name": "Andersson", "role_original": "Ordförande", "role_key": "chairman"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["sven"], "middle_tokens": ["erik"], "last_tokens": ["andersson"], "display_first": "Sven-Erik", "display_last": "Andersson", "display_name": "Sven-Erik Andersson", "role_code": "board_chair"}}
{"source": "bolagsverket", "raw": {"first_name": "Håkan", "last_name": "Öberg", "role_original": "Verkställande direktör", "role_key": "ceo"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["hakan"], "middle_tokens": [], "last_tokens": ["oberg"], "display_first": "Håkan", "display_last": "Öberg", "display_name": "Håkan Öberg", "role_code": "chief_executive_officer"}}
{"source": "bolagsverket", "raw": {"first_name": "Karl Gustav", "last_name": "Bengtsson", "role_original": "Styrelsesuppleant", "role_key": "deputy_board_member"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["karl"], "middle_tokens": ["gustav"], "last_tokens": ["bengtsson"], "display_first": "Karl Gustav", "display_last": "Bengtsson", "display_name": "Karl Gustav Bengtsson", "role_code": "deputy_board_member"}}
{"source": "bolagsverket", "raw": {"first_name": "Carl", "last_name": "von Essen", "role_original": "Styrelseledamot", "role_key": "board_member"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["carl"], "middle_tokens": [], "last_tokens": ["von", "essen"], "display_first": "Carl", "display_last": "von Essen", "display_name": "Carl von Essen", "role_code": "board_member"}}
{"source": "bolagsverket", "raw": {"first_name": "S.E.", "last_name": "Lind", "role_original": "Revisor", "role_key": "auditor"}, "expected": {"parse_status": "partial", "parse_notes": ["initials only"], "first_tokens": ["s"], "middle_tokens": ["e"], "last_tokens": ["lind"], "display_first": "S.E.", "display_last": "Lind", "display_name": "S.E. Lind", "role_code": "auditor"}}
{"source": "bolagsverket", "raw": {"first_name": "Per", "last_name": "", "role_original": "Styrelseledamot", "role_key": "board_member"}, "expected": {"parse_status": "partial", "parse_notes": ["only one name word"], "first_tokens": ["per"], "middle_tokens": [], "last_tokens": [], "display_first": "Per", "display_last": "", "display_name": "Per", "role_code": "board_member"}}
{"source": "bolagsverket", "raw": {"first_name": "", "last_name": "Nilsson", "role_original": "Likvidator", "role_key": "liquidator"}, "expected": {"parse_status": "partial", "parse_notes": ["only one name word"], "first_tokens": [], "middle_tokens": [], "last_tokens": ["nilsson"], "display_first": "", "display_last": "Nilsson", "display_name": "Nilsson", "role_code": "liquidator"}}
{"source": "bolagsverket", "raw": {"first_name": "Styrelseledamot", "last_name": "", "role_original": "Styrelseledamot", "role_key": "unknown"}, "expected": {"parse_status": "no_person", "parse_notes": ["role word in the name field"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "Styrelseledamot", "role_code": null}}
{"source": "bolagsverket", "raw": {"first_name": "Anna", "last_name": "Svensson 2", "role_original": "Styrelseledamot", "role_key": "board_member"}, "expected": {"parse_status": "no_person", "parse_notes": ["digits in the name field"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "Anna Svensson 2", "role_code": "board_member"}}
{"source": "bolagsverket", "raw": {"first_name": "Ekonomibolaget", "last_name": "AB", "role_original": "Styrelseledamot", "role_key": "board_member"}, "expected": {"parse_status": "no_person", "parse_notes": ["company suffix in the name field"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "Ekonomibolaget AB", "role_code": "board_member"}}
{"source": "bolagsverket", "raw": {"first_name": "Ing-Marie", "last_name": "Ek", "role_original": "Styrelseledamot", "role_key": "board_member"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["ing"], "middle_tokens": ["marie"], "last_tokens": ["ek"], "display_first": "Ing-Marie", "display_last": "Ek", "display_name": "Ing-Marie Ek", "role_code": "board_member"}}
{"source": "bolagsverket", "raw": {"first_name": "Anne-Christine", "last_name": "Malmberg Ekbom", "role_original": "Styrelseledamot", "role_key": "board_member"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["anne"], "middle_tokens": ["christine"], "last_tokens": ["malmberg", "ekbom"], "display_first": "Anne-Christine", "display_last": "Malmberg Ekbom", "display_name": "Anne-Christine Malmberg Ekbom", "role_code": "board_member"}}
{"source": "bolagsverket", "raw": {"first_name": "", "last_name": "", "role_original": "Styrelseledamot", "role_key": "board_member"}, "expected": {"parse_status": "no_person", "parse_notes": ["empty name"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "", "role_code": "board_member"}}
{"source": "bolagsverket", "raw": {"first_name": "Åsa", "last_name": "Ödman", "role_original": "Huvudansvarig revisor", "role_key": "auditor"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["asa"], "middle_tokens": [], "last_tokens": ["odman"], "display_first": "Åsa", "display_last": "Ödman", "display_name": "Åsa Ödman", "role_code": "auditor"}}
{"source": "bolagsverket", "raw": {"first_name": "Karl", "last_name": "Bengtsson", "role_original": "Arbetstagarrepresentant", "role_key": "other"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["karl"], "middle_tokens": [], "last_tokens": ["bengtsson"], "display_first": "Karl", "display_last": "Bengtsson", "display_name": "Karl Bengtsson", "role_code": "employee_board_representative"}}
{"source": "bolagsverket", "raw": {"first_name": "Karin", "last_name": "Holm", "role_original": "Firmatecknare", "role_key": "other"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["karin"], "middle_tokens": [], "last_tokens": ["holm"], "display_first": "Karin", "display_last": "Holm", "display_name": "Karin Holm", "role_code": "firmatecknare"}}
{"source": "bolagsverket", "raw": {"first_name": "Vice", "last_name": "VD", "role_original": "Vice VD", "role_key": "other"}, "expected": {"parse_status": "no_person", "parse_notes": ["role word in the name field"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "Vice VD", "role_code": "deputy_chief_executive_officer"}}
{"source": "bolagsverket", "raw": {"first_name": "Dr Anna", "last_name": "Lindqvist", "role_original": "Styrelseledamot", "role_key": "board_member"}, "expected": {"parse_status": "ok", "parse_notes": ["removed title dr"], "first_tokens": ["anna"], "middle_tokens": [], "last_tokens": ["lindqvist"], "display_first": "Anna", "display_last": "Lindqvist", "display_name": "Anna Lindqvist", "role_code": "board_member"}}
{"source": "esef", "raw": {"full_name": "Andersson, Sven-Erik", "role_original": "Styrelsens ordförande", "role_key": "board_chair"}, "expected": {"parse_status": "ok", "parse_notes": ["comma form"], "first_tokens": ["sven"], "middle_tokens": ["erik"], "last_tokens": ["andersson"], "display_first": "Sven-Erik", "display_last": "Andersson", "display_name": "Sven-Erik Andersson", "role_code": "board_chair"}}
{"source": "esef", "raw": {"full_name": "Anna Lindqvist", "role_original": "VD", "role_key": "chief_executive"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["anna"], "middle_tokens": [], "last_tokens": ["lindqvist"], "display_first": "Anna", "display_last": "Lindqvist", "display_name": "Anna Lindqvist", "role_code": "chief_executive_officer"}}
{"source": "esef", "raw": {"full_name": "Dr. Anna Lindqvist", "role_original": "Ledamot", "role_key": "board_member"}, "expected": {"parse_status": "ok", "parse_notes": ["removed title dr"], "first_tokens": ["anna"], "middle_tokens": [], "last_tokens": ["lindqvist"], "display_first": "Anna", "display_last": "Lindqvist", "display_name": "Anna Lindqvist", "role_code": "board_member"}}
{"source": "esef", "raw": {"full_name": "Jur kand Karin Holm", "role_original": "Revisor", "role_key": "auditor"}, "expected": {"parse_status": "ok", "parse_notes": ["removed title jur kand"], "first_tokens": ["karin"], "middle_tokens": [], "last_tokens": ["holm"], "display_first": "Karin", "display_last": "Holm", "display_name": "Karin Holm", "role_code": "auditor"}}
{"source": "esef", "raw": {"full_name": "Carl von Essen", "role_original": "Ledamot", "role_key": "board_member"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["carl"], "middle_tokens": [], "last_tokens": ["von", "essen"], "display_first": "Carl", "display_last": "von Essen", "display_name": "Carl von Essen", "role_code": "board_member"}}
{"source": "esef", "raw": {"full_name": "Anna Maria Svensson", "role_original": "Huvudansvarig revisor", "role_key": "audit_partner"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["anna"], "middle_tokens": ["maria"], "last_tokens": ["svensson"], "display_first": "Anna Maria", "display_last": "Svensson", "display_name": "Anna Maria Svensson", "role_code": "audit_partner"}}
{"source": "esef", "raw": {"full_name": "Öberg, Håkan", "role_original": "Ekonomichef", "role_key": "chief_financial_officer"}, "expected": {"parse_status": "ok", "parse_notes": ["comma form"], "first_tokens": ["hakan"], "middle_tokens": [], "last_tokens": ["oberg"], "display_first": "Håkan", "display_last": "Öberg", "display_name": "Håkan Öberg", "role_code": "chief_financial_officer"}}
{"source": "esef", "raw": {"full_name": "Styrelseordförande", "role_original": "Styrelseordförande", "role_key": "board_chair"}, "expected": {"parse_status": "no_person", "parse_notes": ["role word in the name field"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "Styrelseordförande", "role_code": "board_chair"}}
{"source": "esef", "raw": {"full_name": "2021-05-14", "role_original": "Ledamot", "role_key": "board_member"}, "expected": {"parse_status": "no_person", "parse_notes": ["digits in the name field"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "2021-05-14", "role_code": "board_member"}}
{"source": "esef", "raw": {"full_name": "Nordea Bank AB", "role_original": "Revisor", "role_key": "auditor"}, "expected": {"parse_status": "no_person", "parse_notes": ["company suffix in the name field"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "Nordea Bank AB", "role_code": "auditor"}}
{"source": "esef", "raw": {"full_name": "", "role_original": "Revisor", "role_key": "auditor"}, "expected": {"parse_status": "no_person", "parse_notes": ["empty name"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "", "role_code": "auditor"}}
{"source": "esef", "raw": {"full_name": "Lind", "role_original": "Ledamot", "role_key": "executive"}, "expected": {"parse_status": "partial", "parse_notes": ["only one name word"], "first_tokens": [], "middle_tokens": [], "last_tokens": ["lind"], "display_first": "", "display_last": "Lind", "display_name": "Lind", "role_code": "executive"}}
{"source": "esef", "raw": {"full_name": "S. E. Lind", "role_original": "Revisor", "role_key": "auditor"}, "expected": {"parse_status": "partial", "parse_notes": ["initials only"], "first_tokens": ["s"], "middle_tokens": ["e"], "last_tokens": ["lind"], "display_first": "S. E.", "display_last": "Lind", "display_name": "S. E. Lind", "role_code": "auditor"}}
{"source": "esef", "raw": {"full_name": "van der Berg, Johan", "role_original": "Ledamot", "role_key": "executive"}, "expected": {"parse_status": "ok", "parse_notes": ["comma form"], "first_tokens": ["johan"], "middle_tokens": [], "last_tokens": ["van", "der", "berg"], "display_first": "Johan", "display_last": "van der Berg", "display_name": "Johan van der Berg", "role_code": "executive"}}
{"source": "esef", "raw": {"full_name": "Prof Åsa Ödman", "role_original": "Ledamot", "role_key": "board_member"}, "expected": {"parse_status": "ok", "parse_notes": ["removed title prof"], "first_tokens": ["asa"], "middle_tokens": [], "last_tokens": ["odman"], "display_first": "Åsa", "display_last": "Ödman", "display_name": "Åsa Ödman", "role_code": "board_member"}}
{"source": "esef", "raw": {"full_name": "  Anna   Svensson  ", "role_original": "Ledamot", "role_key": "board_member"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["anna"], "middle_tokens": [], "last_tokens": ["svensson"], "display_first": "Anna", "display_last": "Svensson", "display_name": "Anna Svensson", "role_code": "board_member"}}
{"source": "esef", "raw": {"full_name": "Anna Svensson", "role_original": "Hållbarhetschef", "role_key": "other"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["anna"], "middle_tokens": [], "last_tokens": ["svensson"], "display_first": "Anna", "display_last": "Svensson", "display_name": "Anna Svensson", "role_code": "hållbarhetschef"}}
{"source": "wikidata", "raw": {"full_name": "Anna Svensson", "wikidata_id": "Q123", "birth_year": 1975, "role_original": "board member", "role_key": "P3320"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["anna"], "middle_tokens": [], "last_tokens": ["svensson"], "display_first": "Anna", "display_last": "Svensson", "display_name": "Anna Svensson", "role_code": "board_member"}}
{"source": "wikidata", "raw": {"full_name": "Ingvar Kamprad", "wikidata_id": "Q57187", "role_original": "founder", "role_key": "P112"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["ingvar"], "middle_tokens": [], "last_tokens": ["kamprad"], "display_first": "Ingvar", "display_last": "Kamprad", "display_name": "Ingvar Kamprad", "role_code": "founder"}}
{"source": "wikidata", "raw": {"full_name": "Marcus Wallenberg", "wikidata_id": "Q296", "role_original": "chairperson", "role_key": "P488"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["marcus"], "middle_tokens": [], "last_tokens": ["wallenberg"], "display_first": "Marcus", "display_last": "Wallenberg", "display_name": "Marcus Wallenberg", "role_code": "board_chair"}}
{"source": "wikidata", "raw": {"full_name": "Håkan Öberg", "wikidata_id": "Q9", "role_original": "owner of", "role_key": "P127"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["hakan"], "middle_tokens": [], "last_tokens": ["oberg"], "display_first": "Håkan", "display_last": "Öberg", "display_name": "Håkan Öberg", "role_code": "owner"}}
{"source": "wikidata", "raw": {"full_name": "Per-Olof Söderberg", "wikidata_id": "Q77", "role_original": "board member", "role_key": "P3320"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["per"], "middle_tokens": ["olof"], "last_tokens": ["soderberg"], "display_first": "Per-Olof", "display_last": "Söderberg", "display_name": "Per-Olof Söderberg", "role_code": "board_member"}}
{"source": "wikidata", "raw": {"full_name": "Jan Stenbeck", "wikidata_id": "Q55", "role_original": "chief executive", "role_key": "P999"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["jan"], "middle_tokens": [], "last_tokens": ["stenbeck"], "display_first": "Jan", "display_last": "Stenbeck", "display_name": "Jan Stenbeck", "role_code": "chief executive"}}
{"source": "wikidata", "raw": {"full_name": "Wallenberg", "wikidata_id": "Q296", "role_original": "chairperson", "role_key": "P488"}, "expected": {"parse_status": "partial", "parse_notes": ["only one name word"], "first_tokens": [], "middle_tokens": [], "last_tokens": ["wallenberg"], "display_first": "", "display_last": "Wallenberg", "display_name": "Wallenberg", "role_code": "board_chair"}}
{"source": "wikidata", "raw": {"full_name": "AB Volvo", "wikidata_id": "Q215293", "role_original": "owner of", "role_key": "P127"}, "expected": {"parse_status": "no_person", "parse_notes": ["company suffix in the name field"], "first_tokens": [], "middle_tokens": [], "last_tokens": [], "display_first": "", "display_last": "", "display_name": "AB Volvo", "role_code": "owner"}}
{"source": "wikidata", "raw": {"full_name": "Carl von Essen", "wikidata_id": "Q800", "birth_year": 1968, "role_original": "board member", "role_key": "P3320"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["carl"], "middle_tokens": [], "last_tokens": ["von", "essen"], "display_first": "Carl", "display_last": "von Essen", "display_name": "Carl von Essen", "role_code": "board_member"}}
{"source": "wikidata", "raw": {"full_name": "von Essen", "wikidata_id": "Q801", "role_original": "board member", "role_key": "P3320"}, "expected": {"parse_status": "partial", "parse_notes": ["only one name word"], "first_tokens": [], "middle_tokens": [], "last_tokens": ["von", "essen"], "display_first": "", "display_last": "von Essen", "display_name": "von Essen", "role_code": "board_member"}}
{"source": "reviewer", "raw": {"first_name": "Anna", "last_name": "Svensson", "role_original": "Styrelseledamot"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["anna"], "middle_tokens": [], "last_tokens": ["svensson"], "display_first": "Anna", "display_last": "Svensson", "display_name": "Anna Svensson", "role_code": "styrelseledamot"}}
{"source": "reviewer", "raw": {"full_name": "Anna-Karin de la Motte", "role_original": "Ordförande"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["anna"], "middle_tokens": ["karin"], "last_tokens": ["de", "la", "motte"], "display_first": "Anna-Karin", "display_last": "de la Motte", "display_name": "Anna-Karin de la Motte", "role_code": "ordförande"}}
{"source": "reviewer", "raw": {"full_name": "Karin Holm", "role_original": "Auktoriserad revisor"}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["karin"], "middle_tokens": [], "last_tokens": ["holm"], "display_first": "Karin", "display_last": "Holm", "display_name": "Karin Holm", "role_code": "auktoriserad revisor"}}
{"source": "reviewer_draft", "raw": {"full_name": "Karin Holm", "role_original": ""}, "expected": {"parse_status": "ok", "parse_notes": [], "first_tokens": ["karin"], "middle_tokens": [], "last_tokens": ["holm"], "display_first": "Karin", "display_last": "Holm", "display_name": "Karin Holm", "role_code": null}}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_normalize_se.py -q`
Expected: 56 passed (50 corpus cases + 6 unit tests).

- [ ] **Step 7: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/roles.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/normalize_se.py \
  corpscout/services/dagster_v3/tests/fixtures/se_persons/golden.jsonl \
  corpscout/services/dagster_v3/tests/test_se_company_person_normalize_se.py
git commit -m "$(cat <<'EOF'
feat(dagster): Swedish person normalizer with a golden corpus

The three per-source role maps move into the person package (their old modules
go with the company_people package). Display spelling, identity tokens with
diacritic and hyphen folding, particles, the comma form, titles, role mapping
with passthrough, and the ok/partial/no_person rules, pinned by fifty cases.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 3: The normalize asset, its SQL and the clickhouse-local proof

**Files:**
- Create: `src/dagster_v3/defs/se_company/person/normalize.py`, `src/dagster_v3/defs/se_company/person/assets.py`
- Create: `src/dagster_v3/defs/se_company/person/docs/person-design.md`
- Test: `tests/test_se_company_person_normalize.py`, `tests/test_se_company_person_normalize_clickhouse_local.py`

**Interfaces:**
- Consumes: Task 1's `tables`; Task 2's `RawPerson`, `NormalizedPerson`, `normalize_se_person`, `NORMALIZER_VERSION`; `dagster_v3.defs.se_company.basic_info.extract.scope_pages` and `SCAN_QUERY_SETTINGS`; `dagster_v3.defs.se_company.common.normalized_se_company_ids`; `dagster_v3.defs.clickhouse.resolved.assert_clickhouse_tables_exist`; `dagster_clickhouse.ClickhouseResource`. During this task `tests/test_se_company_person_clickhouse_local.py` still exists and still owns `_clickhouse_local_command`; import it from there exactly as the address local tests do today. Task 4 moves it and re-points this file with the others.
- Produces: `normalize_person(raw: RawPerson) -> NormalizedPerson`; `RAW_ROW_COLUMNS` (15 names); `changed_scope_sql()`, `all_scope_sql()`, `changed_rows_sql()`, `all_rows_sql()`, `normalized_insert_sql()`; `normalized_row(raw_row, normalized_at) -> tuple` (23 values in `tables.NORMALIZED_COLUMNS` order); `NormalizeCounts(companies, pages, rows, ok, partial, no_person)` with `as_metadata()`; `normalize_companies(client, company_ids, *, changed_only, normalized_at, page_size, log=None)`; `normalize_all(client, *, changed_only, normalized_at, page_size, log=None)`; `PAGE_SIZE`, `NORMALIZE_ID_BOUND_QUERY_SETTINGS`, `SCRATCH_SCOPE_PREFIX`; asset `se_company_person_normalize` with `PersonNormalizeConfig(changed_only=True, company_ids=[], page_size=PAGE_SIZE)`.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_person_normalize.py`:

```python
"""The normalize asset's scan, row shaping and writes (spec section 4), against a scripted
ClickHouse client. The SQL texts run for real in
tests/test_se_company_person_normalize_clickhouse_local.py."""

import hashlib
from datetime import UTC, datetime

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.normalize import (
    NORMALIZE_ID_BOUND_QUERY_SETTINGS,
    PAGE_SIZE,
    SCRATCH_SCOPE_PREFIX,
    NormalizeCounts,
    all_rows_sql,
    all_scope_sql,
    changed_rows_sql,
    changed_scope_sql,
    normalize_all,
    normalize_companies,
    normalized_insert_sql,
    normalized_row,
)
from dagster_v3.defs.se_company.person.normalize_se import NORMALIZER_VERSION

STAMP = datetime(2026, 9, 9, 12, 0, 0, 123000, tzinfo=UTC)

# (company_id, source, slot, suggestion_id, full_name, first_name, last_name, birth_year,
#  wikidata_id, role_original, role_key, fiscal_year, role_from, role_to, data)
RAW_BV = ("5561552760", "bolagsverket", "rec1:sig1", "a" * 64, None, "Anna", "Svensson",
          None, None, "Styrelseledamot", "board_member", 2024, None, None, '{"signatory_kind":"board"}')
RAW_ESEF = ("5561552760", "esef", "doc7:2", "b" * 64, "Öberg, Håkan", None, None,
            None, None, "VD", "chief_executive", 2023, None, None, '{"section":"signatures"}')
RAW_TOMBSTONE = ("5560125220", "wikidata", "Q9:l1", "c" * 64, None, None, None,
                 None, None, None, None, None, None, None, "{}")


class FakeClient:
    def __init__(self, *, scope_ids, rows):
        self.scope_pages = [list(scope_ids)]
        self.rows = list(rows)
        self.statements: list[tuple[str, object, object]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params, settings))
        if sql.startswith(("CREATE TABLE", "DROP TABLE", "INSERT INTO")):
            return []
        if sql.startswith(f"SELECT company_id FROM {SCRATCH_SCOPE_PREFIX}"):
            return [(company_id,) for company_id in (self.scope_pages.pop(0) if self.scope_pages else [])]
        if f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in sql:
            wanted = set(params["company_ids"])
            return [row for row in self.rows if row[0] in wanted]
        raise AssertionError(sql)

    def inserts(self):
        prefix = f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE}"
        return [(sql, list(params)) for sql, params, _ in self.statements if sql.startswith(prefix)]


def test_normalized_row_has_the_23_values_in_column_order() -> None:
    row = normalized_row(RAW_BV, STAMP)
    assert len(row) == len(tables.NORMALIZED_COLUMNS) == 23
    by_name = dict(zip(tables.NORMALIZED_COLUMNS, row, strict=True))
    assert by_name["company_id"] == "5561552760" and by_name["source"] == "bolagsverket"
    assert by_name["slot"] == "rec1:sig1" and by_name["suggestion_id"] == "a" * 64
    assert by_name["normalizer_version"] == NORMALIZER_VERSION
    assert by_name["parse_status"] == "ok" and by_name["parse_notes"] == []
    assert by_name["first_tokens"] == ["anna"] and by_name["last_tokens"] == ["svensson"]
    assert by_name["display_name"] == "Anna Svensson"
    # role_key is the source's own code, passed through and used for the mapping; the map is
    # keyed on it, not on the label in role_original.
    assert by_name["role_key"] == "board_member"
    assert by_name["role_code"] == "board_member"
    assert by_name["role_year"] == 2024
    assert by_name["data"] == RAW_BV[14]
    assert by_name["normalized_at"] == STAMP


def test_normalized_id_is_the_suggestion_id_and_the_version() -> None:
    """Spec 3.2 -- NOT a stamp, so re-normalizing an unchanged row writes the identical id
    and the ReplacingMergeTree keeps one version instead of growing one per run."""
    row = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(RAW_ESEF, STAMP), strict=True))
    assert row["normalized_id"] == hashlib.sha256(f"{'b' * 64}\n{NORMALIZER_VERSION}".encode()).hexdigest()
    assert row["parse_status"] == "ok" and row["parse_notes"] == ["comma form"]
    assert row["display_name"] == "Håkan Öberg" and row["role_code"] == "chief_executive_officer"


def test_a_tombstone_row_normalizes_to_no_person() -> None:
    row = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(RAW_TOMBSTONE, STAMP), strict=True))
    assert row["parse_status"] == "no_person" and row["parse_notes"] == ["empty name"]
    assert row["display_name"] == "" and row["role_code"] is None and row["role_key"] is None
    assert row["first_tokens"] == [] and row["last_tokens"] == []
    assert row["data"] == "{}"


def test_the_read_sql_coerces_data_to_a_json_object() -> None:
    """`data` is a String holding a JSON object and the normalized table constrains it to
    one, so the read is what guarantees the write can land: a row whose text is an array, a
    scalar or nothing at all comes back as the empty object rather than violating the
    constraint 20,000 rows into a page."""
    for sql in (changed_rows_sql(), all_rows_sql()):
        assert "if(JSONType(r.data) = 'Object', r.data, '{}') AS data" in sql
        assert "toJSONString" not in sql


def test_sql_texts_read_final_rows_and_bind_ids_and_version() -> None:
    assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in changed_rows_sql()
    assert "LEFT ANTI JOIN" in changed_rows_sql() and "UNION ALL" in changed_rows_sql()
    assert "%(company_ids)s" in changed_rows_sql() and "%(normalizer_version)s" in changed_rows_sql()
    # The normalized table has no suggested_at: a changed observation changes suggestion_id.
    assert "n.suggestion_id != r.suggestion_id" in changed_rows_sql()
    assert "suggested_at" not in changed_rows_sql()
    assert "n.normalizer_version != %(normalizer_version)s" in changed_rows_sql()
    assert f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL" in changed_rows_sql()
    assert "JOIN" not in all_rows_sql()
    assert changed_scope_sql().startswith("SELECT DISTINCT company_id FROM (")
    assert all_scope_sql() == f"SELECT DISTINCT company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"


def test_the_insert_is_the_ordinary_block_statement() -> None:
    """Every column is a type clickhouse-driver knows -- `data` is a String, not the native
    JSON type -- so the insert is the same one-line VALUES header the address entity uses and
    the rows travel as a native block."""
    assert normalized_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} "
        f"({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES"
    )


def test_normalize_companies_reads_the_page_and_inserts_one_row_per_raw_row() -> None:
    client = FakeClient(scope_ids=[], rows=[RAW_BV, RAW_ESEF, RAW_TOMBSTONE])
    counts = normalize_companies(
        client, ["5561552760", "5560125220"], changed_only=True, normalized_at=STAMP, page_size=PAGE_SIZE
    )
    assert counts == NormalizeCounts(companies=2, pages=1, rows=3, ok=2, partial=0, no_person=1)
    [(sql, rows)] = client.inserts()
    assert sql == normalized_insert_sql()
    assert [row[1] for row in rows] == ["bolagsverket", "esef", "wikidata"]
    read = [sql for sql, _, _ in client.statements if "AS r FINAL" in sql]
    assert read == [changed_rows_sql()]
    assert client.statements[0][1] == {
        "company_ids": ["5560125220", "5561552760"], "normalizer_version": NORMALIZER_VERSION
    }
    assert client.statements[0][2] == NORMALIZE_ID_BOUND_QUERY_SETTINGS
    # No scratch table for a targeted call: the ids are paged in memory.
    assert not any(sql.startswith("CREATE TABLE") for sql, _, _ in client.statements)


def test_normalize_companies_with_changed_only_false_reads_every_raw_row() -> None:
    client = FakeClient(scope_ids=[], rows=[RAW_BV])
    normalize_companies(client, ["5561552760"], changed_only=False, normalized_at=STAMP, page_size=PAGE_SIZE)
    assert [sql for sql, _, _ in client.statements if "AS r FINAL" in sql] == [all_rows_sql()]


def test_normalize_all_scans_into_a_scratch_table_and_pages_it() -> None:
    client = FakeClient(scope_ids=["5560125220", "5561552760"], rows=[RAW_BV, RAW_ESEF, RAW_TOMBSTONE])
    counts = normalize_all(client, changed_only=True, normalized_at=STAMP, page_size=PAGE_SIZE)
    assert counts.companies == 2 and counts.pages == 1 and counts.rows == 3
    created = [sql for sql, _, _ in client.statements if sql.startswith("CREATE TABLE")]
    assert len(created) == 1 and created[0].split()[2].startswith(SCRATCH_SCOPE_PREFIX)
    scope_insert = next(sql for sql, _, _ in client.statements if sql.startswith(f"INSERT INTO {SCRATCH_SCOPE_PREFIX}"))
    assert changed_scope_sql() in scope_insert
    assert any(sql.startswith("DROP TABLE IF EXISTS") for sql, _, _ in client.statements)


def test_counts_metadata_keys() -> None:
    assert set(NormalizeCounts(1, 1, 1, 1, 0, 0).as_metadata()) == {
        "companies", "pages", "rows", "ok", "partial", "no_person", "normalizer_version",
    }


def test_a_full_page_renders_under_the_query_size_setting() -> None:
    """changed_rows_sql() binds %(company_ids)s four times (two UNION ALL branches, each with
    the outer WHERE plus the nested key select), so a full page of 12-digit ids renders far
    past basic-info's ID_BOUND_QUERY_SETTINGS -- which is why this module carries its own,
    wider setting. Modelled on the address entity's identical test. No server needed."""
    from types import SimpleNamespace

    from clickhouse_driver.util.escape import escape_params

    from dagster_v3.defs.se_company.basic_info.batch import ID_BOUND_QUERY_SETTINGS

    context = SimpleNamespace(
        server_info=SimpleNamespace(get_timezone=lambda: "UTC"),
        client_settings={"server_side_params": False},
    )
    for count in (PAGE_SIZE, 50_000):
        ids = [str(556_000_000_000 + index) for index in range(count)]
        rendered = changed_rows_sql() % escape_params(
            {"company_ids": ids, "normalizer_version": NORMALIZER_VERSION}, context
        )
        rendered_size = len(rendered.encode("utf-8"))
        if count == PAGE_SIZE:
            assert rendered_size > ID_BOUND_QUERY_SETTINGS["max_query_size"]
        assert rendered_size < NORMALIZE_ID_BOUND_QUERY_SETTINGS["max_query_size"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_normalize.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.person.normalize'`.

- [ ] **Step 3: Write `normalize.py`**

```python
"""The normalize step (spec 2026-09-09 section 4): raw suggestion rows whose normalized row
is missing, computed from another raw version, or computed by an older normalizer are
normalized in Python and written as new versions of se_company_person_normalized.

The shape is the address entity's, module for module. Two details are this entity's own:

1. THE CHANGE SCAN COMPARES suggestion_id, not a timestamp. se_company_person_normalized
   carries no suggested_at, and suggestion_id is already sha256(company_id, source, slot,
   suggested_at), so any changed observation changes it.
2. `data` IS A String HOLDING A JSON OBJECT, constrained to one by the tables that carry it
   (CONSTRAINT valid_data). The read coerces anything else to the empty object, so a
   malformed extractor row degrades to `{}` instead of failing a whole 20,000-company page
   on the insert. Nothing here needs the native JSON type, and nothing here writes text SQL.
"""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.basic_info.extract import SCAN_QUERY_SETTINGS, scope_pages
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.normalize_se import (
    NORMALIZER_VERSION,
    NormalizedPerson,
    RawPerson,
    normalize_se_person,
)

PAGE_SIZE = 20_000
# changed_rows_sql() binds %(company_ids)s four times; see the test of the same name.
NORMALIZE_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 4_194_304, "max_execution_time": 1800}
# This module's own scratch-table prefix (basic_info/extract.py:scope_pages), so a person
# scan's scratch table can never collide with a basic-info or an address one.
SCRATCH_SCOPE_PREFIX = "corpscout._tmp_person_scope_"

RAW_ROW_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "full_name", "first_name", "last_name",
    "birth_year", "wikidata_id", "role_original", "role_key", "fiscal_year", "role_from",
    "role_to", "data",
)


def normalize_person(raw: RawPerson) -> NormalizedPerson:
    """The per-country dispatcher of spec section 4. Only Sweden exists."""
    return normalize_se_person(raw)


def _raw_select(alias: str) -> str:
    """`data` is coerced to a JSON object here so the normalized table's valid_data
    constraint can only ever fire on a row somebody wrote by hand."""
    return ", ".join(
        f"if(JSONType({alias}.data) = 'Object', {alias}.data, '{{}}') AS data"
        if column == "data"
        else f"{alias}.{column} AS {column}"
        for column in RAW_ROW_COLUMNS
    )


def _normalized_keys_sql() -> str:
    return (
        "SELECT company_id, source, slot, suggestion_id, "
        "toString(normalizer_version) AS normalizer_version\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def changed_rows_sql() -> str:
    """The page's raw rows that need (re)normalizing: never normalized, computed from an
    older raw version, or computed by another normalizer version. Two branches instead of a
    LEFT JOIN so the result does not depend on join_use_nulls."""
    return (
        f"SELECT {_raw_select('r')}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        f"LEFT ANTI JOIN ({_normalized_keys_sql()}) AS n\n"
        "    ON n.company_id = r.company_id AND n.source = r.source AND n.slot = r.slot\n"
        "WHERE r.company_id IN %(company_ids)s\n"
        "UNION ALL\n"
        f"SELECT {_raw_select('r')}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        f"INNER JOIN ({_normalized_keys_sql()}) AS n\n"
        "    ON n.company_id = r.company_id AND n.source = r.source AND n.slot = r.slot\n"
        "WHERE r.company_id IN %(company_ids)s\n"
        "    AND (n.suggestion_id != r.suggestion_id "
        "OR n.normalizer_version != %(normalizer_version)s)"
    )


def all_rows_sql() -> str:
    return (
        f"SELECT {_raw_select('r')}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        "WHERE r.company_id IN %(company_ids)s"
    )


def changed_scope_sql() -> str:
    """Company ids with at least one raw row that needs normalizing (the whole table)."""
    keys = (
        "SELECT company_id, source, slot, suggestion_id, "
        "toString(normalizer_version) AS normalizer_version\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL"
    )
    return (
        "SELECT DISTINCT company_id FROM (\n"
        f"SELECT r.company_id AS company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        f"LEFT ANTI JOIN ({keys}) AS n\n"
        "    ON n.company_id = r.company_id AND n.source = r.source AND n.slot = r.slot\n"
        "UNION ALL\n"
        f"SELECT r.company_id AS company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL\n"
        f"INNER JOIN ({keys}) AS n\n"
        "    ON n.company_id = r.company_id AND n.source = r.source AND n.slot = r.slot\n"
        "WHERE n.suggestion_id != r.suggestion_id "
        "OR n.normalizer_version != %(normalizer_version)s\n"
        ") AS changed"
    )


def all_scope_sql() -> str:
    return f"SELECT DISTINCT company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"


def normalized_insert_sql() -> str:
    return (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} "
        f"({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES"
    )


def normalized_row(raw_row: Sequence[Any], normalized_at: datetime) -> tuple[Any, ...]:
    """One insert tuple in tables.NORMALIZED_COLUMNS order from one raw row in RAW_ROW_COLUMNS
    order. `data` and `role_key` pass through untouched -- the read already coerced `data` to
    a JSON object, and `role_key` is the source's own code, which the maps are keyed on."""
    row = dict(zip(RAW_ROW_COLUMNS, raw_row, strict=True))
    normalized = normalize_person(
        RawPerson(
            source=row["source"],
            full_name=row["full_name"],
            first_name=row["first_name"],
            last_name=row["last_name"],
            birth_year=row["birth_year"],
            wikidata_id=row["wikidata_id"],
            role_original=row["role_original"],
            role_key=row["role_key"],
        )
    )
    values = {
        "company_id": row["company_id"],
        "source": row["source"],
        "slot": row["slot"],
        "suggestion_id": row["suggestion_id"],
        "normalized_id": hashlib.sha256(
            f"{row['suggestion_id']}\n{NORMALIZER_VERSION}".encode()
        ).hexdigest(),
        "normalizer_version": NORMALIZER_VERSION,
        "parse_status": normalized.parse_status,
        "parse_notes": list(normalized.parse_notes),
        "first_tokens": list(normalized.first_tokens),
        "middle_tokens": list(normalized.middle_tokens),
        "last_tokens": list(normalized.last_tokens),
        "display_first": normalized.display_first,
        "display_last": normalized.display_last,
        "display_name": normalized.display_name,
        "birth_year": row["birth_year"],
        "wikidata_id": row["wikidata_id"],
        "role_key": row["role_key"],
        "role_code": normalized.role_code,
        "role_year": row["fiscal_year"],
        "role_from": row["role_from"],
        "role_to": row["role_to"],
        "data": row["data"] or "{}",
        "normalized_at": normalized_at,
    }
    return tuple(values[column] for column in tables.NORMALIZED_COLUMNS)


@dataclass(frozen=True, slots=True)
class NormalizeCounts:
    companies: int
    pages: int
    rows: int
    ok: int
    partial: int
    no_person: int

    def as_metadata(self) -> dict[str, Any]:
        return {
            "companies": self.companies,
            "pages": self.pages,
            "rows": self.rows,
            "ok": self.ok,
            "partial": self.partial,
            "no_person": self.no_person,
            "normalizer_version": NORMALIZER_VERSION,
        }


def _normalize_page(
    client: Any, company_ids: Sequence[str], *, changed_only: bool, normalized_at: datetime
) -> dict[str, int]:
    params = {"company_ids": sorted(company_ids), "normalizer_version": NORMALIZER_VERSION}
    raw_rows = client.execute(
        changed_rows_sql() if changed_only else all_rows_sql(),
        params,
        settings=NORMALIZE_ID_BOUND_QUERY_SETTINGS,
    )
    rows = [normalized_row(raw_row, normalized_at) for raw_row in raw_rows]
    status_index = tables.NORMALIZED_COLUMNS.index("parse_status")
    counts = {status: 0 for status in tables.PARSE_STATUSES}
    for row in rows:
        counts[row[status_index]] += 1
    if rows:
        client.execute(normalized_insert_sql(), rows)
    counts["rows"] = len(rows)
    return counts


def _accumulate(pages: list[dict[str, int]], companies: int) -> NormalizeCounts:
    total = {key: sum(page[key] for page in pages) for key in ("rows", *tables.PARSE_STATUSES)}
    return NormalizeCounts(
        companies=companies, pages=len(pages), rows=total["rows"], ok=total["ok"],
        partial=total["partial"], no_person=total["no_person"],
    )


def normalize_companies(
    client: Any, company_ids: Sequence[str], *, changed_only: bool, normalized_at: datetime,
    page_size: int = PAGE_SIZE, log: Any = None,
) -> NormalizeCounts:
    """Normalize the named companies' raw rows, paged in memory (no scan)."""
    ids = sorted(set(company_ids))
    pages: list[dict[str, int]] = []
    for start in range(0, len(ids), page_size):
        pages.append(
            _normalize_page(
                client, ids[start : start + page_size],
                changed_only=changed_only, normalized_at=normalized_at,
            )
        )
        if log is not None:
            log.info("normalized page %d: %d rows", len(pages), pages[-1]["rows"])
    return _accumulate(pages, len(ids))


def normalize_all(
    client: Any, *, changed_only: bool, normalized_at: datetime,
    page_size: int = PAGE_SIZE, log: Any = None,
) -> NormalizeCounts:
    """Scan the raw table once for the companies that need normalizing, then page them."""
    scope_sql = changed_scope_sql() if changed_only else all_scope_sql()
    pages: list[dict[str, int]] = []
    companies = 0
    for page in scope_pages(
        client, scope_sql=scope_sql, params={"normalizer_version": NORMALIZER_VERSION},
        page_size=page_size, settings=SCAN_QUERY_SETTINGS, prefix=SCRATCH_SCOPE_PREFIX,
    ):
        companies += len(page)
        pages.append(
            _normalize_page(client, page, changed_only=changed_only, normalized_at=normalized_at)
        )
        if log is not None:
            log.info("normalized page %d: %d companies, %d rows", len(pages), len(page), pages[-1]["rows"])
    return _accumulate(pages, companies)
```

- [ ] **Step 4: Write `assets.py`**

```python
"""Dagster assets of the person entity. Slice 0 ships the normalize asset; the extractors
and the stopped weekly follow in slice 1, the fold and the precedence export in slice 2."""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.normalize import PAGE_SIZE, normalize_all, normalize_companies

GROUP_NAME = "se_company_person"
NORMALIZE_POOL = "se_company_person_normalize"


class PersonNormalizeConfig(dg.Config):
    changed_only: bool = True
    company_ids: list[str] = Field(default_factory=list)
    page_size: int = Field(default=PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


@dg.asset(
    name="se_company_person_normalize",
    group_name=GROUP_NAME,
    pool=NORMALIZE_POOL,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_NORMALIZED_TABLE, "reads": tables.QUALIFIED_SUGGESTION_TABLE},
    description=(
        "Normalizes raw person suggestions into se_company_person_normalized: rows never "
        "normalized, computed from an older raw version, or computed by an older normalizer "
        "version. changed_only=false re-normalizes every raw row; company_ids targets companies."
    ),
)
def se_company_person_normalize(
    context: dg.AssetExecutionContext, config: PersonNormalizeConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.SUGGESTION_TABLE, tables.NORMALIZED_TABLE)
    )
    normalized_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        if config.company_ids:
            counts = normalize_companies(
                client, config.company_ids, changed_only=config.changed_only,
                normalized_at=normalized_at, page_size=config.page_size, log=context.log,
            )
        else:
            counts = normalize_all(
                client, changed_only=config.changed_only, normalized_at=normalized_at,
                page_size=config.page_size, log=context.log,
            )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "table": tables.QUALIFIED_NORMALIZED_TABLE}
    )
```

Check how `assert_clickhouse_tables_exist` is called in `se_company/address/assets.py` (its `tables=` argument shape) and match it exactly. No `jobs.py` in this slice: the extract job and the stopped weekly arrive with the extractors in slice 1, and a job selecting only the normalize asset would have no purpose.

- [ ] **Step 5: Write the clickhouse-local test**

`tests/test_se_company_person_normalize_clickhouse_local.py`, marked `pytestmark = pytest.mark.integration`, built the way `tests/test_se_company_address_normalize_clickhouse_local.py` is — read that file first and copy its structure rather than inventing a new one. During this task it still imports `_clickhouse_local_command` from `tests.test_se_company_person_clickhouse_local`; Task 4 re-points it.

1. Build the schema: read `000396_corpscout_se_company_person_entity.up.sql`, split on `;`, and keep the `CREATE DATABASE` statement plus every statement containing `CREATE TABLE IF NOT EXISTS corpscout.se_company_person_`. The `SYSTEM STOP/START VIEW` and `ALTER TABLE ... MODIFY QUERY` statements name `se_companies_serving`, which this fixture does not build, so they must be dropped — assert in the test that exactly six CREATE TABLE statements survive the filter.
2. Insert the three raw rows of Task 3's `RAW_BV` / `RAW_ESEF` / `RAW_TOMBSTONE` shapes as one `INSERT INTO corpscout.se_company_person_suggestion (...) VALUES ...`, with `suggested_at` `2026-09-01`, `2026-09-02`, `2026-09-03`, `source_record_id ''`, `document_ref NULL` and `data` as the JSON object text each shape carries. Then insert a fourth row whose `data` is `'{}'` and whose `role_key` is NULL, to prove the tombstone shape passes the `valid_data` constraint.
3. Assert the constraint bites: an `INSERT ... VALUES` whose `data` is `'[1,2]'` raises `Code: 469` (`VIOLATED_CONSTRAINT`). Use `pytest.raises`-style handling on the subprocess result, not a bare run — this is the assertion that keeps `data` an object.
4. For each of `join_use_nulls = 0` and `1`: run `changed_scope_sql()` with `normalizer_version = 'se-person-normalizer-v1'` and assert both company ids come back; run `changed_rows_sql()` for those ids and assert the rows come back in `(company_id, source, slot)` order with `data` already coerced (the fourth row's `'{}'` survives, and a row seeded with `'[1,2]'` cannot exist to be coerced, which is the point of step 3).
5. Insert one normalized row for the Bolagsverket raw row through `normalized_insert_sql()` with the exact tuple `normalized_row(RAW_BV, STAMP)` produces, binding the values as SQL literals the way the address local test does. Then re-run `changed_rows_sql()` under both settings and assert only the ESEF and Wikidata rows remain.
6. Insert a normalized row for the ESEF raw row with `normalizer_version = 'se-person-normalizer-v0'` and assert it is selected again under both settings (the version condition); then insert one for it with a different `suggestion_id` and assert the id condition fires the same way.
7. Assert `SELECT count() FROM corpscout.se_company_person_normalized FINAL` is 2, that `SELECT data FROM corpscout.se_company_person_normalized FINAL WHERE source = 'bolagsverket'` returns the Bolagsverket extras object as text, and that its `role_key` is `'board_member'`.

- [ ] **Step 6: Write the module docs**

`src/dagster_v3/defs/se_company/person/docs/person-design.md`, under 70 lines: a table with one row per module (`tables.py`, `roles.py`, `normalize_se.py`, `normalize.py`, `assets.py`) and its responsibility; the change rule of the normalize asset (missing, different `suggestion_id`, older normalizer version); what the three parse statuses mean; **the `data` contract** — a `String` holding a JSON object, never the native JSON type, with `CONSTRAINT valid_data CHECK JSONType(data) = 'Object'` on every table that carries one and the read coercing anything else to `'{}'` (owner ruling 2026-09-09); **the `role_key` column** — the source's own role code beside the human label in `role_original`, which the three per-source maps are keyed on and which slice 1's extractors must fill; the interrupted-migration runbook for 000395 (`SYSTEM START VIEW corpscout.se_companies_serving`, finish the ALTER if it did not land, `migrate force 395`); and how to run the asset (`changed_only`, `company_ids`, `page_size`). Point at the spec for everything else.

- [ ] **Step 7: Run the tests and the definitions check**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_person_normalize.py tests/test_se_company_person_normalize_se.py \
         tests/test_se_company_person_tables.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_person_normalize_clickhouse_local.py -q -m integration
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg list defs | grep se_company_person
```

Expected: unit tests PASS; the integration test PASSes (or SKIPs with the same reason the address local test skips on this machine — say so in the report, the controller runs it where `clickhouse-local` or docker exists); `All definitions loaded successfully.`; `se_company_person_normalize` listed in group `se_company_person`.

- [ ] **Step 8: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/normalize.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/assets.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/docs/person-design.md \
  corpscout/services/dagster_v3/tests/test_se_company_person_normalize.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_normalize_clickhouse_local.py
git commit -m "$(cat <<'EOF'
feat(dagster): se_company_person_normalize asset

Paged over the suggestion table, changed_only by default, with the change scan
on suggestion_id rather than a timestamp. `data` is a String holding a JSON
object, constrained to one by the DDL and coerced to '{}' by the read, so the
asset stays on the ordinary block read and write.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 4: The `company_people` package, its jobs, its sensor and its tests go

**Files:**
- Create: `tests/clickhouse_local.py` (the shared `clickhouse-local` helper, moved off the dying person test)
- Delete: `src/dagster_v3/defs/company_people/__init__.py`, `corrections.py`, `identity_eval.py`, `merge.py`, `normalization.py`, `roles.py`, `source_views.py` (the whole package directory)
- Delete: `src/dagster_v3/defs/sweden_financial/roles.py`, `src/dagster_v3/defs/esef_filings/roles.py`, `src/dagster_v3/defs/wikidata/roles.py`
- Delete: `tests/test_se_company_person_clickhouse_local.py`, `test_se_company_person_corrections.py`, `test_se_company_person_identity_eval.py`, `test_se_company_person_merge.py`, `test_se_company_person_normalization.py`, `test_se_company_person_normalization_live.py`, `test_se_company_person_roles.py`, `test_se_company_person_source_observations.py`, `test_se_company_person_views.py`
- Modify (import re-point only): `tests/test_se_companies_serving_sql.py`, `test_se_company_address_extractors_clickhouse_local.py`, `test_se_company_address_fold_clickhouse_local.py`, `test_se_company_address_geocode_clickhouse_local.py`, `test_se_company_address_normalize_clickhouse_local.py`, `test_se_company_basic_info_clickhouse_local.py`, `test_se_company_basic_info_extractors_clickhouse_local.py`, `test_sweden_company_source_tables_clickhouse_local.py`, `test_sweden_platsbanken_clickhouse_local.py`, `test_se_company_person_normalize_clickhouse_local.py`
- Modify (docstring only): `src/dagster_v3/defs/se_company/common.py:39-40`, `src/dagster_v3/defs/sweden_financial/audits.py:19`, `tests/test_se_company_common.py:42,288`

**Interfaces:**
- Consumes: Task 2's `se_company/person/roles.py` already holds the three per-source maps, which is why their old modules can go.
- Produces: `tests/clickhouse_local.py` exporting `CLICKHOUSE_IMAGE`, `clickhouse_local_command() -> list[str]`, `literal(value) -> str`, `render(sql, parameters) -> str`. The asset keys `se_company_person_clickhouse`, `se_company_person_llm_suggestions`, `se_company_person_promotion`, `se_company_person_role_draft_clickhouse`, `se_company_person_role_clickhouse`, `se_company_person_identity_evaluation`, `se_company_person_merge_suggestions`, the nine jobs (`se_company_person_job`, `_publish_job`, `_llm_suggestions_job`, `_promotion_job`, `_role_job`, `_role_publish_job`, `_identity_evaluation_job`, `_merge_job`, `_review_job`) and the sensor `se_company_person_correction_sensor` no longer exist. Task 5 removes the backoffice constants that name them.

**Why the whole package and not only the assets:** `rg` finds no importer of `dagster_v3.defs.company_people` outside the package and its own nine test files (checked 2026-09-09; every other hit on the string "company_people" is a comment, a `wikidata_company_people*` asset name, or the unrelated `serbia_apr_company_people` package). The three `roles.py` modules have exactly two importers each — `company_people/roles.py` and `tests/test_se_company_person_roles.py` — and both die here, which is why Task 2 moved their content first. `company_person_role_type`, the catalog those maps point into, is KEPT: it is seeded for Serbia too (`tests/test_serbia_apr_company_people.py` covers it and stays).

- [ ] **Step 1: Write the failing guard**

Append to `tests/test_se_company_common.py` (it already owns the "what belongs to the se_company layer" assertions):

```python
def test_the_retired_people_chain_is_gone_from_the_source_tree() -> None:
    """Slice 0 deleted the 2026-08-19 people model whole. A module left behind here would
    be dead code whose ClickHouse tables no longer exist -- every one of them is on the
    owner-run drop list."""
    defs_root = Path(dagster_v3.defs.__file__).resolve().parent
    assert not (defs_root / "company_people").exists()
    for module in ("sweden_financial/roles.py", "esef_filings/roles.py", "wikidata/roles.py"):
        assert not (defs_root / module).exists(), module
    # The maps themselves live on, in the person package.
    from dagster_v3.defs.se_company.person.roles import SOURCE_ROLE_MAPPINGS

    assert set(SOURCE_ROLE_MAPPINGS) == {"bolagsverket", "esef", "wikidata"}
```

Add `from pathlib import Path` and `import dagster_v3.defs` to that file's imports if they are not there.

- [ ] **Step 2: Run it and watch it fail**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_common.py -q -k retired_people_chain
```

Expected: FAIL — `defs/company_people` still exists.

- [ ] **Step 3: Move the shared `clickhouse-local` helper before its host dies**

Create `tests/clickhouse_local.py`, copying the four helpers out of `tests/test_se_company_person_clickhouse_local.py` (lines 63, 307-345 today) unchanged apart from their names losing the leading underscore:

```python
"""Running SQL against a real ClickHouse engine from a test.

`clickhouse-local` when the machine has a binary, otherwise the pinned server image under
Docker, otherwise the caller skips. Lived in tests/test_se_company_person_clickhouse_local.py
until the people chain that file covered was retired (person slice 0, 2026-09-09); eight
other integration tests imported it from there, which is why it is its own module now.
"""

import shutil
import subprocess
from datetime import datetime
from typing import Any

import pytest

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:26.5"


def clickhouse_local_command() -> list[str]:
    """A `clickhouse-local` invocation, or skip when the machine has none."""
    direct = shutil.which("clickhouse-local")
    if direct:
        return [direct, "--multiquery"]
    binary = shutil.which("clickhouse")
    if binary:
        return [binary, "local", "--multiquery"]
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("no clickhouse-local binary and no docker to run one")
    probe = subprocess.run([docker, "info"], capture_output=True, text=True, timeout=60, check=False)
    if probe.returncode != 0:
        pytest.skip("docker is installed but not running")
    return [docker, "run", "--rm", "-i", CLICKHOUSE_IMAGE, "clickhouse-local", "--multiquery"]


def literal(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        stamp = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"toDateTime64('{stamp}', 3, 'UTC')"
    if isinstance(value, tuple | list):
        return "(" + ", ".join(literal(item) for item in value) + ")"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def render(sql: str, parameters: dict[str, Any]) -> str:
    """Inline clickhouse-driver's `%(name)s` placeholders for the CLI."""
    for name, value in parameters.items():
        sql = sql.replace(f"%({name})s", literal(value))
    assert "%(" not in sql, sql
    return sql
```

Re-point every importer:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3/tests
for f in test_se_companies_serving_sql.py test_se_company_address_extractors_clickhouse_local.py \
         test_se_company_address_fold_clickhouse_local.py test_se_company_address_geocode_clickhouse_local.py \
         test_se_company_address_normalize_clickhouse_local.py test_se_company_basic_info_clickhouse_local.py \
         test_se_company_basic_info_extractors_clickhouse_local.py test_sweden_company_source_tables_clickhouse_local.py \
         test_sweden_platsbanken_clickhouse_local.py test_se_company_person_normalize_clickhouse_local.py; do
  perl -0pi -e 's/from tests\.test_se_company_person_clickhouse_local import \(?\s*_clickhouse_local_command,?\s*(_literal,?\s*)?\)?/from tests.clickhouse_local import clickhouse_local_command$1/s' "$f"
  perl -pi -e 's/\b_clickhouse_local_command\b/clickhouse_local_command/g; s/\b_literal\b/literal/g' "$f"
done
rg -n "test_se_company_person_clickhouse_local|_clickhouse_local_command" . && echo "STILL REFERENCED" || echo "clean"
```

The `perl` re-point is a starting point, not a promise: read every one of the ten diffs, fix the import block by hand where the regex left a stray comma or a half-parenthesised import, and make sure `literal` is imported in the two files that use it (`test_se_companies_serving_sql.py` and, if the address geocode test uses it, that one). The check that matters is that `rg` finds no `test_se_company_person_clickhouse_local` and no `_clickhouse_local_command` anywhere under `tests/`.

- [ ] **Step 4: Delete the package, the three role modules and the nine test files**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
rm -r src/dagster_v3/defs/company_people
rm src/dagster_v3/defs/sweden_financial/roles.py \
   src/dagster_v3/defs/esef_filings/roles.py \
   src/dagster_v3/defs/wikidata/roles.py
rm tests/test_se_company_person_clickhouse_local.py \
   tests/test_se_company_person_corrections.py \
   tests/test_se_company_person_identity_eval.py \
   tests/test_se_company_person_merge.py \
   tests/test_se_company_person_normalization.py \
   tests/test_se_company_person_normalization_live.py \
   tests/test_se_company_person_roles.py \
   tests/test_se_company_person_source_observations.py \
   tests/test_se_company_person_views.py
find . -name __pycache__ -path "*company_people*" -exec rm -r {} +
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
```

Expected: `All definitions loaded successfully.` — nothing outside the deleted set imported any of it.

- [ ] **Step 5: Retire the four docstring references**

- `src/dagster_v3/defs/se_company/common.py:39-40`: the paragraph explains that `normalized_se_company_ids` is not `company_people.source_views.normalized_company_ids` because that one validated 10 digits only. Replace those two sentences with: "The retired people chain had its own 10-digit-only validator; this one accepts both widths the se_company tables publish, which is why the sole traders survive a scoped run."
- `src/dagster_v3/defs/sweden_financial/audits.py:19`: the docstring points at `company_people/tables.py`'s `_SE_XBRL_SIGNATURES_SELECT`. Say instead that the people model that consumed this projection was retired on 2026-09-09 and the signatories table is now read by the person entity's extractor (slice 1).
- `tests/test_se_company_common.py:42`: same substitution as `common.py:39-40`.
- `tests/test_se_company_common.py:288`: it points at `_FakeCorrectionLedgerClient` in the deleted `test_se_company_person_corrections.py`. Re-point it at `tests/test_se_company_basic_info_batch.py`, which uses the same SimpleNamespace technique.

- [ ] **Step 6: Run everything this touched**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_common.py tests/test_serbia_apr_company_people.py \
         tests/test_wikidata_assets.py tests/test_clickhouse_leaf_checks.py \
         tests/test_se_company_person_normalize.py tests/test_se_company_person_normalize_se.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
rg -n "company_people\." src tests | rg -v "serbia_apr_company_people|wikidata_company_people"
rg -n "se_company_person_job|se_company_person_role_job|se_company_person_correction_sensor" src
```

Expected: tests pass, `dg check defs` clean, and both `rg` commands return nothing (`tests/test_serbia_apr_company_people.py` and every `wikidata_company_people*` asset stay — they are different things that merely share a word).

- [ ] **Step 7: Commit**

```bash
git add -u corpscout/services/dagster_v3/src corpscout/services/dagster_v3/tests
git add corpscout/services/dagster_v3/tests/clickhouse_local.py
git commit -m "$(cat <<'EOF'
refactor(dagster): retire the 2026-08-19 SE people chain

The company_people package with its nine jobs, its correction sensor and its
nine test files, plus the three per-source role modules whose maps moved into
the person package. The shared clickhouse-local helper eight other integration
tests imported from the person test moves to tests/clickhouse_local.py first.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 5: The backoffice People area and the company People tab go

**Files (all under `corpscout/services/backoffice`):**
- Delete (routes): `app/routes/admin-se-people.tsx`, `admin-se-people-person.tsx`, `admin-se-people-pipeline.tsx`, `admin-se-people-stale-corrections.tsx`, `admin-se-company-people.tsx`, `app/routes/admin-se-people.test.ts`
- Delete (libs): `app/lib/se-company-person.server.ts`, `se-company-person-pipeline.server.ts`, `se-company-person-pipeline.server.test.ts`, `se-company-person-pipeline.test.ts`, `se-company-people.server.ts`, `se-people-simple-sync.server.ts`, `se-people-simple-sync.server.test.ts`, `se-people-sources.server.ts`, `se-people-sources.ts`, `se-people-tasks.server.ts`, `se-people-tasks.server.test.ts`, `se-person-merge-suggestions.ts`
- Delete (components): `app/components/admin/se-company-people.tsx`, `se-people-actions.tsx`, `se-people-actions.test.tsx`, `se-people-pipeline.tsx`, `se-people-sources-table.tsx`, `se-person-review-workspace.tsx`, `se-stale-corrections-table.tsx`
- Delete (tests): `tests/admin-se-people.test.ts`, `tests/admin-se-people-person.test.tsx`, `tests/admin-se-people-pipeline.test.ts`, `tests/admin-se-stale-corrections.test.tsx`, `tests/se-company-person.server.test.ts`, `tests/se-people-sources.server.test.ts`, `tests/se-people-sources.test.ts`, `tests/se-people-sources-table.test.tsx`
- Modify: `app/routes.ts` (five route entries), `app/lib/se-company-tabs.ts` (the People tab), `app/components/admin/admin-sidebar.tsx` (three nav entries), `app/routes/admin-layout.tsx` (the People breadcrumbs), `app/lib/clickhouse.server.ts` (`chInsertSeCompanyPersonCorrections`), `app/lib/dagster.server.ts` (the whole `SE_COMPANY_PERSON_*` block), `app/lib/dagster.server.test.ts:408` (a fixture sensor name), `app/lib/technologies.ts:5,99` and `app/lib/technologies.server.ts:84` (comments pointing at `se-people-sources.ts`), `app/lib/se-company-info-lists.server.ts:141` (a comment naming the dropped table)
- Modify: `tests/se-company-tabs.server.test.ts`, `tests/admin-se-company-area.test.tsx`, `tests/clickhouse-writer.server.test.ts`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `/admin/se/people`, `/admin/se/people/person/:companyId/:personId`, `/admin/se/people/pipeline`, `/admin/se/people/stale-corrections` and the company tab `/admin/se/company/:companyId/people` no longer resolve; `chInsertSeCompanyPersonCorrections` and every `SE_COMPANY_PERSON_*` constant no longer exist. Slice 3 re-adds a People tab on `app/routes/admin-se-company-person.tsx` and a rewritten `/admin/se/people` list; nothing here is ported.

**Why all of it and not a port:** spec section 7 — "The old person workspace, pipeline and stale-corrections pages are deleted, not ported." Every one of these modules reads a table on the drop list (`se_company_person`, `_role`, `_role_draft`, `_correction`, `_enrichment_observation`, `_collision_candidate`) or launches a job Task 4 deleted. `rg` confirms their only importers are each other, `routes.ts` and the three multi-tab test files (checked 2026-09-09); `app/lib/technologies.ts` and `technologies.server.ts` name `se-people-sources.ts` in comments only.

- [ ] **Step 1: Write the failing tab test**

In `tests/admin-se-company-area.test.tsx`, change the tab-strip case (today at line 1276) to the nine-tab list and add a guard beside it:

```tsx
  it("is exactly Info, Address, Financial, ESEF, Domains, Technology, Contracts, Jobs, Listed, in that order", () => {
    expect(SE_COMPANY_TABS.map((tab) => tab.label)).toEqual([
      "Info",
      "Address",
      "Financial",
      "ESEF",
      "Domains",
      "Technology",
      "Contracts",
      "Jobs",
      "Publicly traded",
    ]);
  });

  it("has no People tab: the 2026-08-19 people model was retired and slice 3 adds the new one", () => {
    expect(SE_COMPANY_TABS.map((tab) => tab.value)).not.toContain("people");
  });
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd corpscout/services/backoffice
npx vitest run tests/admin-se-company-area.test.tsx
```

Expected: FAIL — "People" is still in the tab strip.

- [ ] **Step 3: Delete the modules and their tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
rm app/routes/admin-se-people.tsx app/routes/admin-se-people-person.tsx \
   app/routes/admin-se-people-pipeline.tsx app/routes/admin-se-people-stale-corrections.tsx \
   app/routes/admin-se-company-people.tsx app/routes/admin-se-people.test.ts
rm app/lib/se-company-person.server.ts app/lib/se-company-person-pipeline.server.ts \
   app/lib/se-company-person-pipeline.server.test.ts app/lib/se-company-person-pipeline.test.ts \
   app/lib/se-company-people.server.ts app/lib/se-people-simple-sync.server.ts \
   app/lib/se-people-simple-sync.server.test.ts app/lib/se-people-sources.server.ts \
   app/lib/se-people-sources.ts app/lib/se-people-tasks.server.ts \
   app/lib/se-people-tasks.server.test.ts app/lib/se-person-merge-suggestions.ts
rm app/components/admin/se-company-people.tsx app/components/admin/se-people-actions.tsx \
   app/components/admin/se-people-actions.test.tsx app/components/admin/se-people-pipeline.tsx \
   app/components/admin/se-people-sources-table.tsx app/components/admin/se-person-review-workspace.tsx \
   app/components/admin/se-stale-corrections-table.tsx
rm tests/admin-se-people.test.ts tests/admin-se-people-person.test.tsx \
   tests/admin-se-people-pipeline.test.ts tests/admin-se-stale-corrections.test.tsx \
   tests/se-company-person.server.test.ts tests/se-people-sources.server.test.ts \
   tests/se-people-sources.test.ts tests/se-people-sources-table.test.tsx
```

- [ ] **Step 4: Take the routes, the tab, the nav and the breadcrumbs out**

`app/routes.ts`: delete the four top-level entries — `route("se/people", ...)`, `route("se/people/person/:companyId/:personId", ...)` with its comment block, `route("se/people/pipeline", ...)` with its comment block, and `route("se/people/stale-corrections", ...)` — and the nested `route("people", "routes/admin-se-company-people.tsx")` inside the company layout. Update the layout's comment "One company, nine tabs" to the count the strip now has.

`app/lib/se-company-tabs.ts`: delete the `{ value: "people", label: "People" }` entry from `SE_COMPANY_TABS`.

`app/components/admin/admin-sidebar.tsx`: delete the three entries pointing at `/admin/se/people`, `/admin/se/people/stale-corrections` and `/admin/se/people/pipeline` (today lines 47-60), the `render={<Link to="/admin/se/people" />}` block at line 103, and any icon import left unused by those deletions.

`app/routes/admin-layout.tsx`: delete every `BreadcrumbLink` whose `to` is `/admin/se/people` (lines 182, 218, 260, 282, 303, 309) together with the branch each one belongs to, and the `<BreadcrumbPage>People</BreadcrumbPage>` at line 315. Read each branch before cutting: some of these are inside `if (onPeoplePage)`-style guards whose condition also goes, and the file must still render breadcrumbs for every surviving admin page.

- [ ] **Step 5: Delete the writer and the job constants**

`app/lib/clickhouse.server.ts`: delete `chInsertSeCompanyPersonCorrections` (lines 128-138 today, comment included). `chInsertCompanyDomains` above it and `chInsertSeBasicInfoPrecedence` below it stay.

`app/lib/dagster.server.ts`: delete the whole `SE_COMPANY_PERSON_*` block (lines 58-108 today) — the comment header, `SE_COMPANY_PERSON_IDENTITY_EVALUATION_JOB`, `_IDENTITY_EVALUATION_ASSET`, `SE_COMPANY_PERSON_JOB`, `_ROLE_DRAFT_ASSET`, `_ASSET`, `_ROLE_ASSET`, `_PUBLISH_JOB`, `_LLM_SUGGESTIONS_JOB`, `_LLM_SUGGESTIONS_ASSET`, `_PROMOTION_JOB`, `_PROMOTION_ASSET`, `_MERGE_JOB`, `_MERGE_ASSET`, `_ROLE_JOB`, `_REVIEW_JOB` and their doc comments. `SE_COMPANY_ADDRESS_FOLD_COMPANIES_ASSET` immediately above stays.

`app/lib/dagster.server.test.ts:408`: the mocked `sensorsOrError` fixture names `se_company_person_correction_sensor`, a sensor that no longer exists. Rename it to `wikidata_company_people_sensor`, which does — the case is about filtering instigator states, not about that particular sensor.

`app/lib/se-company-info-lists.server.ts:141`: the comment says `has_people` is "0 | 1 -- has a published row in corpscout.se_company_person". Say instead: "0 | 1 -- has an active row in the person entity's main table (migration 000395); empty until slice 2's first fold."

`app/lib/technologies.ts:5,99` and `app/lib/technologies.server.ts:84`: three comments cite `se-people-sources.ts` / `se-people-sources.server.ts` as the pattern they mirror. Re-point them at `app/lib/se-companies-tabs.ts` and `app/lib/se-company-info-lists.server.ts`, which carry the same catalog and filter shapes.

- [ ] **Step 6: Trim the three multi-tab test files**

`tests/se-company-tabs.server.test.ts`: delete the `~/lib/se-company-people.server` import block (`PEOPLE_ROLES_SQL`, `PEOPLE_SQL`, `loadSeCompanyPeople`), any `chInsertSeCompanyPersonCorrections` line in the hoisted `vi.mock`, the people SQL constants from the statement list the file pins, and every `it(...)` case that calls `loadSeCompanyPeople`. Every other tab's coverage stays.

`tests/admin-se-company-area.test.tsx`: delete the `SeCompanyPeopleTab` import (line 22), the `SeCompanyPersonRow` type import (line 45), the `people` fixture (line 331), the whole `describe("people tab")` block (line 367 onward) and the people case at line 1234. The header, tab-path, address, financial, ESEF, jobs and technology blocks stay; the tab-strip case is the one Step 1 already rewrote.

`tests/clickhouse-writer.server.test.ts`: delete the `chInsertSeCompanyPersonCorrections` import and every case that calls it. If a case covers "an empty batch inserts nothing" only through that function, re-point it at `chInsertSeBasicInfoPrecedence` rather than dropping the coverage.

- [ ] **Step 7: Run the suite and the type check**

```bash
cd corpscout/services/backoffice
npx vitest run tests/admin-se-company-area.test.tsx tests/se-company-tabs.server.test.ts \
  tests/clickhouse-writer.server.test.ts tests/se-company-info-lists.server.test.ts \
  app/lib/dagster.server.test.ts
npm run typecheck
rg -n "se-company-people|se-company-person\.server|se-people-|se-person-merge|SeCompanyPeopleTab|chInsertSeCompanyPersonCorrections|SE_COMPANY_PERSON_" app tests
```

Expected: tests pass, typecheck clean, and `rg` returns nothing. `npm run typecheck` is the real gate here — a route entry left behind in `routes.ts` fails it with a missing-module error.

- [ ] **Step 8: Commit**

```bash
git add -u corpscout/services/backoffice/app corpscout/services/backoffice/tests
git commit -m "$(cat <<'EOF'
refactor(backoffice): retire the SE People area and the company People tab

Four admin routes, the company tab, twelve libs, seven components, the person
correction writer and every SE_COMPANY_PERSON_* job constant. Slice 3 builds
the new tab and list on the person entity; none of this is ported.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 6: The SE Management section, its dbt models and the `company_serving` contract go

**Files (backoffice):**
- Modify: `app/components/detail/lazy-company-section.tsx:10,120-124`, `app/routes/country-company-detail.tsx:83`, `app/lib/company-sections.server.ts:25,145-148,183-184,259-356`
- **Untouched, deliberately:** `app/components/detail/management-section.tsx`, `tests/management-section.test.tsx`, `app/routes/country-company-detail.tsx:15,181-187` (the legacy-mode render), `app/lib/queries.server.ts`, `app/lib/countries.ts`, `tests/fr-detail.queries.test.ts`

**Files (dagster_v3):**
- Delete: `src/dagster_v3/defs/company_serving/dbt/models/company_management_current_build.sql`
- Modify: `.../dbt/models/schema.yml:47`, `.../dbt/models/company_section_presence_current_build.sql:16`, `.../dbt/models/company_section_item_source_links_build.sql:343-404,479`, `.../dbt/models/sources.yml:22,24,29-32`
- Modify: `src/dagster_v3/defs/company_serving/tables.py:118-131,324,339,341-343`, `src/dagster_v3/defs/company_serving/publish.py:299-302`
- Modify: `tests/test_company_serving.py:75`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `COMPANY_SECTION_NAMES` and `VALID_SECTIONS` no longer contain `"management"`; `tables.MANAGEMENT` and `tables.HISTORY_TABLES["company_management_current"]` no longer exist; `corpscout.company_management_current` has no writer, which is what puts it on Task 7's drop list.

**Why only the SE half (controller ruling, 2026-09-09):** `ManagementSection` has two call sites. The serving-mode section list reaches it through `company-sections.server.ts` and `company_management_current` — that is the SE path, and it is what this slice retires. The legacy detail mode reaches it through `getCompanyDetail`'s raw-source reads (`auditQuery`, `wikidataPeopleQuery`, the ESEF people rows), which is how France and every other country renders officers today; that path stays whole. So the component file, its test, `queries.server.ts`, `countries.ts` (including `FR_WIKIDATA_PEOPLE_QUERY`) and `tests/fr-detail.queries.test.ts` are all **untouched**.

The component has no SE-only branch to cut: the survey (2026-09-09) found its one country-specific line is `officer.person_profile_available === false ? "" : "/country/<cc>/person/<id>"`, a public route that this slice does not touch, and the legacy call site passes `officers={[]}` anyway. If a later reading finds a branch reachable only from the deleted serving path, cut that branch and nothing else. `rg -n "ManagementSection|management-section" app tests` must still find the legacy call site after this task.

- [ ] **Step 1: Write the failing tests**

In `tests/se-company-info-lists.server.test.ts` (or any surviving backoffice test file that imports `company-sections.server`), add:

```ts
import { COMPANY_SECTION_NAMES } from "~/lib/company-sections.server";

describe("company sections after the people retirement", () => {
  it("has no management section: the public Management block was retired with the people chain", () => {
    expect(COMPANY_SECTION_NAMES).not.toContain("management");
  });
});
```

In `tests/test_company_serving.py`, change the section list case (line 75) to drop `"management"`, and add:

```python
def test_the_management_contract_is_gone() -> None:
    """company_management_current lost its dbt model and its reader in person slice 0; the
    table is on the owner-run drop list, so nothing may still promise to publish it."""
    assert "management" not in tables.VALID_SECTIONS
    assert all(contract.name != "company_management_current" for contract in tables.CURRENT_TABLES)
    assert "company_management_current" not in tables.HISTORY_TABLES
```

- [ ] **Step 2: Run them and watch them fail**

```bash
cd corpscout/services/backoffice && npx vitest run tests/se-company-info-lists.server.test.ts
cd ../dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync pytest tests/test_company_serving.py -q
```

Expected: both FAIL — `"management"` is still in both lists.

- [ ] **Step 3: Take the SE serving call site out, and only that one**

`app/components/detail/lazy-company-section.tsx`: delete the `ManagementSection` import (line 10) and the whole `case "management":` branch (lines 120-124). This file renders the serving-mode sections only, so nothing else here reaches the component.

`app/routes/country-company-detail.tsx`: delete the `"management"` entry from `sectionOrder` (line 83) — the serving-mode list. **Keep** the `ManagementSection` import (line 15) and the `<ManagementSection ... />` element (lines 181-187): that is the legacy detail mode France and the other countries render, and it reads raw sources, not `company_management_current`.

`app/components/detail/management-section.tsx` and `tests/management-section.test.tsx` are **not deleted**.

- [ ] **Step 4: Delete the SE serving loader**

`app/lib/company-sections.server.ts`: delete `"management"` from `COMPANY_SECTION_NAMES` (line 25), the `| { section: "management"; officers: ...; wikidataPeople: ...; esefPeople: ... }` member of the `CompanySectionData` union (lines 145-148), the `case "management": return getManagementSection(country, companyId);` dispatch (lines 183-184), the `ManagementServingRow` interface and the whole `getManagementSection` function (lines 259-356), and any type import at the top of the file (`OfficerRow`, `WikidataPersonRow`, `EsefPersonObservation`) the file no longer uses.

- [ ] **Step 5: Confirm the loader chain still has a consumer**

The legacy detail mode keeps every one of these alive, so nothing in `queries.server.ts` or `countries.ts` is deleted in this slice. Prove it rather than assume it:

```bash
cd corpscout/services/backoffice
for symbol in OfficerRow PeopleMatchRow AuditRow WikidataPersonRow EsefPersonObservation \
              wikidataPeople esefPeople esefPeopleRows auditQuery wikidataPeopleQuery \
              FR_WIKIDATA_PEOPLE_QUERY; do
  echo "== $symbol"; rg -n "\b$symbol\b" app tests; done
```

Every one of the eleven must still show a consumer outside `company-sections.server.ts` — `management-section.tsx`, `country-company-detail.tsx`, `queries.server.ts` itself or `tests/fr-detail.queries.test.ts`. If one comes back with **no** consumer at all, it was serving-only after all: delete it under the keep rule and say which in the report. The three type imports `company-sections.server.ts` itself drops in Step 4 are the expected difference and are not a signal that the type is dead.

- [ ] **Step 6: Delete the dbt model and the management halves of the other two**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
rm src/dagster_v3/defs/company_serving/dbt/models/company_management_current_build.sql
```

`dbt/models/schema.yml`: delete the `- name: company_management_current_build` entry (line 47) and its description block.

`dbt/models/company_section_presence_current_build.sql`: delete the `SELECT country_code, company_id, 'management', management_id, resolved_at FROM {{ ref('company_management_current_build') }}` branch (line 16) together with the `UNION ALL` that joins it.

`dbt/models/company_section_item_source_links_build.sql`: delete the `registry_management` CTE (line 347), the `management_candidates` CTE (line 365, including its `ref('company_management_current_build')` and the two source joins at lines 393 and 396), the `management` CTE (line 399) and the `UNION ALL SELECT * FROM management` line (479). The `wikidata_persons` read at line 66 belongs to the wikidata section and **stays**.

`dbt/models/sources.yml`: delete `wikidata_company_people` (line 22), `se_financial_report_signatories` (line 24) and the `esef_document_people` entry with its `asset_key` meta block (lines 29-32) — **after** confirming with `rg -n "wikidata_company_people|se_financial_report_signatories|esef_document_people" src/dagster_v3/defs/company_serving/dbt/models/` that no model still references them. `wikidata_persons` (line 23) **stays**. If the other session's ESEF migration has already landed on main, `esef_document_people` may have been renamed to `esef_document_people_legacy` with a new `se_esef_document_people` view beside it — in that case remove whichever entry this project no longer reads and leave the other alone.

- [ ] **Step 7: Delete the `company_serving` contract**

`src/dagster_v3/defs/company_serving/tables.py`: delete the `MANAGEMENT = CurrentTable(...)` block (lines 118-131 through its column tuple and key), its entry in `CURRENT_TABLES` (line 324), its entry in `HISTORY_TABLES` (line 339, `"company_management_observations"`), and `"management"` from `VALID_SECTIONS` (line 343). Both tables the contract named — `company_management_current` and `company_management_observations` — lose their last writer here and are dropped together in Task 7.

`src/dagster_v3/defs/company_serving/publish.py`: delete the `"management": (...)` entry from `expected_queries` in `_validate_presence_counts` (lines 299-302).

- [ ] **Step 8: Run everything this touched**

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/company_serving/dbt \
  --profiles-dir src/dagster_v3/defs/company_serving/dbt
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_company_serving.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
rg -n "company_management_current|company_management_observations" src tests
cd ../backoffice && npx vitest run && npm run typecheck
rg -n "getManagementSection|company_management_current" app tests
rg -n "ManagementSection" app tests
```

Expected: `dbt parse` succeeds (a dangling `ref()` fails it loudly, which is the point of running it before `dg check defs`, which loads the manifest); `dg check defs` clean; the first two `rg` commands return nothing; the **third returns the legacy call site and the component's own test**, which is how you know Task 6 stayed inside Sweden; the backoffice suite green apart from the pre-existing failures, typecheck clean.

- [ ] **Step 9: Commit**

```bash
git add -u corpscout/services/dagster_v3/src corpscout/services/dagster_v3/tests \
           corpscout/services/backoffice/app corpscout/services/backoffice/tests
git commit -m "$(cat <<'EOF'
refactor: retire the SE Management section and its serving contract

The serving-mode call site and loader, the company_management_current dbt model
with the management halves of the presence and source-link models, and the
MANAGEMENT entry in the company_serving contract. The component and the legacy
detail mode France renders stay. Both management tables are on the drop list.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 7: The three owner-run SQL scripts

**Files:**
- Create: `corpscout/clickhouse/operations/se_person_retirement_precheck.sql`, `se_person_retirement_drops.sql`, `se_person_retirement_postcheck.sql`
- Create: `tests/test_se_person_retirement_drops.py`

**Interfaces:**
- Consumes: nothing from earlier tasks. This task's `DROP_ORDER` is its own ordered tuple; Task 8's ledger guard has its own, longer, unordered one (it also covers three objects that are already gone and only lose their DDL now).
- Produces: `corpscout/clickhouse/operations/se_person_retirement_drops.sql`, twelve statements, one per line, in `DROP_ORDER`; Task 9 pipes exactly this file.

**Location:** `corpscout/clickhouse/operations/`, the house directory for owner-run work beside the ledger they retire from, as `se_address_retirement_*.sql` did on 2026-09-08. `tests/test_clickhouse_migrations.py` already resolves it as `Path(__file__).resolve().parents[3] / "clickhouse" / "operations"`; this test uses the same expression.

- [ ] **Step 1: Write the failing test**

Create `tests/test_se_person_retirement_drops.py`:

```python
"""The person slice-0 drop scripts say exactly what the spec's retirement list says, in
dependency order.

The scripts live in corpscout/clickhouse/operations/ beside the ledger they retire from, and
they are owner-run: nothing in this repo executes them, so this file is the only thing
standing between a typo and a dropped production table. It reads the SQL, parses the object
names out, and compares them as WHOLE names -- corpscout.se_company_person is a prefix of
five entries here AND of the six tables the entity keeps, and company_person_role prefixes
the role catalog that stays.
"""

import re
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[3] / "clickhouse" / "operations"
DROPS = SCRIPTS / "se_person_retirement_drops.sql"
PRECHECK = SCRIPTS / "se_person_retirement_precheck.sql"
POSTCHECK = SCRIPTS / "se_person_retirement_postcheck.sql"

# Spec section 8's retirement list, ordered so every object is dropped after everything that
# reads it. The three source views read se_financial_report_signatories, esef_document_people
# and the wikidata tables -- all KEPT -- so they only have to precede nothing in particular,
# but they go first because the retired assets read THEM. company_management_current is a
# dbt-built table whose model went in Task 6, and company_management_observations is its
# history twin, which the same contract wrote and which nothing has read since (controller
# ruling 2026-09-09: it joins the list rather than being left an orphan). se_company_person
# is last: the role, draft, correction, observation and collision tables all key off it.
DROP_ORDER = (
    ("VIEW", "se_company_person_bolagsverket"),
    ("VIEW", "se_company_person_esef"),
    ("VIEW", "se_company_person_wikidata"),
    ("TABLE", "company_management_current"),
    ("TABLE", "company_management_observations"),
    ("TABLE", "se_company_person_collision_candidate"),
    ("TABLE", "se_company_person_enrichment_observation"),
    ("TABLE", "se_company_person_correction"),
    ("TABLE", "se_company_person_role_draft"),
    ("TABLE", "se_company_person_role"),
    ("TABLE", "se_company_person_v1_role_baseline"),
    ("TABLE", "se_company_person"),
)

# Never droppable. The entity's six tables (se_company_person_v2 and its siblings), the role
# catalog Serbia shares, the raw sources every extractor will read in slice 1, and the
# serving view.
KEPT = (
    "se_company_person_suggestion",
    "se_company_person_normalized",
    "se_company_person_v2",
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

_DROP = re.compile(r"^DROP (TABLE|VIEW) IF EXISTS corpscout\.(\w+);$", re.MULTILINE)


def _statements(path: Path) -> list[tuple[str, str]]:
    return [(kind, name) for kind, name in _DROP.findall(path.read_text(encoding="utf-8"))]


def test_the_drop_script_drops_exactly_the_retirement_list_in_order() -> None:
    assert _statements(DROPS) == list(DROP_ORDER)


def test_the_drop_script_names_no_kept_object() -> None:
    """Whole-name comparison. A substring check would call se_company_person_role a hit on
    the entity's se_company_person_rule, or -- written the other way round -- would call
    company_person_role_type unsafe because company_person_role is being dropped."""
    dropped = {name for _, name in _statements(DROPS)}
    assert dropped.isdisjoint(KEPT)
    assert len(dropped) == len(DROP_ORDER)
    assert "se_company_person_v2" not in dropped
    assert "se_company_person" in dropped


def test_the_three_source_views_are_the_only_drop_views() -> None:
    """se_company_person_bolagsverket/_esef/_wikidata are plain VIEWs (000330, replaced in
    place by 000331). Everything else on the list is a table, including the dbt-built
    company_management_current -- confirm each engine in the precheck before running."""
    views = [name for kind, name in _statements(DROPS) if kind == "VIEW"]
    assert views == [
        "se_company_person_bolagsverket",
        "se_company_person_esef",
        "se_company_person_wikidata",
    ]


def test_no_script_uses_sync() -> None:
    """SYNC turns each drop into a blocking wait for a full data removal. The owner runs the
    file as one pipe; the default async drop is what the 480-second UNDROP window is
    measured against."""
    for path in (DROPS, PRECHECK, POSTCHECK):
        assert " SYNC" not in path.read_text(encoding="utf-8").upper(), path.name


def test_the_precheck_and_postcheck_cover_the_same_objects() -> None:
    names = [name for _, name in DROP_ORDER]
    for path in (PRECHECK, POSTCHECK):
        sql = path.read_text(encoding="utf-8")
        for name in names:
            assert f"'{name}'" in sql, f"{path.name} does not cover {name}"


def test_the_precheck_reports_the_engine_and_the_row_count() -> None:
    """The engine tells the owner whether a name is a table or a plain view before the drop
    statement assumes it; the row count is what goes in the ledger. se_company_person_v1_role_baseline
    is asset-created and may not exist at all -- its absence from the engine listing is fine."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "engine" in sql
    assert "total_rows" in sql
    assert "FROM system.tables" in sql


def test_the_precheck_counts_the_three_plain_views_itself() -> None:
    """system.tables.total_rows is NULL for a plain View, so the three source views would be
    the objects whose size the ledger could not record. They get their own SELECT count()."""
    sql = PRECHECK.read_text(encoding="utf-8")
    for view in ("bolagsverket", "esef", "wikidata"):
        assert f"SELECT count() AS se_company_person_{view}_rows" in sql
        assert f"FROM corpscout.se_company_person_{view}" in sql


def test_the_precheck_gates_on_the_serving_view_being_repointed() -> None:
    """The one reader that must be off the old tables before they go. Migration 000395 does
    that; this gate proves it landed."""
    sql = PRECHECK.read_text(encoding="utf-8")
    assert "se_company_person_v2" in sql
    assert "system.view_refreshes" in sql


def test_the_postcheck_asserts_absence_rather_than_reporting_it() -> None:
    sql = POSTCHECK.read_text(encoding="utf-8")
    assert "count() = 0 AS all_dropped" in sql
    assert "groupArray(name) AS still_present" in sql
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_se_person_retirement_drops.py -q
```

Expected: every test errors with `FileNotFoundError` on `corpscout/clickhouse/operations/se_person_retirement_drops.sql`.

- [ ] **Step 3: Write the precheck**

Create `corpscout/clickhouse/operations/se_person_retirement_precheck.sql`:

```sql
-- SE person slice 0 precheck. Run BEFORE se_person_retirement_drops.sql, record the output
-- in the ledger, and check the four gates below.
--
-- Gate 1: nothing left in ClickHouse reads any of these. The match is on a FROM or a JOIN,
-- so a provenance string literal in a view body cannot hold the gate open, and the trailing
-- character class stops corpscout.se_company_person_v2 and the entity's five other tables
-- from matching the se_company_person alternative -- the alternatives are ordered
-- longest-first for the same reason.
SELECT count() = 0 AS no_readers, groupArray(name) AS readers
FROM system.tables
WHERE database = 'corpscout'
  AND engine IN ('View', 'MaterializedView')
  AND name NOT IN (
      'se_company_person_bolagsverket', 'se_company_person_esef',
      'se_company_person_wikidata', 'company_management_current',
      'company_management_observations', 'se_company_person_collision_candidate',
      'se_company_person_enrichment_observation', 'se_company_person_correction',
      'se_company_person_role_draft', 'se_company_person_role',
      'se_company_person_v1_role_baseline', 'se_company_person'
  )
  AND match(create_table_query,
      '(FROM|JOIN)\\s+(corpscout\\.)?(se_company_person_enrichment_observation|se_company_person_collision_candidate|company_management_observations|se_company_person_v1_role_baseline|se_company_person_bolagsverket|se_company_person_role_draft|se_company_person_correction|se_company_person_wikidata|company_management_current|se_company_person_esef|se_company_person_role|se_company_person)([^_a-zA-Z0-9]|$)');

-- Gate 2: engine and size of every object about to go. se_company_person_v1_role_baseline
-- was created by an asset, not a migration, and may simply not exist -- a missing row here
-- is expected and the DROP is written IF EXISTS. total_rows is NULL for the three plain
-- Views, which is correct and is why Gate 2b exists.
SELECT
    name,
    engine,
    total_rows,
    formatReadableSize(total_bytes) AS size
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_person_bolagsverket', 'se_company_person_esef',
      'se_company_person_wikidata', 'company_management_current',
      'company_management_observations', 'se_company_person_collision_candidate',
      'se_company_person_enrichment_observation', 'se_company_person_correction',
      'se_company_person_role_draft', 'se_company_person_role',
      'se_company_person_v1_role_baseline', 'se_company_person'
  )
ORDER BY name;

-- Gate 2b: the three row counts Gate 2 cannot give, so the ledger records a real number for
-- every object destroyed.
SELECT count() AS se_company_person_bolagsverket_rows FROM corpscout.se_company_person_bolagsverket;

SELECT count() AS se_company_person_esef_rows FROM corpscout.se_company_person_esef;

SELECT count() AS se_company_person_wikidata_rows FROM corpscout.se_company_person_wikidata;

-- Gate 3: the serving view already reads the new entity, and it is healthy at its last
-- refresh. If has_new_person_table is 0, migration 000395 has not been applied here and the
-- drops must not run: se_companies_serving would start failing on its next refresh.
SELECT
    countIf(position(create_table_query, 'se_company_person_v2') > 0) > 0 AS has_new_person_table,
    countIf(match(create_table_query, 'se_company_person_role([^_a-zA-Z0-9]|$)') ) = 0 AS no_old_role_table
FROM system.tables
WHERE database = 'corpscout' AND name = 'se_companies_serving';

SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';

-- Gate 4: the entity's own six tables exist, so the drops cannot be run against a database
-- the migration never reached. Expect 6.
SELECT count() AS entity_tables_present, groupArray(name) AS present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_person_suggestion', 'se_company_person_normalized', 'se_company_person_v2',
      'se_company_person_history', 'se_company_person_rule', 'se_company_person_precedence'
  );
```

- [ ] **Step 4: Write the drop script**

Create `corpscout/clickhouse/operations/se_person_retirement_drops.sql`:

```sql
-- SE person slice 0: the 2026-08-19 people model leaves ClickHouse. OWNER-RUN, by hand,
-- after the slice-0 dagster deploy is live, migration 000395 is applied, and
-- se_person_retirement_precheck.sql is clean. Nothing in this repo executes this file
-- (dev-phase ledger policy, owner ruling 2026-08-25: a drop whose gate cannot be checked at
-- write time never goes in the ledger).
--
-- ORDER MATTERS. The three source views go first: every retired asset read them, and they
-- read only KEPT raw tables, so nothing is left dangling. company_management_current follows
-- its dbt model out, with company_management_observations -- its history twin, written by
-- the same contract and read by nothing -- immediately after. Then the five tables that key
-- off the person table, then the asset-made baseline, then se_company_person itself.
--
-- NO SYNC, deliberately. An async drop is what leaves the roughly 480-second UNDROP window
-- these drops are gated on. If one turns out to be wrong, run
-- UNDROP TABLE corpscout.<name> inside that window. UNDROP works for tables, not for views
-- -- the three views are recreated, if it ever comes to that, from migration 000331.
--
-- se_company_person_v1_role_baseline was created by an asset rather than a migration and
-- may not exist at all, which is why every statement is IF EXISTS.
--
-- KEPT FOR GOOD, and absent from this file by construction (a test asserts it):
-- the entity's six tables -- corpscout.se_company_person_suggestion, _normalized, _v2,
-- _history, _rule, _precedence, whose names se_company_person merely prefixes --
-- corpscout.company_person_role_type (the role catalog, shared with Serbia, which
-- company_person_role only prefixes) and the raw sources se_financial_report_signatories,
-- esef_document_people, wikidata_company_people, wikidata_persons and
-- wikidata_company_identifiers.

DROP VIEW IF EXISTS corpscout.se_company_person_bolagsverket;
DROP VIEW IF EXISTS corpscout.se_company_person_esef;
DROP VIEW IF EXISTS corpscout.se_company_person_wikidata;
DROP TABLE IF EXISTS corpscout.company_management_current;
DROP TABLE IF EXISTS corpscout.company_management_observations;
DROP TABLE IF EXISTS corpscout.se_company_person_collision_candidate;
DROP TABLE IF EXISTS corpscout.se_company_person_enrichment_observation;
DROP TABLE IF EXISTS corpscout.se_company_person_correction;
DROP TABLE IF EXISTS corpscout.se_company_person_role_draft;
DROP TABLE IF EXISTS corpscout.se_company_person_role;
DROP TABLE IF EXISTS corpscout.se_company_person_v1_role_baseline;
DROP TABLE IF EXISTS corpscout.se_company_person;
```

- [ ] **Step 5: Write the postcheck**

Create `corpscout/clickhouse/operations/se_person_retirement_postcheck.sql`:

```sql
-- SE person slice 0 postcheck. Run immediately after se_person_retirement_drops.sql.
-- all_dropped must be 1 and still_present must be empty. If it is not, the UNDROP window
-- has NOT been spent on anything -- re-run the drop for the names listed.
SELECT
    count() = 0 AS all_dropped,
    groupArray(name) AS still_present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_person_bolagsverket', 'se_company_person_esef',
      'se_company_person_wikidata', 'company_management_current',
      'company_management_observations', 'se_company_person_collision_candidate',
      'se_company_person_enrichment_observation', 'se_company_person_correction',
      'se_company_person_role_draft', 'se_company_person_role',
      'se_company_person_v1_role_baseline', 'se_company_person'
  );

-- The kept objects are all still there -- the point of the whole-name matching. Expect 12
-- (the entity's six, the role catalog, the five raw sources). The serving view is checked
-- separately below because it is a view, not a table row here.
SELECT count() AS kept_present, groupArray(name) AS kept
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_person_suggestion', 'se_company_person_normalized', 'se_company_person_v2',
      'se_company_person_history', 'se_company_person_rule', 'se_company_person_precedence',
      'company_person_role_type', 'se_financial_report_signatories', 'esef_document_people',
      'wikidata_company_people', 'wikidata_persons', 'wikidata_company_identifiers'
  );

-- And the serving view still refreshes. Re-run after the next :45. Every people flag reads
-- se_company_person_v2, which is empty until slice 2's first fold, so has_people is 0
-- everywhere -- that is the expected reading, not a failure.
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';

SELECT count() AS serving_rows, countIf(has_people = 1) AS with_people
FROM corpscout.se_companies_serving;
```

- [ ] **Step 6: Run the test**

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_se_person_retirement_drops.py -q
```

Expected: 9 passed.

- [ ] **Step 7: Commit**

```bash
git add corpscout/clickhouse/operations/se_person_retirement_precheck.sql \
        corpscout/clickhouse/operations/se_person_retirement_drops.sql \
        corpscout/clickhouse/operations/se_person_retirement_postcheck.sql \
        corpscout/services/dagster_v3/tests/test_se_person_retirement_drops.py
git commit -m "$(cat <<'EOF'
chore(se-person): the slice-0 drop scripts, order-pinned

Precheck, twelve drops in dependency order, postcheck, in
corpscout/clickhouse/operations beside the ledger they retire from. Owner-run;
the test pins the order against the spec's retirement list and asserts no kept
object is named, matching whole names because se_company_person prefixes both
five of the drops and all six tables the entity keeps.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 8: The dropped objects' DDL leaves the ledger

**Files:**
- Modify (emptied, up and down): `corpscout/clickhouse/migrations/000288_corpscout_se_company_person_draft`, `000291_corpscout_se_company_person`, `000292_corpscout_se_company_person_roles`, `000293_corpscout_se_company_person_roles_by_year`, `000295_corpscout_se_company_person_corrections`, `000296_corpscout_se_company_person_correction_writer_grants`, `000330_corpscout_se_company_person_views`, `000331_corpscout_se_company_person_views_observed_at`
- Modify (one statement out): `000267_corpscout_company_serving_tables`, `000289_corpscout_company_person_semantic_hashes`, `000290_corpscout_company_person_source_observations_and_role_types`
- Modify (conditional): `000395_corpscout_esef_country_agnostic_products` — the other session's migration, if it is on main at merge time
- Modify: `tests/test_clickhouse_migrations.py` (`EMPTIED_MIGRATIONS`, two new guards, three tests deleted or narrowed)

**Interfaces:**
- Consumes: nothing from earlier tasks (this task is files and tests only). It must run **after** Tasks 4 and 6, whose deleted tests read some of this DDL.
- Produces: eight new names in `EMPTIED_MIGRATIONS`; the guards `test_the_slice_0_migrations_are_emptied_and_registered` and `test_no_up_migration_declares_a_slice_0_dropped_object`, and the tuples `SLICE_0_EMPTIED` and `SLICE_0_DROPPED_OBJECTS` they read. `EXPECTED_MIGRATIONS` is unchanged by this task (Task 1 already added the one new name).

**Three objects join this edit although this slice does not drop them** (the 4c precedent, where the legacy geocode pair's DDL left the ledger long after its own hand-drop): `se_company_person_draft` and `se_company_person_draft_legacy` were dropped by migrations 000332 and 000328 on 2026-08-27, and `company_person_role` by 000292 on 2026-08-19. All three are declared by 000288, 000289 or 000290 — the same files this slice edits anyway — and leaving their DDL in place would fail the second guard. `company_person_role_type` is a different object and **stays**: whole-name matching is what tells them apart.

**A full replay of the ledger was already broken before this slice, and a replay failure is not this slice's regression.** 000332's up drops `se_company_person_draft` although 000288/000290 no longer create it, exactly as 000298 grants on a table 000297 no longer creates. Production is forward-only and nothing replays the ledger; the requirement on an edited file is that it still parses as a whole, which is what `tests/test_clickhouse_migrations.py` checks by reading every file.

- [ ] **Step 1: Write the failing ledger pins**

Append to `tests/test_clickhouse_migrations.py`:

```python
SLICE_0_EMPTIED = (
    "000288_corpscout_se_company_person_draft",
    "000291_corpscout_se_company_person",
    "000292_corpscout_se_company_person_roles",
    "000293_corpscout_se_company_person_roles_by_year",
    "000295_corpscout_se_company_person_corrections",
    "000296_corpscout_se_company_person_correction_writer_grants",
    "000330_corpscout_se_company_person_views",
    "000331_corpscout_se_company_person_views_observed_at",
)

# Every name whose DDL leaves the ledger in person slice 0: the twelve the owner-run script
# drops, plus se_company_person_draft, se_company_person_draft_legacy and company_person_role,
# dropped by migrations back in August and only losing their DDL now. WHOLE-NAME matching
# only: se_company_person prefixes the six tables the entity KEEPS, and company_person_role
# prefixes company_person_role_type, the catalog that stays.
SLICE_0_DROPPED_OBJECTS = (
    "se_company_person",
    "se_company_person_role",
    "se_company_person_role_draft",
    "se_company_person_correction",
    "se_company_person_enrichment_observation",
    "se_company_person_collision_candidate",
    "se_company_person_v1_role_baseline",
    "se_company_person_bolagsverket",
    "se_company_person_esef",
    "se_company_person_wikidata",
    "se_company_person_draft",
    "se_company_person_draft_legacy",
    "company_person_role",
    "company_management_current",
    "company_management_observations",
)

SLICE_0_KEPT_OBJECTS = (
    "se_company_person_suggestion",
    "se_company_person_normalized",
    "se_company_person_v2",
    "se_company_person_history",
    "se_company_person_rule",
    "se_company_person_precedence",
    "company_person_role_type",
)


def test_the_slice_0_migrations_are_emptied_and_registered() -> None:
    """Dev-phase ledger policy: an object dropped by hand loses its DDL from the file that
    declared it, and the file stays for history with the database statement alone."""
    for migration in SLICE_0_EMPTIED:
        assert migration in EMPTIED_MIGRATIONS, migration
        for suffix in (".up.sql", ".down.sql"):
            sql = _migration_sql(f"{migration}{suffix}")
            assert _statement_lines(sql) == ["CREATE DATABASE IF NOT EXISTS corpscout;"], (
                f"{migration}{suffix}"
            )
            assert "dropped by hand" in sql, f"{migration}{suffix} lost its removal comment"


def test_no_up_migration_declares_a_slice_0_dropped_object() -> None:
    """No CREATE and no later ALTER may still build one of them on the way UP.

    UP FILES ONLY, deliberately: 000328's and 000332's DOWN files recreate the draft tables
    they dropped, which is the history of those drops and is left alone. A DROP is history of
    a drop, not a declaration, so 000292's, 000328's and 000332's up files pass by
    construction -- the pattern matches only declarations.

    Whole names: `company_person_role_type` must NOT trip on `company_person_role`, and the
    entity's own six tables must not trip on `se_company_person`. The regex's `(\\w+)` group
    captures the full identifier, so the comparison below is already whole-name; the kept
    list is asserted alongside to keep it that way if anyone rewrites the pattern.
    """
    pattern = re.compile(
        r"(CREATE TABLE IF NOT EXISTS|CREATE TABLE|CREATE OR REPLACE VIEW"
        r"|CREATE VIEW IF NOT EXISTS|CREATE MATERIALIZED VIEW|ALTER TABLE)\s+(?:corpscout\.)?(\w+)",
        re.IGNORECASE,
    )
    declared: set[str] = set()
    for path in sorted(MIGRATIONS_DIR.glob("*.up.sql")):
        for _, name in pattern.findall(path.read_text(encoding="utf-8")):
            assert name not in SLICE_0_DROPPED_OBJECTS, f"{path.name} declares {name}"
            declared.add(name)
    for kept in SLICE_0_KEPT_OBJECTS:
        assert kept in declared, f"no migration declares the kept object {kept}"
```

Note the pattern includes bare `CREATE TABLE` as well as `CREATE TABLE IF NOT EXISTS`: 000290 and 000293 use the bare form.

- [ ] **Step 2: Run them and watch them fail**

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py -q \
  -k "slice_0_migrations_are_emptied or no_up_migration_declares_a_slice_0"
```

Expected: two failures — the eight names are not in `EMPTIED_MIGRATIONS`, and 000288 (among others) still declares `se_company_person_draft`.

- [ ] **Step 3: Empty the eight files**

Every one of the sixteen files becomes exactly this, byte for byte (five comment lines, one statement, no semicolon inside a comment, a statement last):

```sql
-- SE person slice 0 (2026-09-09): this migration's objects -- the 2026-08-19 people model
-- (the person draft and its legacy twin, the resolved person table, the role and role-draft
-- tables, the correction ledger and enrichment observations with their writer grants, the
-- collision-candidate table and the three per-source read views) -- were dropped by hand on
-- the server and their DDL left this file per the dev-phase ledger policy. The file stays
-- for history.
CREATE DATABASE IF NOT EXISTS corpscout;
```

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/clickhouse/migrations
cat > /tmp/emptied_person.sql <<'EOF'
-- SE person slice 0 (2026-09-09): this migration's objects -- the 2026-08-19 people model
-- (the person draft and its legacy twin, the resolved person table, the role and role-draft
-- tables, the correction ledger and enrichment observations with their writer grants, the
-- collision-candidate table and the three per-source read views) -- were dropped by hand on
-- the server and their DDL left this file per the dev-phase ledger policy. The file stays
-- for history.
CREATE DATABASE IF NOT EXISTS corpscout;
EOF
for m in 000288_corpscout_se_company_person_draft \
         000291_corpscout_se_company_person \
         000292_corpscout_se_company_person_roles \
         000293_corpscout_se_company_person_roles_by_year \
         000295_corpscout_se_company_person_corrections \
         000296_corpscout_se_company_person_correction_writer_grants \
         000330_corpscout_se_company_person_views \
         000331_corpscout_se_company_person_views_observed_at; do
  cp /tmp/emptied_person.sql "$m.up.sql"
  cp /tmp/emptied_person.sql "$m.down.sql"
done
```

000296 is an access migration, so it sits in `EXPECTED_ACCESS_MIGRATIONS` rather than `EXPECTED_MIGRATIONS` and the four file guards never reach it — it is emptied and registered anyway, because the grants it carried named two tables that no longer exist and a reader of the ledger should not have to work that out. Leave `000241_corpscout_person_correction_writer_role` alone: it creates the role, which stays (spec 3.7), and its grant on the long-dropped `country_person_correction` is house precedent for leaving an access migration's dangling grant in place.

- [ ] **Step 4: Take one statement out of each of the three partial files**

`000267_corpscout_company_serving_tables.up.sql` declares **both** management tables — `company_management_current` at line 111 and `company_management_observations` at line 461 (verified 2026-09-09) — and both are on Task 7's drop list. Delete both `CREATE TABLE IF NOT EXISTS ... ( ... );` blocks and put in place of the first, immediately above the next surviving `CREATE TABLE`:

```sql
-- company_management_current and company_management_observations were dropped by hand in SE
-- person slice 0 (2026-09-09) and their DDL left this file per the dev-phase ledger policy.
-- Every other serving table this migration creates stays.
```

`000267_...down.sql`: delete the matching `DROP TABLE IF EXISTS corpscout.company_management_current;` (line 17) and `DROP TABLE IF EXISTS corpscout.company_management_observations;` (line 3) lines. The file keeps seventeen other serving tables' creates and drops, so it does **not** join `EMPTIED_MIGRATIONS`.

`000289_corpscout_company_person_semantic_hashes.up.sql`: delete the last statement — the `ALTER TABLE corpscout.se_company_person_draft ADD COLUMN IF NOT EXISTS wikidata_person_id ...;` block with its two comment lines above it. `000289_...down.sql`: delete the leading `ALTER TABLE corpscout.se_company_person_draft DROP COLUMN IF EXISTS wikidata_person_id;` statement. The four ALTERs on the KEPT source tables (`wikidata_persons`, `wikidata_company_people`, `esef_document_people`, `se_financial_report_signatories`) stay in both files, so this file does not join `EMPTIED_MIGRATIONS` either. Check afterwards that the up file still ends with a statement, not the comment you just wrote.

`000290_corpscout_company_person_source_observations_and_role_types.up.sql`: delete the `RENAME TABLE corpscout.se_company_person_draft TO corpscout.se_company_person_draft_legacy;` statement and the `CREATE TABLE corpscout.se_company_person_draft ( ... );` block, keeping the `CREATE TABLE corpscout.company_person_role_type ... AS SELECT ... FROM VALUES(...)` block and its seed rows. Add above the surviving create:

```sql
-- The source-observation draft table this migration also created was dropped by migration
-- 000332 on 2026-08-27, and its DDL left this file in SE person slice 0 (2026-09-09) per the
-- dev-phase ledger policy. The role catalog below stays -- Serbia seeds into it too.
```

`000290_...down.sql`: keep `DROP TABLE IF EXISTS corpscout.company_person_role_type;` and delete the draft drop and the rename-back. The file still drops something, so it stays out of `EMPTIED_MIGRATIONS`.

- [ ] **Step 5: Register the eight in `EMPTIED_MIGRATIONS`**

In `tests/test_clickhouse_migrations.py`, extend the existing set (do not reorder or reformat what is there):

```python
    # SE person slice 0 (2026-09-09): the 2026-08-19 people model was dropped by hand and
    # its DDL left these files. 000296 is the writer-grant migration -- its two grants named
    # tables that are gone, so it is emptied even though access migrations usually keep a
    # dangling grant (000241 and 000298 both do). 000267, 000289 and 000290 are NOT here --
    # each still declares something that stays (the other seventeen serving tables, the four
    # source-table hash columns, company_person_role_type) and only lost the statements that
    # named a dropped object.
    "000288_corpscout_se_company_person_draft",
    "000291_corpscout_se_company_person",
    "000292_corpscout_se_company_person_roles",
    "000293_corpscout_se_company_person_roles_by_year",
    "000295_corpscout_se_company_person_corrections",
    "000296_corpscout_se_company_person_correction_writer_grants",
    "000330_corpscout_se_company_person_views",
    "000331_corpscout_se_company_person_views_observed_at",
```

- [ ] **Step 6: Delete or narrow the tests that pinned the removed DDL**

In `tests/test_clickhouse_migrations.py`:

- delete `test_sweden_company_person_draft_and_roles_are_migrated` (line 3491) — it reads 000288, now empty;
- delete `test_company_person_draft_is_reshaped_into_source_observations` (line 3544) — it reads the half of 000290 that just went;
- delete `test_normalized_sweden_company_people_table_is_migrated` (line 3609) — it reads 000291, now empty;
- narrow `test_company_person_source_semantic_hashes_are_migrated` (line 3513): drop `"se_company_person_draft"` from the table-name loop and the two `wikidata_person_id` assertions (one per file), and add a line asserting the removal held:

```python
    assert "corpscout.se_company_person_draft" not in sql
```

- `test_canonical_company_person_role_types_are_seeded` (line 3566) and `test_employee_board_representative_role_is_added` (line 3594) **stay**: both read the half of 000290 and all of 000294 that the role catalog keeps.

- [ ] **Step 7: The conditional 000395 ESEF edit**

If the other session's ESEF migration is on main at merge time (its name begins `000395_` before this slice's is renumbered, and it re-issues `CREATE OR REPLACE VIEW corpscout.se_company_person_esef` alongside renaming `esef_document_people` to `_legacy` and creating `se_esef_document_people`): delete **only** its `se_company_person_esef` statement from both its up and its down file, and put in its place

```sql
-- The se_company_person_esef read view was dropped by hand in SE person slice 0
-- (2026-09-09) and its DDL left this file per the dev-phase ledger policy. This migration's
-- ESEF products -- esef_document_people_legacy and the country-scoped se_esef_document_people
-- view -- are untouched.
```

Leave every other statement in that migration alone; it does not join `EMPTIED_MIGRATIONS`. If it is not on main, skip this step and say so in the report — `test_no_up_migration_declares_a_slice_0_dropped_object` will catch it the moment it lands.

- [ ] **Step 8: Run the ledger suite**

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_company_serving.py \
  tests/test_se_person_retirement_drops.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests -q --ignore=tests/test_se_company_address_extractors_clickhouse_local.py
cd ../backoffice && npx vitest run && npm run typecheck
```

Expected: green apart from the pre-existing failures listed in Global Constraints.

- [ ] **Step 9: Commit**

```bash
git add -u corpscout/clickhouse/migrations corpscout/services/dagster_v3/tests
git commit -m "$(cat <<'EOF'
chore(clickhouse): the retired people model's DDL leaves the ledger

Eight files emptied and registered, three trimmed by one statement each
(000267's company_management_current, 000289's draft ALTER, 000290's draft
create and rename -- the role catalog stays). Two guards pin the emptying and
that no up migration still declares a dropped object, matching whole names
because company_person_role prefixes the catalog that stays.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
EOF
)"
```

---

### Task 9: Prod run (controller)

The code retirement must be **live** before the drops: no deployed asset or page may name a dropped object when it goes. The migration-file edits have no server effect, so they ride along with the deploy.

1. [x] Whole-branch review, then merge `se-person-entity` into main **through a worktree that checks main out** (the main checkout may sit on another branch). If main has taken 000395 in the meantime, renumber first — both migration files, `EXPECTED_MIGRATIONS`, `MIGRATION` in `tests/test_se_companies_serving_mv.py`, and the `migrate force 395` line in the migration header — then re-run `pytest tests/test_clickhouse_migrations.py tests/test_se_companies_serving_mv.py tests/test_se_person_retirement_drops.py -q`. If the ESEF 000395 landed, do Task 8 Step 7 now.
2. [x] Deploy dagster per the worktree deploy recipe (pristine worktree, dbt-state refresh, `.env`); hot-sync the host. Confirm in the UI that the group `se_company_person` holds exactly one asset, `se_company_person_normalize`; that none of the nine retired jobs and no `se_company_person_correction_sensor` are present; and that `dg check defs` was clean on the host.
3. [x] Apply the migration: `make -s -C <deploy-worktree>/corpscout clickhouse-migrate-up-one`. If the client drops mid-file, follow the runbook in `person-design.md` (`SYSTEM START VIEW corpscout.se_companies_serving`, finish the ALTER if it did not land, `migrate force 395`). Verify `SELECT version, dirty FROM corpscout.schema_migrations` reads `395 0` (or the renumbered value).
4. [x] Verify the six tables and the re-point: `SELECT name, engine FROM system.tables WHERE database='corpscout' AND name LIKE 'se_company_person%' ORDER BY name` lists the entity's six beside the old model's five; `SELECT position(create_table_query, 'se_company_person_v2') > 0 FROM system.tables WHERE database='corpscout' AND name='se_companies_serving'` is 1.
5. [x] Wait for the next `:45` and confirm the serving refresh: `SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE database='corpscout' AND view='se_companies_serving'` reports a success with an empty exception; `SELECT count(), countIf(has_people=1), countIf(people_bolagsverket=1), countIf(people_esef=1) FROM corpscout.se_companies_serving` gives about 3.5M rows and **0, 0, 0** — the three people flags read an empty table until slice 2's first fold, which is the expected reading, not a failure.
6. [x] Materialize `se_company_person_normalize` once: it must succeed with `rows = 0` (no raw rows exist until slice 1).
7. [x] Run the precheck and record all four gates in the ledger:
   ```bash
   ssh companycollect "docker exec -i clickhouse-clickhouse-1 clickhouse-client --multiquery" \
     < corpscout/clickhouse/operations/se_person_retirement_precheck.sql
   ```
   Gate 1 must report `no_readers = 1` with an empty array. Gate 2's `engine` column must show `View` for the three `se_company_person_*` source views and a MergeTree family for the rest — if any differs, stop and adjust the statement rather than running the file; a missing `se_company_person_v1_role_baseline` row is expected. Gate 3 must show `has_new_person_table = 1` and `no_old_role_table = 1`. Gate 4 must show `entity_tables_present = 6`. Copy the `(name, engine, total_rows, size)` rows plus Gate 2b's three view counts into the ledger entry.
8. [x] Hand the owner the exact command, to be run with `!` from the main checkout:
   ```bash
   ssh companycollect "docker exec -i clickhouse-clickhouse-1 clickhouse-client --multiquery" \
     < corpscout/clickhouse/operations/se_person_retirement_drops.sql
   ```
   UNDROP is possible for about 480 seconds after each drop (`UNDROP TABLE corpscout.<name>`, tables only — the three views would have to be recreated from 000331 instead, so keep a copy of their `create_table_query` from Gate 1's output before running). Stay at the terminal until the postcheck is clean.
9. [x] Run the postcheck the same way. `all_dropped` must be 1, `still_present` empty, `kept_present` 12, and the serving view still healthy.
10. [x] Record `SELECT count() FROM system.tables WHERE database = 'corpscout' AND NOT startsWith(name, '.inner') AND NOT startsWith(name, '.tmp.inner')` before step 8 and after step 9. The difference is the number of drop-list objects that actually existed, which Gate 2 already listed: twelve if every one was present (delta −12), eleven if `se_company_person_v1_role_baseline` was absent (−11). The six new tables were added back in step 3, before this baseline is taken, so they do not enter the arithmetic. Compute the expected number from Gate 2's row list rather than assuming.
11. [x] Backoffice smoke (local, `npm run dev` on `http://localhost:5183`): the admin companies list loads and its has-people filter returns nothing (the flags are 0 until slice 2); a company's admin area shows nine tabs with no People tab and every remaining tab loads; `/admin/se/people`, `/admin/se/people/pipeline` and `/admin/se/people/stale-corrections` 404; the public company detail page for an SE company renders with **no** Management section and no console error, while an FR company's detail page still renders **its** Management section (the legacy mode Task 6 left alone); the sidebar has no People entries.
12. [x] Record in spec section 9 (slice 0 shipped: the migration number, the six tables, the serving re-point, what was deleted in code, the twelve objects dropped with their recorded row counts, the eight emptied migrations and the three partial ones); archive the ledger under `.superpowers/sdd/2026-09-09-se-company-person-0-tables-normalizer-retirement/`; update memory `se-basic-info-design` (or open a `se-person-entity` memory) with the slice-0 record, the `data`-as-`String`-with-`valid_data` contract and the `role_key` column slice 1's extractors must fill.

---

## Self-review

- **Spec coverage.** Section 3, table by table: 3.1 `se_company_person_suggestion`, 3.2 `_normalized`, 3.3 `_v2`, 3.4 `_history`, 3.5 `_rule`, 3.6 `_precedence` — all six in Task 1's migration, pinned column-for-column by Task 1's DDL test; 3.7's "kept as is" (`company_person_role_type`, the `corpscout_person_correction_writer` role) is enforced by Task 7's `KEPT` tuple, Task 8's `SLICE_0_KEPT_OBJECTS` and the ruling in Task 8 Step 3 to leave 000241 alone. Section 4: 4.1 display spelling, 4.2 identity tokens, 4.3 roles through the three moved maps with passthrough, 4.4 the three statuses — Task 2, with fifty corpus cases; the stored layer and its change rule — Task 3. Section 8's retirement paragraph, item by item: the `defs/company_people/` package with its jobs and the correction sensor — Task 4; the five backoffice routes, their libs, components, writers and tests — Task 5; the SE Management section, `company-sections.server.ts`'s management half and the two dbt models — Task 6 (narrowed to Sweden by controller ruling: the component and the legacy detail mode France renders stay); the tables, the three views and both management tables — Task 7's script, run in Task 9; the migration-file edits under the ledger policy — Task 8; "the serving view is re-pointed at the new tables in the same migration that creates them, so the flags read empty tables until the first fold" — Task 1, verified in Task 9 step 5. Section 9 item 0 is exactly Tasks 1 to 9. Section 10's names: `tables`, `roles`, `normalize_se`, `normalize`, `assets` and the asset `se_company_person_normalize` — Tasks 1 to 3; the modules section 10 also lists (`suggestions`, `bolagsverket`, `esef`, `wikidata`, `precedence`, `fold`, `batch`, `jobs`) belong to slices 1 and 2 and are deliberately absent here, which is why no `jobs.py` is created.
- **Placeholder scan.** Every code step carries its content: the full DDL of all six tables and both migration files' statement lists, `tables.py` and `roles.py` and `normalize_se.py` and `normalize.py` and `assets.py` in full, all fifty corpus lines, every test function with its assertions, all three SQL scripts verbatim, the exact `rm` lists and `rg` verifications for each deletion, the emptied-file body byte for byte and the three partial edits by statement. Four things are deliberately not inlined and each names an exact command or file instead: the 171-line rendered serving-view body (Task 1 Step 4 generates it — inlining it would guarantee drift from the builder the drift pin compares it against), the bodies of the deleted files, the clickhouse-local test (Task 3 Step 5 is a numbered procedure over SQL the same task defines, plus an explicit instruction to copy the address twin's structure), and `person-design.md` (Task 3 Step 6 lists its required sections). The keep-rule pass in Task 6 Step 5 is a command whose output drives the edit, with the 2026-09-09 answer written out beside it.
- **Type consistency.** `tables.NORMALIZED_COLUMNS` has 23 entries and `normalized_row` returns 23 values, asserted in Task 3's first test; `tables.SUGGESTION_COLUMNS` has 18; `RAW_ROW_COLUMNS` has 15 and the three `RAW_*` fixtures in that test have 15. `NormalizeCounts` is positional `companies, pages, rows, ok, partial, no_person` everywhere, and its status keys equal `tables.PARSE_STATUSES` (`ok`, `partial`, `no_person`) so `_normalize_page`'s counter cannot miss a status. `NormalizedPerson`'s field names are identical in the dataclass (Task 2), in every corpus line's `expected` object, and in `normalized_row`'s `values` dict (Task 3). `role_key` is one name in four places: the DDL column on both tables (Task 1), `tables.SUGGESTION_COLUMNS` / `NORMALIZED_COLUMNS` and `RAW_ROW_COLUMNS`, `RawPerson.role_key` with `role_code_for`'s keyword (Task 2), and the corpus lines' `raw` objects. `companies_current.COMPANY_PERSON_TABLE` (Task 1 Step 3) and `tables.QUALIFIED_MAIN_TABLE` (Task 1 Step 7) both resolve to `corpscout.se_company_person_v2`, and Task 7's `KEPT` and Task 8's `SLICE_0_KEPT_OBJECTS` both name it unqualified. `clickhouse_local_command` and `literal` are the names Task 4 creates and the names its `perl` re-point produces in all ten importers.
- **Ordering.** Task 2 must precede Task 4 (the role maps must have a new home before their old modules go). Task 4 must precede Task 8 in the sense that Task 8's emptying of 000290/000291/000292/000293/000295/000330/000331 would fail the person test files Task 4 deletes; likewise Task 6 must precede Task 8's 000267 edit, because `tests/test_company_serving.py` reads that file. Task 3's clickhouse-local test imports the helper from the file Task 4 deletes, which is why Task 4's Step 3 re-points it along with the other nine. Task 7 is independent of Tasks 1 to 6 and could be done first; Task 9 needs all eight.
- **What this slice deliberately leaves standing.** Nothing of the old people model. `corpscout.company_management_observations` (declared by 000267 alongside `company_management_current`) loses its writer in Task 6 and is dropped beside it in Task 7 — controller ruling 2026-09-09, so no orphan is left behind. What stays is what the spec keeps: `company_person_role_type` (seeded for Serbia too), the `corpscout_person_correction_writer` role created by 000241, the five raw source tables the slice-1 extractors will read, and — outside Sweden — `management-section.tsx` with the legacy detail mode France and the other countries render.
