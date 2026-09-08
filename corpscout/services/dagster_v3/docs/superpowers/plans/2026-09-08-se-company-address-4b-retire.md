# SE Company Address Slice 4b: Rename and Retire — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the address entity its final name (`corpscout.se_company_address`) in one migration, then delete the whole old address chain — its writers, jobs, schedules, sensor, checks, freshness leaves and dead backoffice — so that nothing but slice 4c's owner-run drops is left of the model that shipped on 2026-08-24.

**Architecture:** One migration does the rename: with the serving view stopped, one `RENAME TABLE` swaps `se_company_address` to `se_company_address_legacy` and `se_company_address_v2` into its place, one `ALTER TABLE ... MODIFY QUERY` re-points `se_companies_serving` at the new name (proven on prod 2026-09-08: MODIFY QUERY keeps the view's data and the next refresh uses the new query — no staged `_next` swap, no `SYSTEM WAIT VIEW`), and `SYSTEM START VIEW` restarts the refresh loop. Every module that names the table does so through exactly one constant, so the code half of the rename is one edit per module. The retirement is then four deletions that go in dependency order — the `se_company` publisher and its two per-source artifacts, then the `sweden_company` register-address publish and the canonical/shared/demand/resolution/store chain the geocoder used to take its demand from, then the weekly job's selection, then the backoffice's dead Address tab and its correction queue. Nothing here drops a ClickHouse object: the drop list is handed to slice 4c.

**Tech Stack:** Python 3.14 / Dagster 1.13.9 (`uv run --frozen --no-sync`), ClickHouse 26.5 (migrations via golang-migrate, `make -s -C corpscout clickhouse-migrate-up-one`), dbt (two projects: `company_serving`, `company_domain_suggestions`), React Router v7 backoffice (vitest, `npm run typecheck`).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`, sections 3.3 (the rename), 6 (the warm step), 9 (slice 4 cutover: reader switch, retirement, the `se_company_address_v2_weekly` rename), 10 (names). Predecessor plan: `2026-09-08-se-company-address-4a-readers.md` (its readers switched; this slice renames what they read and stops what they left behind).

## Global Constraints

- Dagster commands from `corpscout/services/dagster_v3` with `uv run --frozen --no-sync ...` and `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix`; `uv run --frozen --no-sync dg check defs` before every commit touching `src/`. For the two edited dbt projects run `uv run --frozen --no-sync dbt parse --project-dir <project> --profiles-dir <project>` (company_serving: `src/dagster_v3/defs/company_serving/dbt`; company_domain_suggestions: `src/dagster_v3/defs/company_domain_suggestions/dbt`), then `dg check defs`, which loads the dbt manifests.
- Backoffice commands from `corpscout/services/backoffice`: `npx vitest run <files>` and `npm run typecheck`.
- **The new name is a prefix of five other table names.** `corpscout.se_company_address` is a prefix of `corpscout.se_company_address_suggestion`, `_normalized`, `_history`, `_rule` and `_precedence`. Every string match on the table — a test assertion, a fake ClickHouse client's dispatch branch, an `rg` check — must carry the alias or a trailing token (`corpscout.se_company_address AS a FINAL`, `corpscout.se_company_address AS m FINAL`), never the bare name. A dispatch branch written as `sql.includes("FROM corpscout.se_company_address")` would answer the normalized query with the main table's rows.
- **Nothing in this slice drops a ClickHouse object, and no historical migration file is edited** (ledger policy, memory `clickhouse-ledger-squash-planned`). Migration `000384` keeps declaring the entity's main table under its build name `se_company_address_v2`; the deployed table is renamed by `000393`. Every test that reads the DDL out of `000384` therefore keeps naming `se_company_address_v2` as the *DDL* name and replays the rename itself.
- **Migration numbers collide.** Other sessions merge to main daily (000391 was taken while 4a was in flight). Number this one 000393; the controller renumbers it at merge if main has taken it, which means renaming both files, `EXPECTED_MIGRATIONS`, `MIGRATION` in `tests/test_se_companies_serving_mv.py` and the `migrate force 393` line in the migration header.
- **No semicolon may appear inside a `--` comment in any migration file** (`test_clickhouse_migration_line_comments_do_not_contain_semicolons`), and the file must end with a statement, not prose (`test_every_migration_ends_with_a_statement_not_a_comment`). Write the recovery note as "run SYSTEM START VIEW corpscout.se_companies_serving by hand" with no trailing semicolon.
- Never `from __future__ import annotations` in a module that defines Dagster assets.
- Delete a Python module only when `rg` shows no importer outside the deleted set; otherwise delete the asset functions and keep the module. Every helper the new chain imports stays: `address_resolution_shadow.ensure_reference_documents` / `ensure_reference_postings` / `replace_reference_postings` / `reference_postings_key` / `INDEX_SCOPE` / the two qualified reference-table constants, `address_resolution/resolution.py` and `address_resolution/search_documents.py`, `geocode_store`, `geocode_demand.QUERY_BATCH_SIZE`, `geocode_serving_overlay.GEOCODE_FALLBACK_PROVIDER`, `centroid_keys`, `centroid_assets`, `shared_addresses`, `address_canonicalization`, `osm_tables`.
- Commit by explicit path only; trailers in this order at the end of every commit message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- Pre-existing unrelated failures, not regressions of this slice: `tests/test_se_company_address_extractors_clickhouse_local.py` (broken since main's 000390 — the fixture lacks `se_ratsit_company_translated`), `tests/test_schedule_cron_contracts.py` (four `(minute, hour)` collisions), `tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values`; backoffice `admin-se-company-esef.test.tsx` and the live-ClickHouse timeouts under the full suite.

## The rename, in one table

| what | before 4b | after 4b |
| --- | --- | --- |
| the entity's main table | `corpscout.se_company_address_v2` | `corpscout.se_company_address` |
| the old final table | `corpscout.se_company_address` | `corpscout.se_company_address_legacy` (dropped in 4c) |
| `se_company/address/tables.py` | `MAIN_TABLE = "se_company_address_v2"` | `MAIN_TABLE = "se_company_address"` |
| `sweden_company/companies_current.py` | `COMPANY_ADDRESS_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_address_v2"` | `... .se_company_address` |
| dbt source (both projects) | `source('corpscout', 'se_company_address_v2')` | `source('corpscout', 'se_company_address')` |
| backoffice | one `const` per module, four copies | one shared `app/lib/se-address-tables.ts` |
| the extract schedule | `se_company_address_v2_weekly` | `se_company_address_weekly` (STOPPED) |

---

### Task 1: Migration 000393 renames the tables and re-points the serving view

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/companies_current.py:81` (`COMPANY_ADDRESS_TABLE` and the comment above it)
- Create: `corpscout/clickhouse/migrations/000393_corpscout_se_company_address_rename.up.sql`, `...down.sql`
- Modify: `tests/test_se_companies_serving_mv.py` (the drift pin moves to 000393's MODIFY QUERY body)
- Modify: `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`, after `"000392_corpscout_se_companies_serving_address_entity"`)
- Modify: `tests/test_se_companies_serving_sql.py:396,448` (the clickhouse-local fixture creates and fills the table under its new name)
- Modify: `src/dagster_v3/defs/se_company/address/docs/address-design.md` (a slice-4b section with the interrupted-rename runbook)

**Interfaces:**
- Consumes: `companies_current.build_se_companies_serving_sql() -> str` (unchanged signature; its render changes because the constant it interpolates changes), `tests/se_company_ddl.table_block(table: str) -> str`.
- Produces: migration name `000393_corpscout_se_company_address_rename`; `COMPANY_ADDRESS_TABLE == "corpscout.se_company_address"`, which Task 2's `tables.QUALIFIED_MAIN_TABLE` must agree with.

- [ ] **Step 1: Point the serving builder at the final name**

In `companies_current.py`, replace the constant and its comment:

```python
# The SE address entity (migration 000384, renamed to its final name by 000393). This
# constant is the one place the view names it.
COMPANY_ADDRESS_TABLE = f"{CLICKHOUSE_DATABASE}.se_company_address"
```

- [ ] **Step 2: Render the new view body into a scratch file**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix \
  uv run --frozen --no-sync python -c \
  "from dagster_v3.defs.sweden_company.companies_current import build_se_companies_serving_sql; print(build_se_companies_serving_sql())" \
  > /tmp/serving_new.sql
```

Extract 000392's body for the down file the same way (it is the `_v2` render, verbatim):

```bash
cd corpscout/services/dagster_v3
uv run --frozen --no-sync python -c "
from pathlib import Path
sql = Path('../../clickhouse/migrations/000392_corpscout_se_companies_serving_address_entity.up.sql').read_text()
create = [s for s in sql.split(';') if 'CREATE MATERIALIZED VIEW' in s][0]
print(create[create.index(chr(10) + 'AS ') + 4:].strip())
" > /tmp/serving_old.sql
```

`/tmp/serving_new.sql` must contain `corpscout.se_company_address AS a FINAL` and no `se_company_address_v2`; `/tmp/serving_old.sql` the reverse. Both end with the `SETTINGS join_algorithm = 'grace_hash,hash', ... max_memory_usage = 12884901888` block — that block is part of the body and must be included in the MODIFY QUERY.

- [ ] **Step 3: Write the up migration**

`000393_corpscout_se_company_address_rename.up.sql` — header comment in 000392's house style, then exactly five statements. Paste `/tmp/serving_new.sql` where the body is marked; do not hand-edit it.

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- THE ADDRESS ENTITY TAKES ITS FINAL NAME (spec 2026-09-06 section 3.3, slice 4b).
-- corpscout.se_company_address_v2 -- built beside the old model since 000384 and the only
-- address table any reader has named since slice 4a -- becomes corpscout.se_company_address,
-- and the old final table of the 2026-08-24 model parks under _legacy until slice 4c drops
-- it. One RENAME TABLE moves both, so there is no instant at which the name resolves to
-- nothing.
--
-- NO STAGED _next SWAP, and that is the difference from 000391 and 000392. Those two
-- REPLACED the serving view's definition, which meant building a second view and swapping
-- names. This migration changes only the TABLE NAME the same definition reads, and ALTER
-- TABLE ... MODIFY QUERY does that in place: the refreshable view keeps the rows it is
-- already serving and its next scheduled refresh (hourly at :45, migration 000366) runs the
-- new query. Proven on prod 2026-09-08 against a scratch refreshable view over a renamed
-- source: MODIFY QUERY after the rename is accepted, the target data survives, the next
-- refresh succeeds. So there is no _next to populate and no SYSTEM WAIT VIEW to sit through.
--
-- THE VIEW IS STOPPED FIRST because between the RENAME and the MODIFY QUERY its stored
-- query names a table that no longer exists. A refresh landing in that window would fail;
-- a stopped view cannot refresh at all.
--
-- IF THE MIGRATE CLIENT DROPS between the STOP and the START, the view is left stopped and
-- serving its last contents at full speed with nothing raising anywhere. Recovery is by
-- hand: check corpscout.se_companies_serving in system.view_refreshes, run SYSTEM START VIEW
-- corpscout.se_companies_serving, then migrate force 393 so the ledger records where the
-- database actually is. The address design doc's runbook section has the full sequence.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

SYSTEM STOP VIEW corpscout.se_companies_serving;

RENAME TABLE
    corpscout.se_company_address TO corpscout.se_company_address_legacy,
    corpscout.se_company_address_v2 TO corpscout.se_company_address;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
<<< the contents of /tmp/serving_new.sql, verbatim >>>;

SYSTEM START VIEW corpscout.se_companies_serving;
```

- [ ] **Step 4: Write the down migration**

`000393_corpscout_se_company_address_rename.down.sql` — the same shape reversed, so the RENAME lands before the query that needs the `_v2` name:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000393: the entity goes back to se_company_address_v2, the 2026-08-24 model's
-- final table comes back out of _legacy under its own name, and the serving view is
-- re-pointed at the _v2 render 000392 deployed. The rename runs BEFORE the MODIFY QUERY
-- here for the same reason it runs after it in the up file: the query must never be set to
-- a name that does not exist yet. Only meaningful while se_company_address_legacy still
-- exists -- after slice 4c's drop, roll forward instead.

SYSTEM STOP VIEW corpscout.se_companies_serving;

RENAME TABLE
    corpscout.se_company_address TO corpscout.se_company_address_v2,
    corpscout.se_company_address_legacy TO corpscout.se_company_address;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
<<< the contents of /tmp/serving_old.sql, verbatim >>>;

SYSTEM START VIEW corpscout.se_companies_serving;
```

- [ ] **Step 5: Rewrite the drift pin**

`tests/test_se_companies_serving_mv.py` — keep `_statements`, `_body`, `_normalized` and `_executable`; replace the constants, the `_embedded_select` helper and the four structural tests. The module docstring says what 000393 changes (the address half's table name, in place, via MODIFY QUERY; no staged swap because the definition is otherwise unchanged).

```python
MIGRATION = "000393_corpscout_se_company_address_rename"
PREVIOUS_MIGRATION = "000392_corpscout_se_companies_serving_address_entity"
VIEW = "corpscout.se_companies_serving"
ENTITY = "corpscout.se_company_address"
ENTITY_V2 = "corpscout.se_company_address_v2"
LEGACY = "corpscout.se_company_address_legacy"


def _sql_of(migration: str, suffix: str) -> str:
    return (MIGRATIONS_DIR / f"{migration}.{suffix}.sql").read_text(encoding="utf-8")


def _sql(suffix: str) -> str:
    return _sql_of(MIGRATION, suffix)


def _modify_query_body(sql: str) -> str:
    """The SELECT an ALTER TABLE ... MODIFY QUERY installs, without its trailing semicolon."""
    [statement] = [s for s in _statements(sql) if "MODIFY QUERY" in s]
    marker = "MODIFY QUERY\n"
    return statement[statement.index(marker) + len(marker) :]


def _previous_view_body(sql: str) -> str:
    """000392's embedded SELECT: the render the down file must restore."""
    [statement] = [s for s in _statements(sql) if "CREATE MATERIALIZED VIEW" in s]
    marker = "\nAS "
    return statement[statement.index(marker) + len(marker) :]


def test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it() -> None:
    assert _normalized(_modify_query_body(_sql("up"))) == _normalized(
        build_se_companies_serving_sql()
    )


def test_the_pin_is_not_vacuous() -> None:
    body = _modify_query_body(_sql("up"))
    assert len(body) > 2000
    assert "groupArray" in body
    assert "primary_geocode_class" in body
    assert f"{ENTITY} AS a FINAL" in body
    assert ENTITY_V2 not in body
    assert "se_address_geocodes_served" not in body
    assert "corpscout.se_company_basic_info AS i FINAL" in body
    # The SETTINGS block travels with the body: a MODIFY QUERY that dropped it would leave
    # the hourly refresh running without the grace-hash join and the external-sort budget.
    assert "SETTINGS join_algorithm = 'grace_hash,hash'" in body
    assert "max_memory_usage = 12884901888" in body


def test_the_up_migration_stops_renames_repoints_and_starts() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 5
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    rename = _body(statements[2])
    assert rename.startswith("RENAME TABLE")
    assert f"{ENTITY} TO {LEGACY}" in rename
    assert f"{ENTITY_V2} TO {ENTITY}" in rename
    assert _body(statements[3]).startswith(f"ALTER TABLE {VIEW}\nMODIFY QUERY\n")
    assert _body(statements[4]) == f"SYSTEM START VIEW {VIEW}"
    # No staged swap: the definition is unchanged apart from the table name it reads.
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    assert "CREATE MATERIALIZED VIEW" not in _sql("up")


def test_neither_file_drops_anything() -> None:
    """Slice 4b renames; slice 4c drops, by hand, under the ledger policy."""
    for suffix in ("up", "down"):
        executable = _executable(_sql(suffix))
        assert "DROP" not in executable.upper(), suffix


def test_the_down_migration_restores_the_v2_render_after_renaming_back() -> None:
    statements = _statements(_sql("down"))

    assert len(statements) == 5
    assert _body(statements[1]) == f"SYSTEM STOP VIEW {VIEW}"
    rename = _body(statements[2])
    assert f"{ENTITY} TO {ENTITY_V2}" in rename
    assert f"{LEGACY} TO {ENTITY}" in rename
    assert _body(statements[4]) == f"SYSTEM START VIEW {VIEW}"
    # The restored query is 000392's, character for character.
    assert _normalized(_modify_query_body(_sql("down"))) == _normalized(
        _previous_view_body(_sql_of(PREVIOUS_MIGRATION, "up"))
    )


def test_the_up_migration_documents_the_interrupted_rename_recovery() -> None:
    up = _sql("up")
    assert "SYSTEM START VIEW" in up
    assert "migrate force 393" in up
```

- [ ] **Step 6: Fix the executable serving suite's fixture**

`tests/test_se_companies_serving_sql.py` — the schema still comes from 000384's DDL, which declares the `_v2` name; the builder now reads the renamed one, so the fixture replays the rename:

```python
        # Migration 000384 declares the entity under its build name; 000393 renames the
        # deployed table and never edits that file, so the local schema renames it here.
        table_block("se_company_address_v2").replace(
            "corpscout.se_company_address_v2", "corpscout.se_company_address"
        ),
```

and the seed insert becomes `f"INSERT INTO corpscout.se_company_address ({ADDRESS_COLUMNS}) VALUES\n"`.

- [ ] **Step 7: Add the migration to the ledger contract**

In `tests/test_clickhouse_migrations.py`, append `"000393_corpscout_se_company_address_rename",` to `EXPECTED_MIGRATIONS`.

- [ ] **Step 8: Document the rename and its recovery**

In `src/dagster_v3/defs/se_company/address/docs/address-design.md`, add a `## Rename and retirement (slice 4b, 2026-09-08)` section after `## Readers (slice 4a, 2026-09-08)`: the table is `corpscout.se_company_address` from migration 000393; the old final is `se_company_address_legacy` until 4c; the migration is a stop/rename/MODIFY QUERY/start, not a staged swap, because only the source name changes. Then replace the `### If a serving swap is interrupted` section's exit paragraph with a `### If the 000393 rename is interrupted` subsection:

1. `SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE database = 'corpscout' AND view = 'se_companies_serving'` — a stopped view still lists here.
2. If the RENAME landed but the MODIFY QUERY did not, run the `ALTER TABLE ... MODIFY QUERY` statement verbatim from the migration file; if neither landed, re-running the whole up file is safe once the view has been started again.
3. `SYSTEM START VIEW corpscout.se_companies_serving`.
4. `migrate force 393`.

- [ ] **Step 9: Run the tests**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_companies_serving_mv.py tests/test_clickhouse_migrations.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_companies_serving_sql.py -q -m integration
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
```

Expected: all pass. The serving-SQL suite needs `clickhouse-local` on PATH; if it is unavailable, say so rather than reporting a pass.

- [ ] **Step 10: Commit**

```bash
git add corpscout/clickhouse/migrations/000393_corpscout_se_company_address_rename.up.sql \
        corpscout/clickhouse/migrations/000393_corpscout_se_company_address_rename.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company/companies_current.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md \
        corpscout/services/dagster_v3/tests/test_se_companies_serving_mv.py \
        corpscout/services/dagster_v3/tests/test_se_companies_serving_sql.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): 000393 renames the address entity to se_company_address"
```

---

### Task 2: The entity's own constant, the dbt sources and the reconciliation count

**Files:**
- Modify: `src/dagster_v3/defs/se_company/address/tables.py:6` (`MAIN_TABLE`), `src/dagster_v3/defs/se_company/address/assets.py:215` (the fold asset's description text)
- Modify: `src/dagster_v3/defs/company_serving/publish.py:328-341` (the `addresses` reconciliation count and its comment)
- Modify: `src/dagster_v3/defs/company_serving/dbt/models/sources.yml:51-53`, `src/dagster_v3/defs/company_serving/dbt/models/company_section_presence_current_build.sql:39`, `src/dagster_v3/defs/company_serving/dbt/models/company_section_item_source_links_build.sql:435`
- Modify: `src/dagster_v3/defs/company_domain_suggestions/dbt/models/sources.yml:10-11`, `src/dagster_v3/defs/company_domain_suggestions/dbt/models/staging/stg_se_company_match_features.sql:123,157`
- Modify: `tests/test_se_company_address_tables.py:39-41,84`, `tests/test_company_serving.py:104-110`, `tests/test_company_domain_suggestions_dbt.py:90-101`, `tests/test_se_company_address_fold_clickhouse_local.py:111-119`
- Modify: `src/dagster_v3/defs/se_company/address/docs/address-design.md` (the names paragraph)

**Interfaces:**
- Consumes: `tables.MAIN_TABLE`, `tables.QUALIFIED_MAIN_TABLE` (every writer in `se_company/address/` already reads the table only through these two — verified: `batch.py`, `fold.py`, `assets.py` carry no literal).
- Produces: `tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_address"`; dbt source key `source('corpscout', 'se_company_address')` for the three models that read it.

- [ ] **Step 1: Write the failing pins**

`tests/test_se_company_address_tables.py` — the DDL name and the deployed name part company, so name them separately:

```python
# Migration 000384 declares the main table under the build name it was created with;
# 000393 renames the DEPLOYED table and, under the ledger policy, does not touch that file.
MAIN_DDL_TABLE = "se_company_address_v2"


def test_main_table_is_one_row_per_company_and_published_address() -> None:
    block = table_block(MAIN_DDL_TABLE)
    assert declared_columns(MAIN_DDL_TABLE) == list(tables.MAIN_COLUMNS)
    ...  # the five shape assertions are unchanged
```

and in `test_column_tuples_agree_with_each_other`:

```python
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_address"
    # The five sibling tables keep names the new one is a PREFIX of, which is why every
    # string match on the main table elsewhere carries an alias.
    assert tables.QUALIFIED_SUGGESTION_TABLE.startswith(tables.QUALIFIED_MAIN_TABLE)
```

`tests/test_company_serving.py` (the reconciliation assertions):

```python
    assert "FROM corpscout.se_company_address AS addresses FINAL" in sql
    assert "se_company_address_v2" not in sql
```

`tests/test_company_domain_suggestions_dbt.py`: swap `"se_company_address_v2"` for `"se_company_address"` in the source list and add `assert "source('corpscout', 'se_company_address_v2')" not in model_sql`.

- [ ] **Step 2: Run them and watch them fail**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_address_tables.py tests/test_company_serving.py tests/test_company_domain_suggestions_dbt.py -q
```

Expected: FAIL on the three renamed expectations.

- [ ] **Step 3: Rename the constant and the dbt source**

`tables.py`:

```python
MAIN_TABLE = "se_company_address"
```

with the module docstring gaining "…pinned against migrations 000382-000387; the main table was built as `se_company_address_v2` and renamed by 000393."

Both `sources.yml`: rename the `se_company_address_v2` entry to `se_company_address` and delete the `se_company_addresses_current` entry (a stale declaration since slice 4a — no model selects it). Both files keep every other entry untouched.

The three dbt models: `{{ source('corpscout', 'se_company_address') }}`. In `stg_se_company_match_features.sql`, also update the line-123 comment that names the retired projection.

`publish.py`: `"FROM corpscout.se_company_address AS addresses FINAL "`, and the comment above it gains "(renamed from `se_company_address_v2` by migration 000393)". Nothing else in `company_serving/` is touched — another session works in that module.

`assets.py:215`: "…buckets into se_company_address: compatible suggestions merge into one …".

- [ ] **Step 4: Replay the rename in the fold's clickhouse-local schema**

`tests/test_se_company_address_fold_clickhouse_local.py`, in `_schema_statements()`:

```python
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        # 000384 declares the main table under its build name; 000393 renames the deployed
        # table without editing that file, so the local schema applies the rename here.
        text = text.replace("corpscout.se_company_address_v2", tables.QUALIFIED_MAIN_TABLE)
```

(`tests/test_se_company_address_normalize_clickhouse_local.py` needs no change: it lists 000384 for the schema but nothing in the normalize path reads the main table.)

- [ ] **Step 5: Run the tests and both dbt parses**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_address_tables.py tests/test_se_company_address_batch.py \
         tests/test_company_serving.py tests/test_company_serving_dbt.py \
         tests/test_company_domain_suggestions_dbt.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_address_fold_clickhouse_local.py -q -m integration
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/company_serving/dbt --profiles-dir src/dagster_v3/defs/company_serving/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/company_domain_suggestions/dbt --profiles-dir src/dagster_v3/defs/company_domain_suggestions/dbt
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
```

Expected: all pass. `rg -n "se_company_address_v2" src/` must now return only `jobs.py`'s schedule name (Task 5) and doc prose.

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/tables.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/assets.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/publish.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/sources.yml \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_section_presence_current_build.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_serving/dbt/models/company_section_item_source_links_build.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_domain_suggestions/dbt/models/sources.yml \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_domain_suggestions/dbt/models/staging/stg_se_company_match_features.sql \
        corpscout/services/dagster_v3/tests/test_se_company_address_tables.py \
        corpscout/services/dagster_v3/tests/test_company_serving.py \
        corpscout/services/dagster_v3/tests/test_company_domain_suggestions_dbt.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_fold_clickhouse_local.py
git commit -m "refactor(dagster): every reader names the address entity se_company_address"
```

---

### Task 3: The old `se_company` publisher and its two per-source artifacts go

**Files:**
- Delete: `src/dagster_v3/defs/se_company/address_legacy.py`, `src/dagster_v3/defs/se_company/address_rules.py`, `src/dagster_v3/defs/se_company/scb.py`, `src/dagster_v3/defs/se_company/bolagsverket.py`
- Delete: `tests/test_se_company_address.py`, `tests/test_se_company_address_rules.py`, `tests/test_se_company_address_scb.py`, `tests/test_se_company_address_bolagsverket.py`, `tests/test_se_company_address_layout.py`, `tests/test_se_company_address_clickhouse_local.py`
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py:229-241` (three leaves), `tests/se_company_ddl.py`, `tests/test_clickhouse_leaf_checks.py`, `tests/test_se_company_basic_info_batch.py:298`, `src/dagster_v3/defs/se_company/info_rules.py:4`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: the asset keys `se_company_address_clickhouse`, `se_company_address_scb_clickhouse`, `se_company_address_bolagsverket_clickhouse`, the jobs `se_company_address_job` / `se_company_address_review_job`, the sensor `se_company_address_correction_sensor` and the schedule `se_company_address_weekly` no longer exist. Task 5 takes that schedule name.
- `tests/se_company_ddl.py` keeps `MIGRATIONS_DIR`, `_migration_for`, `table_block`, `_column_changes`, `declared_columns` and loses `ADDRESS_MIGRATION`, `ENVELOPE`, `address_artifact_tables`, `projection_aliases` (their only callers are the deleted tests — verified by `rg`).

**Why these four modules and not only the assets:** `address_rules.py` is imported by `address_legacy.py` alone; `se_company/scb.py` and `se_company/bolagsverket.py` each contain nothing but their address artifact (the info artifacts that shared them were retired in basic-info slice 4). Do not confuse them with `se_company/address/scb.py` and `se_company/address/bolagsverket.py`, which are the new extractors and stay.

- [ ] **Step 1: Write the failing guard**

In `tests/test_clickhouse_leaf_checks.py`, add a registry-versus-graph guard (the existing spot-check list keeps `sweden_company_*` entries that Task 4 removes; this new test is what makes both prunings checkable):

```python
# Asset keys whose leaves retired with the SE address chain (slice 4b). A leaf whose asset
# no longer exists produces a freshness check that can never go green again.
RETIRED_LEAF_ASSET_KEYS = (
    "se_company_address_clickhouse",
    "se_company_address_scb_clickhouse",
    "se_company_address_bolagsverket_clickhouse",
)


def test_no_leaf_names_a_retired_address_asset() -> None:
    keys = {spec.asset_key for spec in chk.CLICKHOUSE_LEAVES}
    for retired in RETIRED_LEAF_ASSET_KEYS:
        assert retired not in keys, retired


def test_every_leaf_hangs_off_an_asset_that_exists() -> None:
    """A leaf is a promise that some asset publishes that table. When the asset goes and the
    leaf stays, the freshness check keeps firing against a materialization that will never
    happen again -- the failure mode this whole retirement is cleaning up."""
    graph_keys = {key.path[-1] for key in _repo().asset_graph.get_all_asset_keys()}
    for spec in chk.CLICKHOUSE_LEAVES:
        assert spec.asset_key in graph_keys, spec.asset_key
```

Leave `test_registry_covers_the_known_scheduled_leaves` alone here: its spot-check tuple names no `se_company_address*` leaf (only `sweden_*` ones, which Task 4 removes).

- [ ] **Step 2: Run it and watch it fail**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_clickhouse_leaf_checks.py -q
```

Expected: FAIL — `se_company_address_clickhouse` is still in the registry.

- [ ] **Step 3: Delete the modules and their tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
rm src/dagster_v3/defs/se_company/address_legacy.py \
   src/dagster_v3/defs/se_company/address_rules.py \
   src/dagster_v3/defs/se_company/scb.py \
   src/dagster_v3/defs/se_company/bolagsverket.py
rm tests/test_se_company_address.py tests/test_se_company_address_rules.py \
   tests/test_se_company_address_scb.py tests/test_se_company_address_bolagsverket.py \
   tests/test_se_company_address_layout.py tests/test_se_company_address_clickhouse_local.py
```

- [ ] **Step 4: Prune the three leaves**

In `src/dagster_v3/defs/common/clickhouse_checks.py`, delete the comment block and the three `ClickhouseLeaf` entries for `se_company_address_bolagsverket_clickhouse`, `se_company_address_scb_clickhouse` and `se_company_address_clickhouse` (lines 229-241 today).

- [ ] **Step 5: Trim the shared DDL helper and the dangling references**

`tests/se_company_ddl.py`: delete `ADDRESS_MIGRATION`, `ENVELOPE`, `address_artifact_tables()` and `projection_aliases()`, and trim the module docstring to "Shared by the se_company tests: reads the migration DDL, never a registry."

`tests/test_se_company_basic_info_batch.py:298`: the docstring points at the deleted `tests/test_se_company_address.py` — re-point it at `tests/test_se_company_address_batch.py`, which uses the same SimpleNamespace technique.

`src/dagster_v3/defs/se_company/info_rules.py:4`: the docstring names `address_rules.py` and `address_legacy.py` as what stayed; say instead that the address model they belonged to retired in address slice 4b.

- [ ] **Step 6: Run the suite**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_clickhouse_leaf_checks.py tests/test_se_company_common.py \
         tests/test_se_company_address_tables.py tests/test_se_company_basic_info_batch.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
rg -n "address_legacy|address_rules|se_company_address_scb_clickhouse|se_company_address_clickhouse" src tests
```

Expected: tests pass, `dg check defs` clean, and `rg` returns nothing but Task 4's not-yet-edited `sweden_company` comments. `test_every_leaf_hangs_off_an_asset_that_exists` will still fail on the four `sweden_company` leaves — that is Task 4's; note it and move on only if those four are the only failures.

- [ ] **Step 7: Commit**

```bash
git add -u corpscout/services/dagster_v3/src/dagster_v3/defs/se_company \
           corpscout/services/dagster_v3/tests
git add corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py
git commit -m "refactor(dagster): retire the old se_company address publisher and artifacts"
```

---

### Task 4: The `sweden_company` register-address publish, the identity chain and the geocoder's demand side go, and the weekly is trimmed

**Files:**
- Modify: `src/dagster_v3/defs/sweden_company/assets.py:271-302` (delete `sweden_company_addresses_clickhouse`), `:387-398` (its entry in `sweden_company_refresh_job`'s selection), `:409-425` (its entry in `defs`)
- Modify: `src/dagster_v3/defs/sweden_company/clickhouse.py:53-60,563-748` (delete `SwedenAddressPublishResult`, `publish_sweden_company_clickhouse_addresses`, `_append_changed_address_observations`, `_address_change_counts`, `_insert_address_changes`, `_replace_current_address_snapshot`)
- Modify: `src/dagster_v3/defs/sweden_company/tables.py:205-211` (delete `SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS` only)
- Modify: `src/dagster_v3/defs/sweden_company/address_geocoding_assets.py` (the large cut, below)
- Modify: `src/dagster_v3/defs/sweden_company/companies_current_asset.py:36,47` (deps)
- Modify: `src/dagster_v3/defs/se_company/address/warm.py` and `src/dagster_v3/defs/se_company/address/assets.py` (the OSM-snapshot freshness check moves onto the warm asset)
- Delete: `src/dagster_v3/defs/sweden_company/address_resolution_assets.py`, `src/dagster_v3/defs/sweden_company/geocode_legacy_adoption.py`
- Modify: `src/dagster_v3/defs/common/clickhouse_checks.py:198-229` (four leaves out, one in)
- Delete: `tests/test_sweden_company_address_history.py`, `tests/test_sweden_geocode_checks.py`, `tests/test_sweden_geocode_store_append.py`, `tests/test_sweden_geocode_store_clickhouse_local.py`, `tests/test_sweden_geocode_legacy_adoption.py`
- Modify: `tests/test_sweden_company_assets.py`, `tests/test_sweden_company_address_geocoding.py`, `tests/test_se_companies_current_asset.py`, `tests/test_clickhouse_leaf_checks.py`, `tests/test_se_company_address_warm.py` (receives the three freshness tests)
- Modify: `src/dagster_v3/defs/sweden_company/docs/sweden_company-design.md`, `docs/sweden-data-sources.md`

**Interfaces:**
- Consumes: `se_address_geocodes_warm` (the asset key Task 5's weekly keeps and this task's `companies_current_asset` deps on), `CENTROIDS_ASSET_KEY == "sweden_geocode_centroids_clickhouse"`, `COMPANIES_CURRENT_ASSET_KEY == "sweden_companies_current_clickhouse"`.
- Produces: `sweden_company_address_geocoding_weekly_job`'s selection is exactly `("sweden_osm_pbf_s3", "sweden_osm_addresses_duckdb", CENTROIDS_ASSET_KEY, "se_address_geocodes_warm", COMPANIES_CURRENT_ASSET_KEY)`; `sweden_company_refresh_job` keeps its other six assets; the module still exports `defs` holding the centroids asset, the companies-current asset, their two checks, the weekly job and the weekly schedule. New in `se_company/address/warm.py`: `MAX_OSM_SNAPSHOT_AGE`, `SNAPSHOT_FRESHNESS_SQL`, `fetch_osm_snapshot_freshness(client) -> datetime | None`, `osm_snapshot_is_fresh(*, snapshot_at, now) -> bool`; new in `se_company/address/assets.py`: the check `se_address_geocodes_osm_snapshot_freshness_check` under the name `osm_snapshot_fresh` on asset `se_address_geocodes_warm`; new leaf `ClickhouseLeaf("se_address_geocodes_warm", ("se_address_geocodes",), WEEKLY)`.

**Deleted assets (10):** `sweden_company_addresses_clickhouse`, `sweden_company_canonical_addresses_duckdb`, `sweden_company_canonical_addresses_clickhouse`, `sweden_shared_addresses_duckdb`, `sweden_shared_addresses_clickhouse`, `sweden_address_geocode_demand_duckdb`, `sweden_address_geocode_store_clickhouse`, `sweden_address_geocode_store_backfill_clickhouse`, `sweden_address_geocode_legacy_adoption_clickhouse`, plus the four in `address_resolution_assets.py` (`sweden_address_resolution_golden_evaluation`, `..._shadow_duckdb`, `..._current_duckdb`, `..._unmatched_diagnostics_duckdb`).

**Deleted jobs (8):** `sweden_company_address_geocoding_job`, `sweden_shared_address_identity_job`, `sweden_shared_address_geocoding_job`, `sweden_address_geocode_store_backfill_job`, `sweden_address_geocode_legacy_adoption_job`, `sweden_address_resolution_shadow_job`, `sweden_address_resolution_publish_job`, `sweden_address_resolution_diagnostics_job`. Every one of them selects only deleted assets, so each would fail to resolve if it stayed.

**Deleted checks (4), one moved:** deleted are `sweden_shared_addresses_complete_check`, `sweden_address_geocode_store_complete_check`, `sweden_company_address_exact_match_rate_check` and `sweden_address_geocodes_serving_view_refresh_check`. R3 names the rate check and the view-refresh check; the other two hang off `sweden_shared_addresses_clickhouse` and `sweden_address_geocode_store_clickhouse`, which are being deleted — a `@dg.asset_check(asset=...)` cannot outlive its host — and the store-completeness check's coverage term reads `se_addresses_current`, which retires with the identity chain. The fifth, `sweden_company_address_osm_snapshot_freshness_check`, is **moved rather than deleted** (controller ruling, 2026-09-08): the store is still written, by the geocode function from inside the warm step and the fold, so the "coordinates come from a stale OSM extract" warning still has something true to say. It re-hosts on `se_address_geocodes_warm` with the same nine-day threshold, the same WARN severity and the same message, and the store table gets its freshness/row-count leaf back on the same asset.

**Kept, deliberately:** `address_resolution_shadow.py` (the new geocode function imports `ensure_reference_postings`, `INDEX_SCOPE` and the two qualified reference-table constants; `batch.py` imports `ensure_reference_documents`; `tests/test_address_resolution.py` drives `replace_sweden_address_resolution_shadow` and `replace_sweden_address_resolution_unmatched_diagnostics` directly), `geocode_demand.py` (`address/adoption.py` imports `QUERY_BATCH_SIZE`), `address_canonicalization.py` and `shared_addresses.py` (`geocode_serving_overlay` and `geocode_store` read their constants), `geocode_serving_overlay.py` (`GEOCODE_FALLBACK_PROVIDER` is used by `fold.py`, `warm.py`, `geocode.py` and `companies_current.py`; its `se_address_geocodes_served` builder retires with the view in 4c), `sweden_company/tables.py`'s address table-name constants (they are migration 000084's column contract, pinned by `test_sweden_company_registry_migration_covers_exported_columns`).

- [ ] **Step 1: Write the failing tests**

`tests/test_sweden_company_assets.py` — drop `"sweden_company_addresses_clickhouse"` from both sets (the job selection at line 28 and the group/pool loop at line 45) and add:

```python
    assert "sweden_company_addresses_clickhouse" not in asset_keys
```

`tests/test_sweden_company_address_geocoding.py` — replace `test_sweden_company_address_geocoding_assets_are_company_enhancements` wholesale with:

```python
def test_the_weekly_is_the_only_geocoding_job_and_selects_five_assets() -> None:
    """Slice 4b: the canonical/shared/demand/resolution/store chain is gone, and with it
    every job that selected it. What is left is the weekly, which refreshes the OSM extract,
    republishes the centroids, warms the address entity's geocode cache and forces the
    serving view's refresh -- the five steps the entity actually needs."""
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    weekly_job = repo.get_job("sweden_company_address_geocoding_weekly_job")
    schedule = repo.get_schedule_def("sweden_company_address_geocoding_weekly")

    assert {key.path[-1] for key in weekly_job.asset_layer.executable_asset_keys} == {
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        "sweden_geocode_centroids_clickhouse",
        "se_address_geocodes_warm",
        "sweden_companies_current_clickhouse",
    }
    assert schedule.job.name == "sweden_company_address_geocoding_weekly_job"
    assert schedule.cron_schedule == "5 4 * * 2"
    assert schedule.execution_timezone == "Europe/Stockholm"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING

    graph_keys = {key.path[-1] for key in repo.asset_graph.get_all_asset_keys()}
    job_names = {job.name for job in repo.get_all_jobs()}
    for retired in (
        "sweden_company_addresses_clickhouse",
        "sweden_company_canonical_addresses_duckdb",
        "sweden_company_canonical_addresses_clickhouse",
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
        "sweden_address_geocode_demand_duckdb",
        "sweden_address_resolution_golden_evaluation",
        "sweden_address_resolution_shadow_duckdb",
        "sweden_address_resolution_current_duckdb",
        "sweden_address_resolution_unmatched_diagnostics_duckdb",
        "sweden_address_geocode_store_clickhouse",
        "sweden_address_geocode_store_backfill_clickhouse",
        "sweden_address_geocode_legacy_adoption_clickhouse",
    ):
        assert retired not in graph_keys, retired
    for retired_job in (
        "sweden_company_address_geocoding_job",
        "sweden_shared_address_identity_job",
        "sweden_shared_address_geocoding_job",
        "sweden_address_geocode_store_backfill_job",
        "sweden_address_geocode_legacy_adoption_job",
        "sweden_address_resolution_shadow_job",
        "sweden_address_resolution_publish_job",
        "sweden_address_resolution_diagnostics_job",
    ):
        assert retired_job not in job_names, retired_job
```

`tests/test_se_companies_current_asset.py` — add the deps pin:

```python
def test_the_refresh_asset_runs_after_the_centroids_and_the_warm() -> None:
    """The store-append asset it used to wait on is gone; the warm step is what now puts the
    week's new OSM extract into the geocode cache the fold reads, so it is what this must
    follow to force a refresh that reflects the week."""
    from dagster_v3.definitions import defs as load_defs

    node = load_defs().get_repository_def().asset_graph.get(
        dg.AssetKey("sweden_companies_current_clickhouse")
    )
    assert {key.path[-1] for key in node.parent_keys} == {
        "sweden_geocode_centroids_clickhouse",
        "se_address_geocodes_warm",
    }
```

`tests/test_clickhouse_leaf_checks.py` — extend `RETIRED_LEAF_ASSET_KEYS` with `"sweden_company_addresses_clickhouse"`, `"sweden_company_canonical_addresses_clickhouse"`, `"sweden_shared_addresses_clickhouse"`, `"sweden_address_geocode_store_clickhouse"`; delete the three of those the spot-check tuple in `test_registry_covers_the_known_scheduled_leaves` lists (`sweden_address_geocode_store_clickhouse`, `sweden_company_canonical_addresses_clickhouse`, `sweden_shared_addresses_clickhouse` — `sweden_company_addresses_clickhouse` was never spot-checked), delete that test's `members` block (lines 76-86 today, which asserts the canonical leaf's tables and cadence), and pin the store's new host:

```python
def test_the_geocode_cache_is_watched_through_the_warm_asset() -> None:
    """The store-append asset that used to carry this leaf retired in slice 4b, but
    corpscout.se_address_geocodes is still written every week -- by the geocode function,
    from inside the warm step. Moving the leaf keeps the table watched; dropping it would
    have left the entity's only cache with no freshness or row-count check at all."""
    warm = next(
        leaf for leaf in chk.CLICKHOUSE_LEAVES if leaf.asset_key == "se_address_geocodes_warm"
    )
    assert warm.tables == ("se_address_geocodes",)
    assert warm.max_age == chk.WEEKLY
```

`tests/test_se_company_address_warm.py` — pin the check's new host (its three behaviour tests arrive in Step 8):

```python
def test_the_osm_freshness_warn_hangs_off_the_warm_asset() -> None:
    """It used to hang off sweden_address_geocode_store_clickhouse. A check left on a
    retired asset is a check that quietly stops running."""
    assert {key.asset_key for key in
            assets.se_address_geocodes_osm_snapshot_freshness_check.check_keys} == {
        dg.AssetKey("se_address_geocodes_warm")
    }
```

- [ ] **Step 2: Run them and watch them fail**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_sweden_company_assets.py tests/test_se_companies_current_asset.py \
         tests/test_clickhouse_leaf_checks.py tests/test_se_company_address_warm.py -q
```

Expected: FAIL — the assets and leaves are still there, there is no `se_address_geocodes_warm` leaf, and `assets.se_address_geocodes_osm_snapshot_freshness_check` does not exist yet.

- [ ] **Step 3: Delete the register-address publish**

`sweden_company/assets.py`: remove the `sweden_company_addresses_clickhouse` asset (its `@dg.asset` decorator through the `return dg.MaterializeResult(...)`), its line in `sweden_company_refresh_job`'s `AssetSelection.assets(...)` and its line in `defs(assets=[...])`. Remove the now-unused `publish_sweden_company_clickhouse_addresses` import. The job stays: it still feeds `se_scb_companies` and `se_bolagsverket_companies`, which the new extractors read.

`sweden_company/clickhouse.py`: delete the `SwedenAddressPublishResult` dataclass and the five address functions. Keep `_snapshot_join` (four other publishers call it).

`sweden_company/tables.py`: delete `SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS`. Keep `SE_COMPANY_ADDRESS_BASE_COLUMNS`, `COMPANY_ADDRESSES_TABLE_CH`, `COMPANY_ADDRESSES_CURRENT_TABLE_CH` and the two qualified names — `test_sweden_company_registry_migration_covers_exported_columns` pins them against migration 000084, which is still on the ledger.

- [ ] **Step 4: Cut `address_geocoding_assets.py` down to the weekly**

Delete, in file order: the asset-key constants for canonical/shared/demand/resolution/store/backfill/adoption (lines 38-42 and 52-57 today, keeping `GROUP_NAME`, `WEEKLY_CRON_SCHEDULE` and `WEEKLY_EXECUTION_TIMEZONE`); `MAX_SERVING_VIEW_REFRESH_AGE`, `_EPOCH`, `MIN_EXACT_MATCH_RATE_PERCENT`, `MAX_EXACT_MATCH_RATE_CHANGE_PERCENTAGE_POINTS`; the whole retirement-DDL comment block with `LEGACY_PAIR_RETIREMENT_DROP_SQL` and `CANONICAL_RETIREMENT_DROP_SQL` (both already executed on prod); `_GEOCODED_STATUS_LIST`, `_VALID_STATUS_LIST`, `SwedenGeocodeExactMatchStats`, `EXACT_MATCH_RATE_SQL`, `SERVING_VIEW_REFRESH_SQL`, `fetch_sweden_geocode_exact_match_stats`, `exact_match_rate_is_stable`, `serving_view_refresh_is_healthy`, `EXACT_MATCH_RATE_CHECK_NAME`, `EXACT_MATCH_RATE_CHECK_KEY`, `previous_exact_match_rate_percent`; the nine assets and their config classes; `build_store_append_regression_sql`, `epoch_milliseconds`, `build_geocode_store_backfill_sql`, `GEOCODE_STORE_BACKFILL_SQL`, `BACKFILL_PREFLIGHT_SQL`, `BACKFILL_STORE_GUARD_SQL`, `STORE_INVARIANTS_SQL`, `STORE_COVERAGE_SQL`, `_ADOPTED_EXACT_FILTER_SQL`, `ADOPTION_DEMOTION_SQL`; the four deleted checks; the seven non-weekly jobs. Drop every import that is now unused (`dataclass`, `datetime`/`timedelta`, `Any`, `ClickhouseResource`, `DuckDBResource`, the three `clickhouse.resolved` helpers, `osm_tables`, the six `sweden_company` sibling modules, `SWEDEN_ADDRESS_RESOLUTION_POLICY`).

`MAX_OSM_SNAPSHOT_AGE`, `SNAPSHOT_FRESHNESS_SQL`, `fetch_sweden_geocode_snapshot_freshness`, `osm_snapshot_is_fresh` and `sweden_company_address_osm_snapshot_freshness_check` are **cut from this module but not deleted** — Step 4a moves them.

Trim the weekly job's selection and its description:

```python
sweden_company_address_geocoding_weekly_job = dg.define_asset_job(
    name="sweden_company_address_geocoding_weekly_job",
    selection=dg.AssetSelection.assets(
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        CENTROIDS_ASSET_KEY,
        "se_address_geocodes_warm",
        COMPANIES_CURRENT_ASSET_KEY,
    ),
    tags={"country": "SE", "pipeline": "address_geocoding"},
    description=(
        "Refreshes the Sweden Geofabrik snapshot and OSM address index, republishes the "
        "postcode and city centroids, warms the address entity's geocode cache against the "
        "new extract, and forces the companies serving view to refresh."
    ),
)
```

and the schedule's description (delete the sentence about `se_address_geocodes_current`, which no longer has a check behind it here):

```python
    description=(
        "Weekly Sweden OSM snapshot and address index, then the centroids and the address "
        "entity's geocode cache are rebuilt against the new extract and the companies "
        "serving view is refreshed."
    ),
```

`defs` keeps `assets=[sweden_geocode_centroids_clickhouse, sweden_companies_current_clickhouse]`, `asset_checks=[sweden_geocode_centroid_area_sanity_check, sweden_companies_current_refresh_check]`, `jobs=[sweden_company_address_geocoding_weekly_job]`, `schedules=[sweden_company_address_geocoding_weekly]`. Keep the module docstring honest about what it now holds.

- [ ] **Step 4a: Move the OSM-snapshot freshness warn onto the warm asset**

The predicate and its query go to `se_company/address/warm.py`, beside the step that writes the store — it is pure and unit-testable there, exactly as it was in its old home. Widen the datetime import to `from datetime import UTC, datetime, timedelta`, add `QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE` to the existing `geocode_store` import, and append:

```python
# --- OSM snapshot freshness ----------------------------------------------------------------
# Moved here from sweden_company/address_geocoding_assets.py in slice 4b. It hung off the
# store-append asset, which retired with the demand chain; what writes the store now is
# geocode_addresses, called from this warm step and from the fold, so this asset is the
# honest host. Threshold, severity and message are unchanged: nine days is one weekly cycle
# plus slack, and one missed OSM refresh is what it is meant to catch.
MAX_OSM_SNAPSHOT_AGE = timedelta(days=9)

# max(source_snapshot_at) over the WHOLE store rather than a versioned read: the store is
# append-only, so the newest snapshot any outcome was computed against is the newest snapshot
# the matcher has seen, and ranking every identity to learn it would cost a great deal to
# answer the same question.
SNAPSHOT_FRESHNESS_SQL = f"""SELECT max(source_snapshot_at)
FROM {QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE}"""


def fetch_osm_snapshot_freshness(client: Any) -> datetime | None:
    """The newest OSM snapshot any stored outcome was computed against."""
    [(snapshot_at,)] = client.execute(SNAPSHOT_FRESHNESS_SQL)
    return snapshot_at


def osm_snapshot_is_fresh(*, snapshot_at: datetime | None, now: datetime) -> bool:
    """False for an empty store: no outcome has ever been computed, which is not fresh."""
    if snapshot_at is None:
        return False
    normalized_snapshot_at = (
        snapshot_at.replace(tzinfo=UTC)
        if snapshot_at.tzinfo is None
        else snapshot_at.astimezone(UTC)
    )
    return now.astimezone(UTC) - normalized_snapshot_at <= MAX_OSM_SNAPSHOT_AGE
```

The check itself goes to `se_company/address/assets.py`, directly under `se_address_geocodes_warm` (it must reference the asset object). Extend the `warm` import to `from dagster_v3.defs.se_company.address.warm import (CHUNK_SIZE as WARM_CHUNK_SIZE, MAX_OSM_SNAPSHOT_AGE, fetch_osm_snapshot_freshness, osm_snapshot_is_fresh, warm_geocodes)`:

```python
@dg.asset_check(
    asset=se_address_geocodes_warm,
    name="osm_snapshot_fresh",
    description=(
        "Warns when stored Sweden coordinates come from an OSM snapshot over nine "
        "days old."
    ),
)
def se_address_geocodes_osm_snapshot_freshness_check(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    """The one thing still watching the age of the matcher's reference data.

    A stale extract is invisible from every other angle: the cache answers fast, the fold
    publishes rows, and every outcome carries a policy and a reference that agree with each
    other -- they are simply all computed against an OSM snapshot nobody refreshed. WARN,
    because a week-late extract is not a reason to fail a run.
    """
    checked_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        snapshot_at = fetch_osm_snapshot_freshness(client)
    snapshot_age_hours = (
        (checked_at - snapshot_at.astimezone(UTC)).total_seconds() / 3600
        if snapshot_at is not None and snapshot_at.tzinfo is not None
        else None
    )
    return dg.AssetCheckResult(
        passed=osm_snapshot_is_fresh(snapshot_at=snapshot_at, now=checked_at),
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "latest_osm_snapshot_at": (
                snapshot_at.isoformat() if snapshot_at is not None else None
            ),
            "snapshot_age_hours": snapshot_age_hours,
            "maximum_snapshot_age_hours": MAX_OSM_SNAPSHOT_AGE.total_seconds() / 3600,
        },
    )
```

- [ ] **Step 5: Delete the two modules that lost their last importer**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
rm src/dagster_v3/defs/sweden_company/address_resolution_assets.py \
   src/dagster_v3/defs/sweden_company/geocode_legacy_adoption.py
rg -n "address_resolution_assets|geocode_legacy_adoption" src tests
```

Expected from `rg`: only prose (a docstring line in `tests/test_address_resolution.py` naming `address_resolution_assets._evaluate_golden_corpus` — rewrite it to name `evaluate_golden_address_resolution_corpus`, the function it actually mirrors).

- [ ] **Step 6: Re-point the serving-refresh asset**

`companies_current_asset.py`: replace the `GEOCODE_STORE_ASSET_KEY` constant with

```python
WARM_ASSET_KEY = "se_address_geocodes_warm"
```

and the deps with `deps=[dg.AssetKey(CENTROIDS_ASSET_KEY), dg.AssetKey(WARM_ASSET_KEY)]`. Update the module docstring's second paragraph: the freshest inputs are now the centroids and the warm step, and the store-append asset it used to name retired with the old chain.

- [ ] **Step 7: Prune the four leaves and re-host the store's**

In `clickhouse_checks.py` delete the `ClickhouseLeaf` entries for `sweden_company_addresses_clickhouse`, `sweden_company_canonical_addresses_clickhouse` and `sweden_shared_addresses_clickhouse`, with their comment blocks, and replace the `sweden_address_geocode_store_clickhouse` entry with the same table under its new host:

```python
    # The geocode cache. Its old host, the store-append asset, retired with the demand chain
    # in slice 4b; corpscout.se_address_geocodes is now written by geocode_addresses from
    # inside the weekly warm step (and, between weeks, from the fold), so the warm asset is
    # what materializes on the weekly cadence this leaf measures.
    ClickhouseLeaf(
        "se_address_geocodes_warm",
        ("se_address_geocodes",),
        WEEKLY,
    ),
```

Keep the "NO LEAF FOR se_address_geocodes_current" comment as it stands: that name is still a refreshable materialized view with no Dagster writer, and it retires in slice 4c.

- [ ] **Step 8: Delete the tests of deleted code**

```bash
rm tests/test_sweden_company_address_history.py tests/test_sweden_geocode_checks.py \
   tests/test_sweden_geocode_store_append.py tests/test_sweden_geocode_store_clickhouse_local.py \
   tests/test_sweden_geocode_legacy_adoption.py
```

Before deleting `tests/test_sweden_geocode_checks.py`, move its three freshness cases into `tests/test_se_company_address_warm.py`, beside the check's new host. They need a fake that answers an arbitrary statement, which the file's existing `FakeClient` cannot be (it asserts the SQL is `warm.keys_sql(...)`):

```python
class SnapshotClient:
    """Answers the freshness query and records what it was asked."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.executed: list[str] = []

    def execute(self, sql: str, params: Any = None, settings: Any = None) -> list[tuple[Any, ...]]:
        self.executed.append(sql)
        assert sql == warm.SNAPSHOT_FRESHNESS_SQL
        return self.rows


class SnapshotResource:
    def __init__(self, client: Any) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        yield self._client


def test_the_freshness_query_reads_the_store_and_nothing_else() -> None:
    assert warm.SNAPSHOT_FRESHNESS_SQL.strip().startswith("SELECT max(source_snapshot_at)")
    assert QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE in warm.SNAPSHOT_FRESHNESS_SQL
    assert "canonical" not in warm.SNAPSHOT_FRESHNESS_SQL
    assert "se_company_address_geocodes" not in warm.SNAPSHOT_FRESHNESS_SQL


def test_the_freshness_check_reports_the_age_of_the_newest_stored_snapshot() -> None:
    client = SnapshotClient([(datetime(1999, 1, 1, tzinfo=UTC),)])

    result = assets.se_address_geocodes_osm_snapshot_freshness_check.node_def.compute_fn.decorated_fn(
        SnapshotResource(client)
    )

    assert not result.passed
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert client.executed == [warm.SNAPSHOT_FRESHNESS_SQL]
    assert result.metadata["maximum_snapshot_age_hours"] == 216.0


def test_an_empty_store_reports_no_snapshot_rather_than_raising() -> None:
    assert warm.fetch_osm_snapshot_freshness(SnapshotClient([(None,)])) is None
    assert not warm.osm_snapshot_is_fresh(snapshot_at=None, now=datetime.now(UTC))
```

(the file gains `import dagster as dg`, `from contextlib import contextmanager`, `from collections.abc import Iterator`, `from dagster_v3.defs.se_company.address import assets` and `from dagster_v3.defs.sweden_company.geocode_store import QUALIFIED_CLICKHOUSE_GEOCODE_STORE_TABLE`.)

In `tests/test_sweden_company_address_geocoding.py` delete `test_sweden_company_address_geocoding_quality_thresholds` (it imports `exact_match_rate_is_stable` and the old `osm_snapshot_is_fresh`; the freshness half of what it covered moves with the predicate, the rate half retires with the check), `test_the_canonical_publish_carries_only_the_members_bridge` (it monkeypatches the deleted asset) and `test_the_retirement_drops_live_as_pinned_sql_outside_the_ledger` (it pins the deleted constants). Keep the migration-shape tests, the canonicalization and shared-address invariant tests, and the chunked-load tests: they drive modules this slice keeps. Re-point the ledger-policy guard so it keeps its teeth for slice 4c:

```python
def test_no_new_migration_drops_a_slice_4c_retirement() -> None:
    """The gated drops stay out of the ledger (owner ruling 2026-08-25, paid for in UNDROPs).
    Slice 4b renames; slice 4c drops these by hand once their gates hold. Only migrations
    NEWER than 000392 are checked: 000256's own rename-swap legitimately drops the VIEW that
    se_company_addresses_current used to be, and history is not what this guard is about."""
    migrations = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    up_files = [p for p in sorted(migrations.glob("*.up.sql")) if p.name >= "000393"]
    assert up_files, "no migration up files newer than 000392 -- this guard would pass vacuously"
    pattern = re.compile(
        r"drop\s+(?:table|view)\s+(?:if\s+exists\s+)?(?:corpscout\.)?"
        r"(se_company_address_legacy"
        r"|se_company_address_scb"
        r"|se_company_address_bolagsverket"
        r"|se_company_address_correction"
        r"|se_company_addresses"
        r"|se_company_addresses_current"
        r"|se_company_addresses_canonical_current"
        r"|se_company_address_members_current"
        r"|se_addresses_current"
        r"|se_company_address_links_current"
        r"|se_address_geocodes_current"
        r"|se_address_geocodes_served)\b",
        re.IGNORECASE,
    )
    for path in up_files:
        found = pattern.search(path.read_text(encoding="utf-8"))
        assert found is None, f"{path.name} drops {found.group(1)}"
```

- [ ] **Step 9: Update the two docs**

`src/dagster_v3/defs/sweden_company/docs/sweden_company-design.md`: delete the `sweden_company_addresses_clickhouse` row from the publish-asset table (line 133) and the `se_company_addresses` bullet with the `se_company_addresses_current` paragraph (lines 137-139); the DuckDB `company_addresses` table row (line 97) stays — the normalize asset still builds it, and removing that CTE is not this slice's. Add one line: the SE address model is the address entity (`corpscout.se_company_address`), built by `se_company/address`, and this pipeline no longer publishes addresses.

`docs/sweden-data-sources.md`: drop `se_company_addresses 4.40M` from the row-16 table cell, delete the `se_company_addresses` bullet at line 55, and say the addresses of a Swedish company now come from `corpscout.se_company_address`.

- [ ] **Step 10: Run everything this touched**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_sweden_company_assets.py tests/test_sweden_company_clickhouse.py \
         tests/test_sweden_company_code_quality.py tests/test_sweden_company_source_tables.py \
         tests/test_sweden_company_address_geocoding.py tests/test_se_companies_current_asset.py \
         tests/test_se_company_address_warm.py tests/test_se_company_address_assets.py \
         tests/test_clickhouse_leaf_checks.py tests/test_sweden_geocode_store.py \
         tests/test_sweden_geocode_demand.py tests/test_sweden_geocode_store_current_mv.py \
         tests/test_address_resolution.py tests/test_sweden_centroid_assets.py \
         tests/test_geocode_serving_overlay.py tests/test_se_address_geocodes_served_view.py \
         tests/test_clickhouse_migrations.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
```

Expected: all pass, `dg check defs` clean, and `test_every_leaf_hangs_off_an_asset_that_exists` now green.

- [ ] **Step 11: Commit**

```bash
git add -u corpscout/services/dagster_v3/src/dagster_v3/defs/sweden_company \
           corpscout/services/dagster_v3/tests \
           corpscout/services/dagster_v3/docs/sweden-data-sources.md
git add corpscout/services/dagster_v3/src/dagster_v3/defs/common/clickhouse_checks.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/warm.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/assets.py
git commit -m "refactor(dagster): retire the shared-identity address chain and trim the weekly"
```

---

### Task 5: The address weekly takes its final name

**Files:**
- Modify: `src/dagster_v3/defs/se_company/address/jobs.py:23-29`
- Modify: `tests/test_se_company_address_jobs.py:22`
- Modify: `src/dagster_v3/defs/se_company/address/docs/address-design.md:140-144`

**Interfaces:**
- Consumes: `assets.EXTRACTOR_ASSET_NAMES`, `NORMALIZE_ASSET`, `WEEKLY_RUN_CONFIG` (unchanged).
- Produces: the schedule `se_company_address_weekly`, cron `5 7 * * 1`, STOPPED, on `se_company_address_extract_job`. The name is free because Task 3 deleted `address_legacy.py`'s schedule of the same name.

- [ ] **Step 1: Write the failing test**

`tests/test_se_company_address_jobs.py`:

```python
def test_the_weekly_is_registered_stopped_with_execute_and_the_page_size() -> None:
    """The `_v2` interim name existed only to avoid colliding with the old model's schedule,
    which retired in slice 4b; the extract weekly now carries the canonical name. It stays
    STOPPED, and it stays extract + normalize only -- the fold is manual by design."""
    schedule = _repo().get_schedule_def("se_company_address_weekly")
    assert schedule.cron_schedule == "5 7 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job_name == "se_company_address_extract_job"
    assert not any(s.name == "se_company_address_v2_weekly" for s in _repo().schedule_defs)
    ops = jobs.WEEKLY_RUN_CONFIG["ops"]
    for name in assets.EXTRACTOR_ASSET_NAMES:
        assert ops[name] == {"config": {"execute": True, "page_size": jobs.WEEKLY_PAGE_SIZE}}
    assert ops["se_company_address_normalize"] == {"config": {"changed_only": True}}
    assert jobs.WEEKLY_PAGE_SIZE == 20_000
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_address_jobs.py -q
```

Expected: FAIL with `DagsterInvariantViolationError` / no schedule named `se_company_address_weekly`.

- [ ] **Step 3: Rename the schedule**

```python
se_company_address_weekly = dg.ScheduleDefinition(
    name="se_company_address_weekly",
    job=se_company_address_extract_job,
    cron_schedule="5 7 * * 1",
    run_config=WEEKLY_RUN_CONFIG,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

The module docstring gains: the old model's schedule of this name was deleted in slice 4b, so the interim `_v2` name is no longer needed.

- [ ] **Step 4: Update the design doc**

`address-design.md` lines 140-144: `se_company_address_weekly` schedules the extract job Mondays 07:05 UTC with `execute: true`, `page_size: 20000` and `changed_only: true`, registered STOPPED; drop the paragraph explaining the `v2` interim name and say instead that it took the canonical name when slice 4b retired the old model's schedule.

- [ ] **Step 5: Run and check**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync \
  pytest tests/test_se_company_address_jobs.py tests/test_se_company_address_assets.py -q
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
rg -n "se_company_address_v2" src tests
```

Expected: pass, clean, and `rg` returns nothing outside the two clickhouse-local schema replays and the DDL-name pins added in Tasks 1 and 2.

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/jobs.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md \
        corpscout/services/dagster_v3/tests/test_se_company_address_jobs.py
git commit -m "feat(dagster): the address extract weekly takes the se_company_address_weekly name"
```

---

### Task 6: The backoffice names the entity through one constant

**Files (backoffice, all paths from `corpscout/services/backoffice`):**
- Create: `app/lib/se-address-tables.ts`
- Modify: `app/lib/address-quality.server.ts:8`, `app/lib/address-companies.server.ts:18`, `app/lib/company-sections.server.ts:25`, `app/lib/se-company-address-entity.server.ts:45,197`
- Modify: `app/lib/countries.ts:763-767` (`placeQuery`) and `:1208-1239` (`detail.addressQuery`)
- Modify: `tests/address-quality.test.ts:84-113`, `tests/address-companies.server.test.ts:47-70`, `tests/company-serving-sections.test.ts:49-70`, `tests/se-company-address-entity.server.test.ts:315,339`, `tests/countries.test.ts:301-320`

**Interfaces:**
- Produces: `export const SE_COMPANY_ADDRESS_TABLE = "corpscout.se_company_address";` from `~/lib/se-address-tables`, imported by all four server modules. No other backoffice module may spell the table.

**The prefix trap:** `corpscout.se_company_address` is a prefix of `corpscout.se_company_address_suggestion` and `_normalized`. `tests/se-company-address-entity.server.test.ts:315` dispatches its fake client on `sql.includes("FROM corpscout.se_company_address_v2")`; after the rename that branch must be `sql.includes("FROM corpscout.se_company_address AS m FINAL")`, or the normalized query gets the published rows.

**The `countries.ts` gap — a slice-4a miss, closed here (controller ruling, 2026-09-08):** the SE country config's `placeQuery` and `detail.addressQuery` still read `corpscout.se_company_addresses_current`, which is on slice 4c's drop list. Slice 4a switched the bespoke SE pages and missed these two: the generic company list's Place column (`queries.server.ts:287,436`) and the generic company detail's address block (`queries.server.ts:3196`). They are a reader switch, not a drop, so they belong in 4b rather than 4c, and they must land before 4c drops the table under them.

- [ ] **Step 1: Write the failing tests**

`tests/address-quality.test.ts`, inside `it("reads the published address entity and never the retired chain")`, replace the positive assertion and add the never-contains half:

```ts
    for (const sql of queries.filter((q: string) => q.includes("address."))) {
      expect(sql).toContain("FROM corpscout.se_company_address AS address FINAL");
      expect(sql).toContain("WHERE address.active = 1");
      // Slice 4b renamed the table; the interim name must not survive anywhere.
      expect(sql).not.toContain("se_company_address_v2");
    }
```

`tests/address-companies.server.test.ts`:

```ts
    expect(sql.match(/corpscout\.se_company_address AS/g)).toHaveLength(2);
    expect(sql).not.toContain("se_company_address_v2");
```

`tests/company-serving-sections.test.ts`:

```ts
    expect(sectionServer).toContain(
      'import { SE_COMPANY_ADDRESS_TABLE } from "~/lib/se-address-tables"',
    );
    expect(sectionServer).toContain("FROM ${SE_COMPANY_ADDRESS_TABLE} AS address FINAL");
    expect(sectionServer).not.toContain("se_company_address_v2");
```

`tests/se-company-address-entity.server.test.ts`: the dispatch branch and

```ts
    expect(ADDRESS_MAIN_SQL).toContain("FROM corpscout.se_company_address AS m FINAL");
    expect(ADDRESS_MAIN_SQL).not.toContain("se_company_address_v2");
```

`tests/countries.test.ts` (lines 311-317):

```ts
    expect(se.placeQuery).toContain("corpscout.se_company_address FINAL");
    expect(se.placeQuery).toContain("active = 1");
    expect(se.detail?.addressQuery).toContain("FROM corpscout.se_company_address FINAL");
    expect(se.detail?.addressQuery).toContain("active = 1");
    expect(se.detail?.addressQuery).toContain("AS geocode_address");
    expect(se.detail?.addressQuery).toContain("AS geocode_street");
    expect(se.detail?.addressQuery).toContain("AS geocode_postal_code");
    expect(se.detail?.addressQuery).toContain("AS address_country_code");
    for (const retired of ["se_company_addresses_current", "se_company_address_v2"]) {
      expect(se.placeQuery).not.toContain(retired);
      expect(se.detail?.addressQuery).not.toContain(retired);
    }
```

- [ ] **Step 2: Run them and watch them fail**

```bash
cd corpscout/services/backoffice
npx vitest run tests/address-quality.test.ts tests/address-companies.server.test.ts \
  tests/company-serving-sections.test.ts tests/se-company-address-entity.server.test.ts \
  tests/countries.test.ts
```

Expected: FAIL on the renamed table in all five.

- [ ] **Step 3: Add the shared constant**

`app/lib/se-address-tables.ts`:

```ts
/**
 * The published SE address entity (spec 2026-09-06, section 3.3), under the final name
 * migration 000393 gave it. Every backoffice read of the published addresses names it
 * through this constant.
 *
 * MIND THE PREFIX: `corpscout.se_company_address_suggestion`, `_normalized`, `_history`,
 * `_rule` and `_precedence` all start with this string, so a match on the table -- in a
 * test, or in a fake client's dispatch -- has to carry the alias that follows it.
 */
export const SE_COMPANY_ADDRESS_TABLE = "corpscout.se_company_address";
```

- [ ] **Step 4: Point the four modules at it**

In each of `address-quality.server.ts`, `address-companies.server.ts` and `company-sections.server.ts`, delete the local `const` and its comment and import `SE_COMPANY_ADDRESS_TABLE` from `~/lib/se-address-tables`; keep the local identifier name each file already interpolates (`ADDRESS_TABLE` becomes an import alias where that is less churn: `import { SE_COMPANY_ADDRESS_TABLE as ADDRESS_TABLE } from "~/lib/se-address-tables";`). In `se-company-address-entity.server.ts`, import the constant and interpolate it in `ADDRESS_MAIN_SQL` (`FROM ${SE_COMPANY_ADDRESS_TABLE} AS m FINAL`), and update the `SeAddressRow` doc comment to name `se_company_address`.

- [ ] **Step 5: Switch the two generic country queries**

`countries.ts`, SE `placeQuery`:

```ts
    placeQuery: `SELECT toString(company_id) AS company_id,
            argMax(ifNull(city, ''), has(kinds, 'postal')) AS place
     FROM corpscout.se_company_address FINAL
     WHERE company_id IN {ids:Array(String)} AND active = 1
     GROUP BY company_id`,
```

(the old query preferred the postal row's `post_town`; `kinds` is the entity's array of the row's contributing kinds, so `has(kinds, 'postal')` is the same preference).

SE `detail.addressQuery` — the entity stores parsed components and a display line, so the whole WITH block of source normalization goes:

```ts
      addressQuery: `SELECT
  arrayStringConcat(arrayMap(x -> toString(x), kinds), ',') AS address_type,
  normalized_address AS full_address,
  if(
    geocode_status = 'foreign',
    '',
    arrayStringConcat(arrayFilter(x -> x != '', [
      trim(concat(ifNull(street_name, ''), ' ', ifNull(house_number, ''))),
      trim(concat(ifNull(postal_code, ''), ' ', ifNull(city, '')))
    ]), ', ')
  ) AS geocode_address,
  if(
    geocode_status = 'foreign',
    '',
    trim(concat(ifNull(street_name, ''), ' ', ifNull(house_number, '')))
  ) AS geocode_street,
  if(geocode_status = 'foreign', '', ifNull(postal_code, '')) AS geocode_postal_code,
  toString(country_code) AS address_country_code,
  toUInt8(geocode_status = 'foreign') AS address_is_foreign
FROM corpscout.se_company_address FINAL
WHERE company_id = {id:String}
  AND active = 1
ORDER BY address_type
LIMIT 10`,
```

`full_address` is the normalizer's display line, which already carries the `c/o` prefix and the spaced postcode the old SQL rebuilt by hand; the geocode fields deliberately leave the unit out, exactly as the old query stripped its `N TR` suffix.

- [ ] **Step 6: Run the tests and the type check**

```bash
cd corpscout/services/backoffice
npx vitest run tests/address-quality.test.ts tests/address-companies.server.test.ts \
  tests/company-serving-sections.test.ts tests/se-company-address-entity.server.test.ts \
  tests/countries.test.ts tests/se-company-geocoding-list.server.test.ts
npm run typecheck
rg -n "se_company_address_v2|se_company_addresses_current" app
```

Expected: tests pass, typecheck clean, `rg` returns only `app/lib/se-company-address.server.ts` (deleted in Task 7).

- [ ] **Step 7: Commit**

```bash
git add corpscout/services/backoffice/app/lib/se-address-tables.ts \
        corpscout/services/backoffice/app/lib/address-quality.server.ts \
        corpscout/services/backoffice/app/lib/address-companies.server.ts \
        corpscout/services/backoffice/app/lib/company-sections.server.ts \
        corpscout/services/backoffice/app/lib/se-company-address-entity.server.ts \
        corpscout/services/backoffice/app/lib/countries.ts \
        corpscout/services/backoffice/tests/address-quality.test.ts \
        corpscout/services/backoffice/tests/address-companies.server.test.ts \
        corpscout/services/backoffice/tests/company-serving-sections.test.ts \
        corpscout/services/backoffice/tests/se-company-address-entity.server.test.ts \
        corpscout/services/backoffice/tests/countries.test.ts
git commit -m "refactor(backoffice): one constant names the renamed address entity"
```

---

### Task 7: The dead Address tab and the correction queue go

**Files (backoffice):**
- Delete: `app/components/admin/se-company-address.tsx`, `app/lib/se-company-address.server.ts`, `app/lib/se-address-review-form.ts`, `app/routes/admin-se-company-address-corrections.tsx`, `app/lib/se-company-address-lists.server.ts`, `app/lib/se-address-corrections.ts`, `app/components/admin/se-company-address-corrections-table.tsx`
- Delete: `tests/se-company-address.test.tsx`, `tests/se-company-address.server.test.ts`, `tests/se-address-review-form.test.ts`, `tests/se-address-corrections.test.ts`, `tests/se-company-address-corrections-table.test.tsx`, `tests/se-company-address-corrections.live.test.ts`, `tests/se-company-address-lists.server.test.ts`
- Modify: `app/routes.ts:196-199`, `app/components/admin/admin-sidebar.tsx:73-78`, `app/routes/admin-layout.tsx:85-86,176-195`, `app/lib/clickhouse.server.ts:197-205`
- Modify: `tests/clickhouse-writer.server.test.ts:14,90,106`, `tests/se-company-tabs.server.test.ts`, `tests/admin-se-company-area.test.tsx`

**Interfaces:**
- Consumes: nothing from earlier tasks. The live Address tab (`app/routes/admin-se-company-address.tsx` → `se-address-workspace.tsx` → `se-company-address-entity.server.ts`) is untouched.
- Produces: `chInsertSeCompanyAddressCorrections` no longer exists; `chInsertSeCompanyAddressRules` and `chInsertSeCompanyAddressSuggestions` stay (the entity module writes through them).

**Why the whole queue and not just the writer:** `se-company-address.server.ts` and `se-address-review-form.ts` are imported only by the dead `se-company-address.tsx` component, which no route renders. The corrections queue is alive but write-orphaned: its only writer was that dead module's `chInsertSeCompanyAddressCorrections` call, and `corpscout.se_company_address_correction` holds 0 rows on prod, so no decision is lost. `se-company-address-lists.server.ts` serves nothing but that route (verified: its only non-test importer is `admin-se-company-address-corrections.tsx`), so it goes whole.

- [ ] **Step 1: Delete the modules and their tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/backoffice
rm app/components/admin/se-company-address.tsx \
   app/components/admin/se-company-address-corrections-table.tsx \
   app/lib/se-company-address.server.ts \
   app/lib/se-address-review-form.ts \
   app/lib/se-company-address-lists.server.ts \
   app/lib/se-address-corrections.ts \
   app/routes/admin-se-company-address-corrections.tsx
rm tests/se-company-address.test.tsx tests/se-company-address.server.test.ts \
   tests/se-address-review-form.test.ts tests/se-address-corrections.test.ts \
   tests/se-company-address-corrections-table.test.tsx \
   tests/se-company-address-corrections.live.test.ts \
   tests/se-company-address-lists.server.test.ts
```

- [ ] **Step 2: Take the route, the nav link and the breadcrumb out**

`app/routes.ts`: delete the `route("se/company-address/corrections", "routes/admin-se-company-address-corrections.tsx")` entry.

`app/components/admin/admin-sidebar.tsx`: delete the `"Address corrections"` entry and the now-unused `MapPinIcon` import.

`app/routes/admin-layout.tsx`: delete `onCompanyAddressCorrectionsPage` and the whole `if (onCompanyAddressCorrectionsPage) { ... }` breadcrumb branch.

- [ ] **Step 3: Delete the correction writer**

`app/lib/clickhouse.server.ts`: delete `chInsertSeCompanyAddressCorrections` (lines 197-205) and any now-unused table constant it alone used. `chInsertSeCompanyAddressRules` immediately above it stays.

`tests/clickhouse-writer.server.test.ts`: delete the import and the two cases that call it (lines 90 and 106); if a case covers "an empty batch inserts nothing" only through this function, re-point it at `chInsertSeCompanyAddressRules` rather than dropping the coverage.

- [ ] **Step 4: Trim the two multi-tab test files**

`tests/se-company-tabs.server.test.ts`: delete the `chInsertSeCompanyAddressCorrections` line from the hoisted `vi.mock`, the `~/lib/se-company-address.server` import block, the four address SQL constants from the statement list the file pins, the `it("reads the address final on its own, with no join chain behind it")` case and the address half of `it("reads the live rows, the tombstones and the ledger of one company")`. Every other tab's coverage stays.

`tests/admin-se-company-area.test.tsx`: delete the `insertAddressCorrections` mock member and its `chInsertSeCompanyAddressCorrections` line, the `SeCompanyAddressTab` and `~/lib/se-company-address.server` imports, and the whole `describe("address tab")` block (five cases). The header, tab-path, financial, people, jobs and technology blocks stay.

- [ ] **Step 5: Run the suite and the type check**

```bash
cd corpscout/services/backoffice
npx vitest run tests/clickhouse-writer.server.test.ts tests/se-company-tabs.server.test.ts \
  tests/admin-se-company-area.test.tsx tests/se-company-info-lists.server.test.ts \
  tests/se-company-address-entity.server.test.ts tests/se-address-edit-sheet.test.tsx \
  tests/se-address-fields.test.ts tests/se-address-decision-form.test.ts
npm run typecheck
rg -n "se-company-address.server|se-address-review-form|se-address-corrections|se-company-address-lists|chInsertSeCompanyAddressCorrections" app tests
```

Expected: tests pass, typecheck clean, `rg` returns nothing.

- [ ] **Step 6: Commit**

```bash
git add -u corpscout/services/backoffice/app corpscout/services/backoffice/tests
git commit -m "refactor(backoffice): retire the old address tab and the correction queue"
```

---

### Task 8: Prod run (controller)

1. [ ] Whole-branch review; merge to main. If main took 000393 in the meantime, renumber first (both migration files, `EXPECTED_MIGRATIONS`, `MIGRATION` in `tests/test_se_companies_serving_mv.py`, and the `migrate force <n>` line in the migration header), then re-run `pytest tests/test_clickhouse_migrations.py tests/test_se_companies_serving_mv.py -q`.
2. [ ] Deploy dagster per the worktree deploy recipe (pristine worktree, dbt-state refresh, `.env`); hot-sync the host. Between this step and step 3 the deployed code names `corpscout.se_company_address` while the database still holds `_v2`. **The window is acceptable and deliberate** (controller ruling, 2026-09-08): nothing that reads the main table is scheduled — `se_company_address_weekly` is STOPPED, the fold is manual by design, and the only RUNNING schedule, `sweden_company_address_geocoding_weekly` (Tuesdays 04:05 Europe/Stockholm), touches the normalized layer and the geocode cache but never the main table. So the only way to hit it is to launch a fold by hand: don't, until step 3 reports success. The reverse order (migrate first) has the same window with the old code broken instead.
3. [ ] Apply the migration: `make -s -C <deploy-worktree>/corpscout clickhouse-migrate-up-one`. If the client drops mid-file, follow the runbook in `address-design.md` (`SYSTEM START VIEW corpscout.se_companies_serving`, finish the ALTER if it did not land, `migrate force 393`).
4. [ ] Verify the rename and the view: `SELECT name FROM system.tables WHERE database='corpscout' AND name LIKE 'se_company_address%'` shows `se_company_address` and `se_company_address_legacy`; after the next :45 refresh, `SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE database='corpscout' AND view='se_companies_serving'` reports a success with an empty exception; `SELECT count() FROM corpscout.se_companies_serving` is around 3.5M and `countIf(address_count > 0)` around 3.49M (slice 2b's published population).
5. [ ] Confirm the schedules in the Dagster UI: `se_company_address_weekly` present and STOPPED (it inherits the stored state of the deleted schedule of the same name, which was also STOPPED — if it shows RUNNING, stop it), `sweden_company_address_geocoding_weekly` RUNNING with the five-asset selection, and none of the eight deleted jobs present.
6. [ ] Backoffice smoke (local, `npm run dev`): the company Address tab (published rows, history, the reviewer's edit sheet, a targeted fold launch), the companies list and its geocoding list, the address-quality queue with each filter, the same-building lookup, the generic company detail's address block and the generic list's Place column for SE.
7. [ ] Record in the ledger and in spec section 9 (slice 4b shipped: the migration number, the rename, what was deleted, the weekly's new shape); archive the ledger; update memory.
8. [ ] Hand slice 4c its drop list, all owner-run under the ledger policy, in this order (each drop after the one that reads it): `se_company_address_legacy`, `se_company_address_scb`, `se_company_address_bolagsverket`, `se_company_address_correction`, `se_company_addresses`, `se_company_addresses_current`, `se_company_address_members_current` (`se_company_addresses_canonical_current` is already gone: `CANONICAL_RETIREMENT_DROP_SQL` ran), then the `se_address_geocodes_served` view, then `se_addresses_current` and `se_company_address_links_current` (the view enriches from them), then the `se_address_geocodes_current` materialized view (the parked `se_address_geocodes_current_retired` table is already gone; verified on prod 2026-09-08). Record row counts beside the ledger entry before each drop; UNDROP is possible for about 480 seconds. 4c also owns the migration-file edits and the fixture cleanup under the ledger policy, and re-establishing `geocode_serving_overlay`'s retirement once `se_address_geocodes_served` is gone. Kept for good: `se_address_geocodes` (the entity's geocode cache), `se_postcode_centroids`, `se_city_centroids`, the matcher, the policy constant and the OSM workbench.

## Self-review

- **Spec coverage.** Section 3.3's rename: Task 1 (the migration and the serving view) and Task 2 (the entity's own constant). Section 6's warm step: it is the weekly's remaining geocode step after Task 4's trim, and Task 4 re-points the serving refresh at it, gives it the geocode cache's freshness leaf and re-hosts the OSM-snapshot warn on it. Section 9's Retirement paragraph, item by item: `se_company_address_scb` / `se_company_address_bolagsverket` writers and the old `se_company_address` publisher with its sensor and weekly — Task 3; `se_company_addresses` / `se_company_addresses_current` register-load steps — Task 4; canonical, members, `se_addresses_current`, links, the demand scan and `geocode_legacy_adoption` — Task 4; the `se_company_address_v2_weekly` rename — Task 5; the drops themselves — Task 8 step 8, handed to 4c as the spec requires. Section 10's names: Task 2 (tables) and Task 5 (schedule). The spec's own drop list omits `se_address_geocodes_served`; Task 8 names it, with its ordering constraint.
- **Placeholders.** Every code step carries the code: the migration's five statements and both comment headers, the drift pin's helpers and all six tests, the weekly selection, the deps pin, the four backoffice assertions, the two rewritten `countries.ts` queries. The one text not inlined is the 171-line rendered view body, which Step 2 of Task 1 generates with an exact command — inlining it would guarantee drift from the builder the drift pin compares it against.
- **Type consistency.** `tables.QUALIFIED_MAIN_TABLE` (Task 2) and `companies_current.COMPANY_ADDRESS_TABLE` (Task 1) both resolve to `corpscout.se_company_address`, and the backoffice's `SE_COMPANY_ADDRESS_TABLE` (Task 6) is the same string. `WARM_ASSET_KEY = "se_address_geocodes_warm"` in Task 4 matches the asset name in `se_company/address/assets.py:272`, the weekly selection's string, the new `ClickhouseLeaf`'s key and the moved check's host, all in the same task. The moved helpers keep one name each on both sides of the move: `MAX_OSM_SNAPSHOT_AGE`, `SNAPSHOT_FRESHNESS_SQL` and `osm_snapshot_is_fresh` are unchanged, and `fetch_sweden_geocode_snapshot_freshness` becomes `fetch_osm_snapshot_freshness` (the `sweden_geocode` prefix means nothing inside the address package) — the moved tests in Step 8 call it under the new name. `RETIRED_LEAF_ASSET_KEYS` is defined in Task 3 and extended, not redefined, in Task 4; it keeps `sweden_address_geocode_store_clickhouse`, whose leaf is gone, while the table it watched is re-covered by the warm leaf asserted alongside it. Task 5 depends on Task 3 having deleted the schedule whose name it takes; Task 4's weekly trim must land with its asset deletions, not with Task 5, because a job selection naming a deleted asset does not resolve.
