# SE LLM Enhance Slice 1: The Tables, the Alters and the Gap View — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the LLM-enhancement pattern's storage on the ground for the person match — `corpscout.llm_queue_se_company_person` and `corpscout.llm_response_se_company_person` — widen `se_company_person_match`'s sorting key with `request_id` so a second prompt version can never silently unmerge a person, and publish `corpscout.se_company_person_match_gap`, the refreshable view that names the companies still carrying a deterministic call-name or double-surname gap. Nothing calls a model in this slice: it is the schema, the view and the proofs that both survive a real ClickHouse.

**Architecture:** One migration, `000406_corpscout_se_company_person_llm_enhance`, with five statements: the queue table, the response table, the pair table's single `ADD COLUMN … , MODIFY ORDER BY …` alter, the state table's `ADD COLUMN`, and the refreshable view. The two new tables are `ReplacingMergeTree`s keyed `(request_id, company_id)` — a request's companies are always read together — and both carry the same `valid_company_id` constraint the eleven other person tables carry. The pair alter is ONE statement because ClickHouse extends a sorting key only with a column added by the same `ALTER`; the column takes the type's implicit zero value (`''`), so every stored row's new key component is identical and no part has to be re-sorted — metadata only. The gap view follows the repo's refreshable form exactly as `se_company_person_role` does (000402): the engine is declared INSIDE the view, it is created `EMPTY` so the migrate client's 300 s read timeout can never leave the ledger dirty, it refreshes hourly at `:30` — the free half of the hour between the role view's `:20` and the serving view's `:45` — and its SELECT is the exact rendering of a new builder in `person/tables.py`, drift-pinned against the migration text by a test. A side effect the migration forces and this slice therefore owns: `tests/se_company_ddl.py::declared_columns` REPLAYS later `ADD COLUMN` clauses, so the moment 000406 exists the pair and state tuples in `tables.py` must carry `request_id` too, and `match.match_row` / `match.match_state_row` gain a defaulted `request_id` keyword so the live match asset keeps writing the tuples it always wrote.

**Tech Stack:** Python 3.14 / Dagster 1.13.9 (`uv run --frozen --no-sync`, tests with `pytest`), ClickHouse 26.5.1.882 on prod (golang-migrate, `make -s -C corpscout clickhouse-migrate-up-one`), `clickhouse/clickhouse-server:26.5` under Docker for the integration proofs.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-13-se-llm-enhance-design.md` — **section 4** (the tables and migration 000406, including 4.3's two alters and 4.4's down), **section 5** (the match-gap view: 5.1's two definitions, 5.2's SQL and the four notes under it), **section 8** only for its DDL consequence (`request_id` in the pair table's sort key; the `batch.py::match_pairs_sql()` amendment is SLICE 2 and this plan must not write it), **section 11** (the tests this slice owns), **section 12** (the rulings quoted verbatim in Global Constraints below), **section 13** (names) and **section 14 slice 1** (the scope and the shipped record to append). Sections 2 and 3 are the facts and the reasoning the above rests on. Sibling plans in this directory, for shape: `2026-09-12-se-company-person-5-roles.md` (the refreshable-MV builder, its drift pin, the 000402 SETTINGS block and the prod runbook) and `2026-09-12-se-ratsit-3-addresses.md` (header, Global Constraints, Files/Interfaces blocks, the commit-message steps and the deploy recipe).

## Global Constraints

- **Worktree and branch.** All work happens in `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info` on branch `se-llm-enhance` (already checked out, clean, one commit ahead of `main`: the spec, `865a31c18`). Never `cd` to the main checkout.
- **Never `git stash`.** Commit by explicit path only — never `git add -A`, never `git add .`. Write the message to a file and use `git commit -F <file>`; the trailers below must be the last two lines, each on its own line:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
  ```
- **Dagster commands** run from `corpscout/services/dagster_v3`. Export the env first for anything that loads the definitions tree: `set -a && . ./.env && set +a`, then `uv run --frozen --no-sync pytest ...`. `uv run --frozen --no-sync dg check defs` before any commit that touches `src/`.
- **No model is called and no asset is written in this slice.** `person/llm_enhance.py`, the four assets, `SYSTEM_PROMPT_V2`, the `batch.py::match_pairs_sql()` amendment and every backoffice file belong to slices 2 and 3. This plan touches `tables.py`, `match.py`'s two row builders, the migration pair and tests — nothing else in `src/`.
- **No new dependencies** — no package is added to `pyproject.toml`, `uv.lock`, `package.json` or `pnpm-lock.yaml`.
- **Migrations are forward-only and only ever target the `corpscout` database** (`dagster_v3/CLAUDE.md`, "ClickHouse migrations"). Every migration name goes into `EXPECTED_MIGRATIONS`.
- **No semicolon may appear inside a `--` comment in a migration file** (`tests/test_clickhouse_migrations.py::test_clickhouse_migration_line_comments_do_not_contain_semicolons` and `::test_no_semicolon_inside_sql_comments`): golang-migrate splits the file on `;` without stripping comments, so a `;` in a comment becomes an empty query (code 62). The file must also END with a statement, not prose (`::test_every_migration_ends_with_a_statement_not_a_comment`).
- **`ORDER BY` cannot contain `Nullable` columns** (`allow_nullable_key` is off — `dagster_v3/CLAUDE.md`). Both new tables and the view key on non-nullable columns only; nothing here needs an `assumeNotNull`.
- **`EMPTY` plus a manual refresh, never `SYSTEM WAIT VIEW`.** The migrate client's `read_timeout` is 300 s; a first build over 1.27M active persons and 5.8M normalized rows can outlive it, and a dropped client leaves the ledger dirty. The migration creates the view empty and returns in milliseconds; the controller runs `SYSTEM REFRESH VIEW` and polls `system.view_refreshes`.
- **Whole-name matching, always.** `corpscout.se_company_person` is a prefix of `_suggestion`, `_normalized`, `_history`, `_rule`, `_precedence`, `_role`, `_match` and `_match_state`, and `se_company_person_match` is a prefix of `se_company_person_match_state` and of `se_company_person_match_gap`. Every string match on a table name — a test assertion, an `rg` check, a fixture's dispatch branch — carries the alias or the token that follows it (`corpscout.se_company_person_match AS m FINAL`, `ALTER TABLE corpscout.se_company_person_match\n`), never the bare name.
- **Commands against prod ClickHouse go through the session scratch runner**, read-only unless the step says otherwise: `bash $S/ch.sh <file.sql>` where `S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/basic-info-0` (it is `ssh companycollect "docker exec -i clickhouse-clickhouse-1 clickhouse-client --multiquery"`). A task subagent never touches prod — Task 3 is the controller's.
- **`lowerUTF8`, never `lower`**, if a case fold is ever needed here (it is not: the gap view compares normalizer tokens, which are already lowercase ASCII). **`FINAL` goes after the alias** (`… AS p FINAL`). **Never write `toString(x) AS x`** — the alias shadows the column for every later expression in the same SELECT.
- **`system.tables.create_table_query` is the live DDL** when prod and the file are compared; `system.tables.sorting_key` is the live sort key.
- **Do not fix pre-existing failing tests.** Record anything already red at the branch point and leave it alone.
- Never `from __future__ import annotations` in a module inside the definitions tree.

### Rulings copied verbatim from spec section 12

> - **The migration number may collide.** This branch's highest is `000404`; another session's working tree already carries an unmerged `000405_corpscout_esef_domains`. Slice 1 re-checks `main` AND the prod `schema_migrations` ledger immediately before merging and renumbers if `000406` has been taken. This is the standing rule for every migration branch here.
> - **`MODIFY ORDER BY` is the one risky statement.** It is the documented way to extend a sorting key and it is metadata-only, but it must be in the SAME `ALTER` as the `ADD COLUMN` and the column must default to the type's zero value. The clickhouse-local test of section 11 proves it against a populated table before prod. If a server ever refuses it, the fallback is a rebuild inside its own migration (`CREATE … LIKE` with the new key, `INSERT SELECT`, `EXCHANGE TABLES`) — 159,789 rows, seconds of work — and NOT dropping the maximum rule.
> - **The gap view generalizes the 2026-09-11 measurements** (section 5.1), so its counts will not equal 3,258 and 2,380. The first refresh's numbers are the new baseline, recorded in slice 1's shipped note.
> - **The gap view is the heaviest hourly refresh this entity owns**: two `ARRAY JOIN`s over 1.27M active persons, a self-join of ~2M member rows and an anti-join. It carries the 000402 SETTINGS block (grace_hash, external group-by/sort, 12 GiB cap) for exactly that reason, and its first build is run by hand and TIMED; if it exceeds ten minutes the refresh moves to every 6 hours rather than growing the memory cap.
> - **`llm_queue_se_company_person` and `llm_response_se_company_person` are the only corpscout tables whose names do not start with their entity's prefix.** That is the pattern's name, not an oversight: `llm_<step>_<source table>` reads as "the LLM queue OF se_company_person". Every string match on a table name in this repo compares whole names, so nothing is broken by the new prefix.

## Baseline, verified at the branch point (2026-09-13)

- `git branch --show-current` → `se-llm-enhance`; `git status --porcelain` is empty; `git log --oneline main..HEAD` is exactly `865a31c18 docs(spec): LLM enhancement per source table, first use the person match v2`.
- `git ls-tree --name-only main corpscout/clickhouse/migrations/ | tail` ends at `000404_corpscout_se_financial_readers_entity` — **000405 and 000406 are both free on `main`**, and the prod ledger's newest row is `404` with `dirty = 0`. Spec section 12 reserves 000405 for another session's unmerged `000405_corpscout_esef_domains`, which is why this one is 000406.
- No `clickhouse-local` and no `clickhouse` binary on this machine; **Docker IS running**, so `tests/clickhouse_local.py::clickhouse_local_command()` falls back to `clickhouse/clickhouse-server:26.5` and the integration proofs really run. If one ever skips, say it skipped — never report a skip as a pass.
- `corpscout.se_company_person_role` and `corpscout.se_companies_serving` are the ONLY two rows in prod's `system.view_refreshes` (`:20` and `:45`). **`:30` is free**, which is the offset spec section 5 chose.

## Prod facts, measured 2026-09-13 while writing this plan

| fact | value |
| --- | --- |
| ClickHouse | `26.5.1.882` |
| ledger head | `404`, not dirty |
| `se_company_person_match` | **162,192 rows** under `FINAL`, 111,281 companies, exactly one `prompt_version` (`se-person-match-v1`); `sorting_key` = `company_id, candidate_a, candidate_b` |
| `se_company_person_match_state` | **124,646 rows** under `FINAL`, 108 of them with a non-empty `error`; `sorting_key` = `(company_id)` |
| `se_company_person` | 1,273,763 active persons over 677,256 companies |
| `se_company_person_normalized` | 5,795,956 rows with `parse_status = 'ok'` and a machine source — the gap view's member input |
| refreshable views | `se_company_person_role` at `:20`, `se_companies_serving` at `:45`; nothing at `:30` |

**These do not match the spec's section 2 table, and the code wins.** Spec section 2 records 159,789 pairs / 122,837 companies and a 122,837-row state table; those are the 2026-09-11 measurements and prod has moved since. Task 3 reads the live numbers again before and after the alter and compares each table's count against ITSELF, never against the spec's figure.

## Proven against ClickHouse 26.5 while this plan was written

Run under `clickhouse/clickhouse-server:26.5` via Docker, with 000396's and 000399's real `CREATE TABLE`s, a 13-person / 14-normalized-row fixture and two pre-loaded pair rows, under BOTH `join_use_nulls` settings:

1. **Spec section 4.3's alter as written is REFUSED.** `ADD COLUMN IF NOT EXISTS request_id String DEFAULT '', MODIFY ORDER BY (…)` fails with
   `Code: 36. DB::Exception: Newly added column request_id has a default expression, so adding expressions that use it to the sorting key is forbidden. (BAD_ARGUMENTS)`.
   Dropping the explicit `DEFAULT ''` — leaving `ADD COLUMN IF NOT EXISTS request_id String` — succeeds. The stored value is identical (`''` is `String`'s implicit zero), which is exactly what the spec's own prose asks for ("with the type's zero default — here `''`"); it is the explicit DEFAULT EXPRESSION that ClickHouse refuses in a key. **The migration in Task 1 uses the no-DEFAULT form on the pair table and keeps `DEFAULT ''` on the state table, where the column is not in the key.**
2. After the alter, `system.tables.sorting_key` reads `company_id, candidate_a, candidate_b, request_id`, both pre-existing rows survive, and `countIf(request_id = '')` equals `count()`.
3. Inserting a `se-person-match-v2` row for the SAME candidate pair under `request_id = 'req-0002'` leaves **two rows under `FINAL`** — `('', v1, 0.9)` and `('req-0002', v2, 0.95)`. That is the whole point of section 4.3.
4. Spec section 5.2's SELECT parses and answers correctly over the six fixtures, identically under `join_use_nulls` 0 and 1: only the call-name company (`1, 0`) and the double-surname company (`0, 1`) come back; the already-matched, overlapping-source, birth-year-conflict and single-source companies do not.
5. `CREATE MATERIALIZED VIEW … REFRESH EVERY 1 HOUR OFFSET 30 MINUTE ENGINE = MergeTree ORDER BY (company_id) EMPTY AS` followed by the SELECT of point 4 is accepted, and the inferred columns are exactly
   `company_id String, call_name_pairs UInt32, double_surname_pairs UInt32, computed_at DateTime64(3, 'UTC')` — the spec's declared shape, with no Nullable in the key.

Task 2 turns runs 1–4 into the committed test.

## Consequences the spec did not name, which this slice must absorb

`tests/se_company_ddl.py::declared_columns(table)` reads the creating migration's `CREATE TABLE` block **and then replays every later `ADD COLUMN` / `DROP COLUMN` aimed at that table**. So the instant 000406 lands:

- `declared_columns("se_company_person_match")` returns 16 names and `declared_columns("se_company_person_match_state")` returns 14, while `tables.MATCH_COLUMNS` / `MATCH_STATE_COLUMNS` hold 15 and 13 — `tests/test_se_company_person_tables.py::test_match_table_is_one_row_per_unordered_pair` and `::test_match_state_is_one_row_per_matched_company` go red;
- so both tuples gain `"request_id"` at the end, and because `match.match_row()` / `match.match_state_row()` build their tuple as `tuple(values[column] for column in tables.MATCH_COLUMNS)`, both gain a keyword-only `request_id: str = ""`. The live match asset keeps writing exactly what it wrote, now naming the column explicitly as `''` — which is what spec section 4.3 says v1's rows carry. Slice 2's apply passes the real id and needs no further change to these two functions;
- `tests/test_se_company_person_fold_clickhouse_local.py` builds its pair and state INSERTs from those tuples but replays only 000396's and 000399's `CREATE TABLE`s, so it must also replay 000406's two `ALTER`s and its two fixture rows must gain the sixteenth/fourteenth value.

The ALTER's formatting is load-bearing for that replay: `_column_changes` matches `^\s*ADD COLUMN(?: IF NOT EXISTS)? ([a-z_0-9]+) ` **one clause per line**, so `    ADD COLUMN IF NOT EXISTS request_id String,` must sit on its own line and `MODIFY ORDER BY` on the next.

---

## File map

**Task 1 — the migration, the names, the builder and the pins (one commit):**

| file | responsibility after this task |
| --- | --- |
| `corpscout/clickhouse/migrations/000406_corpscout_se_company_person_llm_enhance.up.sql` (create) | six statements: `CREATE DATABASE`, the queue table, the response table, the pair alter (one statement, `ADD COLUMN` + `MODIFY ORDER BY`), the state alter, and the refreshable `CREATE MATERIALIZED VIEW … EMPTY AS <render>` |
| `corpscout/clickhouse/migrations/000406_corpscout_se_company_person_llm_enhance.down.sql` (create) | `CREATE DATABASE`, `DROP VIEW` the gap view, `DROP TABLE` the response and queue tables, `ALTER … DROP COLUMN request_id` on the STATE table only, with the comment saying why the pair table keeps its column |
| `src/dagster_v3/defs/se_company/person/tables.py` (modify) | `LLM_QUEUE_TABLE`, `LLM_RESPONSE_TABLE`, `MATCH_GAP_VIEW` and their `QUALIFIED_*` twins; `LLM_QUEUE_COLUMNS`, `LLM_RESPONSE_COLUMNS`, `MATCH_GAP_VIEW_COLUMNS`, `MATCH_GAP_VIEW_ORDER_BY`; `request_id` appended to `MATCH_COLUMNS` and `MATCH_STATE_COLUMNS`; `build_se_company_person_match_gap_sql()` — the one place the view's SELECT exists |
| `src/dagster_v3/defs/se_company/person/match.py` (modify: `match_row`, `match_state_row`) | both take `request_id: str = ""` and put it in the values dict, so the tuple keeps matching the widened column list |
| `tests/test_se_company_person_match_gap_view.py` (create) | the drift pin (migration body == a fresh builder render), the anti-vacuous check, section 5.1's two rules spelled out, the machine-source / threshold / `ok` pins against `match.py` and `fold.py`, the DDL shape of both migration files, the non-nullable key and the 000402 SETTINGS block |
| `tests/test_se_company_person_tables.py` (modify) | the two new tables pinned against the migration DDL through `se_company_ddl`; the two widened tuples; the gap view's constants |
| `tests/test_clickhouse_migrations.py` (modify) | `000406_corpscout_se_company_person_llm_enhance` in `EXPECTED_MIGRATIONS`, and `test_000406_pairs_the_llm_queue_and_response_and_widens_the_match_sort_key` |
| `tests/test_se_company_person_fold_clickhouse_local.py` (modify) | replays 000406's two alters beside 000396's and 000399's creates; the two fixture rows carry the new column |

**Task 2 — the clickhouse-local proof (one commit):**

| file | responsibility after this task |
| --- | --- |
| `tests/test_se_company_person_match_gap_clickhouse_local.py` (create) | one script per `join_use_nulls` setting: 000399's tables populated, 000406's alters applied, the sort key read back from `system.tables`, the two versions of one pair coexisting, and the gap SELECT over six fixtures that encode every inclusion and every exclusion of spec section 5.1 |

**Task 3 — prod run (controller). No repo change except the shipped record and the plan ticks.**

## Interfaces

- **Task 1 produces**, and Tasks 2 and 3 consume:
  - migration name `000406_corpscout_se_company_person_llm_enhance` (renumber in the gate step if it has been taken);
  - `tables.LLM_QUEUE_TABLE == "llm_queue_se_company_person"`, `tables.QUALIFIED_LLM_QUEUE_TABLE == "corpscout.llm_queue_se_company_person"`;
  - `tables.LLM_RESPONSE_TABLE == "llm_response_se_company_person"`, `tables.QUALIFIED_LLM_RESPONSE_TABLE == "corpscout.llm_response_se_company_person"`;
  - `tables.MATCH_GAP_VIEW == "se_company_person_match_gap"`, `tables.QUALIFIED_MATCH_GAP_VIEW == "corpscout.se_company_person_match_gap"`;
  - `tables.LLM_QUEUE_COLUMNS == ("request_id", "company_id", "queued_at", "queued_by", "note")`;
  - `tables.LLM_RESPONSE_COLUMNS == ("request_id", "company_id", "provider", "model", "prompt_version", "input_hash", "candidates", "prompt_tokens", "completion_tokens", "raw_response", "error", "attempts", "source_run_id", "responded_at")`;
  - `tables.MATCH_GAP_VIEW_COLUMNS == ("company_id", "call_name_pairs", "double_surname_pairs", "computed_at")`;
  - `tables.MATCH_GAP_VIEW_ORDER_BY == ("company_id",)`;
  - `tables.MATCH_COLUMNS[-1] == "request_id"` and `tables.MATCH_STATE_COLUMNS[-1] == "request_id"`;
  - `tables.build_se_company_person_match_gap_sql() -> str` — the SELECT, no trailing semicolon, interpolating `QUALIFIED_MAIN_TABLE`, `QUALIFIED_NORMALIZED_TABLE`, `QUALIFIED_MATCH_TABLE` and `QUALIFIED_MATCH_STATE_TABLE`;
  - `match.match_row(..., request_id: str = "")` and `match.match_state_row(..., request_id: str = "")`.
- **Unchanged and relied upon:** `tables.QUALIFIED_MAIN_TABLE` (`"corpscout.se_company_person"`), `tables.QUALIFIED_NORMALIZED_TABLE`, `tables.QUALIFIED_MATCH_TABLE`, `tables.QUALIFIED_MATCH_STATE_TABLE`, `match.MACHINE_SOURCES == ("bolagsverket", "esef", "wikidata", "ratsit")`, `fold.FOLDABLE_STATUS == "ok"`, `fold.MATCH_THRESHOLD == 0.8`, `batch.MATCH_PAIR_SELECT_COLUMNS`, `tests/se_company_ddl.py::table_block` / `declared_columns`, `tests/clickhouse_local.py::clickhouse_local_command`.
- **Deliberately NOT produced in this slice:** no Dagster asset, job, schedule, pool or sensor; no `person/llm_enhance.py`; no `SYSTEM_PROMPT_V2` and no change to `build_match_request` or `PersonMatchProfile`; **no change to `batch.py::match_pairs_sql()`** (spec section 8 — slice 2); no backoffice file.

---

### Task 1: Migration 000406 creates the queue, the response table, the two alters and the gap view; `tables.py` owns the view's SELECT

**Files:**
- Create: `corpscout/clickhouse/migrations/000406_corpscout_se_company_person_llm_enhance.up.sql`
- Create: `corpscout/clickhouse/migrations/000406_corpscout_se_company_person_llm_enhance.down.sql`
- Create: `corpscout/services/dagster_v3/tests/test_se_company_person_match_gap_view.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/tables.py` (module docstring; constants after `MATCH_STATE_TABLE`; qualified names after `QUALIFIED_ROLE_VIEW`; `MATCH_COLUMNS` and `MATCH_STATE_COLUMNS`; new tuples and the builder at the end of the module)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/match.py` (`match_row` and `match_state_row` only)
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_person_tables.py` (the two match-table tests' tuples, plus three new tests)
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`, one new test at the end of the file)
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_person_fold_clickhouse_local.py` (`_schema_statements` and the two match fixtures)

**Interfaces:**
- Consumes: `tables.QUALIFIED_MAIN_TABLE`, `tables.QUALIFIED_NORMALIZED_TABLE`, `tables.QUALIFIED_MATCH_TABLE`, `tables.QUALIFIED_MATCH_STATE_TABLE`, `match.MACHINE_SOURCES`, `fold.FOLDABLE_STATUS`, `fold.MATCH_THRESHOLD`, `tests/se_company_ddl.py::table_block` and `::declared_columns`.
- Produces: everything in the Interfaces block above.

- [x] **Step 1: Gate — prove 000406 is free on `main` AND on the prod ledger before anything is written**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
git branch --show-current
git fetch --all -q
ls corpscout/clickhouse/migrations | tail -6
git ls-tree --name-only main corpscout/clickhouse/migrations/ | tail -6
```

Expected: `se-llm-enhance`; both listings end at `000404_corpscout_se_financial_readers_entity.{down,up}.sql`. **If either shows a `000406_*`, stop and renumber** — pick the next free number, use it everywhere this plan writes `000406` (both migration files, `EXPECTED_MIGRATIONS`, `MIGRATION` in `tests/test_se_company_person_match_gap_view.py`, the new test's name in `tests/test_clickhouse_migrations.py`, `MIGRATIONS` in `tests/test_se_company_person_match_gap_clickhouse_local.py`, `ENHANCE_MIGRATION_FILE` in `tests/test_se_company_person_fold_clickhouse_local.py`, and every `000406` in the spec's sections 4, 11, 13 and 14) and say so in the report.

```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/basic-info-0
cat > $S/llm1_gate.sql <<'SQL'
SELECT version, dirty FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1;
SQL
bash $S/ch.sh $S/llm1_gate.sql
```

Expected: `404	0`. A number **at or above 406** means the name is taken on prod — renumber as above. A `dirty = 1` at the head means a previous migration failed mid-flight: stop and tell the owner, never migrate over a dirty ledger.

- [x] **Step 2: Write the failing pin test**

Create `corpscout/services/dagster_v3/tests/test_se_company_person_match_gap_view.py`:

```python
"""Migration 000406's refreshable view, and the pin that keeps its body machine-rendered.

`corpscout.se_company_person_match_gap` (spec 2026-09-13 section 5, LLM-enhance slice 1) is
a REFRESHABLE materialized view over four tables the person entity already has: one row per
company that still carries a deterministic call-name or double-surname gap the stored
matches have not closed. Nothing writes it -- the fold, the normalizer, the rules, the
precedence and the match asset never see it -- so the only thing that can drift is the
SELECT itself. This file couples the two halves exactly as
tests/test_se_company_person_role_view.py does for 000402: the migration's body must be
`build_se_company_person_match_gap_sql()`'s render, and the DDL around it must be the
refreshable form this repo uses (engine INSIDE the view, `EMPTY`, hourly at :30).

It also pins the three constants the SELECT spells out by hand, against the modules that
own them: tables.py cannot import match.py or fold.py (both import tables.py), so the four
machine sources, the `ok` parse status and the 0.8 threshold are literals in the builder and
equalities here.
"""

from pathlib import Path

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.fold import FOLDABLE_STATUS, MATCH_THRESHOLD
from dagster_v3.defs.se_company.person.match import MACHINE_SOURCES
from dagster_v3.defs.se_company.person.tables import build_se_company_person_match_gap_sql

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = "000406_corpscout_se_company_person_llm_enhance"
VIEW = "corpscout.se_company_person_match_gap"
MAIN = "corpscout.se_company_person"
NORMALIZED = "corpscout.se_company_person_normalized"
MATCH = "corpscout.se_company_person_match"
MATCH_STATE = "corpscout.se_company_person_match_state"
QUEUE = "corpscout.llm_queue_se_company_person"
RESPONSE = "corpscout.llm_response_se_company_person"


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
        build_se_company_person_match_gap_sql()
    )


def test_the_pin_is_not_vacuous() -> None:
    body = _view_body(_sql("up"))

    assert body.startswith("WITH\n")
    # Whole-name matching: MAIN prefixes every other table named here, so it is counted
    # with the alias that follows it. The main table is read TWICE -- once for the member
    # spellings, once to map a matched member back to its person.
    assert body.count(f"FROM {MAIN} AS p FINAL") == 2
    assert body.count("ARRAY JOIN p.normalized_ids AS member_id") == 2
    assert f"INNER JOIN {NORMALIZED} AS n FINAL" in body
    assert "ON n.company_id = p.company_id AND n.normalized_id = member_id" in body
    assert f"FROM {MATCH} AS m FINAL" in body
    assert f"    FROM {MATCH_STATE} FINAL" in body
    # The view never reads itself, and nothing about the queue or the response table
    # belongs in it -- it is derived from the person entity alone.
    assert VIEW not in body
    assert QUEUE not in body and RESPONSE not in body
    for column in tables.MATCH_GAP_VIEW_COLUMNS:
        assert f" AS {column}" in body, column


def test_the_two_rules_are_spelled_as_section_5_1_defines_them() -> None:
    """Call name: equal surname token lists, one side's given-token SET a STRICT superset of
    the other's. Double surname: equal given sets, one side's surname two tokens and the
    other's one, and the single token one of the two. Both rules need DISJOINT source sets
    and a non-conflicting birth year, and both normalize the pair with least/greatest so the
    anti-join lines up whichever side the superset sat on."""
    body = _view_body(_sql("up"))

    assert "INNER JOIN members AS b ON a.company_id = b.company_id AND a.surname = b.surname" in body
    assert "AND hasAll(a.given_set, b.given_set)" in body
    assert "AND length(a.given_set) > length(b.given_set)" in body

    assert "INNER JOIN members AS b ON a.company_id = b.company_id AND a.given = b.given" in body
    assert "AND length(a.last_tokens) = 2" in body
    assert "AND length(b.last_tokens) = 1" in body
    assert "AND has(a.last_tokens, b.last_tokens[1])" in body

    assert body.count("AND empty(arrayIntersect(a.sources, b.sources))") == 2
    assert body.count(
        "AND (a.birth_year IS NULL OR b.birth_year IS NULL OR a.birth_year = b.birth_year)"
    ) == 2
    assert body.count("least(a.person_key, b.person_key) AS person_key_a") == 2
    assert body.count("greatest(a.person_key, b.person_key) AS person_key_b") == 2
    assert body.count("WHERE a.person_key != b.person_key") == 2
    # A pair counts only when no stored pair at or above the threshold already joins the two
    # persons: that LEFT ANTI JOIN is what makes this a "still needs work" list.
    assert "LEFT ANTI JOIN matched_pairs AS m" in body
    assert "AND m.person_key_a = g.person_key_a" in body
    assert "AND m.person_key_b = g.person_key_b" in body


def test_the_member_leg_reads_only_ok_rows_of_the_four_machine_sources() -> None:
    """The same gate `match.build_candidates` applies, in SQL: a reviewer row can never
    contribute a member, so a reviewer-only person contributes nothing at all. tables.py
    cannot import match.py or fold.py (both import tables.py), so the literals live in the
    builder and this is the equality that keeps them honest."""
    body = _view_body(_sql("up"))
    sources = ", ".join(f"'{source}'" for source in MACHINE_SOURCES)

    assert f"AND n.source IN ({sources})" in body
    assert f"WHERE p.active = 1 AND n.parse_status = '{FOLDABLE_STATUS}'" in body
    assert MACHINE_SOURCES == ("bolagsverket", "esef", "wikidata", "ratsit")
    assert FOLDABLE_STATUS == "ok"


def test_the_matched_leg_uses_the_folds_threshold_and_an_error_free_state_row() -> None:
    body = _view_body(_sql("up"))

    assert f"WHERE m.confidence >= {MATCH_THRESHOLD}" in body
    assert MATCH_THRESHOLD == 0.8
    assert "WHERE error = ''" in body
    assert "ON s.company_id = m.company_id AND s.input_hash = m.input_hash" in body


def test_the_up_migration_is_two_tables_two_alters_and_one_refreshable_view() -> None:
    statements = _statements(_sql("up"))

    assert len(statements) == 6
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    create = _body(statements[5])
    assert create.startswith(f"CREATE MATERIALIZED VIEW {VIEW}\n")
    assert "\nREFRESH EVERY 1 HOUR OFFSET 30 MINUTE\n" in create
    # The engine lives INSIDE the view (000326/000391/000402's form), so the view IS the
    # table every reader queries and there is no second object to keep in step.
    assert "\nENGINE = MergeTree\n" in create
    assert f"\nORDER BY ({', '.join(tables.MATCH_GAP_VIEW_ORDER_BY)})\n" in create
    # EMPTY: the CREATE returns at once and the first build is an explicit SYSTEM REFRESH
    # VIEW in the runbook -- never a SYSTEM WAIT VIEW, which outlives the migrate client's
    # read_timeout of 300 s and leaves the ledger dirty.
    assert "\nEMPTY\nAS WITH\n" in create
    assert "SYSTEM WAIT VIEW" not in _sql("up")
    # Nothing is destroyed on the way up.
    assert "DROP" not in _executable(_sql("up")).upper()
    assert "TRUNCATE" not in _executable(_sql("up")).upper()


def test_the_pair_alter_is_one_statement_and_carries_no_default_expression() -> None:
    """ClickHouse extends a sorting key only with a column added by the SAME ALTER, and
    refuses a key column that has a DEFAULT EXPRESSION -- proven on 26.5 while this was
    written: `ADD COLUMN request_id String DEFAULT '', MODIFY ORDER BY (...)` fails with
    code 36, BAD_ARGUMENTS. Without the clause the column still stores String's zero value,
    which is the `''` spec section 4.3 asks for. The state table's copy is NOT in a key, so
    it keeps the explicit DEFAULT."""
    statements = _statements(_sql("up"))
    pair_alter = _body(statements[3])
    state_alter = _body(statements[4])

    assert pair_alter.startswith(f"ALTER TABLE {MATCH}\n")
    assert "\n    ADD COLUMN IF NOT EXISTS request_id String,\n" in pair_alter
    assert "DEFAULT" not in pair_alter
    assert pair_alter.endswith(
        "    MODIFY ORDER BY (company_id, candidate_a, candidate_b, request_id)"
    )

    assert state_alter.startswith(f"ALTER TABLE {MATCH_STATE}\n")
    assert state_alter.endswith("    ADD COLUMN IF NOT EXISTS request_id String DEFAULT ''")
    assert "MODIFY ORDER BY" not in state_alter


def test_the_down_migration_removes_everything_it_can() -> None:
    """Forward-only in spirit: the view and the two new tables go, and the state table's
    column goes, but the PAIR table keeps `request_id` -- it is in that table's sorting key
    and ClickHouse cannot shrink a sorting key. Undoing it would mean rebuilding a live
    table, which a down migration must not do."""
    statements = _statements(_sql("down"))

    assert statements[0] == "CREATE DATABASE IF NOT EXISTS corpscout"
    assert _body(statements[1]) == f"DROP VIEW IF EXISTS {VIEW}"
    assert _body(statements[2]) == f"DROP TABLE IF EXISTS {RESPONSE}"
    assert _body(statements[3]) == f"DROP TABLE IF EXISTS {QUEUE}"
    assert _body(statements[4]) == (
        f"ALTER TABLE {MATCH_STATE}\n    DROP COLUMN IF EXISTS request_id"
    )
    assert len(statements) == 5
    # One object in, one object out: the inline engine means the view owns its MergeTree and
    # DROP VIEW takes the data with it.
    assert f"DROP TABLE IF EXISTS {VIEW}" not in _sql("down")
    assert "CREATE MATERIALIZED VIEW" not in _sql("down")
    # The pair table is named ONCE in the down file, in prose, never in a statement.
    assert f"ALTER TABLE {MATCH}\n" not in _executable(_sql("down"))


def test_the_sort_keys_carry_no_nullable_column() -> None:
    """`allow_nullable_key` is off (dagster_v3/CLAUDE.md). Every key column here is a plain
    String or the view's own `company_id`, so nothing needs unwrapping -- and the view's two
    counters are cast to UInt32 in the SELECT rather than left as the aggregate's UInt64."""
    body = _view_body(_sql("up"))

    assert tables.MATCH_GAP_VIEW_ORDER_BY == ("company_id",)
    assert "toUInt32(countIf(g.is_call_name = 1)) AS call_name_pairs" in body
    assert "toUInt32(countIf(g.is_double_surname = 1)) AS double_surname_pairs" in body
    assert "Nullable" not in body
    executable_up = _executable(_sql("up"))
    # Both new tables key on (request_id, company_id) -- two plain Strings -- and neither
    # declares a Nullable column at all.
    assert executable_up.count("ORDER BY (request_id, company_id)") == 2
    assert "Nullable" not in executable_up


def test_the_select_carries_the_same_refresh_bounding_settings_since_000347() -> None:
    """The heaviest hourly refresh this entity owns (spec section 12): two ARRAY JOINs over
    1.27M active persons, a self-join of ~5.8M member rows and an anti-join. It ends with
    the identical trailing SETTINGS block 000347/000391/000402 carry -- copied verbatim, not
    a hand-typed near-copy."""
    settings_block = (
        "SETTINGS join_algorithm = 'grace_hash,hash',\n"
        "    grace_hash_join_initial_buckets = 16,\n"
        "    max_bytes_before_external_group_by = 8589934592,\n"
        "    max_bytes_before_external_sort = 8589934592,\n"
        "    max_memory_usage = 12884901888"
    )

    assert build_se_company_person_match_gap_sql().endswith(settings_block)
    assert _view_body(_sql("up")).endswith(settings_block)
```

- [x] **Step 3: Run it to watch it fail on the missing builder**

```bash
cd corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run --frozen --no-sync pytest tests/test_se_company_person_match_gap_view.py -q
```

Expected: a collection error — `ImportError: cannot import name 'build_se_company_person_match_gap_sql' from 'dagster_v3.defs.se_company.person.tables'`.

- [x] **Step 4: Add the names, the widened tuples and the builder to `tables.py`**

In `src/dagster_v3/defs/se_company/person/tables.py`, replace the module docstring's final sentence (the two lines beginning `Slice 5 added the derived role view`) and the closing `"""` with — this block is a docstring fragment, not a statement:

```python
Slice 5 added the derived role view (ROLE_VIEW, build_se_company_person_role_sql), created by
migration 000402. The LLM-enhance slice 1 (migration 000406) added the per-source-table LLM
pair -- LLM_QUEUE_TABLE and LLM_RESPONSE_TABLE -- the request_id column on both match tables,
and the match-gap view (MATCH_GAP_VIEW, build_se_company_person_match_gap_sql).
"""
```

After `MATCH_STATE_TABLE` (the LLM matching phase block), add the pattern's two tables and the view:

```python
# The LLM-enhancement pattern (spec 2026-09-13 sections 3 and 4, migration 000406): one
# queue table and one response table PER SOURCE TABLE that needs an LLM pass, mapped to that
# table's own unit id -- here company_id, the unit a person-match prompt is built over.
# THESE ARE THE ONLY TWO corpscout TABLES WHOSE NAMES DO NOT START WITH THEIR ENTITY'S
# PREFIX, and that is the pattern's name rather than an oversight: `llm_<step>_<source
# table>` reads as "the LLM queue OF se_company_person". Every string match on a table name
# in this repo compares whole names, so the new prefix breaks nothing.
LLM_QUEUE_TABLE = "llm_queue_se_company_person"
LLM_RESPONSE_TABLE = "llm_response_se_company_person"

# Slice 1 of the same spec (section 5): the companies that still carry a deterministic
# call-name or double-surname gap no stored match has closed. A refreshable materialized
# view over the main table, the normalized rows and the two match tables, rebuilt hourly at
# :30 -- the free half of the hour between the role view's :20 and the serving view's :45.
# DERIVED: nothing in this package writes it, and the fold never reads it.
MATCH_GAP_VIEW = "se_company_person_match_gap"
```

After `QUALIFIED_ROLE_VIEW`, add the three qualified twins:

```python
QUALIFIED_LLM_QUEUE_TABLE = f"{DATABASE}.{LLM_QUEUE_TABLE}"
QUALIFIED_LLM_RESPONSE_TABLE = f"{DATABASE}.{LLM_RESPONSE_TABLE}"
QUALIFIED_MATCH_GAP_VIEW = f"{DATABASE}.{MATCH_GAP_VIEW}"
```

Replace `MATCH_COLUMNS` and `MATCH_STATE_COLUMNS` with the widened tuples:

```python
# request_id is last on BOTH, which is where migration 000406's ADD COLUMN puts it (no
# AFTER clause), and tests/se_company_ddl.py replays that ALTER -- so these tuples are the
# DEPLOYED column lists, not 000399's. On the pair table the column is also the fourth
# component of the sorting key: v1's rows carry '' and every request's rows carry its id, so
# two prompt versions of one candidate pair coexist instead of replacing each other.
MATCH_COLUMNS: tuple[str, ...] = (
    "company_id", "candidate_a", "candidate_b", "members_a", "members_b",
    "source_a", "source_b", "name_a", "name_b", "confidence", "reason",
    "model", "prompt_version", "input_hash", "matched_at", "request_id",
)
# On the state table request_id is NOT in the key: that table is one row per company by
# design -- the certification the fold joins on -- and an apply REPLACES it. The column
# records which request certified the company, which is what a revert deletes by.
MATCH_STATE_COLUMNS: tuple[str, ...] = (
    "company_id", "input_hash", "candidates", "sources", "pairs", "model",
    "prompt_version", "prompt_tokens", "completion_tokens", "raw_response", "error",
    "source_run_id", "matched_at", "request_id",
)
```

At the end of the module, after `build_se_company_person_role_sql()`, add the new tuples and the builder:

```python
# The queue holds the UNIT IDS of one request and nothing else. No version column: within a
# request a company appears once (both minters de-duplicate before inserting) and every
# other column is identical for every row of a request, so there is nothing for a version to
# choose between. request_id leads the key because every read is "this request's companies".
LLM_QUEUE_COLUMNS: tuple[str, ...] = (
    "request_id", "company_id", "queued_at", "queued_by", "note",
)
# One row per company per request, the newest answer winning. `candidates` is the list length
# the answer was produced for (the apply's sanity check and the cost readout) and
# `source_run_id` is the Dagster run that wrote the row.
LLM_RESPONSE_COLUMNS: tuple[str, ...] = (
    "request_id", "company_id", "provider", "model", "prompt_version", "input_hash",
    "candidates", "prompt_tokens", "completion_tokens", "raw_response", "error",
    "attempts", "source_run_id", "responded_at",
)
# The gap view's columns in DDL order, and its sort key. One row per company; the two
# counters are the number of UNCLOSED pairs of each kind.
MATCH_GAP_VIEW_COLUMNS: tuple[str, ...] = (
    "company_id", "call_name_pairs", "double_surname_pairs", "computed_at",
)
MATCH_GAP_VIEW_ORDER_BY: tuple[str, ...] = ("company_id",)


def build_se_company_person_match_gap_sql() -> str:
    """The SELECT behind `corpscout.se_company_person_match_gap` (spec section 5.2).

    One row per company that still carries a deterministic gap the stored matches have not
    closed. Both rules are computed over the MEMBER SPELLINGS of two ACTIVE published
    persons of one company whose source sets are DISJOINT -- the normalized rows the fold
    built each person from, joined back through `normalized_ids` exactly as the role view
    does, because the person row carries the precedence-chosen SPELLING and never the
    tokens. The tokens are the normalizer's: lowercased, diacritic-free, hyphen-split,
    particles glued to the surname.

    - CALL NAME: the two members' surname token lists are EQUAL and one member's set of
      given-name tokens is a STRICT SUPERSET of the other's. Ratsit's `erik bo bengtsson`
      against Bolagsverket's `bo bengtsson`.
    - DOUBLE SURNAME: the two members' given-name token SETS are equal, one member's surname
      is TWO tokens and the other's is ONE, and the single token is one of the two.
      `anna ek svensson` against `anna svensson`.

    A pair counts only when NO stored pair at or above 0.8 already joins the two persons for
    the company's current, error-free input -- that LEFT ANTI JOIN is what makes this a
    "still needs work" list rather than a name-rule report.

    Four things the SQL does on purpose. `least`/`greatest` normalize every pair to one
    direction, so the anti-join lines up whichever side the superset (or the two-token
    surname) sat on. The self-join produces each unordered pair twice; both rules are
    asymmetric, so only one direction survives the WHERE and DISTINCT absorbs the rest. A
    pair cannot be both kinds -- one rule needs equal surnames, the other different ones --
    so the counters never double-count. And a reviewer-only person has no `ok` machine
    member, so it contributes no member row at all, while a reviewer person that carries
    machine members takes part as a person like any other.

    THREE LITERALS ARE SPELLED OUT HERE rather than imported: the four machine sources
    (`match.MACHINE_SOURCES`), the foldable parse status (`fold.FOLDABLE_STATUS`) and the
    0.8 threshold (`fold.MATCH_THRESHOLD`). Both of those modules import THIS one, so the
    import cannot go the other way; tests/test_se_company_person_match_gap_view.py holds the
    equalities instead.

    Ends with the same SETTINGS block every serving refresh has carried since
    000347/000391/000402: grace_hash spill joins, external group-by/sort and a 12 GiB cap.
    This is the heaviest hourly refresh the entity owns and that block is why it fits.

    THE VIEW IS DERIVED AND NOTHING WRITES IT. It lags a fold by at most an hour, and until
    the first manual refresh lands it answers with zero rows -- which every reader treats as
    "no gap".
    """
    return f"""WITH
members AS (
    SELECT
        p.company_id AS company_id,
        p.person_key AS person_key,
        p.birth_year AS birth_year,
        p.sources AS sources,
        arrayStringConcat(n.last_tokens, ' ') AS surname,
        arraySort(arrayDistinct(arrayConcat(n.first_tokens, n.middle_tokens))) AS given_set,
        arrayStringConcat(arraySort(arrayDistinct(arrayConcat(n.first_tokens, n.middle_tokens))), ' ') AS given,
        n.last_tokens AS last_tokens
    FROM {QUALIFIED_MAIN_TABLE} AS p FINAL
    ARRAY JOIN p.normalized_ids AS member_id
    INNER JOIN {QUALIFIED_NORMALIZED_TABLE} AS n FINAL
      ON n.company_id = p.company_id AND n.normalized_id = member_id
    WHERE p.active = 1 AND n.parse_status = 'ok'
      AND n.source IN ('bolagsverket', 'esef', 'wikidata', 'ratsit')
),
member_person AS (
    SELECT p.company_id AS company_id, member_id AS normalized_id, p.person_key AS person_key
    FROM {QUALIFIED_MAIN_TABLE} AS p FINAL
    ARRAY JOIN p.normalized_ids AS member_id
    WHERE p.active = 1
),
matched AS (
    SELECT m.company_id AS company_id, m.members_a AS members_a, m.members_b AS members_b
    FROM {QUALIFIED_MATCH_TABLE} AS m FINAL
    INNER JOIN (
        SELECT company_id, input_hash
        FROM {QUALIFIED_MATCH_STATE_TABLE} FINAL
        WHERE error = ''
    ) AS s ON s.company_id = m.company_id AND s.input_hash = m.input_hash
    WHERE m.confidence >= 0.8
),
matched_left AS (
    SELECT company_id, member_a, members_b FROM matched ARRAY JOIN members_a AS member_a
),
matched_ids AS (
    SELECT company_id, member_a, member_b FROM matched_left ARRAY JOIN members_b AS member_b
),
matched_pairs AS (
    SELECT DISTINCT
        ka.company_id AS company_id,
        least(ka.person_key, kb.person_key) AS person_key_a,
        greatest(ka.person_key, kb.person_key) AS person_key_b
    FROM matched_ids AS mi
    INNER JOIN member_person AS ka
      ON ka.company_id = mi.company_id AND ka.normalized_id = mi.member_a
    INNER JOIN member_person AS kb
      ON kb.company_id = mi.company_id AND kb.normalized_id = mi.member_b
    WHERE ka.person_key != kb.person_key
),
call_name AS (
    SELECT DISTINCT
        a.company_id AS company_id,
        least(a.person_key, b.person_key) AS person_key_a,
        greatest(a.person_key, b.person_key) AS person_key_b
    FROM members AS a
    INNER JOIN members AS b ON a.company_id = b.company_id AND a.surname = b.surname
    WHERE a.person_key != b.person_key
      AND empty(arrayIntersect(a.sources, b.sources))
      AND hasAll(a.given_set, b.given_set)
      AND length(a.given_set) > length(b.given_set)
      AND (a.birth_year IS NULL OR b.birth_year IS NULL OR a.birth_year = b.birth_year)
),
double_surname AS (
    SELECT DISTINCT
        a.company_id AS company_id,
        least(a.person_key, b.person_key) AS person_key_a,
        greatest(a.person_key, b.person_key) AS person_key_b
    FROM members AS a
    INNER JOIN members AS b ON a.company_id = b.company_id AND a.given = b.given
    WHERE a.person_key != b.person_key
      AND empty(arrayIntersect(a.sources, b.sources))
      AND length(a.last_tokens) = 2
      AND length(b.last_tokens) = 1
      AND has(a.last_tokens, b.last_tokens[1])
      AND (a.birth_year IS NULL OR b.birth_year IS NULL OR a.birth_year = b.birth_year)
),
gap AS (
    SELECT company_id, person_key_a, person_key_b, 1 AS is_call_name, 0 AS is_double_surname
    FROM call_name
    UNION ALL
    SELECT company_id, person_key_a, person_key_b, 0 AS is_call_name, 1 AS is_double_surname
    FROM double_surname
)
SELECT
    g.company_id AS company_id,
    toUInt32(countIf(g.is_call_name = 1)) AS call_name_pairs,
    toUInt32(countIf(g.is_double_surname = 1)) AS double_surname_pairs,
    now64(3, 'UTC') AS computed_at
FROM gap AS g
LEFT ANTI JOIN matched_pairs AS m
  ON m.company_id = g.company_id
 AND m.person_key_a = g.person_key_a
 AND m.person_key_b = g.person_key_b
GROUP BY g.company_id
SETTINGS join_algorithm = 'grace_hash,hash',
    grace_hash_join_initial_buckets = 16,
    max_bytes_before_external_group_by = 8589934592,
    max_bytes_before_external_sort = 8589934592,
    max_memory_usage = 12884901888"""
```

- [x] **Step 5: Give `match.py`'s two row builders the new column**

`match_row` and `match_state_row` build their tuple as `tuple(values[column] for column in tables.MATCH_COLUMNS)`, so a column added to the tuple without a value raises `KeyError`. In `src/dagster_v3/defs/se_company/person/match.py`, add one keyword-only parameter and one dict entry to each — nothing else in the file moves.

In `match_row`, replace everything from the signature's last parameter through the end of the docstring with — `matched_at: datetime,` is repeated here as the anchor, not as a second copy:

```python
    matched_at: datetime,
    request_id: str = "",
) -> tuple[Any, ...]:
    """One insert tuple in tables.MATCH_COLUMNS order.

    `request_id` is the fourth component of the pair table's sorting key since migration
    000406: the match asset leaves it empty, which is what every v1 row carries, and the
    LLM-enhance apply passes the request it is applying so that request's pairs are their
    own rows and can be reverted on their own.
    """
```

and in its `values` dict, after `"input_hash": input_hash, "matched_at": matched_at,`:

```python
        "request_id": request_id,
```

In `match_state_row`, the same replacement — signature tail plus docstring, with `matched_at: datetime,` as the anchor:

```python
    matched_at: datetime,
    request_id: str = "",
) -> tuple[Any, ...]:
    """One insert tuple in tables.MATCH_STATE_COLUMNS order.

    `request_id` records which request certified the company (migration 000406). It is NOT
    in this table's key -- one row per company, replaced by whoever certifies it last.
    """
```

and in its `values` dict, after `"source_run_id": source_run_id, "matched_at": matched_at,`:

```python
        "request_id": request_id,
```

- [x] **Step 6: Run the pin test again to watch it fail on the missing migration**

```bash
cd corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run --frozen --no-sync pytest tests/test_se_company_person_match_gap_view.py -q
```

Expected: every test in the file errors with `FileNotFoundError: … 000406_corpscout_se_company_person_llm_enhance.up.sql` — the builder now imports, and the migration is what is missing.

- [x] **Step 7: Write the migration**

Create `corpscout/clickhouse/migrations/000406_corpscout_se_company_person_llm_enhance.up.sql`. **No `;` may appear in any `--` comment**, the file must end with a statement, and the `ADD COLUMN` clauses must each sit alone on their line (`tests/se_company_ddl.py::_column_changes` replays them line by line):

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- THE LLM-ENHANCEMENT PATTERN, FIRST USE (spec 2026-09-13 sections 3 and 4, slice 1). An
-- LLM pass over a source table is a QUEUE, a RESPONSE table and four steps -- queue, run,
-- apply, clean up -- and every one of them belongs to the table it enhances. Per source
-- table X that needs LLM augmentation there are exactly two tables, llm_queue_<X> and
-- llm_response_<X>, mapped to X's own unit id. Nothing generic goes into the schema: a
-- shared llm_queue would need a string entity discriminator in its sort key, an unconstrained
-- unit id, and one table's retention policy imposed on every consumer.
--
-- THESE ARE THE ONLY TWO corpscout TABLES WHOSE NAMES DO NOT START WITH THEIR ENTITY'S
-- PREFIX. That is the pattern's name, not an oversight: llm_<step>_<source table> reads as
-- "the LLM queue OF se_company_person". Every string match on a table name in this repo
-- compares whole names, so nothing is broken by the new prefix.
--
-- THE UNIT IS THE COMPANY, because that is what a person-match prompt is built over: all of
-- a company's people go into one prompt, since identity is decided by comparing them with
-- each other. The queue therefore holds company ids, not person keys -- even when the
-- reviewer reached it from a person row.

-- The unit ids of one request and nothing else. No version column: within a request a
-- company appears once (both minters de-duplicate before inserting) and every column beside
-- the key is identical for every row of a request, so there is nothing for a version to
-- choose between. request_id leads the sort key because every read is "this request's
-- companies".
CREATE TABLE IF NOT EXISTS corpscout.llm_queue_se_company_person
(
    request_id String,
    company_id String,
    queued_at DateTime64(3, 'UTC'),
    queued_by String,
    note String DEFAULT '',
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree
ORDER BY (request_id, company_id);

-- One row per company per request, the newest answer winning. Nothing downstream reads it:
-- it is the paid evidence, and the apply step's input. Two columns go beyond the owner's
-- list and both earn their place -- `candidates` is the list length the answer was produced
-- for, which is the apply's sanity check and the cost readout, and `source_run_id` is the
-- Dagster run that wrote the row, which is how a Requests page links to the last run without
-- a tag query against Dagster.
--
-- RE-RUNNING THE SAME REQUEST UNDER A DIFFERENT PROMPT VERSION REPLACES THAT REQUEST'S
-- RESPONSES. A request holds exactly one effective answer per company, the most recent
-- run's. To compare two prompts, queue two requests.
CREATE TABLE IF NOT EXISTS corpscout.llm_response_se_company_person
(
    request_id String,
    company_id String,
    provider LowCardinality(String),
    model LowCardinality(String),
    prompt_version LowCardinality(String),
    input_hash FixedString(64),
    candidates UInt16,
    prompt_tokens UInt32,
    completion_tokens UInt32,
    raw_response String,
    error String DEFAULT '',
    attempts UInt8,
    source_run_id String,
    responded_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(responded_at)
ORDER BY (request_id, company_id);

-- WHY THE PAIR TABLE'S SORT KEY HAS TO GROW. Without request_id in it, a v2 pair row
-- REPLACES the v1 row for the same candidate pair, so a v2 that scores a pair lower than v1
-- silently unmerges a person the moment the next fold runs. With it, v1's rows (request_id
-- empty) and every request's rows coexist as distinct rows, and the fold takes the MAXIMUM
-- confidence across them. It is also what makes a revert exact: deleting a request's pair
-- rows cannot touch another request's.
--
-- ONE STATEMENT ON PURPOSE. ClickHouse extends a sorting key only with a column added by the
-- SAME ALTER, and the column must take the type's zero value so that every stored row's new
-- key component is identical and no part has to be re-sorted -- metadata only, no rewrite.
-- Two separate statements are refused.
--
-- AND NO DEFAULT CLAUSE. `ADD COLUMN request_id String DEFAULT ''` with a MODIFY ORDER BY is
-- refused on 26.5 with code 36, BAD_ARGUMENTS, "Newly added column request_id has a default
-- expression, so adding expressions that use it to the sorting key is forbidden". Without
-- the clause the column still stores String's zero value, which is the empty string this
-- design wants -- it is the DEFAULT EXPRESSION, not the value, that a key column may not
-- carry.
ALTER TABLE corpscout.se_company_person_match
    ADD COLUMN IF NOT EXISTS request_id String,
    MODIFY ORDER BY (company_id, candidate_a, candidate_b, request_id);

-- The state table's sort key is NOT extended: it is one row per company by design -- the
-- certification the fold joins on -- and an apply REPLACES it. request_id here records which
-- request certified the company, which is what a revert deletes by. The column is not in a
-- key, so it keeps its explicit DEFAULT.
ALTER TABLE corpscout.se_company_person_match_state
    ADD COLUMN IF NOT EXISTS request_id String DEFAULT '';

-- THE MATCH-GAP VIEW (spec section 5): one row per company that still carries a
-- deterministic call-name or double-surname gap no stored match at or above 0.8 has closed.
-- It is what turns "re-send everything" into a named, affordable request.
--
-- DERIVED, NOT WRITTEN. Nothing inserts into it. The fold, the normalizer, the reviewer
-- rules, the precedence and the match asset are untouched by this migration. The view is
-- recomputed whole on every refresh, so it cannot drift from the four tables it reads -- it
-- can only LAG them, by at most one hour.
--
-- THE REFRESHABLE FORM THIS REPO USES (000326, 000335, 000391, 000392, 000402): the engine
-- is declared INSIDE the view, so corpscout.se_company_person_match_gap IS the MergeTree
-- readers query and there is no separate target table to keep in step. The down file's DROP
-- VIEW takes that inner table with it.
--
-- REFRESH AT :30. se_company_person_role rebuilds at :20 and se_companies_serving at :45 for
-- 13 to 15 minutes -- :30 is the free half of the hour.
--
-- CREATED EMPTY, FIRST BUILD BY HAND. This is the heaviest hourly refresh the person entity
-- owns: two ARRAY JOINs over 1.27M active persons, a self-join of about 5.8M member rows and
-- an anti-join. The migrate client's read_timeout is 300 seconds, so EMPTY skips the initial
-- refresh, this CREATE returns in milliseconds, and the ledger can never be left dirty by a
-- dropped client. The controller then runs SYSTEM REFRESH VIEW
-- corpscout.se_company_person_match_gap and polls system.view_refreshes. Until that lands
-- the view answers with zero rows, which every reader treats as "no gap".
--
-- ALSO BOUNDS THE REFRESH ITSELF: the SELECT carries the same trailing SETTINGS every
-- serving refresh has carried since 000347/000391 -- grace_hash spill joins, external
-- group-by and sort, and a 12 GiB max_memory_usage. If the first build exceeds ten minutes
-- the refresh moves to every 6 hours rather than the cap growing.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- person/tables.py::build_se_company_person_match_gap_sql(), drift-pinned by dagster_v3
-- tests/test_se_company_person_match_gap_view.py.

CREATE MATERIALIZED VIEW corpscout.se_company_person_match_gap
REFRESH EVERY 1 HOUR OFFSET 30 MINUTE
ENGINE = MergeTree
ORDER BY (company_id)
EMPTY
AS WITH
members AS (
    SELECT
        p.company_id AS company_id,
        p.person_key AS person_key,
        p.birth_year AS birth_year,
        p.sources AS sources,
        arrayStringConcat(n.last_tokens, ' ') AS surname,
        arraySort(arrayDistinct(arrayConcat(n.first_tokens, n.middle_tokens))) AS given_set,
        arrayStringConcat(arraySort(arrayDistinct(arrayConcat(n.first_tokens, n.middle_tokens))), ' ') AS given,
        n.last_tokens AS last_tokens
    FROM corpscout.se_company_person AS p FINAL
    ARRAY JOIN p.normalized_ids AS member_id
    INNER JOIN corpscout.se_company_person_normalized AS n FINAL
      ON n.company_id = p.company_id AND n.normalized_id = member_id
    WHERE p.active = 1 AND n.parse_status = 'ok'
      AND n.source IN ('bolagsverket', 'esef', 'wikidata', 'ratsit')
),
member_person AS (
    SELECT p.company_id AS company_id, member_id AS normalized_id, p.person_key AS person_key
    FROM corpscout.se_company_person AS p FINAL
    ARRAY JOIN p.normalized_ids AS member_id
    WHERE p.active = 1
),
matched AS (
    SELECT m.company_id AS company_id, m.members_a AS members_a, m.members_b AS members_b
    FROM corpscout.se_company_person_match AS m FINAL
    INNER JOIN (
        SELECT company_id, input_hash
        FROM corpscout.se_company_person_match_state FINAL
        WHERE error = ''
    ) AS s ON s.company_id = m.company_id AND s.input_hash = m.input_hash
    WHERE m.confidence >= 0.8
),
matched_left AS (
    SELECT company_id, member_a, members_b FROM matched ARRAY JOIN members_a AS member_a
),
matched_ids AS (
    SELECT company_id, member_a, member_b FROM matched_left ARRAY JOIN members_b AS member_b
),
matched_pairs AS (
    SELECT DISTINCT
        ka.company_id AS company_id,
        least(ka.person_key, kb.person_key) AS person_key_a,
        greatest(ka.person_key, kb.person_key) AS person_key_b
    FROM matched_ids AS mi
    INNER JOIN member_person AS ka
      ON ka.company_id = mi.company_id AND ka.normalized_id = mi.member_a
    INNER JOIN member_person AS kb
      ON kb.company_id = mi.company_id AND kb.normalized_id = mi.member_b
    WHERE ka.person_key != kb.person_key
),
call_name AS (
    SELECT DISTINCT
        a.company_id AS company_id,
        least(a.person_key, b.person_key) AS person_key_a,
        greatest(a.person_key, b.person_key) AS person_key_b
    FROM members AS a
    INNER JOIN members AS b ON a.company_id = b.company_id AND a.surname = b.surname
    WHERE a.person_key != b.person_key
      AND empty(arrayIntersect(a.sources, b.sources))
      AND hasAll(a.given_set, b.given_set)
      AND length(a.given_set) > length(b.given_set)
      AND (a.birth_year IS NULL OR b.birth_year IS NULL OR a.birth_year = b.birth_year)
),
double_surname AS (
    SELECT DISTINCT
        a.company_id AS company_id,
        least(a.person_key, b.person_key) AS person_key_a,
        greatest(a.person_key, b.person_key) AS person_key_b
    FROM members AS a
    INNER JOIN members AS b ON a.company_id = b.company_id AND a.given = b.given
    WHERE a.person_key != b.person_key
      AND empty(arrayIntersect(a.sources, b.sources))
      AND length(a.last_tokens) = 2
      AND length(b.last_tokens) = 1
      AND has(a.last_tokens, b.last_tokens[1])
      AND (a.birth_year IS NULL OR b.birth_year IS NULL OR a.birth_year = b.birth_year)
),
gap AS (
    SELECT company_id, person_key_a, person_key_b, 1 AS is_call_name, 0 AS is_double_surname
    FROM call_name
    UNION ALL
    SELECT company_id, person_key_a, person_key_b, 0 AS is_call_name, 1 AS is_double_surname
    FROM double_surname
)
SELECT
    g.company_id AS company_id,
    toUInt32(countIf(g.is_call_name = 1)) AS call_name_pairs,
    toUInt32(countIf(g.is_double_surname = 1)) AS double_surname_pairs,
    now64(3, 'UTC') AS computed_at
FROM gap AS g
LEFT ANTI JOIN matched_pairs AS m
  ON m.company_id = g.company_id
 AND m.person_key_a = g.person_key_a
 AND m.person_key_b = g.person_key_b
GROUP BY g.company_id
SETTINGS join_algorithm = 'grace_hash,hash',
    grace_hash_join_initial_buckets = 16,
    max_bytes_before_external_group_by = 8589934592,
    max_bytes_before_external_sort = 8589934592,
    max_memory_usage = 12884901888;
```

Create `corpscout/clickhouse/migrations/000406_corpscout_se_company_person_llm_enhance.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Undoes 000406 as far as ClickHouse allows. The view owns its MergeTree (the engine is
-- declared inside it), so one DROP VIEW removes the definition and the data together. The
-- two new tables go with it -- the response table first, because it is the evidence a queue
-- row points at, and nothing else in the database references either of them.
--
-- THE PAIR TABLE KEEPS request_id AND THAT IS DELIBERATE. The column is in
-- corpscout.se_company_person_match's SORTING KEY, and ClickHouse cannot shrink a sorting
-- key: undoing it would mean rebuilding a live table inside a down migration, which this
-- ledger does not do. The column is harmless to every reader that does not name it -- it
-- stores String's zero value on every row the match asset wrote -- and a re-applied 000406
-- finds it already there, because the ALTER is written IF NOT EXISTS.
--
-- The STATE table's column is not in a key, so it can go and does.

DROP VIEW IF EXISTS corpscout.se_company_person_match_gap;

DROP TABLE IF EXISTS corpscout.llm_response_se_company_person;

DROP TABLE IF EXISTS corpscout.llm_queue_se_company_person;

ALTER TABLE corpscout.se_company_person_match_state
    DROP COLUMN IF EXISTS request_id;
```

- [x] **Step 8: Run the pin test to green**

```bash
cd corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run --frozen --no-sync pytest tests/test_se_company_person_match_gap_view.py -q
```

Expected: **10 passed**. A failure in `test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it` means the migration's SELECT and the builder's render differ — fix the MIGRATION by pasting the render, never the builder.

- [x] **Step 9: Watch the ledger and table suites break, then register 000406**

```bash
cd corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_se_company_person_tables.py -q
```

Expected, three named failures — record them before fixing:
- `test_clickhouse_migration_files_are_explicit` — two files on disk that `EXPECTED_MIGRATIONS` does not name;
- `test_match_table_is_one_row_per_unordered_pair` — `declared_columns("se_company_person_match")` now replays 000406's `ADD COLUMN` and returns 16 names against the 15 the tuple had (fixed in step 4, so this one is green already if step 4 landed — if it is still red, the ALTER's formatting is wrong: the clause must be alone on its line);
- `test_the_match_tables_join_the_entitys_column_tuples` — the two hard-coded tuple literals in the test still hold 15 and 13 names.

In `tests/test_clickhouse_migrations.py`, add the entry to `EXPECTED_MIGRATIONS` immediately after `"000404_corpscout_se_financial_readers_entity",`:

```python
    "000406_corpscout_se_company_person_llm_enhance",
)
```

and add this test at the END of the file, after `test_000404_repoints_the_two_financial_readers_to_the_entity`:

```python
def test_000406_pairs_the_llm_queue_and_response_and_widens_the_match_sort_key() -> None:
    """LLM-enhance slice 1 (spec 2026-09-13 sections 4 and 5): two tables of the pattern, two
    alters and one refreshable view, in that order. The pair-table alter is ONE statement --
    ClickHouse extends a sorting key only with a column added by the same ALTER, and refuses
    one that carries a DEFAULT expression -- and the view is created EMPTY with the hourly
    refresh, so the migrate client's 300 s read timeout can never leave the ledger dirty. The
    view's body is drift-pinned in test_se_company_person_match_gap_view.py."""
    up = _migration_sql("000406_corpscout_se_company_person_llm_enhance.up.sql")
    down = _migration_sql("000406_corpscout_se_company_person_llm_enhance.down.sql")
    executable_up = "\n".join(line.split("--")[0] for line in up.splitlines())

    assert up.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    for table in ("llm_queue_se_company_person", "llm_response_se_company_person"):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n" in up, table
        assert f"DROP TABLE IF EXISTS corpscout.{table};" in down, table
    assert executable_up.count("CREATE TABLE IF NOT EXISTS") == 2
    assert executable_up.count("ORDER BY (request_id, company_id)") == 2
    assert "ENGINE = ReplacingMergeTree\n" in up            # the queue has no version column
    assert "ENGINE = ReplacingMergeTree(responded_at)" in up
    assert up.count(
        "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')"
    ) == 2

    # ONE statement for the pair table, with both clauses and no DEFAULT expression.
    [pair_alter] = [
        statement
        for statement in executable_up.split(";")
        if "ALTER TABLE corpscout.se_company_person_match\n" in statement
    ]
    assert "ADD COLUMN IF NOT EXISTS request_id String," in pair_alter
    assert "MODIFY ORDER BY (company_id, candidate_a, candidate_b, request_id)" in pair_alter
    assert "DEFAULT" not in pair_alter
    # The state table gains the column and keeps its one-row-per-company key.
    assert (
        "ALTER TABLE corpscout.se_company_person_match_state\n"
        "    ADD COLUMN IF NOT EXISTS request_id String DEFAULT '';"
    ) in up
    assert executable_up.count("MODIFY ORDER BY") == 1

    # The refreshable view, created EMPTY at :30, never waited on.
    assert "CREATE MATERIALIZED VIEW corpscout.se_company_person_match_gap\n" in up
    assert "REFRESH EVERY 1 HOUR OFFSET 30 MINUTE\n" in up
    assert "\nEMPTY\nAS WITH\n" in up
    assert "SYSTEM WAIT VIEW" not in up
    assert "DROP" not in executable_up.upper()

    # The down file removes what it can and says why the pair column stays.
    assert "DROP VIEW IF EXISTS corpscout.se_company_person_match_gap;" in down
    assert (
        "ALTER TABLE corpscout.se_company_person_match_state\n"
        "    DROP COLUMN IF EXISTS request_id;"
    ) in down
    executable_down = "\n".join(line.split("--")[0] for line in down.splitlines())
    assert "ALTER TABLE corpscout.se_company_person_match\n" not in executable_down
```

- [x] **Step 10: Pin the two new tables and the view's constants beside the other DDL pins**

In `tests/test_se_company_person_tables.py`, update the two tuple literals inside `test_the_match_tables_join_the_entitys_column_tuples` so they end with `"request_id"`:

```python
    assert tables.MATCH_COLUMNS == (
        "company_id", "candidate_a", "candidate_b", "members_a", "members_b",
        "source_a", "source_b", "name_a", "name_b", "confidence", "reason",
        "model", "prompt_version", "input_hash", "matched_at", "request_id",
    )
    assert tables.MATCH_STATE_COLUMNS == (
        "company_id", "input_hash", "candidates", "sources", "pairs", "model",
        "prompt_version", "prompt_tokens", "completion_tokens", "raw_response", "error",
        "source_run_id", "matched_at", "request_id",
    )
```

In `test_match_table_is_one_row_per_unordered_pair`, replace the comment above the `input_hash`-not-in-the-key assertion so it says what the DEPLOYED key is, and add the two lines that pin the alter's effect (the `declared_columns` assertion at the top of that test already replays it):

```python
    # 000399 DECLARES the three-column key; migration 000406 extends it to
    # (company_id, candidate_a, candidate_b, request_id), which is why the deployed key is
    # asserted in test_se_company_person_match_gap_view.py against the ALTER and here only
    # against this file's own CREATE. input_hash is in NEITHER key, so it versions nothing: a
    # re-match of the same pair under the same request REPLACES its row. What supersedes a
    # previous input is the fold's join on the state row's hash (batch.match_pairs_sql).
    assert "input_hash" not in block.split("ORDER BY", 1)[1]
    assert "request_id" not in block              # added by 000406, not declared here
    assert tables.MATCH_COLUMNS[-1] == "request_id"
```

and the same pair of lines at the end of `test_match_state_is_one_row_per_matched_company`:

```python
    # 000406 adds request_id here too -- not in the key: one row per company, replaced by
    # whoever certifies it last, and the column records which request did.
    assert "request_id" not in block
    assert tables.MATCH_STATE_COLUMNS[-1] == "request_id"
```

Then add three tests after `test_the_role_view_constants_describe_the_slice_5_view`:

```python
def test_the_llm_queue_table_holds_one_requests_unit_ids() -> None:
    """Migration 000406, spec section 4.1. The queue is the unit ids of one request and
    nothing else: no version column, because within a request a company appears once and
    every column beside the key is identical for every row of that request."""
    block = table_block("llm_queue_se_company_person")
    assert declared_columns("llm_queue_se_company_person") == list(tables.LLM_QUEUE_COLUMNS)
    assert tables.LLM_QUEUE_COLUMNS == (
        "request_id", "company_id", "queued_at", "queued_by", "note",
    )
    assert "ENGINE = ReplacingMergeTree\n" in block
    assert "ORDER BY (request_id, company_id)" in block
    assert COMPANY_ID_CHECK in block
    assert "    request_id String," in block
    assert "    queued_at DateTime64(3, 'UTC')," in block
    assert "    note String DEFAULT ''," in block
    # No version column and no data column, so no valid_data constraint either.
    assert "JSONType" not in block


def test_the_llm_response_table_is_one_answer_per_request_and_company() -> None:
    """Spec section 4.2. The paid evidence and the apply step's input: the newest answer for
    a company within a request wins, and re-running the same request under another prompt
    version REPLACES that request's answers rather than adding to them."""
    block = table_block("llm_response_se_company_person")
    assert declared_columns("llm_response_se_company_person") == list(
        tables.LLM_RESPONSE_COLUMNS
    )
    assert tables.LLM_RESPONSE_COLUMNS == (
        "request_id", "company_id", "provider", "model", "prompt_version", "input_hash",
        "candidates", "prompt_tokens", "completion_tokens", "raw_response", "error",
        "attempts", "source_run_id", "responded_at",
    )
    assert "ENGINE = ReplacingMergeTree(responded_at)" in block
    assert "ORDER BY (request_id, company_id)" in block
    assert COMPANY_ID_CHECK in block
    assert "    provider LowCardinality(String)," in block
    assert "    prompt_version LowCardinality(String)," in block
    # input_hash is why this table is typed to the person match and not generic: only this
    # unit has one, and the apply's staleness check is what makes it load-bearing.
    assert "    input_hash FixedString(64)," in block
    assert "    candidates UInt16," in block
    assert "    attempts UInt8," in block
    assert "    error String DEFAULT ''," in block
    assert "    raw_response String," in block


def test_the_pattern_tables_are_the_only_two_without_the_entity_prefix() -> None:
    """Spec section 12's ruling, asserted rather than remembered: `llm_<step>_<source table>`
    is the pattern's name. Both names END with the entity's name instead of starting with
    it, and whole-name matching is what keeps that harmless."""
    for name in (tables.LLM_QUEUE_TABLE, tables.LLM_RESPONSE_TABLE):
        assert name.endswith(f"_{tables.MAIN_TABLE}")
        assert not name.startswith(tables.MAIN_TABLE)
    assert tables.QUALIFIED_LLM_QUEUE_TABLE == "corpscout.llm_queue_se_company_person"
    assert tables.QUALIFIED_LLM_RESPONSE_TABLE == "corpscout.llm_response_se_company_person"


def test_the_match_gap_view_constants_describe_the_slice_1_view() -> None:
    """Migration 000406's derived view (spec section 5). Like the role view it carries the
    entity's prefix and is not a table, so whole-name matching applies to it -- and it is a
    prefix collision waiting to happen with se_company_person_match, which is why nothing
    ever matches the pair table's name without the token that follows it."""
    assert tables.MATCH_GAP_VIEW == "se_company_person_match_gap"
    assert tables.QUALIFIED_MATCH_GAP_VIEW == "corpscout.se_company_person_match_gap"
    assert tables.MATCH_GAP_VIEW.startswith(f"{tables.MATCH_TABLE}_")
    assert tables.MATCH_GAP_VIEW != tables.MATCH_TABLE
    assert tables.MATCH_GAP_VIEW_COLUMNS == (
        "company_id", "call_name_pairs", "double_surname_pairs", "computed_at",
    )
    assert tables.MATCH_GAP_VIEW_ORDER_BY == ("company_id",)
    for column in tables.MATCH_GAP_VIEW_ORDER_BY:
        assert column in tables.MATCH_GAP_VIEW_COLUMNS, column
```

- [x] **Step 11: Replay 000406's alters in the fold's clickhouse-local test**

`tests/test_se_company_person_fold_clickhouse_local.py` builds its match INSERTs from `tables.MATCH_COLUMNS` / `MATCH_STATE_COLUMNS` but replays only 000396's and 000399's `CREATE TABLE`s, so it now names a column its fixture schema does not have. Add the migration beside the other two, near `MATCH_MIGRATION_FILE`:

```python
ENHANCE_MIGRATION_FILE = "000406_corpscout_se_company_person_llm_enhance.up.sql"
```

extend `_schema_statements()`'s loop and its filter — the docstring gains one sentence, and only the two `ALTER`s of 000406 are replayed (never its two new tables and never its refreshable view, which this fixture has no use for):

```python
def _schema_statements() -> list[str]:
    """CREATE DATABASE plus the six CREATE TABLEs of 000396 and the two of 000399, then the
    two ALTER TABLEs of 000406 -- never 000396's SYSTEM STOP/START VIEW or ALTER TABLE ...
    MODIFY QUERY, which name se_companies_serving, a view this fixture does not build, and
    never 000406's two new tables or its refreshable view, which the fold never reads.
    000396 declares the main table under its build name and 000398 renames the DEPLOYED table
    without touching that file, so the rename is replayed here: batch.py reads
    tables.QUALIFIED_MAIN_TABLE, which is the renamed name. The 000406 alters matter because
    tables.MATCH_COLUMNS and MATCH_STATE_COLUMNS are the DEPLOYED column lists and both end
    with request_id."""
    statements: list[str] = []
    for name in (MIGRATION_FILE, MATCH_MIGRATION_FILE, ENHANCE_MIGRATION_FILE):
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith("CREATE DATABASE") or (
                "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_" in statement
            ) or statement.startswith((
                f"ALTER TABLE {tables.QUALIFIED_MATCH_TABLE}\n",
                f"ALTER TABLE {tables.QUALIFIED_MATCH_STATE_TABLE}\n",
            )):
                statements.append(
                    statement.replace(
                        "corpscout.se_company_person_v2", tables.QUALIFIED_MAIN_TABLE
                    )
                )
    return statements
```

and give both fixture rows their new last value — `_match_insert`:

```python
    row = (company_id, low, high, [low], [high], sources[low], sources[high],
           names[low], names[high], 0.93, "call name",
           "deepseek-v4-flash", "se-person-match-v1", MATCH_HASH, MATCHED_AT, "")
```

and `_match_state_insert`:

```python
    row = (company_id, MATCH_HASH, 2, 2, 1, "deepseek-v4-flash", "se-person-match-v1",
           480, 60, MATCH_ANSWER, "", "run-match", MATCHED_AT, "")
```

The empty string is what every pre-000406 row carries and what the match asset keeps writing: this fixture is v1's world, and the fold must behave in it exactly as it did before the key grew.

- [x] **Step 12: Run everything this task touches, then commit**

```bash
cd corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run --frozen --no-sync dg check defs
uv run --frozen --no-sync pytest tests/test_se_company_person_match_gap_view.py \
  tests/test_se_company_person_tables.py tests/test_clickhouse_migrations.py \
  tests/test_se_company_person_match.py tests/test_se_company_person_batch.py \
  tests/test_se_company_person_fold.py tests/test_se_company_person_role_view.py \
  tests/test_se_company_person_fold_clickhouse_local.py -q
```

Expected: `dg check defs` green (no definition changed, but the package imports), and every suite green — the pin file's 10, the four new table tests, the new migration test, and the fold's clickhouse-local rounds passing with the widened tuples. `tests/test_se_company_person_fold_clickhouse_local.py` runs the ClickHouse image under Docker and takes a few minutes; if it SKIPS, say it skipped.

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
cat > /tmp/llm-enhance-task1.txt <<'MSG'
feat(se-person): LLM queue and response tables, request_id in the pair key, the match-gap view

Migration 000406 puts the LLM-enhancement pattern's storage on the ground for the
person match. corpscout.llm_queue_se_company_person holds the company ids of one
request; corpscout.llm_response_se_company_person holds one answer per company per
request -- the paid evidence and the apply step's input. Both key on
(request_id, company_id), because every read is "this request's companies".

se_company_person_match gains request_id as the fourth component of its sorting key,
in ONE ALTER with the ADD COLUMN, which is the only form ClickHouse accepts -- and
without a DEFAULT clause, which it refuses outright on a key column (code 36) even
though the stored value is the same empty string. That is what stops a second prompt
version from replacing a v1 pair and silently unmerging a person. The state table
gains the column outside its key, to record which request certified the company.

corpscout.se_company_person_match_gap is the refreshable view that names the
companies still carrying a call-name or double-surname gap no stored pair at or above
0.8 has closed. Its SELECT lives in person/tables.py::build_se_company_person_match_gap_sql()
and the migration body is pinned against a fresh render, as the role and serving views
are. It refreshes hourly at :30 -- the free half of the hour -- and is created EMPTY so
the first build is an explicit SYSTEM REFRESH VIEW.

tests/se_company_ddl.py replays later ALTERs, so MATCH_COLUMNS and MATCH_STATE_COLUMNS
gain request_id with the migration and match_row/match_state_row take it as a defaulted
keyword: the match asset keeps writing what it always wrote, now spelled out as ''.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/clickhouse/migrations/000406_corpscout_se_company_person_llm_enhance.up.sql \
  corpscout/clickhouse/migrations/000406_corpscout_se_company_person_llm_enhance.down.sql \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/tables.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/match.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_match_gap_view.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_tables.py \
  corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_fold_clickhouse_local.py
git commit -F /tmp/llm-enhance-task1.txt
```

(Run the two `git` commands from the worktree root, not from `dagster_v3`.)

---

### Task 2: The clickhouse-local proof — the alter on a populated table, and the gap view's six fixtures

**Files:**
- Create: `corpscout/services/dagster_v3/tests/test_se_company_person_match_gap_clickhouse_local.py`

**Interfaces:**
- Consumes: `tests/clickhouse_local.py::clickhouse_local_command`, `tables.QUALIFIED_MAIN_TABLE`, `tables.QUALIFIED_MATCH_TABLE`, `tables.QUALIFIED_MATCH_STATE_TABLE`, `tables.MATCH_COLUMNS`, `tables.build_se_company_person_match_gap_sql()`, and migrations `000396`, `000399`, `000406` as files.
- Produces: nothing importable — it is the behavioural proof that Task 1's DDL and SELECT do what they claim on a real engine.

> **This is the test spec section 12 calls for before prod.** It proves the combined `ALTER` against a POPULATED pair table, and it proves both of section 5.1's definitions and all four of its exclusions. What it does NOT prove is slice 2's business: `batch.py::match_pairs_sql()` is untouched here, so "two versions of a pair come back as ONE row carrying the higher confidence" is slice 2's test, in `tests/test_se_company_person_fold_clickhouse_local.py`, where spec section 11 puts it.

- [x] **Step 1: Write the proof**

Create `corpscout/services/dagster_v3/tests/test_se_company_person_match_gap_clickhouse_local.py`:

```python
"""Migration 000406 on a real ClickHouse (spec 2026-09-13 sections 4.3 and 5).

Four claims a fake client cannot settle:

1. The pair table's combined `ADD COLUMN ... , MODIFY ORDER BY ...` applies to a POPULATED
   table: it is accepted, `system.tables.sorting_key` gains request_id, every stored row
   survives, and each one reads back with request_id = '' (String's zero value -- the column
   carries no DEFAULT clause, because a key column may not).
2. After it, a v2 answer for the SAME candidate pair under its own request_id is a SECOND
   row under FINAL rather than a replacement. That is the whole reason the key grew.
3. The gap view's SELECT -- the builder's render, run as a plain SELECT -- finds exactly the
   companies spec section 5.1 defines: a call-name pair no stored match closed, and a
   double-surname pair.
4. And it excludes the four cases that must never appear: a call-name pair already matched at
   or above 0.8, a pair whose persons share a source, a pair whose birth years conflict, and
   a company with a single machine source. A reviewer member is not a member at all.
5. The two new tables accept a row built from `tables.LLM_QUEUE_COLUMNS` and
   `tables.LLM_RESPONSE_COLUMNS` -- every type, the company-id constraint and both defaults --
   and read back under FINAL as one row per (request, company).

Both `join_use_nulls` settings run: the SELECT joins six times, so the parametrization is a
live risk here rather than a guard.
"""

import subprocess
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.person import tables
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
ENTITY_MIGRATION = "000396_corpscout_se_company_person_entity.up.sql"
MATCH_MIGRATION = "000399_corpscout_se_company_person_match.up.sql"
ENHANCE_MIGRATION = "000406_corpscout_se_company_person_llm_enhance.up.sql"

STAMP = "toDateTime64('2026-09-13 00:00:00', 3, 'UTC')"
MATCHED_AT = "toDateTime64('2026-09-13 01:00:00', 3, 'UTC')"
V2_AT = "toDateTime64('2026-09-13 02:00:00', 3, 'UTC')"

GAP_CALL_NAME = "5560000001"      # erik bo bengtsson (ratsit) vs bo bengtsson (bolagsverket)
ALREADY_MATCHED = "5560000002"    # the same shape, with a stored pair at 0.9
GAP_DOUBLE = "5560000003"         # anna ek svensson (ratsit) vs anna svensson (bolagsverket)
SHARED_SOURCE = "5560000004"      # the persons' source sets overlap on ratsit
BIRTH_CONFLICT = "5560000005"     # 1970 against 1980
SINGLE_SOURCE = "5560000006"      # both persons come from bolagsverket alone
PAIR_ONLY = "5560000007"          # a pair row and no persons: the alter's second company


def _id(tag: str) -> str:
    """A FixedString(64) whose prefix says what it is."""
    return tag.ljust(64, "0")


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, list):
        return "[" + ", ".join(_literal(item) for item in value) + "]"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


# (company_id, person_key, birth_year, sources, [(normalized_id, source, first, middle, last)])
PEOPLE = (
    (GAP_CALL_NAME, "p1a", None, ["ratsit"],
     [("n1a", "ratsit", ["erik"], ["bo"], ["bengtsson"])]),
    (GAP_CALL_NAME, "p1b", None, ["bolagsverket"],
     [("n1b", "bolagsverket", ["bo"], [], ["bengtsson"])]),
    # A reviewer-only person shaped to form a SECOND call-name pair with p1a if the member
    # leg ever stopped filtering on the machine sources. It must contribute nothing.
    (GAP_CALL_NAME, "p1r", None, ["reviewer"],
     [("n1r", "reviewer", ["bo"], [], ["bengtsson"])]),
    (ALREADY_MATCHED, "p2a", None, ["ratsit"],
     [("n2a", "ratsit", ["erik"], ["bo"], ["bengtsson"])]),
    (ALREADY_MATCHED, "p2b", None, ["bolagsverket"],
     [("n2b", "bolagsverket", ["bo"], [], ["bengtsson"])]),
    (GAP_DOUBLE, "p3a", None, ["ratsit"],
     [("n3a", "ratsit", ["anna"], [], ["ek", "svensson"])]),
    (GAP_DOUBLE, "p3b", None, ["bolagsverket"],
     [("n3b", "bolagsverket", ["anna"], [], ["svensson"])]),
    (SHARED_SOURCE, "p4a", None, ["ratsit", "esef"],
     [("n4a", "ratsit", ["erik"], ["bo"], ["bengtsson"]),
      ("n4c", "esef", ["erik"], ["bo"], ["bengtsson"])]),
    (SHARED_SOURCE, "p4b", None, ["ratsit"],
     [("n4b", "ratsit", ["bo"], [], ["bengtsson"])]),
    (BIRTH_CONFLICT, "p5a", 1970, ["ratsit"],
     [("n5a", "ratsit", ["erik"], ["bo"], ["bengtsson"])]),
    (BIRTH_CONFLICT, "p5b", 1980, ["bolagsverket"],
     [("n5b", "bolagsverket", ["bo"], [], ["bengtsson"])]),
    (SINGLE_SOURCE, "p6a", None, ["bolagsverket"],
     [("n6a", "bolagsverket", ["erik"], ["bo"], ["bengtsson"])]),
    (SINGLE_SOURCE, "p6b", None, ["bolagsverket"],
     [("n6b", "bolagsverket", ["bo"], [], ["bengtsson"])]),
)


def _schema() -> list[str]:
    """000396's two tables the view reads, 000399's two match tables, and 000406's two
    ALTERs -- never 000396's serving-view statements, and never 000406's own new tables or
    its refreshable view (the SELECT is run as a plain SELECT, which is what the migration
    installs as the view's body)."""
    statements: list[str] = []
    wanted_creates = (
        "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_normalized\n",
        "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_v2\n",
        "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_match\n",
        "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_match_state\n",
    )
    for name in (ENTITY_MIGRATION, MATCH_MIGRATION):
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith("CREATE DATABASE"):
                if statement not in statements:
                    statements.append(statement)
            elif statement.startswith(wanted_creates):
                # 000398 renames the deployed table; the builder reads the renamed name.
                statements.append(
                    statement.replace(
                        "corpscout.se_company_person_v2", tables.QUALIFIED_MAIN_TABLE
                    )
                )
    assert len(statements) == 5, statements
    return statements


def _alters() -> list[str]:
    """000406's two ALTER statements, exactly as the migration writes them."""
    text = (MIGRATIONS_DIR / ENHANCE_MIGRATION).read_text(encoding="utf-8")
    alters = []
    for raw in text.split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement.startswith((
            f"ALTER TABLE {tables.QUALIFIED_MATCH_TABLE}\n",
            f"ALTER TABLE {tables.QUALIFIED_MATCH_STATE_TABLE}\n",
        )):
            alters.append(statement)
    assert len(alters) == 2, alters
    return alters


def _pattern_tables() -> list[str]:
    """000406's two CREATE TABLEs -- the pattern's queue and response pair."""
    text = (MIGRATIONS_DIR / ENHANCE_MIGRATION).read_text(encoding="utf-8")
    creates = []
    for raw in text.split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement.startswith((
            f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_LLM_QUEUE_TABLE}\n",
            f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_LLM_RESPONSE_TABLE}\n",
        )):
            creates.append(statement)
    assert len(creates) == 2, creates
    return creates


def _queue_insert() -> str:
    """One queue row in tables.LLM_QUEUE_COLUMNS order -- `note` takes its DEFAULT."""
    values = {
        "request_id": "'0123456789abcdef0123456789abcdef'",
        "company_id": f"'{GAP_CALL_NAME}'",
        "queued_at": STAMP,
        "queued_by": "'backoffice'",
        "note": "'se-person-match-v2 gap'",
    }
    return (
        f"INSERT INTO {tables.QUALIFIED_LLM_QUEUE_TABLE} "
        f"({', '.join(tables.LLM_QUEUE_COLUMNS)}) VALUES "
        f"({', '.join(values[column] for column in tables.LLM_QUEUE_COLUMNS)})"
    )


def _response_insert() -> str:
    """One response row in tables.LLM_RESPONSE_COLUMNS order."""
    values = {
        "request_id": "'0123456789abcdef0123456789abcdef'",
        "company_id": f"'{GAP_CALL_NAME}'",
        "provider": "'deepseek'",
        "model": "'deepseek-v4-flash'",
        "prompt_version": "'se-person-match-v2'",
        "input_hash": f"'{_id('hash1')}'",
        "candidates": "2",
        "prompt_tokens": "1128",
        "completion_tokens": "64",
        # raw_response is a plain String with no constraint -- the model's exact text. The
        # fixture keeps it simple on purpose: what is proved here is the column list and the
        # types, not the parser (that is slice 2's).
        "raw_response": "'no pairs'",
        "error": "''",
        "attempts": "1",
        "source_run_id": "'run-enhance-1'",
        "responded_at": V2_AT,
    }
    return (
        f"INSERT INTO {tables.QUALIFIED_LLM_RESPONSE_TABLE} "
        f"({', '.join(tables.LLM_RESPONSE_COLUMNS)}) VALUES "
        f"({', '.join(values[column] for column in tables.LLM_RESPONSE_COLUMNS)})"
    )


def _person_insert() -> str:
    rows = []
    for company_id, key, birth_year, sources, members in PEOPLE:
        rows.append(
            f"('{company_id}', '{_id(key)}', {_literal(birth_year)}, {_literal(sources)}, "
            f"{_literal([_id(member[0]) for member in members])}, 1, {STAMP}, 'se-person-fold-v2')"
        )
    return (
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} (company_id, person_key, birth_year, "
        "sources, normalized_ids, active, folded_at, fold_version) VALUES " + ", ".join(rows)
    )


def _normalized_insert() -> str:
    rows = []
    for company_id, _key, _birth_year, _sources, members in PEOPLE:
        for normalized_id, source, first, middle, last in members:
            rows.append(
                f"('{company_id}', '{source}', '{normalized_id}', '{_id(normalized_id)}', "
                f"'ok', {_literal(first)}, {_literal(middle)}, {_literal(last)}, {STAMP})"
            )
    return (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} (company_id, source, slot, "
        "normalized_id, parse_status, first_tokens, middle_tokens, last_tokens, "
        "normalized_at) VALUES " + ", ".join(rows)
    )


def _pair_insert(*, company_id: str, members_a: str, members_b: str, confidence: float,
                 prompt_version: str, input_hash: str, matched_at: str,
                 request_id: str | None) -> str:
    """A pair row. `request_id=None` is the pre-ALTER shape: the column does not exist yet."""
    columns = [
        "company_id", "candidate_a", "candidate_b", "members_a", "members_b", "source_a",
        "source_b", "name_a", "name_b", "confidence", "reason", "model", "prompt_version",
        "input_hash", "matched_at",
    ]
    values = [
        f"'{company_id}'", f"'{_id('ca-' + company_id)}'", f"'{_id('cb-' + company_id)}'",
        f"['{_id(members_a)}']", f"['{_id(members_b)}']", "'ratsit'", "'bolagsverket'",
        "'Erik Bo Bengtsson'", "'Bo Bengtsson'", repr(confidence), "'same person'",
        "'deepseek-v4-flash'", f"'{prompt_version}'", f"'{_id(input_hash)}'", matched_at,
    ]
    if request_id is not None:
        columns.append("request_id")
        values.append(f"'{request_id}'")
    return (
        f"INSERT INTO {tables.QUALIFIED_MATCH_TABLE} ({', '.join(columns)}) "
        f"VALUES ({', '.join(values)})"
    )


def _state_insert(company_id: str, input_hash: str) -> str:
    return (
        f"INSERT INTO {tables.QUALIFIED_MATCH_STATE_TABLE} (company_id, input_hash, "
        "candidates, sources, pairs, model, prompt_version, prompt_tokens, "
        "completion_tokens, raw_response, error, source_run_id, matched_at) VALUES "
        f"('{company_id}', '{_id(input_hash)}', 2, 2, 1, 'deepseek-v4-flash', "
        f"'se-person-match-v1', 900, 60, '{{}}', '', 'run-1', {MATCHED_AT})"
    )


def _script(join_use_nulls: int) -> str:
    key_sql = (
        "SELECT sorting_key FROM system.tables "
        f"WHERE database = 'corpscout' AND name = '{tables.MATCH_TABLE}'"
    )
    statements = [
        f"SET join_use_nulls = {join_use_nulls}",
        *_schema(),
        *_pattern_tables(),
        _person_insert(),
        _normalized_insert(),
        # Two pair rows written BEFORE the alter, in the pre-000406 shape: one for the
        # already-matched company, one for a company with no persons at all.
        _pair_insert(company_id=ALREADY_MATCHED, members_a="n2a", members_b="n2b",
                     confidence=0.9, prompt_version="se-person-match-v1",
                     input_hash="hash2", matched_at=MATCHED_AT, request_id=None),
        _pair_insert(company_id=PAIR_ONLY, members_a="n7a", members_b="n7b",
                     confidence=0.95, prompt_version="se-person-match-v1",
                     input_hash="hash7", matched_at=MATCHED_AT, request_id=None),
        _state_insert(ALREADY_MATCHED, "hash2"),
        _state_insert(PAIR_ONLY, "hash7"),
        "SELECT '@@key_before'",
        key_sql,
        "SELECT '@@rows_before'",
        f"SELECT count() FROM {tables.QUALIFIED_MATCH_TABLE}",
        *_alters(),
        "SELECT '@@key_after'",
        key_sql,
        "SELECT '@@rows_after'",
        f"SELECT count(), countIf(request_id = '') FROM {tables.QUALIFIED_MATCH_TABLE}",
        "SELECT '@@state_after'",
        f"SELECT count(), countIf(request_id = '') FROM {tables.QUALIFIED_MATCH_STATE_TABLE}",
        # The v2 answer for the SAME candidate pair, under its own request.
        _pair_insert(company_id=ALREADY_MATCHED, members_a="n2a", members_b="n2b",
                     confidence=0.95, prompt_version="se-person-match-v2",
                     input_hash="hash2", matched_at=V2_AT, request_id="req-0002"),
        "SELECT '@@versions'",
        f"SELECT request_id, prompt_version, confidence FROM {tables.QUALIFIED_MATCH_TABLE} "
        f"FINAL WHERE company_id = '{ALREADY_MATCHED}' ORDER BY request_id",
        # The pattern's own two tables, written from the column tuples they own.
        _queue_insert(),
        _response_insert(),
        "SELECT '@@queue'",
        f"SELECT request_id, company_id, queued_by, note "
        f"FROM {tables.QUALIFIED_LLM_QUEUE_TABLE} FINAL ORDER BY request_id, company_id",
        "SELECT '@@response'",
        f"SELECT request_id, company_id, provider, model, prompt_version, "
        f"toString(input_hash), candidates, prompt_tokens, completion_tokens, "
        f"raw_response, error, attempts, source_run_id "
        f"FROM {tables.QUALIFIED_LLM_RESPONSE_TABLE} FINAL ORDER BY request_id, company_id",
        "SELECT '@@gap'",
        tables.build_se_company_person_match_gap_sql(),
    ]
    return ";\n".join(statements) + ";\n"


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in lines:
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        else:
            result[current].append(line.split("\t"))
    return result


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def run(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    script = _script(request.param)
    try:
        completed = subprocess.run(
            clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return _sections([line for line in completed.stdout.splitlines() if line.strip()])


def test_the_combined_alter_widens_the_sort_key_on_a_populated_table(run) -> None:
    """Spec section 4.3 and the one risk of section 12. The ALTER is accepted against a table
    that already holds rows -- it is metadata-only, because every stored row's new key
    component is the same empty string, so no part has to be re-sorted."""
    assert run["key_before"] == [["company_id, candidate_a, candidate_b"]]
    assert run["key_after"] == [["company_id, candidate_a, candidate_b, request_id"]]


def test_every_stored_pair_survives_the_alter_with_an_empty_request(run) -> None:
    assert run["rows_before"] == [["2"]]
    # count() and countIf(request_id = '') are equal: nothing was lost and nothing acquired a
    # request it never had.
    assert run["rows_after"] == [["2", "2"]]
    assert run["state_after"] == [["2", "2"]]


def test_two_prompt_versions_of_one_pair_coexist_after_the_alter(run) -> None:
    """The whole reason the key grew: without request_id in it, the v2 row REPLACES the v1
    row for the same candidate pair, and a v2 that scored it LOWER would silently unmerge a
    person at the next fold. With it, both rows stand and the fold takes the maximum."""
    assert run["versions"] == [
        ["", "se-person-match-v1", "0.9"],
        ["req-0002", "se-person-match-v2", "0.95"],
    ]


def test_the_two_new_tables_accept_the_pattern_insert_tuples(run) -> None:
    """Spec sections 4.1 and 4.2, proved against the real DDL: a row built from
    tables.LLM_QUEUE_COLUMNS and one from tables.LLM_RESPONSE_COLUMNS are accepted by the
    types, the valid_company_id constraint and both DEFAULT clauses, and each reads back as
    one row per (request, company) under FINAL."""
    assert run["queue"] == [[
        "0123456789abcdef0123456789abcdef", GAP_CALL_NAME, "backoffice",
        "se-person-match-v2 gap",
    ]]
    assert run["response"] == [[
        "0123456789abcdef0123456789abcdef", GAP_CALL_NAME, "deepseek", "deepseek-v4-flash",
        "se-person-match-v2", _id("hash1"), "2", "1128", "64", "no pairs", "", "1",
        "run-enhance-1",
    ]]


def test_the_gap_view_finds_exactly_the_two_open_pairs(run) -> None:
    """Spec section 5.1's two definitions, and all four of its exclusions, in one readout.

    Included: the call-name company (one pair, no double surname) and the double-surname
    company (one pair, no call name) -- and never both counters for one pair, because the two
    rules disagree about the surnames by construction.

    Excluded: the company whose call-name pair a stored 0.9 match already closed; the company
    whose two persons share `ratsit`; the company whose birth years conflict; and the company
    whose only machine source is Bolagsverket. The reviewer-shaped third person of the
    call-name company is excluded too -- it would have made that company's count 2.
    """
    rows = sorted(row[:3] for row in run["gap"])

    assert rows == [
        [GAP_CALL_NAME, "1", "0"],
        [GAP_DOUBLE, "0", "1"],
    ]
    # computed_at is the fourth column and is stamped by now64, so it is only checked for
    # presence -- a missing value here would mean the SELECT lost a column.
    assert all(len(row) == 4 and row[3] for row in run["gap"])
```

- [x] **Step 2: Run it**

```bash
cd corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run --frozen --no-sync pytest tests/test_se_company_person_match_gap_clickhouse_local.py -q
```

Expected: **10 passed** (five tests × two `join_use_nulls` settings). The first run pulls `clickhouse/clickhouse-server:26.5` if Docker has not cached it. If the file SKIPS (no binary and no Docker), say it skipped — a skip is not a pass, and this is the test that stands between the combined `ALTER` and prod.

If `test_the_combined_alter_widens_the_sort_key_on_a_populated_table` fails with
`Code: 36 … Newly added column request_id has a default expression`, the migration has re-acquired a `DEFAULT ''` on the pair table's `ADD COLUMN`. Remove the clause; the stored value is the same.

- [x] **Step 3: Commit**

```bash
cd corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run --frozen --no-sync pytest tests/test_se_company_person_match_gap_clickhouse_local.py \
  tests/test_se_company_person_match_gap_view.py -q
```

Expected: 20 passed.

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
cat > /tmp/llm-enhance-task2.txt <<'MSG'
test(se-person): prove 000406's alter and the gap view against a real ClickHouse

The combined ADD COLUMN + MODIFY ORDER BY is the one risky statement in this slice,
so it is proved against a POPULATED se_company_person_match before it reaches prod:
the sorting key gains request_id, both stored rows survive with an empty request, and
a v2 answer for the same candidate pair under its own request_id becomes a SECOND row
under FINAL instead of replacing the first. The same script writes one row into each of
the pattern's new tables from their own column tuples, so the types, the company-id
constraint and both DEFAULT clauses are proved rather than assumed.

The gap view's SELECT -- the builder's render, run as a plain SELECT -- is checked
against six fixtures that encode every rule of spec section 5.1: a call-name pair, a
double-surname pair, a call-name pair a stored 0.9 match already closed, a pair whose
persons share a source, a birth-year conflict, and a single-source company. A
reviewer-shaped person is seeded into the call-name company precisely so that a
regression in the machine-source filter would show up as a count of 2.

Both join_use_nulls settings run: the SELECT joins six times.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/services/dagster_v3/tests/test_se_company_person_match_gap_clickhouse_local.py
git commit -F /tmp/llm-enhance-task2.txt
```

---

### Task 3: Prod run (controller)

**The controller runs this task; a task subagent never touches prod.** Every step is a read-only `SELECT`, a migration the owner approves, or one `SYSTEM REFRESH VIEW`. Nothing here is a code change until Step 8.

**Files:**
- Modify: `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-13-se-llm-enhance-design.md` (the **Shipped** record under section 14 slice 1, Step 8)
- Modify: this plan (ticked, Step 8)

**Interfaces:**
- Consumes: Tasks 1 and 2, merged to `main`. Migration `000406_corpscout_se_company_person_llm_enhance`, view `corpscout.se_company_person_match_gap`.
- Produces: the first gap baseline (companies, call-name pairs, double-surname pairs), the refresh's duration and peak memory, and the Shipped record under spec section 14 slice 1.

> **Does the Dagster host need a deploy?** The rule: **deploy only when a Dagster definition changed** — an asset, a job, a schedule, a sensor, a pool, a dbt model, or code a running asset executes. **In this slice `src/` changes twice**: `person/tables.py` (constants, two widened tuples, the view builder) and `person/match.py` (a defaulted `request_id` keyword on two row builders). No asset, job, schedule, pool or dbt model moves, and no asset's BEHAVIOUR changes — the match asset writes the same values, now naming the column. **So the deploy is OPTIONAL for this slice** and the next deploy of `main` for any other reason picks the constants up.
>
> **If one happens anyway it must come AFTER the migration**, and that ordering is not negotiable: `match.match_insert_sql()` renders its column list from `tables.MATCH_COLUMNS`, so deployed-but-unmigrated code would insert into a `request_id` column that does not exist yet and every `se_company_person_match` run would fail. Migrated-but-undeployed is harmless in both directions (the old code writes 15 columns, the 16th takes its zero value). The `se_company_person_match` weekly is STOPPED and stays so, which removes the window entirely — verify that in Step 2 rather than assuming it.

- [x] **Step 1: Re-check the number, review, merge**

Spec section 12's standing rule, run again at merge time — `main` moves daily and another session holds `000405_corpscout_esef_domains`:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info
git fetch --all -q
git ls-tree --name-only main corpscout/clickhouse/migrations/ | tail -6
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/basic-info-0
cat > $S/llm1_gate.sql <<'SQL'
SELECT version, dirty FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1;
SQL
bash $S/ch.sh $S/llm1_gate.sql
```

Expected: `main`'s newest migration is below 000406 and the ledger head is below `406` with `dirty = 0`. **If 000406 has been taken on either side, renumber before merging** — the six places are listed in Task 1 step 1, plus this plan's own mentions — then re-run:

```bash
cd corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py \
  tests/test_se_company_person_match_gap_view.py tests/test_se_company_person_tables.py \
  tests/test_se_company_person_match_gap_clickhouse_local.py -q
```

Review the branch end to end (`git diff main...se-llm-enhance`), then the owner merges to `main`. If the main checkout sits on another branch, merge through a worktree that has `main` checked out (memory `se-worktree-deploy-recipe`).

- [x] **Step 2: Confirm prod's state before anything moves**

```bash
S=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/basic-info-0
cat > $S/llm1_before.sql <<'SQL'
-- (a) the pair table, before: rows, companies, prompt versions, the live key
SELECT count() AS pairs, uniqExact(company_id) AS companies,
       groupUniqArray(prompt_version) AS prompt_versions
FROM corpscout.se_company_person_match FINAL;
SELECT sorting_key FROM system.tables
WHERE database = 'corpscout' AND name = 'se_company_person_match';

-- (b) the state table, before
SELECT count() AS state_rows, countIf(error != '') AS errored
FROM corpscout.se_company_person_match_state FINAL;
SELECT sorting_key FROM system.tables
WHERE database = 'corpscout' AND name = 'se_company_person_match_state';

-- (c) the parts the ALTER must not rewrite, and the bytes it touches
SELECT count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS on_disk
FROM system.parts
WHERE database = 'corpscout' AND table = 'se_company_person_match' AND active;

-- (d) the view's inputs, which decide what the first refresh costs
SELECT countIf(active = 1) AS active_persons, uniqExactIf(company_id, active = 1) AS companies
FROM corpscout.se_company_person FINAL;
SELECT count() AS ok_machine_rows
FROM corpscout.se_company_person_normalized FINAL
WHERE parse_status = 'ok' AND source IN ('bolagsverket', 'esef', 'wikidata', 'ratsit');

-- (e) the refresh calendar :30 has to fit into
SELECT view, status, toString(last_success_time) AS started, toString(next_refresh_time) AS next
FROM system.view_refreshes WHERE database = 'corpscout' ORDER BY view;

-- (f) neither new name may exist yet
SELECT name FROM system.tables WHERE database = 'corpscout'
  AND name IN ('llm_queue_se_company_person', 'llm_response_se_company_person',
               'se_company_person_match_gap');
SQL
bash $S/ch.sh $S/llm1_before.sql | tee $S/llm1_before.out
```

Expected, as of 2026-09-13 (re-read them, do not assume): (a) ~162,192 pairs over ~111,281 companies, one prompt version `se-person-match-v1`, key `company_id, candidate_a, candidate_b`; (b) ~124,646 state rows, 108 errored, key `(company_id)`; (d) ~1,273,763 active persons over ~677,256 companies and ~5,795,956 `ok` machine normalized rows; (e) exactly two refreshable views, `se_company_person_role` (`:20`) and `se_companies_serving` (`:45`) — **nothing at `:30`**; (f) **no rows**. A row in (f) means somebody has already created one of these by hand — stop and tell the owner.

Also confirm the person weeklies are stopped, so no match run can collide with the alter:

```bash
cat > /tmp/instigators.json <<'JSON'
{"query":"query Instigators($repositorySelector: RepositorySelector!) { schedulesOrError(repositorySelector: $repositorySelector) { __typename ... on Schedules { results { name cronSchedule scheduleState { status } } } } }","variables":{"repositorySelector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__"}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/instigators.json \
  | python3 -c "import json,sys; [print(s['name'], s['cronSchedule'], s['scheduleState']['status']) for s in json.load(sys.stdin)['data']['schedulesOrError']['results'] if 'se_company_person' in s['name']]"
```

Expected: `se_company_person_weekly … STOPPED`. **RUNNING here means stop and tell the owner.**

- [x] **Step 3: Apply the migration from the merged `main` checkout**

No refresh window to dodge: this migration touches no existing view, and `se_companies_serving` (`:45`) is not named anywhere in it. The alter is metadata-only, so there is no window to dodge on the pair table either — but run it outside `:20` and `:45` anyway, to keep the readouts clean.

```bash
make -s -C corpscout clickhouse-migrate-up-one
make -s -C corpscout clickhouse-migrate-version
```

Expected: the first returns in **seconds** — the two `CREATE TABLE`s are empty, both alters are metadata-only, and the view is created `EMPTY` with no `SYSTEM WAIT VIEW` — and the version reads `406` with no `(dirty)`.

If the client drops mid-flight, check what actually landed before touching the ledger:

```bash
cat > $S/llm1_landed.sql <<'SQL'
SELECT name FROM system.tables WHERE database = 'corpscout'
  AND name IN ('llm_queue_se_company_person', 'llm_response_se_company_person',
               'se_company_person_match_gap')
ORDER BY name;
SELECT sorting_key FROM system.tables
WHERE database = 'corpscout' AND name = 'se_company_person_match';
SQL
bash $S/ch.sh $S/llm1_landed.sql
```

All three names present and the key already four columns → `make -s -C corpscout clickhouse-migrate-force VERSION=406`. Anything missing → re-run the up-one; every statement is `IF NOT EXISTS`, so a partial replay is safe.

- [x] **Step 4: Read the alter back**

```bash
cat > $S/llm1_after.sql <<'SQL'
-- the key grew, and nothing else did
SELECT sorting_key FROM system.tables
WHERE database = 'corpscout' AND name = 'se_company_person_match';
SELECT sorting_key FROM system.tables
WHERE database = 'corpscout' AND name = 'se_company_person_match_state';

-- every row survived, and every one of them carries the empty request
SELECT count() AS pairs, countIf(request_id = '') AS empty_request,
       uniqExact(company_id) AS companies, groupUniqArray(prompt_version) AS prompt_versions
FROM corpscout.se_company_person_match FINAL;
SELECT count() AS state_rows, countIf(request_id = '') AS empty_request
FROM corpscout.se_company_person_match_state FINAL;

-- metadata-only: the part count and the bytes are the ones step 2(c) read
SELECT count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS on_disk
FROM system.parts
WHERE database = 'corpscout' AND table = 'se_company_person_match' AND active;

-- and the two new tables are there and empty
SELECT 'queue' AS t, count() FROM corpscout.llm_queue_se_company_person
UNION ALL
SELECT 'response', count() FROM corpscout.llm_response_se_company_person;
SQL
bash $S/ch.sh $S/llm1_after.sql | tee $S/llm1_after.out
```

Expected: `company_id, candidate_a, candidate_b, request_id` on the pair table and `(company_id)` unchanged on the state table; `pairs` and `state_rows` **exactly** the numbers step 2 read, with `empty_request` equal to each count; the part count, row count and on-disk size unchanged from step 2(c) — **that equality is the proof the `MODIFY ORDER BY` was metadata-only**; both new tables present with 0 rows. A part count or byte size that MOVED means ClickHouse rewrote the table: record it, it is a finding worth the spec's risk note being updated.

- [x] **Step 5: Force the view's first build and TIME it**

The view is empty until this runs (or until the next `:30` tick, whichever comes first). Note the wall clock before and after — spec section 12 puts a ten-minute ceiling on this build.

```bash
date -u +%H:%M:%S
cat > $S/llm1_refresh.sql <<'SQL'
SYSTEM REFRESH VIEW corpscout.se_company_person_match_gap;
SQL
bash $S/ch.sh $S/llm1_refresh.sql
cat > $S/llm1_refresh_poll.sql <<'SQL'
SELECT view, status, toString(last_success_time) AS started,
       toString(last_refresh_time) AS finished, toString(next_refresh_time) AS next,
       retry, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_company_person_match_gap';
SQL
bash $S/ch.sh $S/llm1_refresh_poll.sql
```

`SYSTEM REFRESH VIEW` returns immediately — it SCHEDULES the refresh. Poll the second file until `status` is `Scheduled` with an empty `exception`. **Read the two timestamps the right way round: `last_success_time` is the refresh's START and `last_refresh_time` is its END** — the duration is the difference, and `last_refresh_time` alone with no `last_success_time` means the attempt failed. **Never use `SYSTEM WAIT VIEW`**: it blocks for the whole build and is exactly the statement that outlived a client at 000391.

If `exception` is `MEMORY_LIMIT_EXCEEDED` or a join error, do NOT edit the applied migration and do NOT raise the cap: spec section 12's ruling is that the refresh moves to every 6 hours, which is a follow-up migration, and the owner decides.

- [x] **Step 6: The first gap baseline — the numbers this slice exists to produce**

```bash
cat > $S/llm1_baseline.sql <<'SQL'
-- (a) the headline baseline
SELECT count() AS companies_with_a_gap,
       sum(call_name_pairs) AS call_name_pairs_total,
       sum(double_surname_pairs) AS double_surname_pairs_total,
       countIf(call_name_pairs > 0) AS companies_call_name,
       countIf(double_surname_pairs > 0) AS companies_double_surname,
       countIf(call_name_pairs > 0 AND double_surname_pairs > 0) AS companies_both,
       max(call_name_pairs + double_surname_pairs) AS worst_company,
       toString(max(computed_at)) AS computed_at
FROM corpscout.se_company_person_match_gap;

-- (b) the ten companies with the most open pairs
SELECT g.company_id, g.call_name_pairs, g.double_surname_pairs,
       g.call_name_pairs + g.double_surname_pairs AS pairs
FROM corpscout.se_company_person_match_gap AS g
ORDER BY pairs DESC, g.company_id
LIMIT 10;

-- (c) twenty companies with their people, so the owner can eyeball the definitions
SELECT g.company_id AS company_id,
       g.call_name_pairs AS call_name_pairs,
       g.double_surname_pairs AS double_surname_pairs,
       arraySort(groupArray(p.display_name)) AS people
FROM corpscout.se_company_person_match_gap AS g
INNER JOIN corpscout.se_company_person AS p FINAL ON p.company_id = g.company_id
WHERE p.active = 1
GROUP BY g.company_id, g.call_name_pairs, g.double_surname_pairs
ORDER BY g.company_id
LIMIT 20;

-- (d) what the refresh cost: duration and peak memory
SELECT toString(event_time) AS finished,
       round(query_duration_ms / 1000, 1) AS seconds,
       formatReadableSize(memory_usage) AS peak_memory,
       formatReadableSize(read_bytes) AS read_bytes,
       read_rows,
       type
FROM system.query_log
WHERE event_date >= today() - 1
  AND query LIKE '%se_company_person_match_gap%'
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
ORDER BY event_time DESC
LIMIT 5;
SQL
bash $S/ch.sh $S/llm1_baseline.sql | tee $S/llm1_baseline.out
```

Expected shape, not expected values: (a) a five-figure company count with `call_name_pairs` and `double_surname_pairs` both **above** 3,258 and 2,380 — spec sections 5.1 and 12 say so explicitly, because the token rules generalize the 2026-09-11 suffix-and-display-name measurements; `companies_both` may well be non-zero (a company can carry one pair of each kind, though a single pair can never be both). (b) and (c) are the owner's eyeball: in (c) each row's `people` list should visibly contain a longer and a shorter spelling of one name (`Erik Bo Bengtsson` beside `Bo Bengtsson`) or a double and a single surname (`Anna Ek Svensson` beside `Anna Svensson`). A row where nothing looks related is a finding — record the company id.

**If the numbers are absurd** (zero companies, or more than a few hundred thousand), do not rerun blindly: read (d) first, then re-run the same SELECT live against the four tables and compare — a difference between the stored view and a live run of its own SELECT is the one thing the refresh exists to prevent.

> **Correction (2026-09-13 prod run):** the query-log readout of this step matches the status polls, not the
> refresh. Read the refresh as the `QueryFinish` row with `query_kind = 'Insert'` into the view's
> `.tmp.inner_id…` table in the refresh window (it was 425.6 s, peak 2.05 GiB).

- [x] **Step 7: Deploy only if the owner wants the constants on the host now**

Not required by this slice (see the box at the top of this task). If it is done anyway, it happens AFTER step 3, from a **pristine worktree at the merge commit** — `light_sync` rsyncs the working tree with `--delete-after`, so a dirty tree ships another session's WIP:

```bash
D=/private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect/62b23c62-a06d-4dca-84a0-a4a0f3f72968/scratchpad/deploy-worktree
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git worktree add "$D" main 2>/dev/null || (cd "$D" && git fetch --all -q && git reset --hard && git checkout --detach main)
test -f "$D/corpscout/services/dagster_v3/.env" || cp corpscout/services/dagster_v3/.env "$D/corpscout/services/dagster_v3/.env"
cd "$D/corpscout/services/dagster_v3"
uv sync --frozen
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/finland_ytj/dbt --profiles-dir src/dagster_v3/defs/finland_ytj/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/exchange_rates_v2/dbt --profiles-dir src/dagster_v3/defs/exchange_rates_v2/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/company_serving/dbt --profiles-dir src/dagster_v3/defs/company_serving/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/company_domain_suggestions/dbt --profiles-dir src/dagster_v3/defs/company_domain_suggestions/dbt
uv run --frozen --no-sync dg utils refresh-defs-state
uv run --frozen --no-sync dg check defs
cd ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml; echo "RC=$?"
```

The `.env` copy is mandatory — the scratch worktree's gitignored `.env` is deleted by the overnight `/private/tmp` cleanup, and without it `dg utils refresh-defs-state` and `dg check defs` fail with a cryptic YAML/column error and ansible stops at its localhost pre-task. The dbt-state refresh is mandatory too (the playbook asserts the manifests exist, never that they are fresh). `RC=` is captured explicitly — piping the playbook to `tail` masks its exit code. The `company_domain_suggestions` adapter traceback during `refresh-defs-state` is known non-fatal noise.

- [x] **Step 8: Record and archive**

- Append the slice-1 **Shipped** record to spec **section 14 slice 1**: the migration number it actually got; the two new tables and their key; the pair table's widened sorting key **and the deviation that got it there** (the `DEFAULT ''` clause spec section 4.3 writes is refused on a key column with code 36, so the migration omits it and the stored value is unchanged); the before/after row, part and byte counts proving the alter was metadata-only; the view's offset, its first-build duration and peak memory; the **first gap baseline** (companies with a gap, call-name pairs, double-surname pairs, the split and the worst company) as the new baseline the 3,258 / 2,380 figures are explicitly NOT compared against; the sample of 20 the owner eyeballed; and the consequence this slice absorbed (`MATCH_COLUMNS` / `MATCH_STATE_COLUMNS` gained `request_id` because `se_company_ddl.declared_columns` replays ALTERs, and `match_row` / `match_state_row` gained a defaulted keyword).
- Note explicitly whether the Dagster host was deployed, and that the ordering rule is migrate-then-deploy.
- Tick this plan's boxes and archive the ledger under `.superpowers/sdd/llm-enhance/`.
- Update memory `se-person-entity.md`: LLM-enhance slice 1 live — the queue and response tables exist and are empty, `se_company_person_match` keys on four columns with `request_id` last, and `corpscout.se_company_person_match_gap` refreshes hourly at `:30` with the recorded baseline. Note the `DEFAULT`-on-a-key-column gotcha, since it will recur the next time a sorting key grows.

---

## Self-review

**1. Spec coverage — sections 4, 5, 11 and 12, sentence by sentence, for what slice 1 owns.**

| the spec says | where it lands |
| --- | --- |
| §4: migration `000406_corpscout_se_company_person_llm_enhance`, four statements up in this order (queue, response, pair alter, state alter), the view fifth | Task 1 step 7's up file; `test_the_up_migration_is_two_tables_two_alters_and_one_refreshable_view` asserts six statements in that order |
| §4.1: `llm_queue_se_company_person`, five columns, `valid_company_id`, `ReplacingMergeTree`, `ORDER BY (request_id, company_id)`, no version column | Task 1 step 7 (DDL), step 4 (`LLM_QUEUE_COLUMNS`), step 10's `test_the_llm_queue_table_holds_one_requests_unit_ids`, Task 2's queue insert and readback |
| §4.2: `llm_response_se_company_person`, fourteen columns incl. `candidates` and `source_run_id`, `ReplacingMergeTree(responded_at)`, same key | Task 1 step 7, step 4 (`LLM_RESPONSE_COLUMNS`), step 10's `test_the_llm_response_table_is_one_answer_per_request_and_company`, Task 2's response insert and readback |
| §4.3: the pair alter is ONE statement with `ADD COLUMN` + `MODIFY ORDER BY`; the state alter adds the column outside the key; why the key has to grow | Task 1 step 7 with the reasoning in the file's comments; `test_the_pair_alter_is_one_statement_and_carries_no_default_expression`; `test_000406_pairs_the_llm_queue_and_response_and_widens_the_match_sort_key`; Task 2's `test_the_combined_alter_widens_the_sort_key_on_a_populated_table` and `test_two_prompt_versions_of_one_pair_coexist_after_the_alter` |
| §4.4: the down drops the view and the two tables and the STATE column, never the pair column, and says why in a comment | Task 1 step 7's down file; `test_the_down_migration_removes_everything_it_can` |
| §4.4: `EXPECTED_MIGRATIONS` gains the entry after `000404` | Task 1 step 9 |
| §5: the view's columns, engine, key, `REFRESH EVERY 1 HOUR OFFSET 30 MINUTE`, `EMPTY`, and why `:30` | Task 1 steps 4 and 7; `test_the_up_migration_is_two_tables_two_alters_and_one_refreshable_view`; Task 3 step 2(e) re-checks that `:30` is still free before the migration lands |
| §5.1: the call-name rule, the double-surname rule, disjoint sources, the birth-year clause, "no stored pair at or above 0.8" | the builder's docstring and SQL (Task 1 step 4); `test_the_two_rules_are_spelled_as_section_5_1_defines_them`; Task 2's six fixtures, one per inclusion and exclusion |
| §5.1: the counts will be HIGHER than 3,258 / 2,380 and the first refresh is the new baseline | Global Constraints (verbatim §12 ruling); Task 3 step 6's expected shape; Task 3 step 8's record |
| §5.2: the SQL, rendered by `build_se_company_person_match_gap_sql()` and pinned against the migration | Task 1 steps 4 and 7; `test_the_view_body_is_the_builder_render_and_has_not_drifted_from_it` |
| §5.2's four notes (least/greatest, each pair seen twice, the two kinds never overlap, a reviewer-only person contributes nothing) | the builder's docstring; `test_the_two_rules_are_spelled_as_section_5_1_defines_them`; Task 2's reviewer-shaped decoy person, which would make the call-name count 2 if the source filter broke |
| §8, DDL consequence only: `request_id` in the pair table's sort key | Task 1 step 7. **The `batch.py::match_pairs_sql()` amendment is explicitly NOT written** — Interfaces "Deliberately NOT produced", and the note above Task 2 says where spec §11 puts its test |
| §11: `test_se_company_person_match_gap_view.py` — render pin, no Nullable in the key, the 000402 SETTINGS block, the two rules | Task 1 step 2, ten tests |
| §11: `test_clickhouse_migrations.py` — 000406 registered, the pair alter is ONE statement with both clauses, the view created `EMPTY` with the hourly refresh | Task 1 step 9 |
| §11: the clickhouse-local proof — the two new tables accept the insert tuples, the combined ALTER applies to a populated pair table, the gap SELECT finds one pair of each kind and nothing after a 0.9 pair is stored | Task 2, five tests × two settings. **Split deliberately**: the `match_pairs_sql` max-confidence claim spec §11 lists in the same bullet needs slice 2's amendment, so it stays in slice 2 and this plan says so |
| §12: the five rulings that bind slice 1 | quoted verbatim in Global Constraints; the number gate is Task 1 step 1 and Task 3 step 1; the `MODIFY ORDER BY` risk is Task 2 and Task 3 step 4's part/byte equality; the ten-minute ceiling is Task 3 step 5 |
| §13: every name | the Interfaces block, Task 1 step 4, and the name scan below |
| §14 slice 1: migration, names, builder, `EXPECTED_MIGRATIONS`, the two test files, the clickhouse-local proof; prod re-check, apply, refresh by hand and time it, record the first counts | Tasks 1, 2 and 3; the Shipped record is Task 3 step 8 |

**2. Placeholder scan.** No TBD, no TODO, no "similar to Task N", no "add tests for the above", no "implement appropriate …". Every step carries the literal SQL, Python or shell it installs; both migration files and both test files are written out whole. The only deliberately unwritten text is Task 3 step 8's Shipped record, whose content is enumerated item by item. All 25 ASCII `...` hits in the file are real syntax or real quotations: `tuple[str, ...]` annotations, `tuple[Any, ...]` return types, `match_row(..., request_id: str = "")` in the Interfaces shorthand, `pytest ...` in a generic constraint, and two places quoting a SQL form (`ADD COLUMN ... , MODIFY ORDER BY ...`, `ALTER TABLE ... MODIFY QUERY`) — the second of which is copied verbatim from the docstring already in the repo.

**3. Name consistency.** `build_se_company_person_match_gap_sql`, `LLM_QUEUE_TABLE`, `LLM_RESPONSE_TABLE`, `MATCH_GAP_VIEW`, the three `QUALIFIED_*` twins, `LLM_QUEUE_COLUMNS`, `LLM_RESPONSE_COLUMNS`, `MATCH_GAP_VIEW_COLUMNS` and `MATCH_GAP_VIEW_ORDER_BY` are spelled identically in the Interfaces block, Task 1 steps 2, 4, 9 and 10, and Task 2 — and they are exactly spec §13's list, with nothing invented. The table names `llm_queue_se_company_person`, `llm_response_se_company_person` and `se_company_person_match_gap` and the migration name `000406_corpscout_se_company_person_llm_enhance` appear identically everywhere, migration files included. The view's four columns are one list, repeated identically in `MATCH_GAP_VIEW_COLUMNS`, the builder's aliases, the migration body and Task 3 step 6's readouts. Whole-name matching is respected throughout: `se_company_person_match` is only ever matched with the token that follows it (`AS m FINAL`, `\n`), which is what keeps `_state` and `_gap` out of its way.

**Machine-checked while writing this plan**, not merely read: the builder's render, the migration's view body and the SQL proved on ClickHouse 26.5 are the same text under whitespace normalization; every assertion in `test_se_company_person_match_gap_view.py` was executed against the plan's own migration text and passed; and the two migration files were run end to end on `clickhouse/clickhouse-server:26.5` — the up file produced both tables with `ORDER BY (request_id, company_id)`, the widened pair key, the unchanged state key and the empty view, and the down file left the pair column standing (1) with the state column gone (0), exactly as §4.4 requires.

**4. Choices this plan made where the spec left room** — all four belong in the Shipped record:

- **The pair alter carries no `DEFAULT` clause.** Spec §4.3 writes `ADD COLUMN IF NOT EXISTS request_id String DEFAULT ''`. ClickHouse 26.5 refuses exactly that when the column joins the sorting key (code 36, BAD_ARGUMENTS, "Newly added column request_id has a default expression, so adding expressions that use it to the sorting key is forbidden"). The clause is dropped and the spec's INTENT is preserved exactly — §4.3's own prose asks for "the type's zero default — here `''`", which is what a String column stores without one. The state table's copy keeps `DEFAULT ''` because it is not in a key. This is the one place the migration is not the spec's literal text, and both the migration comment and the test say why.
- **A new test file for the clickhouse-local proof, not an extension of the fold's.** Spec §11 puts four claims in `test_se_company_person_fold_clickhouse_local.py`; two of them (the `match_pairs_sql` maximum, the gap view after a 0.9 pair is stored) depend on slice 2's `batch.py` amendment. Putting slice 1's two claims in their own file keeps each test file about one migration and leaves slice 2 its own extension point. The fold's file is still touched, but only for what the widened tuples force (step 11).
- **`MATCH_COLUMNS` and `MATCH_STATE_COLUMNS` move in slice 1, and `match.py` with them.** Not a preference: `tests/se_company_ddl.py::declared_columns` replays later `ADD COLUMN`s, so the pins go red the moment the migration exists. The alternative — relaxing the pin to `[*MATCH_COLUMNS, "request_id"]` for one slice — would leave `tables.py` lying about the deployed schema and would have to be undone in slice 2. The two row builders take the column as a keyword-only parameter defaulting to `""`, so the match asset's behaviour is unchanged and slice 2's apply only has to pass a value.
- **The Dagster deploy is optional in this slice, and the ORDERING is the part that matters.** Nothing in the definitions tree changes behaviour, so no deploy is required; but `match_insert_sql()` now renders `request_id` into its column list, so a deploy BEFORE the migration would break every `se_company_person_match` run. Task 3 states the rule, verifies the weekly is stopped, and puts the optional deploy after the migration.

**5. Where the brief or the spec disagreed with the code, and what this plan does.**

- **Spec §4.3's alter does not run as written** — see the first bullet above. The code (ClickHouse) wins; the plan carries the working form and records the deviation for the Shipped note.
- **Spec §2's row counts are stale.** It records 159,789 pairs and a 122,837-row state table; prod on 2026-09-13 holds 162,192 pairs over 111,281 companies and 124,646 state rows (108 errored). The plan's Prod-facts table carries the measured numbers, says the spec's are the 2026-09-11 history, and Task 3 compares each table against ITSELF across the alter rather than against any figure written down earlier.
- **`se_address_geocodes_current` is not a refreshable view any more.** Spec §2 lists refresh offsets "`:00` geocodes, `:20` `se_company_person_role`, `:45` `se_companies_serving`", but prod's `system.view_refreshes` holds exactly two rows — the role view and the serving view. `:30` is free either way, so the choice stands; Task 3 step 2(e) re-reads the calendar instead of trusting the list.
- **Spec §11's clickhouse-local bullet mixes slice-1 and slice-2 claims** — split as described above, with the boundary stated in the plan so slice 2's writer knows what is already proved.
- **Spec §13 does not name a constant for the four machine sources, the `ok` status or the 0.8 threshold**, and `tables.py` cannot import `match.py` or `fold.py` (both import `tables.py`). Rather than invent three names the spec did not ask for, the builder spells the literals out — exactly as `build_se_company_person_role_sql()` does — and `test_the_member_leg_reads_only_ok_rows_of_the_four_machine_sources` and `test_the_matched_leg_uses_the_folds_threshold_and_an_error_free_state_row` hold the equalities against `match.MACHINE_SOURCES`, `fold.FOLDABLE_STATUS` and `fold.MATCH_THRESHOLD`.
- **The brief asked for the gap baseline "companies with a gap, sum of call_name_pairs, sum of double_surname_pairs, top 10, a 20-row sample"** — all present in Task 3 step 6, plus the split by kind and the worst company, because the first two numbers alone cannot say whether one enormous company is carrying the total.
