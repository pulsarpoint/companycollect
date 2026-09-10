# SE company person entity, slice 1: extractors — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill `se_company_person_suggestion` from the three sources that carry Swedish people — Bolagsverket annual-report signatories, the ESEF people view and Wikidata — through the basic-info extract helper, wire the extract job and its stopped weekly schedule, run it on prod and read the normalizer's `parse_status` distribution per source.

**Architecture:** The address entity's shape, with one structural difference forced by the data. An address source delivers **one** row per company, so its change scan can compare a single `observed_at` watermark and its tombstone is "the register says `has_company = 0`". A person source delivers **many** rows per company (one per signature line, per extracted person, per Wikidata statement), the person suggestion table carries **no `observed_at` column**, and `se_financial_report_signatories` is rebuilt whole on every run with one uniform `resolved_at` — so a timestamp watermark would either re-extract 5.5M rows weekly or never notice a signatory that disappeared. Each person extractor therefore scans on a **per-company state hash**: the sha256 of the company's whole delivered row set on the source side, compared with the same hash over the company's live (non-tombstone) suggestion rows. A company is visited when only one side has it (new, or gone) or the two hashes differ; when they agree it is skipped, so the scan converges. The page select is then `live rows UNION ALL tombstones`, the tombstones being the stored live slots the source no longer delivers, found by a `LEFT ANTI JOIN` against the same `live` CTE. `suggestion_id` and `suggested_at` come from one `WITH`-bound `now64(3, 'UTC')` per statement, exactly as for addresses.

**Tech Stack:** Dagster 1.13.9 (`uv run --frozen --no-sync`), Python 3.14, pytest, ClickHouse 26.5 (`clickhouse-local` through docker for the integration tests), clickhouse-driver 0.2.10 through `dagster_clickhouse.ClickhouseResource`.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md` — slice 1 is section 9 item 1; its content is sections 3.1 (the suggestion table, slots 3.1.1, `data` 3.1.2, `role_key`), 4.3 (role years per source), 6 (extractors, job, weekly) and 10 (names). Slice 0 (the six tables, `roles.py`, `normalize_se.py`, `normalize.py` and the `se_company_person_normalize` asset) is on prod since 2026-09-09.

## Global Constraints

- Dagster commands run from `corpscout/services/dagster_v3` as `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/<file> -q`; `uv run --frozen --no-sync dg check defs` must pass before every commit that touches `src/`.
- **Repo root:** `/Users/graovic/pulsarpoint/ppoint/companycollect`. Always `cd` with absolute paths.
- **No migration.** Every column this slice writes already exists on `corpscout.se_company_person_suggestion` (migration 000396, 18 columns, verified 2026-09-09). If a task turns out to need a column that is not there, stop and raise it rather than inventing one: a new migration takes the next free number at write time and **the controller renumbers it at merge** (both file names, the `EXPECTED_MIGRATIONS` entry in `tests/test_clickhouse_migrations.py`, and any `migrate force NNN` line in the file header).
- **The suggestion table records neither the run nor the extractor version.** Its 18 columns are `company_id, source, slot, suggestion_id, suggested_at, source_record_id, full_name, first_name, last_name, birth_year, wikidata_id, role_original, role_key, fiscal_year, role_from, role_to, document_ref, data` — there is no `source_run_id` and no `extractor_version`, unlike the address and basic-info suggestion tables. `run_extractor` still binds `%(source_run_id)s` and `%(extractor_version)s` into every page's params; the person target's trailing SQL simply does not consume them, which clickhouse-driver allows. The extractor version constants stay in the modules for the asset metadata and for this plan's tests.
- **`data` is a `String` holding a JSON object**, never ClickHouse's native `JSON` type (owner ruling 2026-09-09). The table carries `CONSTRAINT valid_data CHECK JSONType(data) = 'Object'`, so **every** `data` expression an extractor writes must render an object. Build it with `toJSONString(map(...))` and **never** by concatenating source text. Every map value is a `String` (`toString(...)` numerics and enums, `arrayStringConcat(...)` arrays, `ifNull(x, '')` nullables): a `Map` needs one value type, and a uniform string map keeps the rendering deterministic, which the state hash depends on.
- **`role_key` is the source's own machine code** (Bolagsverket's `role_kind`, ESEF's `role_category`, Wikidata's property id) and `role_original` the human label beside it. `roles.py::role_code_for` keys on `role_key` first. Extractors fill both columns directly from the source; neither is read out of `data`.
- **Whole-name matching on table names.** `se_company_person` is a prefix of `se_company_person_suggestion`, `_normalized`, `_v2`, `_history`, `_rule` and `_precedence`; `company_person_role` prefixes the kept catalog `company_person_role_type`. Every string match on a table name — a test assertion, a fake client's dispatch branch, a migration-statement filter, an `rg` check — must compare **whole names** (split into tokens, or anchor the CREATE line with its trailing newline), never a bare `in` on the qualified string.
- No `from __future__ import annotations` in any module that defines a `@dg.asset`.
- **Nothing in this plan executes DDL against a server.** The prod run in Task 6 is Dagster runs plus read-only `SELECT`s.
- **Another session merges to main daily.** Merge through a worktree that checks main out (memory `se-worktree-deploy-recipe`); the main checkout may sit on another branch.
- Commit by explicit path after every task; never `git add -A`. Trailers, contiguous at the end of every commit message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- The basic-info and address extractors' SQL texts and asset names must not change: every existing test in `tests/test_se_company_basic_info_extract.py`, `tests/test_se_company_basic_info_extractors_sql.py`, `tests/test_se_company_address_extractors_sql.py`, `tests/test_se_company_address_jobs.py` stays untouched and green.
- Pre-existing unrelated failures, **not** regressions of this slice: `tests/test_se_company_address_extractors_clickhouse_local.py` (broken since main's 000390 — the fixture lacks `se_ratsit_company_translated`), `tests/test_se_company_basic_info_clickhouse_local.py`, `tests/test_schedule_cron_contracts.py` (pre-existing `(minute, hour)` collisions), `tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values`, `tests/test_backfill_policy_contracts.py`, `tests/test_ted_procurement_parser.py`, `tests/test_ted_procurement_publish.py`, `tests/test_nace_categories.py`.

---

## Verified facts this plan is built on

Measured against prod ClickHouse 26.5.1.882 on 2026-09-09 with read-only `SELECT`s. An implementer does not need to re-measure; the numbers are the acceptance targets of Task 6.

| fact | value |
| --- | --- |
| `se_financial_report_signatories` | plain `MergeTree`, 5,559,541 rows, 577,932 companies; **rebuilt whole** (stage + `EXCHANGE TABLES`) so `resolved_at` has exactly **one** distinct value table-wide |
| its `signatory_uid` | `MATERIALIZED` sha256 of `company_id, statement_key, signatory_kind, person_seq` — unique across all 5,559,541 rows |
| its `source_record_uid` | `DEFAULT` sha256 over `statement_key`, one per report |
| max signatories per company | 96 (so `groupArray` per company is tiny) |
| Bolagsverket rows inside the universe with a name | **5,559,317 rows over 577,901 companies** (31 signatory companies are not in `se_company_basic_info`) |
| `se_esef_document_people` | a `VIEW` that already reads its product `FINAL` — **never write `FINAL` after it**; 9,610 rows, 384 companies, all inside the universe; `(company_id, source_document_id, candidate_uid)` unique |
| ESEF duplicate people | one document can hold rows from two extraction models; 1,722 `(document, name)` pairs have more than one `candidate_uid`. They stay: nothing is dropped at the raw layer, and the fold merges them into one person with two members. |
| Wikidata SE people | 504 rows, 245 companies, 456 persons; every row has a `Q…` id and a `wikidata_persons` row; `(company_id, wikidata_company_people.source_record_id)` unique |
| `wikidata_company_people.source_record_id` | `Q<company>:P<property>:Q<person>` — literally spec 3.1.1's "the QID plus the company link id" |
| `wikidata_persons` columns | `name, name_normalized, description, birth_year, image_url, wikidata_url` — **no occupations, nationality or sitelinks** |
| `se_company_basic_info.lei` | filled for only **392** companies, so the Wikidata LEI link must come from `company_identifier` (115,440 SE current LEIs), as `basic_info/wikidata.py` does |
| the full unscoped Bolagsverket state-hash scope | runs in **12 s** and returns exactly 577,901 companies |
| cron slot `15 7` | **taken** by `france_sirene_register_schedule`; free minutes in hour 7 are 20, 25, 35, 50, 55 |

---

## File structure

| file | change |
| --- | --- |
| `src/dagster_v3/defs/se_company/basic_info/extract.py` | **modify** — `run_extractor`/`define_suggestion_asset` accept an optional `changed_scope_override` |
| `src/dagster_v3/defs/se_company/person/suggestions.py` | **create** — `PERSON_SELECT_COLUMNS`, `PERSON_TARGET`, `live_select_sql`, `person_select_sql`, `person_changed_scope_sql`, `person_state_sql`, `define_person_suggestion_asset` |
| `src/dagster_v3/defs/se_company/person/bolagsverket.py` | **create** — signatory extractor |
| `src/dagster_v3/defs/se_company/person/esef.py` | **create** — ESEF people extractor |
| `src/dagster_v3/defs/se_company/person/wikidata.py` | **create** — Wikidata extractor |
| `src/dagster_v3/defs/se_company/person/jobs.py` | **create** — `se_company_person_extract_job`, `se_company_person_weekly` (STOPPED) |
| `src/dagster_v3/defs/se_company/person/assets.py` | **modify** — `EXTRACTOR_SOURCES`, `EXTRACTOR_ASSET_NAMES`, `deps` on the normalize asset |
| `src/dagster_v3/defs/se_company/person/docs/person-design.md` | **modify** — the extractor section, and two slice-0 sentences that slice 1 makes wrong |
| `tests/test_se_company_basic_info_extract.py` | **modify** — append two tests for the override |
| `tests/test_se_company_person_extractors_sql.py` | **create** — text and structure pins for the three extractors |
| `tests/test_se_company_person_extractors_clickhouse_local.py` | **create** — the three extractors on a real engine, both `join_use_nulls` settings |
| `tests/test_se_company_person_jobs.py` | **create** — job selection, schedule state, normalize upstream |
| `tests/fixtures/se_company_person_source_tables.sql` | **create** — prod DDL snapshot of the three source tables no migration can supply cleanly |

Task 2 creates `suggestions.py` and Bolagsverket together because the target and the shared select/scope builders have no meaning until one source uses them; Tasks 3 and 4 then add one module each and extend the two test files. Task 1 is split out because it changes a file three other entities depend on and a reviewer could reject it on its own.

---

### Task 1: The extract helper takes a caller-built change scope

**Why:** `changed_scope_sql` compares `candidate.observed_at` with `argMax(observed_at, suggested_at)` read from the suggestion table. `se_company_person_suggestion` has no `observed_at` column at all, so the person extractors cannot use it and must supply their own scope text. One optional parameter, defaulting to `None`, leaves every existing caller byte-identical.

**Files:**
- Modify: `src/dagster_v3/defs/se_company/basic_info/extract.py`
- Test: `tests/test_se_company_basic_info_extract.py` (append; existing tests untouched)

**Interfaces:**
- Produces: `run_extractor(..., changed_scope_override: str | None = None)` and `define_suggestion_asset(..., changed_scope_override: str | None = None)`. When it is `None` (every existing caller) the behaviour and the rendered texts are exactly what they are today. When it is a string, `_scan_pages` uses it verbatim as the scope query instead of calling `changed_scope_sql(current_sql=..., target=...)`; `config.since` still wins over both, so the `since` escape hatch keeps working off `current_sql`.

- [x] **Step 1: Write the failing tests** — append to `tests/test_se_company_basic_info_extract.py`, reusing the file's existing imports and its `FakeClient`

```python
def test_the_default_change_scope_is_still_the_helpers_own() -> None:
    client = FakeClient(candidates=0, scope_pages=[["5560125220"]])
    run_extractor(
        client, source="scb", extractor_version="v", current_sql="SELECT 1 AS company_id, 2 AS observed_at",
        select_sql="SELECT 1", select_params=None, source_run_id="r", config=ExtractConfig(),
    )
    scope_insert = next(sql for sql, _, _ in client.statements if sql.startswith("INSERT INTO corpscout._tmp_"))
    assert changed_scope_sql(current_sql="SELECT 1 AS company_id, 2 AS observed_at") in scope_insert


def test_a_changed_scope_override_replaces_the_helpers_scan_but_not_since() -> None:
    override = "SELECT company_id FROM corpscout.some_scope WHERE source = %(source)s"
    client = FakeClient(candidates=0, scope_pages=[["5560125220"]])
    run_extractor(
        client, source="scb", extractor_version="v", current_sql="SELECT 1 AS company_id, 2 AS observed_at",
        select_sql="SELECT 1", select_params=None, source_run_id="r", config=ExtractConfig(),
        changed_scope_override=override,
    )
    scope_insert = next(sql for sql, _, _ in client.statements if sql.startswith("INSERT INTO corpscout._tmp_"))
    assert override in scope_insert
    assert "argMax(observed_at, suggested_at)" not in scope_insert

    # `since` still wins: it is the deliberate "ignore what is stored" escape hatch.
    since_client = FakeClient(candidates=0, scope_pages=[["5560125220"]])
    run_extractor(
        since_client, source="scb", extractor_version="v",
        current_sql="SELECT 1 AS company_id, 2 AS observed_at", select_sql="SELECT 1",
        select_params=None, source_run_id="r",
        config=ExtractConfig(since="2026-09-01T00:00:00Z"), changed_scope_override=override,
    )
    since_insert = next(sql for sql, _, _ in since_client.statements if sql.startswith("INSERT INTO corpscout._tmp_"))
    assert "parseDateTime64BestEffort(%(since)s, 3, 'UTC')" in since_insert
    assert override not in since_insert


def test_define_suggestion_asset_forwards_the_override() -> None:
    asset = define_suggestion_asset(
        source="scb", extractor_version="v", current_sql="SELECT 1", select_sql="SELECT 1",
        description="d", changed_scope_override="SELECT company_id FROM corpscout.some_scope",
    )
    assert asset.key == dg.AssetKey("se_basic_info_suggestions_scb")
```

- [x] **Step 2: Run to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_basic_info_extract.py -q`
Expected: the two override tests FAIL with `TypeError: run_extractor() got an unexpected keyword argument 'changed_scope_override'`; `test_the_default_change_scope_is_still_the_helpers_own` PASSES already, and so does everything else in the file.

- [x] **Step 3: Thread the parameter through**

In `extract.py`, `_scan_pages` gains the parameter and stops building the scope when it is given:

```python
def _scan_pages(
    client: Any, *, source: str, current_sql: str, config: ExtractConfig, select_params: dict[str, Any],
    target: SuggestionTarget, changed_scope_override: str | None = None,
) -> Iterator[list[str]]:
    if config.since:
        scope_sql = since_scope_sql(current_sql=current_sql)
    elif changed_scope_override is not None:
        # A caller whose suggestion table has no observed_at watermark builds its own scope
        # (the person entity's per-company state hash, spec 2026-09-09 section 6).
        scope_sql = changed_scope_override
    else:
        scope_sql = changed_scope_sql(current_sql=current_sql, target=target)
    params = {**select_params, "source": source}
    if config.since:
        params["since"] = config.since
    return scope_pages(
        client, scope_sql=scope_sql, params=params, page_size=config.page_size, settings=SCAN_QUERY_SETTINGS,
        prefix=target.scratch_prefix,
    )
```

`run_extractor` gains `changed_scope_override: str | None = None` as its last keyword parameter and passes it to `_scan_pages`; `define_suggestion_asset` gains the same parameter and passes it to `run_extractor`. Nothing else changes.

- [x] **Step 4: Run the four suites that own these texts**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_basic_info_extract.py tests/test_se_company_basic_info_extractors_sql.py tests/test_se_company_address_extractors_sql.py tests/test_se_company_address_jobs.py -q`
Expected: all PASS. Then `uv run --frozen --no-sync dg check defs`: green.

- [x] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/extract.py \
  corpscout/services/dagster_v3/tests/test_se_company_basic_info_extract.py
git commit -m "refactor(dagster): suggestion extract helper accepts a caller-built change scope"
```

---

### Task 2: The person target, the shared select/scope shapes, and the Bolagsverket extractor

**Files:**
- Create: `src/dagster_v3/defs/se_company/person/suggestions.py`, `src/dagster_v3/defs/se_company/person/bolagsverket.py`
- Modify: `src/dagster_v3/defs/se_company/person/assets.py` (add `EXTRACTOR_SOURCES` and `EXTRACTOR_ASSET_NAMES` only; the normalize asset's `deps` come in Task 5)
- Create: `tests/test_se_company_person_extractors_sql.py`

**Interfaces:**
- Consumes: Task 1's `changed_scope_override`; `basic_info/extract.py::SuggestionTarget`, `define_suggestion_asset`, `insert_page_sql`; `person/tables.py`; `person/normalize.py::SCRATCH_SCOPE_PREFIX`; `person/assets.py::GROUP_NAME`.
- Produces: `PERSON_SELECT_COLUMNS` (16), `PERSON_STATE_COLUMNS` (14), `NULL_SQL`, `LIVE_ROW_PREDICATE`, `PERSON_WITH_SQL`, `PERSON_TRAILING_SELECT_SQL`, `PERSON_TARGET`, `live_select_sql(*, columns, from_sql, where_sql, with_sql="")`, `person_state_sql(alias)`, `stored_live_sql(*, source, columns, scoped)`, `person_changed_scope_sql(*, source, live_sql)`, `person_select_sql(*, source, live_sql)`, `define_person_suggestion_asset(**kwargs)`; and from `bolagsverket.py`: `PERSON_SOURCE`, `BOLAGSVERKET_PERSON_EXTRACTOR_VERSION`, `BOLAGSVERKET_COLUMN_SQL`, `bolagsverket_live_sql(*, scoped=False)`, `bolagsverket_current_sql()`, `bolagsverket_changed_scope_sql()`, `bolagsverket_select_sql()`, asset `se_company_person_suggestions_bolagsverket`; `assets.EXTRACTOR_SOURCES = ("bolagsverket", "esef", "wikidata")`, `assets.EXTRACTOR_ASSET_NAMES`.

- [x] **Step 1: Write the failing test** — `tests/test_se_company_person_extractors_sql.py`

```python
"""Structure and text pins of the person extractors (spec 2026-09-09 sections 3.1 and 6):
every source maps all sixteen raw columns, the slots are the spec's, the select pairs live
rows with per-slot tombstones, the scope is a state hash on both sides, and the INSERT
stamps suggestion_id from the same bound instant as suggested_at. The SQL runs for real in
tests/test_se_company_person_extractors_clickhouse_local.py."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.person import assets, bolagsverket, tables
from dagster_v3.defs.se_company.person.normalize import SCRATCH_SCOPE_PREFIX
from dagster_v3.defs.se_company.person.suggestions import (
    LIVE_ROW_PREDICATE,
    NULL_SQL,
    PERSON_SELECT_COLUMNS,
    PERSON_STATE_COLUMNS,
    PERSON_TARGET,
    PERSON_TRAILING_SELECT_SQL,
    person_state_sql,
)

EXTRACTORS = {
    "bolagsverket": (
        bolagsverket.BOLAGSVERKET_COLUMN_SQL,
        bolagsverket.bolagsverket_live_sql(scoped=True),
        bolagsverket.bolagsverket_select_sql(),
        bolagsverket.bolagsverket_changed_scope_sql(),
        bolagsverket.se_company_person_suggestions_bolagsverket,
    ),
}


def test_the_target_writes_the_eighteen_suggestion_columns() -> None:
    assert PERSON_SELECT_COLUMNS == (
        "company_id", "source", "slot", "source_record_id", "full_name", "first_name",
        "last_name", "birth_year", "wikidata_id", "role_original", "role_key",
        "fiscal_year", "role_from", "role_to", "document_ref", "data",
    )
    assert PERSON_TARGET.qualified_table == tables.QUALIFIED_SUGGESTION_TABLE
    assert PERSON_TARGET.insert_columns == (*PERSON_SELECT_COLUMNS, "suggestion_id", "suggested_at")
    assert sorted(PERSON_TARGET.insert_columns) == sorted(tables.SUGGESTION_COLUMNS)
    assert PERSON_TARGET.asset_prefix == "se_company_person_suggestions_"
    assert PERSON_TARGET.group_name == assets.GROUP_NAME == "se_company_person"
    assert PERSON_TARGET.scratch_prefix == SCRATCH_SCOPE_PREFIX == "corpscout._tmp_person_scope_"
    # The suggestion table has neither column, so the trailing SQL consumes neither binding.
    assert "source_run_id" not in PERSON_TRAILING_SELECT_SQL
    assert "extractor_version" not in PERSON_TRAILING_SELECT_SQL
    assert PERSON_STATE_COLUMNS == tuple(
        c for c in PERSON_SELECT_COLUMNS if c not in ("company_id", "source")
    )


def test_the_insert_binds_one_stamp_for_the_id_and_the_timestamp() -> None:
    sql = insert_page_sql(select_sql="SELECT 1", target=PERSON_TARGET)
    assert sql.startswith(
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} "
        f"({', '.join(PERSON_TARGET.insert_columns)})\nWITH (SELECT now64(3, 'UTC')) AS stamp\n"
    )
    assert (
        "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', "
        "candidate.slot, '\\n', toString(stamp))))) AS suggestion_id, stamp AS suggested_at"
    ) in sql
    assert sql.count("now64(") == 1


def test_every_extractor_maps_all_sixteen_columns_and_names_its_source_once() -> None:
    for source, (columns, live_sql, select_sql, _, _) in EXTRACTORS.items():
        assert set(columns) == set(PERSON_SELECT_COLUMNS), source
        assert columns["source"] == f"'{source}'", source
        for column in PERSON_SELECT_COLUMNS:
            assert f" AS {column}" in live_sql, (source, column)
        # live branch + tombstone branch.
        assert select_sql.count(f" AS {column}") >= 1, (source, column)


def test_every_extractor_binds_company_ids_exactly_twice_in_its_page_select() -> None:
    """jobs.py's page size depends on this: the helper runs every page under
    max_query_size 1 MiB and 20,000 twelve-digit ids render to about 260 KB per binding.
    The `live` CTE is referenced twice but written once, which is what keeps it at two."""
    for source, (_, _, select_sql, scope_sql, _) in EXTRACTORS.items():
        assert select_sql.count("%(company_ids)s") == 2, source
        assert "%(company_ids)s" not in scope_sql, source


def test_every_select_pairs_live_rows_with_tombstones_for_vanished_slots() -> None:
    for source, (_, _, select_sql, _, _) in EXTRACTORS.items():
        assert select_sql.startswith("WITH live AS ("), source
        assert "\nUNION ALL\n" in select_sql, source
        assert (
            "LEFT ANTI JOIN (SELECT company_id, slot FROM live) AS live_slots\n"
            "    ON live_slots.company_id = stored.company_id AND live_slots.slot = stored.slot"
        ) in select_sql, source
        assert f"WHERE source = '{source}' AND {LIVE_ROW_PREDICATE}" in select_sql, source
        assert "'{}' AS data" in select_sql, source
        for column in ("full_name", "first_name", "last_name", "birth_year", "wikidata_id",
                       "role_original", "role_key", "fiscal_year", "role_from", "role_to",
                       "document_ref"):
            assert f"    {NULL_SQL[column]} AS {column}" in select_sql, (source, column)


def test_the_changed_scope_compares_one_state_hash_per_company_on_both_sides() -> None:
    state = person_state_sql("live")
    assert state.startswith("lower(hex(SHA256(arrayStringConcat(arraySort(groupArray(concat(")
    assert "toString(length(ifNull(toString(live.slot), ''))), ':', ifNull(toString(live.slot), '')" in state
    assert "live.data" in state and "live.company_id" not in state
    for source, (_, _, _, scope_sql, _) in EXTRACTORS.items():
        assert scope_sql.count(state) == 2, source
        assert scope_sql.endswith("GROUP BY company_id\nHAVING count() < 2 OR uniqExact(state) > 1"), source
        assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL" in scope_sql, source
        assert f"WHERE source = '{source}' AND {LIVE_ROW_PREDICATE}" in scope_sql, source


def test_bolagsverket_slot_is_the_report_record_uid_and_the_signatory_uid() -> None:
    columns = bolagsverket.BOLAGSVERKET_COLUMN_SQL
    assert columns["slot"] == "concat(s.source_record_uid, ':', toString(s.signatory_uid))"
    assert columns["source_record_id"] == "s.source_record_uid"
    assert columns["full_name"] == NULL_SQL["full_name"]
    assert columns["first_name"] == "nullIf(trim(s.first_name), '')"
    assert columns["last_name"] == "nullIf(trim(s.last_name), '')"
    assert columns["role_original"] == "nullIf(trim(s.role_original), '')"
    assert columns["role_key"] == "nullIf(trim(toString(s.role_kind)), '')"
    assert columns["document_ref"] == "nullIf(s.statement_key, '')"
    assert columns["data"] == (
        "toJSONString(map('signatory_kind', toString(s.signatory_kind), "
        "'statement_key', s.statement_key, 'person_seq', toString(s.person_seq)))"
    )
    live = bolagsverket.bolagsverket_live_sql()
    assert "FROM corpscout.se_financial_report_signatories AS s" in live
    assert (
        "INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
        "    ON universe.company_id = s.company_id"
    ) in live
    assert "WHERE (trim(s.first_name) != '' OR trim(s.last_name) != '')" in live
    assert "%(company_ids)s" not in live
    assert "%(company_ids)s" in bolagsverket.bolagsverket_live_sql(scoped=True)
    assert bolagsverket.BOLAGSVERKET_PERSON_EXTRACTOR_VERSION == "bolagsverket-person-v1"


def test_the_current_sql_is_only_the_since_escape_hatch() -> None:
    current = bolagsverket.bolagsverket_current_sql()
    assert current.endswith("GROUP BY s.company_id")
    assert "max(s.resolved_at) AS observed_at" in current
    assert "%(company_ids)s" not in current


def test_assets_are_named_grouped_and_declared() -> None:
    assert assets.EXTRACTOR_SOURCES == ("bolagsverket", "esef", "wikidata")
    assert assets.EXTRACTOR_ASSET_NAMES == tuple(
        f"se_company_person_suggestions_{s}" for s in assets.EXTRACTOR_SOURCES
    )
    for source, (_, _, _, _, asset) in EXTRACTORS.items():
        assert asset.key == dg.AssetKey(f"se_company_person_suggestions_{source}")
        assert asset.group_names_by_key[asset.key] == "se_company_person"
```

- [x] **Step 2: Run to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_extractors_sql.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.person.suggestions'`.

- [x] **Step 3: Write `suggestions.py`**

```python
"""The person entity's target for the shared suggestion extract helper (spec 2026-09-09
section 6), and the two SQL shapes every person source needs.

A person source delivers MANY rows per company and se_company_person_suggestion carries no
observed_at column, so neither of the address entity's two moves works here.

1. THE CHANGE SCAN IS A PER-COMPANY STATE HASH, not a timestamp. Each side -- what the
   source delivers now, and the company's live (non-tombstone) suggestion rows -- is
   reduced to one sha256 over its sorted, length-prefixed rows. A company is visited when
   only one side has it (never suggested, or gone from the source) or the two hashes
   differ; equal hashes mean there is nothing to write, so the scan converges after one
   pass. A timestamp would not: se_financial_report_signatories is rebuilt whole on every
   run and all 5.5M of its rows carry the same resolved_at, so `newer than last time` is
   either everything or nothing, and a signature line that disappeared has no row left to
   carry a stamp at all.
2. TOMBSTONES ARE PER SLOT, not per company. The page select is `live UNION ALL
   tombstones`, the tombstones being the stored live slots the source no longer delivers --
   a LEFT ANTI JOIN of the stored slots against the same `live` CTE. Writing the tombstone
   takes the row out of `live`, so the next scan sees equal hashes and stops.

The `live` CTE is referenced twice but written once, and that matters: `%(company_ids)s`
renders about 13 bytes per id and the helper executes every page under
ID_BOUND_QUERY_SETTINGS' max_query_size of 1 MiB. Each person page select binds the ids
exactly twice (the live CTE and the stored-slot read), which is why jobs.py pages at 10,000.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget, define_suggestion_asset
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.assets import GROUP_NAME
from dagster_v3.defs.se_company.person.normalize import SCRATCH_SCOPE_PREFIX

# The sixteen columns a source supplies: every suggestion column except the two the INSERT
# stamps. There is no source_run_id and no extractor_version on this table.
PERSON_SELECT_COLUMNS: tuple[str, ...] = tuple(
    column for column in tables.SUGGESTION_COLUMNS if column not in ("suggestion_id", "suggested_at")
)
# What a company's state hash covers: everything a source delivers except its own key.
PERSON_STATE_COLUMNS: tuple[str, ...] = tuple(
    column for column in PERSON_SELECT_COLUMNS if column not in ("company_id", "source")
)

# The eleven nullable person columns, with the exact CAST each side of every UNION ALL uses,
# so a tombstone row and a source that does not deliver a column agree on the type.
NULLABLE_PERSON_COLUMNS: tuple[tuple[str, str], ...] = (
    ("full_name", "String"), ("first_name", "String"), ("last_name", "String"),
    ("birth_year", "UInt16"), ("wikidata_id", "String"), ("role_original", "String"),
    ("role_key", "String"), ("fiscal_year", "UInt16"), ("role_from", "Date"),
    ("role_to", "Date"), ("document_ref", "String"),
)
NULL_SQL: Mapping[str, str] = {
    column: f"CAST(NULL AS Nullable({type_}))" for column, type_ in NULLABLE_PERSON_COLUMNS
}

# A live row always carries a name (every extractor filters nameless source rows out); a
# tombstone carries none. Testing the three name columns is the cheap form of `every person
# column is NULL`, and it is what keeps a tombstone out of both sides of the state hash.
LIVE_ROW_PREDICATE = "(full_name IS NOT NULL OR first_name IS NOT NULL OR last_name IS NOT NULL)"

# Two now64() occurrences in one statement are not guaranteed the same instant (measured at
# about 0.5% of executions on the address entity, and when they differ every row of the page
# is affected). A scalar subquery binds it once; toString(DateTime64(3, 'UTC')) prints
# YYYY-MM-DD HH:MM:SS.mmm, which is what the id hashes.
PERSON_WITH_SQL = "WITH (SELECT now64(3, 'UTC')) AS stamp\n"
PERSON_TRAILING_SELECT_SQL = (
    "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', "
    "candidate.slot, '\\n', toString(stamp))))) AS suggestion_id, stamp AS suggested_at"
)

PERSON_TARGET = SuggestionTarget(
    database=tables.DATABASE,
    table=tables.SUGGESTION_TABLE,
    insert_columns=(*PERSON_SELECT_COLUMNS, "suggestion_id", "suggested_at"),
    select_columns=PERSON_SELECT_COLUMNS,
    trailing_select_sql=PERSON_TRAILING_SELECT_SQL,
    asset_prefix="se_company_person_suggestions_",
    group_name=GROUP_NAME,
    scratch_prefix=SCRATCH_SCOPE_PREFIX,
    with_sql=PERSON_WITH_SQL,
)


def live_select_sql(
    *, columns: Mapping[str, str], from_sql: str, where_sql: str, with_sql: str = ""
) -> str:
    """One source's current rows, projected onto PERSON_SELECT_COLUMNS in order.

    `columns` maps every one of the sixteen names to its SQL expression, so a source that
    forgets one fails at import instead of writing a short row into a UNION ALL whose other
    branch has all sixteen.
    """
    missing = sorted(set(PERSON_SELECT_COLUMNS) - set(columns))
    extra = sorted(set(columns) - set(PERSON_SELECT_COLUMNS))
    if missing or extra:
        raise ValueError(f"live_select_sql columns: missing={missing} extra={extra}")
    projection = ",\n".join(f"    {columns[column]} AS {column}" for column in PERSON_SELECT_COLUMNS)
    return f"{with_sql}SELECT\n{projection}\n{from_sql}\n{where_sql}"


def person_state_sql(alias: str) -> str:
    """One company's whole delivered state for one source, as a single hash.

    Length-prefixed fields (the shape corpscout's own person_profile_hash uses) so no value
    containing the separator can imitate another row; arraySort so the row order a scan
    happens to produce cannot change the hash; ifNull(toString(...), '') so a NULL and an
    empty string are told apart by the length prefix rather than by concat returning NULL.
    """
    fields: list[str] = []
    for column in PERSON_STATE_COLUMNS:
        value = f"ifNull(toString({alias}.{column}), '')"
        fields.append(f"toString(length({value})), ':', {value}")
    row = ", '\\n', ".join(fields)
    return f"lower(hex(SHA256(arrayStringConcat(arraySort(groupArray(concat({row}))), '\\n'))))"


def stored_live_sql(*, source: str, columns: Sequence[str], scoped: bool) -> str:
    """The company's live (non-tombstone) suggestion rows for one source.

    The source is a literal rather than %(source)s because `run_extractor` binds `source`
    only into the scope's params, never into the page select's.
    """
    scope = "\n    AND company_id IN %(company_ids)s" if scoped else ""
    return (
        f"SELECT company_id, {', '.join(columns)}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n"
        f"WHERE source = '{source}' AND {LIVE_ROW_PREDICATE}{scope}"
    )


def person_changed_scope_sql(*, source: str, live_sql: str) -> str:
    """Companies whose delivered state differs from what is stored.

    Both sides are aliased `live` so the state expression is one identical text: any drift
    between them would re-extract every company on every run. Two aggregations and no join,
    so the result cannot depend on join_use_nulls. `count() < 2` catches a company only one
    side has -- new, or vanished from the source and due its tombstones.
    """
    state = person_state_sql("live")
    stored = stored_live_sql(source=source, columns=PERSON_STATE_COLUMNS, scoped=False)
    return (
        "SELECT company_id FROM (\n"
        f"    SELECT company_id, {state} AS state\n"
        f"    FROM ({live_sql}) AS live\n"
        "    GROUP BY company_id\n"
        "    UNION ALL\n"
        f"    SELECT company_id, {state} AS state\n"
        f"    FROM ({stored}) AS live\n"
        "    GROUP BY company_id\n"
        ") AS sides\n"
        "GROUP BY company_id\n"
        "HAVING count() < 2 OR uniqExact(state) > 1"
    )


def person_select_sql(*, source: str, live_sql: str) -> str:
    """The page's rows: everything the source still delivers, plus one tombstone per stored
    live slot it no longer delivers (spec 3.1: every person column NULL, `data` '{}')."""
    live_projection = ", ".join(f"live.{column} AS {column}" for column in PERSON_SELECT_COLUMNS)
    tombstone_values: dict[str, str] = {
        "company_id": "stored.company_id",
        "source": f"'{source}'",
        "slot": "stored.slot",
        "source_record_id": "''",
        "data": "'{}'",
        **NULL_SQL,
    }
    tombstone_projection = ",\n".join(
        f"    {tombstone_values[column]} AS {column}" for column in PERSON_SELECT_COLUMNS
    )
    stored_slots = stored_live_sql(source=source, columns=("slot",), scoped=True)
    return (
        f"WITH live AS (\n{live_sql}\n)\n"
        f"SELECT {live_projection}\n"
        "FROM live\n"
        "UNION ALL\n"
        f"SELECT\n{tombstone_projection}\n"
        f"FROM (\n{stored_slots}\n) AS stored\n"
        "LEFT ANTI JOIN (SELECT company_id, slot FROM live) AS live_slots\n"
        "    ON live_slots.company_id = stored.company_id AND live_slots.slot = stored.slot"
    )


def define_person_suggestion_asset(**kwargs: Any) -> dg.AssetsDefinition:
    return define_suggestion_asset(target=PERSON_TARGET, **kwargs)
```

- [x] **Step 4: Write `bolagsverket.py`**

```python
"""Bolagsverket annual-report signatories -> raw person suggestions (spec 2026-09-09
sections 3.1 and 6).

One suggestion per signature line. se_financial_report_signatories holds one row per
(report, signatory kind, person sequence), so a person who signs two years -- or signs one
report both in the board section and on the certification -- has two slots, which is what
spec 3.1.1 asks for. The slot is the report's source_record_uid plus the table's own
MATERIALIZED signatory_uid; ClickHouse computes both from the row's natural key, so a
re-parse that reproduces the same signature line reproduces the same slot and rewrites the
row in place instead of tombstoning it and inventing a new one.

The universe is se_company_basic_info: 31 of the 577,932 signatory companies have no
basic-info row (measured 2026-09-09) and are dropped here rather than published as company
ids nothing downstream knows.
"""

import dagster as dg

from dagster_v3.defs.se_company.person.suggestions import (
    NULL_SQL,
    define_person_suggestion_asset,
    live_select_sql,
    person_changed_scope_sql,
    person_select_sql,
)

PERSON_SOURCE = "bolagsverket"
BOLAGSVERKET_PERSON_EXTRACTOR_VERSION = "bolagsverket-person-v1"

UNIVERSE_JOIN_SQL = (
    "INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
    "    ON universe.company_id = s.company_id"
)

BOLAGSVERKET_COLUMN_SQL: dict[str, str] = {
    "company_id": "s.company_id",
    "source": f"'{PERSON_SOURCE}'",
    "slot": "concat(s.source_record_uid, ':', toString(s.signatory_uid))",
    "source_record_id": "s.source_record_uid",
    # Bolagsverket delivers the split name, never one string.
    "full_name": NULL_SQL["full_name"],
    "first_name": "nullIf(trim(s.first_name), '')",
    "last_name": "nullIf(trim(s.last_name), '')",
    "birth_year": NULL_SQL["birth_year"],
    "wikidata_id": NULL_SQL["wikidata_id"],
    "role_original": "nullIf(trim(s.role_original), '')",
    # role_kind is the map key in roles.py; 'unknown' is its roleless code.
    "role_key": "nullIf(trim(toString(s.role_kind)), '')",
    # fiscal_year is Int32 in the source and Nullable(UInt16) here (spec 4.3: this is the
    # role year); anything outside UInt16 is a parse artefact, not a year.
    "fiscal_year": "if(s.fiscal_year BETWEEN 1900 AND 2155, toUInt16(s.fiscal_year), CAST(NULL AS Nullable(UInt16)))",
    "role_from": NULL_SQL["role_from"],
    "role_to": NULL_SQL["role_to"],
    "document_ref": "nullIf(s.statement_key, '')",
    # Spec 3.1.2's Bolagsverket extras. The report has no fiscal-year-end column here --
    # fiscal_year (the year itself) is the only period marker the table carries, and it has
    # its own column above.
    "data": (
        "toJSONString(map('signatory_kind', toString(s.signatory_kind), "
        "'statement_key', s.statement_key, 'person_seq', toString(s.person_seq)))"
    ),
}


def bolagsverket_live_sql(*, scoped: bool = False) -> str:
    where_sql = "WHERE (trim(s.first_name) != '' OR trim(s.last_name) != '')"
    if scoped:
        where_sql += "\n    AND s.company_id IN %(company_ids)s"
    return live_select_sql(
        columns=BOLAGSVERKET_COLUMN_SQL,
        from_sql=f"FROM corpscout.se_financial_report_signatories AS s\n{UNIVERSE_JOIN_SQL}",
        where_sql=where_sql,
    )


def bolagsverket_current_sql() -> str:
    """(company_id, observed_at) for `since` only. The change scan is the state hash: the
    whole table is rebuilt on every run with a single resolved_at, so this watermark says
    `all` or `none` and nothing in between."""
    return (
        "SELECT s.company_id AS company_id, max(s.resolved_at) AS observed_at\n"
        "FROM corpscout.se_financial_report_signatories AS s\n"
        f"{UNIVERSE_JOIN_SQL}\n"
        "GROUP BY s.company_id"
    )


def bolagsverket_changed_scope_sql() -> str:
    return person_changed_scope_sql(source=PERSON_SOURCE, live_sql=bolagsverket_live_sql())


def bolagsverket_select_sql() -> str:
    return person_select_sql(source=PERSON_SOURCE, live_sql=bolagsverket_live_sql(scoped=True))


se_company_person_suggestions_bolagsverket = define_person_suggestion_asset(
    source=PERSON_SOURCE,
    extractor_version=BOLAGSVERKET_PERSON_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=bolagsverket_select_sql(),
    changed_scope_override=bolagsverket_changed_scope_sql(),
    deps=[
        dg.AssetKey("se_financial_report_signatories_clickhouse"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every Swedish annual-report signature line as a raw person suggestion in "
        "se_company_person_suggestion (slot = report record uid + signatory uid); a line the "
        "rebuilt source no longer delivers is tombstoned. execute=false previews."
    ),
)
```

In `person/assets.py`, add directly under `NORMALIZE_POOL`:

```python
EXTRACTOR_SOURCES: tuple[str, ...] = ("bolagsverket", "esef", "wikidata")
EXTRACTOR_ASSET_NAMES: tuple[str, ...] = tuple(
    f"se_company_person_suggestions_{source}" for source in EXTRACTOR_SOURCES
)
```

- [x] **Step 5: Run the pins to verify they pass**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_extractors_sql.py tests/test_se_company_person_normalize.py tests/test_se_company_person_tables.py -q`
Expected: all PASS. Then `uv run --frozen --no-sync dg check defs`: green, and `uv run --frozen --no-sync dg list defs | grep se_company_person_suggestions` lists exactly `se_company_person_suggestions_bolagsverket`.

- [x] **Step 6: Write the source-table fixture** — `tests/fixtures/se_company_person_source_tables.sql`

Three tables no migration can supply cleanly: `se_financial_report_signatories` is a 000143 `CREATE` plus a 000287 `RENAME` plus `ALTER`s in 000244 and 000289, and the two Wikidata tables are likewise spread over 000018/000152/000244/000268/000289. `company_identifier`, `esef_entity_registry_map` and `wikidata_company_identifiers` are **not** repeated here — `tests/fixtures/se_basic_info_source_tables.sql` already has them and the harness loads both files.

```sql
-- Production SHOW CREATE TABLE snapshot (2026-09-09), SETTINGS stripped.
-- Harness fixture only -- not a migration, never apply to a real ClickHouse.
-- The MATERIALIZED and DEFAULT expressions are load-bearing: the Bolagsverket extractor's
-- slot is source_record_uid + signatory_uid, and both are computed by ClickHouse here.

CREATE TABLE IF NOT EXISTS corpscout.se_financial_report_signatories (
    `company_id` String,
    `fiscal_year` Int32,
    `statement_key` String,
    `source_record_uid` String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nstructured\nsweden_financial\nannual_report_xhtml\n', statement_key, '\n', statement_key)))),
    `signatory_kind` LowCardinality(String),
    `person_seq` UInt16,
    `signatory_uid` FixedString(64) MATERIALIZED lower(hex(SHA256(concat('sweden-financial-report-signatory-v1\n', company_id, '\n', statement_key, '\n', signatory_kind, '\n', toString(person_seq))))),
    `first_name` String,
    `last_name` String,
    `person_profile_hash` FixedString(64) MATERIALIZED lower(hex(SHA256(concat('company-person-profile-v1\n', toString(length(lowerUTF8(trimBoth(first_name)))), ':', lowerUTF8(trimBoth(first_name)), '\n', toString(length(lowerUTF8(trimBoth(last_name)))), ':', lowerUTF8(trimBoth(last_name)))))),
    `role_original` String,
    `role_kind` LowCardinality(String),
    `person_role_hash` FixedString(64) MATERIALIZED lower(hex(SHA256(concat('company-person-role-v1\n', toString(length(lowerUTF8(trimBoth(role_original)))), ':', lowerUTF8(trimBoth(role_original)), '\n', toString(length(lowerUTF8(trimBoth(role_kind)))), ':', lowerUTF8(trimBoth(role_kind)), '\n', toString(length(lowerUTF8(trimBoth(signatory_kind)))), ':', lowerUTF8(trimBoth(signatory_kind)), '\n', toString(fiscal_year)))))),
    `resolved_at` DateTime64(3, 'UTC')
) ENGINE = MergeTree ORDER BY (company_id, fiscal_year, statement_key, signatory_kind, person_seq);

CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_people (
    `company_wikidata_id` String,
    `person_wikidata_id` String,
    `role_property` LowCardinality(String),
    `role_label` LowCardinality(String),
    `start_date` Nullable(Date),
    `end_date` Nullable(Date),
    `is_current` UInt8,
    `source_system` LowCardinality(String),
    `source_run_id` String,
    `source_record_id` String,
    `source_payload_hash` FixedString(64),
    `retrieved_at` DateTime64(3, 'UTC'),
    `resolved_at` DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(resolved_at) ORDER BY (company_wikidata_id, role_property, person_wikidata_id);

CREATE TABLE IF NOT EXISTS corpscout.wikidata_persons (
    `person_wikidata_id` String,
    `source_record_uid` String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nstructured\nwikidata\nwikidata_person_item\n', person_wikidata_id, '\n', lowerUTF8(toString(source_payload_hash)))))),
    `name` String,
    `name_normalized` String,
    `description` Nullable(String),
    `birth_year` Nullable(UInt16),
    `image_url` Nullable(String),
    `wikidata_url` Nullable(String),
    `source_system` LowCardinality(String),
    `source_run_id` String,
    `source_record_id` String,
    `source_payload_hash` FixedString(64),
    `retrieved_at` DateTime64(3, 'UTC'),
    `resolved_at` DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(resolved_at) ORDER BY (person_wikidata_id);
```

- [x] **Step 7: Write the clickhouse-local proof** — `tests/test_se_company_person_extractors_clickhouse_local.py`

```python
"""The person extractors' SQL on a real ClickHouse (spec 2026-09-09 sections 3.1 and 6).

Claims a fake client cannot settle:
1. Each page select really produces the sixteen suggestion columns, in a UNION ALL whose
   live and tombstone branches agree on every column type.
2. `data` is a JSON object on every row -- the table's CONSTRAINT valid_data (Code: 469) is
   the judge, and the tombstone branch's literal '{}' has to pass it too.
3. suggestion_id equals sha256(company_id, source, slot, suggested_at) for every row, so
   the WITH-bound stamp really is the instant the id hashed.
4. The state-hash scope selects a company before anything is suggested, selects nothing
   after the page is written (it converges), selects it again when the rebuilt source drops
   one of its rows, and converges again once the tombstone is written -- under
   join_use_nulls 0 and 1, which the scope's two aggregations exist to be immune to.
5. A company outside se_company_basic_info never reaches the suggestion table.
6. The normalize hand-off (`changed_rows_sql()` + `normalized_row()`) reads what these
   extractors wrote and gives the expected parse statuses, the tombstone included.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.person import bolagsverket, tables
from dagster_v3.defs.se_company.person.normalize import (
    RAW_ROW_COLUMNS,
    changed_rows_sql,
    normalized_row,
)
from dagster_v3.defs.se_company.person.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.person.suggestions import PERSON_SELECT_COLUMNS, PERSON_TARGET
from tests.clickhouse_local import clickhouse_local_command, render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
# (file, the exact CREATE TABLE header line to keep). The trailing newline anchors the whole
# name: `corpscout.se_company_person_` would also be a prefix of five dropped tables, and
# `corpscout.esef_document_people` is a prefix of esef_document_people_legacy.
WANTED_CREATES = (
    ("000396_corpscout_se_company_person_entity.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_suggestion\n"),
    ("000396_corpscout_se_company_person_entity.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_normalized\n"),
    ("000377_corpscout_se_company_basic_info.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info\n"),
    ("000395_corpscout_esef_country_agnostic_products.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.esef_document_people\n"),
)

COMPANY_BV = "5561552760"
COMPANY_OUTSIDE = "5569999999"
STATEMENT_KEY = "st1"
BOARD_DATA = '{"signatory_kind":"board_signature","statement_key":"st1","person_seq":"1"}'
CERT_DATA = '{"signatory_kind":"certification","statement_key":"st1","person_seq":"1"}'

RAW_ROW_CHECK_COLUMNS = (*PERSON_SELECT_COLUMNS, "suggested_at")
RAW_ROWS_SQL = (
    f"SELECT {', '.join(PERSON_SELECT_COLUMNS)}, toString(suggested_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL ORDER BY company_id, source, slot"
)
# The same hash formula as PERSON_TRAILING_SELECT_SQL: '\\n' in the Python source is a
# literal backslash-n in the SQL text, which ClickHouse's string literal turns into a real
# newline when concat() builds the hashed string.
IDENTITY_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    "WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\\n', toString(source), '\\n', "
    "slot, '\\n', toString(suggested_at)))))"
)
DATA_CHECK_SQL = (
    f"SELECT countIf(JSONType(data) != 'Object') FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"
)
OUTSIDE_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    f"WHERE company_id = '{COMPANY_OUTSIDE}'"
)


def _statements(path: Path, keep) -> list[str]:
    out: list[str] = []
    for raw in path.read_text(encoding="utf-8").split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement and keep(statement):
            out.append(statement)
    return out


def _schema() -> list[str]:
    schema = ["CREATE DATABASE IF NOT EXISTS corpscout"]
    for name, header in WANTED_CREATES:
        found = _statements(MIGRATIONS_DIR / name, lambda s, h=header: s.startswith(h.rstrip("\n")) and h in s + "\n")
        assert len(found) == 1, (name, header, len(found))
        schema += found
    for fixture in ("se_basic_info_source_tables.sql", "se_company_person_source_tables.sql"):
        schema += [s.strip() for s in (FIXTURES_DIR / fixture).read_text(encoding="utf-8").split(";") if s.strip()]
    return schema


def _insert(select_sql: str, ids: list[str], *, extractor_version: str) -> str:
    return render(
        insert_page_sql(select_sql=select_sql, target=PERSON_TARGET),
        {"company_ids": ids, "source_run_id": "run-1", "extractor_version": extractor_version},
    )


def _scope(scope_sql: str, source: str) -> str:
    return render(scope_sql, {"source": source}) + "\nORDER BY company_id"


def _ordered(sql: str, order_by: str) -> str:
    return f"SELECT * FROM ({sql}) AS ordered ORDER BY {order_by}"


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


def _as_row(fields: list[str]) -> tuple[str | None, ...]:
    assert len(fields) == len(RAW_ROW_COLUMNS), fields
    return tuple(None if field == "\\N" else field for field in fields)


SIGNATORY_COLUMNS = (
    "company_id, fiscal_year, statement_key, signatory_kind, person_seq, "
    "first_name, last_name, role_original, role_kind, resolved_at"
)
BOARD_ROW = (
    f"('{COMPANY_BV}', 2024, '{STATEMENT_KEY}', 'board_signature', 1, 'Anna', 'Svensson', "
    "'Styrelseledamot', 'board_member', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)
CERT_ROW = (
    f"('{COMPANY_BV}', 2024, '{STATEMENT_KEY}', 'certification', 1, 'Anna', 'Svensson', "
    "'Styrelseledamot', 'board_member', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)
OUTSIDE_ROW = (
    f"('{COMPANY_OUTSIDE}', 2024, 'st9', 'board_signature', 1, 'Nils', 'Utanfor', "
    "'', 'unknown', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)


def _signatory_insert(rows: tuple[str, ...]) -> str:
    return (
        f"INSERT INTO corpscout.se_financial_report_signatories ({SIGNATORY_COLUMNS}) VALUES "
        + ", ".join(rows)
    )


def _script_statements() -> list[str]:
    bv_scope = _scope(bolagsverket.bolagsverket_changed_scope_sql(), "bolagsverket")
    bv_insert = _insert(
        bolagsverket.bolagsverket_select_sql(), [COMPANY_BV],
        extractor_version=bolagsverket.BOLAGSVERKET_PERSON_EXTRACTOR_VERSION,
    )
    changed_rows = _ordered(
        render(changed_rows_sql(), {"company_ids": [COMPANY_BV], "normalizer_version": NORMALIZER_VERSION}),
        "company_id, source, slot",
    )
    return [
        *_schema(),
        # The universe: COMPANY_OUTSIDE deliberately has no basic-info row.
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_BV}')",
        _signatory_insert((BOARD_ROW, CERT_ROW, OUTSIDE_ROW)),
        "SELECT '@@bv_scope_1'",
        bv_scope,
        bv_insert,
        "SELECT '@@bv_rows_1'",
        RAW_ROWS_SQL,
        "SELECT '@@identity_check'",
        IDENTITY_CHECK_SQL,
        "SELECT '@@data_check'",
        DATA_CHECK_SQL,
        "SELECT '@@outside_check'",
        OUTSIDE_CHECK_SQL,
        "SELECT '@@bv_scope_2'",
        bv_scope,
        # The source is rebuilt whole (stage + EXCHANGE TABLES on prod) and this time the
        # certification signature line is gone.
        "SELECT sleep(0.01) FORMAT Null",
        "TRUNCATE TABLE corpscout.se_financial_report_signatories",
        _signatory_insert((BOARD_ROW, OUTSIDE_ROW)),
        "SELECT '@@bv_scope_3'",
        bv_scope,
        bv_insert,
        "SELECT '@@bv_rows_2'",
        RAW_ROWS_SQL,
        "SELECT '@@bv_scope_4'",
        bv_scope,
        "SELECT '@@data_check_2'",
        DATA_CHECK_SQL,
        "SELECT '@@changed_rows'",
        changed_rows,
    ]


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    script = f"SET join_use_nulls = {request.param};\n" + ";\n".join(_script_statements()) + ";\n"
    try:
        completed = subprocess.run(
            clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return _sections([line for line in completed.stdout.splitlines() if line.strip()])


def _rows(sections: dict[str, list[list[str]]], name: str) -> list[dict[str, str]]:
    return [dict(zip(RAW_ROW_CHECK_COLUMNS, fields, strict=True)) for fields in sections[name]]


def test_the_scope_selects_the_company_then_converges(sections) -> None:
    assert sections["bv_scope_1"] == [[COMPANY_BV]]
    assert sections["bv_scope_2"] == []


def test_both_signature_lines_land_as_their_own_slot(sections) -> None:
    rows = _rows(sections, "bv_rows_1")
    assert len(rows) == 2
    assert {row["data"] for row in rows} == {BOARD_DATA, CERT_DATA}
    assert len({row["slot"] for row in rows}) == 2
    for row in rows:
        assert row["source"] == "bolagsverket"
        assert row["first_name"] == "Anna" and row["last_name"] == "Svensson"
        assert row["full_name"] == "\\N"
        assert row["role_original"] == "Styrelseledamot"
        assert row["role_key"] == "board_member"
        assert row["fiscal_year"] == "2024"
        assert row["document_ref"] == STATEMENT_KEY
        assert row["birth_year"] == row["wikidata_id"] == "\\N"
        assert row["role_from"] == row["role_to"] == "\\N"
        # slot = report record uid + signatory uid, both 64 hex characters.
        record_uid, signatory_uid = row["slot"].split(":")
        assert len(record_uid) == len(signatory_uid) == 64
        assert row["source_record_id"] == record_uid


def test_suggestion_id_matches_the_stamp_hash_and_data_is_always_an_object(sections) -> None:
    assert sections["identity_check"] == [["0"]]
    assert sections["data_check"] == [["0"]]
    assert sections["data_check_2"] == [["0"]]


def test_a_company_outside_the_basic_info_universe_is_never_written(sections) -> None:
    assert sections["outside_check"] == [["0"]]


def test_a_vanished_signature_line_is_tombstoned_and_the_scope_reconverges(sections) -> None:
    assert sections["bv_scope_3"] == [[COMPANY_BV]]
    before = {row["slot"]: row for row in _rows(sections, "bv_rows_1")}
    rows = _rows(sections, "bv_rows_2")
    assert len(rows) == 2
    tombstones = [row for row in rows if row["data"] == "{}"]
    assert len(tombstones) == 1
    [tombstone] = tombstones
    for column in ("full_name", "first_name", "last_name", "birth_year", "wikidata_id",
                   "role_original", "role_key", "fiscal_year", "role_from", "role_to",
                   "document_ref"):
        assert tombstone[column] == "\\N", column
    assert tombstone["source_record_id"] == ""
    assert before[tombstone["slot"]]["data"] == CERT_DATA
    assert tombstone["suggested_at"] > before[tombstone["slot"]]["suggested_at"]
    [survivor] = [row for row in rows if row["data"] != "{}"]
    assert survivor["data"] == BOARD_DATA
    assert survivor["suggested_at"] > before[survivor["slot"]]["suggested_at"]
    assert sections["bv_scope_4"] == []


def test_the_normalize_hand_off_gives_the_expected_parse_statuses(sections) -> None:
    rows = [normalized_row(_as_row(fields), None) for fields in sections["changed_rows"]]
    status = tables.NORMALIZED_COLUMNS.index("parse_status")
    notes = tables.NORMALIZED_COLUMNS.index("parse_notes")
    code = tables.NORMALIZED_COLUMNS.index("role_code")
    name = tables.NORMALIZED_COLUMNS.index("display_name")
    by_status = sorted((row[status], row[name], row[code], tuple(row[notes])) for row in rows)
    assert by_status == [
        ("no_person", "", None, ("empty name",)),
        ("ok", "Anna Svensson", "board_member", ()),
    ]
```

`normalized_row(row, None)` passes `None` as `normalized_at`: this test never inserts into the normalized table, so the stamp is unused. If a later change makes it load-bearing, pass `datetime(2026, 9, 9, tzinfo=UTC)`.

- [x] **Step 8: Run the proof**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_extractors_clickhouse_local.py -q -m integration`
Expected: PASS twice (both `join_use_nulls` parameters), via the docker fallback if the machine has no `clickhouse-local` binary.

- [x] **Step 9: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/suggestions.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/bolagsverket.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/assets.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_extractors_sql.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_extractors_clickhouse_local.py \
  corpscout/services/dagster_v3/tests/fixtures/se_company_person_source_tables.sql
git commit -m "feat(dagster): person suggestion target and the Bolagsverket signatory extractor"
```

---

### Task 3: The ESEF extractor

**Files:**
- Create: `src/dagster_v3/defs/se_company/person/esef.py`
- Modify: `tests/test_se_company_person_extractors_sql.py` (add the ESEF entry and its own pins)
- Modify: `tests/test_se_company_person_extractors_clickhouse_local.py` (add the ESEF fixture rows and sections)

**Interfaces:**
- Consumes: Task 2's `NULL_SQL`, `live_select_sql`, `person_changed_scope_sql`, `person_select_sql`, `define_person_suggestion_asset`.
- Produces: `PERSON_SOURCE = "esef"`, `ESEF_PERSON_EXTRACTOR_VERSION = "esef-person-v1"`, `ESEF_COLUMN_SQL`, `esef_live_sql(*, scoped=False)`, `esef_current_sql()`, `esef_changed_scope_sql()`, `esef_select_sql()`, asset `se_company_person_suggestions_esef`.

- [x] **Step 1: Write the failing test** — add to `tests/test_se_company_person_extractors_sql.py`

Add `from dagster_v3.defs.se_company.person import esef` to the imports, add the entry to `EXTRACTORS` (so every shared test above covers it too):

```python
    "esef": (
        esef.ESEF_COLUMN_SQL,
        esef.esef_live_sql(scoped=True),
        esef.esef_select_sql(),
        esef.esef_changed_scope_sql(),
        esef.se_company_person_suggestions_esef,
    ),
```

and append:

```python
def test_esef_slot_is_the_document_and_the_extractions_candidate_uid() -> None:
    columns = esef.ESEF_COLUMN_SQL
    assert columns["slot"] == "concat(e.source_document_id, ':', toString(e.candidate_uid))"
    assert columns["source_record_id"] == "toString(e.source_record_uid)"
    assert columns["full_name"] == "nullIf(trim(e.name), '')"
    assert columns["first_name"] == NULL_SQL["first_name"]
    assert columns["last_name"] == NULL_SQL["last_name"]
    assert columns["role_original"] == "nullIf(trim(e.role), '')"
    assert columns["role_key"] == "nullIf(trim(toString(e.role_category)), '')"
    assert columns["role_from"] == "accurateCastOrNull(e.effective_from, 'Date')"
    assert columns["role_to"] == "accurateCastOrNull(e.effective_to, 'Date')"
    assert columns["document_ref"] == "nullIf(e.source_document_id, '')"
    assert "'evidence_ids', arrayStringConcat(e.evidence_ids, ',')" in columns["data"]
    assert columns["data"].startswith("toJSONString(map(")
    live = esef.esef_live_sql()
    assert "FROM corpscout.se_esef_document_people AS e" in live
    # The view already reads its product FINAL (esef_filings/country_views.py): a consumer
    # that adds another FINAL after a view name is a bug.
    assert "se_esef_document_people AS e FINAL" not in live
    assert "WHERE trim(e.name) != ''" in live
    assert esef.ESEF_PERSON_EXTRACTOR_VERSION == "esef-person-v1"
```

- [x] **Step 2: Run to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_extractors_sql.py -q`
Expected: FAIL with `ImportError: cannot import name 'esef' from 'dagster_v3.defs.se_company.person'`.

- [x] **Step 3: Write `esef.py`**

```python
"""ESEF document people -> raw person suggestions (spec 2026-09-09 sections 3.1 and 6).

Reads corpscout.se_esef_document_people, the country-scoped view of the country-agnostic
extraction (migration 000395). A CONSUMER NEVER WRITES FINAL AFTER THE VIEW NAME: the view
already reads its ReplacingMergeTree product FINAL and carries the register-verified Swedish
link inside it, which is also why this extractor needs no LEI join of its own.

The slot is the document id plus `candidate_uid`, the extraction's own per-person id (spec
3.1.1 calls it "the person's index in the extraction"; the table has no index column and
candidate_uid is the id the extraction assigns). candidate_uid is derived from the extracted
item, so a re-extraction under a different model produces new slots and tombstones the old
ones -- which is exactly what a raw observation layer should record. One document can also
carry rows from two extraction models at once (1,722 (document, name) pairs did on
2026-09-09); both are published, and the fold merges them into one person with two members.

When the dedicated per-filing extraction table lands (the ESEF slice-2 plan), this module
changes in one place: the view name in `from_sql`.
"""

import dagster as dg

from dagster_v3.defs.se_company.person.suggestions import (
    NULL_SQL,
    define_person_suggestion_asset,
    live_select_sql,
    person_changed_scope_sql,
    person_select_sql,
)

PERSON_SOURCE = "esef"
ESEF_PERSON_EXTRACTOR_VERSION = "esef-person-v1"

FROM_SQL = (
    "FROM corpscout.se_esef_document_people AS e\n"
    "INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
    "    ON universe.company_id = e.company_id"
)

ESEF_COLUMN_SQL: dict[str, str] = {
    "company_id": "e.company_id",
    "source": f"'{PERSON_SOURCE}'",
    "slot": "concat(e.source_document_id, ':', toString(e.candidate_uid))",
    "source_record_id": "toString(e.source_record_uid)",
    # ESEF delivers one name string; the normalizer splits it.
    "full_name": "nullIf(trim(e.name), '')",
    "first_name": NULL_SQL["first_name"],
    "last_name": NULL_SQL["last_name"],
    "birth_year": NULL_SQL["birth_year"],
    "wikidata_id": NULL_SQL["wikidata_id"],
    "role_original": "nullIf(trim(e.role), '')",
    # role_category is the map key in roles.py; 'other' is not in its map and falls through
    # to the label, which is the owner's never-bucket rule.
    "role_key": "nullIf(trim(toString(e.role_category)), '')",
    # The document's fiscal year is this row's role year (spec 4.3).
    "fiscal_year": "if(e.fiscal_year BETWEEN 1900 AND 2155, e.fiscal_year, CAST(NULL AS Nullable(UInt16)))",
    # effective_from/to are Date32 in the source and Date here; accurateCastOrNull turns a
    # date outside Date's range into NULL instead of wrapping it into a wrong year.
    "role_from": "accurateCastOrNull(e.effective_from, 'Date')",
    "role_to": "accurateCastOrNull(e.effective_to, 'Date')",
    "document_ref": "nullIf(e.source_document_id, '')",
    # Spec 3.1.2's ESEF extras. evidence_ids is the "section it came from" this extraction
    # actually carries, flattened to a comma list so every map value is one String.
    "data": (
        "toJSONString(map('organization', e.organization, 'status', toString(e.status), "
        "'confidence', toString(e.confidence), "
        "'evidence_ids', arrayStringConcat(e.evidence_ids, ','), "
        "'model_provider', toString(e.model_provider), 'model_name', e.model_name, "
        "'prompt_version', e.prompt_version))"
    ),
}


def esef_live_sql(*, scoped: bool = False) -> str:
    where_sql = "WHERE trim(e.name) != ''"
    if scoped:
        where_sql += "\n    AND e.company_id IN %(company_ids)s"
    return live_select_sql(columns=ESEF_COLUMN_SQL, from_sql=FROM_SQL, where_sql=where_sql)


def esef_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        "SELECT e.company_id AS company_id, max(e.extracted_at) AS observed_at\n"
        f"{FROM_SQL}\n"
        "GROUP BY e.company_id"
    )


def esef_changed_scope_sql() -> str:
    return person_changed_scope_sql(source=PERSON_SOURCE, live_sql=esef_live_sql())


def esef_select_sql() -> str:
    return person_select_sql(source=PERSON_SOURCE, live_sql=esef_live_sql(scoped=True))


se_company_person_suggestions_esef = define_person_suggestion_asset(
    source=PERSON_SOURCE,
    extractor_version=ESEF_PERSON_EXTRACTOR_VERSION,
    current_sql=esef_current_sql(),
    select_sql=esef_select_sql(),
    changed_scope_override=esef_changed_scope_sql(),
    deps=[
        dg.AssetKey("esef_document_people_clickhouse"),
        dg.AssetKey("esef_entity_registry_map_clickhouse"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every person the ESEF extraction found in a Swedish filing as a raw person suggestion "
        "in se_company_person_suggestion (slot = document id + candidate uid); a candidate the "
        "extraction no longer produces is tombstoned. execute=false previews."
    ),
)
```

- [x] **Step 4: Add the ESEF rows and sections to the clickhouse-local proof**

In `tests/test_se_company_person_extractors_clickhouse_local.py`:

Imports gain `esef` from the person package, and the SE view: the harness renders it from the same builder migration 000395 embeds, so the test cannot drift from the deployed view.

```python
from dagster_v3.defs.esef_filings import tables as esef_tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql
from dagster_v3.defs.se_company.person import bolagsverket, esef, tables

ESEF_PEOPLE_VIEW = next(
    view for view in esef_tables.SE_ESEF_VIEWS if view.table == "esef_document_people"
)
LEI = "1FOLRR5RWTWWI397R131"
DOCUMENT_ID = f"{LEI}-2023-12-31-ESEF-SE-0"
ESEF_DATA = (
    '{"organization":"Exempel AB","status":"current","confidence":"0.95",'
    '"evidence_ids":"E0006","model_provider":"glm","model_name":"z-ai\\/glm",'
    '"prompt_version":"esef-people-v1"}'
)
```

`_schema()` appends the view after the tables: `schema.append(build_se_esef_view_sql(ESEF_PEOPLE_VIEW).rstrip(";"))`.

`_script_statements()` gains, right after the basic-info insert:

```python
        "INSERT INTO corpscout.esef_entity_registry_map (lei, country_iso2, registry_id_raw, "
        f"registry_id, match_source, link_status) VALUES ('{LEI}', 'SE', '{COMPANY_BV}', "
        f"'{COMPANY_BV}', 'gleif', 'register_verified')",
        "INSERT INTO corpscout.esef_document_people (candidate_uid, source_record_uid, "
        "source_document_id, lei, fiscal_year, name, role, role_category, organization, status, "
        "effective_from, effective_to, confidence, evidence_ids, model_provider, model_name, "
        "prompt_version, source_run_id, extracted_at) VALUES "
        f"('{'c' * 64}', '{'r' * 64}', '{DOCUMENT_ID}', '{LEI}', 2023, 'Öberg, Håkan', "
        "'Verkställande direktör', 'chief_executive', 'Exempel AB', 'current', "
        "toDate32('2021-07-16'), NULL, 0.95, ['E0006'], 'glm', 'z-ai/glm', 'esef-people-v1', "
        "'run-0', toDateTime64('2026-09-02 00:00:00', 3, 'UTC'))",
```

and, beside each Bolagsverket section, the ESEF twin (`esef_scope`/`esef_insert` built with the same `_scope`/`_insert` helpers, source `"esef"`, `esef.ESEF_PERSON_EXTRACTOR_VERSION`): `@@esef_scope_1` before the insert, `@@esef_scope_2` after it. `RAW_ROWS_SQL` already reads every source, so `bv_rows_1`/`bv_rows_2` become `raw_rows_1`/`raw_rows_2` and the Bolagsverket assertions filter on `row["source"] == "bolagsverket"`. `changed_rows` binds the same single company.

New assertions:

```python
def test_the_esef_row_keeps_the_delivered_full_name_dates_and_extras(sections) -> None:
    assert sections["esef_scope_1"] == [[COMPANY_BV]]
    assert sections["esef_scope_2"] == []
    [row] = [r for r in _rows(sections, "raw_rows_1") if r["source"] == "esef"]
    assert row["slot"] == f"{DOCUMENT_ID}:{'c' * 64}"
    assert row["source_record_id"] == "r" * 64
    assert row["full_name"] == "Öberg, Håkan"
    assert row["first_name"] == row["last_name"] == "\\N"
    assert row["role_original"] == "Verkställande direktör"
    assert row["role_key"] == "chief_executive"
    assert row["fiscal_year"] == "2023"
    assert row["role_from"] == "2021-07-16" and row["role_to"] == "\\N"
    assert row["document_ref"] == DOCUMENT_ID
    assert row["data"] == ESEF_DATA
```

and `test_the_normalize_hand_off_gives_the_expected_parse_statuses` grows its expected list by `("ok", "Håkan Öberg", "chief_executive_officer", ())` — the comma form of rule 4.1, and `role_code_for("esef", role_key="chief_executive")` maps to the catalog code.

- [x] **Step 5: Run**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_extractors_sql.py tests/test_se_company_person_extractors_clickhouse_local.py -q`
Expected: all PASS. `uv run --frozen --no-sync dg check defs`: green.

- [x] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/esef.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_extractors_sql.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_extractors_clickhouse_local.py
git commit -m "feat(dagster): ESEF raw person extractor"
```

---

### Task 4: The Wikidata extractor

**Files:**
- Create: `src/dagster_v3/defs/se_company/person/wikidata.py`
- Modify: `tests/test_se_company_person_extractors_sql.py`
- Modify: `tests/test_se_company_person_extractors_clickhouse_local.py`

**Interfaces:**
- Consumes: Task 2's shared builders; the linking shape of `basic_info/wikidata.py::wikidata_links_cte_sql`.
- Produces: `PERSON_SOURCE = "wikidata"`, `WIKIDATA_PERSON_EXTRACTOR_VERSION = "wikidata-person-v1"`, `WIKIDATA_COLUMN_SQL`, `wikidata_links_cte_sql(*, scoped=False)`, `wikidata_live_sql(*, scoped=False)`, `wikidata_current_sql()`, `wikidata_changed_scope_sql()`, `wikidata_select_sql()`, asset `se_company_person_suggestions_wikidata`.

- [x] **Step 1: Write the failing test** — add to `tests/test_se_company_person_extractors_sql.py`

Add `wikidata` to the imports and this entry to `EXTRACTORS`:

```python
    "wikidata": (
        wikidata.WIKIDATA_COLUMN_SQL,
        wikidata.wikidata_live_sql(scoped=True),
        wikidata.wikidata_select_sql(),
        wikidata.wikidata_changed_scope_sql(),
        wikidata.se_company_person_suggestions_wikidata,
    ),
```

and append:

```python
def test_wikidata_slot_is_the_link_record_id_and_the_link_is_orgnr_or_lei() -> None:
    columns = wikidata.WIKIDATA_COLUMN_SQL
    assert columns["slot"] == "link.source_record_id"
    assert columns["source_record_id"] == "person.source_record_uid"
    assert columns["full_name"] == "nullIf(trim(person.name), '')"
    assert columns["birth_year"] == "person.birth_year"
    assert columns["wikidata_id"] == "nullIf(link.person_wikidata_id, '')"
    assert columns["role_original"] == "nullIf(trim(toString(link.role_label)), '')"
    assert columns["role_key"] == "nullIf(trim(toString(link.role_property)), '')"
    assert columns["fiscal_year"] == NULL_SQL["fiscal_year"]
    assert columns["role_from"] == "link.start_date" and columns["role_to"] == "link.end_date"
    assert columns["document_ref"] == NULL_SQL["document_ref"]
    for key in ("'description'", "'image_url'", "'wikidata_url'", "'name_normalized'", "'is_current'"):
        assert key in columns["data"], key

    live = wikidata.wikidata_live_sql()
    assert live.startswith("WITH universe AS (")
    assert "SELECT company_id FROM corpscout.se_company_basic_info FINAL" in live
    assert "identifiers.identifier_type = 'se_orgnr'" in live
    assert "identifiers.issuer_scheme = 'lei' AND identifiers.is_current = 1" in live
    assert "INNER JOIN corpscout.wikidata_company_people AS link FINAL" in live
    assert "INNER JOIN corpscout.wikidata_persons AS person FINAL" in live
    # Wikidata blank nodes (.well-known/genid/...) are not people and carry a URL as a name.
    assert "WHERE match(link.person_wikidata_id, '^Q[0-9]+$') AND trim(person.name) != ''" in live
    assert "%(company_ids)s" not in live
    assert wikidata.wikidata_live_sql(scoped=True).count("%(company_ids)s") == 1
    assert wikidata.WIKIDATA_PERSON_EXTRACTOR_VERSION == "wikidata-person-v1"
```

- [x] **Step 2: Run to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_extractors_sql.py -q`
Expected: FAIL with `ImportError: cannot import name 'wikidata' from 'dagster_v3.defs.se_company.person'`.

- [x] **Step 3: Write `wikidata.py`**

```python
"""Wikidata company-person statements -> raw person suggestions (spec 2026-09-09 sections
3.1 and 6).

The company link mirrors basic_info/wikidata.py: a Swedish company reaches a Wikidata entity
either directly (wikidata_company_identifiers.identifier_type 'se_orgnr', digits stripped)
or through a current LEI in company_identifier. Two differences from that module, both
deliberate:

* The universe is se_company_basic_info rather than the union of the two register tables --
  this entity's universe is the folded company, and reading it once means the page select
  binds %(company_ids)s exactly once on this side.
* The LEI still comes from company_identifier and NOT from se_company_basic_info.lei, which
  is filled for only 392 companies against company_identifier's 115,440 current SE LEIs
  (measured 2026-09-09).

The slot is wikidata_company_people.source_record_id, which is literally
`Q<company>:P<property>:Q<person>` -- spec 3.1.1's "the QID plus the company link id". One
person under two properties for one company is two slots, and the fold unions their roles.

wikidata_persons also holds blank nodes whose name is a `.well-known/genid/...` URL; the
`^Q[0-9]+$` guard keeps them out. It excludes nothing real: all 504 Swedish rows on
2026-09-09 had a Q id and a wikidata_persons row.
"""

import dagster as dg

from dagster_v3.defs.se_company.person.suggestions import (
    NULL_SQL,
    define_person_suggestion_asset,
    live_select_sql,
    person_changed_scope_sql,
    person_select_sql,
)

PERSON_SOURCE = "wikidata"
WIKIDATA_PERSON_EXTRACTOR_VERSION = "wikidata-person-v1"

FROM_SQL = (
    "FROM links\n"
    "INNER JOIN corpscout.wikidata_company_people AS link FINAL\n"
    "    ON link.company_wikidata_id = links.wikidata_id\n"
    "INNER JOIN corpscout.wikidata_persons AS person FINAL\n"
    "    ON person.person_wikidata_id = link.person_wikidata_id"
)
WHERE_SQL = "WHERE match(link.person_wikidata_id, '^Q[0-9]+$') AND trim(person.name) != ''"

WIKIDATA_COLUMN_SQL: dict[str, str] = {
    "company_id": "links.company_id",
    "source": f"'{PERSON_SOURCE}'",
    "slot": "link.source_record_id",
    "source_record_id": "person.source_record_uid",
    # Wikidata delivers one label; the normalizer splits it.
    "full_name": "nullIf(trim(person.name), '')",
    "first_name": NULL_SQL["first_name"],
    "last_name": NULL_SQL["last_name"],
    "birth_year": "person.birth_year",
    "wikidata_id": "nullIf(link.person_wikidata_id, '')",
    "role_original": "nullIf(trim(toString(link.role_label)), '')",
    # The property id is the map key in roles.py (P169, P112, ...).
    "role_key": "nullIf(trim(toString(link.role_property)), '')",
    # Wikidata carries a span, not a fiscal year (spec 4.3: the fold expands the span).
    "fiscal_year": NULL_SQL["fiscal_year"],
    "role_from": "link.start_date",
    "role_to": "link.end_date",
    "document_ref": NULL_SQL["document_ref"],
    # Spec 3.1.2's Wikidata extras, limited to what wikidata_persons actually carries: it
    # has no occupations, nationality or sitelinks columns.
    "data": (
        "toJSONString(map('description', ifNull(person.description, ''), "
        "'image_url', ifNull(person.image_url, ''), "
        "'wikidata_url', ifNull(person.wikidata_url, ''), "
        "'name_normalized', person.name_normalized, "
        "'is_current', toString(link.is_current)))"
    ),
}


def wikidata_links_cte_sql(*, scoped: bool = False) -> str:
    """CTEs `universe`, `company_leis`, `links` (company_id, wikidata_id).

    `scoped=True` narrows `universe` to `%(company_ids)s` up front, so a page never rebuilds
    the whole 3.5M-company link universe -- and binds the ids exactly once for this whole
    side of the select.
    """
    company_ids_filter = " WHERE company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH universe AS (\n"
        f"    SELECT company_id FROM corpscout.se_company_basic_info FINAL{company_ids_filter}\n"
        "),\n"
        "company_leis AS (\n"
        "    SELECT identifiers.company_id AS company_id, upperUTF8(identifiers.issuer_id) AS lei\n"
        "    FROM corpscout.company_identifier AS identifiers\n"
        "    INNER JOIN universe AS companies ON companies.company_id = identifiers.company_id\n"
        "    WHERE identifiers.country_code = 'SE' AND identifiers.issuer_scheme = 'lei' "
        "AND identifiers.is_current = 1\n"
        "    GROUP BY identifiers.company_id, lei\n"
        "),\n"
        "links AS (\n"
        "    SELECT company_id, wikidata_id FROM (\n"
        "        SELECT companies.company_id AS company_id, identifiers.wikidata_id AS wikidata_id\n"
        "        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL\n"
        "        INNER JOIN universe AS companies\n"
        "            ON companies.company_id = replaceRegexpAll(identifiers.identifier_value, '[^0-9]', '')\n"
        "        WHERE identifiers.identifier_type = 'se_orgnr'\n"
        "        UNION ALL\n"
        "        SELECT leis.company_id AS company_id, identifiers.wikidata_id AS wikidata_id\n"
        "        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL\n"
        "        INNER JOIN company_leis AS leis ON leis.lei = upperUTF8(identifiers.identifier_value)\n"
        "        WHERE identifiers.identifier_type = 'lei'\n"
        "    )\n"
        "    GROUP BY company_id, wikidata_id\n"
        ")\n"
    )


def wikidata_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=WIKIDATA_COLUMN_SQL,
        from_sql=FROM_SQL,
        where_sql=WHERE_SQL,
        with_sql=wikidata_links_cte_sql(scoped=scoped),
    )


def wikidata_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        f"{wikidata_links_cte_sql()}"
        "SELECT links.company_id AS company_id, max(link.resolved_at) AS observed_at\n"
        "FROM links\n"
        "INNER JOIN corpscout.wikidata_company_people AS link FINAL\n"
        "    ON link.company_wikidata_id = links.wikidata_id\n"
        "GROUP BY links.company_id"
    )


def wikidata_changed_scope_sql() -> str:
    return person_changed_scope_sql(source=PERSON_SOURCE, live_sql=wikidata_live_sql())


def wikidata_select_sql() -> str:
    return person_select_sql(source=PERSON_SOURCE, live_sql=wikidata_live_sql(scoped=True))


se_company_person_suggestions_wikidata = define_person_suggestion_asset(
    source=PERSON_SOURCE,
    extractor_version=WIKIDATA_PERSON_EXTRACTOR_VERSION,
    current_sql=wikidata_current_sql(),
    select_sql=wikidata_select_sql(),
    changed_scope_override=wikidata_changed_scope_sql(),
    deps=[
        dg.AssetKey("wikidata_company_people"),
        dg.AssetKey("wikidata_persons"),
        dg.AssetKey("wikidata_company_identifiers"),
        dg.AssetKey("company_identifier_clickhouse"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every Wikidata person statement about a linked Swedish company as a raw person "
        "suggestion in se_company_person_suggestion (slot = Q<company>:P<property>:Q<person>, "
        "with the label, birth year, QID, role span and description); a statement Wikidata no "
        "longer carries is tombstoned. execute=false previews."
    ),
)
```

- [x] **Step 4: Add the Wikidata rows and sections to the clickhouse-local proof**

`COMPANY_WD = "5560125220"` gets its own basic-info row, and:

```python
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_WD}')",
        "INSERT INTO corpscout.wikidata_company_identifiers (wikidata_id, identifier_type, "
        "wikidata_property_id, identifier_value, is_primary, source_system, source_run_id, "
        "source_record_id, source_payload_hash, retrieved_at, resolved_at) VALUES "
        f"('Q9', 'se_orgnr', 'P6460', '556012-5220', 1, 'wikidata', 'run-0', 'i1', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC'))",
        "INSERT INTO corpscout.wikidata_persons (person_wikidata_id, name, name_normalized, "
        "description, birth_year, image_url, wikidata_url, source_system, source_run_id, "
        "source_record_id, source_payload_hash, retrieved_at, resolved_at) VALUES "
        "('Q404522', 'Jens Fischer', 'jens fischer', 'Swedish cinematographer', 1946, NULL, "
        f"'http://www.wikidata.org/entity/Q404522', 'wikidata', 'run-0', 'p1', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC')), "
        "('_blank1', 'http://www.wikidata.org/.well-known/genid/abc', 'genid', NULL, NULL, NULL, "
        f"NULL, 'wikidata', 'run-0', 'p2', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC'))",
        "INSERT INTO corpscout.wikidata_company_people (company_wikidata_id, person_wikidata_id, "
        "role_property, role_label, start_date, end_date, is_current, source_system, source_run_id, "
        "source_record_id, source_payload_hash, retrieved_at, resolved_at) VALUES "
        "('Q9', 'Q404522', 'P169', 'chief executive officer', toDate('2021-07-16'), NULL, 1, "
        f"'wikidata', 'run-0', 'Q9:P169:Q404522', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC')), "
        "('Q9', '_blank1', 'P112', 'founder', NULL, NULL, 1, "
        f"'wikidata', 'run-0', 'Q9:P112:_blank1', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC'))",
```

`@@wd_scope_1` before the Wikidata insert (`[[COMPANY_WD]]`) and `@@wd_scope_2` after it (`[]`); `RAW_ROWS_SQL` picks the row up. `changed_rows` binds `[COMPANY_BV, COMPANY_WD]`.

```python
WIKIDATA_DATA = (
    '{"description":"Swedish cinematographer","image_url":"",'
    '"wikidata_url":"http:\\/\\/www.wikidata.org\\/entity\\/Q404522",'
    '"name_normalized":"jens fischer","is_current":"1"}'
)


def test_the_wikidata_row_carries_the_qid_birth_year_span_and_description(sections) -> None:
    assert sections["wd_scope_1"] == [[COMPANY_WD]]
    assert sections["wd_scope_2"] == []
    rows = [r for r in _rows(sections, "raw_rows_1") if r["source"] == "wikidata"]
    # The blank-node statement is not a person and never lands.
    assert len(rows) == 1
    [row] = rows
    assert row["company_id"] == COMPANY_WD
    assert row["slot"] == "Q9:P169:Q404522"
    assert row["full_name"] == "Jens Fischer"
    assert row["birth_year"] == "1946"
    assert row["wikidata_id"] == "Q404522"
    assert row["role_original"] == "chief executive officer"
    assert row["role_key"] == "P169"
    assert row["fiscal_year"] == "\\N"
    assert row["role_from"] == "2021-07-16" and row["role_to"] == "\\N"
    assert row["document_ref"] == "\\N"
    assert row["data"] == WIKIDATA_DATA
```

and `test_the_normalize_hand_off_gives_the_expected_parse_statuses` grows its expected list by `("ok", "Jens Fischer", "chief_executive_officer", ())` (`role_code_for("wikidata", role_key="P169")`).

- [x] **Step 5: Run**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_extractors_sql.py tests/test_se_company_person_extractors_clickhouse_local.py -q`
Expected: all PASS — including the shared `test_every_extractor_binds_company_ids_exactly_twice_in_its_page_select`, which now covers all three. `uv run --frozen --no-sync dg check defs`: green; `uv run --frozen --no-sync dg list defs | grep se_company_person_suggestions` lists all three.

- [x] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/wikidata.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_extractors_sql.py \
  corpscout/services/dagster_v3/tests/test_se_company_person_extractors_clickhouse_local.py
git commit -m "feat(dagster): Wikidata raw person extractor"
```

---

### Task 5: The extract job, the stopped weekly, and the docs

**Files:**
- Create: `src/dagster_v3/defs/se_company/person/jobs.py`
- Modify: `src/dagster_v3/defs/se_company/person/assets.py` (the normalize asset gains `deps`)
- Modify: `src/dagster_v3/defs/se_company/person/docs/person-design.md`
- Create: `tests/test_se_company_person_jobs.py`

**Interfaces:**
- Consumes: `assets.EXTRACTOR_ASSET_NAMES` from Task 2.
- Produces: `NORMALIZE_ASSET`, `WEEKLY_PAGE_SIZE = 10_000`, `WEEKLY_RUN_CONFIG`, `se_company_person_extract_job`, `se_company_person_weekly`.

- [x] **Step 1: Write the failing test** — `tests/test_se_company_person_jobs.py`

```python
"""The person extract job and its STOPPED weekly (spec 2026-09-09 section 6), plus the
normalize asset's dependence on the three extractors."""

import dagster as dg

from dagster_v3.defs.se_company.person import assets, jobs


def _repo():
    from dagster_v3.definitions import defs as load_defs

    return load_defs().get_repository_def()


def test_the_job_selects_the_three_extractors_and_the_normalize_asset() -> None:
    job = _repo().get_job("se_company_person_extract_job")
    selected = {key.path[-1] for key in job.asset_layer.executable_asset_keys}
    assert selected == {*assets.EXTRACTOR_ASSET_NAMES, "se_company_person_normalize"}


def test_the_weekly_is_stopped_on_a_minute_hour_no_other_schedule_uses() -> None:
    repo = _repo()
    schedule = repo.get_schedule_def("se_company_person_weekly")
    # Spec section 6 says Monday 07:15 UTC; 07:15 is taken by
    # france_sirene_register_schedule and tests/test_schedule_cron_contracts.py forbids a
    # shared (minute, hour), so this entity took the next free minute of the same hour.
    assert schedule.cron_schedule == "25 7 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job_name == "se_company_person_extract_job"
    taken = {
        (other.cron_schedule.split()[0], other.cron_schedule.split()[1])
        for other in repo.schedule_defs
        if other.name != "se_company_person_weekly" and isinstance(other.cron_schedule, str)
    }
    assert ("25", "7") not in taken


def test_the_weekly_runs_every_extractor_and_the_normalizer_with_the_page_size() -> None:
    ops = jobs.WEEKLY_RUN_CONFIG["ops"]
    for name in assets.EXTRACTOR_ASSET_NAMES:
        assert ops[name] == {"config": {"execute": True, "page_size": jobs.WEEKLY_PAGE_SIZE}}
    assert ops["se_company_person_normalize"] == {"config": {"changed_only": True}}
    # Every person page select binds %(company_ids)s twice and the helper runs it under
    # max_query_size 1 MiB; 10,000 twelve-digit ids render to about 130 KB per binding.
    assert jobs.WEEKLY_PAGE_SIZE == 10_000


def test_the_normalize_asset_runs_after_the_extractors() -> None:
    node = _repo().asset_graph.get(dg.AssetKey("se_company_person_normalize"))
    assert {k.path[-1] for k in node.parent_keys} == set(assets.EXTRACTOR_ASSET_NAMES)


def test_no_person_sensor_yet() -> None:
    assert not any("se_company_person" in sensor.name for sensor in _repo().sensor_defs)
```

- [x] **Step 2: Run to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_jobs.py -q`
Expected: FAIL (`ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.person.jobs'`).

- [x] **Step 3: Write `jobs.py` and the normalize deps**

```python
"""The person extract job and its STOPPED weekly (spec 2026-09-09 section 6).

The fold stays manual (slice 2), so the weekly stops at the normalized layer.
"""

import dagster as dg

from dagster_v3.defs.se_company.person.assets import EXTRACTOR_ASSET_NAMES

NORMALIZE_ASSET = "se_company_person_normalize"
# Half the address entity's page. Every person page select binds %(company_ids)s twice (the
# live CTE and the stored-slot read) and the helper runs it under ID_BOUND_QUERY_SETTINGS'
# max_query_size of 1 MiB; 10,000 twelve-digit ids render to about 130 KB per binding, so
# the rendered statement stays well inside it.
WEEKLY_PAGE_SIZE = 10_000

WEEKLY_RUN_CONFIG = {
    "ops": {
        **{
            name: {"config": {"execute": True, "page_size": WEEKLY_PAGE_SIZE}}
            for name in EXTRACTOR_ASSET_NAMES
        },
        NORMALIZE_ASSET: {"config": {"changed_only": True}},
    }
}

se_company_person_extract_job = dg.define_asset_job(
    "se_company_person_extract_job",
    selection=dg.AssetSelection.assets(*EXTRACTOR_ASSET_NAMES, NORMALIZE_ASSET),
)
# Spec section 6 asks for Monday 07:15 UTC; 07:15 belongs to
# france_sirene_register_schedule and tests/test_schedule_cron_contracts.py requires a
# unique (minute, hour), so this is the next free minute of the same hour.
se_company_person_weekly = dg.ScheduleDefinition(
    name="se_company_person_weekly",
    job=se_company_person_extract_job,
    cron_schedule="25 7 * * 1",
    run_config=WEEKLY_RUN_CONFIG,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

In `person/assets.py`, the `@dg.asset` decorator of `se_company_person_normalize` gains
`deps=[dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES]` (the constants are already defined above it from Task 2).

- [x] **Step 4: Update `docs/person-design.md`**

Two slice-0 sentences become wrong in this slice and must be corrected, not merely appended to:

1. In "The `data` contract", the sentence ending `-- no native-JSON insert path, no toJSONString.` The read/write path is still ordinary `String`, but the extractors **build** `data` with `toJSONString(map(...))`. Replace the tail with: `-- no native-JSON insert path. The extractors are the only place that BUILDS an object, and they do it with toJSONString(map(...)) over all-String values, never by concatenating source text, so the constraint holds by construction.`
2. In "The `role_key` column", `Slice 1's extractors fill role_key out of each raw row's data` is wrong: they fill it from the source's own column (`role_kind`, `role_category`, `role_property`). Replace with `Slice 1's extractors fill role_key straight from the source's own code column -- Bolagsverket's role_kind, ESEF's role_category, Wikidata's role_property -- and never out of data.`

Then add an "Extractors (slice 1)" section, under 30 lines:

| module | source | slot | notes |
| --- | --- | --- | --- |
| `bolagsverket.py` | `se_financial_report_signatories` | report `source_record_uid` + `signatory_uid` | split name, `role_kind` as `role_key`, fiscal year as the role year, `data` = signatory kind, statement key, person seq |
| `esef.py` | `se_esef_document_people` (a view — never `FINAL` after it) | `source_document_id` + `candidate_uid` | full name, `role_category` as `role_key`, document fiscal year, `data` = organization, status, confidence, evidence ids, model, prompt |
| `wikidata.py` | `wikidata_company_people` + `wikidata_persons`, linked by orgnr or LEI | `Q<company>:P<property>:Q<person>` | full name, birth year, QID, property id as `role_key`, the role span, `data` = description, image, url, normalized name, is_current |

plus prose for: the universe (`se_company_basic_info`, everything else dropped); the change rule (the per-company state hash, why not a timestamp, and that it converges); tombstones (per slot, `LEFT ANTI JOIN` against the same `live` CTE, every person column NULL and `data` `{}`); the one `WITH`-bound `now64()` that both `suggestion_id` and `suggested_at` come from; the fact that the suggestion table stores neither `source_run_id` nor `extractor_version`; `execute: false` for a preview; and the job plus the stopped weekly at `25 7 * * 1`.

- [x] **Step 5: Run**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_jobs.py tests/test_se_company_person_normalize.py tests/test_se_company_person_extractors_sql.py tests/test_schedule_cron_contracts.py -q`
Expected: the person suites PASS. `test_schedule_cron_contracts.py::test_every_schedule_fires_on_a_unique_minute_hour_pair` still fails on its **pre-existing** collisions — read the assertion message and confirm `se_company_person_weekly` is **not** among the reported names; if it is, pick another free minute of hour 7 (20, 35, 50 or 55) and update the test, `jobs.py` and the docs together. `uv run --frozen --no-sync dg check defs`: green.

- [x] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/jobs.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/assets.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/docs/person-design.md \
  corpscout/services/dagster_v3/tests/test_se_company_person_jobs.py
git commit -m "feat(dagster): se_company_person_extract_job and its stopped weekly"
```

---

### Task 6: Prod run and readout (controller)

Not a coding task: the controller merges, deploys and runs. Every query below is read-only.

1. [x] Whole-branch review; run the full person suite plus the neighbours the shared helper touches:
   `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_extractors_sql.py tests/test_se_company_person_extractors_clickhouse_local.py tests/test_se_company_person_jobs.py tests/test_se_company_person_normalize.py tests/test_se_company_person_normalize_clickhouse_local.py tests/test_se_company_person_normalize_se.py tests/test_se_company_person_tables.py tests/test_se_company_basic_info_extract.py tests/test_se_company_basic_info_extractors_sql.py tests/test_se_company_address_extractors_sql.py tests/test_se_company_address_jobs.py -q`
   and confirm the only failures anywhere are the ones listed in Global Constraints.
2. [x] Merge to main **through a worktree that checks main out** (memory `se-worktree-deploy-recipe`); hot-sync the dagster host from the deploy worktree at the merge commit; `dg list defs` on the host lists the three `se_company_person_suggestions_*` assets, `se_company_person_extract_job` and a STOPPED `se_company_person_weekly`.
3. [x] **Preview** each extractor (`execute: false`, `page_size: 10000`), one Dagster run each. Expected `companies` / `candidates`, measured on prod 2026-09-09:

   | asset | companies | candidates |
   | --- | --- | --- |
   | `se_company_person_suggestions_bolagsverket` | 577,901 | 5,559,317 |
   | `se_company_person_suggestions_esef` | 384 | 9,610 |
   | `se_company_person_suggestions_wikidata` | 245 | 504 |

   A materially different number means the source moved since; record it and carry on. The Bolagsverket scan itself took 12 s in isolation.
4. [x] **Execute** the three (`execute: true`, `page_size: 10000`), then `se_company_person_normalize` (`changed_only: true`, `page_size: 20000`) — or the job in one go with `WEEKLY_RUN_CONFIG`. Record `inserted` per source and the normalize `companies`/`pages`/`rows`/`ok`/`partial`/`no_person`.
5. [x] **Readouts** on ClickHouse (`ssh companycollect "docker exec -i clickhouse-clickhouse-1 clickhouse-client --query \"...\""`):

   ```sql
   -- raw rows, distinct companies and tombstones per source
   SELECT source, count() AS rows, uniqExact(company_id) AS companies,
          countIf(full_name IS NULL AND first_name IS NULL AND last_name IS NULL) AS tombstones
   FROM corpscout.se_company_person_suggestion FINAL
   GROUP BY source ORDER BY source;

   -- id mismatches: a suggestion company that is not in the universe. Must be 0.
   SELECT count() FROM (
       SELECT DISTINCT company_id FROM corpscout.se_company_person_suggestion FINAL
   ) AS s
   LEFT ANTI JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS u
       ON u.company_id = s.company_id;

   -- data object-check violations. Must be 0.
   SELECT countIf(JSONType(data) != 'Object') FROM corpscout.se_company_person_suggestion FINAL;

   -- suggestion_id agrees with its own stamp. Must be 0.
   SELECT count() FROM corpscout.se_company_person_suggestion FINAL
   WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\n', toString(source), '\n', slot, '\n', toString(suggested_at)))));

   -- parse_status per source
   SELECT source, parse_status, count()
   FROM corpscout.se_company_person_normalized FINAL
   GROUP BY source, parse_status ORDER BY source, parse_status;

   -- the top parse_notes per source
   SELECT source, note, count() AS n FROM (
       SELECT source, arrayJoin(parse_notes) AS note
       FROM corpscout.se_company_person_normalized FINAL
   ) GROUP BY source, note ORDER BY n DESC LIMIT 20;

   -- role coverage: how much of each source normalized to a catalog code
   SELECT source, countIf(role_code IS NULL) AS roleless, uniqExact(role_code) AS codes, count()
   FROM corpscout.se_company_person_normalized FINAL GROUP BY source ORDER BY source;
   ```

   Expect roughly 2.0M `roleless` Bolagsverket rows: `role_kind = 'unknown'` is `roles.py`'s roleless code and had 2,018,104 rows on 2026-09-09. Expect a large Bolagsverket `no_person` count too — the August audit found role words and dates in the name field — and read the notes to see which rule fires.
6. [x] **Convergence check:** re-run the three extractors in preview (`execute: false`). Each must report `companies: 0`. This is the single most important readout of the slice: a non-zero number means the state hash the extractor writes and the one it reads back disagree, and the weekly would rewrite every company forever. If it happens, do **not** re-run in execute mode — diff one company's two states by hand with `person_state_sql` before anything else.
7. [x] **Spot-check twenty Bolagsverket rows by hand** against the source:

   ```sql
   SELECT p.company_id, p.slot, p.first_name, p.last_name, p.role_original, p.role_key,
          p.fiscal_year, p.document_ref, p.data
   FROM corpscout.se_company_person_suggestion FINAL AS p
   WHERE p.source = 'bolagsverket' AND p.first_name IS NOT NULL
   ORDER BY cityHash64(p.slot) LIMIT 20;
   ```

   For each, read the source row back and confirm every field:

   ```sql
   SELECT company_id, statement_key, signatory_kind, person_seq, first_name, last_name,
          role_original, role_kind, fiscal_year,
          concat(source_record_uid, ':', toString(signatory_uid)) AS slot
   FROM corpscout.se_financial_report_signatories
   WHERE concat(source_record_uid, ':', toString(signatory_uid)) IN (<the twenty slots>);
   ```

   Every disagreement is a bug in this slice: fix it, re-run the extractor, and re-check.
8. [x] Record every count and finding in spec section 9 item 1 (the shipped record, in the style of item 0), archive the ledger, and update memory (`se-basic-info-design` / the person entity note) with: the state-hash change scan and why a timestamp does not work here, the three slot shapes, the ESEF `candidate_uid` churn that the pending ESEF slice-2 plan will cause, and the `25 7` cron slot.

## Self-review

**1. Spec coverage.**
- 3.1 (the raw row, one per company/source/slot; tombstones with every person column NULL): Tasks 2 to 4 write the sixteen source columns and the per-slot tombstone branch; Task 2's clickhouse-local proof lands both shapes and the `valid_data` constraint passes on `'{}'`.
- 3.1.1 (slots): Bolagsverket = report `source_record_uid` + `signatory_uid` (Task 2, both real columns of the table — no invented uid was needed); ESEF = `source_document_id` + `candidate_uid` (Task 3, with the "index" deviation stated in the module docstring and in the report); Wikidata = `source_record_id`, literally `Q<company>:P<property>:Q<person>` (Task 4).
- 3.1.2 (`data`): one `toJSONString(map(...))` per source, all-String values, in Tasks 2 to 4; the object CHECK is proved in Task 2's local test (`data_check` and `data_check_2` are both 0) and read again on prod in Task 6.
- 3.1 `role_key`: filled from `role_kind` / `role_category` / `role_property` in Tasks 2 to 4 and pinned by `test_*_slot_is_*` in each.
- 4.3 (role years per source): Bolagsverket and ESEF fill `fiscal_year`, which `normalize.py` copies into `role_year`; Wikidata fills `role_from`/`role_to` and leaves `fiscal_year` NULL, so the fold expands the span. Pinned in the SQL tests and read back in Task 6's role-coverage query.
- 6 (extractors, the shared helper, the job, the stopped weekly): Tasks 1 to 5.
- 9 item 1 (a prod run with the `parse_status` distribution per source and a spot check of twenty Bolagsverket rows): Task 6, steps 5 and 7.
- 10 (names): assets `se_company_person_suggestions_{bolagsverket,esef,wikidata}`, job `se_company_person_extract_job`, schedule `se_company_person_weekly`, modules `suggestions`, `bolagsverket`, `esef`, `wikidata`, `jobs` — all pinned by tests in Tasks 2 to 5. `ratsit.py` is deliberately not written (spec section 6: reserved until the data exists).
- Deviations, both stated in the plan and in the report: the weekly is `25 7 * * 1` rather than 07:15 (the slot is taken and the cron contract forbids sharing), and the change scan is a state hash rather than the helper's `observed_at` watermark (the column does not exist and the source is rebuilt whole).

**2. Placeholder scan.** Every code step carries the real module or test source; the two "modify an existing test file" steps (Tasks 3 and 4) name the exact entry to add, the exact assertions and the exact fixture rows rather than saying "similar to Task 2". Task 5's docs step quotes the two sentences to replace verbatim. Task 6 is a controller checklist with every query written out and the expected number beside it. No "TBD", no "add error handling", no "write tests for the above".

**3. Type consistency.**
- `PERSON_SELECT_COLUMNS` (16) + `suggestion_id` + `suggested_at` = `tables.SUGGESTION_COLUMNS` (18), asserted in Task 2.
- `PERSON_STATE_COLUMNS` is derived from `PERSON_SELECT_COLUMNS`, so the state hash cannot drift from the projection.
- `NULL_SQL` is one dict used by both the tombstone branch and every source's missing columns, so the two sides of every `UNION ALL` carry the identical `CAST(NULL AS Nullable(...))` text and therefore the same type.
- `live_select_sql` raises on a missing or extra column name, so a source that forgets one fails at import rather than at the insert.
- Every extractor exposes the same four functions with the same names and shapes (`<source>_live_sql(*, scoped=False)`, `<source>_current_sql()`, `<source>_changed_scope_sql()`, `<source>_select_sql()`) plus `PERSON_SOURCE` and `<SOURCE>_PERSON_EXTRACTOR_VERSION`; the shared tests in `EXTRACTORS` iterate over exactly that shape.
- `EXTRACTOR_ASSET_NAMES` is defined once in `assets.py` and consumed by `jobs.py`, the normalize asset's `deps` and both test files.
- `normalize.py::RAW_ROW_COLUMNS` is `(company_id, source, slot, suggestion_id, full_name, first_name, last_name, birth_year, wikidata_id, role_original, role_key, fiscal_year, role_from, role_to, data)` — every one of those eleven person columns is written by all three extractors (NULL where the source has nothing), and `document_ref` and `source_record_id`, which the normalizer does not read, are written for the backoffice's evidence link.
- `define_person_suggestion_asset(**kwargs)` forwards the same keyword names `define_suggestion_asset` takes, including Task 1's new `changed_scope_override`.
