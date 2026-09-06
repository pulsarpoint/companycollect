# SE company address entity, slice 1: extractors — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill `se_company_address_suggestion` from the three register sources (SCB, Bolagsverket, Ratsit) through the basic-info extract helper, wire the extract job and its stopped weekly schedule, run it on prod, and read the normalizer's `parse_status` distribution.

**Architecture:** The basic-info extract helper (`basic_info/extract.py`) is generalized with a `SuggestionTarget` value that names the table, the columns, the trailing INSERT expressions, the asset prefix, the group and the scratch prefix; basic info keeps its byte-identical SQL through a default target, and the address entity supplies its own. Each address extractor is one module with a `current_sql()` (company id and `observed_at` per source row, tombstones included) and a `select_sql()` producing the thirteen raw columns; the helper does the change scan, the paging, the preview and the insert, and computes `suggestion_id` and `suggested_at` in the INSERT itself. A job selects the three extractors and the normalize asset; a weekly schedule is registered stopped.

**Tech Stack:** Dagster 1.13.9 (`uv run --frozen --no-sync`), Python 3.14, pytest, ClickHouse 26.5 (`clickhouse-local` through docker for the integration test).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md` (sections 3.1, 7, 9 slice 1, 10).

## Global Constraints

- All work in `corpscout/services/dagster_v3` of the worktree; tests run as `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/<file> -q`; `uv run --frozen --no-sync dg check defs` must pass before every commit that touches `src/`.
- The basic-info extractors' SQL texts and asset names must not change: every existing test in `tests/test_se_company_basic_info_extract.py`, `tests/test_se_company_basic_info_extractors_sql.py`, `tests/test_se_company_basic_info_jobs.py` and `tests/test_se_company_basic_info_assets.py` stays untouched and green.
- Address extractor assets are `se_company_address_suggestions_scb`, `se_company_address_suggestions_bolagsverket`, `se_company_address_suggestions_ratsit`, group `se_company_address`, extractor versions `scb-address-v1`, `bolagsverket-address-v1`, `ratsit-address-v1`.
- Raw rows (spec 3.1): SCB writes kind `visiting_or_postal`, slot `''`, columns `care_of`, `street_address`, `postal_code`, `post_town` as delivered (trimmed, empty to NULL); Bolagsverket writes kind `postal`, slot `''`, the packed `postal_address` into `raw_address`; Ratsit writes kind `postal`, slot `company`, `address_street` to `street_address`, `address_postal_code` to `postal_code`, `address_locality` to `post_town`, `address_county` to `county`; every other address column NULL. A register row with `has_company = 0` (a tombstone) writes a row with every address column NULL, so the fold withdraws the address. `country_code` is NULL for all three (Sweden by default; the normalizer decides `foreign` from the text).
- In the INSERT, a `WITH (SELECT now64(3, 'UTC')) AS stamp` scalar subquery binds one instant for the whole statement (two bare `now64()` calls in one statement are not guaranteed the same millisecond -- measured about 0.5% of executions differ, desynchronizing a whole page), and `suggested_at = stamp`, `suggestion_id = lower(hex(SHA256(concat(company_id, '\n', toString(source), '\n', slot, '\n', toString(stamp)))))`, so the id and the stamp always agree, and `toString(DateTime64(3, 'UTC'))` prints `YYYY-MM-DD HH:MM:SS.mmm`, the format `normalize.py`'s `clickhouse_stamp` uses for `normalized_id`. `decided_by`, `note`, `replaces_key` are NULL; `source_run_id` and `extractor_version` are the run's.
- The change scan is the helper's: a company is visited when the source has never suggested it or its source row's `observed_at` is newer than the current suggestion row's for that source (one slot per source in this slice, so the (company, source) scope is exact).
- Job `se_company_address_extract_job` selects the three extractors and `se_company_address_normalize`; schedule `se_company_address_v2_weekly` (interim name: the old model's `address_legacy.py` still registers `se_company_address_weekly` until the cutover, which renames this one), cron `5 7 * * 1`, `DefaultScheduleStatus.STOPPED`, run config `execute: true`, `page_size: 20000` per extractor and `changed_only: true` for the normalize asset.
- No `from __future__ import annotations` in any module that defines a `@dg.asset`.
- Commit by explicit path after every task; never `git add -A`. Trailers, contiguous at the end of every commit message: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` then `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`.

---

## File structure

- Modify: `src/dagster_v3/defs/se_company/basic_info/extract.py` — `SuggestionTarget`, `BASIC_INFO_TARGET`, `target=` parameters.
- Create: `src/dagster_v3/defs/se_company/address/suggestions.py` — `ADDRESS_SELECT_COLUMNS`, `ADDRESS_TARGET`, `define_address_suggestion_asset`.
- Create: `src/dagster_v3/defs/se_company/address/scb.py`, `bolagsverket.py`, `ratsit.py` — one extractor each.
- Create: `src/dagster_v3/defs/se_company/address/jobs.py` — the job and the stopped weekly.
- Modify: `src/dagster_v3/defs/se_company/address/assets.py` — `EXTRACTOR_SOURCES`, `EXTRACTOR_ASSET_NAMES`, `deps` on the normalize asset.
- Modify: `src/dagster_v3/defs/se_company/address/docs/address-design.md` — the extractor section.
- Tests: `tests/test_se_company_basic_info_extract.py` (append target tests only), `tests/test_se_company_address_extractors_sql.py`, `tests/test_se_company_address_jobs.py`, `tests/test_se_company_address_extractors_clickhouse_local.py`.

---

### Task 1: `SuggestionTarget` in the shared extract helper

**Files:**
- Modify: `src/dagster_v3/defs/se_company/basic_info/extract.py`
- Test: `tests/test_se_company_basic_info_extract.py` (append; existing tests untouched)

**Interfaces:**
- Produces: `SuggestionTarget(database, table, insert_columns, select_columns, trailing_select_sql, asset_prefix, group_name, scratch_prefix)` frozen dataclass with `qualified_table` property; `BASIC_INFO_TARGET`; `changed_scope_sql(*, current_sql, target=BASIC_INFO_TARGET)`, `insert_page_sql(*, select_sql, target=BASIC_INFO_TARGET)`, `run_extractor(..., target=BASIC_INFO_TARGET)`, `define_suggestion_asset(..., target=BASIC_INFO_TARGET)`; `since_scope_sql` and `count_page_sql` unchanged.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_se_company_basic_info_extract.py`)

```python
from dagster_v3.defs.se_company.basic_info.extract import BASIC_INFO_TARGET, SuggestionTarget


def test_the_default_target_is_basic_info_and_its_texts_are_unchanged() -> None:
    assert BASIC_INFO_TARGET.qualified_table == tables.QUALIFIED_SUGGESTION_TABLE
    assert BASIC_INFO_TARGET.insert_columns == tables.SUGGESTION_INSERT_COLUMNS
    assert BASIC_INFO_TARGET.select_columns == SUGGESTION_SELECT_COLUMNS
    assert BASIC_INFO_TARGET.asset_prefix == "se_basic_info_suggestions_"
    assert BASIC_INFO_TARGET.scratch_prefix == SCRATCH_SCOPE_PREFIX
    # The explicit default renders exactly what the implicit default rendered before targets existed.
    assert insert_page_sql(select_sql="SELECT 1") == insert_page_sql(select_sql="SELECT 1", target=BASIC_INFO_TARGET)
    assert insert_page_sql(select_sql="SELECT 1").endswith(
        "now64(3, 'UTC') AS suggested_at, %(source_run_id)s AS source_run_id, "
        "%(extractor_version)s AS extractor_version\nFROM (SELECT 1) AS candidate"
    )
    assert changed_scope_sql(current_sql="SELECT 1") == changed_scope_sql(current_sql="SELECT 1", target=BASIC_INFO_TARGET)


def test_another_target_renames_the_table_the_columns_the_asset_and_the_scratch_prefix() -> None:
    other = SuggestionTarget(
        database="corpscout", table="other_suggestion", insert_columns=("company_id", "source", "x", "stamp"),
        select_columns=("company_id", "source", "x"), trailing_select_sql="now64(3, 'UTC') AS stamp",
        asset_prefix="other_suggestions_", group_name="other", scratch_prefix="corpscout._tmp_other_scope_",
    )
    assert other.qualified_table == "corpscout.other_suggestion"
    assert insert_page_sql(select_sql="SELECT 1", target=other) == (
        "INSERT INTO corpscout.other_suggestion (company_id, source, x, stamp)\n"
        "SELECT candidate.company_id, candidate.source, candidate.x, now64(3, 'UTC') AS stamp\n"
        "FROM (SELECT 1) AS candidate"
    )
    assert "FROM corpscout.other_suggestion WHERE source = %(source)s" in changed_scope_sql(current_sql="SELECT 1", target=other)
    asset = define_suggestion_asset(
        source="scb", extractor_version="v", current_sql="SELECT 1", select_sql="SELECT 1", description="d", target=other,
    )
    assert asset.key == dg.AssetKey("other_suggestions_scb")
    assert asset.group_names_by_key[asset.key] == "other"
    client = FakeClient(candidates=0, scope_pages=[["5560125220"]])
    run_extractor(
        client, source="scb", extractor_version="v", current_sql="SELECT 1", select_sql="SELECT 1",
        select_params=None, source_run_id="r", config=ExtractConfig(), target=other,
    )
    created = next(s for s, _, _ in client.statements if s.startswith("CREATE TABLE"))
    assert created.split()[2].startswith("corpscout._tmp_other_scope_")
```

Use the file's existing `FakeClient` (it answers scratch page reads by prefix `SCRATCH_SCOPE_PREFIX`; extend its page-read match to `sql.startswith("SELECT company_id FROM corpscout._tmp_")` so the other prefix is served too, keeping every existing assertion intact) and its existing imports (`dg`, `tables`, `ExtractConfig`, `run_extractor`, `define_suggestion_asset`, `changed_scope_sql`, `insert_page_sql`, `SUGGESTION_SELECT_COLUMNS`, `SCRATCH_SCOPE_PREFIX`).

- [ ] **Step 2: Run to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_basic_info_extract.py -q`
Expected: the two new tests FAIL with `ImportError: cannot import name 'BASIC_INFO_TARGET'`; the rest pass.

- [ ] **Step 3: Generalize the helper**

In `extract.py`, after `SCAN_QUERY_SETTINGS` and before `ExtractConfig`:

```python
@dataclass(frozen=True, slots=True)
class SuggestionTarget:
    """Where an extractor writes and how its assets are named. Basic info is the default;
    the address entity supplies its own (spec 2026-09-06 section 7)."""

    database: str
    table: str
    insert_columns: tuple[str, ...]
    select_columns: tuple[str, ...]
    # The expressions the INSERT ... SELECT appends after the candidate columns, in the order
    # insert_columns continues after select_columns; may bind %(source_run_id)s and
    # %(extractor_version)s.
    trailing_select_sql: str
    asset_prefix: str
    group_name: str
    scratch_prefix: str

    @property
    def qualified_table(self) -> str:
        return f"{self.database}.{self.table}"


BASIC_INFO_TARGET = SuggestionTarget(
    database=tables.DATABASE,
    table=tables.SUGGESTION_TABLE,
    insert_columns=tables.SUGGESTION_INSERT_COLUMNS,
    select_columns=SUGGESTION_SELECT_COLUMNS,
    trailing_select_sql=(
        "CAST(NULL AS Nullable(String)) AS decided_by, CAST(NULL AS Nullable(String)) AS note, "
        "now64(3, 'UTC') AS suggested_at, %(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version"
    ),
    asset_prefix="se_basic_info_suggestions_",
    group_name=GROUP_NAME,
    scratch_prefix=SCRATCH_SCOPE_PREFIX,
)
```

(`SCRATCH_SCOPE_PREFIX` is defined lower in the file today; move its definition above `SuggestionTarget`.) Then:

- `changed_scope_sql(*, current_sql: str, target: SuggestionTarget = BASIC_INFO_TARGET)`: replace both `{tables.QUALIFIED_SUGGESTION_TABLE}` with `{target.qualified_table}`.
- `insert_page_sql(*, select_sql: str, target: SuggestionTarget = BASIC_INFO_TARGET)`:

```python
    selected = ", ".join(f"candidate.{column}" for column in target.select_columns)
    return (
        f"INSERT INTO {target.qualified_table} ({', '.join(target.insert_columns)})\n"
        f"SELECT {selected}, {target.trailing_select_sql}\n"
        f"FROM ({select_sql}) AS candidate"
    )
```

- `_scan_pages(client, *, source, current_sql, config, select_params, target)`: build the scope with `changed_scope_sql(current_sql=current_sql, target=target)` and call `scope_pages(..., prefix=target.scratch_prefix)`.
- `run_extractor(..., target: SuggestionTarget = BASIC_INFO_TARGET)`: pass `target` to `_scan_pages` and to `insert_page_sql`.
- `define_suggestion_asset(..., target: SuggestionTarget = BASIC_INFO_TARGET)`: `name=f"{target.asset_prefix}{source}"`, `group_name=target.group_name`, `metadata={"table": target.qualified_table, "source": source}`, `assert_clickhouse_tables_exist(clickhouse, database=target.database, tables=(target.table,))`, `run_extractor(..., target=target)`, and `"table": target.qualified_table` in the result metadata.
- Add `"SuggestionTarget"`, `"BASIC_INFO_TARGET"` to `__all__`.

- [ ] **Step 4: Run the basic-info suites to verify nothing moved**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_basic_info_extract.py tests/test_se_company_basic_info_extractors_sql.py tests/test_se_company_basic_info_jobs.py tests/test_se_company_basic_info_assets.py tests/test_se_company_basic_info_llm.py -q`
Expected: all PASS. Run `uv run --frozen --no-sync dg check defs`: green.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/extract.py \
  corpscout/services/dagster_v3/tests/test_se_company_basic_info_extract.py
git commit -m "refactor(dagster): suggestion extract helper takes a target; basic info is the default"
```

---

### Task 2: The address target and the three extractors

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/suggestions.py`, `.../address/scb.py`, `.../address/bolagsverket.py`, `.../address/ratsit.py`
- Modify: `src/dagster_v3/defs/se_company/address/assets.py` (add `EXTRACTOR_SOURCES`, `EXTRACTOR_ASSET_NAMES`; no other change in this task)
- Test: `tests/test_se_company_address_extractors_sql.py`

**Interfaces:**
- Consumes: Task 1's `SuggestionTarget`, `define_suggestion_asset`; `basic_info/scb.py::SCB_RECORD_UID_SQL`; `basic_info/ratsit.py::ratsit_current_sql`, `RATSIT_SELECT_PARAMS`; `address/tables.py`; `address/normalize.py::SCRATCH_SCOPE_PREFIX`; `address/assets.py::GROUP_NAME`.
- Produces: `ADDRESS_SELECT_COLUMNS` (13), `ADDRESS_TRAILING_SELECT_SQL`, `ADDRESS_TARGET`, `define_address_suggestion_asset(**kwargs)`; per source: `scb_current_sql()`, `scb_select_sql()`, `SCB_ADDRESS_EXTRACTOR_VERSION`, asset `se_company_address_suggestions_scb` (likewise `bolagsverket_*`, `ratsit_*`); `assets.EXTRACTOR_SOURCES = ("scb", "bolagsverket", "ratsit")`, `assets.EXTRACTOR_ASSET_NAMES`.

- [ ] **Step 1: Write the failing test**

`tests/test_se_company_address_extractors_sql.py`:

```python
"""Text pins of the three address extractors (spec section 7): the thirteen raw columns in
order, FINAL reads, id binding, tombstones, and the INSERT that stamps suggestion_id."""

import dagster as dg

from dagster_v3.defs.se_company.address import assets, bolagsverket, ratsit, scb, tables
from dagster_v3.defs.se_company.address.normalize import SCRATCH_SCOPE_PREFIX
from dagster_v3.defs.se_company.address.suggestions import (
    ADDRESS_SELECT_COLUMNS,
    ADDRESS_TARGET,
    ADDRESS_TRAILING_SELECT_SQL,
)
from dagster_v3.defs.se_company.basic_info.extract import changed_scope_sql, insert_page_sql
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION


def _aliases(sql: str) -> list[str]:
    """Column names of the top-level projection: the token after the last ' AS ' of each
    top-level comma-separated expression, or the bare column when there is no alias.
    Depth-aware, so commas and FROM inside function calls and subqueries do not count.
    (tests/se_company_ddl.py::projection_aliases expects CTE-shaped SQL and raises here.)"""
    after_select = sql.split("SELECT", 1)[1]
    depth = 0
    end = len(after_select)
    for index, char in enumerate(after_select):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and after_select.startswith("FROM ", index) and after_select[index - 1] in " \n":
            end = index
            break
    expressions: list[str] = []
    current: list[str] = []
    depth = 0
    for char in after_select[:end]:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            expressions.append("".join(current))
            current = []
        else:
            current.append(char)
    expressions.append("".join(current))
    names = []
    for expression in expressions:
        text = expression.strip()
        names.append(text.rsplit(" AS ", 1)[1].strip() if " AS " in text else text.rsplit(".", 1)[-1].strip())
    return names

EXTRACTORS = {
    "scb": (scb.scb_current_sql(), scb.scb_select_sql(), scb.se_company_address_suggestions_scb),
    "bolagsverket": (bolagsverket.bolagsverket_current_sql(), bolagsverket.bolagsverket_select_sql(), bolagsverket.se_company_address_suggestions_bolagsverket),
    "ratsit": (ratsit.ratsit_current_sql(), ratsit.ratsit_select_sql(), ratsit.se_company_address_suggestions_ratsit),
}


def test_the_target_writes_the_raw_table_with_every_column_once() -> None:
    assert ADDRESS_SELECT_COLUMNS == ("company_id", "source", "slot", "source_record_uid", "observed_at", "kind", *tables.RAW_ADDRESS_COLUMNS)
    assert ADDRESS_TARGET.qualified_table == tables.QUALIFIED_SUGGESTION_TABLE
    assert ADDRESS_TARGET.insert_columns == (
        *ADDRESS_SELECT_COLUMNS, "suggestion_id", "decided_by", "note", "replaces_key", "suggested_at", "source_run_id", "extractor_version",
    )
    assert sorted(ADDRESS_TARGET.insert_columns) == sorted(tables.SUGGESTION_COLUMNS)
    assert ADDRESS_TARGET.asset_prefix == "se_company_address_suggestions_"
    assert ADDRESS_TARGET.group_name == assets.GROUP_NAME == "se_company_address"
    assert ADDRESS_TARGET.scratch_prefix == SCRATCH_SCOPE_PREFIX


def test_the_insert_stamps_suggestion_id_from_the_same_now64_as_suggested_at() -> None:
    sql = insert_page_sql(select_sql="SELECT 1", target=ADDRESS_TARGET)
    assert sql.startswith(f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} ({', '.join(ADDRESS_TARGET.insert_columns)})\n")
    assert (
        "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', candidate.slot, '\\n', "
        "toString(now64(3, 'UTC')))))) AS suggestion_id"
    ) in sql
    assert "CAST(NULL AS Nullable(FixedString(64))) AS replaces_key" in sql
    assert "now64(3, 'UTC') AS suggested_at, %(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version" in sql
    assert ADDRESS_TRAILING_SELECT_SQL in sql


def test_every_select_yields_the_thirteen_columns_in_order_and_binds_the_page() -> None:
    for source, (current_sql, select_sql, _) in EXTRACTORS.items():
        assert _aliases(select_sql) == list(ADDRESS_SELECT_COLUMNS), source
        assert "%(company_ids)s" in select_sql, source
        assert " FINAL" in select_sql, source
        assert f"'{source}' AS source" in select_sql, source
        assert _aliases(current_sql) == ["company_id", "observed_at"], source
        assert "%(company_ids)s" not in current_sql, source


def test_scb_maps_the_four_delivered_columns_and_tombstones() -> None:
    sql = scb.scb_select_sql()
    assert "'visiting_or_postal' AS kind" in sql and "'' AS slot" in sql
    assert "FROM corpscout.se_scb_companies FINAL" in sql
    assert "has_company = 1" not in scb.scb_current_sql()
    for column in ("care_of", "street_address", "postal_code", "post_town"):
        assert f"if(has_company = 1, nullIf(trim(ifNull({column}, '')), ''), CAST(NULL AS Nullable(String))) AS {column}" in sql, column
    assert "CAST(NULL AS Nullable(String)) AS raw_address" in sql
    assert "CAST(NULL AS Nullable(String)) AS county" in sql
    assert "CAST(NULL AS Nullable(String)) AS country_code" in sql
    assert scb.SCB_ADDRESS_EXTRACTOR_VERSION == "scb-address-v1"


def test_bolagsverket_keeps_the_packed_string_raw_and_tombstones() -> None:
    sql = bolagsverket.bolagsverket_select_sql()
    assert "'postal' AS kind" in sql and "'' AS slot" in sql
    assert "FROM corpscout.se_bolagsverket_companies FINAL" in sql
    assert "if(has_company = 1, nullIf(trim(ifNull(postal_address, '')), ''), CAST(NULL AS Nullable(String))) AS raw_address" in sql
    for column in ("care_of", "street_address", "postal_code", "post_town", "county", "country_code"):
        assert f"CAST(NULL AS Nullable(String)) AS {column}" in sql, column
    assert "text_translations" not in sql and "text_translations" not in bolagsverket.bolagsverket_current_sql()
    assert bolagsverket.BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION == "bolagsverket-address-v1"


def test_ratsit_takes_the_newest_report_into_the_company_slot() -> None:
    sql = ratsit.ratsit_select_sql()
    assert "'postal' AS kind" in sql and "'company' AS slot" in sql
    assert "nullIf(trim(ifNull(address_street, '')), '') AS street_address" in sql
    assert "nullIf(trim(ifNull(address_postal_code, '')), '') AS postal_code" in sql
    assert "nullIf(trim(ifNull(address_locality, '')), '') AS post_town" in sql
    assert "nullIf(trim(ifNull(address_county, '')), '') AS county" in sql
    assert "WHERE normalizer_version = %(normalizer_version)s AND company_id IN %(company_ids)s" in sql
    assert sql.endswith("ORDER BY normalized_at DESC, result_sha256 DESC\nLIMIT 1 BY company_id")
    assert ratsit.RATSIT_ADDRESS_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    assert ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION == "ratsit-address-v1"


def test_assets_are_named_grouped_and_declared() -> None:
    assert assets.EXTRACTOR_SOURCES == ("scb", "bolagsverket", "ratsit")
    assert assets.EXTRACTOR_ASSET_NAMES == tuple(f"se_company_address_suggestions_{s}" for s in assets.EXTRACTOR_SOURCES)
    for source, (_, _, asset) in EXTRACTORS.items():
        assert asset.key == dg.AssetKey(f"se_company_address_suggestions_{source}")
        assert asset.group_names_by_key[asset.key] == "se_company_address"
    assert "FROM corpscout.se_company_address_suggestion WHERE source = %(source)s" in changed_scope_sql(
        current_sql=scb.scb_current_sql(), target=ADDRESS_TARGET,
    )
```

- [ ] **Step 2: Run to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_extractors_sql.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.address.suggestions'`.

- [ ] **Step 3: Write the modules**

`address/assets.py`, add after `NORMALIZE_POOL`:

```python
EXTRACTOR_SOURCES: tuple[str, ...] = ("scb", "bolagsverket", "ratsit")
EXTRACTOR_ASSET_NAMES: tuple[str, ...] = tuple(f"se_company_address_suggestions_{source}" for source in EXTRACTOR_SOURCES)
```

`address/suggestions.py`:

```python
"""The address entity's target for the shared suggestion extract helper (spec section 7):
the raw table, its thirteen source-provided columns, and the INSERT expressions that stamp
suggestion_id and suggested_at from one now64()."""

from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.assets import GROUP_NAME
from dagster_v3.defs.se_company.address.normalize import SCRATCH_SCOPE_PREFIX
from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget, define_suggestion_asset

ADDRESS_SELECT_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "source_record_uid", "observed_at", "kind", *tables.RAW_ADDRESS_COLUMNS,
)

# now64() is evaluated once per query, so the id hashes the very stamp the row carries; the
# stamp prints as YYYY-MM-DD HH:MM:SS.mmm, the format normalize.py hashes for normalized_id.
ADDRESS_TRAILING_SELECT_SQL = (
    "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', candidate.slot, '\\n', "
    "toString(now64(3, 'UTC')))))) AS suggestion_id, "
    "CAST(NULL AS Nullable(String)) AS decided_by, CAST(NULL AS Nullable(String)) AS note, "
    "CAST(NULL AS Nullable(FixedString(64))) AS replaces_key, "
    "now64(3, 'UTC') AS suggested_at, %(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version"
)

ADDRESS_TARGET = SuggestionTarget(
    database=tables.DATABASE,
    table=tables.SUGGESTION_TABLE,
    insert_columns=(
        *ADDRESS_SELECT_COLUMNS,
        "suggestion_id", "decided_by", "note", "replaces_key", "suggested_at", "source_run_id", "extractor_version",
    ),
    select_columns=ADDRESS_SELECT_COLUMNS,
    trailing_select_sql=ADDRESS_TRAILING_SELECT_SQL,
    asset_prefix="se_company_address_suggestions_",
    group_name=GROUP_NAME,
    scratch_prefix=SCRATCH_SCOPE_PREFIX,
)


def define_address_suggestion_asset(**kwargs: Any) -> dg.AssetsDefinition:
    return define_suggestion_asset(target=ADDRESS_TARGET, **kwargs)
```

`address/scb.py`:

```python
"""SCB register record -> raw address suggestion (spec section 7): the four delivered
columns as delivered, kind visiting_or_postal, a tombstone row when SCB stopped delivering."""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import define_address_suggestion_asset
from dagster_v3.defs.se_company.basic_info.scb import SCB_RECORD_UID_SQL

SCB_ADDRESS_EXTRACTOR_VERSION = "scb-address-v1"


def _delivered(column: str) -> str:
    return f"if(has_company = 1, nullIf(trim(ifNull({column}, '')), ''), CAST(NULL AS Nullable(String))) AS {column}"


def scb_current_sql() -> str:
    # Tombstones included: a company SCB stopped delivering must write a NULL row.
    return "SELECT company_id, observed_at FROM corpscout.se_scb_companies FINAL"


def scb_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'scb' AS source,\n"
        "    '' AS slot,\n"
        f"    {SCB_RECORD_UID_SQL} AS source_record_uid,\n"
        "    observed_at AS observed_at,\n"
        "    'visiting_or_postal' AS kind,\n"
        "    CAST(NULL AS Nullable(String)) AS raw_address,\n"
        f"    {_delivered('care_of')},\n"
        f"    {_delivered('street_address')},\n"
        f"    {_delivered('postal_code')},\n"
        f"    {_delivered('post_town')},\n"
        "    CAST(NULL AS Nullable(String)) AS county,\n"
        "    CAST(NULL AS Nullable(String)) AS country_code\n"
        "FROM corpscout.se_scb_companies FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


se_company_address_suggestions_scb = define_address_suggestion_asset(
    source="scb",
    extractor_version=SCB_ADDRESS_EXTRACTOR_VERSION,
    current_sql=scb_current_sql(),
    select_sql=scb_select_sql(),
    deps=[dg.AssetKey("sweden_company_scb_companies_clickhouse")],
    description=(
        "SCB's care-of, street, postcode and town as delivered into se_company_address_suggestion "
        "(kind visiting_or_postal, slot ''); a tombstoned company writes a NULL row."
    ),
)
```

`address/bolagsverket.py`:

```python
"""Bolagsverket register record -> raw address suggestion (spec section 7): the packed
postal_address string as delivered, kind postal; the normalizer does the parsing."""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import define_address_suggestion_asset

BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION = "bolagsverket-address-v1"

BOLAGSVERKET_ADDRESS_RECORD_UID_SQL = (
    "lower(hex(SHA256(concat('company-source-record-v1\\nstructured\\n', 'sweden_bolagsverket', "
    "'\\nregistry_company\\n', source_record_id, '\\n', lowerUTF8(source_payload_hash)))))"
)


def bolagsverket_current_sql() -> str:
    # The register row alone: addresses do not depend on text_translations, and tombstones
    # are included so a company Bolagsverket stopped delivering writes a NULL row.
    return "SELECT company_id, observed_at FROM corpscout.se_bolagsverket_companies FINAL"


def bolagsverket_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'bolagsverket' AS source,\n"
        "    '' AS slot,\n"
        f"    {BOLAGSVERKET_ADDRESS_RECORD_UID_SQL} AS source_record_uid,\n"
        "    observed_at AS observed_at,\n"
        "    'postal' AS kind,\n"
        "    if(has_company = 1, nullIf(trim(ifNull(postal_address, '')), ''), CAST(NULL AS Nullable(String))) AS raw_address,\n"
        "    CAST(NULL AS Nullable(String)) AS care_of,\n"
        "    CAST(NULL AS Nullable(String)) AS street_address,\n"
        "    CAST(NULL AS Nullable(String)) AS postal_code,\n"
        "    CAST(NULL AS Nullable(String)) AS post_town,\n"
        "    CAST(NULL AS Nullable(String)) AS county,\n"
        "    CAST(NULL AS Nullable(String)) AS country_code\n"
        "FROM corpscout.se_bolagsverket_companies FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


se_company_address_suggestions_bolagsverket = define_address_suggestion_asset(
    source="bolagsverket",
    extractor_version=BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=bolagsverket_select_sql(),
    deps=[dg.AssetKey("sweden_company_bolagsverket_companies_clickhouse")],
    description=(
        "Bolagsverket's packed postal address (street$care-of$town$postcode$country) as delivered into "
        "se_company_address_suggestion (kind postal, slot ''); a tombstoned company writes a NULL row."
    ),
)
```

`address/ratsit.py`:

```python
"""Ratsit's normalized company report -> raw address suggestion (spec section 7): the
company address, newest report per company, kind postal, slot company."""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import define_address_suggestion_asset
from dagster_v3.defs.se_company.basic_info.ratsit import ratsit_current_sql as _basic_info_ratsit_current_sql
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

RATSIT_ADDRESS_EXTRACTOR_VERSION = "ratsit-address-v1"
RATSIT_ADDRESS_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}


def ratsit_current_sql() -> str:
    return _basic_info_ratsit_current_sql()


def ratsit_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'ratsit' AS source,\n"
        "    'company' AS slot,\n"
        "    concat('ratsit:', toString(result_sha256)) AS source_record_uid,\n"
        "    toDateTime64(normalized_at, 3, 'UTC') AS observed_at,\n"
        "    'postal' AS kind,\n"
        "    CAST(NULL AS Nullable(String)) AS raw_address,\n"
        "    CAST(NULL AS Nullable(String)) AS care_of,\n"
        "    nullIf(trim(ifNull(address_street, '')), '') AS street_address,\n"
        "    nullIf(trim(ifNull(address_postal_code, '')), '') AS postal_code,\n"
        "    nullIf(trim(ifNull(address_locality, '')), '') AS post_town,\n"
        "    nullIf(trim(ifNull(address_county, '')), '') AS county,\n"
        "    CAST(NULL AS Nullable(String)) AS country_code\n"
        "FROM corpscout.se_ratsit_company FINAL\n"
        "WHERE normalizer_version = %(normalizer_version)s AND company_id IN %(company_ids)s\n"
        "ORDER BY normalized_at DESC, result_sha256 DESC\n"
        "LIMIT 1 BY company_id"
    )


se_company_address_suggestions_ratsit = define_address_suggestion_asset(
    source="ratsit",
    extractor_version=RATSIT_ADDRESS_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    select_params=RATSIT_ADDRESS_SELECT_PARAMS,
    deps=[dg.AssetKey("se_ratsit_normalized")],
    description=(
        "Ratsit's company address from the newest normalized report into se_company_address_suggestion "
        "(kind postal, slot company); the care-of Ratsit glues onto the street is split by the normalizer."
    ),
)
```

The three `deps` keys are the ones `basic_info/scb.py`, `basic_info/bolagsverket.py` and `basic_info/ratsit.py` declare (`sweden_company_scb_companies_clickhouse`, `sweden_company_bolagsverket_companies_clickhouse`, `se_ratsit_normalized`).

- [ ] **Step 4: Run to verify it passes**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_extractors_sql.py tests/test_se_company_basic_info_extractors_sql.py -q`
Expected: all PASS. `uv run --frozen --no-sync dg check defs`: green; `uv run --frozen --no-sync dg list defs | grep se_company_address_suggestions` lists the three assets.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/suggestions.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/scb.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/bolagsverket.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/ratsit.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/assets.py \
  corpscout/services/dagster_v3/tests/test_se_company_address_extractors_sql.py
git commit -m "feat(dagster): SCB, Bolagsverket and Ratsit raw address extractors"
```

---

### Task 3: The extract job, the stopped weekly, and the normalize asset's upstream

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/jobs.py`
- Modify: `src/dagster_v3/defs/se_company/address/assets.py` (the normalize asset gains `deps`)
- Modify: `src/dagster_v3/defs/se_company/address/docs/address-design.md` (extractor section)
- Test: `tests/test_se_company_address_jobs.py`

**Interfaces:**
- Consumes: `assets.EXTRACTOR_ASSET_NAMES`.
- Produces: `WEEKLY_PAGE_SIZE = 20_000`, `WEEKLY_RUN_CONFIG`, `se_company_address_extract_job`, `se_company_address_v2_weekly`.

- [ ] **Step 1: Write the failing test**

`tests/test_se_company_address_jobs.py`:

```python
"""The address extract job and its STOPPED weekly (spec section 7), plus the normalize
asset's dependence on the three extractors."""

import dagster as dg

from dagster_v3.defs.se_company.address import assets, jobs


def _repo():
    from dagster_v3.definitions import defs as load_defs

    return load_defs().get_repository_def()


def test_the_job_selects_the_three_extractors_and_the_normalize_asset() -> None:
    job = _repo().get_job("se_company_address_extract_job")
    selected = {key.path[-1] for key in job.asset_layer.executable_asset_keys}
    assert selected == {*assets.EXTRACTOR_ASSET_NAMES, "se_company_address_normalize"}


def test_the_weekly_is_registered_stopped_with_execute_and_the_page_size() -> None:
    schedule = _repo().get_schedule_def("se_company_address_v2_weekly")
    assert schedule.cron_schedule == "5 7 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job_name == "se_company_address_extract_job"
    ops = jobs.WEEKLY_RUN_CONFIG["ops"]
    for name in assets.EXTRACTOR_ASSET_NAMES:
        assert ops[name] == {"config": {"execute": True, "page_size": jobs.WEEKLY_PAGE_SIZE}}
    assert ops["se_company_address_normalize"] == {"config": {"changed_only": True}}
    assert jobs.WEEKLY_PAGE_SIZE == 20_000


def test_the_normalize_asset_runs_after_the_extractors() -> None:
    node = _repo().asset_graph.get(dg.AssetKey("se_company_address_normalize"))
    assert {k.path[-1] for k in node.parent_keys} == set(assets.EXTRACTOR_ASSET_NAMES)


def test_no_sensor_for_addresses_yet() -> None:
    assert not any("se_company_address_suggest" in s.name or "address_normalize" in s.name for s in _repo().sensor_defs)
```

These are the accessors `tests/test_se_company_basic_info_jobs.py` uses for the same questions.

- [ ] **Step 2: Run to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_jobs.py -q`
Expected: FAIL (`get_job` raises for the unknown job).

- [ ] **Step 3: Write `jobs.py` and the deps**

`address/jobs.py`:

```python
"""The address extract job and its STOPPED weekly (spec section 7). The fold stays manual."""

import dagster as dg

from dagster_v3.defs.se_company.address.assets import EXTRACTOR_ASSET_NAMES

NORMALIZE_ASSET = "se_company_address_normalize"
# The two register scans are the expensive ones: 20,000 ids per page renders inside the
# extract helper's ID_BOUND_QUERY_SETTINGS (one binding per statement).
WEEKLY_PAGE_SIZE = 20_000

WEEKLY_RUN_CONFIG = {
    "ops": {
        **{name: {"config": {"execute": True, "page_size": WEEKLY_PAGE_SIZE}} for name in EXTRACTOR_ASSET_NAMES},
        NORMALIZE_ASSET: {"config": {"changed_only": True}},
    }
}

se_company_address_extract_job = dg.define_asset_job(
    "se_company_address_extract_job",
    selection=dg.AssetSelection.assets(*EXTRACTOR_ASSET_NAMES, NORMALIZE_ASSET),
)
se_company_address_v2_weekly = dg.ScheduleDefinition(
    name="se_company_address_v2_weekly",
    job=se_company_address_extract_job,
    cron_schedule="5 7 * * 1",
    run_config=WEEKLY_RUN_CONFIG,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

In `address/assets.py`, the normalize asset's decorator gains `deps=[dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES]` (define `EXTRACTOR_ASSET_NAMES` above the asset).

`address/docs/address-design.md`: add an "Extractors (slice 1)" section (under 20 lines): the three assets, what each maps (SCB four columns and kind, Bolagsverket packed string, Ratsit newest report and slot `company`), tombstones as NULL rows, `suggestion_id` stamped from the INSERT's `now64()`, the change rule (helper's, per company and source), preview with `execute: false`, the job and the stopped weekly.

- [ ] **Step 4: Run to verify it passes**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_jobs.py tests/test_se_company_address_normalize.py tests/test_se_company_basic_info_jobs.py -q`
Expected: all PASS. `uv run --frozen --no-sync dg check defs`: green.

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/jobs.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/assets.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md \
  corpscout/services/dagster_v3/tests/test_se_company_address_jobs.py
git commit -m "feat(dagster): se_company_address_extract_job and its stopped weekly"
```

---

### Task 4: The extractors against clickhouse-local

**Files:**
- Test: `tests/test_se_company_address_extractors_clickhouse_local.py`

**Interfaces:**
- Consumes: Task 2's SQL functions and `ADDRESS_TARGET`; `basic_info/extract.py::changed_scope_sql`, `insert_page_sql`; `address/normalize.py::changed_rows_sql`, `normalized_row`, `RAW_ROW_COLUMNS`; the helpers of `tests/test_se_company_basic_info_extractors_clickhouse_local.py` (`_bind` via `tests/test_se_company_basic_info_clickhouse_local`, `_clickhouse_local_command`, its `_schema`/`_run`/`_scope`/`_insert`/`_labelled` shapes) and the fixture `tests/fixtures/se_basic_info_source_tables.sql` (it holds `se_ratsit_company`; `se_scb_companies` and `se_bolagsverket_companies` come from migrations `000373_corpscout_se_scb_companies` and `000374_corpscout_se_bolagsverket_companies`).

- [ ] **Step 1: Write the test** (`pytestmark = pytest.mark.integration`; the schema is migrations 000373, 000374, 000382, 000383 plus the fixture; parametrize the scan assertions over `join_use_nulls` 0 and 1 like the template)

1. Insert one register row per source: an SCB row for `5561552760` (`care_of NULL`, `street_address 'SICKLA INDUSTRIVÄG 19'`, `postal_code '13134'`, `post_town 'NACKA'`, `has_company 1`, `observed_at 2026-09-01`, `source_record_id 's1'`, `source_payload_hash 'h1'`, the remaining non-nullable columns `''` or `0` as the migration requires); a Bolagsverket row for the same company (`postal_address 'Box 5305$$STOCKHOLM$10247$SE-LAND'`, `has_company 1`, `observed_at 2026-09-02`); a Ratsit row for `5560125220` (`address_street 'c/o Anna Svensson Storgatan 5'`, `address_postal_code '11122'`, `address_locality 'Stockholm'`, `address_county 'Stockholms län'`, `normalized_at 2026-09-03`, `normalizer_version = RATSIT_NORMALIZER_VERSION`, `result_sha256` any 64-hex).
2. For each source run `changed_scope_sql(current_sql=..., target=ADDRESS_TARGET)` bound with `source` (and `normalizer_version` for Ratsit) and assert the expected company id comes back; then `insert_page_sql(select_sql=..., target=ADDRESS_TARGET)` bound with the page, `source_run_id 'run-1'`, `extractor_version` the module's constant.
3. Assert the raw table (`FINAL`) holds three rows with: `(5561552760, scb, '')` → `street_address 'SICKLA INDUSTRIVÄG 19'`, `post_town 'NACKA'`, `kind 'visiting_or_postal'`, `raw_address NULL`; `(5561552760, bolagsverket, '')` → `raw_address 'Box 5305$$STOCKHOLM$10247$SE-LAND'`, `street_address NULL`, `kind 'postal'`; `(5560125220, ratsit, company)` → `street_address 'c/o Anna Svensson Storgatan 5'`, `county 'Stockholms län'`; all with `extractor_version` as set and `decided_by`, `note`, `replaces_key` NULL.
4. Assert `suggestion_id` agrees with the stamp for every row: `SELECT count() FROM ... FINAL WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\n', toString(source), '\n', slot, '\n', toString(suggested_at)))))` is `0`.
5. Re-run the three scopes: each returns no id (converged).
6. Tombstone: insert a newer SCB row for `5561552760` with `has_company 0`, `observed_at 2026-09-05`, every address column NULL; the SCB scope selects the company again; after the insert the SCB raw row (`FINAL`) has `street_address`, `postal_code`, `post_town`, `care_of` all NULL and the newer `suggested_at`.
7. Run `changed_rows_sql()` from `normalize.py` for both companies (bind `company_ids` and `normalizer_version = NORMALIZER_VERSION`), feed each returned row through `normalized_row(row, STAMP)` in Python and assert the statuses: SCB `no_address` (tombstoned), Bolagsverket `ok` with `box '5305'`, Ratsit `ok` with `care_of 'anna svensson'`, `street_name 'storgatan'`, `house_number '5'`. (The select's column order equals `RAW_ROW_COLUMNS`; parse the TSV output of clickhouse-local into the tuple shape `normalized_row` expects, `\N` as `None`, the `suggested_at` string as-is.)

- [ ] **Step 2: Run it**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_extractors_clickhouse_local.py -q -m integration`
Expected: PASS via the docker fallback.

- [ ] **Step 3: Commit**

```bash
git add corpscout/services/dagster_v3/tests/test_se_company_address_extractors_clickhouse_local.py
git commit -m "test(dagster): address extractors and tombstones against clickhouse-local"
```

---

### Task 5: Prod run and readout (controller)

1. [ ] Whole-branch review; the owner merges; hot-sync the dagster host from the deploy worktree at the merge commit; `dg list defs` on the host shows the three extractors and the job.
2. [ ] Preview each extractor (`execute: false`, `page_size: 20000`): SCB and Bolagsverket report every company in their registers (about 1.82M and 2.86M), Ratsit about 84k; record the counts.
3. [ ] Execute the three (`execute: true`, `page_size: 20000`), then `se_company_address_normalize` (`changed_only: true`, `page_size: 20000`); record `inserted` per source and the normalize counts.
4. [ ] Readout on ClickHouse: `SELECT source, parse_status, count() FROM corpscout.se_company_address_normalized FINAL GROUP BY source, parse_status ORDER BY source, parse_status`; the top 20 `parse_notes` prefixes per source (`substring(parse_notes, 1, 40)`); and a spot-check of 20 random Bolagsverket rows joining `corpscout.se_company_addresses_current` (`source = 'bolagsverket'`) on `company_id`, comparing the old chain's `street_address`, `postal_code`, `post_town` with the new `normalized_address` components. Every disagreement is either a normalizer bug (fix, bump `NORMALIZER_VERSION`, re-run normalize) or a documented rule difference.
5. [ ] Record the counts and findings in the ledger and in spec section 9 (slice 1); archive the ledger.

## Self-review

- Spec coverage: section 3.1 raw rows and tombstones (Tasks 2, 4); section 7 extractors, the shared helper, preview, the job and stopped weekly (Tasks 1 to 3); section 9 slice 1 prod run and `parse_status` readout (Task 5); section 10 asset names (Task 2).
- Placeholders: Task 4 is a numbered procedure over Task 2's SQL and the template's helpers, with every literal value given; Task 5 is the controller's checklist with the exact queries.
- Type consistency: `SuggestionTarget` field names are used identically in Tasks 1 and 2; `ADDRESS_SELECT_COLUMNS` (13) plus the seven trailing columns equal `tables.SUGGESTION_COLUMNS` (20); `EXTRACTOR_ASSET_NAMES` is defined in `assets.py` and consumed by `jobs.py` and the tests; `define_address_suggestion_asset(**kwargs)` forwards the same keyword names `define_suggestion_asset` takes.
