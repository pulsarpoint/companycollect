# SE Basic Info — Slice 1: Tables, Precedence and the Fold — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the four basic-info entity tables, the precedence dictionary, the pure per-company fold, the batch layer that writes diff-only main and history rows, and the three Dagster assets (partitioned fold, targeted fold, precedence export) — everything slice 2's extractors write into and slice 3's backoffice reads from.

**Architecture:** A new package `dagster_v3.defs.se_company.basic_info` with one module per responsibility: `tables.py` (names and column tuples pinned against the migration DDL), `precedence.py` (the numeric precedence map, exported to ClickHouse), `fold.py` (a pure function from suggestion rows to one main row), `batch.py` (SQL texts plus the paged read → fold → diff → insert loop against a ClickHouse client), `assets.py` (64 hash-bucket partitions, the targeted fold, the precedence export). The suggestion table is empty until slice 2, so slice 1 ships with the fold proven on the fake client and on a real ClickHouse with hand-inserted suggestion rows.

**Tech Stack:** Python 3.14, Dagster 1.13.9 (`dg`), `dagster_clickhouse.ClickhouseResource` over clickhouse-driver (client-side `%(name)s` parameters), ClickHouse 26.5, golang-migrate ledger in `corpscout/clickhouse/migrations`, pytest, clickhouse-local/Docker harness.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md` — sections 3.2–3.5 (tables), 4 (precedence), 5 (the fold), 6 (Dagster), 9 (testing), 11 (names). The slice-0 handoff under section 10 names the source-table contract (`FINAL ... WHERE has_company = 1`, raw date twins) that slice 2 reads; slice 1 never reads the source tables.

## Global Constraints

- Migration files: first line `CREATE DATABASE IF NOT EXISTS corpscout;`, last line a statement, NO semicolon inside any comment, `ORDER BY` keys non-nullable, one table per migration, numbers 000376–000379 (slice 0 took 000373–000375; verify with `ls corpscout/clickhouse/migrations | tail -2` → `000375_corpscout_retire_se_company_registry.{down,up}.sql` before creating files). Every new migration name goes into `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py` right after the 000375 entry.
- Names (spec section 11): tables `se_company_basic_info_suggestion`, `se_company_basic_info`, `se_company_basic_info_history`, `se_company_basic_info_precedence`; assets `se_company_basic_info_fold`, `se_company_basic_info_fold_companies`, `se_company_basic_info_precedence_clickhouse`; module `dagster_v3.defs.se_company.basic_info`; sources `scb`, `bolagsverket`, `wikidata`, `esef`, `ratsit`, `llm`, `reviewer`; Dagster group `se_company_basic_info`.
- Suggestion semantics (spec 3.2): NULL in a value column = "no opinion", never "says empty"; one current row per company and source = `ReplacingMergeTree(suggested_at) ORDER BY (company_id, source)`, read with `FINAL`; `content_hash` is MATERIALIZED over the nine value columns, NULL-safe (NULL and `''` hash differently).
- Fold rules (spec 5): per field the highest precedence wins, ties to the newest `observed_at`, then the smaller `source_record_uid`; `description_language` follows the `description` winner; no row unless SCB or Bolagsverket supplies `legal_name`; `status` is `''` and every `_source` is `''` when a field has no winner; the batch writes only rows that differ from the current main row and one history row per changed company (`changed_fields` = every non-NULL field on the first publish); pages of 20,000 companies.
- Dagster (spec 6): 64 static partitions on `modulo(cityHash64(company_id), 64)`, `BackfillPolicy.multi_run(max_partitions_per_run=1)`, one pool `se_company_basic_info_fold` on both fold assets, `changed_only` default true on the partitioned fold, `company_ids` required on the targeted fold, no sensor, no schedule in this slice.
- No `from __future__ import annotations` in modules that define assets (Dagster context-type validation).
- Tests need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix` in front of any pytest that loads Definitions and of `dg check defs`; run everything with `uv run --frozen --no-sync`.
- Commit by explicit path only; Conventional Commits; every message ends with these two contiguous lines:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- Development-phase ledger policy (owner, 2026-09-03): a table that turns out unused is later removed from its migration file and dropped by hand; nothing in this slice drops anything.

---

## File structure

```
corpscout/clickhouse/migrations/
  000376_corpscout_se_company_basic_info_suggestion.{up,down}.sql
  000377_corpscout_se_company_basic_info.{up,down}.sql
  000378_corpscout_se_company_basic_info_history.{up,down}.sql
  000379_corpscout_se_company_basic_info_precedence.{up,down}.sql
corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/
  __init__.py        -- package docstring only
  tables.py          -- DATABASE, table names, qualified names, column tuples, FOLDED_FIELDS
  precedence.py      -- BASIC_INFO_PRECEDENCE, precedence_for(), precedence_rows()
  fold.py            -- Suggestion, BasicInfoRow, FOLD_VERSION, fold_basic_info()
  batch.py           -- SQL texts, FoldCounts, fold_companies(), fold_bucket()
  assets.py          -- partitions, configs, the three assets
corpscout/services/dagster_v3/tests/
  test_se_company_basic_info_tables.py            -- DDL pins (Task 1)
  test_se_company_basic_info_precedence.py         -- Task 2
  test_se_company_basic_info_fold.py               -- Task 3
  test_se_company_basic_info_batch.py              -- Task 4
  test_se_company_basic_info_clickhouse_local.py   -- Task 5 (Docker)
  test_se_company_basic_info_assets.py             -- Task 6
```

---

### Task 1: Migrations 000376–000379 and `tables.py`

**Files:**
- Create: `corpscout/clickhouse/migrations/000376_corpscout_se_company_basic_info_suggestion.up.sql` and `.down.sql`
- Create: `corpscout/clickhouse/migrations/000377_corpscout_se_company_basic_info.up.sql` and `.down.sql`
- Create: `corpscout/clickhouse/migrations/000378_corpscout_se_company_basic_info_history.up.sql` and `.down.sql`
- Create: `corpscout/clickhouse/migrations/000379_corpscout_se_company_basic_info_precedence.up.sql` and `.down.sql`
- Create: `src/dagster_v3/defs/se_company/basic_info/__init__.py`, `src/dagster_v3/defs/se_company/basic_info/tables.py`
- Modify: `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`, four entries after `"000375_corpscout_retire_se_company_registry",`)
- Test: `tests/test_se_company_basic_info_tables.py`

**Interfaces:**
- Produces: `tables.DATABASE = "corpscout"`, `SUGGESTION_TABLE`, `MAIN_TABLE`, `HISTORY_TABLE`, `PRECEDENCE_TABLE` (bare names), `QUALIFIED_SUGGESTION_TABLE` etc. (`corpscout.<name>`), `VALUE_COLUMNS` (nine), `FOLDED_FIELDS` (eight, no `description_language`), `SUGGESTION_INSERT_COLUMNS`, `MAIN_COLUMNS`, `HISTORY_COLUMNS`, `PRECEDENCE_COLUMNS`. Tasks 3–6 import these.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_basic_info_tables.py`:

```python
"""The four basic-info entity tables (spec 3.2-3.5), pinned against the migration DDL
through tests/se_company_ddl.py so tables.py and the deployed schema cannot drift."""

from dagster_v3.defs.se_company.basic_info import tables
from tests.se_company_ddl import declared_columns, table_block


def test_suggestion_table_is_one_current_row_per_company_and_source() -> None:
    block = table_block("se_company_basic_info_suggestion")
    assert declared_columns("se_company_basic_info_suggestion") == [
        *tables.SUGGESTION_INSERT_COLUMNS[:16],
        "content_hash",
        *tables.SUGGESTION_INSERT_COLUMNS[16:],
    ]
    assert "ENGINE = ReplacingMergeTree(suggested_at)" in block
    assert "ORDER BY (company_id, source)" in block
    assert "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in block
    # NULL means no opinion: every value column is Nullable, and the hash tells NULL
    # from '' because a NULL contributes '~' and a value contributes '=' plus the value.
    for column in tables.VALUE_COLUMNS:
        assert f"    {column} Nullable(" in block, column
    assert "content_hash FixedString(64) MATERIALIZED lower(hex(SHA256(" in block
    for column in tables.VALUE_COLUMNS:
        assert f"if({column} IS NULL, '~', concat('=', " in block, column
    assert "decided_by Nullable(String)" in block
    assert "note Nullable(String)" in block


def test_main_table_carries_a_source_beside_every_folded_value() -> None:
    block = table_block("se_company_basic_info")
    assert declared_columns("se_company_basic_info") == list(tables.MAIN_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(folded_at)" in block
    assert "ORDER BY company_id" in block
    for field in tables.FOLDED_FIELDS:
        assert f"    {field}_source LowCardinality(String)" in block, field
    # status is '' when unknown, like the old table -- never NULL.
    assert "    status LowCardinality(String)," in block
    assert "    legal_name String," in block
    assert "description_language Nullable(String)" in block
    assert "description_language_source" not in block
    assert "fold_version LowCardinality(String)" in block


def test_history_table_is_the_main_row_plus_changed_fields() -> None:
    block = table_block("se_company_basic_info_history")
    assert declared_columns("se_company_basic_info_history") == list(tables.HISTORY_COLUMNS)
    assert tables.HISTORY_COLUMNS == (*tables.MAIN_COLUMNS, "changed_fields")
    assert "changed_fields Array(String)" in block
    assert "ENGINE = MergeTree" in block
    assert "ORDER BY (company_id, folded_at)" in block


def test_precedence_table_is_exported_never_edited() -> None:
    block = table_block("se_company_basic_info_precedence")
    assert declared_columns("se_company_basic_info_precedence") == list(tables.PRECEDENCE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(exported_at)" in block
    assert "ORDER BY (field, source)" in block
    assert "precedence UInt32" in block


def test_column_tuples_agree_with_each_other() -> None:
    assert tables.VALUE_COLUMNS == (
        "legal_name", "legal_form_code", "status", "incorporation_date", "lei",
        "wikidata_id", "description", "description_language", "description_sv",
    )
    assert tables.FOLDED_FIELDS == tuple(c for c in tables.VALUE_COLUMNS if c != "description_language")
    assert tables.QUALIFIED_SUGGESTION_TABLE == "corpscout.se_company_basic_info_suggestion"
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_basic_info"
    assert tables.QUALIFIED_HISTORY_TABLE == "corpscout.se_company_basic_info_history"
    assert tables.QUALIFIED_PRECEDENCE_TABLE == "corpscout.se_company_basic_info_precedence"
```

Add to `tests/test_clickhouse_migrations.py`, in `EXPECTED_MIGRATIONS` right after `"000375_corpscout_retire_se_company_registry",`:

```python
    "000376_corpscout_se_company_basic_info_suggestion",
    "000377_corpscout_se_company_basic_info",
    "000378_corpscout_se_company_basic_info_history",
    "000379_corpscout_se_company_basic_info_precedence",
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_se_company_basic_info_tables.py tests/test_clickhouse_migrations.py -q -p no:warnings`
Expected: FAIL — `ModuleNotFoundError: dagster_v3.defs.se_company.basic_info` and the ledger test reporting four missing files.

- [ ] **Step 3: Write the migrations**

`000376_corpscout_se_company_basic_info_suggestion.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- One current row per company and source: what that source currently suggests for the
-- basic-info entity (2026-09-03 SE basic-info design, section 3.2). Sources are scb,
-- bolagsverket, wikidata, esef, ratsit, llm and reviewer. NULL in a value column means
-- the source has no opinion, never that the source says empty. An extractor inserts a
-- new version only when content_hash differs from the current row, so an unchanged
-- refresh writes nothing. decided_by and note are set on reviewer rows only.
-- content_hash covers the nine value columns and tells NULL ('~') from a value ('=' and
-- the value), joined by the unit separator, so a released field hashes differently from
-- an empty one.
CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_suggestion
(
    company_id String,
    source LowCardinality(String),
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    legal_name Nullable(String),
    legal_form_code Nullable(String),
    status Nullable(String),
    incorporation_date Nullable(Date32),
    lei Nullable(String),
    wikidata_id Nullable(String),
    description Nullable(String),
    description_language Nullable(String),
    description_sv Nullable(String),
    decided_by Nullable(String),
    note Nullable(String),
    suggested_at DateTime64(3, 'UTC'),
    content_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        if(legal_name IS NULL, '~', concat('=', legal_name)), '\x1F',
        if(legal_form_code IS NULL, '~', concat('=', legal_form_code)), '\x1F',
        if(status IS NULL, '~', concat('=', status)), '\x1F',
        if(incorporation_date IS NULL, '~', concat('=', toString(incorporation_date))), '\x1F',
        if(lei IS NULL, '~', concat('=', lei)), '\x1F',
        if(wikidata_id IS NULL, '~', concat('=', wikidata_id)), '\x1F',
        if(description IS NULL, '~', concat('=', description)), '\x1F',
        if(description_language IS NULL, '~', concat('=', description_language)), '\x1F',
        if(description_sv IS NULL, '~', concat('=', description_sv))
    )))),
    source_run_id String,
    extractor_version LowCardinality(String),

    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source);
```

`000376_...down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_company_basic_info_suggestion;
```

`000377_corpscout_se_company_basic_info.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per published company: the fold of every current suggestion row by the
-- per-field precedence of section 4 (2026-09-03 SE basic-info design, section 3.3).
-- Beside every folded value sits the source that supplied it. A _source column is ''
-- when the field has no value, status itself is '' then (not nullable, as on the old
-- table) and every other value column is NULL. description_language follows the row
-- that won description and has no source column of its own. A company gets a row only
-- when SCB or Bolagsverket supplies its legal name. Legal-form labels are not stored,
-- the serving view joins se_code_labels as today. folded_at is the version.
CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info
(
    company_id String,
    legal_name String,
    legal_name_source LowCardinality(String),
    legal_form_code Nullable(String),
    legal_form_code_source LowCardinality(String),
    status LowCardinality(String),
    status_source LowCardinality(String),
    incorporation_date Nullable(Date32),
    incorporation_date_source LowCardinality(String),
    lei Nullable(String),
    lei_source LowCardinality(String),
    wikidata_id Nullable(String),
    wikidata_id_source LowCardinality(String),
    description Nullable(String),
    description_source LowCardinality(String),
    description_language Nullable(String),
    description_sv Nullable(String),
    description_sv_source LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,

    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(folded_at)
ORDER BY company_id;
```

`000377_...down.sql`: `CREATE DATABASE IF NOT EXISTS corpscout;` blank line, `DROP TABLE IF EXISTS corpscout.se_company_basic_info;`

`000378_corpscout_se_company_basic_info_history.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Append-only: one row per change of a company's se_company_basic_info row, written by
-- the fold when the folded row differs from the current main row, including the first
-- publish (2026-09-03 SE basic-info design, section 3.4). The columns are the main
-- row's, plus changed_fields naming the fields whose value or source changed (every
-- non-NULL field on the first publish).
CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_history
(
    company_id String,
    legal_name String,
    legal_name_source LowCardinality(String),
    legal_form_code Nullable(String),
    legal_form_code_source LowCardinality(String),
    status LowCardinality(String),
    status_source LowCardinality(String),
    incorporation_date Nullable(Date32),
    incorporation_date_source LowCardinality(String),
    lei Nullable(String),
    lei_source LowCardinality(String),
    wikidata_id Nullable(String),
    wikidata_id_source LowCardinality(String),
    description Nullable(String),
    description_source LowCardinality(String),
    description_language Nullable(String),
    description_sv Nullable(String),
    description_sv_source LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    changed_fields Array(String)
)
ENGINE = MergeTree
ORDER BY (company_id, folded_at);
```

`000378_...down.sql`: the CREATE DATABASE line, blank line, `DROP TABLE IF EXISTS corpscout.se_company_basic_info_history;`

`000379_corpscout_se_company_basic_info_precedence.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- The per-field, per-source precedence of section 4 as exported from
-- dagster_v3.defs.se_company.basic_info.precedence by the
-- se_company_basic_info_precedence_clickhouse asset (2026-09-03 SE basic-info design,
-- section 3.5). Read by the backoffice for display and validation. Never edited here,
-- the Python dictionary is the only source and a re-export replaces every pair.
CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_precedence
(
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    exported_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(exported_at)
ORDER BY (field, source);
```

`000379_...down.sql`: the CREATE DATABASE line, blank line, `DROP TABLE IF EXISTS corpscout.se_company_basic_info_precedence;`

- [ ] **Step 4: Write `tables.py` and the package**

`src/dagster_v3/defs/se_company/basic_info/__init__.py`:

```python
"""The SE company basic-info entity: suggestion table, precedence, fold, main and history.

Spec: docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md. One package per
entity; addresses, industries and people follow the same shape in their own slices.
"""
```

`src/dagster_v3/defs/se_company/basic_info/tables.py`:

```python
"""Table names and column tuples of the basic-info entity, pinned against the DDL."""

DATABASE = "corpscout"

SUGGESTION_TABLE = "se_company_basic_info_suggestion"
MAIN_TABLE = "se_company_basic_info"
HISTORY_TABLE = "se_company_basic_info_history"
PRECEDENCE_TABLE = "se_company_basic_info_precedence"

QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"
QUALIFIED_MAIN_TABLE = f"{DATABASE}.{MAIN_TABLE}"
QUALIFIED_HISTORY_TABLE = f"{DATABASE}.{HISTORY_TABLE}"
QUALIFIED_PRECEDENCE_TABLE = f"{DATABASE}.{PRECEDENCE_TABLE}"

# The nine value columns a suggestion row carries, in DDL order. content_hash covers
# exactly these.
VALUE_COLUMNS: tuple[str, ...] = (
    "legal_name",
    "legal_form_code",
    "status",
    "incorporation_date",
    "lei",
    "wikidata_id",
    "description",
    "description_language",
    "description_sv",
)

# The fields the fold decides with a precedence map: every value column except
# description_language, which follows the description winner (spec 4 and 5).
FOLDED_FIELDS: tuple[str, ...] = tuple(c for c in VALUE_COLUMNS if c != "description_language")

# What an extractor (or the backoffice) inserts. content_hash is MATERIALIZED and never
# listed: ClickHouse computes it on insert.
SUGGESTION_INSERT_COLUMNS: tuple[str, ...] = (
    "company_id",
    "source",
    "source_record_uid",
    "observed_at",
    *VALUE_COLUMNS,
    "decided_by",
    "note",
    "suggested_at",
    "source_run_id",
    "extractor_version",
)

# The main row, in DDL order: each folded field followed by its _source, with
# description_language after description_source and without a source of its own.
MAIN_COLUMNS: tuple[str, ...] = (
    "company_id",
    "legal_name",
    "legal_name_source",
    "legal_form_code",
    "legal_form_code_source",
    "status",
    "status_source",
    "incorporation_date",
    "incorporation_date_source",
    "lei",
    "lei_source",
    "wikidata_id",
    "wikidata_id_source",
    "description",
    "description_source",
    "description_language",
    "description_sv",
    "description_sv_source",
    "folded_at",
    "fold_version",
    "source_run_id",
)

HISTORY_COLUMNS: tuple[str, ...] = (*MAIN_COLUMNS, "changed_fields")

PRECEDENCE_COLUMNS: tuple[str, ...] = ("field", "source", "precedence", "exported_at")
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_se_company_basic_info_tables.py tests/test_clickhouse_migrations.py -q -p no:warnings`
Expected: PASS. If `declared_columns` does not list `content_hash` (it is a MATERIALIZED column with an expression spanning lines), read `tests/se_company_ddl.py::declared_columns` and, if its regex needs the column on one line, adjust the first assertion of `test_suggestion_table_...` to compare against `[c for c in declared if c != "content_hash"]` and pin `content_hash` by text instead — report which you did.

Then: `uv run --frozen --no-sync ruff check src/dagster_v3/defs/se_company/basic_info tests/test_se_company_basic_info_tables.py`

- [ ] **Step 6: Commit**

```bash
git add corpscout/clickhouse/migrations/000376_corpscout_se_company_basic_info_suggestion.up.sql \
        corpscout/clickhouse/migrations/000376_corpscout_se_company_basic_info_suggestion.down.sql \
        corpscout/clickhouse/migrations/000377_corpscout_se_company_basic_info.up.sql \
        corpscout/clickhouse/migrations/000377_corpscout_se_company_basic_info.down.sql \
        corpscout/clickhouse/migrations/000378_corpscout_se_company_basic_info_history.up.sql \
        corpscout/clickhouse/migrations/000378_corpscout_se_company_basic_info_history.down.sql \
        corpscout/clickhouse/migrations/000379_corpscout_se_company_basic_info_precedence.up.sql \
        corpscout/clickhouse/migrations/000379_corpscout_se_company_basic_info_precedence.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/__init__.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/tables.py \
        corpscout/services/dagster_v3/tests/test_se_company_basic_info_tables.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(clickhouse): SE basic-info suggestion, main, history and precedence tables

Migrations 000376-000379 create the four entity tables of the 2026-09-03 SE
basic-info design, and basic_info/tables.py pins their names and column order.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 2: The precedence dictionary

**Files:**
- Create: `src/dagster_v3/defs/se_company/basic_info/precedence.py`
- Test: `tests/test_se_company_basic_info_precedence.py`

**Interfaces:**
- Consumes: `tables.FOLDED_FIELDS`.
- Produces: `BASIC_INFO_PRECEDENCE: dict[str, dict[str, int]]`, `SOURCES: tuple[str, ...]`, `precedence_for(field, source) -> int | None`, `precedence_rows() -> list[tuple[str, str, int]]` (sorted by field then descending precedence then source). Tasks 3 and 6 use these.

- [ ] **Step 1: Write the failing tests**

```python
"""Spec section 4: numbers per field per source, gaps for future sources, reviewer on top."""

import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.precedence import (
    BASIC_INFO_PRECEDENCE,
    SOURCES,
    precedence_for,
    precedence_rows,
)


def test_every_folded_field_has_a_map_and_the_reviewer_tops_each() -> None:
    assert tuple(BASIC_INFO_PRECEDENCE) == tables.FOLDED_FIELDS
    for field, by_source in BASIC_INFO_PRECEDENCE.items():
        assert by_source["reviewer"] == 10000, field
        assert max(by_source.values()) == 10000, field
        assert len(set(by_source.values())) == len(by_source), f"{field}: precedences must be distinct"
        assert set(by_source) <= set(SOURCES), field


def test_the_numbers_of_the_spec() -> None:
    assert BASIC_INFO_PRECEDENCE["legal_name"] == {
        "reviewer": 10000, "scb": 1000, "bolagsverket": 900, "ratsit": 300, "wikidata": 200,
    }
    assert BASIC_INFO_PRECEDENCE["description"] == {
        "reviewer": 10000, "llm": 2000, "esef": 800, "wikidata": 600, "scb": 400, "ratsit": 300,
    }
    assert BASIC_INFO_PRECEDENCE["lei"] == {"reviewer": 10000, "esef": 1000}
    assert BASIC_INFO_PRECEDENCE["wikidata_id"] == {"reviewer": 10000, "wikidata": 1000}


@pytest.mark.parametrize(
    ("field", "source", "expected"),
    [
        ("legal_name", "scb", 1000),
        ("legal_name", "esef", None),
        ("description_sv", "llm", 2000),
        ("status", "wikidata", None),
        ("description_language", "scb", None),
    ],
)
def test_precedence_for_is_none_when_a_source_cannot_supply_a_field(field, source, expected) -> None:
    assert precedence_for(field, source) == expected


def test_precedence_rows_are_the_export_in_a_stable_order() -> None:
    rows = precedence_rows()
    assert rows[:3] == [
        ("legal_name", "reviewer", 10000),
        ("legal_name", "scb", 1000),
        ("legal_name", "bolagsverket", 900),
    ]
    assert len(rows) == sum(len(m) for m in BASIC_INFO_PRECEDENCE.values())
    assert rows == sorted(rows, key=lambda r: (tables.FOLDED_FIELDS.index(r[0]), -r[2], r[1]))
```

- [ ] **Step 2: Run to verify failure**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_se_company_basic_info_precedence.py -q -p no:warnings`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write `precedence.py`**

```python
"""Per-field, per-source precedence of the basic-info fold (spec section 4).

The numbers are the owner's to adjust in review. Gaps leave room for new sources; a
source absent from a field's map cannot supply that field. description_language has no
map: it follows the row that won description. The reviewer is a source like the others,
only ranked above every automated one.
"""

from dagster_v3.defs.se_company.basic_info import tables

SOURCES: tuple[str, ...] = ("scb", "bolagsverket", "wikidata", "esef", "ratsit", "llm", "reviewer")

BASIC_INFO_PRECEDENCE: dict[str, dict[str, int]] = {
    "legal_name": {"reviewer": 10000, "scb": 1000, "bolagsverket": 900, "ratsit": 300, "wikidata": 200},
    "legal_form_code": {"reviewer": 10000, "scb": 1000, "bolagsverket": 900},
    "status": {"reviewer": 10000, "scb": 1000, "bolagsverket": 900, "ratsit": 300},
    "incorporation_date": {"reviewer": 10000, "scb": 1000, "bolagsverket": 900, "wikidata": 200},
    "lei": {"reviewer": 10000, "esef": 1000},
    "wikidata_id": {"reviewer": 10000, "wikidata": 1000},
    "description": {"reviewer": 10000, "llm": 2000, "esef": 800, "wikidata": 600, "scb": 400, "ratsit": 300},
    "description_sv": {"reviewer": 10000, "llm": 2000, "scb": 400, "ratsit": 300},
}

assert tuple(BASIC_INFO_PRECEDENCE) == tables.FOLDED_FIELDS


def precedence_for(field: str, source: str) -> int | None:
    """The precedence of `source` for `field`, or None when it cannot supply it."""
    return BASIC_INFO_PRECEDENCE.get(field, {}).get(source)


def precedence_rows() -> list[tuple[str, str, int]]:
    """Every (field, source, precedence) pair, fields in fold order, highest first."""
    rows: list[tuple[str, str, int]] = []
    for field in tables.FOLDED_FIELDS:
        by_source = BASIC_INFO_PRECEDENCE[field]
        for source, precedence in sorted(by_source.items(), key=lambda item: (-item[1], item[0])):
            rows.append((field, source, precedence))
    return rows
```

- [ ] **Step 4: Run to verify pass, ruff, commit**

Run the Step 2 command (PASS) and `uv run --frozen --no-sync ruff check src/dagster_v3/defs/se_company/basic_info tests/test_se_company_basic_info_precedence.py`.

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/precedence.py \
        corpscout/services/dagster_v3/tests/test_se_company_basic_info_precedence.py
git commit -m "feat(dagster): SE basic-info precedence per field and source

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 3: The pure fold

**Files:**
- Create: `src/dagster_v3/defs/se_company/basic_info/fold.py`
- Test: `tests/test_se_company_basic_info_fold.py`

**Interfaces:**
- Consumes: `tables.FOLDED_FIELDS`, `precedence.precedence_for`.
- Produces: `Suggestion` (frozen dataclass: `company_id, source, source_record_uid, observed_at: datetime, legal_name, legal_form_code, status, incorporation_date: date | None, lei, wikidata_id, description, description_language, description_sv`, all values `str | None` unless noted), `BasicInfoRow` (frozen dataclass with `company_id`, every folded field and its `<field>_source: str`, `description_language: str | None`, `fold_version: str`, `source_run_id: str`; method `as_tuple(folded_at) -> tuple` in `tables.MAIN_COLUMNS` order; method `changed_fields_against(other: BasicInfoRow | None) -> list[str]`), `FOLD_VERSION = "fold-v1"`, `REGISTER_SOURCES = ("scb", "bolagsverket")`, `fold_basic_info(company_id, suggestions, *, source_run_id) -> BasicInfoRow | None`. Task 4 uses all of these.

- [ ] **Step 1: Write the failing tests**

```python
"""Spec section 5: the fold as a pure function, one case per rule."""

from datetime import UTC, date, datetime

import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.fold import (
    FOLD_VERSION,
    BasicInfoRow,
    Suggestion,
    fold_basic_info,
)

T1 = datetime(2026, 9, 1, tzinfo=UTC)
T2 = datetime(2026, 9, 2, tzinfo=UTC)


def suggestion(source: str, *, uid: str = "u", observed_at: datetime = T1, **values) -> Suggestion:
    base = {field: None for field in tables.VALUE_COLUMNS}
    base.update(values)
    return Suggestion(
        company_id="5560000000", source=source, source_record_uid=uid, observed_at=observed_at, **base
    )


def test_highest_precedence_wins_per_field_and_carries_its_source() -> None:
    row = fold_basic_info(
        "5560000000",
        [
            suggestion("scb", legal_name="SCB AB", status="active", incorporation_date=date(1990, 1, 2)),
            suggestion("bolagsverket", legal_name="Bolagsverket AB", legal_form_code="AB"),
            suggestion("wikidata", legal_name="Wiki AB", wikidata_id="Q1", description="A firm", description_language="en"),
            suggestion("esef", lei="5493001KJTIIGC8Y1R12", description="ESEF text", description_language="en"),
        ],
        source_run_id="run-1",
    )
    assert row is not None
    assert (row.legal_name, row.legal_name_source) == ("SCB AB", "scb")
    assert (row.legal_form_code, row.legal_form_code_source) == ("AB", "bolagsverket")
    assert (row.status, row.status_source) == ("active", "scb")
    assert (row.incorporation_date, row.incorporation_date_source) == (date(1990, 1, 2), "scb")
    assert (row.lei, row.lei_source) == ("5493001KJTIIGC8Y1R12", "esef")
    assert (row.wikidata_id, row.wikidata_id_source) == ("Q1", "wikidata")
    assert (row.description, row.description_source) == ("ESEF text", "esef")
    assert row.description_language == "en"
    assert (row.description_sv, row.description_sv_source) == (None, "")
    assert row.fold_version == FOLD_VERSION
    assert row.source_run_id == "run-1"


def test_null_is_no_opinion_so_a_lower_source_fills_the_gap() -> None:
    row = fold_basic_info(
        "5560000000",
        [suggestion("scb", legal_name="SCB AB"), suggestion("bolagsverket", legal_name="B AB", legal_form_code="HB")],
        source_run_id="r",
    )
    assert row is not None
    assert (row.legal_form_code, row.legal_form_code_source) == ("HB", "bolagsverket")


def test_a_source_without_precedence_for_a_field_cannot_supply_it() -> None:
    row = fold_basic_info(
        "5560000000",
        [suggestion("scb", legal_name="SCB AB"), suggestion("esef", status="active", wikidata_id="Q9")],
        source_run_id="r",
    )
    assert row is not None
    assert (row.status, row.status_source) == ("", "")
    assert (row.wikidata_id, row.wikidata_id_source) == (None, "")


def test_reviewer_beats_everything_and_llm_beats_esef_on_description() -> None:
    row = fold_basic_info(
        "5560000000",
        [
            suggestion("scb", legal_name="SCB AB", description="scb text", description_language="sv"),
            suggestion("llm", description="llm text", description_language="en", description_sv="llm sv"),
            suggestion("reviewer", legal_name="Reviewed AB"),
        ],
        source_run_id="r",
    )
    assert row is not None
    assert (row.legal_name, row.legal_name_source) == ("Reviewed AB", "reviewer")
    assert (row.description, row.description_source, row.description_language) == ("llm text", "llm", "en")
    assert (row.description_sv, row.description_sv_source) == ("llm sv", "llm")


def test_ties_go_to_the_newest_observation_then_the_smaller_uid() -> None:
    # Two rows of the same source cannot exist in the table, but the fold is a pure
    # function and the rule must hold for equal precedence across sources too: give
    # wikidata and ratsit the same number by construction of the test inputs.
    older = suggestion("esef", uid="b", observed_at=T1, description="old")
    newer = suggestion("esef", uid="a", observed_at=T2, description="new")
    row = fold_basic_info(
        "5560000000", [suggestion("scb", legal_name="X AB"), older, newer], source_run_id="r"
    )
    assert row is not None
    assert row.description == "new"
    same_time_b = suggestion("esef", uid="b", observed_at=T1, description="b text")
    same_time_a = suggestion("esef", uid="a", observed_at=T1, description="a text")
    row = fold_basic_info(
        "5560000000", [suggestion("scb", legal_name="X AB"), same_time_b, same_time_a], source_run_id="r"
    )
    assert row is not None
    assert row.description == "a text"


def test_no_row_without_a_register_legal_name() -> None:
    assert fold_basic_info("5560000000", [suggestion("wikidata", legal_name="Wiki AB")], source_run_id="r") is None
    assert fold_basic_info("5560000000", [suggestion("reviewer", legal_name="Rev AB")], source_run_id="r") is None
    assert fold_basic_info("5560000000", [], source_run_id="r") is None
    # A register row with a NULL legal_name is not a supply either.
    assert fold_basic_info("5560000000", [suggestion("scb", status="active")], source_run_id="r") is None


def test_description_language_follows_the_description_winner_only() -> None:
    row = fold_basic_info(
        "5560000000",
        [
            suggestion("scb", legal_name="SCB AB", description="sv text", description_language="sv"),
            suggestion("wikidata", description_language="en"),
        ],
        source_run_id="r",
    )
    assert row is not None
    assert (row.description, row.description_language) == ("sv text", "sv")


def test_as_tuple_follows_main_columns_and_changed_fields_diff_values_and_sources() -> None:
    row = fold_basic_info("5560000000", [suggestion("scb", legal_name="SCB AB", status="active")], source_run_id="r")
    assert row is not None
    folded_at = datetime(2026, 9, 3, 12, tzinfo=UTC)
    values = row.as_tuple(folded_at)
    assert len(values) == len(tables.MAIN_COLUMNS)
    assert values[tables.MAIN_COLUMNS.index("legal_name")] == "SCB AB"
    assert values[tables.MAIN_COLUMNS.index("status_source")] == "scb"
    assert values[tables.MAIN_COLUMNS.index("folded_at")] == folded_at
    # First publish: every non-NULL field, status counts only when not ''.
    assert row.changed_fields_against(None) == ["legal_name", "status"]
    other = fold_basic_info(
        "5560000000",
        [suggestion("scb", legal_name="SCB AB"), suggestion("bolagsverket", status="active")],
        source_run_id="r",
    )
    assert other is not None
    # Same status value, different source: still a change.
    assert row.changed_fields_against(other) == ["status"]
    assert row.changed_fields_against(row) == []


def test_company_id_mismatch_is_refused() -> None:
    with pytest.raises(ValueError, match="company_id"):
        fold_basic_info("5561111111", [suggestion("scb", legal_name="X")], source_run_id="r")
```

- [ ] **Step 2: Run to verify failure**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_se_company_basic_info_fold.py -q -p no:warnings`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write `fold.py`**

```python
"""The per-company fold of suggestion rows into one basic-info row (spec section 5).

Pure: no I/O, no clock. The batch layer reads and writes; this module decides.
"""

from dataclasses import dataclass, fields
from datetime import date, datetime
from typing import Any

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.precedence import precedence_for

FOLD_VERSION = "fold-v1"
REGISTER_SOURCES: tuple[str, ...] = ("scb", "bolagsverket")


@dataclass(frozen=True, slots=True)
class Suggestion:
    """One current suggestion row. None in a value field means no opinion."""

    company_id: str
    source: str
    source_record_uid: str
    observed_at: datetime
    legal_name: str | None
    legal_form_code: str | None
    status: str | None
    incorporation_date: date | None
    lei: str | None
    wikidata_id: str | None
    description: str | None
    description_language: str | None
    description_sv: str | None


@dataclass(frozen=True, slots=True)
class BasicInfoRow:
    """One folded main row. A _source is '' when the field has no value."""

    company_id: str
    legal_name: str
    legal_name_source: str
    legal_form_code: str | None
    legal_form_code_source: str
    status: str
    status_source: str
    incorporation_date: date | None
    incorporation_date_source: str
    lei: str | None
    lei_source: str
    wikidata_id: str | None
    wikidata_id_source: str
    description: str | None
    description_source: str
    description_language: str | None
    description_sv: str | None
    description_sv_source: str
    fold_version: str
    source_run_id: str

    def as_tuple(self, folded_at: datetime) -> tuple[Any, ...]:
        """The row in tables.MAIN_COLUMNS order, ready for an INSERT ... VALUES."""
        values = {f.name: getattr(self, f.name) for f in fields(self)}
        values["folded_at"] = folded_at
        return tuple(values[column] for column in tables.MAIN_COLUMNS)

    def _value_and_source(self, field: str) -> tuple[Any, str]:
        value = getattr(self, field)
        if field == "status" and value == "":
            value = None
        return value, getattr(self, f"{field}_source")

    def changed_fields_against(self, other: "BasicInfoRow | None") -> list[str]:
        """The folded fields whose value or source differ from `other` (every non-NULL
        field when there is no other row). description_language rides with description."""
        changed: list[str] = []
        for field in tables.FOLDED_FIELDS:
            value, source = self._value_and_source(field)
            if other is None:
                if value is not None:
                    changed.append(field)
                continue
            other_value, other_source = other._value_and_source(field)
            if value != other_value or source != other_source:
                changed.append(field)
            elif field == "description" and self.description_language != other.description_language:
                changed.append(field)
        return changed


def _winner(field: str, suggestions: list[Suggestion]) -> Suggestion | None:
    candidates = []
    for suggestion in suggestions:
        if getattr(suggestion, field) is None:
            continue
        precedence = precedence_for(field, suggestion.source)
        if precedence is None:
            continue
        candidates.append((-precedence, -suggestion.observed_at.timestamp(), suggestion.source_record_uid, suggestion))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[:3])
    return candidates[0][3]


def fold_basic_info(
    company_id: str, suggestions: list[Suggestion], *, source_run_id: str
) -> BasicInfoRow | None:
    """Fold every current suggestion row of one company, or None when no register
    (SCB or Bolagsverket) row supplies a legal name."""
    for suggestion in suggestions:
        if suggestion.company_id != company_id:
            raise ValueError(
                f"suggestion company_id {suggestion.company_id!r} is not {company_id!r}"
            )
    if not any(s.source in REGISTER_SOURCES and s.legal_name is not None for s in suggestions):
        return None

    values: dict[str, Any] = {"company_id": company_id}
    for field in tables.FOLDED_FIELDS:
        winner = _winner(field, suggestions)
        if winner is None:
            values[field] = "" if field == "status" else None
            values[f"{field}_source"] = ""
        else:
            values[field] = getattr(winner, field)
            values[f"{field}_source"] = winner.source
            if field == "description":
                values["description_language"] = winner.description_language
    values.setdefault("description_language", None)
    return BasicInfoRow(fold_version=FOLD_VERSION, source_run_id=source_run_id, **values)
```

- [ ] **Step 4: Run to verify pass, ruff, commit**

Run the Step 2 command (PASS) and `uv run --frozen --no-sync ruff check src/dagster_v3/defs/se_company/basic_info tests/test_se_company_basic_info_fold.py`.

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/fold.py \
        corpscout/services/dagster_v3/tests/test_se_company_basic_info_fold.py
git commit -m "feat(dagster): pure per-company fold of SE basic-info suggestions

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 4: The batch layer

**Files:**
- Create: `src/dagster_v3/defs/se_company/basic_info/batch.py`
- Test: `tests/test_se_company_basic_info_batch.py`

**Interfaces:**
- Consumes: `fold.fold_basic_info`, `fold.Suggestion`, `fold.BasicInfoRow`, `fold.FOLD_VERSION`, `tables.*`.
- Produces: SQL text functions `bucket_company_ids_sql() -> str` (parameter `bucket`), `suggestion_watermarks_sql() -> str` and `main_watermarks_sql() -> str` (parameter `company_ids`), `current_suggestions_sql() -> str`, `current_main_rows_sql() -> str` (parameter `company_ids`), `main_insert_sql() -> str`, `history_insert_sql() -> str`; `FoldCounts(companies, considered, folded, changed, unchanged, unpublished)`; `suggestion_from_row(row) -> Suggestion`, `main_row_from_row(row) -> BasicInfoRow`; `fold_companies(client, company_ids, *, changed_only, source_run_id, folded_at, page_size=20_000, log=None) -> FoldCounts`; `fold_bucket(client, bucket, *, changed_only, source_run_id, folded_at, log=None) -> FoldCounts`. `client` is a clickhouse-driver client (`execute(sql, params)`), obtained by the asset from `ClickhouseResource.get_connection()`. Tasks 5 and 6 use these.

- [ ] **Step 1: Write the failing tests**

```python
"""Spec section 5, batch layer: changed-only selection, diff-only writes, history rows."""

from datetime import UTC, date, datetime

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.batch import (
    FoldCounts,
    bucket_company_ids_sql,
    current_main_rows_sql,
    current_suggestions_sql,
    fold_bucket,
    fold_companies,
    history_insert_sql,
    main_insert_sql,
    main_watermarks_sql,
    suggestion_watermarks_sql,
)
from dagster_v3.defs.se_company.basic_info.fold import FOLD_VERSION

T0 = datetime(2026, 9, 1, tzinfo=UTC)
T1 = datetime(2026, 9, 2, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 3, 12, tzinfo=UTC)


def suggestion_row(company_id: str, source: str, observed_at: datetime = T1, **values) -> tuple:
    """A row in the SELECT order of current_suggestions_sql."""
    base = {field: None for field in tables.VALUE_COLUMNS}
    base.update(values)
    return (company_id, source, f"{source}-uid", observed_at, *[base[c] for c in tables.VALUE_COLUMNS])


def main_row(company_id: str, **overrides) -> tuple:
    """A row in the SELECT order of current_main_rows_sql (tables.MAIN_COLUMNS minus
    folded_at, fold_version and source_run_id, which the fold does not compare)."""
    base = {
        "company_id": company_id, "legal_name": "", "legal_name_source": "",
        "legal_form_code": None, "legal_form_code_source": "", "status": "", "status_source": "",
        "incorporation_date": None, "incorporation_date_source": "", "lei": None, "lei_source": "",
        "wikidata_id": None, "wikidata_id_source": "", "description": None, "description_source": "",
        "description_language": None, "description_sv": None, "description_sv_source": "",
    }
    base.update(overrides)
    return tuple(base[c] for c in tables.MAIN_COLUMNS if c not in ("folded_at", "fold_version", "source_run_id"))


class FakeClient:
    """Answers the batch layer's SELECTs from scripted rows and records every INSERT."""

    def __init__(self, *, suggestions, mains=(), suggestion_marks=(), main_marks=(), bucket_ids=()):
        self.suggestions = list(suggestions)
        self.mains = list(mains)
        self.suggestion_marks = list(suggestion_marks)
        self.main_marks = list(main_marks)
        self.bucket_ids = list(bucket_ids)
        self.statements: list[tuple[str, object]] = []
        self.inserts: list[tuple[str, list[tuple]]] = []

    def execute(self, sql: str, params=None):
        self.statements.append((sql, params))
        if sql.startswith("INSERT INTO"):
            self.inserts.append((sql, list(params)))
            return []
        ids = set(params["company_ids"]) if params and "company_ids" in params else None
        if "max(suggested_at)" in sql:
            return [r for r in self.suggestion_marks if r[0] in ids]
        if "max(folded_at)" in sql:
            return [r for r in self.main_marks if r[0] in ids]
        if f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL" in sql:
            return [r for r in self.suggestions if r[0] in ids]
        if f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL" in sql:
            return [r for r in self.mains if r[0] in ids]
        if "modulo(cityHash64(company_id), 64)" in sql:
            return [(i,) for i in self.bucket_ids]
        raise AssertionError(sql)


def test_sql_texts_bind_company_ids_and_read_final_rows() -> None:
    assert "%(company_ids)s" in current_suggestions_sql()
    assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL" in current_suggestions_sql()
    assert "ORDER BY company_id, source" in current_suggestions_sql()
    assert f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL" in current_main_rows_sql()
    assert "max(suggested_at)" in suggestion_watermarks_sql()
    assert "max(folded_at)" in main_watermarks_sql()
    assert "modulo(cityHash64(company_id), 64) = %(bucket)s" in bucket_company_ids_sql()
    assert "%" not in bucket_company_ids_sql().replace("%(bucket)s", "")
    assert main_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({', '.join(tables.MAIN_COLUMNS)}) VALUES"
    )
    assert history_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} ({', '.join(tables.HISTORY_COLUMNS)}) VALUES"
    )


def test_first_publish_writes_main_and_history_with_every_non_null_field() -> None:
    client = FakeClient(
        suggestions=[
            suggestion_row("5560000000", "scb", legal_name="SCB AB", status="active"),
            suggestion_row("5560000000", "wikidata", wikidata_id="Q1"),
        ],
    )
    counts = fold_companies(
        client, ["5560000000"], changed_only=False, source_run_id="run-1", folded_at=FOLDED_AT
    )
    assert counts == FoldCounts(companies=1, considered=1, folded=1, changed=1, unchanged=0, unpublished=0)
    (main_sql, main_rows), (history_sql, history_rows) = client.inserts
    assert main_sql == main_insert_sql() and history_sql == history_insert_sql()
    assert len(main_rows) == 1 and len(history_rows) == 1
    row = dict(zip(tables.MAIN_COLUMNS, main_rows[0]))
    assert row["legal_name"] == "SCB AB" and row["legal_name_source"] == "scb"
    assert row["wikidata_id"] == "Q1" and row["wikidata_id_source"] == "wikidata"
    assert row["folded_at"] == FOLDED_AT and row["fold_version"] == FOLD_VERSION
    assert row["source_run_id"] == "run-1"
    history = dict(zip(tables.HISTORY_COLUMNS, history_rows[0]))
    assert history["changed_fields"] == ["legal_name", "status", "wikidata_id"]
    assert history["legal_name"] == "SCB AB"


def test_an_unchanged_company_writes_nothing() -> None:
    client = FakeClient(
        suggestions=[suggestion_row("5560000000", "scb", legal_name="SCB AB", status="active")],
        mains=[main_row("5560000000", legal_name="SCB AB", legal_name_source="scb", status="active", status_source="scb")],
    )
    counts = fold_companies(client, ["5560000000"], changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert counts.unchanged == 1 and counts.changed == 0
    assert client.inserts == []


def test_a_changed_source_alone_is_a_change_and_names_the_field() -> None:
    client = FakeClient(
        suggestions=[
            suggestion_row("5560000000", "scb", legal_name="SCB AB"),
            suggestion_row("5560000000", "bolagsverket", legal_name="SCB AB", status="active"),
        ],
        mains=[main_row("5560000000", legal_name="SCB AB", legal_name_source="scb", status="active", status_source="scb")],
    )
    counts = fold_companies(client, ["5560000000"], changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert counts.changed == 1
    history = dict(zip(tables.HISTORY_COLUMNS, client.inserts[1][1][0]))
    assert history["changed_fields"] == ["status"]
    assert history["status_source"] == "bolagsverket"


def test_changed_only_keeps_new_companies_and_those_with_newer_suggestions() -> None:
    client = FakeClient(
        suggestions=[
            suggestion_row("5560000000", "scb", legal_name="A AB"),
            suggestion_row("5561111111", "scb", legal_name="B AB"),
            suggestion_row("5562222222", "scb", legal_name="C AB"),
        ],
        mains=[
            main_row("5561111111", legal_name="B AB", legal_name_source="scb"),
            main_row("5562222222", legal_name="C old", legal_name_source="scb"),
        ],
        suggestion_marks=[("5560000000", T1), ("5561111111", T0), ("5562222222", T1)],
        main_marks=[("5561111111", T1), ("5562222222", T0)],
    )
    counts = fold_companies(
        client, ["5560000000", "5561111111", "5562222222"], changed_only=True, source_run_id="r", folded_at=FOLDED_AT
    )
    # 5561111111 was folded after its newest suggestion: skipped before any fold.
    assert counts == FoldCounts(companies=3, considered=2, folded=2, changed=2, unchanged=0, unpublished=0)
    read = [p["company_ids"] for s, p in client.statements if s == current_suggestions_sql()]
    assert read == [["5560000000", "5562222222"]]


def test_a_company_without_register_legal_name_is_unpublished_and_untouched() -> None:
    client = FakeClient(suggestions=[suggestion_row("5560000000", "wikidata", legal_name="Wiki AB")])
    counts = fold_companies(client, ["5560000000"], changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert counts.unpublished == 1 and counts.folded == 0
    assert client.inserts == []


def test_pages_bound_each_read_and_write() -> None:
    ids = [f"556{i:07d}" for i in range(5)]
    client = FakeClient(suggestions=[suggestion_row(i, "scb", legal_name=f"{i} AB") for i in ids])
    counts = fold_companies(client, ids, changed_only=False, source_run_id="r", folded_at=FOLDED_AT, page_size=2)
    assert counts.companies == 5 and counts.changed == 5
    read_sizes = [len(p["company_ids"]) for s, p in client.statements if s == current_suggestions_sql()]
    assert read_sizes == [2, 2, 1]
    assert [len(rows) for sql, rows in client.inserts if sql == main_insert_sql()] == [2, 2, 1]


def test_fold_bucket_reads_the_partition_ids_then_folds_them() -> None:
    client = FakeClient(
        suggestions=[suggestion_row("5560000000", "scb", legal_name="A AB")],
        bucket_ids=["5560000000"],
    )
    counts = fold_bucket(client, 7, changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert counts.companies == 1 and counts.changed == 1
    assert client.statements[0][1] == {"bucket": 7}


def test_invalid_company_ids_are_refused_before_any_query() -> None:
    client = FakeClient(suggestions=[])
    import pytest

    with pytest.raises(ValueError, match="company id"):
        fold_companies(client, ["not-an-id"], changed_only=False, source_run_id="r", folded_at=FOLDED_AT)
    assert client.statements == []
```

- [ ] **Step 2: Run to verify failure**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_se_company_basic_info_batch.py -q -p no:warnings`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write `batch.py`**

```python
"""Read current suggestion rows, fold in memory, write only what changed (spec section 5).

Every SELECT is a function returning its exact text so the clickhouse-local harness runs
the same SQL. Parameters bind client-side through clickhouse-driver's %(name)s syntax,
which is why the partition filter says modulo(...) rather than the % operator.
"""

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.fold import BasicInfoRow, Suggestion, fold_basic_info
from dagster_v3.defs.se_company.common import normalized_se_company_ids

BUCKET_COUNT = 64
PAGE_SIZE = 20_000

_SUGGESTION_SELECT_COLUMNS = ("company_id", "source", "source_record_uid", "observed_at", *tables.VALUE_COLUMNS)
_MAIN_COMPARE_COLUMNS = tuple(
    c for c in tables.MAIN_COLUMNS if c not in ("folded_at", "fold_version", "source_run_id")
)


@dataclass(frozen=True, slots=True)
class FoldCounts:
    companies: int
    considered: int
    folded: int
    changed: int
    unchanged: int
    unpublished: int

    def as_metadata(self) -> dict[str, int]:
        return {
            "companies": self.companies,
            "considered": self.considered,
            "folded": self.folded,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "unpublished": self.unpublished,
        }


def bucket_company_ids_sql() -> str:
    return (
        "SELECT DISTINCT company_id\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE}\n"
        f"WHERE modulo(cityHash64(company_id), {BUCKET_COUNT}) = %(bucket)s\n"
        "ORDER BY company_id"
    )


def suggestion_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(suggested_at) AS suggested_at\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def main_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(folded_at) AS folded_at\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def current_suggestions_sql() -> str:
    return (
        f"SELECT {', '.join(_SUGGESTION_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s\n"
        "ORDER BY company_id, source"
    )


def current_main_rows_sql() -> str:
    return (
        f"SELECT {', '.join(_MAIN_COMPARE_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def main_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({', '.join(tables.MAIN_COLUMNS)}) VALUES"


def history_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} ({', '.join(tables.HISTORY_COLUMNS)}) VALUES"


def suggestion_from_row(row: Sequence[Any]) -> Suggestion:
    return Suggestion(**dict(zip(_SUGGESTION_SELECT_COLUMNS, row)))


def main_row_from_row(row: Sequence[Any]) -> BasicInfoRow:
    values = dict(zip(_MAIN_COMPARE_COLUMNS, row))
    return BasicInfoRow(fold_version="", source_run_id="", **values)


def _pages(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def _changed_company_ids(client: Any, company_ids: list[str]) -> list[str]:
    params = {"company_ids": company_ids}
    suggested = dict(client.execute(suggestion_watermarks_sql(), params))
    folded = dict(client.execute(main_watermarks_sql(), params))
    return [
        company_id
        for company_id in company_ids
        if company_id in suggested
        and (company_id not in folded or suggested[company_id] > folded[company_id])
    ]


def fold_companies(
    client: Any,
    company_ids: Sequence[str],
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    page_size: int = PAGE_SIZE,
    log: Callable[..., object] | None = None,
) -> FoldCounts:
    """Fold the given companies in pages; write only rows that differ from the current
    main row, one history row per changed company."""
    # Sorted, de-duplicated, validated: the helper raises "Sweden company ids must be 10
    # or 12 digits" on a bad id, before any query.
    ids = list(normalized_se_company_ids(company_ids))
    considered = folded = changed = unchanged = unpublished = 0
    for page in _pages(ids, page_size):
        scope = _changed_company_ids(client, page) if changed_only else page
        considered += len(scope)
        if not scope:
            continue
        params = {"company_ids": scope}
        by_company: dict[str, list[Suggestion]] = defaultdict(list)
        for row in client.execute(current_suggestions_sql(), params):
            suggestion = suggestion_from_row(row)
            by_company[suggestion.company_id].append(suggestion)
        current = {row[0]: main_row_from_row(row) for row in client.execute(current_main_rows_sql(), params)}
        main_rows: list[tuple[Any, ...]] = []
        history_rows: list[tuple[Any, ...]] = []
        for company_id in scope:
            folded_row = fold_basic_info(company_id, by_company.get(company_id, []), source_run_id=source_run_id)
            if folded_row is None:
                unpublished += 1
                continue
            folded += 1
            changed_fields = folded_row.changed_fields_against(current.get(company_id))
            if not changed_fields:
                unchanged += 1
                continue
            changed += 1
            values = folded_row.as_tuple(folded_at)
            main_rows.append(values)
            history_rows.append((*values, changed_fields))
        if main_rows:
            client.execute(main_insert_sql(), main_rows)
            client.execute(history_insert_sql(), history_rows)
        if log is not None:
            log(
                "Folded basic info page: companies=%d considered=%d changed=%d unchanged=%d unpublished=%d",
                len(page), len(scope), len(main_rows), unchanged, unpublished,
            )
    return FoldCounts(
        companies=len(ids), considered=considered, folded=folded, changed=changed,
        unchanged=unchanged, unpublished=unpublished,
    )


def fold_bucket(
    client: Any,
    bucket: int,
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    log: Callable[..., object] | None = None,
) -> FoldCounts:
    """Fold every company whose id hashes into `bucket` (0..63)."""
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"bucket out of range: {bucket}")
    company_ids = [row[0] for row in client.execute(bucket_company_ids_sql(), {"bucket": bucket})]
    return fold_companies(
        client, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, log=log,
    )
```

`normalized_se_company_ids` (`defs/se_company/common.py:34`) returns the ids sorted and de-duplicated and raises `ValueError("Sweden company ids must be 10 or 12 digits: ...")` on a bad one — which is why the batch tests above list ids in ascending order and the invalid-id test matches on `"company id"`.

- [ ] **Step 4: Run to verify pass, ruff, commit**

Run the Step 2 command (PASS) and `uv run --frozen --no-sync ruff check src/dagster_v3/defs/se_company/basic_info tests/test_se_company_basic_info_batch.py`.

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/batch.py \
        corpscout/services/dagster_v3/tests/test_se_company_basic_info_batch.py
git commit -m "feat(dagster): SE basic-info batch fold writes only changed rows and their history

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 5: The clickhouse-local harness

**Files:**
- Test: `tests/test_se_company_basic_info_clickhouse_local.py`

**Interfaces:**
- Consumes: the four migration files, `batch.*_sql()` texts, `tables.*`. Uses `tests.test_se_company_person_clickhouse_local._clickhouse_local_command` (Docker or a local binary; skips only when neither exists — on this machine Docker is available and the test must run).

The harness runs SQL only (clickhouse-local has no Python client): it applies the real DDL, inserts suggestion rows, and executes the exact SQL texts the batch layer sends, under both `join_use_nulls` settings, comparing to hand-written output. The Python fold itself is covered by Task 3; the batch loop by Task 4; here the claims are about ClickHouse: FINAL gives the current row per (company, source), `content_hash` tells NULL from `''`, and the watermark queries and inserts work on the real schema.

- [ ] **Step 1: Write the harness**

```python
"""The basic-info tables and the batch layer's SQL on a real ClickHouse (spec section 9).

Claims a fake client cannot settle:
1. The real DDL applies, and FINAL on the suggestion table returns the newest version per
   (company_id, source) -- a re-suggestion replaces, a second source adds.
2. content_hash tells NULL (no opinion) from '' (says empty), so a release hashes
   differently from an empty value and an extractor's hash anti-join sees the change.
3. The watermark queries and the main/history INSERTs accept the exact texts and row
   shapes batch.py sends, under join_use_nulls 0 and 1.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.batch import (
    bucket_company_ids_sql,
    current_main_rows_sql,
    current_suggestions_sql,
    history_insert_sql,
    main_insert_sql,
    main_watermarks_sql,
    suggestion_watermarks_sql,
)
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000376_corpscout_se_company_basic_info_suggestion.up.sql",
    "000377_corpscout_se_company_basic_info.up.sql",
    "000378_corpscout_se_company_basic_info_history.up.sql",
    "000379_corpscout_se_company_basic_info_precedence.up.sql",
)


def _schema_statements() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                statements.append(statement)
    return statements


def _run(statements: list[str], *, join_use_nulls: int) -> list[str]:
    script = f"SET join_use_nulls = {join_use_nulls};\n" + ";\n".join(statements) + ";\n"
    completed = subprocess.run(
        _clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _bind(sql: str, **params: object) -> str:
    """Render batch.py's %(name)s parameters the way clickhouse-driver does."""
    rendered = sql
    for name, value in params.items():
        if isinstance(value, list):
            literal = "(" + ", ".join(f"'{v}'" for v in value) + ")"
        elif isinstance(value, str):
            literal = f"'{value}'"
        else:
            literal = str(value)
        rendered = rendered.replace(f"%({name})s", literal)
    return rendered


def _suggestion(company_id: str, source: str, suggested_at: str, *, legal_name: str | None, status: str | None) -> str:
    def lit(value: str | None) -> str:
        return "NULL" if value is None else f"'{value}'"

    return (
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} "
        "(company_id, source, source_record_uid, observed_at, legal_name, status, suggested_at, source_run_id, extractor_version) VALUES "
        f"('{company_id}', '{source}', '{source}-uid', toDateTime64('{suggested_at}', 3, 'UTC'), "
        f"{lit(legal_name)}, {lit(status)}, toDateTime64('{suggested_at}', 3, 'UTC'), 'run-1', 'x-v1')"
    )


@pytest.mark.parametrize("join_use_nulls", [0, 1], ids=["join_use_nulls_off", "join_use_nulls_on"])
def test_final_returns_the_current_row_per_company_and_source(join_use_nulls: int) -> None:
    script = _schema_statements() + [
        _suggestion("5560000000", "scb", "2026-09-01 00:00:00", legal_name="Old AB", status="active"),
        _suggestion("5560000000", "scb", "2026-09-02 00:00:00", legal_name="New AB", status="active"),
        _suggestion("5560000000", "wikidata", "2026-09-01 12:00:00", legal_name="Wiki AB", status=None),
        _suggestion("5561111111", "bolagsverket", "2026-09-01 00:00:00", legal_name="B AB", status=None),
        _bind(current_suggestions_sql(), company_ids=["5560000000", "5561111111"]),
        _bind(suggestion_watermarks_sql(), company_ids=["5560000000", "5561111111"]) + " ORDER BY company_id",
        _bind(bucket_company_ids_sql(), bucket=0).replace(
            "WHERE modulo(cityHash64(company_id), 64) = 0", "WHERE 1 = 1"
        ),
    ]
    lines = _run(script, join_use_nulls=join_use_nulls)
    # current_suggestions_sql: three rows, the scb row is the 2026-09-02 version.
    assert lines[0].startswith("5560000000\tscb\tscb-uid\t2026-09-02 00:00:00.000\tNew AB")
    assert lines[1].startswith("5560000000\twikidata\twikidata-uid\t2026-09-01 12:00:00.000\tWiki AB")
    assert lines[2].startswith("5561111111\tbolagsverket\tbolagsverket-uid")
    # watermarks: the newest suggested_at per company.
    assert lines[3] == "5560000000\t2026-09-02 00:00:00.000"
    assert lines[4] == "5561111111\t2026-09-01 00:00:00.000"
    # the bucket query (with its filter widened to every company) lists both ids.
    assert lines[5:7] == ["5560000000", "5561111111"]


def test_content_hash_tells_null_from_empty_and_is_stable() -> None:
    script = _schema_statements() + [
        _suggestion("5560000000", "reviewer", "2026-09-01 00:00:00", legal_name="X AB", status=None),
        _suggestion("5560000000", "scb", "2026-09-01 00:00:00", legal_name="X AB", status=""),
        _suggestion("5561111111", "scb", "2026-09-05 00:00:00", legal_name="X AB", status=None),
        f"SELECT company_id, source, content_hash FROM {tables.QUALIFIED_SUGGESTION_TABLE} ORDER BY company_id, source",
    ]
    lines = _run(script, join_use_nulls=0)
    reviewer_hash = lines[0].split("\t")[2]
    scb_empty_hash = lines[1].split("\t")[2]
    other_company_same_values_hash = lines[2].split("\t")[2]
    assert len(reviewer_hash) == 64 and reviewer_hash == reviewer_hash.lower()
    assert reviewer_hash != scb_empty_hash  # NULL status vs '' status
    # The hash covers only the nine value columns: same values elsewhere -> same hash.
    assert reviewer_hash == other_company_same_values_hash


@pytest.mark.parametrize("join_use_nulls", [0, 1], ids=["join_use_nulls_off", "join_use_nulls_on"])
def test_main_and_history_inserts_accept_the_batch_row_shape(join_use_nulls: int) -> None:
    main_values = (
        "('5560000000', 'X AB', 'scb', NULL, '', 'active', 'scb', toDate32('1990-01-02'), 'scb', "
        "NULL, '', NULL, '', NULL, '', NULL, NULL, '', toDateTime64('2026-09-03 12:00:00', 3, 'UTC'), 'fold-v1', 'run-1')"
    )
    script = _schema_statements() + [
        f"{main_insert_sql()} {main_values}",
        f"{history_insert_sql()} {main_values[:-1]}, ['legal_name', 'status', 'incorporation_date'])",
        _bind(current_main_rows_sql(), company_ids=["5560000000"]),
        _bind(main_watermarks_sql(), company_ids=["5560000000"]),
        f"SELECT company_id, changed_fields FROM {tables.QUALIFIED_HISTORY_TABLE}",
    ]
    lines = _run(script, join_use_nulls=join_use_nulls)
    assert lines[0].startswith("5560000000\tX AB\tscb\t\\N\t\tactive\tscb\t1990-01-02\tscb")
    assert lines[1] == "5560000000\t2026-09-03 12:00:00.000"
    assert lines[2] == "5560000000\t['legal_name','status','incorporation_date']"
```

- [ ] **Step 2: Run the harness**

Run: `cd corpscout/services/dagster_v3 && uv run --frozen --no-sync pytest tests/test_se_company_basic_info_clickhouse_local.py -q -p no:warnings -m integration -v`
Expected: 5 passed on Docker (a skip is a failure of this step on this machine). If clickhouse-local rejects the `'\x1F'` escape in the MATERIALIZED expression, replace it in 000376 with `char(31)` (both files: the DDL and the Task 1 test's `if(... IS NULL, '~', concat('=', ` pins do not mention the separator) and report it. If `INSERT ... VALUES` with an `Array(String)` literal needs a different bracket style, fix the test's literal, not the SQL text.

- [ ] **Step 3: ruff and commit**

`uv run --frozen --no-sync ruff check tests/test_se_company_basic_info_clickhouse_local.py`

```bash
git add corpscout/services/dagster_v3/tests/test_se_company_basic_info_clickhouse_local.py
git commit -m "test(dagster): prove the SE basic-info tables and batch SQL on a real ClickHouse

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 6: The three assets

**Files:**
- Create: `src/dagster_v3/defs/se_company/basic_info/assets.py`
- Test: `tests/test_se_company_basic_info_assets.py`

**Interfaces:**
- Consumes: `batch.fold_bucket`, `batch.fold_companies`, `batch.BUCKET_COUNT`, `precedence.precedence_rows`, `tables.*`, `dagster_v3.defs.clickhouse.resolved.assert_clickhouse_tables_exist(clickhouse, database=, tables=)`, `dagster_clickhouse.ClickhouseResource`.
- Produces: `BASIC_INFO_FOLD_PARTITIONS` (64 keys `bucket_00`..`bucket_63`), `basic_info_bucket_index(partition_key) -> int`, `BasicInfoFoldConfig(changed_only: bool = True)`, `BasicInfoFoldCompaniesConfig(company_ids: list[str], changed_only: bool = False)`, assets `se_company_basic_info_fold`, `se_company_basic_info_fold_companies`, `se_company_basic_info_precedence_clickhouse`, `FOLD_POOL = "se_company_basic_info_fold"`, `GROUP_NAME = "se_company_basic_info"`. Slice 2 adds the extractors and the job; slice 3 launches `se_company_basic_info_fold_companies`.

- [ ] **Step 1: Write the failing tests**

```python
"""Spec section 6: the fold assets and the precedence export, registered and configured."""

import dagster as dg
import pytest

from dagster_v3.defs.se_company.basic_info.assets import (
    BASIC_INFO_FOLD_PARTITIONS,
    FOLD_POOL,
    GROUP_NAME,
    BasicInfoFoldCompaniesConfig,
    BasicInfoFoldConfig,
    basic_info_bucket_index,
)
from dagster_v3.defs.se_company.basic_info.batch import BUCKET_COUNT


def test_partitions_are_sixty_four_hash_buckets() -> None:
    keys = BASIC_INFO_FOLD_PARTITIONS.get_partition_keys()
    assert len(keys) == BUCKET_COUNT == 64
    assert keys[0] == "bucket_00" and keys[-1] == "bucket_63"
    assert basic_info_bucket_index("bucket_07") == 7
    with pytest.raises(ValueError):
        basic_info_bucket_index("bucket_64")
    with pytest.raises(ValueError):
        basic_info_bucket_index("07")


def test_configs_default_the_way_the_spec_says() -> None:
    assert BasicInfoFoldConfig().changed_only is True
    targeted = BasicInfoFoldCompaniesConfig(company_ids=["5560000000"])
    assert targeted.changed_only is False
    with pytest.raises(ValueError):
        BasicInfoFoldCompaniesConfig(company_ids=[])


def test_assets_are_registered_with_pool_partitions_and_backfill_policy() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    graph = repo.asset_graph
    fold = graph.get(dg.AssetKey("se_company_basic_info_fold"))
    assert fold.group_name == GROUP_NAME
    assert fold.partitions_def == BASIC_INFO_FOLD_PARTITIONS
    assert fold.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    targeted = graph.get(dg.AssetKey("se_company_basic_info_fold_companies"))
    assert targeted.group_name == GROUP_NAME
    assert targeted.partitions_def is None
    export = graph.get(dg.AssetKey("se_company_basic_info_precedence_clickhouse"))
    assert export.group_name == GROUP_NAME
    # One pool serializes the two fold assets (tests/test_sweden_company_assets.py pins
    # pools the same way).
    assert fold.pools == {FOLD_POOL}
    assert targeted.pools == {FOLD_POOL}
    assert export.pools == set()
    # No automation in this slice: nothing schedules or senses these assets.
    for schedule in repo.schedule_defs:
        assert "basic_info" not in schedule.name
    for sensor in repo.sensor_defs:
        assert "basic_info" not in sensor.name
```

`export.pools == set()` assumes an asset without `pool=` reports an empty set; if Dagster reports `None` or `{None}` instead, assert that and report it.

- [ ] **Step 2: Run to verify failure**

Run: `cd corpscout/services/dagster_v3 && WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_basic_info_assets.py -q -p no:warnings`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write `assets.py`**

```python
"""The fold assets and the precedence export (spec section 6). Everything manual: no
schedule, no sensor, until the fold has proven itself on production."""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.batch import BUCKET_COUNT, fold_bucket, fold_companies
from dagster_v3.defs.se_company.basic_info.precedence import precedence_rows
from dagster_v3.defs.se_company.common import normalized_se_company_ids

GROUP_NAME = "se_company_basic_info"
FOLD_POOL = "se_company_basic_info_fold"

BASIC_INFO_FOLD_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket:02d}" for bucket in range(BUCKET_COUNT)]
)


def basic_info_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"invalid basic-info fold partition key: {partition_key!r}")
    bucket = int(suffix)
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"basic-info fold bucket out of range: {bucket}")
    return bucket


class BasicInfoFoldConfig(dg.Config):
    # True: only companies whose newest suggestion is later than their main row's
    # folded_at (or that have no main row). False re-folds the whole bucket, which still
    # writes only rows that differ.
    changed_only: bool = True


class BasicInfoFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        # Sorted, de-duplicated; raises on an id that is not 10 or 12 digits.
        return list(normalized_se_company_ids(value))


_FOLD_TABLES = (tables.SUGGESTION_TABLE, tables.MAIN_TABLE, tables.HISTORY_TABLE)


@dg.asset(
    name="se_company_basic_info_fold",
    partitions_def=BASIC_INFO_FOLD_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=FOLD_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE},
    description=(
        "Folds every current suggestion row of the companies in one of 64 hash buckets "
        "into se_company_basic_info by the per-field precedence, writing only rows that "
        "differ and one history row per change. Manual: launch a partition or a backfill "
        "from the UI."
    ),
)
def se_company_basic_info_fold(
    context: dg.AssetExecutionContext, config: BasicInfoFoldConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    bucket = basic_info_bucket_index(context.partition_key)
    with clickhouse.get_connection() as client:
        counts = fold_bucket(
            client, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "bucket": bucket, "changed_only": config.changed_only,
                  "table": tables.QUALIFIED_MAIN_TABLE}
    )


@dg.asset(
    name="se_company_basic_info_fold_companies",
    pool=FOLD_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE},
    description=(
        "The targeted fold: the companies named in config.company_ids, whatever their "
        "bucket. The backoffice's Fold now button launches this asset for one company."
    ),
)
def se_company_basic_info_fold_companies(
    context: dg.AssetExecutionContext, config: BasicInfoFoldCompaniesConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    with clickhouse.get_connection() as client:
        counts = fold_companies(
            client, config.company_ids, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "changed_only": config.changed_only, "table": tables.QUALIFIED_MAIN_TABLE}
    )


@dg.asset(
    name="se_company_basic_info_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports BASIC_INFO_PRECEDENCE to se_company_basic_info_precedence for the "
        "backoffice to display and validate against. The Python dictionary is the only "
        "source; re-run after changing it."
    ),
)
def se_company_basic_info_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,))
    exported_at = datetime.now(UTC)
    rows = [(field, source, precedence, exported_at) for field, source, precedence in precedence_rows()]
    with clickhouse.get_connection() as client:
        client.execute(
            f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} ({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
            rows,
        )
        stale = int(
            client.execute(
                f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL WHERE exported_at < %(exported_at)s",
                {"exported_at": exported_at},
            )[0][0]
        )
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; "
            "they stay until removed by hand", stale,
        )
    return dg.MaterializeResult(
        metadata={"pairs": len(rows), "stale_pairs": stale, "table": tables.QUALIFIED_PRECEDENCE_TABLE}
    )
```

- [ ] **Step 4: Run to verify pass, then the definitions check**

Run the Step 2 command (PASS), then:
```bash
uv run --frozen --no-sync ruff check src/dagster_v3/defs/se_company/basic_info tests/test_se_company_basic_info_assets.py
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
```
Expected: ruff clean, `dg check defs` exits 0. Also run `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_backfill_policy_contracts.py tests/test_schedule_cron_contracts.py -q -p no:warnings` and confirm the failure set is exactly the pre-existing one (those two tests fail on this branch before your change; they must not fail for a new reason — read their output and report).

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/assets.py \
        corpscout/services/dagster_v3/tests/test_se_company_basic_info_assets.py
git commit -m "feat(dagster): SE basic-info fold assets and precedence export

se_company_basic_info_fold folds one of 64 hash buckets per run,
se_company_basic_info_fold_companies folds named companies, and
se_company_basic_info_precedence_clickhouse exports the precedence table. No
schedule and no sensor: the fold is manual until it has proven itself.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 7: Docs, whole suite, handoff

**Files:**
- Create: `src/dagster_v3/defs/se_company/basic_info/docs/basic_info-design.md`
- Modify: `docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md` (a status line under section 10's slice 1 entry)

- [ ] **Step 1: Write the module doc**

`src/dagster_v3/defs/se_company/basic_info/docs/basic_info-design.md`:

```markdown
# se_company.basic_info

The basic-info entity of the 2026-09-03 SE basic-info design
(`docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md`).

| Module | Responsibility |
| --- | --- |
| `tables.py` | Table names and column tuples, pinned against migrations 000376-000379 |
| `precedence.py` | `BASIC_INFO_PRECEDENCE`: numbers per field per source; the reviewer is a source ranked 10000 |
| `fold.py` | `fold_basic_info`: pure, one company, highest precedence wins, ties to newest `observed_at` then smaller uid; no row without a register legal name |
| `batch.py` | Reads current suggestion rows (`FINAL`), folds in pages of 20,000, writes only rows that differ plus one history row per change |
| `assets.py` | `se_company_basic_info_fold` (64 hash buckets, `multi_run(1)`, pool `se_company_basic_info_fold`), `se_company_basic_info_fold_companies` (targeted), `se_company_basic_info_precedence_clickhouse` |

Suggestion rows come from the slice-2 extractors (`se_basic_info_suggestions_<source>`) and,
for the `reviewer` source, from the backoffice (slice 3). NULL in a value column is "no
opinion". `content_hash` is materialized by ClickHouse over the nine value columns and
tells NULL from `''`.

Operating the fold: materialize one `bucket_NN` partition or launch a backfill of all 64
from the UI; `changed_only` (default true) skips companies folded after their newest
suggestion. `se_company_basic_info_fold_companies` takes `company_ids` and re-folds them
whatever their bucket. Nothing is scheduled.
```

- [ ] **Step 2: Whole suite, harness, definitions**

```bash
cd corpscout/services/dagster_v3
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests -q -p no:warnings -m "not integration"
uv run --frozen --no-sync pytest tests/test_se_company_basic_info_clickhouse_local.py -q -p no:warnings -m integration
WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync dg check defs
uv run --frozen --no-sync ruff check src/dagster_v3/defs/se_company/basic_info tests
```
Expected: only the 5 pre-existing failures (backfill_policy, duckdb_bulk_loading, nace staged flow, schedule_cron, lantmateriet docs); harness 5 passed; defs OK; ruff clean.

- [ ] **Step 3: Record the slice-1 status in the spec**

Under section 10's entry `1. Tables, precedence, fold function, the two fold assets, the precedence export.` append:

```markdown
   Built <YYYY-MM-DD> (plan `2026-09-03-se-basic-info-1-tables-fold.md`): migrations
   000376-000379, package `dagster_v3.defs.se_company.basic_info` (`tables`, `precedence`,
   `fold`, `batch`, `assets`). Slice 2's extractors insert through
   `tables.SUGGESTION_INSERT_COLUMNS` (never `content_hash`, ClickHouse materializes it) and
   dedupe on `content_hash` against `FINAL` rows; they must set `observed_at` to the source
   observation time, since ties break on it. The fold assets exist but the suggestion table
   is empty until slice 2 runs.
```

- [ ] **Step 4: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/basic_info/docs/basic_info-design.md \
        corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md
git commit -m "docs(dagster): describe the SE basic-info package and record the slice-1 handoff

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5"
```

---

### Task 8: Production — apply the migrations and export the precedence (owner-gated)

Run by the coordinator after the branch is merged to main, each step confirmed with the owner. Nothing here folds anything: the suggestion table stays empty until slice 2.

- [ ] **Step 1: Ledger check.** `ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT version, dirty FROM corpscout.schema_migrations ORDER BY sequence DESC LIMIT 1\""` must answer `375	0`.
- [ ] **Step 2: Apply 000376, 000377, 000378, 000379** one at a time with `make -C <checkout>/corpscout clickhouse-migrate-up-one` (needs `corpscout/.env` in that checkout), reading the version after each (`376`, `377`, `378`, `379`, all clean).
- [ ] **Step 3: Confirm** the four tables exist and are empty: `SELECT name, engine, total_rows FROM system.tables WHERE database = 'corpscout' AND name LIKE 'se_company_basic_info%' ORDER BY name`.
- [ ] **Step 4: Deploy** from a pristine worktree of the merged commit (the light_sync recipe: `.env` copied, `uv sync --frozen`, the two `dbt parse` calls, `dg utils refresh-defs-state`, `dg check defs`, then `ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml` with the RC captured; `failed=0` in the recap).
- [ ] **Step 5: Materialize `se_company_basic_info_precedence_clickhouse`** from the UI or GraphQL; expected metadata `pairs` = 33 (the sum of the eight maps), `stale_pairs` = 0. Confirm: `SELECT count() FROM corpscout.se_company_basic_info_precedence FINAL` = 33.
- [ ] **Step 6: Smoke the targeted fold on an empty table:** launch `se_company_basic_info_fold_companies` with `company_ids: ["5560000000"]`; expected metadata `companies` 1, `unpublished` 1, no rows written.

---

## Self-review

**Spec coverage.** 3.2 suggestion table → Task 1 (DDL, NULL semantics, content_hash NULL-safe, CHECK). 3.3 main table → Task 1 (per-field `_source`, `status ''`, `folded_at` version, `fold_version`). 3.4 history → Task 1 (`changed_fields`, MergeTree ORDER BY (company_id, folded_at)); written only on change → Task 4. 3.5 precedence table → Task 1; exported from code → Task 6. Section 4 dictionary with the spec's numbers, absence = cannot supply, `description_language` unmapped → Task 2. Section 5 rules 1–4 → Task 3 (winner, ties, publish rule, fold_version + run id); batch steps 1–4 and pages of 20,000 → Task 4 (`FINAL` per company and source, watermark selection, diff on values and sources, history per changed company). Section 6 fold assets (64 partitions on `cityHash64 % 64`, `multi_run(1)`, one pool, `changed_only` default true, `company_ids` required, precedence export, no sensor, no schedule) → Task 6; the extractors and the job are slice 2 by the spec. Section 9 pure-function tests → Task 3; fake client batch tests → Task 4; clickhouse-local both `join_use_nulls` → Task 5; extractor SQL and backoffice → later slices. Section 11 names → used verbatim throughout.

**Deliberately not in this slice:** the `max_removed_fraction` guard parked in the slice-0 handoff belongs to the source-table publisher, not the fold; reviewer-row semantics beyond the table shape (slice 3); the extract job and weekly schedule (slice 2).

**Placeholder scan.** No TBD/TODO. Task 8 Step 5's `33` is computed from the section-4 maps (5+3+4+4+2+2+6+4). Task 7 Step 3's `<YYYY-MM-DD>` is the day Task 6 is committed, filled at execution.

**Type consistency.** `Suggestion`'s fields = `_SUGGESTION_SELECT_COLUMNS` order (`company_id, source, source_record_uid, observed_at, *VALUE_COLUMNS`) — `suggestion_from_row` zips them by name so order matters only in the SELECT. `BasicInfoRow.as_tuple` follows `tables.MAIN_COLUMNS`; `main_row_from_row` zips `_MAIN_COMPARE_COLUMNS` (MAIN_COLUMNS minus the three run columns) and fills `fold_version`/`source_run_id` with `''`, which `changed_fields_against` never compares. `history_rows` = `(*main_tuple, changed_fields)` = `HISTORY_COLUMNS` order. `FoldCounts` fields are used identically in Tasks 4 and 6 (`as_metadata`). `BUCKET_COUNT` is defined once in `batch.py` and imported by `assets.py`; the SQL uses `modulo(cityHash64(company_id), 64)` with the literal from the same constant. `precedence_rows()` returns `(field, source, precedence)` and Task 6 appends `exported_at` in `PRECEDENCE_COLUMNS` order.
