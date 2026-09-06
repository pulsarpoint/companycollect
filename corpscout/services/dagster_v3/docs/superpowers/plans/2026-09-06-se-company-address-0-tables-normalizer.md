# SE company address entity, slice 0: tables and normalizer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the six address-entity tables, the `se_company/address` package, the Swedish address normalizer with its golden corpus, and the normalize asset that turns raw suggestion rows into normalized rows.

**Architecture:** Mirrors `se_company/basic_info`: migrations own the schema, `tables.py` pins names and column tuples against the DDL, a pure normalizer function per country (Sweden only) with a JSONL golden corpus, and one Dagster asset that scans the raw suggestion table for rows whose normalized version is missing, older, or on an older normalizer, normalizes them in Python and inserts new versions. Nothing folds yet (slice 2) and nothing writes raw rows yet (slice 1), so slice 0 ends with empty tables on prod and a green `dg check defs`.

**Tech Stack:** ClickHouse 26.5 (golang-migrate), Dagster 1.13.9 (`uv run --frozen --no-sync`), Python 3.14, pytest, `clickhouse-local` for the integration test.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md` (sections 3, 4, 10).

## Global Constraints

- All work in `corpscout/services/dagster_v3` of the worktree; tests run as `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/<file> -q`; `uv run --frozen --no-sync dg check defs` must pass before every commit that touches `src/`.
- Migrations are numbered `000382` to `000387` in this order: `se_company_address_suggestion`, `se_company_address_normalized`, `se_company_address_v2`, `se_company_address_history`, `se_company_address_rule`, `se_company_address_precedence`. Each `.up.sql` starts with `CREATE DATABASE IF NOT EXISTS corpscout;`, has no `;` inside comments, ends with a statement, and has a `.down.sql` that drops the table. Every new migration name goes into `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`, in order, after `000381_corpscout_se_company_basic_info_precedence_rules`.
- Column names and order in `tables.py` equal the DDL exactly (`tests/se_company_ddl.py::declared_columns` is the judge). Every `company_id` column on the suggestion, normalized, main and rule tables carries `CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')`; the precedence table's is `CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')`; the history table has none.
- The new main table is named `se_company_address_v2` (the old final table keeps `se_company_address` until the cutover renames both).
- `NORMALIZER_VERSION = "se-address-normalizer-v1"`. Parse statuses are exactly `ok`, `partial`, `no_address`, `foreign`. Identity components, in order: `country_code, postal_code, city, street_name, box, house_number, unit, care_of`; `address_key` is `sha256("\n".join(components))` with NULL as `''`. Bolagsverket's packed string is five `$`-separated parts: street line, care-of name, town, postcode, country token (`SE-LAND` = Sweden; any other token's first two letters are the country code and make the row `foreign`).
- The normalizer never expands abbreviations, corrects spelling or guesses a house number; it only splits, folds and classifies. Dropped text goes into `parse_notes`.
- The normalize asset is `se_company_address_normalize`, group `se_company_address`, pool `se_company_address_normalize`, page size 20,000 companies, `changed_only` default true; `normalized_at` is one stamp per run; `normalized_id = sha256(f"{company_id}\n{source}\n{slot}\n{stamp}")` with `stamp` formatted `%Y-%m-%d %H:%M:%S.mmm` UTC.
- No `from __future__ import annotations` in any module that defines a `@dg.asset`.
- Commit by explicit path after every task; never `git add -A`. Trailers, contiguous at the end of every commit message: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` then `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`.

---

## File structure

- `corpscout/clickhouse/migrations/000382_corpscout_se_company_address_suggestion.{up,down}.sql` … `000387_corpscout_se_company_address_precedence.{up,down}.sql` — the six tables.
- `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/__init__.py` — package docstring pointing at the spec.
- `.../address/tables.py` — table names, qualified names, column tuples, the catalogues (sources, kinds, parse statuses).
- `.../address/normalize_se.py` — `RawAddress`, `NormalizedAddress`, `normalize_se_address`, `address_key`, `NORMALIZER_VERSION`. Pure, no Dagster, no ClickHouse.
- `.../address/normalize.py` — the country dispatcher, the SQL texts of the normalize scan and insert, the row shaping, `normalize_companies` / `normalize_all` batch functions against a ClickHouse client.
- `.../address/assets.py` — `GROUP_NAME`, `NORMALIZE_POOL`, `AddressNormalizeConfig`, the `se_company_address_normalize` asset.
- `.../address/docs/address-design.md` — module map, like `basic_info/docs/basic_info-design.md`.
- `corpscout/services/dagster_v3/tests/fixtures/se_addresses/golden.jsonl` — the golden corpus.
- `corpscout/services/dagster_v3/tests/test_se_company_address_tables.py`, `test_se_company_address_normalize_se.py`, `test_se_company_address_normalize.py`, `test_se_company_address_clickhouse_local.py`.

---

### Task 1: The six tables

**Files:**
- Create: the twelve migration files listed under Global Constraints.
- Create: `src/dagster_v3/defs/se_company/address/__init__.py`, `src/dagster_v3/defs/se_company/address/tables.py`
- Modify: `tests/test_clickhouse_migrations.py` (`EXPECTED_MIGRATIONS`, append six names after `000381_corpscout_se_company_basic_info_precedence_rules`)
- Test: `tests/test_se_company_address_tables.py`

**Interfaces:**
- Produces: `tables.DATABASE`, `SUGGESTION_TABLE`, `NORMALIZED_TABLE`, `MAIN_TABLE`, `HISTORY_TABLE`, `RULE_TABLE`, `PRECEDENCE_TABLE` and their `QUALIFIED_*` twins; `RAW_ADDRESS_COLUMNS`, `SUGGESTION_COLUMNS`, `COMPONENT_COLUMNS`, `NORMALIZED_COLUMNS`, `GEOCODE_COLUMNS`, `MAIN_COLUMNS`, `HISTORY_COLUMNS`, `RULE_COLUMNS`, `PRECEDENCE_COLUMNS`, `SOURCES`, `KINDS`, `PARSE_STATUSES` (Tasks 3 and 4 and slices 1 and 2 use them).

- [ ] **Step 1: Write the failing test**

`tests/test_se_company_address_tables.py`:

```python
"""The six address-entity tables (spec 2026-09-06 section 3), pinned against the migration
DDL through tests/se_company_ddl.py so tables.py and the deployed schema cannot drift."""

from dagster_v3.defs.se_company.address import tables
from tests.se_company_ddl import declared_columns, table_block

COMPANY_ID_CHECK = "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')"


def test_suggestion_table_is_one_current_row_per_company_source_and_slot() -> None:
    block = table_block("se_company_address_suggestion")
    assert declared_columns("se_company_address_suggestion") == list(tables.SUGGESTION_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(suggested_at)" in block
    assert "ORDER BY (company_id, source, slot)" in block
    assert COMPANY_ID_CHECK in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    replaces_key Nullable(FixedString(64))," in block
    for column in tables.RAW_ADDRESS_COLUMNS:
        assert f"    {column} Nullable(String)," in block, column
    assert "MATERIALIZED" not in block


def test_normalized_table_has_the_same_key_and_its_own_version() -> None:
    block = table_block("se_company_address_normalized")
    assert declared_columns("se_company_address_normalized") == list(tables.NORMALIZED_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(normalized_at)" in block
    assert "ORDER BY (company_id, source, slot)" in block
    assert COMPANY_ID_CHECK in block
    assert "    normalized_id FixedString(64)," in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    address_key FixedString(64)," in block
    for column in tables.COMPONENT_COLUMNS:
        assert f"    {column} Nullable(String)," in block, column
    assert "    country_code LowCardinality(String)," in block
    assert "    parse_status LowCardinality(String)," in block
    assert "    normalizer_version LowCardinality(String)," in block


def test_main_table_is_one_row_per_company_and_published_address() -> None:
    block = table_block("se_company_address_v2")
    assert declared_columns("se_company_address_v2") == list(tables.MAIN_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(folded_at)" in block
    assert "ORDER BY (company_id, address_key)" in block
    assert COMPANY_ID_CHECK in block
    assert "    normalized_ids Array(FixedString(64))," in block
    assert "    sources Array(LowCardinality(String))," in block
    assert "    active UInt8," in block
    assert "    geocode_policy LowCardinality(String)," in block
    assert "    geocoded_at Nullable(DateTime64(3, 'UTC'))," in block


def test_history_table_is_the_main_row_keyed_by_fold_time() -> None:
    block = table_block("se_company_address_history")
    assert declared_columns("se_company_address_history") == list(tables.HISTORY_COLUMNS)
    assert tables.HISTORY_COLUMNS == tables.MAIN_COLUMNS
    assert "CONSTRAINT valid_company_id" not in block
    assert "ENGINE = MergeTree" in block
    assert "ORDER BY (company_id, address_key, folded_at)" in block


def test_rule_table_is_a_per_company_decision_per_address() -> None:
    block = table_block("se_company_address_rule")
    assert declared_columns("se_company_address_rule") == list(tables.RULE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, address_key, action)" in block
    assert COMPANY_ID_CHECK in block
    assert "    removed UInt8 DEFAULT 0," in block


def test_precedence_table_has_the_basic_info_shape() -> None:
    block = table_block("se_company_address_precedence")
    assert declared_columns("se_company_address_precedence") == list(tables.PRECEDENCE_COLUMNS)
    assert tables.PRECEDENCE_COLUMNS == (
        "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
    )
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, field, source)" in block
    assert "CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')" in block


def test_column_tuples_agree_with_each_other() -> None:
    assert tables.QUALIFIED_SUGGESTION_TABLE == "corpscout.se_company_address_suggestion"
    assert tables.QUALIFIED_NORMALIZED_TABLE == "corpscout.se_company_address_normalized"
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_address_v2"
    assert tables.QUALIFIED_HISTORY_TABLE == "corpscout.se_company_address_history"
    assert tables.QUALIFIED_RULE_TABLE == "corpscout.se_company_address_rule"
    assert tables.QUALIFIED_PRECEDENCE_TABLE == "corpscout.se_company_address_precedence"
    assert tables.SOURCES == ("scb", "bolagsverket", "ratsit", "reviewer", "reviewer_draft")
    assert tables.PARSE_STATUSES == ("ok", "partial", "no_address", "foreign")
    assert tables.KINDS == ("postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown")
    for column in tables.COMPONENT_COLUMNS:
        assert column in tables.NORMALIZED_COLUMNS and column in tables.MAIN_COLUMNS
    for column in tables.GEOCODE_COLUMNS:
        assert column in tables.MAIN_COLUMNS
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_tables.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.address'`.

- [ ] **Step 3: Write the six migrations**

`corpscout/clickhouse/migrations/000382_corpscout_se_company_address_suggestion.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Raw address suggestions (spec 2026-09-06 section 3.1): what each source delivered, one
-- current row per company, source and slot, never normalized. A source that stops
-- delivering an address for a slot writes a row with every address column NULL.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_suggestion
(
    company_id String,
    source LowCardinality(String),
    slot String,
    suggestion_id FixedString(64),
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    kind LowCardinality(String),
    raw_address Nullable(String),
    care_of Nullable(String),
    street_address Nullable(String),
    postal_code Nullable(String),
    post_town Nullable(String),
    county Nullable(String),
    country_code Nullable(String),
    decided_by Nullable(String),
    note Nullable(String),
    replaces_key Nullable(FixedString(64)),
    suggested_at DateTime64(3, 'UTC'),
    source_run_id String,
    extractor_version LowCardinality(String),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source, slot);
```

`000382_corpscout_se_company_address_suggestion.down.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_company_address_suggestion;
```

`000383_corpscout_se_company_address_normalized.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Normalized address suggestions (spec section 3.2): the same key and row count as the raw
-- table, written only by the normalize asset. normalized_id names this version and
-- suggestion_id the raw version it was computed from.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_normalized
(
    company_id String,
    source LowCardinality(String),
    slot String,
    normalized_id FixedString(64),
    suggestion_id FixedString(64),
    suggested_at DateTime64(3, 'UTC'),
    kind LowCardinality(String),
    care_of Nullable(String),
    box Nullable(String),
    street_name Nullable(String),
    house_number Nullable(String),
    unit Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code LowCardinality(String),
    normalized_address String,
    address_key FixedString(64),
    parse_status LowCardinality(String),
    parse_notes String,
    normalizer_version LowCardinality(String),
    normalized_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, source, slot);
```

`000383_...down.sql`: same two statements as 000382's down with `se_company_address_normalized`.

`000384_corpscout_se_company_address_v2.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Published addresses (spec section 3.3): one row per company and published address,
-- written by the fold. Built as se_company_address_v2 because the old final table keeps
-- the name se_company_address until the cutover renames both.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_v2
(
    company_id String,
    address_key FixedString(64),
    care_of Nullable(String),
    box Nullable(String),
    street_name Nullable(String),
    house_number Nullable(String),
    unit Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code LowCardinality(String),
    normalized_address String,
    kinds Array(LowCardinality(String)),
    sources Array(LowCardinality(String)),
    slots Array(String),
    normalized_ids Array(FixedString(64)),
    text_source LowCardinality(String),
    active UInt8,
    inactive_reason LowCardinality(String),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_status LowCardinality(String),
    geocode_method LowCardinality(String),
    geocode_confidence Nullable(Float64),
    geocode_precision LowCardinality(String),
    geocode_policy LowCardinality(String),
    geocode_reference String,
    geocoded_at Nullable(DateTime64(3, 'UTC')),
    normalizer_version LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(folded_at)
ORDER BY (company_id, address_key);
```

`000385_corpscout_se_company_address_history.up.sql`: the same column list as 000384 without the CONSTRAINT line, with this header comment and engine:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Address history (spec section 3.4): the published row appended whenever anything but its
-- geocode block changes. Append-only, no company_id constraint (the main table enforces it).
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_history
(
    company_id String,
    address_key FixedString(64),
    ... the 29 columns of 000384 after address_key, verbatim, ending with
    source_run_id String
)
ENGINE = MergeTree
ORDER BY (company_id, address_key, folded_at);
```

(Copy the column lines from 000384 exactly; the `...` above is only this plan's abbreviation, the file lists every column.)

`000386_corpscout_se_company_address_rule.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Per-company address decisions (spec section 3.5): a hide rule keyed by the published
-- address, released by a later version with removed = 1.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_rule
(
    company_id String,
    address_key FixedString(64),
    action LowCardinality(String),
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, address_key, action);
```

`000387_corpscout_se_company_address_precedence.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Address source precedence (spec section 3.6): the basic-info shape. company_id '' rows
-- are the global order exported from code, a spelling tie-break only, never a filter.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_precedence
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
```

Every `.down.sql` is `CREATE DATABASE IF NOT EXISTS corpscout;` then `DROP TABLE IF EXISTS corpscout.<table>;`.

- [ ] **Step 4: Write `tables.py` and the package init**

`src/dagster_v3/defs/se_company/address/__init__.py`:

```python
"""The SE company address entity on the basic-info shape.

Spec: docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md. Raw suggestions
(what sources deliver) are normalized into a stored layer with its own version, folded per
company into published addresses with in-page geocoding, and appended to history on change.
"""
```

`src/dagster_v3/defs/se_company/address/tables.py`:

```python
"""Table names and column tuples of the address entity, pinned against migrations 000382-000387."""

DATABASE = "corpscout"
SUGGESTION_TABLE = "se_company_address_suggestion"
NORMALIZED_TABLE = "se_company_address_normalized"
MAIN_TABLE = "se_company_address_v2"
HISTORY_TABLE = "se_company_address_history"
RULE_TABLE = "se_company_address_rule"
PRECEDENCE_TABLE = "se_company_address_precedence"

QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"
QUALIFIED_NORMALIZED_TABLE = f"{DATABASE}.{NORMALIZED_TABLE}"
QUALIFIED_MAIN_TABLE = f"{DATABASE}.{MAIN_TABLE}"
QUALIFIED_HISTORY_TABLE = f"{DATABASE}.{HISTORY_TABLE}"
QUALIFIED_RULE_TABLE = f"{DATABASE}.{RULE_TABLE}"
QUALIFIED_PRECEDENCE_TABLE = f"{DATABASE}.{PRECEDENCE_TABLE}"

SOURCES: tuple[str, ...] = ("scb", "bolagsverket", "ratsit", "reviewer", "reviewer_draft")
KINDS: tuple[str, ...] = ("postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown")
PARSE_STATUSES: tuple[str, ...] = ("ok", "partial", "no_address", "foreign")

RAW_ADDRESS_COLUMNS: tuple[str, ...] = (
    "raw_address", "care_of", "street_address", "postal_code", "post_town", "county", "country_code",
)
SUGGESTION_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "source_record_uid", "observed_at", "kind",
    *RAW_ADDRESS_COLUMNS,
    "decided_by", "note", "replaces_key", "suggested_at", "source_run_id", "extractor_version",
)
COMPONENT_COLUMNS: tuple[str, ...] = (
    "care_of", "box", "street_name", "house_number", "unit", "postal_code", "city",
)
NORMALIZED_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "normalized_id", "suggestion_id", "suggested_at", "kind",
    *COMPONENT_COLUMNS,
    "country_code", "normalized_address", "address_key", "parse_status", "parse_notes",
    "normalizer_version", "normalized_at",
)
GEOCODE_COLUMNS: tuple[str, ...] = (
    "latitude", "longitude", "geocode_status", "geocode_method", "geocode_confidence",
    "geocode_precision", "geocode_policy", "geocode_reference", "geocoded_at",
)
MAIN_COLUMNS: tuple[str, ...] = (
    "company_id", "address_key",
    *COMPONENT_COLUMNS,
    "country_code", "normalized_address", "kinds", "sources", "slots", "normalized_ids",
    "text_source", "active", "inactive_reason",
    *GEOCODE_COLUMNS,
    "normalizer_version", "folded_at", "fold_version", "source_run_id",
)
HISTORY_COLUMNS: tuple[str, ...] = MAIN_COLUMNS
RULE_COLUMNS: tuple[str, ...] = (
    "company_id", "address_key", "action", "removed", "decided_by", "note", "decided_at",
)
PRECEDENCE_COLUMNS: tuple[str, ...] = (
    "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
)
```

Then append to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`, right after `"000381_corpscout_se_company_basic_info_precedence_rules",`:

```python
    "000382_corpscout_se_company_address_suggestion",
    "000383_corpscout_se_company_address_normalized",
    "000384_corpscout_se_company_address_v2",
    "000385_corpscout_se_company_address_history",
    "000386_corpscout_se_company_address_rule",
    "000387_corpscout_se_company_address_precedence",
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_tables.py tests/test_clickhouse_migrations.py -q`
Expected: all PASS (the migrations test also checks every up has a down and the ledger is contiguous).

- [ ] **Step 6: Commit**

```bash
git add corpscout/clickhouse/migrations/00038[2-7]_corpscout_se_company_address_*.sql \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/__init__.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/tables.py \
  corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
  corpscout/services/dagster_v3/tests/test_se_company_address_tables.py
git commit -m "feat(clickhouse): SE company address entity tables (000382-000387)"
```

---

### Task 2: The Swedish normalizer and its golden corpus

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/normalize_se.py`
- Create: `tests/fixtures/se_addresses/golden.jsonl`
- Test: `tests/test_se_company_address_normalize_se.py`

**Interfaces:**
- Produces: `RawAddress(raw_address, care_of, street_address, postal_code, post_town, county, country_code)` (all `str | None`, default `None`); `NormalizedAddress(care_of, box, street_name, house_number, unit, postal_code, city, country_code, normalized_address, parse_status, parse_notes)`; `normalize_se_address(raw: RawAddress) -> NormalizedAddress`; `identity_components(n: NormalizedAddress) -> tuple[str, ...]`; `address_key(n: NormalizedAddress) -> str` (64 hex chars); `NORMALIZER_VERSION`.

The corpus is real register rows (34, sampled from prod on 2026-09-06 across the three sources) plus 13 synthetic cases for patterns the sample missed; every expected value was produced by this exact implementation and reviewed by hand. The file format is one JSON object per line: `{"source": ..., "raw": {<RawAddress fields that are not None>}, "expected": {<every NormalizedAddress field>}}`.

- [ ] **Step 1: Write the failing test**

`tests/test_se_company_address_normalize_se.py`:

```python
"""The Swedish address normalizer (spec section 4): a golden corpus of real register rows plus
synthetic edge cases, and the identity key it feeds."""

import json
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.address.normalize_se import (
    NORMALIZER_VERSION,
    NormalizedAddress,
    RawAddress,
    address_key,
    identity_components,
    normalize_se_address,
)

CORPUS = Path(__file__).resolve().parent / "fixtures" / "se_addresses" / "golden.jsonl"


def corpus() -> list[dict]:
    return [json.loads(line) for line in CORPUS.read_text(encoding="utf-8").splitlines() if line.strip()]


CASES = corpus()


@pytest.mark.parametrize("case", CASES, ids=[f"{c['source']}:{json.dumps(c['raw'], ensure_ascii=False)[:60]}" for c in CASES])
def test_golden_corpus(case: dict) -> None:
    result = normalize_se_address(RawAddress(**case["raw"]))
    expected = case["expected"]
    for field_name, value in expected.items():
        assert getattr(result, field_name) == value, field_name


def test_the_corpus_covers_every_status_and_every_source() -> None:
    statuses = {c["expected"]["parse_status"] for c in CASES}
    assert statuses == {"ok", "partial", "no_address", "foreign"}
    assert {c["source"] for c in CASES} >= {"scb", "bolagsverket", "ratsit", "reviewer"}
    assert len(CASES) >= 40


def test_identity_is_the_eight_components_in_spec_order() -> None:
    n = normalize_se_address(RawAddress(care_of="c/o Anna Svensson", street_address="Kungsgatan 4 A, 3 tr",
                                        postal_code="11143", post_town="Stockholm", country_code="SE"))
    assert identity_components(n) == ("SE", "11143", "stockholm", "kungsgatan", "", "4A", "3 tr", "anna svensson")
    assert address_key(n) == address_key(normalize_se_address(RawAddress(
        care_of="C/O ANNA SVENSSON", street_address="KUNGSGATAN 4A 3 TR", postal_code="111 43", post_town="STOCKHOLM")))
    assert len(address_key(n)) == 64


def test_no_address_rows_share_the_empty_identity_key() -> None:
    a = normalize_se_address(RawAddress())
    b = normalize_se_address(RawAddress(street_address="Okänd adress", postal_code="00000", post_town="OKÄND"))
    assert a.parse_status == b.parse_status == "no_address"
    assert address_key(a) == address_key(b)


def test_version_constant() -> None:
    assert NORMALIZER_VERSION == "se-address-normalizer-v1"
    assert NormalizedAddress.__slots__  # frozen dataclass with slots, hashable inputs to the fold
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_normalize_se.py -q`
Expected: FAIL with `ModuleNotFoundError` (or `FileNotFoundError` for the corpus).

- [ ] **Step 3: Write the golden corpus**

Create `tests/fixtures/se_addresses/golden.jsonl` with exactly these lines (47):

```jsonl
{"source": "bolagsverket", "raw": {"raw_address": "Box 292$Svenska Standardbolag AB$FALUN$79127$SE-LAND"}, "expected": {"care_of": "svenska standardbolag ab", "box": "292", "street_name": null, "house_number": null, "unit": null, "postal_code": "79127", "city": "falun", "country_code": "SE", "normalized_address": "c/o Svenska Standardbolag AB, Box 292, 791 27 Falun", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Box 1067$c/o Bolagspartner avveckling$LUND$22104$SE-LAND"}, "expected": {"care_of": "bolagspartner avveckling", "box": "1067", "street_name": null, "house_number": null, "unit": null, "postal_code": "22104", "city": "lund", "country_code": "SE", "normalized_address": "c/o Bolagspartner avveckling, Box 1067, 221 04 Lund", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Våxtorpsgränd 26 lgh 1106$c/o Ali Hussien$ÄLVSJÖ$12573$SE-LAND"}, "expected": {"care_of": "ali hussien", "box": null, "street_name": "våxtorpsgränd", "house_number": "26", "unit": "lgh 1106", "postal_code": "12573", "city": "älvsjö", "country_code": "SE", "normalized_address": "c/o Ali Hussien, Våxtorpsgränd 26 lgh 1106, 125 73 Älvsjö", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Råsundavägen 4$RADPOINT AB$SOLNA$16967$SE-LAND"}, "expected": {"care_of": "radpoint ab", "box": null, "street_name": "råsundavägen", "house_number": "4", "unit": null, "postal_code": "16967", "city": "solna", "country_code": "SE", "normalized_address": "c/o Radpoint AB, Råsundavägen 4, 169 67 Solna", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Storholmsbackarna 24 lgh 1102$Mohammed Said$SKÄRHOLMEN$12743$SE-LAND"}, "expected": {"care_of": "mohammed said", "box": null, "street_name": "storholmsbackarna", "house_number": "24", "unit": "lgh 1102", "postal_code": "12743", "city": "skärholmen", "country_code": "SE", "normalized_address": "c/o Mohammed Said, Storholmsbackarna 24 lgh 1102, 127 43 Skärholmen", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Ingenjör Bååths gata 11, T2$c/o ICA Fastigheter Sverige AB$VÄSTERÅS$72184$SE-LAND"}, "expected": {"care_of": "ica fastigheter sverige ab", "box": null, "street_name": "ingenjör bååths gata", "house_number": "11", "unit": "t2", "postal_code": "72184", "city": "västerås", "country_code": "SE", "normalized_address": "c/o ICA Fastigheter Sverige AB, Ingenjör Bååths gata 11 t2, 721 84 Västerås", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Honungsgatan 13b$$UPPSALA$75254$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "honungsgatan", "house_number": "13B", "unit": null, "postal_code": "75254", "city": "uppsala", "country_code": "SE", "normalized_address": "Honungsgatan 13B, 752 54 Uppsala", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Vretstorp-Johannesdal$$VINGÅKER$64395$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "vretstorp-johannesdal", "house_number": null, "unit": null, "postal_code": "64395", "city": "vingåker", "country_code": "SE", "normalized_address": "Vretstorp-Johannesdal, 643 95 Vingåker", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "$$ADRESS SAKNAS$99999$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": null, "house_number": null, "unit": null, "postal_code": null, "city": null, "country_code": "SE", "normalized_address": "", "parse_status": "no_address", "parse_notes": "source marks the address as unknown"}}
{"source": "bolagsverket", "raw": {"raw_address": "Villa Granvik, 1851 Norrsund$$BLIDÖ$76017$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "villa granvik", "house_number": "1851", "unit": null, "postal_code": "76017", "city": "blidö", "country_code": "SE", "normalized_address": "Villa Granvik 1851, 760 17 Blidö", "parse_status": "ok", "parse_notes": "dropped trailing text 'norrsund'"}}
{"source": "bolagsverket", "raw": {"raw_address": "Lindöhällsvägen 72$Malmberg Ekbom, Anne-Christine$NORRKÖPING$60365$SE-LAND"}, "expected": {"care_of": "malmberg ekbom, anne-christine", "box": null, "street_name": "lindöhällsvägen", "house_number": "72", "unit": null, "postal_code": "60365", "city": "norrköping", "country_code": "SE", "normalized_address": "c/o Malmberg Ekbom, Anne-Christine, Lindöhällsvägen 72, 603 65 Norrköping", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Frillesåsvägen 83$$FRILLESÅS$43962$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "frillesåsvägen", "house_number": "83", "unit": null, "postal_code": "43962", "city": "frillesås", "country_code": "SE", "normalized_address": "Frillesåsvägen 83, 439 62 Frillesås", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"street_address": "BOX 13", "postal_code": "43905", "post_town": "ÅSA"}, "expected": {"care_of": null, "box": "13", "street_name": null, "house_number": null, "unit": null, "postal_code": "43905", "city": "åsa", "country_code": "SE", "normalized_address": "Box 13, 439 05 Åsa", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"care_of": "KRISTIN JOHANSSON", "street_address": "GRANVÄGEN 5", "postal_code": "26261", "post_town": "ÄNGELHOLM"}, "expected": {"care_of": "kristin johansson", "box": null, "street_name": "granvägen", "house_number": "5", "unit": null, "postal_code": "26261", "city": "ängelholm", "country_code": "SE", "normalized_address": "c/o Kristin Johansson, Granvägen 5, 262 61 Ängelholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"care_of": "PETER HERMELIN", "street_address": "RIDDARGATAN 38", "postal_code": "11457", "post_town": "STOCKHOLM"}, "expected": {"care_of": "peter hermelin", "box": null, "street_name": "riddargatan", "house_number": "38", "unit": null, "postal_code": "11457", "city": "stockholm", "country_code": "SE", "normalized_address": "c/o Peter Hermelin, Riddargatan 38, 114 57 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"care_of": "MATTIAS ENGBERG", "street_address": "GÄVLEGATAN 32", "postal_code": "11365", "post_town": "STOCKHOLM"}, "expected": {"care_of": "mattias engberg", "box": null, "street_name": "gävlegatan", "house_number": "32", "unit": null, "postal_code": "11365", "city": "stockholm", "country_code": "SE", "normalized_address": "c/o Mattias Engberg, Gävlegatan 32, 113 65 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"care_of": "ICA FASTIGHETER SVERIGE AB", "street_address": "INGENJÖR BÅÅTHS GATA 11  T2", "postal_code": "72184", "post_town": "VÄSTERÅS"}, "expected": {"care_of": "ica fastigheter sverige ab", "box": null, "street_name": "ingenjör bååths gata", "house_number": "11", "unit": "t2", "postal_code": "72184", "city": "västerås", "country_code": "SE", "normalized_address": "c/o Ica Fastigheter Sverige AB, Ingenjör Bååths Gata 11 t2, 721 84 Västerås", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"street_address": "LASARETTSGATAN 6  LGH 1202", "postal_code": "58225", "post_town": "LINKÖPING"}, "expected": {"care_of": null, "box": null, "street_name": "lasarettsgatan", "house_number": "6", "unit": "lgh 1202", "postal_code": "58225", "city": "linköping", "country_code": "SE", "normalized_address": "Lasarettsgatan 6 lgh 1202, 582 25 Linköping", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"street_address": "BASTUGATAN 40 A LGH 1101", "postal_code": "11825", "post_town": "STOCKHOLM"}, "expected": {"care_of": null, "box": null, "street_name": "bastugatan", "house_number": "40A", "unit": "lgh 1101", "postal_code": "11825", "city": "stockholm", "country_code": "SE", "normalized_address": "Bastugatan 40A lgh 1101, 118 25 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"street_address": "ERIKVÄDERHATTSGATA", "postal_code": "41832", "post_town": "GÖTEBORG"}, "expected": {"care_of": null, "box": null, "street_name": "erikväderhattsgata", "house_number": null, "unit": null, "postal_code": "41832", "city": "göteborg", "country_code": "SE", "normalized_address": "Erikväderhattsgata, 418 32 Göteborg", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"street_address": "MELIORACIJAS 21-3 OZOLNIEKI LV-3018", "postal_code": "00000", "post_town": "UTLANDET"}, "expected": {"care_of": null, "box": null, "street_name": null, "house_number": null, "unit": null, "postal_code": null, "city": null, "country_code": "", "normalized_address": "", "parse_status": "foreign", "parse_notes": "post town marks the address as foreign"}}
{"source": "scb", "raw": {"street_address": "Okänd adress", "postal_code": "00000", "post_town": "OKÄND"}, "expected": {"care_of": null, "box": null, "street_name": null, "house_number": null, "unit": null, "postal_code": null, "city": null, "country_code": "SE", "normalized_address": "", "parse_status": "no_address", "parse_notes": "source marks the address as unknown"}}
{"source": "scb", "raw": {"care_of": "FERAS KALLA FUTURE AB /", "street_address": "FOLKESTALEDEN 7  RETUNA MEDICAPLASTIC", "postal_code": "63510", "post_town": "ESKILSTUNA"}, "expected": {"care_of": "feras kalla future ab", "box": null, "street_name": "folkestaleden", "house_number": "7", "unit": null, "postal_code": "63510", "city": "eskilstuna", "country_code": "SE", "normalized_address": "c/o Feras Kalla Future AB, Folkestaleden 7, 635 10 Eskilstuna", "parse_status": "ok", "parse_notes": "dropped trailing text 'retuna medicaplastic'"}}
{"source": "scb", "raw": {"care_of": "TORBJÖRN SÖDERSTRÖM", "street_address": "BAROMETERGATAN 22", "postal_code": "21117", "post_town": "MALMÖ"}, "expected": {"care_of": "torbjörn söderström", "box": null, "street_name": "barometergatan", "house_number": "22", "unit": null, "postal_code": "21117", "city": "malmö", "country_code": "SE", "normalized_address": "c/o Torbjörn Söderström, Barometergatan 22, 211 17 Malmö", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "Box 716", "postal_code": "39127", "post_town": "Kalmar", "county": "Kalmar län"}, "expected": {"care_of": null, "box": "716", "street_name": null, "house_number": null, "unit": null, "postal_code": "39127", "city": "kalmar", "country_code": "SE", "normalized_address": "Box 716, 391 27 Kalmar", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "c/o Fredrik Hökerberg Sandholm Brahegatan 17", "postal_code": "11437", "post_town": "Stockholm", "county": "Stockholms län"}, "expected": {"care_of": "fredrik hökerberg sandholm", "box": null, "street_name": "brahegatan", "house_number": "17", "unit": null, "postal_code": "11437", "city": "stockholm", "country_code": "SE", "normalized_address": "c/o Fredrik Hökerberg Sandholm, Brahegatan 17, 114 37 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "c/o Daniel Zetterman Ångloksgatan 45", "postal_code": "72233", "post_town": "Västerås", "county": "Västmanlands län"}, "expected": {"care_of": "daniel zetterman", "box": null, "street_name": "ångloksgatan", "house_number": "45", "unit": null, "postal_code": "72233", "city": "västerås", "country_code": "SE", "normalized_address": "c/o Daniel Zetterman, Ångloksgatan 45, 722 33 Västerås", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "c/o Kontoret Stockholmsvägen 104", "postal_code": "18730", "post_town": "Täby", "county": "Stockholms län"}, "expected": {"care_of": "kontoret", "box": null, "street_name": "stockholmsvägen", "house_number": "104", "unit": null, "postal_code": "18730", "city": "täby", "country_code": "SE", "normalized_address": "c/o Kontoret, Stockholmsvägen 104, 187 30 Täby", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "Vidingsjögatan 17, lgh 1101", "postal_code": "58957", "post_town": "Linköping", "county": "Östergötlands län"}, "expected": {"care_of": null, "box": null, "street_name": "vidingsjögatan", "house_number": "17", "unit": "lgh 1101", "postal_code": "58957", "city": "linköping", "country_code": "SE", "normalized_address": "Vidingsjögatan 17 lgh 1101, 589 57 Linköping", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "Tjustgatan 11 lgh 1302", "postal_code": "60218", "post_town": "Norrköping", "county": "Östergötlands län"}, "expected": {"care_of": null, "box": null, "street_name": "tjustgatan", "house_number": "11", "unit": "lgh 1302", "postal_code": "60218", "city": "norrköping", "country_code": "SE", "normalized_address": "Tjustgatan 11 lgh 1302, 602 18 Norrköping", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "c/o Behdad Bazargani Eklundavägen 1A", "postal_code": "75655", "post_town": "Uppsala", "county": "Uppsala län"}, "expected": {"care_of": "behdad bazargani", "box": null, "street_name": "eklundavägen", "house_number": "1A", "unit": null, "postal_code": "75655", "city": "uppsala", "country_code": "SE", "normalized_address": "c/o Behdad Bazargani, Eklundavägen 1A, 756 55 Uppsala", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {}, "expected": {"care_of": null, "box": null, "street_name": null, "house_number": null, "unit": null, "postal_code": null, "city": null, "country_code": "SE", "normalized_address": "", "parse_status": "no_address", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "c/o Mays Inredning Stora Torget 6", "postal_code": "76133", "post_town": "Norrtälje", "county": "Stockholms län"}, "expected": {"care_of": "mays inredning", "box": null, "street_name": "stora torget", "house_number": "6", "unit": null, "postal_code": "76133", "city": "norrtälje", "country_code": "SE", "normalized_address": "c/o Mays Inredning, Stora Torget 6, 761 33 Norrtälje", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "Sjöviksvägen 70", "postal_code": "11359", "post_town": "Stockholm", "county": "Stockholms län"}, "expected": {"care_of": null, "box": null, "street_name": "sjöviksvägen", "house_number": "70", "unit": null, "postal_code": "11359", "city": "stockholm", "country_code": "SE", "normalized_address": "Sjöviksvägen 70, 113 59 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "c/o Martin Berglund Strombergs Väg 132, lgh 1303", "postal_code": "90728", "post_town": "Umeå", "county": "Västerbottens län"}, "expected": {"care_of": "martin berglund", "box": null, "street_name": "strombergs väg", "house_number": "132", "unit": "lgh 1303", "postal_code": "90728", "city": "umeå", "country_code": "SE", "normalized_address": "c/o Martin Berglund, Strombergs Väg 132 lgh 1303, 907 28 Umeå", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "c/o Alexander Hagberg Pilvägen19", "postal_code": "18157", "post_town": "Lidingö", "county": "Stockholms län"}, "expected": {"care_of": "alexander hagberg", "box": null, "street_name": "pilvägen", "house_number": "19", "unit": null, "postal_code": "18157", "city": "lidingö", "country_code": "SE", "normalized_address": "c/o Alexander Hagberg, Pilvägen 19, 181 57 Lidingö", "parse_status": "ok", "parse_notes": ""}}
{"source": "ratsit", "raw": {"street_address": "c/o Ann-Kathrine Aldberg Ringstavägen 1A 1tr", "postal_code": "84573", "post_town": "Berg", "county": "Jämtlands län"}, "expected": {"care_of": "ann-kathrine aldberg", "box": null, "street_name": "ringstavägen", "house_number": "1A", "unit": "1tr", "postal_code": "84573", "city": "berg", "country_code": "SE", "normalized_address": "c/o Ann-Kathrine Aldberg, Ringstavägen 1A 1tr, 845 73 Berg", "parse_status": "ok", "parse_notes": ""}}
{"source": "scb", "raw": {"street_address": "STORGATAN 5", "postal_code": "11122"}, "expected": {"care_of": null, "box": null, "street_name": "storgatan", "house_number": "5", "unit": null, "postal_code": "11122", "city": null, "country_code": "SE", "normalized_address": "Storgatan 5, 111 22", "parse_status": "partial", "parse_notes": "missing city"}}
{"source": "scb", "raw": {"care_of": "SEB, STIFTELSER & FÖRETAG", "postal_code": "10640", "post_town": "STOCKHOLM"}, "expected": {"care_of": "seb, stiftelser & företag", "box": null, "street_name": null, "house_number": null, "unit": null, "postal_code": "10640", "city": "stockholm", "country_code": "SE", "normalized_address": "", "parse_status": "no_address", "parse_notes": ""}}
{"source": "reviewer", "raw": {"street_address": "Storgatan 5-7", "postal_code": "111 22", "post_town": "Stockholm", "country_code": "SE"}, "expected": {"care_of": null, "box": null, "street_name": "storgatan", "house_number": "5-7", "unit": null, "postal_code": "11122", "city": "stockholm", "country_code": "SE", "normalized_address": "Storgatan 5-7, 111 22 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "reviewer", "raw": {"care_of": "c/o Anna Svensson", "street_address": "Kungsgatan 4 A, 3 tr", "postal_code": "11143", "post_town": "Stockholm", "country_code": "SE"}, "expected": {"care_of": "anna svensson", "box": null, "street_name": "kungsgatan", "house_number": "4A", "unit": "3 tr", "postal_code": "11143", "city": "stockholm", "country_code": "SE", "normalized_address": "c/o Anna Svensson, Kungsgatan 4A 3 tr, 111 43 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "reviewer", "raw": {"street_address": "Box 5305", "postal_code": "10247", "post_town": "Stockholm", "country_code": "SE"}, "expected": {"care_of": null, "box": "5305", "street_name": null, "house_number": null, "unit": null, "postal_code": "10247", "city": "stockholm", "country_code": "SE", "normalized_address": "Box 5305, 102 47 Stockholm", "parse_status": "ok", "parse_notes": ""}}
{"source": "reviewer", "raw": {"street_address": "Hauptstrasse 1", "postal_code": "10115", "post_town": "Berlin", "country_code": "DE"}, "expected": {"care_of": null, "box": null, "street_name": null, "house_number": null, "unit": null, "postal_code": null, "city": null, "country_code": "DE", "normalized_address": "", "parse_status": "foreign", "parse_notes": "country DE"}}
{"source": "bolagsverket", "raw": {"raw_address": "PL 1702$$BERGVIK$82023$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "pl", "house_number": "1702", "unit": null, "postal_code": "82023", "city": "bergvik", "country_code": "SE", "normalized_address": "Pl 1702, 820 23 Bergvik", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Böstofta 16:1$$ESLÖV$24194$SE-LAND"}, "expected": {"care_of": null, "box": null, "street_name": "böstofta 16:1", "house_number": null, "unit": null, "postal_code": "24194", "city": "eslöv", "country_code": "SE", "normalized_address": "Böstofta 16:1, 241 94 Eslöv", "parse_status": "ok", "parse_notes": ""}}
{"source": "bolagsverket", "raw": {"raw_address": "Storgatan 1$$LONDON$SW1A$GB-LAND"}, "expected": {"care_of": null, "box": null, "street_name": null, "house_number": null, "unit": null, "postal_code": null, "city": null, "country_code": "GB", "normalized_address": "", "parse_status": "foreign", "parse_notes": "country GB"}}
{"source": "scb", "raw": {"street_address": "KUNGSGATAN 10", "postal_code": "1234", "post_town": "STOCKHOLM"}, "expected": {"care_of": null, "box": null, "street_name": "kungsgatan", "house_number": "10", "unit": null, "postal_code": null, "city": "stockholm", "country_code": "SE", "normalized_address": "Kungsgatan 10, Stockholm", "parse_status": "partial", "parse_notes": "postcode '1234' is not a valid five-digit code; missing postcode"}}
```

- [ ] **Step 4: Write the normalizer**

`src/dagster_v3/defs/se_company/address/normalize_se.py`, exactly:

```python
"""The Swedish address normalizer (spec 2026-09-06 section 4).

Pure: raw fields in, folded components, a display line, an identity key and a parse status
out. It splits, folds and classifies; it never expands abbreviations, corrects spelling or
guesses a house number (that is the geocoder's job). Every behaviour change bumps
NORMALIZER_VERSION, which is what re-normalizes stored rows.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass

NORMALIZER_VERSION = "se-address-normalizer-v1"

_UNKNOWN_TOWNS = {"okänd", "okand", "adress saknas"}
_FOREIGN_TOWNS = {"utlandet"}
_UNKNOWN_STREETS = {"okänd adress", "adress okänd", "okand adress", "adress saknas"}
_INVALID_POSTCODES = {"00000", "99999"}
_CARE_OF_PREFIX = re.compile(r"^(?:c/o|c\.o\.|co|att|attn|att:)\s+", re.IGNORECASE)
_BOX = re.compile(r"^(?:box|postbox|p\.?\s?o\.?\s?box)\s+(?P<box>[0-9]+[a-zåäö]?)$", re.IGNORECASE)
_NUMBER = re.compile(
    r"^(?P<name>.*?\S)\s+(?P<number>[0-9]+(?:\s?-\s?[0-9]+)?(?:\s?[a-zåäö])?)(?:[\s,.]+(?P<rest>\S.*))?$",
    re.IGNORECASE,
)
_UNIT = re.compile(
    r"^(?:lgh\s*[0-9]+|[0-9]+\s*tr\.?|tr\s*[0-9]+|bv|nb|t[0-9]+|[0-9]+\s*(?:vån|van)\.?|vån\s*[0-9]+|uppg\.?\s*[a-z0-9]+|ing\.?\s*[a-z0-9]+)$",
    re.IGNORECASE,
)
_GLUED_NUMBER = re.compile(r"^(?P<word>[a-zåäöé]+)(?P<number>[0-9]+[a-zåäö]?)$")
_STREET_PREFIXES = {"stora", "lilla", "norra", "södra", "östra", "västra", "gamla", "nya", "övre", "nedre", "sankt", "s:t", "st", "s:ta"}
_STREET_GENERICS = {"gata", "gatan", "väg", "vägen", "torg", "torget", "plan", "gränd", "gränden", "allé", "allén", "stig", "stigen", "led", "leden", "backe", "backen", "ring", "ringen", "park", "parken", "hamn", "hamnen", "kaj", "kajen", "bro", "bron", "esplanad", "esplanaden", "boulevard", "promenad", "promenaden", "gård", "gården"}
_KEEP_UPPER = {"ab", "hb", "kb", "ek", "ab:s"}


@dataclass(frozen=True, slots=True)
class RawAddress:
    raw_address: str | None = None
    care_of: str | None = None
    street_address: str | None = None
    postal_code: str | None = None
    post_town: str | None = None
    county: str | None = None
    country_code: str | None = None


@dataclass(frozen=True, slots=True)
class NormalizedAddress:
    care_of: str | None
    box: str | None
    street_name: str | None
    house_number: str | None
    unit: str | None
    postal_code: str | None
    city: str | None
    country_code: str
    normalized_address: str
    parse_status: str
    parse_notes: str


def _clean(value: str | None) -> str:
    """NFKC, whitespace collapse, trailing punctuation dropped; case kept."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", value)
    text = re.sub(r"[\s ]+", " ", text).strip(" .,;:-/")
    return text


def _fold(value: str | None) -> str:
    return _clean(value).casefold()


def _display(value: str) -> str:
    """Delivered casing, except an all-caps text is title-cased (company suffixes kept upper)."""
    text = _clean(value)
    if text != text.upper() and text != text.lower():
        return text
    words = []
    for word in text.casefold().split(" "):
        if word in _KEEP_UPPER:
            words.append(word.upper())
        else:
            words.append("-".join(p[:1].upper() + p[1:] for p in word.split("-")))
    return " ".join(words)


def _split_packed(raw: str) -> RawAddress:
    parts = [p.strip() for p in raw.split("$")]
    parts += [""] * (5 - len(parts))
    line1, line2, town, code, country = parts[:5]
    return RawAddress(
        care_of=line2 or None,
        street_address=line1 or None,
        postal_code=code or None,
        post_town=town or None,
        country_code=(country.upper()[:2] if country else None),
    )


def _split_street(line: str, notes: list[str]) -> tuple[str | None, str | None, str | None, str | None]:
    """-> (box, street_name, house_number, unit) from a folded street line."""
    box = _BOX.match(line)
    if box:
        return box.group("box"), None, None, None
    line = re.sub(r"\s*,\s*", " ", line)
    m = _NUMBER.match(line)
    if not m:
        glued = _GLUED_NUMBER.match(line)
        if glued:
            return None, glued.group("word"), glued.group("number"), None
        return None, line or None, None, None
    name = m.group("name")
    number = re.sub(r"\s+", "", m.group("number"))
    rest = (m.group("rest") or "").strip(" .,")
    unit = None
    if rest:
        if _UNIT.match(rest):
            unit = re.sub(r"\s+", " ", rest)
        else:
            notes.append(f"dropped trailing text '{rest}'")
    return None, name, number, unit


def _split_care_of_street(line: str, notes: list[str]) -> tuple[str | None, str]:
    """Ratsit packs 'c/o <name> <street> <number>' into one string: split at the street."""
    stripped = re.sub(r"\s*,\s*", " ", _CARE_OF_PREFIX.sub("", line))
    tokens = stripped.split(" ")
    number_at = None
    for i, tok in enumerate(tokens):
        if i > 0 and (re.fullmatch(r"[0-9]+(?:-[0-9]+)?[a-zåäö]?", tok) or _GLUED_NUMBER.match(tok)):
            number_at = i
            break
    if number_at is None:
        notes.append("care-of without a street number")
        return stripped or None, ""
    if _GLUED_NUMBER.match(tokens[number_at]):
        start = number_at
    else:
        start = number_at - 1
        if tokens[start] in _STREET_GENERICS and start >= 1:
            start -= 1
        while start >= 1 and tokens[start - 1] in _STREET_PREFIXES:
            start -= 1
    care_of = " ".join(tokens[:start]).strip() or None
    street = " ".join(tokens[start:])
    return care_of, street


def normalize_se_address(raw: RawAddress) -> NormalizedAddress:
    notes: list[str] = []
    if raw.raw_address:
        raw = _split_packed(raw.raw_address)
    care_of_display = _display(_CARE_OF_PREFIX.sub("", _clean(raw.care_of)))
    care_of = care_of_display.casefold()
    street_line = _fold(raw.street_address)
    street_display_source = _clean(raw.street_address)
    town = _fold(raw.post_town)
    town_display = _display(raw.post_town or "")
    code = re.sub(r"\D", "", raw.postal_code or "")
    country = (raw.country_code or "").strip().upper()

    if town in _FOREIGN_TOWNS:
        return NormalizedAddress(None, None, None, None, None, None, None, country or "", "", "foreign", "post town marks the address as foreign")
    if country and country != "SE":
        return NormalizedAddress(None, None, None, None, None, None, None, country, "", "foreign", f"country {country}")
    if town in _UNKNOWN_TOWNS or street_line in _UNKNOWN_STREETS:
        return NormalizedAddress(None, None, None, None, None, None, None, "SE", "", "no_address", "source marks the address as unknown")
    if not street_line and not care_of:
        return NormalizedAddress(None, None, None, None, None, None, None, "SE", "", "no_address", "")

    if street_line.startswith(("c/o ", "co ", "att ", "att: ")) and not care_of:
        care_of, street_line = _split_care_of_street(street_line, notes)
        care_of_display = _display(care_of or "")
        street_display_source = ""
    box, street_name, house_number, unit = _split_street(street_line, notes) if street_line else (None, None, None, None)

    if code and (len(code) != 5 or code in _INVALID_POSTCODES):
        notes.append(f"postcode '{raw.postal_code}' is not a valid five-digit code")
        code = ""
    if town in _UNKNOWN_TOWNS:
        town = ""

    has_location = bool(box or street_name)
    if not has_location:
        status = "no_address"
    elif code and town:
        status = "ok"
    else:
        status = "partial"
        notes.append("missing postcode" if not code else "missing city")

    line_parts = []
    if care_of:
        line_parts.append(f"c/o {care_of_display}")
    if box:
        line_parts.append(f"Box {box.upper()}")
    elif street_name:
        mixed_case = street_display_source not in ("", street_display_source.upper(), street_display_source.lower())
        street = _display_kept(street_display_source, street_name) if mixed_case else _display(street_name)
        if house_number:
            street += f" {house_number.upper()}"
        if unit:
            street += f" {unit}"
        line_parts.append(street)
    postal = " ".join(p for p in (f"{code[:3]} {code[3:]}" if code else "", town_display if town else "") if p)
    if postal:
        line_parts.append(postal)
    display = ", ".join(line_parts) if has_location else ""

    return NormalizedAddress(
        care_of=care_of or None,
        box=box.upper() if box else None,
        street_name=street_name or None,
        house_number=house_number.upper() if house_number else None,
        unit=unit or None,
        postal_code=code or None,
        city=town or None,
        country_code="SE",
        normalized_address=display,
        parse_status=status,
        parse_notes="; ".join(notes),
    )


def _display_kept(source_line: str, street_name: str) -> str:
    """The street name in the delivered casing: the folded name is a prefix of the folded line."""
    folded = re.sub(r"\s*,\s*", " ", source_line.casefold())
    if folded.startswith(street_name):
        return re.sub(r"\s*,\s*", " ", source_line)[: len(street_name)]
    return _display(street_name)


IDENTITY_FIELDS: tuple[str, ...] = (
    "country_code", "postal_code", "city", "street_name", "box", "house_number", "unit", "care_of",
)


def identity_components(normalized: NormalizedAddress) -> tuple[str, ...]:
    """Spec section 5.1: the eight components, NULL as ''."""
    return tuple(getattr(normalized, field_name) or "" for field_name in IDENTITY_FIELDS)


def address_key(normalized: NormalizedAddress) -> str:
    return hashlib.sha256("\n".join(identity_components(normalized)).encode("utf-8")).hexdigest()

```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_normalize_se.py -q`
Expected: 51 passed (47 corpus cases + 4 unit tests). If a corpus case fails, the implementation drifted from the plan's code: fix the implementation, never the expectation, and report the diff.

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/normalize_se.py \
  corpscout/services/dagster_v3/tests/fixtures/se_addresses/golden.jsonl \
  corpscout/services/dagster_v3/tests/test_se_company_address_normalize_se.py
git commit -m "feat(dagster): Swedish address normalizer with a golden corpus"
```

---

### Task 3: The normalize asset

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/normalize.py`, `src/dagster_v3/defs/se_company/address/assets.py`
- Test: `tests/test_se_company_address_normalize.py`

**Interfaces:**
- Consumes: Task 1's `tables`; Task 2's `RawAddress`, `NormalizedAddress`, `normalize_se_address`, `address_key`, `NORMALIZER_VERSION`; `dagster_v3.defs.se_company.basic_info.extract.scope_pages` and `SCAN_QUERY_SETTINGS`; `dagster_v3.defs.se_company.basic_info.batch.ID_BOUND_QUERY_SETTINGS`; `dagster_v3.defs.se_company.common.normalized_se_company_ids`; `dagster_v3.defs.clickhouse.resolved.assert_clickhouse_tables_exist`; `dagster_clickhouse.ClickhouseResource`.
- Produces: `normalize_address(raw: RawAddress) -> NormalizedAddress` (the country dispatcher); `changed_scope_sql()`, `all_scope_sql()`, `changed_rows_sql()`, `all_rows_sql()`, `normalized_insert_sql()`; `normalized_row(raw_row: tuple, normalized_at: datetime) -> tuple` (the 22-value insert tuple in `tables.NORMALIZED_COLUMNS` order); `NormalizeCounts` with `companies, pages, rows, ok, partial, no_address, foreign` and `as_metadata()`; `normalize_companies(client, company_ids, *, changed_only, normalized_at, page_size, log=None) -> NormalizeCounts`; `normalize_all(client, *, changed_only, normalized_at, page_size, log=None) -> NormalizeCounts`; asset `se_company_address_normalize` with config `AddressNormalizeConfig(changed_only: bool = True, company_ids: list[str] = [], page_size: int = 20_000)`.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_address_normalize.py`:

```python
"""The normalize asset's scan, row shaping and writes (spec section 4), against a scripted
ClickHouse client. The SQL texts run for real in test_se_company_address_clickhouse_local.py."""

from datetime import UTC, datetime

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.normalize import (
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
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.basic_info.extract import SCRATCH_SCOPE_PREFIX

STAMP = datetime(2026, 9, 6, 12, 0, 0, 123000, tzinfo=UTC)

# (company_id, source, slot, suggestion_id, suggested_at, kind, raw_address, care_of,
#  street_address, postal_code, post_town, county, country_code)
RAW_SCB = ("5561552760", "scb", "", "a" * 64, datetime(2026, 9, 1, tzinfo=UTC), "visiting_or_postal",
           None, None, "SICKLA INDUSTRIVÄG 19", "13134", "NACKA", None, None)
RAW_BV = ("5561552760", "bolagsverket", "", "b" * 64, datetime(2026, 9, 2, tzinfo=UTC), "postal",
          "Box 5305$$STOCKHOLM$10247$SE-LAND", None, None, None, None, None, None)
RAW_EMPTY = ("5560125220", "ratsit", "company", "c" * 64, datetime(2026, 9, 3, tzinfo=UTC), "postal",
             None, None, None, None, None, None, None)


class FakeClient:
    def __init__(self, *, scope_ids, rows):
        self.scope_pages = [list(scope_ids)]
        self.rows = list(rows)
        self.statements: list[tuple[str, object, object]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params, settings))
        if sql.startswith(("CREATE TABLE", "DROP TABLE")):
            return []
        if sql.startswith("INSERT INTO"):
            return []
        if sql.startswith(f"SELECT company_id FROM {SCRATCH_SCOPE_PREFIX}"):
            return [(i,) for i in (self.scope_pages.pop(0) if self.scope_pages else [])]
        if f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in sql:
            ids = set(params["company_ids"])
            return [r for r in self.rows if r[0] in ids]
        raise AssertionError(sql)

    def inserts(self):
        return [(s, list(p)) for s, p, _ in self.statements if s.startswith(f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE}")]


def test_normalized_row_has_the_22_values_in_column_order() -> None:
    row = normalized_row(RAW_SCB, STAMP)
    assert len(row) == len(tables.NORMALIZED_COLUMNS) == 22
    by_name = dict(zip(tables.NORMALIZED_COLUMNS, row, strict=True))
    assert by_name["company_id"] == "5561552760" and by_name["source"] == "scb" and by_name["slot"] == ""
    assert by_name["suggestion_id"] == "a" * 64
    assert by_name["suggested_at"] == RAW_SCB[4]
    assert by_name["kind"] == "visiting_or_postal"
    assert by_name["street_name"] == "sickla industriväg" and by_name["house_number"] == "19"
    assert by_name["postal_code"] == "13134" and by_name["city"] == "nacka" and by_name["country_code"] == "SE"
    assert by_name["normalized_address"] == "Sickla Industriväg 19, 131 34 Nacka"
    assert by_name["parse_status"] == "ok" and by_name["parse_notes"] == ""
    assert by_name["normalizer_version"] == NORMALIZER_VERSION
    assert by_name["normalized_at"] == STAMP
    assert len(by_name["address_key"]) == 64 and len(by_name["normalized_id"]) == 64


def test_normalized_id_is_the_key_plus_the_stamp() -> None:
    import hashlib
    row = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(RAW_BV, STAMP), strict=True))
    expected = hashlib.sha256("5561552760\nbolagsverket\n\n2026-09-06 12:00:00.123".encode()).hexdigest()
    assert row["normalized_id"] == expected
    assert row["box"] == "5305" and row["street_name"] is None
    assert row["normalized_address"] == "Box 5305, 102 47 Stockholm"


def test_an_empty_raw_row_normalizes_to_no_address_with_the_empty_key() -> None:
    row = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(RAW_EMPTY, STAMP), strict=True))
    assert row["parse_status"] == "no_address"
    assert row["normalized_address"] == ""
    assert all(row[c] is None for c in tables.COMPONENT_COLUMNS)


def test_sql_texts_read_final_rows_and_bind_ids_and_version() -> None:
    assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in changed_rows_sql()
    assert "LEFT ANTI JOIN" in changed_rows_sql() and "UNION ALL" in changed_rows_sql()
    assert "%(company_ids)s" in changed_rows_sql() and "%(normalizer_version)s" in changed_rows_sql()
    assert "r.suggested_at > n.suggested_at" in changed_rows_sql()
    assert "n.normalizer_version != %(normalizer_version)s" in changed_rows_sql()
    assert f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL" in changed_rows_sql()
    assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} AS r FINAL" in all_rows_sql()
    assert "JOIN" not in all_rows_sql()
    assert changed_scope_sql().startswith("SELECT DISTINCT company_id FROM (")
    assert all_scope_sql() == f"SELECT DISTINCT company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"
    assert normalized_insert_sql() == (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} ({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES"
    )


def test_normalize_companies_reads_the_page_and_inserts_one_row_per_raw_row() -> None:
    client = FakeClient(scope_ids=[], rows=[RAW_SCB, RAW_BV, RAW_EMPTY])
    counts = normalize_companies(client, ["5561552760", "5560125220"], changed_only=True, normalized_at=STAMP, page_size=20_000)
    assert counts == NormalizeCounts(companies=2, pages=1, rows=3, ok=2, partial=0, no_address=1, foreign=0)
    [(sql, rows)] = client.inserts()
    assert sql == normalized_insert_sql()
    assert [r[1] for r in rows] == ["scb", "bolagsverket", "ratsit"]
    read = [s for s, _, _ in client.statements if "AS r FINAL" in s]
    assert read == [changed_rows_sql()]
    assert client.statements[0][1] == {"company_ids": ["5560125220", "5561552760"], "normalizer_version": NORMALIZER_VERSION}
    # No scratch table for a targeted call: the ids are paged in memory.
    assert not any(s.startswith("CREATE TABLE") for s, _, _ in client.statements)


def test_normalize_companies_with_changed_only_false_reads_every_raw_row() -> None:
    client = FakeClient(scope_ids=[], rows=[RAW_SCB])
    normalize_companies(client, ["5561552760"], changed_only=False, normalized_at=STAMP, page_size=20_000)
    read = [s for s, _, _ in client.statements if "AS r FINAL" in s]
    assert read == [all_rows_sql()]


def test_normalize_all_scans_into_a_scratch_table_and_pages_it() -> None:
    client = FakeClient(scope_ids=["5560125220", "5561552760"], rows=[RAW_SCB, RAW_BV, RAW_EMPTY])
    counts = normalize_all(client, changed_only=True, normalized_at=STAMP, page_size=20_000)
    assert counts.companies == 2 and counts.pages == 1 and counts.rows == 3
    created = [s for s, _, _ in client.statements if s.startswith("CREATE TABLE")]
    assert len(created) == 1 and created[0].split()[2].startswith(SCRATCH_SCOPE_PREFIX)
    scope_insert = next(s for s, _, _ in client.statements if s.startswith(f"INSERT INTO {SCRATCH_SCOPE_PREFIX}"))
    assert changed_scope_sql() in scope_insert
    assert any(s.startswith("DROP TABLE IF EXISTS") for s, _, _ in client.statements)


def test_counts_metadata_keys() -> None:
    assert set(NormalizeCounts(1, 1, 1, 1, 0, 0, 0).as_metadata()) == {
        "companies", "pages", "rows", "ok", "partial", "no_address", "foreign", "normalizer_version",
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_normalize.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.address.normalize'`.

- [ ] **Step 3: Write `normalize.py`**

```python
"""The normalize step (spec section 4): raw suggestion rows whose normalized row is missing,
older than the raw version, or on an older normalizer are normalized in Python and written
as new versions of se_company_address_normalized. One function per country; only Sweden."""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.normalize_se import (
    NORMALIZER_VERSION,
    NormalizedAddress,
    RawAddress,
    address_key,
    normalize_se_address,
)
from dagster_v3.defs.se_company.basic_info.batch import ID_BOUND_QUERY_SETTINGS
from dagster_v3.defs.se_company.basic_info.extract import SCAN_QUERY_SETTINGS, scope_pages

PAGE_SIZE = 20_000

RAW_ROW_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "suggested_at", "kind", *tables.RAW_ADDRESS_COLUMNS,
)


def normalize_address(raw: RawAddress) -> NormalizedAddress:
    """The per-country dispatcher of spec section 4. Only Sweden exists; every SE company's
    addresses go through the Swedish rules, and the rules themselves decide `foreign`."""
    return normalize_se_address(raw)


def _raw_select(alias: str) -> str:
    return ", ".join(f"{alias}.{column} AS {column}" for column in RAW_ROW_COLUMNS)


def _normalized_keys_sql() -> str:
    return (
        "SELECT company_id, source, slot, suggested_at, toString(normalizer_version) AS normalizer_version\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def changed_rows_sql() -> str:
    """The page's raw rows that need (re)normalizing: never normalized, raw newer than the
    normalized row, or normalized on another normalizer version. Two branches instead of a
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
        "    AND (r.suggested_at > n.suggested_at OR n.normalizer_version != %(normalizer_version)s)"
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
        "SELECT company_id, source, slot, suggested_at, toString(normalizer_version) AS normalizer_version\n"
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
        "WHERE r.suggested_at > n.suggested_at OR n.normalizer_version != %(normalizer_version)s\n"
        ") AS changed"
    )


def all_scope_sql() -> str:
    return f"SELECT DISTINCT company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"


def normalized_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} ({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES"


def clickhouse_stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M:%S.") + f"{moment.microsecond // 1000:03d}"


def normalized_row(raw_row: Sequence[Any], normalized_at: datetime) -> tuple[Any, ...]:
    """One insert tuple in tables.NORMALIZED_COLUMNS order from one raw row in RAW_ROW_COLUMNS order."""
    row = dict(zip(RAW_ROW_COLUMNS, raw_row, strict=True))
    raw = RawAddress(**{column: row[column] for column in tables.RAW_ADDRESS_COLUMNS})
    normalized = normalize_address(raw)
    normalized_id = hashlib.sha256(
        f"{row['company_id']}\n{row['source']}\n{row['slot']}\n{clickhouse_stamp(normalized_at)}".encode()
    ).hexdigest()
    values = {
        "company_id": row["company_id"],
        "source": row["source"],
        "slot": row["slot"],
        "normalized_id": normalized_id,
        "suggestion_id": row["suggestion_id"],
        "suggested_at": row["suggested_at"],
        "kind": row["kind"],
        "care_of": normalized.care_of,
        "box": normalized.box,
        "street_name": normalized.street_name,
        "house_number": normalized.house_number,
        "unit": normalized.unit,
        "postal_code": normalized.postal_code,
        "city": normalized.city,
        "country_code": normalized.country_code,
        "normalized_address": normalized.normalized_address,
        "address_key": address_key(normalized),
        "parse_status": normalized.parse_status,
        "parse_notes": normalized.parse_notes,
        "normalizer_version": NORMALIZER_VERSION,
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
    no_address: int
    foreign: int

    def as_metadata(self) -> dict[str, Any]:
        return {
            "companies": self.companies,
            "pages": self.pages,
            "rows": self.rows,
            "ok": self.ok,
            "partial": self.partial,
            "no_address": self.no_address,
            "foreign": self.foreign,
            "normalizer_version": NORMALIZER_VERSION,
        }


def _normalize_page(client: Any, company_ids: Sequence[str], *, changed_only: bool, normalized_at: datetime) -> dict[str, int]:
    params = {"company_ids": sorted(company_ids), "normalizer_version": NORMALIZER_VERSION}
    raw_rows = client.execute(changed_rows_sql() if changed_only else all_rows_sql(), params, settings=ID_BOUND_QUERY_SETTINGS)
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
        partial=total["partial"], no_address=total["no_address"], foreign=total["foreign"],
    )


def normalize_companies(
    client: Any, company_ids: Sequence[str], *, changed_only: bool, normalized_at: datetime,
    page_size: int = PAGE_SIZE, log: Any = None,
) -> NormalizeCounts:
    """Normalize the named companies' raw rows, paged in memory (no scan)."""
    ids = sorted(set(company_ids))
    pages = []
    for start in range(0, len(ids), page_size):
        page = ids[start : start + page_size]
        pages.append(_normalize_page(client, page, changed_only=changed_only, normalized_at=normalized_at))
        if log is not None:
            log.info("normalized page %d: %d rows", len(pages), pages[-1]["rows"])
    return _accumulate(pages, len(ids))


def normalize_all(
    client: Any, *, changed_only: bool, normalized_at: datetime, page_size: int = PAGE_SIZE, log: Any = None,
) -> NormalizeCounts:
    """Scan the raw table once for the companies that need normalizing, then page them."""
    scope_sql = changed_scope_sql() if changed_only else all_scope_sql()
    pages = []
    companies = 0
    for page in scope_pages(client, scope_sql=scope_sql, params={"normalizer_version": NORMALIZER_VERSION},
                            page_size=page_size, settings=SCAN_QUERY_SETTINGS):
        companies += len(page)
        pages.append(_normalize_page(client, page, changed_only=changed_only, normalized_at=normalized_at))
        if log is not None:
            log.info("normalized page %d: %d companies, %d rows", len(pages), len(page), pages[-1]["rows"])
    return _accumulate(pages, companies)
```

- [ ] **Step 4: Write `assets.py`**

```python
"""Dagster assets of the address entity. Slice 0 ships the normalize asset; the fold, the
precedence export and the extractors follow in slices 1 and 2."""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.normalize import PAGE_SIZE, normalize_all, normalize_companies
from dagster_v3.defs.se_company.common import normalized_se_company_ids

GROUP_NAME = "se_company_address"
NORMALIZE_POOL = "se_company_address_normalize"


class AddressNormalizeConfig(dg.Config):
    changed_only: bool = True
    company_ids: list[str] = Field(default_factory=list)
    page_size: int = Field(default=PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return normalized_se_company_ids(value)


@dg.asset(
    name="se_company_address_normalize",
    group_name=GROUP_NAME,
    pool=NORMALIZE_POOL,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_NORMALIZED_TABLE, "reads": tables.QUALIFIED_SUGGESTION_TABLE},
    description=(
        "Normalizes raw address suggestions into se_company_address_normalized: rows never "
        "normalized, newer than their normalized row, or normalized on an older normalizer "
        "version. changed_only=false re-normalizes every raw row; company_ids targets companies."
    ),
)
def se_company_address_normalize(
    context: dg.AssetExecutionContext, config: AddressNormalizeConfig, clickhouse: ClickhouseResource
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
    return dg.MaterializeResult(metadata={**counts.as_metadata(), "table": tables.QUALIFIED_NORMALIZED_TABLE})
```

Check how `assert_clickhouse_tables_exist` is called in `basic_info/assets.py` (its `tables=` argument shape) and match it exactly.

- [ ] **Step 5: Run the tests and the definitions check**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_normalize.py tests/test_se_company_address_normalize_se.py -q`
Expected: all PASS.
Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` and the asset `se_company_address_normalize` in group `se_company_address` (`uv run --frozen --no-sync dg list defs | grep se_company_address`).

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/normalize.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/assets.py \
  corpscout/services/dagster_v3/tests/test_se_company_address_normalize.py
git commit -m "feat(dagster): se_company_address_normalize asset"
```

---

### Task 4: The clickhouse-local integration test and the module docs

**Files:**
- Create: `tests/test_se_company_address_clickhouse_local.py`, `src/dagster_v3/defs/se_company/address/docs/address-design.md`

**Interfaces:**
- Consumes: Tasks 1 to 3. Uses `_clickhouse_local_command()` from `tests/test_se_company_person_clickhouse_local.py` and the migration files directly, the way `tests/test_se_company_basic_info_clickhouse_local.py` does (read that file first and copy its DDL-loading and settings-loop helpers rather than inventing new ones).

- [ ] **Step 1: Write the test**

`tests/test_se_company_address_clickhouse_local.py` (marked `pytest.mark.integration`, skipped when no `clickhouse-local` or docker is available):

1. Load the six migrations' `.up.sql` into a `clickhouse-local` session (same helper as the basic-info local test).
2. Insert three raw rows (the `RAW_SCB`, `RAW_BV`, `RAW_EMPTY` shapes of Task 3 as SQL VALUES with `suggested_at` `2026-09-01`, `2026-09-02`, `2026-09-03`, `observed_at` equal to `suggested_at`, `source_record_uid ''`, `source_run_id 'test'`, `extractor_version 'test-v1'`).
3. For each of `join_use_nulls = 0` and `1`: run `changed_scope_sql()` with `normalizer_version = 'se-address-normalizer-v1'` and assert the two company ids come back; run `changed_rows_sql()` for those ids and assert three rows in `(company_id, source, slot)` order.
4. Insert one normalized row for the SCB raw row through `normalized_insert_sql()` with the exact tuple `normalized_row(raw_row, STAMP)` produces (bind values as SQL literals), then re-run `changed_rows_sql()` under both settings and assert only the Bolagsverket and Ratsit rows remain; then insert a normalized row for the Bolagsverket raw row with `normalizer_version = 'se-address-normalizer-v0'` and assert it is selected again under both settings (the version condition).
5. Assert `SELECT count() FROM corpscout.se_company_address_normalized FINAL` is 2.

- [ ] **Step 2: Run it**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_clickhouse_local.py -q -m integration`
Expected: PASS (or SKIP with the same reason the basic-info local test skips on this machine; if it skips locally, say so in the report, the controller runs it where clickhouse-local exists).

- [ ] **Step 3: Write the module docs**

`src/dagster_v3/defs/se_company/address/docs/address-design.md`: a table with one row per module (`tables.py`, `normalize_se.py`, `normalize.py`, `assets.py`) and its responsibility, a paragraph on the change rule of the normalize asset (missing, newer raw, older normalizer version), the `parse_status` meanings, the packed Bolagsverket format, and how to run it (`changed_only`, `company_ids`, `page_size`), pointing at the spec for everything else. Under 60 lines.

- [ ] **Step 4: Commit**

```bash
git add corpscout/services/dagster_v3/tests/test_se_company_address_clickhouse_local.py \
  corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md
git commit -m "test(dagster): address normalize SQL against clickhouse-local; module docs"
```

---

### Task 5: Cutover (controller)

1. [ ] Whole-branch review, then the owner merges `se-basic-info` into main.
2. [ ] Check main and prod `schema_migrations` for a number collision (ledger head must be 381), then apply the six migrations one at a time from the deploy worktree: `make -C <deploy-worktree>/corpscout clickhouse-migrate-up-one` six times, verifying `SELECT version, dirty FROM corpscout.schema_migrations` reaches `387 0`.
3. [ ] Hot-sync the dagster host from the deploy worktree at the merge commit (`uv sync --frozen`, the two dbt parses, `dg utils refresh-defs-state`, `dg check defs`, `ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml`), confirm the asset appears in the UI.
4. [ ] Materialize `se_company_address_normalize` once: it must succeed with `rows = 0` (no raw rows exist until slice 1).
5. [ ] Record in the ledger and in spec section 9; archive the ledger.

## Self-review

- Spec coverage: section 3 (six tables, lineage ids) Task 1; section 4 (normalizer, statuses, versioning, packed format, the normalize asset and its change rule) Tasks 2 and 3; section 10 names Tasks 1 to 3; the clickhouse-local proof and docs Task 4; nothing of sections 5 to 9 belongs to slice 0.
- Placeholders: the normalizer code and the corpus in Task 2 were generated from the draft that was validated against 260 sampled register rows and inserted verbatim; Task 1's history migration uses an explicit "copy the 30 columns" instruction with the exact source; Task 4 step 1 is a numbered procedure over the SQL texts Task 3 defines.
- Type consistency: `RAW_ROW_COLUMNS` (13 values) matches the `RAW_*` tuples in the tests; `normalized_row` yields 22 values = `len(tables.NORMALIZED_COLUMNS)`; `NormalizeCounts` positional order is `companies, pages, rows, ok, partial, no_address, foreign` everywhere; `scope_pages` is called with `params={"normalizer_version": ...}` because `changed_scope_sql()` binds that name and `all_scope_sql()` binds nothing (an unused bound parameter is fine for the driver).
