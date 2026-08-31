# Unified XBRL Extraction — Phase 1 (Finland) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared XBRL/iXBRL extraction library in `xbrl_common` (transforms, canonical row shapes, full-fidelity extractor, parity harness, taxonomy dictionary builder) and migrate Finland PRH to it behind a parity gate.

**Architecture:** The unified extractor emits canonical row dicts (documents/contexts/units/facts); a Finland adapter wraps it in the existing `StatementParser` callable signature so the existing partitioned parse drivers are reused with a parameterized row contract. New ClickHouse tables are built under `_next` names alongside the old ones; a parity asset diffs old vs new; cutover renames tables and retires the old parser.

**Tech Stack:** Python 3.14, lxml, polars, duckdb, Dagster (`dg`), ClickHouse (golang-migrate), Arelle (taxonomy dictionary only), pytest.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-08-31-unified-xbrl-extraction-design.md` — read it first. One clarification decided during planning (treat as spec): a source table = identity columns + canonical columns **+ optionally appended source-specific derived columns** (Finland appends `mcy_member_code`/`ref_member_code`, which its metric mapping joins on).

## Global Constraints

- Working directory for all commands: `corpscout/services/dagster_v3`. Always `cd` there with an absolute path in the same shell command as the work (cwd resets between calls).
- Use `uv run` for everything: `uv run pytest ...`, `uv run dg check defs`.
- **No `from __future__ import annotations` in any module that defines `@dg.asset`** (breaks Dagster context-type validation). Library modules without assets may use it.
- ESEF (`defs/esef_filings/`) is untouched. The existing light parser `defs/xbrl_common/parser.py` is untouched (UK still uses it).
- ClickHouse DDL is migration-owned: migrations live in `corpscout/clickhouse/migrations/` (repo-root relative), every new migration name is appended to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`, and no `;` inside SQL comment lines. **The migration ledger races with parallel sessions**: before creating each migration, run `ls ../../clickhouse/migrations | tail -3` and use the next free number; the numbers used in this plan (000364–000367) are the expected ones — if taken, renumber consistently and keep this plan's relative order.
- ClickHouse non-nullable String columns must receive `''`, never `NULL` (native driver `.encode()` dies on None). `ORDER BY` keys must be non-nullable.
- S3 raw data is additive-only — nothing in this plan writes to existing raw objects.
- All new Finland parse/export assets use the existing pool `FINLAND_XBRL_DUCKDB_POOL` (from `defs/finland_xbrl/assets/common.py`).
- Commit **by explicit path only** — the working tree carries unrelated WIP from parallel sessions. Never `git add -A` / `git add .`.
- Conventional Commits messages; end commit messages with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Canonical parser version string everywhere: `XBRL_COMMON_PARSER_VERSION = "xbrl-common-1.0.0"`.

## File Structure

```
src/dagster_v3/defs/xbrl_common/
  parser.py            (existing, untouched — UK metrics)
  transforms.py        (NEW, Task 1)  ixt transformation registry
  tables.py            (NEW, Task 2)  canonical column tuples + polars schemas + XbrlRowContract
  extractor.py         (NEW, Tasks 3–4)  SourceProfile, ExtractedFiling, extract_filing
  parity.py            (NEW, Task 8)  old-vs-new row diffing
  taxonomy.py          (NEW, Task 9)  Arelle-once concept dictionary builder
src/dagster_v3/defs/finland_xbrl/
  unified_adapter.py   (NEW, Task 5)  parse_statement_xml_unified (StatementParser signature)
  unified_clickhouse.py (NEW, Task 7) row converters + export for _next tables
  assets/data_snapshot_xml_duckdb.py (MODIFY, Task 6)  row-contract parameterization + unified assets
  assets/data_daily_xml_duckdb.py    (MODIFY, Task 6)  unified daily parse asset
  assets/unified_publish.py (NEW, Task 7)  fi_xbrl_unified_clickhouse asset
  assets/parity.py     (NEW, Task 8)  fi_xbrl_parity asset
  assets/taxonomy_dictionary.py (NEW, Task 9)  package download + dictionary + export assets
  assets/__init__.py   (MODIFY, Tasks 6–9)  register new assets
  assets/jobs.py       (MODIFY, Task 6)  unified parse in incremental job
tests/
  test_xbrl_transforms.py       (NEW, Task 1)
  test_xbrl_canonical_tables.py (NEW, Task 2)
  test_xbrl_extractor.py        (NEW, Tasks 3–4)
  test_finland_unified_adapter.py (NEW, Task 5)
  test_finland_unified_assets.py  (NEW, Tasks 6–7)
  test_xbrl_parity.py           (NEW, Task 8)
  test_xbrl_taxonomy.py         (NEW, Task 9)
  test_clickhouse_migrations.py (MODIFY, Tasks 7–9)
corpscout/clickhouse/migrations/ (repo root)
  000364_corpscout_fi_xbrl_unified_next_tables.{up,down}.sql   (Task 7)
  000365_corpscout_fi_xbrl_parity_report.{up,down}.sql         (Task 8)
  000366_corpscout_fi_taxonomy_dictionary.{up,down}.sql        (Task 9)
  000367_corpscout_fi_xbrl_unified_cutover.{up,down}.sql       (Task 11)
```

---

### Task 1: iXBRL transformation registry

**Files:**
- Create: `src/dagster_v3/defs/xbrl_common/transforms.py`
- Test: `tests/test_xbrl_transforms.py`

**Interfaces:**
- Produces: `apply_transform(format_qname: str, raw_text: str) -> TransformResult`; `TransformResult(kind: str, value: str)` with `kind ∈ {"numeric","date","text","boolean","empty"}`; `class UnknownTransform(ValueError)`; `XBRL_COMMON_PARSER_VERSION = "xbrl-common-1.0.0"`.
- `format_qname` is the raw attribute value (`"ixt:num-dot-decimal"`); only the local part after `:` is dispatched, normalized by lowercasing and stripping `-`. This makes v1 (`numdotdecimal`) and v2–v4 (`num-dot-decimal`) names hit the same handler.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_xbrl_transforms.py
import pytest

from dagster_v3.defs.xbrl_common.transforms import (
    TransformResult,
    UnknownTransform,
    apply_transform,
)


@pytest.mark.parametrize(
    ("fmt", "raw", "expected"),
    [
        ("ixt:num-dot-decimal", "1,234,567.89", TransformResult("numeric", "1234567.89")),
        ("ixt:numdotdecimal", "1 234 567.89", TransformResult("numeric", "1234567.89")),
        ("ixt:num-dot-decimal", "1 234.5", TransformResult("numeric", "1234.5")),
        ("ixt:num-comma-decimal", "1.234.567,89", TransformResult("numeric", "1234567.89")),
        ("ixt:numcommadecimal", "1 234,5", TransformResult("numeric", "1234.5")),
        ("ixt:num-unit-decimal", "1 234 kr 56", TransformResult("numeric", "1234.56")),
        ("ixt:zerodash", "-", TransformResult("numeric", "0")),
        ("ixt:fixed-zero", "anything", TransformResult("numeric", "0")),
        ("ixt:fixed-empty", "anything", TransformResult("empty", "")),
        ("ixt:fixed-false", "x", TransformResult("boolean", "false")),
        ("ixt:fixed-true", "x", TransformResult("boolean", "true")),
        ("ixt:booleanfalse", "no", TransformResult("boolean", "false")),
        ("ixt:booleantrue", "yes", TransformResult("boolean", "true")),
        ("ixt:date-day-month-year", "31.12.2024", TransformResult("date", "2024-12-31")),
        ("ixt:datedaymonthyear", "31/12/2024", TransformResult("date", "2024-12-31")),
        ("ixt:date-day-month-year", "1.1.2024", TransformResult("date", "2024-01-01")),
        ("ixt:date-year-month-day", "2024-12-31", TransformResult("date", "2024-12-31")),
        ("ixt:dateyearmonthday", "2024.12.31", TransformResult("date", "2024-12-31")),
        ("ixt:date-month-day-year", "12/31/2024", TransformResult("date", "2024-12-31")),
        ("ixt:date-month-year", "12.2024", TransformResult("text", "2024-12")),
        ("ixt4:date-day-monthname-year-en", "31 December 2024", TransformResult("date", "2024-12-31")),
        ("ixt:datedaymonthnameyearen", "1 jan 2024", TransformResult("date", "2024-01-01")),
        ("ixt:date-day-monthname-year-sv", "31 december 2024", TransformResult("date", "2024-12-31")),
        ("ixt:date-day-monthname-year-fi", "31 joulukuuta 2024", TransformResult("date", "2024-12-31")),
    ],
)
def test_apply_transform(fmt, raw, expected):
    assert apply_transform(fmt, raw) == expected


def test_unknown_transform_raises():
    with pytest.raises(UnknownTransform):
        apply_transform("ixt:date-tolkien-calendar", "3019-03-25")


def test_bad_numeric_input_raises_value_error():
    with pytest.raises(ValueError):
        apply_transform("ixt:num-dot-decimal", "not a number")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/services/dagster_v3 && uv run pytest tests/test_xbrl_transforms.py -q`
Expected: FAIL — `ModuleNotFoundError` / import error for `transforms`.

- [ ] **Step 3: Implement**

```python
# src/dagster_v3/defs/xbrl_common/transforms.py
"""iXBRL transformation registry (ixt v1-v4 numeric/date families).

Dispatch is on the format's local name, lowercased with hyphens stripped, so
v1 names (``numdotdecimal``) and v2-v4 names (``num-dot-decimal``) share
handlers. Unknown transforms raise ``UnknownTransform``; the extractor turns
that into a document warning plus a raw text value — it never guesses.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

XBRL_COMMON_PARSER_VERSION = "xbrl-common-1.0.0"

_SPACES = "    "
_STRIP_RE = re.compile(f"[{_SPACES}]")


@dataclass(frozen=True)
class TransformResult:
    kind: str  # numeric | date | text | boolean | empty
    value: str


class UnknownTransform(ValueError):
    pass


_MONTHS_EN = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12, "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTHS_SV = {
    "januari": 1, "februari": 2, "mars": 3, "april": 4, "maj": 5, "juni": 6,
    "juli": 7, "augusti": 8, "september": 9, "oktober": 10, "november": 11,
    "december": 12,
}
_MONTHS_FI = {
    "tammikuuta": 1, "helmikuuta": 2, "maaliskuuta": 3, "huhtikuuta": 4,
    "toukokuuta": 5, "kesäkuuta": 6, "heinäkuuta": 7, "elokuuta": 8,
    "syyskuuta": 9, "lokakuuta": 10, "marraskuuta": 11, "joulukuuta": 12,
    "tammikuu": 1, "helmikuu": 2, "maaliskuu": 3, "huhtikuu": 4,
    "toukokuu": 5, "kesäkuu": 6, "heinäkuu": 7, "elokuu": 8,
    "syyskuu": 9, "lokakuu": 10, "marraskuu": 11, "joulukuu": 12,
}
_MONTH_NAMES: dict[str, int] = {**_MONTHS_EN, **_MONTHS_SV, **_MONTHS_FI}

_DATE_SPLIT_RE = re.compile(r"[.\-/\s]+")
_MONTHNAME_RE = re.compile(
    r"^\s*(\d{1,2})\.?\s+([^\s\d.]+)\.?\s+(\d{4})\s*$", re.UNICODE
)


def _numeric(raw: str, *, decimal_sep: str, thousand_seps: str) -> str:
    text = _STRIP_RE.sub("", raw.strip())
    for sep in thousand_seps:
        text = text.replace(sep, "")
    if decimal_sep != ".":
        text = text.replace(decimal_sep, ".")
    if not re.fullmatch(r"-?\d+(\.\d+)?", text):
        raise ValueError(f"not a decimal after transform: {raw!r}")
    return text


def _num_dot_decimal(raw: str) -> TransformResult:
    return TransformResult("numeric", _numeric(raw, decimal_sep=".", thousand_seps=","))


def _num_comma_decimal(raw: str) -> TransformResult:
    return TransformResult("numeric", _numeric(raw, decimal_sep=",", thousand_seps="."))


def _num_unit_decimal(raw: str) -> TransformResult:
    match = re.fullmatch(r"\s*([\d.,\s   ]+?)\D+?(\d+)\s*", raw)
    if match is None:
        raise ValueError(f"not a unit-decimal value: {raw!r}")
    integer = _STRIP_RE.sub("", match.group(1)).replace(".", "").replace(",", "")
    if not integer.isdigit():
        raise ValueError(f"not a unit-decimal value: {raw!r}")
    return TransformResult("numeric", f"{integer}.{match.group(2)}")


def _date_from_parts(day: str, month: str, year: str) -> TransformResult:
    day_i, month_i, year_i = int(day), int(month), int(year)
    if not (1 <= month_i <= 12 and 1 <= day_i <= 31):
        raise ValueError(f"invalid date parts: {day}/{month}/{year}")
    return TransformResult("date", f"{year_i:04d}-{month_i:02d}-{day_i:02d}")


def _date_dmy(raw: str) -> TransformResult:
    parts = _DATE_SPLIT_RE.split(raw.strip())
    if len(parts) != 3:
        raise ValueError(f"not a day-month-year date: {raw!r}")
    return _date_from_parts(parts[0], parts[1], parts[2])


def _date_ymd(raw: str) -> TransformResult:
    parts = _DATE_SPLIT_RE.split(raw.strip())
    if len(parts) != 3:
        raise ValueError(f"not a year-month-day date: {raw!r}")
    return _date_from_parts(parts[2], parts[1], parts[0])


def _date_mdy(raw: str) -> TransformResult:
    parts = _DATE_SPLIT_RE.split(raw.strip())
    if len(parts) != 3:
        raise ValueError(f"not a month-day-year date: {raw!r}")
    return _date_from_parts(parts[1], parts[0], parts[2])


def _date_month_year(raw: str) -> TransformResult:
    parts = _DATE_SPLIT_RE.split(raw.strip())
    if len(parts) != 2:
        raise ValueError(f"not a month-year date: {raw!r}")
    month_i, year_i = int(parts[0]), int(parts[1])
    if not 1 <= month_i <= 12:
        raise ValueError(f"invalid month: {raw!r}")
    # No day component exists, so this cannot become a full date - normalize
    # to YYYY-MM and keep kind=text.
    return TransformResult("text", f"{year_i:04d}-{month_i:02d}")


def _date_day_monthname_year(raw: str) -> TransformResult:
    match = _MONTHNAME_RE.match(raw)
    if match is None:
        raise ValueError(f"not a day-monthname-year date: {raw!r}")
    month = _MONTH_NAMES.get(match.group(2).lower().rstrip("."))
    if month is None:
        raise ValueError(f"unknown month name in: {raw!r}")
    return _date_from_parts(match.group(1), str(month), match.group(3))


_HANDLERS = {
    "numdotdecimal": _num_dot_decimal,
    "numcommadecimal": _num_comma_decimal,
    "numunitdecimal": _num_unit_decimal,
    "numcommadot": _num_dot_decimal,
    "zerodash": lambda raw: TransformResult("numeric", "0"),
    "numdash": lambda raw: TransformResult("numeric", "0"),
    "fixedzero": lambda raw: TransformResult("numeric", "0"),
    "fixedempty": lambda raw: TransformResult("empty", ""),
    "fixedfalse": lambda raw: TransformResult("boolean", "false"),
    "fixedtrue": lambda raw: TransformResult("boolean", "true"),
    "booleanfalse": lambda raw: TransformResult("boolean", "false"),
    "booleantrue": lambda raw: TransformResult("boolean", "true"),
    "datedaymonthyear": _date_dmy,
    "dateyearmonthday": _date_ymd,
    "datemonthdayyear": _date_mdy,
    "datemonthyear": _date_month_year,
    "dateslasheu": _date_dmy,
    "dateslashus": _date_mdy,
    "datedoteu": _date_dmy,
    "datedotus": _date_mdy,
}
# monthname variants share one handler across languages/suffixes
for _lang in ("", "en", "sv", "fi", "no", "da"):
    _HANDLERS[f"datedaymonthnameyear{_lang}"] = _date_day_monthname_year


def apply_transform(format_qname: str, raw_text: str) -> TransformResult:
    local = format_qname.rpartition(":")[2].lower().replace("-", "")
    handler = _HANDLERS.get(local)
    if handler is None:
        raise UnknownTransform(f"unsupported ixt transform: {format_qname}")
    return handler(raw_text)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_xbrl_transforms.py -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/xbrl_common/transforms.py tests/test_xbrl_transforms.py
git commit -m "feat(xbrl): ixt transformation registry for unified extraction"
```

Deferred from the spec, deliberately: the corpus `format=`-coverage script arrives with the Sweden phase — Finland filings are plain XBRL and carry no `format` attributes, so there is no Finnish corpus to scan.

---

### Task 2: Canonical row shapes and row contract

**Files:**
- Create: `src/dagster_v3/defs/xbrl_common/tables.py`
- Test: `tests/test_xbrl_canonical_tables.py`

**Interfaces:**
- Produces: `XBRL_DOCUMENT_COLUMNS`, `XBRL_CONTEXT_COLUMNS`, `XBRL_UNIT_COLUMNS`, `XBRL_FACT_COLUMNS` (tuples of canonical column names, **no identity columns**); matching `*_POLARS_SCHEMA` dicts; `XbrlRowContract` dataclass bundling per-table `(columns, schema)` used by the parameterized Finland parse driver in Task 6.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_xbrl_canonical_tables.py
import polars as pl

from dagster_v3.defs.xbrl_common import tables as xt


def test_canonical_columns_match_schemas():
    assert list(xt.XBRL_DOCUMENT_COLUMNS) == list(xt.XBRL_DOCUMENT_POLARS_SCHEMA)
    assert list(xt.XBRL_CONTEXT_COLUMNS) == list(xt.XBRL_CONTEXT_POLARS_SCHEMA)
    assert list(xt.XBRL_UNIT_COLUMNS) == list(xt.XBRL_UNIT_POLARS_SCHEMA)
    assert list(xt.XBRL_FACT_COLUMNS) == list(xt.XBRL_FACT_POLARS_SCHEMA)


def test_canonical_fact_columns_exact():
    assert xt.XBRL_FACT_COLUMNS == (
        "fact_ordinal", "concept_qname", "concept_namespace", "concept_local_name",
        "context_id", "unit_id", "currency", "decimals", "precision", "is_nil",
        "xml_lang", "value_kind", "raw_value", "numeric_value", "date_value",
        "text_value", "dimensions", "is_comparative", "parser_version", "parsed_at",
    )


def test_row_contract_composes_identity_and_extras():
    contract = xt.XbrlRowContract.build(
        document_identity=("statement_key", "business_id"),
        row_identity=("statement_key",),
        fact_identity=("statement_key", "business_id"),
        context_extras=("mcy_member_code",),
        fact_extras=("mcy_member_code",),
    )
    assert contract.documents.columns[:2] == ["statement_key", "business_id"]
    assert contract.contexts.columns[0] == "statement_key"
    assert contract.contexts.columns[-1] == "mcy_member_code"
    assert contract.facts.columns[-1] == "mcy_member_code"
    assert contract.contexts.schema["mcy_member_code"] == pl.Utf8
    assert set(xt.XBRL_FACT_COLUMNS) <= set(contract.facts.columns)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_xbrl_canonical_tables.py -q`
Expected: FAIL — `xbrl_common.tables` does not exist.

- [ ] **Step 3: Implement**

```python
# src/dagster_v3/defs/xbrl_common/tables.py
"""Canonical XBRL row shapes shared by all national sources.

A source table = source identity columns + these canonical columns +
optionally appended source-specific derived columns (e.g. Finland's
mcy_member_code). Identity and extras are Utf8 unless the source overrides.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

XBRL_DOCUMENT_POLARS_SCHEMA = {
    "xml_sha256": pl.Utf8,
    "xml_size_bytes": pl.Int64,
    "root_name": pl.Utf8,
    "schema_refs": pl.Utf8,
    "taxonomy_entrypoint": pl.Utf8,
    "reported_entity_id": pl.Utf8,
    "reported_company_name": pl.Utf8,
    "reported_period_start": pl.Utf8,
    "reported_period_end": pl.Utf8,
    "contexts_count": pl.Int64,
    "units_count": pl.Int64,
    "facts_count": pl.Int64,
    "validation_warnings": pl.Utf8,
    "parser_version": pl.Utf8,
    "parsed_at": pl.Utf8,
}
XBRL_DOCUMENT_COLUMNS = tuple(XBRL_DOCUMENT_POLARS_SCHEMA)

XBRL_CONTEXT_POLARS_SCHEMA = {
    "context_id": pl.Utf8,
    "entity_identifier": pl.Utf8,
    "entity_scheme": pl.Utf8,
    "period_type": pl.Utf8,
    "instant_date": pl.Utf8,
    "period_start": pl.Utf8,
    "period_end": pl.Utf8,
    "dimensions": pl.Utf8,
    "is_comparative": pl.Boolean,
    "parser_version": pl.Utf8,
    "parsed_at": pl.Utf8,
}
XBRL_CONTEXT_COLUMNS = tuple(XBRL_CONTEXT_POLARS_SCHEMA)

XBRL_UNIT_POLARS_SCHEMA = {
    "unit_id": pl.Utf8,
    "measures": pl.Utf8,
    "numerator_measures": pl.Utf8,
    "denominator_measures": pl.Utf8,
    "is_divide": pl.Boolean,
    "currency": pl.Utf8,
    "parser_version": pl.Utf8,
    "parsed_at": pl.Utf8,
}
XBRL_UNIT_COLUMNS = tuple(XBRL_UNIT_POLARS_SCHEMA)

XBRL_FACT_POLARS_SCHEMA = {
    "fact_ordinal": pl.Int64,
    "concept_qname": pl.Utf8,
    "concept_namespace": pl.Utf8,
    "concept_local_name": pl.Utf8,
    "context_id": pl.Utf8,
    "unit_id": pl.Utf8,
    "currency": pl.Utf8,
    "decimals": pl.Utf8,
    "precision": pl.Utf8,
    "is_nil": pl.Boolean,
    "xml_lang": pl.Utf8,
    "value_kind": pl.Utf8,
    "raw_value": pl.Utf8,
    "numeric_value": pl.Utf8,
    "date_value": pl.Utf8,
    "text_value": pl.Utf8,
    "dimensions": pl.Utf8,
    "is_comparative": pl.Boolean,
    "parser_version": pl.Utf8,
    "parsed_at": pl.Utf8,
}
XBRL_FACT_COLUMNS = tuple(XBRL_FACT_POLARS_SCHEMA)

TAXONOMY_CONCEPT_COLUMNS = (
    "taxonomy_version",
    "concept_qname",
    "concept_namespace",
    "concept_local_name",
    "substitution_group",
    "is_abstract",
    "item_type",
    "balance",
    "period_type",
    "presentation_parent",
    "presentation_order",
    "presentation_role",
    "calculation_parent",
    "calculation_weight",
    "calculation_role",
    "loaded_at",
)
TAXONOMY_LABEL_COLUMNS = (
    "taxonomy_version",
    "concept_qname",
    "language",
    "label_role",
    "label",
    "loaded_at",
)


@dataclass(frozen=True)
class TableContract:
    columns: list[str]
    schema: dict[str, pl.DataType]


@dataclass(frozen=True)
class XbrlRowContract:
    documents: TableContract
    contexts: TableContract
    units: TableContract
    facts: TableContract

    @staticmethod
    def build(
        *,
        document_identity: tuple[str, ...],
        row_identity: tuple[str, ...],
        fact_identity: tuple[str, ...],
        context_extras: tuple[str, ...] = (),
        fact_extras: tuple[str, ...] = (),
    ) -> "XbrlRowContract":
        def _table(
            identity: tuple[str, ...],
            canonical: dict[str, pl.DataType],
            extras: tuple[str, ...] = (),
        ) -> TableContract:
            schema: dict[str, pl.DataType] = {name: pl.Utf8 for name in identity}
            schema.update(canonical)
            schema.update({name: pl.Utf8 for name in extras})
            return TableContract(columns=list(schema), schema=schema)

        return XbrlRowContract(
            documents=_table(document_identity, XBRL_DOCUMENT_POLARS_SCHEMA),
            contexts=_table(row_identity, XBRL_CONTEXT_POLARS_SCHEMA, context_extras),
            units=_table(row_identity, XBRL_UNIT_POLARS_SCHEMA),
            facts=_table(fact_identity, XBRL_FACT_POLARS_SCHEMA, fact_extras),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_xbrl_canonical_tables.py -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/xbrl_common/tables.py tests/test_xbrl_canonical_tables.py
git commit -m "feat(xbrl): canonical row shapes and source row contract"
```

---

### Task 3: Unified extractor — plain XBRL

**Files:**
- Create: `src/dagster_v3/defs/xbrl_common/extractor.py`
- Test: `tests/test_xbrl_extractor.py`

**Interfaces:**
- Consumes: `apply_transform`, `UnknownTransform`, `XBRL_COMMON_PARSER_VERSION` (Task 1); canonical column names (Task 2).
- Produces: `SourceProfile(source_slug, canonical_prefixes, reported_concepts)`; `ExtractedFiling(document, contexts, units, facts, warnings)` where `document` is one dict of canonical document columns (minus counts filled at the end) and the lists hold canonical row dicts; `extract_filing(body: bytes, *, profile: SourceProfile, parsed_at: datetime) -> ExtractedFiling`. Task 4 extends the same file with iXBRL; write plain-XBRL support first with the dispatch stub in place.

Row semantics (both tasks):
- `dimensions` is a JSON array of `[dimension_qname, member_qname, typed_value]`; explicit members have `typed_value == ""`, typed members carry the inner element's text and `member_qname == ""`.
- QNames are canonicalized through `profile.canonical_prefixes` (namespace → prefix), falling back to the element's own prefix.
- `value_kind` for a fact: `"empty"` if nil or blank; `"numeric"` if it has a `unitRef` and parses as a finite Decimal; `"date"` if no unit and the value is an ISO date (or a date transform produced one); else `"text"`.
- `is_comparative` on contexts and facts: the context's effective date (`instant` or `period_end`) differs from the document's `reported_period_end` (resolved via `profile.reported_concepts` after facts are gathered). When no reported period end exists, every context gets `is_comparative=False`.
- `reported_concepts` maps canonical concept qname → one of `reported_entity_id` / `reported_company_name` / `reported_period_start` / `reported_period_end`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_xbrl_extractor.py
import json
from datetime import UTC, datetime

from dagster_v3.defs.xbrl_common.extractor import SourceProfile, extract_filing

PARSED_AT = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)

PROFILE = SourceProfile(
    source_slug="testland",
    canonical_prefixes={
        "http://example.org/met": "t_met",
        "http://example.org/dim": "t_dim",
        "http://example.org/dom": "t_dom",
        "http://www.xbrl.org/2003/iso4217": "iso4217",
    },
    reported_concepts={
        "t_met:entityId": "reported_entity_id",
        "t_met:companyName": "reported_company_name",
        "t_met:periodStart": "reported_period_start",
        "t_met:periodEnd": "reported_period_end",
    },
)

PLAIN_XBRL = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:link="http://www.xbrl.org/2003/linkbase"
      xmlns:xlink="http://www.w3.org/1999/xlink"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:met="http://example.org/met"
      xmlns:dim="http://example.org/dim"
      xmlns:dom="http://example.org/dom"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <link:schemaRef xlink:type="simple" xlink:href="http://example.org/entry.xsd"/>
  <xbrli:context id="cur">
    <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-12-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="prior">
    <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2023-01-01</xbrli:startDate><xbrli:endDate>2023-12-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="cur_dim">
    <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2024-12-31</xbrli:instant></xbrli:period>
    <xbrli:scenario>
      <xbrldi:explicitMember dimension="dim:Segment">dom:Retail</xbrldi:explicitMember>
    </xbrli:scenario>
  </xbrli:context>
  <xbrli:unit id="eur"><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unit>
  <met:entityId contextRef="cur">1234567-8</met:entityId>
  <met:companyName contextRef="cur">Testland Oy</met:companyName>
  <met:periodEnd contextRef="cur">2024-12-31</met:periodEnd>
  <met:revenue contextRef="cur" unitRef="eur" decimals="0">1000000</met:revenue>
  <met:revenue contextRef="prior" unitRef="eur" decimals="0">900000</met:revenue>
  <met:assets contextRef="cur_dim" unitRef="eur" decimals="0">555</met:assets>
  <met:note contextRef="cur" xsi:nil="true"/>
</xbrl>
"""


def test_plain_xbrl_document_fields():
    filing = extract_filing(PLAIN_XBRL, profile=PROFILE, parsed_at=PARSED_AT)
    doc = filing.document
    assert doc["root_name"] == "xbrl"
    assert json.loads(doc["schema_refs"]) == ["http://example.org/entry.xsd"]
    assert doc["taxonomy_entrypoint"] == "http://example.org/entry.xsd"
    assert doc["reported_entity_id"] == "1234567-8"
    assert doc["reported_company_name"] == "Testland Oy"
    assert doc["reported_period_end"] == "2024-12-31"
    assert doc["contexts_count"] == 3
    assert doc["units_count"] == 1
    assert doc["facts_count"] == 7
    assert doc["parser_version"] == "xbrl-common-1.0.0"


def test_plain_xbrl_contexts_and_comparative():
    filing = extract_filing(PLAIN_XBRL, profile=PROFILE, parsed_at=PARSED_AT)
    by_id = {c["context_id"]: c for c in filing.contexts}
    assert by_id["cur"]["period_type"] == "duration"
    assert by_id["cur"]["is_comparative"] is False
    assert by_id["prior"]["is_comparative"] is True
    assert by_id["cur_dim"]["period_type"] == "instant"
    assert by_id["cur_dim"]["is_comparative"] is False
    assert json.loads(by_id["cur_dim"]["dimensions"]) == [["t_dim:Segment", "t_dom:Retail", ""]]


def test_plain_xbrl_facts():
    filing = extract_filing(PLAIN_XBRL, profile=PROFILE, parsed_at=PARSED_AT)
    revenue = [f for f in filing.facts if f["concept_qname"] == "t_met:revenue"]
    assert len(revenue) == 2
    current = next(f for f in revenue if f["context_id"] == "cur")
    assert current["value_kind"] == "numeric"
    assert current["numeric_value"] == "1000000"
    assert current["currency"] == "EUR"
    assert current["is_comparative"] is False
    prior = next(f for f in revenue if f["context_id"] == "prior")
    assert prior["is_comparative"] is True
    nil = next(f for f in filing.facts if f["concept_qname"] == "t_met:note")
    assert nil["is_nil"] is True and nil["value_kind"] == "empty"
    period_end = next(f for f in filing.facts if f["concept_qname"] == "t_met:periodEnd")
    assert period_end["value_kind"] == "date"
    assert period_end["date_value"] == "2024-12-31"
    assert [f["fact_ordinal"] for f in filing.facts] == list(range(1, 8))


def test_malformed_body_yields_empty_filing_with_warning():
    filing = extract_filing(b"this is not xml at all \x00", profile=PROFILE, parsed_at=PARSED_AT)
    assert filing.facts == []
    assert filing.warnings  # at least one warning explains the failure
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_xbrl_extractor.py -q`
Expected: FAIL — extractor module missing.

- [ ] **Step 3: Implement**

```python
# src/dagster_v3/defs/xbrl_common/extractor.py
"""Unified full-fidelity XBRL / iXBRL fact extractor (canonical rows).

Emits canonical row dicts per defs/xbrl_common/tables.py. Source adapters
prepend identity columns and append source-specific derived columns.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from lxml import etree

from dagster_v3.defs.xbrl_common.transforms import (
    XBRL_COMMON_PARSER_VERSION,
    UnknownTransform,
    apply_transform,
)

XBRLI_NS = "http://www.xbrl.org/2003/instance"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
XLINK_NS = "http://www.w3.org/1999/xlink"
XBRLDI_NS = "http://xbrl.org/2006/xbrldi"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XML_LANG_ATTR = "{http://www.w3.org/XML/1998/namespace}lang"
IX_NS = "http://www.xbrl.org/2013/inlineXBRL"
IX_2008_NS = "http://www.xbrl.org/2008/inlineXBRL"
_STRUCTURAL_NS = frozenset({XBRLI_NS, XBRLDI_NS, LINK_NS})

_XML_PARSER = etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class SourceProfile:
    source_slug: str
    canonical_prefixes: dict[str, str]
    reported_concepts: dict[str, str]


@dataclass
class ExtractedFiling:
    document: dict
    contexts: list[dict] = field(default_factory=list)
    units: list[dict] = field(default_factory=list)
    facts: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _canonical_qname(
    namespace: str | None, local: str, element: etree._Element, profile: SourceProfile
) -> str:
    if not namespace:
        return local
    prefix = profile.canonical_prefixes.get(namespace)
    if prefix is None:
        for cand_prefix, cand_ns in (element.nsmap or {}).items():
            if cand_ns == namespace and cand_prefix:
                prefix = cand_prefix
                break
    return f"{prefix}:{local}" if prefix else local


def _canonical_qname_text(value: str, element: etree._Element, profile: SourceProfile) -> str:
    prefix, sep, local = value.strip().partition(":")
    if not sep:
        return value.strip()
    namespace = (element.nsmap or {}).get(prefix)
    return _canonical_qname(namespace, local, element, profile) if namespace else value.strip()


def _iso(parsed_at: datetime) -> str:
    return parsed_at.isoformat()


def _context_rows(root: etree._Element, profile: SourceProfile, parsed_at: datetime) -> list[dict]:
    rows: list[dict] = []
    for element in root.iter(f"{{{XBRLI_NS}}}context"):
        identifier = element.find(f"{{{XBRLI_NS}}}entity/{{{XBRLI_NS}}}identifier")
        period = element.find(f"{{{XBRLI_NS}}}period")
        instant = period.findtext(f"{{{XBRLI_NS}}}instant") if period is not None else None
        start = period.findtext(f"{{{XBRLI_NS}}}startDate") if period is not None else None
        end = period.findtext(f"{{{XBRLI_NS}}}endDate") if period is not None else None
        dimensions: list[list[str]] = []
        for member in element.findall(f".//{{{XBRLDI_NS}}}explicitMember"):
            dimensions.append(
                [
                    _canonical_qname_text(member.get("dimension", ""), member, profile),
                    _canonical_qname_text((member.text or "").strip(), member, profile),
                    "",
                ]
            )
        for member in element.findall(f".//{{{XBRLDI_NS}}}typedMember"):
            typed_value = ""
            for child in member:
                typed_value = "".join(child.itertext()).strip()
                break
            dimensions.append(
                [
                    _canonical_qname_text(member.get("dimension", ""), member, profile),
                    "",
                    typed_value,
                ]
            )
        rows.append(
            {
                "context_id": element.get("id", ""),
                "entity_identifier": (
                    (identifier.text or "").strip() if identifier is not None else ""
                ),
                "entity_scheme": identifier.get("scheme", "") if identifier is not None else "",
                "period_type": (
                    "instant" if instant else "duration" if (start or end) else "none"
                ),
                "instant_date": (instant or "").strip(),
                "period_start": (start or "").strip(),
                "period_end": (end or "").strip(),
                "dimensions": json.dumps(dimensions, ensure_ascii=False),
                "is_comparative": False,
                "parser_version": XBRL_COMMON_PARSER_VERSION,
                "parsed_at": _iso(parsed_at),
            }
        )
    return rows


def _unit_currency(measures: list[str]) -> str:
    if len(measures) != 1:
        return ""
    prefix, sep, code = measures[0].partition(":")
    if sep and prefix.lower() == "iso4217":
        return code.upper()
    return ""


def _unit_rows(root: etree._Element, profile: SourceProfile, parsed_at: datetime) -> list[dict]:
    rows: list[dict] = []
    for element in root.iter(f"{{{XBRLI_NS}}}unit"):
        direct = [
            _canonical_qname_text((m.text or "").strip(), m, profile)
            for m in element.findall(f"{{{XBRLI_NS}}}measure")
        ]
        numerator = [
            _canonical_qname_text((m.text or "").strip(), m, profile)
            for m in element.findall(
                f"{{{XBRLI_NS}}}divide/{{{XBRLI_NS}}}unitNumerator/{{{XBRLI_NS}}}measure"
            )
        ]
        denominator = [
            _canonical_qname_text((m.text or "").strip(), m, profile)
            for m in element.findall(
                f"{{{XBRLI_NS}}}divide/{{{XBRLI_NS}}}unitDenominator/{{{XBRLI_NS}}}measure"
            )
        ]
        rows.append(
            {
                "unit_id": element.get("id", ""),
                "measures": json.dumps(direct, ensure_ascii=False),
                "numerator_measures": json.dumps(numerator, ensure_ascii=False),
                "denominator_measures": json.dumps(denominator, ensure_ascii=False),
                "is_divide": bool(numerator or denominator),
                "currency": _unit_currency(direct),
                "parser_version": XBRL_COMMON_PARSER_VERSION,
                "parsed_at": _iso(parsed_at),
            }
        )
    return rows


def _decimal_or_none(raw: str) -> Decimal | None:
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def _classify_value(
    *, raw_value: str, is_nil: bool, unit_id: str
) -> tuple[str, str, str, str]:
    """Return (value_kind, numeric_value, date_value, text_value)."""
    if is_nil or not raw_value:
        return "empty", "", "", ""
    if unit_id:
        parsed = _decimal_or_none(raw_value)
        if parsed is not None:
            return "numeric", str(parsed), "", ""
        return "text", "", "", raw_value
    if _ISO_DATE_RE.match(raw_value):
        try:
            date.fromisoformat(raw_value)
            return "date", "", raw_value, ""
        except ValueError:
            pass
    return "text", "", "", raw_value


def _fact_row_base(
    *,
    element: etree._Element,
    namespace: str,
    local: str,
    ordinal: int,
    contexts_by_id: dict[str, dict],
    profile: SourceProfile,
    units_by_id: dict[str, dict],
    parsed_at: datetime,
) -> dict:
    context_id = element.get("contextRef", "")
    context = contexts_by_id.get(context_id)
    unit_id = element.get("unitRef") or ""
    unit = units_by_id.get(unit_id)
    return {
        "fact_ordinal": ordinal,
        "concept_qname": _canonical_qname(namespace, local, element, profile),
        "concept_namespace": namespace or "",
        "concept_local_name": local,
        "context_id": context_id,
        "unit_id": unit_id,
        "currency": unit["currency"] if unit else "",
        "decimals": element.get("decimals") or "",
        "precision": element.get("precision") or "",
        "is_nil": (element.get(f"{{{XSI_NS}}}nil") or "").lower() in {"1", "true"},
        "xml_lang": element.get(XML_LANG_ATTR) or "",
        "value_kind": "",
        "raw_value": "",
        "numeric_value": "",
        "date_value": "",
        "text_value": "",
        "dimensions": context["dimensions"] if context else "[]",
        "is_comparative": False,
        "parser_version": XBRL_COMMON_PARSER_VERSION,
        "parsed_at": _iso(parsed_at),
    }


def _plain_fact_rows(
    root: etree._Element,
    profile: SourceProfile,
    contexts_by_id: dict[str, dict],
    units_by_id: dict[str, dict],
    parsed_at: datetime,
) -> list[dict]:
    rows: list[dict] = []
    for element in root.iter():
        if not isinstance(element.tag, str) or element.get("contextRef") is None:
            continue
        qname = etree.QName(element)
        if qname.namespace in _STRUCTURAL_NS or qname.namespace in (IX_NS, IX_2008_NS):
            continue
        row = _fact_row_base(
            element=element,
            namespace=qname.namespace or "",
            local=qname.localname,
            ordinal=len(rows) + 1,
            contexts_by_id=contexts_by_id,
            profile=profile,
            units_by_id=units_by_id,
            parsed_at=parsed_at,
        )
        raw_value = "".join(element.itertext()).strip()
        row["raw_value"] = raw_value
        kind, numeric, date_value, text = _classify_value(
            raw_value=raw_value, is_nil=row["is_nil"], unit_id=row["unit_id"]
        )
        row.update(
            {"value_kind": kind, "numeric_value": numeric, "date_value": date_value, "text_value": text}
        )
        rows.append(row)
    return rows


def _resolve_reported(document: dict, facts: list[dict], profile: SourceProfile) -> None:
    for fact in facts:
        column = profile.reported_concepts.get(fact["concept_qname"])
        if column and not document.get(column):
            document[column] = fact["raw_value"]


def _apply_comparative(document: dict, contexts: list[dict], facts: list[dict]) -> None:
    reported_end = document.get("reported_period_end") or ""
    by_id: dict[str, bool] = {}
    for context in contexts:
        effective = context["instant_date"] or context["period_end"]
        comparative = bool(reported_end and effective and effective != reported_end)
        context["is_comparative"] = comparative
        by_id[context["context_id"]] = comparative
    for fact in facts:
        fact["is_comparative"] = by_id.get(fact["context_id"], False)


def _is_inline(root: etree._Element) -> bool:
    if etree.QName(root).localname.lower() == "html":
        return True
    for element in root.iter():
        if isinstance(element.tag, str) and element.tag.startswith(f"{{{IX_NS}}}"):
            return True
    return False


def extract_filing(
    body: bytes | str, *, profile: SourceProfile, parsed_at: datetime
) -> ExtractedFiling:
    content = body.encode("utf-8") if isinstance(body, str) else body
    document: dict = {
        "xml_sha256": "",
        "xml_size_bytes": len(content),
        "root_name": "",
        "schema_refs": "[]",
        "taxonomy_entrypoint": "",
        "reported_entity_id": "",
        "reported_company_name": "",
        "reported_period_start": "",
        "reported_period_end": "",
        "contexts_count": 0,
        "units_count": 0,
        "facts_count": 0,
        "validation_warnings": "[]",
        "parser_version": XBRL_COMMON_PARSER_VERSION,
        "parsed_at": _iso(parsed_at),
    }
    filing = ExtractedFiling(document=document)
    try:
        root = etree.fromstring(content, parser=_XML_PARSER)
    except etree.XMLSyntaxError as exc:
        root = None
        filing.warnings.append(f"unparseable XML: {exc}")
    if root is None:
        if not filing.warnings:
            filing.warnings.append("unparseable XML: empty parse result")
        document["validation_warnings"] = json.dumps(filing.warnings, ensure_ascii=False)
        return filing

    document["root_name"] = etree.QName(root).localname
    schema_refs = [
        element.get(f"{{{XLINK_NS}}}href") or ""
        for element in root.iter(f"{{{LINK_NS}}}schemaRef")
    ]
    document["schema_refs"] = json.dumps(schema_refs, ensure_ascii=False)
    document["taxonomy_entrypoint"] = schema_refs[0] if schema_refs else ""

    filing.contexts = _context_rows(root, profile, parsed_at)
    filing.units = _unit_rows(root, profile, parsed_at)
    contexts_by_id = {c["context_id"]: c for c in filing.contexts}
    units_by_id = {u["unit_id"]: u for u in filing.units}

    if _is_inline(root):
        filing.facts = _inline_fact_rows(
            root, profile, contexts_by_id, units_by_id, parsed_at, filing.warnings
        )
    else:
        filing.facts = _plain_fact_rows(root, profile, contexts_by_id, units_by_id, parsed_at)

    _resolve_reported(document, filing.facts, profile)
    _apply_comparative(document, filing.contexts, filing.facts)
    document["contexts_count"] = len(filing.contexts)
    document["units_count"] = len(filing.units)
    document["facts_count"] = len(filing.facts)
    document["validation_warnings"] = json.dumps(filing.warnings, ensure_ascii=False)
    return filing


def _inline_fact_rows(
    root: etree._Element,
    profile: SourceProfile,
    contexts_by_id: dict[str, dict],
    units_by_id: dict[str, dict],
    parsed_at: datetime,
    warnings: list[str],
) -> list[dict]:
    # Implemented in Task 4. Present as a stub so the module imports; the
    # plain-XBRL tests never reach it.
    raise NotImplementedError("iXBRL support arrives in Task 4")
```

Note: `xml_sha256` stays `""` at this layer — the adapter owns document identity (Finland computes it from the body, Task 5).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_xbrl_extractor.py -q`
Expected: all 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/xbrl_common/extractor.py tests/test_xbrl_extractor.py
git commit -m "feat(xbrl): unified extractor - plain XBRL path"
```

---

### Task 4: Unified extractor — iXBRL

**Files:**
- Modify: `src/dagster_v3/defs/xbrl_common/extractor.py` (replace the `_inline_fact_rows` stub)
- Test: `tests/test_xbrl_extractor.py` (append)

**Interfaces:**
- Consumes: everything defined in Task 3; `apply_transform`/`UnknownTransform` from Task 1.
- Produces: working `_inline_fact_rows`; `extract_filing` now handles iXBRL end-to-end. No signature changes.

Behavior:
- Facts are `ix:nonFraction`, `ix:nonNumeric`, `ix:fraction` found anywhere (including inside `ix:hidden`); document order via `root.iter`.
- Concept qname comes from the `name` attribute, resolved through the element's nsmap and canonicalized.
- Text value = element text with `ix:exclude` subtrees removed, then `continuedAt` chains followed through `ix:continuation` elements (loop-safe: a visited-id set).
- `format` runs through `apply_transform`; `UnknownTransform` → warning + `value_kind="text"` with the raw text. For `nonFraction`: after transform, `sign="-"` negates and `scale` multiplies by `10**scale`.
- `ix:fraction`: numerator/denominator child values joined as `num/den` in `raw_value`, `value_kind="text"` (fractions are rare; fidelity over precision here, and a warning is recorded).
- Nested fact elements are extracted independently; the outer fact's text includes nested content (documented choice from the spec planning).

- [ ] **Step 1: Append the failing tests**

```python
# append to tests/test_xbrl_extractor.py

IXBRL = b"""<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:ixt="http://www.xbrl.org/inlineXBRL/transformation/2015-02-26"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:link="http://www.xbrl.org/2003/linkbase"
      xmlns:xlink="http://www.w3.org/1999/xlink"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:met="http://example.org/met">
<head><title>t</title></head>
<body>
<div style="display:none">
  <ix:header>
    <ix:hidden>
      <ix:nonNumeric name="met:entityId" contextRef="cur">1234567-8</ix:nonNumeric>
      <ix:nonNumeric name="met:periodEnd" contextRef="cur" format="ixt:date-day-month-year">31.12.2024</ix:nonNumeric>
    </ix:hidden>
    <ix:resources>
      <xbrli:context id="cur">
        <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-12-31</xbrli:endDate></xbrli:period>
      </xbrli:context>
      <xbrli:context id="prior">
        <xbrli:entity><xbrli:identifier scheme="http://example.org/id">1234567-8</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:startDate>2023-01-01</xbrli:startDate><xbrli:endDate>2023-12-31</xbrli:endDate></xbrli:period>
      </xbrli:context>
      <xbrli:unit id="eur"><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unit>
    </ix:resources>
    <ix:references>
      <link:schemaRef xlink:type="simple" xlink:href="http://example.org/entry.xsd"/>
    </ix:references>
  </ix:header>
</div>
<p>Revenue was
  <ix:nonFraction name="met:revenue" contextRef="cur" unitRef="eur"
                  decimals="0" format="ixt:num-comma-decimal" scale="3">1.234,5</ix:nonFraction>
 thousand euro.</p>
<p>Loss:
  <ix:nonFraction name="met:profit" contextRef="cur" unitRef="eur"
                  decimals="0" sign="-" format="ixt:num-dot-decimal">2,500</ix:nonFraction></p>
<p><ix:nonNumeric name="met:description" contextRef="cur" continuedAt="c1">Part one
  <ix:exclude><span>IGNORED</span></ix:exclude></ix:nonNumeric></p>
<p><ix:continuation id="c1"> and part two.</ix:continuation></p>
<p><ix:nonFraction name="met:weird" contextRef="cur" unitRef="eur"
                   format="ixt:num-tolkien">999</ix:nonFraction></p>
</body>
</html>
"""


def test_ixbrl_transform_scale_and_sign():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    revenue = next(f for f in filing.facts if f["concept_qname"] == "t_met:revenue")
    assert revenue["value_kind"] == "numeric"
    assert revenue["numeric_value"] == "1234500.0"
    assert revenue["currency"] == "EUR"
    profit = next(f for f in filing.facts if f["concept_qname"] == "t_met:profit")
    assert profit["numeric_value"] == "-2500"


def test_ixbrl_hidden_and_reported_and_schema_refs():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    assert filing.document["reported_entity_id"] == "1234567-8"
    assert filing.document["reported_period_end"] == "2024-12-31"
    assert filing.document["taxonomy_entrypoint"] == "http://example.org/entry.xsd"
    period_end = next(f for f in filing.facts if f["concept_qname"] == "t_met:periodEnd")
    assert period_end["value_kind"] == "date"
    assert period_end["date_value"] == "2024-12-31"


def test_ixbrl_continuation_and_exclude():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    description = next(f for f in filing.facts if f["concept_qname"] == "t_met:description")
    assert "IGNORED" not in description["text_value"]
    assert "Part one" in description["text_value"]
    assert "and part two." in description["text_value"]


def test_ixbrl_unknown_transform_degrades_to_text_with_warning():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    weird = next(f for f in filing.facts if f["concept_qname"] == "t_met:weird")
    assert weird["value_kind"] == "text"
    assert weird["text_value"] == "999"
    assert any("num-tolkien" in w for w in filing.warnings)


def test_ixbrl_contexts_parsed_from_resources():
    filing = extract_filing(IXBRL, profile=PROFILE, parsed_at=PARSED_AT)
    assert {c["context_id"] for c in filing.contexts} == {"cur", "prior"}
    prior = next(c for c in filing.contexts if c["context_id"] == "prior")
    assert prior["is_comparative"] is True
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `uv run pytest tests/test_xbrl_extractor.py -q`
Expected: Task 3 tests PASS; the 5 new tests FAIL with `NotImplementedError`.

- [ ] **Step 3: Replace the `_inline_fact_rows` stub**

```python
_IX_FACT_TAGS = (
    f"{{{IX_NS}}}nonFraction",
    f"{{{IX_NS}}}nonNumeric",
    f"{{{IX_NS}}}fraction",
)


def _text_excluding(element: etree._Element) -> str:
    parts: list[str] = [element.text or ""]
    for child in element:
        if isinstance(child.tag, str) and child.tag == f"{{{IX_NS}}}exclude":
            parts.append(child.tail or "")
            continue
        parts.append(_text_excluding(child))
        parts.append(child.tail or "")
    return "".join(parts)


def _continued_text(
    element: etree._Element, continuations: dict[str, etree._Element]
) -> str:
    parts = [_text_excluding(element)]
    seen: set[str] = set()
    next_id = element.get("continuedAt")
    while next_id and next_id not in seen:
        seen.add(next_id)
        continuation = continuations.get(next_id)
        if continuation is None:
            break
        parts.append(_text_excluding(continuation))
        next_id = continuation.get("continuedAt")
    return "".join(parts)


def _inline_fact_rows(
    root: etree._Element,
    profile: SourceProfile,
    contexts_by_id: dict[str, dict],
    units_by_id: dict[str, dict],
    parsed_at: datetime,
    warnings: list[str],
) -> list[dict]:
    continuations = {
        element.get("id", ""): element
        for element in root.iter(f"{{{IX_NS}}}continuation")
        if element.get("id")
    }
    rows: list[dict] = []
    for element in root.iter(*_IX_FACT_TAGS):
        name = element.get("name", "")
        prefix, sep, local = name.partition(":")
        namespace = (element.nsmap or {}).get(prefix if sep else None) or ""
        row = _fact_row_base(
            element=element,
            namespace=namespace,
            local=local if sep else name,
            ordinal=len(rows) + 1,
            contexts_by_id=contexts_by_id,
            profile=profile,
            units_by_id=units_by_id,
            parsed_at=parsed_at,
        )
        tag_local = etree.QName(element).localname

        if tag_local == "fraction":
            numerator = element.findtext(f"{{{IX_NS}}}numerator") or ""
            denominator = element.findtext(f"{{{IX_NS}}}denominator") or ""
            raw_value = f"{numerator.strip()}/{denominator.strip()}"
            warnings.append(f"ix:fraction stored as text: {row['concept_qname']}")
            row.update(
                {"raw_value": raw_value, "value_kind": "text", "text_value": raw_value}
            )
            rows.append(row)
            continue

        raw_text = _continued_text(element, continuations).strip()
        row["raw_value"] = raw_text
        fmt = element.get("format")

        if row["is_nil"] or not raw_text:
            row["value_kind"] = "empty"
            rows.append(row)
            continue

        transformed_kind: str | None = None
        transformed_value = raw_text
        if fmt:
            try:
                result = apply_transform(fmt, raw_text)
                transformed_kind, transformed_value = result.kind, result.value
            except UnknownTransform:
                warnings.append(f"unknown ixt transform {fmt!r} on {row['concept_qname']}")
            except ValueError as exc:
                warnings.append(
                    f"ixt transform {fmt!r} failed on {row['concept_qname']}: {exc}"
                )

        if tag_local == "nonFraction":
            numeric = (
                _decimal_or_none(transformed_value)
                if transformed_kind in (None, "numeric")
                else None
            )
            if numeric is None and transformed_kind is None:
                numeric = _decimal_or_none(raw_text)
            if numeric is not None:
                scale = element.get("scale")
                if scale:
                    try:
                        numeric = numeric * (Decimal(10) ** int(scale))
                    except (ValueError, InvalidOperation):
                        warnings.append(
                            f"invalid scale {scale!r} on {row['concept_qname']}"
                        )
                if element.get("sign") == "-":
                    numeric = -numeric
                row.update({"value_kind": "numeric", "numeric_value": str(numeric)})
            else:
                row.update({"value_kind": "text", "text_value": raw_text})
        else:  # nonNumeric
            if transformed_kind == "date":
                row.update({"value_kind": "date", "date_value": transformed_value})
            elif transformed_kind == "empty":
                row["value_kind"] = "empty"
            elif transformed_kind == "boolean":
                row.update({"value_kind": "text", "text_value": transformed_value})
            else:
                kind, numeric_v, date_v, text_v = _classify_value(
                    raw_value=transformed_value, is_nil=False, unit_id=""
                )
                row.update(
                    {
                        "value_kind": kind,
                        "numeric_value": numeric_v,
                        "date_value": date_v,
                        "text_value": text_v,
                    }
                )
        rows.append(row)
    return rows
```

- [ ] **Step 4: Run the full extractor suite**

Run: `uv run pytest tests/test_xbrl_extractor.py tests/test_xbrl_transforms.py -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/xbrl_common/extractor.py tests/test_xbrl_extractor.py
git commit -m "feat(xbrl): unified extractor - iXBRL path (hidden, continuation, exclude, transforms)"
```

---

### Task 5: Finland adapter

**Files:**
- Create: `src/dagster_v3/defs/finland_xbrl/unified_adapter.py`
- Test: `tests/test_finland_unified_adapter.py`

**Interfaces:**
- Consumes: `extract_filing`, `SourceProfile` (Tasks 3–4); `XbrlRowContract` (Task 2); `ParsedStatement` and `statement_key_for` from `dagster_v3.defs.finland_xbrl.parser`; table-name constants from `dagster_v3.defs.finland_xbrl.tables`.
- Produces:
  - `FINLAND_PROFILE: SourceProfile`
  - `FINLAND_UNIFIED_CONTRACT: XbrlRowContract` — identity + canonical + Finland extras (`mcy_member_code`, `ref_member_code` on contexts and facts).
  - `parse_statement_xml_unified(*, business_id, financial_date, registration_date, source_url, xml_object_key, source_run_id, body, parsed_at) -> ParsedStatement` — same signature as the legacy `parse_statement_xml`, so the existing drivers accept it via their `parser=` parameter. Its `rows_by_table` uses the same table-name keys (`tables.STATEMENT_DOCUMENTS_TABLE`, etc.) but rows are in the unified contract's shape.

Finland specifics:
- `FINLAND_PROFILE.canonical_prefixes` = the existing `CANONICAL_PREFIXES` from `finland_xbrl/parser.py` (same URIs).
- `reported_concepts` = `{"fi_met:si289": "reported_entity_id", "fi_met:si168": "reported_company_name", "fi_met:di120": "reported_period_start", "fi_met:di121": "reported_period_end"}`.
- `mcy_member_code` / `ref_member_code` derived from the canonical `dimensions` JSON: the member whose dimension qname is `MCY`/`REF` or ends with `:MCY`/`:REF`.
- Document identity columns: `statement_key, source_run_id, business_id, financial_date, registration_date, source_url, xml_object_key` (in this order). Context/unit identity: `statement_key`. Fact identity: `statement_key, business_id, financial_date`.
- The adapter computes `xml_sha256` from `body` and `statement_key` via the existing `statement_key_for`.
- Comparative fallback: when the extractor found no `reported_period_end`, use `financial_date` (matches legacy behavior) — apply by re-running the comparative pass with the fallback before building rows.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_finland_unified_adapter.py
import json
from datetime import UTC, datetime

from dagster_v3.defs.finland_xbrl import parser as legacy_parser
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.unified_adapter import (
    FINLAND_UNIFIED_CONTRACT,
    parse_statement_xml_unified,
)

PARSED_AT = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)

FINLAND_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met"
      xmlns:fi_dim="http://www.suomi.fi/xbrl/crr/dict/dim"
      xmlns:fi_MC="http://www.suomi.fi/xbrl/crr/dict/dom/MC">
  <xbrli:context id="cur">
    <xbrli:entity><xbrli:identifier scheme="http://www.prh.fi">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-12-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="cur_mc">
    <xbrli:entity><xbrli:identifier scheme="http://www.prh.fi">1234567-8</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-12-31</xbrli:endDate></xbrli:period>
    <xbrli:scenario>
      <xbrldi:explicitMember dimension="fi_dim:MCY">fi_MC:x673</xbrldi:explicitMember>
    </xbrli:scenario>
  </xbrli:context>
  <xbrli:unit id="eur"><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unit>
  <fi_met:si289 contextRef="cur">1234567-8</fi_met:si289>
  <fi_met:si168 contextRef="cur">Testi Oy</fi_met:si168>
  <fi_met:di121 contextRef="cur">2024-12-31</fi_met:di121>
  <fi_met:md103 contextRef="cur_mc" unitRef="eur" decimals="0">500000</fi_met:md103>
</xbrl>
"""

KWARGS = dict(
    business_id="1234567-8",
    financial_date="2024-12-31",
    registration_date="2025-04-01",
    source_url="https://example.fi/statement",
    xml_object_key="finland/xml/1234567-8.xml",
    source_run_id="run-1",
    body=FINLAND_XML,
    parsed_at=PARSED_AT,
)


def test_unified_rows_have_contract_columns():
    parsed = parse_statement_xml_unified(**KWARGS)
    doc = parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE][0]
    assert list(doc) == FINLAND_UNIFIED_CONTRACT.documents.columns
    fact = parsed.rows_by_table[tables.FACTS_TABLE][0]
    assert list(fact) == FINLAND_UNIFIED_CONTRACT.facts.columns


def test_unified_document_identity_and_reported():
    parsed = parse_statement_xml_unified(**KWARGS)
    doc = parsed.rows_by_table[tables.STATEMENT_DOCUMENTS_TABLE][0]
    assert doc["business_id"] == "1234567-8"
    assert doc["reported_entity_id"] == "1234567-8"
    assert doc["reported_company_name"] == "Testi Oy"
    assert doc["reported_period_end"] == "2024-12-31"
    assert doc["statement_key"] == legacy_parser.statement_key_for(
        "1234567-8", "2024-12-31", "2025-04-01", doc["xml_sha256"]
    )


def test_unified_mcy_member_derived_from_dimensions():
    parsed = parse_statement_xml_unified(**KWARGS)
    revenue = next(
        f for f in parsed.rows_by_table[tables.FACTS_TABLE]
        if f["concept_qname"] == "fi_met:md103"
    )
    assert revenue["mcy_member_code"] == "fi_MC:x673"
    assert revenue["value_kind"] == "numeric"
    assert revenue["numeric_value"] == "500000"
    assert revenue["currency"] == "EUR"


def test_mini_parity_against_legacy_parser():
    """Same input through both parsers: same fact count and identical
    (concept_qname, context_id, numeric_value) triples for numeric facts."""
    unified = parse_statement_xml_unified(**KWARGS)
    legacy = legacy_parser.parse_statement_xml(**KWARGS)

    def triples(rows):
        return sorted(
            (r["concept_qname"], r["context_id"], r["numeric_value"])
            for r in rows
            if r["value_kind"] == "numeric"
        )

    unified_facts = unified.rows_by_table[tables.FACTS_TABLE]
    legacy_facts = legacy.rows_by_table[tables.FACTS_TABLE]
    assert len(unified_facts) == len(legacy_facts)
    assert triples(unified_facts) == triples(legacy_facts)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_finland_unified_adapter.py -q`
Expected: FAIL — `unified_adapter` missing.

- [ ] **Step 3: Implement**

```python
# src/dagster_v3/defs/finland_xbrl/unified_adapter.py
"""Finland adapter: unified extractor behind the legacy StatementParser signature."""

import hashlib
import json
from datetime import datetime

from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.parser import (
    CANONICAL_PREFIXES,
    ParsedStatement,
    statement_key_for,
)
from dagster_v3.defs.xbrl_common.extractor import SourceProfile, extract_filing
from dagster_v3.defs.xbrl_common.tables import XbrlRowContract

FINLAND_PROFILE = SourceProfile(
    source_slug="finland_prh",
    canonical_prefixes=dict(CANONICAL_PREFIXES),
    reported_concepts={
        "fi_met:si289": "reported_entity_id",
        "fi_met:si168": "reported_company_name",
        "fi_met:di120": "reported_period_start",
        "fi_met:di121": "reported_period_end",
    },
)

FINLAND_UNIFIED_CONTRACT = XbrlRowContract.build(
    document_identity=(
        "statement_key",
        "source_run_id",
        "business_id",
        "financial_date",
        "registration_date",
        "source_url",
        "xml_object_key",
    ),
    row_identity=("statement_key",),
    fact_identity=("statement_key", "business_id", "financial_date"),
    context_extras=("mcy_member_code", "ref_member_code"),
    fact_extras=("mcy_member_code", "ref_member_code"),
)


def _member_for(dimensions_json: str, suffix: str) -> str:
    for dimension_qname, member_qname, _typed in json.loads(dimensions_json):
        if dimension_qname == suffix or dimension_qname.endswith(f":{suffix}"):
            return member_qname
    return ""


def parse_statement_xml_unified(
    *,
    business_id: str,
    financial_date: str,
    registration_date: str | None,
    source_url: str,
    xml_object_key: str,
    source_run_id: str,
    body: bytes,
    parsed_at: datetime,
) -> ParsedStatement:
    filing = extract_filing(body, profile=FINLAND_PROFILE, parsed_at=parsed_at)
    xml_sha256 = hashlib.sha256(body).hexdigest()
    statement_key = statement_key_for(
        business_id, financial_date, registration_date or "", xml_sha256
    )

    if not filing.document["reported_period_end"]:
        # Legacy behavior: fall back to the requested financial_date for
        # comparative flagging when the filing does not report a period end.
        reported_end = financial_date
        by_id: dict[str, bool] = {}
        for context in filing.contexts:
            effective = context["instant_date"] or context["period_end"]
            context["is_comparative"] = bool(effective and effective != reported_end)
            by_id[context["context_id"]] = context["is_comparative"]
        for fact in filing.facts:
            fact["is_comparative"] = by_id.get(fact["context_id"], False)

    document = {
        "statement_key": statement_key,
        "source_run_id": source_run_id,
        "business_id": business_id,
        "financial_date": financial_date,
        "registration_date": registration_date or "",
        "source_url": source_url,
        "xml_object_key": xml_object_key,
        **filing.document,
        "xml_sha256": xml_sha256,
    }
    document = {name: document[name] for name in FINLAND_UNIFIED_CONTRACT.documents.columns}

    contexts = []
    for row in filing.contexts:
        contexts.append(
            {
                "statement_key": statement_key,
                **row,
                "mcy_member_code": _member_for(row["dimensions"], "MCY"),
                "ref_member_code": _member_for(row["dimensions"], "REF"),
            }
        )
    units = [{"statement_key": statement_key, **row} for row in filing.units]
    facts = []
    for row in filing.facts:
        facts.append(
            {
                "statement_key": statement_key,
                "business_id": business_id,
                "financial_date": financial_date,
                **row,
                "mcy_member_code": _member_for(row["dimensions"], "MCY"),
                "ref_member_code": _member_for(row["dimensions"], "REF"),
            }
        )

    return ParsedStatement(
        statement_key=statement_key,
        rows_by_table={
            tables.STATEMENT_DOCUMENTS_TABLE: [document],
            tables.CONTEXTS_TABLE: contexts,
            tables.UNITS_TABLE: units,
            tables.FACTS_TABLE: facts,
        },
        warnings=list(filing.warnings),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_finland_unified_adapter.py -q`
Expected: all PASS. If the mini-parity test fails, diff the triples — the unified extractor must match the legacy parser on this fixture; fix the extractor (not the test) unless the difference is a documented improvement (then assert the improved value and note it in the test).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/unified_adapter.py tests/test_finland_unified_adapter.py
git commit -m "feat(finland-xbrl): unified extractor adapter with legacy parser signature"
```

---

### Task 6: Parameterized parse driver + unified parse assets

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/data_snapshot_xml_duckdb.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/data_daily_xml_duckdb.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/jobs.py`
- Test: `tests/test_finland_unified_assets.py`

**Interfaces:**
- Consumes: `parse_statement_xml_unified`, `FINLAND_UNIFIED_CONTRACT` (Task 5); `XbrlRowContract`/`TableContract` (Task 2).
- Produces: `materialize_data_snapshot_xml_duckdb(..., row_contract: XbrlRowContract | None = None)` (None → legacy `tables.*` columns/schemas, preserving current behavior); new path helpers `xml_snapshot_unified_duckdb_path(partition_key)` / `xml_daily_unified_duckdb_path(partition_key)` rooted at `data/finland_xbrl/xml_snapshot_unified_duckdb` and `data/finland_xbrl/xml_daily_unified_duckdb`; `list_xml_unified_duckdb_paths()`; new assets `data_snapshot_xml_unified_duckdb` (XML_SNAPSHOT_PARTITIONS) and `data_daily_xml_unified_duckdb` (DAILY_PARTITIONS), both `pool=FINLAND_XBRL_DUCKDB_POOL`, `deps` on the same upstream raw assets as their legacy twins, calling the same drivers with `parser=parse_statement_xml_unified` and `row_contract=FINLAND_UNIFIED_CONTRACT`.

Implementation notes:
1. In `materialize_data_snapshot_xml_duckdb` (and the daily equivalent in `data_daily_xml_duckdb.py` — read it first; it mirrors the snapshot driver), replace each hardcoded `tables.X_COLUMNS`/`tables.X_POLARS_SCHEMA` pair with values resolved once at the top:
   ```python
   documents_contract = row_contract.documents if row_contract else TableContract(
       columns=tables.STATEMENT_DOCUMENTS_COLUMNS, schema=tables.STATEMENT_DOCUMENTS_POLARS_SCHEMA
   )
   # ...same for contexts/units/facts...
   ```
   then use `documents_contract.columns` / `.schema` everywhere below. Signature gains `row_contract: XbrlRowContract | None = None`. Do the same for `run_finland_xbrl_parse` only if the daily driver routes through it (check; modify whichever function the daily asset actually calls).
2. New assets are thin wrappers copying their legacy twin exactly, changing only: asset name (`data_snapshot_xml_unified_duckdb`), description, `duckdb_path=xml_snapshot_unified_duckdb_path(...)`, `temp_dir` (`.../xml_snapshot_unified_parse_tmp/...`), `parser=parse_statement_xml_unified`, `row_contract=FINLAND_UNIFIED_CONTRACT`.
3. Register both new assets in `assets/__init__.py` (`defs = dg.Definitions(assets=[...])` list and `__all__`).
4. In `jobs.py`, add `"data_daily_xml_unified_duckdb"` to `finland_xbrl_incremental_job`'s selection (keeps the unified duckdbs current daily during the parity window) and add `"data_snapshot_xml_unified_duckdb"` to `finland_xbrl_xml_snapshot_job`'s selection.
5. **No `from __future__ import annotations`** in these asset modules.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_finland_unified_assets.py
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    materialize_data_snapshot_xml_duckdb,
)
from dagster_v3.defs.finland_xbrl.unified_adapter import (
    FINLAND_UNIFIED_CONTRACT,
    parse_statement_xml_unified,
)
from tests.test_finland_unified_adapter import FINLAND_XML


class FakeObjectStore:
    def __init__(self, objects: dict[str, bytes]):
        self._objects = objects

    def exists(self, key: str, *, bucket: str) -> bool:
        return key in self._objects

    def read_bytes(self, key: str, *, bucket: str) -> bytes:
        return self._objects[key]


MANIFEST = (
    '{"business_id": "1234567-8", "financial_date": "2024-12-31", '
    '"registration_date": "2025-04-01", "source_url": "https://example.fi", '
    '"xml_object_key": "xml/1.xml"}\n'
)


def test_unified_parse_writes_contract_columns(tmp_path: Path):
    from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
        xml_snapshot_manifest_key,
        xml_snapshot_success_key,
    )

    manifest_key = xml_snapshot_manifest_key("2024-01-01", "2024-12-31")
    success_key = xml_snapshot_success_key("2024-01-01", "2024-12-31")
    store = FakeObjectStore(
        {
            manifest_key: MANIFEST.encode(),
            success_key: b"ok",
            "xml/1.xml": FINLAND_XML,
        }
    )
    duckdb_path = tmp_path / "unified" / "data.duckdb"
    materialize_data_snapshot_xml_duckdb(
        partition_key="2024-01",
        registered_date_start="2024-01-01",
        registered_date_end="2024-12-31",
        object_store=store,
        duckdb_path=duckdb_path,
        temp_dir=tmp_path / "tmp",
        run_id="run-1",
        parser=parse_statement_xml_unified,
        row_contract=FINLAND_UNIFIED_CONTRACT,
    )
    with duckdb.connect(str(duckdb_path)) as connection:
        fact_columns = [
            row[0]
            for row in connection.execute(
                "select column_name from information_schema.columns "
                "where table_name = 'facts' order by ordinal_position"
            ).fetchall()
        ]
        assert fact_columns == FINLAND_UNIFIED_CONTRACT.facts.columns
        count = connection.execute("select count(*) from facts").fetchone()[0]
        assert count > 0


def test_legacy_call_without_contract_unchanged(tmp_path: Path):
    from dagster_v3.defs.finland_xbrl import tables
    from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
        xml_snapshot_manifest_key,
        xml_snapshot_success_key,
    )

    manifest_key = xml_snapshot_manifest_key("2024-01-01", "2024-12-31")
    success_key = xml_snapshot_success_key("2024-01-01", "2024-12-31")
    store = FakeObjectStore(
        {
            manifest_key: MANIFEST.encode(),
            success_key: b"ok",
            "xml/1.xml": FINLAND_XML,
        }
    )
    duckdb_path = tmp_path / "legacy" / "data.duckdb"
    materialize_data_snapshot_xml_duckdb(
        partition_key="2024-01",
        registered_date_start="2024-01-01",
        registered_date_end="2024-12-31",
        object_store=store,
        duckdb_path=duckdb_path,
        temp_dir=tmp_path / "tmp2",
        run_id="run-1",
    )
    with duckdb.connect(str(duckdb_path)) as connection:
        fact_columns = [
            row[0]
            for row in connection.execute(
                "select column_name from information_schema.columns "
                "where table_name = 'facts' order by ordinal_position"
            ).fetchall()
        ]
        assert fact_columns == tables.FACTS_COLUMNS
```

Adjust `MANIFEST`/manifest reading to the real `read_xml_snapshot_manifest_rows` format — read that function first (`assets/data_snapshot_xml_duckdb.py:247`) and encode the manifest exactly as it expects (it may be CSV or JSONL; match it). The test must go through the real manifest reader, not around it.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_finland_unified_assets.py -q`
Expected: FAIL — `row_contract` is not a parameter yet.

- [ ] **Step 3: Implement** (per Implementation notes above)

- [ ] **Step 4: Run tests + defs check + existing suite**

Run: `uv run pytest tests/test_finland_unified_assets.py tests/test_finland_xbrl_parsed_assets.py tests/test_finland_xbrl_assets.py -q && uv run dg check defs`
Expected: all PASS (legacy tests prove the default path is unchanged); `dg check defs` OK.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/finland_xbrl/assets/data_snapshot_xml_duckdb.py \
        src/dagster_v3/defs/finland_xbrl/assets/data_daily_xml_duckdb.py \
        src/dagster_v3/defs/finland_xbrl/assets/__init__.py \
        src/dagster_v3/defs/finland_xbrl/assets/jobs.py \
        tests/test_finland_unified_assets.py
git commit -m "feat(finland-xbrl): unified parse assets via parameterized row contract"
```

---

### Task 7: `_next` ClickHouse tables + unified export asset

**Files:**
- Create: `../../clickhouse/migrations/000364_corpscout_fi_xbrl_unified_next_tables.up.sql` and `.down.sql` (path relative to `dagster_v3/`; check the ledger tail first per Global Constraints)
- Create: `src/dagster_v3/defs/finland_xbrl/unified_clickhouse.py`
- Create: `src/dagster_v3/defs/finland_xbrl/assets/unified_publish.py`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`, `tests/test_clickhouse_migrations.py`
- Test: `tests/test_finland_unified_assets.py` (append)

**Interfaces:**
- Consumes: `FINLAND_UNIFIED_CONTRACT` (Task 5), unified duckdb paths (Task 6), `ClickhouseResource` patterns from `finland_xbrl/clickhouse.py` (reuse its `_replace_clickhouse_table_with_rows` approach — copy the helper, don't import private functions across modules if the codebase style forbids it; it doesn't — importing from the sibling module is fine here since both are `finland_xbrl`).
- Produces: asset `fi_xbrl_unified_clickhouse` (unpartitioned, `pool=FINLAND_XBRL_DUCKDB_POOL`, deps on both unified parse assets) reading ALL unified parse duckdbs and replacing four CH tables: `fi_xbrl_documents_next`, `fi_xbrl_contexts_next`, `fi_xbrl_units_next`, `fi_xbrl_facts_next`. Column-order contract test pins code to migration.

Migration `000364_corpscout_fi_xbrl_unified_next_tables.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_documents_next
(
    statement_key String,
    source_run_id String,
    business_id String,
    financial_date Date,
    registration_date Nullable(Date),
    source_url String,
    xml_object_key String,
    xml_sha256 String,
    xml_size_bytes UInt64,
    root_name LowCardinality(String),
    schema_refs String,
    taxonomy_entrypoint String,
    reported_entity_id String,
    reported_company_name String,
    reported_period_start Nullable(Date),
    reported_period_end Nullable(Date),
    contexts_count UInt32,
    units_count UInt32,
    facts_count UInt32,
    validation_warnings String,
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (business_id, financial_date, statement_key);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_contexts_next
(
    statement_key String,
    context_id String,
    entity_identifier String,
    entity_scheme String,
    period_type LowCardinality(String),
    instant_date Nullable(Date),
    period_start Nullable(Date),
    period_end Nullable(Date),
    dimensions String,
    is_comparative UInt8,
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC'),
    mcy_member_code String,
    ref_member_code String
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (statement_key, context_id);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_units_next
(
    statement_key String,
    unit_id String,
    measures String,
    numerator_measures String,
    denominator_measures String,
    is_divide UInt8,
    currency LowCardinality(String),
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (statement_key, unit_id);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_facts_next
(
    statement_key String,
    business_id String,
    financial_date Date,
    fact_ordinal UInt32,
    concept_qname LowCardinality(String),
    concept_namespace String,
    concept_local_name LowCardinality(String),
    context_id String,
    unit_id String,
    currency LowCardinality(String),
    decimals String,
    precision String,
    is_nil UInt8,
    xml_lang LowCardinality(String),
    value_kind LowCardinality(String),
    raw_value String,
    numeric_value Nullable(Decimal(38, 6)),
    date_value Nullable(Date),
    text_value String,
    dimensions String,
    is_comparative UInt8,
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC'),
    mcy_member_code String,
    ref_member_code String
)
ENGINE = ReplacingMergeTree(parsed_at)
PARTITION BY toYYYYMM(financial_date)
ORDER BY (business_id, financial_date, concept_qname, fact_ordinal);
```

Down migration: `DROP TABLE IF EXISTS` for the four tables.

`unified_clickhouse.py` responsibilities (full implementation required, modeled on `clickhouse.py`'s `clickhouse_fact_row`/`_replace_clickhouse_table_with_rows`):
- `UNIFIED_DOCUMENTS_CLICKHOUSE_COLUMNS` etc. — tuples matching the migration column order exactly.
- Row converters string→typed: dates via `date.fromisoformat` (empty → None only for Nullable columns; non-nullable `financial_date` must always parse — raise on empty), `numeric_value` via `Decimal(...).quantize(Decimal("0.000001"))`, booleans → `1/0` ints, `parsed_at` via `datetime.fromisoformat`.
- `export_finland_unified_clickhouse(*, clickhouse, documents, contexts, units, facts) -> dict[str, int]` — per table: create `..._stage` clone (`CREATE TABLE ... AS ...`), insert rows in batches of 50_000, `EXCHANGE TABLES`, drop stage; **raise `ValueError` when `facts` is empty** (refuse to blank a populated table); return row counts.

Asset `fi_xbrl_unified_clickhouse` in `assets/unified_publish.py`: reads all unified duckdb files (`list_xml_unified_duckdb_paths()`), concatenates table rows (same reader approach as `read_xml_parse_duckdb_rows` — write `read_xml_unified_duckdb_rows` beside it using the contract's columns), calls the exporter, returns `MaterializeResult` with row counts. `deps=[data_snapshot_xml_unified_duckdb, data_daily_xml_unified_duckdb]`.

- [ ] **Step 1: Append failing tests** — (a) contract test: parse the migration file with `re.findall(r"^\s{4}(\w+)", ...)` per CREATE TABLE block and assert equality with `UNIFIED_FACTS_CLICKHOUSE_COLUMNS` etc. (mirror the style of existing migration-pinning tests — see `tests/test_se_companies_serving_mv.py` for the drift-pin pattern); (b) converter test: one unified fact row dict → typed tuple (Decimal value, date, ints for booleans); (c) empty-facts export raises `ValueError` (fake clickhouse client).
- [ ] **Step 2: Run** `uv run pytest tests/test_finland_unified_assets.py -q` — new tests FAIL.
- [ ] **Step 3: Write migration files; append `"000364_corpscout_fi_xbrl_unified_next_tables"` to `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`.**
- [ ] **Step 4: Implement `unified_clickhouse.py` + `assets/unified_publish.py`; register asset in `assets/__init__.py`; add `"fi_xbrl_unified_clickhouse"` to `finland_xbrl_incremental_job` and `finland_xbrl_publish_job` selections in `jobs.py`.**
- [ ] **Step 5: Run** `uv run pytest tests/test_finland_unified_assets.py tests/test_clickhouse_migrations.py -q && uv run dg check defs` — all PASS.
- [ ] **Step 6: Commit**

```bash
git add ../../clickhouse/migrations/000364_corpscout_fi_xbrl_unified_next_tables.up.sql \
        ../../clickhouse/migrations/000364_corpscout_fi_xbrl_unified_next_tables.down.sql \
        src/dagster_v3/defs/finland_xbrl/unified_clickhouse.py \
        src/dagster_v3/defs/finland_xbrl/assets/unified_publish.py \
        src/dagster_v3/defs/finland_xbrl/assets/__init__.py \
        src/dagster_v3/defs/finland_xbrl/assets/jobs.py \
        tests/test_finland_unified_assets.py tests/test_clickhouse_migrations.py
git commit -m "feat(finland-xbrl): _next ClickHouse tables and unified export asset"
```

---

### Task 8: Parity harness + Finland parity asset

**Files:**
- Create: `src/dagster_v3/defs/xbrl_common/parity.py`
- Create: `src/dagster_v3/defs/finland_xbrl/assets/parity.py`
- Create: `../../clickhouse/migrations/000365_corpscout_fi_xbrl_parity_report.up.sql` / `.down.sql`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`, `tests/test_clickhouse_migrations.py`
- Test: `tests/test_xbrl_parity.py`

**Interfaces:**
- Produces (in `xbrl_common/parity.py`):

```python
@dataclass(frozen=True)
class ParityResult:
    document_key: str
    status: str           # "match" | "explained" | "mismatch"
    old_fact_count: int
    new_fact_count: int
    value_mismatches: int
    missing_in_new: int
    missing_in_old: int
    details: str          # JSON: up to 20 example diffs

def compare_document_facts(
    *,
    document_key: str,
    old_facts: list[dict],
    new_facts: list[dict],
    explained_rules: list[Callable[[dict], bool]] = (),
) -> ParityResult
```

Comparison semantics: numeric facts only (`value_kind == "numeric"`), keyed by `(concept_qname, context_id, mcy_member_code or "", ref_member_code or "")`; values compared as `Decimal` with equality after normalization (`Decimal("500000") == Decimal("500000.0")` — compare `a.compare(b) == 0`). A fact key present on one side only counts as missing; a shared key with unequal values counts as a value mismatch unless an `explained_rules` predicate accepts the new-side fact dict (rules encode documented improvements, e.g. a transform fixing a mangled value). `status = "match"` if no diffs, `"explained"` if all diffs are rule-accepted, else `"mismatch"`.

- Finland asset `fi_xbrl_parity` (in `assets/parity.py`): unpartitioned, `pool=FINLAND_XBRL_DUCKDB_POOL`, `deps=[data_snapshot_xml_unified_duckdb, data_daily_xml_unified_duckdb]`. It loads facts per `statement_key` from BOTH duckdb sets (legacy `list_xml_parse_duckdb_paths()` and unified `list_xml_unified_duckdb_paths()`), runs `compare_document_facts` per statement, and replaces `corpscout.fi_xbrl_parity_report` (stage+EXCHANGE, same helper pattern as Task 7). `explained_rules` starts empty (`FINLAND_EXPLAINED_RULES: list = []`) — rules are added by hand during the Task 10 review as differences are understood. Asset metadata: total/match/explained/mismatch counts.

Migration `000365_corpscout_fi_xbrl_parity_report.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_parity_report
(
    document_key String,
    status LowCardinality(String),
    old_fact_count UInt32,
    new_fact_count UInt32,
    value_mismatches UInt32,
    missing_in_new UInt32,
    missing_in_old UInt32,
    details String,
    compared_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(compared_at)
ORDER BY (document_key);
```

- [ ] **Step 1: Write failing tests** — synthetic old/new fact lists covering: identical → `match`; value changed → `mismatch` with 1 `value_mismatches` and a `details` entry naming the key; value changed but rule accepts → `explained`; key only in old → `missing_in_new`; `Decimal` normalization (`"500000"` vs `"500000.000000"` → match).

```python
# tests/test_xbrl_parity.py
import json

from dagster_v3.defs.xbrl_common.parity import compare_document_facts


def _fact(concept, context, value, mcy=""):
    return {
        "concept_qname": concept, "context_id": context, "value_kind": "numeric",
        "numeric_value": value, "mcy_member_code": mcy, "ref_member_code": "",
    }


def test_identical_facts_match():
    old = [_fact("fi_met:md103", "cur", "500000", "fi_MC:x673")]
    new = [_fact("fi_met:md103", "cur", "500000.000000", "fi_MC:x673")]
    result = compare_document_facts(document_key="d1", old_facts=old, new_facts=new)
    assert result.status == "match"


def test_value_mismatch_reported():
    old = [_fact("fi_met:md103", "cur", "500000")]
    new = [_fact("fi_met:md103", "cur", "999")]
    result = compare_document_facts(document_key="d1", old_facts=old, new_facts=new)
    assert result.status == "mismatch"
    assert result.value_mismatches == 1
    assert "fi_met:md103" in json.loads(result.details)[0]["key"]


def test_explained_rule_downgrades_mismatch():
    old = [_fact("fi_met:md103", "cur", "500000")]
    new = [_fact("fi_met:md103", "cur", "999")]
    rule = lambda fact: fact["concept_qname"] == "fi_met:md103"
    result = compare_document_facts(
        document_key="d1", old_facts=old, new_facts=new, explained_rules=[rule]
    )
    assert result.status == "explained"


def test_missing_keys_counted():
    old = [_fact("fi_met:a", "cur", "1"), _fact("fi_met:b", "cur", "2")]
    new = [_fact("fi_met:a", "cur", "1"), _fact("fi_met:c", "cur", "3")]
    result = compare_document_facts(document_key="d1", old_facts=old, new_facts=new)
    assert result.missing_in_new == 1
    assert result.missing_in_old == 1
    assert result.status == "mismatch"
```

- [ ] **Step 2: Run** `uv run pytest tests/test_xbrl_parity.py -q` — FAIL (module missing).
- [ ] **Step 3: Implement `xbrl_common/parity.py`**

```python
# src/dagster_v3/defs/xbrl_common/parity.py
"""Old-vs-new fact parity for extractor migrations (numeric facts only)."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

_MAX_DETAILS = 20


@dataclass(frozen=True)
class ParityResult:
    document_key: str
    status: str
    old_fact_count: int
    new_fact_count: int
    value_mismatches: int
    missing_in_new: int
    missing_in_old: int
    details: str


def _key(fact: dict) -> tuple[str, str, str, str]:
    return (
        fact["concept_qname"],
        fact["context_id"],
        fact.get("mcy_member_code") or "",
        fact.get("ref_member_code") or "",
    )


def _numeric_by_key(facts: list[dict]) -> dict[tuple[str, str, str, str], str]:
    return {
        _key(fact): fact["numeric_value"]
        for fact in facts
        if fact.get("value_kind") == "numeric"
    }


def _values_equal(old: str, new: str) -> bool:
    try:
        return Decimal(old).compare(Decimal(new)) == 0
    except InvalidOperation:
        return old == new


def compare_document_facts(
    *,
    document_key: str,
    old_facts: list[dict],
    new_facts: list[dict],
    explained_rules: Sequence[Callable[[dict], bool]] = (),
) -> ParityResult:
    old_by_key = _numeric_by_key(old_facts)
    new_by_key = _numeric_by_key(new_facts)
    new_fact_by_key = {
        _key(fact): fact for fact in new_facts if fact.get("value_kind") == "numeric"
    }
    details: list[dict] = []
    value_mismatches = 0
    unexplained = 0

    for key, old_value in old_by_key.items():
        if key not in new_by_key:
            unexplained += 1
            if len(details) < _MAX_DETAILS:
                details.append({"key": ":".join(key), "old": old_value, "new": None})
            continue
        new_value = new_by_key[key]
        if not _values_equal(old_value, new_value):
            value_mismatches += 1
            accepted = any(rule(new_fact_by_key[key]) for rule in explained_rules)
            if not accepted:
                unexplained += 1
            if len(details) < _MAX_DETAILS:
                details.append(
                    {"key": ":".join(key), "old": old_value, "new": new_value,
                     "explained": accepted}
                )

    missing_in_new = sum(1 for key in old_by_key if key not in new_by_key)
    missing_in_old = sum(1 for key in new_by_key if key not in old_by_key)
    for key in new_by_key:
        if key not in old_by_key:
            unexplained += 1
            if len(details) < _MAX_DETAILS:
                details.append({"key": ":".join(key), "old": None, "new": new_by_key[key]})

    has_diffs = value_mismatches or missing_in_new or missing_in_old
    status = "match" if not has_diffs else ("mismatch" if unexplained else "explained")
    return ParityResult(
        document_key=document_key,
        status=status,
        old_fact_count=len(old_by_key),
        new_fact_count=len(new_by_key),
        value_mismatches=value_mismatches,
        missing_in_new=missing_in_new,
        missing_in_old=missing_in_old,
        details=json.dumps(details, ensure_ascii=False),
    )
```

Note the semantics: missing keys are never "explained" (rules apply to value differences only), so `test_missing_keys_counted` must stay `mismatch` even with rules present.
- [ ] **Step 4: Run** `uv run pytest tests/test_xbrl_parity.py -q` — PASS.
- [ ] **Step 5: Write migration 000365 (+ ledger check + `EXPECTED_MIGRATIONS`), implement `assets/parity.py`, register asset.** Run `uv run pytest tests/test_clickhouse_migrations.py -q && uv run dg check defs` — PASS.
- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/xbrl_common/parity.py src/dagster_v3/defs/finland_xbrl/assets/parity.py \
        ../../clickhouse/migrations/000365_corpscout_fi_xbrl_parity_report.up.sql \
        ../../clickhouse/migrations/000365_corpscout_fi_xbrl_parity_report.down.sql \
        src/dagster_v3/defs/finland_xbrl/assets/__init__.py \
        tests/test_xbrl_parity.py tests/test_clickhouse_migrations.py
git commit -m "feat(xbrl): parity harness and Finland parity report asset"
```

---

### Task 9: Taxonomy dictionary builder + Finland dictionary

**Files:**
- Create: `src/dagster_v3/defs/xbrl_common/taxonomy.py`
- Create: `src/dagster_v3/defs/finland_xbrl/assets/taxonomy_dictionary.py`
- Create: `../../clickhouse/migrations/000366_corpscout_fi_taxonomy_dictionary.up.sql` / `.down.sql`
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/__init__.py`, `tests/test_clickhouse_migrations.py`
- Test: `tests/test_xbrl_taxonomy.py`

**Interfaces:**
- Produces (in `xbrl_common/taxonomy.py`):

```python
def concept_rows_from_model(model_xbrl, *, taxonomy_version: str, profile: SourceProfile,
                            loaded_at: str) -> tuple[list[dict], list[dict]]
    # -> (concept_rows, label_rows) shaped per TAXONOMY_CONCEPT_COLUMNS / TAXONOMY_LABEL_COLUMNS

def load_taxonomy_package(*, package_path: Path, entrypoint_url: str | None = None,
                          cache_dir: Path | None = None):
    # Arelle session: Cntlr + ModelManager, packages=[str(package_path)];
    # if entrypoint_url is None, enumerate entrypoints from the package's
    # META-INF/taxonomyPackage.xml (zipfile + lxml, tag {http://xbrl.org/2016/taxonomy-package}entryPointDocument
    # @xlink:href) and load the first; return (model_xbrl, entrypoint_url).
```

`concept_rows_from_model` walks `model_xbrl.qnameConcepts.values()`: skip concepts whose namespace starts with `http://www.xbrl.org/` (structural); emit identity (canonical qname via `profile.canonical_prefixes`), `substitution_group` (qname string or ""), `is_abstract`, `item_type` (`concept.typeQname` string or ""), `balance` (`concept.balance` or ""), `period_type` (`concept.periodType` or ""). Presentation and calculation parents come from `model_xbrl.relationshipSet(XbrlConst.parentChild)` / `relationshipSet(XbrlConst.summationItem)`: for each relationship, the CHILD concept's row gets `presentation_parent`/`presentation_order`/`presentation_role` (and calculation equivalents with `calculation_weight`); a concept in multiple roles emits one row per (concept, presentation_role) — the CH `ORDER BY` includes the role columns. Labels via `concept.label(labelrole, lang=..., fallbackToQname=False)` is per-lookup; instead iterate `model_xbrl.relationshipSet(XbrlConst.conceptLabel).modelRelationships` and read each label resource's `role`, `xmlLang`, `stringValue` — one label row each. Follow `sweden_financial/concepts.py`'s Arelle usage (`Cntlr.Cntlr(logFileName="logToStdErr")`, `ModelManager.initialize`) as the reference for session setup.

- Finland assets (in `assets/taxonomy_dictionary.py`):
  1. `finland_taxonomy_package_s3` — downloads the official SBR distribution zip (`TAXONOMY_SOURCE_URL` from `finland_xbrl/taxonomy.py`) with `dlt.sources.helpers.requests` Client and uploads to S3 `XBRL_BUCKET` under `finland_xbrl/taxonomy/{TAXONOMY_VERSION}/package.zip` — **skip download when the object already exists** (additive-only, idempotent). Metadata: size, key, skipped flag.
  2. `fi_taxonomy_dictionary_clickhouse` — deps on the package asset; reads the zip from S3 to a temp file, `load_taxonomy_package`, `concept_rows_from_model` with `FINLAND_PROFILE` and `taxonomy_version=TAXONOMY_VERSION`, replaces `corpscout.fi_taxonomy_concepts` and `corpscout.fi_taxonomy_labels` (stage+EXCHANGE; refuse empty concept rows). If Arelle cannot load the package's entrypoints (DPM-heavy package), the asset must fail with a clear error naming the entrypoint tried — do not publish partial dictionaries.

Migration `000366_corpscout_fi_taxonomy_dictionary.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS corpscout.fi_taxonomy_concepts
(
    taxonomy_version LowCardinality(String),
    concept_qname String,
    concept_namespace String,
    concept_local_name String,
    substitution_group String,
    is_abstract UInt8,
    item_type String,
    balance LowCardinality(String),
    period_type LowCardinality(String),
    presentation_parent String,
    presentation_order Float64,
    presentation_role String,
    calculation_parent String,
    calculation_weight Float64,
    calculation_role String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (taxonomy_version, concept_qname, presentation_role, calculation_role);

CREATE TABLE IF NOT EXISTS corpscout.fi_taxonomy_labels
(
    taxonomy_version LowCardinality(String),
    concept_qname String,
    language LowCardinality(String),
    label_role String,
    label String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (taxonomy_version, concept_qname, language, label_role);
```

- [ ] **Step 1: Write failing tests** — `concept_rows_from_model` against a small fake model (stub objects with `qnameConcepts`, `relationshipSet(...)` returning stub relationships, label resources with `role`/`xmlLang`/`stringValue`); assert row shapes equal `TAXONOMY_CONCEPT_COLUMNS`/`TAXONOMY_LABEL_COLUMNS`, structural namespaces skipped, presentation parent/order filled from relationships. Also test entrypoint enumeration from a minimal in-memory zip containing `META-INF/taxonomyPackage.xml`.
- [ ] **Step 2: Run** `uv run pytest tests/test_xbrl_taxonomy.py -q` — FAIL.
- [ ] **Step 3: Implement `xbrl_common/taxonomy.py`.**

```python
# src/dagster_v3/defs/xbrl_common/taxonomy.py
"""Arelle-once taxonomy dictionary builder (offline, per taxonomy version)."""

from __future__ import annotations

import zipfile
from pathlib import Path

from lxml import etree

from dagster_v3.defs.xbrl_common.extractor import SourceProfile

_TP_NS = "http://xbrl.org/2016/taxonomy-package"
_XLINK_NS = "http://www.w3.org/1999/xlink"
_STRUCTURAL_PREFIX = "http://www.xbrl.org/"


def package_entrypoints(package_path: Path) -> list[str]:
    with zipfile.ZipFile(package_path) as archive:
        candidates = [
            name for name in archive.namelist()
            if name.endswith("META-INF/taxonomyPackage.xml")
        ]
        if not candidates:
            raise ValueError(f"no META-INF/taxonomyPackage.xml in {package_path}")
        root = etree.fromstring(archive.read(candidates[0]))
    hrefs = [
        element.get(f"{{{_XLINK_NS}}}href") or ""
        for element in root.iter(f"{{{_TP_NS}}}entryPointDocument")
    ]
    hrefs = [href for href in hrefs if href]
    if not hrefs:
        raise ValueError(f"taxonomy package lists no entry points: {package_path}")
    return hrefs


def load_taxonomy_package(
    *,
    package_path: Path,
    entrypoint_url: str | None = None,
    cache_dir: Path | None = None,
):
    from arelle import Cntlr, ModelManager, PackageManager

    controller = Cntlr.Cntlr(logFileName="logToStdErr")
    if cache_dir is not None:
        controller.webCache.cacheDir = str(cache_dir)
    PackageManager.init(controller)
    PackageManager.addPackage(controller, str(package_path))
    PackageManager.rebuildRemappings(controller)
    manager = ModelManager.initialize(controller)
    entrypoint = entrypoint_url or package_entrypoints(package_path)[0]
    model_xbrl = manager.load(entrypoint)
    if model_xbrl is None or not model_xbrl.qnameConcepts:
        raise ValueError(f"Arelle could not load taxonomy entrypoint: {entrypoint}")
    return model_xbrl, entrypoint


def concept_rows_from_model(
    model_xbrl,
    *,
    taxonomy_version: str,
    profile: SourceProfile,
    loaded_at: str,
) -> tuple[list[dict], list[dict]]:
    from arelle import XbrlConst

    def canonical(qname) -> str:
        if qname is None:
            return ""
        prefix = profile.canonical_prefixes.get(qname.namespaceURI)
        return f"{prefix}:{qname.localName}" if prefix else str(qname)

    presentation: dict[str, list[tuple[str, float, str]]] = {}
    for rel in model_xbrl.relationshipSet(XbrlConst.parentChild).modelRelationships:
        child = canonical(rel.toModelObject.qname)
        presentation.setdefault(child, []).append(
            (canonical(rel.fromModelObject.qname), float(rel.order or 0.0),
             rel.linkrole or "")
        )
    calculation: dict[str, list[tuple[str, float, str]]] = {}
    for rel in model_xbrl.relationshipSet(XbrlConst.summationItem).modelRelationships:
        child = canonical(rel.toModelObject.qname)
        calculation.setdefault(child, []).append(
            (canonical(rel.fromModelObject.qname), float(rel.weight or 0.0),
             rel.linkrole or "")
        )

    concept_rows: list[dict] = []
    for concept in model_xbrl.qnameConcepts.values():
        namespace = concept.qname.namespaceURI or ""
        if namespace.startswith(_STRUCTURAL_PREFIX):
            continue
        qname = canonical(concept.qname)
        pres = presentation.get(qname) or [("", 0.0, "")]
        calc = calculation.get(qname) or [("", 0.0, "")]
        for pres_parent, pres_order, pres_role in pres:
            for calc_parent, calc_weight, calc_role in calc:
                concept_rows.append(
                    {
                        "taxonomy_version": taxonomy_version,
                        "concept_qname": qname,
                        "concept_namespace": namespace,
                        "concept_local_name": concept.qname.localName,
                        "substitution_group": canonical(
                            concept.substitutionGroupQname
                        ),
                        "is_abstract": bool(concept.isAbstract),
                        "item_type": (
                            str(concept.typeQname) if concept.typeQname else ""
                        ),
                        "balance": concept.balance or "",
                        "period_type": concept.periodType or "",
                        "presentation_parent": pres_parent,
                        "presentation_order": pres_order,
                        "presentation_role": pres_role,
                        "calculation_parent": calc_parent,
                        "calculation_weight": calc_weight,
                        "calculation_role": calc_role,
                        "loaded_at": loaded_at,
                    }
                )

    label_rows: list[dict] = []
    for rel in model_xbrl.relationshipSet(XbrlConst.conceptLabel).modelRelationships:
        concept = rel.fromModelObject
        label = rel.toModelObject
        if concept is None or label is None:
            continue
        namespace = concept.qname.namespaceURI or ""
        if namespace.startswith(_STRUCTURAL_PREFIX):
            continue
        label_rows.append(
            {
                "taxonomy_version": taxonomy_version,
                "concept_qname": canonical(concept.qname),
                "language": (label.xmlLang or "").lower(),
                "label_role": label.role or "",
                "label": label.stringValue or "",
                "loaded_at": loaded_at,
            }
        )
    return concept_rows, label_rows
```

The cross-product of presentation × calculation parents is deliberate (a concept usually has ≤1 of each; the CH `ORDER BY` includes both role columns so ReplacingMergeTree keys stay unique). If Arelle's API differs on any attribute (`rel.order`, `label.xmlLang`), consult `sweden_financial/concepts.py` which already walks the same structures, and adjust — the test fakes must mirror the real attribute names used.

Run tests — PASS.
- [ ] **Step 4: Migration 000366 (+ ledger check + `EXPECTED_MIGRATIONS`), implement the two Finland assets, register in `assets/__init__.py`.** Run `uv run pytest tests/test_clickhouse_migrations.py -q && uv run dg check defs` — PASS.
- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/xbrl_common/taxonomy.py \
        src/dagster_v3/defs/finland_xbrl/assets/taxonomy_dictionary.py \
        ../../clickhouse/migrations/000366_corpscout_fi_taxonomy_dictionary.up.sql \
        ../../clickhouse/migrations/000366_corpscout_fi_taxonomy_dictionary.down.sql \
        src/dagster_v3/defs/finland_xbrl/assets/__init__.py \
        tests/test_xbrl_taxonomy.py tests/test_clickhouse_migrations.py
git commit -m "feat(xbrl): Arelle taxonomy dictionary builder and Finland SBR dictionary"
```

---

### Task 10: Deploy, backfill, parity review (operational gate)

No code. Operator/controller steps — this is the gate before Task 11.

- [ ] Apply migrations 000364–000366 on prod ClickHouse (check `migrate version` first; ledger may have moved — renumber per Global Constraints if needed).
- [ ] Deploy dagster_v3 (pristine-worktree light_sync recipe; verify worktree SHA before playbook; check for leaked deploy lock).
- [ ] Backfill `data_snapshot_xml_unified_duckdb` (all XML_SNAPSHOT_PARTITIONS) and `data_daily_xml_unified_duckdb` (all DAILY_PARTITIONS) via UI/GraphQL backfills — `BackfillPolicy.multi_run(1)` throttles them; the pool serializes against the daily schedule.
- [ ] Materialize `fi_xbrl_unified_clickhouse`, then `fi_xbrl_parity`.
- [ ] Materialize `finland_taxonomy_package_s3` then `fi_taxonomy_dictionary_clickhouse`.
- [ ] Review `corpscout.fi_xbrl_parity_report`: `SELECT status, count() FROM corpscout.fi_xbrl_parity_report GROUP BY status`. For each mismatch class, read `details`, decide: extractor bug (fix in Tasks 3–5 code, re-run) or documented improvement (add an `explained_rules` predicate with a comment, re-run parity).
- [ ] **Gate:** zero `mismatch` rows, and the owner has seen the match/explained counts. Only then proceed to Task 11.

---

### Task 11: Cutover

**Files:**
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/financial_metrics.py` (+ its callers if signatures shift)
- Modify: `src/dagster_v3/defs/finland_xbrl/assets/financial_publish.py`, `assets/jobs.py`, `assets/__init__.py`
- Create: `../../clickhouse/migrations/000367_corpscout_fi_xbrl_unified_cutover.up.sql` / `.down.sql`
- Delete (retire): legacy parse assets `data_snapshot_xml_duckdb` / `data_daily_xml_duckdb` asset definitions, `src/dagster_v3/defs/finland_xbrl/parser.py` usage from the pipeline (`parser.py` itself stays until `statement_key_for` and `ParsedStatement` move — move both into `unified_adapter.py` and delete `parser.py`)
- Modify: `tests/` — legacy-parser tests retire with it; unified tests take over
- Test: existing suites must stay green

Steps:

- [ ] **Step 1: Repoint metrics.** In `financial_metrics.py`, switch the fact source to the unified duckdbs (`list_xml_unified_duckdb_paths`) and rename consumed columns: `reported_business_id` → `reported_entity_id` (documents); `mcy_member_code` is unchanged (Finland extra). Update the SQL/polars references found at `financial_metrics.py:112,225,258,268,275-277`. Run `uv run pytest tests/test_finland_xbrl_comprehensive.py tests/test_finland_xbrl_assets.py -q` and fix fixtures to the unified shape.
- [ ] **Step 2: Repoint CH publish.** `financial_publish.py`'s `fi_xbrl_parsed_clickhouse` retires; the incremental/publish jobs reference `fi_xbrl_unified_clickhouse` instead (jobs.py selections updated; remove `fi_financial_statements_ch`/`fi_xbrl_contexts_ch`/`fi_xbrl_units_ch`/`fi_xbrl_facts_ch` names).
- [ ] **Step 3: Check downstream consumers before dropping tables.** From repo root: `rg -l "fi_xbrl_facts_raw|fi_xbrl_contexts|fi_xbrl_units|fi_financial_statements" corpscout/services/backoffice corpscout/services/dagster_v3/src`. Every hit must be repointed to the new names (or confirmed dead) before the migration below. `fi_financial_metrics` (the metrics table) is NOT dropped — only re-fed.
- [ ] **Step 4: Cutover migration 000367** (ledger check first):

```sql
DROP TABLE IF EXISTS corpscout.fi_xbrl_facts_raw;
DROP TABLE IF EXISTS corpscout.fi_xbrl_contexts;
DROP TABLE IF EXISTS corpscout.fi_xbrl_units;
DROP TABLE IF EXISTS corpscout.fi_financial_statements;
RENAME TABLE corpscout.fi_xbrl_documents_next TO corpscout.fi_xbrl_documents;
RENAME TABLE corpscout.fi_xbrl_contexts_next TO corpscout.fi_xbrl_contexts;
RENAME TABLE corpscout.fi_xbrl_units_next TO corpscout.fi_xbrl_units;
RENAME TABLE corpscout.fi_xbrl_facts_next TO corpscout.fi_xbrl_facts;
```

Update `unified_clickhouse.py` table constants to the post-rename names in the same commit. Down migration: reverse renames (recreating dropped legacy tables is not supported — document that in a `--` comment; the down only un-renames).
**Precondition gate (destructive DDL rule):** apply this migration only after Step 3's audit is clean AND Task 10's parity gate passed AND `fi_xbrl_documents_next` row count ≥ the legacy `fi_financial_statements` count.

- [ ] **Step 5: Retire the legacy parse chain.** Remove `data_snapshot_xml_duckdb`/`data_daily_xml_duckdb` assets from `Definitions`/jobs (cancel any in-flight backfills of them first — partitions-def-change rule), move `ParsedStatement` + `statement_key_for` into `unified_adapter.py`, delete `parser.py`, update all imports, delete/port its tests.
- [ ] **Step 6: Full check.** `uv run pytest tests/ -k "finland or xbrl" -q && uv run dg check defs` — green (pre-existing unrelated failures excepted; note them).
- [ ] **Step 7: Commit** (explicit paths of every touched file), deploy, run the incremental job once end-to-end on prod, verify `fi_financial_metrics` row count is within 1% of its pre-cutover count.

---

## Verification (whole plan)

1. `uv run pytest tests/test_xbrl_transforms.py tests/test_xbrl_canonical_tables.py tests/test_xbrl_extractor.py tests/test_finland_unified_adapter.py tests/test_finland_unified_assets.py tests/test_xbrl_parity.py tests/test_xbrl_taxonomy.py -q` — all green.
2. `uv run dg check defs` — green.
3. Prod: parity report shows 0 mismatches; `fi_xbrl_facts` (post-rename) row count ≥ legacy `fi_xbrl_facts_raw` count observed pre-cutover; `fi_financial_metrics` refreshed from unified facts within 1% of prior count; `fi_taxonomy_concepts` > 1000 rows with non-empty `label` coverage in `fi`, `sv`, `en`.
