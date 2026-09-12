# SE company financial entity, slice 2: the extractors — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill `se_company_financial_suggestion` from the four sources (Bolagsverket reported, Bolagsverket comparative, ESEF, Ratsit) on the person entity's per-company state-hash change scan, lifted into a shared module, with an extract job and a stopped weekly, proven on a real ClickHouse, and run on prod.

**Architecture:** `se_company/state_scan.py` is the person entity's scan (state hash per company, tombstone per key) made generic over a `StateScan` shape; `person/suggestions.py` keeps its public names and delegates to it, rendering byte-identical SQL. `financial/suggestions.py` declares the financial target (59 supplied columns, four stamped) and its scan (period_key as the key; a tombstone copies scope and period_end so the DDL's CHECK holds). One module per source renders a `live` SELECT in the shared column order; the shared helper wraps it into the change scope and the page select. Every module text below was rendered and executed on ClickHouse 26.5 against a prod-snapshot fixture on 2026-09-12 before this plan was written; Task 6's test is that run.

**Tech Stack:** ClickHouse 26.5 (window functions, argMaxIf, Join-free UNION ALL), Dagster (`define_suggestion_asset`, `define_asset_job`, `ScheduleDefinition`), pytest with `tests/clickhouse_local.py` (Docker image `clickhouse/clickhouse-server:26.5`, present locally).

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` — sections 4.1, 7, 8, 11, 12 item 2, 13; the package note `src/dagster_v3/defs/se_company/financial/docs/financial-design.md` ("Notes for the extractors").

## Global Constraints

- Branch `se-financial-entity`, worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity` (at 70e9dbe7f, equal to main; `.env` files and `.venv` in place). Every path below is relative to `corpscout/services/dagster_v3` inside that worktree unless it starts with `corpscout/`. Never touch the main checkout at `/Users/graovic/pulsarpoint/ppoint/companycollect`. Do not use `git stash`.
- No migration in this slice. The suggestion table's CHECKs bind every row an extractor writes: `period_key = concat(scope, ':', toString(period_end))`, `scope IN ('standalone', 'consolidated')`, `amount_scale IN (1, 1000, 1000000)`, `ifNull(currency, 'x') != ''`, `source IN (the six)`.
- The person entity's rendered SQL must not change: Task 1 dumps the four person extractors' texts before the lift and diffs them after; `tests/test_se_company_person_extractors_sql.py` and `tests/test_se_company_person_extractors_clickhouse_local.py` must stay green.
- Sources and versions: `bolagsverket` (`bolagsverket-financial-v1`), `bolagsverket_comparative` (`bolagsverket-comparative-financial-v1`), `esef` (`esef-financial-v1`), `ratsit` (`ratsit-financial-v1`). Every extractor joins the universe `se_company_basic_info FINAL`. Currency is NULL, never `''`. A live row carries at least one figure or an employee count.
- Field mappings (spec 7): Bolagsverket reported → the twelve register metrics (`operating_profit_loss` is `operating_result`, `profit_loss` is `net_result`) with `filing_fiscal_year = source_fiscal_year`, the fuller statement wins a duplicated period then the smaller `statement_key`; comparative → revenue and total assets from the filing with the greatest `source_fiscal_year`; ESEF → scope `consolidated_ifrs` only, newest `fxo_id` version wins field by field with older versions filling gaps, `source_record_uid` = the newest `fxo_id`; Ratsit → the latest report per company, scope `company` → `standalone`, `_amount_original` = published figure × unit scale (MSEK 1000000, TSEK 1000, SEK 1) with `amount_scale` recording it, USD twins copied, undated periods keyed on Dec 31 of the fiscal year with `period_end_derived = 1` and guarded to 1900..2299, rows without a unit skipped, the longer of duplicate periods wins then the later report and period index, `source_record_uid = 'ratsit:<company>:<report index>:<period index>'`.
- The weekly is `55 7 * * 1`, defined STOPPED (07:55 Monday was free on 2026-09-12; verify with `rg -n 'cron_schedule="55 7 ' src/` returning only this file).
- Tests run with `uv run --env-file .env pytest ... -q` from `corpscout/services/dagster_v3`; defs-loading tests need the `.env`; `uv run ruff check <files>` clean.
- Commit messages follow Conventional Commits and end with EXACTLY these two lines, pasted verbatim:
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_013oGzirJgExBzVuy9GQBYHz
- Task 9 (prod) runs after the branch's final review and merge, on the owner's standing "do it" for this slice; it only inserts suggestion rows.

---

## File structure

| File | Responsibility |
|---|---|
| `src/dagster_v3/defs/se_company/state_scan.py` (new) | The generic state-hash scan: `StateScan`, `live_select_sql`, `state_sql`, `stored_live_sql`, `changed_scope_sql`, `select_sql`, `define_scan_asset` |
| `src/dagster_v3/defs/se_company/person/suggestions.py` (replace) | The person shape as a `StateScan`; same public names, delegating |
| `src/dagster_v3/defs/se_company/financial/suggestions.py` (new) | The financial target, columns, predicate, tombstone map, `scan_for`, the three wrappers, `period_months_sql`, `universe_join_sql` |
| `src/dagster_v3/defs/se_company/financial/bolagsverket.py` (new) | Sources `bolagsverket` and `bolagsverket_comparative` |
| `src/dagster_v3/defs/se_company/financial/esef.py` (new) | Source `esef` |
| `src/dagster_v3/defs/se_company/financial/ratsit.py` (new) | Source `ratsit` |
| `src/dagster_v3/defs/se_company/financial/assets.py` (modify) | `EXTRACTOR_SOURCES`, `EXTRACTOR_ASSET_NAMES` |
| `src/dagster_v3/defs/se_company/financial/jobs.py` (new) | The extract job and the stopped weekly |
| `tests/test_se_company_state_scan.py` (new) | The generic scan's rendered texts and validations |
| `tests/test_se_company_financial_suggestions.py` (new) | The financial target and scan shape |
| `tests/test_se_company_financial_extractors_sql.py` (new) | Each extractor's rendered SQL and asset wiring |
| `tests/fixtures/se_company_financial_source_tables.sql` (new) | Prod `SHOW CREATE TABLE` snapshot (2026-09-12) of the six source tables |
| `tests/test_se_company_financial_extractors_clickhouse_local.py` (new) | The four extractors on the real engine |
| `tests/test_se_company_financial_jobs.py` (new) | Job selection, schedule, run config |
| `src/dagster_v3/defs/se_company/financial/docs/financial-design.md` (modify) | Per-module table for the extractors |

---

### Task 1: Lift the state-hash scan into `se_company/state_scan.py`

**Files:**
- Create: `src/dagster_v3/defs/se_company/state_scan.py`
- Replace: `src/dagster_v3/defs/se_company/person/suggestions.py`
- Test: `tests/test_se_company_state_scan.py` (new); the existing `tests/test_se_company_person_extractors_sql.py` and `tests/test_se_company_person_extractors_clickhouse_local.py` (unchanged, must stay green)

**Interfaces:**
- Consumes: `SuggestionTarget`, `define_suggestion_asset` from `dagster_v3.defs.se_company.basic_info.extract`.
- Produces: `state_scan.StateScan(target, select_columns, state_columns, key_column, live_row_predicate, tombstone_columns, tombstone_values)`; `state_scan.live_select_sql(scan, *, columns, from_sql, where_sql, with_sql="") -> str`; `state_scan.state_sql(scan, alias) -> str`; `state_scan.stored_live_sql(scan, *, source, columns, scoped) -> str`; `state_scan.changed_scope_sql(scan, *, source, live_sql) -> str`; `state_scan.select_sql(scan, *, source, live_sql) -> str`; `state_scan.define_scan_asset(scan, **kwargs)`. The person module keeps `PERSON_SELECT_COLUMNS`, `PERSON_STATE_COLUMNS`, `NULLABLE_PERSON_COLUMNS`, `NULL_SQL`, `LIVE_ROW_PREDICATE`, `PERSON_WITH_SQL`, `PERSON_TRAILING_SELECT_SQL`, `PERSON_TARGET`, `live_select_sql`, `person_state_sql`, `stored_live_sql`, `person_changed_scope_sql`, `person_select_sql`, `define_person_suggestion_asset`.

- [ ] **Step 1: Record the person entity's rendered SQL before touching anything**

```bash
mkdir -p .superpowers-golden && uv run --env-file .env python - <<'PY'
from pathlib import Path
from dagster_v3.defs.se_company.person import bolagsverket as b, esef as e, ratsit as r, wikidata as w
out = Path(".superpowers-golden/person_sql_before.txt")
parts = []
for name, mod, live, scope, select in (
    ("bolagsverket", b, b.bolagsverket_live_sql, b.bolagsverket_changed_scope_sql, b.bolagsverket_select_sql),
    ("esef", e, e.esef_live_sql, e.esef_changed_scope_sql, e.esef_select_sql),
    ("ratsit", r, r.ratsit_live_sql, r.ratsit_changed_scope_sql, r.ratsit_select_sql),
    ("wikidata", w, w.wikidata_live_sql, w.wikidata_changed_scope_sql, w.wikidata_select_sql),
):
    parts += [f"### {name} live", live(), f"### {name} live scoped", live(scoped=True), f"### {name} scope", scope(), f"### {name} select", select()]
out.write_text("\n".join(parts))
print(out, len(parts))
PY
```

(`.superpowers-golden/` is a scratch directory in the worktree root; do not commit it. If a wikidata function is named differently, use the names `wikidata.py` exports and note it in the report.)

- [ ] **Step 2: Write the failing test for the generic scan**

`tests/test_se_company_state_scan.py`:

```python
"""The shared state-hash scan (financial slice 2 lifted it out of the person entity): the
rendered texts for a three-column toy entity, and the two import-time validations."""

import pytest

from dagster_v3.defs.se_company import state_scan
from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget

TARGET = SuggestionTarget(
    database="corpscout", table="toy_suggestion", insert_columns=("company_id", "source", "k", "v", "stamp"),
    select_columns=("company_id", "source", "k", "v"), trailing_select_sql="now64(3) AS stamp",
    asset_prefix="toy_", group_name="toy", scratch_prefix="corpscout._tmp_toy_",
)
SCAN = state_scan.StateScan(
    target=TARGET, select_columns=("company_id", "source", "k", "v"), state_columns=("k", "v"),
    key_column="k", live_row_predicate="(v IS NOT NULL)", tombstone_columns=("k",),
    tombstone_values={"company_id": "stored.company_id", "source": "'s'", "k": "stored.k", "v": "CAST(NULL AS Nullable(String))"},
)


def test_state_hash_is_length_prefixed_sorted_and_null_safe() -> None:
    assert state_scan.state_sql(SCAN, "live") == (
        "lower(hex(SHA256(arrayStringConcat(arraySort(groupArray(concat("
        "toString(length(ifNull(toString(live.k), ''))), ':', ifNull(toString(live.k), ''), '\\n', "
        "toString(length(ifNull(toString(live.v), ''))), ':', ifNull(toString(live.v), '')))), '\\n'))))"
    )


def test_stored_live_rows_filter_the_source_literal_and_the_predicate() -> None:
    assert state_scan.stored_live_sql(SCAN, source="s", columns=("k",), scoped=True) == (
        "SELECT company_id, k\nFROM corpscout.toy_suggestion FINAL\n"
        "WHERE source = 's' AND (v IS NOT NULL)\n    AND company_id IN %(company_ids)s"
    )


def test_changed_scope_unions_both_sides_under_one_alias() -> None:
    sql = state_scan.changed_scope_sql(SCAN, source="s", live_sql="SELECT 1 AS company_id, 'a' AS k, 'b' AS v")
    assert sql.count("AS live\n") == 2 and sql.count("GROUP BY company_id") == 3
    assert sql.endswith("HAVING count() < 2 OR uniqExact(state) > 1")


def test_select_unions_live_rows_with_tombstones_anti_joined_on_the_key() -> None:
    sql = state_scan.select_sql(SCAN, source="s", live_sql="SELECT 1 AS company_id, 's' AS source, 'a' AS k, 'b' AS v")
    assert sql.startswith("WITH live AS (\n")
    assert "SELECT live.company_id AS company_id, live.source AS source, live.k AS k, live.v AS v\nFROM live\nUNION ALL" in sql
    assert "    stored.company_id AS company_id,\n    's' AS source,\n    stored.k AS k,\n    CAST(NULL AS Nullable(String)) AS v" in sql
    assert sql.endswith(
        "LEFT ANTI JOIN (SELECT company_id, k FROM live) AS live_ks\n"
        "    ON live_ks.company_id = stored.company_id AND live_ks.k = stored.k"
    )


def test_live_select_refuses_a_missing_or_extra_column() -> None:
    with pytest.raises(ValueError, match="missing=\\['v'\\]"):
        state_scan.live_select_sql(SCAN, columns={"company_id": "1", "source": "'s'", "k": "'a'"}, from_sql="FROM t", where_sql="WHERE 1")
    text = state_scan.live_select_sql(SCAN, columns={"company_id": "1", "source": "'s'", "k": "'a'", "v": "'b'"}, from_sql="FROM t", where_sql="WHERE 1", with_sql="WITH x AS (SELECT 1)\n")
    assert text == "WITH x AS (SELECT 1)\nSELECT\n    1 AS company_id,\n    's' AS source,\n    'a' AS k,\n    'b' AS v\nFROM t\nWHERE 1"


def test_a_scan_validates_its_tombstone_map_and_key() -> None:
    with pytest.raises(ValueError, match="tombstone_values: missing=\\['v'\\]"):
        state_scan.StateScan(target=TARGET, select_columns=("company_id", "source", "k", "v"), state_columns=("k", "v"), key_column="k", live_row_predicate="1", tombstone_columns=("k",), tombstone_values={"company_id": "1", "source": "'s'", "k": "1"})
    with pytest.raises(ValueError, match="must include the key column"):
        state_scan.StateScan(target=TARGET, select_columns=("company_id", "source", "k", "v"), state_columns=("k", "v"), key_column="k", live_row_predicate="1", tombstone_columns=(), tombstone_values={"company_id": "1", "source": "'s'", "k": "1", "v": "1"})
```

- [ ] **Step 3: Run it to verify it fails**

```bash
uv run --env-file .env pytest tests/test_se_company_state_scan.py -q
```

Expected: FAIL at import, `ModuleNotFoundError: No module named 'dagster_v3.defs.se_company.state_scan'`.

- [ ] **Step 4: Write `state_scan.py`**

`src/dagster_v3/defs/se_company/state_scan.py`, exactly:

```python
"""The per-company state-hash change scan every entity without an observed_at watermark
shares: the person entity (slot-keyed) and the financial entity (period-keyed). Lifted from
person/suggestions.py on 2026-09-12 (financial slice 2); the person module keeps its names
and delegates here.

1. THE CHANGE SCAN IS A PER-COMPANY STATE HASH, not a timestamp. Each side -- what the
   source delivers now, and the company's live (non-tombstone) suggestion rows -- is
   reduced to one sha256 over its sorted, length-prefixed rows. A company is visited when
   only one side has it (never suggested, or gone from the source) or the two hashes
   differ; equal hashes mean there is nothing to write, so the scan converges after one
   pass. A timestamp would not: the source tables are rebuilt whole on every run.
2. TOMBSTONES ARE PER KEY (a slot, a period), not per company. The page select is
   `live UNION ALL tombstones`, the tombstones being the stored live keys the source no
   longer delivers -- a LEFT ANTI JOIN of the stored keys against the same `live` CTE.
   Writing the tombstone takes the row out of `live`, so the next scan sees equal hashes.

The `live` CTE is referenced twice but written once; each page select binds
%(company_ids)s exactly twice (the live CTE and the stored-key read), which is why the
entities page at 5,000 to 10,000 ids under ID_BOUND_QUERY_SETTINGS' 1 MiB max_query_size.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget, define_suggestion_asset


@dataclass(frozen=True)
class StateScan:
    """One entity's shape for the scan.

    select_columns: what a source supplies, in the target's select order (no id, stamp
    or run columns). state_columns: the subset hashed per company (everything but
    company_id and source). key_column: the per-row key inside a company (person: slot,
    financial: period_key). live_row_predicate: SQL over the suggestion table telling a
    live row from a tombstone. tombstone_columns: the stored columns a tombstone copies
    (the key, plus whatever the table's CHECKs derive it from). tombstone_values: the SQL
    for every select column of a tombstone row; it may name `stored.<column>` for any
    column in tombstone_columns.
    """

    target: SuggestionTarget
    select_columns: tuple[str, ...]
    state_columns: tuple[str, ...]
    key_column: str
    live_row_predicate: str
    tombstone_columns: tuple[str, ...]
    tombstone_values: Mapping[str, str]

    def __post_init__(self) -> None:
        missing = sorted(set(self.select_columns) - set(self.tombstone_values))
        extra = sorted(set(self.tombstone_values) - set(self.select_columns))
        if missing or extra:
            raise ValueError(f"tombstone_values: missing={missing} extra={extra}")
        if self.key_column not in self.tombstone_columns:
            raise ValueError(f"tombstone_columns must include the key column {self.key_column!r}")


def live_select_sql(
    scan: StateScan, *, columns: Mapping[str, str], from_sql: str, where_sql: str, with_sql: str = ""
) -> str:
    """One source's current rows, projected onto scan.select_columns in order.

    `columns` maps every select column to its SQL expression, so a source that forgets one
    fails at import instead of writing a short row into a UNION ALL whose other branch has
    them all.
    """
    missing = sorted(set(scan.select_columns) - set(columns))
    extra = sorted(set(columns) - set(scan.select_columns))
    if missing or extra:
        raise ValueError(f"live_select_sql columns: missing={missing} extra={extra}")
    projection = ",\n".join(f"    {columns[column]} AS {column}" for column in scan.select_columns)
    return f"{with_sql}SELECT\n{projection}\n{from_sql}\n{where_sql}"


def state_sql(scan: StateScan, alias: str) -> str:
    """One company's whole delivered state for one source, as a single hash.

    Length-prefixed fields so no value containing the separator can imitate another row;
    arraySort so the row order a scan happens to produce cannot change the hash;
    ifNull(toString(...), '') so a NULL and an empty string are told apart by the length
    prefix rather than by concat returning NULL.
    """
    fields: list[str] = []
    for column in scan.state_columns:
        value = f"ifNull(toString({alias}.{column}), '')"
        fields.append(f"toString(length({value})), ':', {value}")
    row = ", '\\n', ".join(fields)
    return f"lower(hex(SHA256(arrayStringConcat(arraySort(groupArray(concat({row}))), '\\n'))))"


def stored_live_sql(scan: StateScan, *, source: str, columns: Sequence[str], scoped: bool) -> str:
    """The company's live (non-tombstone) suggestion rows for one source.

    The source is a literal rather than %(source)s because `run_extractor` binds `source`
    only into the scope's params, never into the page select's.
    """
    scope = "\n    AND company_id IN %(company_ids)s" if scoped else ""
    return (
        f"SELECT company_id, {', '.join(columns)}\n"
        f"FROM {scan.target.qualified_table} FINAL\n"
        f"WHERE source = '{source}' AND {scan.live_row_predicate}{scope}"
    )


def changed_scope_sql(scan: StateScan, *, source: str, live_sql: str) -> str:
    """Companies whose delivered state differs from what is stored.

    Both sides are aliased `live` so the state expression is one identical text: any drift
    between them would re-extract every company on every run. Two aggregations and no join,
    so the result cannot depend on join_use_nulls. `count() < 2` catches a company only one
    side has -- new, or vanished from the source and due its tombstones.
    """
    state = state_sql(scan, "live")
    stored = stored_live_sql(scan, source=source, columns=scan.state_columns, scoped=False)
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


def select_sql(scan: StateScan, *, source: str, live_sql: str) -> str:
    """The page's rows: everything the source still delivers, plus one tombstone per stored
    live key it no longer delivers."""
    live_projection = ", ".join(f"live.{column} AS {column}" for column in scan.select_columns)
    tombstone_projection = ",\n".join(
        f"    {scan.tombstone_values[column]} AS {column}" for column in scan.select_columns
    )
    stored_keys = stored_live_sql(scan, source=source, columns=scan.tombstone_columns, scoped=True)
    key = scan.key_column
    alias = f"live_{key}s"  # live_slots for the person entity, live_period_keys for financials
    return (
        f"WITH live AS (\n{live_sql}\n)\n"
        f"SELECT {live_projection}\n"
        "FROM live\n"
        "UNION ALL\n"
        f"SELECT\n{tombstone_projection}\n"
        f"FROM (\n{stored_keys}\n) AS stored\n"
        f"LEFT ANTI JOIN (SELECT company_id, {key} FROM live) AS {alias}\n"
        f"    ON {alias}.company_id = stored.company_id AND {alias}.{key} = stored.{key}"
    )


def define_scan_asset(scan: StateScan, **kwargs: Any) -> dg.AssetsDefinition:
    return define_suggestion_asset(target=scan.target, **kwargs)
```

- [ ] **Step 5: Replace `person/suggestions.py` with the delegating version**

`src/dagster_v3/defs/se_company/person/suggestions.py`, exactly:

```python
"""The person entity's target for the shared suggestion extract helper (spec 2026-09-09
section 6), and the two SQL shapes every person source needs.

A person source delivers MANY rows per company and se_company_person_suggestion carries no
observed_at column, so the change scan is the per-company state hash and tombstones are per
slot. Since financial slice 2 (2026-09-12) that machinery lives in
dagster_v3.defs.se_company.state_scan; this module keeps its public names (the four person
extractors and their tests import them) and delegates. The rendered SQL is byte-identical to
what this module rendered before the lift.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company import state_scan
from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.assets import GROUP_NAME

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
    scratch_prefix=tables.SCRATCH_SCOPE_PREFIX,
    with_sql=PERSON_WITH_SQL,
)

PERSON_SCAN = state_scan.StateScan(
    target=PERSON_TARGET,
    select_columns=PERSON_SELECT_COLUMNS,
    state_columns=PERSON_STATE_COLUMNS,
    key_column="slot",
    live_row_predicate=LIVE_ROW_PREDICATE,
    tombstone_columns=("slot",),
    tombstone_values={
        "company_id": "stored.company_id",
        "slot": "stored.slot",
        "source_record_id": "''",
        "data": "'{}'",
        **NULL_SQL,
        # `source` is filled per source below: the scan is shared by four extractors.
        "source": "__SOURCE__",
    },
)


def _scan_for(source: str) -> state_scan.StateScan:
    values = dict(PERSON_SCAN.tombstone_values)
    values["source"] = f"'{source}'"
    return state_scan.StateScan(
        target=PERSON_SCAN.target, select_columns=PERSON_SCAN.select_columns,
        state_columns=PERSON_SCAN.state_columns, key_column=PERSON_SCAN.key_column,
        live_row_predicate=PERSON_SCAN.live_row_predicate,
        tombstone_columns=PERSON_SCAN.tombstone_columns, tombstone_values=values,
    )


def live_select_sql(
    *, columns: Mapping[str, str], from_sql: str, where_sql: str, with_sql: str = ""
) -> str:
    """One source's current rows, projected onto PERSON_SELECT_COLUMNS in order."""
    return state_scan.live_select_sql(
        PERSON_SCAN, columns=columns, from_sql=from_sql, where_sql=where_sql, with_sql=with_sql
    )


def person_state_sql(alias: str) -> str:
    """One company's whole delivered state for one source, as a single hash."""
    return state_scan.state_sql(PERSON_SCAN, alias)


def stored_live_sql(*, source: str, columns: Sequence[str], scoped: bool) -> str:
    """The company's live (non-tombstone) suggestion rows for one source."""
    return state_scan.stored_live_sql(PERSON_SCAN, source=source, columns=columns, scoped=scoped)


def person_changed_scope_sql(*, source: str, live_sql: str) -> str:
    """Companies whose delivered state differs from what is stored."""
    return state_scan.changed_scope_sql(PERSON_SCAN, source=source, live_sql=live_sql)


def person_select_sql(*, source: str, live_sql: str) -> str:
    """The page's rows: everything the source still delivers, plus one tombstone per stored
    live slot it no longer delivers (spec 3.1: every person column NULL, `data` '{}')."""
    return state_scan.select_sql(_scan_for(source), source=source, live_sql=live_sql)


def define_person_suggestion_asset(**kwargs: Any) -> dg.AssetsDefinition:
    return state_scan.define_scan_asset(PERSON_SCAN, **kwargs)
```

- [ ] **Step 6: Prove nothing moved**

```bash
uv run --env-file .env pytest tests/test_se_company_state_scan.py tests/test_se_company_person_extractors_sql.py -q
uv run --env-file .env python - <<'PY'
from pathlib import Path
from dagster_v3.defs.se_company.person import bolagsverket as b, esef as e, ratsit as r, wikidata as w
parts = []
for name, mod, live, scope, select in (
    ("bolagsverket", b, b.bolagsverket_live_sql, b.bolagsverket_changed_scope_sql, b.bolagsverket_select_sql),
    ("esef", e, e.esef_live_sql, e.esef_changed_scope_sql, e.esef_select_sql),
    ("ratsit", r, r.ratsit_live_sql, r.ratsit_changed_scope_sql, r.ratsit_select_sql),
    ("wikidata", w, w.wikidata_live_sql, w.wikidata_changed_scope_sql, w.wikidata_select_sql),
):
    parts += [f"### {name} live", live(), f"### {name} live scoped", live(scoped=True), f"### {name} scope", scope(), f"### {name} select", select()]
after = "\n".join(parts); before = Path(".superpowers-golden/person_sql_before.txt").read_text()
print("IDENTICAL" if after == before else "DIFFERENT")
PY
uv run --env-file .env pytest tests/test_se_company_person_extractors_clickhouse_local.py -q -m integration
uv run ruff check src/dagster_v3/defs/se_company/state_scan.py src/dagster_v3/defs/se_company/person/suggestions.py tests/test_se_company_state_scan.py
```

Expected: 6 + the person SQL tests passed; `IDENTICAL`; the person clickhouse-local test green on the engine; ruff clean. `DIFFERENT` is a stop: report the diff, do not adjust the golden file.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/state_scan.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/person/suggestions.py \
        corpscout/services/dagster_v3/tests/test_se_company_state_scan.py
git commit -m "refactor(se-company): lift the person entity's state-hash scan into state_scan.py"
```

---

### Task 2: `financial/suggestions.py`

**Files:**
- Create: `src/dagster_v3/defs/se_company/financial/suggestions.py`
- Test: `tests/test_se_company_financial_suggestions.py`

**Interfaces:**
- Consumes: `state_scan` (Task 1); `tables` and `assets.GROUP_NAME` of the financial package (slice 1).
- Produces: `STAMPED_COLUMNS`, `FINANCIAL_SELECT_COLUMNS` (59), `FINANCIAL_STATE_COLUMNS` (57), `NULLABLE_COLUMN_TYPES`, `NULL_SQL`, `MONEY_NULL_SQL`, `LIVE_ROW_PREDICATE`, `FINANCIAL_WITH_SQL`, `FINANCIAL_TRAILING_SELECT_SQL`, `FINANCIAL_TARGET`, `universe_join_sql(alias)`, `period_months_sql(start, end)`, `tombstone_values(source)`, `scan_for(source)`, `live_select_sql(*, columns, from_sql, where_sql, with_sql="")`, `financial_changed_scope_sql(*, source, live_sql)`, `financial_select_sql(*, source, live_sql)`, `define_financial_suggestion_asset(**kwargs)`.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_financial_suggestions.py`:

```python
"""The financial entity's suggestion target and scan shape (spec section 7)."""

from dagster_v3.defs.se_company.financial import suggestions, tables


def test_the_source_supplies_every_column_but_the_four_stamps() -> None:
    assert suggestions.STAMPED_COLUMNS == ("suggestion_id", "suggested_at", "source_run_id", "extractor_version")
    assert len(suggestions.FINANCIAL_SELECT_COLUMNS) == 59
    assert set(suggestions.FINANCIAL_SELECT_COLUMNS) | set(suggestions.STAMPED_COLUMNS) == set(tables.SUGGESTION_COLUMNS)
    assert suggestions.FINANCIAL_STATE_COLUMNS == tuple(c for c in suggestions.FINANCIAL_SELECT_COLUMNS if c not in ("company_id", "source"))
    assert len(suggestions.FINANCIAL_STATE_COLUMNS) == 57
    assert "period_key" in suggestions.FINANCIAL_STATE_COLUMNS and "source_record_uid" in suggestions.FINANCIAL_STATE_COLUMNS


def test_the_target_inserts_every_table_column_and_hashes_the_period_key() -> None:
    target = suggestions.FINANCIAL_TARGET
    assert target.qualified_table == "corpscout.se_company_financial_suggestion"
    assert set(target.insert_columns) == set(tables.SUGGESTION_COLUMNS)
    assert target.insert_columns == (*suggestions.FINANCIAL_SELECT_COLUMNS, *suggestions.STAMPED_COLUMNS)
    assert target.with_sql == "WITH (SELECT now64(3, 'UTC')) AS stamp\n"
    assert "candidate.period_key, '\\n', toString(stamp))))) AS suggestion_id, stamp AS suggested_at" in target.trailing_select_sql
    assert "%(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version" in target.trailing_select_sql
    assert target.asset_prefix == "se_company_financial_suggestions_"
    assert target.scratch_prefix == "corpscout._tmp_financial_scope_"


def test_a_live_row_has_a_figure_or_employees() -> None:
    assert suggestions.LIVE_ROW_PREDICATE.startswith("(revenue_amount_original IS NOT NULL OR ")
    assert suggestions.LIVE_ROW_PREDICATE.endswith(" OR employees IS NOT NULL)")
    assert suggestions.LIVE_ROW_PREDICATE.count(" IS NOT NULL") == 41


def test_a_tombstone_copies_the_key_and_its_two_check_columns_and_nulls_every_value() -> None:
    values = suggestions.tombstone_values("ratsit")
    assert set(values) == set(suggestions.FINANCIAL_SELECT_COLUMNS)
    assert values["period_key"] == "stored.period_key" and values["scope"] == "stored.scope" and values["period_end"] == "stored.period_end"
    assert values["source"] == "'ratsit'" and values["amount_scale"] == "toUInt32(1)" and values["period_end_derived"] == "toUInt8(0)"
    assert values["currency"] == "CAST(NULL AS Nullable(String))"
    for column in tables.SUGGESTION_VALUE_COLUMNS:
        assert values[column].startswith("CAST(NULL AS Nullable("), column
    scan = suggestions.scan_for("ratsit")
    assert scan.key_column == "period_key" and scan.tombstone_columns == ("period_key", "scope", "period_end")


def test_period_months_is_the_rounded_month_count_or_null() -> None:
    assert suggestions.period_months_sql("a", "b") == (
        "if(a IS NULL OR b IS NULL, CAST(NULL AS Nullable(UInt16)), toUInt16(round((dateDiff('day', a, b) + 1) / 30.4375)))"
    )
    assert suggestions.universe_join_sql("m").endswith("ON universe.company_id = m.company_id")
```

- [ ] **Step 2: Run them to verify they fail**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_suggestions.py -q
```

Expected: FAIL at import (`cannot import name 'suggestions'`).

- [ ] **Step 3: Write the module**

`src/dagster_v3/defs/se_company/financial/suggestions.py`, exactly:

```python
"""The financial entity's target for the shared suggestion extract helper and the shape of
its state-hash scan (spec 2026-09-11 section 7; state_scan.py holds the machinery).

A source delivers one row per company and period; the row's key is period_key
('<scope>:<period_end>', the DDL's CHECK derives it from the two columns beside it). The
change scan hashes everything a source delivers per company (every column but company_id
and source), so a changed figure, a changed date or a vanished period all select the
company once, and the page writes the live rows plus one tombstone per stored period the
source no longer delivers. A live row always carries at least one figure or an employee
count (every extractor filters empty rows out); a tombstone carries none, and copies scope
and period_end from the stored row so the CHECK on period_key still holds.

currency is NULL when a source names none, never '' (the DDL refuses ''); money on a row
without a currency can never win the fold (spec 6.2).
"""

from collections.abc import Mapping
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company import state_scan
from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget
from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.assets import GROUP_NAME

# What a source supplies: every column of the table except the four the INSERT stamps.
STAMPED_COLUMNS: tuple[str, ...] = ("suggestion_id", "suggested_at", "source_run_id", "extractor_version")
FINANCIAL_SELECT_COLUMNS: tuple[str, ...] = tuple(
    column for column in tables.SUGGESTION_COLUMNS if column not in STAMPED_COLUMNS
)
# What a company's state hash covers: everything a source delivers except its own key.
FINANCIAL_STATE_COLUMNS: tuple[str, ...] = tuple(
    column for column in FINANCIAL_SELECT_COLUMNS if column not in ("company_id", "source")
)

# The nullable columns with the exact CAST each side of every UNION ALL uses, so a tombstone
# row and a source that does not deliver a column agree on the type.
NULLABLE_COLUMN_TYPES: dict[str, str] = {
    "period_start": "Date32", "fiscal_year": "UInt16", "period_months": "UInt16",
    "filing_fiscal_year": "UInt16", "currency": "String", "employees": "UInt64",
    "fx_rate_to_usd": "Decimal(38, 12)", "fx_rate_date": "Date32",
    **{column: "Decimal(38, 6)" for column in tables.MONETARY_SUGGESTION_COLUMNS},
}
NULL_SQL: Mapping[str, str] = {
    column: f"CAST(NULL AS Nullable({type_}))" for column, type_ in NULLABLE_COLUMN_TYPES.items()
}
MONEY_NULL_SQL: Mapping[str, str] = {
    column: NULL_SQL[column] for column in tables.MONETARY_SUGGESTION_COLUMNS
}

# A live row has at least one figure or an employee count; a tombstone has none.
LIVE_ROW_PREDICATE = "(" + " OR ".join(f"{column} IS NOT NULL" for column in tables.SUGGESTION_VALUE_COLUMNS) + ")"

# One clock read per statement (two now64() calls were measured to differ), hashed into the
# lineage id with the row's key.
FINANCIAL_WITH_SQL = "WITH (SELECT now64(3, 'UTC')) AS stamp\n"
FINANCIAL_TRAILING_SELECT_SQL = (
    "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', "
    "candidate.period_key, '\\n', toString(stamp))))) AS suggestion_id, stamp AS suggested_at, "
    "%(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version"
)

FINANCIAL_TARGET = SuggestionTarget(
    database=tables.DATABASE,
    table=tables.SUGGESTION_TABLE,
    insert_columns=(*FINANCIAL_SELECT_COLUMNS, *STAMPED_COLUMNS),
    select_columns=FINANCIAL_SELECT_COLUMNS,
    trailing_select_sql=FINANCIAL_TRAILING_SELECT_SQL,
    asset_prefix="se_company_financial_suggestions_",
    group_name=GROUP_NAME,
    scratch_prefix=tables.SCRATCH_SCOPE_PREFIX,
    with_sql=FINANCIAL_WITH_SQL,
)

# Every extractor joins the published universe; a company without a basic-info row is never
# suggested (the person extractors do the same).
UNIVERSE_JOIN_SQL = (
    "INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
    "    ON universe.company_id = {alias}.company_id"
)


def universe_join_sql(alias: str) -> str:
    return UNIVERSE_JOIN_SQL.format(alias=alias)


def period_months_sql(start: str, end: str) -> str:
    """The rounded month count between two Date32 expressions, NULL when either is NULL."""
    return (
        f"if({start} IS NULL OR {end} IS NULL, CAST(NULL AS Nullable(UInt16)), "
        f"toUInt16(round((dateDiff('day', {start}, {end}) + 1) / 30.4375)))"
    )


def tombstone_values(source: str) -> dict[str, str]:
    """A tombstone row for `source`: the key and the two columns its CHECK derives it from
    copied from the stored row, every value NULL, the defaults for the rest."""
    return {
        "company_id": "stored.company_id",
        "source": f"'{source}'",
        "period_key": "stored.period_key",
        "source_record_uid": "''",
        "scope": "stored.scope",
        "period_end": "stored.period_end",
        "period_end_derived": "toUInt8(0)",
        "amount_scale": "toUInt32(1)",
        "fx_source": "''",
        "decided_by": "''",
        "note": "''",
        **NULL_SQL,
    }


def scan_for(source: str) -> state_scan.StateScan:
    return state_scan.StateScan(
        target=FINANCIAL_TARGET,
        select_columns=FINANCIAL_SELECT_COLUMNS,
        state_columns=FINANCIAL_STATE_COLUMNS,
        key_column="period_key",
        live_row_predicate=LIVE_ROW_PREDICATE,
        tombstone_columns=("period_key", "scope", "period_end"),
        tombstone_values=tombstone_values(source),
    )


def live_select_sql(
    *, columns: Mapping[str, str], from_sql: str, where_sql: str, with_sql: str = ""
) -> str:
    return state_scan.live_select_sql(
        scan_for("x"), columns=columns, from_sql=from_sql, where_sql=where_sql, with_sql=with_sql
    )


def financial_changed_scope_sql(*, source: str, live_sql: str) -> str:
    return state_scan.changed_scope_sql(scan_for(source), source=source, live_sql=live_sql)


def financial_select_sql(*, source: str, live_sql: str) -> str:
    return state_scan.select_sql(scan_for(source), source=source, live_sql=live_sql)


def define_financial_suggestion_asset(**kwargs: Any) -> dg.AssetsDefinition:
    return state_scan.define_scan_asset(scan_for("x"), **kwargs)
```

- [ ] **Step 4: Run the tests and ruff**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_suggestions.py -q
uv run ruff check src/dagster_v3/defs/se_company/financial tests/test_se_company_financial_suggestions.py
```

Expected: 5 passed; ruff clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/suggestions.py \
        corpscout/services/dagster_v3/tests/test_se_company_financial_suggestions.py
git commit -m "feat(se-financial): the suggestion target and state-hash scan shape of the financial entity"
```

---

### Task 3: `financial/bolagsverket.py` — the reported and comparative sources

**Files:**
- Create: `src/dagster_v3/defs/se_company/financial/bolagsverket.py`
- Test: `tests/test_se_company_financial_extractors_sql.py` (new; Tasks 4 and 5 append to it)

**Interfaces:**
- Consumes: Task 2's module.
- Produces: `SOURCE_REPORTED`, `SOURCE_COMPARATIVE`, `BOLAGSVERKET_EXTRACTOR_VERSION`, `BOLAGSVERKET_COMPARATIVE_EXTRACTOR_VERSION`, `reported_live_sql(scoped=False)`, `comparative_live_sql(scoped=False)`, `bolagsverket_current_sql()`, `reported_changed_scope_sql()`, `reported_select_sql()`, `comparative_changed_scope_sql()`, `comparative_select_sql()`, assets `se_company_financial_suggestions_bolagsverket` and `se_company_financial_suggestions_bolagsverket_comparative`.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_financial_extractors_sql.py` — the whole file below; Tasks 4 and 5 already have their tests in it, so their imports fail until those modules exist. Write the file now with the imports of `esef` and `ratsit` commented out and the two functions that use them (`test_esef_...`, `test_ratsit_...`, and the `esef`/`ratsit` lines of the last two tests) skipped with `pytest.skip` markers; Tasks 4 and 5 remove the skips as they land. The final text is:

```python
"""The four financial extractors' SQL (spec section 7): the contracts a fake client cannot
settle are in the clickhouse-local test; these pin the text each module renders."""

from dagster_v3.defs.se_company.financial import bolagsverket, esef, ratsit
from dagster_v3.defs.se_company.financial.suggestions import FINANCIAL_SELECT_COLUMNS, FINANCIAL_TARGET
from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql


def _projection(sql: str) -> list[str]:
    """The aliases of the outer SELECT (the last bare `SELECT` line up to the next unindented line)."""
    lines = sql.splitlines()
    start = max(i for i, line in enumerate(lines) if line == "SELECT") + 1
    aliases = []
    for line in lines[start:]:
        if not line.startswith("    "):
            break
        aliases.append(line.rsplit(" AS ", 1)[1].rstrip(","))
    return aliases


def test_bolagsverket_reported_picks_the_fuller_statement_per_period() -> None:
    live = bolagsverket.reported_live_sql()
    assert "PARTITION BY m.company_id, m.report_period_end ORDER BY " in live
    assert live.index("toUInt8(m.revenue_amount_original IS NOT NULL) + ") < live.index("DESC, m.statement_key ASC")
    assert "WHERE m.observation_kind = 'reported'" in live and live.endswith("WHERE m.rn = 1")
    assert "FROM corpscout.se_bolagsverket_financial_metrics AS m FINAL" in live
    assert "ON universe.company_id = m.company_id" in live
    assert "    concat('standalone:', toString(m.report_period_end)) AS period_key" in live
    assert "    'standalone' AS scope" in live and "    m.statement_key AS source_record_uid" in live
    assert "    m.operating_profit_loss_amount_original AS operating_result_amount_original" in live
    assert "    m.profit_loss_amount_usd AS net_result_amount_usd" in live
    assert "    CAST(NULL AS Nullable(Decimal(38, 6))) AS ebitda_amount_original" in live
    assert "    nullIf(toString(m.currency), '') AS currency" in live and "    toUInt32(1) AS amount_scale" in live
    assert "    m.employees AS employees" in live and "    m.source_fiscal_year AS filing_fiscal_year" in live
    assert "%(company_ids)s" not in live and "AND m.company_id IN %(company_ids)s" in bolagsverket.reported_live_sql(scoped=True)


def test_bolagsverket_comparative_takes_the_newest_restating_filing_and_two_figures() -> None:
    live = bolagsverket.comparative_live_sql()
    assert "WHERE m.observation_kind = 'comparative'" in live
    assert "ORDER BY ifNull(m.source_fiscal_year, 0) DESC, m.statement_key ASC" in live
    assert "    m.revenue_amount_original AS revenue_amount_original" in live
    assert "    m.total_assets_amount_usd AS total_assets_amount_usd" in live
    assert "    CAST(NULL AS Nullable(Decimal(38, 6))) AS equity_amount_original" in live
    assert "    CAST(NULL AS Nullable(UInt64)) AS employees" in live
    assert "    'bolagsverket_comparative' AS source" in live


def test_esef_composes_versions_newest_first_and_maps_scope_and_types() -> None:
    live = esef.esef_live_sql()
    assert "toUInt32OrZero(extract(m.fxo_id, '-([0-9]+)$')) AS version" in live
    assert "INNER JOIN corpscout.se_esef_filings AS f\n        ON f.lei = m.lei AND f.period_end = m.period_end AND f.fxo_id = m.fxo_id" in live
    assert "WHERE m.scope = 'consolidated_ifrs'" in live and "GROUP BY m.company_id, m.period_end" in live
    assert "argMaxIf(m.revenue_amount_original, m.version, m.revenue_amount_original IS NOT NULL) AS revenue_amount_original" in live
    assert "argMaxIf(m.currency, m.version, m.currency != '') AS currency" in live
    assert "    concat('consolidated:', toString(e.period_end)) AS period_key" in live
    assert "    CAST(e.cash_and_bank_amount_original AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_original" in live
    assert "    CAST(if(e.employees < 0, NULL, e.employees) AS Nullable(UInt64)) AS employees" in live
    assert "    CAST(e.fx_rate_to_usd AS Nullable(Decimal(38, 12))) AS fx_rate_to_usd" in live
    assert "    nullIf(toString(e.currency), '') AS currency" in live
    assert "AND f.company_id IN %(company_ids)s" in esef.esef_live_sql(scoped=True)


def test_ratsit_scales_by_unit_derives_the_end_date_and_ranks_duplicates() -> None:
    live = ratsit.ratsit_live_sql()
    assert "argMax(r.result_sha256, r.normalized_at) AS result_sha256" in live
    assert "multiIf(p.scope = 'company', 'standalone', p.scope = 'consolidated', 'consolidated', '') AS entity_scope" in live
    assert "ifNull(p.period_end, makeDate32(p.fiscal_year, 12, 31)) AS effective_end" in live
    assert "PARTITION BY p.company_id, entity_scope, effective_end\n            ORDER BY ifNull(p.period_months, 0) DESC, p.financial_report_index DESC, p.period_index DESC" in live
    assert "WHERE p.monetary_unit IS NOT NULL\n        AND p.scope IN ('company', 'consolidated')\n        AND (p.period_end IS NOT NULL OR p.fiscal_year BETWEEN 1900 AND 2299)" in live
    assert "    p.revenue_amount * multiIf(p.monetary_unit = 'MSEK', 1000000, p.monetary_unit = 'TSEK', 1000, 1) AS revenue_amount_original" in live
    assert "    p.revenue_amount_usd AS revenue_amount_usd" in live
    assert "    toUInt8(p.period_end IS NULL) AS period_end_derived" in live
    assert "    'SEK' AS currency" in live and "    CAST(p.employee_count AS Nullable(UInt64)) AS employees" in live
    assert "    CAST(NULL AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_original" in live
    scoped = ratsit.ratsit_live_sql(scoped=True)
    assert scoped.count("%(company_ids)s") == 2  # the report CTE and the periods scan


def test_every_extractor_projects_the_select_columns_in_order_and_inserts_the_target() -> None:
    for live in (bolagsverket.reported_live_sql(), bolagsverket.comparative_live_sql(), esef.esef_live_sql(), ratsit.ratsit_live_sql()):
        assert _projection(live) == list(FINANCIAL_SELECT_COLUMNS)
    insert = insert_page_sql(select_sql=bolagsverket.reported_select_sql(), target=FINANCIAL_TARGET)
    assert insert.startswith(f"INSERT INTO corpscout.se_company_financial_suggestion ({', '.join(FINANCIAL_TARGET.insert_columns)})\nWITH (SELECT now64(3, 'UTC')) AS stamp\n")
    assert insert.count("live_period_keys") == 3


def test_the_four_assets_carry_their_sources_and_deps() -> None:
    import dagster as dg
    assets = {
        bolagsverket.se_company_financial_suggestions_bolagsverket: ("bolagsverket", "se_bolagsverket_financial_metrics_clickhouse"),
        bolagsverket.se_company_financial_suggestions_bolagsverket_comparative: ("bolagsverket_comparative", "se_bolagsverket_financial_metrics_clickhouse"),
        esef.se_company_financial_suggestions_esef: ("esef", "esef_financial_metrics_clickhouse"),
        ratsit.se_company_financial_suggestions_ratsit: ("ratsit", "se_ratsit_financial_periods_usd"),
    }
    for asset, (source, dep) in assets.items():
        assert asset.key == dg.AssetKey(f"se_company_financial_suggestions_{source}")
        assert dg.AssetKey(dep) in asset.dependency_keys and dg.AssetKey("se_company_basic_info_fold") in asset.dependency_keys
        spec = next(iter(asset.specs))
        assert spec.metadata["source"] == source and spec.group_name == "se_company_financial"
```

- [ ] **Step 2: Run the bolagsverket tests to verify they fail**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_extractors_sql.py -q -k bolagsverket
```

Expected: FAIL at import (`cannot import name 'bolagsverket'`).

- [ ] **Step 3: Write the module**

`src/dagster_v3/defs/se_company/financial/bolagsverket.py`, exactly:

```python
"""Bolagsverket annual accounts -> financial suggestions (spec 2026-09-11 section 7): the
reported filings as source `bolagsverket`, the restated prior-year columns of later filings
as source `bolagsverket_comparative`.

se_bolagsverket_financial_metrics holds one row per filing and represented period:
observation_kind `reported` for the period the filing covers, `comparative` for the
prior-year column it restates. Both are standalone (legal-entity) accounts, so every period
key is `standalone:<report_period_end>`. Where a period has two reported statements (14
groups on 2026-09-11, the same filing archived twice) the one with more figures wins, then
the smaller statement key; where several later filings restate the same period the newest
filing (greatest source_fiscal_year) wins. The table is rebuilt whole every Saturday with a
single resolved_at, which is why the change scan is the state hash.
"""

import dagster as dg

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.suggestions import (
    MONEY_NULL_SQL,
    NULL_SQL,
    define_financial_suggestion_asset,
    financial_changed_scope_sql,
    financial_select_sql,
    live_select_sql,
    period_months_sql,
    universe_join_sql,
)

SOURCE_REPORTED = "bolagsverket"
SOURCE_COMPARATIVE = "bolagsverket_comparative"
BOLAGSVERKET_EXTRACTOR_VERSION = "bolagsverket-financial-v1"
BOLAGSVERKET_COMPARATIVE_EXTRACTOR_VERSION = "bolagsverket-comparative-financial-v1"

# entity field -> metrics column stem (the twelve register metrics; the other eight are NULL).
BOLAGSVERKET_MONEY: dict[str, str] = {
    "revenue": "revenue",
    "operating_result": "operating_profit_loss",
    "net_result": "profit_loss",
    "total_assets": "total_assets",
    "equity": "equity",
    "liabilities": "liabilities",
    "cash_and_bank": "cash_and_bank",
    "current_assets": "current_assets",
    "current_liabilities": "current_liabilities",
    "personnel_expenses": "personnel_expenses",
    "wages_and_salaries": "wages_and_salaries",
}
# The two columns a later filing restates (spec section 7).
COMPARATIVE_MONEY: dict[str, str] = {"revenue": "revenue", "total_assets": "total_assets"}


def _money_sql(mapping: dict[str, str]) -> dict[str, str]:
    values: dict[str, str] = dict(MONEY_NULL_SQL)
    for field, stem in mapping.items():
        values[tables.original_column(field)] = f"m.{stem}_amount_original"
        values[tables.usd_column(field)] = f"m.{stem}_amount_usd"
    return values


def _column_sql(source: str, mapping: dict[str, str], employees: bool) -> dict[str, str]:
    return {
        "company_id": "m.company_id",
        "source": f"'{source}'",
        "period_key": "concat('standalone:', toString(m.report_period_end))",
        "source_record_uid": "m.statement_key",
        "scope": "'standalone'",
        "period_end": "m.report_period_end",
        "period_end_derived": "toUInt8(0)",
        "period_start": "m.report_period_start",
        "fiscal_year": "m.fiscal_year",
        "period_months": period_months_sql("m.report_period_start", "m.report_period_end"),
        "filing_fiscal_year": "m.source_fiscal_year",
        "currency": "nullIf(toString(m.currency), '')",
        "amount_scale": "toUInt32(1)",
        **_money_sql(mapping),
        "employees": "m.employees" if employees else NULL_SQL["employees"],
        "fx_rate_to_usd": "m.fx_rate_to_usd",
        "fx_rate_date": "m.fx_rate_date",
        "fx_source": "m.fx_source",
        "decided_by": "''",
        "note": "''",
    }


FIGURE_COUNT_SQL = " + ".join(
    f"toUInt8(m.{stem}_amount_original IS NOT NULL)" for stem in BOLAGSVERKET_MONEY.values()
) + " + toUInt8(m.employees IS NOT NULL)"


def _ranked_cte_sql(*, observation_kind: str, order_sql: str, scoped: bool) -> str:
    company_filter = "\n        AND m.company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH ranked AS (\n"
        "    SELECT\n"
        "        m.*,\n"
        "        row_number() OVER (\n"
        f"            PARTITION BY m.company_id, m.report_period_end ORDER BY {order_sql}\n"
        "        ) AS rn\n"
        "    FROM corpscout.se_bolagsverket_financial_metrics AS m FINAL\n"
        f"    {universe_join_sql('m')}\n"
        f"    WHERE m.observation_kind = '{observation_kind}'\n"
        f"        AND m.report_period_end IS NOT NULL{company_filter}\n"
        ")\n"
    )


REPORTED_ORDER_SQL = f"{FIGURE_COUNT_SQL} DESC, m.statement_key ASC"
COMPARATIVE_ORDER_SQL = "ifNull(m.source_fiscal_year, 0) DESC, m.statement_key ASC"
LIVE_WHERE_SQL = "WHERE m.rn = 1"


def reported_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=_column_sql(SOURCE_REPORTED, BOLAGSVERKET_MONEY, employees=True),
        from_sql="FROM ranked AS m",
        where_sql=LIVE_WHERE_SQL,
        with_sql=_ranked_cte_sql(observation_kind="reported", order_sql=REPORTED_ORDER_SQL, scoped=scoped),
    )


def comparative_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=_column_sql(SOURCE_COMPARATIVE, COMPARATIVE_MONEY, employees=False),
        from_sql="FROM ranked AS m",
        where_sql=LIVE_WHERE_SQL,
        with_sql=_ranked_cte_sql(observation_kind="comparative", order_sql=COMPARATIVE_ORDER_SQL, scoped=scoped),
    )


def bolagsverket_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        "SELECT m.company_id AS company_id, max(m.resolved_at) AS observed_at\n"
        "FROM corpscout.se_bolagsverket_financial_metrics AS m FINAL\n"
        f"{universe_join_sql('m')}\n"
        "GROUP BY m.company_id"
    )


def reported_changed_scope_sql() -> str:
    return financial_changed_scope_sql(source=SOURCE_REPORTED, live_sql=reported_live_sql())


def reported_select_sql() -> str:
    return financial_select_sql(source=SOURCE_REPORTED, live_sql=reported_live_sql(scoped=True))


def comparative_changed_scope_sql() -> str:
    return financial_changed_scope_sql(source=SOURCE_COMPARATIVE, live_sql=comparative_live_sql())


def comparative_select_sql() -> str:
    return financial_select_sql(source=SOURCE_COMPARATIVE, live_sql=comparative_live_sql(scoped=True))


BOLAGSVERKET_DEPS = [
    dg.AssetKey("se_bolagsverket_financial_metrics_clickhouse"),
    dg.AssetKey("se_company_basic_info_fold"),
]

se_company_financial_suggestions_bolagsverket = define_financial_suggestion_asset(
    source=SOURCE_REPORTED,
    extractor_version=BOLAGSVERKET_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=reported_select_sql(),
    changed_scope_override=reported_changed_scope_sql(),
    deps=BOLAGSVERKET_DEPS,
    description=(
        "Every reported Bolagsverket annual-account period of a published company as a "
        "standalone financial suggestion in se_company_financial_suggestion (one row per "
        "period end; the fuller statement wins a duplicated period); a period the rebuilt "
        "source no longer delivers is tombstoned. execute=false previews."
    ),
)

se_company_financial_suggestions_bolagsverket_comparative = define_financial_suggestion_asset(
    source=SOURCE_COMPARATIVE,
    extractor_version=BOLAGSVERKET_COMPARATIVE_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=comparative_select_sql(),
    changed_scope_override=comparative_changed_scope_sql(),
    deps=BOLAGSVERKET_DEPS,
    description=(
        "The prior-year revenue and total assets a later Bolagsverket filing restates, as "
        "source bolagsverket_comparative in se_company_financial_suggestion (one row per "
        "restated period end, from the newest restating filing; filing_fiscal_year names it); "
        "a restatement the rebuilt source no longer delivers is tombstoned. execute=false previews."
    ),
)
```

- [ ] **Step 4: Run the tests and ruff**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_extractors_sql.py -q -k bolagsverket
uv run ruff check src/dagster_v3/defs/se_company/financial tests/test_se_company_financial_extractors_sql.py
```

Expected: 2 passed (the reported and comparative tests); ruff clean.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/bolagsverket.py \
        corpscout/services/dagster_v3/tests/test_se_company_financial_extractors_sql.py
git commit -m "feat(se-financial): Bolagsverket reported and comparative periods as financial suggestions"
```

---

### Task 4: `financial/esef.py`

**Files:**
- Create: `src/dagster_v3/defs/se_company/financial/esef.py`
- Modify: `tests/test_se_company_financial_extractors_sql.py` (restore the `esef` import and un-skip its test)

**Interfaces:**
- Produces: `SOURCE = "esef"`, `ESEF_EXTRACTOR_VERSION`, `esef_live_sql(scoped=False)`, `esef_current_sql()`, `esef_changed_scope_sql()`, `esef_select_sql()`, asset `se_company_financial_suggestions_esef`.

- [ ] **Step 1: Un-skip the ESEF test, run it, see it fail on the missing module**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_extractors_sql.py -q -k esef
```

- [ ] **Step 2: Write the module**

`src/dagster_v3/defs/se_company/financial/esef.py`, exactly:

```python
"""ESEF consolidated IFRS metrics -> financial suggestions (spec 2026-09-11 section 7).

esef_financial_metrics is keyed by LEI and fxo_id; corpscout.se_esef_filings (the
country-scoped view of migration 000395) carries the register-verified Swedish company_id
for each filing, so the join on (lei, period_end, fxo_id) is the whole country mapping. A
CONSUMER NEVER WRITES FINAL AFTER THE VIEW NAME; the metrics table is read FINAL here.

Scope `consolidated_ifrs` is the only one the table carries for Swedish filers (measured
2026-09-11); it maps to the entity's `consolidated`, and any other scope value is skipped.
A period can have several filing versions (61 amended periods): the newest version -- the
numeric suffix of fxo_id -- wins field by field and older versions fill its gaps, today's
serving-view logic, so an amendment that drops a metric does not lose it. A blank currency
becomes NULL, which keeps that row's money out of the fold (spec 6.2).
"""

import dagster as dg

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.suggestions import (
    MONEY_NULL_SQL,
    NULL_SQL,
    define_financial_suggestion_asset,
    financial_changed_scope_sql,
    financial_select_sql,
    live_select_sql,
    period_months_sql,
    universe_join_sql,
)

SOURCE = "esef"
ESEF_EXTRACTOR_VERSION = "esef-financial-v1"

# entity field -> metrics column stem (nine fields; the other eleven are NULL).
ESEF_MONEY: dict[str, str] = {
    "revenue": "revenue",
    "operating_result": "operating_profit",
    "net_result": "profit_loss",
    "total_assets": "total_assets",
    "equity": "equity",
    "liabilities": "liabilities",
    "cash_and_bank": "cash",
    "personnel_expenses": "personnel_expenses",
}
VERSION_SQL = "toUInt32OrZero(extract(m.fxo_id, '-([0-9]+)$'))"


def _composed_money_sql() -> tuple[list[str], dict[str, str]]:
    """The argMaxIf aggregates of the `composed` CTE and the projection that reads them."""
    aggregates: list[str] = []
    values: dict[str, str] = dict(MONEY_NULL_SQL)
    for field, stem in ESEF_MONEY.items():
        for suffix, entity in (("original", tables.original_column(field)), ("usd", tables.usd_column(field))):
            source_column = f"{stem}_amount_{suffix}"
            aggregates.append(
                f"        argMaxIf(m.{source_column}, m.version, m.{source_column} IS NOT NULL) AS {entity}"
            )
            values[entity] = f"CAST(e.{entity} AS Nullable(Decimal(38, 6)))"
    return aggregates, values


def _composed_cte_sql(*, scoped: bool) -> str:
    company_filter = "\n        AND f.company_id IN %(company_ids)s" if scoped else ""
    money_aggregates, _ = _composed_money_sql()
    aggregates = ",\n".join(money_aggregates)
    return (
        "WITH versions AS (\n"
        "    SELECT\n"
        "        f.company_id AS company_id,\n"
        "        m.period_end AS period_end,\n"
        "        m.fxo_id AS fxo_id,\n"
        f"        {VERSION_SQL} AS version,\n"
        "        m.period_start AS period_start,\n"
        "        m.fiscal_year AS fiscal_year,\n"
        "        m.currency AS currency,\n"
        + "".join(
            f"        m.{stem}_amount_{suffix} AS {stem}_amount_{suffix},\n"
            for stem in ESEF_MONEY.values() for suffix in ("original", "usd")
        )
        + "        m.employees AS employees,\n"
        "        m.fx_rate_to_usd AS fx_rate_to_usd,\n"
        "        m.fx_rate_date AS fx_rate_date,\n"
        "        m.fx_source AS fx_source\n"
        "    FROM corpscout.esef_financial_metrics AS m FINAL\n"
        "    INNER JOIN corpscout.se_esef_filings AS f\n"
        "        ON f.lei = m.lei AND f.period_end = m.period_end AND f.fxo_id = m.fxo_id\n"
        f"    {universe_join_sql('f')}\n"
        f"    WHERE m.scope = 'consolidated_ifrs'{company_filter}\n"
        "),\n"
        "composed AS (\n"
        "    SELECT\n"
        "        m.company_id AS company_id,\n"
        "        m.period_end AS period_end,\n"
        "        argMax(m.fxo_id, m.version) AS fxo_id,\n"
        "        argMaxIf(m.period_start, m.version, m.period_start IS NOT NULL) AS period_start,\n"
        "        argMax(m.fiscal_year, m.version) AS fiscal_year,\n"
        "        argMaxIf(m.currency, m.version, m.currency != '') AS currency,\n"
        f"{aggregates},\n"
        "        argMaxIf(m.employees, m.version, m.employees IS NOT NULL) AS employees,\n"
        "        argMaxIf(m.fx_rate_to_usd, m.version, m.fx_rate_to_usd IS NOT NULL) AS fx_rate_to_usd,\n"
        "        argMaxIf(m.fx_rate_date, m.version, m.fx_rate_date IS NOT NULL) AS fx_rate_date,\n"
        "        argMaxIf(m.fx_source, m.version, m.fx_source != '') AS fx_source\n"
        "    FROM versions AS m\n"
        "    GROUP BY m.company_id, m.period_end\n"
        ")\n"
    )


def _column_sql() -> dict[str, str]:
    _, money = _composed_money_sql()
    return {
        "company_id": "e.company_id",
        "source": f"'{SOURCE}'",
        "period_key": "concat('consolidated:', toString(e.period_end))",
        "source_record_uid": "e.fxo_id",
        "scope": "'consolidated'",
        "period_end": "e.period_end",
        "period_end_derived": "toUInt8(0)",
        "period_start": "e.period_start",
        "fiscal_year": "if(e.fiscal_year BETWEEN 1900 AND 2299, toUInt16(e.fiscal_year), CAST(NULL AS Nullable(UInt16)))",
        "period_months": period_months_sql("e.period_start", "e.period_end"),
        "filing_fiscal_year": NULL_SQL["filing_fiscal_year"],
        "currency": "nullIf(toString(e.currency), '')",
        "amount_scale": "toUInt32(1)",
        **money,
        "employees": "CAST(if(e.employees < 0, NULL, e.employees) AS Nullable(UInt64))",
        "fx_rate_to_usd": "CAST(e.fx_rate_to_usd AS Nullable(Decimal(38, 12)))",
        "fx_rate_date": "e.fx_rate_date",
        "fx_source": "toString(e.fx_source)",
        "decided_by": "''",
        "note": "''",
    }


def esef_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=_column_sql(),
        from_sql="FROM composed AS e",
        where_sql="WHERE 1 = 1",
        with_sql=_composed_cte_sql(scoped=scoped),
    )


def esef_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        "SELECT f.company_id AS company_id, max(toDateTime64(m.resolved_at, 3, 'UTC')) AS observed_at\n"
        "FROM corpscout.esef_financial_metrics AS m FINAL\n"
        "INNER JOIN corpscout.se_esef_filings AS f\n"
        "    ON f.lei = m.lei AND f.period_end = m.period_end AND f.fxo_id = m.fxo_id\n"
        f"{universe_join_sql('f')}\n"
        "GROUP BY f.company_id"
    )


def esef_changed_scope_sql() -> str:
    return financial_changed_scope_sql(source=SOURCE, live_sql=esef_live_sql())


def esef_select_sql() -> str:
    return financial_select_sql(source=SOURCE, live_sql=esef_live_sql(scoped=True))


se_company_financial_suggestions_esef = define_financial_suggestion_asset(
    source=SOURCE,
    extractor_version=ESEF_EXTRACTOR_VERSION,
    current_sql=esef_current_sql(),
    select_sql=esef_select_sql(),
    changed_scope_override=esef_changed_scope_sql(),
    deps=[
        dg.AssetKey("esef_financial_metrics_clickhouse"),
        dg.AssetKey("esef_entity_registry_map_clickhouse"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every consolidated IFRS period of a Swedish ESEF filer as a financial suggestion in "
        "se_company_financial_suggestion (one row per period end; the newest filing version "
        "wins field by field, older versions fill its gaps); a period the rebuilt source no "
        "longer delivers is tombstoned. execute=false previews."
    ),
)
```

- [ ] **Step 3: Run the tests and ruff, then commit**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_extractors_sql.py -q -k "esef or bolagsverket"
uv run ruff check src/dagster_v3/defs/se_company/financial tests/test_se_company_financial_extractors_sql.py
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/esef.py corpscout/services/dagster_v3/tests/test_se_company_financial_extractors_sql.py
git commit -m "feat(se-financial): ESEF consolidated periods as financial suggestions, versions composed newest first"
```

---

### Task 5: `financial/ratsit.py`

**Files:**
- Create: `src/dagster_v3/defs/se_company/financial/ratsit.py`
- Modify: `tests/test_se_company_financial_extractors_sql.py` (restore the `ratsit` import, un-skip its test and the last two tests' ratsit lines)

**Interfaces:**
- Produces: `SOURCE = "ratsit"`, `RATSIT_EXTRACTOR_VERSION`, `ratsit_live_sql(scoped=False)`, `ratsit_current_sql()`, `ratsit_changed_scope_sql()`, `ratsit_select_sql()`, asset `se_company_financial_suggestions_ratsit`.

- [ ] **Step 1: Un-skip the Ratsit test, run it, see it fail on the missing module**

- [ ] **Step 2: Write the module**

`src/dagster_v3/defs/se_company/financial/ratsit.py`, exactly:

```python
"""Ratsit financial periods -> financial suggestions (spec 2026-09-11 section 7), after
slice 0 filled the USD twins.

se_ratsit_financial_periods holds each company's latest report (older report hashes are
replaced by the normalizer), one row per report period: scope `company` is the entity's
standalone, `consolidated` stays consolidated, anything else is skipped. Figures are
published in the row's monetary_unit (MSEK for every row today), so the original is the
figure times the unit's scale and amount_scale records it; the USD twins are already
full-unit dollars. A missing period end becomes Dec 31 of the fiscal year with
period_end_derived = 1 -- guarded to Date32's range, because makeDate32 returns 1970-01-01
for an out-of-range year instead of clamping; a row with neither a date nor a fiscal year
in range is skipped, as is a row without a unit (it would scale as SEK). Where a report
carries two periods with one scope and end (577 on 2026-09-11) the longer one wins, then
the later report and period index.
"""

import dagster as dg

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.suggestions import (
    MONEY_NULL_SQL,
    NULL_SQL,
    define_financial_suggestion_asset,
    financial_changed_scope_sql,
    financial_select_sql,
    live_select_sql,
    universe_join_sql,
)

SOURCE = "ratsit"
RATSIT_EXTRACTOR_VERSION = "ratsit-financial-v1"

# entity field -> periods column stem (seventeen fields; cash_and_bank, personnel_expenses
# and wages_and_salaries are NULL: Ratsit does not publish them).
RATSIT_MONEY: dict[str, str] = {
    "revenue": "revenue_amount",
    "operating_costs": "operating_costs_amount",
    "operating_result": "operating_profit_amount",
    "result_after_financial_items": "profit_after_financial_items_amount",
    "net_result": "net_income_amount",
    "ebitda": "ebitda_amount",
    "total_assets": "total_assets_amount",
    "fixed_assets": "fixed_assets_amount",
    "current_assets": "current_assets_amount",
    "equity": "equity_amount",
    "share_capital": "share_capital_amount",
    "untaxed_reserves": "untaxed_reserves_amount",
    "provisions": "provisions_amount",
    "liabilities": "liabilities_amount",
    "long_term_liabilities": "long_term_liabilities_amount",
    "current_liabilities": "current_liabilities_amount",
    "dividend": "dividend_amount",
}
SCALE_SQL = "multiIf(p.monetary_unit = 'MSEK', 1000000, p.monetary_unit = 'TSEK', 1000, 1)"
SCOPE_SQL = "multiIf(p.scope = 'company', 'standalone', p.scope = 'consolidated', 'consolidated', '')"
EFFECTIVE_END_SQL = "ifNull(p.period_end, makeDate32(p.fiscal_year, 12, 31))"


def _money_sql() -> dict[str, str]:
    values: dict[str, str] = dict(MONEY_NULL_SQL)
    for field, stem in RATSIT_MONEY.items():
        values[tables.original_column(field)] = f"p.{stem} * {SCALE_SQL}"
        values[tables.usd_column(field)] = f"p.{stem}_usd"
    return values


def _column_sql() -> dict[str, str]:
    return {
        "company_id": "p.company_id",
        "source": f"'{SOURCE}'",
        "period_key": "concat(p.entity_scope, ':', toString(p.effective_end))",
        "source_record_uid": (
            "concat('ratsit:', p.company_id, ':', toString(p.financial_report_index), ':', "
            "toString(p.period_index))"
        ),
        "scope": "p.entity_scope",
        "period_end": "p.effective_end",
        "period_end_derived": "toUInt8(p.period_end IS NULL)",
        "period_start": "p.period_start",
        "fiscal_year": "CAST(p.fiscal_year AS Nullable(UInt16))",
        "period_months": "p.period_months",
        "filing_fiscal_year": NULL_SQL["filing_fiscal_year"],
        "currency": "'SEK'",
        "amount_scale": f"toUInt32({SCALE_SQL})",
        **_money_sql(),
        "employees": "CAST(p.employee_count AS Nullable(UInt64))",
        "fx_rate_to_usd": "p.fx_rate_to_usd",
        "fx_rate_date": "p.fx_rate_date",
        "fx_source": "toString(p.fx_source)",
        "decided_by": "''",
        "note": "''",
    }


def _ranked_cte_sql(*, scoped: bool) -> str:
    company_filter = "\n        AND r.company_id IN %(company_ids)s" if scoped else ""
    period_filter = "\n        AND p.company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH report AS (\n"
        "    SELECT r.company_id AS company_id, argMax(r.result_sha256, r.normalized_at) AS result_sha256\n"
        "    FROM corpscout.se_ratsit_financial_reports AS r FINAL\n"
        f"    {universe_join_sql('r')}\n"
        f"    WHERE 1 = 1{company_filter}\n"
        "    GROUP BY r.company_id\n"
        "),\n"
        "ranked AS (\n"
        "    SELECT\n"
        "        p.*,\n"
        f"        {SCOPE_SQL} AS entity_scope,\n"
        f"        {EFFECTIVE_END_SQL} AS effective_end,\n"
        "        row_number() OVER (\n"
        "            PARTITION BY p.company_id, entity_scope, effective_end\n"
        "            ORDER BY ifNull(p.period_months, 0) DESC, p.financial_report_index DESC, p.period_index DESC\n"
        "        ) AS rn\n"
        "    FROM corpscout.se_ratsit_financial_periods AS p FINAL\n"
        "    INNER JOIN report ON report.company_id = p.company_id AND report.result_sha256 = p.result_sha256\n"
        "    WHERE p.monetary_unit IS NOT NULL\n"
        "        AND p.scope IN ('company', 'consolidated')\n"
        f"        AND (p.period_end IS NOT NULL OR p.fiscal_year BETWEEN 1900 AND 2299){period_filter}\n"
        ")\n"
    )


def ratsit_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=_column_sql(),
        from_sql="FROM ranked AS p",
        where_sql="WHERE p.rn = 1",
        with_sql=_ranked_cte_sql(scoped=scoped),
    )


def ratsit_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        "SELECT r.company_id AS company_id, toDateTime64(max(r.normalized_at), 3, 'UTC') AS observed_at\n"
        "FROM corpscout.se_ratsit_financial_reports AS r FINAL\n"
        f"{universe_join_sql('r')}\n"
        "GROUP BY r.company_id"
    )


def ratsit_changed_scope_sql() -> str:
    return financial_changed_scope_sql(source=SOURCE, live_sql=ratsit_live_sql())


def ratsit_select_sql() -> str:
    return financial_select_sql(source=SOURCE, live_sql=ratsit_live_sql(scoped=True))


se_company_financial_suggestions_ratsit = define_financial_suggestion_asset(
    source=SOURCE,
    extractor_version=RATSIT_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    changed_scope_override=ratsit_changed_scope_sql(),
    deps=[
        dg.AssetKey("se_ratsit_financial_periods_usd"),
        dg.AssetKey("se_ratsit_financial_reports"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every period of a published company's latest Ratsit report as a financial suggestion "
        "in se_company_financial_suggestion (standalone or consolidated; figures scaled from "
        "the published unit, USD twins from the source); a period a re-scan no longer "
        "delivers is tombstoned. execute=false previews."
    ),
)
```

- [ ] **Step 3: Run the whole SQL test file and ruff, then commit**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_extractors_sql.py -q
uv run ruff check src/dagster_v3/defs/se_company/financial tests/test_se_company_financial_extractors_sql.py
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/ratsit.py corpscout/services/dagster_v3/tests/test_se_company_financial_extractors_sql.py
git commit -m "feat(se-financial): Ratsit periods as financial suggestions, scaled from the published unit"
```

Expected: 6 passed (no skips left); ruff clean.

---

### Task 6: The four extractors on a real ClickHouse

**Files:**
- Create: `tests/fixtures/se_company_financial_source_tables.sql`
- Create: `tests/test_se_company_financial_extractors_clickhouse_local.py`

**Interfaces:**
- Consumes: Tasks 2 to 5; migrations 000377 (basic info) and 000401 (the entity); `build_se_esef_view_sql(SE_ESEF_VIEWS[0])` for `se_esef_filings`.

- [ ] **Step 1: Write the fixture**

`tests/fixtures/se_company_financial_source_tables.sql` — the production `SHOW CREATE TABLE` snapshot of 2026-09-12 with CODECs and the `index_granularity` SETTINGS stripped (a harness fixture, never a migration). Exactly:

```sql
-- Production SHOW CREATE TABLE snapshot (2026-09-12), CODECs and SETTINGS stripped.
-- Harness fixture only -- not a migration, never apply to a real ClickHouse.
CREATE TABLE corpscout.se_bolagsverket_financial_metrics
(
    `country_iso2` LowCardinality(String),
    `source_slug` LowCardinality(String),
    `source_run_id` String,
    `source_record_id` String,
    `statement_key` String,
    `source_record_uid` String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nstructured\nsweden_financial\nannual_report_xhtml\n', statement_key, '\n', statement_key)))),
    `company_id` String,
    `report_period_start` Nullable(Date32),
    `report_period_end` Nullable(Date32),
    `fiscal_year` Nullable(UInt16),
    `observation_kind` LowCardinality(String) DEFAULT 'reported',
    `source_fiscal_year` Nullable(UInt16) DEFAULT fiscal_year,
    `reported_company_name` Nullable(String),
    `source_archive_url` String,
    `source_archive_key` String,
    `source_archive_name` String,
    `nested_zip_name` String,
    `xhtml_object_key` String,
    `xhtml_source_uri` String,
    `taxonomy_entrypoint` Nullable(String),
    `currency` LowCardinality(String),
    `revenue_amount_original` Nullable(Decimal(38, 6)),
    `revenue_amount_usd` Nullable(Decimal(38, 6)),
    `operating_profit_loss_amount_original` Nullable(Decimal(38, 6)),
    `operating_profit_loss_amount_usd` Nullable(Decimal(38, 6)),
    `profit_loss_amount_original` Nullable(Decimal(38, 6)),
    `profit_loss_amount_usd` Nullable(Decimal(38, 6)),
    `total_assets_amount_original` Nullable(Decimal(38, 6)),
    `total_assets_amount_usd` Nullable(Decimal(38, 6)),
    `equity_amount_original` Nullable(Decimal(38, 6)),
    `equity_amount_usd` Nullable(Decimal(38, 6)),
    `liabilities_amount_original` Nullable(Decimal(38, 6)),
    `liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `cash_and_bank_amount_original` Nullable(Decimal(38, 6)),
    `cash_and_bank_amount_usd` Nullable(Decimal(38, 6)),
    `current_assets_amount_original` Nullable(Decimal(38, 6)),
    `current_assets_amount_usd` Nullable(Decimal(38, 6)),
    `current_receivables_amount_original` Nullable(Decimal(38, 6)),
    `current_receivables_amount_usd` Nullable(Decimal(38, 6)),
    `current_liabilities_amount_original` Nullable(Decimal(38, 6)),
    `current_liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `personnel_expenses_amount_original` Nullable(Decimal(38, 6)),
    `personnel_expenses_amount_usd` Nullable(Decimal(38, 6)),
    `wages_and_salaries_amount_original` Nullable(Decimal(38, 6)),
    `wages_and_salaries_amount_usd` Nullable(Decimal(38, 6)),
    `employees` Nullable(UInt64),
    `source_fact_count` UInt64,
    `mapped_fact_count` UInt64,
    `unmapped_numeric_fact_count` UInt64,
    `metric_warnings` String,
    `mapping_version` LowCardinality(String),
    `fx_rate_to_usd` Nullable(Decimal(38, 12)),
    `fx_rate_date` Nullable(Date32),
    `fx_source` String,
    `source_payload_hash` FixedString(64),
    `resolved_at` DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_id, ifNull(report_period_end, toDate32('1970-01-01')), statement_key)

;
CREATE TABLE corpscout.esef_financial_metrics
(
    `lei` String,
    `entity_name` String,
    `fxo_id` String,
    `country` LowCardinality(String),
    `scope` LowCardinality(String),
    `fiscal_year` Int32,
    `period_start` Nullable(Date32),
    `period_end` Date32,
    `currency` LowCardinality(String),
    `revenue_amount_original` Nullable(Decimal(38, 2)),
    `revenue_amount_usd` Nullable(Decimal(38, 2)),
    `operating_profit_amount_original` Nullable(Decimal(38, 2)),
    `operating_profit_amount_usd` Nullable(Decimal(38, 2)),
    `profit_loss_amount_original` Nullable(Decimal(38, 2)),
    `profit_loss_amount_usd` Nullable(Decimal(38, 2)),
    `total_assets_amount_original` Nullable(Decimal(38, 2)),
    `total_assets_amount_usd` Nullable(Decimal(38, 2)),
    `equity_amount_original` Nullable(Decimal(38, 2)),
    `equity_amount_usd` Nullable(Decimal(38, 2)),
    `liabilities_amount_original` Nullable(Decimal(38, 2)),
    `liabilities_amount_usd` Nullable(Decimal(38, 2)),
    `cash_amount_original` Nullable(Decimal(38, 2)),
    `cash_amount_usd` Nullable(Decimal(38, 2)),
    `personnel_expenses_amount_original` Nullable(Decimal(38, 2)),
    `personnel_expenses_amount_usd` Nullable(Decimal(38, 2)),
    `employees` Nullable(Int64),
    `mapped_fact_count` UInt32,
    `source_fact_count` UInt32,
    `mapping_version` LowCardinality(String),
    `fx_rate_to_usd` Nullable(Float64),
    `fx_rate_date` Nullable(Date32),
    `fx_source` LowCardinality(String),
    `viewer_url` String,
    `source_run_id` String,
    `resolved_at` DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id)

;
CREATE TABLE corpscout.esef_filings
(
    `lei` String,
    `entity_name` String,
    `fxo_id` String,
    `country` LowCardinality(String),
    `period_end` Date32,
    `date_added` Date32,
    `processed_at` Nullable(DateTime64(6)),
    `json_url` String,
    `package_url` String,
    `report_url` String,
    `viewer_url` String,
    `package_sha256` String,
    `error_count` UInt32,
    `warning_count` UInt32,
    `inconsistency_count` UInt32,
    `has_json_facts` UInt8,
    `source_url` String,
    `source_run_id` String,
    `resolved_at` DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (lei, period_end, fxo_id)

;
CREATE TABLE corpscout.esef_entity_registry_map
(
    `lei` String,
    `country_iso2` LowCardinality(String),
    `registry_id_raw` String,
    `registry_id` String,
    `match_source` LowCardinality(String),
    `link_status` LowCardinality(String) DEFAULT 'gleif',
    `source_run_id` String,
    `resolved_at` DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (country_iso2, registry_id, lei)

;
CREATE TABLE corpscout.se_ratsit_financial_reports
(
    `company_id` String,
    `result_sha256` FixedString(64),
    `normalizer_version` LowCardinality(String),
    `financial_report_index` UInt16,
    `scope` LowCardinality(String),
    `monetary_unit` LowCardinality(Nullable(String)),
    `period_count` UInt16,
    `normalized_at` DateTime64(6, 'UTC'),
    CONSTRAINT se_ratsit_financial_report_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_financial_report_result_hash CHECK match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_financial_report_normalizer CHECK normalizer_version != '',
    CONSTRAINT se_ratsit_financial_report_scope CHECK trimBoth(scope) != '',
    CONSTRAINT se_ratsit_financial_report_unit CHECK (monetary_unit IS NULL) OR (monetary_unit IN ('SEK', 'TSEK', 'MSEK'))
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, financial_report_index)

;
CREATE TABLE corpscout.se_ratsit_financial_periods
(
    `company_id` String,
    `result_sha256` FixedString(64),
    `normalizer_version` LowCardinality(String),
    `financial_report_index` UInt16,
    `period_index` UInt16,
    `period_kind` LowCardinality(String) DEFAULT '',
    `scope` LowCardinality(String),
    `monetary_unit` LowCardinality(Nullable(String)),
    `fiscal_year` UInt16,
    `period_start` Nullable(Date32),
    `period_end` Nullable(Date32),
    `period_months` Nullable(UInt16),
    `revenue_amount` Nullable(Decimal(38, 6)),
    `revenue_amount_usd` Nullable(Decimal(38, 6)),
    `operating_costs_amount` Nullable(Decimal(38, 6)),
    `operating_costs_amount_usd` Nullable(Decimal(38, 6)),
    `operating_profit_amount` Nullable(Decimal(38, 6)),
    `operating_profit_amount_usd` Nullable(Decimal(38, 6)),
    `profit_after_financial_items_amount` Nullable(Decimal(38, 6)),
    `profit_after_financial_items_amount_usd` Nullable(Decimal(38, 6)),
    `net_income_amount` Nullable(Decimal(38, 6)),
    `net_income_amount_usd` Nullable(Decimal(38, 6)),
    `current_assets_amount` Nullable(Decimal(38, 6)),
    `current_assets_amount_usd` Nullable(Decimal(38, 6)),
    `fixed_assets_amount` Nullable(Decimal(38, 6)),
    `fixed_assets_amount_usd` Nullable(Decimal(38, 6)),
    `share_capital_amount` Nullable(Decimal(38, 6)),
    `share_capital_amount_usd` Nullable(Decimal(38, 6)),
    `equity_amount` Nullable(Decimal(38, 6)),
    `equity_amount_usd` Nullable(Decimal(38, 6)),
    `untaxed_reserves_amount` Nullable(Decimal(38, 6)),
    `untaxed_reserves_amount_usd` Nullable(Decimal(38, 6)),
    `provisions_amount` Nullable(Decimal(38, 6)),
    `provisions_amount_usd` Nullable(Decimal(38, 6)),
    `long_term_liabilities_amount` Nullable(Decimal(38, 6)),
    `long_term_liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `current_liabilities_amount` Nullable(Decimal(38, 6)),
    `current_liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `liabilities_amount` Nullable(Decimal(38, 6)),
    `liabilities_amount_usd` Nullable(Decimal(38, 6)),
    `total_assets_amount` Nullable(Decimal(38, 6)),
    `total_assets_amount_usd` Nullable(Decimal(38, 6)),
    `balance_sheet_total_amount` Nullable(Decimal(38, 6)),
    `balance_sheet_total_amount_usd` Nullable(Decimal(38, 6)),
    `cash_liquidity_percent` Nullable(Decimal(18, 6)),
    `equity_ratio_percent` Nullable(Decimal(18, 6)),
    `net_profit_margin_percent` Nullable(Decimal(18, 6)),
    `ebitda_amount` Nullable(Decimal(38, 6)),
    `ebitda_amount_usd` Nullable(Decimal(38, 6)),
    `personnel_cost_per_employee_msek` Nullable(Decimal(38, 6)),
    `personnel_cost_per_employee_usd` Nullable(Decimal(38, 6)),
    `revenue_per_employee_msek` Nullable(Decimal(38, 6)),
    `revenue_per_employee_usd` Nullable(Decimal(38, 6)),
    `revenue_change_percent` Nullable(Decimal(18, 6)),
    `average_salary` Nullable(Decimal(38, 6)),
    `dividend_amount` Nullable(Decimal(38, 6)),
    `dividend_amount_usd` Nullable(Decimal(38, 6)),
    `employee_count` Nullable(UInt32),
    `fx_rate_to_usd` Nullable(Decimal(38, 12)),
    `fx_rate_date` Nullable(Date32),
    `fx_source` LowCardinality(String) DEFAULT '',
    `normalized_at` DateTime64(6, 'UTC'),
    CONSTRAINT se_ratsit_financial_period_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT se_ratsit_financial_period_result_hash CHECK match(toString(result_sha256), '^[0-9a-f]{64}$'),
    CONSTRAINT se_ratsit_financial_period_normalizer CHECK normalizer_version != '',
    CONSTRAINT se_ratsit_financial_period_scope CHECK trimBoth(scope) != '',
    CONSTRAINT se_ratsit_financial_period_unit CHECK (monetary_unit IS NULL) OR (monetary_unit IN ('SEK', 'TSEK', 'MSEK')),
    CONSTRAINT se_ratsit_financial_period_year CHECK (fiscal_year >= 1800) AND (fiscal_year <= 2200),
    CONSTRAINT se_ratsit_financial_period_dates CHECK (period_start IS NULL) OR (period_end IS NULL) OR (period_start <= period_end),
    CONSTRAINT se_ratsit_financial_period_months CHECK (period_months IS NULL) OR (period_months > 0),
    CONSTRAINT se_ratsit_financial_period_kind CHECK period_kind IN ('', 'employment_only', 'financial_only', 'financial_and_employment')
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, financial_report_index, period_index)

;
```

- [ ] **Step 2: Write the test**

`tests/test_se_company_financial_extractors_clickhouse_local.py`:

```python
"""The financial extractors' SQL on a real ClickHouse (spec 2026-09-11 section 7). Claims a
fake client cannot settle:
1. Each page select produces the fifty-nine supplied columns in a UNION ALL whose live and
   tombstone branches agree on every type, and every inserted row passes the table's CHECKs.
2. suggestion_id equals sha256(company_id, source, period_key, suggested_at) on every row.
3. The state-hash scope selects a company before anything is suggested, nothing after the
   page is written (it converges), selects it again when the rebuilt source drops a period,
   and converges once the tombstone is written -- under join_use_nulls 0 and 1.
4. Bolagsverket: the fuller of two statements wins a period; the comparative source takes the
   newest restating filing and carries revenue and total assets only.
5. ESEF: the newest version wins field by field with the older version filling its gaps; a
   blank currency becomes NULL; a negative employee count becomes NULL; the EUR filer keeps EUR.
6. Ratsit: only the latest report; MSEK scaled to full units with amount_scale 1000000; the
   USD twin copied; an undated period keyed on Dec 31 of its fiscal year and flagged; the
   longer of two periods with one end wins; an employment-only period publishes employees
   alone; a row without a unit, and a row with neither a date nor a fiscal year in range,
   are skipped; a consolidated report maps to the consolidated scope.
7. A company outside se_company_basic_info never reaches the suggestion table.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.esef_filings import tables as esef_tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql
from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.financial import bolagsverket, esef, ratsit
from dagster_v3.defs.se_company.financial.suggestions import FINANCIAL_TARGET
from tests.clickhouse_local import clickhouse_local_command, render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "se_company_financial_source_tables.sql"
TABLE = FINANCIAL_TARGET.qualified_table
A, B, OUT = "5567081699", "5560000002", "5569999999"   # A: every source; B: ESEF (EUR) + Ratsit consolidated; OUT: not in the universe
LEI_A, LEI_B = "AAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBB"


def _statements(text: str) -> list[str]:
    out = []
    for raw in text.split(";"):
        statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
        if statement:
            out.append(statement)
    return out


def _schema() -> list[str]:
    schema = ["CREATE DATABASE IF NOT EXISTS corpscout"]
    schema += [s for s in _statements((MIGRATIONS_DIR / "000401_corpscout_se_company_financial_entity.up.sql").read_text(encoding="utf-8")) if s.startswith("CREATE TABLE")]
    schema += [s for s in _statements((MIGRATIONS_DIR / "000377_corpscout_se_company_basic_info.up.sql").read_text(encoding="utf-8")) if s.startswith("CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info\n")]
    schema += _statements(FIXTURE.read_text(encoding="utf-8"))
    schema.append(build_se_esef_view_sql(esef_tables.SE_ESEF_VIEWS[0]).rstrip(";"))
    return schema


ROWS = [
    f"INSERT INTO corpscout.se_company_basic_info (company_id, legal_name, legal_name_source, status, status_source, folded_at, fold_version, source_run_id) VALUES ('{A}', 'A AB', 'scb', 'active', 'scb', now64(3), 'v1', 'r'), ('{B}', 'B AB', 'scb', 'active', 'scb', now64(3), 'v1', 'r')",
    # Bolagsverket: A reported 2023; 2022 twice (s22a fuller than s22b); 2022 restated by the 2023 and 2024 filings (2024 newest); OUT is not in the universe.
    f"""INSERT INTO corpscout.se_bolagsverket_financial_metrics (country_iso2, source_slug, source_run_id, source_record_id, statement_key, source_record_uid, company_id, report_period_start, report_period_end, fiscal_year, observation_kind, source_fiscal_year, currency, revenue_amount_original, revenue_amount_usd, operating_profit_loss_amount_original, operating_profit_loss_amount_usd, profit_loss_amount_original, profit_loss_amount_usd, total_assets_amount_original, total_assets_amount_usd, equity_amount_original, equity_amount_usd, employees, fx_rate_to_usd, fx_rate_date, fx_source, mapping_version, resolved_at) VALUES
    ('SE','sweden_financial','r','s23:1','s23','u23','{A}','2023-01-01','2023-12-31',2023,'reported',2023,'SEK',59016040,5876000,946563,94000,946563,94000,54302472,5400000,3379581,336000,2100,0.0996,'2023-12-29','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s22a:1','s22a','u22a','{A}','2022-01-01','2022-12-31',2022,'reported',2022,'SEK',54910071,5500000,NULL,NULL,64959,6500,39228252,3900000,2433018,243000,1800,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s22b:1','s22b','u22b','{A}','2022-01-01','2022-12-31',2022,'reported',2022,'SEK',54910071,5500000,NULL,NULL,NULL,NULL,39228252,3900000,NULL,NULL,NULL,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s23:2','s23','u23','{A}','2022-01-01','2022-12-31',2022,'comparative',2023,'SEK',54910071,5500000,NULL,NULL,NULL,NULL,39228252,3900000,NULL,NULL,NULL,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','s24:2','s24','u24','{A}','2022-01-01','2022-12-31',2022,'comparative',2024,'SEK',54900000,5499000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0.1,'2022-12-30','ecb','m1',now64(3)),
    ('SE','sweden_financial','r','sX:1','sX','uX','{OUT}','2023-01-01','2023-12-31',2023,'reported',2023,'SEK',1,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0.1,'2023-12-29','ecb','m1',now64(3))""",
    # ESEF: A has two versions of 2023 (v0 revenue + equity in SEK; v1 revenue + employees, blank currency); B files in EUR with a negative employee count.
    f"INSERT INTO corpscout.esef_entity_registry_map (lei, country_iso2, registry_id_raw, registry_id, match_source, link_status, source_run_id, resolved_at) VALUES ('{LEI_A}','SE','{A}','{A}','register','register_verified','r',now64(3)), ('{LEI_B}','SE','{B}','{B}','register','register_verified','r',now64(3))",
    f"INSERT INTO corpscout.esef_filings (lei, entity_name, fxo_id, country, period_end, date_added, processed_at, json_url, package_url, report_url, viewer_url, package_sha256, error_count, warning_count, inconsistency_count, has_json_facts, source_url, source_run_id, resolved_at) VALUES ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-0','SE','2023-12-31','2024-04-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3)), ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-1','SE','2023-12-31','2024-05-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3)), ('{LEI_B}','B AB','{LEI_B}-2023-12-31-ESEF-SE-0','SE','2023-12-31','2024-04-01',now64(3),'','','','','',0,0,0,1,'','r',now64(3))",
    f"""INSERT INTO corpscout.esef_financial_metrics (lei, entity_name, fxo_id, country, scope, fiscal_year, period_start, period_end, currency, revenue_amount_original, revenue_amount_usd, equity_amount_original, equity_amount_usd, employees, mapped_fact_count, source_fact_count, mapping_version, fx_rate_to_usd, fx_rate_date, fx_source, viewer_url, source_run_id, resolved_at) VALUES
    ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-0','SE','consolidated_ifrs',2023,'2023-01-01','2023-12-31','SEK',1296506000,129000000,2030344000,202000000,NULL,5,9,'m',0.0996,'2023-12-29','ecb','','r',now64(3)),
    ('{LEI_A}','A AB','{LEI_A}-2023-12-31-ESEF-SE-1','SE','consolidated_ifrs',2023,'2023-01-01','2023-12-31','',1300000000,129400000,NULL,NULL,4200,5,9,'m',0.0996,'2023-12-29','ecb','','r',now64(3)),
    ('{LEI_B}','B AB','{LEI_B}-2023-12-31-ESEF-SE-0','SE','consolidated_ifrs',2023,'2023-01-01','2023-12-31','EUR',5000000,5400000,NULL,NULL,-1,5,9,'m',1.08,'2023-12-29','ecb','','r',now64(3))""",
    # Ratsit: A's latest report is 'a'*64 (the older 'e'*64 must not leak); B's consolidated report.
    f"INSERT INTO corpscout.se_ratsit_financial_reports (company_id, result_sha256, normalizer_version, financial_report_index, scope, monetary_unit, period_count, normalized_at) VALUES ('{A}', repeat('e',64), 'v2', 0, 'company', 'MSEK', 1, '2026-01-01 00:00:00'), ('{A}', repeat('a',64), 'v2', 0, 'company', 'MSEK', 6, '2026-09-01 00:00:00'), ('{B}', repeat('b',64), 'v2', 0, 'consolidated', 'MSEK', 1, '2026-09-01 00:00:00')",
    f"""INSERT INTO corpscout.se_ratsit_financial_periods (company_id, result_sha256, normalizer_version, financial_report_index, period_index, period_kind, scope, monetary_unit, fiscal_year, period_start, period_end, period_months, revenue_amount, revenue_amount_usd, equity_amount, equity_amount_usd, employee_count, fx_rate_to_usd, fx_rate_date, fx_source, normalized_at) VALUES
    ('{A}', repeat('e',64), 'v2', 0, 0, 'financial_only', 'company', 'MSEK', 2017, '2017-01-01', '2017-12-31', 12, 1, 100000, NULL, NULL, NULL, 0.1, '2017-12-29', 'ecb', '2026-01-01 00:00:00'),
    ('{A}', repeat('a',64), 'v2', 0, 0, 'financial_and_employment', 'company', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 60.3, 6005001.802437, 3.4, 338000, 21, 0.099585436193, '2023-12-29', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'v2', 0, 1, 'financial_only', 'company', 'MSEK', 2021, NULL, NULL, NULL, 50, 5000000, NULL, NULL, NULL, 0.1, '2021-12-30', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'v2', 0, 2, 'financial_only', 'company', 'MSEK', 2020, '2020-07-01', '2020-12-31', 6, 20, 2000000, NULL, NULL, NULL, 0.1, '2020-12-30', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'v2', 0, 3, 'financial_only', 'company', 'MSEK', 2020, '2020-01-01', '2020-12-31', 12, 40, 4000000, NULL, NULL, NULL, 0.1, '2020-12-30', 'ECB EXR', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'v2', 0, 4, 'employment_only', 'company', 'MSEK', 2019, '2019-01-01', '2019-12-31', 12, NULL, NULL, NULL, NULL, 15, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'v2', 0, 5, 'financial_only', 'company', NULL, 2018, '2018-01-01', '2018-12-31', 12, 9, NULL, NULL, NULL, NULL, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{A}', repeat('a',64), 'v2', 0, 6, 'financial_only', 'company', 'MSEK', 1850, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, '', '2026-09-01 00:00:00'),
    ('{B}', repeat('b',64), 'v2', 0, 0, 'financial_only', 'consolidated', 'MSEK', 2023, '2023-01-01', '2023-12-31', 12, 100, 10000000, NULL, NULL, NULL, 0.1, '2023-12-29', 'ECB EXR', '2026-09-01 00:00:00')""",
]

READ_SQL = (
    "SELECT source, company_id, period_key, toString(period_end_derived), ifNull(currency, 'NULL'), toString(amount_scale), "
    "ifNull(toString(revenue_amount_original), 'NULL'), ifNull(toString(revenue_amount_usd), 'NULL'), "
    "ifNull(toString(equity_amount_original), 'NULL'), ifNull(toString(employees), 'NULL'), "
    "ifNull(toString(filing_fiscal_year), 'NULL'), source_record_uid, ifNull(toString(period_months), 'NULL'), ifNull(toString(fx_rate_date), 'NULL') "
    f"FROM {TABLE} FINAL ORDER BY source, company_id, period_key FORMAT TSV"
)
IDENTITY_SQL = (
    f"SELECT count() FROM {TABLE} FINAL WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\\n', "
    "toString(source), '\\n', period_key, '\\n', toString(suggested_at)))))"
)
TOMBSTONE_SQL = (
    "SELECT source, company_id, period_key, scope, toString(period_end), toString(amount_scale), "
    "toString(revenue_amount_original IS NULL), toString(employees IS NULL), ifNull(currency, 'NULL') "
    f"FROM {TABLE} FINAL WHERE source = 'bolagsverket' AND period_key = 'standalone:2023-12-31' FORMAT TSV"
)
SOURCES = (
    ("bolagsverket", bolagsverket.reported_changed_scope_sql(), bolagsverket.reported_select_sql(), bolagsverket.BOLAGSVERKET_EXTRACTOR_VERSION),
    ("bolagsverket_comparative", bolagsverket.comparative_changed_scope_sql(), bolagsverket.comparative_select_sql(), bolagsverket.BOLAGSVERKET_COMPARATIVE_EXTRACTOR_VERSION),
    ("esef", esef.esef_changed_scope_sql(), esef.esef_select_sql(), esef.ESEF_EXTRACTOR_VERSION),
    ("ratsit", ratsit.ratsit_changed_scope_sql(), ratsit.ratsit_select_sql(), ratsit.RATSIT_EXTRACTOR_VERSION),
)


def _scope(scope_sql: str, source: str) -> str:
    return render(scope_sql, {"source": source}) + "\nORDER BY company_id"


def _insert(select_sql: str, ids: list[str], version: str) -> str:
    return render(insert_page_sql(select_sql=select_sql, target=FINANCIAL_TARGET), {"company_ids": ids, "source_run_id": "run-1", "extractor_version": version})


def _run(join_use_nulls: int) -> dict[str, list[str]]:
    script = [f"SET join_use_nulls = {join_use_nulls}"] + _schema() + ROWS
    for name, scope_sql, select_sql, version in SOURCES:
        script += [f"SELECT '## scope-before {name}'", _scope(scope_sql, name), _insert(select_sql, [A, B, OUT], version), f"SELECT '## scope-after {name}'", _scope(scope_sql, name)]
    script += ["SELECT '## rows'", READ_SQL, "SELECT '## identity'", IDENTITY_SQL]
    bolagsverket_scope, bolagsverket_select, bolagsverket_version = SOURCES[0][1], SOURCES[0][2], SOURCES[0][3]
    script += [
        "ALTER TABLE corpscout.se_bolagsverket_financial_metrics DELETE WHERE statement_key = 's23' AND observation_kind = 'reported' SETTINGS mutations_sync = 2",
        "SELECT '## scope-after-delete'", _scope(bolagsverket_scope, "bolagsverket"),
        _insert(bolagsverket_select, [A], bolagsverket_version),
        "SELECT '## scope-after-tombstone'", _scope(bolagsverket_scope, "bolagsverket"),
        "SELECT '## tombstone'", TOMBSTONE_SQL,
    ]
    completed = subprocess.run(clickhouse_local_command(), input=";\n".join(script) + ";\n", capture_output=True, text=True, timeout=900)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    sections: dict[str, list[str]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("## "):
            current = line[3:]
            sections[current] = []
        elif line.strip():
            sections[current].append(line)
    return sections


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_the_four_extractors_write_the_expected_periods_and_converge(join_use_nulls: int) -> None:
    s = _run(join_use_nulls)
    assert s["scope-before bolagsverket"] == [A] and s["scope-after bolagsverket"] == []
    assert s["scope-before bolagsverket_comparative"] == [A] and s["scope-after bolagsverket_comparative"] == []
    assert s["scope-before esef"] == [B, A] and s["scope-after esef"] == []
    assert s["scope-before ratsit"] == [B, A] and s["scope-after ratsit"] == []
    rows = [line.split("\t") for line in s["rows"]]
    assert rows == [
        ["bolagsverket", A, "standalone:2022-12-31", "0", "SEK", "1", "54910071", "5500000", "2433018", "1800", "2022", "s22a", "12", "2022-12-30"],
        ["bolagsverket", A, "standalone:2023-12-31", "0", "SEK", "1", "59016040", "5876000", "3379581", "2100", "2023", "s23", "12", "2023-12-29"],
        ["bolagsverket_comparative", A, "standalone:2022-12-31", "0", "SEK", "1", "54900000", "5499000", "NULL", "NULL", "2024", "s24", "12", "2022-12-30"],
        ["esef", B, "consolidated:2023-12-31", "0", "EUR", "1", "5000000", "5400000", "NULL", "NULL", "NULL", f"{LEI_B}-2023-12-31-ESEF-SE-0", "12", "2023-12-29"],
        ["esef", A, "consolidated:2023-12-31", "0", "SEK", "1", "1300000000", "129400000", "2030344000", "4200", "NULL", f"{LEI_A}-2023-12-31-ESEF-SE-1", "12", "2023-12-29"],
        ["ratsit", B, "consolidated:2023-12-31", "0", "SEK", "1000000", "100000000", "10000000", "NULL", "NULL", "NULL", f"ratsit:{B}:0:0", "12", "2023-12-29"],
        ["ratsit", A, "standalone:2019-12-31", "0", "SEK", "1000000", "NULL", "NULL", "NULL", "15", "NULL", f"ratsit:{A}:0:4", "12", "NULL"],
        ["ratsit", A, "standalone:2020-12-31", "0", "SEK", "1000000", "40000000", "4000000", "NULL", "NULL", "NULL", f"ratsit:{A}:0:3", "12", "2020-12-30"],
        ["ratsit", A, "standalone:2021-12-31", "1", "SEK", "1000000", "50000000", "5000000", "NULL", "NULL", "NULL", f"ratsit:{A}:0:1", "NULL", "2021-12-30"],
        ["ratsit", A, "standalone:2023-12-31", "0", "SEK", "1000000", "60300000", "6005001.802437", "3400000", "21", "NULL", f"ratsit:{A}:0:0", "12", "2023-12-29"],
    ]
    assert s["identity"] == ["0"]
    assert s["scope-after-delete"] == [A] and s["scope-after-tombstone"] == []
    assert [line.split("\t") for line in s["tombstone"]] == [["bolagsverket", A, "standalone:2023-12-31", "standalone", "2023-12-31", "1", "1", "1", "NULL"]]
```

- [ ] **Step 3: Run it on the engine**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_extractors_clickhouse_local.py -q -m integration
uv run ruff check tests/test_se_company_financial_extractors_clickhouse_local.py
```

Expected: 2 passed (join_use_nulls 0 and 1), about a minute each. This exact scenario produced exactly these rows on 2026-09-12 with the module texts of Tasks 2 to 5; a difference is a transcription slip in one of them, not an expectation to adjust — report it.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/tests/fixtures/se_company_financial_source_tables.sql \
        corpscout/services/dagster_v3/tests/test_se_company_financial_extractors_clickhouse_local.py
git commit -m "test(se-financial): the four extractors on clickhouse-local against the prod-snapshot sources"
```

---

### Task 7: Extract job and stopped weekly

**Files:**
- Modify: `src/dagster_v3/defs/se_company/financial/assets.py` (add two constants after `GROUP_NAME`)
- Create: `src/dagster_v3/defs/se_company/financial/jobs.py`
- Test: `tests/test_se_company_financial_jobs.py`

**Interfaces:**
- Produces: `assets.EXTRACTOR_SOURCES`, `assets.EXTRACTOR_ASSET_NAMES`; `jobs.se_company_financial_extract_job`, `jobs.se_company_financial_weekly`, `jobs.WEEKLY_RUN_CONFIG`, `jobs.RATSIT_USD_ASSET`.

- [ ] **Step 1: Write the failing tests**

`tests/test_se_company_financial_jobs.py`:

```python
"""The extract job and the stopped weekly (spec section 8)."""

import dagster as dg

from dagster_v3.defs.se_company.financial import jobs
from dagster_v3.defs.se_company.financial.assets import EXTRACTOR_ASSET_NAMES, EXTRACTOR_SOURCES
from dagster_v3.definitions import defs as load_project_defs


def test_the_extractor_names_follow_the_sources() -> None:
    assert EXTRACTOR_SOURCES == ("bolagsverket", "bolagsverket_comparative", "esef", "ratsit")
    assert EXTRACTOR_ASSET_NAMES == tuple(f"se_company_financial_suggestions_{s}" for s in EXTRACTOR_SOURCES)


def test_the_job_runs_the_ratsit_usd_step_and_the_four_extractors() -> None:
    repository = load_project_defs().get_repository_def()
    assert repository.has_job("se_company_financial_extract_job")
    job = repository.get_job("se_company_financial_extract_job")
    keys = {key.to_user_string() for key in job.asset_layer.executable_asset_keys}
    assert keys == {"se_ratsit_financial_periods_usd", *EXTRACTOR_ASSET_NAMES}
    for name in EXTRACTOR_ASSET_NAMES:
        node = repository.asset_graph.get(dg.AssetKey(name))
        assert node.group_name == "se_company_financial" and node.partitions_def is None


def test_the_weekly_is_stopped_at_a_free_minute_and_executes() -> None:
    schedule = jobs.se_company_financial_weekly
    assert schedule.cron_schedule == "55 7 * * 1"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert schedule.job.name == "se_company_financial_extract_job"
    config = jobs.WEEKLY_RUN_CONFIG["ops"]
    assert config["se_ratsit_financial_periods_usd"] == {"config": {"execute": True}}
    for name in EXTRACTOR_ASSET_NAMES:
        assert config[name] == {"config": {"execute": True, "page_size": 5000}}
```

- [ ] **Step 2: Run them to verify they fail**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_jobs.py -q
```

Expected: FAIL at import (`cannot import name 'jobs'` / `EXTRACTOR_ASSET_NAMES`).

- [ ] **Step 3: Add the constants and the jobs module**

In `src/dagster_v3/defs/se_company/financial/assets.py`, directly after `GROUP_NAME = "se_company_financial"`:

```python
EXTRACTOR_SOURCES: tuple[str, ...] = ("bolagsverket", "bolagsverket_comparative", "esef", "ratsit")
EXTRACTOR_ASSET_NAMES: tuple[str, ...] = tuple(
    f"se_company_financial_suggestions_{source}" for source in EXTRACTOR_SOURCES
)
```

`src/dagster_v3/defs/se_company/financial/jobs.py`, exactly:

```python
"""The financial extract job (the Ratsit USD step first, then the four extractors) and its
STOPPED weekly (spec section 8). The fold (slice 3) stays manual, so the weekly stops at the
suggestion layer."""

import dagster as dg

from dagster_v3.defs.se_company.financial.assets import EXTRACTOR_ASSET_NAMES

RATSIT_USD_ASSET = "se_ratsit_financial_periods_usd"
# 5,000 ids per page: every financial page select binds %(company_ids)s two to three times
# (the live CTE, the stored-key read, and Ratsit's periods scan) under ID_BOUND_QUERY_SETTINGS'
# 1 MiB max_query_size; 5,000 twelve-digit ids render to about 65 KB per binding.
WEEKLY_PAGE_SIZE = 5_000
WEEKLY_RUN_CONFIG = {
    "ops": {
        RATSIT_USD_ASSET: {"config": {"execute": True}},
        **{name: {"config": {"execute": True, "page_size": WEEKLY_PAGE_SIZE}} for name in EXTRACTOR_ASSET_NAMES},
    }
}

se_company_financial_extract_job = dg.define_asset_job(
    "se_company_financial_extract_job",
    selection=dg.AssetSelection.assets(RATSIT_USD_ASSET, *EXTRACTOR_ASSET_NAMES),
)
# Monday 07:55 UTC: tests/test_schedule_cron_contracts.py requires a unique (minute, hour)
# and 07:55 was free on 2026-09-12 (the person weekly holds 07:25).
se_company_financial_weekly = dg.ScheduleDefinition(
    name="se_company_financial_weekly",
    job=se_company_financial_extract_job,
    cron_schedule="55 7 * * 1",
    run_config=WEEKLY_RUN_CONFIG,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
```

- [ ] **Step 4: Run the tests, the definitions check, the cron check and ruff**

```bash
uv run --env-file .env pytest tests/test_se_company_financial_jobs.py tests/test_se_company_financial_assets.py -q
uv run dg check defs
rg -n 'cron_schedule="55 7 ' src/dagster_v3/defs
uv run ruff check src/dagster_v3/defs/se_company/financial tests/test_se_company_financial_jobs.py
```

Expected: all passed; `dg check defs` no errors; the rg prints only `financial/jobs.py`; ruff clean. If `job.asset_layer.executable_asset_keys` does not exist on this Dagster version, use `{key for key in job.asset_layer.asset_keys}` instead and note it in the report.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/assets.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/jobs.py \
        corpscout/services/dagster_v3/tests/test_se_company_financial_jobs.py
git commit -m "feat(se-financial): the extract job (Ratsit USD first, four extractors) and its stopped weekly"
```

---

### Task 8: Docs and whole-slice check

**Files:**
- Modify: `src/dagster_v3/defs/se_company/financial/docs/financial-design.md`
- Modify: `docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md` (section 12 item 2)

- [ ] **Step 1: Add the extractor table to the design note**

Append a section `## Extractors (slice 2)` with this table and paragraph:

```markdown
## Extractors (slice 2)

| Module | Source | Reads | Key rule |
|---|---|---|---|
| `bolagsverket.py` | `bolagsverket` | `se_bolagsverket_financial_metrics` reported rows | one row per period end, the fuller statement wins, then the smaller statement key |
| `bolagsverket.py` | `bolagsverket_comparative` | the same table's comparative rows | revenue and total assets from the newest restating filing; `filing_fiscal_year` names it |
| `esef.py` | `esef` | `esef_financial_metrics` through `se_esef_filings` | `consolidated_ifrs` only; newest `fxo_id` version wins field by field, older versions fill gaps |
| `ratsit.py` | `ratsit` | `se_ratsit_financial_periods` for the latest `se_ratsit_financial_reports` hash | figures scaled from `monetary_unit`, USD twins copied, undated periods keyed on Dec 31 and flagged, the longer duplicate wins |

All four use `suggestions.py`'s target and the shared `se_company/state_scan.py` (the person
entity's per-company state hash, lifted in this slice): a company is visited when what the
source delivers now differs from its stored live rows, and a period the source stopped
delivering gets a tombstone that copies scope and period end so the table's CHECK holds.
The job `se_company_financial_extract_job` runs `se_ratsit_financial_periods_usd` first, then
the four extractors; the weekly `se_company_financial_weekly` (Monday 07:55 UTC) is defined
STOPPED until the fold (slice 3) exists. Every extractor previews by default (`execute: false`).
```

- [ ] **Step 2: Record the slice in the spec**

Section 12 item 2: append, wrapped like its neighbours: `Code complete 2026-09-12 on branch se-financial-entity (plan 2026-09-12-se-company-financial-2-extractors.md); prod runs pending.`

- [ ] **Step 3: Run the slice's suites and the wider suite once**

```bash
uv run --env-file .env pytest tests/test_se_company_state_scan.py tests/test_se_company_financial_suggestions.py tests/test_se_company_financial_extractors_sql.py tests/test_se_company_financial_jobs.py tests/test_se_company_financial_assets.py tests/test_se_company_person_extractors_sql.py -q
set -a; source .env; set +a; uv run pytest tests -q -p no:cacheprovider --ignore=tests/test_schedule_cron_contracts.py --deselect tests/test_backfill_policy_contracts.py::test_every_partitioned_asset_uses_multi_run_backfill_policy --deselect tests/test_duckdb_bulk_loading_contract.py::test_production_has_only_the_explicit_ted_executemany_debt --deselect tests/test_nace_categories.py::test_nace_assets_are_registered_as_staged_flow --deselect tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values --deselect tests/test_technology_aliases_clickhouse.py::test_catalog_asset_publishes_aliases_clears_them_and_rejects_bad_input 2>&1 | tail -3
```

Expected: all green; the wider run `N passed, 3 skipped, 5 deselected`, no failures.

- [ ] **Step 4: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/financial/docs/financial-design.md \
        corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-company-financial-entity-design.md
git commit -m "docs(se-financial): the extractors in the package note; spec records slice 2 code complete"
```

---

### Task 9: Prod runs (after the final review and the merge)

**Files:** none. Runs against the dagster and companycollect hosts.

**Preconditions:** the whole-branch review is clean and the branch is merged into main (from the main checkout, `git merge --no-ff se-financial-entity`; the owner's dirty files never overlap the branch's); `git merge --ff-only main` in the worktree so it equals main; prod ledger still 401.

- [ ] **Step 1: Deploy dagster_v3 from the worktree (the deploy recipe, absolute paths)**

```bash
D=/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-financial-entity/corpscout/services/dagster_v3
cd "$D" && test -z "$(git diff main --stat)" && test -f "$D/.env" && test -z "$(git status --porcelain src)"
uv sync --frozen
uv run --frozen --no-sync dbt parse --project-dir "$D/src/dagster_v3/defs/finland_ytj/dbt" --profiles-dir "$D/src/dagster_v3/defs/finland_ytj/dbt"
uv run --frozen --no-sync dbt parse --project-dir "$D/src/dagster_v3/defs/exchange_rates_v2/dbt" --profiles-dir "$D/src/dagster_v3/defs/exchange_rates_v2/dbt"
uv run --frozen --no-sync dg utils refresh-defs-state
uv run --frozen --no-sync dg check defs
cd "$D/ansible" && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml > /tmp/light_sync_slice2.log 2>&1; echo "RC=$?"; tail -2 /tmp/light_sync_slice2.log
```

Expected: `RC=0`, `failed=0`. Confirm the four assets are live: the GraphQL `assetNodes` query for `se_company_financial_suggestions_bolagsverket` prints group `se_company_financial` and its two dependency keys.

- [ ] **Step 2: Run each extractor, preview then execute, in-process on the host**

The run queue was still held by the ESEF refresh runs on 2026-09-12; run in-process, one source at a time, in this order: `bolagsverket`, `bolagsverket_comparative`, `esef`, `ratsit`. For each, a preview:

```bash
ssh dagster 'cd /opt/companycollect/corpscout/dagster_v3 && sudo -n bash -c "set -a; source .env; set +a; export HOME=/root DAGSTER_HOME=/opt/companycollect/corpscout/dagster_v3 DAGSTER_DISABLE_TELEMETRY=1 PYTHONDONTWRITEBYTECODE=1 VIRTUAL_ENV=/opt/companycollect/corpscout/dagster_v3/.venv PATH=/root/.local/bin:/opt/companycollect/corpscout/dagster_v3/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin TERM=dumb; timeout 7200 .venv/bin/dagster asset materialize -m dagster_v3.definitions -a defs --select se_company_financial_suggestions_<source> --config-json "{\"ops\": {\"se_company_financial_suggestions_<source>\": {\"config\": {\"execute\": false}}}}" > /tmp/fin_<source>_preview.log 2>&1; echo exit=\$?"; grep -E "RUN_SUCCESS|RUN_FAILURE|Traceback" /tmp/fin_<source>_preview.log | cut -c1-160'
```

then read the materialization metadata (`companies`, `pages`, `candidates`, `execute false`) through the GraphQL `assetMaterializations` query used in slices 0 and 1.

Before the unbounded execute, run a one-company execute smoke per source: the same in-process command with `--config-json` `{"ops": {"se_company_financial_suggestions_<source>": {"config": {"execute": true, "company_ids": ["5567081699"]}}}}`, logged to `/tmp/fin_<source>_smoke.log`, followed by the readout `SELECT source, period_key, currency, revenue_amount_original, amount_scale, employees FROM corpscout.se_company_financial_suggestion FINAL WHERE company_id = '5567081699' AND source = '<source>' ORDER BY period_key`. That path bypasses the scan entirely and writes one company's real page, exercising the INSERT, every CAST and every CHECK on one real company before the unbounded execute touches the full source.

Then the same command with `\"execute\": true` (unbounded, no `company_ids`) into `/tmp/fin_<source>_execute.log`. Expected orders of magnitude: bolagsverket about 580k companies / 3.05M candidates; comparative about 530k companies; esef about 400 companies / 1.3k candidates; ratsit about 720k companies / 3.1M candidates. A page runs the live CTE for 5,000 companies; expect the Bolagsverket and Ratsit executes to take tens of minutes each. Run a second preview after each execute: `candidates` must be 0 (the scan converged).

**If the scope query fails** (`MEMORY_LIMIT_EXCEEDED`, or `TIMEOUT_EXCEEDED` against `max_execution_time` 1800s, on the scope INSERT): retry with `since: "1970-01-01T00:00:00Z"` added to the run config. That replaces the state-hash scope with the cheap per-company `*_current_sql()` maximum and visits every company once instead of hashing the whole source; the page select itself is unchanged, so the rows it writes are identical and the next normal (state-hash) scan still converges.

Record wall time per source (preview, smoke, execute) in Step 4's record, next to the expected magnitudes.

- [ ] **Step 3: Readouts**

```bash
ssh -o ConnectTimeout=20 companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --multiquery --format PrettyCompactMonoBlock' <<'SQL'
SELECT source, count() AS rows, uniqExact(company_id) AS companies, countIf(period_end_derived = 1) AS derived, countIf(currency IS NULL) AS no_currency, countIf(scope = 'consolidated') AS consolidated, min(period_end) AS first_end, max(period_end) AS last_end FROM corpscout.se_company_financial_suggestion FINAL GROUP BY source ORDER BY source;
SELECT source, countIf(revenue_amount_original IS NOT NULL) AS with_revenue, countIf(revenue_amount_usd IS NULL AND revenue_amount_original IS NOT NULL) AS revenue_without_usd, countIf(employees IS NOT NULL) AS with_employees FROM corpscout.se_company_financial_suggestion FINAL GROUP BY source ORDER BY source;
SELECT currency, count() FROM corpscout.se_company_financial_suggestion FINAL GROUP BY currency ORDER BY count() DESC LIMIT 6;
SELECT count() AS tombstones FROM corpscout.se_company_financial_suggestion FINAL WHERE revenue_amount_original IS NULL AND total_assets_amount_original IS NULL AND employees IS NULL AND equity_amount_original IS NULL AND net_result_amount_original IS NULL;
SELECT count() AS ratsit_no_unit FROM corpscout.se_ratsit_financial_periods AS p FINAL INNER JOIN (SELECT r.company_id AS company_id, argMax(r.result_sha256, (r.normalized_at, r.result_sha256)) AS result_sha256 FROM corpscout.se_ratsit_financial_reports AS r FINAL WHERE r.normalizer_version = 'ratsit-normalizer-v2' GROUP BY r.company_id) AS report ON report.company_id = p.company_id AND report.result_sha256 = p.result_sha256 WHERE p.normalizer_version = 'ratsit-normalizer-v2' AND p.monetary_unit IS NULL;
SELECT count() AS ratsit_no_period FROM corpscout.se_ratsit_financial_periods AS p FINAL INNER JOIN (SELECT r.company_id AS company_id, argMax(r.result_sha256, (r.normalized_at, r.result_sha256)) AS result_sha256 FROM corpscout.se_ratsit_financial_reports AS r FINAL WHERE r.normalizer_version = 'ratsit-normalizer-v2' GROUP BY r.company_id) AS report ON report.company_id = p.company_id AND report.result_sha256 = p.result_sha256 WHERE p.normalizer_version = 'ratsit-normalizer-v2' AND p.period_end IS NULL AND p.fiscal_year NOT BETWEEN 1900 AND 2299;
SELECT scope, count() FROM corpscout.esef_financial_metrics FINAL WHERE scope != 'consolidated_ifrs' GROUP BY scope ORDER BY scope;
SELECT countIf(revenue_amount_original IS NULL AND operating_profit_loss_amount_original IS NULL AND profit_loss_amount_original IS NULL AND total_assets_amount_original IS NULL AND equity_amount_original IS NULL AND liabilities_amount_original IS NULL AND cash_and_bank_amount_original IS NULL AND current_assets_amount_original IS NULL AND current_liabilities_amount_original IS NULL AND personnel_expenses_amount_original IS NULL AND wages_and_salaries_amount_original IS NULL AND employees IS NULL) AS bolagsverket_empty FROM corpscout.se_bolagsverket_financial_metrics FINAL WHERE observation_kind = 'reported';
SELECT countIf(revenue_amount_original IS NULL AND total_assets_amount_original IS NULL) AS bolagsverket_comparative_empty FROM corpscout.se_bolagsverket_financial_metrics FINAL WHERE observation_kind = 'comparative';
SELECT countIf(revenue_amount_original IS NULL AND operating_profit_amount_original IS NULL AND profit_loss_amount_original IS NULL AND total_assets_amount_original IS NULL AND equity_amount_original IS NULL AND liabilities_amount_original IS NULL AND cash_amount_original IS NULL AND personnel_expenses_amount_original IS NULL AND employees IS NULL) AS esef_empty FROM corpscout.esef_financial_metrics FINAL WHERE scope = 'consolidated_ifrs';
SELECT countIf(revenue_amount IS NULL AND operating_costs_amount IS NULL AND operating_profit_amount IS NULL AND profit_after_financial_items_amount IS NULL AND net_income_amount IS NULL AND ebitda_amount IS NULL AND total_assets_amount IS NULL AND fixed_assets_amount IS NULL AND current_assets_amount IS NULL AND equity_amount IS NULL AND share_capital_amount IS NULL AND untaxed_reserves_amount IS NULL AND provisions_amount IS NULL AND liabilities_amount IS NULL AND long_term_liabilities_amount IS NULL AND current_liabilities_amount IS NULL AND dividend_amount IS NULL AND employee_count IS NULL) AS ratsit_empty FROM corpscout.se_ratsit_financial_periods AS p FINAL INNER JOIN (SELECT r.company_id AS company_id, argMax(r.result_sha256, (r.normalized_at, r.result_sha256)) AS result_sha256 FROM corpscout.se_ratsit_financial_reports AS r FINAL WHERE r.normalizer_version = 'ratsit-normalizer-v2' GROUP BY r.company_id) AS report ON report.company_id = p.company_id AND report.result_sha256 = p.result_sha256 WHERE p.normalizer_version = 'ratsit-normalizer-v2';
SELECT source, period_key, currency, revenue_amount_original, revenue_amount_usd, amount_scale, employees FROM corpscout.se_company_financial_suggestion FINAL WHERE company_id = '5567081699' AND period_end = '2023-12-31' ORDER BY source;
SQL
```

The five queries after `tombstones` are spec 7's per-outcome skip counts (I2), read out against the
sources with the same FINAL / latest-report-of-the-running-normalizer-version conventions the
extractors use: Ratsit periods with no unit, Ratsit periods with neither a date nor a fiscal year in
range, ESEF rows outside the mapped `consolidated_ifrs` scope (watch for a new scope value here), and
per source the count of rows with no figure and no employee count -- the population C1's live-row
predicate now skips instead of writing.

Expected: no tombstones on the first run; `revenue_without_usd` for ratsit equals the 301 pre-2006 rows of slice 0 (their USD is NULL by design); for `5567081699` 2023: bolagsverket 59,016,040 SEK, esef 1,296,506,000 SEK consolidated, ratsit 60,300,000 SEK with scale 1000000.

- [ ] **Step 4: Record**

Append to spec section 12 item 2 the prod record: date, run ids per source, rows and companies per source, the derived count, tombstones 0, the convergence check. Commit on main as `docs(se-financial): slice 2 shipped, prod record`, fast-forward the worktree, update the memory file, then write slice 3's plan (the fold).

---

## Self-review

**Spec coverage.** Section 7's four extractors → Tasks 3 to 5 (rules verified on the engine in Task 6); the state-hash lift → Task 1; the target and tombstone shape of 4.1 → Task 2; section 8's job and stopped weekly → Task 7; section 11's clickhouse-local tests with the measured edge cases (duplicate statements, restatement from two filings, an ESEF amendment dropping a metric, an EUR filer, a blank ESEF currency, an undated Ratsit row, two Ratsit periods with one end, an employment-only period, a tombstone, a company outside the universe) → Task 6; section 12 item 2's prod runs with counts → Task 9. Not in this slice: the fold, the backoffice, `current_receivables` (Bolagsverket publishes it but the spec's twenty fields do not include it; noted for the owner).

**Placeholder scan.** None; every module, test and fixture is given in full; the prod commands carry their expected magnitudes.

**Type consistency.** `StateScan` fields are the ones `person/suggestions.py` and `financial/suggestions.py` construct; `state_scan.select_sql` aliases the anti-join `live_<key>s` (`live_slots`, `live_period_keys`), which the person SQL test pins and the financial SQL test counts; `FINANCIAL_TARGET.insert_columns` = 59 select + 4 stamped = the 63 table columns; `define_financial_suggestion_asset(**kwargs)` takes the same keyword arguments as `define_suggestion_asset` (source, extractor_version, current_sql, select_sql, select_params, deps, description, changed_scope_override); `EXTRACTOR_ASSET_NAMES` in `assets.py` matches the four asset names the modules define and the job selects.
