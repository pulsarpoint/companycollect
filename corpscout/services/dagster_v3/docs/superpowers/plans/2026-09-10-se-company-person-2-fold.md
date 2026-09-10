# SE company person entity, slice 2: the fold — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish every Swedish company's people into `corpscout.se_company_person_v2` from its normalized rows — identity grouping into persons, reviewer rules (hide, merge, split), the member/roles/`data` blocks, history and set replacement — with the spelling precedence exported for the backoffice, and the first full fold over 64 hash buckets on prod.

**Architecture:** The address entity's slice-2b shape, minus the geocoder. A pure fold (`person/fold.py`) turns one company's normalized `ok` rows plus its current published rows, its active rules and the precedence into the new published set and the history entries. A batch layer (`person/batch.py`) selects changed companies, pages 20,000 at a time, reads its four inputs `FINAL`, runs the pure fold per company, writes history then main. Three assets expose it (`se_company_person_fold` on 64 static buckets with no pool, `se_company_person_fold_companies` for the backoffice's targeted fold, `se_company_person_precedence_clickhouse` for the dictionary export). Keep `se_company/address/{precedence,fold,batch,assets}.py` open beside this plan: every structural decision here mirrors one of them.

**Tech Stack:** Python 3.14, Dagster 1.13.9 (`dg`), clickhouse-driver 0.2.10 through `dagster_clickhouse.ClickhouseResource`, ClickHouse 26.5, pytest, clickhouse-local (docker) for the integration proof.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-09-se-company-person-entity-design.md` — slice 2 is section 9 item 2; its content is sections 3.3 to 3.6, 4.3 (role years), 5 with 5.1 to 5.6, and 10 (names). Slices 0 and 1 (the six tables, the normalizer, the three extractors, the extract job) are on prod since 2026-09-09 and 2026-09-10.

## Global Constraints

- Dagster commands run from `corpscout/services/dagster_v3` as `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/<file> -q`; `uv run --frozen --no-sync dg check defs` must pass before every commit that touches `src/`.
- **Repo root:** `/Users/graovic/pulsarpoint/ppoint/companycollect`. The branch is `se-person-entity` in the worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info`. Always use absolute paths.
- **No migration.** Every column this slice writes exists on migration `000396` (verified 2026-09-10: `se_company_person_v2` 29 columns, `se_company_person_history` 32, `se_company_person_rule` 9, `se_company_person_precedence` 8). If a task turns out to need a column that is not there, **stop and raise it** rather than inventing one.
- **Table names and column tuples come from `person/tables.py`; never retype them.** `MAIN_COLUMNS` has 29 entries, `HISTORY_COLUMNS == (*MAIN_COLUMNS, "changed_at", "change_kind", "fold_run_id")` has 32.
- No `from __future__ import annotations` in any module that defines a `@dg.asset`.
- A non-nullable ClickHouse `String`/`LowCardinality(String)` column never receives `None`: `''` instead. Arrays are inserted as Python **lists** (`Array(Array(String))` as a list of lists).
- Every SELECT that binds a page of company ids passes `settings=FOLD_ID_BOUND_QUERY_SETTINGS` (1 MiB `max_query_size`, 1800 s `max_execution_time`); ClickHouse's default `max_query_size` is 262,144 bytes and a 20,000-id page renders past it. A render-size guard test pins the worst case.
- **`data` is a `String` holding a JSON object**, never the native `JSON` type (owner ruling 2026-09-09). `se_company_person_v2` carries `CONSTRAINT valid_data CHECK JSONType(data) = 'Object'`, so the fold's merged `data` must always render an object — `json.dumps` of a `dict`, `'{}'` when empty — and a member's unparseable `data` degrades to `{}` instead of failing a 20,000-company page.
- **Whole-name matching on table names.** `se_company_person` prefixes `se_company_person_suggestion`, `_normalized`, `_v2`, `_history`, `_rule`, `_precedence`. Every string match on a table name — a test assertion, a fake client's dispatch branch, a migration-statement filter — compares whole names, never a bare `in` on the qualified string.
- **The fold has no pool and no DuckDB.** The address fold takes the OSM workbench pool because it geocodes; this one does not geocode, so its buckets run in parallel (spec section 5).
- **Nothing in this plan executes DDL against a server.** Task 6 is Dagster runs plus read-only `SELECT`s.
- **Another session merges to main daily.** Merge through a worktree that has main checked out (memory `se-worktree-deploy-recipe`); the main checkout may sit on another branch.
- Commit by explicit path after every task; never `git add -A`. Trailers, contiguous at the end of every commit message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- Pre-existing unrelated failures, **not** regressions of this slice: `tests/test_se_company_address_extractors_clickhouse_local.py`, `tests/test_se_company_basic_info_clickhouse_local.py`, `tests/test_schedule_cron_contracts.py`, `tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values`, `tests/test_backfill_policy_contracts.py`, `tests/test_ted_procurement_parser.py`, `tests/test_ted_procurement_publish.py`, `tests/test_nace_categories.py`.

---

## Verified facts this plan is built on

Read out of the code and the migration on 2026-09-10 (prod numbers from the slice-1 shipped record in spec section 9). An implementer does not re-measure; the numbers are Task 6's acceptance targets.

| fact | value |
| --- | --- |
| `se_company_person_normalized` on prod | 5,569,431 rows over 578,356 companies |
| `parse_status` distribution | `ok` 5,508,877 / `partial` 59,216 / `no_person` 1,338 — **only `ok` folds** |
| per source | bolagsverket 5,559,317 rows (2,018,042 of them roleless, `role_code IS NULL`) and 632 distinct `role_code`; esef 9,610 / 404 codes; wikidata 504 / 5 codes |
| `se_company_person_v2`, `_history`, `_rule`, `_precedence` | **all four empty** — this slice writes their first rows |
| the serving view | `has_people`, `people_bolagsverket`, `people_esef` already read `se_company_person_v2 FINAL WHERE active = 1` (migration 000396), so they light up at the first fold's next refresh (hourly at :45) |
| `se_company_person_v2` | `ReplacingMergeTree(folded_at) ORDER BY (company_id, person_key)` — **two sets of one company may never produce the same `person_key`**, or the engine silently collapses two people into one |
| `se_company_person_history` | plain `MergeTree ORDER BY (company_id, person_key, changed_at)`, no constraints, append-only |
| `se_company_person_rule` | `ReplacingMergeTree(created_at) ORDER BY (company_id, rule_id)`; `active UInt8` (**not** `removed`, unlike the address rule table); `person_keys Array(FixedString(64))`, `slots Array(String)` |
| `se_company_person_precedence` | `ReplacingMergeTree(decided_at) ORDER BY (company_id, field, source)`; `removed UInt8`; global rows are `company_id = ''` |
| `normalize_se_person` | already folds diacritics, hyphens and initials into `first_tokens`/`middle_tokens`/`last_tokens` — the fold compares **tokens**, never display text |
| Wikidata role spans | `role_from`/`role_to` are `Nullable(Date)`, so a year is never below 1970 (ClickHouse `Date`'s floor) and a span can never exceed ~57 years |
| ESEF rows | carry both a `fiscal_year` (from the document) and an `effective_from`/`effective_to` span |
| max signatories per company | 96 (slice-1 measurement) — the identity grouping's per-company work is tiny |

---

## File map

| File | Responsibility |
| --- | --- |
| `src/dagster_v3/defs/se_company/person/precedence.py` (create) | The `name` spelling precedence: the five-source dictionary, `precedence_for`, `precedence_rows`. |
| `src/dagster_v3/defs/se_company/person/fold.py` (create) | Pure: `NormalizedRow`, `PersonRule`, `PublishedPerson`, `HistoryEntry`, `FoldResult`, the identity/rules/roles/`data` helpers and `fold_company_persons`. |
| `src/dagster_v3/defs/se_company/person/batch.py` (create) | SQL texts, selection, paging, history-then-main writes, `FoldCounts`, `fold_companies`, `fold_bucket`. |
| `src/dagster_v3/defs/se_company/person/assets.py` (modify) | `export_precedence` + `se_company_person_precedence_clickhouse`, `PERSON_FOLD_PARTITIONS`, `person_bucket_index`, the two config classes, `targeted_fold`, `se_company_person_fold`, `se_company_person_fold_companies`. |
| `src/dagster_v3/defs/se_company/person/docs/person-design.md` (modify) | Module-map rows for the three new modules and three assets; the fold's rules and selection. |
| `tests/test_se_company_person_precedence.py` (create) | Dictionary, rows, company override, the export. |
| `tests/test_se_company_person_fold.py` (create) | The pure fold (Tasks 2 and 3 both write into this file). |
| `tests/test_se_company_person_batch.py` (create) | The batch with a fake ClickHouse client. |
| `tests/test_se_company_person_assets.py` (create) | Asset wiring: partitions, no pool, config bounds, the targeted fold's order. |
| `tests/test_se_company_person_fold_clickhouse_local.py` (create) | End-to-end fold against clickhouse-local over fixture rows in all six tables, both `join_use_nulls` settings. |

### Interfaces every task agrees on

```python
# person/precedence.py (Task 1)
FIELD = "name"
PERSON_PRECEDENCE: dict[str, int]                      # reviewer 20000, ratsit 1000, bolagsverket 900, wikidata 600, esef 400
def precedence_for(source: str, company_precedence: Mapping[str, int] | None = None) -> int
def precedence_rows() -> list[tuple[str, str, int]]

# person/assets.py (Task 1)
def export_precedence(client: Any, exported_at: datetime) -> tuple[int, int]        # (pairs, stale pairs)

# person/fold.py (Tasks 2 and 3)
FOLD_VERSION = "se-person-fold-v1"
FOLDABLE_STATUS = "ok"; EXCLUDED_SOURCES = ("reviewer_draft",); REVIEWER_SOURCE = "reviewer"
HIDDEN = "hidden"; WITHDRAWN = "withdrawn"; CREATED = "created"; UPDATED = "updated"; REACTIVATED = "reactivated"
class NormalizedRow      # 18 fields, Task 2
class PersonRule         # company_id, rule_id, kind, person_keys, slots
class PublishedPerson    # 29 fields == tables.MAIN_COLUMNS; as_tuple(folded_at), history_tuple(...), changed_against(other)
class HistoryEntry       # row: PublishedPerson (the PREVIOUS image), change_kind: str
class FoldResult         # rows, history, persons, created, updated, hidden, withdrawn, reactivated, unchanged, stale_rules, sets_split_by_birth_year
def identity_sets_before_split(rows: Sequence[NormalizedRow]) -> tuple[tuple[NormalizedRow, ...], ...]  # Task 2
def identity_sets(rows: Sequence[NormalizedRow]) -> tuple[tuple[NormalizedRow, ...], ...]                 # Task 2
def assign_keys(company_id: str, sets) -> list[tuple[tuple[NormalizedRow, ...], str]]                     # Task 2
def canonical_tokens(members: Sequence[NormalizedRow]) -> tuple[str, ...]                                 # Task 2
def person_key(company_id: str, canonical: Sequence[str], discriminator: str = "") -> str                 # Task 2
def apply_rules(sets, rules, previous_members) -> tuple[list[list[NormalizedRow]], int]                   # Task 2
def role_years_for(row: NormalizedRow, current_year: int) -> tuple[int, ...]                              # Task 3
def role_block(members, current_year: int) -> RoleBlock                                                   # Task 3
def merge_member_data(members, company_precedence) -> str                                                 # Task 3
def change_kind_for(previous: PublishedPerson, new: PublishedPerson) -> str                               # Task 3
def fold_company_persons(company_id, rows, published, rules, company_precedence, *, source_run_id, current_year) -> FoldResult   # Task 3

# person/batch.py (Task 4)
BUCKET_COUNT = 64; PAGE_SIZE = 20_000
FOLD_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 1_048_576, "max_execution_time": 1800}
NORMALIZED_SELECT_COLUMNS; MAIN_SELECT_COLUMNS (== tables.MAIN_COLUMNS); RULE_SELECT_COLUMNS
class FoldCounts         # companies, considered, pages, persons, created, updated, hidden, withdrawn, reactivated, unchanged, stale_rules, sets_split_by_birth_year; as_metadata()
def fold_companies(client, company_ids, *, changed_only, source_run_id, folded_at, page_size=PAGE_SIZE, log=None) -> FoldCounts
def fold_bucket(client, bucket, *, changed_only, source_run_id, folded_at, page_size=PAGE_SIZE, log=None) -> FoldCounts

# person/assets.py (Task 5)
PERSON_FOLD_PARTITIONS: dg.StaticPartitionsDefinition        # bucket_00 .. bucket_63
def person_bucket_index(partition_key: str) -> int
class PersonFoldConfig; class PersonFoldCompaniesConfig
def targeted_fold(client, company_ids, *, changed_only, source_run_id, folded_at, page_size, log, logger) -> tuple[NormalizeCounts, FoldCounts]
```

---

### Task 1: Precedence module and export asset

**Files:**
- Create: `src/dagster_v3/defs/se_company/person/precedence.py`
- Modify: `src/dagster_v3/defs/se_company/person/assets.py` (append `_precedence_export_timestamp`, `export_precedence`, the asset)
- Test: `tests/test_se_company_person_precedence.py`

**Interfaces:**
- Consumes: `person/tables.py` (`QUALIFIED_PRECEDENCE_TABLE`, `PRECEDENCE_COLUMNS`), `person/assets.py` (`GROUP_NAME`, `assert_clickhouse_tables_exist`). Copy `_precedence_export_timestamp` from `address/assets.py:88-93`; do not import a private name across packages.
- Produces: `FIELD`, `PERSON_PRECEDENCE`, `precedence_for`, `precedence_rows`, `export_precedence`, asset `se_company_person_precedence_clickhouse`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_se_company_person_precedence.py
"""Spec section 3.6: one field, `name`. It decides whose SPELLING is published and never
whether a person is published -- every source's people publish -- so an unranked source
ranks 0 instead of being filtered out."""

from datetime import UTC, datetime

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.assets import export_precedence
from dagster_v3.defs.se_company.person.precedence import (
    FIELD,
    PERSON_PRECEDENCE,
    precedence_for,
    precedence_rows,
)


def test_the_global_order_is_the_spec_order() -> None:
    assert FIELD == "name"
    assert PERSON_PRECEDENCE == {
        "reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "wikidata": 600, "esef": 400,
    }


def test_the_reviewer_outranks_every_source_including_the_reserved_ratsit() -> None:
    """Slice 3's backoffice writes reviewer rows at source `reviewer`; the fold already
    ranks them above ratsit, which is reserved and has no extractor yet."""
    assert precedence_for("reviewer") > precedence_for("ratsit") > precedence_for("bolagsverket")


def test_an_unranked_source_gets_zero_not_none() -> None:
    assert precedence_for("reviewer_draft") == 0
    assert precedence_for("scb") == 0


def test_a_company_row_replaces_the_global_number_for_that_source() -> None:
    assert precedence_for("esef", {"esef": 5000}) == 5000
    assert precedence_for("bolagsverket", {"esef": 5000}) == 900


def test_rows_are_highest_first_with_the_field_name() -> None:
    assert precedence_rows() == [
        ("name", "reviewer", 20000),
        ("name", "ratsit", 1000),
        ("name", "bolagsverket", 900),
        ("name", "wikidata", 600),
        ("name", "esef", 400),
    ]


class FakeClient:
    def __init__(self, stale: int = 0) -> None:
        self.calls: list[tuple[str, object]] = []
        self.stale = stale

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params))
        if sql.startswith("SELECT count()"):
            return [(self.stale,)]
        return []


def test_export_inserts_global_rows_and_counts_stale_pairs() -> None:
    client = FakeClient(stale=2)
    exported_at = datetime(2026, 9, 10, 8, 0, 0, 123000, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (5, 2)
    insert_sql, rows = client.calls[0]
    assert insert_sql == (
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
        f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
    )
    assert rows[0] == ("", "name", "reviewer", 20000, 0, "code", "", exported_at)
    assert all(row[0] == "" for row in rows)
    count_sql, params = client.calls[1]
    assert "company_id = ''" in count_sql and "FINAL" in count_sql
    assert params == {"exported_at": "2026-09-10 08:00:00.123"}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_precedence.py -q`
Expected: FAIL with `ModuleNotFoundError: ...person.precedence` / `ImportError: cannot import name 'export_precedence'`.

- [ ] **Step 3: Write the module**

```python
# src/dagster_v3/defs/se_company/person/precedence.py
"""Per-source spelling precedence of the person fold (spec section 3.6).

ONE FIELD, `name`. It decides which member's spelling a published person shows --
display_name, first_name, last_name and text_source -- and, through the same order, which
member's `data` keys win a collision (spec 5.5). It NEVER decides whether a person is
published: every person from every source is published and a person seen by several sources
is one row keeping every source (spec section 2), so a source absent from this map ranks 0
rather than being excluded.

WHY THESE NUMBERS. `reviewer` outranks everything so an activated reviewer correction spells
its own row (the backoffice writes those in slice 3, at source `reviewer`; the fold already
ranks them here). `ratsit` is reserved at 1000 -- the source has the highest trust of the
machine sources and no extractor yet. Bolagsverket delivers a first/last split from the
register, Wikidata a curated label, ESEF an LLM extraction from a PDF-shaped filing: that is
the 900 / 600 / 400 order. `reviewer_draft` never reaches the fold (the batch filters it by
source) and no other source has people.
"""

from collections.abc import Mapping

FIELD = "name"

PERSON_PRECEDENCE: dict[str, int] = {
    "reviewer": 20000,
    "ratsit": 1000,
    "bolagsverket": 900,
    "wikidata": 600,
    "esef": 400,
}


def precedence_for(source: str, company_precedence: Mapping[str, int] | None = None) -> int:
    """The company's own row for `source` when one exists (spec 3.6 allows them; nothing
    writes them yet), else the global number, else 0."""
    if company_precedence is not None and source in company_precedence:
        return int(company_precedence[source])
    return PERSON_PRECEDENCE.get(source, 0)


def precedence_rows() -> list[tuple[str, str, int]]:
    """Every (field, source, precedence) pair, highest first, for the export asset."""
    return [
        (FIELD, source, precedence)
        for source, precedence in sorted(
            PERSON_PRECEDENCE.items(), key=lambda item: (-item[1], item[0])
        )
    ]
```

- [ ] **Step 4: Append the export and its asset to `person/assets.py`**

Add to the imports at the top of `person/assets.py` (keep every existing one):

```python
from typing import Any

from dagster_v3.defs.se_company.person.precedence import precedence_rows
```

Append at the end of the module:

```python
def _precedence_export_timestamp(exported_at: datetime) -> str:
    """``exported_at`` as a UTC ``%Y-%m-%d %H:%M:%S.mmm`` string for ``toDateTime64(..., 3,
    'UTC')``: a bare tz-aware datetime parameter would let the stale-pairs comparison depend
    on the server's default timezone and drop sub-second precision (basic info and the
    address entity do the same)."""
    return exported_at.strftime("%Y-%m-%d %H:%M:%S.") + f"{exported_at.microsecond // 1000:03d}"


def export_precedence(client: Any, exported_at: datetime) -> tuple[int, int]:
    """Insert every (field, source, precedence) pair as a global rule (company_id '',
    decided_by 'code') and count global rules exported before this run that the dictionary
    no longer names. Returns (pairs inserted, stale pairs remaining). Never touches a
    company-scoped row."""
    rows = [
        ("", field, source, precedence, 0, "code", "", exported_at)
        for field, source, precedence in precedence_rows()
    ]
    client.execute(
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
        f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
        rows,
    )
    stale = int(
        client.execute(
            f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL "
            "WHERE company_id = '' AND decided_at < toDateTime64(%(exported_at)s, 3, 'UTC')",
            {"exported_at": _precedence_export_timestamp(exported_at)},
        )[0][0]
    )
    return len(rows), stale


@dg.asset(
    name="se_company_person_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports PERSON_PRECEDENCE to se_company_person_precedence as global rules "
        "(company_id '', field 'name') for the fold and the backoffice to read. The Python "
        "dictionary is the only source for these rows; re-run after changing it, which "
        "re-folds every company (the export's stamp is newer than their last fold). Never "
        "touches a company-scoped row."
    ),
)
def se_company_person_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,)
    )
    exported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        pairs, stale = export_precedence(client, exported_at)
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; "
            "they stay until removed by hand",
            stale,
        )
    return dg.MaterializeResult(
        metadata={"pairs": pairs, "stale_pairs": stale, "table": tables.QUALIFIED_PRECEDENCE_TABLE}
    )
```

- [ ] **Step 5: Run the tests and the defs check**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_precedence.py tests/test_se_company_person_tables.py tests/test_se_company_person_jobs.py -q`
Expected: PASS.

Run: `uv run --frozen --no-sync dg check defs`
Expected: OK.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/se_company/person/precedence.py \
        src/dagster_v3/defs/se_company/person/assets.py \
        tests/test_se_company_person_precedence.py
git commit -m "feat(dagster): person name precedence and its ClickHouse export"
```

---

### Task 2: The pure fold, part 1 — identity, canonical key, rules

**Files:**
- Create: `src/dagster_v3/defs/se_company/person/fold.py` (the input types, the identity grouping, the key, the rule application; Task 3 appends the row builder and the lifecycle)
- Test: `tests/test_se_company_person_fold.py` (Task 3 appends to the same file)

**Interfaces:**
- Consumes: `person/tables.py` (`MAIN_COLUMNS`), `person/precedence.py::precedence_for` (Task 1), `person/normalize_se.py::normalize_se_person` (tests only).
- Produces: `FOLD_VERSION`, `FOLDABLE_STATUS`, `EXCLUDED_SOURCES`, `REVIEWER_SOURCE`, `NormalizedRow`, `PersonRule`, `identity_sets_before_split`, `identity_sets`, `canonical_tokens`, `person_key`, `assign_keys`, `apply_rules`.

**The rules this task implements** (spec 5.1 and 5.2, made exact):

1. Input is one company's normalized rows with `parse_status = 'ok'`. `partial` and `no_person` never fold (spec 4.4). The fold raises `ValueError` on any other status, on an excluded source, or on a `company_id` mismatch, so a wrong SELECT fails loudly instead of publishing nonsense.
2. Two rows are **the same person** when (a) `first_tokens` and `last_tokens` are equal and their `middle_tokens` are equal, **or** one side's middle tokens are a proper subset of the other's *and that other is the unique minimal superset* among the distinct middle-token sets of the company's `ok` rows sharing those first and last tokens; **or** (b) both carry the same non-empty `wikidata_id`; and in neither case when both carry a `birth_year` and the years differ.
   "Unique minimal superset" is what makes "Anna Svensson" fold with "Anna Maria Svensson" but stay alone when both "Anna Maria Svensson" and "Anna Karin Svensson" are present: `{}` then has two minimal supersets, and those two are not subsets of each other, so all three stay apart.
   Subsets compare **sets** of middle tokens, not their order.
3. Sets are the transitive closure of that relation. Every matching pair shares either the (first, last) token pair or a QID, so the closure is computed inside those two groupings — never over all pairs.
4. A closed set can still hold two distinct birth years, reached through a member carrying none. It is then **split by birth year**: one sub-set per year seeded with the members carrying it, and every year-less member attached, breadth first, to the sub-set holding a member it directly matches (ties, and they happen, go to the smallest year). This is deterministic and tested.
5. The **canonical name** of a set is the folded tokens of its most complete member: most tokens, then the longest joined string, then alphabetically first. `person_key = sha256(company_id + "\n" + " ".join(canonical tokens))`, lower hex, exactly the way `address/normalize_se.py::address_key` builds its key.
6. Two sets of one company **may not** share a key (`ReplacingMergeTree ORDER BY (company_id, person_key)` would collapse them into one person). Same-name-different-birth-year sets, and split rules, both produce that collision, so every colliding set — not just the later ones — takes a discriminator: `sha256(company_id + "\n" + canonical + "\n" + the set's birth year, or the smallest "source:slot" of the set when it has no year)`. A set alone under its canonical name keeps the plain key, which is what makes keys stable across folds.
7. Rules (spec 5.2) apply after the grouping, `active = 0` ignored, in kind order **merge, then split**, and inside a kind by `rule_id`. A merge rule's keys resolve through the previous published rows: key -> that row's `(member_sources, member_slots)` pairs -> the new sets holding any of those members; the resolved sets are joined and the joined set is re-keyed from its own canonical name. A split rule's slots leave their sets and form one set of their own. A key or slot that resolves to nothing is ignored and the rule counted in `stale_rules`. Hide rules are not applied here — they are a flag on the finished row (Task 3).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_se_company_person_fold.py
"""The pure person fold (spec 2026-09-09 section 5).

Part 1 (Task 2): identity, the canonical name and key, the rules.
Part 2 (Task 3): the person row, roles, `data`, lifecycle and history.
"""

import dataclasses
from datetime import date

import pytest

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.fold import (
    FOLD_VERSION,
    HistoryEntry,
    NormalizedRow,
    PersonRule,
    PublishedPerson,
    canonical_tokens,
    fold_company_persons,
    identity_sets,
    person_key,
)

C = "5561552760"
OTHER = "5560125220"


def row(
    source: str = "bolagsverket",
    slot: str = "s1",
    *,
    first: str = "anna",
    middles: tuple[str, ...] = (),
    last: str = "svensson",
    display: str | None = None,
    birth_year: int | None = None,
    wikidata_id: str | None = None,
    role_code: str | None = None,
    role_year: int | None = None,
    role_from: date | None = None,
    role_to: date | None = None,
    data: str = "{}",
    company_id: str = C,
    parse_status: str = "ok",
) -> NormalizedRow:
    """One normalized `ok` row. The display spelling defaults to the title-cased tokens, so
    a test that cares about spelling passes `display` explicitly."""
    display_first = " ".join(part.title() for part in (first, *middles))
    display_last = last.title()
    return NormalizedRow(
        company_id=company_id, source=source, slot=slot,
        normalized_id=f"{source}-{slot}".ljust(64, "0"), parse_status=parse_status,
        first_tokens=(first,), middle_tokens=tuple(middles), last_tokens=(last,),
        display_first=display_first, display_last=display_last,
        display_name=display or f"{display_first} {display_last}",
        birth_year=birth_year, wikidata_id=wikidata_id, role_code=role_code,
        role_year=role_year, role_from=role_from, role_to=role_to, data=data,
    )


def fold(rows, published=(), rules=(), precedence=None, *, run="run-1", year=2026):
    return fold_company_persons(
        C, rows, published, rules, precedence, source_run_id=run, current_year=year
    )


def names(result) -> list[str]:
    return sorted(person.display_name for person in result.rows)


def test_two_sources_spelling_one_name_are_one_person() -> None:
    result = fold([row("bolagsverket", "s1"), row("esef", "e1")])
    assert len(result.rows) == 1
    assert result.rows[0].sources == ("bolagsverket", "esef")
    assert result.persons == 1


def test_anna_folds_with_anna_maria_when_she_is_the_only_more_complete_name() -> None:
    result = fold([row("bolagsverket", "s1"), row("esef", "e1", middles=("maria",))])
    assert len(result.rows) == 1
    # Spelling follows the name precedence (bolagsverket 900 beats esef 400); identity
    # follows the most complete member, so the key is Anna Maria's (spec 5.1 vs 5.3).
    assert result.rows[0].display_name == "Anna Svensson"
    assert result.rows[0].text_source == "bolagsverket"
    assert result.rows[0].person_key == person_key(C, ("anna", "maria", "svensson"))
    assert canonical_tokens([row(middles=("maria",))]) == ("anna", "maria", "svensson")


def test_anna_stays_alone_when_two_middle_names_compete() -> None:
    """The spec's own example: with both Anna Maria and Anna Karin present, `{}` has two
    minimal supersets, so the bare Anna joins neither and the two full names stay apart."""
    result = fold([
        row("bolagsverket", "s1"),
        row("bolagsverket", "s2", middles=("maria",)),
        row("bolagsverket", "s3", middles=("karin",)),
    ])
    assert names(result) == ["Anna Karin Svensson", "Anna Maria Svensson", "Anna Svensson"]


def test_a_chain_of_middle_names_folds_through_its_unique_minimal_superset() -> None:
    result = fold([
        row("bolagsverket", "s1"),
        row("bolagsverket", "s2", middles=("maria",)),
        row("bolagsverket", "s3", middles=("maria", "karin")),
    ])
    assert len(result.rows) == 1
    assert result.rows[0].display_name == "Anna Maria Karin Svensson"


def test_the_normalizers_tokens_already_fold_diacritics_and_hyphens() -> None:
    """Identity is over tokens, never over display text: the normalizer folded Hakan/Håkan
    and Sven-Erik/Sven Erik before the fold ever saw them."""
    from dagster_v3.defs.se_company.person.normalize_se import RawPerson, normalize_se_person

    a = normalize_se_person(RawPerson(source="esef", full_name="Håkan Öberg"))
    b = normalize_se_person(RawPerson(source="wikidata", full_name="Hakan Oberg"))
    assert (a.first_tokens, a.last_tokens) == (b.first_tokens, b.last_tokens) == (("hakan",), ("oberg",))
    hyphen = normalize_se_person(RawPerson(source="esef", full_name="Sven-Erik Nilsson"))
    spaced = normalize_se_person(RawPerson(source="wikidata", full_name="Sven Erik Nilsson"))
    assert (hyphen.first_tokens, hyphen.middle_tokens) == (spaced.first_tokens, spaced.middle_tokens)


def test_two_spellings_with_the_same_qid_are_one_person() -> None:
    result = fold([
        row("wikidata", "q1", first="karl", last="andersson", wikidata_id="Q42"),
        row("esef", "e1", first="carl", last="anderson", wikidata_id="Q42"),
    ])
    assert len(result.rows) == 1
    assert result.rows[0].wikidata_id == "Q42"
    # Member order is precedence descending: wikidata 600 before esef 400.
    assert result.rows[0].sources == ("wikidata", "esef")
    assert result.rows[0].display_name == "Karl Andersson"


def test_the_same_name_with_two_birth_years_is_two_persons_with_two_keys() -> None:
    result = fold([
        row("bolagsverket", "s1", birth_year=1970),
        row("bolagsverket", "s2", birth_year=1980),
    ])
    assert len(result.rows) == 2
    assert {person.birth_year for person in result.rows} == {1970, 1980}
    assert len({person.person_key for person in result.rows}) == 2


def test_a_year_less_row_between_two_birth_years_splits_by_year() -> None:
    """The year-less Anna Maria matches both dated rows through the middle-name rule, so the
    closed set holds two years and is split; she attaches to the sub-set she matched
    through, the smallest year on a genuine tie."""
    sets = identity_sets([
        row("bolagsverket", "s1", middles=("maria",), birth_year=1970),
        row("bolagsverket", "s2", middles=("maria",), birth_year=1980),
        row("esef", "e1", middles=("maria",)),
    ])
    assert len(sets) == 2
    by_year = {members[0].birth_year: {member.slot for member in members} for members in sets}
    assert by_year == {1970: {"s1", "e1"}, 1980: {"s2"}}


def test_the_canonical_key_is_stable_across_two_folds() -> None:
    first = fold([row("bolagsverket", "s1")])
    again = fold(
        [row("bolagsverket", "s1"), row("esef", "e1")],
        published=[dataclasses.replace(first.rows[0], folded_at=None)],
    )
    assert again.rows[0].person_key == first.rows[0].person_key
    assert first.rows[0].person_key == person_key(C, ("anna", "svensson"))


def test_two_persons_with_the_same_canonical_name_never_share_a_key() -> None:
    """ReplacingMergeTree ORDER BY (company_id, person_key) would collapse them into one
    person, so both colliding sets take a discriminator."""
    result = fold([
        row("bolagsverket", "s1", birth_year=1970),
        row("bolagsverket", "s2", birth_year=1980),
    ])
    keys = {person.person_key for person in result.rows}
    assert len(keys) == 2
    assert person_key(C, ("anna", "svensson")) not in keys
    # the birth year is the discriminator: it does not move when a slot comes or goes
    assert person_key(C, ("anna", "svensson"), "1970") in keys
    assert person_key(C, ("anna", "svensson"), "1980") in keys


def test_a_merge_rule_joins_the_sets_behind_its_keys_and_re_keys_the_result() -> None:
    first = fold([
        row("bolagsverket", "s1", middles=("maria",)),
        row("esef", "e1", first="hakan", last="oberg"),
    ])
    published = [dataclasses.replace(person, folded_at=None) for person in first.rows]
    keys = sorted(person.person_key for person in first.rows)
    merged = fold(
        [row("bolagsverket", "s1", middles=("maria",)), row("esef", "e1", first="hakan", last="oberg")],
        published=published,
        rules=[PersonRule(C, "r" * 64, "merge", tuple(keys), ())],
    )
    live = [person for person in merged.rows if person.active == 1]
    assert len(live) == 1
    assert live[0].display_name == "Anna Maria Svensson"       # the most complete member
    assert live[0].member_slots == ("s1", "e1")
    assert merged.stale_rules == 0


def test_a_merge_rule_key_that_resolves_to_nothing_is_ignored_and_counted() -> None:
    result = fold(
        [row("bolagsverket", "s1")],
        rules=[PersonRule(C, "r" * 64, "merge", ("f" * 64, "e" * 64), ())],
    )
    assert len(result.rows) == 1
    assert result.stale_rules == 1


def test_a_split_rule_moves_its_slots_into_a_set_of_their_own() -> None:
    result = fold(
        [row("bolagsverket", "s1"), row("esef", "e1")],
        rules=[PersonRule(C, "r" * 64, "split", (), ("e1",))],
    )
    assert len(result.rows) == 2
    assert {person.member_slots for person in result.rows} == {("s1",), ("e1",)}
    assert len({person.person_key for person in result.rows}) == 2
    assert result.stale_rules == 0


def test_a_split_rule_whose_slots_are_gone_is_counted_stale() -> None:
    result = fold([row("bolagsverket", "s1")], rules=[PersonRule(C, "r" * 64, "split", (), ("gone",))])
    assert len(result.rows) == 1 and result.stale_rules == 1


def test_rules_apply_in_kind_order_merge_then_split() -> None:
    """A split rule sees the sets a merge rule already joined, never the other way round --
    otherwise a reviewer who merged two people and then split one member out would get the
    member back in the joined set."""
    first = fold([row("bolagsverket", "s1"), row("esef", "e1", first="hakan", last="oberg")])
    published = [dataclasses.replace(person, folded_at=None) for person in first.rows]
    keys = sorted(person.person_key for person in first.rows)
    result = fold(
        [row("bolagsverket", "s1"), row("esef", "e1", first="hakan", last="oberg")],
        published=published,
        rules=[
            PersonRule(C, "a" * 64, "merge", tuple(keys), ()),
            PersonRule(C, "b" * 64, "split", (), ("e1",)),
        ],
    )
    live = sorted(
        (person for person in result.rows if person.active == 1), key=lambda p: p.member_slots
    )
    assert [person.member_slots for person in live] == [("e1",), ("s1",)]
    assert result.stale_rules == 0


def test_bad_input_is_refused() -> None:
    with pytest.raises(ValueError):
        fold([row("bolagsverket", "s1", parse_status="partial")])
    with pytest.raises(ValueError):
        fold([row("reviewer_draft", "d1")])
    with pytest.raises(ValueError):
        fold([row("bolagsverket", "s1", company_id=OTHER)])
```

- [ ] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_fold.py -q`
Expected: FAIL with `ModuleNotFoundError: ...person.fold`.

- [ ] **Step 3: Write the first half of `fold.py`**

```python
# src/dagster_v3/defs/se_company/person/fold.py
"""The per-company fold of normalized person rows into published persons (spec 2026-09-09
section 5).

Pure: no I/O, no clock (the batch passes `current_year`). This module decides which persons
exist, which observations each one merges, whose spelling is published, which roles and
which `data` the row carries, which rules moved them and which previously published keys are
withdrawn. `batch.py` reads, writes and counts.

WHY IDENTITY IS ONLY EVER WITHIN ONE COMPANY: Sweden publishes no person identifier, so
"same person" is a claim this fold can only make about observations of one company (spec
section 2). person_key therefore hashes the company id together with the canonical name.
"""

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, replace
from datetime import date, datetime
from typing import Any

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.precedence import precedence_for

FOLD_VERSION = "se-person-fold-v1"
# Only `ok` rows fold. `partial` (one name word, initials only) and `no_person` (a role word,
# a number, a company suffix) are stored with their notes and never become a person (4.4).
FOLDABLE_STATUS = "ok"
EXCLUDED_SOURCES: tuple[str, ...] = ("reviewer_draft",)
REVIEWER_SOURCE = "reviewer"
MERGE, SPLIT, HIDE = "merge", "split", "hide"
HIDDEN, WITHDRAWN = "hidden", "withdrawn"
CREATED, UPDATED, REACTIVATED = "created", "updated", "reactivated"
# Everything a fold compares to decide whether a person CHANGED. The three excluded columns
# differ on every run by construction, so comparing them would make every row "changed".
_COMPARED: tuple[str, ...] = tuple(
    column
    for column in tables.MAIN_COLUMNS
    if column not in ("folded_at", "fold_version", "source_run_id")
)
# Array columns: tuples in Python, lists on the way into clickhouse-driver.
_LIST_COLUMNS: tuple[str, ...] = (
    "sources", "slots", "normalized_ids", *tables.MEMBER_COLUMNS,
    "role_codes", "role_years", "current_roles",
)


@dataclass(frozen=True, slots=True)
class NormalizedRow:
    """One current normalized row (spec 3.2) as the batch read it."""

    company_id: str
    source: str
    slot: str
    normalized_id: str
    parse_status: str
    first_tokens: tuple[str, ...]
    middle_tokens: tuple[str, ...]
    last_tokens: tuple[str, ...]
    display_first: str
    display_last: str
    display_name: str
    birth_year: int | None
    wikidata_id: str | None
    role_code: str | None
    role_year: int | None
    role_from: date | None
    role_to: date | None
    data: str

    def member_id(self) -> tuple[str, str]:
        """What a rule and a published row identify a member by."""
        return (self.source, self.slot)

    def tokens(self) -> tuple[str, ...]:
        return (*self.first_tokens, *self.middle_tokens, *self.last_tokens)


@dataclass(frozen=True, slots=True)
class PersonRule:
    """One active row of se_company_person_rule (spec 3.5)."""

    company_id: str
    rule_id: str
    kind: str
    person_keys: tuple[str, ...]
    slots: tuple[str, ...]


def _row_order(row: NormalizedRow) -> tuple[str, str]:
    return (row.source, row.slot)


def _name_key(row: NormalizedRow) -> tuple[tuple[str, ...], tuple[str, ...]]:
    return (row.first_tokens, row.last_tokens)


def _years_conflict(a: NormalizedRow, b: NormalizedRow) -> bool:
    return a.birth_year is not None and b.birth_year is not None and a.birth_year != b.birth_year


def _unique_minimal_superset(
    subject: frozenset[str], candidates: Sequence[frozenset[str]]
) -> frozenset[str] | None:
    """The one smallest middle-token set that properly contains `subject`, or None when
    there is none or more than one. "Anna Svensson" folds into "Anna Maria Svensson" only
    while Maria is the ONLY minimal way to complete the name."""
    supersets = [candidate for candidate in candidates if subject < candidate]
    minimal = [
        candidate
        for candidate in supersets
        if not any(other < candidate for other in supersets)
    ]
    return minimal[0] if len(minimal) == 1 else None


def _middles_match(
    a: NormalizedRow,
    b: NormalizedRow,
    middles_by_name: Mapping[tuple[tuple[str, ...], tuple[str, ...]], tuple[frozenset[str], ...]],
) -> bool:
    left, right = frozenset(a.middle_tokens), frozenset(b.middle_tokens)
    if left == right:
        return True
    candidates = middles_by_name[_name_key(a)]
    if left < right:
        return _unique_minimal_superset(left, candidates) == right
    if right < left:
        return _unique_minimal_superset(right, candidates) == left
    return False


def _matches(a: NormalizedRow, b: NormalizedRow, middles_by_name) -> bool:
    """The guarded pairwise relation of spec 5.1."""
    if _years_conflict(a, b):
        return False
    if a.wikidata_id and b.wikidata_id and a.wikidata_id == b.wikidata_id:
        return True
    return _name_key(a) == _name_key(b) and _middles_match(a, b, middles_by_name)


def _middles_by_name(rows: Sequence[NormalizedRow]) -> dict:
    grouped: dict = defaultdict(set)
    for row in rows:
        grouped[_name_key(row)].add(frozenset(row.middle_tokens))
    return {name: tuple(middles) for name, middles in grouped.items()}


def _split_by_birth_year(
    members: Sequence[NormalizedRow], middles_by_name
) -> tuple[tuple[NormalizedRow, ...], ...]:
    """A closed set holding two birth years is split by year; year-less members attach,
    breadth first, to the sub-set holding a member they directly match. On a genuine tie
    (a year-less member matching both years) the smallest year wins, deterministically."""
    years = sorted({member.birth_year for member in members if member.birth_year is not None})
    if len(years) <= 1:
        return (tuple(members),)
    buckets: dict[int, list[NormalizedRow]] = {
        year: [member for member in members if member.birth_year == year] for year in years
    }
    pending = [member for member in members if member.birth_year is None]
    while pending:
        remaining: list[NormalizedRow] = []
        progressed = False
        for member in pending:
            hits = sorted(
                year
                for year, bucket in buckets.items()
                if any(_matches(member, other, middles_by_name) for other in bucket)
            )
            if hits:
                buckets[hits[0]].append(member)
                progressed = True
            else:
                remaining.append(member)
        if not progressed:
            # Unreachable for a connected set (every member is joined to some seed through
            # the closure), and here so no observation can ever be dropped silently.
            buckets[years[0]].extend(remaining)
            remaining = []
        pending = remaining
    return tuple(
        tuple(sorted(bucket, key=_row_order)) for _, bucket in sorted(buckets.items())
    )


def identity_sets_before_split(
    rows: Sequence[NormalizedRow],
) -> tuple[tuple[NormalizedRow, ...], ...]:
    """The transitive closure of the guarded relation, BEFORE the birth-year split (spec
    5.1). `fold_company_persons` counts the sets this returns that still hold two years, for
    the `sets_split_by_birth_year` metric.

    Every matching pair shares either the (first_tokens, last_tokens) pair or a QID, so the
    closure is computed inside those two groupings -- never over all pairs, which for a
    96-signatory company would be 4,560 comparisons and 5.5M rows of them on prod."""
    ordered = sorted(rows, key=_row_order)
    parent = list(range(len(ordered)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    middles_by_name = _middles_by_name(ordered)
    by_name: dict = defaultdict(list)
    by_qid: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(ordered):
        by_name[_name_key(row)].append(index)
        if row.wikidata_id:
            by_qid[row.wikidata_id].append(index)
    for indexes in (*by_name.values(), *by_qid.values()):
        for position, left in enumerate(indexes):
            for right in indexes[position + 1 :]:
                if _matches(ordered[left], ordered[right], middles_by_name):
                    union(left, right)

    grouped: dict[int, list[NormalizedRow]] = defaultdict(list)
    for index, row in enumerate(ordered):
        grouped[find(index)].append(row)
    return tuple(tuple(grouped[root]) for root in sorted(grouped))


def identity_sets(rows: Sequence[NormalizedRow]) -> tuple[tuple[NormalizedRow, ...], ...]:
    """The company's persons as sets of observations: the closure, then the birth-year
    split of any set that still holds two years (spec 5.1)."""
    middles_by_name = _middles_by_name(rows)
    sets: list[tuple[NormalizedRow, ...]] = []
    for members in identity_sets_before_split(rows):
        sets.extend(_split_by_birth_year(members, middles_by_name))
    return tuple(sets)


def canonical_tokens(members: Sequence[NormalizedRow]) -> tuple[str, ...]:
    """The folded tokens of the most complete member: most tokens, then the longest joined
    string, then alphabetically first (spec 5.1)."""
    def completeness(row: NormalizedRow) -> tuple[int, int, str]:
        tokens = row.tokens()
        joined = " ".join(tokens)
        return (-len(tokens), -len(joined), joined)

    return min(members, key=completeness).tokens()


def person_key(company_id: str, canonical: Sequence[str], discriminator: str = "") -> str:
    """sha256(company id, canonical name[, discriminator]), lower hex -- the same shape as
    `address/normalize_se.py::address_key`. The discriminator is only ever set when two sets
    of one company share a canonical name (`assign_keys`)."""
    parts = [company_id, " ".join(canonical)]
    if discriminator:
        parts.append(discriminator)
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def assign_keys(
    company_id: str, sets: Sequence[Sequence[NormalizedRow]]
) -> list[tuple[tuple[NormalizedRow, ...], str]]:
    """(members, person_key) per set, with collisions broken.

    Two sets of one company can genuinely share a canonical name -- two Anna Svenssons with
    different birth years, or a split rule that separates two identical spellings -- and the
    main table is a ReplacingMergeTree ORDER BY (company_id, person_key), so a shared key
    would silently publish one person instead of two. Every colliding set (not only the
    later ones, which would move the plain key from one person to another when a new set
    appears) takes a discriminator: its birth year when it has one (a set never holds two,
    and a year does not move when slots come and go), else the smallest "source:slot" of
    its members (the split-rule case). A set alone under its name keeps the plain key,
    which is what makes keys stable across folds."""
    canonical = [tuple(canonical_tokens(members)) for members in sets]
    shared = {name for name in canonical if canonical.count(name) > 1}
    assigned: list[tuple[tuple[NormalizedRow, ...], str]] = []
    for members, name in zip(sets, canonical, strict=True):
        discriminator = ""
        if name in shared:
            years = {member.birth_year for member in members if member.birth_year is not None}
            discriminator = (
                str(min(years)) if years else min(f"{member.source}:{member.slot}" for member in members)
            )
        assigned.append((tuple(members), person_key(company_id, name, discriminator)))
    return assigned


def _apply_merge(
    sets: list[list[NormalizedRow]], rule: PersonRule, previous_members: Mapping[str, frozenset]
) -> tuple[list[list[NormalizedRow]], bool]:
    """Join the sets holding the members of the rule's keys. A key that names no previously
    published row, or whose members are all gone, resolves to nothing: it is ignored and the
    rule reported stale (spec 5.2)."""
    targets: list[int] = []
    stale = False
    for key in rule.person_keys:
        members = previous_members.get(key, frozenset())
        hits = [
            index
            for index, group in enumerate(sets)
            if any(row.member_id() in members for row in group)
        ]
        if not hits:
            stale = True
            continue
        targets.extend(hits)
    ordered = sorted(set(targets))
    if len(ordered) < 2:
        return sets, stale
    joined = [row for index in ordered for row in sets[index]]
    rebuilt: list[list[NormalizedRow]] = []
    for index, group in enumerate(sets):
        if index == ordered[0]:
            rebuilt.append(sorted(joined, key=_row_order))
        elif index not in set(ordered):
            rebuilt.append(group)
    return rebuilt, stale


def _apply_split(
    sets: list[list[NormalizedRow]], rule: PersonRule
) -> tuple[list[list[NormalizedRow]], bool]:
    """The rule's slots leave their sets and form one set of their own (spec 5.2)."""
    wanted = set(rule.slots)
    moved = [row for group in sets for row in group if row.slot in wanted]
    if not moved:
        return sets, True
    kept = [[row for row in group if row.slot not in wanted] for group in sets]
    return [*[group for group in kept if group], sorted(moved, key=_row_order)], False


def apply_rules(
    sets: Sequence[Sequence[NormalizedRow]],
    rules: Sequence[PersonRule],
    previous_members: Mapping[str, frozenset],
) -> tuple[list[list[NormalizedRow]], int]:
    """Merge rules then split rules, each kind ordered by rule_id, and the count of rules
    with at least one key or slot that resolved to nothing. Hide rules are a flag on the
    finished row, not a regrouping, and are applied by `fold_company_persons`."""
    working = [list(members) for members in sets]
    stale = 0
    for kind, apply in ((MERGE, _apply_merge), (SPLIT, _apply_split)):
        for rule in sorted((rule for rule in rules if rule.kind == kind), key=lambda r: r.rule_id):
            if kind == MERGE:
                working, rule_stale = _apply_merge(working, rule, previous_members)
            else:
                working, rule_stale = _apply_split(working, rule)
            stale += int(rule_stale)
    return working, stale
```

- [ ] **Step 4: Run the part-1 tests until green**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_fold.py -q`
Expected: the test file does NOT import yet — it names `fold_company_persons`, `PublishedPerson` and `HistoryEntry`, which Task 3 writes — so do not run it here. Run `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync python -c "from dagster_v3.defs.se_company.person import fold"` → no error, then continue straight into Task 3. Tasks 2 and 3 are executed by ONE implementer as one unit (controller ruling 2026-09-10): the Task 2 commit below is optional; a single commit at the end of Task 3 with the whole fold module and its green test file is the expected shape.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/person/fold.py
git commit -m "feat(dagster): person identity grouping, canonical key and reviewer rules"
```

---

### Task 3: The pure fold, part 2 — the person row, roles, `data`, lifecycle and history

**Files:**
- Modify: `src/dagster_v3/defs/se_company/person/fold.py` (append; Task 2 wrote the first half)
- Test: `tests/test_se_company_person_fold.py` (append)

**Interfaces:**
- Consumes: everything Task 2 produced (`NormalizedRow`, `PersonRule`, `identity_sets`, `canonical_tokens`, `person_key`, `assign_keys`, `apply_rules`, the constants), `precedence.precedence_for`, `tables.MAIN_COLUMNS`/`HISTORY_COLUMNS`.
- Produces: `RoleBlock`, `PublishedPerson`, `HistoryEntry`, `FoldResult`, `role_years_for`, `role_block`, `merge_member_data`, `change_kind_for`, `fold_company_persons` — everything `batch.py` (Task 4) calls.

**The rules this task implements** (spec 5.3 to 5.6, made exact):

1. **Member order** is `(-precedence_for(source, company_precedence), source, slot)`. Every `member_*` array, `slots` and `normalized_ids` follow it; `sources` is the same order deduplicated (the serving view asks `has(sources, 'bolagsverket')`, so it is a set, while `slots`/`normalized_ids` are per-member lineage and therefore parallel to `member_slots`).
2. **Spelling** (`display_name`, `first_name`, `last_name`, `text_source`) comes from the highest-`name`-precedence member, ties broken by the most complete spelling: most tokens, then the longest `display_name`, then alphabetically, then `(source, slot)`.
3. `birth_year` and `wikidata_id` come from the first member in member order that carries one. A birth-year conflict cannot reach here (the identity guard); a QID conflict can (two QIDs on rows joined by name), and the highest-precedence member's wins.
4. **Roles** (spec 5.4 and 4.3) are the union over members of `(role_code, year)` pairs, each with the sources that saw it in member order, sorted by year then code. A member's years are: its `role_year` (Bolagsverket's and ESEF's fiscal year) when it has one; else its `role_from`..`role_to` span expanded year by year, `role_to` missing meaning "to the current UTC year" (this is Wikidata's case: `role_year` is always NULL there); else `role_to`'s year alone; else **the current UTC year alone** (a role with no date is taken as held now; controller ruling 2026-09-10, 290 of Wikidata's 466 roles on prod carry no date). A roleless member (`role_code IS NULL`, 2,018,042 Bolagsverket rows on prod) contributes no pair and still counts as a member. `first_year`/`last_year` are the min and max; `current_roles` are the codes of `last_year`.
5. **`data`** (spec 5.5) merges the members' objects key by key in precedence order: a key present in one member is taken, a key present in several takes the higher-precedence member's value, two objects merge one level down, arrays are replaced. A `reviewer` member's value wins outright — taken whole, never merged into a lower member's object. The result is a JSON object string with sorted keys (so the row's `changed_against` comparison is stable), `'{}'` when nothing.
6. **Lifecycle** (spec 5.6). Every set publishes `active = 1, inactive_reason = ''` unless a hide rule names its key (`active = 0, inactive_reason = 'hidden'`). Every previously published key with no set this fold is re-emitted `active = 0, inactive_reason = 'withdrawn'`, keeping its blocks: its observations are gone or all tombstoned (a tombstone normalizes to `no_person`, which never folds).
7. **History** is appended before the main write, one row per person whose published columns changed, carrying the **previous** main row's columns plus `changed_at`, `change_kind` and `fold_run_id`. A person published for the first time has no previous image, so its `created` row carries the new image — the genesis entry that makes the backoffice's timeline start somewhere. `change_kind` is `created` (no previous row), `withdrawn` (the row becomes withdrawn and was not), `hidden` (becomes hidden and was not), `reactivated` (`active` goes 0 -> 1, which covers both a withdrawn person returning and a hide rule being reset), else `updated`.
8. **"Changed"** compares the 26 published columns except `folded_at`, `fold_version`, `source_run_id`. A person whose columns did not change gets **no history row** — but its main row is still rewritten with this fold's `folded_at`, exactly as the address fold rewrites the whole set. That rewrite is what makes the selection converge: `main_watermarks_sql` reads `max(folded_at)` per company, so a company selected because of a rule or a precedence export that changed nothing must still advance its watermark or it would be selected again on every later run (Task 4's re-run proof).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_se_company_person_fold.py`:

```python
# --- part 2: the person row, roles, data, lifecycle -------------------------------------

from dagster_v3.defs.se_company.person.fold import (  # noqa: E402  (kept beside part 1's import)
    change_kind_for,
    merge_member_data,
    role_block,
    role_years_for,
)


def published(result, key: str | None = None):
    """The row of `key`, or the only row."""
    if key is None:
        assert len(result.rows) == 1
        return result.rows[0]
    return next(person for person in result.rows if person.person_key == key)


def refold(result, rows, rules=(), precedence=None, *, run="run-2", year=2026):
    """Feed a fold's output back in as the published set, the way the batch does."""
    previous = [dataclasses.replace(person, folded_at=None) for person in result.rows]
    return fold(rows, published=previous, rules=rules, precedence=precedence, run=run, year=year)


def test_the_person_row_takes_its_spelling_from_the_highest_precedence_member() -> None:
    result = fold([
        row("esef", "e1", display="HÅKAN ÖBERG", first="hakan", last="oberg"),
        row("bolagsverket", "s1", display="Håkan Öberg", first="hakan", last="oberg"),
    ])
    person = published(result)
    assert (person.display_name, person.text_source) == ("Håkan Öberg", "bolagsverket")
    assert (person.first_name, person.last_name) == ("Hakan", "Oberg")


def test_a_precedence_tie_takes_the_most_complete_spelling() -> None:
    result = fold([
        row("bolagsverket", "s1"),
        row("bolagsverket", "s2", middles=("maria",)),
    ])
    assert published(result).display_name == "Anna Maria Svensson"


def test_a_hide_rule_follows_the_person_when_a_fuller_spelling_re_keys_the_set() -> None:
    """A hide names the key as it was; a later member with a longer name moves the set to
    a new canonical key, and the hide resolves through the previous members instead of
    going stale (otherwise a Remove would silently reverse)."""
    first = fold([row("bolagsverket", "s1")])
    old_key = first.rows[0].person_key
    hide = PersonRule(C, "h" * 64, "hide", (old_key,), ())
    hidden = fold([row("bolagsverket", "s1")], published=first.rows, rules=[hide])
    assert hidden.rows[0].active == 0 and hidden.rows[0].inactive_reason == "hidden"
    grown = fold(
        [row("bolagsverket", "s1"), row("esef", "e1", middles=("maria",))],
        published=hidden.rows, rules=[hide],
    )
    new = [person for person in grown.rows if person.person_key != old_key]
    assert len(new) == 1 and new[0].active == 0 and new[0].inactive_reason == "hidden"
    assert grown.stale_rules == 0


def test_a_company_precedence_row_overrides_the_global_order() -> None:
    rows = [
        row("esef", "e1", display="HAKAN OBERG", first="hakan", last="oberg"),
        row("bolagsverket", "s1", display="Håkan Öberg", first="hakan", last="oberg"),
    ]
    assert published(fold(rows, precedence={"esef": 5000})).text_source == "esef"


def test_members_are_ordered_by_precedence_then_slot_and_the_arrays_are_parallel() -> None:
    result = fold([
        row("esef", "e2", data='{"confidence":"0.9"}'),
        row("esef", "e1"),
        row("wikidata", "q1", birth_year=1970, wikidata_id="Q42"),
        row("bolagsverket", "s1"),
    ])
    person = published(result)
    assert person.member_sources == ("bolagsverket", "wikidata", "esef", "esef")
    assert person.member_slots == ("s1", "q1", "e1", "e2")
    assert person.slots == person.member_slots
    assert person.sources == ("bolagsverket", "wikidata", "esef")
    assert person.member_birth_years == (None, 1970, None, None)
    assert person.member_wikidata_ids == ("", "Q42", "", "")
    assert person.member_names == ("Anna Svensson",) * 4
    assert person.member_data == ("{}", "{}", "{}", '{"confidence":"0.9"}')
    assert person.normalized_ids == tuple(
        f"{source}-{slot}".ljust(64, "0")
        for source, slot in zip(person.member_sources, person.member_slots, strict=True)
    )


def test_birth_year_and_qid_come_from_whichever_member_carries_one() -> None:
    person = published(fold([
        row("bolagsverket", "s1"),
        row("wikidata", "q1", birth_year=1970, wikidata_id="Q42"),
    ]))
    assert (person.birth_year, person.wikidata_id) == (1970, "Q42")


def test_roles_are_a_union_of_pairs_with_their_sources_sorted_by_year_then_code() -> None:
    person = published(fold([
        row("bolagsverket", "s1", role_code="board_member", role_year=2024),
        row("bolagsverket", "s2", role_code="board_chair", role_year=2024),
        row("esef", "e1", role_code="board_member", role_year=2024),
        row("bolagsverket", "s3", role_code="board_member", role_year=2023),
    ]))
    assert person.role_codes == ("board_member", "board_chair", "board_member")
    assert person.role_years == (2023, 2024, 2024)
    assert person.role_sources == (("bolagsverket",), ("bolagsverket",), ("bolagsverket", "esef"))
    assert (person.first_year, person.last_year) == (2023, 2024)
    assert set(person.current_roles) == {"board_chair", "board_member"}


def test_a_wikidata_span_expands_year_by_year_and_an_open_span_reaches_the_current_year() -> None:
    closed = row(
        "wikidata", "q1", role_code="founder",
        role_from=date(2019, 5, 1), role_to=date(2021, 3, 1),
    )
    assert role_years_for(closed, 2026) == (2019, 2020, 2021)
    open_span = row("wikidata", "q2", role_code="owner", role_from=date(2024, 1, 1))
    assert role_years_for(open_span, 2026) == (2024, 2025, 2026)
    person = published(fold([open_span]))
    assert person.current_roles == ("owner",) and person.last_year == 2026


def test_a_fiscal_year_wins_over_a_span_and_a_year_less_role_is_current() -> None:
    """ESEF delivers both a document fiscal year and an effective span; spec 4.3 says the
    document's fiscal year is that row's role year. A role with no year information at all
    is taken as held now (controller ruling 2026-09-10: 290 of Wikidata's 466 roles on prod
    carry no date; dropping them would hide most Wikidata roles), so it makes the pair
    (code, current_year); a row with only an end date makes (code, end year)."""
    esef = row("esef", "e1", role_code="auditor", role_year=2023, role_from=date(2020, 1, 1))
    assert role_years_for(esef, 2026) == (2023,)
    dateless = row("wikidata", "q3", role_code="executive")
    assert role_years_for(dateless, 2026) == (2026,)
    end_only = row("wikidata", "q4", role_code="owner", role_to=date(2021, 6, 1))
    assert role_years_for(end_only, 2026) == (2021,)
    person = published(fold([dateless]))
    assert person.role_codes == ("executive",) and person.first_year == 2026
    assert person.current_roles == ("executive",) and person.member_slots == ("q3",)


def test_a_roleless_member_still_counts_as_a_member() -> None:
    person = published(fold([
        row("bolagsverket", "s1", role_code=None, role_year=2024),
        row("bolagsverket", "s2", role_code="board_member", role_year=2024),
    ]))
    assert person.member_slots == ("s1", "s2")
    assert person.role_codes == ("board_member",) and person.role_years == (2024,)


def test_role_block_keeps_a_code_the_normalizer_could_not_map() -> None:
    """Owner ruling 2026-08-28: an unmapped label publishes as itself. The fold keeps it."""
    block = role_block([row("bolagsverket", "s1", role_code="styrelseledarmot", role_year=2024)], 2026)
    assert block.role_codes == ("styrelseledarmot",)


def test_data_merges_key_by_key_by_precedence_with_nested_objects_and_replaced_arrays() -> None:
    members = [
        row("bolagsverket", "s1", data='{"kind":"board","extra":{"a":"1","b":"2"},"tags":["x"]}'),
        row("esef", "e1", data='{"kind":"llm","confidence":"0.9","extra":{"b":"9","c":"3"},"tags":["y","z"]}'),
    ]
    merged = json.loads(merge_member_data(members, None))
    assert merged["kind"] == "board"                       # bolagsverket 900 > esef 400
    assert merged["confidence"] == "0.9"                   # only ESEF has it
    assert merged["extra"] == {"a": "1", "b": "2", "c": "3"}   # one level down, higher wins
    assert merged["tags"] == ["x"]                         # arrays replace, never concatenate
    assert merge_member_data([row("bolagsverket", "s1")], None) == "{}"
    assert merge_member_data([row("bolagsverket", "s1", data="not json")], None) == "{}"


def test_reviewer_data_keys_win_outright() -> None:
    members = [
        row("bolagsverket", "s1", data='{"extra":{"a":"1","b":"2"}}'),
        row("reviewer", "r00000000000000001", data='{"extra":{"b":"9"}}'),
    ]
    merged = json.loads(merge_member_data(members, None))
    assert merged["extra"] == {"b": "9"}                   # taken whole, not merged


def test_the_row_data_is_the_merged_object_and_the_key_order_is_stable() -> None:
    person = published(fold([
        row("esef", "e1", data='{"b":"2","a":"1"}'),
        row("bolagsverket", "s1", data='{"c":"3"}'),
    ]))
    assert person.data == '{"a":"1","b":"2","c":"3"}'


def test_a_first_fold_creates_and_an_identical_second_fold_changes_nothing() -> None:
    rows = [row("bolagsverket", "s1", role_code="board_member", role_year=2024)]
    first = fold(rows)
    assert (first.created, first.updated, first.unchanged) == (1, 0, 0)
    assert [entry.change_kind for entry in first.history] == ["created"]
    assert first.history[0].row.person_key == first.rows[0].person_key
    again = refold(first, rows)
    assert (again.created, again.updated, again.unchanged) == (0, 0, 1)
    assert again.history == ()
    assert again.rows[0].person_key == first.rows[0].person_key


def test_a_new_member_updates_the_person_and_history_keeps_the_previous_image() -> None:
    first = fold([row("bolagsverket", "s1")])
    second = refold(first, [row("bolagsverket", "s1"), row("esef", "e1")])
    assert (second.updated, second.unchanged) == (1, 0)
    entry = second.history[0]
    assert entry.change_kind == "updated"
    assert entry.row.sources == ("bolagsverket",)          # the PREVIOUS image
    assert second.rows[0].sources == ("bolagsverket", "esef")


def test_a_key_that_loses_every_observation_is_withdrawn_once_then_left_alone() -> None:
    first = fold([row("bolagsverket", "s1")])
    gone = refold(first, [])
    withdrawn = published(gone)
    assert (withdrawn.active, withdrawn.inactive_reason) == (0, "withdrawn")
    assert withdrawn.sources == ("bolagsverket",)          # its blocks are kept
    assert [entry.change_kind for entry in gone.history] == ["withdrawn"]
    assert gone.history[0].row.active == 1                 # the previous, live image
    again = refold(gone, [], run="run-3")
    assert again.history == () and again.unchanged == 1 and again.withdrawn == 0
    assert published(again).inactive_reason == "withdrawn"


def test_a_withdrawn_person_who_comes_back_is_reactivated() -> None:
    rows = [row("bolagsverket", "s1")]
    gone = refold(fold(rows), [])
    back = refold(gone, rows, run="run-3")
    person = published(back)
    assert (person.active, person.inactive_reason) == (1, "")
    assert [entry.change_kind for entry in back.history] == ["reactivated"]
    assert back.reactivated == 1


def test_a_hide_rule_marks_the_set_inactive_and_a_reset_reactivates_it() -> None:
    rows = [row("bolagsverket", "s1")]
    first = fold(rows)
    key = first.rows[0].person_key
    hidden = refold(first, rows, rules=[PersonRule(C, "h" * 64, "hide", (key,), ())])
    person = published(hidden)
    assert (person.active, person.inactive_reason) == (0, "hidden")
    assert [entry.change_kind for entry in hidden.history] == ["hidden"]
    assert hidden.persons == 0 and hidden.hidden == 1
    reset = refold(hidden, rows, run="run-3")               # the batch stopped passing the rule
    assert (published(reset).active, published(reset).inactive_reason) == (1, "")
    assert [entry.change_kind for entry in reset.history] == ["reactivated"]


def test_a_hide_rule_for_a_key_no_set_carries_is_counted_stale() -> None:
    result = fold([row("bolagsverket", "s1")], rules=[PersonRule(C, "h" * 64, "hide", ("f" * 64,), ())])
    assert published(result).active == 1 and result.stale_rules == 1


def test_change_kind_for_names_every_transition() -> None:
    live = fold([row("bolagsverket", "s1")]).rows[0]
    hidden = dataclasses.replace(live, active=0, inactive_reason="hidden")
    withdrawn = dataclasses.replace(live, active=0, inactive_reason="withdrawn")
    assert change_kind_for(live, dataclasses.replace(live, display_name="Other")) == "updated"
    assert change_kind_for(live, hidden) == "hidden"
    assert change_kind_for(live, withdrawn) == "withdrawn"
    assert change_kind_for(withdrawn, live) == "reactivated"
    assert change_kind_for(hidden, live) == "reactivated"
    assert set(tables.CHANGE_KINDS) == {"created", "updated", "hidden", "withdrawn", "reactivated"}


def test_as_tuple_follows_main_columns_and_history_tuple_appends_the_change_block() -> None:
    from datetime import UTC, datetime

    folded_at = datetime(2026, 9, 10, 9, 0, tzinfo=UTC)
    person = fold([row("bolagsverket", "s1", role_code="board_member", role_year=2024)]).rows[0]
    values = person.as_tuple(folded_at)
    assert len(values) == len(tables.MAIN_COLUMNS) == 29
    by_name = dict(zip(tables.MAIN_COLUMNS, values, strict=True))
    assert by_name["folded_at"] == folded_at and by_name["fold_version"] == FOLD_VERSION
    assert by_name["source_run_id"] == "run-1" and by_name["data"] == "{}"
    for column in ("sources", "slots", "normalized_ids", *tables.MEMBER_COLUMNS, "role_codes",
                   "role_years", "current_roles", "role_sources"):
        assert isinstance(by_name[column], list), column
    assert by_name["role_sources"] == [["bolagsverket"]]
    for column in ("company_id", "person_key", "display_name", "first_name", "last_name",
                   "text_source", "inactive_reason", "data", "fold_version", "source_run_id"):
        assert by_name[column] is not None, column
    history = HistoryEntry(person, "created").row.history_tuple(
        changed_at=folded_at, change_kind="created", fold_run_id="run-1"
    )
    assert len(history) == len(tables.HISTORY_COLUMNS) == 32
    assert history[-3:] == (folded_at, "created", "run-1")
    assert dict(zip(tables.HISTORY_COLUMNS, history, strict=True))["folded_at"] == folded_at
```

Add `import json` to the test module's imports.

- [ ] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_fold.py -q`
Expected: FAIL with `ImportError: cannot import name 'PublishedPerson'`.

- [ ] **Step 3: Append the second half of `fold.py`**

```python
@dataclass(frozen=True, slots=True)
class RoleBlock:
    """The six role columns of spec 3.3, computed together because they share one union."""

    role_codes: tuple[str, ...]
    role_years: tuple[int, ...]
    role_sources: tuple[tuple[str, ...], ...]
    current_roles: tuple[str, ...]
    first_year: int | None
    last_year: int | None


@dataclass(frozen=True, slots=True)
class PublishedPerson:
    """One main-table row (spec 3.3): the 29 columns of tables.MAIN_COLUMNS in order.

    `folded_at` is the stamp the row carries in ClickHouse -- None for a row this fold just
    built, which the batch stamps at write time through `as_tuple`. `history_tuple` keeps the
    stored stamp instead, because a history row is the PREVIOUS image and its `folded_at` is
    when that image was published; together with `changed_at` it bounds the window the image
    was live.
    """

    company_id: str
    person_key: str
    display_name: str
    first_name: str
    last_name: str
    birth_year: int | None
    wikidata_id: str | None
    sources: tuple[str, ...]
    slots: tuple[str, ...]
    normalized_ids: tuple[str, ...]
    member_sources: tuple[str, ...]
    member_slots: tuple[str, ...]
    member_names: tuple[str, ...]
    member_birth_years: tuple[int | None, ...]
    member_wikidata_ids: tuple[str, ...]
    member_data: tuple[str, ...]
    role_codes: tuple[str, ...]
    role_years: tuple[int, ...]
    role_sources: tuple[tuple[str, ...], ...]
    current_roles: tuple[str, ...]
    first_year: int | None
    last_year: int | None
    text_source: str
    data: str
    active: int
    inactive_reason: str
    folded_at: datetime | None
    fold_version: str
    source_run_id: str

    def as_tuple(self, folded_at: datetime) -> tuple[Any, ...]:
        values = {field.name: getattr(self, field.name) for field in fields(self)}
        for name in _LIST_COLUMNS:
            values[name] = list(values[name])
        values["role_sources"] = [list(sources) for sources in self.role_sources]
        values["folded_at"] = folded_at
        return tuple(values[column] for column in tables.MAIN_COLUMNS)

    def history_tuple(
        self, *, changed_at: datetime, change_kind: str, fold_run_id: str
    ) -> tuple[Any, ...]:
        return (
            *self.as_tuple(self.folded_at or changed_at),
            changed_at,
            change_kind,
            fold_run_id,
        )

    def changed_against(self, other: "PublishedPerson | None") -> bool:
        if other is None:
            return True
        return any(getattr(self, name) != getattr(other, name) for name in _COMPARED)


@dataclass(frozen=True, slots=True)
class HistoryEntry:
    """One row of se_company_person_history: the PREVIOUS main row and what happened to it.
    A `created` entry carries the new row, the only image there is."""

    row: PublishedPerson
    change_kind: str


@dataclass(frozen=True, slots=True)
class FoldResult:
    rows: tuple[PublishedPerson, ...]
    history: tuple[HistoryEntry, ...]
    persons: int          # active rows in `rows`
    created: int
    updated: int
    hidden: int           # history rows of kind hidden, not the number of hidden rows
    withdrawn: int
    reactivated: int
    unchanged: int        # rows rewritten with no history row
    stale_rules: int
    sets_split_by_birth_year: int


def role_years_for(row: NormalizedRow, current_year: int) -> tuple[int, ...]:
    """The years one member's role was observed (spec 4.3).

    A fiscal year (Bolagsverket's report year, ESEF's document year) is the row's year and
    wins over any span the same row carries. Otherwise the span is expanded year by year,
    an open end meaning "still now" -- Wikidata's case, whose rows never carry a fiscal
    year. A row with a role and no year at all is taken as held now and makes the pair
    (code, current_year): on prod 290 of Wikidata's 466 roles carry no date, and dropping
    them would hide most Wikidata roles (controller ruling 2026-09-10). A row with only an
    end date makes (code, end year)."""
    if row.role_year is not None:
        return (int(row.role_year),)
    if row.role_from is not None:
        end = row.role_to.year if row.role_to is not None else current_year
        return tuple(range(row.role_from.year, max(end, row.role_from.year) + 1))
    if row.role_to is not None:
        return (row.role_to.year,)
    return (current_year,)


def role_block(members: Sequence[NormalizedRow], current_year: int) -> RoleBlock:
    """The union of (role code, year) pairs with the sources that saw each (spec 5.4).
    `members` is already in member order, which is the order the sources come out in."""
    seen: dict[tuple[str, int], list[str]] = {}
    for member in members:
        if not member.role_code:
            continue
        for year in role_years_for(member, current_year):
            sources = seen.setdefault((member.role_code, year), [])
            if member.source not in sources:
                sources.append(member.source)
    ordered = sorted(seen, key=lambda pair: (pair[1], pair[0]))
    years = tuple(year for _, year in ordered)
    last_year = max(years) if years else None
    return RoleBlock(
        role_codes=tuple(code for code, _ in ordered),
        role_years=years,
        role_sources=tuple(tuple(seen[pair]) for pair in ordered),
        current_roles=tuple(sorted({code for code, year in ordered if year == last_year})),
        first_year=min(years) if years else None,
        last_year=last_year,
    )


def _data_object(text: str) -> dict[str, Any]:
    """A member's `data` as a dict. The tables constrain it to a JSON object, so anything
    else is a hand-written row: it degrades to {} instead of failing a 20,000-company page."""
    try:
        parsed = json.loads(text or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _member_order(row: NormalizedRow, company_precedence) -> tuple[int, str, str]:
    return (-precedence_for(row.source, company_precedence), row.source, row.slot)


def merge_member_data(members: Sequence[NormalizedRow], company_precedence) -> str:
    """The members' `data` objects merged by `name` precedence (spec 5.5): the higher member
    wins a shared key, two objects merge one level down, arrays are replaced. A reviewer's
    value is taken whole -- "reviewer keys win outright" means their object is not merged
    into a machine source's. Keys are sorted so the row compares stably across folds."""
    ordered = sorted(members, key=lambda row: _member_order(row, company_precedence))
    merged: dict[str, Any] = {}
    for member in reversed(ordered):          # lowest precedence first, so higher overwrites
        reviewer = member.source == REVIEWER_SOURCE
        for key, value in _data_object(member.data).items():
            existing = merged.get(key)
            if not reviewer and isinstance(value, dict) and isinstance(existing, dict):
                merged[key] = {**existing, **value}
            else:
                merged[key] = value
    return json.dumps(merged, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _text_member(members: Sequence[NormalizedRow], company_precedence) -> NormalizedRow:
    """Whose spelling the person shows (spec 5.3): the highest `name` precedence, ties by
    the most complete spelling."""
    def order(row: NormalizedRow) -> tuple:
        return (
            -precedence_for(row.source, company_precedence),
            -len(row.tokens()),
            -len(row.display_name),
            row.display_name,
            row.source,
            row.slot,
        )

    return min(members, key=order)


def _published_from(
    company_id: str,
    members: Sequence[NormalizedRow],
    key: str,
    *,
    company_precedence,
    source_run_id: str,
    current_year: int,
    hidden: bool,
) -> PublishedPerson:
    ordered = sorted(members, key=lambda row: _member_order(row, company_precedence))
    text = _text_member(ordered, company_precedence)
    roles = role_block(ordered, current_year)
    sources: list[str] = []
    for member in ordered:
        if member.source not in sources:
            sources.append(member.source)
    return PublishedPerson(
        company_id=company_id,
        person_key=key,
        display_name=text.display_name,
        first_name=text.display_first,
        last_name=text.display_last,
        birth_year=next((m.birth_year for m in ordered if m.birth_year is not None), None),
        wikidata_id=next((m.wikidata_id for m in ordered if m.wikidata_id), None),
        sources=tuple(sources),
        slots=tuple(member.slot for member in ordered),
        normalized_ids=tuple(member.normalized_id for member in ordered),
        member_sources=tuple(member.source for member in ordered),
        member_slots=tuple(member.slot for member in ordered),
        member_names=tuple(member.display_name for member in ordered),
        member_birth_years=tuple(member.birth_year for member in ordered),
        member_wikidata_ids=tuple(member.wikidata_id or "" for member in ordered),
        member_data=tuple(member.data or "{}" for member in ordered),
        role_codes=roles.role_codes,
        role_years=roles.role_years,
        role_sources=roles.role_sources,
        current_roles=roles.current_roles,
        first_year=roles.first_year,
        last_year=roles.last_year,
        text_source=text.source,
        data=merge_member_data(ordered, company_precedence),
        active=0 if hidden else 1,
        inactive_reason=HIDDEN if hidden else "",
        folded_at=None,
        fold_version=FOLD_VERSION,
        source_run_id=source_run_id,
    )


def change_kind_for(previous: PublishedPerson, new: PublishedPerson) -> str:
    """What happened to a person whose columns changed (spec 3.4's five kinds). Reactivation
    covers both a withdrawn person coming back and a hide rule being reset."""
    if new.inactive_reason == WITHDRAWN and previous.inactive_reason != WITHDRAWN:
        return WITHDRAWN
    if new.inactive_reason == HIDDEN and previous.inactive_reason != HIDDEN:
        return HIDDEN
    if new.active == 1 and previous.active == 0:
        return REACTIVATED
    return UPDATED


def fold_company_persons(
    company_id: str,
    rows: Sequence[NormalizedRow],
    published: Sequence[PublishedPerson],
    rules: Sequence[PersonRule],
    company_precedence: Mapping[str, int] | None,
    *,
    source_run_id: str,
    current_year: int,
) -> FoldResult:
    """One company's whole published set, plus the history entries for what changed.

    `rows` are the company's current normalized rows with parse_status 'ok'; `published` its
    current main rows; `rules` its ACTIVE rules (the batch filters active = 0); and
    `company_precedence` its own precedence rows, if any. The result's `rows` are written to
    the main table as a set -- active, hidden and withdrawn alike -- so the company's fold
    watermark advances even when nothing about it changed."""
    for row in rows:
        if row.company_id != company_id:
            raise ValueError(f"row company_id {row.company_id!r} is not {company_id!r}")
        if row.parse_status != FOLDABLE_STATUS:
            raise ValueError(f"{row.source}/{row.slot}: parse_status {row.parse_status!r} never folds")
        if row.source in EXCLUDED_SOURCES:
            raise ValueError(f"{row.source}/{row.slot}: source never folds")

    grouped = identity_sets(rows)
    sets_split = sum(
        1
        for members in identity_sets_before_split(rows)
        if len({member.birth_year for member in members if member.birth_year is not None}) > 1
    )
    previous_members = {
        person.person_key: frozenset(
            zip(person.member_sources, person.member_slots, strict=True)
        )
        for person in published
    }
    ruled, stale_rules = apply_rules(grouped, rules, previous_members)
    assigned = assign_keys(company_id, ruled)
    # A hide names a key as it was. It resolves by key equality first; when the key is
    # gone (a fuller spelling re-keyed the set), through the previous published members,
    # as a merge rule does -- otherwise a reviewer's Remove would silently reverse.
    live_keys = {key for _, key in assigned}
    member_pairs = {key: {(m.source, m.slot) for m in members} for members, key in assigned}
    hide_keys: set[str] = set()
    for rule in rules:
        if rule.kind != HIDE:
            continue
        matched = False
        for key in rule.person_keys:
            if key in live_keys:
                hide_keys.add(key)
                matched = True
                continue
            before = previous_members.get(key) or frozenset()
            for new_key, pairs in member_pairs.items():
                if before & pairs:
                    hide_keys.add(new_key)
                    matched = True
        if not matched:
            stale_rules += 1

    new_rows = [
        _published_from(
            company_id, members, key,
            company_precedence=company_precedence, source_run_id=source_run_id,
            current_year=current_year, hidden=key in hide_keys,
        )
        for members, key in assigned
    ]
    previous_by_key = {person.person_key: person for person in published}
    out: list[PublishedPerson] = []
    history: list[HistoryEntry] = []
    counts = {CREATED: 0, UPDATED: 0, HIDDEN: 0, WITHDRAWN: 0, REACTIVATED: 0}
    unchanged = 0
    for person in sorted(new_rows, key=lambda row: row.person_key):
        previous = previous_by_key.get(person.person_key)
        if previous is None:
            history.append(HistoryEntry(person, CREATED))
            counts[CREATED] += 1
        elif person.changed_against(previous):
            kind = change_kind_for(previous, person)
            history.append(HistoryEntry(previous, kind))
            counts[kind] += 1
        else:
            unchanged += 1
        out.append(person)
    for key in sorted(previous_by_key.keys() - {person.person_key for person in new_rows}):
        previous = previous_by_key[key]
        gone = replace(
            previous, active=0, inactive_reason=WITHDRAWN,
            fold_version=FOLD_VERSION, source_run_id=source_run_id,
        )
        if gone.changed_against(previous):
            history.append(HistoryEntry(previous, WITHDRAWN))
            counts[WITHDRAWN] += 1
        else:
            unchanged += 1
        out.append(gone)
    return FoldResult(
        rows=tuple(out),
        history=tuple(history),
        persons=sum(1 for person in out if person.active == 1),
        created=counts[CREATED],
        updated=counts[UPDATED],
        hidden=counts[HIDDEN],
        withdrawn=counts[WITHDRAWN],
        reactivated=counts[REACTIVATED],
        unchanged=unchanged,
        stale_rules=stale_rules,
        sets_split_by_birth_year=sets_split,
    )
```

`identity_sets_before_split` is the closure without the birth-year split; Task 2 already
defines it beside `identity_sets`, and it is called here only to count the sets that had to
be split.

- [ ] **Step 4: Run the whole fold suite until green**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_fold.py -q`
Expected: PASS, 38 tests.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/person/fold.py tests/test_se_company_person_fold.py
git commit -m "feat(dagster): person row, roles, data merge and fold lifecycle with history"
```

---

### Task 4: The batch layer (selection, paging, writes, counts)

**Files:**
- Create: `src/dagster_v3/defs/se_company/person/batch.py`
- Test: `tests/test_se_company_person_batch.py`

**Interfaces:**
- Consumes: Task 3's `fold.py` (`NormalizedRow`, `PersonRule`, `PublishedPerson`, `fold_company_persons`, `EXCLUDED_SOURCES`, `FOLDABLE_STATUS`), `person/tables.py`, `person/precedence.py::FIELD`, `se_company/common.py::normalized_se_company_ids`.
- Produces: the SQL text functions, `NORMALIZED_SELECT_COLUMNS`, `MAIN_SELECT_COLUMNS`, `RULE_SELECT_COLUMNS`, `normalized_row_from_row`, `main_row_from_row`, `rule_from_row`, `FoldCounts`, `fold_companies`, `fold_bucket`, `BUCKET_COUNT`, `PAGE_SIZE`, `FOLD_ID_BOUND_QUERY_SETTINGS`.

**Selection** (spec 5.6, made exact). A company in the page is folded when

- it has **no** main rows and at least one `ok` normalized row (nothing published yet), or
- it has main rows and its newest input is newer than `max(folded_at)`, where "newest input" is the newest of: its newest normalized row of any status (a row flipping `ok` -> `no_person` changes the published set as much as a new one), its newest rule version **of any `active` value** (a Reset writes a new version of the same rule row with `active = 0` and a newer `created_at`; filtering on `active = 1` here would make the reset invisible and the rule permanent), its newest own precedence row, and the **global** precedence export's stamp.

The global precedence stamp is one scalar read per `fold_companies` call, not per page: changing the dictionary and re-exporting therefore re-folds every company once, which is the point — the export decides every published spelling.

Rerunning a folded bucket then selects nothing, because the fold rewrites every row of every folded company with this run's `folded_at`, which is newer than all four inputs.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_se_company_person_batch.py
"""The batch around the pure person fold: selection, paging, history before main. A fake
client answers each SELECT by its SQL-text function name and records every statement."""

from datetime import UTC, datetime
from typing import Any

import pytest

from dagster_v3.defs.se_company.person import batch, tables
from dagster_v3.defs.se_company.person.fold import FOLD_VERSION, PublishedPerson

T0 = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
T1 = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)
T2 = datetime(2026, 9, 3, 8, 0, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 10, 9, 0, tzinfo=UTC)
A, B, F = "5560000001", "5560000002", "5560000003"


def normalized_row(company_id: str, source: str, slot: str, *, first="anna", middles=(),
                   last="svensson", parse_status="ok", birth_year=None, wikidata_id=None,
                   role_code="board_member", role_year=2024, role_from=None, role_to=None,
                   data="{}") -> tuple:
    """One row in NORMALIZED_SELECT_COLUMNS order, the shape current_normalized_sql returns."""
    display_first = " ".join(part.title() for part in (first, *middles))
    values = {
        "company_id": company_id, "source": source, "slot": slot,
        "normalized_id": f"{source}-{slot}".ljust(64, "0"), "parse_status": parse_status,
        "first_tokens": [first], "middle_tokens": list(middles), "last_tokens": [last],
        "display_first": display_first, "display_last": last.title(),
        "display_name": f"{display_first} {last.title()}", "birth_year": birth_year,
        "wikidata_id": wikidata_id, "role_code": role_code, "role_year": role_year,
        "role_from": role_from, "role_to": role_to, "data": data,
    }
    return tuple(values[column] for column in batch.NORMALIZED_SELECT_COLUMNS)


def person(company_id: str, key: str, **overrides) -> PublishedPerson:
    values: dict[str, Any] = dict(
        company_id=company_id, person_key=key, display_name="Anna Svensson",
        first_name="Anna", last_name="Svensson", birth_year=None, wikidata_id=None,
        sources=("bolagsverket",), slots=("s1",), normalized_ids=("bolagsverket-s1".ljust(64, "0"),),
        member_sources=("bolagsverket",), member_slots=("s1",), member_names=("Anna Svensson",),
        member_birth_years=(None,), member_wikidata_ids=("",), member_data=("{}",),
        role_codes=("board_member",), role_years=(2024,), role_sources=(("bolagsverket",),),
        current_roles=("board_member",), first_year=2024, last_year=2024,
        text_source="bolagsverket", data="{}", active=1, inactive_reason="",
        folded_at=T2, fold_version=FOLD_VERSION, source_run_id="run-0",
    )
    values.update(overrides)
    return PublishedPerson(**values)


def main_row(company_id: str, key: str, **overrides) -> tuple:
    """The read shape: MAIN_SELECT_COLUMNS is tables.MAIN_COLUMNS, so a PublishedPerson's own
    tuple is exactly what current_main_rows_sql returns."""
    row = person(company_id, key, **overrides)
    return row.as_tuple(row.folded_at)


class FakeClient:
    def __init__(self, answers: dict[str, list]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, Any, Any]] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params, settings))
        if sql.startswith("INSERT"):
            self.inserts.append((sql, list(params)))
            return []
        for name, rows in self.answers.items():
            if sql == getattr(batch, name)():
                return rows
        raise AssertionError(f"unexpected SQL: {sql[:90]}")


def empty_scan(**extra) -> dict[str, list]:
    answers = {
        "normalized_watermarks_sql": [], "main_watermarks_sql": [], "rule_watermarks_sql": [],
        "company_precedence_watermarks_sql": [], "global_precedence_watermark_sql": [(EPOCH,)],
        "current_normalized_sql": [], "current_main_rows_sql": [], "active_rules_sql": [],
        "company_precedence_sql": [],
    }
    answers.update(extra)
    return answers


def run(client, ids, *, changed_only=True, page_size=batch.PAGE_SIZE):
    return batch.fold_companies(
        client, ids, changed_only=changed_only, source_run_id="run-1",
        folded_at=FOLDED_AT, page_size=page_size,
    )


def inserted(client, sql_name: str) -> list[dict]:
    columns = tables.MAIN_COLUMNS if sql_name == "main_insert_sql" else tables.HISTORY_COLUMNS
    statement = getattr(batch, sql_name)()
    return [
        dict(zip(columns, values, strict=True))
        for sql, rows in client.inserts if sql == statement for values in rows
    ]


def test_sql_texts_bind_ids_read_final_and_filter_drafts_and_non_ok_rows() -> None:
    current = batch.current_normalized_sql()
    assert "%(company_ids)s" in current and "FINAL" in current
    assert "source != 'reviewer_draft'" in current
    assert "parse_status = 'ok'" in current
    assert "ORDER BY company_id, source, slot" in current
    watermarks = batch.normalized_watermarks_sql()
    assert watermarks.count("FINAL") == 1 and "countIf(parse_status = 'ok')" in watermarks
    assert "source != 'reviewer_draft'" in watermarks
    rules = batch.active_rules_sql()
    assert "FINAL" in rules and "active = 1" in rules
    # The rule WATERMARK must not filter active: a Reset writes active = 0 with a newer
    # created_at, and a company that cannot see it would keep the rule forever.
    assert "active" not in batch.rule_watermarks_sql()
    assert "field = 'name'" in batch.company_precedence_sql() and "removed = 0" in batch.company_precedence_sql()
    assert "company_id = ''" in batch.global_precedence_watermark_sql()
    assert batch.main_insert_sql().startswith(f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} (")
    assert batch.history_insert_sql().startswith(f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} (")
    assert f"modulo(cityHash64(company_id), {batch.BUCKET_COUNT})" in batch.bucket_company_ids_sql()
    assert batch.MAIN_SELECT_COLUMNS == tables.MAIN_COLUMNS


def test_a_first_fold_writes_history_then_main() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T1, 2)],
        current_normalized_sql=[normalized_row(A, "bolagsverket", "s1"), normalized_row(A, "esef", "e1")],
    ))
    counts = run(client, [A])
    assert (counts.companies, counts.considered, counts.pages) == (1, 1, 1)
    assert (counts.persons, counts.created, counts.unchanged) == (1, 1, 0)
    assert [sql.split(" (")[0] for sql, _ in client.inserts] == [
        f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE}",
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE}",
    ]
    [row] = inserted(client, "main_insert_sql")
    assert row["sources"] == ["bolagsverket", "esef"] and row["folded_at"] == FOLDED_AT
    assert row["fold_version"] == FOLD_VERSION and row["source_run_id"] == "run-1"
    assert row["role_sources"] == [["bolagsverket", "esef"]]
    [history] = inserted(client, "history_insert_sql")
    assert (history["change_kind"], history["fold_run_id"], history["changed_at"]) == (
        "created", "run-1", FOLDED_AT
    )


def test_an_unchanged_company_rewrites_main_without_history() -> None:
    """The rewrite is what advances max(folded_at) so the selection converges: a company
    selected by a rule or a precedence export that changed nothing must still stamp its
    rows, or it would be selected again on every later run."""
    from dagster_v3.defs.se_company.person.fold import person_key

    stored = person(A, person_key(A, ("anna", "svensson")))
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T1, 1)], main_watermarks_sql=[(A, T0)],
        current_normalized_sql=[normalized_row(A, "bolagsverket", "s1")],
        current_main_rows_sql=[stored.as_tuple(T0)],
    ))
    counts = run(client, [A])
    assert (counts.created, counts.updated, counts.unchanged) == (0, 0, 1)
    assert [sql.split(" (")[0] for sql, _ in client.inserts] == [
        f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE}"
    ]
    [row] = inserted(client, "main_insert_sql")
    assert row["person_key"] == stored.person_key and row["folded_at"] == FOLDED_AT


def test_a_key_the_new_fold_no_longer_produces_is_withdrawn() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T2, 1)], main_watermarks_sql=[(A, T0)],
        current_normalized_sql=[normalized_row(A, "bolagsverket", "s1", first="hakan", last="oberg")],
        current_main_rows_sql=[person(A, "k" * 64).as_tuple(T0)],
    ))
    counts = run(client, [A])
    rows = {row["person_key"]: row for row in inserted(client, "main_insert_sql")}
    assert len(rows) == 2 and counts.created == 1 and counts.withdrawn == 1
    assert (rows["k" * 64]["active"], rows["k" * 64]["inactive_reason"]) == (0, "withdrawn")
    assert rows["k" * 64]["sources"] == ["bolagsverket"]        # its blocks are kept
    history = {entry["change_kind"] for entry in inserted(client, "history_insert_sql")}
    assert history == {"created", "withdrawn"}


def test_changed_only_selection_rules() -> None:
    client = FakeClient(empty_scan(
        # A: folded after its newest input -> skipped. B: a rule is newer -> folded.
        # F: no main row and an ok row -> folded.
        normalized_watermarks_sql=[(A, T0, 1), (B, T0, 1), (F, T1, 1)],
        main_watermarks_sql=[(A, T1), (B, T1)],
        rule_watermarks_sql=[(B, T2)],
        current_normalized_sql=[normalized_row(B, "bolagsverket", "s1"), normalized_row(F, "esef", "e1")],
    ))
    counts = run(client, [A, B, F])
    assert (counts.companies, counts.considered) == (3, 2)
    scoped = [params["company_ids"] for sql, params, _ in client.calls if sql == batch.current_normalized_sql()]
    assert scoped == [[B, F]]


def test_a_newer_precedence_export_selects_every_folded_company() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T0, 1)], main_watermarks_sql=[(A, T1)],
        global_precedence_watermark_sql=[(T2,)],
        current_normalized_sql=[normalized_row(A, "bolagsverket", "s1")],
    ))
    assert run(client, [A]).considered == 1


def test_a_company_precedence_row_selects_only_that_company() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T0, 1), (B, T0, 1)], main_watermarks_sql=[(A, T1), (B, T1)],
        company_precedence_watermarks_sql=[(B, T2)],
        current_normalized_sql=[normalized_row(B, "bolagsverket", "s1")],
        company_precedence_sql=[(B, "esef", 5000)],
    ))
    assert run(client, [A, B]).considered == 1


def test_a_company_with_only_partial_rows_and_no_main_row_is_never_selected() -> None:
    client = FakeClient(empty_scan(normalized_watermarks_sql=[(A, T1, 0)]))
    counts = run(client, [A])
    assert counts.considered == 0 and client.inserts == []


def test_rules_and_a_company_precedence_row_reach_the_fold() -> None:
    client = FakeClient(empty_scan(
        normalized_watermarks_sql=[(A, T1, 2)], rule_watermarks_sql=[(A, T1)],
        current_normalized_sql=[
            normalized_row(A, "bolagsverket", "s1"),
            normalized_row(A, "esef", "e1", first="hakan", last="oberg"),
        ],
        active_rules_sql=[(A, "r" * 64, "split", [], ["e1"])],
        company_precedence_sql=[(A, "esef", 5000)],
    ))
    counts = run(client, [A])
    rows = inserted(client, "main_insert_sql")
    assert len(rows) == 2 and counts.persons == 2 and counts.stale_rules == 0
    assert {row["text_source"] for row in rows} == {"bolagsverket", "esef"}


def test_every_id_bound_read_passes_the_query_settings_and_pages() -> None:
    client = FakeClient(empty_scan())
    run(client, [A, B, F], page_size=2)
    bound = [(params, settings) for sql, params, settings in client.calls if "%(company_ids)s" in sql]
    assert bound and all(settings == batch.FOLD_ID_BOUND_QUERY_SETTINGS for _, settings in bound)
    assert [params["company_ids"] for params, _ in bound][:4] == [[A, B]] * 4
    global_calls = [c for c in client.calls if c[0] == batch.global_precedence_watermark_sql()]
    assert len(global_calls) == 1               # one scalar per call, not per page


def test_a_full_page_renders_under_the_query_size_setting() -> None:
    """Rendered the way tests/test_se_company_address_batch.py renders it: the driver's
    escape_params against a SimpleNamespace context, with 12-digit ids (the wider of the two
    widths normalized_se_company_ids admits). No server needed."""
    from types import SimpleNamespace

    from clickhouse_driver.util.escape import escape_params

    DEFAULT_MAX_QUERY_SIZE = 262_144
    context = SimpleNamespace(
        server_info=SimpleNamespace(get_timezone=lambda: "UTC"),
        client_settings={"server_side_params": False},
    )
    ids = [str(556000000000 + index) for index in range(batch.PAGE_SIZE)]
    sizes = []
    for text in (batch.current_normalized_sql(), batch.current_main_rows_sql(),
                 batch.normalized_watermarks_sql(), batch.active_rules_sql(),
                 batch.company_precedence_sql()):
        rendered = text % escape_params({"company_ids": ids}, context)
        sizes.append(len(rendered.encode()))
        assert sizes[-1] < batch.FOLD_ID_BOUND_QUERY_SETTINGS["max_query_size"]
    assert max(sizes) > DEFAULT_MAX_QUERY_SIZE   # the raised setting is not decoration


def test_fold_bucket_reads_the_bucket_ids_then_folds_them() -> None:
    client = FakeClient(empty_scan(bucket_company_ids_sql=[(A,)]))
    counts = batch.fold_bucket(
        client, 7, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT
    )
    assert counts.companies == 1
    assert client.calls[0][1] == {"bucket": 7}
    assert client.calls[0][2] == batch.FOLD_ID_BOUND_QUERY_SETTINGS
    with pytest.raises(ValueError):
        batch.fold_bucket(client, 64, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)


def test_fold_counts_as_metadata_names_every_counter() -> None:
    counts = batch.FoldCounts(
        companies=1, considered=2, pages=3, persons=4, created=5, updated=6, hidden=7,
        withdrawn=8, reactivated=9, unchanged=10, stale_rules=11, sets_split_by_birth_year=12,
    )
    assert set(counts.as_metadata()) == {
        "companies", "considered", "pages", "persons", "created", "updated", "hidden",
        "withdrawn", "reactivated", "unchanged", "stale_rules", "sets_split_by_birth_year",
        "fold_version",
    }
    assert counts.as_metadata()["fold_version"] == FOLD_VERSION


def test_invalid_company_ids_are_refused_before_any_query() -> None:
    client = FakeClient({})
    with pytest.raises(ValueError):
        run(client, ["12"])
    assert client.calls == []
```

- [ ] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_batch.py -q`
Expected: FAIL with `ModuleNotFoundError: ...person.batch`.

- [ ] **Step 3: Write the module**

```python
# src/dagster_v3/defs/se_company/person/batch.py
"""Read normalized rows, fold in memory, write history then main (spec 2026-09-09 section
5.6).

Every SELECT is a function returning its exact text, so the clickhouse-local harness runs the
same SQL the asset runs. Parameters bind client-side through clickhouse-driver's %(name)s
syntax. Unlike the address fold there is no geocoder here: no DuckDB, no pool, no cache --
the page is four reads, a pure fold and two inserts.
"""

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.fold import (
    EXCLUDED_SOURCES,
    FOLD_VERSION,
    FOLDABLE_STATUS,
    NormalizedRow,
    PersonRule,
    PublishedPerson,
    fold_company_persons,
)
from dagster_v3.defs.se_company.person.precedence import FIELD as PRECEDENCE_FIELD

BUCKET_COUNT = 64
PAGE_SIZE = 20_000

# clickhouse-driver renders %(company_ids)s into the statement text; a 20,000-id page is
# about 300 KB, past ClickHouse's 262,144-byte default max_query_size. Each statement here
# binds the id list once, so 1 MiB is >3x the measured worst case (guard test in
# tests/test_se_company_person_batch.py). max_execution_time makes a pathological page fail
# visibly instead of holding a connection forever.
FOLD_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 1_048_576, "max_execution_time": 1800}

NORMALIZED_SELECT_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "normalized_id", "parse_status",
    "first_tokens", "middle_tokens", "last_tokens",
    "display_first", "display_last", "display_name",
    "birth_year", "wikidata_id", "role_code", "role_year", "role_from", "role_to", "data",
)
# The main read takes every column: the fold compares 26 of them, and a history row is the
# PREVIOUS image, which needs that row's own folded_at, fold_version and source_run_id too.
MAIN_SELECT_COLUMNS: tuple[str, ...] = tables.MAIN_COLUMNS
RULE_SELECT_COLUMNS: tuple[str, ...] = ("company_id", "rule_id", "kind", "person_keys", "slots")
_EXCLUDED_SQL = " AND ".join(f"source != '{source}'" for source in EXCLUDED_SOURCES)


@dataclass(frozen=True, slots=True)
class FoldCounts:
    companies: int          # ids handed in
    considered: int         # ids the selection picked (== companies when changed_only=False)
    pages: int
    persons: int            # active rows written
    created: int            # the five change kinds are history rows, not row states
    updated: int
    hidden: int
    withdrawn: int
    reactivated: int
    unchanged: int          # rows rewritten with no history row
    stale_rules: int        # active rules with a key or slot that resolved to nothing
    sets_split_by_birth_year: int

    def as_metadata(self) -> dict[str, Any]:
        return {
            "companies": self.companies, "considered": self.considered, "pages": self.pages,
            "persons": self.persons, "created": self.created, "updated": self.updated,
            "hidden": self.hidden, "withdrawn": self.withdrawn,
            "reactivated": self.reactivated, "unchanged": self.unchanged,
            "stale_rules": self.stale_rules,
            "sets_split_by_birth_year": self.sets_split_by_birth_year,
            "fold_version": FOLD_VERSION,
        }


def bucket_company_ids_sql() -> str:
    """The same hash and modulus as the address fold -- never a second hash function."""
    return (
        "SELECT DISTINCT company_id\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE}\n"
        f"WHERE modulo(cityHash64(company_id), {BUCKET_COUNT}) = %(bucket)s\n"
        "ORDER BY company_id"
    )


def normalized_watermarks_sql() -> str:
    """Newest normalized version per company and how many of its current rows fold. FINAL,
    so a row whose current version is no_person does not count as foldable through an older
    ok version -- and the max is over EVERY status, because a row leaving `ok` changes the
    published set as much as a new one arriving."""
    return (
        "SELECT company_id, max(normalized_at) AS normalized_at, "
        f"countIf(parse_status = '{FOLDABLE_STATUS}') AS foldable\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        f"WHERE company_id IN %(company_ids)s AND {_EXCLUDED_SQL}\n"
        "GROUP BY company_id"
    )


def main_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(folded_at) AS folded_at\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def rule_watermarks_sql() -> str:
    """Every rule version, whatever its `active` value: a Reset is a NEW version of the same
    rule row with active = 0 and a newer created_at, and a company that could not see it
    would apply the rule for ever. No FINAL: max() over the versions is the newest anyway."""
    return (
        "SELECT company_id, max(created_at) AS created_at\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def company_precedence_watermarks_sql() -> str:
    return (
        "SELECT company_id, max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def global_precedence_watermark_sql() -> str:
    """One scalar for the whole run: the newest export of the global order. A dictionary
    change therefore re-folds every company once, which is exactly what it should do -- the
    order decides every published spelling. An empty table answers with the epoch."""
    return (
        "SELECT max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE}\n"
        "WHERE company_id = ''"
    )


def current_normalized_sql() -> str:
    return (
        f"SELECT {', '.join(NORMALIZED_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        f"WHERE company_id IN %(company_ids)s AND {_EXCLUDED_SQL} "
        f"AND parse_status = '{FOLDABLE_STATUS}'\n"
        "ORDER BY company_id, source, slot"
    )


def current_main_rows_sql() -> str:
    return (
        f"SELECT {', '.join(MAIN_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def active_rules_sql() -> str:
    return (
        f"SELECT {', '.join(RULE_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND active = 1\n"
        "ORDER BY company_id, rule_id"
    )


def company_precedence_sql() -> str:
    return (
        "SELECT company_id, source, precedence\n"
        f"FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL\n"
        f"WHERE company_id IN %(company_ids)s AND field = '{PRECEDENCE_FIELD}' AND removed = 0"
    )


def main_insert_sql() -> str:
    return f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} ({', '.join(tables.MAIN_COLUMNS)}) VALUES"


def history_insert_sql() -> str:
    return (
        f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} "
        f"({', '.join(tables.HISTORY_COLUMNS)}) VALUES"
    )


def normalized_row_from_row(row: Sequence[Any]) -> NormalizedRow:
    values = dict(zip(NORMALIZED_SELECT_COLUMNS, row, strict=True))
    for name in ("first_tokens", "middle_tokens", "last_tokens"):
        values[name] = tuple(values[name])
    return NormalizedRow(**values)


def main_row_from_row(row: Sequence[Any]) -> PublishedPerson:
    values = dict(zip(MAIN_SELECT_COLUMNS, row, strict=True))
    for name in ("sources", "slots", "normalized_ids", *tables.MEMBER_COLUMNS,
                 "role_codes", "role_years", "current_roles"):
        values[name] = tuple(values[name])
    values["role_sources"] = tuple(tuple(sources) for sources in values["role_sources"])
    return PublishedPerson(**values)


def rule_from_row(row: Sequence[Any]) -> PersonRule:
    company_id, rule_id, kind, person_keys, slots = row
    return PersonRule(company_id, str(rule_id), kind, tuple(person_keys), tuple(slots))


def _pages(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _changed_company_ids(
    client: Any, company_ids: list[str], *, global_precedence_at: datetime | None
) -> list[str]:
    params = {"company_ids": company_ids}

    def read(sql: str) -> list:
        return client.execute(sql, params, settings=FOLD_ID_BOUND_QUERY_SETTINGS)

    normalized = {row[0]: (row[1], int(row[2])) for row in read(normalized_watermarks_sql())}
    folded = dict(read(main_watermarks_sql()))
    ruled = dict(read(rule_watermarks_sql()))
    decided = dict(read(company_precedence_watermarks_sql()))
    changed: list[str] = []
    for company_id in company_ids:
        if company_id not in normalized:
            continue
        newest, foldable = normalized[company_id]
        for stamp in (ruled.get(company_id), decided.get(company_id), global_precedence_at):
            if stamp is not None and stamp > newest:
                newest = stamp
        if company_id not in folded:
            if foldable > 0:
                changed.append(company_id)
        elif newest > folded[company_id]:
            changed.append(company_id)
    return changed


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
    """Fold the given companies in pages of `page_size`.

    Every folded company's whole set is rewritten with this `folded_at` -- active, hidden and
    withdrawn rows alike -- so the changed_only selection converges; history rows are written
    only where a compared column changed, and always BEFORE the main insert."""
    ids = list(normalized_se_company_ids(company_ids))
    current_year = (folded_at.astimezone(UTC) if folded_at.tzinfo else folded_at).year
    global_precedence_at = None
    if changed_only and ids:
        rows = client.execute(global_precedence_watermark_sql())
        global_precedence_at = rows[0][0] if rows else None
    considered = pages = persons = unchanged = stale_rules = sets_split = 0
    kinds = {"created": 0, "updated": 0, "hidden": 0, "withdrawn": 0, "reactivated": 0}
    for page in _pages(ids, page_size):
        pages += 1
        scope = (
            _changed_company_ids(client, page, global_precedence_at=global_precedence_at)
            if changed_only
            else page
        )
        considered += len(scope)
        if not scope:
            continue
        params = {"company_ids": scope}

        def read(sql: str) -> list:
            return client.execute(sql, params, settings=FOLD_ID_BOUND_QUERY_SETTINGS)

        by_company: dict[str, list[NormalizedRow]] = defaultdict(list)
        for row in read(current_normalized_sql()):
            normalized = normalized_row_from_row(row)
            by_company[normalized.company_id].append(normalized)
        current: dict[str, list[PublishedPerson]] = defaultdict(list)
        for row in read(current_main_rows_sql()):
            published = main_row_from_row(row)
            current[published.company_id].append(published)
        rules: dict[str, list[PersonRule]] = defaultdict(list)
        for row in read(active_rules_sql()):
            rule = rule_from_row(row)
            rules[rule.company_id].append(rule)
        precedence: dict[str, dict[str, int]] = defaultdict(dict)
        for company_id, source, number in read(company_precedence_sql()):
            precedence[company_id][source] = int(number)

        main_rows: list[tuple[Any, ...]] = []
        history_rows: list[tuple[Any, ...]] = []
        page_persons = 0
        for company_id in scope:
            rows = by_company.get(company_id, [])
            previous = current.get(company_id, [])
            if not rows and not previous:
                continue
            result = fold_company_persons(
                company_id, rows, previous, rules.get(company_id, []),
                precedence.get(company_id), source_run_id=source_run_id,
                current_year=current_year,
            )
            page_persons += result.persons
            unchanged += result.unchanged
            stale_rules += result.stale_rules
            sets_split += result.sets_split_by_birth_year
            for kind in kinds:
                kinds[kind] += getattr(result, kind)
            main_rows.extend(row.as_tuple(folded_at) for row in result.rows)
            history_rows.extend(
                entry.row.history_tuple(
                    changed_at=folded_at, change_kind=entry.change_kind, fold_run_id=source_run_id
                )
                for entry in result.history
            )
        persons += page_persons
        if history_rows:
            # History first: a crash between the two statements costs at most a page's
            # duplicate history rows on retry -- each attempt with its own changed_at, so the
            # plain MergeTree keeps both -- which is cheaper than the other order's failure
            # mode, a published row whose first history row is missing.
            client.execute(history_insert_sql(), history_rows)
        if main_rows:
            client.execute(main_insert_sql(), main_rows)
        if log is not None:
            log(
                "Folded person page %d: companies=%d considered=%d persons=%d rows=%d history=%d",
                pages, len(page), len(scope), page_persons, len(main_rows), len(history_rows),
            )
    return FoldCounts(
        companies=len(ids), considered=considered, pages=pages, persons=persons,
        created=kinds["created"], updated=kinds["updated"], hidden=kinds["hidden"],
        withdrawn=kinds["withdrawn"], reactivated=kinds["reactivated"], unchanged=unchanged,
        stale_rules=stale_rules, sets_split_by_birth_year=sets_split,
    )


def fold_bucket(
    client: Any,
    bucket: int,
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    page_size: int = PAGE_SIZE,
    log: Callable[..., object] | None = None,
) -> FoldCounts:
    """Fold every company whose id hashes into `bucket` (0..63)."""
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"bucket out of range: {bucket}")
    company_ids = [
        row[0]
        for row in client.execute(
            bucket_company_ids_sql(), {"bucket": bucket}, settings=FOLD_ID_BOUND_QUERY_SETTINGS
        )
    ]
    return fold_companies(
        client, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
```

- [ ] **Step 4: Run the tests until green**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_batch.py tests/test_se_company_person_fold.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/person/batch.py tests/test_se_company_person_batch.py
git commit -m "feat(dagster): person fold batch with selection, paging and history-first writes"
```

---

### Task 5: The fold assets, the clickhouse-local proof and the design doc

**Files:**
- Modify: `src/dagster_v3/defs/se_company/person/assets.py`
- Modify: `src/dagster_v3/defs/se_company/person/docs/person-design.md`
- Test: `tests/test_se_company_person_assets.py`, `tests/test_se_company_person_fold_clickhouse_local.py`

**Interfaces:**
- Consumes: Task 4's `batch.py`; `person/normalize.py::normalize_companies` and `NormalizeCounts`; `assert_clickhouse_tables_exist`; `normalized_se_company_ids`.
- Produces: `PERSON_FOLD_PARTITIONS`, `person_bucket_index`, `PersonFoldConfig`, `PersonFoldCompaniesConfig`, `targeted_fold`, assets `se_company_person_fold` and `se_company_person_fold_companies`.

- [ ] **Step 1: Write the failing asset tests**

```python
# tests/test_se_company_person_assets.py
"""Wiring of the person fold assets: 64 bucket partitions, NO pool (this fold opens no
DuckDB, so buckets run in parallel), config bounds, and the targeted fold's order."""

from types import SimpleNamespace

import dagster as dg
import pytest

from dagster_v3.defs.se_company.person import assets, batch


def test_the_fold_has_sixty_four_bucket_partitions_and_no_pool() -> None:
    keys = assets.PERSON_FOLD_PARTITIONS.get_partition_keys()
    assert keys[0] == "bucket_00" and keys[-1] == "bucket_63" and len(keys) == batch.BUCKET_COUNT
    fold = assets.se_company_person_fold
    assert fold.partitions_def is assets.PERSON_FOLD_PARTITIONS
    assert fold.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    for asset in (fold, assets.se_company_person_fold_companies):
        assert asset.op.pool is None
        assert set(asset.required_resource_keys) >= {"clickhouse"}
        assert asset.group_names_by_key[asset.key] == assets.GROUP_NAME


def test_the_normalize_asset_keeps_its_own_pool() -> None:
    """The fold has no pool; the normalize asset's stays as slice 0 shipped it."""
    assert assets.se_company_person_normalize.op.pool == assets.NORMALIZE_POOL


def test_bucket_index_parses_and_refuses() -> None:
    assert assets.person_bucket_index("bucket_07") == 7
    with pytest.raises(ValueError):
        assets.person_bucket_index("bucket_64")
    with pytest.raises(ValueError):
        assets.person_bucket_index("07")


def test_config_defaults_and_bounds() -> None:
    assert assets.PersonFoldConfig().changed_only is True
    assert assets.PersonFoldConfig().page_size == batch.PAGE_SIZE
    with pytest.raises(ValueError):
        assets.PersonFoldConfig(page_size=0)
    targeted = assets.PersonFoldCompaniesConfig(company_ids=["5560000002", "5560000001", "5560000001"])
    assert targeted.company_ids == ["5560000001", "5560000002"] and targeted.changed_only is False
    with pytest.raises(ValueError):
        assets.PersonFoldCompaniesConfig(company_ids=[])


def test_targeted_fold_normalizes_the_ids_before_folding_them(monkeypatch) -> None:
    """Spec section 7: Fold now must parse a reviewer's brand-new suggestion, so the
    targeted asset normalizes first -- always changed_only, so it touches only rows that
    need it -- and then folds the same ids with the caller's changed_only."""
    from datetime import UTC, datetime

    calls: list[tuple] = []

    def fake_normalize(client, ids, *, changed_only, normalized_at, page_size, log):
        calls.append(("normalize", list(ids), changed_only))
        return SimpleNamespace(as_metadata=lambda: {"rows": 3})

    def fake_fold(client, ids, *, changed_only, source_run_id, folded_at, page_size, log):
        calls.append(("fold", list(ids), changed_only, source_run_id))
        return SimpleNamespace(as_metadata=lambda: {"persons": 2})

    monkeypatch.setattr(assets, "normalize_companies", fake_normalize)
    monkeypatch.setattr(assets, "fold_companies", fake_fold)
    now = datetime(2026, 9, 10, 20, 0, tzinfo=UTC)
    normalized, folded = assets.targeted_fold(
        object(), ["5560000001"], changed_only=False, source_run_id="run-1", folded_at=now,
        page_size=20_000, log=None, logger=None,
    )
    assert calls == [("normalize", ["5560000001"], True), ("fold", ["5560000001"], False, "run-1")]
    assert normalized.as_metadata() == {"rows": 3} and folded.as_metadata() == {"persons": 2}
```

- [ ] **Step 2: Add the fold assets**

In `person/assets.py`, add to the imports:

```python
import re
from collections.abc import Callable, Sequence

from dagster_v3.defs.se_company.person.batch import (
    BUCKET_COUNT,
    PAGE_SIZE as FOLD_PAGE_SIZE,
    FoldCounts,
    fold_bucket,
    fold_companies,
)
from dagster_v3.defs.se_company.person.normalize import (
    PAGE_SIZE,
    NormalizeCounts,
    normalize_all,
    normalize_companies,
)
```

(That `normalize` import REPLACES the existing `from ...normalize import PAGE_SIZE, normalize_all, normalize_companies` line — one import of the module, extended with `NormalizeCounts`; the batch page size is aliased as `FOLD_PAGE_SIZE`.) Then append:

```python
PERSON_FOLD_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket:02d}" for bucket in range(BUCKET_COUNT)]
)
_FOLD_TABLES = (
    tables.NORMALIZED_TABLE, tables.MAIN_TABLE, tables.HISTORY_TABLE,
    tables.RULE_TABLE, tables.PRECEDENCE_TABLE,
)


def person_bucket_index(partition_key: str) -> int:
    match = re.fullmatch(r"bucket_(\d{2})", partition_key)
    if match is None:
        raise ValueError(f"invalid person fold partition key: {partition_key!r}")
    bucket = int(match.group(1))
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"person fold bucket out of range: {bucket}")
    return bucket


class PersonFoldConfig(dg.Config):
    # True: only companies whose newest normalized row, rule version or precedence row (their
    # own, or the global export) is newer than their last fold, plus companies never folded
    # that have a foldable row. False re-folds the whole bucket; history rows are written
    # either way only where a compared column changed.
    changed_only: bool = True
    # Companies per page. Lower it if a page's rows press the host's memory.
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)


class PersonFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


def _fold_metadata(counts: FoldCounts, config: dg.Config, **extra) -> dict:
    return {
        **counts.as_metadata(),
        "changed_only": config.changed_only,
        "page_size": config.page_size,
        "table": tables.QUALIFIED_MAIN_TABLE,
        "history_table": tables.QUALIFIED_HISTORY_TABLE,
        **extra,
    }


def targeted_fold(
    client: Any,
    company_ids: Sequence[str],
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    page_size: int,
    log: Callable[..., object] | None,
    logger: Any = None,
) -> tuple[NormalizeCounts, FoldCounts]:
    """The targeted fold normalizes the companies' raw rows first (spec section 7: a
    reviewer's brand-new suggestion has to parse before Fold now can publish it), always
    changed_only so only rows never normalized, on another suggestion_id or on an older
    normalizer version are touched; then folds the same ids with the caller's changed_only.
    `log` (a plain callable) goes to fold_companies, which calls it directly; `logger` (an
    object with `.info`) goes to normalize_companies, which calls `log.info(...)` -- the two
    want different shapes, so the caller passes both."""
    normalized = normalize_companies(
        client, company_ids, changed_only=True, normalized_at=folded_at,
        page_size=page_size, log=logger,
    )
    folded = fold_companies(
        client, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
    return normalized, folded


@dg.asset(
    name="se_company_person_fold",
    partitions_def=PERSON_FOLD_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={
        "table": tables.QUALIFIED_MAIN_TABLE,
        "history_table": tables.QUALIFIED_HISTORY_TABLE,
    },
    description=(
        "Folds the current normalized person rows of the companies in one of 64 hash buckets "
        "into se_company_person_v2: observations of one person merge into one row with every "
        "source, reviewer rules hide, merge and split, previously published keys without a "
        "set are withdrawn, and every change is appended to se_company_person_history first. "
        "No pool: this fold opens no DuckDB, so buckets run in parallel. Manual: launch a "
        "partition or a backfill from the UI."
    ),
)
def se_company_person_fold(
    context: dg.AssetExecutionContext, config: PersonFoldConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    bucket = person_bucket_index(context.partition_key)
    with clickhouse.get_connection() as client:
        counts = fold_bucket(
            client, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config, bucket=bucket))


@dg.asset(
    name="se_company_person_fold_companies",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={
        "table": tables.QUALIFIED_MAIN_TABLE,
        "history_table": tables.QUALIFIED_HISTORY_TABLE,
    },
    description=(
        "The targeted person fold: the companies named in config.company_ids, whatever their "
        "bucket. The backoffice's Fold now button launches this asset for one company. "
        "Normalizes the companies' raw rows first, so a reviewer's draft parses on Fold now."
    ),
)
def se_company_person_fold_companies(
    context: dg.AssetExecutionContext,
    config: PersonFoldCompaniesConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE,
        tables=(tables.SUGGESTION_TABLE, *_FOLD_TABLES),
    )
    folded_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        normalized, counts = targeted_fold(
            client, config.company_ids, changed_only=config.changed_only,
            source_run_id=context.run_id, folded_at=folded_at, page_size=config.page_size,
            log=context.log.info, logger=context.log,
        )
    return dg.MaterializeResult(
        metadata={
            **_fold_metadata(counts, config),
            **{f"normalize_{key}": value for key, value in normalized.as_metadata().items()},
        }
    )
```

Update the module docstring: slice 2 ships the fold, the targeted fold and the precedence export.

- [ ] **Step 3: Write the clickhouse-local proof**

```python
# tests/test_se_company_person_fold_clickhouse_local.py
"""The person fold end to end against a real ClickHouse (clickhouse-local).

Claims a fake client cannot settle (spec 2026-09-09 section 5, R9 of the slice-2 brief):

1. Migration 000396's six tables accept every shape this slice writes: raw suggestions,
   normalized rows built by normalize.normalized_row, a 29-value main tuple from
   PublishedPerson.as_tuple (arrays, Array(Array(String)), Array(Nullable(UInt16)),
   FixedString(64), Nullable(UInt16)), a 32-value history tuple, a rule row and the five
   precedence rows the export writes.
2. A company folds end to end through the REAL batch SQL -- selection, the four page reads
   under FINAL, history then main -- and the main row reads back as the fold built it.
3. Re-running the same fold selects NOTHING: the rewrite advanced max(folded_at) past every
   input watermark.
4. A hide rule deactivates its person; a merge rule joins two persons and withdraws the key
   it absorbed; a normalized row whose current version turns into a tombstone (no_person)
   withdraws its person.

`_LocalClient` is a clickhouse-driver-shaped client over `clickhouse-local`: the process is
stateless, so the session keeps every statement it has run and replays the whole script for
each SELECT. That costs one process per read (about ten per fold round) and keeps the test
honest -- batch.py runs unmodified, with its own SQL.

Both `join_use_nulls` settings run: none of these statements joins, so the parametrization is
a guard against a future one, not a live risk.
"""

import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.person import batch, tables
from dagster_v3.defs.se_company.person.assets import export_precedence
from dagster_v3.defs.se_company.person.fold import person_key
from dagster_v3.defs.se_company.person.normalize import RAW_ROW_COLUMNS, normalized_row
from tests.clickhouse_local import clickhouse_local_command

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION_FILE = "000396_corpscout_se_company_person_entity.up.sql"

COMPANY = "5561552760"          # two persons: Anna Svensson (2 members) and Håkan Öberg
HIDE_CO = "5560000003"          # one person, hidden by a rule in round 3
GONE_CO = "5560000004"          # one person, tombstoned in round 3
SUGGESTED_AT = datetime(2026, 9, 9, 12, 0, tzinfo=UTC)
NORMALIZED_AT = datetime(2026, 9, 9, 13, 0, tzinfo=UTC)
TOMBSTONED_AT = datetime(2026, 9, 10, 10, 0, tzinfo=UTC)   # after FIRST_FOLD_AT, so round 3 selects GONE_CO
EXPORTED_AT = datetime(2026, 9, 10, 8, 0, tzinfo=UTC)
RULE_AT = datetime(2026, 9, 10, 10, 30, tzinfo=UTC)
FIRST_FOLD_AT = datetime(2026, 9, 10, 9, 0, tzinfo=UTC)
SECOND_FOLD_AT = datetime(2026, 9, 10, 11, 0, tzinfo=UTC)

# (company_id, source, slot, suggestion_id, full_name, first_name, last_name, birth_year,
#  wikidata_id, role_original, role_key, fiscal_year, role_from, role_to, data)
RAW = (
    (COMPANY, "bolagsverket", "rec1:sig1", "a" * 64, None, "Anna", "Svensson", None, None,
     "Styrelseledamot", "board_member", 2024, None, None, '{"signatory_kind":"board"}'),
    (COMPANY, "esef", "doc7:2", "b" * 64, "Anna Maria Svensson", None, None, None, None,
     "VD", "chief_executive", 2023, None, None, '{"organization":"ACME"}'),
    (COMPANY, "wikidata", "Q1:P169:Q9", "c" * 64, "Håkan Öberg", None, None, 1962, "Q9",
     "chief executive officer", "P169", None, date(2020, 1, 1), None, '{"description":"CEO"}'),
    (HIDE_CO, "bolagsverket", "rec2:sig1", "d" * 64, None, "Erik", "Larsson", None, None,
     "Revisor", "auditor", 2022, None, None, "{}"),
    (GONE_CO, "bolagsverket", "rec3:sig1", "e" * 64, None, "Karin", "Nilsson", None, None,
     "Ordförande", "chairman", 2021, None, None, "{}"),
)
# GONE_CO's slot after the source stopped delivering it: every person column NULL, which the
# normalizer classifies no_person and the fold therefore never sees.
TOMBSTONE = (GONE_CO, "bolagsverket", "rec3:sig1", "f" * 64, None, None, None, None, None,
             None, None, None, None, None, "{}")

COMPANY_IDS = [COMPANY, HIDE_CO, GONE_CO]


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        return f"toDateTime64('{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', 3, 'UTC')"
    if isinstance(value, date):
        return f"toDate('{value.isoformat()}')"
    if isinstance(value, list):
        return "[" + ", ".join(_literal(item) for item in value) + "]"
    if isinstance(value, tuple):
        return "(" + ", ".join(_literal(item) for item in value) + ")"
    if isinstance(value, (int, float)):
        return repr(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _schema_statements() -> list[str]:
    """CREATE DATABASE plus the six CREATE TABLEs of 000396 -- never its SYSTEM STOP/START
    VIEW or ALTER TABLE ... MODIFY QUERY, which name se_companies_serving, a view this
    fixture does not build."""
    text = (MIGRATIONS_DIR / MIGRATION_FILE).read_text(encoding="utf-8")
    statements: list[str] = []
    for raw in text.split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement.upper().startswith("CREATE DATABASE") or (
            "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_" in statement
        ):
            statements.append(statement)
    return statements


_QUERY_COLUMNS: dict[str, tuple[str, ...]] = {
    batch.bucket_company_ids_sql(): ("company_id",),
    batch.normalized_watermarks_sql(): ("company_id", "normalized_at", "foldable"),
    batch.main_watermarks_sql(): ("company_id", "folded_at"),
    batch.rule_watermarks_sql(): ("company_id", "created_at"),
    batch.company_precedence_watermarks_sql(): ("company_id", "decided_at"),
    batch.global_precedence_watermark_sql(): ("decided_at",),
    batch.current_normalized_sql(): batch.NORMALIZED_SELECT_COLUMNS,
    batch.current_main_rows_sql(): batch.MAIN_SELECT_COLUMNS,
    batch.active_rules_sql(): batch.RULE_SELECT_COLUMNS,
    batch.company_precedence_sql(): ("company_id", "source", "precedence"),
}
_DATETIME_COLUMNS = frozenset({"normalized_at", "folded_at", "created_at", "decided_at"})
_DATE_COLUMNS = frozenset({"role_from", "role_to"})


def _value(column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in _DATETIME_COLUMNS:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)
    if column in _DATE_COLUMNS:
        return date.fromisoformat(value)
    return value


class _LocalClient:
    """A clickhouse-driver-shaped client over clickhouse-local. Writes are remembered and
    replayed before every read, because each invocation starts with an empty server."""

    def __init__(self, setting: int) -> None:
        self.statements: list[str] = [f"SET join_use_nulls = {setting}", *_schema_statements()]

    def add(self, statement: str) -> None:
        self.statements.append(statement)

    def _run(self, query: str) -> list[str]:
        script = ";\n".join([*self.statements, query]) + ";\n"
        try:
            completed = subprocess.run(
                clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
            )
        except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
            pytest.skip(f"clickhouse-local is unusable here: {exc}")
        assert completed.returncode == 0, completed.stderr or completed.stdout
        return [line for line in completed.stdout.splitlines() if line.strip()]

    def execute(self, sql, params=None, settings=None):
        if sql.startswith("INSERT"):
            values = ", ".join(_literal(tuple(row)) for row in params)
            self.add(f"{sql} {values}")
            return []
        # Anything not in the map is a one-off read (export_precedence's stale count):
        # returned raw, with no per-column conversion.
        columns = _QUERY_COLUMNS.get(sql)
        rendered = sql
        for name, value in (params or {}).items():
            rendered = rendered.replace(
                f"%({name})s", _literal(tuple(value) if isinstance(value, list) else value)
            )
        assert "%(" not in rendered, rendered
        lines = self._run(f"SELECT * FROM ({rendered}) AS q FORMAT JSONCompactEachRow")
        if columns is None:
            return [tuple(json.loads(line)) for line in lines]
        return [
            tuple(_value(column, item) for column, item in zip(columns, json.loads(line), strict=True))
            for line in lines
        ]

    def read(self, query: str) -> list[list[str]]:
        """A read the batch does not make: the test's own assertions."""
        return [line.split("\t") for line in self._run(query)]


def _raw_insert(rows, suggested_at: datetime) -> str:
    ordered = []
    for raw in rows:
        values = dict(zip(RAW_ROW_COLUMNS, raw, strict=True))
        values.update(suggested_at=suggested_at, source_record_id="", document_ref=None)
        ordered.append(tuple(values[column] for column in tables.SUGGESTION_COLUMNS))
    return (
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} "
        f"({', '.join(tables.SUGGESTION_COLUMNS)}) VALUES "
        + ", ".join(_literal(row) for row in ordered)
    )


def _normalized_insert(rows, normalized_at: datetime) -> str:
    values = ", ".join(_literal(tuple(normalized_row(raw, normalized_at))) for raw in rows)
    return (
        f"INSERT INTO {tables.QUALIFIED_NORMALIZED_TABLE} "
        f"({', '.join(tables.NORMALIZED_COLUMNS)}) VALUES {values}"
    )


def _rule_insert(company_id: str, rule_id: str, kind: str, person_keys, slots, active: int) -> str:
    row = (company_id, rule_id, kind, list(person_keys), list(slots), active, "", RULE_AT, "reviewer")
    return (
        f"INSERT INTO {tables.QUALIFIED_RULE_TABLE} "
        f"({', '.join(tables.RULE_COLUMNS)}) VALUES {_literal(row)}"
    )


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def folded(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Three rounds against one clickhouse-local session: the first fold, the re-run, and a
    fold under a hide rule, a merge rule and a tombstone."""
    client = _LocalClient(request.param)
    client.add(_raw_insert(RAW, SUGGESTED_AT))
    client.add(_normalized_insert(RAW, NORMALIZED_AT))
    export_precedence(client, EXPORTED_AT)          # the five global rows, through the real code

    first = batch.fold_companies(
        client, COMPANY_IDS, changed_only=True, source_run_id="run-1", folded_at=FIRST_FOLD_AT
    )
    rerun = batch.fold_companies(
        client, COMPANY_IDS, changed_only=True, source_run_id="run-2", folded_at=SECOND_FOLD_AT
    )
    anna = person_key(COMPANY, ("anna", "maria", "svensson"))
    hakan = person_key(COMPANY, ("hakan", "oberg"))
    erik = person_key(HIDE_CO, ("erik", "larsson"))
    client.add(_rule_insert(HIDE_CO, "1" * 64, "hide", [erik], [], 1))
    client.add(_rule_insert(COMPANY, "2" * 64, "merge", [anna, hakan], [], 1))
    client.add(_raw_insert((TOMBSTONE,), TOMBSTONED_AT))
    client.add(_normalized_insert((TOMBSTONE,), TOMBSTONED_AT))
    third = batch.fold_companies(
        client, COMPANY_IDS, changed_only=True, source_run_id="run-3", folded_at=SECOND_FOLD_AT
    )
    return {
        "client": client, "first": first, "rerun": rerun, "third": third,
        "keys": {"anna": anna, "hakan": hakan, "erik": erik},
    }


def test_the_first_fold_publishes_every_person_with_its_members_and_roles(folded) -> None:
    counts = folded["first"]
    assert (counts.companies, counts.considered) == (3, 3)
    assert (counts.persons, counts.created, counts.withdrawn) == (4, 4, 0)
    assert counts.stale_rules == 0 and counts.sets_split_by_birth_year == 0
    rows = folded["client"].read(
        f"SELECT person_key, display_name, text_source, arrayStringConcat(sources, ','), "
        f"arrayStringConcat(arrayMap(x -> toString(x), role_years), ','), "
        f"arrayStringConcat(current_roles, ','), active "
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{COMPANY}' "
        "ORDER BY display_name"
    )
    by_name = {row[1]: row for row in rows}
    anna = by_name["Anna Svensson"]                  # the spelling is bolagsverket's (900)
    assert anna[0] == folded["keys"]["anna"]
    assert anna[2] == "bolagsverket"                 # 900 beats esef 400 for the spelling
    assert anna[3] == "bolagsverket,esef"
    assert anna[4] == "2023,2024"
    assert anna[6] == "1"
    hakan = by_name["Håkan Öberg"]
    assert hakan[3] == "wikidata"
    # The Wikidata span 2020-01-01 with no end expands to every year up to the fold's year.
    assert hakan[4].split(",")[0] == "2020" and hakan[4].split(",")[-1] == str(FIRST_FOLD_AT.year)
    assert hakan[5] == "chief_executive_officer"


def test_the_history_of_the_first_fold_is_one_created_row_per_person(folded) -> None:
    rows = folded["client"].read(
        f"SELECT change_kind, count() FROM {tables.QUALIFIED_HISTORY_TABLE} "
        "WHERE fold_run_id = 'run-1' GROUP BY change_kind"
    )
    assert rows == [["created", "4"]]


def test_re_running_the_fold_selects_nothing(folded) -> None:
    """The selection converges: every input watermark is older than the folded_at the first
    run stamped on every row."""
    counts = folded["rerun"]
    assert counts.considered == 0 and counts.persons == 0 and counts.created == 0
    rows = folded["client"].read(
        f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE fold_run_id = 'run-2'"
    )
    assert rows == [["0"]]


def test_a_rule_a_merge_and_a_tombstone_all_move_their_persons(folded) -> None:
    counts = folded["third"]
    assert counts.considered == 3 and counts.stale_rules == 0
    client = folded["client"]
    hidden = client.read(
        f"SELECT active, inactive_reason FROM {tables.QUALIFIED_MAIN_TABLE} FINAL "
        f"WHERE company_id = '{HIDE_CO}'"
    )
    assert hidden == [["0", "hidden"]]
    withdrawn = client.read(
        f"SELECT active, inactive_reason, length(sources) FROM {tables.QUALIFIED_MAIN_TABLE} "
        f"FINAL WHERE company_id = '{GONE_CO}'"
    )
    assert withdrawn == [["0", "withdrawn", "1"]]
    merged = client.read(
        f"SELECT person_key, length(member_slots), active, inactive_reason "
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL WHERE company_id = '{COMPANY}' "
        "ORDER BY active DESC"
    )
    assert merged[0][:2] == [folded["keys"]["anna"], "3"]      # the merge absorbed Håkan
    assert merged[1] == [folded["keys"]["hakan"], "1", "0", "withdrawn"]
    kinds = client.read(
        f"SELECT change_kind, count() FROM {tables.QUALIFIED_HISTORY_TABLE} "
        "WHERE fold_run_id = 'run-3' GROUP BY change_kind ORDER BY change_kind"
    )
    assert kinds == [["hidden", "1"], ["updated", "1"], ["withdrawn", "2"]]


def test_no_company_ever_carries_one_person_key_twice(folded) -> None:
    """FINAL collapses a shared key silently, so the check cannot read the main table alone:
    every key the fold ever created is one `created` history row (the history table is a
    plain MergeTree, nothing collapses there), and the main table must carry exactly that
    many distinct (company_id, person_key) pairs."""
    created = folded["client"].read(
        f"SELECT count() FROM {tables.QUALIFIED_HISTORY_TABLE} WHERE change_kind = 'created'"
    )
    distinct = folded["client"].read(
        f"SELECT uniqExact((company_id, person_key)) FROM {tables.QUALIFIED_MAIN_TABLE} FINAL"
    )
    assert created == distinct and created != [["0"]]


def test_the_precedence_export_wrote_its_five_global_rows(folded) -> None:
    rows = folded["client"].read(
        f"SELECT source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL "
        "WHERE company_id = '' AND field = 'name' ORDER BY precedence DESC"
    )
    assert rows == [["reviewer", "20000"], ["ratsit", "1000"], ["bolagsverket", "900"],
                    ["wikidata", "600"], ["esef", "400"]]
```

- [ ] **Step 4: Run both suites**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_assets.py -q`
Expected: PASS.

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_person_fold_clickhouse_local.py -m integration -q`
Expected: PASS. It runs about 60 `clickhouse-local` invocations (two settings x three fold rounds x ~10 reads), so budget two to five minutes under docker. If a claim fails, print the failing script: `_LocalClient._run` already asserts on the process output, which carries ClickHouse's own error.

Run: `uv run --frozen --no-sync dg check defs` → OK, and
`uv run --frozen --no-sync dg list defs | rg se_company_person` shows `se_company_person_fold`, `se_company_person_fold_companies`, `se_company_person_precedence_clickhouse` beside the four slice-0/1 assets.

- [ ] **Step 5: Update `person-design.md`**

Add these rows to the module table (after `assets.py`):

```markdown
| `precedence.py` | The `name` spelling order (`PERSON_PRECEDENCE`, `precedence_for`, `precedence_rows`): reviewer 20000, ratsit 1000 (reserved), bolagsverket 900, wikidata 600, esef 400. It decides the published spelling and the `data` merge, never who is published |
| `fold.py` | The pure fold: identity sets (equal first/last tokens with the unique-minimal-superset middle rule, or a shared QID, never across two birth years), the canonical name and `person_key`, the reviewer rules, the member/roles/`data` blocks, the lifecycle diff and the history entries |
| `batch.py` | The fold's SQL and paging: selection, the four page reads under `FINAL`, history-then-main writes, `FoldCounts`, `fold_companies`, `fold_bucket` |
```

and these to the asset list:

```markdown
| `se_company_person_fold` | 64 static buckets (`bucket_00`..`bucket_63`, `modulo(cityHash64(company_id), 64)`), no pool, `BackfillPolicy.multi_run(max_partitions_per_run=1)`; config `changed_only` (default true) and `page_size` (default 20,000) |
| `se_company_person_fold_companies` | The targeted fold for the backoffice's Fold now: normalizes `company_ids` first (always `changed_only`), then folds them (`changed_only` false by default) |
| `se_company_person_precedence_clickhouse` | Exports `PERSON_PRECEDENCE` as the global (`company_id = ''`, `field = 'name'`) rows; re-running it re-folds every company |
```

Then append a "## The fold" section with: the identity rules (2 in Task 2's list), the person row/roles/`data` rules (1 to 5 in Task 3's list), the lifecycle and the five change kinds, and this selection paragraph:

```markdown
A company is folded when it has no main row and at least one `ok` normalized row, or when its
newest input is newer than its `max(folded_at)`. "Newest input" is the newest of: its newest
normalized row **of any status** (a row leaving `ok` changes the published set), its newest
rule version **of any `active` value** (a Reset is a new version with `active = 0`, and
filtering it out here would make a rule permanent), its own newest precedence row, and the
global precedence export's stamp — so re-exporting the dictionary re-folds every company
once. Re-running a folded bucket selects nothing, because the fold rewrites every row of
every folded company with the run's `folded_at`, whether or not anything changed; the history
table is what records what actually changed.
```

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/se_company/person/assets.py \
        src/dagster_v3/defs/se_company/person/docs/person-design.md \
        tests/test_se_company_person_assets.py \
        tests/test_se_company_person_fold_clickhouse_local.py
git commit -m "feat(dagster): person fold assets, clickhouse-local proof and design doc"
```

---

### Task 6: Prod run (controller)

The controller runs this task; a task subagent never touches prod. Every step is a Dagster run or a read-only `SELECT`.

- [ ] **Step 1: Whole-branch review, merge, deploy**

1. Review the branch end to end (`git -C <worktree> diff main...se-person-entity`).
2. The owner merges. The main checkout sits on `main` today, so `git -C /Users/graovic/pulsarpoint/ppoint/companycollect merge se-person-entity`; if it is on another branch, merge through the deploy worktree (memory `se-worktree-deploy-recipe`).
3. Deploy the dagster host from a pristine worktree at the merge commit (the dbt-state refresh is mandatory; see the same memory).
4. On the host: `dg check defs` green, and `dg list defs | rg se_company_person` lists `se_company_person_fold`, `se_company_person_fold_companies` and `se_company_person_precedence_clickhouse`.

- [ ] **Step 2: Export the precedence**

Materialize `se_company_person_precedence_clickhouse`. Expect `pairs = 5`, `stale_pairs = 0`.
Verify: `SELECT source, precedence FROM corpscout.se_company_person_precedence FINAL WHERE company_id = '' ORDER BY precedence DESC` → the five rows of spec 3.6.
**Do this before the first fold**: the export's `decided_at` is one of the fold's selection watermarks, so exporting afterwards would re-select every company on the next run.

- [ ] **Step 3: Fold ONE bucket and read it**

Materialize `se_company_person_fold`, partition `bucket_00`, default config. Record the wall time (it sizes the other 63) and the metadata: `considered` (expect roughly 578,356 / 64 ≈ 9,000 companies), `persons`, `created` (= `persons` + hidden, all rows are new), `updated = hidden = withdrawn = reactivated = 0`, `unchanged = 0`, `stale_rules = 0`, `sets_split_by_birth_year` (expect a handful).

Readouts, all with the bucket filter `modulo(cityHash64(company_id), 64) = 0`:

```sql
-- rows, active rows, companies
SELECT count(), countIf(active = 1), uniqExact(company_id)
FROM corpscout.se_company_person_v2 FINAL
WHERE modulo(cityHash64(company_id), 64) = 0;

-- members per person
SELECT length(member_slots) AS members, count()
FROM corpscout.se_company_person_v2 FINAL
WHERE active = 1 AND modulo(cityHash64(company_id), 64) = 0
GROUP BY members ORDER BY members;

-- the source-set combinations
SELECT arrayStringConcat(arraySort(sources), '+') AS combo, count()
FROM corpscout.se_company_person_v2 FINAL
WHERE active = 1 AND modulo(cityHash64(company_id), 64) = 0
GROUP BY combo ORDER BY count() DESC;

-- roles per year
SELECT year, count() FROM (
  SELECT arrayJoin(role_years) AS year FROM corpscout.se_company_person_v2 FINAL
  WHERE active = 1 AND modulo(cityHash64(company_id), 64) = 0
) GROUP BY year ORDER BY year;

-- no (company_id, person_key) twice: FINAL would hide a collision, so cross-check the
-- history (plain MergeTree, one `created` row per key the fold ever made) against the
-- distinct keys of the main table, and both against the run metadata's `created` sum.
SELECT
  (SELECT count() FROM corpscout.se_company_person_history
    WHERE change_kind = 'created' AND modulo(cityHash64(company_id), 64) = 0) AS created_rows,
  (SELECT uniqExact((company_id, person_key)) FROM corpscout.se_company_person_v2 FINAL
    WHERE modulo(cityHash64(company_id), 64) = 0) AS distinct_keys,
  (SELECT count() FROM corpscout.se_company_person_v2 FINAL
    WHERE modulo(cityHash64(company_id), 64) = 0) AS main_rows;
-- expected: created_rows = distinct_keys = main_rows = the bucket run's `created` metadata

-- history by change kind
SELECT change_kind, count() FROM corpscout.se_company_person_history
WHERE modulo(cityHash64(company_id), 64) = 0 GROUP BY change_kind ORDER BY change_kind;

-- ten persons with two or more sources, for the hand spot-check
SELECT company_id, display_name, birth_year, sources, member_names, role_codes, role_years, text_source
FROM corpscout.se_company_person_v2 FINAL
WHERE active = 1 AND length(sources) > 1 AND modulo(cityHash64(company_id), 64) = 0
ORDER BY cityHash64(person_key) LIMIT 10;
```

Acceptance: `count() = countIf(active = 1)` (nothing is hidden or withdrawn on a first fold), history rows = main rows and all of kind `created`, 0 duplicate keys. Spot-check the ten multi-source persons against `corpscout.se_company_person_normalized FINAL` for the same company: every member accounted for, the published spelling from the highest-precedence member, the role years the union of the members'.

- [ ] **Step 4: Re-run `bucket_00` (the convergence proof)**

Materialize `se_company_person_fold` `bucket_00` again with the default config: `considered = 0`, `persons = 0`, and no new history (`SELECT count() FROM corpscout.se_company_person_history WHERE fold_run_id = '<the second run id>'` → 0). If `considered > 0`, stop and find which watermark is newer than `folded_at` before backfilling anything.

- [ ] **Step 5: Backfill the other 63 buckets**

Launch a UI backfill of `bucket_01`..`bucket_63` (`multi_run(1)`, so one run per partition; no pool, so several run in parallel — keep an eye on `max_concurrent_runs`). Poll; record the total wall time and the summed metadata.

- [ ] **Step 6: Full readouts (spec 9 item 2)**

The same seven queries without the bucket filter, plus:

```sql
-- active persons and the companies that have them
SELECT count(), uniqExact(company_id) FROM corpscout.se_company_person_v2 FINAL WHERE active = 1;

-- row states
SELECT active, inactive_reason, count() FROM corpscout.se_company_person_v2 FINAL
GROUP BY active, inactive_reason ORDER BY active, inactive_reason;

-- roles the fold dated as "held now" (no year at all on the row; 290 Wikidata rows on 2026-09-10)
SELECT source, count() FROM corpscout.se_company_person_normalized FINAL
WHERE parse_status = 'ok' AND role_code IS NOT NULL AND role_year IS NULL
  AND role_from IS NULL AND role_to IS NULL GROUP BY source;

-- the serving view after its next refresh (hourly at :45)
SELECT countIf(has_people = 1), countIf(people_bolagsverket = 1), countIf(people_esef = 1), count()
FROM corpscout.se_companies_serving;
```

Expect: active persons somewhere near 2 to 3 million over about 578,000 companies (5.5M `ok` observations, most companies signing the same board year after year — the members-per-person distribution is the number to record, not to predict); `stale_rules = 0` (no rules exist yet); `hidden = withdrawn = 0`; `has_people` roughly the company count of the first query.

- [ ] **Step 7: Record and archive**

1. Append the shipped record to spec section 9 item 2: the plan file, the merge commit, what shipped, the prod numbers of steps 3 to 6, and every ruling made on the way (the ones this plan already made are listed in its self-review).
2. Tick this plan's checkboxes and archive the ledger.
3. Update the memory file `se-basic-info-design.md` (or a new `se-person-entity.md`) with: the fold is live, the selection's four watermarks, the "re-export re-folds everything" consequence, and the open questions from `plan-2-report.md` the owner has not ruled on.

---

## Self-review

**1. Spec coverage**

| spec | where |
| --- | --- |
| 3.3 `se_company_person_v2`, 29 columns | Task 3 `PublishedPerson` (29 fields in `MAIN_COLUMNS` order), `as_tuple`; Task 4 `main_insert_sql`; Task 5's clickhouse-local claim 1 |
| 3.4 history: the previous row plus `changed_at`, `change_kind`, `fold_run_id` | Task 3 `HistoryEntry`, `history_tuple`, `change_kind_for`; Task 4 writes history before main |
| 3.5 rules hide / merge / split, `active = 0` undoes | Task 2 `apply_rules`, Task 3's hide flag and stale count, Task 4 `active_rules_sql` + `rule_watermarks_sql` |
| 3.6 precedence, `name` only, the five rows | Task 1 |
| 4.3 role years, the Wikidata span expanded | Task 3 `role_years_for`, `role_block` |
| 5.1 identity, canonical name, `person_key` | Task 2 `identity_sets`, `canonical_tokens`, `person_key`, `assign_keys` |
| 5.2 rules bound to the fold's output, stale reported, surviving re-folds | Task 2 `apply_rules` (resolution through the previous published members), Task 4 (the rules are re-read and re-applied every fold) |
| 5.3 the person row | Task 3 `_published_from`, `_text_member` |
| 5.4 roles | Task 3 `role_block` |
| 5.5 `data` | Task 3 `merge_member_data` |
| 5.6 lifecycle, history, selection | Task 3 `fold_company_persons`, Task 4 `_changed_company_ids` and the four watermark statements |
| 9 item 2 readouts | Task 6 steps 3, 4 and 6 |
| 10 names | `precedence`, `fold`, `batch` modules; assets `se_company_person_fold`, `se_company_person_fold_companies`, `se_company_person_precedence_clickhouse` |

**2. Rulings this plan made where the spec left room** (all repeated in `plan-2-report.md` for the owner):

- **Every folded company's whole set is rewritten**, not only its changed persons, and "unchanged" means "no history row". A company selected by a rule or a precedence export that changed nothing must still advance `max(folded_at)`, or the same ruling's "rerunning a folded bucket selects nothing" cannot hold.
- **A `created` history row carries the new image**, there being no previous one; every other kind carries the previous image, as spec 3.4 says.
- **A colliding canonical name disambiguates the key** with the set's smallest `source:slot`. Without it, two Anna Svenssons with different birth years — the exact case the birth-year guard creates — would share a `person_key` and the ReplacingMergeTree would publish one of them.
- **A role with no year at all is taken as held now** and makes the pair `(code, current_year)`; a row with only an end date makes `(code, end year)`. Controller ruling 2026-09-10 over the writer's "no pair": 290 of Wikidata's 466 dated roles on prod carry no date, so dropping them would hide most Wikidata roles. Task 6 step 6 counts the rows per source.
- **A fiscal year wins over a span on the same row** (ESEF carries both; spec 4.3 makes the document's fiscal year that row's role year).
- **`reactivated` covers un-hiding** as well as a withdrawn person returning.
- **A hide rule resolves by key equality first, then through the previous members** (controller ruling 2026-09-10): a key that a fuller spelling re-keyed still hides the set its members moved to, so a reviewer's Remove survives; a key nothing carries is stale.
- **A key absorbed by a merge rule, or replaced by a fuller canonical name or a new namesake, is written `withdrawn`** (disclosed: spec 5.6 names only tombstones, and the table has no other inactive reason); its history row says where the identity went only through the new key's `created` row.
- **Open and dateless Wikidata spans follow the clock, the selection does not**: a company whose inputs never change keeps the `last_year` of its last fold. Task 6 notes a yearly `changed_only: false` re-fold in `person-design.md`.
- **The rule watermark ignores `active`** so a Reset is visible; the rule READ filters `active = 1`.
- `FoldCounts` carries a `considered` counter beyond the eleven the brief named — the re-run proof and Task 6 step 4 need it.

**3. Placeholder scan:** every step carries its real code, SQL or command. The two places that point at existing code rather than repeating it are Task 1's `_precedence_export_timestamp` (copied verbatim from `address/assets.py`, and the copy is written out here) and Task 5's `person-design.md` rows (written out as markdown). No "TBD", no "similar to Task N", no test described instead of written.

**4. Type consistency:**

- `NormalizedRow` has 18 fields; `batch.NORMALIZED_SELECT_COLUMNS` has the same 18 names in the same order (`normalized_row_from_row` zips them `strict=True`).
- `PublishedPerson` has 29 fields = `tables.MAIN_COLUMNS` = `batch.MAIN_SELECT_COLUMNS`; `as_tuple` returns 29 values, `history_tuple` 32 = `tables.HISTORY_COLUMNS`.
- `PersonRule` fields = `batch.RULE_SELECT_COLUMNS` (5).
- `fold_company_persons(company_id, rows, published, rules, company_precedence, *, source_run_id, current_year)` is called with exactly that shape in Task 4 and stubbed with it in Task 5's asset test.
- `fold_companies(client, company_ids, *, changed_only, source_run_id, folded_at, page_size, log)` — no `duckdb` parameter anywhere (the address twin's second positional argument does not exist here); `targeted_fold` calls it with the same keywords, and Task 5's monkeypatched fake has the same signature.
- `FoldResult`'s nine counters are summed one-for-one into `FoldCounts`' twelve (`companies`, `considered` and `pages` come from the batch itself; `persons` and the five change kinds, `unchanged`, `stale_rules` and `sets_split_by_birth_year` from the fold).
- `identity_sets_before_split` is defined in Task 2 and called in Task 3; `assign_keys` likewise; `role_block`/`RoleBlock` are defined and used only in Task 3.
