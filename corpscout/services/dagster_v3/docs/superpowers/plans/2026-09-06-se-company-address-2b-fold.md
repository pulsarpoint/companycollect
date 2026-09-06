# SE Company Address Slice 2b: Fold, Precedence Export, Fold Assets — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish every company's addresses into `se_company_address_v2` from its normalized rows: compatibility grouping, the hide rule, set replacement, history, and a geocode block computed in-page by the slice-2a function, with the source precedence exported for the backoffice.

**Architecture:** A pure fold (`address/fold.py`) turns one company's normalized rows plus its current published rows, hide rules and precedence into the new published set (candidates active or hidden, previous keys withdrawn), without a geocode block. A batch layer (`address/batch.py`) pages companies, runs the fold, hands the page's distinct location keys to `geocode.geocode_addresses` (cache, then matcher, then centroid overlay), attaches the outcome, writes history before main. Two assets expose it (64 hash buckets under the OSM workbench pool, and a targeted fold by company ids); one asset exports the precedence dictionary. Everything mirrors `basic_info/{fold,batch,assets,precedence}.py`, which the implementer should keep open beside the brief.

**Tech Stack:** Python 3.14, Dagster 1.13.9 (`dg`), clickhouse-driver, DuckDB (the OSM workbench via `DuckDBResource`), pytest, clickhouse-local (docker) for the integration test.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`, sections 3.3 to 3.6, 5, 6, 9 (slice 2), 10. Section 5 was amended for this plan on 2026-09-06 (two-phase fold, `suggested_at` as the recency tie-break, the OSM pool, the selection predicate); the amendments are binding.

## Global Constraints

- All commands under `corpscout/services/dagster_v3`, always `uv run --frozen --no-sync ...`; tests need `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix` in the environment; `uv run --frozen --no-sync dg check defs` must pass before every commit that touches `src/`.
- No `from __future__ import annotations` in any module that defines a `@dg.asset`.
- A non-nullable ClickHouse `String`/`LowCardinality(String)` column never receives `None`: `''` instead. Arrays are inserted as Python lists.
- Every SELECT that binds a page of company ids (or keys) passes `settings=FOLD_ID_BOUND_QUERY_SETTINGS` (1 MiB `max_query_size`, 1800 s `max_execution_time`); ClickHouse's default `max_query_size` is 262,144 bytes and a 20,000-id page renders past it. A render-size guard test pins the worst case.
- One DuckDB file, one pool: every asset that opens the OSM workbench (`data/sweden_address_osm_source.duckdb`) takes `pool=sweden_address_osm.tables.DUCKDB_POOL` (`"sweden_address_osm_duckdb"`). Both fold assets open it.
- Table names and column tuples come from `address/tables.py`; never retype them. `MAIN_COLUMNS` has 31 entries, `HISTORY_COLUMNS == MAIN_COLUMNS`.
- The fold never publishes a row without a geocode block (spec section 6): a candidate that needs a geocode and got no outcome fails the page with `RuntimeError`.
- `foreign` and `no_address` rows never reach `geocode_addresses` (it raises `ValueError`).
- Commit by explicit path only; never `git add -A`. Commit trailers, in this order, each on its own line at the end of the message:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5`
- Pre-existing unrelated failures on main that are NOT this plan's concern: `tests/test_schedule_cron_contracts.py` (four schedule collisions), `tests/test_sweden_address_geocoding.py::test_lantmateriet_credentials_are_documented_without_values`.

---

## File map

| File | Responsibility |
| --- | --- |
| `src/dagster_v3/defs/se_company/address/precedence.py` (create) | The global spelling precedence (`text` field): dictionary, `precedence_for`, `precedence_rows`. |
| `src/dagster_v3/defs/se_company/address/normalize_se.py` (modify) | `display_line(...)` factored out of `normalize_se_address` so the fold can compose a merged address's text. |
| `src/dagster_v3/defs/se_company/address/geocode.py` (modify) | `GeocodeOutcome.matched_at`; the cache read carries the store's `matched_at`. |
| `src/dagster_v3/defs/se_company/address/fold.py` (create) | Pure: `NormalizedRow`, `PublishedAddress`, `FoldResult`, `fold_company_addresses`. |
| `src/dagster_v3/defs/se_company/address/batch.py` (create) | SQL texts, selection, paging, in-page geocoding, history-then-main writes, `FoldCounts`, `fold_companies`, `fold_bucket`. |
| `src/dagster_v3/defs/se_company/address/assets.py` (modify) | `se_company_address_fold`, `se_company_address_fold_companies`, `se_company_address_precedence_clickhouse`. |
| `src/dagster_v3/defs/se_company/address/docs/address-design.md` (modify) | Module map rows for the three new modules and the three assets. |
| `tests/test_se_company_address_precedence.py` (create) | Dictionary, rows, company override. |
| `tests/test_se_company_address_normalize_se.py` (modify) | `display_line` reproduces the corpus lines. |
| `tests/test_se_company_address_geocode.py` (modify) | `matched_at` on cache hits and fresh outcomes. |
| `tests/test_se_company_address_fold.py` (create) | The pure fold. |
| `tests/test_se_company_address_batch.py` (create) | Batch with a fake ClickHouse client and a stubbed geocode function. |
| `tests/test_se_company_address_fold_clickhouse_local.py` (create) | The batch SQL texts against clickhouse-local, both `join_use_nulls` settings. |
| `tests/test_se_company_address_assets.py` (create) | Asset wiring: partitions, pool, config validation, metadata keys. |

Interfaces every task must agree on (defined in Tasks 2 to 4, consumed by 4 and 5):

```python
# normalize_se.py (Task 2)
def display_line(*, care_of, box, street_name, house_number, unit, postal_code, city, street_display=None) -> str

# geocode.py (Task 2)
GeocodeOutcome.matched_at: datetime            # new last field

# fold.py (Task 3)
FOLD_VERSION = "address-fold-v1"
PUBLISHABLE_STATUSES = ("ok", "partial", "foreign")
class NormalizedRow            # 18 fields, see Task 3
class PublishedAddress         # MAIN_COLUMNS minus folded_at, plus fold_version/source_run_id; as_tuple(folded_at), location_key(), needs_geocode(), with_geocode(outcome), changed_against(other)
class FoldResult(rows: tuple[PublishedAddress, ...], published: int, hidden: int, withdrawn: int)
def fold_company_addresses(company_id, rows, published, hidden_keys, company_precedence, *, source_run_id) -> FoldResult

# batch.py (Task 4)
FOLD_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 1_048_576, "max_execution_time": 1800}
BUCKET_COUNT = 64; PAGE_SIZE = 20_000
class FoldCounts(...)          # as_metadata()
def fold_companies(client, duckdb, company_ids, *, changed_only, source_run_id, folded_at, page_size=PAGE_SIZE, log=None) -> FoldCounts
def fold_bucket(client, duckdb, bucket, *, changed_only, source_run_id, folded_at, page_size=PAGE_SIZE, log=None) -> FoldCounts
```

---

### Task 1: Precedence module and export asset

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/precedence.py`
- Modify: `src/dagster_v3/defs/se_company/address/assets.py` (append the export asset)
- Test: `tests/test_se_company_address_precedence.py`

**Interfaces:**
- Consumes: `address/tables.py` (`QUALIFIED_PRECEDENCE_TABLE`, `PRECEDENCE_COLUMNS`), `basic_info/assets.py::_precedence_export_timestamp` (copy it, do not import a private name).
- Produces: `precedence_for(source, company_precedence=None) -> int`, `precedence_rows() -> list[tuple[str, str, int]]`, `FIELD = "text"`, asset `se_company_address_precedence_clickhouse`, `export_precedence(client, exported_at) -> tuple[int, int]`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_se_company_address_precedence.py
"""Spec section 3.6: one field, `text`; a spelling tie-break only, so an unranked source
still publishes (rank 0) instead of being filtered."""

from datetime import UTC, datetime

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.assets import export_precedence
from dagster_v3.defs.se_company.address.precedence import (
    ADDRESS_PRECEDENCE,
    FIELD,
    precedence_for,
    precedence_rows,
)


def test_the_global_order_is_the_spec_order() -> None:
    assert FIELD == "text"
    assert ADDRESS_PRECEDENCE == {"reviewer": 20000, "bolagsverket": 1000, "scb": 900, "ratsit": 300}


def test_an_unranked_source_gets_zero_not_none() -> None:
    assert precedence_for("workplace") == 0
    assert precedence_for("reviewer_draft") == 0


def test_a_company_row_replaces_the_global_number_for_that_source() -> None:
    assert precedence_for("scb", {"scb": 5000}) == 5000
    assert precedence_for("bolagsverket", {"scb": 5000}) == 1000


def test_rows_are_highest_first_with_the_field_name() -> None:
    assert precedence_rows() == [
        ("text", "reviewer", 20000),
        ("text", "bolagsverket", 1000),
        ("text", "scb", 900),
        ("text", "ratsit", 300),
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
    exported_at = datetime(2026, 9, 7, 8, 0, 0, 123000, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (4, 2)
    insert_sql, rows = client.calls[0]
    assert insert_sql == (
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} ({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
    )
    assert rows[0] == ("", "text", "reviewer", 20000, 0, "code", "", exported_at)
    assert all(row[0] == "" for row in rows)
    count_sql, params = client.calls[1]
    assert "company_id = ''" in count_sql and "FINAL" in count_sql
    assert params == {"exported_at": "2026-09-07 08:00:00.123"}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_precedence.py -q`
Expected: FAIL with `ModuleNotFoundError` / `ImportError` on `precedence` and `export_precedence`.

- [ ] **Step 3: Write the module**

```python
# src/dagster_v3/defs/se_company/address/precedence.py
"""Per-source spelling precedence of the address fold (spec section 3.6).

One field, `text`: whose components are published when two members of one merged address
are equally complete. It never decides whether an address is published -- every source
with an extractor publishes -- so a source absent from the map ranks 0 rather than being
excluded. The numbers are the owner's to adjust in review; `reviewer` outranks everything
so an activated reviewer address spells its own row. `reviewer_draft` never reaches the
fold (the batch filters it by source).
"""

from collections.abc import Mapping

FIELD = "text"

ADDRESS_PRECEDENCE: dict[str, int] = {
    "reviewer": 20000,
    "bolagsverket": 1000,
    "scb": 900,
    "ratsit": 300,
}


def precedence_for(source: str, company_precedence: Mapping[str, int] | None = None) -> int:
    """The company's own row for `source` when one exists (spec 3.6 allows them; nothing
    writes them yet), else the global number, else 0."""
    if company_precedence is not None and source in company_precedence:
        return int(company_precedence[source])
    return ADDRESS_PRECEDENCE.get(source, 0)


def precedence_rows() -> list[tuple[str, str, int]]:
    """Every (field, source, precedence) pair, highest first, for the export asset."""
    return [
        (FIELD, source, precedence)
        for source, precedence in sorted(ADDRESS_PRECEDENCE.items(), key=lambda item: (-item[1], item[0]))
    ]
```

- [ ] **Step 4: Append the export to `assets.py`**

Add these imports at the top of `address/assets.py` (keep the existing ones):

```python
from typing import Any

from dagster_v3.defs.se_company.address.precedence import precedence_rows
```

Append at the end of the module:

```python
def _precedence_export_timestamp(exported_at: datetime) -> str:
    """``exported_at`` as a UTC ``%Y-%m-%d %H:%M:%S.mmm`` string for ``toDateTime64(..., 3,
    'UTC')``: a bare tz-aware datetime parameter would let the stale-pairs comparison depend
    on the server's default timezone and drop sub-second precision (basic info does the same)."""
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
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} ({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
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
    name="se_company_address_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports ADDRESS_PRECEDENCE to se_company_address_precedence as global rules "
        "(company_id '', field 'text') for the backoffice to display. The Python dictionary "
        "is the only source for these rows; re-run after changing it. Never touches a "
        "company-scoped row."
    ),
)
def se_company_address_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,))
    exported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        pairs, stale = export_precedence(client, exported_at)
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; they stay until removed by hand",
            stale,
        )
    return dg.MaterializeResult(metadata={"pairs": pairs, "stale_pairs": stale, "table": tables.QUALIFIED_PRECEDENCE_TABLE})
```

- [ ] **Step 5: Run the tests and the defs check**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_precedence.py tests/test_se_company_address_jobs.py tests/test_se_company_address_layout.py -q && uv run --frozen --no-sync dg check defs`
Expected: all PASS; `dg check defs` OK.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/se_company/address/precedence.py src/dagster_v3/defs/se_company/address/assets.py tests/test_se_company_address_precedence.py
git commit -m "feat(dagster): address spelling precedence and its ClickHouse export"
```

---

### Task 2: Shared helpers the fold needs (`display_line`, `GeocodeOutcome.matched_at`)

**Files:**
- Modify: `src/dagster_v3/defs/se_company/address/normalize_se.py` (lines 181-262: the line-building block of `normalize_se_address`)
- Modify: `src/dagster_v3/defs/se_company/address/geocode.py` (`GeocodeOutcome`, `CACHE_COLUMNS`, `_read_cache`, `_outcome_from_result`, and the docstring of `GeocodeOutcome`)
- Test: `tests/test_se_company_address_normalize_se.py`, `tests/test_se_company_address_geocode.py`, `tests/test_se_company_address_geocode_clickhouse_local.py`

**Interfaces:**
- Produces: `display_line(*, care_of, box, street_name, house_number, unit, postal_code, city, street_display=None) -> str` in `normalize_se.py`; `GeocodeOutcome.matched_at: datetime` as the new LAST field of the dataclass (every constructor call in `geocode.py` passes it by keyword).
- The 61-case golden corpus must stay byte-identical: `normalize_se_address` calls `display_line` with the street display it already computes.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_se_company_address_normalize_se.py`:

```python
def test_display_line_reproduces_every_corpus_line_from_components() -> None:
    """The fold composes a merged address's text from the union of its members'
    components. For every corpus case whose street is not delivered in mixed case,
    display_line over the stored components must equal the normalizer's own line;
    mixed-case streets keep their delivered casing only inside normalize_se_address."""
    from dagster_v3.defs.se_company.address.normalize_se import display_line

    checked = 0
    for case in _golden_cases():
        expected = case["expected"]
        if expected["parse_status"] not in ("ok", "partial"):
            continue
        raw = case["raw"]
        street_source = raw.get("street_address") or ""
        if raw.get("raw_address"):
            street_source = raw["raw_address"].split("$")[0]
        if street_source not in ("", street_source.upper(), street_source.lower()):
            continue
        composed = display_line(
            care_of=expected["care_of"], box=expected["box"], street_name=expected["street_name"],
            house_number=expected["house_number"], unit=expected["unit"],
            postal_code=expected["postal_code"], city=expected["city"],
        )
        assert composed == expected["normalized_address"], raw
        checked += 1
    assert checked >= 20


def test_display_line_orders_care_of_box_street_and_postal_parts() -> None:
    from dagster_v3.defs.se_company.address.normalize_se import display_line

    assert display_line(care_of="anna svensson", box="123", street_name=None, house_number=None,
                        unit=None, postal_code="11122", city="stockholm") == "c/o Anna Svensson, Box 123, 111 22 Stockholm"
    assert display_line(care_of=None, box=None, street_name="storgatan", house_number="5b",
                        unit="lgh 1201", postal_code=None, city="lund") == "Storgatan 5B lgh 1201, Lund"
    assert display_line(care_of=None, box=None, street_name="storgatan", house_number="5",
                        unit=None, postal_code="11122", city="stockholm", street_display="StorGatan") == "StorGatan 5, 111 22 Stockholm"
```

`_golden_cases()` is whatever helper the module already uses to load `tests/fixtures/se_addresses/golden.jsonl`; if the file loads the corpus inline in a parametrized test, add a small `_golden_cases()` returning the parsed list.

Append to `tests/test_se_company_address_geocode.py` (next to the existing cache-hit tests; reuse that file's fixtures for a cached row and a scripted result):

```python
def test_a_cache_hit_carries_the_stores_matched_at_and_a_fresh_outcome_the_runs() -> None:
    """geocoded_at on the published row must say when the outcome was computed: the
    cached row's matched_at for a hit, this run's matched_at for a miss."""
    ...  # build one cached row whose matched_at is CACHED_AT (a datetime in 2026-08),
         # one miss; call geocode_addresses with matched_at=RUN_AT; assert
         # outcomes[hit].matched_at == CACHED_AT and outcomes[miss].matched_at == RUN_AT
```

Write the test body with the file's existing FakeClickhouse/FakeDuckDB helpers; the assertion is the two `matched_at` values above.

- [ ] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_normalize_se.py tests/test_se_company_address_geocode.py -q`
Expected: FAIL with `ImportError: cannot import name 'display_line'` and `AttributeError: matched_at`.

- [ ] **Step 3: Factor `display_line` out of `normalize_se_address`**

In `normalize_se.py`, add after `_display`:

```python
def display_line(
    *,
    care_of: str | None,
    box: str | None,
    street_name: str | None,
    house_number: str | None,
    unit: str | None,
    postal_code: str | None,
    city: str | None,
    street_display: str | None = None,
) -> str:
    """The one display line of an address from its stored components: `c/o Name`, then
    `Box N` or `Street 5B unit`, then `111 22 City`, comma-joined. `street_display`
    supplies the delivered street casing when the caller has it (normalize_se_address
    does); otherwise the street is title-cased like every other part. The fold composes a
    merged address this way when the union of its members' components differs from the
    published member's own."""
    parts: list[str] = []
    if care_of:
        parts.append(f"c/o {_display(care_of)}")
    if box:
        parts.append(f"Box {box.upper()}")
    elif street_name:
        street = street_display or _display(street_name)
        if house_number:
            street += f" {house_number.upper()}"
        if unit:
            street += f" {unit}"
        parts.append(street)
    postal = " ".join(
        p for p in (f"{postal_code[:3]} {postal_code[3:]}" if postal_code else "", _display(city) if city else "") if p
    )
    if postal:
        parts.append(postal)
    return ", ".join(parts)
```

Then replace the `line_parts = []` ... `display = ", ".join(line_parts) if has_location else ""` block of `normalize_se_address` with:

```python
    if street_name and not box:
        mixed_case = street_display_source not in ("", street_display_source.upper(), street_display_source.lower())
        street_display = _display_kept(street_display_source, street_name) if mixed_case else _display(street_name)
    else:
        street_display = None
    display = display_line(
        care_of=care_of or None, box=box or None, street_name=street_name or None,
        house_number=house_number or None, unit=unit or None, postal_code=code or None,
        city=town or None, street_display=street_display,
    )
```

Note the previous code used `care_of_display` (the delivered casing after `_display`) and `town_display = _display(raw.post_town)`; `display_line` applies `_display` to the casefolded `care_of` and `town`. Run the corpus: if any of the 61 cases changes, keep the old behaviour by passing the delivered strings through two more keyword arguments (`care_of_display`, `city_display`, both optional) rather than by changing the corpus. Report which path you took.

- [ ] **Step 4: Add `matched_at` to the geocode outcome**

In `geocode.py`:
- `GeocodeOutcome`: add `matched_at: datetime` as the last field, docstring line: "`matched_at`: when the outcome was computed -- the cached row's stamp for a hit, this run's for a miss; the fold publishes it as `geocoded_at`."
- `CACHE_COLUMNS`: append `"matched_at"` (it is a `STORE_COLUMNS` member; `cache_lookup_sql` projects `CACHE_COLUMNS`, so the SELECT picks it up).
- `_read_cache`: `matched_at=row["matched_at"]`.
- `_outcome_from_result`: add a `matched_at: datetime` keyword parameter and pass it through; its caller in `geocode_addresses` passes the function's `matched_at`.
- Any other `GeocodeOutcome(...)` constructor in `geocode.py` or its tests gets `matched_at=`.

- [ ] **Step 5: Run the unit tests, the docker integration test and the defs check**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_normalize_se.py tests/test_se_company_address_geocode.py tests/test_se_company_address_adoption.py tests/test_se_company_address_normalize.py -q`
Expected: PASS, the corpus count unchanged (61 cases).

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_geocode_clickhouse_local.py -m integration -q`
Expected: PASS (the cache round-trip now reads 16 columns).

Run: `uv run --frozen --no-sync dg check defs` → OK.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/se_company/address/normalize_se.py src/dagster_v3/defs/se_company/address/geocode.py tests/test_se_company_address_normalize_se.py tests/test_se_company_address_geocode.py tests/test_se_company_address_geocode_clickhouse_local.py
git commit -m "feat(dagster): address display_line helper and matched_at on geocode outcomes"
```

---

### Task 3: The pure fold

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/fold.py`
- Test: `tests/test_se_company_address_fold.py`

**Interfaces:**
- Consumes: `tables.COMPONENT_COLUMNS`, `tables.MAIN_COLUMNS`, `tables.GEOCODE_COLUMNS`; `normalize_se.NormalizedAddress`, `normalize_se.address_key`, `normalize_se.location_key`, `normalize_se.display_line` (Task 2); `precedence.precedence_for` (Task 1); `geocode.GeocodeOutcome` (with `matched_at`, Task 2); `geocode_serving_overlay.GEOCODE_FALLBACK_STATUS` (`"matched_area"`).
- Produces: everything the batch (Task 4) calls, listed in the file map's interface block.

Rules (spec 5.1 to 5.4, amended 2026-09-06), in the order the code applies them:

1. Input rows are already filtered by the batch (`source != 'reviewer_draft'`, `parse_status IN ('ok','partial','foreign')`); the fold raises `ValueError` on any other status or on a `company_id` mismatch, so a wrong SELECT fails loudly.
2. Sort: completeness (count of non-None among the seven components) descending, then `precedence_for(source, company_precedence)` descending, then `suggested_at` (UTC) descending, then `source`, then `slot`.
3. `ok` and `foreign` rows, in sort order: each joins the first candidate it is compatible with, else starts one. Compatible: same `country_code`, `postal_code`, `city`; if either side has a `box`, both boxes equal, else `street_name` equal; and each of `house_number`, `unit`, `care_of` equal or None on at least one side.
4. `partial` rows afterwards, in sort order: a partial joins the one candidate whose `country_code`, `city` and `street_name` equal its own (all three non-None) when exactly one such candidate exists (and the house_number/unit/care_of rule holds); otherwise it starts its own candidate.
5. A candidate's components: per component, the members' shared value, or the only value present. `text_source` = first member's source; `normalized_address` = first member's own text when the union equals the first member's components, else `display_line(**union)`. `address_key` = `normalize_se.address_key` over a `NormalizedAddress` built from the union (identity = the eight fields). `kinds` = distinct member kinds in member order; `sources`, `slots`, `normalized_ids` parallel in member order. `country_code` = shared. `normalizer_version` = first member's. `parse_status` for the geocode view: `'foreign'` when the first member is foreign, `'ok'` when postal_code and city are present, else `'partial'`.
6. A candidate whose key is in `hidden_keys` gets `active = 0, inactive_reason = 'hidden'`; otherwise `active = 1, inactive_reason = ''`.
7. Every current published row whose key produced no candidate is re-emitted as `active = 0, inactive_reason = 'withdrawn'`, components, provenance and geocode block copied, `fold_version`/`source_run_id` this fold's. Already-withdrawn rows are re-emitted the same way (unchanged, so no history).
8. Geocode block on candidates: `foreign` candidates get `geocode_status = 'foreign'`, everything else empty/None, `geocode_policy = ''`, `geocode_reference = ''`, `geocoded_at = None`, and `needs_geocode()` is False. Every other candidate leaves the block empty and `needs_geocode()` True; the batch fills it via `with_geocode(outcome)`: `latitude`, `longitude`, `geocode_status = outcome.match_status`, `geocode_method = outcome.coordinate_method if outcome.match_status == GEOCODE_FALLBACK_STATUS else outcome.match_method`, `geocode_confidence = outcome.match_confidence`, `geocode_precision = outcome.geocode_precision`, `geocode_policy = outcome.policy_version`, `geocode_reference = outcome.reference_md5`, `geocoded_at = outcome.matched_at`.
9. `changed_against(other)`: True when `other is None` or any of `care_of, box, street_name, house_number, unit, postal_code, city, country_code, normalized_address, kinds, sources, slots, text_source, active, inactive_reason` differ. Never the geocode block, `normalized_ids`, `normalizer_version`, `fold_version`, `source_run_id`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_se_company_address_fold.py
"""The pure address fold (spec section 5, amended 2026-09-06)."""

from datetime import UTC, datetime

import pytest

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.fold import (
    FOLD_VERSION,
    FoldResult,
    NormalizedRow,
    PublishedAddress,
    fold_company_addresses,
)
from dagster_v3.defs.se_company.address.geocode import GeocodeOutcome
from dagster_v3.defs.se_company.address.normalize_se import NormalizedAddress, address_key, location_key

C = "5560000001"
T1 = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
T2 = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)


def row(source: str, slot: str = "postal", *, care_of=None, box=None, street_name="storgatan", house_number="5",
        unit=None, postal_code="11122", city="stockholm", country_code="SE", parse_status="ok",
        normalized_address="Storgatan 5, 111 22 Stockholm", kind="postal", suggested_at=T1, nid=None) -> NormalizedRow:
    normalized = NormalizedAddress(care_of, box, street_name, house_number, unit, postal_code, city,
                                   country_code, normalized_address, parse_status, "")
    return NormalizedRow(
        company_id=C, source=source, slot=slot, normalized_id=nid or f"{source}-{slot}".ljust(64, "0"), kind=kind,
        care_of=care_of, box=box, street_name=street_name, house_number=house_number, unit=unit,
        postal_code=postal_code, city=city, country_code=country_code, normalized_address=normalized_address,
        address_key=address_key(normalized), parse_status=parse_status,
        normalizer_version="se-address-normalizer-v2", suggested_at=suggested_at,
    )


def fold(rows, published=(), hidden=frozenset(), precedence=None) -> FoldResult:
    return fold_company_addresses(C, rows, published, hidden, precedence, source_run_id="run-1")


def outcome(key: str, status: str = "matched_exact", **overrides) -> GeocodeOutcome:
    values = dict(
        location_key=key, match_status=status, match_method="raw_full_exact", match_confidence=0.98,
        latitude=59.3, longitude=18.1, geocode_provider="osm", geocode_precision="address",
        coordinate_method="resolver", coordinate_locality="", coordinate_supporting_point_count=1,
        coordinate_spread_meters=None, policy_version="se-address-resolution-policy-v7",
        reference_md5="ref", from_cache=True, matched_at=T2,
    )
    values.update(overrides)
    return GeocodeOutcome(**values)


def test_two_sources_with_the_same_address_publish_one_row_with_parallel_provenance() -> None:
    result = fold([row("scb"), row("bolagsverket", kind="registered")])
    assert (result.published, result.hidden, result.withdrawn) == (1, 0, 0)
    published = result.rows[0]
    assert published.text_source == "bolagsverket"  # 1000 beats 900 on a completeness tie
    assert published.sources == ("bolagsverket", "scb")
    assert published.slots == ("postal", "postal")
    assert published.normalized_ids == (row("bolagsverket").normalized_id, row("scb").normalized_id)
    assert published.kinds == ("registered", "postal")
    assert published.active == 1 and published.inactive_reason == ""
    assert published.needs_geocode()


def test_the_more_complete_member_supplies_the_components_and_the_text() -> None:
    scb = row("scb", unit="lgh 1201", normalized_address="Storgatan 5 lgh 1201, 111 22 Stockholm")
    result = fold([scb, row("bolagsverket")])
    published = result.rows[0]
    assert published.unit == "lgh 1201"
    assert published.text_source == "scb"
    assert published.normalized_address == "Storgatan 5 lgh 1201, 111 22 Stockholm"
    assert published.address_key == scb.address_key


def test_a_missing_part_on_each_side_merges_into_a_text_composed_from_the_union() -> None:
    a = row("scb", unit="lgh 1201", normalized_address="Storgatan 5 lgh 1201, 111 22 Stockholm")
    b = row("ratsit", care_of="anna svensson", normalized_address="c/o Anna Svensson, Storgatan 5, 111 22 Stockholm")
    result = fold([a, b])
    assert len(result.rows) == 1
    published = result.rows[0]
    assert (published.care_of, published.unit) == ("anna svensson", "lgh 1201")
    assert published.normalized_address == "c/o Anna Svensson, Storgatan 5 lgh 1201, 111 22 Stockholm"
    assert published.address_key not in (a.address_key, b.address_key)


def test_different_house_numbers_are_two_addresses() -> None:
    result = fold([row("scb"), row("bolagsverket", house_number="7", normalized_address="Storgatan 7, 111 22 Stockholm")])
    assert result.published == 2
    assert {r.house_number for r in result.rows} == {"5", "7"}


def test_a_box_and_a_street_are_two_addresses_and_two_boxes_never_merge() -> None:
    box_a = row("scb", street_name=None, house_number=None, box="100", normalized_address="Box 100, 111 22 Stockholm")
    box_b = row("bolagsverket", street_name=None, house_number=None, box="200", normalized_address="Box 200, 111 22 Stockholm")
    result = fold([box_a, box_b, row("ratsit")])
    assert result.published == 3


def test_a_partial_row_joins_the_only_candidate_with_its_city_and_street() -> None:
    partial = row("ratsit", postal_code=None, parse_status="partial", normalized_address="Storgatan 5, Stockholm")
    result = fold([row("scb"), partial])
    assert result.published == 1
    assert result.rows[0].postal_code == "11122"
    assert result.rows[0].sources == ("scb", "ratsit")


def test_a_partial_row_with_two_matching_candidates_publishes_alone() -> None:
    partial = row("ratsit", postal_code=None, house_number=None, parse_status="partial", normalized_address="Storgatan, Stockholm")
    result = fold([row("scb"), row("bolagsverket", house_number="7", normalized_address="Storgatan 7, 111 22 Stockholm"), partial])
    assert result.published == 3
    own = [r for r in result.rows if r.sources == ("ratsit",)][0]
    assert own.postal_code is None


def test_a_foreign_row_publishes_alone_with_a_foreign_geocode_status_and_no_geocode_need() -> None:
    foreign = row("scb", street_name=None, house_number=None, postal_code=None, city=None, country_code="",
                  parse_status="foreign", normalized_address="")
    result = fold([foreign, row("bolagsverket")])
    assert result.published == 2
    published = [r for r in result.rows if r.geocode_status == "foreign"][0]
    assert not published.needs_geocode()
    assert (published.latitude, published.geocode_policy, published.geocoded_at) == (None, "", None)


def test_a_hide_rule_keeps_the_row_but_inactive() -> None:
    result = fold([row("scb")], hidden={row("scb").address_key})
    assert (result.published, result.hidden) == (0, 1)
    assert (result.rows[0].active, result.rows[0].inactive_reason) == (0, "hidden")
    assert result.rows[0].sources == ("scb",)


def test_a_hide_rule_for_an_unknown_key_is_inert() -> None:
    result = fold([row("scb")], hidden={"f" * 64})
    assert (result.published, result.hidden) == (1, 0)


def test_a_previous_key_without_a_candidate_is_withdrawn_with_its_geocode_kept() -> None:
    first = fold([row("scb")]).rows[0]
    previous = first.with_geocode(outcome(first.location_key()))
    result = fold([row("bolagsverket", house_number="7", normalized_address="Storgatan 7, 111 22 Stockholm")], published=[previous])
    assert (result.published, result.withdrawn) == (1, 1)
    withdrawn = [r for r in result.rows if r.inactive_reason == "withdrawn"][0]
    assert withdrawn.address_key == previous.address_key
    assert (withdrawn.active, withdrawn.latitude, withdrawn.geocode_status) == (0, 59.3, "matched_exact")
    assert withdrawn.source_run_id == "run-1" and withdrawn.fold_version == FOLD_VERSION
    assert not withdrawn.needs_geocode()


def test_a_withdrawn_address_that_comes_back_is_active_again() -> None:
    previous = fold([row("scb")]).rows[0]
    gone = fold([], published=[previous]).rows[0]
    assert gone.inactive_reason == "withdrawn"
    back = fold([row("scb")], published=[gone]).rows[0]
    assert (back.active, back.inactive_reason) == (1, "")
    assert back.changed_against(gone)


def test_a_more_complete_text_changes_the_key_so_the_old_row_is_withdrawn() -> None:
    previous = fold([row("scb")]).rows[0]
    richer = row("scb", unit="lgh 1201", normalized_address="Storgatan 5 lgh 1201, 111 22 Stockholm")
    result = fold([richer], published=[previous])
    keys = {r.address_key: r.inactive_reason for r in result.rows}
    assert keys == {previous.address_key: "withdrawn", richer.address_key: ""}


def test_changed_against_ignores_the_geocode_block_and_normalized_ids() -> None:
    base = fold([row("scb")]).rows[0]
    geocoded = base.with_geocode(outcome(base.location_key()))
    assert not geocoded.changed_against(base)
    renormalized = fold([row("scb", nid="n" * 64)]).rows[0]
    assert not renormalized.changed_against(base)
    assert base.changed_against(None)
    hidden = fold([row("scb")], hidden={base.address_key}).rows[0]
    assert hidden.changed_against(base)


def test_with_geocode_maps_the_outcome_and_the_fallback_names_its_coordinate_method() -> None:
    base = fold([row("scb")]).rows[0]
    exact = base.with_geocode(outcome(base.location_key()))
    assert (exact.geocode_status, exact.geocode_method, exact.geocode_confidence) == ("matched_exact", "raw_full_exact", 0.98)
    assert (exact.geocode_policy, exact.geocode_reference, exact.geocoded_at) == ("se-address-resolution-policy-v7", "ref", T2)
    area = base.with_geocode(outcome(base.location_key(), "matched_area", coordinate_method="centroid_median", geocode_precision="postcode"))
    assert (area.geocode_status, area.geocode_method, area.geocode_precision) == ("matched_area", "centroid_median", "postcode")


def test_with_geocode_refuses_another_keys_outcome() -> None:
    base = fold([row("scb")]).rows[0]
    with pytest.raises(ValueError):
        base.with_geocode(outcome("0" * 64))


def test_as_tuple_follows_main_columns_with_lists_for_arrays_and_no_none_in_strings() -> None:
    first = fold([row("scb")]).rows[0]
    base = first.with_geocode(outcome(first.location_key()))
    folded_at = datetime(2026, 9, 7, 9, 0, tzinfo=UTC)
    values = base.as_tuple(folded_at)
    assert len(values) == len(tables.MAIN_COLUMNS) == 31
    by_name = dict(zip(tables.MAIN_COLUMNS, values))
    assert by_name["folded_at"] == folded_at and by_name["fold_version"] == FOLD_VERSION
    assert isinstance(by_name["sources"], list) and isinstance(by_name["normalized_ids"], list)
    for column in ("company_id", "address_key", "country_code", "normalized_address", "text_source", "inactive_reason",
                   "geocode_status", "geocode_method", "geocode_precision", "geocode_policy", "geocode_reference",
                   "normalizer_version", "fold_version", "source_run_id"):
        assert by_name[column] is not None, column


def test_location_key_drops_care_of_and_matches_the_normalizer() -> None:
    base = fold([row("scb", care_of="anna svensson", normalized_address="c/o Anna Svensson, Storgatan 5, 111 22 Stockholm")]).rows[0]
    assert base.location_key() == location_key(row("scb").as_normalized_address())


def test_ties_break_on_precedence_then_recency_then_source_and_slot() -> None:
    older = row("scb", suggested_at=T1)
    newer = row("ratsit", suggested_at=T2)
    assert fold([older, newer]).rows[0].text_source == "scb"           # 900 beats 300
    assert fold([older, newer], precedence={"ratsit": 5000}).rows[0].text_source == "ratsit"
    assert fold([row("workplace_a", suggested_at=T1), row("workplace_b", suggested_at=T2)]).rows[0].text_source == "workplace_b"


def test_bad_input_is_refused() -> None:
    with pytest.raises(ValueError):
        fold([row("scb", parse_status="no_address")])
    with pytest.raises(ValueError):
        fold([row("reviewer_draft")])
    with pytest.raises(ValueError):
        fold([dataclasses.replace(row("scb"), company_id="5560000002")])
```

(`import dataclasses` at the top of the test module.)

- [ ] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_fold.py -q`
Expected: FAIL with `ModuleNotFoundError: ... address.fold`.

- [ ] **Step 3: Write the module**

```python
# src/dagster_v3/defs/se_company/address/fold.py
"""The per-company fold of normalized address rows into the published set (spec section 5,
amended 2026-09-06).

Pure: no I/O, no clock. The batch layer reads, geocodes and writes; this module decides
which addresses exist, which members each one merges, whose spelling is published, which
are hidden by a rule and which previously published keys are withdrawn. The geocode block
is attached afterwards by `PublishedAddress.with_geocode`, because a merged address's
location key is only known once the union of its members' components is.
"""

from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.geocode import GeocodeOutcome
from dagster_v3.defs.se_company.address.normalize_se import (
    NormalizedAddress,
    address_key,
    display_line,
    location_key,
)
from dagster_v3.defs.se_company.address.precedence import precedence_for
from dagster_v3.defs.sweden_company.geocode_serving_overlay import GEOCODE_FALLBACK_STATUS

FOLD_VERSION = "address-fold-v1"
PUBLISHABLE_STATUSES: tuple[str, ...] = ("ok", "partial", "foreign")
EXCLUDED_SOURCES: tuple[str, ...] = ("reviewer_draft",)
HIDDEN = "hidden"
WITHDRAWN = "withdrawn"
FOREIGN_GEOCODE_STATUS = "foreign"
# The one-sided components: equal, or missing on one side, for two rows to be compatible.
_ONE_SIDED: tuple[str, ...] = ("house_number", "unit", "care_of")
_COMPARED: tuple[str, ...] = (
    *tables.COMPONENT_COLUMNS, "country_code", "normalized_address", "kinds", "sources", "slots",
    "text_source", "active", "inactive_reason",
)


@dataclass(frozen=True, slots=True)
class NormalizedRow:
    """One current normalized row the batch read (spec 3.2), with the raw version's stamp."""

    company_id: str
    source: str
    slot: str
    normalized_id: str
    kind: str
    care_of: str | None
    box: str | None
    street_name: str | None
    house_number: str | None
    unit: str | None
    postal_code: str | None
    city: str | None
    country_code: str
    normalized_address: str
    address_key: str
    parse_status: str
    normalizer_version: str
    suggested_at: datetime

    def components(self) -> dict[str, str | None]:
        return {name: getattr(self, name) for name in tables.COMPONENT_COLUMNS}

    def completeness(self) -> int:
        return sum(1 for value in self.components().values() if value is not None)

    def as_normalized_address(self) -> NormalizedAddress:
        return NormalizedAddress(
            self.care_of, self.box, self.street_name, self.house_number, self.unit, self.postal_code, self.city,
            self.country_code, self.normalized_address, self.parse_status, "",
        )


@dataclass(frozen=True, slots=True)
class PublishedAddress:
    """One main-table row (spec 3.3) minus `folded_at`, which `as_tuple` takes."""

    company_id: str
    address_key: str
    care_of: str | None
    box: str | None
    street_name: str | None
    house_number: str | None
    unit: str | None
    postal_code: str | None
    city: str | None
    country_code: str
    normalized_address: str
    kinds: tuple[str, ...]
    sources: tuple[str, ...]
    slots: tuple[str, ...]
    normalized_ids: tuple[str, ...]
    text_source: str
    active: int
    inactive_reason: str
    latitude: float | None
    longitude: float | None
    geocode_status: str
    geocode_method: str
    geocode_confidence: float | None
    geocode_precision: str
    geocode_policy: str
    geocode_reference: str
    geocoded_at: datetime | None
    normalizer_version: str
    fold_version: str
    source_run_id: str

    def as_normalized_address(self) -> NormalizedAddress:
        status = FOREIGN_GEOCODE_STATUS if self.geocode_status == FOREIGN_GEOCODE_STATUS else (
            "ok" if self.postal_code and self.city else "partial"
        )
        return NormalizedAddress(
            self.care_of, self.box, self.street_name, self.house_number, self.unit, self.postal_code, self.city,
            self.country_code, self.normalized_address, status, "",
        )

    def location_key(self) -> str:
        return location_key(self.as_normalized_address())

    def needs_geocode(self) -> bool:
        """Candidates (active or hidden) that are not foreign. Withdrawn rows keep the
        block they had."""
        return self.inactive_reason != WITHDRAWN and self.geocode_status != FOREIGN_GEOCODE_STATUS and self.geocode_policy == ""

    def with_geocode(self, outcome: GeocodeOutcome) -> "PublishedAddress":
        if outcome.location_key != self.location_key():
            raise ValueError(f"outcome for {outcome.location_key} handed to {self.location_key()}")
        method = outcome.coordinate_method if outcome.match_status == GEOCODE_FALLBACK_STATUS else outcome.match_method
        return replace(
            self,
            latitude=outcome.latitude, longitude=outcome.longitude, geocode_status=outcome.match_status,
            geocode_method=method or "", geocode_confidence=outcome.match_confidence,
            geocode_precision=outcome.geocode_precision or "", geocode_policy=outcome.policy_version,
            geocode_reference=outcome.reference_md5, geocoded_at=outcome.matched_at,
        )

    def as_tuple(self, folded_at: datetime) -> tuple[Any, ...]:
        values = {f.name: getattr(self, f.name) for f in fields(self)}
        for name in ("kinds", "sources", "slots", "normalized_ids"):
            values[name] = list(values[name])
        values["folded_at"] = folded_at
        return tuple(values[column] for column in tables.MAIN_COLUMNS)

    def changed_against(self, other: "PublishedAddress | None") -> bool:
        if other is None:
            return True
        return any(getattr(self, name) != getattr(other, name) for name in _COMPARED)


@dataclass(frozen=True, slots=True)
class FoldResult:
    rows: tuple[PublishedAddress, ...]
    published: int
    hidden: int
    withdrawn: int


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _sort_key(row: NormalizedRow, company_precedence: Mapping[str, int] | None) -> tuple:
    return (
        -row.completeness(),
        -precedence_for(row.source, company_precedence),
        -_as_utc(row.suggested_at).timestamp(),
        row.source,
        row.slot,
    )


def _location_equal(a: Mapping[str, str | None], b: Mapping[str, str | None]) -> bool:
    if a["box"] is not None or b["box"] is not None:
        return a["box"] == b["box"]
    return a["street_name"] == b["street_name"]


def _one_sided_ok(a: Mapping[str, str | None], b: Mapping[str, str | None]) -> bool:
    return all(a[name] is None or b[name] is None or a[name] == b[name] for name in _ONE_SIDED)


class _Candidate:
    """A published address being assembled: its members in sort order and the union of
    their components."""

    def __init__(self, first: NormalizedRow) -> None:
        self.members: list[NormalizedRow] = [first]
        self.union: dict[str, str | None] = first.components()
        self.country_code = first.country_code

    def compatible(self, row: NormalizedRow) -> bool:
        c = row.components()
        return (
            row.country_code == self.country_code
            and c["postal_code"] == self.union["postal_code"]
            and c["city"] == self.union["city"]
            and _location_equal(c, self.union)
            and _one_sided_ok(c, self.union)
        )

    def partial_compatible(self, row: NormalizedRow) -> bool:
        c = row.components()
        return (
            row.country_code == self.country_code
            and c["city"] is not None and c["street_name"] is not None
            and c["city"] == self.union["city"] and c["street_name"] == self.union["street_name"]
            and _one_sided_ok(c, self.union)
        )

    def add(self, row: NormalizedRow) -> None:
        self.members.append(row)
        for name, value in row.components().items():
            if self.union[name] is None:
                self.union[name] = value


def _published_from(candidate: _Candidate, company_id: str, hidden_keys: Set[str], source_run_id: str) -> PublishedAddress:
    first = candidate.members[0]
    union = candidate.union
    foreign = first.parse_status == "foreign"
    status = "foreign" if foreign else ("ok" if union["postal_code"] and union["city"] else "partial")
    identity = NormalizedAddress(
        union["care_of"], union["box"], union["street_name"], union["house_number"], union["unit"],
        union["postal_code"], union["city"], candidate.country_code, "", status, "",
    )
    text = first.normalized_address if union == first.components() else display_line(**union)
    key = address_key(identity)
    hidden = key in hidden_keys
    kinds: list[str] = []
    for member in candidate.members:
        if member.kind not in kinds:
            kinds.append(member.kind)
    return PublishedAddress(
        company_id=company_id, address_key=key, **union, country_code=candidate.country_code,
        normalized_address=text, kinds=tuple(kinds),
        sources=tuple(m.source for m in candidate.members), slots=tuple(m.slot for m in candidate.members),
        normalized_ids=tuple(m.normalized_id for m in candidate.members), text_source=first.source,
        active=0 if hidden else 1, inactive_reason=HIDDEN if hidden else "",
        latitude=None, longitude=None, geocode_status=FOREIGN_GEOCODE_STATUS if foreign else "",
        geocode_method="", geocode_confidence=None, geocode_precision="", geocode_policy="", geocode_reference="",
        geocoded_at=None, normalizer_version=first.normalizer_version, fold_version=FOLD_VERSION,
        source_run_id=source_run_id,
    )


def fold_company_addresses(
    company_id: str,
    rows: Sequence[NormalizedRow],
    published: Sequence[PublishedAddress],
    hidden_keys: Set[str],
    company_precedence: Mapping[str, int] | None,
    *,
    source_run_id: str,
) -> FoldResult:
    """The company's new published set: every candidate (active, or hidden by a rule) and
    every previously published key without a candidate as withdrawn. Candidates carry no
    geocode block yet except the foreign ones (spec 5.2 to 5.3)."""
    for row in rows:
        if row.company_id != company_id:
            raise ValueError(f"row company_id {row.company_id!r} is not {company_id!r}")
        if row.parse_status not in PUBLISHABLE_STATUSES:
            raise ValueError(f"{row.source}/{row.slot}: parse_status {row.parse_status!r} is not publishable")
        if row.source in EXCLUDED_SOURCES:
            raise ValueError(f"{row.source}/{row.slot}: source never folds")
    ordered = sorted(rows, key=lambda r: _sort_key(r, company_precedence))
    candidates: list[_Candidate] = []
    for row in (r for r in ordered if r.parse_status != "partial"):
        target = next((c for c in candidates if c.compatible(row)), None)
        if target is None:
            candidates.append(_Candidate(row))
        else:
            target.add(row)
    for row in (r for r in ordered if r.parse_status == "partial"):
        matching = [c for c in candidates if c.partial_compatible(row)]
        if len(matching) == 1:
            matching[0].add(row)
        else:
            candidates.append(_Candidate(row))

    out = [_published_from(c, company_id, hidden_keys, source_run_id) for c in candidates]
    live_keys = {row.address_key for row in out}
    withdrawn = [
        replace(previous, active=0, inactive_reason=WITHDRAWN, fold_version=FOLD_VERSION, source_run_id=source_run_id)
        for previous in published
        if previous.address_key not in live_keys
    ]
    hidden = sum(1 for row in out if row.inactive_reason == HIDDEN)
    return FoldResult(
        rows=tuple([*out, *withdrawn]),
        published=len(out) - hidden,
        hidden=hidden,
        withdrawn=len(withdrawn),
    )
```

Note on `_published_from`'s `**union`: the union dict's keys are exactly `COMPONENT_COLUMNS` (`care_of, box, street_name, house_number, unit, postal_code, city`), all `PublishedAddress` fields.

- [ ] **Step 4: Run the tests until green**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_fold.py -q`
Expected: PASS (20 tests). If `test_a_missing_part_on_each_side_merges...` fails on the composed text's casing, the fix is in `display_line`'s use of `_display`, not in the test.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/address/fold.py tests/test_se_company_address_fold.py
git commit -m "feat(dagster): pure address fold with compatibility grouping, hide rules and withdrawal"
```

---

### Task 4: The batch layer (selection, paging, in-page geocoding, writes)

**Files:**
- Create: `src/dagster_v3/defs/se_company/address/batch.py`
- Test: `tests/test_se_company_address_batch.py`

**Interfaces:**
- Consumes: Task 3's `fold.py`; `geocode.geocode_addresses`, `geocode.GeocodeOutcome`; `address_resolution_shadow.ensure_reference_documents(duckdb, log=...) -> str`; `SWEDEN_ADDRESS_RESOLUTION_POLICY.version`; `normalize_se.NORMALIZER_VERSION`; `geocode_store.LEGACY_ADOPTED_POLICY_VERSION`; `common.normalized_se_company_ids`.
- Produces: the SQL text functions below, `FoldCounts`, `fold_companies`, `fold_bucket`, `BUCKET_COUNT`, `PAGE_SIZE`, `FOLD_ID_BOUND_QUERY_SETTINGS`.

Selection (spec 5.5, amended): a company in the page is folded when
- it has main rows and (its newest non-draft normalized row is newer than its newest `folded_at`, or its newest rule version, removed included, is newer, or it appears in the stale-geocode set), or
- it has no main rows and at least one publishable normalized row (`ok`, `partial`, `foreign`).

The stale-geocode set: `DISTINCT company_id FROM main FINAL WHERE ... inactive_reason != 'withdrawn' AND geocode_status != 'foreign' AND geocode_policy != 'legacy_adopted_v1' AND (geocode_policy != %(policy)s OR geocode_reference != %(reference)s OR normalizer_version != %(normalizer)s)`. Withdrawn rows are never re-geocoded; foreign rows carry no versions; the imported `legacy_adopted_v1` family is an unconditional cache hit (geocode `_is_hit`), so it is current by definition.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_se_company_address_batch.py
"""The batch around the pure fold: selection, paging, in-page geocoding, history before
main. A fake client records every statement; geocode_addresses is stubbed."""

from datetime import UTC, datetime
from typing import Any

import pytest

from dagster_v3.defs.se_company.address import batch, tables
from dagster_v3.defs.se_company.address.fold import FOLD_VERSION
from dagster_v3.defs.se_company.address.geocode import GeocodeOutcome
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION, NormalizedAddress, address_key, location_key
from dagster_v3.defs.sweden_company.address_resolution_policy import SWEDEN_ADDRESS_RESOLUTION_POLICY

POLICY = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
REFERENCE = "ref-1"
T0 = datetime(2026, 8, 1, 8, 0, tzinfo=UTC)
T1 = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
T2 = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)
FOLDED_AT = datetime(2026, 9, 7, 9, 0, tzinfo=UTC)
A, B, F = "5560000001", "5560000002", "5560000003"


def normalized_row(company_id: str, source: str, slot: str = "postal", *, street_name="storgatan", house_number="5",
                   postal_code="11122", city="stockholm", care_of=None, box=None, unit=None, country_code="SE",
                   parse_status="ok", text="Storgatan 5, 111 22 Stockholm", suggested_at=T1, kind="postal") -> tuple:
    key = address_key(NormalizedAddress(care_of, box, street_name, house_number, unit, postal_code, city, country_code, text, parse_status, ""))
    return (company_id, source, slot, f"{source}-{slot}".ljust(64, "0"), kind, care_of, box, street_name, house_number, unit,
            postal_code, city, country_code, text, key, parse_status, NORMALIZER_VERSION, suggested_at)


def main_row(company_id: str, key: str, **overrides) -> tuple:
    values = {
        "company_id": company_id, "address_key": key, "care_of": None, "box": None, "street_name": "storgatan",
        "house_number": "5", "unit": None, "postal_code": "11122", "city": "stockholm", "country_code": "SE",
        "normalized_address": "Storgatan 5, 111 22 Stockholm", "kinds": ["postal"], "sources": ["scb"], "slots": ["postal"],
        "normalized_ids": ["scb-postal".ljust(64, "0")], "text_source": "scb", "active": 1, "inactive_reason": "",
        "latitude": 59.3, "longitude": 18.1, "geocode_status": "matched_exact", "geocode_method": "raw_full_exact",
        "geocode_confidence": 0.98, "geocode_precision": "address", "geocode_policy": POLICY, "geocode_reference": REFERENCE,
        "geocoded_at": T0, "normalizer_version": NORMALIZER_VERSION,
    }
    values.update(overrides)
    return tuple(values[c] for c in batch.MAIN_COMPARE_COLUMNS)


class FakeClient:
    """Answers each SELECT from the dict keyed by the SQL-text function name; records
    every call with its settings."""

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
        raise AssertionError(f"unexpected SQL: {sql[:80]}")


def stub_geocode(monkeypatch, outcomes_by_key: dict[str, GeocodeOutcome] | None = None, *, calls: list | None = None):
    def fake(addresses, *, clickhouse, duckdb, run_id, matched_at, log=None):
        if calls is not None:
            calls.append(dict(addresses))
        result = {}
        for key, address in addresses.items():
            assert address.parse_status not in ("foreign", "no_address")
            result[key] = (outcomes_by_key or {}).get(key) or GeocodeOutcome(
                location_key=key, match_status="matched_exact", match_method="raw_full_exact", match_confidence=0.9,
                latitude=59.0, longitude=18.0, geocode_provider="osm", geocode_precision="address", coordinate_method="resolver",
                coordinate_locality="", coordinate_supporting_point_count=1, coordinate_spread_meters=None,
                policy_version=POLICY, reference_md5=REFERENCE, from_cache=False, matched_at=matched_at,
            )
        return result
    monkeypatch.setattr(batch, "geocode_addresses", fake)
    monkeypatch.setattr(batch, "ensure_reference_documents", lambda duckdb, log=None: REFERENCE)


def run(client, ids, *, changed_only=True, page_size=batch.PAGE_SIZE):
    return batch.fold_companies(client, object(), ids, changed_only=changed_only, source_run_id="run-1",
                                folded_at=FOLDED_AT, page_size=page_size)


def test_sql_texts_bind_ids_read_final_and_filter_drafts_and_no_address() -> None:
    assert "%(company_ids)s" in batch.current_normalized_sql() and "FINAL" in batch.current_normalized_sql()
    assert "source != 'reviewer_draft'" in batch.current_normalized_sql()
    assert "parse_status IN ('ok', 'partial', 'foreign')" in batch.current_normalized_sql()
    assert batch.normalized_watermarks_sql().count("FINAL") == 1 and "countIf" in batch.normalized_watermarks_sql()
    assert "FINAL" in batch.hidden_keys_sql() and "action = 'hide'" in batch.hidden_keys_sql() and "removed = 0" in batch.hidden_keys_sql()
    assert "field = 'text'" in batch.company_precedence_sql()
    stale = batch.stale_companies_sql()
    for fragment in ("inactive_reason != 'withdrawn'", "geocode_status != 'foreign'", "legacy_adopted_v1", "%(policy)s", "%(reference)s", "%(normalizer)s"):
        assert fragment in stale
    assert batch.main_insert_sql().startswith(f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE} (")
    assert batch.history_insert_sql().startswith(f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} (")
    assert f"modulo(cityHash64(company_id), {batch.BUCKET_COUNT})" in batch.bucket_company_ids_sql()


def test_first_fold_writes_history_then_main_with_a_geocode_block(monkeypatch) -> None:
    calls: list = []
    stub_geocode(monkeypatch, calls=calls)
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T1, 1)], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb"), normalized_row(A, "bolagsverket", kind="registered")],
        "current_main_rows_sql": [], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A])
    assert (counts.considered, counts.folded, counts.published, counts.changed, counts.unchanged) == (1, 1, 1, 1, 0)
    assert (counts.geocoded, counts.cache_hits, counts.matched) == (1, 0, 1)
    assert [sql.split(" (")[0] for sql, _ in client.inserts] == [f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE}", f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE}"]
    row = dict(zip(tables.MAIN_COLUMNS, client.inserts[1][1][0]))
    assert (row["geocode_status"], row["geocode_policy"], row["geocode_reference"], row["geocoded_at"]) == ("matched_exact", POLICY, REFERENCE, FOLDED_AT)
    assert row["sources"] == ["bolagsverket", "scb"] and row["folded_at"] == FOLDED_AT and row["fold_version"] == FOLD_VERSION
    assert len(calls[0]) == 1  # one merged address, one location key


def test_an_unchanged_company_rewrites_main_without_history(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    key = normalized_row(A, "scb")[14]
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T1, 1)], "main_watermarks_sql": [(A, T0)], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb")], "current_main_rows_sql": [main_row(A, key)],
        "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A])
    assert (counts.changed, counts.unchanged) == (0, 1)
    assert [sql.split(" (")[0] for sql, _ in client.inserts] == [f"INSERT INTO {tables.QUALIFIED_MAIN_TABLE}"]


def test_changed_only_selection_rules(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({
        # A: main newer than everything -> skipped. B: rule newer -> folded. F: no main, publishable -> folded.
        "normalized_watermarks_sql": [(A, T0, 1), (B, T0, 1), (F, T1, 1)],
        "main_watermarks_sql": [(A, T1), (B, T1)],
        "rule_watermarks_sql": [(B, T2)],
        "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(B, "scb"), normalized_row(F, "scb")],
        "current_main_rows_sql": [main_row(B, normalized_row(B, "scb")[14])], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A, B, F])
    assert (counts.companies, counts.considered) == (3, 2)
    scoped = [params["company_ids"] for sql, params, _ in client.calls if sql == batch.current_normalized_sql()]
    assert scoped == [[B, F]]


def test_a_company_with_only_no_address_rows_and_no_main_row_is_never_selected(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({"normalized_watermarks_sql": [(A, T1, 0)], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": []})
    counts = run(client, [A])
    assert counts.considered == 0 and client.inserts == []


def test_a_stale_geocode_policy_selects_the_company(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    key = normalized_row(A, "scb")[14]
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T0, 1)], "main_watermarks_sql": [(A, T1)], "rule_watermarks_sql": [], "stale_companies_sql": [(A,)],
        "current_normalized_sql": [normalized_row(A, "scb")], "current_main_rows_sql": [main_row(A, key, geocode_policy="se-address-resolution-policy-v6")],
        "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A])
    assert counts.considered == 1
    stale_params = [params for sql, params, _ in client.calls if sql == batch.stale_companies_sql()][0]
    assert stale_params == {"company_ids": [A], "policy": POLICY, "reference": REFERENCE, "normalizer": NORMALIZER_VERSION}


def test_foreign_rows_publish_without_reaching_geocode(monkeypatch) -> None:
    calls: list = []
    stub_geocode(monkeypatch, calls=calls)
    foreign = normalized_row(F, "scb", street_name=None, house_number=None, postal_code=None, city=None, country_code="", parse_status="foreign", text="")
    client = FakeClient({
        "normalized_watermarks_sql": [(F, T1, 1)], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [foreign], "current_main_rows_sql": [], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [F])
    assert counts.published == 1 and counts.geocoded == 0 and calls == []
    row = dict(zip(tables.MAIN_COLUMNS, client.inserts[1][1][0]))
    assert (row["geocode_status"], row["latitude"], row["geocode_policy"]) == ("foreign", None, "")


def test_withdrawn_rows_keep_their_block_and_are_not_geocoded(monkeypatch) -> None:
    calls: list = []
    stub_geocode(monkeypatch, calls=calls)
    old_key = "a" * 64
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T2, 1)], "main_watermarks_sql": [(A, T1)], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb", house_number="7", text="Storgatan 7, 111 22 Stockholm")],
        "current_main_rows_sql": [main_row(A, old_key)], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    counts = run(client, [A])
    assert (counts.published, counts.withdrawn, counts.changed) == (1, 1, 2)
    assert len(calls[0]) == 1
    rows = [dict(zip(tables.MAIN_COLUMNS, values)) for values in client.inserts[1][1]]
    withdrawn = [r for r in rows if r["address_key"] == old_key][0]
    assert (withdrawn["active"], withdrawn["inactive_reason"], withdrawn["latitude"]) == (0, "withdrawn", 59.3)


def test_a_hide_rule_and_a_company_precedence_row_reach_the_fold(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    key = normalized_row(A, "scb")[14]
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T1, 1)], "main_watermarks_sql": [], "rule_watermarks_sql": [(A, T1)], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb"), normalized_row(A, "ratsit")], "current_main_rows_sql": [],
        "hidden_keys_sql": [(A, key)], "company_precedence_sql": [(A, "ratsit", 5000)],
    })
    counts = run(client, [A])
    assert (counts.published, counts.hidden) == (0, 1)
    row = dict(zip(tables.MAIN_COLUMNS, client.inserts[1][1][0]))
    assert (row["active"], row["inactive_reason"], row["text_source"]) == (0, "hidden", "ratsit")


def test_a_missing_outcome_fails_the_page(monkeypatch) -> None:
    monkeypatch.setattr(batch, "geocode_addresses", lambda addresses, **kwargs: {})
    monkeypatch.setattr(batch, "ensure_reference_documents", lambda duckdb, log=None: REFERENCE)
    client = FakeClient({
        "normalized_watermarks_sql": [(A, T1, 1)], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": [],
        "current_normalized_sql": [normalized_row(A, "scb")], "current_main_rows_sql": [], "hidden_keys_sql": [], "company_precedence_sql": [],
    })
    with pytest.raises(RuntimeError):
        run(client, [A])
    assert client.inserts == []


def test_every_id_bound_read_passes_the_query_settings_and_pages(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({"normalized_watermarks_sql": [], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": []})
    run(client, [A, B, F], page_size=2)
    bound = [(params, settings) for sql, params, settings in client.calls if "%(company_ids)s" in sql]
    assert bound and all(settings == batch.FOLD_ID_BOUND_QUERY_SETTINGS for _, settings in bound)
    assert [params["company_ids"] for params, _ in bound][:4] == [[A, B]] * 4


def test_a_full_page_renders_under_the_query_size_setting() -> None:
    from clickhouse_driver.util.escape import escape_params
    ids = [str(556000000000 + i) for i in range(batch.PAGE_SIZE)]
    for text in (batch.current_normalized_sql(), batch.current_main_rows_sql(), batch.normalized_watermarks_sql(), batch.stale_companies_sql()):
        params = {"company_ids": ids, "policy": POLICY, "reference": "0" * 32, "normalizer": NORMALIZER_VERSION}
        rendered = text % escape_params(params, context=None)
        assert len(rendered.encode()) < batch.FOLD_ID_BOUND_QUERY_SETTINGS["max_query_size"]


def test_fold_bucket_reads_the_bucket_ids_then_folds_them(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({"bucket_company_ids_sql": [(A,)], "normalized_watermarks_sql": [], "main_watermarks_sql": [], "rule_watermarks_sql": [], "stale_companies_sql": []})
    counts = batch.fold_bucket(client, object(), 7, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)
    assert counts.companies == 1
    assert client.calls[0][1] == {"bucket": 7}
    with pytest.raises(ValueError):
        batch.fold_bucket(client, object(), 64, changed_only=True, source_run_id="run-1", folded_at=FOLDED_AT)


def test_fold_counts_as_metadata_names_every_counter() -> None:
    counts = batch.FoldCounts(companies=1, considered=2, folded=3, published=4, hidden=5, withdrawn=6, changed=7, unchanged=8,
                              unpublished=9, geocoded=10, cache_hits=11, matched=12)
    assert set(counts.as_metadata()) == {"companies", "considered", "folded", "published", "hidden", "withdrawn", "changed",
                                         "unchanged", "unpublished", "geocoded", "cache_hits", "matched"}


def test_invalid_company_ids_are_refused_before_any_query(monkeypatch) -> None:
    stub_geocode(monkeypatch)
    client = FakeClient({})
    with pytest.raises(ValueError):
        run(client, ["12"])
    assert client.calls == []
```

Check how `tests/test_se_company_basic_info_batch.py::test_a_full_page_renders_under_the_query_size_setting` renders the statement (it may use `clickhouse_driver.Client.substitute_params` or the escape helper) and use the same call here.

- [ ] **Step 2: Run them to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_batch.py -q`
Expected: FAIL with `ModuleNotFoundError: ... address.batch`.

- [ ] **Step 3: Write the module**

```python
# src/dagster_v3/defs/se_company/address/batch.py
"""Read normalized rows, fold in memory, geocode the page's distinct location keys, write
history then main (spec sections 5 and 6, amended 2026-09-06).

Every SELECT is a function returning its exact text so the clickhouse-local harness runs
the same SQL. Parameters bind client-side through clickhouse-driver's %(name)s syntax.
"""

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.fold import (
    EXCLUDED_SOURCES,
    PUBLISHABLE_STATUSES,
    NormalizedRow,
    PublishedAddress,
    fold_company_addresses,
)
from dagster_v3.defs.se_company.address.geocode import geocode_addresses
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.address.precedence import FIELD as PRECEDENCE_FIELD
from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.sweden_company.address_resolution_policy import SWEDEN_ADDRESS_RESOLUTION_POLICY
from dagster_v3.defs.sweden_company.address_resolution_shadow import ensure_reference_documents
from dagster_v3.defs.sweden_company.geocode_store import LEGACY_ADOPTED_POLICY_VERSION

BUCKET_COUNT = 64
PAGE_SIZE = 20_000

# clickhouse-driver renders %(company_ids)s into the statement text; a 20,000-id page is
# about 300 KB, past ClickHouse's 262,144-byte default max_query_size. Each statement here
# binds the id list once, so 1 MiB is >3x the measured worst case (guard test in
# tests/test_se_company_address_batch.py). max_execution_time makes a pathological page
# fail visibly instead of holding the pool slot forever.
FOLD_ID_BOUND_QUERY_SETTINGS = {"max_query_size": 1_048_576, "max_execution_time": 1800}

NORMALIZED_SELECT_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "normalized_id", "kind", *tables.COMPONENT_COLUMNS, "country_code",
    "normalized_address", "address_key", "parse_status", "normalizer_version", "suggested_at",
)
MAIN_COMPARE_COLUMNS: tuple[str, ...] = tuple(
    c for c in tables.MAIN_COLUMNS if c not in ("folded_at", "fold_version", "source_run_id")
)
_PUBLISHABLE_SQL = ", ".join(f"'{status}'" for status in PUBLISHABLE_STATUSES)
_EXCLUDED_SQL = " AND ".join(f"source != '{source}'" for source in EXCLUDED_SOURCES)


@dataclass(frozen=True, slots=True)
class FoldCounts:
    companies: int
    considered: int
    folded: int        # companies with at least one candidate
    published: int     # active rows written
    hidden: int
    withdrawn: int
    changed: int       # history rows
    unchanged: int
    unpublished: int   # considered companies with no candidate and no main row
    geocoded: int      # distinct location keys handed to geocode_addresses
    cache_hits: int
    matched: int       # keys the matcher resolved this run (misses)

    def as_metadata(self) -> dict[str, int]:
        return {
            "companies": self.companies, "considered": self.considered, "folded": self.folded,
            "published": self.published, "hidden": self.hidden, "withdrawn": self.withdrawn,
            "changed": self.changed, "unchanged": self.unchanged, "unpublished": self.unpublished,
            "geocoded": self.geocoded, "cache_hits": self.cache_hits, "matched": self.matched,
        }


def bucket_company_ids_sql() -> str:
    return (
        "SELECT DISTINCT company_id\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE}\n"
        f"WHERE modulo(cityHash64(company_id), {BUCKET_COUNT}) = %(bucket)s\n"
        "ORDER BY company_id"
    )


def normalized_watermarks_sql() -> str:
    """Newest non-draft normalized version per company and how many current rows are
    publishable. FINAL, so a row whose current version is no_address does not count as
    publishable through an older version."""
    return (
        f"SELECT company_id, max(normalized_at) AS normalized_at, countIf(parse_status IN ({_PUBLISHABLE_SQL})) AS publishable\n"
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
    return (
        "SELECT company_id, max(decided_at) AS decided_at\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE}\n"
        "WHERE company_id IN %(company_ids)s\n"
        "GROUP BY company_id"
    )


def stale_companies_sql() -> str:
    """Companies with a live (not withdrawn, not foreign) row geocoded under another
    policy or OSM extract, or folded from another normalizer version. The imported
    `legacy_adopted_v1` family is an unconditional cache hit, so it is never stale."""
    return (
        "SELECT DISTINCT company_id\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND inactive_reason != 'withdrawn' AND geocode_status != 'foreign'\n"
        f"  AND geocode_policy != '{LEGACY_ADOPTED_POLICY_VERSION}'\n"
        "  AND (geocode_policy != %(policy)s OR geocode_reference != %(reference)s OR normalizer_version != %(normalizer)s)"
    )


def current_normalized_sql() -> str:
    return (
        f"SELECT {', '.join(NORMALIZED_SELECT_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        f"WHERE company_id IN %(company_ids)s AND {_EXCLUDED_SQL} AND parse_status IN ({_PUBLISHABLE_SQL})\n"
        "ORDER BY company_id, source, slot"
    )


def current_main_rows_sql() -> str:
    return (
        f"SELECT {', '.join(MAIN_COMPARE_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_MAIN_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def hidden_keys_sql() -> str:
    return (
        "SELECT company_id, address_key\n"
        f"FROM {tables.QUALIFIED_RULE_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s AND action = 'hide' AND removed = 0"
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
    return f"INSERT INTO {tables.QUALIFIED_HISTORY_TABLE} ({', '.join(tables.HISTORY_COLUMNS)}) VALUES"


def normalized_row_from_row(row: Sequence[Any]) -> NormalizedRow:
    return NormalizedRow(**dict(zip(NORMALIZED_SELECT_COLUMNS, row, strict=True)))


# Comparison and withdrawal only: fold_version and source_run_id are filled with "" and
# replaced by the fold before anything is written.
def main_row_from_row(row: Sequence[Any]) -> PublishedAddress:
    values = dict(zip(MAIN_COMPARE_COLUMNS, row, strict=True))
    for name in ("kinds", "sources", "slots", "normalized_ids"):
        values[name] = tuple(values[name])
    return PublishedAddress(fold_version="", source_run_id="", **values)


def _pages(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def _changed_company_ids(client: Any, company_ids: list[str], *, policy: str, reference: str) -> list[str]:
    params = {"company_ids": company_ids}
    normalized = {
        row[0]: (row[1], int(row[2]))
        for row in client.execute(normalized_watermarks_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS)
    }
    folded = dict(client.execute(main_watermarks_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS))
    ruled = dict(client.execute(rule_watermarks_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS))
    stale = {
        row[0]
        for row in client.execute(
            stale_companies_sql(),
            {**params, "policy": policy, "reference": reference, "normalizer": NORMALIZER_VERSION},
            settings=FOLD_ID_BOUND_QUERY_SETTINGS,
        )
    }
    changed: list[str] = []
    for company_id in company_ids:
        if company_id not in normalized:
            continue
        newest, publishable = normalized[company_id]
        rule_mark = ruled.get(company_id)
        if rule_mark is not None and rule_mark > newest:
            newest = rule_mark
        if company_id not in folded:
            if publishable > 0:
                changed.append(company_id)
        elif newest > folded[company_id] or company_id in stale:
            changed.append(company_id)
    return changed


def fold_companies(
    client: Any,
    duckdb: Any,
    company_ids: Sequence[str],
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    page_size: int = PAGE_SIZE,
    log: Callable[..., object] | None = None,
) -> FoldCounts:
    """Fold the given companies in pages. Every folded company's whole set is rewritten
    with this `folded_at` (active, hidden and withdrawn rows alike) so the changed_only
    selection converges; history rows only where the compared fields changed. The page's
    distinct location keys go through geocode_addresses once, before the writes; the
    function inserts its own fresh outcomes into the cache, so a crash between it and the
    main insert costs nothing on retry."""
    ids = list(normalized_se_company_ids(company_ids))
    policy = SWEDEN_ADDRESS_RESOLUTION_POLICY.version
    reference = ensure_reference_documents(duckdb, log=log)
    considered = folded = published = hidden = withdrawn = changed = unchanged = unpublished = 0
    geocoded = cache_hits = matched = 0
    for page in _pages(ids, page_size):
        scope = _changed_company_ids(client, page, policy=policy, reference=reference) if changed_only else page
        considered += len(scope)
        if not scope:
            continue
        params = {"company_ids": scope}
        by_company: dict[str, list[NormalizedRow]] = defaultdict(list)
        for row in client.execute(current_normalized_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS):
            normalized = normalized_row_from_row(row)
            by_company[normalized.company_id].append(normalized)
        current: dict[str, list[PublishedAddress]] = defaultdict(list)
        for row in client.execute(current_main_rows_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS):
            published_row = main_row_from_row(row)
            current[published_row.company_id].append(published_row)
        hidden_keys: dict[str, set[str]] = defaultdict(set)
        for company_id, key in client.execute(hidden_keys_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS):
            hidden_keys[company_id].add(key)
        precedence: dict[str, dict[str, int]] = defaultdict(dict)
        for company_id, source, number in client.execute(company_precedence_sql(), params, settings=FOLD_ID_BOUND_QUERY_SETTINGS):
            precedence[company_id][source] = int(number)

        pending: list[tuple[str, PublishedAddress]] = []
        for company_id in scope:
            rows = by_company.get(company_id, [])
            previous = current.get(company_id, [])
            if not rows and not previous:
                unpublished += 1
                continue
            result = fold_company_addresses(
                company_id, rows, previous, hidden_keys.get(company_id, set()), precedence.get(company_id),
                source_run_id=source_run_id,
            )
            if result.published or result.hidden:
                folded += 1
            published += result.published
            hidden += result.hidden
            withdrawn += result.withdrawn
            pending.extend((company_id, row) for row in result.rows)

        to_geocode = {row.location_key(): row.as_normalized_address() for _, row in pending if row.needs_geocode()}
        outcomes = geocode_addresses(
            to_geocode, clickhouse=client, duckdb=duckdb, run_id=source_run_id, matched_at=folded_at, log=log,
        ) if to_geocode else {}
        geocoded += len(to_geocode)
        cache_hits += sum(1 for outcome in outcomes.values() if outcome.from_cache)
        matched += sum(1 for outcome in outcomes.values() if not outcome.from_cache)

        main_rows: list[tuple[Any, ...]] = []
        history_rows: list[tuple[Any, ...]] = []
        for company_id, row in pending:
            if row.needs_geocode():
                outcome = outcomes.get(row.location_key())
                if outcome is None:
                    raise RuntimeError(f"no geocode outcome for {company_id} {row.location_key()}")
                row = row.with_geocode(outcome)
            previous_row = next((p for p in current.get(company_id, []) if p.address_key == row.address_key), None)
            values = row.as_tuple(folded_at)
            main_rows.append(values)
            if row.changed_against(previous_row):
                changed += 1
                history_rows.append(values)
            else:
                unchanged += 1
        if history_rows:
            # History first: a failure between the two statements costs a duplicate history
            # row on retry, never a published row whose first history is missing.
            client.execute(history_insert_sql(), history_rows)
        if main_rows:
            client.execute(main_insert_sql(), main_rows)
        if log is not None:
            log(
                "Folded address page: companies=%d considered=%d published=%d hidden=%d withdrawn=%d "
                "history=%d geocoded=%d hits=%d matched=%d",
                len(page), len(scope), published, hidden, withdrawn, len(history_rows), len(to_geocode),
                sum(1 for o in outcomes.values() if o.from_cache), sum(1 for o in outcomes.values() if not o.from_cache),
            )
    return FoldCounts(
        companies=len(ids), considered=considered, folded=folded, published=published, hidden=hidden,
        withdrawn=withdrawn, changed=changed, unchanged=unchanged, unpublished=unpublished,
        geocoded=geocoded, cache_hits=cache_hits, matched=matched,
    )


def fold_bucket(
    client: Any,
    duckdb: Any,
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
    company_ids = [row[0] for row in client.execute(bucket_company_ids_sql(), {"bucket": bucket})]
    return fold_companies(
        client, duckdb, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
```

The per-page log line reports running totals for `published/hidden/withdrawn`; keep it as written (the page-local counters are visible in the test-facing `FoldCounts`).

- [ ] **Step 4: Run the tests until green**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_batch.py tests/test_se_company_address_fold.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/se_company/address/batch.py tests/test_se_company_address_batch.py
git commit -m "feat(dagster): address fold batch with selection, in-page geocoding and history-first writes"
```

---

### Task 5: Fold assets, the clickhouse-local proof and docs

**Files:**
- Modify: `src/dagster_v3/defs/se_company/address/assets.py`
- Modify: `src/dagster_v3/defs/se_company/address/docs/address-design.md`
- Test: `tests/test_se_company_address_assets.py`, `tests/test_se_company_address_fold_clickhouse_local.py`

**Interfaces:**
- Consumes: Task 4's `batch.py`; `sweden_address_osm.tables.DUCKDB_POOL`; `dagster_duckdb.DuckDBResource` (resource key `sweden_address_osm_duckdb`, see `sweden_address_osm/assets.py:123`); `assert_clickhouse_tables_exist`.
- Produces: assets `se_company_address_fold` (partitioned `bucket_00`..`bucket_63`, `BackfillPolicy.multi_run(max_partitions_per_run=1)`, `pool=DUCKDB_POOL`) and `se_company_address_fold_companies` (`pool=DUCKDB_POOL`); `ADDRESS_FOLD_PARTITIONS`, `address_bucket_index`, `AddressFoldConfig`, `AddressFoldCompaniesConfig`.

- [ ] **Step 1: Write the failing asset tests**

```python
# tests/test_se_company_address_assets.py
"""Wiring of the address fold assets: partitions, the OSM workbench pool, config bounds."""

import dagster as dg
import pytest

from dagster_v3.defs.se_company.address import assets, batch
from dagster_v3.defs.sweden_address_osm import tables as osm_tables


def test_the_fold_has_sixty_four_bucket_partitions_and_the_workbench_pool() -> None:
    keys = assets.ADDRESS_FOLD_PARTITIONS.get_partition_keys()
    assert keys[0] == "bucket_00" and keys[-1] == "bucket_63" and len(keys) == batch.BUCKET_COUNT
    fold = assets.se_company_address_fold
    assert fold.partitions_def is assets.ADDRESS_FOLD_PARTITIONS
    assert fold.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    for asset in (fold, assets.se_company_address_fold_companies):
        assert asset.op.pool == osm_tables.DUCKDB_POOL
        assert set(asset.required_resource_keys) >= {"clickhouse", "sweden_address_osm_duckdb"}
        assert asset.group_names_by_key[asset.key] == assets.GROUP_NAME


def test_bucket_index_parses_and_refuses() -> None:
    assert assets.address_bucket_index("bucket_07") == 7
    with pytest.raises(ValueError):
        assets.address_bucket_index("bucket_64")
    with pytest.raises(ValueError):
        assets.address_bucket_index("07")


def test_config_defaults_and_bounds() -> None:
    assert assets.AddressFoldConfig().changed_only is True
    assert assets.AddressFoldConfig().page_size == batch.PAGE_SIZE
    with pytest.raises(ValueError):
        assets.AddressFoldConfig(page_size=0)
    targeted = assets.AddressFoldCompaniesConfig(company_ids=["5560000002", "5560000001", "5560000001"])
    assert targeted.company_ids == ["5560000001", "5560000002"] and targeted.changed_only is False
    with pytest.raises(ValueError):
        assets.AddressFoldCompaniesConfig(company_ids=[])
```

Check the exact attribute names against `tests/test_se_company_basic_info_assets.py` (it pins the same things for the basic-info fold) and use whatever accessors it uses for the pool and the resource keys.

- [ ] **Step 2: Write the clickhouse-local test**

Model on `tests/test_se_company_address_normalize_clickhouse_local.py` (`_run`, `_bind`, `_literal`, `_sections`, the `join_use_nulls` parametrization, `_clickhouse_local_command`). Load migrations 000383 to 000387 plus the raw table 000382 if the harness needs it. Claims to prove, one section each:

1. `normalized_watermarks_sql` with FINAL: a company with an `ok` version then a `no_address` version of the same key reports `publishable = 0` and the newer `normalized_at`.
2. `stale_companies_sql` selects a company whose active row is on another policy, and neither a company whose only stale row is withdrawn, nor a foreign row, nor a `legacy_adopted_v1` row.
3. `main_insert_sql` accepts a full 31-value tuple from `PublishedAddress.as_tuple` (arrays as lists, `FixedString(64)` keys, `Nullable(Float64)`, `Nullable(DateTime64)`) and `current_main_rows_sql` reads it back into `main_row_from_row` equal to the original minus `fold_version`/`source_run_id`; `history_insert_sql` accepts the same tuple.
4. `hidden_keys_sql` with FINAL: a hide rule then its `removed = 1` release yields no key.

Bind parameters with the harness's `_bind`, ordering the SELECTs with `_ordered` where needed. Mark `pytestmark = pytest.mark.integration`.

- [ ] **Step 3: Run both to verify they fail**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_assets.py -q` → FAIL (`AttributeError: ADDRESS_FOLD_PARTITIONS`).
Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_fold_clickhouse_local.py -m integration -q` → the SQL-level assertions run against real statements; expected to pass once the module exists, so this step's expected result is the import error only.

- [ ] **Step 4: Add the fold assets**

In `address/assets.py`, add imports:

```python
import re

from dagster_duckdb import DuckDBResource

from dagster_v3.defs.se_company.address.batch import BUCKET_COUNT, PAGE_SIZE as FOLD_PAGE_SIZE, fold_bucket, fold_companies
from dagster_v3.defs.sweden_address_osm import tables as osm_tables
```

(The existing `PAGE_SIZE` import from `normalize` stays; alias the batch one as `FOLD_PAGE_SIZE`.) Then append:

```python
ADDRESS_FOLD_PARTITIONS = dg.StaticPartitionsDefinition([f"bucket_{bucket:02d}" for bucket in range(BUCKET_COUNT)])


def address_bucket_index(partition_key: str) -> int:
    match = re.fullmatch(r"bucket_(\d{2})", partition_key)
    if match is None:
        raise ValueError(f"invalid address fold partition key: {partition_key!r}")
    bucket = int(match.group(1))
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"address fold bucket out of range: {bucket}")
    return bucket


class AddressFoldConfig(dg.Config):
    # True: only companies whose newest normalized row, rule version or geocode/normalizer
    # version is newer than their fold (or that have no main row). False re-folds the whole
    # bucket; history only where a compared field changed.
    changed_only: bool = True
    # Companies per page; also the geocode batch: a page's distinct location keys go to
    # the matcher together. Lower it if a run presses the host's memory.
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)


class AddressFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


_FOLD_TABLES = (tables.NORMALIZED_TABLE, tables.MAIN_TABLE, tables.HISTORY_TABLE, tables.RULE_TABLE, tables.PRECEDENCE_TABLE)
_GEOCODE_TABLES = ("se_address_geocodes", "se_postcode_centroids", "se_city_centroids")


def _fold_metadata(counts, config, **extra) -> dict:
    return {
        **counts.as_metadata(), "changed_only": config.changed_only, "page_size": config.page_size,
        "table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE, **extra,
    }


@dg.asset(
    name="se_company_address_fold",
    partitions_def=ADDRESS_FOLD_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=osm_tables.DUCKDB_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "duckdb", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "Folds the current normalized address rows of the companies in one of 64 hash "
        "buckets into se_company_address_v2: compatible suggestions merge into one "
        "published address, hide rules deactivate, previously published keys without a "
        "candidate are withdrawn, and every candidate gets its geocode from the cache or "
        "the OSM matcher inside the page. Takes the OSM workbench pool so an extract swap "
        "never races a fold. Manual: launch a partition or a backfill from the UI."
    ),
)
def se_company_address_fold(
    context: dg.AssetExecutionContext, config: AddressFoldConfig, clickhouse: ClickhouseResource,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(*_FOLD_TABLES, *_GEOCODE_TABLES))
    bucket = address_bucket_index(context.partition_key)
    with clickhouse.get_connection() as client, sweden_address_osm_duckdb.get_connection() as duckdb:
        counts = fold_bucket(
            client, duckdb, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config, bucket=bucket))


@dg.asset(
    name="se_company_address_fold_companies",
    pool=osm_tables.DUCKDB_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "duckdb", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "The targeted address fold: the companies named in config.company_ids, whatever "
        "their bucket. The backoffice's Fold now button launches this asset for one company."
    ),
)
def se_company_address_fold_companies(
    context: dg.AssetExecutionContext, config: AddressFoldCompaniesConfig, clickhouse: ClickhouseResource,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(*_FOLD_TABLES, *_GEOCODE_TABLES))
    with clickhouse.get_connection() as client, sweden_address_osm_duckdb.get_connection() as duckdb:
        counts = fold_companies(
            client, duckdb, config.company_ids, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config))
```

Update the module docstring (slice 2b ships the fold and the export). Check `sweden_company/address_geocoding_assets.py:339-344` for how `DuckDBResource.get_connection()` is used there and match it.

- [ ] **Step 5: Docs**

In `address/docs/address-design.md` add module-map rows for `precedence.py`, `fold.py`, `batch.py`, and the three assets (one line each: what it does, its pool, its config), plus one paragraph "Selection" quoting the four conditions and the two exclusions (withdrawn, foreign, `legacy_adopted_v1`).

- [ ] **Step 6: Run everything**

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_assets.py tests/test_se_company_address_batch.py tests/test_se_company_address_fold.py tests/test_se_company_address_precedence.py tests/test_se_company_address_jobs.py tests/test_se_company_address_layout.py tests/test_se_company_address_geocode.py tests/test_se_company_address_normalize_se.py -q`
Expected: PASS.

Run: `WEBTECH_API_URL=http://localhost:1 WEBTECH_S3_PATH=s3://bucket/prefix uv run --frozen --no-sync pytest tests/test_se_company_address_fold_clickhouse_local.py tests/test_se_company_address_geocode_clickhouse_local.py -m integration -q`
Expected: PASS.

Run: `uv run --frozen --no-sync dg check defs` → OK, and `uv run --frozen --no-sync dg list defs | rg se_company_address` shows the two fold assets and the precedence export.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_v3/defs/se_company/address/assets.py src/dagster_v3/defs/se_company/address/docs/address-design.md tests/test_se_company_address_assets.py tests/test_se_company_address_fold_clickhouse_local.py
git commit -m "feat(dagster): address fold assets on the OSM workbench pool, clickhouse-local proof, docs"
```

---

### Task 6: Prod run (controller)

1. [ ] Whole-branch review; the owner merges; hot-sync the dagster host from the deploy worktree at the merge commit; `dg list defs` on the host shows `se_company_address_fold`, `se_company_address_fold_companies`, `se_company_address_precedence_clickhouse`.
2. [ ] `se_company_address_precedence_clickhouse`: expect `pairs = 4`, `stale_pairs = 0`.
3. [ ] `se_company_address_fold` partition `bucket_00`, default config: expect about 55,000 companies considered (no main rows yet), about 32,000 location keys geocoded of which about 900 are matcher misses (prod count on 2026-09-06: 2,078,107 distinct location keys, 2,019,072 cached, 59,035 misses), `hidden = 0`, `withdrawn = 0`, `changed = published`. Record the wall time; it sizes the backfill (63 more buckets, sequential under the pool).
4. [ ] Readouts after bucket 00 (`FINAL`, bucket filter `modulo(cityHash64(company_id), 64) = 0`): rows, active rows, companies; `geocode_status` distribution; `text_source` distribution; the count of rows with more than one source; 20 random companies against the old `se_company_address` (`is_current = 1`) lines (identical / superset / different, each difference traced to a spec-5 rule); history rows = main rows; `SELECT count() FROM se_address_geocodes WHERE address_identity_run_id = '<run id>'` equals the metadata's `matched`.
5. [ ] Re-run `bucket_00` with the default config: `considered = 0` (selection converges).
6. [ ] Backfill the remaining 63 partitions from the UI (`bucket_01`..`bucket_63`, `multi_run(1)`); poll; record the total time and the summed metadata.
7. [ ] Full readouts: active rows, companies with an active row (expect about 3.49M), geocode status distribution, matcher misses in total (expect about 59k), the first full `se_address_geocodes_current` refresh duration after the backfill.
8. [ ] Record in the ledger and spec section 9 (slice 2b shipped); archive the ledger; update the memory file.

## Self-review

- Spec coverage: 5.1 identity (Task 3 `_published_from` computes the key over the union); 5.2 compatibility, partial rule, ordering, union, text_source (Task 3); 5.3 hide rule and set replacement with withdrawal (Task 3); 5.4 history comparison and lineage (`normalized_ids` carried, not compared) (Tasks 3 and 4); 5.5 selection with the amended exclusions (Task 4); 6 in-page geocoding through `geocode_addresses`, cache written before main, foreign never handed over, no row without a block (Tasks 2 and 4); 3.6 precedence export (Task 1); the OSM pool (Task 5); 9 slice 2 prod steps (Task 6); 10 names (`precedence`, `fold`, `batch`, the three assets).
- Placeholders: Task 2's geocode test body is described rather than written because it must reuse that file's existing fixtures; every other test is written out. Task 5's clickhouse-local test lists its four claims and the harness to copy.
- Type consistency: `NormalizedRow` has 18 fields = `NORMALIZED_SELECT_COLUMNS`; `PublishedAddress` fields = `MAIN_COLUMNS` minus `folded_at` (30); `MAIN_COMPARE_COLUMNS` = 28; `FoldCounts` has 12 counters in both the dataclass and the test; `fold_companies(client, duckdb, ids, ...)` in Tasks 4 and 5; `GeocodeOutcome.matched_at` is set in Task 2 and read in Task 3.
