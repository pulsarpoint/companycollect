# Ratsit slice 3 — Ratsit addresses: the postal town and the establishments

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `se_company/address/ratsit.py` so the Ratsit company address carries the register's postal town instead of the municipality (wrong on 260,862 of 928,491 rows) and every establishment with a street and a postcode becomes a `workplace` address in its own slot, then re-extract, normalize, warm and fold on prod so ~947k company rows and ~732k establishment rows reach `corpscout.se_company_address`.

**Architecture:** The extractor stays one SQL text on the shared extract helper, but it grows three parts. (1) A `towns` dictionary subquery over `corpscout.se_scb_companies FINAL WHERE has_company = 1` — the most frequent trimmed `post_town` per digits-only postcode — is LEFT JOINed onto every delivered row, so the published `post_town` is the register's spelling and the Ratsit company row's `location_key` finally equals the register row's. (2) The live branch is a `UNION ALL` of the current report's company row (slot `company`, kind `postal`) and one row per establishment of that same report carrying a street and a postcode (slot `est:<identifier>`, kind `workplace`). (3) Because one company now has many slots, the select gains a per-slot tombstone branch — a new `address/suggestions.py::address_select_sql(*, live_sql, source)` on the shape of `person/suggestions.py::person_select_sql`, with one addition the person entity does not need: the address change scan IS the `observed_at` watermark, so a tombstone is stamped with the current report's `observed_at` rather than the stored row's. The module also stops borrowing basic info's translation-aware `ratsit_current_sql` (its translation stamp exceeds the stored address `observed_at` and re-selects 77k companies every run) for its own over `se_ratsit_company FINAL`. Version `ratsit-address-v2`. No migration, no new table, no change to the normalizer, the fold or `ADDRESS_PRECEDENCE`.

**Tech Stack:** Python 3.14, Dagster 1.13.9 (`uv run pytest`, `uv run dg check defs`), ClickHouse 26.5 (`corpscout` database, `clickhouse-driver` `%(name)s` parameters), pytest 9 with the `integration` marker and `clickhouse-local` (binary or the `clickhouse/clickhouse-server:26.5` Docker image) for the real-engine proof; DuckDB + the OSM workbench for the fold's geocoding; the prod Dagster GraphQL API over `ssh dagster` and `clickhouse-client` over `ssh companycollect` for the run; React Router v7 on `localhost:5183` (the owner's dev server, main checkout) for the Address-tab smoke.

**Spec:** `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md` — this plan is section 8 item 3, and its content is section 5 (5.1 the postal-town dictionary, 5.2 rows, 5.3 tombstones, 5.4 fold effect, 5.5 tests, 5.6 prod run) resting on the facts of section 2, the rulings of section 6 and the names of section 7.

## Global Constraints

- Work only in the worktree `/Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info` on branch `se-ratsit-addresses`. **Never `git stash`.** Never `git add -A` or `git add .` — stage by explicit path, every time.
- Commit with a message file, never `-m`: `git commit -F "$MSGFILE"`. Every message ends with the two trailers, in this order:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
  ```
- **The fix is extract-time.** The shared extract helper (`basic_info/extract.py`), the normalizer (`address/normalize.py`, `address/normalize_se.py`) and the fold (`address/fold.py`, `address/batch.py`) are **not changed**. A normalizer change means a `NORMALIZER_VERSION` bump and a full re-normalize of 3.9M rows; it is not in this slice.
- `raw_address` stays **NULL** on every Ratsit row. The normalizer parses that column (it is Bolagsverket's packed string), so putting Ratsit's original locality there would be read as an address. The care-of Ratsit glues onto the street stays on `street_address`; the normalizer splits it.
- `ADDRESS_PRECEDENCE` in `address/precedence.py` is **unchanged**: `{"reviewer": 20000, "bolagsverket": 1000, "scb": 900, "esef": 500, "ratsit": 300}`. Do **not** materialize `se_company_address_precedence_clickhouse` — re-exporting moves a fold watermark.
- No new ClickHouse migration and no new table. Every table this slice reads exists: `corpscout.se_ratsit_company` and `corpscout.se_ratsit_establishments` (000343 + 000346), `corpscout.se_scb_companies` (000373), `corpscout.se_company_address_suggestion` (000382).
- No new dependencies, in either project.
- `tables.KINDS` already contains `workplace` (`("postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown")`) and the backoffice already labels it (`app/lib/se-address-fields.ts`: `ADDRESS_KINDS`, `workplace: "Workplace"`). **No backoffice code change in this slice.**
- The three weeklies (`se_company_basic_info_weekly`, `se_company_address_weekly`, `se_company_person_weekly`) stay **STOPPED** (owner decision 2026-09-08). The prod runs are launched by hand.
- Extractor version `ratsit-address-v2`; asset `se_company_address_suggestions_ratsit`; source literal `ratsit`; kinds `postal` and `workplace`; slots `company` and `est:<identifier>`. These strings never vary.
- Do not "fix" pre-existing failing tests found on `main`.
- Python style of this package: no `from __future__ import annotations` in modules that define `@dg.asset`; SQL is built as explicit string concatenation with `%(name)s` parameters, never f-string interpolation of user data. Nested subqueries are embedded **unindented** (`f"FROM (\n{inner}\n) AS alias"`), which is what `state_scan.py` and `person/suggestions.py` do.
- **Any test that loads the whole definitions tree needs the gitignored `.env` exported first.** `dg` auto-loads it; plain `pytest` does not, so `tests/test_se_company_address_jobs.py` and `tests/test_se_company_address_assets.py` (they call `_repo()` / `load_defs()`) fail with `ValidationError ... WebtechScannerComponent api_url Input should be a valid string` without it. Before every `uv run pytest` that touches those files, and before `uv run dg check defs`, run from `corpscout/services/dagster_v3`:
  ```bash
  set -a && . ./.env && set +a
  ```
  Never commit `.env`, and never "fix" the component to make the unexported case pass.

## File Structure

| file | what happens |
| --- | --- |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/suggestions.py` | modified — gains `ADDRESS_LIVE_ROW_PREDICATE`, `ADDRESS_TOMBSTONE_COLUMNS` and `address_select_sql(*, live_sql, source)`; `ADDRESS_TARGET` and `define_address_suggestion_asset` untouched |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/ratsit.py` | **rewritten** — `ADDRESS_SOURCE`, `RATSIT_ADDRESS_EXTRACTOR_VERSION = "ratsit-address-v2"`, `RATSIT_ADDRESS_SELECT_PARAMS`, `TOWNS_SQL`, `EST_ROWS_SQL`, `EST_SLOT_SQL`, `POST_TOWN_SQL`, `ratsit_report_sql`, `ratsit_establishments_sql`, `ratsit_rows_sql`, `ratsit_live_sql`, `ratsit_current_sql`, `ratsit_select_sql`, and the asset with its three table-named deps |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/jobs.py` | modified — `WEEKLY_PAGE_SIZE` 20,000 → 10,000 with the reason (the Ratsit page binds `%(company_ids)s` three times) |
| `corpscout/services/dagster_v3/tests/test_se_company_address_jobs.py:34` | modified — the `WEEKLY_PAGE_SIZE` pin follows |
| `corpscout/services/dagster_v3/tests/test_se_company_address_extractors_sql.py` | modified — the pinned Ratsit test is rewritten, three new tests (the tombstone branch, the deps, the current SQL), and the thirteen-column loop keeps passing |
| `corpscout/services/dagster_v3/tests/fixtures/se_company_address_source_tables.sql` | **created** — the `corpscout.se_ratsit_establishments` CREATE with 000346's columns inlined |
| `corpscout/services/dagster_v3/tests/test_se_company_address_extractors_clickhouse_local.py` | modified — loads the new fixture, seeds the SCB register towns, a municipality-town Ratsit company and four establishments, and proves the dictionary, the two kinds, the repeated identifier and the second-run tombstone |
| `corpscout/services/dagster_v3/tests/test_se_company_address_fold.py` | modified — one new test: a `workplace` member at the postal location publishes one row with `kinds = ("postal", "workplace")` |
| `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md` | modified — the `ratsit` extractor bullet describes the dictionary, both kinds, both slots and the per-slot tombstone |
| `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md` | modified — lines 44 and 55: establishments no longer "come later"; the `workplace` extractor is no longer out of scope |
| `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md` | modified in Task 4 — the Shipped record under section 8 item 3 |
| this plan | ticked in Task 4 |

Not touched, deliberately: `address/scb.py` and `address/bolagsverket.py` (their single-slot NULL-row tombstones on `has_company = 0` stay exactly as they are), `address/esef.py`, `address/normalize.py`, `address/normalize_se.py`, `address/fold.py`, `address/batch.py`, `address/precedence.py`, `address/warm.py`, `address/tables.py`, `address/assets.py` (`EXTRACTOR_SOURCES` already lists `ratsit`), `se_company/state_scan.py` and `person/suggestions.py` (the address tombstone needs a join the generic scan has no place for — see Task 1), every ClickHouse migration, and the whole `corpscout/services/backoffice` tree.

---

### Task 1: The postal-town dictionary, the establishments, the per-slot tombstones

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/suggestions.py` (append after `ADDRESS_TARGET`)
- Rewrite: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/ratsit.py` (whole file, 52 lines today)
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/jobs.py:11-13`
- Test: `corpscout/services/dagster_v3/tests/test_se_company_address_extractors_sql.py`
- Test: `corpscout/services/dagster_v3/tests/test_se_company_address_jobs.py:34`

**Interfaces:**
- Consumes: `define_address_suggestion_asset(**kwargs)` forwarding to `define_suggestion_asset(source, extractor_version, current_sql, select_sql, select_params, deps, description, target, changed_scope_override)`; `ADDRESS_SELECT_COLUMNS` (`("company_id", "source", "slot", "source_record_uid", "observed_at", "kind", "raw_address", "care_of", "street_address", "postal_code", "post_town", "county", "country_code")`); `tables.RAW_ADDRESS_COLUMNS`, `tables.QUALIFIED_SUGGESTION_TABLE`; `RATSIT_NORMALIZER_VERSION` from `dagster_v3.defs.sweden_ratsit.normalization`.
- Produces, for Tasks 2-4: `suggestions.ADDRESS_LIVE_ROW_PREDICATE: str`, `suggestions.ADDRESS_TOMBSTONE_COLUMNS: tuple[str, ...]`, `suggestions.address_select_sql(*, live_sql: str, source: str) -> str`; `ratsit.ADDRESS_SOURCE: str`, `ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION: str`, `ratsit.RATSIT_ADDRESS_SELECT_PARAMS: dict[str, str]`, `ratsit.TOWNS_SQL: str`, `ratsit.EST_ROWS_SQL: str`, `ratsit.EST_SLOT_SQL: str`, `ratsit.POST_TOWN_SQL: str`, `ratsit.ratsit_report_sql(*, scoped: bool = False) -> str`, `ratsit.ratsit_establishments_sql(*, scoped: bool = False) -> str`, `ratsit.ratsit_rows_sql(*, scoped: bool = False) -> str`, `ratsit.ratsit_live_sql(*, scoped: bool = False) -> str`, `ratsit.ratsit_current_sql() -> str`, `ratsit.ratsit_select_sql() -> str`, `ratsit.se_company_address_suggestions_ratsit` (asset key `se_company_address_suggestions_ratsit`).

- [ ] **Step 1: Write the failing SQL contract tests**

In `tests/test_se_company_address_extractors_sql.py`, extend the import from `suggestions` (it currently pulls three names):

```python
from dagster_v3.defs.se_company.address.suggestions import (
    ADDRESS_LIVE_ROW_PREDICATE,
    ADDRESS_SELECT_COLUMNS,
    ADDRESS_TARGET,
    ADDRESS_TOMBSTONE_COLUMNS,
    ADDRESS_TRAILING_SELECT_SQL,
)
```

Replace `test_ratsit_takes_the_newest_report_into_the_company_slot` (lines 127-137) entirely with the four tests below. Everything else in the file stays as it is — in particular `EXTRACTORS` keeps its three entries and `test_every_select_yields_the_thirteen_columns_in_order_and_binds_the_page` keeps running over them unchanged (`_aliases` splits on the FIRST `SELECT`, which in the new text is the `live` CTE's own thirteen-column projection, and stops at its first top-level `FROM`).

```python
def test_ratsit_delivers_the_company_row_and_one_workplace_row_per_establishment() -> None:
    """Spec 2026-09-11 sections 5.1 and 5.2. The company row keeps slot `company` and kind
    `postal`; every establishment of the SAME report with a street and a postcode adds a
    `workplace` row in slot `est:<identifier>`, suffixed with the establishment index when
    one report repeats the identifier (324 rows on 2026-09-10). Both take `post_town` from
    the SCB register dictionary instead of Ratsit's locality, which is the municipality on
    260,862 of 928,491 company addresses."""
    sql = ratsit.ratsit_select_sql()

    # (1) the dictionary: the register's most frequent trimmed town per digits-only postcode,
    #     ties by the alphabetically first spelling.
    assert ratsit.TOWNS_SQL == (
        "SELECT\n"
        "    replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') AS postal_code_digits,\n"
        "    trim(ifNull(post_town, '')) AS town\n"
        "FROM corpscout.se_scb_companies FINAL\n"
        "WHERE has_company = 1\n"
        "    AND replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') != ''\n"
        "    AND trim(ifNull(post_town, '')) != ''\n"
        "GROUP BY postal_code_digits, town\n"
        "ORDER BY count() DESC, town\n"
        "LIMIT 1 BY postal_code_digits"
    )
    assert ratsit.TOWNS_SQL in sql
    assert ") AS towns ON towns.postal_code_digits = r.postal_code_digits" in sql
    # A postcode the register does not know (none today) keeps Ratsit's own locality.
    assert ratsit.POST_TOWN_SQL == (
        "nullIf(if(ifNull(towns.town, '') != '', ifNull(towns.town, ''), r.locality), '')"
    )
    assert f"    {ratsit.POST_TOWN_SQL} AS post_town,\n" in sql

    # (2) the two row kinds and their slots.
    assert "    'company' AS slot,\n" in sql
    assert "    'postal' AS kind,\n" in sql
    assert ratsit.EST_ROWS_SQL == "count() OVER (PARTITION BY est.company_id, est.identifier)"
    assert ratsit.EST_SLOT_SQL == (
        "if(count() OVER (PARTITION BY est.company_id, est.identifier) > 1, "
        "concat('est:', est.identifier, ':', toString(est.establishment_index)), "
        "concat('est:', est.identifier))"
    )
    assert f"    {ratsit.EST_SLOT_SQL} AS slot,\n" in sql
    assert "    'workplace' AS kind,\n" in sql
    assert "workplace" in tables.KINDS
    assert "FROM corpscout.se_ratsit_establishments AS e FINAL" in sql
    assert (
        "WHERE trim(ifNull(e.address_street, '')) != '' "
        "AND trim(ifNull(e.address_postal_code, '')) != ''"
    ) in sql

    # (3) source_record_uid: the report hash, plus the establishment index on a workplace row.
    assert "    concat('ratsit:', toString(report.result_sha256)) AS source_record_uid,\n" in sql
    assert (
        "    concat('ratsit:', toString(est.result_sha256), ':est:', "
        "toString(est.establishment_index)) AS source_record_uid,\n"
    ) in sql

    # (4) observed_at is the report's normalized_at on every row (spec 5.2).
    assert "    toDateTime64(report.normalized_at, 3, 'UTC') AS observed_at,\n" in sql
    assert "    toDateTime64(est.normalized_at, 3, 'UTC') AS observed_at,\n" in sql

    # (5) raw_address and care_of stay NULL: the normalizer PARSES raw_address (it is
    #     Bolagsverket's packed string), and Ratsit glues the care-of onto the street for it.
    assert "    CAST(NULL AS Nullable(String)) AS raw_address,\n" in sql
    assert "    CAST(NULL AS Nullable(String)) AS care_of,\n" in sql
    assert "    CAST(NULL AS Nullable(String)) AS country_code\n" in sql
    assert "    nullIf(r.street_address, '') AS street_address,\n" in sql
    assert "    nullIf(r.postal_code, '') AS postal_code,\n" in sql
    assert "    nullIf(r.county, '') AS county,\n" in sql

    # (6) the report is the newest per company and the establishments join ITS key, so a
    #     superseded scan's workplaces can never appear.
    assert sql.count(
        "ORDER BY c.normalized_at DESC, c.result_sha256 DESC\nLIMIT 1 BY c.company_id"
    ) == 2
    assert (
        "    ON report.company_id = e.company_id\n"
        "    AND report.result_sha256 = e.result_sha256\n"
        "    AND report.normalizer_version = e.normalizer_version\n"
    ) in sql

    assert ratsit.ADDRESS_SOURCE == "ratsit"
    assert ratsit.RATSIT_ADDRESS_SELECT_PARAMS == {"normalizer_version": RATSIT_NORMALIZER_VERSION}
    assert ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION == "ratsit-address-v2"


def test_ratsit_pairs_live_rows_with_tombstones_for_vanished_slots() -> None:
    """Spec 5.3. One company now has many slots, so a slot the newest report stops
    delivering must be nulled rather than left behind. The shape is
    person/suggestions.py::person_select_sql's -- a `live` CTE written once and read twice,
    LEFT ANTI JOINed against the stored live slots -- plus the one join the address entity
    needs: its change scan IS the observed_at watermark, so a tombstone carries the current
    report's observed_at, never the stored row's, or argMax(observed_at, suggested_at) could
    pick the stale stamp out of the page's tie and re-select the company for ever."""
    sql = ratsit.ratsit_select_sql()
    assert sql.startswith("WITH live AS (\n")
    assert "\nUNION ALL\n" in sql
    assert ADDRESS_LIVE_ROW_PREDICATE == "street_address IS NOT NULL"
    assert ADDRESS_TOMBSTONE_COLUMNS == ("slot", "kind")
    assert (
        f"WHERE source = 'ratsit' AND {ADDRESS_LIVE_ROW_PREDICATE} "
        "AND company_id IN %(company_ids)s"
    ) in sql
    assert (
        "LEFT ANTI JOIN (SELECT company_id, slot FROM live) AS live_slots\n"
        "    ON live_slots.company_id = stored.company_id AND live_slots.slot = stored.slot"
    ) in sql
    assert (
        "INNER JOIN (\n"
        "SELECT company_id, max(observed_at) AS observed_at FROM live GROUP BY company_id\n"
        ") AS report ON report.company_id = stored.company_id"
    ) in sql
    # The tombstone keeps its slot and its kind and nulls every address column.
    assert "    stored.company_id AS company_id,\n" in sql
    assert "    stored.slot AS slot,\n" in sql
    assert "    stored.kind AS kind,\n" in sql
    assert "    '' AS source_record_uid,\n" in sql
    assert "    report.observed_at AS observed_at,\n" in sql
    for column in tables.RAW_ADDRESS_COLUMNS:
        assert f"    CAST(NULL AS Nullable(String)) AS {column}" in sql, column
    # The live branch binds the page once per report occurrence, the stored slots once.
    assert sql.count("%(company_ids)s") == 3
    assert sql.count("%(normalizer_version)s") == 2
    assert ratsit.ratsit_live_sql().count("%(company_ids)s") == 0
    assert ratsit.ratsit_live_sql(scoped=True).count("%(company_ids)s") == 2
    # SCB and Bolagsverket keep their own single-slot NULL-row tombstones, untouched.
    assert "UNION ALL" not in scb.scb_select_sql()
    assert "UNION ALL" not in bolagsverket.bolagsverket_select_sql()


def test_the_ratsit_address_asset_reads_the_two_ratsit_tables_and_the_scb_register() -> None:
    """Spec 5.1. `se_ratsit_normalized` is the multi-asset's FUNCTION name, not an asset key
    -- a dep on it makes a phantom node, and that is exactly what the v1 module carried. The
    keys the multi-asset declares are the table names; `sweden_company_scb_companies_clickhouse`
    is the key address/scb.py already uses for the register the dictionary reads. The lineage
    ruling holds: an extractor reads register source tables, never another extractor's output
    nor a fold."""
    asset = ratsit.se_company_address_suggestions_ratsit
    deps = {dep.asset_key for dep in asset.specs_by_key[asset.key].deps}
    assert deps == {
        dg.AssetKey("se_ratsit_company"),
        dg.AssetKey("se_ratsit_establishments"),
        dg.AssetKey("sweden_company_scb_companies_clickhouse"),
    }
    assert dg.AssetKey("se_ratsit_normalized") not in deps
    assert dg.AssetKey("se_company_basic_info_fold") not in deps
    assert dg.AssetKey("se_company_address_fold") not in deps


def test_the_ratsit_address_current_sql_is_the_reports_own_stamp() -> None:
    """Spec 5.2: the address module gets its own `ratsit_current_sql` over se_ratsit_company
    instead of reusing basic info's translation-aware one, whose greatest(normalized_at,
    business_description_translated_at) stamp exceeds the observed_at this select writes and
    re-selects the 77k translated companies on every address run."""
    current = ratsit.ratsit_current_sql()
    assert current == (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "SELECT\n"
        "    c.company_id AS company_id,\n"
        "    toDateTime64(c.normalized_at, 3, 'UTC') AS observed_at\n"
        "FROM corpscout.se_ratsit_company AS c FINAL\n"
        "WHERE c.normalizer_version = %(normalizer_version)s\n"
        "ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "LIMIT 1 BY c.company_id\n"
        ")"
    )
    assert "se_ratsit_company_translated" not in current
    assert "se_ratsit_company_translated" not in ratsit.ratsit_select_sql()
    assert "%(company_ids)s" not in current
```

- [ ] **Step 2: Run the tests and watch them fail**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
uv run pytest tests/test_se_company_address_extractors_sql.py -v
```

Expected: `ImportError: cannot import name 'ADDRESS_LIVE_ROW_PREDICATE' from 'dagster_v3.defs.se_company.address.suggestions'` — the whole module fails to collect, which is the right failure for a contract that does not exist yet.

- [ ] **Step 3: Add the tombstone helper to `address/suggestions.py`**

Append after the `ADDRESS_TARGET` definition and before `define_address_suggestion_asset`:

```python
# A live address row always carries a street; a tombstone carries none. This is the address
# entity's LIVE_ROW_PREDICATE (person/suggestions.py has the same idea over its three name
# columns), and it is what keeps an already-tombstoned slot out of the tombstone branch.
ADDRESS_LIVE_ROW_PREDICATE = "street_address IS NOT NULL"

# What a tombstone copies from the stored row: the slot it retires, and the kind it keeps
# (spec 2026-09-11 section 5.3).
ADDRESS_TOMBSTONE_COLUMNS: tuple[str, ...] = ("slot", "kind")


def address_select_sql(*, live_sql: str, source: str) -> str:
    """The page's rows: everything the source still delivers, plus one tombstone per stored
    live slot it no longer delivers (spec 2026-09-11 section 5.3).

    The shape is person/suggestions.py::person_select_sql's -- a `live` CTE written once and
    read twice, a LEFT ANTI JOIN of the stored live slots against it -- with one addition the
    person entity does not need. The address suggestion table HAS an observed_at column and
    the change scan is that watermark (basic_info/extract.py::changed_scope_sql compares the
    source's current stamp with argMax(observed_at, suggested_at) over the stored rows). Every
    row a page writes shares one suggested_at, so a tombstone stamped with the stored row's
    OLDER observed_at could win that argMax out of the tie and re-select the company on every
    run for ever. The tombstone therefore takes the page's own observed_at for its company,
    which `live` already carries on every row of that company.

    `source` is a literal rather than %(source)s because run_extractor binds `source` only
    into the scope query's params, never into the page select's.

    A source with one slot per company (SCB, Bolagsverket) needs none of this: it tombstones
    by nulling its single row in place, on `has_company = 0`.
    """
    live_projection = ", ".join(f"live.{column} AS {column}" for column in ADDRESS_SELECT_COLUMNS)
    tombstone: dict[str, str] = {
        "company_id": "stored.company_id",
        "source": f"'{source}'",
        "slot": "stored.slot",
        # A tombstone names no source record, exactly as the person entity's does.
        "source_record_uid": "''",
        "observed_at": "report.observed_at",
        "kind": "stored.kind",
        **{column: "CAST(NULL AS Nullable(String))" for column in tables.RAW_ADDRESS_COLUMNS},
    }
    tombstone_projection = ",\n".join(
        f"    {tombstone[column]} AS {column}" for column in ADDRESS_SELECT_COLUMNS
    )
    stored_keys = (
        f"SELECT company_id, {', '.join(ADDRESS_TOMBSTONE_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n"
        f"WHERE source = '{source}' AND {ADDRESS_LIVE_ROW_PREDICATE} "
        "AND company_id IN %(company_ids)s"
    )
    vanished = (
        "SELECT stored.company_id AS company_id, stored.slot AS slot, stored.kind AS kind\n"
        f"FROM (\n{stored_keys}\n) AS stored\n"
        "LEFT ANTI JOIN (SELECT company_id, slot FROM live) AS live_slots\n"
        "    ON live_slots.company_id = stored.company_id AND live_slots.slot = stored.slot"
    )
    return (
        f"WITH live AS (\n{live_sql}\n)\n"
        f"SELECT {live_projection}\n"
        "FROM live\n"
        "UNION ALL\n"
        f"SELECT\n{tombstone_projection}\n"
        f"FROM (\n{vanished}\n) AS stored\n"
        "INNER JOIN (\n"
        "SELECT company_id, max(observed_at) AS observed_at FROM live GROUP BY company_id\n"
        ") AS report ON report.company_id = stored.company_id"
    )
```

`tables` is already imported at the top of the file (`from dagster_v3.defs.se_company.address import tables`); no new import is needed.

- [ ] **Step 4: Rewrite `address/ratsit.py`**

Replace the whole file (52 lines today) with:

```python
"""Ratsit's normalized company report -> raw address suggestions (spec 2026-09-11 section 5).

TWO ROW KINDS PER COMPANY. The company's own postal address keeps slot `company` and kind
`postal`; every establishment of the SAME report that carries a street AND a postcode adds a
row in slot `est:<identifier>` with kind `workplace` (846,718 establishment rows on
2026-09-10, 732,626 of them with both; `identifier` is on every row and unique within a
company on all but 324, which get the establishment index appended). 288,840 establishments
repeat the company's own postal street and postcode, so the fold merges them into the postal
address and its `kinds` becomes ['postal', 'workplace']; the rest publish as their own
addresses. The largest company has 1,718 establishments.

THE POSTAL TOWN COMES FROM THE REGISTER, NOT FROM RATSIT. Ratsit delivers the MUNICIPALITY as
the locality on 260,862 of 928,491 company addresses (Stockholm for Bromma 9,871, Göteborg
for Västra Frölunda 6,930, Nacka for Saltsjö-Boo 4,575, Gotland for Visby 3,355 ...). The
normalizer's `city` is part of `location_key`, `address_key` and every fold compatibility
test, so such a row folded into its own set beside the register address and geocoded as its
own key -- the ~26k near-duplicate second addresses of the address entity's follow-up list.
`TOWNS_SQL` rebuilds the register's postcode -> town dictionary per page from the same SCB
rows address/scb.py reads (15,698 postcodes; 38 carry more than one spelling, 12 a minority
above 5%), and every delivered row takes its town from there. A postcode the register does
not know (none today) keeps Ratsit's own locality. Establishment localities are already the
postal town (4 of 732,626 differ), so the dictionary changes nothing for them; it is applied
uniformly rather than only to the company row, so one rule explains every published town.

THE JOIN KEY IS DIGITS ONLY. `replaceRegexpAll(..., '[^0-9]', '')` on both sides, which is
what normalize_se.py's `re.sub(r"\\D", "", ...)` does to the postcode it stores, so a
delivered `111 22` and a register `11122` are the same postcode. The DELIVERED `postal_code`
stays as delivered (trimmed), per spec 5.2.

PER-SLOT TOMBSTONES. With many slots per company, a slot the newest report stops delivering
must be nulled rather than left behind: `suggestions.address_select_sql` adds the
live UNION ALL tombstones branch, on the shape of person/suggestions.py. Ratsit never
deletes companies, so there is no whole-company tombstone.

ITS OWN `current_sql`. basic_info/ratsit.py's stamp is `greatest(normalized_at,
business_description_translated_at)` over se_ratsit_company_translated, which exceeds the
`observed_at` this select writes and would re-select the 77k translated companies on every
address run. This module reads se_ratsit_company directly instead.
"""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import (
    address_select_sql,
    define_address_suggestion_asset,
)
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

ADDRESS_SOURCE = "ratsit"
# v2 (2026-09-12): the register's postal town, one workplace row per establishment, per-slot
# tombstones, and this module's own current_sql.
RATSIT_ADDRESS_EXTRACTOR_VERSION = "ratsit-address-v2"
RATSIT_ADDRESS_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}

# The register's postcode -> postal town dictionary (spec 5.1): the most frequent trimmed
# spelling per digits-only postcode, ties broken by the alphabetically first spelling.
# Recomputed per page (1.8M SCB rows, a second or two); no table, no migration. The inner
# aliases are `postal_code_digits`/`town` rather than the column names they derive from --
# `expr(postal_code) AS postal_code` is a cyclic alias in ClickHouse.
TOWNS_SQL = (
    "SELECT\n"
    "    replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') AS postal_code_digits,\n"
    "    trim(ifNull(post_town, '')) AS town\n"
    "FROM corpscout.se_scb_companies FINAL\n"
    "WHERE has_company = 1\n"
    "    AND replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') != ''\n"
    "    AND trim(ifNull(post_town, '')) != ''\n"
    "GROUP BY postal_code_digits, town\n"
    "ORDER BY count() DESC, town\n"
    "LIMIT 1 BY postal_code_digits"
)

# A window, not a second join: the page select already binds %(company_ids)s three times and
# the integration test runs under join_use_nulls 0 AND 1. ClickHouse computes windows after
# the inner subquery's WHERE, so the count covers only the establishments that actually
# become rows -- the same set the slot is drawn from.
EST_ROWS_SQL = "count() OVER (PARTITION BY est.company_id, est.identifier)"
# 324 of 846,718 rows repeat an identifier inside one company; those two rows would otherwise
# share a slot, collapse in the ReplacingMergeTree and leave the change scan re-selecting the
# company for ever. The suffix is the establishment index, which is unique by construction.
EST_SLOT_SQL = (
    f"if({EST_ROWS_SQL} > 1, "
    "concat('est:', est.identifier, ':', toString(est.establishment_index)), "
    "concat('est:', est.identifier))"
)
# The register's spelling when the postcode is known, Ratsit's locality otherwise. Both
# branches are plain Strings and the ifNull wrappers make the expression read the same under
# join_use_nulls 0 (an unmatched right side is '') and 1 (it is NULL).
POST_TOWN_SQL = "nullIf(if(ifNull(towns.town, '') != '', ifNull(towns.town, ''), r.locality), '')"


def ratsit_report_sql(*, scoped: bool = False) -> str:
    """The current report per company: newest `normalized_at`, ties by the higher
    `result_sha256`, `LIMIT 1 BY company_id`.

    `scoped=True` narrows it to %(company_ids)s up front, so a page picks the current report
    of its 10,000 companies rather than of all 947,200. The text appears twice in the page
    select (the company row and the establishments join), which is why the select binds the
    page three times in total.
    """
    company_filter = "\n    AND c.company_id IN %(company_ids)s" if scoped else ""
    return (
        "SELECT\n"
        "    c.company_id AS company_id,\n"
        "    c.result_sha256 AS result_sha256,\n"
        "    c.normalizer_version AS normalizer_version,\n"
        "    c.normalized_at AS normalized_at,\n"
        "    c.address_street AS address_street,\n"
        "    c.address_postal_code AS address_postal_code,\n"
        "    c.address_locality AS address_locality,\n"
        "    c.address_county AS address_county\n"
        "FROM corpscout.se_ratsit_company AS c FINAL\n"
        f"WHERE c.normalizer_version = %(normalizer_version)s{company_filter}\n"
        "ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "LIMIT 1 BY c.company_id"
    )


def ratsit_establishments_sql(*, scoped: bool = False) -> str:
    """The current report's establishments that carry a street AND a postcode, trimmed once.

    They join the report's own key `(company_id, result_sha256, normalizer_version)`, so a
    superseded scan's workplaces can never reach the suggestion table. `name`, the NACE
    mapping and the employee range are not carried: the address suggestion has no data
    column, and the slot keeps the establishment identifier for a later establishments entity.
    """
    return (
        "SELECT\n"
        "    e.company_id AS company_id,\n"
        "    e.establishment_index AS establishment_index,\n"
        "    trim(ifNull(e.identifier, '')) AS identifier,\n"
        "    trim(ifNull(e.address_street, '')) AS street_address,\n"
        "    trim(ifNull(e.address_postal_code, '')) AS postal_code,\n"
        "    replaceRegexpAll(ifNull(e.address_postal_code, ''), '[^0-9]', '') AS postal_code_digits,\n"
        "    trim(ifNull(e.address_locality, '')) AS locality,\n"
        "    trim(ifNull(e.address_county, '')) AS county,\n"
        "    report.result_sha256 AS result_sha256,\n"
        "    report.normalized_at AS normalized_at\n"
        "FROM corpscout.se_ratsit_establishments AS e FINAL\n"
        f"INNER JOIN (\n{ratsit_report_sql(scoped=scoped)}\n) AS report\n"
        "    ON report.company_id = e.company_id\n"
        "    AND report.result_sha256 = e.result_sha256\n"
        "    AND report.normalizer_version = e.normalizer_version\n"
        "WHERE trim(ifNull(e.address_street, '')) != '' AND trim(ifNull(e.address_postal_code, '')) != ''"
    )


def ratsit_rows_sql(*, scoped: bool = False) -> str:
    """The company row UNION ALL one row per qualifying establishment, in one shape.

    The company row is emitted whether or not it carries a street (as the v1 extractor did):
    ~18,700 of 947,294 reports have no street, and the normalizer files those as `no_address`,
    which is the record that Ratsit knows the company and delivered nothing usable.
    """
    return (
        "SELECT\n"
        "    report.company_id AS company_id,\n"
        "    'company' AS slot,\n"
        "    concat('ratsit:', toString(report.result_sha256)) AS source_record_uid,\n"
        "    toDateTime64(report.normalized_at, 3, 'UTC') AS observed_at,\n"
        "    'postal' AS kind,\n"
        "    trim(ifNull(report.address_street, '')) AS street_address,\n"
        "    trim(ifNull(report.address_postal_code, '')) AS postal_code,\n"
        "    replaceRegexpAll(ifNull(report.address_postal_code, ''), '[^0-9]', '') AS postal_code_digits,\n"
        "    trim(ifNull(report.address_locality, '')) AS locality,\n"
        "    trim(ifNull(report.address_county, '')) AS county\n"
        f"FROM (\n{ratsit_report_sql(scoped=scoped)}\n) AS report\n"
        "UNION ALL\n"
        "SELECT\n"
        "    est.company_id AS company_id,\n"
        f"    {EST_SLOT_SQL} AS slot,\n"
        "    concat('ratsit:', toString(est.result_sha256), ':est:', toString(est.establishment_index)) AS source_record_uid,\n"
        "    toDateTime64(est.normalized_at, 3, 'UTC') AS observed_at,\n"
        "    'workplace' AS kind,\n"
        "    est.street_address AS street_address,\n"
        "    est.postal_code AS postal_code,\n"
        "    est.postal_code_digits AS postal_code_digits,\n"
        "    est.locality AS locality,\n"
        "    est.county AS county\n"
        f"FROM (\n{ratsit_establishments_sql(scoped=scoped)}\n) AS est"
    )


def ratsit_live_sql(*, scoped: bool = False) -> str:
    """The thirteen raw suggestion columns, in ADDRESS_SELECT_COLUMNS order, with the
    register's postal town joined on.

    `raw_address` and `care_of` stay NULL: the normalizer PARSES `raw_address` (it is
    Bolagsverket's packed five-part string) and splits the care-of Ratsit glues onto the
    street. `country_code` stays NULL; every Ratsit address is Swedish and the normalizer
    defaults to SE.
    """
    return (
        "SELECT\n"
        "    r.company_id AS company_id,\n"
        f"    '{ADDRESS_SOURCE}' AS source,\n"
        "    r.slot AS slot,\n"
        "    r.source_record_uid AS source_record_uid,\n"
        "    r.observed_at AS observed_at,\n"
        "    r.kind AS kind,\n"
        "    CAST(NULL AS Nullable(String)) AS raw_address,\n"
        "    CAST(NULL AS Nullable(String)) AS care_of,\n"
        "    nullIf(r.street_address, '') AS street_address,\n"
        "    nullIf(r.postal_code, '') AS postal_code,\n"
        f"    {POST_TOWN_SQL} AS post_town,\n"
        "    nullIf(r.county, '') AS county,\n"
        "    CAST(NULL AS Nullable(String)) AS country_code\n"
        f"FROM (\n{ratsit_rows_sql(scoped=scoped)}\n) AS r\n"
        f"LEFT JOIN (\n{TOWNS_SQL}\n) AS towns ON towns.postal_code_digits = r.postal_code_digits"
    )


def ratsit_current_sql() -> str:
    """(company_id, observed_at) for the change scan and the `since` escape hatch.

    The current report's `normalized_at`, stamped exactly as every live row stamps it. Using
    basic info's translation-aware stamp here (the v1 extractor did) makes `candidate.observed_at`
    permanently greater than the `observed_at` the select writes for the 213,283 translated
    companies, so the scan re-selects them on every run.
    """
    return (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "SELECT\n"
        "    c.company_id AS company_id,\n"
        "    toDateTime64(c.normalized_at, 3, 'UTC') AS observed_at\n"
        "FROM corpscout.se_ratsit_company AS c FINAL\n"
        "WHERE c.normalizer_version = %(normalizer_version)s\n"
        "ORDER BY c.normalized_at DESC, c.result_sha256 DESC\n"
        "LIMIT 1 BY c.company_id\n"
        ")"
    )


def ratsit_select_sql() -> str:
    return address_select_sql(live_sql=ratsit_live_sql(scoped=True), source=ADDRESS_SOURCE)


se_company_address_suggestions_ratsit = define_address_suggestion_asset(
    source=ADDRESS_SOURCE,
    extractor_version=RATSIT_ADDRESS_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    select_params=RATSIT_ADDRESS_SELECT_PARAMS,
    deps=[
        # The table-named keys the se_ratsit_normalized multi-asset declares. Never
        # dg.AssetKey("se_ratsit_normalized"): that is the multi-asset function's name, not a
        # key, and a dep on it makes a phantom node in the graph (spec 5.1).
        dg.AssetKey("se_ratsit_company"),
        dg.AssetKey("se_ratsit_establishments"),
        # The register the postal-town dictionary reads -- the key address/scb.py declares.
        dg.AssetKey("sweden_company_scb_companies_clickhouse"),
    ],
    description=(
        "Ratsit's newest normalized report into se_company_address_suggestion: the company "
        "address (kind postal, slot company) plus one workplace row per establishment with a "
        "street and a postcode (slot est:<identifier>, suffixed with the establishment index "
        "when a report repeats the identifier). The postal town comes from the SCB register's "
        "postcode dictionary, not from Ratsit's locality, which is the municipality on about "
        "28% of company addresses; the care-of Ratsit glues onto the street is split by the "
        "normalizer. A slot the newest report no longer delivers is tombstoned with a NULL "
        "row. execute=false previews."
    ),
)
```

- [ ] **Step 5: Lower the STOPPED weekly's page size**

The Ratsit page select now binds `%(company_ids)s` three times, and `run_extractor` runs every page under `ID_BOUND_QUERY_SETTINGS`' `max_query_size` of 1,048,576 bytes. At 20,000 twelve-digit ids (~300 KB per binding) the rendered statement is ~900 KB — inside the limit today, with no headroom. In `src/dagster_v3/defs/se_company/address/jobs.py`, replace lines 11-13:

```python
# The two register scans are the expensive ones: 20,000 ids per page renders inside the
# extract helper's ID_BOUND_QUERY_SETTINGS (one binding per statement).
WEEKLY_PAGE_SIZE = 20_000
```

with:

```python
# The Ratsit page select binds %(company_ids)s THREE times (its report subquery appears in
# both live branches, and the stored-slot read once) and the helper runs every page under
# ID_BOUND_QUERY_SETTINGS' max_query_size of 1 MiB. 20,000 twelve-digit ids render to about
# 300 KB per binding -- ~900 KB in one statement, with no headroom. 10,000 is the number the
# person weekly settled on for its two bindings, and it is what the prod runs use.
WEEKLY_PAGE_SIZE = 10_000
```

And in `tests/test_se_company_address_jobs.py:34`, replace `assert jobs.WEEKLY_PAGE_SIZE == 20_000` with:

```python
    assert jobs.WEEKLY_PAGE_SIZE == 10_000
```

- [ ] **Step 6: Run the unit tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run pytest tests/test_se_company_address_extractors_sql.py tests/test_se_company_address_jobs.py \
  tests/test_se_company_address_assets.py tests/test_se_company_address_tables.py \
  tests/test_se_company_address_precedence.py -v
```

Expected: all pass. The two that matter most are `test_every_select_yields_the_thirteen_columns_in_order_and_binds_the_page` (the alias helper reads the `live` CTE's projection and still sees the thirteen names in order) and `test_ratsit_pairs_live_rows_with_tombstones_for_vanished_slots`.

If `_aliases` returns the report subquery's columns instead, the live SQL grew a leading `WITH` — it must not: the CTEs live inside the `FROM` subqueries precisely so the first `SELECT` of the rendered text is the thirteen-column projection.

- [ ] **Step 7: Check the definitions still load**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
set -a && . ./.env && set +a
uv run dg check defs
```

Expected: `All components validated successfully` / no errors. This is also what proves the three dep keys resolve to real assets rather than creating phantom nodes.

- [ ] **Step 8: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'MSG'
feat(se-address): Ratsit's register postal town and establishment workplaces

The Ratsit address extractor becomes ratsit-address-v2: the postal town comes
from the SCB register's postcode dictionary instead of Ratsit's locality (the
municipality on 260,862 of 928,491 company addresses), every establishment of
the current report with a street and a postcode becomes a `workplace` row in
slot est:<identifier>, and a slot the newest report stops delivering is
tombstoned through a new address/suggestions.py::address_select_sql on the
shape of person/suggestions.py::person_select_sql. The module also reads
se_ratsit_company for its own observed_at instead of basic info's
translation-aware stamp, which re-selected the translated companies every run.
The STOPPED weekly's page size drops to 10,000: the page now binds the id list
three times.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/suggestions.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/ratsit.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/jobs.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_extractors_sql.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_jobs.py
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```

---

### Task 2: The Ratsit address rows on a real ClickHouse, and the fold's `workplace` merge

**Files:**
- Create: `corpscout/services/dagster_v3/tests/fixtures/se_company_address_source_tables.sql`
- Create: `corpscout/services/dagster_v3/tests/test_se_company_address_ratsit_clickhouse_local.py`
- Modify: `corpscout/services/dagster_v3/tests/test_se_company_address_extractors_clickhouse_local.py:35-42` (load the new fixture too)
- Test: `corpscout/services/dagster_v3/tests/test_se_company_address_fold.py` (one new test, appended)

**Interfaces:**
- Consumes: everything Task 1 produced (`ratsit.ratsit_current_sql()`, `ratsit.ratsit_select_sql()`, `ratsit.RATSIT_ADDRESS_SELECT_PARAMS`, `ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION`), plus the existing helpers `changed_scope_sql(current_sql, target)`, `insert_page_sql(select_sql, target)`, `ADDRESS_TARGET`, `changed_rows_sql()`, `normalized_row(raw_row, normalized_at)`, `RAW_ROW_COLUMNS`, `NORMALIZER_VERSION`, `clickhouse_local_command()`, `_bind(sql, **params)` from `tests.test_se_company_basic_info_clickhouse_local`, and `tests/test_se_company_address_fold.py`'s own `row(...)` / `fold(...)` helpers.
- Produces: nothing other tasks import. The new fixture file is loaded by two test modules.

- [ ] **Step 1: Create the establishments fixture**

`corpscout/services/dagster_v3/tests/fixtures/se_company_address_source_tables.sql`:

```sql
-- Harness fixture only -- not a migration, never apply to a real ClickHouse.
-- corpscout.se_ratsit_establishments: migration 000343's CREATE with 000346's eleven v2
-- columns (the NACE block and the employee range) inlined in their ALTER order; CODECs and
-- CONSTRAINTs stripped like the neighbouring snapshots. It cannot ride a migration replay:
-- 000343 also creates se_ratsit_company with CHECK constraints the other fixtures' rows do
-- not satisfy, and 000346 only ALTERs this table. se_ratsit_company needs no entry here --
-- its DDL is already in se_basic_info_source_tables.sql, which both tests also load.
CREATE TABLE IF NOT EXISTS corpscout.se_ratsit_establishments (
    `company_id` String,
    `result_sha256` FixedString(64),
    `normalizer_version` LowCardinality(String),
    `establishment_index` UInt16,
    `name` Nullable(String),
    `identifier` Nullable(String),
    `industry_code` Nullable(String),
    `industry_description` Nullable(String),
    `source_industry_code` Nullable(String),
    `source_industry_code_set` LowCardinality(String) DEFAULT '',
    `industry_description_original` Nullable(String),
    `nace_revision` LowCardinality(String) DEFAULT '',
    `nace_code` Nullable(String),
    `nace_normalized_code` Nullable(String),
    `nace_mapping_method` LowCardinality(String) DEFAULT '',
    `nace_mapping_status` LowCardinality(String) DEFAULT '',
    `address_street` Nullable(String),
    `address_postal_code` Nullable(String),
    `address_locality` Nullable(String),
    `address_county` Nullable(String),
    `number_of_employees_raw` Nullable(String),
    `number_of_employees` Nullable(UInt32),
    `employee_count_min` Nullable(UInt32),
    `employee_count_max` Nullable(UInt32),
    `employee_count_open_ended` Bool DEFAULT false,
    `normalized_at` DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, result_sha256, normalizer_version, establishment_index);
```

- [ ] **Step 2: Let the existing four-extractor test see the table**

`tests/test_se_company_address_extractors_clickhouse_local.py` builds its schema from `MIGRATIONS` plus one `FIXTURE`; the Ratsit select now reads `se_ratsit_establishments`, so the file must exist or every Ratsit statement in that script fails to parse. Replace lines 35-42:

```python
# se_ratsit_company is not created by any of the five migrations above (its own migration,
# 000343, is out of scope here), so the fixture supplies it with no risk of colliding with a
# CREATE TABLE the migrations already issued for one of these five. 000390's two views
# (se_bolagsverket_companies_translated, se_ratsit_company_translated -- basic-info's ratsit
# extractor, reused here unchanged, reads the latter) need only se_bolagsverket_companies
# (000374), se_ratsit_company and text_translations (both already in the fixture); its two
# INSERT INTO ... SELECT statements are data moves the schema replay skips.
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "se_basic_info_source_tables.sql"
```

with:

```python
# se_ratsit_company is not created by any of the five migrations above (its own migration,
# 000343, is out of scope here), so the fixtures supply it with no risk of colliding with a
# CREATE TABLE the migrations already issued for one of these five. 000390's two views
# (se_bolagsverket_companies_translated, se_ratsit_company_translated) need only
# se_bolagsverket_companies (000374), se_ratsit_company and text_translations (both already
# in the fixture); its two INSERT INTO ... SELECT statements are data moves the schema replay
# skips. The second fixture carries se_ratsit_establishments, which ratsit-address-v2 reads:
# this script seeds no establishment rows, but the table has to exist for the select to parse.
FIXTURES = (
    Path(__file__).resolve().parent / "fixtures" / "se_basic_info_source_tables.sql",
    Path(__file__).resolve().parent / "fixtures" / "se_company_address_source_tables.sql",
)
```

and in `_schema()` replace the single-file read (line 182):

```python
    fixture = [s.strip() for s in FIXTURE.read_text(encoding="utf-8").split(";") if s.strip()]
```

with:

```python
    fixture = [
        statement
        for path in FIXTURES
        for statement in (s.strip() for s in path.read_text(encoding="utf-8").split(";"))
        if statement
    ]
```

Nothing else in that file changes: it seeds no establishments, its one Ratsit postcode (`11122`) is absent from its one SCB register row (`13134 NACKA`), so the dictionary leaves `post_town` as Ratsit's `Stockholm`, and every existing assertion — `ratsit_scope_1 == [[COMPANY_RATSIT]]`, `len(rows) == 3`, `reconverged == []`, the `parse_status == "ok"` / `care_of == "anna svensson"` hand-off — still holds.

- [ ] **Step 3: Write the failing Ratsit integration test**

`corpscout/services/dagster_v3/tests/test_se_company_address_ratsit_clickhouse_local.py`:

```python
"""ratsit-address-v2 on a real ClickHouse (spec 2026-09-11 sections 5.1-5.3).

The register dictionary decides the postal town (frequency, then alphabetical, `has_company
= 1` only, unknown postcode falls back to Ratsit's locality); the current report yields the
company row plus one `workplace` row per establishment with a street and a postcode, with the
establishment index appended when the report repeats an identifier; an establishment of a
superseded report never appears; and a second scan that drops an establishment tombstones its
slot with the new report's observed_at, after which the scope converges. Runs under
join_use_nulls 0 and 1."""

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.address import ratsit, tables
from dagster_v3.defs.se_company.address.normalize import RAW_ROW_COLUMNS, changed_rows_sql, normalized_row
from dagster_v3.defs.se_company.address.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.address.suggestions import ADDRESS_TARGET
from dagster_v3.defs.se_company.basic_info.extract import changed_scope_sql, insert_page_sql
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from tests.clickhouse_local import clickhouse_local_command
from tests.test_se_company_basic_info_clickhouse_local import _bind

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000373_corpscout_se_scb_companies.up.sql",
    "000382_corpscout_se_company_address_suggestion.up.sql",
    "000383_corpscout_se_company_address_normalized.up.sql",
)
FIXTURES = (
    Path(__file__).resolve().parent / "fixtures" / "se_basic_info_source_tables.sql",
    Path(__file__).resolve().parent / "fixtures" / "se_company_address_source_tables.sql",
)

# The company whose Ratsit locality is the municipality (Malmö for Oxie) and whose current
# report carries four establishments, one of them streetless and two sharing an identifier.
COMPANY_TOWN_FIX = "5560001112"
# A postcode the register does not know: the row keeps Ratsit's own locality.
COMPANY_UNKNOWN_CODE = "5560002223"
# A postcode two register spellings tie on: the alphabetically first wins.
COMPANY_TIE = "5560003334"

OLD_SHA = "a" * 64
CURRENT_SHA = "b" * 64
UNKNOWN_SHA = "d" * 64
TIE_SHA = "e" * 64
RESCAN_SHA = "f" * 64

OLD_AT = "2026-09-01 00:00:00"
CURRENT_AT = "2026-09-02 00:00:00"
RESCAN_AT = "2026-09-06 00:00:00"
STAMP = datetime(2026, 9, 12, 12, 0, 0, tzinfo=UTC)

ROW_COLUMNS = (
    "company_id", "slot", "kind", "source_record_uid", "street_address", "postal_code",
    "post_town", "county", "raw_address", "care_of", "country_code", "observed_at",
    "extractor_version",
)
# ifNull(..., 'NULL') rather than the TSV \N marker: every address column is Nullable(String)
# and this prints the same text under join_use_nulls 0 and 1.
ROWS_SQL = (
    "SELECT company_id, slot, kind, source_record_uid, "
    "ifNull(street_address, 'NULL'), ifNull(postal_code, 'NULL'), ifNull(post_town, 'NULL'), "
    "ifNull(county, 'NULL'), ifNull(raw_address, 'NULL'), ifNull(care_of, 'NULL'), "
    "ifNull(country_code, 'NULL'), toString(observed_at), extractor_version "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL WHERE source = 'ratsit' "
    "ORDER BY company_id, slot"
)


def _schema() -> list[str]:
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                statements.append(statement)
    for path in FIXTURES:
        statements.extend(s.strip() for s in path.read_text(encoding="utf-8").split(";") if s.strip())
    return statements


def _run(statements: list[str], *, join_use_nulls: int) -> list[str]:
    script = f"SET join_use_nulls = {join_use_nulls};\n" + ";\n".join(statements) + ";\n"
    completed = subprocess.run(
        clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


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


def _scope() -> str:
    sql = _bind(
        changed_scope_sql(current_sql=ratsit.ratsit_current_sql(), target=ADDRESS_TARGET),
        source="ratsit",
        **ratsit.RATSIT_ADDRESS_SELECT_PARAMS,
    )
    return f"{sql}\nORDER BY company_id"


def _insert(ids: list[str]) -> str:
    return _bind(
        insert_page_sql(select_sql=ratsit.ratsit_select_sql(), target=ADDRESS_TARGET),
        company_ids=ids,
        source_run_id="run-1",
        extractor_version=ratsit.RATSIT_ADDRESS_EXTRACTOR_VERSION,
        **ratsit.RATSIT_ADDRESS_SELECT_PARAMS,
    )


def _scb(company_id: str, postal_code: str, post_town: str, has_company: int) -> str:
    return (
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, postal_code, post_town, "
        "has_company, observed_at, source_run_id, source_record_id, source_payload_hash) VALUES "
        f"('{company_id}', '{company_id}', '{postal_code}', '{post_town}', {has_company}, "
        "toDateTime64('2026-09-01 00:00:00', 3, 'UTC'), '', 's', 'h')"
    )


def _report(company_id: str, sha: str, normalized_at: str, street: str, code: str, locality: str, county: str) -> str:
    return (
        "INSERT INTO corpscout.se_ratsit_company (company_id, result_sha256, normalizer_version, schema_version, "
        "parser_version, requested_url, source_url, result_bucket, result_object_key, name, organization_number, "
        "address_street, address_postal_code, address_locality, address_county, normalized_at) VALUES "
        f"('{company_id}', '{sha}', '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', 'k', 'Test AB', "
        f"'{company_id[-10:]}', '{street}', '{code}', '{locality}', '{county}', "
        f"toDateTime64('{normalized_at}', 6, 'UTC'))"
    )


def _establishment(
    company_id: str, sha: str, normalized_at: str, index: int, identifier: str,
    street: str, code: str, locality: str, county: str,
) -> str:
    return (
        "INSERT INTO corpscout.se_ratsit_establishments (company_id, result_sha256, normalizer_version, "
        "establishment_index, name, identifier, address_street, address_postal_code, address_locality, "
        "address_county, normalized_at) VALUES "
        f"('{company_id}', '{sha}', '{RATSIT_NORMALIZER_VERSION}', {index}, 'Site {index}', '{identifier}', "
        f"'{street}', '{code}', '{locality}', '{county}', toDateTime64('{normalized_at}', 6, 'UTC'))"
    )


def _register_rows() -> list[str]:
    """238 40 -> OXIE (two spellings against one, and three has_company = 0 rows that would
    otherwise win with FELSTAD), 11122/111 43 -> STOCKHOLM, 11255 -> a two-way tie."""
    return [
        _scb("5570000001", "238 40", "OXIE", 1),
        _scb("5570000002", "23840", "OXIE", 1),
        _scb("5570000003", "238 40", " Oxie ", 1),
        _scb("5570000004", "11122", "STOCKHOLM", 1),
        _scb("5570000005", "111 43", "STOCKHOLM", 1),
        _scb("5570000006", "11255", "BETA", 1),
        _scb("5570000007", "11255", "ALFA", 1),
        _scb("5570000008", "23840", "FELSTAD", 0),
        _scb("5570000009", "23840", "FELSTAD", 0),
        _scb("5570000010", "23840", "FELSTAD", 0),
    ]


def _ratsit_rows() -> list[str]:
    return [
        # The superseded report and its establishment: neither may reach the suggestions.
        _report(COMPANY_TOWN_FIX, OLD_SHA, OLD_AT, "Gamlagatan 9", "23840", "Malmö", "Skåne län"),
        _establishment(COMPANY_TOWN_FIX, OLD_SHA, OLD_AT, 0, "EST-OLD", "Gamlagatan 9", "23840", "Oxie", "Skåne län"),
        # The current report: locality is the MUNICIPALITY, which the dictionary corrects.
        _report(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, "Oxievägen 12", "238 40", "Malmö", "Skåne län"),
        # 0: shares the company's postal street and postcode (the fold merges it).
        _establishment(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, 0, "EST-1", "Oxievägen 12", "238 40", "Oxie", "Skåne län"),
        # 1 and 3 repeat the identifier EST-2, so BOTH get the index appended.
        _establishment(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, 1, "EST-2", "Storgatan 1", "11122", "Stockholm", "Stockholms län"),
        # 2 has no street: skipped entirely, no slot, no row.
        _establishment(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, 2, "EST-3", "", "11122", "Stockholm", "Stockholms län"),
        _establishment(COMPANY_TOWN_FIX, CURRENT_SHA, CURRENT_AT, 3, "EST-2", "Kungsgatan 5", "111 43", "Stockholm", "Stockholms län"),
        _report(COMPANY_UNKNOWN_CODE, UNKNOWN_SHA, CURRENT_AT, "Ödevägen 3", "98765", "Ödeby", "Ödeby län"),
        _report(COMPANY_TIE, TIE_SHA, CURRENT_AT, "Tievägen 4", "11255", "Tievik", "Tie län"),
    ]


def _rescan_rows() -> list[str]:
    """A newer report for COMPANY_TOWN_FIX that keeps only EST-1: the two EST-2 slots must be
    tombstoned with the NEW report's observed_at."""
    return [
        _report(COMPANY_TOWN_FIX, RESCAN_SHA, RESCAN_AT, "Oxievägen 12", "238 40", "Malmö", "Skåne län"),
        _establishment(COMPANY_TOWN_FIX, RESCAN_SHA, RESCAN_AT, 0, "EST-1", "Oxievägen 12", "238 40", "Oxie", "Skåne län"),
    ]


def _statements() -> list[str]:
    return [
        *_schema(),
        *_register_rows(),
        *_ratsit_rows(),
        "SELECT '@@scope_1'",
        _scope(),
        _insert([COMPANY_TOWN_FIX, COMPANY_UNKNOWN_CODE, COMPANY_TIE]),
        "SELECT '@@rows_1'",
        ROWS_SQL,
        "SELECT '@@scope_after_1'",
        _scope(),
        # now64(3) resolves to milliseconds, so the second page must not tie the first page's
        # suggested_at -- the ReplacingMergeTree version column is suggested_at.
        "SELECT sleep(0.01) FORMAT Null",
        *_rescan_rows(),
        "SELECT '@@scope_2'",
        _scope(),
        _insert([COMPANY_TOWN_FIX]),
        "SELECT '@@rows_2'",
        ROWS_SQL,
        "SELECT '@@scope_after_2'",
        _scope(),
        "SELECT '@@changed_rows'",
        "SELECT * FROM (\n"
        + _bind(changed_rows_sql(), company_ids=[COMPANY_TOWN_FIX], normalizer_version=NORMALIZER_VERSION)
        + "\n) AS ordered ORDER BY slot",
    ]


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    return _sections(_run(_statements(), join_use_nulls=request.param))


def _by_slot(rows: list[list[str]]) -> dict[tuple[str, str], dict[str, str]]:
    return {
        (fields[0], fields[1]): dict(zip(ROW_COLUMNS, fields, strict=True))
        for fields in rows
    }
```

Append the test functions to the same file. **Every string below is the actual output of this
script against `clickhouse/clickhouse-server:26.5`, run while writing this plan under
`join_use_nulls` 0 and 1 — the two transcripts are identical.**

```python
def test_the_scope_selects_every_company_with_a_report_then_converges(
    sections: dict[str, list[list[str]]],
) -> None:
    assert sections["scope_1"] == [[COMPANY_TOWN_FIX], [COMPANY_UNKNOWN_CODE], [COMPANY_TIE]]
    assert sections["scope_after_1"] == []


def test_the_company_row_takes_the_registers_postal_town(sections: dict[str, list[list[str]]]) -> None:
    """Spec 5.1. `23840` has OXIE twice and ` Oxie ` once among the delivering register rows,
    so OXIE wins on frequency after the trim; the three FELSTAD rows carry `has_company = 0`
    and would win if the filter were missing. Ratsit's own locality here is the MUNICIPALITY
    (Malmö), which is what the entity published before this slice."""
    rows = _by_slot(sections["rows_1"])
    company = rows[(COMPANY_TOWN_FIX, "company")]
    assert company["kind"] == "postal"
    assert company["source_record_uid"] == f"ratsit:{CURRENT_SHA}"
    assert company["street_address"] == "Oxievägen 12"
    assert company["postal_code"] == "238 40"          # as delivered; only the join key is digits
    assert company["post_town"] == "OXIE"              # not "Malmö"
    assert company["county"] == "Skåne län"
    assert company["raw_address"] == company["care_of"] == company["country_code"] == "NULL"
    assert company["observed_at"] == "2026-09-02 00:00:00.000"
    assert company["extractor_version"] == "ratsit-address-v2"


def test_an_unknown_postcode_keeps_ratsits_locality_and_a_tie_takes_the_first_spelling(
    sections: dict[str, list[list[str]]],
) -> None:
    """Spec 5.1: `when the postcode is unknown to the register (none today) the row keeps
    Ratsit's locality`, and `ties broken by the alphabetically first spelling`."""
    rows = _by_slot(sections["rows_1"])
    assert rows[(COMPANY_UNKNOWN_CODE, "company")]["post_town"] == "Ödeby"
    assert rows[(COMPANY_TIE, "company")]["post_town"] == "ALFA"   # ALFA and BETA tie 1-1


def test_each_establishment_with_a_street_and_a_postcode_becomes_a_workplace_row(
    sections: dict[str, list[list[str]]],
) -> None:
    """Spec 5.2. Four establishments on the current report produce three rows: the streetless
    one is skipped, and the two that share the identifier EST-2 BOTH take the index suffix --
    a bare `est:EST-2` on either would collapse the pair in the ReplacingMergeTree."""
    rows = _by_slot(sections["rows_1"])
    assert sorted(slot for company_id, slot in rows if company_id == COMPANY_TOWN_FIX) == [
        "company", "est:EST-1", "est:EST-2:1", "est:EST-2:3",
    ]
    shared = rows[(COMPANY_TOWN_FIX, "est:EST-1")]
    assert shared["kind"] == "workplace"
    assert shared["source_record_uid"] == f"ratsit:{CURRENT_SHA}:est:0"
    # Same street and postcode as the company row: the fold merges the two (spec 5.4).
    assert (shared["street_address"], shared["postal_code"], shared["post_town"]) == (
        "Oxievägen 12", "238 40", "OXIE",
    )
    elsewhere = rows[(COMPANY_TOWN_FIX, "est:EST-2:1")]
    assert elsewhere["source_record_uid"] == f"ratsit:{CURRENT_SHA}:est:1"
    assert (elsewhere["street_address"], elsewhere["postal_code"], elsewhere["post_town"]) == (
        "Storgatan 1", "11122", "STOCKHOLM",
    )
    repeated = rows[(COMPANY_TOWN_FIX, "est:EST-2:3")]
    assert repeated["source_record_uid"] == f"ratsit:{CURRENT_SHA}:est:3"
    assert (repeated["street_address"], repeated["postal_code"], repeated["post_town"]) == (
        "Kungsgatan 5", "111 43", "STOCKHOLM",
    )
    # `EST-3` carried no street, and `EST-OLD` belongs to the superseded report.
    assert (COMPANY_TOWN_FIX, "est:EST-3") not in rows
    assert (COMPANY_TOWN_FIX, "est:EST-OLD") not in rows
    # Every row of the company carries the current report's stamp and hash.
    for company_id, slot in rows:
        if company_id == COMPANY_TOWN_FIX:
            assert rows[(company_id, slot)]["observed_at"] == "2026-09-02 00:00:00.000", slot
            assert OLD_SHA not in rows[(company_id, slot)]["source_record_uid"], slot


def test_a_rescan_that_drops_an_establishment_tombstones_its_slot(
    sections: dict[str, list[list[str]]],
) -> None:
    """Spec 5.3. The newer report keeps only EST-1, so both EST-2 slots get a NULL row that
    keeps the slot and the kind and carries the NEW report's observed_at -- if it carried the
    stored row's, argMax(observed_at, suggested_at) could pick the stale stamp out of the
    page's tie and re-select the company on every run for ever."""
    assert sections["scope_2"] == [[COMPANY_TOWN_FIX]]
    rows = _by_slot(sections["rows_2"])
    for slot in ("est:EST-2:1", "est:EST-2:3"):
        tombstone = rows[(COMPANY_TOWN_FIX, slot)]
        assert tombstone["kind"] == "workplace", slot
        assert tombstone["source_record_uid"] == "", slot
        assert tombstone["observed_at"] == "2026-09-06 00:00:00.000", slot
        for column in ("street_address", "postal_code", "post_town", "county",
                       "raw_address", "care_of", "country_code"):
            assert tombstone[column] == "NULL", (slot, column)
    survivor = rows[(COMPANY_TOWN_FIX, "est:EST-1")]
    assert survivor["source_record_uid"] == f"ratsit:{RESCAN_SHA}:est:0"
    assert survivor["observed_at"] == "2026-09-06 00:00:00.000"
    assert survivor["street_address"] == "Oxievägen 12"
    # The other two companies did not move.
    assert rows[(COMPANY_UNKNOWN_CODE, "company")]["observed_at"] == "2026-09-02 00:00:00.000"
    assert rows[(COMPANY_TIE, "company")]["observed_at"] == "2026-09-02 00:00:00.000"
    # And the scan converges: writing the tombstone takes the slot out of the live set.
    assert sections["scope_after_2"] == []


def test_the_normalize_hand_off_files_the_tombstones_as_no_address(
    sections: dict[str, list[list[str]]],
) -> None:
    """The normalizer is untouched by this slice; this proves the rows it now receives parse
    the way the fold needs. The corrected town reaches `city`, and a tombstone becomes
    `no_address`, which is what makes the fold drop that slot from the set (spec 5.3)."""
    rows = [
        tuple(None if field == "\\N" else field for field in fields)
        for fields in sections["changed_rows"]
    ]
    assert len(rows) == 4
    assert [row[2] for row in rows] == ["company", "est:EST-1", "est:EST-2:1", "est:EST-2:3"]
    by_slot = {row[2]: row for row in rows}
    for slot in ("company", "est:EST-1"):
        result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_slot[slot], STAMP), strict=True))
        assert result["parse_status"] == "ok", slot
        assert result["street_name"] == "oxievägen", slot
        assert result["house_number"] == "12", slot
        assert result["postal_code"] == "23840", slot
        assert result["city"] == "oxie", slot      # the register town, folded -- not "malmö"
        assert result["normalized_address"] == "Oxievägen 12, 238 40 Oxie", slot
    assert by_slot["company"][5] == "postal"
    assert by_slot["est:EST-1"][5] == "workplace"
    for slot in ("est:EST-2:1", "est:EST-2:3"):
        result = dict(zip(tables.NORMALIZED_COLUMNS, normalized_row(by_slot[slot], STAMP), strict=True))
        assert result["parse_status"] == "no_address", slot
        assert result["kind"] == "workplace", slot
        assert result["city"] is None and result["street_name"] is None, slot
```

- [ ] **Step 4: Run the integration tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
uv run pytest tests/test_se_company_address_ratsit_clickhouse_local.py \
  tests/test_se_company_address_extractors_clickhouse_local.py -v -m integration
```

Expected: both files pass, twice each (`join_use_nulls_off` and `join_use_nulls_on`). The first `docker run` pulls `clickhouse/clickhouse-server:26.5` if the machine has no `clickhouse-local` binary; each script takes well under the 900 s timeout. If `clickhouse-local` is missing AND docker is not running, both files `skip` — that is a machine problem, not a failure, but the task is not done until they have actually run.

- [ ] **Step 5: Write the fold's `workplace` test**

Append to `tests/test_se_company_address_fold.py` (it already has `row(...)`, `fold(...)`, `C`, `T1`):

```python
def test_a_workplace_establishment_at_the_postal_address_publishes_one_row() -> None:
    """Spec 5.4: `kinds` is the DISTINCT member kinds in member order, so the 288,840
    establishments that repeat the company's own postal street and postcode merge into the
    postal row instead of publishing a second address. The members tie on completeness, on
    source precedence and on suggested_at, so `_sort_key` falls through to the slot and
    `company` sorts before `est:EST-1`."""
    result = fold([row("ratsit", "company"), row("ratsit", "est:EST-1", kind="workplace")])
    assert (result.published, result.hidden, result.withdrawn) == (1, 0, 0)
    published = result.rows[0]
    assert published.kinds == ("postal", "workplace")
    assert published.sources == ("ratsit", "ratsit")
    assert published.slots == ("company", "est:EST-1")
    assert published.text_source == "ratsit"
    assert published.active == 1 and published.inactive_reason == ""


def test_a_workplace_establishment_elsewhere_publishes_its_own_row() -> None:
    """The other ~444k establishments: a different street is a different address, so the
    company publishes two rows and the workplace one geocodes on its own location key."""
    result = fold([
        row("ratsit", "company"),
        row(
            "ratsit", "est:EST-2", kind="workplace", street_name="kungsgatan", house_number="5",
            postal_code="11143", normalized_address="Kungsgatan 5, 111 43 Stockholm",
        ),
    ])
    assert (result.published, result.hidden, result.withdrawn) == (2, 0, 0)
    assert {published.kinds for published in result.rows} == {("postal",), ("workplace",)}
    assert {published.slots for published in result.rows} == {("company",), ("est:EST-2",)}
```

- [ ] **Step 6: Run the fold tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
uv run pytest tests/test_se_company_address_fold.py -v
```

Expected: every test passes, including the two new ones.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'MSG'
test(se-address): prove ratsit-address-v2 on a real ClickHouse

A clickhouse-local script over the SCB register, a Ratsit company whose
locality is the municipality and four establishments: the dictionary picks the
register town by frequency (has_company = 1 only) and alphabetically on a tie,
an unknown postcode keeps Ratsit's locality, the streetless establishment and
the superseded report's are skipped, a repeated identifier suffixes both slots,
and a second scan that drops an establishment tombstones its slot with the new
report's observed_at before the scope converges. Runs under join_use_nulls 0
and 1. Plus two fold unit tests for the workplace merge.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/services/dagster_v3/tests/fixtures/se_company_address_source_tables.sql \
        corpscout/services/dagster_v3/tests/test_se_company_address_ratsit_clickhouse_local.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_extractors_clickhouse_local.py \
        corpscout/services/dagster_v3/tests/test_se_company_address_fold.py
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```

---

### Task 3: The docs stop saying establishments come later

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md:124-126`
- Modify: `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md:41-44` and `:55`

**Interfaces:**
- Consumes: the names Task 1 produced (`ratsit-address-v2`, kind `workplace`, slots `company` and `est:<identifier>`).
- Produces: nothing code depends on.

- [ ] **Step 1: Find every "comes later" claim**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
rg -n "workplace" src/dagster_v3/defs/se_company/address/docs/address-design.md \
  docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md
```

Expected, before the edits: three hits, all in the 2026-09-06 spec — line 44 ("establishments come later as kind `workplace`"), line 55 ("Out of scope: the workplace extractor, …") and line 78 (the `kind` column comment, which is a list of allowed values and is **correct as it stands** — do not touch it). `address-design.md` has no hit, which is itself the problem: its `ratsit` bullet describes only the company row.

- [ ] **Step 2: Update the address package's design doc**

In `src/dagster_v3/defs/se_company/address/docs/address-design.md`, replace the `ratsit` bullet (lines 124-126):

```markdown
- `ratsit` reads `se_ratsit_company` FINAL, newest normalized report per company:
  `address_street`, `address_postal_code`, `address_locality`, `address_county`, kind
  `postal`, slot `company`.
```

with:

```markdown
- `ratsit` (`ratsit-address-v2`) reads `se_ratsit_company` FINAL and
  `se_ratsit_establishments` FINAL, newest normalized report per company: the company's
  `address_street`, `address_postal_code` and `address_county` as delivered, kind `postal`,
  slot `company`, plus one row per establishment of that report carrying a street and a
  postcode — kind `workplace`, slot `est:<identifier>` with the establishment index appended
  when a report repeats the identifier. `post_town` on every row comes from a postcode →
  town dictionary rebuilt per page from `se_scb_companies` FINAL (`has_company = 1`, the
  most frequent trimmed spelling per digits-only postcode, ties alphabetically), because
  Ratsit delivers the municipality as the locality on about 28% of company addresses and the
  normalizer's `city` is part of both `location_key` and `address_key`. A slot the newest
  report no longer delivers gets a NULL row through
  `suggestions.py::address_select_sql`, which pairs the live rows with per-slot tombstones
  stamped with the current report's `observed_at`; `scb` and `bolagsverket` keep their
  single-slot tombstone instead.
```

- [ ] **Step 3: Update the 2026-09-06 address spec**

In `docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md`, replace lines 41-44:

```markdown
- Sources in the first cut: SCB, Bolagsverket, Ratsit's company address, the reviewer. ESEF
  joined as a source in slice 3 of the 2026-09-08 ESEF design
  (`2026-09-08-esef-entity-link-people-addresses-design.md`), kind `registered`; Ratsit's
  establishments come later as kind `workplace`.
```

with:

```markdown
- Sources in the first cut: SCB, Bolagsverket, Ratsit's company address, the reviewer. ESEF
  joined as a source in slice 3 of the 2026-09-08 ESEF design
  (`2026-09-08-esef-entity-link-people-addresses-design.md`), kind `registered`; Ratsit's
  establishments joined in slice 3 of the 2026-09-11 Ratsit design
  (`2026-09-11-se-ratsit-source-design.md`), kind `workplace`, slot `est:<identifier>`, in
  the same slice that took the Ratsit postal town from the SCB register.
```

and line 55:

```markdown
Out of scope: the workplace extractor, other countries' normalizers (the normalizer is one
```

with:

```markdown
Out of scope for THIS spec (the workplace extractor shipped later, in the 2026-09-11 Ratsit
design's slice 3): other countries' normalizers (the normalizer is one
```

- [ ] **Step 4: Confirm nothing else claims the extractor is missing**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info/corpscout/services/dagster_v3
rg -n "come later|comes later|workplace extractor" docs/superpowers/specs src/dagster_v3/defs/se_company
```

Expected: no hit that refers to the address entity's establishments. (Hits from other entities' docs are not this slice's business.)

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'MSG'
docs(se-address): establishments are a workplace source, not a later slice

address-design.md's ratsit bullet describes ratsit-address-v2 (the register
postal-town dictionary, both kinds, both slots, the per-slot tombstone), and
the 2026-09-06 address spec stops calling the workplace extractor future work.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/services/dagster_v3/src/dagster_v3/defs/se_company/address/docs/address-design.md \
        corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-06-se-company-address-entity-design.md
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```

---

### Task 4: Prod run (controller)

**The controller runs this task; a task subagent never touches prod.** Every step is a Dagster run, a read-only `SELECT`, or a deploy the owner approves. Nothing here is a code change until Step 12.

**Files:**
- Modify: `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md` (the Shipped record under section 8 item 3, Step 12)
- Modify: this plan (ticked, Step 12)

**Interfaces:**
- Consumes: Tasks 1-3, merged to `main` and deployed. Assets: `se_company_address_suggestions_ratsit` (`ExtractConfig`: `execute`, `company_ids`, `max_companies`, `since`, `page_size` ≤ 20,000), `se_company_address_normalize` (`AddressNormalizeConfig`: `changed_only`, `company_ids`, `page_size` ≤ 50,000), `se_address_geocodes_warm` (`AddressWarmConfig`: `chunk_size` 10,000-5,000,000 default 150,000, `limit` default 0 = every key; pool `sweden_address_osm_duckdb`), `se_company_address_fold` (64 static partitions `bucket_00`..`bucket_63`, `AddressFoldConfig`: `changed_only` default true, `page_size` default 20,000; pool `sweden_address_osm_duckdb`, limit 1).
- Produces: the Shipped record under spec section 8 item 3.

- [ ] **Step 1: Review, merge, deploy**

1. Review the branch end to end: `git -C /Users/graovic/pulsarpoint/ppoint/companycollect/.claude/worktrees/se-basic-info diff main...se-ratsit-addresses`.
2. The owner merges to `main`. If the main checkout sits on another branch, merge through a worktree that has `main` checked out (memory `se-worktree-deploy-recipe`).
3. Deploy the dagster host from a **pristine worktree at the merge commit** — `light_sync` rsyncs the working tree with `--delete-after`, so a dirty tree ships someone else's WIP:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git worktree add /tmp/deploy-worktree HEAD
cp corpscout/services/dagster_v3/.env /tmp/deploy-worktree/corpscout/services/dagster_v3/.env
cd /tmp/deploy-worktree/corpscout/services/dagster_v3
uv sync --frozen
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/finland_ytj/dbt --profiles-dir src/dagster_v3/defs/finland_ytj/dbt
uv run --frozen --no-sync dbt parse --project-dir src/dagster_v3/defs/exchange_rates_v2/dbt --profiles-dir src/dagster_v3/defs/exchange_rates_v2/dbt
uv run --frozen --no-sync dg utils refresh-defs-state
uv run --frozen --no-sync dg check defs
cd ansible && ANSIBLE_BECOME_TIMEOUT=60 ansible-playbook -i inventory.ini light_sync.yml; echo "RC=$?"
```

The dbt-state refresh is **mandatory** and `RC=` is captured explicitly (piping the playbook to `tail` masks its exit code). The missing `.env` copy fails `dg check defs` with a cryptic YAML/column error.

4. Prove the host carries the rewritten asset and its new parents:

```bash
cat > /tmp/address-nodes.json <<'JSON'
{"query":"query Nodes { assetNodes(assetKeys: [{path: [\"se_company_address_suggestions_ratsit\"]}]) { assetKey { path } description dependencyKeys { path } } }"}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/address-nodes.json | python3 -m json.tool
```

Expected: one node whose `dependencyKeys` are exactly `se_ratsit_company`, `se_ratsit_establishments` and `sweden_company_scb_companies_clickhouse` — **no `se_ratsit_normalized`** (the phantom the v1 module carried) — and whose description mentions the workplace rows and the register dictionary.

- [ ] **Step 2: Confirm prod's state before anything runs**

```bash
cat > /tmp/instigators.json <<'JSON'
{"query":"query Instigators($repositorySelector: RepositorySelector!) { schedulesOrError(repositorySelector: $repositorySelector) { __typename ... on Schedules { results { name cronSchedule scheduleState { status } } } } }","variables":{"repositorySelector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__"}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/instigators.json \
  | python3 -c "import json,sys; [print(s['name'], s['cronSchedule'], s['scheduleState']['status']) for s in json.load(sys.stdin)['data']['schedulesOrError']['results'] if 'se_company' in s['name']]"
```

Expected: `se_company_address_weekly 5 7 * * 1 STOPPED`, and the basic-info and person weeklies STOPPED too. **RUNNING anywhere here means stop and tell the owner** (spec section 6).

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
-- (a) the address entity, before
SELECT
    count()                                              AS rows,
    countIf(active = 1)                                  AS active_rows,
    uniqExact(company_id)                                AS companies,
    countIf(has(sources, 'ratsit'))                      AS ratsit_rows,
    countIf(has(kinds, 'workplace'))                     AS workplace_rows,
    countIf(inactive_reason = 'withdrawn')               AS withdrawn_rows
FROM corpscout.se_company_address FINAL;

-- (b) the suggestion and normalized layers, by source
SELECT source, count() AS rows, uniqExact(company_id) AS companies, uniqExact(slot) AS slots,
       countIf(street_address IS NULL) AS null_rows
FROM corpscout.se_company_address_suggestion FINAL GROUP BY source ORDER BY source;

SELECT source, parse_status, count() AS rows
FROM corpscout.se_company_address_normalized FINAL GROUP BY source, parse_status ORDER BY source, rows DESC;

-- (c) what the extractor is about to deliver: the current report per company, its company
--     rows and its qualifying establishments
WITH report AS (
    SELECT c.company_id AS company_id, c.result_sha256 AS result_sha256,
           c.normalizer_version AS normalizer_version,
           ifNull(trim(c.address_street), '') AS street,
           replaceRegexpAll(ifNull(c.address_postal_code, ''), '[^0-9]', '') AS code
    FROM corpscout.se_ratsit_company AS c FINAL
    WHERE c.normalizer_version = 'ratsit-normalizer-v2'
    ORDER BY c.normalized_at DESC, c.result_sha256 DESC
    LIMIT 1 BY c.company_id
)
SELECT
    count()                                   AS company_rows,
    countIf(street != '' AND code != '')      AS company_rows_with_an_address
FROM report;

WITH report AS (
    SELECT c.company_id AS company_id, c.result_sha256 AS result_sha256,
           c.normalizer_version AS normalizer_version
    FROM corpscout.se_ratsit_company AS c FINAL
    WHERE c.normalizer_version = 'ratsit-normalizer-v2'
    ORDER BY c.normalized_at DESC, c.result_sha256 DESC
    LIMIT 1 BY c.company_id
)
SELECT
    count()                                              AS establishment_rows,
    uniqExact(e.company_id)                              AS companies,
    max(cnt)                                             AS max_per_company
FROM (
    SELECT e.company_id AS company_id, e.establishment_index AS establishment_index,
           count() OVER (PARTITION BY e.company_id) AS cnt
    FROM corpscout.se_ratsit_establishments AS e FINAL
    INNER JOIN report ON report.company_id = e.company_id
        AND report.result_sha256 = e.result_sha256
        AND report.normalizer_version = e.normalizer_version
    WHERE trim(ifNull(e.address_street, '')) != '' AND trim(ifNull(e.address_postal_code, '')) != ''
) AS e;

-- (d) the dictionary the page will build, and whether any Ratsit postcode is unknown to it
WITH towns AS (
    SELECT replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') AS postal_code_digits,
           trim(ifNull(post_town, '')) AS town
    FROM corpscout.se_scb_companies FINAL
    WHERE has_company = 1
      AND replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') != ''
      AND trim(ifNull(post_town, '')) != ''
    GROUP BY postal_code_digits, town
    ORDER BY count() DESC, town
    LIMIT 1 BY postal_code_digits
)
SELECT count() AS postcodes_in_the_dictionary FROM towns;

-- (e) how many company rows the town fix actually moves
WITH towns AS (
    SELECT replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') AS postal_code_digits,
           trim(ifNull(post_town, '')) AS town
    FROM corpscout.se_scb_companies FINAL
    WHERE has_company = 1
      AND replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') != ''
      AND trim(ifNull(post_town, '')) != ''
    GROUP BY postal_code_digits, town
    ORDER BY count() DESC, town
    LIMIT 1 BY postal_code_digits
),
report AS (
    SELECT c.company_id AS company_id,
           replaceRegexpAll(ifNull(c.address_postal_code, ''), '[^0-9]', '') AS code,
           trim(ifNull(c.address_locality, '')) AS locality
    FROM corpscout.se_ratsit_company AS c FINAL
    WHERE c.normalizer_version = 'ratsit-normalizer-v2'
    ORDER BY c.normalized_at DESC, c.result_sha256 DESC
    LIMIT 1 BY c.company_id
)
SELECT
    countIf(towns.town = '')                                        AS postcode_unknown_to_the_register,
    countIf(towns.town != '' AND lowerUTF8(towns.town) != lowerUTF8(report.locality)) AS town_corrected,
    countIf(towns.town != '' AND lowerUTF8(towns.town) = lowerUTF8(report.locality)) AS town_already_right
FROM report LEFT JOIN towns ON towns.postal_code_digits = report.code
WHERE report.code != '';
SQL
```

Expected, against spec section 2 (prod 2026-09-10): (a) `rows` ≈ 3,761,803 / `companies` ≈ 3,516,836, `ratsit_rows` the 83,696 the v1 extractor produced, `workplace_rows` **0**; (b) `ratsit` has 83,696 rows in one slot (`company`) and no NULL rows; (c) ≈947,200 company rows, ≈928,560 of them with a street and a postcode, and ≈732,626 establishment rows over ≈766,313 companies with `max_per_company` 1,718; (d) ≈15,698 postcodes; (e) `postcode_unknown_to_the_register` **0** ("every Ratsit postcode is known to the register"), `town_corrected` ≈260,862.

**If (e) reports a non-zero `postcode_unknown_to_the_register`, that is new since 2026-09-10 and not a reason to stop** — those rows keep Ratsit's locality by design; record the number. **If `town_corrected` is near zero or above 500,000, stop and reconcile before writing anything.**

Record (a), (b) and (c): the AFTER steps subtract from these numbers, not from the spec's.

- [ ] **Step 3: Preview the extract with `since`**

Spec 5.6: the town fix changes rows **without moving `observed_at`**, so the change scan alone would skip the 83,696 already-visited companies. `since: "2000-01-01T00:00:00Z"` puts every company with a report in scope. The asset's default is a preview (`execute: false`): the same scope and the same per-page count, nothing written.

```bash
cat > /tmp/ratsit-address-preview.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { pipelineName errors { message path reason } } ... on PythonError { message } ... on InvalidSubsetError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_company_address_suggestions_ratsit"]}]},"runConfigData":{"ops":{"se_company_address_suggestions_ratsit":{"config":{"execute":false,"page_size":10000,"since":"2000-01-01T00:00:00Z"}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/ratsit-address-preview.json | python3 -m json.tool
```

Expected: `"__typename": "LaunchRunSuccess"` with a `runId`. Poll it (one-shot polls every 60 s, never a long-lived local loop — memory `se-person-entity`: local pollers get OOM-killed during long runs):

```bash
cat > /tmp/run.json <<'JSON'
{"query":"query Run($runId: ID!) { runOrError(runId: $runId) { __typename ... on Run { runId status startTime endTime } } }","variables":{"runId":"REPLACE_WITH_RUN_ID"}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/run.json | python3 -m json.tool
```

Then read the materialization metadata:

```bash
cat > /tmp/address-mat.json <<'JSON'
{"query":"query Mat($assetKeys: [AssetKeyInput!]!, $limit: Int!) { assetNodes(assetKeys: $assetKeys) { id assetMaterializations(limit: $limit) { runId timestamp metadataEntries { label __typename ... on IntMetadataEntry { intValue } ... on TextMetadataEntry { text } ... on BoolMetadataEntry { boolValue } ... on FloatMetadataEntry { floatValue } } } } }","variables":{"assetKeys":[{"path":["se_company_address_suggestions_ratsit"]}],"limit":1}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/address-mat.json | python3 -m json.tool
```

Expected (`ExtractCounts.as_metadata`): `execute` false, `companies` ≈ **947,200** (Step 2(c)'s company rows), `pages` **95** (ceiling of companies / 10,000), `candidates` ≈ **1,679,800** (≈947,200 company rows + ≈732,600 establishment rows; there are tombstones only for the 83,696 previously visited companies, which deliver the same `company` slot and therefore produce none), `inserted` **0**, `stopped_at_cap` false.

**If `candidates` is under 1,200,000 or over 2,200,000, stop and reconcile against Step 2(c) before writing anything.** The preview runs the full select once per page, so it costs roughly half of the execute; expect tens of minutes, not seconds.

- [ ] **Step 4: Execute the extract**

The same launch with the gate open — only `"execute": true` differs.

```bash
cat > /tmp/ratsit-address-execute.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { pipelineName errors { message path reason } } ... on PythonError { message } ... on InvalidSubsetError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_company_address_suggestions_ratsit"]}]},"runConfigData":{"ops":{"se_company_address_suggestions_ratsit":{"config":{"execute":true,"page_size":10000,"since":"2000-01-01T00:00:00Z"}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/ratsit-address-execute.json | python3 -m json.tool
```

Poll with `/tmp/run.json` until `SUCCESS`, then re-read the metadata. Expected: `execute` true, `companies` and `candidates` as in the preview, `inserted` **equal to `candidates`**, `stopped_at_cap` false (the cap is 5,000,000 companies). Record the wall time — the basic-info Ratsit run wrote 863,504 rows over 87 pages in 4 minutes; this one writes about twice as many rows through a much heavier select (two report picks, an establishments join and the 1.8M-row dictionary per page), so budget 30-120 minutes.

- [ ] **Step 5: Read out the suggestions and prove convergence**

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
-- the shape of what landed
SELECT
    count()                                          AS rows,
    uniqExact(company_id)                            AS companies,
    countIf(kind = 'postal')                         AS postal_rows,
    countIf(kind = 'workplace')                      AS workplace_rows,
    countIf(street_address IS NULL)                  AS null_rows,
    countIf(slot = 'company')                        AS company_slots,
    countIf(startsWith(slot, 'est:'))                AS establishment_slots,
    countIf(match(slot, '^est:.*:[0-9]+$'))          AS index_suffixed_slots,
    countIf(raw_address IS NOT NULL)                 AS raw_address_rows,
    countIf(care_of IS NOT NULL)                     AS care_of_rows,
    uniqExact(extractor_version)                     AS versions,
    any(extractor_version)                           AS version
FROM corpscout.se_company_address_suggestion FINAL
WHERE source = 'ratsit';

-- the town fix, as stored
SELECT count() AS rows_whose_town_is_a_known_register_town
FROM corpscout.se_company_address_suggestion AS s FINAL
INNER JOIN (
    SELECT replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') AS postal_code_digits,
           trim(ifNull(post_town, '')) AS town
    FROM corpscout.se_scb_companies FINAL
    WHERE has_company = 1
      AND replaceRegexpAll(ifNull(postal_code, ''), '[^0-9]', '') != ''
      AND trim(ifNull(post_town, '')) != ''
    GROUP BY postal_code_digits, town
    ORDER BY count() DESC, town
    LIMIT 1 BY postal_code_digits
) AS towns ON towns.postal_code_digits = replaceRegexpAll(ifNull(s.postal_code, ''), '[^0-9]', '')
WHERE s.source = 'ratsit' AND s.post_town = towns.town;

-- the two named samples of spec 5.6
SELECT company_id, slot, kind, street_address, postal_code, post_town
FROM corpscout.se_company_address_suggestion FINAL
WHERE source = 'ratsit' AND postal_code IS NOT NULL
  AND replaceRegexpAll(postal_code, '[^0-9]', '') IN ('23840', '23841', '23842')
LIMIT 5;

SELECT company_id, slot, kind, street_address, postal_code, post_town
FROM corpscout.se_company_address_suggestion FINAL
WHERE source = 'ratsit' AND post_town ILIKE 'Bromma'
LIMIT 5;
SQL
```

Expected: `rows` equal to Step 4's `inserted`, `companies` ≈ 947,200; `postal_rows` ≈ 947,200 and `workplace_rows` ≈ 732,600; `null_rows` **0** on this first run (every previously stored Ratsit slot was `company`, and every company still delivers one); `index_suffixed_slots` ≈ **324**; `raw_address_rows` and `care_of_rows` **0** (global constraint); `versions` 1 and `version` `ratsit-address-v2`. The Oxie sample must show `post_town` Oxie (not Malmö) and the Bromma sample must exist at all (before this run those rows said Stockholm).

Then re-run the **preview of Step 3 without `since`** — the plain change scan — and expect `companies` **0**:

```bash
cat > /tmp/ratsit-address-converge.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on PythonError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_company_address_suggestions_ratsit"]}]},"runConfigData":{"ops":{"se_company_address_suggestions_ratsit":{"config":{"execute":false,"page_size":10000}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/ratsit-address-converge.json | python3 -m json.tool
```

Expected: `companies` **0**, `pages` **0**, `candidates` **0**. A non-zero count means the stamp the select writes and the stamp `ratsit_current_sql` reports disagree — exactly the bug this slice removed from the translation-aware `current_sql`. Find out which before normalizing.

- [ ] **Step 6: Normalize**

```bash
cat > /tmp/address-normalize.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { pipelineName errors { message path reason } } ... on PythonError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_company_address_normalize"]}]},"runConfigData":{"ops":{"se_company_address_normalize":{"config":{"changed_only":true}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/address-normalize.json | python3 -m json.tool
```

Poll with `/tmp/run.json`; read the metadata with `/tmp/address-mat.json` after swapping the asset key to `se_company_address_normalize`.

Expected (`NormalizeCounts.as_metadata`): `companies` ≈ 947,200, `rows` ≈ 1,679,800 (every Ratsit row is new or newer than its normalized row — the 83,696 old ones got a fresh `suggested_at`), `ok` the large majority, `no_address` ≈ 18,700 (the company rows with no street), `partial` small, `foreign` ~0, `normalizer_version` `se-address-normalizer-v3` unchanged. `changed_only: true` means the other three sources' rows are not re-normalized; **if `rows` comes back near 3.9M the flag did not bite — stop.**

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
SELECT source, parse_status, count() AS rows
FROM corpscout.se_company_address_normalized FINAL
WHERE source = 'ratsit' GROUP BY source, parse_status ORDER BY rows DESC;

SELECT kind, count() AS rows
FROM corpscout.se_company_address_normalized FINAL
WHERE source = 'ratsit' GROUP BY kind ORDER BY rows DESC;

-- the whole point of the slice: the Ratsit company row and the register row now agree on
-- the city, so they share an address_key instead of folding apart.
SELECT count() AS ratsit_rows_sharing_an_scb_address_key
FROM corpscout.se_company_address_normalized AS r FINAL
INNER JOIN (
    SELECT company_id, address_key FROM corpscout.se_company_address_normalized FINAL
    WHERE source = 'scb' AND parse_status IN ('ok', 'partial')
) AS s ON s.company_id = r.company_id AND s.address_key = r.address_key
WHERE r.source = 'ratsit' AND r.slot = 'company' AND r.parse_status IN ('ok', 'partial');
SQL
```

Expected: `kind` splits into `postal` ≈947,200 and `workplace` ≈732,600; the shared-key count is the number to watch — it was small before the slice and should now be in the hundreds of thousands. Record the before value by running the same query **before Step 4** if the controller wants the exact delta; otherwise record only the after number and say so.

- [ ] **Step 7: Warm the geocode cache**

New location keys (every establishment somewhere new, plus the corrected-town company rows whose key now equals the register's) must be matched in bulk before the fold, or every fold page pays the matcher.

```bash
cat > /tmp/address-warm.json <<'JSON'
{"query":"mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { runId status } } ... on RunConfigValidationInvalid { pipelineName errors { message path reason } } ... on PythonError { message } } }","variables":{"executionParams":{"selector":{"repositoryLocationName":"dagster_v3","repositoryName":"__repository__","jobName":"__ASSET_JOB","assetSelection":[{"path":["se_address_geocodes_warm"]}]},"runConfigData":{"ops":{"se_address_geocodes_warm":{"config":{"chunk_size":150000,"limit":0}}}},"mode":"default","executionMetadata":{"tags":[]}}}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/address-warm.json | python3 -m json.tool
```

`chunk_size` is the asset's config key (`AddressWarmConfig.chunk_size`, default already 150,000 — it is spelled out so the run record says which number it used); `limit: 0` means every key. The asset takes the `sweden_address_osm_duckdb` pool, so it will not start while a fold holds it.

Poll with `/tmp/run.json`; read the metadata with the asset key `se_address_geocodes_warm`. Expected (`WarmCounts.as_metadata` plus `chunk_size`/`limit`): `keys` above the 2.08M of the last full warm (the establishments add new locations), `chunks` = ceil(keys / 150,000), `cache_hits` the large majority (the corrected-town company rows hit the register rows' cached keys — that is the whole point of the town fix), `matched` the genuinely new locations, `geocoded` + `fallback` = `matched`. The last full warm took 43 minutes for 2.08M keys; budget an hour.

**Also check the freshness asset check** that runs with it (`osm_snapshot_fresh`): a WARN means the OSM extract is over nine days old, which is worth telling the owner but is not a reason to stop the fold.

- [ ] **Step 8: Back-fill the fold over all 64 buckets**

`se_company_address_fold` is `StaticPartitionsDefinition(["bucket_00" … "bucket_63"])` with `BackfillPolicy.multi_run(max_partitions_per_run=1)` and pool `sweden_address_osm_duckdb` (instance default limit 1), so the backfill produces one run per partition and the pool serializes them. A backfill carries no run config, which is what the defaults want: `changed_only: true` (the fold's watermark sees the newer normalized rows), `page_size: 20000`.

```bash
python3 - <<'PY' > /tmp/address-fold-backfill.json
import json
query = (
    "mutation LaunchBackfill($backfillParams: LaunchBackfillParams!) {"
    " launchPartitionBackfill(backfillParams: $backfillParams) { __typename"
    " ... on LaunchBackfillSuccess { backfillId }"
    " ... on PartitionSetNotFoundError { message }"
    " ... on PythonError { message } } }"
)
params = {
    "partitionNames": [f"bucket_{i:02d}" for i in range(64)],
    "assetSelection": [{"path": ["se_company_address_fold"]}],
    "fromFailure": False,
    "tags": [],
}
print(json.dumps({"query": query, "variables": {"backfillParams": params}}))
PY
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/address-fold-backfill.json | python3 -m json.tool
```

Expected: `LaunchBackfillSuccess` and a `backfillId`. Record it, and record the UTC instant the first run starts — Step 9(e) needs it. Poll the runs by tag (one-shot per tick, every 5-10 minutes):

```bash
cat > /tmp/backfill-runs.json <<'JSON'
{"query":"query BackfillRuns($backfillId: String!) { runsOrError(filter: {tags: [{key: \"dagster/backfill\", value: $backfillId}]}) { __typename ... on Runs { results { runId status tags { key value } } } } }","variables":{"backfillId":"REPLACE_WITH_BACKFILL_ID"}}
JSON
ssh dagster "curl -s -H 'content-type: application/json' --data-binary @- http://localhost:3000/graphql" < /tmp/backfill-runs.json \
  | python3 -c "
import collections, json, sys
runs = json.load(sys.stdin)['data']['runsOrError']['results']
print(len(runs), 'runs:', dict(collections.Counter(r['status'] for r in runs)))
for r in runs:
    if r['status'] not in ('SUCCESS', 'STARTED', 'STARTING', 'QUEUED'):
        print('  !', r['runId'], r['status'], {t['key']: t['value'] for t in r['tags'] if t['key'] == 'dagster/partition'})
"
```

**Read the first finished bucket's metadata before the rest complete** (the limit-1 pool gives a natural checkpoint) — `/tmp/address-mat.json` with the asset key swapped to `se_company_address_fold`.

Expected per bucket (`FoldCounts.as_metadata` plus the asset's `bucket`, `changed_only`, `page_size`, `table`, `history_table`): `changed_only` true, `page_size` 20000, `considered` ≈ 947,200 / 64 ≈ **14,800**, `published` the rows the bucket wrote, `withdrawn` substantial (spec 5.4: the old municipality-town Ratsit-only sets are withdrawn by the whole-set rewrite), `changed` the history rows, `cache_hits` the large majority of `geocoded` (Step 7 warmed them). **If `considered` comes back near 55,000 (the whole bucket, as in address slice 4a's first fold) the run is re-folding everything — stop, `changed_only` was not honoured.**

Expect 64/64 `SUCCESS`. The last full address backfill ran ~74 s per bucket over a much smaller changed set; this one folds ~14,800 companies per bucket with up to 1,718 members each, so budget 2-6 hours for all 64 and record the real total. The 131 companies with over 100 establishments cost seconds, not minutes (spec section 6).

- [ ] **Step 9: Read out the entity (the spec 5.6 numbers)**

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
-- (a) the same shape as Step 2(a): compare line by line
SELECT
    count()                                              AS rows,
    countIf(active = 1)                                  AS active_rows,
    uniqExact(company_id)                                AS companies,
    countIf(has(sources, 'ratsit'))                      AS ratsit_rows,
    countIf(has(kinds, 'workplace'))                     AS workplace_rows,
    countIf(inactive_reason = 'withdrawn')               AS withdrawn_rows
FROM corpscout.se_company_address FINAL;

-- (b) the kind combinations: the 288,840 establishments that repeat the company's postal
--     address must show up as ['postal','workplace'] rows, not as a second address
SELECT arrayStringConcat(kinds, '+') AS kind_combo, count() AS rows
FROM corpscout.se_company_address FINAL
WHERE active = 1 GROUP BY kind_combo ORDER BY rows DESC LIMIT 20;

-- (c) the source combinations
SELECT arrayStringConcat(arraySort(arrayDistinct(sources)), '+') AS combo, count() AS rows
FROM corpscout.se_company_address FINAL
WHERE active = 1 GROUP BY combo ORDER BY rows DESC LIMIT 20;

-- (d) addresses per company, before/after comparison material
SELECT countIf(n = 1) AS one, countIf(n = 2) AS two, countIf(n BETWEEN 3 AND 9) AS few,
       countIf(n >= 10) AS many, max(n) AS most
FROM (
    SELECT company_id, count() AS n FROM corpscout.se_company_address FINAL
    WHERE active = 1 GROUP BY company_id
);

-- (e) history written by this backfill (swap in the instant recorded in Step 8)
SELECT inactive_reason, count() AS rows
FROM corpscout.se_company_address_history
WHERE folded_at >= toDateTime64('REPLACE_WITH_BACKFILL_START', 3, 'UTC')
GROUP BY inactive_reason ORDER BY rows DESC;

-- (f) the Malmö/Oxie sample: a Ratsit company row whose town was the municipality must now
--     sit in the SAME published row as the register address, not beside it
SELECT company_id, normalized_address, kinds, sources, slots, text_source, geocode_status
FROM corpscout.se_company_address FINAL
WHERE active = 1 AND city = 'oxie' AND has(sources, 'ratsit') AND has(sources, 'scb')
ORDER BY cityHash64(company_id) LIMIT 10;

-- (g) the Stockholm/Bromma sample, same test
SELECT company_id, normalized_address, kinds, sources, slots, text_source, geocode_status
FROM corpscout.se_company_address FINAL
WHERE active = 1 AND city = 'bromma' AND has(sources, 'ratsit') AND has(sources, 'scb')
ORDER BY cityHash64(company_id) LIMIT 10;

-- (h) a company with many establishments: one postal row plus its workplaces
SELECT company_id, count() AS addresses, countIf(has(kinds, 'workplace')) AS workplaces
FROM corpscout.se_company_address FINAL
WHERE active = 1 AND has(sources, 'ratsit')
GROUP BY company_id ORDER BY addresses DESC LIMIT 5;

-- (i) geocoding: the workplace rows must not be a wall of misses
SELECT geocode_status, count() AS rows
FROM corpscout.se_company_address FINAL
WHERE active = 1 AND has(kinds, 'workplace') GROUP BY geocode_status ORDER BY rows DESC;
SQL
```

Acceptance, against Step 2(a):

- `ratsit_rows` grows from ≈83,696 to several hundred thousand; `workplace_rows` goes **0 → a few hundred thousand**.
- (b) must contain both `postal+workplace` (or the register kinds first, e.g. `visiting_or_postal+postal+workplace`) and bare `workplace` combos — the first are the 288,840 shared-address establishments, the second the ones elsewhere.
- (f) and (g) must each return rows where `sources` contains **both** `ratsit` and `scb` in ONE published row. A company appearing twice with two `city` spellings in those towns means the dictionary did not apply — stop and check Step 5's readout.
- (e) is dominated by `''` (re-published rows) and `withdrawn`; the withdrawn count is expected to be large (the old municipality-town sets) and is one of the numbers spec 5.6 asks for.
- (i): most workplace rows should carry a matcher status rather than a miss, because Step 7 warmed them.

- [ ] **Step 10: Let the serving refresh land**

`corpscout.se_companies_serving` is a refreshable materialized view (hourly, migration 000392 for the address block). `has_address`, `address_count` and the `addresses` JSON all read `se_company_address FINAL`.

```bash
ssh companycollect 'docker exec -i clickhouse-clickhouse-1 clickhouse-client --database corpscout --format PrettyCompact' <<'SQL'
SELECT
    count()                          AS companies,
    countIf(has_address = 1)         AS with_an_address,
    sum(address_count)               AS total_addresses,
    max(address_count)               AS most_addresses,
    countIf(addresses LIKE '%workplace%') AS rows_naming_a_workplace
FROM corpscout.se_companies_serving;

SELECT view_definition_hash, last_refresh_time, last_success_time, exception
FROM system.view_refreshes WHERE view = 'se_companies_serving';
SQL
```

Expected: `with_an_address` roughly unchanged (Ratsit companies mostly already had an SCB address), `total_addresses` up by the establishments that publish on their own, `rows_naming_a_workplace` above zero, and `exception` empty on the refresh that ran after the backfill finished. A refresh that overlaps a half-finished backfill still succeeds; the next hour's carries the rest (the same thing happened in slice 1).

- [ ] **Step 11: Smoke the Address tab**

On the owner's dev server (`localhost:5183`, the **main** checkout — the backoffice is not deployed; memory `backoffice-runs-locally`), open the Address tab for a company from Step 9(h) (many establishments) and one from Step 9(f) (the Oxie sample):

- the establishment rows render with the **Workplace** kind label (`app/lib/se-address-fields.ts` already maps it — no code change shipped in this slice);
- the Oxie company shows ONE address whose sources list both `scb` and `ratsit`, not two near-duplicates;
- the map pin for a workplace row is where the geocoder put it, and the row's slot reads `est:<identifier>`.

Record what was seen. If the Workplace label is missing or the kind renders raw, that is a backoffice bug to raise with the owner — not something this slice fixes.

- [ ] **Step 12: Write the Shipped record and tick the plan**

Append to `corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md`, under section 8 item 3 (which today ends at "prod re-extract with `since`, normalize, warm, fold."), in the same voice as items 1 and 2: the plan file name and the merge commit; what the code change was; the review outcome; then the prod numbers — the preview's companies/pages/candidates, the execute's inserted and wall time, the convergence preview, the suggestion readout (postal/workplace rows, index-suffixed slots, null rows), the normalize counts by `parse_status` and `kind`, the warm's keys/chunks/cache_hits/matched, the backfill id with 64/64 and the wall time, the entity before → after for `ratsit_rows`, `workplace_rows`, `withdrawn_rows` and addresses per company, the Oxie and Bromma samples, the serving numbers, and the Address-tab smoke. Close with any finding worth carrying (as slice 1 did with the Bolagsverket-description duplication) and confirm the weeklies stayed STOPPED.

Then tick every checkbox in this plan.

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
MSGFILE=$(mktemp)
cat > "$MSGFILE" <<'MSG'
docs(spec): record the Ratsit address slice's prod run

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01RY2W9FTCX9YxUcXtSBaEJ5
MSG
git add corpscout/services/dagster_v3/docs/superpowers/specs/2026-09-11-se-ratsit-source-design.md \
        corpscout/services/dagster_v3/docs/superpowers/plans/2026-09-12-se-ratsit-3-addresses.md
git commit -F "$MSGFILE"
rm -f "$MSGFILE"
```

---

## Self-review

**1. Spec coverage (section 5, plus the facts and rulings it rests on)**

| spec requirement | where it lands |
| --- | --- |
| 5.1 a `towns(postal_code, post_town)` dictionary subquery from `corpscout.se_scb_companies FINAL` with `has_company = 1` | `TOWNS_SQL`, Task 1 Step 4; pinned verbatim in Task 1 Step 1; proved on a real engine by Task 2's three `FELSTAD` rows, which would win without the filter |
| 5.1 the postcode with spaces removed, the most frequent trimmed `post_town`, ties alphabetically, `LIMIT 1 BY postal_code` after `ORDER BY count() DESC, post_town` | `TOWNS_SQL`'s `GROUP BY` + `ORDER BY count() DESC, town` + `LIMIT 1 BY postal_code_digits`; the frequency case (`OXIE` twice vs ` Oxie ` once) and the tie case (`ALFA`/`BETA`) are separate assertions in Task 2 |
| 5.1 every Ratsit row's `post_town` is the dictionary town; an unknown postcode keeps Ratsit's locality | `POST_TOWN_SQL`, applied to BOTH branches through the single outer projection; `test_an_unknown_postcode_keeps_ratsits_locality_and_a_tie_takes_the_first_spelling` |
| 5.1 recomputed per page, no new table, no migration | `TOWNS_SQL` is a subquery inside the page select; Global Constraints; Task 2 needs no migration beyond the three already replayed |
| 5.1 `deps` = `se_ratsit_company`, `se_ratsit_establishments`, `sweden_company_scb_companies_clickhouse`; the `se_ratsit_normalized` phantom goes | Task 1 Step 4's `deps` list with the comment; `test_the_ratsit_address_asset_reads_the_two_ratsit_tables_and_the_scb_register`; Task 4 Step 1(4) reads the deps back off prod |
| 5.1 the lineage ruling: a register source table, never another extractor's output nor the fold | the same test asserts `se_company_basic_info_fold` and `se_company_address_fold` are absent |
| 5.2 the company row: slot `company`, kind `postal`, uid `concat('ratsit:', toString(result_sha256))`, street/postcode/county as delivered (trimmed, empty → NULL), dictionary town, `raw_address` and `care_of` NULL | `ratsit_rows_sql`'s first branch + `ratsit_live_sql`'s projection; Task 1 Step 1 items (2), (3), (5); Task 2's `test_the_company_row_takes_the_registers_postal_town` |
| 5.2 one row per establishment with a non-empty street AND postcode | `ratsit_establishments_sql`'s WHERE; `test_each_establishment_with_a_street_and_a_postcode_becomes_a_workplace_row` (the streetless `EST-3` produces no slot) |
| 5.2 slot `est:<identifier>`, or `est:<identifier>:<establishment_index>` when the identifier repeats | `EST_SLOT_SQL`; both EST-2 rows come out suffixed in Task 2 |
| 5.2 kind `workplace`, uid `concat('ratsit:', toString(result_sha256), ':est:', toString(establishment_index))` | `ratsit_rows_sql`'s second branch; pinned in Task 1 Step 1 and read back in Task 2 |
| 5.2 establishment `name` and employee counts are not carried; the slot keeps the identifier for a later entity | `ratsit_establishments_sql` selects neither; its docstring says why |
| 5.2 `observed_at` on every row is the report's `normalized_at` | both branches stamp `toDateTime64(..., 3, 'UTC')`; Task 2 asserts `2026-09-02 00:00:00.000` on all four rows |
| 5.2 the module's own `ratsit_current_sql` over `se_ratsit_company FINAL`, not basic info's translation-aware one | `ratsit_current_sql`; `test_the_ratsit_address_current_sql_is_the_reports_own_stamp` pins the whole text and asserts `se_ratsit_company_translated` appears nowhere; Task 4 Step 5's convergence preview is the prod proof |
| 5.2 the version becomes `ratsit-address-v2` | `RATSIT_ADDRESS_EXTRACTOR_VERSION`; Global Constraints; asserted in Task 1 Step 1 and read off the stored rows in Task 4 Step 5 |
| 5.3 for the paged companies, every stored live Ratsit slot absent from the live set gets a NULL row, slot and kind kept, `observed_at` the current report's | `address_select_sql`'s tombstone projection + the `report` INNER JOIN; `test_ratsit_pairs_live_rows_with_tombstones_for_vanished_slots`; `test_a_rescan_that_drops_an_establishment_tombstones_its_slot` on a real engine |
| 5.3 a stored live row is one with `source = 'ratsit'` and a non-NULL `street_address` | `ADDRESS_LIVE_ROW_PREDICATE`, pinned in Task 1 Step 1 |
| 5.3 the UNION ALL / LEFT ANTI JOIN shape of `person/suggestions.py::person_select_sql` | `address_select_sql`, with the differences spelled out in its docstring |
| 5.3 the helper is new in `address/suggestions.py` as `address_select_sql(*, live_sql, source)`; SCB and Bolagsverket keep their single-slot tombstones | Task 1 Step 3; the same test asserts neither sibling select grew a `UNION ALL` |
| 5.3 the normalizer files a NULL row as `no_address` and the fold drops that slot; no whole-company tombstone | `test_the_normalize_hand_off_files_the_tombstones_as_no_address`; the module docstring records why Ratsit needs no company tombstone |
| 5.4 a `workplace` member sharing the postal location merges, `kinds` = distinct member kinds in member order | `test_a_workplace_establishment_at_the_postal_address_publishes_one_row` (Task 2 Step 5) and its `elsewhere` twin; Task 4 Step 9(b) reads the combos off prod |
| 5.4 the corrected rows match the register key and geocode from the cache; establishments at new locations are warmed before the fold | Task 4 Step 6's shared-`address_key` query, Step 7's warm (`cache_hits` the large majority), Step 9(i)'s geocode statuses |
| 5.4 Ratsit's spelling precedence stays 300 | Global Constraints; `address/precedence.py` is in the "not touched" list |
| 5.5 the pinned Ratsit SQL test: both kinds, both slot expressions, the dictionary join, the UNION ALL tombstone branch, `%(company_ids)s` in every branch, `ratsit-address-v2` | Task 1 Step 1's first two tests (the binding count is pinned at exactly 3) |
| 5.5 the thirteen-column loop still passes | Task 1 Step 1 leaves `test_every_select_yields_the_thirteen_columns_in_order_and_binds_the_page` untouched, and Step 6 names it as the one to watch; the live SQL deliberately has no leading `WITH` so `_aliases` still reads the thirteen-column projection |
| 5.5 the clickhouse-local test with SCB town rows, a municipality-town company, two establishments (one sharing, one elsewhere, one streetless), a repeated identifier, and a second run tombstone | Task 2 Steps 1-4, all values taken from a real `clickhouse/clickhouse-server:26.5` run under `join_use_nulls` 0 and 1 |
| 5.5 the fold unit test for a `workplace` member sharing the postal location | Task 2 Step 5 |
| 5.6 deploy; extract `execute: true, page_size: 10000, since: "2000-01-01T00:00:00Z"`; ~95 pages, ~947k + ~732k rows | Task 4 Steps 1, 3, 4 |
| 5.6 normalize `changed_only: true`; warm at chunk 150,000; fold backfill over 64 buckets, changed-only, pool `sweden_address_osm_duckdb` | Task 4 Steps 6, 7, 8 |
| 5.6 readouts: `has(sources,'ratsit')`, `has(kinds,'workplace')`, withdrawn history rows, the Malmö/Oxie and Stockholm/Bromma samples, serving address counts, the Address tab | Task 4 Steps 9 (a), (e), (f), (g), 10 and 11 |
| section 2 — the 83,696 visited companies, 260,862 municipality localities, 846,718/732,626 establishments, 324 repeated identifiers, 288,840 shared addresses, max 1,718 | the expected values of Task 4 Steps 2, 5, 8 and 9 |
| section 6 — the 131 large-establishment companies cost seconds; the serving view has only the existing address flags; weeklies stay STOPPED | Task 4 Step 8's budget note, Step 10, Global Constraints + Step 2's schedule check |
| section 7 — names: module `se_company/address/ratsit.py`, asset `se_company_address_suggestions_ratsit`, version `ratsit-address-v2`, slots `company` and `est:<identifier>`, kind `workplace` | used throughout; Global Constraints pins the strings |

Out of scope by the spec itself and therefore absent: a Ratsit establishments entity (only the address slot keeps the identifier), Ratsit's financials, industry codes, summaries and legal form, scheduling the Ratsit scan, any normalizer or fold change, and any backoffice change (the `workplace` label already exists).

**2. Placeholder scan**

No `TBD`, `TODO`, "implement later", "add appropriate error handling", "similar to Task N" or bare "write tests for the above". Every edit shows the text before and after; every command runs as written; every test body is complete code. The only fill-ins are `REPLACE_WITH_RUN_ID`, `REPLACE_WITH_BACKFILL_ID` and `REPLACE_WITH_BACKFILL_START` in Task 4's poll and history payloads — values prod hands back at run time and which cannot be known in advance.

**3. Name consistency**

Checked against the code on this branch, 2026-09-12. Helpers: `define_address_suggestion_asset(**kwargs)` forwarding to `define_suggestion_asset(source, extractor_version, current_sql, select_sql, select_params, deps, description, target, changed_scope_override)`; `changed_scope_sql(current_sql, target)`, `since_scope_sql(current_sql)`, `insert_page_sql(select_sql, target)`, `count_page_sql(select_sql)`, `run_extractor(...)`, `scope_pages(...)`, `ID_BOUND_QUERY_SETTINGS` (`max_query_size` 1,048,576), `SCAN_QUERY_SETTINGS`. Address module: `ADDRESS_SELECT_COLUMNS`, `ADDRESS_TARGET`, `ADDRESS_WITH_SQL`, `ADDRESS_TRAILING_SELECT_SQL`, `tables.RAW_ADDRESS_COLUMNS`, `tables.SUGGESTION_COLUMNS`, `tables.NORMALIZED_COLUMNS`, `tables.KINDS`, `tables.QUALIFIED_SUGGESTION_TABLE`, `SCRATCH_SCOPE_PREFIX = "corpscout._tmp_address_scope_"`. Config classes: `ExtractConfig(execute, company_ids, max_companies, since, page_size ≤ 20_000)`, `AddressNormalizeConfig(changed_only, company_ids, page_size ≤ 50_000)`, `AddressWarmConfig(chunk_size 10_000-5_000_000 default 150_000, limit)`, `AddressFoldConfig(changed_only, page_size)`. Metadata labels: `ExtractCounts` → `companies, pages, candidates, inserted, execute, stopped_at_cap`; `NormalizeCounts` → `companies, pages, rows, ok, partial, no_address, foreign, normalizer_version`; `WarmCounts` → `keys, chunks, cache_hits, matched, geocoded, fallback`; `FoldCounts` → `companies, considered, folded, published, hidden, withdrawn, changed, unchanged, unpublished, geocoded, cache_hits, matched` (plus the asset's `bucket`, `changed_only`, `page_size`, `table`, `history_table`). Tables: `corpscout.se_company_address_suggestion` (000382), `…_normalized` (000383), `corpscout.se_company_address`, `…_history`, `…_rule`, `…_precedence`, `corpscout.se_ratsit_company` and `corpscout.se_ratsit_establishments` (000343 + 000346), `corpscout.se_scb_companies` (000373), `corpscout.se_companies_serving` (000392 for the address block). Ratsit source columns used: report `company_id, result_sha256, normalizer_version, normalized_at, address_street, address_postal_code, address_locality, address_county`; establishment `company_id, result_sha256, normalizer_version, establishment_index, identifier, address_street, address_postal_code, address_locality, address_county`. SCB columns used: `postal_code, post_town, has_company`. Versions: `ratsit-address-v2`, `RATSIT_NORMALIZER_VERSION = "ratsit-normalizer-v2"`, `NORMALIZER_VERSION = "se-address-normalizer-v3"`, `FOLD_VERSION = "address-fold-v1"`. Assets: `se_company_address_suggestions_{scb,bolagsverket,ratsit,esef}`, `se_company_address_normalize`, `se_address_geocodes_warm`, `se_company_address_fold`, `se_company_address_fold_companies`, `se_company_address_precedence_clickhouse`; job `se_company_address_extract_job`; schedule `se_company_address_weekly` (`5 7 * * 1`, STOPPED); pools `se_company_address_normalize` and `sweden_address_osm_duckdb`.

The SQL of Task 1 was executed against `clickhouse/clickhouse-server:26.5` while writing this plan, with Task 2's fixture rows, under `join_use_nulls` 0 **and** 1: the two transcripts are byte-identical apart from their `suggested_at` stamps, and every expected string quoted in Task 2 is that run's actual output. The normalizer outputs quoted in `test_the_normalize_hand_off_files_the_tombstones_as_no_address` came from running `normalize_se_address` on the same inputs.

**Choices made where the spec left room, or where it and the code disagreed**

- **The postcode join key is digits-only, not just space-stripped.** Spec 5.1 says "the postcode with spaces removed"; the brief's amendment list (written after the spec's final review) says "postcode digits only". Chosen: `replaceRegexpAll(..., '[^0-9]', '')` on both sides, which is exactly what `normalize_se.py` does to the postcode it stores (`re.sub(r"\D", "", ...)`), so the dictionary agrees with the normalizer about what a postcode is. It is a strict superset of "spaces removed" and can only match more. The DELIVERED `postal_code` column is untouched (trimmed, as spec 5.2 requires): only the join key is folded.
- **The dictionary's inner aliases are `postal_code_digits` and `town`, not `postal_code` and `post_town`.** ClickHouse rejects `expr(postal_code) AS postal_code` as a cyclic alias, so the two derived columns are named apart from the source columns they read. The semantics — most frequent trimmed spelling per postcode, ties alphabetically — are the spec's; only the identifiers differ, and the join reads `towns.postal_code_digits`.
- **The tombstone carries the page's own `observed_at`, joined from `live`.** Spec 5.3 asks for "`observed_at` the current report's", which `person_select_sql`'s shape cannot supply: its tombstone branch reads only the stored rows. The extra `INNER JOIN (SELECT company_id, max(observed_at) ... FROM live GROUP BY company_id)` is the smallest thing that satisfies it, and it is load-bearing, not cosmetic: every row of a page shares one `suggested_at`, `changed_scope_sql` reads `argMax(observed_at, suggested_at)`, and a tombstone stamped with the stored row's older `observed_at` could win that tie and re-select the company on every run for ever. This is also why `address_select_sql` is written out rather than delegating to `se_company/state_scan.py::select_sql` — that generic shape has no place for the join, and the address entity does not use the state-hash scan at all.
- **`source_record_uid` on a tombstone is `''`.** The spec does not say; the suggestion table has no constraint on the column; the person entity writes `''` for its `source_record_id`. A tombstone names no source record, so `''` it is.
- **The Ratsit live SQL puts its CTEs inside the `FROM` subqueries instead of a leading `WITH`.** `test_every_select_yields_the_thirteen_columns_in_order_and_binds_the_page` reads the aliases of the FIRST `SELECT` in the rendered text, and spec 5.5 requires that loop to keep passing. A leading `WITH report AS (...)` would hand it the report's columns instead. The cost is that the report subquery's text appears twice, so the page binds `%(company_ids)s` three times — which the shared scan's own docstring already calls a normal number ("two or three times").
- **The STOPPED weekly's page size drops from 20,000 to 10,000.** Not in the spec, found by arithmetic: three bindings of 20,000 twelve-digit ids render to about 900 KB against `ID_BOUND_QUERY_SETTINGS`' 1 MiB `max_query_size`, and `jobs.py`'s comment claiming "one binding per statement" becomes false. The weekly is STOPPED so nothing runs today, but leaving a schedule configured to render a near-limit statement is a trap for whoever starts it; 10,000 is what the person weekly settled on and what the prod runs use.
- **A new integration test file rather than growing the four-extractor one.** Spec 5.5 allows either ("or a new one on the person test's pattern"). Chosen: new, because the existing script's assertions are counted (`len(rows) == 3`, `reconverged == []`) over an unfiltered read of the whole suggestion table, and seeding establishments there would force edits to tests that have nothing to do with this slice. The existing file still changes in one place — it must load the new fixture, or the Ratsit select cannot parse now that it reads `se_ratsit_establishments`.
- **The establishments fixture is its own file.** `se_ratsit_establishments` cannot ride a migration replay: 000343 also creates `se_ratsit_company` with CHECK constraints the existing fixtures' rows do not satisfy (`requested_url = concat('https://www.ratsit.se/', right(company_id, 10))`, among others), and 000346 only `ALTER`s the establishments table. Putting the CREATE in a second fixture file, loaded beside `se_basic_info_source_tables.sql`, is the same shape person slice 2 used for `se_ratsit_responsible_people`.
- **The window counts establishments, not identifiers in the raw table.** Spec 5.2 says "when the identifier repeats within the report". Implemented as a window over the rows that actually become suggestions (the street+postcode filter runs in the inner subquery, and ClickHouse computes windows after it), so a repeat caused by a skipped streetless row cannot suffix a slot nobody would collide with. An empty identifier is left to the same rule: one such row per report keeps the bare `est:` slot, two get suffixed.
- **The extractor does not join the basic-info universe.** The person extractor does (its universe IS the folded company); spec 5.1 lists three deps and none of them is a fold, and the v1 address extractor never joined one. A Ratsit company the basic-info entity does not know still gets an address suggestion, as it did before this slice.
- **Preview before execute.** Spec 5.6 says "`se_company_address_suggestions_ratsit` with `execute: true`"; the asset's default is a preview, the scope is the expensive half either way, and 1.68M rows is not a write to start blind. Task 4 previews first with an explicit stop rule on the candidate count, then executes.
