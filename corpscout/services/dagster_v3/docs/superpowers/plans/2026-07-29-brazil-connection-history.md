# Brazil Connection History Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `corpscout.br_company_relations` from a snapshot of who is connected to whom *now* into one row per **spell** of a connection — company X to entity Y in role R, from a start date to an end date, `NULL` meaning still current.

**Architecture:** Each monthly run merges its snapshot into the existing history: read current state, compare to the new snapshot, write a new state into a stage table, `EXCHANGE TABLES`. Atomic and idempotent, no ClickHouse mutations (which it handles badly at 20–25M rows). A merged-snapshot ledger records which months are in the history and lets the merge refuse out-of-order arrivals loudly.

**Tech Stack:** Python 3.14, Dagster, DuckDB, ClickHouse, `uv`, pytest.

Design doc: `src/dagster_v3/defs/brazil_companies/docs/brazil_rfb_socios_history-design.md` — **read §1, §2 and §4 before starting.** They carry the two decisions the code must not quietly reverse.

## Global Constraints

- All commands run from `corpscout/services/dagster_v3/` and are prefixed **`uv run`**.
- **`relation_code` and `relation_since_key` are IN the sort key.** A role change or a re-entry opens a *new row* — it never mutates an existing one. Collapsing either hides the exact change the table exists to show.
- **`ORDER BY` cannot contain `Nullable` columns.** `relation_since_key` is a non-nullable `String` (`''` when RFB omits the date); the typed `relation_since Date32` sits beside it.
- **Non-nullable `String`/`LowCardinality(String)` columns must receive `''`, never `NULL`** — the native driver calls `.encode()` per value and dies on `None`.
- **`end_at` means "gone by this snapshot", never "left on this date".** Do not name it, comment it, or document it as a departure date.
- **Every new asset MUST be added to `defs = dg.Definitions(assets=[...])`** at the bottom of `assets.py`. An asset omitted from that list is invisible to Dagster and **neither `pytest` nor `dg check defs` catches it.** Verify with:
  ```bash
  uv run python -c "
  from dagster_v3.definitions import defs as load_defs
  keys = {k.path[-1] for k in load_defs().get_repository_def().asset_graph.get_all_asset_keys()}
  print('registered:', '<asset_name>' in keys)
  "
  ```
- **No `;` inside a `--` comment in a migration** — the driver splits on `;` without stripping comments.
- **No `from __future__ import annotations`** in modules defining a `@dg.asset`.
- Migration number: this plan writes **`000210`**. Another session renumbers migrations periodically — run `ls corpscout/clickhouse/migrations/ | tail -2` and take the next genuinely free number before creating the file.
- **Commit by explicit path.** The tree carries other sessions' in-flight work; never `git add -A`.
- Validate before finishing any task: `uv run dg check defs` and the task's tests.

---

## Context: what exists today

`br_company_relations` was created by migration `000208` and is **deployed with 0 rows** — nothing has been materialized, so there is no data to migrate and no history to preserve. This is the last moment the change is free.

Current shape (snapshot semantics):
- `ENGINE = MergeTree ORDER BY (cnpj_basico, related_entity_kind, related_tax_id, relation_code)`
- 17 columns including `snapshot_year_month`, no history columns
- `relations.py:build_brazil_rfb_company_relations(...)` builds one row per source row into DuckDB
- `clickhouse.py:export_brazil_comp_rfb_clickhouse_company_relations(...)` exports with **`truncate=True`** — the single line this design removes

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `corpscout/clickhouse/migrations/000210_*.up.sql` / `.down.sql` | SCD2 table + snapshot ledger | create |
| `src/dagster_v3/defs/brazil_companies/rfb/tables.py` | column tuples, table constants | modify |
| `src/dagster_v3/defs/brazil_companies/rfb/relations.py` | snapshot build (add `relation_since_key`) | modify |
| `src/dagster_v3/defs/brazil_companies/rfb/history.py` | **NEW** — the merge, the ordering guard | create |
| `src/dagster_v3/defs/brazil_companies/rfb/clickhouse.py` | export switches to merge | modify |
| `src/dagster_v3/defs/brazil_companies/rfb/source.py` | retention 90 → 365 | modify |
| `tests/test_brazil_comp_rfb_history.py` | **NEW** — merge behaviour | create |
| `tests/test_brazil_comp_rfb_relations.py` | `relation_since_key` | modify |
| `tests/test_brazil_comp_rfb_source.py` | retention constant | modify |
| `tests/test_clickhouse_migrations.py` | ledger + column contract | modify |

The merge goes in its own `history.py` rather than `relations.py`: that file owns the raw→snapshot transform and shares nothing with the state machine.

---

## Task 1: Retention 90 days → one year

Smallest change, entirely independent, and it must land before the first materialization or archives start expiring on the wrong clock.

**Files:**
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/source.py`
- Test: `tests/test_brazil_comp_rfb_source.py`

**Interfaces:**
- Produces: `source.RFB_SOCIOS_RETENTION_DAYS == 365`.

- [ ] **Step 1: Update the failing test**

In `tests/test_brazil_comp_rfb_source.py`, in `test_socios_retention_rule_expires_only_the_personal_data_family`, change the expiry assertion:

```python
    assert rule["Expiration"]["Days"] == 365
```

- [ ] **Step 2: Run it, confirm it fails**

```bash
uv run pytest tests/test_brazil_comp_rfb_source.py -q -k retention
```

Expected: FAIL, `assert 90 == 365`.

- [ ] **Step 3: Change the constant and its rationale**

In `source.py`, replace the constant and the comment above it:

```python
# Socios is the only family carrying personal data -- person names, masked CPFs
# and age bands. One year, not 90 days: the archives are the ONLY path for
# rebuilding br_company_relations' connection history, since that table is
# derived state. Ninety days would make the history unverifiable after a
# quarter. See brazil_rfb_socios_history-design.md section 7.
#
# This is a hard limit, not a soft one -- RFB drops old snapshots, so
# re-fetching past the window is not actually available.
RFB_SOCIOS_RETENTION_DAYS = 365
```

- [ ] **Step 4: Run the tests, confirm they pass**

```bash
uv run pytest tests/test_brazil_comp_rfb_source.py -q
```

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/brazil_companies/rfb/source.py \
        tests/test_brazil_comp_rfb_source.py
git commit -m "feat(corpscout): retain Brazil socios archives one year, not 90 days"
```

---

## Task 2: The SCD2 schema

**Files:**
- Create: `corpscout/clickhouse/migrations/000210_corpscout_br_company_relations_history.up.sql` / `.down.sql`
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/tables.py`
- Test: `tests/test_clickhouse_migrations.py`

**Interfaces:**
- Produces: `tables.BR_COMPANY_RELATIONS_COLUMNS` (the SCD2 tuple below); `tables.BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE_CH = "br_company_relations_snapshots"`; `tables.BR_COMPANY_RELATIONS_SNAPSHOT_COLUMNS`.

- [ ] **Step 1: Confirm the migration number is free**

```bash
ls corpscout/clickhouse/migrations/ | tail -3
```

If `000210` is taken, use the next free number consistently below.

- [ ] **Step 2: Write the failing contract test**

In `tests/test_clickhouse_migrations.py`, add `"000210_corpscout_br_company_relations_history"` to the end of `EXPECTED_MIGRATIONS`, and add:

```python
def test_br_company_relations_history_migration_covers_columns() -> None:
    """One row per spell. relation_code and relation_since_key are IN the sort
    key: a role change or a re-entry opens a new row rather than mutating one,
    which is the change the table exists to show."""
    sql = _migration_sql("000210_corpscout_br_company_relations_history.up.sql")
    down_sql = _migration_sql("000210_corpscout_br_company_relations_history.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.br_company_relations" in sql
    for column in brazil_rfb_tables.BR_COMPANY_RELATIONS_COLUMNS:
        assert f"    {column} " in sql, column
    assert (
        "ORDER BY (\n    cnpj_basico,\n    related_entity_kind,\n"
        "    related_tax_id,\n    relation_code,\n    relation_since_key\n)"
    ) in sql

    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.br_company_relations_snapshots" in sql
    )
    for column in brazil_rfb_tables.BR_COMPANY_RELATIONS_SNAPSHOT_COLUMNS:
        assert f"    {column} " in sql, column

    assert "DROP TABLE IF EXISTS corpscout.br_company_relations" in down_sql
```

- [ ] **Step 3: Run it, confirm it fails**

```bash
uv run pytest tests/test_clickhouse_migrations.py -q -k history
```

Expected: FAIL, `FileNotFoundError`.

- [ ] **Step 4: Write the migration**

`000210_corpscout_br_company_relations_history.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Connection history: one row per SPELL, not per snapshot.
--
-- 000208 created this table with snapshot semantics and the export replaced it
-- wholesale each run, so the ninth run destroyed what the eighth learned. RFB
-- republishes the full register monthly and its mirror eventually drops old
-- months, so a discarded snapshot is gone permanently. Ownership and control
-- changing over time is the signal worth having.
--
-- Dropped and recreated rather than altered: the table is deployed with ZERO
-- rows (nothing has ever been materialized), so there is no data to migrate,
-- and the sort key changes -- which ALTER cannot do.
--
-- relation_code is IN the key deliberately. A partner becoming an administrator
-- closes one row and opens another, because that control shift is precisely
-- what this table exists to show; holding it in a mutable column would hide it.
--
-- relation_since_key is in the key because RFB publishes no departures but DOES
-- publish re-entries: data_entrada_sociedade carries a NEW entry date when
-- someone rejoins, so a second spell is detectable from a single snapshot
-- rather than depending on us having observed the gap.
--
-- start_at and end_at have DIFFERENT precision. start_at is authoritative, from
-- the source's own entry date. end_at means "gone by this snapshot" -- never
-- "left on this date" -- because the source never says when a relationship
-- ended. Its precision is exactly the run cadence.
DROP TABLE IF EXISTS corpscout.br_company_relations;

CREATE TABLE IF NOT EXISTS corpscout.br_company_relations
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    cnpj_basico String,
    related_entity_kind LowCardinality(String),
    related_tax_id String,
    relation_code LowCardinality(String),
    relation_since_key String,
    related_name String,
    related_country String,
    age_band LowCardinality(String),
    representative_tax_id String,
    representative_name String,
    representative_code LowCardinality(String),
    relation_since Nullable(Date32),
    first_seen_snapshot LowCardinality(String),
    last_seen_snapshot LowCardinality(String),
    start_at Nullable(Date32),
    end_at Nullable(Date32),
    is_current UInt8,
    observations UInt32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (
    cnpj_basico,
    related_entity_kind,
    related_tax_id,
    relation_code,
    relation_since_key
);

-- Which months are in the history, so the merge can refuse an out-of-order
-- snapshot and a reader can tell what the history is made of. Without it the
-- ordering guard has nothing to compare against and a gap is invisible.
CREATE TABLE IF NOT EXISTS corpscout.br_company_relations_snapshots
(
    snapshot_year_month LowCardinality(String),
    merged_at DateTime64(3, 'UTC'),
    source_run_id String,
    edges_in_snapshot UInt64,
    spells_opened UInt64,
    spells_closed UInt64,
    spells_total UInt64
)
ENGINE = MergeTree
ORDER BY snapshot_year_month;
```

`.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.br_company_relations_snapshots;

DROP TABLE IF EXISTS corpscout.br_company_relations;
```

- [ ] **Step 5: Update the column tuples**

In `tables.py`, replace `BR_COMPANY_RELATIONS_COLUMNS` with the SCD2 tuple in migration order, and add the ledger tuple:

```python
BR_COMPANY_RELATIONS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "cnpj_basico",
    "related_entity_kind",
    "related_tax_id",
    "relation_code",
    "relation_since_key",
    "related_name",
    "related_country",
    "age_band",
    "representative_tax_id",
    "representative_name",
    "representative_code",
    "relation_since",
    "first_seen_snapshot",
    "last_seen_snapshot",
    "start_at",
    "end_at",
    "is_current",
    "observations",
    "resolved_at",
)
BR_COMPANY_RELATIONS_EXPORT_COLUMNS = BR_COMPANY_RELATIONS_COLUMNS

BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE_CH = "br_company_relations_snapshots"
QUALIFIED_BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE = (
    f"{BRAZIL_COMP_RFB_DATABASE}.{BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE_CH}"
)
BR_COMPANY_RELATIONS_SNAPSHOT_COLUMNS = (
    "snapshot_year_month",
    "merged_at",
    "source_run_id",
    "edges_in_snapshot",
    "spells_opened",
    "spells_closed",
    "spells_total",
)
```

Note `source_run_id`, `source_record_id` and `snapshot_year_month` leave the relations tuple — a spell spans runs, so a single run id no longer describes it, and the month it was seen in lives in `first_seen_snapshot`/`last_seen_snapshot`.

- [ ] **Step 6: Run the contract test, confirm it passes**

```bash
uv run pytest tests/test_clickhouse_migrations.py -q
```

- [ ] **Step 7: Commit**

```bash
git add corpscout/clickhouse/migrations/000210_corpscout_br_company_relations_history.up.sql \
        corpscout/clickhouse/migrations/000210_corpscout_br_company_relations_history.down.sql \
        src/dagster_v3/defs/brazil_companies/rfb/tables.py \
        tests/test_clickhouse_migrations.py
git commit -m "feat(corpscout): schema for Brazil connection history, one row per spell"
```

---

## Task 3: `relation_since_key` on the snapshot build

The merge keys on it, so the snapshot must produce it.

**Files:**
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/relations.py`
- Test: `tests/test_brazil_comp_rfb_relations.py`

**Interfaces:**
- Produces: the DuckDB `company_relations` table gains `relation_since_key String`, non-null, `''` when RFB omits the entry date.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_brazil_comp_rfb_relations.py`:

```python
def test_relation_since_key_is_the_source_entry_date_verbatim(tmp_path: Path) -> None:
    """RFB publishes no departures, but it DOES publish re-entries: a partner who
    rejoins carries a new data_entrada_sociedade. That makes a second spell
    detectable from one snapshot, so the entry date is part of a spell's
    identity -- as text, because ORDER BY cannot hold Nullable.
    """
    socios_path = tmp_path / "socios.duckdb"
    _socios_stage(socios_path)
    connection = duckdb.connect(":memory:")

    relations.build_brazil_rfb_company_relations(
        connection=connection,
        source_run_id="run-1",
        snapshot_year_month="2026-07",
        socios_database_path=socios_path,
    )

    rows = connection.execute(
        f"""
        select relation_since_key, relation_since
        from {DATASET}.{tables.COMPANY_RELATIONS_TABLE}
        order by cnpj_basico, related_entity_kind
        """
    ).fetchall()
    assert rows[0][0] == "20180314"
    assert rows[0][1].isoformat() == "2018-03-14"
    # the row whose entry date is blank keeps '' -- never NULL, because the
    # column is part of a non-nullable sort key
    assert ("", None) in rows
```

- [ ] **Step 2: Run it, confirm it fails**

```bash
uv run pytest tests/test_brazil_comp_rfb_relations.py -q -k relation_since_key
```

Expected: FAIL, `BinderException: Referenced column "relation_since_key" not found`.

- [ ] **Step 3: Emit the column**

In `relations.py`, in the `create or replace table` SELECT, immediately after the `relation_since` expression:

```python
                {_blank('s.data_entrada_sociedade')} as relation_since_key,
```

- [ ] **Step 4: Run the tests, confirm they pass**

```bash
uv run pytest tests/test_brazil_comp_rfb_relations.py -q
```

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/brazil_companies/rfb/relations.py \
        tests/test_brazil_comp_rfb_relations.py
git commit -m "feat(corpscout): carry the source entry date as a spell key"
```

---

## Task 4: The merge

The heart of it. A pure function over two row-sets so it is testable without ClickHouse.

**Files:**
- Create: `src/dagster_v3/defs/brazil_companies/rfb/history.py`
- Create: `tests/test_brazil_comp_rfb_history.py`

**Interfaces:**
- Consumes: `tables.BR_COMPANY_RELATIONS_COLUMNS`.
- Produces:
  - `history.SPELL_KEY: tuple[str, ...]`
  - `history.assert_snapshot_is_newer(snapshot_year_month: str, merged_months: Sequence[str]) -> None` — raises `ValueError`
  - `history.build_merge_select_sql(*, state_table: str, snapshot_table: str, snapshot_year_month: str, snapshot_date: str) -> str`

**Why a SELECT and not a full statement:** the same merge runs in two engines — DuckDB in the tests, ClickHouse in the export. The `FULL OUTER JOIN` and the `CASE` arms are identical in both; only the wrapper differs (`CREATE OR REPLACE TABLE x AS …` vs `INSERT INTO x …`). Returning just the SELECT means the logic under test is character-for-character the logic that ships. A function emitting a whole DuckDB statement would leave the ClickHouse path untested.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_brazil_comp_rfb_history.py`:

```python
import duckdb
import pytest

from dagster_v3.defs.brazil_companies.rfb import history


def test_out_of_order_snapshot_is_refused_loudly() -> None:
    """Manual runs mean months WILL arrive out of order. Absorbing a late one
    silently would corrupt the timeline and hide that the cadence slipped --
    and end_at precision is exactly the cadence."""
    with pytest.raises(ValueError, match="older than"):
        history.assert_snapshot_is_newer("2026-07", ["2026-06", "2026-08"])


def test_same_snapshot_merged_twice_is_refused() -> None:
    with pytest.raises(ValueError, match="already merged"):
        history.assert_snapshot_is_newer("2026-08", ["2026-06", "2026-08"])


def test_first_snapshot_into_an_empty_history_is_allowed() -> None:
    history.assert_snapshot_is_newer("2026-06", [])


def _merge(connection: duckdb.DuckDBPyConnection, month: str, date: str) -> None:
    """Wraps the shared SELECT the way DuckDB wants it. The export wraps the
    same SELECT in an INSERT -- see the interfaces note above."""
    select_sql = history.build_merge_select_sql(
        state_table="state",
        snapshot_table="snap",
        snapshot_year_month=month,
        snapshot_date=date,
    )
    connection.execute(f"create or replace table stage as {select_sql}")
    connection.execute("drop table state")
    connection.execute("alter table stage rename to state")


def _schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        create table state (
            country_iso2 varchar, source_slug varchar, cnpj_basico varchar,
            related_entity_kind varchar, related_tax_id varchar,
            relation_code varchar, relation_since_key varchar,
            related_name varchar, related_country varchar, age_band varchar,
            representative_tax_id varchar, representative_name varchar,
            representative_code varchar, relation_since date,
            first_seen_snapshot varchar, last_seen_snapshot varchar,
            start_at date, end_at date, is_current utinyint,
            observations uinteger, resolved_at timestamp
        )
        """
    )


def _snapshot(connection: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    connection.execute("drop table if exists snap")
    connection.execute(
        """
        create table snap (
            country_iso2 varchar, source_slug varchar, cnpj_basico varchar,
            related_entity_kind varchar, related_tax_id varchar,
            relation_code varchar, relation_since_key varchar,
            related_name varchar, related_country varchar, age_band varchar,
            representative_tax_id varchar, representative_name varchar,
            representative_code varchar, relation_since date
        )
        """
    )
    connection.executemany(
        "insert into snap values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
    )


def _edge(tax_id: str, code: str, since_key: str, name: str = "MARIA SOUZA") -> tuple:
    return (
        "BR", "brazil_rfb", "11111111", "2", tax_id, code, since_key,
        name, "", "0", "", "", "", None,
    )


def test_an_unchanged_edge_extends_rather_than_duplicating() -> None:
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-07", "2026-07-01")

    rows = connection.execute(
        "select first_seen_snapshot, last_seen_snapshot, observations, "
        "is_current, end_at from state"
    ).fetchall()
    assert rows == [("2026-06", "2026-07", 2, 1, None)]


def test_a_disappearing_edge_is_closed_not_deleted() -> None:
    """end_at means 'gone by this snapshot', never 'left on this date' -- the
    source never publishes a departure."""
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [])
    _merge(connection, "2026-07", "2026-07-01")

    assert connection.execute(
        "select is_current, end_at, last_seen_snapshot from state"
    ).fetchall() == [(0, __import__("datetime").date(2026, 7, 1), "2026-06")]


def test_a_role_change_opens_a_second_spell() -> None:
    """Partner -> administrator is the control shift this table exists to show,
    so it must be two rows, not a mutated column."""
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [_edge("***456789**", "22", "20190701")])
    _merge(connection, "2026-07", "2026-07-01")

    rows = connection.execute(
        "select relation_code, is_current, observations from state "
        "order by relation_code"
    ).fetchall()
    assert rows == [("22", 1, 1), ("49", 0, 1)]


def test_a_re_entry_opens_a_second_spell_on_the_new_entry_date() -> None:
    """A returning partner is visible from ONE snapshot because RFB stamps a new
    data_entrada_sociedade -- it does not depend on us having observed the gap."""
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [_edge("***456789**", "49", "20260901")])
    _merge(connection, "2026-07", "2026-07-01")

    rows = connection.execute(
        "select relation_since_key, is_current from state "
        "order by relation_since_key"
    ).fetchall()
    assert rows == [("20190701", 0), ("20260901", 1)]


def test_a_closed_spell_is_not_reopened_or_recounted() -> None:
    connection = duckdb.connect(":memory:")
    _schema(connection)

    _snapshot(connection, [_edge("***456789**", "49", "20190701")])
    _merge(connection, "2026-06", "2026-06-01")
    _snapshot(connection, [])
    _merge(connection, "2026-07", "2026-07-01")
    _snapshot(connection, [])
    _merge(connection, "2026-08", "2026-08-01")

    assert connection.execute(
        "select is_current, observations, end_at from state"
    ).fetchall() == [(0, 1, __import__("datetime").date(2026, 7, 1))]
```

- [ ] **Step 2: Run them, confirm they fail**

```bash
uv run pytest tests/test_brazil_comp_rfb_history.py -q
```

Expected: FAIL, `ModuleNotFoundError: No module named '...rfb.history'`.

- [ ] **Step 3: Write the merge**

Create `src/dagster_v3/defs/brazil_companies/rfb/history.py`:

```python
"""Merge a monthly Socios snapshot into the connection history.

One row per SPELL of a connection: company X to entity Y in role R, from a
start date to an end date, NULL meaning current.

Two things about this model are easy to reverse by accident and must not be:

`relation_code` is part of a spell's identity, so a partner becoming an
administrator closes one spell and opens another. That control shift is the
change the table exists to show; holding the role in a mutable column hides it.

`relation_since_key` is part of a spell's identity because RFB publishes no
departures but DOES publish re-entries -- a partner who rejoins carries a new
data_entrada_sociedade. That makes a second spell detectable from a single
snapshot rather than depending on us having observed the gap.

And `end_at` means "gone by this snapshot", never "left on this date": the
source never says when a relationship ended, so its precision is exactly the
run cadence. See brazil_rfb_socios_history-design.md sections 1, 2 and 4.
"""

from collections.abc import Sequence

# What makes two observations the same spell.
SPELL_KEY: tuple[str, ...] = (
    "cnpj_basico",
    "related_entity_kind",
    "related_tax_id",
    "relation_code",
    "relation_since_key",
)

# Refreshed from the newest observation while a spell is open.
_ATTRIBUTES: tuple[str, ...] = (
    "related_name",
    "related_country",
    "age_band",
    "representative_tax_id",
    "representative_name",
    "representative_code",
    "relation_since",
)


def assert_snapshot_is_newer(
    snapshot_year_month: str, merged_months: Sequence[str]
) -> None:
    """Refuse an out-of-order or repeated snapshot, loudly.

    Manual runs mean months will arrive out of order eventually. Absorbing a
    late one silently would reopen spells a later month had closed and stamp
    dates that contradict the timeline -- and would hide that the cadence had
    slipped, which is what end_at's precision depends on.
    """
    if not merged_months:
        return
    if snapshot_year_month in merged_months:
        raise ValueError(
            f"Brazil RFB snapshot {snapshot_year_month} is already merged into "
            "the connection history"
        )
    newest = max(merged_months)
    if snapshot_year_month < newest:
        raise ValueError(
            f"Brazil RFB snapshot {snapshot_year_month} is older than the newest "
            f"merged snapshot {newest}; merging it would corrupt the timeline. "
            "Rebuild from the S3 archives in ascending order instead -- see "
            "brazil_rfb_socios_history-design.md section 8."
        )


def build_merge_select_sql(
    *,
    state_table: str,
    snapshot_table: str,
    snapshot_year_month: str,
    snapshot_date: str,
) -> str:
    """Full outer join of the history against the new snapshot.

    Returns a SELECT, not a statement, because the same merge runs in DuckDB
    (tests) and ClickHouse (export). Only the wrapper differs; keeping the
    logic in one string means what is tested is what ships.

    Three cases: in both (extend), state only (close), snapshot only (open).
    An already-closed spell is left untouched -- a re-entry arrives as a
    different key because its relation_since_key differs.
    """
    join = " and ".join(f"st.{c} = sn.{c}" for c in SPELL_KEY)
    key_out = ",\n        ".join(f"coalesce(st.{c}, sn.{c}) as {c}" for c in SPELL_KEY)
    attrs_out = ",\n        ".join(
        f"case when sn.cnpj_basico is not null then sn.{c} else st.{c} end as {c}"
        for c in _ATTRIBUTES
    )
    return f"""
    select
        coalesce(st.country_iso2, sn.country_iso2) as country_iso2,
        coalesce(st.source_slug, sn.source_slug) as source_slug,
        {key_out},
        {attrs_out},
        coalesce(st.first_seen_snapshot, '{snapshot_year_month}')
            as first_seen_snapshot,
        case
            when sn.cnpj_basico is not null then '{snapshot_year_month}'
            else st.last_seen_snapshot
        end as last_seen_snapshot,
        coalesce(st.start_at, sn.relation_since) as start_at,
        case
            -- already closed: leave it exactly as it was
            when st.is_current = 0 then st.end_at
            -- present now: still open
            when sn.cnpj_basico is not null then null
            -- was open, absent now: gone BY this snapshot
            else date '{snapshot_date}'
        end as end_at,
        case
            when st.is_current = 0 then 0
            when sn.cnpj_basico is not null then 1
            else 0
        end as is_current,
        coalesce(st.observations, 0)
            + case when sn.cnpj_basico is not null then 1 else 0 end
            as observations,
        now() as resolved_at
    from {state_table} as st
    full outer join {snapshot_table} as sn on {join}
    """
```

- [ ] **Step 4: Run the tests, confirm they pass**

```bash
uv run pytest tests/test_brazil_comp_rfb_history.py -q
```

Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add src/dagster_v3/defs/brazil_companies/rfb/history.py \
        tests/test_brazil_comp_rfb_history.py
git commit -m "feat(corpscout): merge a Socios snapshot into connection history"
```

---

## Task 5: Wire the merge into the export

**Files:**
- Modify: `src/dagster_v3/defs/brazil_companies/rfb/clickhouse.py`
- Test: `tests/test_brazil_comp_rfb_clickhouse.py`

**Interfaces:**
- Consumes: `history.assert_snapshot_is_newer`, `history.build_merge_sql`, the Task 2 tables.
- Produces: `export_brazil_comp_rfb_clickhouse_company_relations(...)` now merges rather than replaces, and returns `dict[str, int]` with `edges_in_snapshot`, `spells_opened`, `spells_closed`, `spells_total`.

- [ ] **Step 1: Write the failing test**

In `tests/test_brazil_comp_rfb_clickhouse.py`, following the existing exporter tests' structure and their `FakeClickHouseClient`:

```python
def test_company_relations_export_merges_rather_than_truncating() -> None:
    """truncate=True would destroy the previous month's history -- the single
    behaviour this design removes."""
    import inspect

    from dagster_v3.defs.brazil_companies.rfb import clickhouse as rfb_clickhouse

    source = inspect.getsource(
        rfb_clickhouse.export_brazil_comp_rfb_clickhouse_company_relations
    )
    assert "truncate=True" not in source
    assert "EXCHANGE TABLES" in source
    assert "assert_snapshot_is_newer" in source
```

- [ ] **Step 2: Run it, confirm it fails**

```bash
uv run pytest tests/test_brazil_comp_rfb_clickhouse.py -q -k merges_rather
```

Expected: FAIL — `truncate=True` is still present.

- [ ] **Step 3: Rewrite the export**

Replace `export_brazil_comp_rfb_clickhouse_company_relations` in `clickhouse.py` with:

```python
def export_brazil_comp_rfb_clickhouse_company_relations(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    snapshot_year_month: str,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Merge this month's snapshot into the connection history.

    Not a replace: the previous months ARE the history. See
    brazil_rfb_socios_history-design.md.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_RFB_DATABASE,
        tables=(
            tables.BR_COMPANY_RELATIONS_TABLE_CH,
            tables.BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE_CH,
        ),
    )
    snapshot_stage = f"_tmp_relations_snapshot_{uuid.uuid4().hex}"
    merge_stage = f"_tmp_{tables.BR_COMPANY_RELATIONS_TABLE_CH}_{uuid.uuid4().hex}"
    qualified_snapshot = _qualified(snapshot_stage)
    qualified_merge = _qualified(merge_stage)
    qualified_target = tables.QUALIFIED_BR_COMPANY_RELATIONS_TABLE
    qualified_ledger = tables.QUALIFIED_BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE
    snapshot_date = f"{snapshot_year_month}-01"

    with clickhouse.get_connection() as client:
        merged_months = [
            str(row[0])
            for row in client.execute(
                f"SELECT snapshot_year_month FROM {qualified_ledger}"
            )
        ]
        # Before anything is written: refuse an out-of-order or repeated month.
        history.assert_snapshot_is_newer(snapshot_year_month, merged_months)

        client.execute(
            f"""
            CREATE TABLE {qualified_snapshot}
            (
                country_iso2 LowCardinality(String),
                source_slug LowCardinality(String),
                cnpj_basico String,
                related_entity_kind LowCardinality(String),
                related_tax_id String,
                relation_code LowCardinality(String),
                relation_since_key String,
                related_name String,
                related_country String,
                age_band LowCardinality(String),
                representative_tax_id String,
                representative_name String,
                representative_code LowCardinality(String),
                relation_since Nullable(Date32)
            )
            ENGINE = MergeTree
            ORDER BY (cnpj_basico, related_tax_id, relation_code)
            """
        )
        try:
            edges_in_snapshot = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema=DLT_DATASET_NAME,
                duckdb_table=tables.COMPANY_RELATIONS_TABLE,
                clickhouse_database=tables.BRAZIL_COMP_RFB_DATABASE,
                clickhouse_table=snapshot_stage,
                columns=tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS,
                truncate=False,
                column_expressions=(
                    CLICKHOUSE_COMPANY_RELATIONS_DATE32_EXPORT_EXPRESSIONS
                ),
            )
            client.execute(f"CREATE TABLE {qualified_merge} AS {qualified_target}")
            client.execute(
                f"INSERT INTO {qualified_merge} "
                + history.build_merge_select_sql(
                    state_table=qualified_target,
                    snapshot_table=qualified_snapshot,
                    snapshot_year_month=snapshot_year_month,
                    snapshot_date=snapshot_date,
                )
            )
            [(spells_total, spells_opened, spells_closed)] = client.execute(
                f"""
                SELECT
                    count(),
                    countIf(first_seen_snapshot = '{snapshot_year_month}'),
                    countIf(
                        is_current = 0
                        AND last_seen_snapshot != '{snapshot_year_month}'
                        AND end_at = toDate('{snapshot_date}')
                    )
                FROM {qualified_merge}
                """
            )
            if int(edges_in_snapshot) > 0 and int(spells_total) == 0:
                raise ValueError(
                    "Brazil RFB connection merge produced no spells from a "
                    f"non-empty snapshot ({edges_in_snapshot} edges); refusing "
                    "to replace the published history"
                )
            client.execute(
                f"EXCHANGE TABLES {qualified_merge} AND {qualified_target}"
            )
            client.execute(
                f"INSERT INTO {qualified_ledger} "
                f"({', '.join(tables.BR_COMPANY_RELATIONS_SNAPSHOT_COLUMNS)}) VALUES",
                [
                    (
                        snapshot_year_month,
                        datetime.now(UTC),
                        source_run_id,
                        int(edges_in_snapshot),
                        int(spells_opened),
                        int(spells_closed),
                        int(spells_total),
                    )
                ],
            )
        finally:
            client.execute(f"DROP TABLE IF EXISTS {qualified_merge}")
            client.execute(f"DROP TABLE IF EXISTS {qualified_snapshot}")

    counts = {
        "edges_in_snapshot": int(edges_in_snapshot),
        "spells_opened": int(spells_opened),
        "spells_closed": int(spells_closed),
        "spells_total": int(spells_total),
    }
    if log is not None:
        log("Merged Brazil RFB connection history: counts=%s", counts)
    return counts
```

Add at the top of `clickhouse.py` if absent: `from datetime import UTC, datetime` and `from dagster_v3.defs.brazil_companies.rfb import history`.

Add to `tables.py` the snapshot-input tuple — the 14 columns the DuckDB build produces, which is `BR_COMPANY_RELATIONS_COLUMNS` minus the six history columns:

```python
BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS = (
    "country_iso2",
    "source_slug",
    "cnpj_basico",
    "related_entity_kind",
    "related_tax_id",
    "relation_code",
    "relation_since_key",
    "related_name",
    "related_country",
    "age_band",
    "representative_tax_id",
    "representative_name",
    "representative_code",
    "relation_since",
)
```

Then update the asset in `assets.py` to pass `snapshot_year_month` and `source_run_id`, and to keep using `read_only_duckdb_connection` as its siblings do:

```python
    with read_only_duckdb_connection(
        duckdb_resource(stage_paths.relations)
    ) as connection:
        counts = export_brazil_comp_rfb_clickhouse_company_relations(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            snapshot_year_month=brazil_comp_rfb_snapshot_year_month(
                context.partition_key
            ),
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=dict(counts))
```

**ClickHouse note:** `FULL OUTER JOIN` is supported, but it requires `join_use_nulls = 1` for the unmatched side to yield `NULL` rather than type defaults — without it, `st.is_current` reads `0` for a brand-new spell and `sn.cnpj_basico` reads `''` rather than `NULL`, so every `IS NOT NULL` branch silently takes the wrong arm. Set it on the merge query:

```python
            client.execute(
                f"INSERT INTO {qualified_merge} " + history.build_merge_select_sql(...),
                settings={"join_use_nulls": 1},
            )
```

This is the single most likely thing to be wrong on first run — the merge will appear to work and quietly mark everything current.

- [ ] **Step 4: Run the tests, confirm they pass**

```bash
uv run pytest tests/test_brazil_comp_rfb_clickhouse.py -q
```

- [ ] **Step 5: Verify definitions and the whole module**

```bash
uv run dg check defs
uv run pytest tests/test_brazil_comp_rfb_history.py \
               tests/test_brazil_comp_rfb_relations.py \
               tests/test_brazil_comp_rfb_clickhouse.py \
               tests/test_brazil_comp_rfb_assets.py \
               tests/test_clickhouse_migrations.py -q
```

- [ ] **Step 6: Commit**

```bash
git add src/dagster_v3/defs/brazil_companies/rfb/clickhouse.py \
        tests/test_brazil_comp_rfb_clickhouse.py
git commit -m "feat(corpscout): publish connection history instead of replacing it"
```

---

## Deployment

Migrations before code, as always. `000210` **drops and recreates** `br_company_relations` — safe only because it holds **0 rows**; confirm that before running:

```sql
SELECT count() FROM corpscout.br_company_relations;  -- must be 0
```

If it is not 0, someone materialized in the meantime and the drop would destroy history. Stop and re-plan.

Then the first materialization, which also needs the Task 1 gate from the socios plan — **verify the 11-column layout against a real archive first**; a wrong column *order* still passes the count guard silently.

## What to measure after the second run

The first run only proves the snapshot path. The merge is unexercised until a second month exists:

```sql
-- spells opened and closed per merged month
SELECT * FROM corpscout.br_company_relations_snapshots ORDER BY snapshot_year_month;

-- does anything actually change month to month?
SELECT is_current, count() FROM corpscout.br_company_relations GROUP BY is_current;

-- role changes: the same person and company under two codes
SELECT cnpj_basico, related_tax_id, groupArray(relation_code) AS codes
FROM corpscout.br_company_relations
GROUP BY cnpj_basico, related_tax_id HAVING length(codes) > 1 LIMIT 20;

-- re-entries: the same person, company and role under two entry dates
SELECT cnpj_basico, related_tax_id, relation_code,
       groupArray(relation_since_key) AS spells
FROM corpscout.br_company_relations
GROUP BY cnpj_basico, related_tax_id, relation_code
HAVING length(spells) > 1 LIMIT 20;
```

The last two are the point of the whole design. If they return nothing after several months, either Brazilian ownership is unusually static or the spell keys are wrong — and it is worth knowing which.
