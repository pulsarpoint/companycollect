# Normalized Listing Identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Supersedes** `2026-07-25-unified-company-listings.md`. That plan materialized a
> denormalized per-country listings table. This one normalizes the same facts into
> three independent layers and demotes `company_listings` to a view. The surviving
> pieces of the old plan — the `se_companies` dedup fix and the volume floor — are
> carried into Task 5 here.

**Goal:** Store listing facts once each, in three layers joined by their natural
keys, so that adding a country costs an entry in an identity table rather than a
pipeline.

**Architecture:** Three tables along their functional dependencies —
`instrument_venues` (what trades where), `isin_lei` (who issued it),
`company_lei` (which company that issuer is) — plus a `company_listings` view
that joins them. Every hop is many-to-one, so the join never fans out.

```text
instrument_venues (isin, mic, venue_source)
        |  N:1 on isin
        v
isin_lei (isin, lei, mapping_source)
        |  N:1 on lei
        v
company_lei (lei, country_code, company_id)
        |
        v
company_listings  [VIEW]
```

**Tech Stack:** ClickHouse (MergeTree, `EXCHANGE TABLES`), Dagster assets,
golang-migrate, pytest.

## Global Constraints

- The migration owns the schema. Python asserts tables exist
  (`assert_clickhouse_tables_exist`) and never issues DDL for a target table.
- `ORDER BY` columns must be non-nullable (`allow_nullable_key` is off).
- Non-nullable `String` / `LowCardinality(String)` columns must receive `''`,
  never `NULL` — the native driver calls `.encode()` per value and dies on `None`.
- SQL line comments must not contain `;` — the migration runner splits on it.
- No `from __future__ import annotations` in modules defining `@dg.asset`.
- Add every migration name to `EXPECTED_MIGRATIONS` in
  `tests/test_clickhouse_migrations.py`.
- Never `git add -A`. Commit by explicit path.
- Refuse to replace a populated table from an empty or under-floor result.
- Validate with `uv run dg check defs` before finishing each task.

## Cardinality Contract

These are the assumptions the design rests on. Task 7 verifies them against live
data; if one fails, the affected grain must change.

| Relationship | Cardinality | Evidence |
|---|---|---|
| ISIN ↔ MIC | many-to-many | FIRDS's own grain |
| ISIN → LEI | N:1 | 0 of 3.04M sampled ISINs had >1 LEI in the 2026-07-24 GLEIF file |
| LEI → company | N:1 | one LEI identifies one legal entity |
| company → LEI | 1:N over time | successor entities (`successor_entity_lei`) |
| LEI → ISIN | 1:N, ~92 avg | 98,246 LEIs across 9.06M ISINs |

**Consequence:** "how many companies are listed" is always
`count(DISTINCT company_id)`, never a row count — they differ by ~2 orders of
magnitude. Any market-cap rollup must restrict to `cfi_category = 'E'`.

## Out of Scope

- `company_market_value` (price × shares outstanding) — separate plan.
- The GLEIF ISIN-LEI file ingest — it has zero Brazilian rows and is a UK/EU
  concern; revisit when the UK is scheduled.
- The Brazil resolver. Brazil has no LEI-free path in this design yet; Task 7
  measures whether it needs one (see "Known Gap").

## Known Gap: non-LEI countries

Every path here runs through LEI. Brazil's `br_cvm_companies` gives CNPJ and
`cvm_code` with no LEI, and Brazilian ISINs are absent from GLEIF's mapping
file. If Task 7 shows Brazilian LEI coverage is poor, layer B needs a sibling —
an `instrument_company` table carrying direct instrument→company edges for
markets where no LEI bridge exists. Do not build that speculatively; the
measurement in Task 7 decides it.

## File Structure

| File | Responsibility |
|---|---|
| `clickhouse/migrations/000172_corpscout_instrument_venues.{up,down}.sql` | Layer A DDL |
| `clickhouse/migrations/000173_corpscout_isin_lei_slim.{up,down}.sql` | Drops layer-A columns from `isin_lei` |
| `clickhouse/migrations/000174_corpscout_company_lei.{up,down}.sql` | Layer C DDL |
| `clickhouse/migrations/000175_corpscout_company_listings_view.{up,down}.sql` | The join view |
| `clickhouse/migrations/000176_corpscout_drop_se_company_listings.{up,down}.sql` | Retires the old table (gated) |
| `defs/instrument_venues/tables.py` | Layer A column contract |
| `defs/instrument_venues/firds.py` | FIRDS → layer A projection |
| `defs/instrument_venues/eodhd.py` | EODHD → layer A projection |
| `defs/instrument_venues/assets.py` | Layer A asset + job |
| `defs/company_lei/tables.py` | Layer C column contract |
| `defs/company_lei/rules.py` | Per-country identity rules registry |
| `defs/company_lei/assets.py` | Layer C asset + job |
| `defs/isin_lei/assets.py` | Modified — drops the two layer-A columns |

---

## Table Schemas

### Layer A — `corpscout.instrument_venues`

Grain `(isin, mic, venue_source)`. Two sources asserting the same admission is
corroboration, not a conflict, so both rows are kept and a consumer picks by
`evidence_tier`. Country-agnostic: no partitioning, no per-country builder.

```sql
CREATE TABLE IF NOT EXISTS corpscout.instrument_venues
(
    isin                         String,
    mic                          LowCardinality(String),
    venue_source                 LowCardinality(String),
    operating_mic                LowCardinality(String),
    evidence_tier                LowCardinality(String),
    cfi_code                     LowCardinality(String),
    cfi_category                 LowCardinality(String),
    instrument_name              String,
    instrument_type              LowCardinality(String),
    ticker                       String,
    trading_currency             LowCardinality(String),
    trading_status               LowCardinality(String),
    is_current                   UInt8,
    admission_date               Nullable(Date),
    first_trade_date             Nullable(Date),
    termination_date             Nullable(Date),
    first_seen_date              Date,
    last_seen_date               Date,
    source_record_id             String,
    source_publication_date      Date,
    source_retrieved_at          DateTime64(3, 'UTC'),
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (isin, mic, venue_source);
```

Notes carried into the migration comment:
- `venue_source` ∈ `esma_firds` | `eodhd`; `evidence_tier` ∈ `regulator` | `vendor`.
- `mic` is the ISO 10383 **market segment** MIC as published by the source.
  `operating_mic` is its parent, `''` when the source does not supply one.
- `cfi_code` classifies the instrument, so it depends on ISIN alone. It is stored
  per venue row because that is how sources publish it. Two rows for one ISIN
  disagreeing on CFI is a data-quality signal, not legitimate variation.

### Layer B — `corpscout.isin_lei` (existing, slimmed)

Migration `000171` created this with `venue_confirmed` and `cfi_category`. Both
are layer-A facts and become redundant once `instrument_venues` exists — drop
them. Final shape:

```sql
isin              String
lei               String
mapping_source    LowCardinality(String)
first_seen_date   Date
last_seen_date    Date
source_run_id     String
resolved_at       DateTime64(3, 'UTC')
ENGINE = MergeTree
ORDER BY (isin, lei, mapping_source)
```

### Layer C — `corpscout.company_lei`

Grain `(lei, country_code, company_id)`. **`lei` leads the sort key** because the
join always arrives from layer B holding an LEI; country filtering is secondary.

```sql
CREATE TABLE IF NOT EXISTS corpscout.company_lei
(
    lei                          String,
    country_code                 LowCardinality(String),
    company_id                   String,
    match_method                 LowCardinality(String),
    match_confidence             LowCardinality(String),
    registration_authority_id    LowCardinality(String),
    registered_as_raw            String,
    company_id_normalized        String,
    lei_entity_status            LowCardinality(String),
    lei_registration_status      LowCardinality(String),
    is_current                   UInt8,
    successor_lei                String,
    first_seen_date              Date,
    last_seen_date               Date,
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (lei, country_code, company_id);
```

Notes:
- `match_method` ∈ `registration_authority` (RA code corroborates the register)
  | `jurisdiction_normalized` (jurisdiction + normalized ID only).
- `match_confidence` ∈ `exact` | `normalized`.
- A row exists **only if `company_id` was found in that country's register.** The
  register is the ground truth; an unresolvable LEI produces no row.
- `is_current = 0` where the LEI has a `successor_lei`, so historical links stay
  queryable without polluting current answers.

### Layer D — `corpscout.company_listings` (VIEW)

```sql
CREATE VIEW IF NOT EXISTS corpscout.company_listings AS
SELECT
    c.country_code            AS country_code,
    c.company_id              AS company_id,
    c.lei                     AS lei,
    v.isin                    AS isin,
    v.mic                     AS mic,
    v.operating_mic           AS operating_mic,
    v.ticker                  AS ticker,
    v.cfi_code                AS cfi_code,
    v.cfi_category            AS cfi_category,
    v.instrument_name         AS instrument_name,
    v.instrument_type         AS instrument_type,
    v.trading_currency        AS trading_currency,
    v.trading_status          AS trading_status,
    v.is_current              AS is_current,
    v.admission_date          AS admission_date,
    v.first_trade_date        AS first_trade_date,
    v.termination_date        AS termination_date,
    v.venue_source            AS venue_source,
    v.evidence_tier           AS evidence_tier,
    c.match_confidence        AS identity_confidence,
    l.mapping_source          AS isin_lei_source
FROM corpscout.instrument_venues AS v
INNER JOIN corpscout.isin_lei AS l ON l.isin = v.isin
INNER JOIN corpscout.company_lei AS c ON c.lei = l.lei
WHERE c.is_current = 1;
```

---

### Task 1: Layer A table and column contract

**Files:**
- Create: `corpscout/clickhouse/migrations/000172_corpscout_instrument_venues.{up,down}.sql`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/__init__.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/tables.py`
- Create: `corpscout/services/dagster_v3/tests/test_instrument_venues.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`

**Interfaces:**
- Produces: `tables.INSTRUMENT_VENUES_TABLE = "instrument_venues"`,
  `tables.INSTRUMENT_VENUES_COLUMNS` (23-tuple in DDL order),
  `tables.QUALIFIED_INSTRUMENT_VENUES_TABLE`, `tables.CLICKHOUSE_DATABASE`,
  `tables.GROUP_NAME = "instrument_venues"`.

- [ ] **Step 1: Write the failing contract test**

Create `tests/test_instrument_venues.py`:

```python
from dagster_v3.defs.instrument_venues import tables


def test_instrument_venues_column_contract() -> None:
    assert tables.INSTRUMENT_VENUES_TABLE == "instrument_venues"
    assert tables.INSTRUMENT_VENUES_COLUMNS == (
        "isin",
        "mic",
        "venue_source",
        "operating_mic",
        "evidence_tier",
        "cfi_code",
        "cfi_category",
        "instrument_name",
        "instrument_type",
        "ticker",
        "trading_currency",
        "trading_status",
        "is_current",
        "admission_date",
        "first_trade_date",
        "termination_date",
        "first_seen_date",
        "last_seen_date",
        "source_record_id",
        "source_publication_date",
        "source_retrieved_at",
        "source_run_id",
        "resolved_at",
    )
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/test_instrument_venues.py -v`
Expected: FAIL `ModuleNotFoundError: No module named 'dagster_v3.defs.instrument_venues'`

- [ ] **Step 3: Create the module**

`defs/instrument_venues/__init__.py` — empty file.

`defs/instrument_venues/tables.py`:

```python
CLICKHOUSE_DATABASE = "corpscout"
GROUP_NAME = "instrument_venues"

INSTRUMENT_VENUES_TABLE = "instrument_venues"
QUALIFIED_INSTRUMENT_VENUES_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{INSTRUMENT_VENUES_TABLE}"
)

FIRDS_VENUE_SOURCE = "esma_firds"
EODHD_VENUE_SOURCE = "eodhd"

REGULATOR_EVIDENCE_TIER = "regulator"
VENDOR_EVIDENCE_TIER = "vendor"

INSTRUMENT_VENUES_COLUMNS = (
    "isin",
    "mic",
    "venue_source",
    "operating_mic",
    "evidence_tier",
    "cfi_code",
    "cfi_category",
    "instrument_name",
    "instrument_type",
    "ticker",
    "trading_currency",
    "trading_status",
    "is_current",
    "admission_date",
    "first_trade_date",
    "termination_date",
    "first_seen_date",
    "last_seen_date",
    "source_record_id",
    "source_publication_date",
    "source_retrieved_at",
    "source_run_id",
    "resolved_at",
)
```

- [ ] **Step 4: Run and confirm it passes**

Run: `uv run pytest tests/test_instrument_venues.py -v`
Expected: PASS

- [ ] **Step 5: Write the failing migration test**

Add to `tests/test_clickhouse_migrations.py`, and add the import
`from dagster_v3.defs.instrument_venues import tables as instrument_venues_tables`
next to the other defs imports:

```python
def test_instrument_venues_migration_covers_columns_in_order() -> None:
    sql = _migration_sql("000172_corpscout_instrument_venues.up.sql")
    down_sql = _migration_sql("000172_corpscout_instrument_venues.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.instrument_venues" in sql
    last_index = -1
    for column_name in instrument_venues_tables.INSTRUMENT_VENUES_COLUMNS:
        index = sql.index(f"    {column_name} ")
        assert index > last_index
        last_index = index

    assert "ENGINE = MergeTree" in sql
    assert "ORDER BY (isin, mic, venue_source)" in sql
    assert "DROP TABLE IF EXISTS corpscout.instrument_venues" in down_sql
```

Add `"000172_corpscout_instrument_venues",` to `EXPECTED_MIGRATIONS` after
`"000171_corpscout_isin_lei",`.

- [ ] **Step 6: Run and confirm failure**

Run: `uv run pytest tests/test_clickhouse_migrations.py -k "instrument_venues or migration_files" -v`
Expected: FAIL `FileNotFoundError: ... 000172_corpscout_instrument_venues.up.sql`

- [ ] **Step 7: Write the migration pair**

`000172_corpscout_instrument_venues.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- What trades where, across every venue source and every country.
-- Grain is (isin, mic, venue_source). Two sources asserting the same admission
-- is corroboration rather than conflict, so both rows are kept and consumers
-- select by evidence_tier.
--
-- mic is the ISO 10383 market segment MIC as published by the source.
-- operating_mic is its parent venue, empty when the source supplies none.
--
-- cfi_code classifies the instrument and therefore depends on ISIN alone. It is
-- stored per venue row because that is how sources publish it. Two rows for one
-- ISIN disagreeing on CFI is a data-quality signal, not real variation.
--
-- This table says nothing about which company owns the instrument. That link is
-- corpscout.isin_lei followed by corpscout.company_lei.
CREATE TABLE IF NOT EXISTS corpscout.instrument_venues
(
    isin                         String,
    mic                          LowCardinality(String),
    venue_source                 LowCardinality(String),
    operating_mic                LowCardinality(String),
    evidence_tier                LowCardinality(String),
    cfi_code                     LowCardinality(String),
    cfi_category                 LowCardinality(String),
    instrument_name              String,
    instrument_type              LowCardinality(String),
    ticker                       String,
    trading_currency             LowCardinality(String),
    trading_status               LowCardinality(String),
    is_current                   UInt8,
    admission_date               Nullable(Date),
    first_trade_date             Nullable(Date),
    termination_date             Nullable(Date),
    first_seen_date              Date,
    last_seen_date               Date,
    source_record_id             String,
    source_publication_date      Date,
    source_retrieved_at          DateTime64(3, 'UTC'),
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (isin, mic, venue_source);
```

`000172_corpscout_instrument_venues.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.instrument_venues;
```

- [ ] **Step 8: Run and confirm passing**

Run: `uv run pytest tests/test_clickhouse_migrations.py tests/test_instrument_venues.py -q`
Expected: PASS (no new failures beyond pre-existing ones)

- [ ] **Step 9: Commit**

```bash
git add corpscout/clickhouse/migrations/000172_corpscout_instrument_venues.up.sql \
        corpscout/clickhouse/migrations/000172_corpscout_instrument_venues.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/__init__.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/tables.py \
        corpscout/services/dagster_v3/tests/test_instrument_venues.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(corpscout): add instrument_venues table"
```

---

### Task 2: FIRDS projection into layer A

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/firds.py`
- Create: `corpscout/services/dagster_v3/tests/test_instrument_venues_firds.py`

**Interfaces:**
- Consumes: `tables.INSTRUMENT_VENUES_COLUMNS` (Task 1).
- Produces: `build_firds_instrument_venues_sql(stage_table: str) -> str`.

Sourced from `firds_instruments_current` (not the event history): this layer
answers "what trades where **now**", unlike `isin_lei`, which answers a durable
identity question and therefore reads the event history.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_instrument_venues_firds.py`:

```python
from dagster_v3.defs.instrument_venues.firds import (
    build_firds_instrument_venues_sql,
)

_STAGE = "`corpscout`.`_tmp_instrument_venues_test`"


def test_firds_projection_reads_current_state_not_history() -> None:
    """Layer A is current admission; identity durability lives in isin_lei."""
    sql = build_firds_instrument_venues_sql(_STAGE)

    assert "FROM corpscout.firds_instruments_current" in sql
    assert "firds_instrument_events" not in sql


def test_firds_projection_is_not_country_filtered() -> None:
    sql = build_firds_instrument_venues_sql(_STAGE)

    assert "competent_authority_country" not in sql
    assert "XSTO" not in sql


def test_firds_projection_marks_regulator_evidence() -> None:
    sql = build_firds_instrument_venues_sql(_STAGE)

    assert "'esma_firds' AS venue_source" in sql
    assert "'regulator' AS evidence_tier" in sql
    assert "'' AS ticker" in sql
    assert "substring(upperUTF8(trimBoth(f.cfi_code)), 1, 1) AS cfi_category" in sql


def test_firds_projection_requires_both_grain_identifiers() -> None:
    sql = build_firds_instrument_venues_sql(_STAGE)

    assert "WHERE trimBoth(f.isin) != ''" in sql
    assert "AND trimBoth(f.mic) != ''" in sql
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_instrument_venues_firds.py -v`
Expected: FAIL `ModuleNotFoundError: ... instrument_venues.firds`

- [ ] **Step 3: Implement the projection**

Create `defs/instrument_venues/firds.py`:

```python
from dagster_v3.defs.instrument_venues import tables


def build_firds_instrument_venues_sql(stage_table: str) -> str:
    """Project current FIRDS admissions into the shared venue contract.

    FIRDS is instrument-scoped and never filtered by country, so EU-admitted
    instruments of non-EU issuers land here too. FIRDS supplies no ticker, so
    that column is empty and EODHD fills the gap on its own rows.
    """
    columns = ", ".join(tables.INSTRUMENT_VENUES_COLUMNS)
    return f"""INSERT INTO {stage_table} ({columns})
SELECT
    upperUTF8(trimBoth(f.isin)) AS isin,
    upperUTF8(trimBoth(f.mic)) AS mic,
    '{tables.FIRDS_VENUE_SOURCE}' AS venue_source,
    upperUTF8(trimBoth(f.relevant_venue_mic)) AS operating_mic,
    '{tables.REGULATOR_EVIDENCE_TIER}' AS evidence_tier,
    upperUTF8(trimBoth(f.cfi_code)) AS cfi_code,
    substring(upperUTF8(trimBoth(f.cfi_code)), 1, 1) AS cfi_category,
    if(f.short_name != '', f.short_name, f.full_name) AS instrument_name,
    '' AS instrument_type,
    '' AS ticker,
    upperUTF8(trimBoth(f.notional_currency)) AS trading_currency,
    'current' AS trading_status,
    toUInt8(1) AS is_current,
    toDate(f.admission_approval_at) AS admission_date,
    toDate(f.first_trade_at) AS first_trade_date,
    toDate(f.termination_at) AS termination_date,
    f.source_publication_date AS first_seen_date,
    f.source_publication_date AS last_seen_date,
    f.source_record_id AS source_record_id,
    f.source_publication_date AS source_publication_date,
    f.source_retrieved_at AS source_retrieved_at,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
FROM corpscout.firds_instruments_current AS f
WHERE trimBoth(f.isin) != ''
  AND trimBoth(f.mic) != ''"""
```

- [ ] **Step 4: Run and confirm passing**

Run: `uv run pytest tests/test_instrument_venues_firds.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/firds.py \
        corpscout/services/dagster_v3/tests/test_instrument_venues_firds.py
git commit -m "feat(corpscout): project FIRDS current admissions into instrument_venues"
```

---

### Task 3: EODHD projection and the layer A asset

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/eodhd.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/assets.py`
- Create: `corpscout/services/dagster_v3/tests/test_instrument_venues_assets.py`

**Interfaces:**
- Consumes: `build_firds_instrument_venues_sql` (Task 2).
- Produces: `build_eodhd_instrument_venues_sql(stage_table: str) -> str`,
  `replace_instrument_venues_clickhouse(*, clickhouse, source_run_id, resolved_at) -> dict[str, object]`,
  asset `instrument_venues_clickhouse`, `INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS`.

EODHD is the vendor tier and the only source for non-FIRDS markets. Its `isin`
is nullable — rows without one cannot join layer B and are dropped here, with the
dropped count reported as metadata.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_instrument_venues_assets.py`:

```python
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.instrument_venues.assets import (
    INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS,
    instrument_venues_clickhouse,
    replace_instrument_venues_clickhouse,
)
from dagster_v3.defs.instrument_venues.eodhd import (
    build_eodhd_instrument_venues_sql,
)

_STAGE = "`corpscout`.`_tmp_instrument_venues_test`"


def test_eodhd_projection_marks_vendor_evidence_and_needs_an_isin() -> None:
    sql = build_eodhd_instrument_venues_sql(_STAGE)

    assert "'eodhd' AS venue_source" in sql
    assert "'vendor' AS evidence_tier" in sql
    assert "FROM corpscout.eodhd_symbols AS s" in sql
    assert "INNER JOIN corpscout.eodhd_symbol_mics AS m" in sql
    assert "trimBoth(ifNull(s.isin, '')) != ''" in sql


def test_eodhd_projection_carries_delisting_into_trading_status() -> None:
    sql = build_eodhd_instrument_venues_sql(_STAGE)

    assert "s.is_delisted" in sql
    assert "AS is_current" in sql


def test_asset_depends_on_both_venue_sources() -> None:
    spec = instrument_venues_clickhouse.specs_by_key[
        instrument_venues_clickhouse.key
    ]

    assert {dep.asset_key for dep in spec.deps} == {
        dg.AssetKey(key) for key in INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS
    }
    assert INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS == (
        "esma_firds_clickhouse",
        "eodhd_reference_complete",
    )


class _FakeClickHouseClient:
    def __init__(self, quality_row: tuple[object, ...]) -> None:
        self.quality_row = quality_row
        self.statements: list[str] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params is not None else ()
            return [(table,) for table in requested]
        if "row_count" in sql:
            return [self.quality_row]
        return []


def _resource(
    monkeypatch: pytest.MonkeyPatch,
    client: _FakeClickHouseClient,
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def test_replace_inserts_both_sources_then_exchanges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # row_count, isin_count, mic_count, venue_key_count, firds_rows,
    # eodhd_rows, invalid_rows, latest_source_publication_date
    client = _FakeClickHouseClient((10, 6, 3, 10, 7, 3, 0, date(2026, 7, 25)))
    resource = _resource(monkeypatch, client)

    metadata = replace_instrument_venues_clickhouse(
        clickhouse=resource,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
    )

    inserts = [s for s in client.statements if s.startswith("INSERT INTO")]
    assert len(inserts) == 2
    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")
    assert metadata["row_count"] == 10
    assert metadata["firds_rows"] == 7
    assert metadata["eodhd_rows"] == 3


def test_replace_refuses_empty_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClickHouseClient((0, 0, 0, 0, 0, 0, 0, None))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="no instrument venue rows"):
        replace_instrument_venues_clickhouse(
            clickhouse=resource,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_replace_refuses_when_a_source_collapses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A populated table must not be replaced by one missing a whole source."""
    client = _FakeClickHouseClient((7, 6, 3, 7, 7, 0, 0, date(2026, 7, 25)))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="contributed no rows"):
        replace_instrument_venues_clickhouse(
            clickhouse=resource,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_instrument_venues_assets.py -v`
Expected: FAIL `ModuleNotFoundError: ... instrument_venues.assets`

- [ ] **Step 3: Implement the EODHD projection**

Create `defs/instrument_venues/eodhd.py`:

```python
from dagster_v3.defs.instrument_venues import tables


def build_eodhd_instrument_venues_sql(stage_table: str) -> str:
    """Project EODHD symbol/venue pairs into the shared venue contract.

    EODHD is the vendor tier and the only venue source for markets outside
    FIRDS. Symbols without an ISIN cannot join corpscout.isin_lei and are
    excluded here rather than stored unjoinable.
    """
    columns = ", ".join(tables.INSTRUMENT_VENUES_COLUMNS)
    return f"""INSERT INTO {stage_table} ({columns})
SELECT
    upperUTF8(trimBoth(ifNull(s.isin, ''))) AS isin,
    upperUTF8(trimBoth(m.mic)) AS mic,
    '{tables.EODHD_VENUE_SOURCE}' AS venue_source,
    upperUTF8(trimBoth(ifNull(x.operating_mic_raw, ''))) AS operating_mic,
    '{tables.VENDOR_EVIDENCE_TIER}' AS evidence_tier,
    '' AS cfi_code,
    '' AS cfi_category,
    s.symbol_name AS instrument_name,
    s.instrument_type AS instrument_type,
    s.ticker AS ticker,
    upperUTF8(trimBoth(ifNull(s.currency, ''))) AS trading_currency,
    if(s.is_delisted = 1, 'delisted', 'current') AS trading_status,
    toUInt8(s.is_delisted = 0) AS is_current,
    CAST(NULL AS Nullable(Date)) AS admission_date,
    CAST(NULL AS Nullable(Date)) AS first_trade_date,
    CAST(NULL AS Nullable(Date)) AS termination_date,
    toDate(m.resolved_at) AS first_seen_date,
    toDate(m.resolved_at) AS last_seen_date,
    s.eodhd_symbol_key AS source_record_id,
    toDate(s.retrieved_at) AS source_publication_date,
    s.retrieved_at AS source_retrieved_at,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
FROM corpscout.eodhd_symbols AS s
INNER JOIN corpscout.eodhd_symbol_mics AS m
    ON m.eodhd_symbol_key = s.eodhd_symbol_key
LEFT JOIN corpscout.eodhd_exchanges AS x
    ON x.exchange_code = s.exchange_code
WHERE trimBoth(ifNull(s.isin, '')) != ''
  AND trimBoth(m.mic) != ''"""
```

- [ ] **Step 4: Implement the asset**

Create `defs/instrument_venues/assets.py`:

```python
import uuid
from datetime import UTC, date, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.instrument_venues import tables
from dagster_v3.defs.instrument_venues.eodhd import (
    build_eodhd_instrument_venues_sql,
)
from dagster_v3.defs.instrument_venues.firds import (
    build_firds_instrument_venues_sql,
)

INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS = (
    "esma_firds_clickhouse",
    "eodhd_reference_complete",
)

_REQUIRED_CLICKHOUSE_TABLES = (
    tables.INSTRUMENT_VENUES_TABLE,
    "firds_instruments_current",
    "eodhd_symbols",
    "eodhd_symbol_mics",
    "eodhd_exchanges",
)

_QUALITY_COLUMNS = (
    "row_count",
    "isin_count",
    "mic_count",
    "venue_key_count",
    "firds_rows",
    "eodhd_rows",
    "invalid_rows",
    "latest_source_publication_date",
)


def _qualified(table_name: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table_name}`"


def _quality_sql(stage_table: str) -> str:
    return f"""SELECT
    count() AS row_count,
    uniqExact(isin) AS isin_count,
    uniqExact(mic) AS mic_count,
    uniqExact((isin, mic, venue_source)) AS venue_key_count,
    countIf(venue_source = '{tables.FIRDS_VENUE_SOURCE}') AS firds_rows,
    countIf(venue_source = '{tables.EODHD_VENUE_SOURCE}') AS eodhd_rows,
    countIf(isin = '' OR mic = '' OR venue_source = '' OR evidence_tier = '')
        AS invalid_rows,
    max(source_publication_date) AS latest_source_publication_date
FROM {stage_table}"""


def _validate_quality(quality: dict[str, object]) -> None:
    row_count = int(quality["row_count"])
    venue_key_count = int(quality["venue_key_count"])
    invalid_rows = int(quality["invalid_rows"])

    if row_count == 0:
        raise ValueError("Instrument venue projection produced no instrument venue rows")
    if venue_key_count != row_count:
        raise ValueError(
            "Instrument venue grain mismatch: "
            f"rows={row_count} unique_keys={venue_key_count}"
        )
    if invalid_rows != 0:
        raise ValueError(f"Instrument venues contain invalid rows: {invalid_rows}")
    for source_column in ("firds_rows", "eodhd_rows"):
        if int(quality[source_column]) == 0:
            raise ValueError(
                f"Instrument venue source contributed no rows: {source_column}"
            )


def replace_instrument_venues_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, object]:
    """Atomically rebuild the cross-source instrument/venue table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=_REQUIRED_CLICKHOUSE_TABLES,
    )
    stage_name = f"_tmp_{tables.INSTRUMENT_VENUES_TABLE}_{uuid.uuid4().hex}"
    qualified_stage = _qualified(stage_name)
    qualified_target = _qualified(tables.INSTRUMENT_VENUES_TABLE)
    parameters = {"source_run_id": source_run_id, "resolved_at": resolved_at}

    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
        primary_error: Exception | None = None
        try:
            client.execute(
                build_firds_instrument_venues_sql(qualified_stage), parameters
            )
            client.execute(
                build_eodhd_instrument_venues_sql(qualified_stage), parameters
            )
            row = client.execute(_quality_sql(qualified_stage))[0]
            quality = {
                column: value
                for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
            }
            _validate_quality(quality)
            client.execute(f"EXCHANGE TABLES {qualified_stage} AND {qualified_target}")
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
            except Exception:
                if primary_error is None:
                    raise

    latest = quality["latest_source_publication_date"]
    return {
        **quality,
        "latest_source_publication_date": (
            latest.isoformat()
            if isinstance(latest, (date, datetime))
            else str(latest or "")
        ),
        "table": tables.QUALIFIED_INSTRUMENT_VENUES_TABLE,
        "source_run_id": source_run_id,
    }


@dg.asset(
    name="instrument_venues_clickhouse",
    deps=[dg.AssetKey(key) for key in INSTRUMENT_VENUES_UPSTREAM_ASSET_KEYS],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    pool="instrument_venues_clickhouse",
    metadata={"table": tables.QUALIFIED_INSTRUMENT_VENUES_TABLE},
    description=(
        "Rebuilds the cross-source instrument and venue table from FIRDS "
        "current admissions and EODHD symbol/MIC pairs. Says what trades "
        "where, not who owns it."
    ),
)
def instrument_venues_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_instrument_venues_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
    )
    context.log.info(
        "Rebuilt instrument venues: rows=%s firds=%s eodhd=%s isins=%s",
        metadata["row_count"],
        metadata["firds_rows"],
        metadata["eodhd_rows"],
        metadata["isin_count"],
    )
    return dg.MaterializeResult(metadata=metadata)


instrument_venues_job = dg.define_asset_job(
    "instrument_venues_job",
    selection=dg.AssetSelection.assets("instrument_venues_clickhouse"),
)

defs = dg.Definitions(
    assets=[instrument_venues_clickhouse],
    jobs=[instrument_venues_job],
)
```

- [ ] **Step 5: Run tests and definition checks**

Run: `uv run pytest tests/test_instrument_venues_assets.py -v && uv run dg check defs`
Expected: 5 passed, `All definitions loaded successfully.`

- [ ] **Step 6: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/eodhd.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/instrument_venues/assets.py \
        corpscout/services/dagster_v3/tests/test_instrument_venues_assets.py
git commit -m "feat(corpscout): add EODHD venue projection and instrument_venues asset"
```

---

### Task 4: Slim `isin_lei` to its layer

**Files:**
- Create: `corpscout/clickhouse/migrations/000173_corpscout_isin_lei_slim.{up,down}.sql`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/isin_lei/tables.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/isin_lei/assets.py`
- Modify: `corpscout/services/dagster_v3/tests/test_isin_lei.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`

`venue_confirmed` and `cfi_category` were added to `isin_lei` in migration
`000171` before `instrument_venues` existed. Both are venue facts and now live in
layer A. Dropping them removes the duplication and stops two tables disagreeing
about an instrument's category.

- [ ] **Step 1: Update the failing contract test**

In `tests/test_isin_lei.py`, change `test_isin_lei_table_contract` to expect:

```python
    assert tables.ISIN_LEI_COLUMNS == (
        "isin",
        "lei",
        "mapping_source",
        "first_seen_date",
        "last_seen_date",
        "source_run_id",
        "resolved_at",
    )
```

Delete `test_projection_keeps_latest_cfi_category_without_filtering_it`. In
`test_replace_isin_lei_is_atomic_and_reports_ambiguity` and the other publish
tests, the quality row is unchanged — those columns were never part of it.

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_isin_lei.py -v`
Expected: FAIL on the tuple comparison

- [ ] **Step 3: Update `tables.py` and the projection**

In `defs/isin_lei/tables.py`, remove `"venue_confirmed"` and `"cfi_category"`
from `ISIN_LEI_COLUMNS`.

In `defs/isin_lei/assets.py`, remove these three lines from the SELECT:

```sql
    toUInt8(1) AS venue_confirmed,
    argMax(cfi_category_observed, source_publication_date) AS cfi_category,
```

and drop `cfi_category_observed` from the `firds_identity` CTE. Update the
docstring: it no longer carries venue or category facts.

- [ ] **Step 4: Run and confirm passing**

Run: `uv run pytest tests/test_isin_lei.py -v`
Expected: PASS

- [ ] **Step 5: Write the migration**

`000173_corpscout_isin_lei_slim.up.sql`:

```sql
-- venue_confirmed and cfi_category are venue facts. They now live in
-- corpscout.instrument_venues (migration 000172), so remove the duplicates
-- here rather than let two tables disagree about one instrument.
ALTER TABLE corpscout.isin_lei DROP COLUMN IF EXISTS venue_confirmed;
ALTER TABLE corpscout.isin_lei DROP COLUMN IF EXISTS cfi_category;
```

`000173_corpscout_isin_lei_slim.down.sql`:

```sql
ALTER TABLE corpscout.isin_lei ADD COLUMN IF NOT EXISTS venue_confirmed UInt8 AFTER mapping_source;
ALTER TABLE corpscout.isin_lei ADD COLUMN IF NOT EXISTS cfi_category LowCardinality(String) AFTER venue_confirmed;
```

Add a migration test asserting both `DROP COLUMN` statements are present, and add
`"000173_corpscout_isin_lei_slim",` to `EXPECTED_MIGRATIONS`.

- [ ] **Step 6: Run the full check and commit**

Run: `uv run pytest tests/test_isin_lei.py tests/test_clickhouse_migrations.py -q && uv run dg check defs`

```bash
git add corpscout/clickhouse/migrations/000173_corpscout_isin_lei_slim.up.sql \
        corpscout/clickhouse/migrations/000173_corpscout_isin_lei_slim.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/isin_lei/tables.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/isin_lei/assets.py \
        corpscout/services/dagster_v3/tests/test_isin_lei.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "refactor(corpscout): move venue facts out of isin_lei"
```

---

### Task 5: Layer C — `company_lei`

**Files:**
- Create: `corpscout/clickhouse/migrations/000174_corpscout_company_lei.{up,down}.sql`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_lei/__init__.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_lei/tables.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_lei/rules.py`
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_lei/assets.py`
- Create: `corpscout/services/dagster_v3/tests/test_company_lei.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`

**Interfaces:**
- Produces: `tables.COMPANY_LEI_COLUMNS` (16-tuple, DDL order),
  `rules.CountryIdentityRule`, `rules.COUNTRY_IDENTITY_RULES`,
  `build_company_lei_insert_sql(stage_table: str, rule: CountryIdentityRule) -> str`,
  `replace_company_lei_clickhouse(...)`, asset `company_lei_clickhouse`.

This carries the two defect fixes from the superseded plan: the register is
joined **deduplicated** (`se_companies` is a `ReplacingMergeTree`; an
undeduplicated join fans out and trips the grain check intermittently), and a
per-country row floor prevents a degraded GLEIF refresh from emptying a country.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_company_lei.py`:

```python
from dagster_v3.defs.company_lei import tables
from dagster_v3.defs.company_lei.assets import build_company_lei_insert_sql
from dagster_v3.defs.company_lei.rules import COUNTRY_IDENTITY_RULES

_STAGE = "`corpscout`.`_tmp_company_lei_test`"
_SE = COUNTRY_IDENTITY_RULES["SE"]


def test_company_lei_column_contract() -> None:
    assert tables.COMPANY_LEI_TABLE == "company_lei"
    assert tables.COMPANY_LEI_COLUMNS == (
        "lei",
        "country_code",
        "company_id",
        "match_method",
        "match_confidence",
        "registration_authority_id",
        "registered_as_raw",
        "company_id_normalized",
        "lei_entity_status",
        "lei_registration_status",
        "is_current",
        "successor_lei",
        "first_seen_date",
        "last_seen_date",
        "source_run_id",
        "resolved_at",
    )


def test_sweden_rule_declares_register_and_normalization() -> None:
    assert _SE.country_code == "SE"
    assert _SE.register_table == "se_companies"
    assert _SE.identifier_length == 10
    assert _SE.min_expected_rows >= 100


def test_sql_deduplicates_the_replacing_merge_tree_register() -> None:
    """se_companies is a ReplacingMergeTree; a raw join fans out."""
    sql = build_company_lei_insert_sql(_STAGE, _SE)

    assert "register_current AS" in sql
    assert "GROUP BY company_id" in sql
    assert "INNER JOIN corpscout.se_companies AS r" not in sql


def test_sql_requires_the_identifier_to_exist_in_the_register() -> None:
    """The register is ground truth; an unresolvable LEI produces no row."""
    sql = build_company_lei_insert_sql(_STAGE, _SE)

    assert "INNER JOIN register_current AS r" in sql
    assert "r.company_id = g.company_id_normalized" in sql


def test_sql_tiers_confidence_by_registration_authority() -> None:
    sql = build_company_lei_insert_sql(_STAGE, _SE)

    assert "registration_authority" in sql
    assert "jurisdiction_normalized" in sql
    assert "registered_at_id" in sql


def test_sql_marks_superseded_leis_as_not_current() -> None:
    sql = build_company_lei_insert_sql(_STAGE, _SE)

    assert "successor_entity_lei" in sql
    assert "AS is_current" in sql


class _FakeClickHouseClient:
    def __init__(self, quality_row: tuple[object, ...]) -> None:
        self.quality_row = quality_row
        self.statements: list[str] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params is not None else ()
            return [(table,) for table in requested]
        if "row_count" in sql:
            return [self.quality_row]
        return []


def _resource(
    monkeypatch: pytest.MonkeyPatch,
    client: _FakeClickHouseClient,
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def test_replace_reports_the_authority_confidence_tier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # row_count, lei_count, company_count, identity_key_count,
    # authority_matched_rows, invalid_rows
    client = _FakeClickHouseClient((800, 800, 800, 800, 640, 0))
    resource = _resource(monkeypatch, client)

    metadata = replace_company_lei_clickhouse(
        clickhouse=resource,
        rule=_SE,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
    )

    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")
    assert metadata["row_count"] == 800
    assert metadata["authority_matched_rows"] == 640
    assert metadata["country_code"] == "SE"


def test_replace_refuses_a_degraded_gleif_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial GLEIF load must not empty a populated country."""
    client = _FakeClickHouseClient((12, 12, 12, 12, 0, 0))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="below the expected floor"):
        replace_company_lei_clickhouse(
            clickhouse=resource,
            rule=_SE,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_replace_refuses_duplicate_identity_grain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClickHouseClient((800, 800, 799, 799, 640, 0))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="grain mismatch"):
        replace_company_lei_clickhouse(
            clickhouse=resource,
            rule=_SE,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)
```

The imports at the top of this test file:

```python
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.company_lei import tables
from dagster_v3.defs.company_lei.assets import (
    build_company_lei_insert_sql,
    replace_company_lei_clickhouse,
)
from dagster_v3.defs.company_lei.rules import COUNTRY_IDENTITY_RULES
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_company_lei.py -v`
Expected: FAIL `ModuleNotFoundError: No module named 'dagster_v3.defs.company_lei'`

- [ ] **Step 3: Create the migration**

`000174_corpscout_company_lei.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Which company an LEI identifies, per country.
-- lei leads the sort key because the join always arrives holding an LEI from
-- corpscout.isin_lei. Country filtering is secondary.
--
-- A row exists only when company_id was found in that country's register. The
-- register is the ground truth: an LEI whose registered_as does not resolve
-- produces no row rather than an unverified guess.
--
-- One company may hold several LEIs over time through entity succession, so
-- is_current distinguishes the live link from superseded ones.
CREATE TABLE IF NOT EXISTS corpscout.company_lei
(
    lei                          String,
    country_code                 LowCardinality(String),
    company_id                   String,
    match_method                 LowCardinality(String),
    match_confidence             LowCardinality(String),
    registration_authority_id    LowCardinality(String),
    registered_as_raw            String,
    company_id_normalized        String,
    lei_entity_status            LowCardinality(String),
    lei_registration_status      LowCardinality(String),
    is_current                   UInt8,
    successor_lei                String,
    first_seen_date              Date,
    last_seen_date               Date,
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (lei, country_code, company_id);
```

`000174_corpscout_company_lei.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.company_lei;
```

Add `"000174_corpscout_company_lei",` to `EXPECTED_MIGRATIONS` and a migration
test mirroring Task 1 Step 5, asserting
`ORDER BY (lei, country_code, company_id)`.

- [ ] **Step 4: Create `tables.py` and `rules.py`**

`defs/company_lei/tables.py`:

```python
CLICKHOUSE_DATABASE = "corpscout"
GROUP_NAME = "company_lei"

COMPANY_LEI_TABLE = "company_lei"
QUALIFIED_COMPANY_LEI_TABLE = f"{CLICKHOUSE_DATABASE}.{COMPANY_LEI_TABLE}"

COMPANY_LEI_COLUMNS = (
    "lei",
    "country_code",
    "company_id",
    "match_method",
    "match_confidence",
    "registration_authority_id",
    "registered_as_raw",
    "company_id_normalized",
    "lei_entity_status",
    "lei_registration_status",
    "is_current",
    "successor_lei",
    "first_seen_date",
    "last_seen_date",
    "source_run_id",
    "resolved_at",
)
```

`defs/company_lei/rules.py`:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class CountryIdentityRule:
    """How one country's national identifier is recovered from a GLEIF record.

    registration_authority_ids is the set of GLEIF Registration Authority codes
    known to publish this country's company register. It starts empty: the codes
    are discovered empirically by the query in Task 7, because a code that
    resolves against the register is better evidence than a code copied from a
    reference list. An empty set simply means every row lands in the lower
    jurisdiction_normalized tier until the codes are filled in.
    """

    country_code: str
    register_table: str
    identifier_length: int
    min_expected_rows: int
    registration_authority_ids: frozenset[str] = frozenset()


COUNTRY_IDENTITY_RULES = {
    "SE": CountryIdentityRule(
        country_code="SE",
        register_table="se_companies",
        identifier_length=10,
        min_expected_rows=500,
    ),
}
```

- [ ] **Step 5: Implement the builder in `assets.py`**

Create `defs/company_lei/assets.py` with `build_company_lei_insert_sql`:

```python
def build_company_lei_insert_sql(
    stage_table: str,
    rule: CountryIdentityRule,
) -> str:
    """Resolve GLEIF records to one country's register.

    The register is joined deduplicated because country company tables are
    ReplacingMergeTree: an undeduplicated join multiplies rows for any
    company_id with unmerged parts and trips the grain check intermittently.
    """
    columns = ", ".join(tables.COMPANY_LEI_COLUMNS)
    authority_ids = rule.registration_authority_ids
    authority_predicate = (
        " OR ".join(f"g.registered_at_id = '{code}'" for code in sorted(authority_ids))
        if authority_ids
        else "0"
    )
    return f"""INSERT INTO {stage_table} ({columns})
WITH
register_current AS
(
    SELECT company_id
    FROM corpscout.{rule.register_table}
    GROUP BY company_id
),
gleif_country AS
(
    SELECT
        upperUTF8(trimBoth(lei)) AS lei,
        argMax(ifNull(registered_at_id, ''), (resolved_at, source_run_id))
            AS registered_at_id,
        argMax(ifNull(registered_as, ''), (resolved_at, source_run_id))
            AS registered_as_raw,
        argMax(ifNull(entity_status, ''), (resolved_at, source_run_id))
            AS lei_entity_status,
        argMax(ifNull(registration_status, ''), (resolved_at, source_run_id))
            AS lei_registration_status,
        argMax(ifNull(successor_entity_lei, ''), (resolved_at, source_run_id))
            AS successor_lei,
        argMax(ifNull(jurisdiction, ''), (resolved_at, source_run_id))
            AS jurisdiction,
        min(toDate(resolved_at)) AS first_seen_date,
        max(toDate(resolved_at)) AS last_seen_date
    FROM corpscout.gleif_lei_records
    WHERE trimBoth(lei) != ''
    GROUP BY lei
),
gleif_normalized AS
(
    SELECT
        *,
        replaceRegexpAll(registered_as_raw, '[^0-9]', '') AS company_id_normalized
    FROM gleif_country
    WHERE upperUTF8(jurisdiction) = '{rule.country_code}'
)
SELECT
    g.lei AS lei,
    '{rule.country_code}' AS country_code,
    r.company_id AS company_id,
    if({authority_predicate}, 'registration_authority', 'jurisdiction_normalized')
        AS match_method,
    if({authority_predicate}, 'exact', 'normalized') AS match_confidence,
    g.registered_at_id AS registration_authority_id,
    g.registered_as_raw AS registered_as_raw,
    g.company_id_normalized AS company_id_normalized,
    g.lei_entity_status AS lei_entity_status,
    g.lei_registration_status AS lei_registration_status,
    toUInt8(g.successor_lei = '') AS is_current,
    g.successor_lei AS successor_lei,
    g.first_seen_date AS first_seen_date,
    g.last_seen_date AS last_seen_date,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
FROM gleif_normalized AS g
INNER JOIN register_current AS r
    ON r.company_id = g.company_id_normalized
WHERE length(g.company_id_normalized) = {rule.identifier_length}"""
```

Then add the publisher and asset to the same file:

```python
import uuid
from datetime import UTC, date, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_lei import tables
from dagster_v3.defs.company_lei.rules import (
    COUNTRY_IDENTITY_RULES,
    CountryIdentityRule,
)

COMPANY_LEI_UPSTREAM_ASSET_KEYS = (
    "gleif_reference_clickhouse",
    "sweden_company_companies_clickhouse",
)

_QUALITY_COLUMNS = (
    "row_count",
    "lei_count",
    "company_count",
    "identity_key_count",
    "authority_matched_rows",
    "invalid_rows",
)


def _qualified(table_name: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table_name}`"


def _quality_sql(stage_table: str) -> str:
    return f"""SELECT
    count() AS row_count,
    uniqExact(lei) AS lei_count,
    uniqExact(company_id) AS company_count,
    uniqExact((lei, country_code, company_id)) AS identity_key_count,
    countIf(match_method = 'registration_authority') AS authority_matched_rows,
    countIf(
        lei = ''
        OR country_code = ''
        OR company_id = ''
        OR match_method = ''
        OR match_confidence = ''
    ) AS invalid_rows
FROM {stage_table}"""


def _validate_quality(
    quality: dict[str, object],
    rule: CountryIdentityRule,
) -> None:
    row_count = int(quality["row_count"])
    identity_key_count = int(quality["identity_key_count"])
    invalid_rows = int(quality["invalid_rows"])

    if row_count == 0:
        raise ValueError(
            f"{rule.country_code} company LEI resolution produced no rows"
        )
    if row_count < rule.min_expected_rows:
        raise ValueError(
            f"{rule.country_code} company LEI rows below the expected floor: "
            f"rows={row_count} floor={rule.min_expected_rows}"
        )
    if identity_key_count != row_count:
        raise ValueError(
            f"{rule.country_code} company LEI grain mismatch: "
            f"rows={row_count} unique_keys={identity_key_count}"
        )
    if invalid_rows != 0:
        raise ValueError(
            f"{rule.country_code} company LEI rows are invalid: {invalid_rows}"
        )


def replace_company_lei_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    rule: CountryIdentityRule,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, object]:
    """Atomically rebuild one country's LEI to company resolution."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(
            tables.COMPANY_LEI_TABLE,
            "gleif_lei_records",
            rule.register_table,
        ),
    )
    stage_name = (
        f"_tmp_{tables.COMPANY_LEI_TABLE}_"
        f"{rule.country_code.lower()}_{uuid.uuid4().hex}"
    )
    qualified_stage = _qualified(stage_name)
    qualified_target = _qualified(tables.COMPANY_LEI_TABLE)

    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
        primary_error: Exception | None = None
        try:
            client.execute(
                build_company_lei_insert_sql(qualified_stage, rule),
                {"source_run_id": source_run_id, "resolved_at": resolved_at},
            )
            row = client.execute(_quality_sql(qualified_stage))[0]
            quality = {
                column: value
                for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
            }
            _validate_quality(quality, rule)
            client.execute(
                f"EXCHANGE TABLES {qualified_stage} AND {qualified_target}"
            )
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
            except Exception:
                if primary_error is None:
                    raise

    return {
        **quality,
        "country_code": rule.country_code,
        "register_table": rule.register_table,
        "table": tables.QUALIFIED_COMPANY_LEI_TABLE,
        "source_run_id": source_run_id,
    }


@dg.asset(
    name="company_lei_clickhouse",
    deps=[dg.AssetKey(key) for key in COMPANY_LEI_UPSTREAM_ASSET_KEYS],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    pool="company_lei_clickhouse",
    metadata={"table": tables.QUALIFIED_COMPANY_LEI_TABLE},
    description=(
        "Resolves GLEIF LEI records to national company identifiers by "
        "validating the normalized registered_as value against the country "
        "register. An LEI that does not resolve produces no row."
    ),
)
def company_lei_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_company_lei_clickhouse(
        clickhouse=clickhouse,
        rule=COUNTRY_IDENTITY_RULES["SE"],
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
    )
    context.log.info(
        "Resolved %s company LEIs: rows=%s companies=%s authority_tier=%s",
        metadata["country_code"],
        metadata["row_count"],
        metadata["company_count"],
        metadata["authority_matched_rows"],
    )
    return dg.MaterializeResult(metadata=metadata)


company_lei_job = dg.define_asset_job(
    "company_lei_job",
    selection=dg.AssetSelection.assets("company_lei_clickhouse"),
)

defs = dg.Definitions(
    assets=[company_lei_clickhouse],
    jobs=[company_lei_job],
)
```

Note the single-country asset: `COUNTRY_IDENTITY_RULES` has one entry, and
`EXCHANGE TABLES` replaces the whole table. When the second country is added,
this must become either one asset per country writing its own partition, or a
single asset looping every rule into one stage table before the swap. Do not
add a second rule without making that change — a second country would otherwise
silently erase the first.

- [ ] **Step 6: Run tests and definition checks**

Run: `uv run pytest tests/test_company_lei.py tests/test_clickhouse_migrations.py -q && uv run dg check defs`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add corpscout/clickhouse/migrations/000174_corpscout_company_lei.up.sql \
        corpscout/clickhouse/migrations/000174_corpscout_company_lei.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_lei/ \
        corpscout/services/dagster_v3/tests/test_company_lei.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(corpscout): add company_lei identity layer"
```

---

### Task 6: The `company_listings` view

**Files:**
- Create: `corpscout/clickhouse/migrations/000175_corpscout_company_listings_view.{up,down}.sql`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`

- [ ] **Step 1: Write the failing migration test**

```python
def test_company_listings_view_joins_the_three_layers() -> None:
    sql = _migration_sql("000175_corpscout_company_listings_view.up.sql")
    down_sql = _migration_sql("000175_corpscout_company_listings_view.down.sql")

    assert "CREATE VIEW IF NOT EXISTS corpscout.company_listings" in sql
    assert "FROM corpscout.instrument_venues AS v" in sql
    assert "INNER JOIN corpscout.isin_lei AS l ON l.isin = v.isin" in sql
    assert "INNER JOIN corpscout.company_lei AS c ON c.lei = l.lei" in sql
    assert "WHERE c.is_current = 1" in sql
    assert "DROP VIEW IF EXISTS corpscout.company_listings" in down_sql
```

Add `"000175_corpscout_company_listings_view",` to `EXPECTED_MIGRATIONS`.

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_clickhouse_migrations.py -k "company_listings_view or migration_files" -v`
Expected: FAIL `FileNotFoundError`

- [ ] **Step 3: Write the migration**

Use the view DDL from the "Layer D" section above verbatim as
`000175_corpscout_company_listings_view.up.sql`, preceded by
`CREATE DATABASE IF NOT EXISTS corpscout;`.

`000175_corpscout_company_listings_view.down.sql`:

```sql
DROP VIEW IF EXISTS corpscout.company_listings;
```

- [ ] **Step 4: Run and commit**

Run: `uv run pytest tests/test_clickhouse_migrations.py -q`

```bash
git add corpscout/clickhouse/migrations/000175_corpscout_company_listings_view.up.sql \
        corpscout/clickhouse/migrations/000175_corpscout_company_listings_view.down.sql \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "feat(corpscout): add company_listings view over the three identity layers"
```

---

### Task 7: Live verification and RA-code discovery

Run after Tasks 1–6 are materialized against real ClickHouse. This is the step
that turns the cardinality contract from assumption into measurement, and it
produces the RA codes that `rules.py` deliberately left empty.

- [ ] **Step 1: Verify the cardinality contract**

```sql
-- ISIN -> LEI must be N:1. Expect 0 rows.
SELECT count() FROM (
    SELECT isin FROM corpscout.isin_lei GROUP BY isin HAVING uniqExact(lei) > 1
);

-- LEI -> company must be N:1 within a country. Expect 0 rows.
SELECT count() FROM (
    SELECT lei, country_code FROM corpscout.company_lei WHERE is_current = 1
    GROUP BY lei, country_code HAVING uniqExact(company_id) > 1
);

-- The view must not fan out beyond its instrument rows.
SELECT
    (SELECT count() FROM corpscout.company_listings) AS view_rows,
    (SELECT count() FROM corpscout.instrument_venues) AS venue_rows;
```

`view_rows` must be less than or equal to `venue_rows`. If it is greater, a hop
is not N:1 and the offending grain must change before anything is built on top.

- [ ] **Step 2: Discover the registration authority codes**

```sql
SELECT
    registration_authority_id,
    count()                         AS leis,
    uniqExact(company_id)           AS companies,
    min(first_seen_date)            AS since
FROM corpscout.company_lei
WHERE country_code = 'SE'
GROUP BY registration_authority_id
ORDER BY leis DESC;
```

The dominant code is Sweden's company register. Add it to
`COUNTRY_IDENTITY_RULES["SE"].registration_authority_ids` and re-materialize;
those rows move from `jurisdiction_normalized` to the `registration_authority`
tier.

- [ ] **Step 3: Measure per-country coverage**

```sql
-- What fraction of SE-jurisdiction LEIs reach a real company?
SELECT
    countIf(g.lei != '')                                       AS gleif_se_leis,
    countIf(c.lei != '')                                       AS resolved_leis,
    round(countIf(c.lei != '') / countIf(g.lei != ''), 4)       AS hit_rate
FROM corpscout.gleif_lei_records AS g
LEFT JOIN corpscout.company_lei AS c ON c.lei = upperUTF8(trimBoth(g.lei))
WHERE upperUTF8(ifNull(g.jurisdiction, '')) = 'SE';
```

Repeat with `'BR'` against `br_companies`. A poor Brazilian hit rate is the
trigger for the non-LEI sibling table described in "Known Gap" — record the
number in that section rather than acting on intuition.

- [ ] **Step 4: Answer the questions this exists for**

```sql
SELECT count(DISTINCT company_id) FROM corpscout.company_listings
WHERE is_current = 1 AND evidence_tier = 'regulator';

SELECT country_code, count(DISTINCT company_id) AS listed_companies
FROM corpscout.company_listings WHERE is_current = 1
GROUP BY country_code ORDER BY listed_companies DESC;
```

- [ ] **Step 5: Record the numbers in this plan and commit**

---

### Task 8: Retire `se_company_listings` (gated)

Do **not** start until Task 7 confirms the view returns Sweden rows matching the
legacy table. `DROP TABLE` is irreversible past ClickHouse's ~480s `UNDROP`
window.

- [ ] **Step 1: Verify parity**

```sql
SELECT count() AS legacy_rows FROM corpscout.se_company_listings;

SELECT count() AS view_rows FROM corpscout.company_listings
WHERE country_code = 'SE' AND evidence_tier = 'regulator';

SELECT count() AS missing
FROM corpscout.se_company_listings AS l
LEFT JOIN corpscout.company_listings AS v
    ON v.company_id = l.company_id AND v.isin = l.isin AND v.mic = l.mic
WHERE v.company_id = '';
```

Proceed only if `missing` is 0. If not, reconcile — do not drop.

- [ ] **Step 2: Write migration `000176_corpscout_drop_se_company_listings`**

Up: `DROP TABLE IF EXISTS corpscout.se_company_listings;` with a comment
recording the verified parity. Down: recreate the table exactly as migration
`000170` defined it.

- [ ] **Step 3: Delete the superseded module and tests**

Delete `defs/company_listings/` entirely (its asset, tables, and
`docs/sweden.md`), plus `tests/test_company_listings.py`. Remove the
`company_listings_tables` import and the
`test_se_company_listings_migration_covers_columns_in_order` test from
`tests/test_clickhouse_migrations.py`. Add
`"000176_corpscout_drop_se_company_listings",` to `EXPECTED_MIGRATIONS`.

- [ ] **Step 4: Write the replacement design doc**

Create `defs/instrument_venues/docs/listing-identity.md` describing the three
layers, the cardinality contract with Task 7's measured numbers, why layer A
reads FIRDS current state while layer B reads event history, the RA-code tiers,
and the known gap for non-LEI countries.

- [ ] **Step 5: Run the full check and commit**

Run: `uv run pytest tests/ -q --continue-on-collection-errors && uv run dg check defs`
Expected: no new failures relative to the pre-existing baseline
