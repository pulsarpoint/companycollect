# Unified Company Listings Implementation Plan

> **SUPERSEDED (2026-07-25)** by
> [`2026-07-25-normalized-listing-identity.md`](2026-07-25-normalized-listing-identity.md).
> This plan materialized one denormalized listings table partitioned by country.
> The replacement normalizes the same facts into three layers joined by their
> natural keys, which removes the country-partitioning machinery entirely and
> stores company identity once per LEI instead of once per instrument-venue row
> (~92 instruments per issuer). Kept for the rationale on partition-level
> replacement, should a materialized rollup ever be needed. Do not implement.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the per-country `se_company_listings` table with one unified,
country-partitioned `corpscout.company_listings` table fed by pluggable
per-country resolvers, so adding country N+1 costs a resolver rather than a
pipeline.

**Architecture:** One physical table `PARTITION BY country_code`. Each country
contributes a `CountryListingResolver` that knows how to build an INSERT for its
own country. A single shared publisher stages that country's rows, validates
them, and swaps them in with `ALTER TABLE ... REPLACE PARTITION`. Refresh
isolation — the property the per-country-table design was protecting — is
preserved by the partition boundary rather than by separate tables.

**Tech Stack:** ClickHouse (MergeTree, `REPLACE PARTITION`), Dagster assets,
golang-migrate migrations, pytest.

## Global Constraints

- The migration owns the schema. Python asserts tables exist
  (`assert_clickhouse_tables_exist`) and never issues DDL for the target table.
- `ORDER BY` columns must be non-nullable (`allow_nullable_key` is off).
- Non-nullable `String` / `LowCardinality(String)` columns must receive `''`,
  never `NULL` — the native driver calls `.encode()` per value and dies on `None`.
- Never `git add -A`. Commit by explicit path.
- Migration ledger is forward-only. Add each migration name to
  `EXPECTED_MIGRATIONS` in `tests/test_clickhouse_migrations.py`.
- SQL line comments must not contain `;` — the migration runner splits on it.
- No `from __future__ import annotations` in modules defining `@dg.asset`.
- Validate with `uv run dg check defs` before finishing each task.

## Out of Scope (separate plans)

- `company_identity_map` — the LEI/ISIN/symbol → company identity fabric.
- The Brazil resolver (`br_cvm_companies` → CNPJ).
- `company_market_value` — price × shares outstanding.
- `corpscout.isin_lei` — already built (migration `000171`); it is an identity
  input, not a listing source, and needs no change here.

## File Structure

| File | Responsibility |
|---|---|
| `clickhouse/migrations/000172_corpscout_company_listings.{up,down}.sql` | Owns the unified table DDL |
| `clickhouse/migrations/000173_corpscout_drop_se_company_listings.{up,down}.sql` | Retires the per-country table (Task 5, gated) |
| `defs/company_listings/tables.py` | Column contract for the unified table |
| `defs/company_listings/publisher.py` | `CountryListingResolver` + `publish_country_listings` — country-agnostic staging, validation, partition swap |
| `defs/company_listings/sweden.py` | Sweden resolver: the FIRDS/GLEIF/EODHD SQL, moved out of `assets.py` |
| `defs/company_listings/assets.py` | Dagster asset + job wiring only |
| `tests/test_company_listings_publisher.py` | Publisher behaviour with a fake client |
| `tests/test_company_listings_sweden.py` | Sweden resolver SQL contract |
| `tests/test_company_listings.py` | Existing file — retargeted at the unified contract |

---

### Task 1: Unified table migration and column contract

**Files:**
- Create: `corpscout/clickhouse/migrations/000172_corpscout_company_listings.up.sql`
- Create: `corpscout/clickhouse/migrations/000172_corpscout_company_listings.down.sql`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/tables.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`

**Interfaces:**
- Produces: `tables.COMPANY_LISTINGS_TABLE = "company_listings"`,
  `tables.COMPANY_LISTINGS_COLUMNS` (30-tuple, exact order below),
  `tables.QUALIFIED_COMPANY_LISTINGS_TABLE`.

- [ ] **Step 1: Write the failing contract test**

Add to `tests/test_company_listings.py`:

```python
def test_unified_company_listings_column_contract() -> None:
    assert tables.COMPANY_LISTINGS_TABLE == "company_listings"
    assert tables.COMPANY_LISTINGS_COLUMNS == (
        "country_code",
        "company_id",
        "mic",
        "instrument_ref",
        "instrument_ref_type",
        "isin",
        "ticker",
        "issuer_lei",
        "identity_match_method",
        "identity_match_confidence",
        "instrument_name",
        "instrument_type",
        "cfi_code",
        "cfi_category",
        "trading_currency",
        "trading_status",
        "is_current",
        "admission_date",
        "first_trade_date",
        "termination_date",
        "resolver",
        "evidence_tier",
        "listing_status_source",
        "status_conflict",
        "source_slug",
        "source_record_id",
        "source_publication_date",
        "source_retrieved_at",
        "source_run_id",
        "resolved_at",
    )
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `uv run pytest tests/test_company_listings.py::test_unified_company_listings_column_contract -v`
Expected: FAIL with `AttributeError: module ... has no attribute 'COMPANY_LISTINGS_TABLE'`

- [ ] **Step 3: Add the contract to `tables.py`**

Append to `defs/company_listings/tables.py` (keep the existing
`SE_COMPANY_LISTINGS_*` values — Task 5 removes them):

```python
COMPANY_LISTINGS_TABLE = "company_listings"
QUALIFIED_COMPANY_LISTINGS_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{COMPANY_LISTINGS_TABLE}"
)

COMPANY_LISTINGS_COLUMNS = (
    "country_code",
    "company_id",
    "mic",
    "instrument_ref",
    "instrument_ref_type",
    "isin",
    "ticker",
    "issuer_lei",
    "identity_match_method",
    "identity_match_confidence",
    "instrument_name",
    "instrument_type",
    "cfi_code",
    "cfi_category",
    "trading_currency",
    "trading_status",
    "is_current",
    "admission_date",
    "first_trade_date",
    "termination_date",
    "resolver",
    "evidence_tier",
    "listing_status_source",
    "status_conflict",
    "source_slug",
    "source_record_id",
    "source_publication_date",
    "source_retrieved_at",
    "source_run_id",
    "resolved_at",
)
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `uv run pytest tests/test_company_listings.py::test_unified_company_listings_column_contract -v`
Expected: PASS

- [ ] **Step 5: Write the failing migration test**

Add to `tests/test_clickhouse_migrations.py` (before
`test_company_procurement_signals_migration_covers_columns`):

```python
def test_company_listings_migration_covers_columns_in_order() -> None:
    sql = _migration_sql("000172_corpscout_company_listings.up.sql")
    down_sql = _migration_sql("000172_corpscout_company_listings.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.company_listings" in sql
    last_index = -1
    for column_name in company_listings_tables.COMPANY_LISTINGS_COLUMNS:
        index = sql.index(f"    {column_name} ")
        assert index > last_index
        last_index = index

    assert "ENGINE = MergeTree" in sql
    assert "PARTITION BY country_code" in sql
    assert "ORDER BY (country_code, company_id, mic, instrument_ref)" in sql
    assert "DROP TABLE IF EXISTS corpscout.company_listings" in down_sql
```

Also add `"000172_corpscout_company_listings",` to `EXPECTED_MIGRATIONS`
immediately after `"000171_corpscout_isin_lei",`.

- [ ] **Step 6: Run it and confirm it fails**

Run: `uv run pytest tests/test_clickhouse_migrations.py -k "company_listings_migration or migration_files" -v`
Expected: FAIL with `FileNotFoundError: ... 000172_corpscout_company_listings.up.sql`

- [ ] **Step 7: Write the migration pair**

`000172_corpscout_company_listings.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per company, venue, and instrument, across every country.
-- Partitioned by country_code so a single country refresh swaps only its own
-- partition and never rebuilds another country's listings.
--
-- instrument_ref is the ISIN where one is known, otherwise the ticker or a
-- vendor symbol, with instrument_ref_type naming which. This lets a resolver
-- for a market without ISIN coverage still emit a valid, non-null grain.
--
-- evidence_tier separates regulator-confirmed listings from vendor-only ones
-- so a cross-country count can require regulatory evidence.
CREATE TABLE IF NOT EXISTS corpscout.company_listings
(
    country_code                 LowCardinality(String),
    company_id                   String,
    mic                          LowCardinality(String),
    instrument_ref               String,
    instrument_ref_type          LowCardinality(String),
    isin                         String,
    ticker                       String,
    issuer_lei                   String,
    identity_match_method        LowCardinality(String),
    identity_match_confidence    LowCardinality(String),
    instrument_name              String,
    instrument_type              LowCardinality(String),
    cfi_code                     LowCardinality(String),
    cfi_category                 LowCardinality(String),
    trading_currency             LowCardinality(String),
    trading_status               LowCardinality(String),
    is_current                   UInt8,
    admission_date               Nullable(Date),
    first_trade_date             Nullable(Date),
    termination_date             Nullable(Date),
    resolver                     LowCardinality(String),
    evidence_tier                LowCardinality(String),
    listing_status_source        LowCardinality(String),
    status_conflict              UInt8,
    source_slug                  LowCardinality(String),
    source_record_id             String,
    source_publication_date      Date,
    source_retrieved_at          DateTime64(3, 'UTC'),
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id, mic, instrument_ref);
```

`000172_corpscout_company_listings.down.sql`:

```sql
DROP TABLE IF EXISTS corpscout.company_listings;
```

- [ ] **Step 8: Run the migration tests and confirm they pass**

Run: `uv run pytest tests/test_clickhouse_migrations.py -q`
Expected: PASS (no new failures beyond the pre-existing ones)

- [ ] **Step 9: Verify the partition key against real ClickHouse**

`PARTITION BY` on a `LowCardinality(String)` column is the one thing here that
cannot be verified by a string test. Apply the migration and confirm:

```sql
INSERT INTO corpscout.company_listings (country_code, company_id, mic, instrument_ref, instrument_ref_type, isin, ticker, issuer_lei, identity_match_method, identity_match_confidence, instrument_name, instrument_type, cfi_code, cfi_category, trading_currency, trading_status, is_current, resolver, evidence_tier, listing_status_source, status_conflict, source_slug, source_record_id, source_publication_date, source_retrieved_at, source_run_id, resolved_at)
VALUES ('SE','5560160680','XSTO','SE0000108656','isin','SE0000108656','TEST','','test','exact','T','Equity','ESVUFR','E','SEK','current',1,'test','regulator','test',0,'test','r1','2026-07-25','2026-07-25 00:00:00','run','2026-07-25 00:00:00');

SELECT partition, name FROM system.parts
WHERE database = 'corpscout' AND table = 'company_listings' AND active;

TRUNCATE TABLE corpscout.company_listings;
```

Expected: one part with `partition = 'SE'`. If ClickHouse rejects the
`LowCardinality` partition key, change the DDL to
`PARTITION BY toString(country_code)` and re-run this step.

- [ ] **Step 10: Commit**

```bash
git add corpscout/clickhouse/migrations/000172_corpscout_company_listings.up.sql \
        corpscout/clickhouse/migrations/000172_corpscout_company_listings.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/tables.py \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py \
        corpscout/services/dagster_v3/tests/test_company_listings.py
git commit -m "feat(corpscout): add unified country-partitioned company_listings table"
```

---

### Task 2: Country-agnostic publisher

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/publisher.py`
- Create: `corpscout/services/dagster_v3/tests/test_company_listings_publisher.py`

**Interfaces:**
- Consumes: `tables.COMPANY_LISTINGS_COLUMNS`,
  `tables.QUALIFIED_COMPANY_LISTINGS_TABLE` from Task 1.
- Produces:
  - `CountryListingResolver` — frozen dataclass with fields
    `country_code: str`, `resolver_name: str`,
    `build_insert_sql: Callable[[str], str]`,
    `required_tables: tuple[str, ...]`, `min_expected_rows: int`.
  - `publish_country_listings(*, clickhouse: ClickhouseResource, resolver: CountryListingResolver, source_run_id: str, resolved_at: datetime) -> dict[str, object]`
  - `QUALITY_COLUMNS: tuple[str, ...]`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_company_listings_publisher.py`:

```python
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime

import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.company_listings.publisher import (
    CountryListingResolver,
    publish_country_listings,
)

_RESOLVER = CountryListingResolver(
    country_code="SE",
    resolver_name="test_resolver",
    build_insert_sql=lambda stage: f"INSERT INTO {stage} (country_code) SELECT 'SE'",
    required_tables=("company_listings", "se_companies"),
    min_expected_rows=2,
)


class _FakeClickHouseClient:
    def __init__(self, quality_row: tuple[object, ...]) -> None:
        self.quality_row = quality_row
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params is not None else ()
            self.table_checks.append(requested)
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


def _quality(rows: int = 3, keys: int = 3, invalid: int = 0) -> tuple[object, ...]:
    return (rows, 2, keys, invalid, date(2026, 7, 25))


def test_publish_swaps_only_the_resolver_country_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClickHouseClient(_quality())
    resource = _resource(monkeypatch, client)

    metadata = publish_country_listings(
        clickhouse=resource,
        resolver=_RESOLVER,
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
    )

    replace = [s for s in client.statements if "REPLACE PARTITION" in s]
    assert len(replace) == 1
    assert "REPLACE PARTITION 'SE'" in replace[0]
    assert "corpscout`.`company_listings`" in replace[0]
    assert metadata["row_count"] == 3
    assert metadata["country_code"] == "SE"
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")


def test_publish_refuses_to_blank_a_partition_below_the_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClickHouseClient(_quality(rows=1, keys=1))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="below the expected floor"):
        publish_country_listings(
            clickhouse=resource,
            resolver=_RESOLVER,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any("REPLACE PARTITION" in s for s in client.statements)
    assert client.statements[-1].startswith("DROP TABLE IF EXISTS")


def test_publish_refuses_duplicate_grain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClickHouseClient(_quality(rows=3, keys=2))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="grain mismatch"):
        publish_country_listings(
            clickhouse=resource,
            resolver=_RESOLVER,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any("REPLACE PARTITION" in s for s in client.statements)


def test_publish_refuses_rows_from_another_country(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClickHouseClient(_quality(invalid=1))
    resource = _resource(monkeypatch, client)

    with pytest.raises(ValueError, match="invalid identity rows"):
        publish_country_listings(
            clickhouse=resource,
            resolver=_RESOLVER,
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        )

    assert not any("REPLACE PARTITION" in s for s in client.statements)
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_company_listings_publisher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.company_listings.publisher'`

- [ ] **Step 3: Implement the publisher**

Create `defs/company_listings/publisher.py`:

```python
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_listings import tables

QUALITY_COLUMNS = (
    "row_count",
    "company_count",
    "listing_key_count",
    "invalid_identity_rows",
    "latest_source_publication_date",
)


@dataclass(frozen=True)
class CountryListingResolver:
    """How one country produces rows for the shared listings contract.

    build_insert_sql receives the qualified stage table name and returns an
    INSERT statement bound with the %(source_run_id)s and %(resolved_at)s
    parameters. min_expected_rows is the floor below which a rebuild is
    refused, so a degraded upstream cannot quietly empty a country.
    """

    country_code: str
    resolver_name: str
    build_insert_sql: Callable[[str], str]
    required_tables: tuple[str, ...]
    min_expected_rows: int


def _qualified(table_name: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table_name}`"


def _quality_sql(stage_table: str, country_code: str) -> str:
    return f"""SELECT
    count() AS row_count,
    uniqExact(company_id) AS company_count,
    uniqExact((country_code, company_id, mic, instrument_ref))
        AS listing_key_count,
    countIf(
        country_code != '{country_code}'
        OR company_id = ''
        OR mic = ''
        OR instrument_ref = ''
        OR instrument_ref_type = ''
        OR resolver = ''
        OR evidence_tier = ''
    ) AS invalid_identity_rows,
    max(source_publication_date) AS latest_source_publication_date
FROM {stage_table}"""


def _validate(quality: dict[str, object], resolver: CountryListingResolver) -> None:
    row_count = int(quality["row_count"])
    listing_key_count = int(quality["listing_key_count"])
    invalid_identity_rows = int(quality["invalid_identity_rows"])

    if row_count < resolver.min_expected_rows:
        raise ValueError(
            f"{resolver.country_code} listings below the expected floor: "
            f"rows={row_count} floor={resolver.min_expected_rows}"
        )
    if listing_key_count != row_count:
        raise ValueError(
            f"{resolver.country_code} listing grain mismatch: "
            f"rows={row_count} unique_keys={listing_key_count}"
        )
    if invalid_identity_rows != 0:
        raise ValueError(
            f"{resolver.country_code} listings contain invalid identity rows: "
            f"{invalid_identity_rows}"
        )


def publish_country_listings(
    *,
    clickhouse: ClickhouseResource,
    resolver: CountryListingResolver,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, object]:
    """Stage, validate, and atomically swap one country's listing partition."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=resolver.required_tables,
    )
    stage_name = (
        f"_tmp_{tables.COMPANY_LISTINGS_TABLE}_"
        f"{resolver.country_code.lower()}_{uuid.uuid4().hex}"
    )
    qualified_stage = _qualified(stage_name)
    qualified_target = _qualified(tables.COMPANY_LISTINGS_TABLE)

    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
        primary_error: Exception | None = None
        try:
            client.execute(
                resolver.build_insert_sql(qualified_stage),
                {"source_run_id": source_run_id, "resolved_at": resolved_at},
            )
            row = client.execute(
                _quality_sql(qualified_stage, resolver.country_code)
            )[0]
            quality = {
                column: value
                for column, value in zip(QUALITY_COLUMNS, row, strict=True)
            }
            _validate(quality, resolver)
            client.execute(
                f"ALTER TABLE {qualified_target} "
                f"REPLACE PARTITION '{resolver.country_code}' "
                f"FROM {qualified_stage}"
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

    latest = quality["latest_source_publication_date"]
    return {
        **quality,
        "latest_source_publication_date": (
            latest.isoformat()
            if isinstance(latest, (date, datetime))
            else str(latest or "")
        ),
        "country_code": resolver.country_code,
        "resolver": resolver.resolver_name,
        "table": tables.QUALIFIED_COMPANY_LISTINGS_TABLE,
        "source_run_id": source_run_id,
    }
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `uv run pytest tests/test_company_listings_publisher.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/publisher.py \
        corpscout/services/dagster_v3/tests/test_company_listings_publisher.py
git commit -m "feat(corpscout): add country-partitioned listings publisher"
```

---

### Task 3: Sweden resolver on the unified contract

**Files:**
- Create: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/sweden.py`
- Create: `corpscout/services/dagster_v3/tests/test_company_listings_sweden.py`

**Interfaces:**
- Consumes: `CountryListingResolver` from Task 2,
  `tables.COMPANY_LISTINGS_COLUMNS` from Task 1.
- Produces: `SWEDEN_RESOLVER: CountryListingResolver`,
  `build_sweden_listings_insert_sql(stage_table: str) -> str`,
  `SWEDEN_MIN_EXPECTED_ROWS: int`.

This carries over the existing FIRDS → GLEIF → `se_companies` logic and fixes
two defects in it: `se_companies` is a `ReplacingMergeTree` joined without
deduplication (unmerged parts fan the join out and trip the grain check
intermittently), and there is no volume floor.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_company_listings_sweden.py`:

```python
from dagster_v3.defs.company_listings.sweden import (
    SWEDEN_RESOLVER,
    build_sweden_listings_insert_sql,
)

_STAGE = "`corpscout`.`_tmp_company_listings_se_test`"


def test_resolver_declares_country_and_floor() -> None:
    assert SWEDEN_RESOLVER.country_code == "SE"
    assert SWEDEN_RESOLVER.resolver_name == "firds_gleif"
    assert SWEDEN_RESOLVER.min_expected_rows >= 100
    assert "company_listings" in SWEDEN_RESOLVER.required_tables
    assert "firds_instruments_current" in SWEDEN_RESOLVER.required_tables


def test_sql_emits_the_unified_contract_columns() -> None:
    sql = build_sweden_listings_insert_sql(_STAGE)

    assert sql.startswith(f"INSERT INTO {_STAGE} (")
    assert "'SE' AS country_code" in sql
    assert "'firds_gleif' AS resolver" in sql
    assert "'regulator' AS evidence_tier" in sql
    assert "AS instrument_ref" in sql
    assert "'isin' AS instrument_ref_type" in sql


def test_sql_deduplicates_the_replacing_merge_tree_register_join() -> None:
    """se_companies is a ReplacingMergeTree; an undeduplicated join fans out."""
    sql = build_sweden_listings_insert_sql(_STAGE)

    assert "se_companies_current AS" in sql
    assert "GROUP BY company_id" in sql
    assert "INNER JOIN se_companies_current AS c" in sql
    assert "INNER JOIN corpscout.se_companies AS c" not in sql


def test_sql_keeps_the_exact_firds_gleif_identity_path() -> None:
    sql = build_sweden_listings_insert_sql(_STAGE)

    assert "FROM corpscout.firds_instruments_current AS f" in sql
    assert "INNER JOIN gleif_sweden AS g ON g.lei = f.issuer_lei" in sql
    assert "'firds_issuer_lei_gleif_registered_as'" in sql
    assert "legal_name" not in sql


def test_sql_carries_vendor_disagreement_without_vendor_columns() -> None:
    """EODHD cross-references belong in company_identity_map, not here."""
    sql = build_sweden_listings_insert_sql(_STAGE)

    assert "AS status_conflict" in sql
    assert "eodhd_symbol_key AS" not in sql
    assert "AS eodhd_is_delisted" not in sql
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_company_listings_sweden.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dagster_v3.defs.company_listings.sweden'`

- [ ] **Step 3: Implement the Sweden resolver**

Create `defs/company_listings/sweden.py`. Start from the existing
`build_se_company_listings_insert_sql` in `assets.py` and apply these changes:

1. Add a `se_companies_current` CTE that deduplicates the register:

```sql
se_companies_current AS
(
    SELECT company_id
    FROM corpscout.se_companies
    GROUP BY company_id
)
```

   and join `INNER JOIN se_companies_current AS c ON c.company_id = g.company_id`
   instead of joining `corpscout.se_companies` directly.

2. Replace the final SELECT list with the unified contract in
   `tables.COMPANY_LISTINGS_COLUMNS` order:

```sql
SELECT
    'SE' AS country_code,
    c.company_id AS company_id,
    f.mic AS mic,
    f.isin AS instrument_ref,
    'isin' AS instrument_ref_type,
    f.isin AS isin,
    e.ticker AS ticker,
    f.issuer_lei AS issuer_lei,
    'firds_issuer_lei_gleif_registered_as' AS identity_match_method,
    'exact' AS identity_match_confidence,
    if(f.short_name != '', f.short_name, f.full_name) AS instrument_name,
    if(e.instrument_type = '', 'Equity', e.instrument_type) AS instrument_type,
    f.cfi_code AS cfi_code,
    substring(f.cfi_code, 1, 1) AS cfi_category,
    f.notional_currency AS trading_currency,
    'current' AS trading_status,
    toUInt8(1) AS is_current,
    toDate(f.admission_approval_at) AS admission_date,
    toDate(f.first_trade_at) AS first_trade_date,
    toDate(f.termination_at) AS termination_date,
    'firds_gleif' AS resolver,
    'regulator' AS evidence_tier,
    'esma_firds_current' AS listing_status_source,
    toUInt8(e.eodhd_symbol_key != '' AND e.eodhd_is_delisted = 1)
        AS status_conflict,
    'esma_firds' AS source_slug,
    f.source_record_id AS source_record_id,
    f.source_publication_date AS source_publication_date,
    f.source_retrieved_at AS source_retrieved_at,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
```

   The `eodhd_by_listing` CTE and its `LEFT JOIN` stay — EODHD still supplies
   `ticker`, `instrument_type`, and the `status_conflict` signal. Only the two
   vendor identity columns are dropped from the output.

3. Declare the resolver:

```python
SWEDEN_MIN_EXPECTED_ROWS = 300

SWEDEN_RESOLVER = CountryListingResolver(
    country_code="SE",
    resolver_name="firds_gleif",
    build_insert_sql=build_sweden_listings_insert_sql,
    required_tables=(
        tables.COMPANY_LISTINGS_TABLE,
        "firds_instruments_current",
        "gleif_lei_records",
        "se_companies",
        "eodhd_symbols",
        "eodhd_symbol_mics",
    ),
    min_expected_rows=SWEDEN_MIN_EXPECTED_ROWS,
)
```

`SWEDEN_MIN_EXPECTED_ROWS = 300` is a deliberately conservative floor: Sweden's
listed universe is well above it, so the gate catches collapse without tripping
on normal variation. Tune it after the first live materialization reports the
real `row_count`.

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `uv run pytest tests/test_company_listings_sweden.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/sweden.py \
        corpscout/services/dagster_v3/tests/test_company_listings_sweden.py
git commit -m "feat(corpscout): add Sweden listings resolver on the unified contract"
```

---

### Task 4: Rewire the asset onto the publisher

**Files:**
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/assets.py`
- Modify: `corpscout/services/dagster_v3/tests/test_company_listings.py`

**Interfaces:**
- Consumes: `SWEDEN_RESOLVER` (Task 3), `publish_country_listings` (Task 2).
- Produces: asset `se_company_listings_clickhouse` (name retained so existing
  schedules and downstream selections keep resolving), `se_company_listings_job`.

- [ ] **Step 1: Rewrite the test file's asset expectations**

Replace the body of `tests/test_company_listings.py` with the Task 1 contract
test plus:

```python
import dagster as dg

from dagster_v3.defs.company_listings.assets import (
    SE_LISTING_UPSTREAM_ASSET_KEYS,
    se_company_listings_clickhouse,
)


def test_asset_dependencies_are_country_scoped() -> None:
    spec = se_company_listings_clickhouse.specs_by_key[
        se_company_listings_clickhouse.key
    ]

    assert {dep.asset_key for dep in spec.deps} == {
        dg.AssetKey(asset_key) for asset_key in SE_LISTING_UPSTREAM_ASSET_KEYS
    }
    assert SE_LISTING_UPSTREAM_ASSET_KEYS == (
        "esma_firds_clickhouse",
        "gleif_reference_clickhouse",
        "eodhd_reference_complete",
        "sweden_company_companies_clickhouse",
    )


def test_asset_targets_the_unified_table() -> None:
    spec = se_company_listings_clickhouse.specs_by_key[
        se_company_listings_clickhouse.key
    ]

    # Dagster wraps raw metadata strings in TextMetadataValue on the spec.
    assert str(spec.metadata["table"]).endswith("corpscout.company_listings")
```

Delete the old `test_se_company_listing_*` tests and the `_FakeClickHouseClient`
block — that behaviour now lives in `test_company_listings_publisher.py` and
`test_company_listings_sweden.py`.

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/test_company_listings.py -v`
Expected: FAIL on `test_asset_targets_the_unified_table` — metadata still reads
`corpscout.se_company_listings`

- [ ] **Step 3: Rewrite `assets.py`**

Replace the whole file with wiring only:

```python
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.company_listings import tables
from dagster_v3.defs.company_listings.publisher import publish_country_listings
from dagster_v3.defs.company_listings.sweden import SWEDEN_RESOLVER

SE_LISTING_UPSTREAM_ASSET_KEYS = (
    "esma_firds_clickhouse",
    "gleif_reference_clickhouse",
    "eodhd_reference_complete",
    "sweden_company_companies_clickhouse",
)


@dg.asset(
    name="se_company_listings_clickhouse",
    deps=[dg.AssetKey(key) for key in SE_LISTING_UPSTREAM_ASSET_KEYS],
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    pool="company_listings_clickhouse",
    metadata={"table": tables.QUALIFIED_COMPANY_LISTINGS_TABLE},
    description=(
        "Rebuilds the Swedish partition of the unified company listings table "
        "from FIRDS issuer LEIs, exact GLEIF registered_as organisation "
        "numbers, and optional exact-ISIN/MIC EODHD ticker evidence."
    ),
)
def se_company_listings_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = publish_country_listings(
        clickhouse=clickhouse,
        resolver=SWEDEN_RESOLVER,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
    )
    context.log.info(
        "Rebuilt %s listings partition: rows=%s companies=%s",
        metadata["country_code"],
        metadata["row_count"],
        metadata["company_count"],
    )
    return dg.MaterializeResult(metadata=metadata)


se_company_listings_job = dg.define_asset_job(
    "se_company_listings_job",
    selection=dg.AssetSelection.assets("se_company_listings_clickhouse"),
)

defs = dg.Definitions(
    assets=[se_company_listings_clickhouse],
    jobs=[se_company_listings_job],
)
```

- [ ] **Step 4: Run tests and definition checks**

Run: `uv run pytest tests/test_company_listings.py tests/test_company_listings_sweden.py tests/test_company_listings_publisher.py -v && uv run dg check defs`
Expected: all pass, `All definitions loaded successfully.`

- [ ] **Step 5: Commit**

```bash
git add corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/assets.py \
        corpscout/services/dagster_v3/tests/test_company_listings.py
git commit -m "refactor(corpscout): publish Sweden listings into the unified table"
```

---

### Task 5: Retire `se_company_listings` (gated)

Do **not** start this task until Task 4 has been materialized against real
ClickHouse and `corpscout.company_listings` partition `SE` is populated.
Dropping a table is irreversible past ClickHouse's ~480s `UNDROP` window.

**Files:**
- Create: `corpscout/clickhouse/migrations/000173_corpscout_drop_se_company_listings.up.sql`
- Create: `corpscout/clickhouse/migrations/000173_corpscout_drop_se_company_listings.down.sql`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/tables.py`
- Modify: `corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py`
- Modify: `corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/docs/sweden.md`

- [ ] **Step 1: Verify the precondition and record the numbers**

```sql
SELECT count() AS unified_rows
FROM corpscout.company_listings WHERE country_code = 'SE';

SELECT count() AS legacy_rows FROM corpscout.se_company_listings;

SELECT countIf(u.company_id = '') AS missing_in_unified
FROM corpscout.se_company_listings AS l
LEFT JOIN (
    SELECT company_id, isin, mic FROM corpscout.company_listings
    WHERE country_code = 'SE'
) AS u ON u.company_id = l.company_id AND u.isin = l.isin AND u.mic = l.mic;
```

Proceed only if `unified_rows` is within a few percent of `legacy_rows` and
`missing_in_unified` is 0. If not, stop and reconcile — do not drop.

- [ ] **Step 2: Write the migration pair**

`000173_corpscout_drop_se_company_listings.up.sql`:

```sql
-- Superseded by corpscout.company_listings partition 'SE' (migration 000172).
-- Precondition verified before this migration was committed: the unified
-- partition carries every legacy row.
DROP TABLE IF EXISTS corpscout.se_company_listings;
```

`000173_corpscout_drop_se_company_listings.down.sql`:

```sql
CREATE TABLE IF NOT EXISTS corpscout.se_company_listings
(
    country_code                 LowCardinality(String),
    company_id                   String,
    issuer_lei                   String,
    isin                         String,
    mic                          LowCardinality(String),
    ticker                       String,
    instrument_name              String,
    instrument_type              LowCardinality(String),
    cfi_code                     LowCardinality(String),
    notional_currency            LowCardinality(String),
    admission_date               Nullable(Date),
    first_trade_date             Nullable(Date),
    termination_date             Nullable(Date),
    trading_status               LowCardinality(String),
    is_current                   UInt8,
    identity_match_method        LowCardinality(String),
    identity_match_confidence    LowCardinality(String),
    listing_status_source        LowCardinality(String),
    status_conflict              UInt8,
    eodhd_symbol_key             String,
    eodhd_is_delisted            Nullable(UInt8),
    source_slug                  LowCardinality(String),
    source_record_id             String,
    source_publication_date      Date,
    source_retrieved_at          DateTime64(3, 'UTC'),
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, isin, mic);
```

- [ ] **Step 3: Remove the legacy contract**

Delete `SE_COMPANY_LISTINGS_TABLE`, `QUALIFIED_SE_COMPANY_LISTINGS_TABLE`, and
`SE_COMPANY_LISTINGS_COLUMNS` from `tables.py`. Delete
`test_se_company_listings_migration_covers_columns_in_order` and
`test_se_company_listing_table_contract`. Add
`"000173_corpscout_drop_se_company_listings",` to `EXPECTED_MIGRATIONS`.

- [ ] **Step 4: Update the design doc**

Rewrite `docs/sweden.md` to describe the Sweden *resolver* rather than a Sweden
table. It must state: the unified table and its partition boundary; the
`jurisdiction = 'SE'` filter on the GLEIF side (currently undocumented); the
EODHD `argMax` tie-break (non-delisted, then primary MIC, then latest
`retrieved_at`); why `is_current` and `trading_status` are constants for this
resolver; the `SWEDEN_MIN_EXPECTED_ROWS` floor; and that the job has no
schedule and is manual-only.

- [ ] **Step 5: Run the full check**

Run: `uv run pytest tests/test_clickhouse_migrations.py tests/test_company_listings.py -q && uv run dg check defs`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add corpscout/clickhouse/migrations/000173_corpscout_drop_se_company_listings.up.sql \
        corpscout/clickhouse/migrations/000173_corpscout_drop_se_company_listings.down.sql \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/tables.py \
        corpscout/services/dagster_v3/src/dagster_v3/defs/company_listings/docs/sweden.md \
        corpscout/services/dagster_v3/tests/test_clickhouse_migrations.py
git commit -m "refactor(corpscout): retire per-country se_company_listings table"
```

---

## Verification

After Task 4, against real ClickHouse:

```sql
-- partition isolation
SELECT partition, sum(rows) FROM system.parts
WHERE database='corpscout' AND table='company_listings' AND active
GROUP BY partition;

-- the questions this exists to answer, in one query each
SELECT count(DISTINCT company_id) FROM corpscout.company_listings
WHERE is_current = 1 AND evidence_tier = 'regulator';

SELECT country_code, count(DISTINCT company_id) AS listed_companies
FROM corpscout.company_listings WHERE is_current = 1
GROUP BY country_code ORDER BY listed_companies DESC;
```

Re-run the Sweden asset twice and confirm the `SE` partition is replaced, not
duplicated, and that no other partition's part count changes.
