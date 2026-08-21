"""Tests for esef_filings ClickHouse exports (Task 5): filings-index and
facts export via the shared DuckDB->ClickHouse exporter, plus the
ClickHouse-native gleif entity-registry map.

No live ClickHouse anywhere -- a FakeClickHouseClient/FakeClickHouseResource
pair records every SQL statement (mirroring
tests/test_brazil_fin_cvm_clickhouse_export.py and
tests/test_sweden_financial_metrics.py's stub patterns). The amount_original
cast, the filings-index NULL-date sentinel, and the facts malformed-date
sentinel are the tests that run a real DuckDB connection through the actual
shared exporter (not monkeypatched) end-to-end, since the whole point is to
prove what a real `try_cast`/`coalesce` produces -- a real Decimal, the
sentinel epoch, or None -- not to check that some function was called.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from typing import Any

import dagster as dg
import duckdb
import pytest

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.document_publish import (
    esef_document_concept_translation_coverage,
    esef_document_concept_translation_load,
)
from dagster_v3.defs.esef_filings.publish import (
    ESEF_FACTS_COLUMN_EXPRESSIONS,
    ESEF_FILINGS_COLUMN_EXPRESSIONS,
    GLEIF_LEI_RECORDS_QUALIFIED_TABLE,
    MATCH_SOURCE_GLEIF_REGISTERED_AS,
    build_esef_entity_registry_map_select,
    export_esef_facts_clickhouse,
    export_esef_filings_clickhouse,
    replace_esef_entity_registry_map_clickhouse,
)
from dagster_v3.defs.gleif.tables import GLEIF_LEI_RECORDS_TABLE
from dagster_v3.defs.translator_load import resource as translator_resource
from dagster_v3.defs.translator_load.resource import TranslatorResource


class FakeClickHouseClient:
    """Records every statement; answers just enough to drive the code paths
    under test (system.tables existence check, row-batch INSERT, the
    ClickHouse-native INSERT...SELECT path, and the pre/post row counts)."""

    def __init__(
        self,
        *,
        would_be_row_count: int = 1,
        post_exchange_row_count: int = 1,
    ) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.inserted_rows: list[tuple[Any, ...]] = []
        self.would_be_row_count = would_be_row_count
        self.post_exchange_row_count = post_exchange_row_count

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.statements.append(sql)
        stripped = sql.strip()
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if isinstance(params, dict) else ()
            self.table_checks.append(requested)
            return [(table,) for table in requested]
        if stripped.startswith("CREATE TABLE"):
            return []
        if stripped.startswith("SELECT count() FROM ("):
            return [(self.would_be_row_count,)]
        if stripped.startswith("INSERT INTO") and "SELECT" in stripped:
            # ClickHouse-native INSERT...SELECT (replace_table_from_select) --
            # no params/rows, unlike the row-batch VALUES path below.
            return []
        if stripped.startswith("INSERT INTO"):
            self.inserted_rows.extend(params or [])
            return []
        if stripped.startswith("EXCHANGE TABLES"):
            return []
        if stripped.startswith("SELECT count() FROM"):
            return [(self.post_exchange_row_count,)]
        if stripped.startswith("DROP TABLE"):
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[FakeClickHouseClient]:
        yield self._client


class _FakeDuckDBConnection:
    """Answers only `select count(*) from <schema>.<table>` -- the shrink
    guard's "would-be staged row count" read (Finding M1) -- with a fixed
    row count. Used where the real shared exporter is monkeypatched away
    (so a real DuckDB connection/table is unnecessary busywork); the real
    round-trip tests below use an actual `duckdb.connect(":memory:")`
    connection instead."""

    def __init__(self, row_count: int) -> None:
        self._row_count = row_count

    def execute(self, sql: str) -> "_FakeDuckDBConnection":
        assert sql.strip().lower().startswith("select count(*) from")
        return self

    def fetchall(self) -> list[tuple[int]]:
        return [(self._row_count,)]


# --- esef_filings_clickhouse -----------------------------------------------


def test_export_esef_filings_clickhouse_uses_export_columns_and_casts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.esef_filings import publish

    asserted: list[tuple[str, tuple[str, ...]]] = []
    exports: list[dict[str, object]] = []

    def fake_assert(
        clickhouse: object, *, database: str, tables: tuple[str, ...]
    ) -> None:
        asserted.append((database, tables))

    def fake_export(**kwargs: object) -> int:
        exports.append(kwargs)
        return 42

    monkeypatch.setattr(publish, "assert_clickhouse_tables_exist", fake_assert)
    monkeypatch.setattr(
        publish, "export_duckdb_connection_table_to_clickhouse", fake_export
    )

    rows = publish.export_esef_filings_clickhouse(
        duckdb_connection=_FakeDuckDBConnection(42),
        clickhouse=FakeClickHouseResource(FakeClickHouseClient()),
    )

    assert rows == 42
    assert asserted == [(tables.ESEF_DATABASE, (tables.ESEF_FILINGS_TABLE,))]
    assert len(exports) == 1
    export = exports[0]
    assert export["duckdb_schema"] == tables.DLT_DATASET_NAME
    assert export["duckdb_table"] == tables.FILINGS_INDEX_TABLE
    assert export["clickhouse_database"] == tables.ESEF_DATABASE
    assert export["clickhouse_table"] == tables.ESEF_FILINGS_TABLE
    assert export["columns"] == tables.ESEF_FILINGS_EXPORT_COLUMNS
    assert export["truncate"] is True
    assert export["column_expressions"] == ESEF_FILINGS_COLUMN_EXPRESSIONS


def test_esef_filings_column_expressions_cast_dates_and_coalesce_nullable_strings() -> (
    None
):
    # period_end/date_added are non-nullable Date32 in migration 000149 (period_end
    # is in the ORDER BY, so it can never become Nullable) -- a missing/malformed
    # source date must sentinel to the epoch, never reach clickhouse_driver as None.
    assert ESEF_FILINGS_COLUMN_EXPRESSIONS["period_end"] == (
        "coalesce(try_cast(period_end as date), DATE '1970-01-01')"
    )
    assert ESEF_FILINGS_COLUMN_EXPRESSIONS["date_added"] == (
        "coalesce(try_cast(date_added as date), DATE '1970-01-01')"
    )
    # processed_at is genuinely Nullable(DateTime64(6)) in the migration -- left
    # as a plain try_cast (NULL is semantically meaningful here, not sentineled).
    assert (
        ESEF_FILINGS_COLUMN_EXPRESSIONS["processed_at"]
        == "try_cast(processed_at as timestamp)"
    )
    for nullable_string_column in (
        "json_url",
        "package_url",
        "report_url",
        "viewer_url",
        "package_sha256",
    ):
        assert (
            ESEF_FILINGS_COLUMN_EXPRESSIONS[nullable_string_column]
            == f"coalesce({nullable_string_column}, '')"
        )
    # Every override key must be a real export column (no drift/typos).
    assert set(ESEF_FILINGS_COLUMN_EXPRESSIONS) <= set(
        tables.ESEF_FILINGS_EXPORT_COLUMNS
    )


# Mirrors assets.py's _FILINGS_INDEX_COLUMN_TYPES (DuckDB staging types for
# esef_filings.filings_index) -- kept local to this test module so the
# real-DuckDB round-trip test below doesn't need to import assets.py.
_FILINGS_INDEX_STAGING_COLUMN_TYPES: dict[str, str] = {
    "lei": "varchar",
    "entity_name": "varchar",
    "fxo_id": "varchar",
    "country": "varchar",
    "period_end": "varchar",
    "date_added": "varchar",
    "processed_at": "varchar",
    "json_url": "varchar",
    "package_url": "varchar",
    "report_url": "varchar",
    "viewer_url": "varchar",
    "package_sha256": "varchar",
    "error_count": "integer",
    "warning_count": "integer",
    "inconsistency_count": "integer",
    "has_json_facts": "boolean",
    "source_url": "varchar",
    "source_run_id": "varchar",
}
assert tuple(_FILINGS_INDEX_STAGING_COLUMN_TYPES) == tables.ESEF_FILINGS_EXPORT_COLUMNS


def test_export_esef_filings_clickhouse_sentinels_null_period_end_and_date_added() -> (
    None
):
    """Regression test: migration 000149 declares esef_filings.period_end and
    .date_added as non-nullable Date32 (period_end is in the ORDER BY, so it
    can never become Nullable). A crawl row with a missing period_end/date_added
    must export the sentinel epoch DATE '1970-01-01' for both -- and must NOT
    export None, which crashes clickhouse_driver's Date32 writer
    (`AttributeError: 'NoneType' object has no attribute 'year'`). Runs the
    real shared exporter (not monkeypatched) end-to-end against a real DuckDB
    connection, same rationale as the facts round-trip test below.
    """
    con = duckdb.connect(":memory:")
    con.execute(f"create schema {tables.DLT_DATASET_NAME}")
    columns_sql = ", ".join(
        f"{name} {kind}" for name, kind in _FILINGS_INDEX_STAGING_COLUMN_TYPES.items()
    )
    con.execute(f"create table {tables.QUALIFIED_FILINGS_INDEX_TABLE} ({columns_sql})")
    placeholders = ", ".join(["?"] * len(_FILINGS_INDEX_STAGING_COLUMN_TYPES))
    con.executemany(
        f"insert into {tables.QUALIFIED_FILINGS_INDEX_TABLE} values ({placeholders})",
        [
            (
                "549300CSLHPO6Y1AZN37",
                "Example Oyj",
                "fx-1",
                "FI",
                None,  # period_end missing from the source crawl
                None,  # date_added missing from the source crawl
                None,  # processed_at -- genuinely Nullable(DateTime64), left None
                None,
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                False,
                "https://filings.xbrl.org/",
                "run-1",
            ),
        ],
    )

    client = FakeClickHouseClient()
    rows = export_esef_filings_clickhouse(
        duckdb_connection=con,
        clickhouse=FakeClickHouseResource(client),
    )

    assert rows == 1
    row = client.inserted_rows[0]
    period_end_index = tables.ESEF_FILINGS_EXPORT_COLUMNS.index("period_end")
    date_added_index = tables.ESEF_FILINGS_EXPORT_COLUMNS.index("date_added")
    processed_at_index = tables.ESEF_FILINGS_EXPORT_COLUMNS.index("processed_at")
    assert row[period_end_index] == date(1970, 1, 1)
    assert row[date_added_index] == date(1970, 1, 1)
    # Not sentineled: processed_at is genuinely Nullable in the migration.
    assert row[processed_at_index] is None


def _seed_filings_index_duckdb(row_count: int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(f"create schema {tables.DLT_DATASET_NAME}")
    columns_sql = ", ".join(
        f"{name} {kind}" for name, kind in _FILINGS_INDEX_STAGING_COLUMN_TYPES.items()
    )
    con.execute(f"create table {tables.QUALIFIED_FILINGS_INDEX_TABLE} ({columns_sql})")
    placeholders = ", ".join(["?"] * len(_FILINGS_INDEX_STAGING_COLUMN_TYPES))
    con.executemany(
        f"insert into {tables.QUALIFIED_FILINGS_INDEX_TABLE} values ({placeholders})",
        [
            (
                f"lei-{i}",
                f"Example {i}",
                f"fx-{i}",
                "FI",
                "2022-12-31",
                "2023-01-01",
                None,
                None,
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                False,
                "https://filings.xbrl.org/",
                "run-1",
            )
            for i in range(row_count)
        ],
    )
    return con


# --- esef_filings_clickhouse shrink guard (Finding M1) ----------------------


def test_export_esef_filings_clickhouse_refuses_on_shrink_below_ratio() -> None:
    # DuckDB source (2 rows) is far below 50% of the existing ClickHouse
    # target (100 rows) -- the guard must refuse BEFORE the shared exporter
    # (and therefore the EXCHANGE) ever runs.
    con = _seed_filings_index_duckdb(2)
    client = FakeClickHouseClient(post_exchange_row_count=100)

    with pytest.raises(ValueError, match="Refusing to replace"):
        export_esef_filings_clickhouse(
            duckdb_connection=con,
            clickhouse=FakeClickHouseResource(client),
        )

    assert not any(s.startswith("CREATE TABLE") for s in client.statements)
    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_export_esef_filings_clickhouse_allow_shrink_overrides_guard() -> None:
    con = _seed_filings_index_duckdb(2)
    client = FakeClickHouseClient(post_exchange_row_count=100)

    rows = export_esef_filings_clickhouse(
        duckdb_connection=con,
        clickhouse=FakeClickHouseResource(client),
        allow_shrink=True,
    )

    assert rows == 2
    assert any(s.startswith("CREATE TABLE") for s in client.statements)
    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_export_esef_filings_clickhouse_no_existing_rows_never_refuses() -> None:
    # A brand-new (empty) target has existing_row_count == 0 --
    # guard_against_clickhouse_table_shrink is a no-op in that case (see its
    # docstring), so a normal first export must proceed without allow_shrink.
    con = _seed_filings_index_duckdb(2)
    client = FakeClickHouseClient(post_exchange_row_count=0)

    rows = export_esef_filings_clickhouse(
        duckdb_connection=con,
        clickhouse=FakeClickHouseResource(client),
    )

    assert rows == 2
    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)


# --- esef_facts_clickhouse --------------------------------------------------


def test_export_esef_facts_clickhouse_uses_export_columns_and_casts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.esef_filings import publish

    asserted: list[tuple[str, tuple[str, ...]]] = []
    exports: list[dict[str, object]] = []

    def fake_assert(
        clickhouse: object, *, database: str, tables: tuple[str, ...]
    ) -> None:
        asserted.append((database, tables))

    def fake_export(**kwargs: object) -> int:
        exports.append(kwargs)
        return 7

    monkeypatch.setattr(publish, "assert_clickhouse_tables_exist", fake_assert)
    monkeypatch.setattr(
        publish, "export_duckdb_connection_table_to_clickhouse", fake_export
    )

    rows = publish.export_esef_facts_clickhouse(
        duckdb_connection=_FakeDuckDBConnection(7),
        clickhouse=FakeClickHouseResource(FakeClickHouseClient()),
    )

    assert rows == 7
    assert asserted == [(tables.ESEF_DATABASE, (tables.ESEF_FACTS_TABLE,))]
    export = exports[0]
    assert export["duckdb_schema"] == tables.DLT_DATASET_NAME
    assert export["duckdb_table"] == tables.FACTS_TABLE
    assert export["clickhouse_table"] == tables.ESEF_FACTS_TABLE
    # period_end_year (local-only staging column) must never be exported.
    assert "period_end_year" not in export["columns"]
    assert export["columns"] == tables.ESEF_FACTS_EXPORT_COLUMNS
    assert export["column_expressions"] == ESEF_FACTS_COLUMN_EXPRESSIONS


def test_esef_facts_column_expressions_cast_amount_original_and_period_dates() -> None:
    assert (
        ESEF_FACTS_COLUMN_EXPRESSIONS["amount_original"]
        == "try_cast(amount_original as decimal(38,2))"
    )
    # period_end is non-nullable Date32 in migration 000149 -- sentinel a
    # missing/malformed source date to the epoch rather than letting None
    # reach clickhouse_driver's Date32 writer.
    assert ESEF_FACTS_COLUMN_EXPRESSIONS["period_end"] == (
        "coalesce(try_cast(period_end as date), DATE '1970-01-01')"
    )
    # period_start/period_instant are genuinely Nullable(Date32) in the
    # migration -- left as plain try_cast, NULL is semantically meaningful.
    assert (
        ESEF_FACTS_COLUMN_EXPRESSIONS["period_start"]
        == "try_cast(period_start as date)"
    )
    assert (
        ESEF_FACTS_COLUMN_EXPRESSIONS["period_instant"]
        == "try_cast(period_instant as date)"
    )
    # period_duration_end (Finding 1 fix) is also genuinely Nullable(Date32)
    # in the migration -- a duration fact's own true end date, distinct from
    # the filing-level period_end. Plain try_cast, NO sentinel: an
    # instant/text fact (or a malformed value) simply has no value here, and
    # NULL is meaningful, not an error to sentinel away.
    assert (
        ESEF_FACTS_COLUMN_EXPRESSIONS["period_duration_end"]
        == "try_cast(period_duration_end as date)"
    )
    assert set(ESEF_FACTS_COLUMN_EXPRESSIONS) <= set(tables.ESEF_FACTS_EXPORT_COLUMNS)


_FACTS_STAGING_COLUMN_TYPES: dict[str, str] = {
    "lei": "varchar",
    "fxo_id": "varchar",
    "period_end": "varchar",
    "fact_id": "varchar",
    "concept_qname": "varchar",
    "concept_namespace": "varchar",
    "concept_local_name": "varchar",
    "period_start": "varchar",
    "period_instant": "varchar",
    "period_duration_end": "varchar",
    "unit": "varchar",
    "currency": "varchar",
    "value_kind": "varchar",
    "raw_value": "varchar",
    "amount_original": "varchar",
    "decimals": "integer",
    "dimensions": "varchar",
    "language": "varchar",
    "source_run_id": "varchar",
}
assert tuple(_FACTS_STAGING_COLUMN_TYPES) == tables.ESEF_FACTS_EXPORT_COLUMNS


def test_export_esef_facts_clickhouse_round_trips_large_decimal_and_null() -> None:
    """Regression test for the Task 4 decision: amount_original is TEXT in
    DuckDB staging. Proves the real shared exporter (not monkeypatched)
    delivers a proper Decimal to ClickHouse for a large monetary value, and
    None (not the string "None" or an error) for a fact with no amount.

    Also covers the period_end sentinel: migration 000149's esef_facts.period_end
    is non-nullable Date32, so a fact whose period_end is a malformed string
    (year-prefix-valid but not a real date, e.g. "2022-99-99" -- a plain
    try_cast alone would NULL it) must export the sentinel epoch
    DATE '1970-01-01', not None.
    """
    con = duckdb.connect(":memory:")
    con.execute(f"create schema {tables.DLT_DATASET_NAME}")
    columns_sql = ", ".join(
        f"{name} {kind}" for name, kind in _FACTS_STAGING_COLUMN_TYPES.items()
    )
    con.execute(f"create table {tables.QUALIFIED_FACTS_TABLE} ({columns_sql})")
    placeholders = ", ".join(["?"] * len(_FACTS_STAGING_COLUMN_TYPES))
    con.executemany(
        f"insert into {tables.QUALIFIED_FACTS_TABLE} values ({placeholders})",
        [
            (
                "549300CSLHPO6Y1AZN37",
                "fx-1",
                "2022-12-31",
                "f-revenue",
                "ns:Revenue",
                "ns",
                "Revenue",
                "2022-01-01",
                None,
                "2022-12-31",  # period_duration_end: this duration fact's true end
                "iso4217:EUR",
                "EUR",
                "monetary",
                "438395039.49",
                "438395039.49",
                -2,
                "",
                "en",
                "run-1",
            ),
            (
                "549300CSLHPO6Y1AZN37",
                "fx-1",
                "2022-12-31",
                "f-narrative",
                "ns:Narrative",
                "ns",
                "Narrative",
                None,
                "2022-12-31",
                None,  # period_duration_end: instant facts never have one
                "",
                "",
                "text",
                "",
                None,
                None,
                "",
                "en",
                "run-1",
            ),
            (
                "549300CSLHPO6Y1AZN37",
                "fx-1",
                "2022-99-99",  # malformed: year-prefix-valid, not a real date
                "f-malformed-period-end",
                "ns:Malformed",
                "ns",
                "Malformed",
                None,
                None,
                "2022-99-99",  # malformed period_duration_end -- must NULL, no sentinel
                "iso4217:EUR",
                "EUR",
                "monetary",
                "",
                None,
                None,
                "",
                "en",
                "run-1",
            ),
        ],
    )

    # Confirm the premise directly: DuckDB's try_cast alone NULLs the
    # malformed string rather than raising or coercing it to some other date.
    [(bare_try_cast,)] = con.execute("select try_cast('2022-99-99' as date)").fetchall()
    assert bare_try_cast is None

    client = FakeClickHouseClient()
    rows = export_esef_facts_clickhouse(
        duckdb_connection=con,
        clickhouse=FakeClickHouseResource(client),
    )

    assert rows == 3
    by_fact_id = {
        row[tables.ESEF_FACTS_EXPORT_COLUMNS.index("fact_id")]: row
        for row in client.inserted_rows
    }
    amount_index = tables.ESEF_FACTS_EXPORT_COLUMNS.index("amount_original")
    revenue_amount = by_fact_id["f-revenue"][amount_index]
    assert revenue_amount == Decimal("438395039.49")
    assert isinstance(revenue_amount, Decimal)
    assert by_fact_id["f-narrative"][amount_index] is None

    period_end_index = tables.ESEF_FACTS_EXPORT_COLUMNS.index("period_end")
    assert by_fact_id["f-malformed-period-end"][period_end_index] == date(1970, 1, 1)
    # Well-formed period_end values must pass through unaffected by the sentinel.
    assert by_fact_id["f-revenue"][period_end_index] == date(2022, 12, 31)

    # period_duration_end (Finding 1 fix): well-formed value passes through as
    # a real date; a malformed value NULLs (genuinely Nullable, no sentinel --
    # contrast with period_end's sentinel above for the very same row).
    period_duration_end_index = tables.ESEF_FACTS_EXPORT_COLUMNS.index(
        "period_duration_end"
    )
    assert by_fact_id["f-revenue"][period_duration_end_index] == date(2022, 12, 31)
    assert by_fact_id["f-narrative"][period_duration_end_index] is None
    assert by_fact_id["f-malformed-period-end"][period_duration_end_index] is None


def _seed_facts_duckdb(row_count: int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(f"create schema {tables.DLT_DATASET_NAME}")
    columns_sql = ", ".join(
        f"{name} {kind}" for name, kind in _FACTS_STAGING_COLUMN_TYPES.items()
    )
    con.execute(f"create table {tables.QUALIFIED_FACTS_TABLE} ({columns_sql})")
    placeholders = ", ".join(["?"] * len(_FACTS_STAGING_COLUMN_TYPES))
    con.executemany(
        f"insert into {tables.QUALIFIED_FACTS_TABLE} values ({placeholders})",
        [
            (
                "549300CSLHPO6Y1AZN37",
                f"fx-{i}",
                "2022-12-31",
                f"f-{i}",
                "ns:Revenue",
                "ns",
                "Revenue",
                "2022-01-01",
                None,
                "2022-12-31",
                "iso4217:EUR",
                "EUR",
                "monetary",
                "100.00",
                "100.00",
                2,
                "",
                "en",
                "run-1",
            )
            for i in range(row_count)
        ],
    )
    return con


# --- esef_facts_clickhouse shrink guard (Finding M1) ------------------------


def test_export_esef_facts_clickhouse_refuses_on_shrink_below_ratio() -> None:
    con = _seed_facts_duckdb(2)
    client = FakeClickHouseClient(post_exchange_row_count=100)

    with pytest.raises(ValueError, match="Refusing to replace"):
        export_esef_facts_clickhouse(
            duckdb_connection=con,
            clickhouse=FakeClickHouseResource(client),
        )

    assert not any(s.startswith("CREATE TABLE") for s in client.statements)
    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_export_esef_facts_clickhouse_allow_shrink_overrides_guard() -> None:
    con = _seed_facts_duckdb(2)
    client = FakeClickHouseClient(post_exchange_row_count=100)

    rows = export_esef_facts_clickhouse(
        duckdb_connection=con,
        clickhouse=FakeClickHouseResource(client),
        allow_shrink=True,
    )

    assert rows == 2
    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)


# --- esef_entity_registry_map_clickhouse ------------------------------------


def test_build_esef_entity_registry_map_select_scope_and_normalizers() -> None:
    sql = build_esef_entity_registry_map_select("run-123")

    assert (
        f"lei IN (SELECT DISTINCT lei FROM {tables.QUALIFIED_ESEF_FILINGS_TABLE})"
        in sql
    )
    assert f"FROM {GLEIF_LEI_RECORDS_QUALIFIED_TABLE}" in sql
    assert "WHERE registered_as != ''" in sql

    # FI normalizer: lowercase + strip spaces + 8-bare-digits -> NNNNNNN-N.
    assert "primary_country_iso2 = 'FI'" in sql
    assert "lowerUTF8(trim(registered_as))" in sql
    assert "'^[0-9]{8}$'" in sql
    assert "substring(replaceAll(lowerUTF8(trim(registered_as)), ' ', ''), 1, 7)" in sql

    # SE normalizer: digits only.
    assert "primary_country_iso2 = 'SE'" in sql
    assert "replaceRegexpAll(registered_as, '[^0-9]', '')" in sql

    # Other countries: trimmed raw passthrough (the multiIf else branch).
    assert sql.strip().count("trim(registered_as)") >= 1

    assert f"'{MATCH_SOURCE_GLEIF_REGISTERED_AS}' AS match_source" in sql
    assert "'run-123' AS source_run_id" in sql

    # Deterministic one-row-per-lei guard against un-merged CH duplicates.
    assert "ORDER BY lei, resolved_at DESC" in sql
    assert "LIMIT 1 BY lei" in sql


def test_build_esef_entity_registry_map_select_escapes_run_id() -> None:
    sql = build_esef_entity_registry_map_select("o'brien's-run")
    assert "'o\\'brien\\'s-run' AS source_run_id" in sql


def test_replace_esef_entity_registry_map_clickhouse_stages_and_exchanges() -> None:
    client = FakeClickHouseClient(would_be_row_count=5, post_exchange_row_count=5)

    rows = replace_esef_entity_registry_map_clickhouse(
        clickhouse=FakeClickHouseResource(client),
        source_run_id="run-1",
    )

    assert rows == 5
    assert client.table_checks == [
        (
            tables.ESEF_ENTITY_REGISTRY_MAP_TABLE,
            GLEIF_LEI_RECORDS_TABLE,
            tables.ESEF_FILINGS_TABLE,
        )
    ]
    assert any(s.startswith("CREATE TABLE") for s in client.statements)
    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)
    assert any(s.startswith("DROP TABLE") for s in client.statements)
    # replace_table_from_select reads the row count back from the target
    # AFTER its own cleanup (drop stage) -- that final readback is the last
    # statement, not the DROP itself.
    assert client.statements[-1] == (
        f"SELECT count() FROM {tables.QUALIFIED_ESEF_ENTITY_REGISTRY_MAP_TABLE}"
    )
    # The pre-create refuse-on-empty check must run before any stage DDL.
    count_check_index = next(
        i
        for i, s in enumerate(client.statements)
        if s.strip().startswith("SELECT count() FROM (")
    )
    create_index = next(
        i for i, s in enumerate(client.statements) if s.startswith("CREATE TABLE")
    )
    assert count_check_index < create_index


def test_replace_esef_entity_registry_map_clickhouse_refuses_on_zero_rows() -> None:
    client = FakeClickHouseClient(would_be_row_count=0)

    with pytest.raises(ValueError, match="would insert 0 rows"):
        replace_esef_entity_registry_map_clickhouse(
            clickhouse=FakeClickHouseResource(client),
            source_run_id="run-1",
        )

    assert not any(s.startswith("CREATE TABLE") for s in client.statements)
    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_esef_entity_map_export_columns_match_migration_order() -> None:
    # tables.ESEF_ENTITY_MAP_EXPORT_COLUMNS is asserted (elsewhere) to match
    # migration 000149's column order; the SELECT built here must project
    # exactly those columns, in order, under those aliases. Anchors are
    # chosen to be non-overlapping substrings (e.g. "AS registry_id_raw" is a
    # prefix of "AS registry_id", so the real registry_id anchor includes the
    # closing paren that precedes it).
    sql = build_esef_entity_registry_map_select("run-1")
    assert (
        "lei",
        "country_iso2",
        "registry_id_raw",
        "registry_id",
        "match_source",
        "source_run_id",
    ) == (tables.ESEF_ENTITY_MAP_EXPORT_COLUMNS)
    anchors = [
        "lei,",
        "AS country_iso2",
        "AS registry_id_raw",
        ") AS registry_id,",
        "AS match_source",
        "AS source_run_id",
    ]
    positions = [sql.index(anchor) for anchor in anchors]
    assert positions == sorted(positions)


# --- esef_document_concept_translation_load / coverage ----------------------
#
# One unknown-language label must not take down the whole publish chain (Task
# 1.3): the loader skips it with a warning and still translates every row in
# a known language, and the coverage check reports missing translations at
# WARN severity rather than failing the run (mirrors the deliberate decision
# in translator_load/coverage.py).


class _FakeConceptLabelClickHouseClient:
    """Answers whatever query the asset under test issues with canned rows --
    each test only ever issues one query through its single connection."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self.statements: list[str] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.statements.append(sql)
        return self._rows


class _FakeConceptLabelClickHouseResource:
    def __init__(self, client: _FakeConceptLabelClickHouseClient) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[_FakeConceptLabelClickHouseClient]:
        yield self._client


class _FakeTranslatorSession:
    """Minimal stand-in for requests.Session (mirrors
    tests/test_translator_load.py's _FakeSession): records every enqueue
    POST and answers queue-stats GETs with an already-idle queue, so
    wait_for_queue_completion returns on its first poll."""

    def __init__(self) -> None:
        self.posts: list[tuple[str, dict]] = []

    def __enter__(self) -> "_FakeTranslatorSession":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        return False

    def post(self, url: str, json: dict, timeout: int = 60):
        self.posts.append((url, json))
        received = len(json["items"])

        class _Resp:
            status_code = 202

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"received": received, "inserted": received}

        return _Resp()

    def get(self, url: str, timeout: int = 10):
        class _Resp:
            def raise_for_status(self_inner) -> None:
                return None

            def json(self_inner) -> dict:
                return {"input": 0, "pending": 0, "output": 0, "failed": 0}

        return _Resp()


def test_translation_load_skips_unsupported_language_and_processes_known_ones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Languages "tr" and "uk" are not in _ESEF_LANGUAGE_NAMES; the asset
    must skip only those rows -- not raise dg.Failure -- while still
    enqueueing the "no" row, and must report both the skipped ROW count and
    the skipped LANGUAGE set in its materialization metadata. Two "tr" rows
    plus one "uk" row means the row count (3) and the language count (2)
    genuinely differ, so a regression that logs/reports the language count
    where the row count belongs would be caught here."""
    client = _FakeConceptLabelClickHouseClient(
        rows=[
            ("tr", "Bilinmeyen etiket 1", 111),
            ("tr", "Bilinmeyen etiket 2", 112),
            ("uk", "Невідома етикетка", 113),
            ("no", "Kjent etikett", 222),
        ]
    )
    session = _FakeTranslatorSession()
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)

    result = esef_document_concept_translation_load.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(),
        _FakeConceptLabelClickHouseResource(client),
        TranslatorResource(base_url="http://translator:8080"),
    )

    assert result.metadata["skipped_unsupported_languages"].value == ["tr", "uk"]
    assert result.metadata["skipped_unsupported_label_count"] == 3
    # Only "tr"/"uk" are skipped -- "no" is still scanned and enqueued.
    assert result.metadata["scanned_by_language"].value == {"no": 1}
    assert len(session.posts) == 1
    _, payload = session.posts[0]
    assert payload["source_lang"] == "no"
    assert payload["source_language_name"] == "Norwegian"


def test_translation_coverage_reports_warn_severity_on_mismatch() -> None:
    client = _FakeConceptLabelClickHouseClient(rows=[(10, 7)])

    result = (
        esef_document_concept_translation_coverage.node_def.compute_fn.decorated_fn(
            _FakeConceptLabelClickHouseResource(client)
        )
    )

    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.WARN
