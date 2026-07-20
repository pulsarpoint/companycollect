"""Tests for esef_filings ClickHouse exports (Task 5): filings-index and
facts export via the shared DuckDB->ClickHouse exporter, plus the
ClickHouse-native gleif entity-registry map.

No live ClickHouse anywhere -- a FakeClickHouseClient/FakeClickHouseResource
pair records every SQL statement (mirroring
tests/test_brazil_fin_cvm_clickhouse_export.py and
tests/test_sweden_financial_metrics.py's stub patterns). The amount_original
cast is the one test that runs a real DuckDB connection through the actual
shared exporter (not monkeypatched) end-to-end, since the whole point is to
prove the try_cast produces a real Decimal (or None), not to check that some
function was called.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.publish import (
    ESEF_FACTS_COLUMN_EXPRESSIONS,
    ESEF_FILINGS_COLUMN_EXPRESSIONS,
    GLEIF_LEI_RECORDS_QUALIFIED_TABLE,
    MATCH_SOURCE_GLEIF_REGISTERED_AS,
    build_esef_entity_registry_map_select,
    export_esef_facts_clickhouse,
    replace_esef_entity_registry_map_clickhouse,
)
from dagster_v3.defs.gleif.tables import GLEIF_LEI_RECORDS_TABLE


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
        duckdb_connection=object(),
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
    assert (
        ESEF_FILINGS_COLUMN_EXPRESSIONS["period_end"] == "try_cast(period_end as date)"
    )
    assert (
        ESEF_FILINGS_COLUMN_EXPRESSIONS["date_added"] == "try_cast(date_added as date)"
    )
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
        duckdb_connection=object(),
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
    assert ESEF_FACTS_COLUMN_EXPRESSIONS["period_end"] == "try_cast(period_end as date)"
    assert (
        ESEF_FACTS_COLUMN_EXPRESSIONS["period_start"]
        == "try_cast(period_start as date)"
    )
    assert (
        ESEF_FACTS_COLUMN_EXPRESSIONS["period_instant"]
        == "try_cast(period_instant as date)"
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
        ],
    )

    client = FakeClickHouseClient()
    rows = export_esef_facts_clickhouse(
        duckdb_connection=con,
        clickhouse=FakeClickHouseResource(client),
    )

    assert rows == 2
    by_fact_id = {
        row[tables.ESEF_FACTS_EXPORT_COLUMNS.index("fact_id")]: row
        for row in client.inserted_rows
    }
    amount_index = tables.ESEF_FACTS_EXPORT_COLUMNS.index("amount_original")
    revenue_amount = by_fact_id["f-revenue"][amount_index]
    assert revenue_amount == Decimal("438395039.49")
    assert isinstance(revenue_amount, Decimal)
    assert by_fact_id["f-narrative"][amount_index] is None


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
