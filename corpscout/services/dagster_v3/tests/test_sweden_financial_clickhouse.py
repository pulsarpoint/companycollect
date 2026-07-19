from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.clickhouse import (
    SE_FINANCIAL_FACTS_TABLE,
    SE_FINANCIAL_REPORTS_TABLE,
    SHRINK_GUARD_MIN_RATIO,
    export_sweden_financial_facts_clickhouse,
    export_sweden_financial_reports_clickhouse,
    guard_against_clickhouse_table_shrink,
)
from dagster_v3.defs.sweden_financial.parsing import SWEDEN_FINANCIAL_DATASET_NAME
from dagster_v3.defs.sweden_financial.storage import (
    existing_sweden_financial_source_duckdb_paths,
    sweden_financial_read_only_partitioned_connection,
)


class FakeClickHouseClient:
    def __init__(self, *, existing_row_count: int = 0) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.insert_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.existing_row_count = existing_row_count

    def execute(
        self,
        sql: str,
        params: dict[str, object] | list[tuple[object, ...]] | None = None,
    ) -> list[tuple[str]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if isinstance(params, dict) else ()
            self.table_checks.append(requested)
            return [(table,) for table in requested]
        if sql.startswith("SELECT count() FROM"):
            return [(self.existing_row_count,)]
        if sql.startswith("CREATE TABLE") or sql.startswith("EXCHANGE TABLES"):
            return []
        if sql.startswith("DROP TABLE"):
            return []
        if sql.startswith("INSERT INTO"):
            rows = params if isinstance(params, list) else []
            self.insert_calls.append((sql, rows))
            return []
        raise AssertionError(sql)


def test_export_sweden_financial_reports_clickhouse_replaces_reports(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_financial_duckdb(tmp_path) as connection:
        rows = export_sweden_financial_reports_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert rows == 1
    assert client.table_checks == [(SE_FINANCIAL_REPORTS_TABLE,)]
    assert client.statements[1].startswith(
        f"SELECT count() FROM `corpscout`.`{SE_FINANCIAL_REPORTS_TABLE}`"
    )
    assert (
        f"CREATE TABLE `corpscout`.`_tmp_{SE_FINANCIAL_REPORTS_TABLE}_"
        in client.statements[2]
    )
    assert len(client.insert_calls) == 1
    assert client.insert_calls[0][0].startswith(
        "INSERT INTO `corpscout`.`_tmp_se_financial_reports_"
    )
    assert client.insert_calls[0][1][0][0:6] == (
        "SE",
        "bolagsverket_annual_reports",
        "run-1",
        "5560000000:2025-12-31:report.xhtml",
        "5560000000:2025-12-31:report.xhtml",
        "5560000000",
    )


def test_export_sweden_financial_facts_clickhouse_replaces_facts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_financial_duckdb(tmp_path) as connection:
        rows = export_sweden_financial_facts_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert rows == 1
    assert client.table_checks == [(SE_FINANCIAL_FACTS_TABLE,)]
    assert client.statements[1].startswith(
        f"SELECT count() FROM `corpscout`.`{SE_FINANCIAL_FACTS_TABLE}`"
    )
    assert (
        f"CREATE TABLE `corpscout`.`_tmp_{SE_FINANCIAL_FACTS_TABLE}_"
        in client.statements[2]
    )
    assert len(client.insert_calls) == 1
    assert client.insert_calls[0][0].startswith(
        "INSERT INTO `corpscout`.`_tmp_se_financial_facts_"
    )
    assert client.insert_calls[0][1][0][0:8] == (
        "SE",
        "bolagsverket_annual_reports",
        "run-1",
        "5560000000:2025-12-31:report.xhtml",
        "5560000000:2025-12-31:report.xhtml",
        "5560000000",
        date(2025, 12, 31),
        1,
    )


def test_guard_against_clickhouse_table_shrink_refuses_at_49_percent() -> None:
    with pytest.raises(ValueError, match="Refusing to replace ClickHouse table"):
        guard_against_clickhouse_table_shrink(
            qualified_table="corpscout.se_financial_reports",
            existing_row_count=100,
            staged_row_count=49,
            allow_shrink=False,
        )


def test_guard_against_clickhouse_table_shrink_message_names_table_existing_staged() -> (
    None
):
    with pytest.raises(ValueError) as exc_info:
        guard_against_clickhouse_table_shrink(
            qualified_table="corpscout.se_financial_reports",
            existing_row_count=100,
            staged_row_count=49,
            allow_shrink=False,
        )

    message = str(exc_info.value)
    assert "corpscout.se_financial_reports" in message
    assert "100" in message
    assert "49" in message
    assert "allow_shrink=True" in message


def test_guard_against_clickhouse_table_shrink_allows_at_51_percent() -> None:
    guard_against_clickhouse_table_shrink(
        qualified_table="corpscout.se_financial_reports",
        existing_row_count=100,
        staged_row_count=51,
        allow_shrink=False,
    )


def test_guard_against_clickhouse_table_shrink_allows_exact_50_percent_boundary() -> None:
    # SHRINK_GUARD_MIN_RATIO is inclusive: staged == exactly half of existing
    # is allowed, only strictly-below-half refuses.
    assert SHRINK_GUARD_MIN_RATIO == 0.5
    guard_against_clickhouse_table_shrink(
        qualified_table="corpscout.se_financial_reports",
        existing_row_count=100,
        staged_row_count=50,
        allow_shrink=False,
    )


def test_guard_against_clickhouse_table_shrink_never_trips_on_empty_existing_table() -> (
    None
):
    # A never-before-populated (or already-empty) target has nothing to
    # shrink from -- the guard is a no-op regardless of staged count.
    guard_against_clickhouse_table_shrink(
        qualified_table="corpscout.se_financial_reports",
        existing_row_count=0,
        staged_row_count=0,
        allow_shrink=False,
    )


def test_guard_against_clickhouse_table_shrink_allow_shrink_overrides() -> None:
    # The explicit override path: even a would-be-refused shrink proceeds
    # when the caller has explicitly opted in.
    guard_against_clickhouse_table_shrink(
        qualified_table="corpscout.se_financial_reports",
        existing_row_count=100,
        staged_row_count=1,
        allow_shrink=True,
    )


def test_export_sweden_financial_reports_clickhouse_refuses_shrink_below_half(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # Regression test for the 2026-07-19 live incident: a full-replace
    # export from a local DuckDB holding only a partial partition set must
    # not silently truncate a populated ClickHouse target.
    client = FakeClickHouseClient(existing_row_count=100)
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_financial_duckdb_with_report_rows(tmp_path, count=49) as connection:
        with pytest.raises(ValueError, match="Refusing to replace ClickHouse table"):
            export_sweden_financial_reports_clickhouse(
                duckdb_connection=connection,
                clickhouse=resource,
            )

    # Refused before the stage table was ever created/inserted into.
    assert not any(
        statement.startswith("CREATE TABLE") for statement in client.statements
    )
    assert client.insert_calls == []


def test_export_sweden_financial_reports_clickhouse_allow_shrink_overrides_guard(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = FakeClickHouseClient(existing_row_count=100)
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_financial_duckdb_with_report_rows(tmp_path, count=49) as connection:
        rows = export_sweden_financial_reports_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
            allow_shrink=True,
        )

    assert rows == 49
    assert any(statement.startswith("CREATE TABLE") for statement in client.statements)
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


def test_export_sweden_financial_facts_clickhouse_refuses_shrink_below_half(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = FakeClickHouseClient(existing_row_count=100)
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_financial_duckdb_with_fact_rows(tmp_path, count=49) as connection:
        with pytest.raises(ValueError, match="Refusing to replace ClickHouse table"):
            export_sweden_financial_facts_clickhouse(
                duckdb_connection=connection,
                clickhouse=resource,
            )

    assert not any(
        statement.startswith("CREATE TABLE") for statement in client.statements
    )
    assert client.insert_calls == []


def test_export_sweden_financial_facts_clickhouse_allow_shrink_overrides_guard(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = FakeClickHouseClient(existing_row_count=100)
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_financial_duckdb_with_fact_rows(tmp_path, count=51) as connection:
        rows = export_sweden_financial_facts_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
            allow_shrink=False,
        )

    # 51% clears the guard on its own -- no override needed.
    assert rows == 51
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


def test_sweden_financial_partitioned_connection_unions_year_files(
    tmp_path: Path,
) -> None:
    _write_year_file(tmp_path, "2025")
    _write_year_file(tmp_path, "2026")

    paths = existing_sweden_financial_source_duckdb_paths(
        years=("2024", "2025", "2026"),
        root=tmp_path,
    )
    assert [path.name for path in paths] == [
        "sweden_financial_source_2025.duckdb",
        "sweden_financial_source_2026.duckdb",
    ]

    with sweden_financial_read_only_partitioned_connection(
        years=("2024", "2025", "2026"),
        table_names=("reports",),
        root=tmp_path,
    ) as connection:
        rows = connection.execute(
            f"""
            select source_record_id
            from {SWEDEN_FINANCIAL_DATASET_NAME}.reports
            order by source_record_id
            """
        ).fetchall()

    assert rows == [
        ("5560000000:2025-12-31:report.xhtml",),
        ("5560000000:2026-12-31:report.xhtml",),
    ]


@contextmanager
def _sweden_financial_duckdb(tmp_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    database_path = tmp_path / "sweden_financial_source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        _create_sweden_financial_tables(connection)
        _insert_sample_report(connection, "2025")
        _insert_sample_fact(connection, "2025")
        yield connection


# Distinct synthetic "years" (each yields a distinct source_record_id/
# period_end) used purely to generate N staged rows for the shrink-guard
# tests -- unrelated to any real Sweden financial partition year.
_SHRINK_GUARD_SYNTHETIC_YEARS_BASE = 3000


@contextmanager
def _sweden_financial_duckdb_with_report_rows(
    tmp_path: Path,
    *,
    count: int,
) -> Iterator[duckdb.DuckDBPyConnection]:
    database_path = tmp_path / "sweden_financial_source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        _create_sweden_financial_tables(connection)
        for offset in range(count):
            _insert_sample_report(
                connection, str(_SHRINK_GUARD_SYNTHETIC_YEARS_BASE + offset)
            )
        yield connection


@contextmanager
def _sweden_financial_duckdb_with_fact_rows(
    tmp_path: Path,
    *,
    count: int,
) -> Iterator[duckdb.DuckDBPyConnection]:
    database_path = tmp_path / "sweden_financial_source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        _create_sweden_financial_tables(connection)
        for offset in range(count):
            _insert_sample_fact(
                connection, str(_SHRINK_GUARD_SYNTHETIC_YEARS_BASE + offset)
            )
        yield connection


def _write_year_file(root: Path, year: str) -> None:
    database_path = root / f"sweden_financial_source_{year}.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        _create_sweden_financial_tables(connection)
        _insert_sample_report(connection, year)


def _create_sweden_financial_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema {SWEDEN_FINANCIAL_DATASET_NAME}")
    connection.execute(
        f"""
        create table {SWEDEN_FINANCIAL_DATASET_NAME}.reports (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            statement_key varchar,
            company_id varchar,
            report_period_start date,
            report_period_end date,
            fiscal_year integer,
            reported_company_name varchar,
            report_language varchar,
            source_archive_key varchar,
            source_archive_name varchar,
            nested_zip_name varchar,
            xhtml_object_key varchar,
            xhtml_sha256 varchar,
            xhtml_size_bytes bigint,
            taxonomy_entrypoint varchar,
            schema_refs varchar,
            contexts_count bigint,
            units_count bigint,
            facts_count bigint,
            parser_version varchar,
            source_payload_hash varchar,
            resolved_at timestamp
        )
        """
    )
    connection.execute(
        f"""
        create table {SWEDEN_FINANCIAL_DATASET_NAME}.facts (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            statement_key varchar,
            company_id varchar,
            report_period_end date,
            fact_ordinal bigint,
            concept_qname varchar,
            concept_namespace varchar,
            concept_local_name varchar,
            context_id varchar,
            unit_id varchar,
            decimals varchar,
            precision varchar,
            value_kind varchar,
            raw_value varchar,
            amount_original decimal(38, 10),
            amount_usd decimal(38, 10),
            date_value date,
            text_value varchar,
            currency varchar,
            dimensions varchar,
            fx_rate_to_usd decimal(38, 12),
            fx_rate_date date,
            fx_source varchar,
            parser_version varchar,
            source_payload_hash varchar,
            resolved_at timestamp
        )
        """
    )


def _insert_sample_report(
    connection: duckdb.DuckDBPyConnection,
    year: str,
) -> None:
    period_end = date(int(year), 12, 31)
    source_record_id = f"5560000000:{period_end.isoformat()}:report.xhtml"
    connection.execute(
        f"""
        insert into {SWEDEN_FINANCIAL_DATASET_NAME}.reports
        values (
            'SE',
            'bolagsverket_annual_reports',
            'run-1',
            ?,
            ?,
            '5560000000',
            ?,
            ?,
            ?,
            'Acme AB',
            'sv',
            'sweden_financial/raw_archives/year=2025/source.zip',
            'source.zip',
            'nested.zip',
            'sweden_financial/report_xhtml/report.xhtml',
            'abc123',
            42,
            'entrypoint',
            '[]',
            1,
            1,
            1,
            'parser-v1',
            'abc123',
            ?
        )
        """,
        [
            source_record_id,
            source_record_id,
            date(int(year), 1, 1),
            period_end,
            int(year),
            datetime(2026, 1, 2, 3, 4, 5),
        ],
    )


def _insert_sample_fact(
    connection: duckdb.DuckDBPyConnection,
    year: str,
) -> None:
    period_end = date(int(year), 12, 31)
    source_record_id = f"5560000000:{period_end.isoformat()}:report.xhtml"
    connection.execute(
        f"""
        insert into {SWEDEN_FINANCIAL_DATASET_NAME}.facts
        values (
            'SE',
            'bolagsverket_annual_reports',
            'run-1',
            ?,
            ?,
            '5560000000',
            ?,
            1,
            'se:Revenue',
            'se',
            'Revenue',
            'ctx-1',
            'SEK',
            '0',
            null,
            'numeric',
            '1000',
            ?,
            null,
            null,
            null,
            'SEK',
            '{{}}',
            null,
            null,
            '',
            'parser-v1',
            'abc123',
            ?
        )
        """,
        [
            source_record_id,
            source_record_id,
            period_end,
            Decimal("1000.0000000000"),
            datetime(2026, 1, 2, 3, 4, 5),
        ],
    )
