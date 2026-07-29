from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.brazil_companies.pgfn import clickhouse, parsing, tables


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: object | None = None):
        if "system.tables" in sql:
            return [(tables.BR_PGFN_COMPANY_DEBTS_TABLE_CH,)]
        if isinstance(params, list):
            self.inserts.append((sql, params))
            return None
        self.statements.append(sql)
        return None


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def _empty_pgfn_connection(path: Path) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(str(path))
    connection.execute(f"create schema {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE} (
            snapshot_year usmallint,
            snapshot_quarter utinyint
        )
        """
    )
    return connection


def test_pgfn_clickhouse_export_inserts_only_requested_partition(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pgfn.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(f"create schema {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}")
        connection.execute(
            f"""
            create table {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
            as select
                'BR'::varchar as country_iso2,
                'brazil_pgfn'::varchar as source_slug,
                'run-1'::varchar as source_run_id,
                'record-1'::varchar as source_record_id,
                2026::usmallint as snapshot_year,
                1::utinyint as snapshot_quarter,
                '2026-03'::varchar as snapshot_month,
                date '2026-03-31' as snapshot_reference_date,
                'fgts'::varchar as source_system,
                'https://example.test/file.zip'::varchar as source_url,
                'archive.zip'::varchar as source_archive_key,
                'arquivo_lai_FGTS_1_202603.csv'::varchar as source_file_name,
                2::ubigint as source_row_number,
                '16584543000133'::varchar as cnpj,
                '16584543'::varchar as cnpj_basico,
                'Pessoa jurídica'::varchar as person_type,
                'Principal'::varchar as debtor_role,
                'Company'::varchar as debtor_name,
                'AC'::varchar as debtor_state,
                'ACRE'::varchar as responsible_unit,
                'PGFN'::varchar as responsible_entity,
                'ACRE'::varchar as inscription_unit,
                'FGAC202500025'::varchar as inscription_number,
                'Em cobrança'::varchar as inscription_situation_type,
                'INSCRITA'::varchar as inscription_situation,
                'Contribuições FGTS'::varchar as main_revenue,
                date '2025-04-03' as inscription_date,
                false as is_lawsuit,
                312038.84::decimal(38, 6) as consolidated_amount_brl,
                current_timestamp as resolved_at
            """
        )
        connection.execute(
            f"""
            insert into {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
            select
                * replace (
                    'run-2' as source_run_id,
                    'record-2' as source_record_id,
                    2025::usmallint as snapshot_year,
                    4::utinyint as snapshot_quarter,
                    '2025-12' as snapshot_month,
                    date '2025-12-31' as snapshot_reference_date
                )
            from {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
            """
        )

        fake_client = FakeClickHouseClient()
        fake_resource = FakeClickHouseResource(fake_client)
        rows = clickhouse.export_brazil_comp_pgfn_company_debts_clickhouse(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_quarter="2026-Q1",
        )

    assert rows == 1
    assert all(
        "EXCHANGE TABLES" not in statement for statement in fake_client.statements
    )
    assert len(fake_client.inserts) == 1
    insert_sql, inserted_rows = fake_client.inserts[0]
    assert tables.BR_PGFN_COMPANY_DEBTS_TABLE_CH in insert_sql
    assert len(inserted_rows[0]) == len(tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS)
    assert (
        inserted_rows[0][
            tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS.index("snapshot_year")
        ]
        == 2026
    )
    assert (
        inserted_rows[0][
            tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS.index("snapshot_quarter")
        ]
        == 1
    )


def test_pgfn_clickhouse_export_coalesces_null_strings(tmp_path: Path) -> None:
    db_path = tmp_path / "pgfn.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(f"create schema {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}")
        connection.execute(
            f"""
            create table {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
            as select
                'BR'::varchar as country_iso2,
                'brazil_pgfn'::varchar as source_slug,
                'run-1'::varchar as source_run_id,
                'record-1'::varchar as source_record_id,
                2026::usmallint as snapshot_year,
                1::utinyint as snapshot_quarter,
                null::varchar as snapshot_month,
                date '2026-03-31' as snapshot_reference_date,
                'fgts'::varchar as source_system,
                'https://example.test/file.zip'::varchar as source_url,
                'archive.zip'::varchar as source_archive_key,
                'arquivo_lai_FGTS_1_202603.csv'::varchar as source_file_name,
                2::ubigint as source_row_number,
                null::varchar as cnpj,
                null::varchar as cnpj_basico,
                null::varchar as person_type,
                null::varchar as debtor_role,
                null::varchar as debtor_name,
                null::varchar as debtor_state,
                null::varchar as responsible_unit,
                null::varchar as responsible_entity,
                null::varchar as inscription_unit,
                null::varchar as inscription_number,
                null::varchar as inscription_situation_type,
                null::varchar as inscription_situation,
                null::varchar as main_revenue,
                null::date as inscription_date,
                null::boolean as is_lawsuit,
                null::decimal(38, 6) as consolidated_amount_brl,
                current_timestamp as resolved_at
            """
        )

        fake_client = FakeClickHouseClient()
        fake_resource = FakeClickHouseResource(fake_client)
        rows = clickhouse.export_brazil_comp_pgfn_company_debts_clickhouse(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            snapshot_quarter="2026-Q1",
        )

    assert rows == 1
    _, inserted_rows = fake_client.inserts[0]
    columns = tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS
    assert inserted_rows[0][columns.index("snapshot_month")] == ""
    assert inserted_rows[0][columns.index("cnpj")] == ""
    assert inserted_rows[0][columns.index("debtor_name")] == ""
    assert inserted_rows[0][columns.index("main_revenue")] == ""
    assert inserted_rows[0][columns.index("inscription_date")] is None
    assert inserted_rows[0][columns.index("is_lawsuit")] is None
    assert inserted_rows[0][columns.index("consolidated_amount_brl")] is None


def test_pgfn_clickhouse_export_uses_small_insert_batches(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_assert_tables_exist(*args, **kwargs) -> None:
        captured["asserted"] = True

    def fake_export_duckdb_connection_table_to_clickhouse(**kwargs: object) -> int:
        captured.update(kwargs)
        return 123

    monkeypatch.setattr(
        clickhouse, "assert_clickhouse_tables_exist", fake_assert_tables_exist
    )
    monkeypatch.setattr(
        clickhouse,
        "export_duckdb_connection_table_to_clickhouse",
        fake_export_duckdb_connection_table_to_clickhouse,
    )

    rows = clickhouse.export_brazil_comp_pgfn_company_debts_clickhouse(
        duckdb_connection=_empty_pgfn_connection(tmp_path / "pgfn.duckdb"),
        clickhouse=FakeClickHouseResource(FakeClickHouseClient()),
        snapshot_quarter="2026-Q1",
    )

    assert rows == 123
    assert captured["batch_size"] == clickhouse.PGFN_CLICKHOUSE_INSERT_BATCH_SIZE
    assert clickhouse.PGFN_CLICKHOUSE_INSERT_BATCH_SIZE < 50_000
    assert captured["duckdb_schema"] == "temp"
    assert captured["duckdb_table"] == clickhouse.PGFN_PARTITION_EXPORT_VIEW
    assert captured["truncate"] is False


def test_pgfn_clickhouse_export_passes_log_to_shared_export(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_assert_tables_exist(*args, **kwargs) -> None:
        captured["asserted"] = True

    def fake_export_duckdb_connection_table_to_clickhouse(**kwargs: object) -> int:
        captured.update(kwargs)
        return 123

    monkeypatch.setattr(
        clickhouse, "assert_clickhouse_tables_exist", fake_assert_tables_exist
    )
    monkeypatch.setattr(
        clickhouse,
        "export_duckdb_connection_table_to_clickhouse",
        fake_export_duckdb_connection_table_to_clickhouse,
    )

    messages: list[str] = []
    rows = clickhouse.export_brazil_comp_pgfn_company_debts_clickhouse(
        duckdb_connection=_empty_pgfn_connection(tmp_path / "pgfn.duckdb"),
        clickhouse=FakeClickHouseResource(FakeClickHouseClient()),
        snapshot_quarter="2026-Q1",
        log=lambda message, *args: messages.append(message % args),
    )

    assert rows == 123
    assert captured["log"] is not None
    captured_log = captured["log"]
    assert callable(captured_log)
    captured_log("batch marker %d", 1)
    assert any(
        "snapshot_quarter=2026-Q1 batch marker 1" in message for message in messages
    )


def test_pgfn_clickhouse_export_refuses_empty_partition(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        clickhouse,
        "assert_clickhouse_tables_exist",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        clickhouse,
        "export_duckdb_connection_table_to_clickhouse",
        lambda **kwargs: 0,
    )

    with pytest.raises(
        ValueError,
        match="Brazil PGFN DuckDB partition 2026-Q1 has 0 rows",
    ):
        clickhouse.export_brazil_comp_pgfn_company_debts_clickhouse(
            duckdb_connection=_empty_pgfn_connection(tmp_path / "pgfn.duckdb"),
            clickhouse=FakeClickHouseResource(FakeClickHouseClient()),
            snapshot_quarter="2026-Q1",
        )
