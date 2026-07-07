from contextlib import contextmanager
from pathlib import Path

import duckdb

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


def test_pgfn_clickhouse_export_replaces_company_debts(tmp_path: Path) -> None:
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

        fake_client = FakeClickHouseClient()
        fake_resource = FakeClickHouseResource(fake_client)
        rows = clickhouse.export_brazil_comp_pgfn_company_debts_clickhouse(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )

    assert rows == 1
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 1
    )
    assert len(fake_client.inserts) == 1
    insert_sql, inserted_rows = fake_client.inserts[0]
    assert tables.BR_PGFN_COMPANY_DEBTS_TABLE_CH in insert_sql
    assert len(inserted_rows[0]) == len(tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS)


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
