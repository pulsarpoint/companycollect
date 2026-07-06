from contextlib import contextmanager
from datetime import date
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_companies.cgu import clickhouse, parsing, tables


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: object | None = None):
        if "system.tables" in sql:
            return [(table,) for table in tables.CLICKHOUSE_TABLES]
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


def test_cgu_clickhouse_export_replaces_selected_table(tmp_path: Path) -> None:
    db_path = tmp_path / "cgu.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(f"create schema {parsing.BRAZIL_CGU_DUCKDB_SCHEMA}")
        connection.execute(
            f"""
            create table {parsing.BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.CEPIM_BLOCKED_ENTITIES_TABLE}
            as select
                'BR'::varchar as country_iso2,
                'brazil_cgu'::varchar as source_slug,
                'run-1'::varchar as source_run_id,
                'record-1'::varchar as source_record_id,
                '2026-07-03'::varchar as snapshot_date,
                'cepim'::varchar as source_dataset,
                'https://example.test/cepim.zip'::varchar as source_url,
                'archive.zip'::varchar as source_archive_key,
                '20260703_CEPIM.csv'::varchar as source_file_name,
                2::ubigint as source_row_number,
                '01994905000197'::varchar as cnpj,
                '01994905'::varchar as cnpj_basico,
                'COOPERATIVA ANCORA'::varchar as entity_name,
                '555842'::varchar as agreement_number,
                'Ministério da Cultura'::varchar as granting_agency,
                'IRREGULARIDADE'::varchar as impediment_reason,
                'payload-hash'::varchar as source_payload_hash,
                current_timestamp as resolved_at
            """
        )

        fake_client = FakeClickHouseClient()
        fake_resource = FakeClickHouseResource(fake_client)
        rows = clickhouse.export_brazil_comp_cgu_table_clickhouse(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            table=tables.CGU_TABLES[tables.CEPIM_BLOCKED_ENTITIES_TABLE],
        )

    assert rows == 1
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 1
    )
    assert len(fake_client.inserts) == 1
    insert_sql, inserted_rows = fake_client.inserts[0]
    assert tables.BR_CGU_CEPIM_BLOCKED_ENTITIES_TABLE_CH in insert_sql
    assert len(inserted_rows[0]) == len(
        tables.CGU_TABLES[tables.CEPIM_BLOCKED_ENTITIES_TABLE].columns
    )
    columns = tables.CGU_TABLES[tables.CEPIM_BLOCKED_ENTITIES_TABLE].columns
    assert inserted_rows[0][columns.index("snapshot_date")] == date(2026, 7, 3)


def test_cgu_clickhouse_export_casts_varchar_sanction_dates(tmp_path: Path) -> None:
    db_path = tmp_path / "cgu.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(f"create schema {parsing.BRAZIL_CGU_DUCKDB_SCHEMA}")
        connection.execute(
            f"""
            create table {parsing.BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.CEIS_COMPANY_SANCTIONS_TABLE}
            as select
                'BR'::varchar as country_iso2,
                'brazil_cgu'::varchar as source_slug,
                'run-1'::varchar as source_run_id,
                'record-1'::varchar as source_record_id,
                '2026-07-06'::varchar as snapshot_date,
                'ceis'::varchar as source_dataset,
                'https://example.test/ceis.zip'::varchar as source_url,
                'archive.zip'::varchar as source_archive_key,
                '20260706_CEIS.csv'::varchar as source_file_name,
                2::ubigint as source_row_number,
                'CEIS'::varchar as registry,
                '90923'::varchar as sanction_id,
                '13221906000188'::varchar as cnpj,
                '13221906'::varchar as cnpj_basico,
                'J'::varchar as person_type,
                'HYLUX REFORMAS E SERVICOS LTDA'::varchar as sanctioned_name,
                'HYLUX REFORMAS E SERVICOS LTDA'::varchar as sanctioning_agency_reported_name,
                'HYLUX REFORMAS E SERVICOS LTDA'::varchar as receita_legal_name,
                'HYLUX SERVICOS'::varchar as receita_trade_name,
                '0200150272334'::varchar as process_number,
                'Declaração de Inidoneidade'::varchar as sanction_category,
                '2018-01-20'::varchar as sanction_start_date,
                null::varchar as sanction_end_date,
                '2018-01-20'::varchar as publication_date,
                'Diário Oficial'::varchar as publication,
                ''::varchar as publication_detail,
                null::varchar as final_judgment_date,
                'Nacional'::varchar as sanction_scope,
                'Prefeitura'::varchar as sanctioning_agency,
                'SP'::varchar as sanctioning_agency_state,
                'Municipal'::varchar as sanctioning_agency_sphere,
                'Lei 8666'::varchar as legal_basis,
                '2026-07-06'::varchar as source_information_date,
                'CEIS'::varchar as information_origin,
                ''::varchar as notes,
                'payload-hash'::varchar as source_payload_hash,
                current_timestamp as resolved_at
            """
        )

        fake_client = FakeClickHouseClient()
        fake_resource = FakeClickHouseResource(fake_client)
        rows = clickhouse.export_brazil_comp_cgu_table_clickhouse(
            duckdb_connection=connection,
            clickhouse=fake_resource,
            table=tables.CGU_TABLES[tables.CEIS_COMPANY_SANCTIONS_TABLE],
        )

    assert rows == 1
    _, inserted_rows = fake_client.inserts[0]
    columns = tables.CGU_TABLES[tables.CEIS_COMPANY_SANCTIONS_TABLE].columns
    assert inserted_rows[0][columns.index("snapshot_date")] == date(2026, 7, 6)
    assert inserted_rows[0][columns.index("sanction_start_date")] == date(2018, 1, 20)
    assert inserted_rows[0][columns.index("sanction_end_date")] is None
    assert inserted_rows[0][columns.index("publication_date")] == date(2018, 1, 20)
    assert inserted_rows[0][columns.index("source_information_date")] == date(
        2026, 7, 6
    )
