from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.clickhouse import export_sweden_company_clickhouse


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.insert_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[str]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params else ()
            return [(table,) for table in requested]
        if sql.startswith("CREATE TABLE") or sql.startswith("EXCHANGE TABLES"):
            return []
        if sql.startswith("DROP TABLE"):
            return []
        if sql.startswith("INSERT INTO"):
            rows = params if isinstance(params, list) else []
            self.insert_calls.append((sql, rows))
            return []
        raise AssertionError(sql)

    def insert_rows(
        self,
        table: str,
        rows: list[tuple[object, ...]],
        column_names: list[str],
    ) -> None:
        columns = ", ".join(f"`{column}`" for column in column_names)
        self.insert_calls.append((f"INSERT INTO {table} ({columns}) VALUES", rows))


def test_export_sweden_company_clickhouse_replaces_companies_addresses_and_industries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "sweden_company_source.duckdb"
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE} (
                company_id varchar,
                registration_number varchar,
                bolagsverket_company_id_raw varchar,
                scb_company_id_raw varchar,
                legal_name varchar,
                legal_name_raw varchar,
                legal_form_code varchar,
                status varchar,
                status_reason varchar,
                incorporation_date date,
                dissolution_date date,
                activity_description varchar,
                source_run_id varchar,
                bolagsverket_source_record_id varchar,
                scb_source_record_id varchar,
                bolagsverket_source_payload_hash varchar,
                scb_source_payload_hash varchar,
                updated_from_raw_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.COMPANY_ADDRESSES_TABLE} (
                company_id varchar,
                address_type varchar,
                source varchar,
                raw_address varchar,
                street_address varchar,
                care_of varchar,
                postal_code varchar,
                post_town varchar,
                country_code varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                updated_from_raw_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.COMPANY_INDUSTRY_CODES_TABLE} (
                company_id varchar,
                sequence integer,
                is_primary boolean,
                sni_code varchar,
                nace_rev2_class_code varchar,
                source_field varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                updated_from_raw_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}
            values (
                '5560000000',
                '5560000000',
                '5560000000$ORGNR-IDORG',
                '5560000000',
                'Acme AB',
                'Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01',
                'AB-ORGFO',
                'active',
                null,
                '2020-01-01',
                null,
                'Runs acme.se',
                'run-1',
                '5560000000$ORGNR-IDORG',
                '5560000000',
                'bolag-hash-1',
                'scb-hash-1',
                '2026-07-03 12:00:00'
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.{tables.COMPANY_ADDRESSES_TABLE}
            values (
                '5560000000',
                'postal',
                'bolagsverket',
                'Box 1$c/o CFO$STOCKHOLM$11122$SE-LAND',
                'Box 1',
                'c/o CFO',
                '11122',
                'STOCKHOLM',
                'SE',
                'run-1',
                '5560000000$ORGNR-IDORG',
                'bolag-hash-1',
                '2026-07-03 12:00:00'
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.{tables.COMPANY_INDUSTRY_CODES_TABLE}
            values (
                '5560000000',
                1,
                true,
                '62010',
                '6201',
                'Ng1',
                'run-1',
                '5560000000',
                'scb-hash-1',
                '2026-07-03 12:00:00'
            )
            """
        )

        counts = export_sweden_company_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert counts == {
        tables.COMPANIES_TABLE_CH: 1,
        tables.COMPANY_ADDRESSES_TABLE_CH: 1,
        tables.INDUSTRIES_TABLE_CH: 1,
    }
    assert (
        f"CREATE TABLE `corpscout`.`_tmp_{tables.COMPANIES_TABLE_CH}_"
        in client.statements[1]
    )
    assert client.insert_calls[0][0].startswith(
        "INSERT INTO `corpscout`.`_tmp_se_companies_"
    )
    assert client.insert_calls[0][1][0][0:4] == (
        "5560000000",
        "5560000000",
        "5560000000$ORGNR-IDORG",
        "5560000000",
    )
