from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from dagster_v3.contact_extraction import (
    COMPANY_CONTACTS_COLUMNS,
    COMPANY_DOMAINS_COLUMNS,
)
from dagster_v3.defs.brazil_companies.rfb import clickhouse, contacts, tables
from tests.test_brazil_comp_rfb_transforms import _build_company_stage


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: object | None = None):
        if "system.tables" in sql:
            return [
                (tables.BR_COMPANIES_TABLE_CH,),
                (tables.BR_ESTABLISHMENTS_TABLE_CH,),
                (tables.BR_COMPANY_CONTACTS_TABLE_CH,),
                (tables.BR_COMPANY_DOMAINS_TABLE_CH,),
                (tables.BR_WEBSITES_TABLE_CH,),
            ]
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


def test_clickhouse_exports_replace_companies_and_establishments(
    tmp_path: Path,
) -> None:
    companies_path = _build_company_stage(tmp_path)
    fake_client = FakeClickHouseClient()
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(companies_path)) as connection:
        company_rows = clickhouse.export_brazil_comp_rfb_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )
        establishment_rows = (
            clickhouse.export_brazil_comp_rfb_clickhouse_establishments(
                duckdb_connection=connection,
                clickhouse=fake_resource,
            )
        )
        assert (
            connection.execute(
                f"select count(*) from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
            ).fetchone()[0]
            == company_rows
        )

    assert company_rows == 2
    assert establishment_rows == 2
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 2
    )
    assert len(fake_client.inserts) == 2
    company_insert_sql, company_insert_rows = fake_client.inserts[0]
    establishment_insert_sql, establishment_insert_rows = fake_client.inserts[1]
    assert tables.BR_COMPANIES_TABLE_CH in company_insert_sql
    assert tables.BR_ESTABLISHMENTS_TABLE_CH in establishment_insert_sql
    assert len(company_insert_rows[0]) == len(tables.BR_COMPANIES_EXPORT_COLUMNS)
    assert len(establishment_insert_rows[0]) == len(
        tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS
    )


def test_clickhouse_company_exports_null_dates_outside_date32_range(
    tmp_path: Path,
) -> None:
    companies_path = _build_company_stage(tmp_path)
    with duckdb.connect(str(companies_path)) as connection:
        connection.execute(
            f"""
            update {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}
            set status_date = date '1893-07-05',
                activity_start_date = date '1893-07-05'
            where cnpj_basico = '12345678'
            """
        )
        connection.execute(
            f"""
            update {tables.DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE}
            set status_date = date '1893-07-05',
                activity_start_date = date '1893-07-05'
            where cnpj = '12345678000190'
            """
        )

        fake_client = FakeClickHouseClient()
        fake_resource = FakeClickHouseResource(fake_client)
        clickhouse.export_brazil_comp_rfb_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )
        clickhouse.export_brazil_comp_rfb_clickhouse_establishments(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )

    company_rows = fake_client.inserts[0][1]
    establishment_rows = fake_client.inserts[1][1]
    company_status_date_index = tables.BR_COMPANIES_EXPORT_COLUMNS.index("status_date")
    company_activity_date_index = tables.BR_COMPANIES_EXPORT_COLUMNS.index(
        "activity_start_date"
    )
    establishment_status_date_index = tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS.index(
        "status_date"
    )
    establishment_activity_date_index = tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS.index(
        "activity_start_date"
    )

    assert company_rows[0][company_status_date_index] is None
    assert company_rows[0][company_activity_date_index] is None
    assert establishment_rows[0][establishment_status_date_index] is None
    assert establishment_rows[0][establishment_activity_date_index] is None


def test_clickhouse_exports_replace_company_contacts_domains_and_websites(
    tmp_path: Path,
) -> None:
    companies_path = _build_company_stage(tmp_path)
    contacts_path = tmp_path / "br_contact_info.duckdb"
    websites_path = tmp_path / "br_websites.duckdb"
    fake_client = FakeClickHouseClient()
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(contacts_path)) as connection:
        contacts.build_brazil_rfb_contact_info(
            connection=connection,
            companies_database_path=companies_path,
            source_run_id="run-contacts",
        )
        company_contacts_rows = (
            clickhouse.export_brazil_comp_rfb_clickhouse_company_contacts(
                duckdb_connection=connection,
                clickhouse=fake_resource,
            )
        )
    with duckdb.connect(str(websites_path)) as connection:
        contacts.build_brazil_rfb_websites(
            connection=connection,
            contact_info_database_path=contacts_path,
        )
        website_rows = clickhouse.export_brazil_comp_rfb_clickhouse_websites(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )
        company_domains_rows = (
            clickhouse.export_brazil_comp_rfb_clickhouse_company_domains(
                duckdb_connection=connection,
                clickhouse=fake_resource,
            )
        )

    assert company_contacts_rows == 2
    assert website_rows == 1
    assert company_domains_rows == 1
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 3
    )
    assert len(fake_client.inserts) == 3
    contacts_insert_sql, contacts_insert_rows = fake_client.inserts[0]
    website_insert_sql, website_insert_rows = fake_client.inserts[1]
    domains_insert_sql, domains_insert_rows = fake_client.inserts[2]
    assert tables.BR_COMPANY_CONTACTS_TABLE_CH in contacts_insert_sql
    assert tables.BR_WEBSITES_TABLE_CH in website_insert_sql
    assert tables.BR_COMPANY_DOMAINS_TABLE_CH in domains_insert_sql
    assert len(contacts_insert_rows[0]) == len(COMPANY_CONTACTS_COLUMNS)
    assert len(website_insert_rows[0]) == len(tables.BR_WEBSITES_EXPORT_COLUMNS)
    assert len(domains_insert_rows[0]) == len(COMPANY_DOMAINS_COLUMNS)

    # The CH `confidence` column is Float32 while the DuckDB stage value is a
    # double (exact 0.9) — compare with approx once the value has passed
    # through a (fake) ClickHouse client round-trip.
    confidence_index = COMPANY_DOMAINS_COLUMNS.index("confidence")
    assert domains_insert_rows[0][confidence_index] == pytest.approx(0.9)


def test_export_column_tuples_are_the_shared_canonical_tuples() -> None:
    # Identity (not just equality) pin: clickhouse.py's export columns ARE the
    # shared dagster_v3.contact_extraction tuples, so the DuckDB stage order,
    # the ClickHouse export order, and the migration DDL order can never diverge.
    assert tables.BR_COMPANY_CONTACTS_EXPORT_COLUMNS is COMPANY_CONTACTS_COLUMNS
    assert tables.BR_COMPANY_DOMAINS_EXPORT_COLUMNS is COMPANY_DOMAINS_COLUMNS
