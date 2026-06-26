from contextlib import contextmanager
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import clickhouse, contacts, tables
from tests.test_brazil_rfb_transforms import _build_company_stage


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: object | None = None):
        if "system.tables" in sql:
            return [
                (tables.BR_COMPANIES_TABLE_CH,),
                (tables.BR_ESTABLISHMENTS_TABLE_CH,),
                (tables.BR_COMPANY_CONTACT_INFO_TABLE_CH,),
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


def test_clickhouse_exports_replace_companies_and_establishments(tmp_path: Path) -> None:
    companies_path = _build_company_stage(tmp_path)
    fake_client = FakeClickHouseClient()
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(companies_path)) as connection:
        company_rows = clickhouse.export_brazil_rfb_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )
        establishment_rows = clickhouse.export_brazil_rfb_clickhouse_establishments(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )
        assert connection.execute(
            f"select count(*) from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
        ).fetchone()[0] == company_rows

    assert company_rows == 2
    assert establishment_rows == 2
    assert sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 2
    assert len(fake_client.inserts) == 2
    company_insert_sql, company_insert_rows = fake_client.inserts[0]
    establishment_insert_sql, establishment_insert_rows = fake_client.inserts[1]
    assert tables.BR_COMPANIES_TABLE_CH in company_insert_sql
    assert tables.BR_ESTABLISHMENTS_TABLE_CH in establishment_insert_sql
    assert len(company_insert_rows[0]) == len(tables.BR_COMPANIES_EXPORT_COLUMNS)
    assert len(establishment_insert_rows[0]) == len(tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS)


def test_clickhouse_exports_replace_contact_info_and_websites(tmp_path: Path) -> None:
    companies_path = _build_company_stage(tmp_path)
    contacts_path = tmp_path / "br_contacts.duckdb"
    fake_client = FakeClickHouseClient()
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(contacts_path)) as connection:
        contacts.build_brazil_rfb_contact_info_and_websites(
            connection=connection,
            companies_database_path=companies_path,
            source_run_id="run-contacts",
        )
        contact_rows = clickhouse.export_brazil_rfb_clickhouse_contact_info(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )
        website_rows = clickhouse.export_brazil_rfb_clickhouse_websites(
            duckdb_connection=connection,
            clickhouse=fake_resource,
        )

    assert contact_rows == 2
    assert website_rows == 1
    assert sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 2
    assert len(fake_client.inserts) == 2
    contact_insert_sql, contact_insert_rows = fake_client.inserts[0]
    website_insert_sql, website_insert_rows = fake_client.inserts[1]
    assert tables.BR_COMPANY_CONTACT_INFO_TABLE_CH in contact_insert_sql
    assert tables.BR_WEBSITES_TABLE_CH in website_insert_sql
    assert len(contact_insert_rows[0]) == len(
        tables.BR_COMPANY_CONTACT_INFO_EXPORT_COLUMNS
    )
    assert len(website_insert_rows[0]) == len(tables.BR_WEBSITES_EXPORT_COLUMNS)
