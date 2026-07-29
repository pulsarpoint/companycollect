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
                (tables.BR_COMPANY_RELATIONS_TABLE_CH,),
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


def test_clickhouse_exports_replace_company_relations(tmp_path: Path) -> None:
    relations_path = tmp_path / "br_company_relations.duckdb"
    fake_client = FakeClickHouseClient()
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.COMPANY_RELATIONS_TABLE} as
            select * from (values
                ('BR', 'brazil_rfb', '12345678', '1', '11111111000191', '22',
                 '20100501', 'PARENT HOLDING LTDA', '', '', '', '', '',
                 date '2010-05-01', timestamp '2026-07-29 00:00:00'),
                ('BR', 'brazil_rfb', '12345678', '2', '***123456**', '49',
                 '20150310', 'JOAO DA SILVA', '', '', '', '', '',
                 date '2015-03-10', timestamp '2026-07-29 00:00:00')
            ) as t(country_iso2, source_slug, cnpj_basico, related_entity_kind,
                   related_tax_id, relation_code, relation_since_key, related_name,
                   related_country, age_band, representative_tax_id,
                   representative_name, representative_code, relation_since,
                   resolved_at)
            """
        )
        relation_rows = (
            clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
                duckdb_connection=connection,
                clickhouse=fake_resource,
            )
        )

    assert relation_rows == 2
    assert (
        sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 1
    )
    assert len(fake_client.inserts) == 1
    relations_insert_sql, relations_insert_rows = fake_client.inserts[0]
    assert tables.BR_COMPANY_RELATIONS_TABLE_CH in relations_insert_sql
    assert len(relations_insert_rows[0]) == len(
        tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS
    )
    related_kind_index = tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS.index(
        "related_entity_kind"
    )
    shipped_kinds = {row[related_kind_index] for row in relations_insert_rows}
    assert shipped_kinds == {"1", "2"}


def test_clickhouse_company_relations_export_nulls_relation_since_outside_date32_range(
    tmp_path: Path,
) -> None:
    """I1: relation_since must get the same Date32 guard as status_date /
    activity_start_date -- the design doc records that this RFB family
    contains dates outside ClickHouse's Date32 range (1900-2299), and one
    bad row must not fail the whole 20-25M-row partition export."""
    relations_path = tmp_path / "br_company_relations.duckdb"
    fake_client = FakeClickHouseClient()
    fake_resource = FakeClickHouseResource(fake_client)

    with duckdb.connect(str(relations_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.COMPANY_RELATIONS_TABLE} as
            select * from (values
                ('BR', 'brazil_rfb', '12345678', '1', '11111111000191', '22',
                 '18991231', 'PARENT HOLDING LTDA', '', '', '', '', '',
                 date '1899-12-31', timestamp '2026-07-29 00:00:00'),
                ('BR', 'brazil_rfb', '12345678', '2', '***123456**', '49',
                 '20150310', 'JOAO DA SILVA', '', '', '', '', '',
                 date '2015-03-10', timestamp '2026-07-29 00:00:00')
            ) as t(country_iso2, source_slug, cnpj_basico, related_entity_kind,
                   related_tax_id, relation_code, relation_since_key, related_name,
                   related_country, age_band, representative_tax_id,
                   representative_name, representative_code, relation_since,
                   resolved_at)
            """
        )
        relation_rows = (
            clickhouse.export_brazil_comp_rfb_clickhouse_company_relations(
                duckdb_connection=connection,
                clickhouse=fake_resource,
            )
        )

    assert relation_rows == 2
    relations_insert_rows = fake_client.inserts[0][1]
    relation_since_index = tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS.index(
        "relation_since"
    )
    relation_since_key_index = (
        tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS.index("relation_since_key")
    )
    relation_since_by_key = {
        row[relation_since_key_index]: row[relation_since_index]
        for row in relations_insert_rows
    }
    assert relation_since_by_key["18991231"] is None
    assert relation_since_by_key["20150310"] is not None


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


def test_company_relations_snapshot_input_is_everything_the_build_produces() -> None:
    """The exporter ships THIS tuple positionally, and a column left out of it is
    never inserted -- ClickHouse fills it with the type default instead. For
    resolved_at (DateTime64) that default is the epoch, on every row, silently.

    So the tuple must be exactly the history shape minus the six columns the
    merge computes, and nothing else may go missing.
    """
    history_only = {
        "first_seen_snapshot",
        "last_seen_snapshot",
        "start_at",
        "end_at",
        "is_current",
        "observations",
    }
    assert set(tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS) == (
        set(tables.BR_COMPANY_RELATIONS_COLUMNS) - history_only
    )
    assert "resolved_at" in tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS
    assert tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS[0] == "country_iso2"


def test_export_column_tuples_are_the_shared_canonical_tuples() -> None:
    # Identity (not just equality) pin: clickhouse.py's export columns ARE the
    # shared dagster_v3.contact_extraction tuples, so the DuckDB stage order,
    # the ClickHouse export order, and the migration DDL order can never diverge.
    assert tables.BR_COMPANY_CONTACTS_EXPORT_COLUMNS is COMPANY_CONTACTS_COLUMNS
    assert tables.BR_COMPANY_DOMAINS_EXPORT_COLUMNS is COMPANY_DOMAINS_COLUMNS
