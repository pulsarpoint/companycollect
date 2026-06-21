from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.gleif.csv_transforms import replace_current_from_dlt_raw_tables
from dagster_v3.defs.gleif.dlt_csv import (
    GLEIF_DLT_RAW_DATASET_NAME,
    GLEIF_RAW_LEI_RECORDS_TABLE,
    GLEIF_RAW_RELATIONSHIPS_TABLE,
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
)


def test_replace_current_from_dlt_raw_tables_builds_normalized_tables(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "gleif_reference.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        seed_raw_tables(connection)

    row_counts = replace_current_from_dlt_raw_tables(
        database_path=database_path,
        load_mode="full",
        publish_date="2026-06-20T16:00:00+00:00",
        run_id="run-1",
    )

    assert row_counts["gleif_lei_records"] == 1
    assert row_counts["gleif_lei_names"] == 2
    assert row_counts["gleif_lei_addresses"] == 2
    assert row_counts["gleif_lei_identifiers"] == 1
    assert row_counts["gleif_lei_relationships"] == 1
    assert row_counts["gleif_lei_relationship_periods"] == 1
    assert row_counts["gleif_lei_reporting_exceptions"] == 1

    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert connection.execute(
            """
            select legal_name, entity_status, registration_status, source_run_id
            from gleif_reference.gleif.gleif_lei_records
            """
        ).fetchall() == [("ACME PLC", "ACTIVE", "ISSUED", "run-1")]
        assert connection.execute(
            """
            select name_type, name, language, sequence
            from gleif_reference.gleif.gleif_lei_names
            order by sequence
            """
        ).fetchall() == [
            ("LEGAL_NAME", "ACME PLC", "en", 0),
            ("OTHER_ENTITY_NAME", "ACME LIMITED", "en", 1),
        ]


def test_full_replace_refuses_empty_lei_records(tmp_path: Path) -> None:
    database_path = tmp_path / "gleif_reference.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        seed_raw_tables(connection, include_lei_row=False)

    with pytest.raises(ValueError, match="0 lei_records"):
        replace_current_from_dlt_raw_tables(
            database_path=database_path,
            load_mode="full",
            publish_date="2026-06-20T16:00:00+00:00",
            run_id="run-empty",
        )


def test_non_nullable_strings_are_coalesced_for_clickhouse(tmp_path: Path) -> None:
    database_path = tmp_path / "gleif_reference.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        seed_raw_tables(
            connection,
            legal_name=None,
            entity_status=None,
            registration_status=None,
        )

    replace_current_from_dlt_raw_tables(
        database_path=database_path,
        load_mode="full",
        publish_date="2026-06-20T16:00:00+00:00",
        run_id="run-null",
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert connection.execute(
            """
            select legal_name, entity_status, registration_status
            from gleif_reference.gleif.gleif_lei_records
            """
        ).fetchall() == [("", "", "")]


def test_relationship_transform_tolerates_missing_deleted_at_column(tmp_path: Path) -> None:
    database_path = tmp_path / "gleif_reference.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        seed_raw_tables(connection, include_relationship_deleted_at=False)

    replace_current_from_dlt_raw_tables(
        database_path=database_path,
        load_mode="full",
        publish_date="2026-06-20T16:00:00+00:00",
        run_id="run-without-deleted-at",
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert connection.execute(
            """
            select deleted_at
            from gleif_reference.gleif.gleif_lei_relationships
            """
        ).fetchall() == [(None,)]


def seed_raw_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    include_lei_row: bool = True,
    legal_name: str | None = "ACME PLC",
    entity_status: str | None = "ACTIVE",
    registration_status: str | None = "ISSUED",
    include_relationship_deleted_at: bool = True,
) -> None:
    connection.execute(f"create schema if not exists {GLEIF_DLT_RAW_DATASET_NAME}")
    connection.execute(f"drop table if exists {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_LEI_RECORDS_TABLE}")
    connection.execute(f"drop table if exists {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_RELATIONSHIPS_TABLE}")
    connection.execute(
        f"drop table if exists {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE}"
    )
    connection.execute(
        f"""
        create table {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_LEI_RECORDS_TABLE} (
          lei varchar,
          entity_legal_name varchar,
          entity_legal_name_xmllang varchar,
          entity_entity_status varchar,
          entity_legal_jurisdiction varchar,
          entity_entity_category varchar,
          entity_entity_sub_category varchar,
          entity_legal_form_entity_legal_form_code varchar,
          entity_legal_form_other_legal_form varchar,
          entity_registration_authority_registration_authority_id varchar,
          entity_registration_authority_other_registration_authority_id varchar,
          entity_registration_authority_registration_authority_entity_id varchar,
          entity_entity_creation_date varchar,
          entity_entity_expiration_date varchar,
          entity_entity_expiration_reason varchar,
          registration_initial_registration_date varchar,
          registration_last_update_date varchar,
          registration_registration_status varchar,
          registration_next_renewal_date varchar,
          registration_managing_lou varchar,
          registration_validation_sources varchar,
          registration_validation_authority_validation_authority_id varchar,
          registration_validation_authority_other_validation_authority_id varchar,
          registration_validation_authority_validation_authority_entity_id varchar,
          conformity_flag varchar,
          entity_legal_address_xmllang varchar,
          entity_legal_address_first_address_line varchar,
          entity_legal_address_city varchar,
          entity_legal_address_region varchar,
          entity_legal_address_country varchar,
          entity_legal_address_postal_code varchar,
          entity_headquarters_address_xmllang varchar,
          entity_headquarters_address_first_address_line varchar,
          entity_headquarters_address_city varchar,
          entity_headquarters_address_region varchar,
          entity_headquarters_address_country varchar,
          entity_headquarters_address_postal_code varchar,
          entity_other_entity_names_other_entity_name_1 varchar,
          entity_other_entity_names_other_entity_name_1_xmllang varchar
        )
        """
    )
    if include_lei_row:
        connection.execute(
            f"""
            insert into {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_LEI_RECORDS_TABLE} values (
              '5493001KJTIIGC8Y1R12',
              ?,
              'en',
              ?,
              'GB',
              'GENERAL',
              null,
              'H0PO',
              null,
              'RA000585',
              null,
              '123456',
              '2020-01-01T00:00:00Z',
              null,
              null,
              '2020-01-02T00:00:00Z',
              '2026-06-20T00:00:00Z',
              ?,
              '2027-06-20T00:00:00Z',
              '213800WAVVOPS85N2205',
              'FULLY_CORROBORATED',
              null,
              null,
              null,
              'CONFORMING',
              'en',
              '1 Market Street',
              'London',
              null,
              'GB',
              'EC1A 1AA',
              'en',
              '2 HQ Street',
              'London',
              null,
              'GB',
              'EC1A 2BB',
              'ACME LIMITED',
              'en'
            )
            """,
            [legal_name, entity_status, registration_status],
        )
    connection.execute(
        f"""
        create table {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_RELATIONSHIPS_TABLE} (
          relationship_start_node_node_id varchar,
          relationship_start_node_node_id_type varchar,
          relationship_end_node_node_id varchar,
          relationship_end_node_node_id_type varchar,
          relationship_relationship_type varchar,
          relationship_relationship_status varchar,
          relationship_period_1_start_date varchar,
          relationship_period_1_end_date varchar,
          relationship_period_1_period_type varchar,
          registration_initial_registration_date varchar,
          registration_last_update_date varchar,
          registration_registration_status varchar,
          registration_next_renewal_date varchar,
          registration_managing_lou varchar,
          registration_validation_sources varchar,
          registration_validation_documents varchar,
          registration_validation_reference varchar
          {", deleted_at varchar" if include_relationship_deleted_at else ""}
        )
        """
    )
    connection.execute(
        f"""
        insert into {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_RELATIONSHIPS_TABLE} values (
          '5493001KJTIIGC8Y1R12',
          'LEI',
          '54930084UKLVMY22DS16',
          'LEI',
          'IS_DIRECTLY_CONSOLIDATED_BY',
          'ACTIVE',
          '2020-01-01',
          null,
          'ACCOUNTING_PERIOD',
          '2020-01-02T00:00:00Z',
          '2026-06-20T00:00:00Z',
          'PUBLISHED',
          '2027-06-20T00:00:00Z',
          '213800WAVVOPS85N2205',
          'FULLY_CORROBORATED',
          'SUPPORTING_DOCUMENTS',
          'annual-report'
          {", null" if include_relationship_deleted_at else ""}
        )
        """
    )
    connection.execute(
        f"""
        create table {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE} (
          lei varchar,
          exception_category varchar,
          exception_reason_1 varchar,
          exception_reference_1 varchar,
          registration_initial_registration_date varchar,
          registration_last_update_date varchar,
          registration_registration_status varchar,
          registration_next_renewal_date varchar,
          registration_managing_lou varchar,
          deleted_at varchar
        )
        """
    )
    connection.execute(
        f"""
        insert into {GLEIF_DLT_RAW_DATASET_NAME}.{GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE} values (
          '5493001KJTIIGC8Y1R12',
          'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT',
          'NO_KNOWN_PERSON',
          'not-disclosed',
          '2020-01-02T00:00:00Z',
          '2026-06-20T00:00:00Z',
          'PUBLISHED',
          '2027-06-20T00:00:00Z',
          '213800WAVVOPS85N2205',
          null
        )
        """
    )
