from __future__ import annotations

from typing import Literal

import duckdb

from dagster_v3.defs.gleif import tables
from dagster_v3.defs.gleif.dlt_csv import (
    GLEIF_DLT_RAW_DATASET_NAME,
    GLEIF_RAW_LEI_RECORDS_TABLE,
    GLEIF_RAW_RELATIONSHIPS_TABLE,
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
)
from dagster_v3.defs.gleif.duckdb_state import (
    DUCKDB_SCHEMA,
    DUCKDB_STAGING_SCHEMA,
    _ensure_all_tables,
    _ensure_empty_tables,
    _ensure_schema,
    _qualified_table,
    _quote,
    _replace_current_tables_from_schema,
    _row_counts,
    _upsert_current_tables_from_schema,
)


def replace_current_from_dlt_raw_tables(
    *,
    connection: duckdb.DuckDBPyConnection,
    catalog_name: str,
    load_mode: Literal["full", "delta"],
    publish_date: str,
    run_id: str,
) -> dict[str, int]:
    _ensure_required_raw_tables(connection)
    _ensure_schema(connection, catalog_name, schema_name=DUCKDB_SCHEMA)
    _ensure_schema(connection, catalog_name, schema_name=DUCKDB_STAGING_SCHEMA)
    if load_mode == "delta":
        _ensure_all_tables(
            connection,
            catalog_name=catalog_name,
            schema_name=DUCKDB_SCHEMA,
        )
    _ensure_empty_tables(
        connection,
        catalog_name=catalog_name,
        schema_name=DUCKDB_STAGING_SCHEMA,
    )
    _build_staging_tables(
        connection,
        catalog_name=catalog_name,
        publish_date=publish_date,
        run_id=run_id,
    )
    staged_counts = _staging_row_counts(connection, catalog_name=catalog_name)
    if load_mode == "full" and staged_counts.get(tables.GLEIF_LEI_RECORDS_TABLE, 0) == 0:
        raise ValueError(
            "GLEIF full normalization produced 0 lei_records rows; "
            "refusing to replace the current tables"
        )
    if load_mode == "full":
        _replace_current_tables_from_schema(
            connection,
            catalog_name=catalog_name,
            source_schema_name=DUCKDB_STAGING_SCHEMA,
        )
    else:
        _upsert_current_tables_from_schema(
            connection,
            catalog_name=catalog_name,
            source_schema_name=DUCKDB_STAGING_SCHEMA,
            source_row_counts=staged_counts,
        )
    return _row_counts(connection, catalog_name=catalog_name)


def _ensure_required_raw_tables(connection: duckdb.DuckDBPyConnection) -> None:
    required_tables = {
        GLEIF_RAW_LEI_RECORDS_TABLE,
        GLEIF_RAW_RELATIONSHIPS_TABLE,
        GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
    }
    existing_tables = {
        row[0]
        for row in connection.execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = ?
            """,
            [GLEIF_DLT_RAW_DATASET_NAME],
        ).fetchall()
    }
    missing_tables = sorted(required_tables - existing_tables)
    if missing_tables:
        raise ValueError(f"Missing GLEIF dlt raw tables: {missing_tables}")


def _build_staging_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    publish_date: str,
    run_id: str,
) -> None:
    _build_lei_records(
        connection,
        catalog_name=catalog_name,
        publish_date=publish_date,
        run_id=run_id,
    )
    _build_lei_names(
        connection,
        catalog_name=catalog_name,
        publish_date=publish_date,
        run_id=run_id,
    )
    _build_lei_addresses(
        connection,
        catalog_name=catalog_name,
        publish_date=publish_date,
        run_id=run_id,
    )
    _build_lei_identifiers(
        connection,
        catalog_name=catalog_name,
        publish_date=publish_date,
        run_id=run_id,
    )
    _build_relationships(
        connection,
        catalog_name=catalog_name,
        publish_date=publish_date,
        run_id=run_id,
    )
    _build_relationship_periods(
        connection,
        catalog_name=catalog_name,
        publish_date=publish_date,
        run_id=run_id,
    )
    _build_reporting_exceptions(
        connection,
        catalog_name=catalog_name,
        publish_date=publish_date,
        run_id=run_id,
    )


def _build_lei_records(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    publish_date: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {_staging_table(catalog_name, tables.GLEIF_LEI_RECORDS_TABLE)} as
        select
          coalesce(lei, '') as lei,
          coalesce(entity_legal_name, '') as legal_name,
          nullif(entity_legal_name_xmllang, '') as legal_name_language,
          coalesce(entity_entity_status, '') as entity_status,
          coalesce(registration_registration_status, '') as registration_status,
          nullif(entity_legal_jurisdiction, '') as jurisdiction,
          nullif(entity_entity_category, '') as category,
          nullif(entity_entity_sub_category, '') as subcategory,
          nullif(entity_legal_form_entity_legal_form_code, '') as legal_form_id,
          nullif(entity_legal_form_other_legal_form, '') as legal_form_other,
          nullif(entity_registration_authority_registration_authority_id, '') as registered_at_id,
          nullif(entity_registration_authority_other_registration_authority_id, '') as registered_at_other,
          nullif(entity_registration_authority_registration_authority_entity_id, '') as registered_as,
          null as associated_entity_lei,
          null as associated_entity_name,
          null as successor_entity_lei,
          null as successor_entity_name,
          try_cast(entity_entity_creation_date as timestamp) as creation_date,
          try_cast(entity_entity_expiration_date as timestamp) as expiration_date,
          nullif(entity_entity_expiration_reason, '') as expiration_reason,
          try_cast(registration_initial_registration_date as timestamp) as initial_registration_date,
          try_cast(registration_last_update_date as timestamp) as last_update_date,
          try_cast(registration_next_renewal_date as timestamp) as next_renewal_date,
          nullif(registration_managing_lou, '') as managing_lou,
          nullif(registration_validation_sources, '') as corroboration_level,
          nullif(registration_validation_authority_validation_authority_id, '') as validated_at_id,
          nullif(registration_validation_authority_other_validation_authority_id, '') as validated_at_other,
          nullif(registration_validation_authority_validation_authority_entity_id, '') as validated_as,
          nullif(conformity_flag, '') as conformity_flag,
          nullif(entity_legal_address_country, '') as legal_address_country,
          nullif(entity_headquarters_address_country, '') as headquarters_address_country,
          coalesce(
            nullif(entity_legal_address_country, ''),
            nullif(entity_headquarters_address_country, ''),
            nullif(entity_legal_jurisdiction, '')
          ) as primary_country_iso2,
          {_timestamp_literal(publish_date)} as golden_copy_publish_date,
          'gleif' as source_system,
          {_string_literal(run_id)} as source_run_id,
          {_timestamp_literal(publish_date)} as retrieved_at,
          {_timestamp_literal(publish_date)} as resolved_at
        from {_raw_table(GLEIF_RAW_LEI_RECORDS_TABLE)}
        """
    )


def _build_lei_names(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    publish_date: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {_staging_table(catalog_name, tables.GLEIF_LEI_NAMES_TABLE)} as
        with names as (
          select
            lei,
            'LEGAL_NAME' as name_type,
            entity_legal_name as name,
            entity_legal_name_xmllang as language,
            'Entity.LegalName' as cdf_type,
            0 as sequence
          from {_raw_table(GLEIF_RAW_LEI_RECORDS_TABLE)}
          where nullif(entity_legal_name, '') is not null
          union all
          select
            lei,
            'OTHER_ENTITY_NAME' as name_type,
            entity_other_entity_names_other_entity_name_1 as name,
            entity_other_entity_names_other_entity_name_1_xmllang as language,
            'Entity.OtherEntityNames.OtherEntityName' as cdf_type,
            1 as sequence
          from {_raw_table(GLEIF_RAW_LEI_RECORDS_TABLE)}
          where nullif(entity_other_entity_names_other_entity_name_1, '') is not null
        )
        select
          coalesce(lei, '') as lei,
          coalesce(name_type, '') as name_type,
          coalesce(name, '') as name,
          coalesce(lower(trim(name)), '') as name_normalized,
          nullif(language, '') as language,
          nullif(cdf_type, '') as cdf_type,
          sequence,
          'gleif' as source_system,
          {_string_literal(run_id)} as source_run_id,
          {_timestamp_literal(publish_date)} as retrieved_at,
          {_timestamp_literal(publish_date)} as resolved_at
        from names
        """
    )


def _build_lei_addresses(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    publish_date: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {_staging_table(catalog_name, tables.GLEIF_LEI_ADDRESSES_TABLE)} as
        with addresses as (
          select
            lei,
            'LEGAL_ADDRESS' as address_role,
            entity_legal_address_xmllang as language,
            entity_legal_address_first_address_line as first_address_line,
            entity_legal_address_city as city,
            entity_legal_address_region as region,
            entity_legal_address_country as country,
            entity_legal_address_postal_code as postal_code
          from {_raw_table(GLEIF_RAW_LEI_RECORDS_TABLE)}
          where coalesce(
            nullif(entity_legal_address_first_address_line, ''),
            nullif(entity_legal_address_city, ''),
            nullif(entity_legal_address_country, ''),
            nullif(entity_legal_address_postal_code, '')
          ) is not null
          union all
          select
            lei,
            'HEADQUARTERS_ADDRESS' as address_role,
            entity_headquarters_address_xmllang as language,
            entity_headquarters_address_first_address_line as first_address_line,
            entity_headquarters_address_city as city,
            entity_headquarters_address_region as region,
            entity_headquarters_address_country as country,
            entity_headquarters_address_postal_code as postal_code
          from {_raw_table(GLEIF_RAW_LEI_RECORDS_TABLE)}
          where coalesce(
            nullif(entity_headquarters_address_first_address_line, ''),
            nullif(entity_headquarters_address_city, ''),
            nullif(entity_headquarters_address_country, ''),
            nullif(entity_headquarters_address_postal_code, '')
          ) is not null
        )
        select
          coalesce(lei, '') as lei,
          coalesce(address_role, '') as address_role,
          nullif(language, '') as language,
          [coalesce(first_address_line, '')] as address_lines,
          null as address_number,
          null as address_number_within_building,
          null as mail_routing,
          nullif(city, '') as city,
          nullif(region, '') as region,
          nullif(country, '') as country,
          nullif(postal_code, '') as postal_code,
          nullif(concat_ws(', ', nullif(first_address_line, ''), nullif(city, ''), nullif(country, '')), '') as normalized_address,
          null::double as latitude,
          null::double as longitude,
          'gleif' as source_system,
          {_string_literal(run_id)} as source_run_id,
          {_timestamp_literal(publish_date)} as retrieved_at,
          {_timestamp_literal(publish_date)} as resolved_at
        from addresses
        """
    )


def _build_lei_identifiers(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    publish_date: str,
    run_id: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {_staging_table(catalog_name, tables.GLEIF_LEI_IDENTIFIERS_TABLE)} as
        select
          coalesce(lei, '') as lei,
          'REGISTRATION_AUTHORITY_ENTITY_ID' as identifier_type,
          coalesce(entity_registration_authority_registration_authority_entity_id, '') as identifier_value,
          nullif(entity_registration_authority_registration_authority_id, '') as identifier_scope,
          'gleif' as mapping_source,
          1 as is_primary,
          'gleif' as source_system,
          {_string_literal(run_id)} as source_run_id,
          {_timestamp_literal(publish_date)} as retrieved_at,
          {_timestamp_literal(publish_date)} as resolved_at
        from {_raw_table(GLEIF_RAW_LEI_RECORDS_TABLE)}
        where nullif(entity_registration_authority_registration_authority_entity_id, '') is not null
        """
    )


def _build_relationships(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    publish_date: str,
    run_id: str,
) -> None:
    relationship_id = (
        "coalesce(relationship_start_node_node_id, '') || ':' || "
        "coalesce(relationship_relationship_type, '') || ':' || "
        "coalesce(relationship_end_node_node_id, '')"
    )
    deleted_at_source = _raw_column_or_null(
        connection,
        table_name=GLEIF_RAW_RELATIONSHIPS_TABLE,
        column_name="deleted_at",
    )
    connection.execute(
        f"""
        create or replace table {_staging_table(catalog_name, tables.GLEIF_LEI_RELATIONSHIPS_TABLE)} as
        select
          {relationship_id} as relationship_record_id,
          coalesce(relationship_start_node_node_id, '') as start_node_lei,
          nullif(relationship_start_node_node_id_type, '') as start_node_type,
          coalesce(relationship_end_node_node_id, '') as end_node_lei,
          nullif(relationship_end_node_node_id_type, '') as end_node_type,
          coalesce(relationship_relationship_type, '') as relationship_type,
          coalesce(relationship_relationship_status, '') as relationship_status,
          null::timestamp as valid_from,
          null::timestamp as valid_to,
          try_cast(registration_initial_registration_date as timestamp) as initial_registration_date,
          try_cast(registration_last_update_date as timestamp) as last_update_date,
          nullif(registration_registration_status, '') as registration_status,
          try_cast(registration_next_renewal_date as timestamp) as next_renewal_date,
          nullif(registration_managing_lou, '') as managing_lou,
          nullif(registration_validation_sources, '') as corroboration_level,
          nullif(registration_validation_documents, '') as corroboration_documents,
          nullif(registration_validation_reference, '') as corroboration_reference,
          try_cast({deleted_at_source} as timestamp) as deleted_at,
          'gleif' as source_system,
          {_string_literal(run_id)} as source_run_id,
          {_timestamp_literal(publish_date)} as retrieved_at,
          {_timestamp_literal(publish_date)} as resolved_at
        from {_raw_table(GLEIF_RAW_RELATIONSHIPS_TABLE)}
        """
    )


def _build_relationship_periods(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    publish_date: str,
    run_id: str,
) -> None:
    relationship_id = (
        "coalesce(relationship_start_node_node_id, '') || ':' || "
        "coalesce(relationship_relationship_type, '') || ':' || "
        "coalesce(relationship_end_node_node_id, '')"
    )
    connection.execute(
        f"""
        create or replace table {_staging_table(catalog_name, tables.GLEIF_LEI_RELATIONSHIP_PERIODS_TABLE)} as
        select
          {relationship_id} as relationship_record_id,
          coalesce(relationship_period_1_period_type, '') as period_type,
          try_cast(relationship_period_1_start_date as date) as start_date,
          try_cast(relationship_period_1_end_date as date) as end_date,
          'gleif' as source_system,
          {_string_literal(run_id)} as source_run_id,
          {_timestamp_literal(publish_date)} as retrieved_at,
          {_timestamp_literal(publish_date)} as resolved_at
        from {_raw_table(GLEIF_RAW_RELATIONSHIPS_TABLE)}
        where coalesce(
          nullif(relationship_period_1_start_date, ''),
          nullif(relationship_period_1_end_date, ''),
          nullif(relationship_period_1_period_type, '')
        ) is not null
        """
    )


def _build_reporting_exceptions(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    publish_date: str,
    run_id: str,
) -> None:
    exception_category = _raw_column_or_null(
        connection,
        table_name=GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
        column_name="exception_category",
    )
    exception_reason = _raw_column_or_null(
        connection,
        table_name=GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
        column_name="exception_reason_1",
    )
    exception_reference = _raw_column_or_null(
        connection,
        table_name=GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
        column_name="exception_reference_1",
    )
    initial_registration_date = _raw_column_or_null(
        connection,
        table_name=GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
        column_name="registration_initial_registration_date",
    )
    last_update_date = _raw_column_or_null(
        connection,
        table_name=GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
        column_name="registration_last_update_date",
    )
    registration_status = _raw_column_or_null(
        connection,
        table_name=GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
        column_name="registration_registration_status",
    )
    next_renewal_date = _raw_column_or_null(
        connection,
        table_name=GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
        column_name="registration_next_renewal_date",
    )
    managing_lou = _raw_column_or_null(
        connection,
        table_name=GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
        column_name="registration_managing_lou",
    )
    exception_id = f"sha256(coalesce(lei, '') || ':' || coalesce({exception_category}, ''))"
    connection.execute(
        f"""
        create or replace table {_staging_table(catalog_name, tables.GLEIF_LEI_REPORTING_EXCEPTIONS_TABLE)} as
        select
          {exception_id} as exception_record_id,
          coalesce(lei, '') as lei,
          coalesce({exception_category}, '') as parent_relationship_type,
          coalesce({exception_category}, '') as exception_category,
          nullif({exception_reason}, '') as exception_reason,
          nullif({exception_reference}, '') as exception_reference,
          try_cast({initial_registration_date} as timestamp) as initial_registration_date,
          try_cast({last_update_date} as timestamp) as last_update_date,
          nullif({registration_status}, '') as registration_status,
          try_cast({next_renewal_date} as timestamp) as next_renewal_date,
          nullif({managing_lou}, '') as managing_lou,
          'gleif' as source_system,
          {_string_literal(run_id)} as source_run_id,
          {_timestamp_literal(publish_date)} as retrieved_at,
          {_timestamp_literal(publish_date)} as resolved_at
        from {_raw_table(GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE)}
        """
    )


def _staging_row_counts(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
) -> dict[str, int]:
    return {
        table_name: int(
            connection.execute(
                f"select count(*) from {_staging_table(catalog_name, table_name)}"
            ).fetchone()[0]
        )
        for table_name in tables.GLEIF_TABLES
    }


def _raw_table(table_name: str) -> str:
    return f"{_quote(GLEIF_DLT_RAW_DATASET_NAME)}.{_quote(table_name)}"


def _raw_column_or_null(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    column_name: str,
) -> str:
    row = connection.execute(
        """
        select 1
        from information_schema.columns
        where table_schema = ?
          and table_name = ?
          and column_name = ?
        limit 1
        """,
        [GLEIF_DLT_RAW_DATASET_NAME, table_name, column_name],
    ).fetchone()
    if row is None:
        return "null"
    return _quote(column_name)


def _staging_table(catalog_name: str, table_name: str) -> str:
    return _qualified_table(
        table_name,
        catalog_name=catalog_name,
        schema_name=DUCKDB_STAGING_SCHEMA,
    )


def _string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _timestamp_literal(value: str) -> str:
    return f"try_cast({_string_literal(value)} as timestamp)"
