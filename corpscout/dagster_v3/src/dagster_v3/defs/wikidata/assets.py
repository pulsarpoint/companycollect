import json
import time
from collections.abc import Callable
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import dlt as dlt_lib
import duckdb
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    replace_duckdb_connection_tables_in_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.wikidata.source import (
    WIKIDATA_DUCKDB_DATASET_NAME,
    WIKIDATA_DUCKDB_PIPELINE_NAME,
    WIKIDATA_LISTED_COMPANIES_TABLE,
    WIKIDATA_RAW_BUCKET,
    WikidataListedCompaniesConfig,
    WikidataRawPullConfig,
    WikidataSparqlClient,
    active_exchanges_object_key,
    active_listed_exchange_row_from_binding,
    augmentation_object_key,
    binding_value,
    build_active_listed_exchanges_query,
    build_company_identifier_augmentation_query,
    build_company_profile_augmentation_query,
    build_company_relationship_augmentation_query,
    build_listed_company_query,
    manifest_object_key,
    page_object_key,
    query_hash,
    response_bindings,
    wikidata_id_from_url,
    wikidata_listed_companies_source,
)
from dagster_v3.defs.wikidata import tables

GROUP_NAME = "wikidata"
WIKIDATA_DUCKDB_SCHEMA = "wikidata"
WIKIDATA_DUCKDB_PATH = Path("data/wikidata.duckdb")
WIKIDATA_DUCKDB_POOL = "wikidata_duckdb"

WIKIDATA_DUCKDB_FINAL_TABLE_ASSET_KEYS = tuple(
    dg.AssetKey(table_name) for table_name in tables.WIKIDATA_TABLES
)


class WikidataDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name != WIKIDATA_LISTED_COMPANIES_TABLE:
            return spec
        return spec.replace_attributes(
            key="wikidata_listed_companies_duckdb",
            deps=[dg.AssetKey("wikidata_company_seed_raw_objects")],
            group_name=GROUP_NAME,
            description=(
                "Active Wikidata listed-company rows parsed from raw S3 pages and loaded to DuckDB with dlt."
            ),
            kinds={"python", "dlt", "duckdb", "wikidata"},
        )


def wikidata_listed_companies_pipeline(database_path: str | Path) -> Any:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    working_dir = database_file.parent / ".dlt"
    working_dir.mkdir(parents=True, exist_ok=True)
    return dlt_lib.pipeline(
        pipeline_name=WIKIDATA_DUCKDB_PIPELINE_NAME,
        destination=dlt_lib.destinations.duckdb(str(database_file)),
        dataset_name=WIKIDATA_DUCKDB_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(working_dir),
    )


@dlt_assets(
    dlt_source=wikidata_listed_companies_source(max_exchanges=1, max_pages=1),
    dlt_pipeline=wikidata_listed_companies_pipeline(WIKIDATA_DUCKDB_PATH),
    name="wikidata_listed_companies_duckdb",
    dagster_dlt_translator=WikidataDltTranslator(),
    pool=WIKIDATA_DUCKDB_POOL,
)
def wikidata_listed_companies_duckdb_asset(
    context: dg.AssetExecutionContext,
    config: WikidataListedCompaniesConfig,
    dlt: DagsterDltResource,
    object_store: ObjectStoreResource,
) -> Iterator[Any]:
    raw_run_id = config.raw_run_id or context.run.run_id
    yield from dlt.run(
        context=context,
        dlt_source=wikidata_listed_companies_source(
            object_store=object_store,
            raw_run_id=raw_run_id,
            max_pages=config.max_pages,
            max_exchanges=config.max_exchanges,
            source_run_id=raw_run_id,
        ),
        dlt_pipeline=wikidata_listed_companies_pipeline(WIKIDATA_DUCKDB_PATH),
    )


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            table_name,
            deps=[dg.AssetKey("wikidata_listed_companies_duckdb")],
            group_name=GROUP_NAME,
            kinds={"python", "duckdb", "wikidata"},
            description=f"Normalized Wikidata DuckDB table for `{table_name}`.",
        )
        for table_name in tables.WIKIDATA_TABLES
    ],
    pool=WIKIDATA_DUCKDB_POOL,
)
def wikidata_company_seed_duckdb_tables(
    wikidata_duckdb: DuckDBResource,
) -> Iterator[dg.MaterializeResult]:
    with wikidata_duckdb.get_connection() as connection:
        row_counts = normalize_wikidata_listed_companies_duckdb(
            connection,
            catalog_name=WIKIDATA_DUCKDB_PATH.stem,
        )
    for table_name in tables.WIKIDATA_TABLES:
        yield dg.MaterializeResult(
            asset_key=table_name,
            metadata={
                "duckdb_path": str(WIKIDATA_DUCKDB_PATH),
                "duckdb_schema": WIKIDATA_DUCKDB_SCHEMA,
                "duckdb_table": table_name,
                "row_count": row_counts[table_name],
            },
        )


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "wikidata", "s3"},
    description="Pulls paged raw Wikidata listed-company SPARQL responses into object storage.",
)
def wikidata_company_seed_raw_objects(
    context: dg.AssetExecutionContext,
    config: WikidataRawPullConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    client = WikidataSparqlClient(timeout_seconds=config.request_timeout_seconds)
    return pull_wikidata_company_seed_raw_objects(
        client=client,
        object_store=object_store,
        config=config,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC).isoformat(),
        sleep=time.sleep,
    )


@dg.asset(
    deps=WIKIDATA_DUCKDB_FINAL_TABLE_ASSET_KEYS,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=WIKIDATA_DUCKDB_POOL,
    description="Exports normalized Wikidata company seed DuckDB tables to migrated ClickHouse tables.",
)
def wikidata_company_seed_clickhouse(
    clickhouse: ClickhouseResource,
    wikidata_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    return _export_wikidata_tables_to_clickhouse(clickhouse, wikidata_duckdb)


def _export_wikidata_tables_to_clickhouse(
    clickhouse: ClickhouseResource,
    wikidata_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.WIKIDATA_TABLES,
    )
    with wikidata_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as client:
            row_counts = replace_duckdb_connection_tables_in_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=client,
                duckdb_schema=_duckdb_schema_qualifier(
                    WIKIDATA_DUCKDB_PATH,
                    WIKIDATA_DUCKDB_SCHEMA,
                ),
                clickhouse_database=RESOLVED_DATABASE,
                tables=tuple(
                    (table_name, tables.WIKIDATA_TABLE_COLUMNS[table_name])
                    for table_name in tables.WIKIDATA_TABLES
                ),
            )
    return dg.MaterializeResult(
        metadata={f"{table_name}_row_count": count for table_name, count in row_counts.items()}
    )


def normalize_wikidata_listed_companies_duckdb(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
) -> dict[str, int]:
    source_table = _duckdb_qualified_table_name(
        catalog_name,
        WIKIDATA_DUCKDB_DATASET_NAME,
        WIKIDATA_LISTED_COMPANIES_TABLE,
    )
    target_schema = _duckdb_qualified_schema_name(catalog_name, WIKIDATA_DUCKDB_SCHEMA)
    _assert_wikidata_listed_companies_table_exists(connection, source_table=source_table)
    _ensure_optional_wikidata_source_columns(connection, source_table=source_table)
    connection.execute(f"create schema if not exists {target_schema}")
    _create_wikidata_companies_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_company_listings_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_company_identifiers_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_company_websites_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_company_relationships_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    _create_wikidata_seed_extraction_runs_table(
        connection,
        source_table=source_table,
        target_schema=target_schema,
    )
    return {
        table_name: _duckdb_table_count(
            connection,
            _duckdb_qualified_table_name(
                catalog_name,
                WIKIDATA_DUCKDB_SCHEMA,
                table_name,
            ),
        )
        for table_name in tables.WIKIDATA_TABLES
    }


def _assert_wikidata_listed_companies_table_exists(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
) -> None:
    try:
        connection.execute(f"select 1 from {source_table} limit 0")
    except Exception as exc:
        raise ValueError(
            "Missing DuckDB source table "
            f"{WIKIDATA_DUCKDB_DATASET_NAME}.{WIKIDATA_LISTED_COMPANIES_TABLE}; "
            "materialize wikidata_listed_companies_duckdb first"
        ) from exc


def _create_wikidata_companies_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANIES_TABLE} as
        with normalized as (
            select
                *,
                try_cast(
                    nullif(regexp_extract(inception_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0), '')
                    as date
                ) as inception_date_value,
                try_cast(nullif(employee_count, '') as ubigint) as employee_count_value,
                try_cast(
                    nullif(
                        regexp_extract(employee_count_point_in_time, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as employee_count_point_in_time_value
            from {source_table}
        )
        select
            company_wikidata_id as wikidata_id,
            max(company_url) as wikidata_url,
            coalesce(nullif(max(company_label), ''), company_wikidata_id) as name,
            lower(coalesce(nullif(max(company_label), ''), company_wikidata_id)) as name_normalized,
            nullif(max(company_description), '') as company_description,
            nullif(max(official_name), '') as official_name,
            nullif(max(headquarters_wikidata_id), '') as headquarters_wikidata_id,
            nullif(max(headquarters_label), '') as headquarters_label,
            nullif(max(headquarters_country_wikidata_id), '') as headquarters_country_wikidata_id,
            nullif(max(headquarters_country_label), '') as headquarters_country_label,
            upper(nullif(max(headquarters_country_iso2), '')) as headquarters_country_iso2,
            case
                when nullif(max(headquarters_country_iso2), '') is not null
                    then 'wikidata_headquarters_p131_p17'
                else null
            end as country_resolution_method,
            case
                when nullif(max(headquarters_country_iso2), '') is not null
                    then 'high'
                else null
            end as country_resolution_confidence,
            max(inception_date_value) as inception_date,
            nullif(max(legal_form_wikidata_id), '') as legal_form_wikidata_id,
            nullif(max(legal_form_label), '') as legal_form_label,
            arg_max(
                employee_count_value,
                coalesce(employee_count_point_in_time_value, date '1900-01-01')
            ) as employee_count,
            max(employee_count_point_in_time_value) as employee_count_point_in_time,
            nullif(max(logo_image), '') as logo_image,
            nullif(max(logo_image_url), '') as logo_image_url,
            nullif(max(industry_wikidata_id), '') as industry_wikidata_id,
            nullif(max(industry_label), '') as industry_label,
            cast(1 as integer) as has_current_listing,
            count(distinct nullif(listing_statement_id, '')) as listing_count,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            company_wikidata_id as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from normalized
        where nullif(company_wikidata_id, '') is not null
        group by company_wikidata_id
        """
    )


def _ensure_optional_wikidata_source_columns(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
) -> None:
    for column_name in (
        "company_description",
        "headquarters_wikidata_id",
        "headquarters_country_wikidata_id",
        "headquarters_country_label",
        "headquarters_country_iso2",
        "inception_date",
        "legal_form_wikidata_id",
        "legal_form_label",
        "employee_count",
        "employee_count_point_in_time",
        "logo_image",
        "logo_image_url",
        "industry_wikidata_id",
        "opencorporates_company_id",
        "eu_vat_number",
        "duns_number",
        "permid",
        "bloomberg_company_id",
        "linkedin_company_id",
        "parent_organization_statement_id",
        "parent_organization_wikidata_id",
        "parent_organization_label",
        "parent_organization_start_date",
        "parent_organization_end_date",
        "child_organization_statement_id",
        "child_organization_wikidata_id",
        "child_organization_label",
        "child_organization_start_date",
        "child_organization_end_date",
        "owned_by_statement_id",
        "owned_by_wikidata_id",
        "owned_by_label",
        "owned_by_start_date",
        "owned_by_end_date",
        "owner_of_statement_id",
        "owner_of_wikidata_id",
        "owner_of_label",
        "owner_of_start_date",
        "owner_of_end_date",
    ):
        connection.execute(
            f"alter table {source_table} add column if not exists {column_name} varchar"
        )


def _create_wikidata_company_listings_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANY_LISTINGS_TABLE} as
        select
            company_wikidata_id as wikidata_id,
            listing_statement_id,
            exchange_wikidata_id,
            max(exchange_name) as exchange_name,
            nullif(max(ticker), '') as ticker,
            nullif(max(isin), '') as isin,
            cast(1 as integer) as is_current,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            max(source_record_id) as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from {source_table}
        where nullif(company_wikidata_id, '') is not null
          and nullif(listing_statement_id, '') is not null
          and nullif(exchange_wikidata_id, '') is not null
        group by company_wikidata_id, listing_statement_id, exchange_wikidata_id
        """
    )


def _create_wikidata_company_identifiers_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANY_IDENTIFIERS_TABLE} as
        with identifiers as (
            select
                company_wikidata_id as wikidata_id,
                'cik' as identifier_type,
                'P5531' as wikidata_property_id,
                cik as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(cik, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'lei' as identifier_type,
                'P1278' as wikidata_property_id,
                lei as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(lei, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'isin' as identifier_type,
                'P946' as wikidata_property_id,
                isin as identifier_value,
                exchange_wikidata_id as identifier_scope,
                cast(0 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(isin, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'opencorporates_company_id' as identifier_type,
                'P1320' as wikidata_property_id,
                opencorporates_company_id as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(opencorporates_company_id, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'eu_vat_number' as identifier_type,
                'P3608' as wikidata_property_id,
                eu_vat_number as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(eu_vat_number, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'duns_number' as identifier_type,
                'P2771' as wikidata_property_id,
                duns_number as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(duns_number, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'permid' as identifier_type,
                'P3347' as wikidata_property_id,
                permid as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(permid, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'bloomberg_company_id' as identifier_type,
                'P3377' as wikidata_property_id,
                bloomberg_company_id as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(bloomberg_company_id, '') is not null

            union all

            select
                company_wikidata_id as wikidata_id,
                'linkedin_company_id' as identifier_type,
                'P4264' as wikidata_property_id,
                linkedin_company_id as identifier_value,
                cast(null as varchar) as identifier_scope,
                cast(1 as integer) as is_primary,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(linkedin_company_id, '') is not null
        )
        select
            wikidata_id,
            identifier_type,
            wikidata_property_id,
            identifier_value,
            identifier_scope,
            max(is_primary) as is_primary,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            wikidata_id || ':' || wikidata_property_id || ':' || identifier_value as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from identifiers
        group by wikidata_id, identifier_type, wikidata_property_id, identifier_value, identifier_scope
        """
    )


def _create_wikidata_company_websites_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    # Internal stage: the domain graph no longer reads wikidata_company_websites
    # directly. Consumed only by wikidata/contacts.py's canonical derivation
    # (wikidata_company_contacts / wikidata_company_domains). Do not build new
    # consumers on it.
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANY_WEBSITES_TABLE} as
        with normalized as (
            select
                company_wikidata_id as wikidata_id,
                website_url,
                lower(trim(website_url)) as website_normalized_url,
                regexp_replace(
                    regexp_replace(
                        regexp_replace(lower(trim(website_url)), '^https?://', ''),
                        '^www\\.',
                        ''
                    ),
                    '/.*$',
                    ''
                ) as website_host,
                source_run_id,
                source_payload_hash,
                retrieved_at
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(website_url, '') is not null
        )
        select
            wikidata_id,
            website_url,
            website_normalized_url,
            website_host,
            website_host as root_domain,
            cast(null as varchar) as website_path,
            'official' as website_kind,
            'wikidata' as confidence,
            'unverified' as validation_status,
            cast(1 as integer) as is_primary_candidate,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            wikidata_id || ':website:' || website_normalized_url as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from normalized
        group by wikidata_id, website_url, website_normalized_url, website_host
        """
    )


def _create_wikidata_company_relationships_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_COMPANY_RELATIONSHIPS_TABLE} as
        with relationships as (
            select
                company_wikidata_id as subject_wikidata_id,
                parent_organization_wikidata_id as object_wikidata_id,
                'parent_organization' as relationship_type,
                'P749' as wikidata_property_id,
                parent_organization_statement_id as relationship_statement_id,
                nullif(parent_organization_label, '') as object_name,
                try_cast(
                    nullif(
                        regexp_extract(parent_organization_start_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as start_date,
                try_cast(
                    nullif(
                        regexp_extract(parent_organization_end_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as end_date,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(parent_organization_wikidata_id, '') is not null

            union all

            select
                company_wikidata_id as subject_wikidata_id,
                child_organization_wikidata_id as object_wikidata_id,
                'child_organization' as relationship_type,
                'P355' as wikidata_property_id,
                child_organization_statement_id as relationship_statement_id,
                nullif(child_organization_label, '') as object_name,
                try_cast(
                    nullif(
                        regexp_extract(child_organization_start_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as start_date,
                try_cast(
                    nullif(
                        regexp_extract(child_organization_end_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as end_date,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(child_organization_wikidata_id, '') is not null

            union all

            select
                company_wikidata_id as subject_wikidata_id,
                owned_by_wikidata_id as object_wikidata_id,
                'owned_by' as relationship_type,
                'P127' as wikidata_property_id,
                owned_by_statement_id as relationship_statement_id,
                nullif(owned_by_label, '') as object_name,
                try_cast(
                    nullif(
                        regexp_extract(owned_by_start_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as start_date,
                try_cast(
                    nullif(
                        regexp_extract(owned_by_end_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as end_date,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(owned_by_wikidata_id, '') is not null

            union all

            select
                company_wikidata_id as subject_wikidata_id,
                owner_of_wikidata_id as object_wikidata_id,
                'owner_of' as relationship_type,
                'P1830' as wikidata_property_id,
                owner_of_statement_id as relationship_statement_id,
                nullif(owner_of_label, '') as object_name,
                try_cast(
                    nullif(
                        regexp_extract(owner_of_start_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as start_date,
                try_cast(
                    nullif(
                        regexp_extract(owner_of_end_date, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}', 0),
                        ''
                    )
                    as date
                ) as end_date,
                source_run_id,
                retrieved_at,
                source_payload_hash
            from {source_table}
            where nullif(company_wikidata_id, '') is not null
              and nullif(owner_of_wikidata_id, '') is not null
        )
        select
            subject_wikidata_id,
            object_wikidata_id,
            relationship_type,
            wikidata_property_id,
            nullif(max(relationship_statement_id), '') as relationship_statement_id,
            max(object_name) as object_name,
            min(start_date) as start_date,
            max(end_date) as end_date,
            case when max(end_date) is null then 1 else 0 end as is_current,
            'wikidata' as source_system,
            max(source_run_id) as source_run_id,
            subject_wikidata_id || ':' || wikidata_property_id || ':' || object_wikidata_id
                as source_record_id,
            max(source_payload_hash) as source_payload_hash,
            cast(max(retrieved_at) as timestamp) as retrieved_at,
            cast(current_timestamp as timestamp) as resolved_at
        from relationships
        group by subject_wikidata_id, object_wikidata_id, relationship_type, wikidata_property_id
        """
    )


def _create_wikidata_seed_extraction_runs_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    target_schema: str,
) -> None:
    connection.execute(
        f"""
        create or replace table {target_schema}.{tables.WIKIDATA_SEED_EXTRACTION_RUNS_TABLE} as
        select
            source_run_id,
            'active_exchange_listing' as query_mode,
            cast(null as varchar) as query_exchange_id,
            max(query_hash) as query_hash,
            count(*) as row_count,
            count(distinct company_wikidata_id) as distinct_company_count,
            count(distinct listing_statement_id) as distinct_listing_count,
            count(*) filter (where nullif(website_url, '') is not null) as companies_with_website_count,
            count(*) filter (where nullif(cik, '') is not null) as companies_with_cik_count,
            count(*) filter (where nullif(lei, '') is not null) as companies_with_lei_count,
            cast(min(retrieved_at) as timestamp) as started_at,
            cast(max(retrieved_at) as timestamp) as completed_at,
            'wikidata' as source_system
        from {source_table}
        group by source_run_id
        """
    )


def _duckdb_table_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(connection.execute(f"select count(*) from {table_name}").fetchone()[0])


def _duckdb_qualified_schema_name(database_path: str | Path, schema_name: str) -> str:
    return _duckdb_qualified_name(_duckdb_catalog_name(database_path), schema_name)


def _duckdb_schema_qualifier(database_path: str | Path, schema_name: str) -> str:
    return f"{_duckdb_catalog_name(database_path)}.{schema_name}"


def _duckdb_qualified_table_name(
    database_path: str | Path,
    schema_name: str,
    table_name: str,
) -> str:
    return _duckdb_qualified_name(_duckdb_catalog_name(database_path), schema_name, table_name)


def _duckdb_catalog_name(database_path: str | Path) -> str:
    return Path(database_path).stem


def _duckdb_qualified_name(*parts: str) -> str:
    return ".".join(_quote_duckdb_identifier(part) for part in parts)


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def pull_wikidata_company_seed_raw_objects(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    run_id: str,
    retrieved_at: str,
    sleep: Callable[[float], None],
) -> dg.MaterializeResult:
    object_store.ensure_bucket(WIKIDATA_RAW_BUCKET)
    retrieved_date = retrieved_at[:10]
    exchange_rows, active_exchanges_key = wikidata_raw_pull_exchange_rows(
        client=client,
        object_store=object_store,
        config=config,
        run_id=run_id,
        retrieved_date=retrieved_date,
        retrieved_at=retrieved_at,
    )
    total_row_count = 0
    total_page_count = 0
    manifest_keys: list[str] = []

    for exchange_index, exchange_row in enumerate(exchange_rows):
        exchange_id = exchange_row["exchange_wikidata_id"]
        exchange_row_count = 0
        exchange_augmentation_row_count = 0
        page_count = 0
        object_keys: list[str] = []
        augmentation_object_keys: list[str] = []
        first_query = build_listed_company_query(
            exchange_id=exchange_id,
            limit=config.page_size,
            offset=0,
        )
        current_query_hash = query_hash(first_query)

        while config.max_pages is None or page_count < config.max_pages:
            offset = page_count * config.page_size
            query = build_listed_company_query(
                exchange_id=exchange_id,
                limit=config.page_size,
                offset=offset,
            )
            payload = client.fetch(query, user_agent=config.user_agent)
            bindings = response_bindings(payload)
            if not bindings:
                break

            page_count += 1
            exchange_row_count += len(bindings)
            object_key = page_object_key(
                retrieved_date=retrieved_date,
                run_id=run_id,
                exchange_id=exchange_id,
                page_number=page_count,
            )
            object_store.write_json(
                object_key,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                bucket=WIKIDATA_RAW_BUCKET,
            )
            object_keys.append(object_key)
            page_augmentation_object_keys, page_augmentation_row_count = (
                pull_wikidata_company_augmentation_raw_objects_for_page(
                    client=client,
                    object_store=object_store,
                    config=config,
                    run_id=run_id,
                    retrieved_date=retrieved_date,
                    exchange_id=exchange_id,
                    page_number=page_count,
                    listed_company_bindings=bindings,
                    sleep=sleep,
                )
            )
            augmentation_object_keys.extend(page_augmentation_object_keys)
            exchange_augmentation_row_count += page_augmentation_row_count

            if len(bindings) < config.page_size:
                break
            if config.request_delay_seconds > 0:
                sleep(config.request_delay_seconds)

        manifest_key = manifest_object_key(
            retrieved_date=retrieved_date,
            run_id=run_id,
            exchange_id=exchange_id,
        )
        manifest = {
            "source": "wikidata",
            "query_mode": "exchange",
            "run_id": run_id,
            "retrieved_date": retrieved_date,
            "exchange_id": exchange_id,
            "exchange_name": exchange_row["exchange_name"],
            "listed_company_count_on_exchange": exchange_row[
                "listed_company_count_on_exchange"
            ],
            "page_size": config.page_size,
            "query_hash": current_query_hash,
            "row_count": exchange_row_count,
            "augmentation_row_count": exchange_augmentation_row_count,
            "page_count": page_count,
            "started_at": retrieved_at,
            "completed_at": datetime.now(UTC).isoformat(),
            "objects": object_keys,
            "augmentation_objects": augmentation_object_keys,
        }
        object_store.write_json(
            manifest_key,
            json.dumps(manifest, sort_keys=True, separators=(",", ":")),
            bucket=WIKIDATA_RAW_BUCKET,
        )
        manifest_keys.append(manifest_key)
        total_row_count += exchange_row_count
        total_page_count += page_count

        if (
            config.request_delay_seconds > 0
            and exchange_index < len(exchange_rows) - 1
        ):
            sleep(config.request_delay_seconds)

    deleted_old_raw_object_count = delete_old_wikidata_raw_snapshot_objects(
        object_store=object_store,
        run_id=run_id,
    )
    return dg.MaterializeResult(
        metadata={
            "bucket": WIKIDATA_RAW_BUCKET,
            "exchange_count": len(exchange_rows),
            "page_count": total_page_count,
            "row_count": total_row_count,
            "manifest_count": len(manifest_keys),
            "manifest_keys": manifest_keys,
            "active_exchanges_key": active_exchanges_key or "",
            "deleted_old_raw_object_count": deleted_old_raw_object_count,
            "retrieved_at": retrieved_at,
        }
    )


def pull_wikidata_company_augmentation_raw_objects_for_page(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    run_id: str,
    retrieved_date: str,
    exchange_id: str,
    page_number: int,
    listed_company_bindings: list[dict[str, Any]],
    sleep: Callable[[float], None],
) -> tuple[list[str], int]:
    company_ids = sorted(
        {
            wikidata_id_from_url(binding_value(binding, "company"))
            for binding in listed_company_bindings
            if wikidata_id_from_url(binding_value(binding, "company"))
        }
    )
    object_keys: list[str] = []
    row_count = 0
    batches = list(_batched(company_ids, config.augmentation_batch_size))
    for batch_number, company_id_batch in enumerate(batches, start=1):
        query_builders: tuple[tuple[str, Callable[[tuple[str, ...]], str]], ...] = (
            ("profile", build_company_profile_augmentation_query),
            ("identifiers", build_company_identifier_augmentation_query),
            ("relationships", build_company_relationship_augmentation_query),
        )
        for query_index, (augmentation_kind, query_builder) in enumerate(
            query_builders,
            start=1,
        ):
            query = query_builder(tuple(company_id_batch))
            payload = client.fetch(query, user_agent=config.user_agent)
            bindings = response_bindings(payload)
            object_key = augmentation_object_key(
                retrieved_date=retrieved_date,
                run_id=run_id,
                exchange_id=exchange_id,
                augmentation_kind=augmentation_kind,
                page_number=page_number,
                batch_number=batch_number,
            )
            object_store.write_json(
                object_key,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                bucket=WIKIDATA_RAW_BUCKET,
            )
            object_keys.append(object_key)
            row_count += len(bindings)

            is_last_query = batch_number == len(batches) and query_index == len(query_builders)
            if config.request_delay_seconds > 0 and not is_last_query:
                sleep(config.request_delay_seconds)

    return object_keys, row_count


def _batched[T](items: list[T], batch_size: int) -> Iterator[list[T]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def wikidata_raw_pull_exchange_rows(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataRawPullConfig,
    run_id: str,
    retrieved_date: str,
    retrieved_at: str,
) -> tuple[list[dict[str, Any]], str | None]:
    configured_exchange_ids = config.configured_exchange_ids()
    if configured_exchange_ids is not None:
        return (
            wikidata_raw_pull_configured_exchange_rows(
                configured_exchange_ids,
                source_run_id=run_id,
                retrieved_at=retrieved_at,
                max_exchanges=config.max_exchanges,
            ),
            None,
        )

    query = build_active_listed_exchanges_query()
    payload = client.fetch(query, user_agent=config.user_agent)
    exchange_list_key = active_exchanges_object_key(
        retrieved_date=retrieved_date,
        run_id=run_id,
    )
    object_store.write_json(
        exchange_list_key,
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        bucket=WIKIDATA_RAW_BUCKET,
    )
    exchange_rows = [
        active_listed_exchange_row_from_binding(
            binding,
            source_run_id=run_id,
            retrieved_at=retrieved_at,
            source_row_number=row_number,
        )
        for row_number, binding in enumerate(response_bindings(payload), start=1)
        if config.max_exchanges is None or row_number <= config.max_exchanges
    ]
    if not exchange_rows:
        raise ValueError("Wikidata active listed-exchange query returned no exchanges")
    return exchange_rows, exchange_list_key


def wikidata_raw_pull_configured_exchange_rows(
    exchange_ids: tuple[str, ...],
    *,
    source_run_id: str,
    retrieved_at: str,
    max_exchanges: int | None,
) -> list[dict[str, Any]]:
    limited_exchange_ids = (
        exchange_ids if max_exchanges is None else exchange_ids[:max_exchanges]
    )
    return [
        {
            "exchange_wikidata_id": exchange_id,
            "exchange_name": "",
            "listed_company_count_on_exchange": 0,
            "source_run_id": source_run_id,
            "retrieved_at": retrieved_at,
            "source_row_number": row_number,
        }
        for row_number, exchange_id in enumerate(limited_exchange_ids, start=1)
    ]


def delete_old_wikidata_raw_snapshot_objects(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
) -> int:
    raw_prefix = "raw/"
    current_run_prefix = f"{raw_prefix}run_id={run_id}/"
    stale_keys = [
        key
        for key in object_store.list_keys(raw_prefix, bucket=WIKIDATA_RAW_BUCKET)
        if not key.startswith(current_run_prefix)
    ]
    return object_store.delete_keys(stale_keys, bucket=WIKIDATA_RAW_BUCKET)


# The canonical-contacts derivation (wikidata_clickhouse_canonical_contacts,
# defs/wikidata/contacts.py) runs after wikidata_company_seed_clickhouse lands
# corpscout.wikidata_company_websites/wikidata_companies, joined via an explicit
# union (not .upstream(), which already covers everything the export needs).
wikidata_company_seed_selection = (
    dg.AssetSelection.assets("wikidata_company_seed_clickhouse").upstream()
    | dg.AssetSelection.assets("wikidata_clickhouse_canonical_contacts")
)

wikidata_company_seed_weekly_job = dg.define_asset_job(
    "wikidata_company_seed_weekly_job",
    selection=wikidata_company_seed_selection,
)


@dg.schedule(
    name="wikidata_company_seed_weekly_schedule",
    cron_schedule="30 3 * * 1",
    execution_timezone="Europe/Belgrade",
    job=wikidata_company_seed_weekly_job,
)
def wikidata_company_seed_weekly_schedule() -> dg.RunRequest:
    return dg.RunRequest()


defs = dg.Definitions(
    assets=[
        wikidata_listed_companies_duckdb_asset,
        wikidata_company_seed_duckdb_tables,
        wikidata_company_seed_raw_objects,
        wikidata_company_seed_clickhouse,
    ],
    jobs=[wikidata_company_seed_weekly_job],
    schedules=[wikidata_company_seed_weekly_schedule],
    resources={"wikidata_duckdb": duckdb_resource(WIKIDATA_DUCKDB_PATH)},
)
