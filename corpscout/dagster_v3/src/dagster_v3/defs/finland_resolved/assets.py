import json

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)
from dagster_v3.defs.clickhouse.resources import (
    CLICKHOUSE_RESOURCE,
    clickhouse_resource_from_env,
)
from dagster_v3.defs.finland_resolved import tables
from dagster_v3.defs.finland_ytj.resources import LocalDuckDBResource

RESOLVED_DUCKDB_SCHEMA = "finland_resolved"
YTJ_DUCKDB_SCHEMA = "finland_prhytj"
YTJ_ALL_COMPANIES_TABLE = "all_companies"


def build_finland_ytj_resolved_tables(
    source_duckdb: LocalDuckDBResource,
) -> dict[str, int]:
    with source_duckdb.connect() as connection:
        connection.execute(f"create schema if not exists {RESOLVED_DUCKDB_SCHEMA}")
        connection.create_function(
            "fi_primary_industry_json",
            _primary_industry_json,
            ["VARCHAR"],
            "VARCHAR",
            null_handling="special",
        )
        connection.execute(_fi_companies_sql())
        connection.execute(_fi_websites_sql())
        connection.execute(_fi_industries_sql())
        return {
            table: int(
                connection.execute(
                    f"select count(*) from {RESOLVED_DUCKDB_SCHEMA}.{table}"
                ).fetchone()[0]
            )
            for table in tables.FINLAND_YTJ_RESOLVED_TABLES
        }


@dg.asset(
    deps=["finland_ytj_all_companies_duckdb"],
    group_name="finland_resolved",
    kinds={"python", "duckdb", "sql"},
    description="Resolved Finland company, website, and industry DuckDB tables derived from YTJ.",
)
def finland_ytj_resolved_duckdb(
    ytj_duckdb: LocalDuckDBResource,
) -> dg.MaterializeResult:
    row_counts = build_finland_ytj_resolved_tables(ytj_duckdb)
    return dg.MaterializeResult(
        metadata={f"{table}_row_count": count for table, count in row_counts.items()}
    )


@dg.asset(
    deps=[finland_ytj_resolved_duckdb],
    group_name="finland_resolved",
    kinds={"python", "duckdb", "clickhouse"},
    description="Exports resolved Finland YTJ DuckDB tables to migrated ClickHouse tables.",
)
def finland_ytj_resolved_clickhouse(
    clickhouse: ClickhouseResource,
    ytj_duckdb: LocalDuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.FINLAND_YTJ_RESOLVED_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = {
            table: export_duckdb_table_to_clickhouse(
                duckdb_path=ytj_duckdb.path(),
                clickhouse_client=client,
                duckdb_schema=RESOLVED_DUCKDB_SCHEMA,
                duckdb_table=table,
                clickhouse_database=RESOLVED_DATABASE,
                clickhouse_table=table,
                columns=tables.RESOLVED_TABLE_COLUMNS[table],
                truncate=True,
            )
            for table in tables.FINLAND_YTJ_RESOLVED_TABLES
        }
    return dg.MaterializeResult(
        metadata={f"{table}_row_count": count for table, count in row_counts.items()}
    )


def _fi_companies_sql() -> str:
    return f"""
    create or replace table {RESOLVED_DUCKDB_SCHEMA}.{tables.FI_COMPANIES_TABLE} as
    select
      business_id,
      country_iso2,
      primary_name as name,
      lower(primary_name) as name_normalized,
      try_cast(nullif(registration_date, '') as date) as registration_date,
      try_cast(nullif(end_date, '') as date) as end_date,
      lifecycle_status,
      coalesce(is_active, false) as is_active,
      cast(null as varchar) as legal_form_code,
      cast(null as varchar) as legal_form_description_original,
      cast(null as varchar) as legal_form_description_language,
      cast(null as varchar) as legal_form_description_en,
      cast(null as timestamp) as legal_form_description_translated_at,
      cast(null as varchar) as legal_form_description_translation_provider,
      cast(null as varchar) as legal_form_description_translation_model,
      nullif(website_normalized_url, '') as primary_website_url,
      nullif(website_host, '') as primary_website_host,
      source_slug as source_system,
      source_run_id,
      source_record_id,
      source_payload_hash,
      now() as resolved_at
    from {YTJ_DUCKDB_SCHEMA}.{YTJ_ALL_COMPANIES_TABLE}
    where business_id is not null and business_id != ''
    """


def _fi_websites_sql() -> str:
    return f"""
    create or replace table {RESOLVED_DUCKDB_SCHEMA}.{tables.FI_WEBSITES_TABLE} as
    select
      business_id,
      website_url,
      website_normalized_url,
      website_host,
      website_path,
      try_cast(nullif(website_registered_on, '') as date) as registered_on,
      try_cast(nullif(website_ended_on, '') as date) as ended_on,
      website_ended_on is null or website_ended_on = '' as is_current,
      true as is_primary,
      source_slug as source_system,
      source_run_id,
      source_record_id,
      source_payload_hash,
      now() as resolved_at
    from {YTJ_DUCKDB_SCHEMA}.{YTJ_ALL_COMPANIES_TABLE}
    where business_id is not null
      and business_id != ''
      and website_normalized_url is not null
      and website_normalized_url != ''
    """


def _fi_industries_sql() -> str:
    return f"""
    create or replace table {RESOLVED_DUCKDB_SCHEMA}.{tables.FI_INDUSTRIES_TABLE} as
    with extracted as (
      select
        *,
        fi_primary_industry_json(raw_company) as industry_json
      from {YTJ_DUCKDB_SCHEMA}.{YTJ_ALL_COMPANIES_TABLE}
    )
    select
      business_id,
      json_extract_string(industry_json, '$.code') as source_industry_code,
      json_extract_string(industry_json, '$.codeSet') as source_industry_code_set,
      json_extract_string(industry_json, '$.description') as description_original,
      coalesce(json_extract_string(industry_json, '$.language'), 'fi') as description_language,
      cast(null as varchar) as description_en,
      cast(null as timestamp) as description_translated_at,
      cast(null as varchar) as description_translation_provider,
      cast(null as varchar) as description_translation_model,
      coalesce(json_extract_string(industry_json, '$.codeSet'), 'NACE_REV_2_1') as nace_revision,
      json_extract_string(industry_json, '$.code') as nace_code,
      regexp_replace(json_extract_string(industry_json, '$.code'), '[^0-9A-Za-z]', '', 'g') as nace_normalized_code,
      'direct_code' as nace_mapping_method,
      case when json_extract_string(industry_json, '$.code') is null then 'missing_source_code' else 'mapped' end as nace_mapping_status,
      true as is_primary,
      source_slug as source_system,
      source_run_id,
      source_record_id,
      source_payload_hash,
      now() as resolved_at
    from extracted
    where business_id is not null and business_id != ''
    """


def _primary_industry_json(raw_company: str | None) -> str | None:
    if not raw_company:
        return None
    try:
        payload = json.loads(raw_company)
    except json.JSONDecodeError:
        return None
    for key in ("businessLine", "mainBusinessLine"):
        business_line = payload.get(key)
        if isinstance(business_line, dict):
            return json.dumps(_normalize_business_line(business_line))
    business_lines = payload.get("businessLines")
    if isinstance(business_lines, list):
        for business_line in business_lines:
            if isinstance(business_line, dict):
                return json.dumps(_normalize_business_line(business_line))
    return None


def _normalize_business_line(business_line: dict[str, object]) -> dict[str, object]:
    description, language = _select_business_line_description(business_line)
    return {
        "code": business_line.get("code") or business_line.get("type"),
        "codeSet": business_line.get("codeSet") or business_line.get("typeCodeSet"),
        "description": description,
        "language": business_line.get("language") or language,
    }


def _select_business_line_description(
    business_line: dict[str, object],
) -> tuple[object, str | None]:
    direct_description = business_line.get("description")
    if direct_description:
        language = business_line.get("language")
        return direct_description, str(language) if language else None

    descriptions = business_line.get("descriptions")
    if not isinstance(descriptions, list):
        return None, None

    selected_description = _find_description_by_language(descriptions, "1")
    if selected_description is None:
        selected_description = _first_description(descriptions)
    if selected_description is None:
        return None, None

    language_code = selected_description.get("languageCode")
    return selected_description.get("description"), _business_line_language(language_code)


def _find_description_by_language(
    descriptions: list[object],
    language_code: str,
) -> dict[str, object] | None:
    for description in descriptions:
        if (
            isinstance(description, dict)
            and str(description.get("languageCode")) == language_code
            and description.get("description")
        ):
            return description
    return None


def _first_description(descriptions: list[object]) -> dict[str, object] | None:
    for description in descriptions:
        if isinstance(description, dict) and description.get("description"):
            return description
    return None


def _business_line_language(language_code: object) -> str | None:
    return {
        "1": "fi",
        "2": "sv",
        "3": "en",
    }.get(str(language_code))


defs = dg.Definitions(
    assets=[
        finland_ytj_resolved_duckdb,
        finland_ytj_resolved_clickhouse,
    ],
    resources={
        "clickhouse": CLICKHOUSE_RESOURCE,
    },
)
