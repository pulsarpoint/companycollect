"""Typed ClickHouse projections of ESEF document company information.

Each projection is an independent ESEF asset. They share the same paid LLM
result as input but publish different serving contracts, so Dagster can run or
retry them separately.
"""

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.esef_filings import tables

GROUP_NAME = "esef_filings"
_COMPANY_INFORMATION_ASSET = dg.AssetKey("esef_document_company_information_clickhouse")
_EXTRACTED_AT_SQL = (
    "coalesce(parseDateTime64BestEffortOrNull(info.extracted_at), info.resolved_at)"
)


def esef_document_people_sql() -> str:
    candidate_uid = _candidate_uid_sql(item_kind_expression="'person'")
    return f"""INSERT INTO {tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_TABLE}
({", ".join(tables.ESEF_DOCUMENT_PEOPLE_COLUMNS)})
SELECT
    {candidate_uid},
    info.source_record_uid,
    info.source_document_id,
    info.country_iso2,
    info.company_id,
    info.fiscal_year,
    JSONExtractString(item_json, 'name'),
    JSONExtractString(item_json, 'role'),
    JSONExtractString(item_json, 'role_category'),
    JSONExtractString(item_json, 'organization'),
    JSONExtractString(item_json, 'status'),
    toDate32(parseDateTimeBestEffortOrNull(nullIf(JSONExtractString(item_json, 'effective_from'), ''))),
    toDate32(parseDateTimeBestEffortOrNull(nullIf(JSONExtractString(item_json, 'effective_to'), ''))),
    toFloat32(JSONExtractFloat(item_json, 'confidence')),
    JSONExtract(item_json, 'evidence_ids', 'Array(String)'),
    info.model_provider,
    info.model_name,
    info.prompt_version,
    info.source_run_id,
    {_EXTRACTED_AT_SQL}
FROM {tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE} AS info
ARRAY JOIN JSONExtractArrayRaw(info.people_json) AS item_json
WHERE info.extraction_status IN ('enriched', 'reused')
  AND info.source_record_uid != ''
  AND JSONExtractString(item_json, 'name') != ''
  AND JSONExtractString(item_json, 'role') != ''"""


def esef_document_business_items_sql() -> str:
    business_rows = "\nUNION ALL\n".join(
        f"""SELECT info.*, '{item_kind}' AS item_kind, arrayJoin(
    JSONExtractArrayRaw(info.{json_column})
) AS item_json
FROM {tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE} AS info
WHERE info.extraction_status IN ('enriched', 'reused')
  AND info.source_record_uid != ''"""
        for item_kind, json_column in (
            ("product_or_service", "products_and_services_json"),
            ("customer_market", "customer_markets_json"),
            ("operating_geography", "operating_geographies_json"),
            ("business_segment", "business_segments_json"),
        )
    )
    candidate_uid = _candidate_uid_sql(item_kind_expression="item_kind")
    return f"""INSERT INTO {tables.QUALIFIED_ESEF_DOCUMENT_BUSINESS_ITEMS_TABLE}
({", ".join(tables.ESEF_DOCUMENT_BUSINESS_ITEM_COLUMNS)})
SELECT
    {candidate_uid},
    info.source_record_uid,
    info.source_document_id,
    info.country_iso2,
    info.company_id,
    info.fiscal_year,
    item_kind,
    JSONExtractString(item_json, 'name'),
    if(item_kind = 'operating_geography', JSONExtractString(item_json, 'geography_type'), ''),
    toFloat32(JSONExtractFloat(item_json, 'confidence')),
    JSONExtract(item_json, 'evidence_ids', 'Array(String)'),
    info.model_provider,
    info.model_name,
    info.prompt_version,
    info.source_run_id,
    {_EXTRACTED_AT_SQL}
FROM ({business_rows}) AS info
WHERE JSONExtractString(item_json, 'name') != ''"""


def esef_document_group_relationships_sql() -> str:
    candidate_uid = _candidate_uid_sql(item_kind_expression="'group_relationship'")
    return f"""INSERT INTO {tables.QUALIFIED_ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE}
({", ".join(tables.ESEF_DOCUMENT_GROUP_RELATIONSHIP_COLUMNS)})
SELECT
    {candidate_uid},
    info.source_record_uid,
    info.source_document_id,
    info.country_iso2,
    info.company_id,
    info.fiscal_year,
    JSONExtractString(item_json, 'related_company_name'),
    JSONExtractString(item_json, 'relationship_type'),
    JSONExtract(item_json, 'ownership_percentage', 'Nullable(Float32)'),
    JSONExtractString(item_json, 'jurisdiction'),
    toFloat32(JSONExtractFloat(item_json, 'confidence')),
    JSONExtract(item_json, 'evidence_ids', 'Array(String)'),
    info.model_provider,
    info.model_name,
    info.prompt_version,
    info.source_run_id,
    {_EXTRACTED_AT_SQL}
FROM {tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE} AS info
ARRAY JOIN JSONExtractArrayRaw(info.material_group_relationships_json) AS item_json
WHERE info.extraction_status IN ('enriched', 'reused')
  AND info.source_record_uid != ''
  AND JSONExtractString(item_json, 'related_company_name') != ''"""


def _candidate_uid_sql(*, item_kind_expression: str) -> str:
    natural_key = (
        "concat(info.prompt_version, ':', info.model_name, ':', "
        f"{item_kind_expression}, ':', item_json)"
    )
    return (
        "lower(hex(SHA256(concat("
        "'company-source-record-v1\\nobservation\\n', "
        "toString(info.source_record_uid), "
        "'\\nesef_typed_candidate\\n', "
        f"toString({natural_key})"
        "))))"
    )


def _publish_projection(
    *,
    clickhouse: ClickhouseResource,
    table_name: str,
    statement: str,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE, table_name),
    )
    with clickhouse.get_connection() as client:
        client.execute(statement)
        row_count = int(
            client.execute(
                f"SELECT count() FROM {tables.ESEF_DATABASE}.{table_name} FINAL"
            )[0][0]
        )
    return dg.MaterializeResult(
        metadata={
            "table": f"{tables.ESEF_DATABASE}.{table_name}",
            "row_count": row_count,
        }
    )


@dg.asset(
    name="esef_document_people_clickhouse",
    deps=[_COMPANY_INFORMATION_ASSET],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql", "llm", "xbrl"},
    metadata={"table": tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_TABLE},
)
def esef_document_people_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _publish_projection(
        clickhouse=clickhouse,
        table_name=tables.ESEF_DOCUMENT_PEOPLE_TABLE,
        statement=esef_document_people_sql(),
    )


@dg.asset(
    name="esef_document_business_items_clickhouse",
    deps=[_COMPANY_INFORMATION_ASSET],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql", "llm", "xbrl"},
    metadata={"table": tables.QUALIFIED_ESEF_DOCUMENT_BUSINESS_ITEMS_TABLE},
)
def esef_document_business_items_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _publish_projection(
        clickhouse=clickhouse,
        table_name=tables.ESEF_DOCUMENT_BUSINESS_ITEMS_TABLE,
        statement=esef_document_business_items_sql(),
    )


@dg.asset(
    name="esef_document_group_relationships_clickhouse",
    deps=[_COMPANY_INFORMATION_ASSET],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql", "llm", "xbrl"},
    metadata={"table": tables.QUALIFIED_ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE},
)
def esef_document_group_relationships_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _publish_projection(
        clickhouse=clickhouse,
        table_name=tables.ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE,
        statement=esef_document_group_relationships_sql(),
    )


defs = dg.Definitions(
    assets=[
        esef_document_people_clickhouse,
        esef_document_business_items_clickhouse,
        esef_document_group_relationships_clickhouse,
    ]
)
