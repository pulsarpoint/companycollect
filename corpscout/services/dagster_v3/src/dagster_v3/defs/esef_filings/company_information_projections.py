"""Typed ClickHouse projections of ESEF document company information.

Each projection is an independent ESEF asset. The business-items and
group-relationships projections share the same paid company-information
enrichment as input and only ever append, so Dagster can run or retry them
separately. The people projection instead reads its own per-filing people
pass (``esef_document_people_extraction``) and REPLACES its table (stage
table + ``EXCHANGE TABLES``), since that pass can be rerun for any document
and must not accumulate duplicate rows for the same candidate.
"""

import uuid
from collections.abc import Callable

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.esef_filings import tables

GROUP_NAME = "esef"
_COMPANY_INFORMATION_ASSET = dg.AssetKey("esef_document_company_information_clickhouse")
_EXTRACTED_AT_SQL = (
    "coalesce(parseDateTime64BestEffortOrNull(info.extracted_at), info.resolved_at)"
)


def esef_document_people_sql(
    *, target: str = tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_TABLE
) -> str:
    candidate_uid = _person_candidate_uid_sql()
    # The candidate identity is (name, role_category, role text) per the 2026-09-12 ruling --
    # a person's committee seat is its own row, not merged into their other roles; every item
    # of one extraction row shares extracted_at, so a tie within that identity is broken by
    # the model's own list order (item_index) -- the first-listed role wins, not ClickHouse's
    # unstable sort.
    return f"""INSERT INTO {target}
({", ".join(tables.ESEF_DOCUMENT_PEOPLE_COLUMNS)})
SELECT
    {candidate_uid} AS candidate_uid,
    info.source_record_uid,
    info.source_document_id,
    info.lei,
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
    info.extracted_at
FROM {tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE} AS info
ARRAY JOIN
    JSONExtractArrayRaw(info.people_json) AS item_json,
    arrayEnumerate(JSONExtractArrayRaw(info.people_json)) AS item_index
WHERE info.extraction_status IN ('extracted', 'reused')
  AND info.source_record_uid != ''
  AND JSONExtractString(item_json, 'name') != ''
  AND JSONExtractString(item_json, 'role') != ''
  AND ifNull(toDate32OrNull(info.period_end), toDate32('1970-01-01')) <= today()
ORDER BY multiIf(JSONExtractString(item_json, 'status') = 'current', 0, JSONExtractString(item_json, 'status') = 'historical', 1, 2), info.extracted_at DESC, item_index
LIMIT 1 BY info.lei, info.fiscal_year, info.source_record_uid, candidate_uid"""


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
    info.lei,
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
WHERE JSONExtractString(item_json, 'name') != ''
  AND ifNull(toDate32OrNull(info.period_end), toDate32('1970-01-01')) <= today()"""


def esef_document_group_relationships_sql() -> str:
    candidate_uid = _candidate_uid_sql(item_kind_expression="'group_relationship'")
    return f"""INSERT INTO {tables.QUALIFIED_ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE}
({", ".join(tables.ESEF_DOCUMENT_GROUP_RELATIONSHIP_COLUMNS)})
SELECT
    {candidate_uid},
    info.source_record_uid,
    info.source_document_id,
    info.lei,
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
  AND JSONExtractString(item_json, 'related_company_name') != ''
  AND ifNull(toDate32OrNull(info.period_end), toDate32('1970-01-01')) <= today()"""


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


def _person_candidate_uid_sql() -> str:
    return (
        "lower(hex(SHA256(concat("
        "'company-source-record-v1\\nobservation\\n', "
        "toString(info.source_record_uid), "
        "'\\nesef_person\\n', "
        "info.prompt_version, "
        "'\\n', "
        "lowerUTF8(trim(replaceRegexpAll(JSONExtractString(item_json, 'name'), "
        "'\\\\s+', ' '))), "
        "'\\n', "
        "JSONExtractString(item_json, 'role_category'), "
        "'\\n', "
        "lowerUTF8(trim(replaceRegexpAll(JSONExtractString(item_json, 'role'), "
        "'\\\\s+', ' ')))"
        "))))"
    )


def _publish_projection(
    *,
    clickhouse: ClickhouseResource,
    table_name: str,
    statement: str,
    source_table: str = tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(source_table, table_name),
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


def _replace_projection(
    *,
    clickhouse: ClickhouseResource,
    table_name: str,
    statement_for: Callable[[str], str],
    source_table: str,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(source_table, table_name),
    )
    target = f"{tables.ESEF_DATABASE}.{table_name}"
    stage = f"{tables.ESEF_DATABASE}._tmp_{table_name}_{uuid.uuid4().hex}"
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {target}")
        try:
            client.execute(statement_for(stage))
            stage_count = int(client.execute(f"SELECT count() FROM {stage}")[0][0])
            target_count = int(
                client.execute(f"SELECT count() FROM {target} FINAL")[0][0]
            )
            if stage_count == 0 and target_count > 0:
                raise ValueError(
                    f"refusing to replace {target}: the rebuilt projection is "
                    f"empty while the table holds {target_count} rows"
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {target}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")
        row_count = int(client.execute(f"SELECT count() FROM {target} FINAL")[0][0])
    return dg.MaterializeResult(
        metadata={
            "table": target,
            "row_count": row_count,
        }
    )


@dg.asset(
    name="esef_document_people_clickhouse",
    deps=[dg.AssetKey("esef_document_people_extraction_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql", "llm", "xbrl"},
    metadata={"table": tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_TABLE},
)
def esef_document_people_clickhouse(
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return _replace_projection(
        clickhouse=clickhouse,
        table_name=tables.ESEF_DOCUMENT_PEOPLE_TABLE,
        statement_for=lambda target: esef_document_people_sql(target=target),
        source_table=tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,
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
