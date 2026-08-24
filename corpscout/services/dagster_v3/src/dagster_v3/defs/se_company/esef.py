"""Swedish company artifacts extracted from ESEF annual-report filings.

Input (source layer): esef_document_company_information_clickhouse →
corpscout.esef_document_company_information (the model-extracted company
description per filing, all countries; several model versions per document may
coexist).
This module keeps Swedish issuers (country_iso2 = 'SE', 10-digit orgnr) with a
non-empty description and writes the standard envelope followed by ESEF's own
typed columns. source_record_uid is the filing's provenance uid, so one row per
filing version; the newest extraction per filing wins.

Assets
  se_company_info_esef_clickhouse → corpscout.se_company_info_esef
Downstream: info.py (description candidate; legal_name always comes from SCB).
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import SE_COMPANY_ID_PATTERN, publish_with_stage

GROUP_NAME = "se_company_esef"
DATABASE = "corpscout"
TABLE = "se_company_info_esef"
# Positional insert list: the envelope (evidence_hash is MATERIALIZED, so omitted) then this
# module's payload, in the order the migration declares them — pinned by the test.
SE_COMPANY_INFO_ESEF_COLUMNS = (
    "company_id",
    "source_record_uid",
    "observed_at",
    "source_run_id",
    "source_document_id",
    "lei",
    "entity_name",
    "fiscal_year",
    "company_description",
    "description_language",
    "description_confidence",
    "products_and_services_json",
    "business_segments_json",
)

SE_COMPANY_INFO_ESEF_SQL = """WITH candidates AS (
    SELECT
        info.company_id AS company_id,
        info.source_record_uid AS source_record_uid,
        info.resolved_at AS observed_at,
        %(source_run_id)s AS source_run_id,
        info.source_document_id AS source_document_id,
        info.lei AS lei,
        '' AS entity_name,
        info.fiscal_year AS fiscal_year,
        info.company_description AS company_description,
        toString(info.description_language) AS description_language,
        info.description_confidence AS description_confidence,
        info.products_and_services_json AS products_and_services_json,
        info.business_segments_json AS business_segments_json
    FROM corpscout.esef_document_company_information AS info
    WHERE info.country_iso2 = 'SE'
      AND match(info.company_id, '{SE_COMPANY_ID_PATTERN}')
      AND trim(info.company_description) != ''
    ORDER BY info.resolved_at DESC, info.model_provider, info.model_name, info.prompt_version
    LIMIT 1 BY info.company_id, info.source_record_uid
)
SELECT
    company_id AS company_id, source_record_uid AS source_record_uid, observed_at AS observed_at, source_run_id AS source_run_id,
    source_document_id AS source_document_id, lei AS lei, entity_name AS entity_name, fiscal_year AS fiscal_year,
    company_description AS company_description, description_language AS description_language,
    description_confidence AS description_confidence, products_and_services_json AS products_and_services_json,
    business_segments_json AS business_segments_json
FROM candidates""".replace("{SE_COMPANY_ID_PATTERN}", SE_COMPANY_ID_PATTERN)


@dg.asset(
    name="se_company_info_esef_clickhouse",
    deps=[
        dg.AssetKey("esef_document_company_information_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{TABLE}"},
    description=(
        "Company description and business text reported in each Swedish ESEF filing; "
        "latest observation per (company, source record) — superseded observations collapse at merge."
    ),
)
def se_company_info_esef_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    """Select SE filings with a description → stage → validate → append new versions."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=DATABASE,
        tables=("esef_document_company_information", TABLE),
    )
    counts = publish_with_stage(
        clickhouse=clickhouse,
        target=TABLE,
        insert_columns=SE_COMPANY_INFO_ESEF_COLUMNS,
        select_sql=SE_COMPANY_INFO_ESEF_SQL,
        select_parameters={"source_run_id": context.run_id},
        invalid_condition="trim(company_id) = '' OR trim(source_record_uid) = '' OR trim(company_description) = ''",
        new_versions_only=True,
    )
    context.log.info(
        "se_company_info_esef: appended=%s total=%s", counts.inserted, counts.total
    )
    return dg.MaterializeResult(
        metadata={
            "appended_count": counts.inserted,
            "total_count": counts.total,
            "table": f"{DATABASE}.{TABLE}",
            "resolved_at": datetime.now(UTC).isoformat(),
        }
    )


defs = dg.Definitions(assets=[se_company_info_esef_clickhouse])
