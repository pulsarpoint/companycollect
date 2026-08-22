"""Swedish company artifacts from Wikidata entities.

Input (source layer): wikidata_companies → corpscout.wikidata_companies (one row
per entity) linked to a Swedish orgnr either directly (wikidata_company_identifiers
identifier_type = 'se_orgnr') or via a current LEI in corpscout.company_identifier;
the register (sweden_company_companies_clickhouse → se_companies) bounds the id
universe. Writes the standard envelope followed by Wikidata's own typed columns.
source_record_uid is 'wikidata:<QID>' — latest observation per (company, source
record); superseded observations collapse at merge.

Assets
  se_company_info_wikidata_clickhouse → corpscout.se_company_info_wikidata
Downstream: info.py (description candidate, inception date, wikidata_id).
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import publish_with_stage

GROUP_NAME = "se_company_wikidata"
DATABASE = "corpscout"
TABLE = "se_company_info_wikidata"
# Positional insert list: the envelope (evidence_hash is MATERIALIZED, so omitted) then this
# module's payload, in the order the migration declares them — pinned by the test.
SE_COMPANY_INFO_WIKIDATA_COLUMNS = (
    "company_id", "source_record_uid", "observed_at", "source_run_id",
    "wikidata_id", "wikidata_url", "name", "official_name", "company_description",
    "inception_date", "legal_form_label", "industry_wikidata_id", "industry_label",
    "headquarters_label", "employee_count",
)

SE_COMPANY_INFO_WIKIDATA_SQL = """WITH swedish_companies AS (
    SELECT company_id FROM corpscout.se_companies FINAL WHERE match(company_id, '^[0-9]{10}$')
),
company_leis AS (
    SELECT identifiers.company_id AS company_id, upperUTF8(identifiers.issuer_id) AS lei
    FROM corpscout.company_identifier AS identifiers
    INNER JOIN swedish_companies AS companies ON companies.company_id = identifiers.company_id
    WHERE identifiers.country_code = 'SE' AND identifiers.issuer_scheme = 'lei' AND identifiers.is_current = 1
    GROUP BY identifiers.company_id, lei
),
links AS (
    SELECT company_id, wikidata_id FROM (
        SELECT companies.company_id AS company_id, identifiers.wikidata_id AS wikidata_id
        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL
        INNER JOIN swedish_companies AS companies
            ON companies.company_id = replaceRegexpAll(identifiers.identifier_value, '[^0-9]', '')
        WHERE identifiers.identifier_type = 'se_orgnr'
        UNION ALL
        SELECT leis.company_id AS company_id, identifiers.wikidata_id AS wikidata_id
        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL
        INNER JOIN company_leis AS leis ON leis.lei = upperUTF8(identifiers.identifier_value)
        WHERE identifiers.identifier_type = 'lei'
    )
    GROUP BY company_id, wikidata_id
),
candidates AS (
    SELECT
        links.company_id AS company_id,
        concat('wikidata:', entities.wikidata_id) AS source_record_uid,
        entities.resolved_at AS observed_at,
        %(source_run_id)s AS source_run_id,
        entities.wikidata_id AS wikidata_id,
        entities.wikidata_url AS wikidata_url,
        entities.name AS name,
        entities.official_name AS official_name,
        entities.company_description AS company_description,
        entities.inception_date AS inception_date,
        entities.legal_form_label AS legal_form_label,
        entities.industry_wikidata_id AS industry_wikidata_id,
        entities.industry_label AS industry_label,
        entities.headquarters_label AS headquarters_label,
        entities.employee_count AS employee_count
    FROM corpscout.wikidata_companies AS entities FINAL
    INNER JOIN links ON links.wikidata_id = entities.wikidata_id
    WHERE trim(entities.name) != ''
)
SELECT
    company_id AS company_id, source_record_uid AS source_record_uid, observed_at AS observed_at, source_run_id AS source_run_id,
    wikidata_id AS wikidata_id, wikidata_url AS wikidata_url, name AS name, official_name AS official_name,
    company_description AS company_description, inception_date AS inception_date, legal_form_label AS legal_form_label,
    industry_wikidata_id AS industry_wikidata_id, industry_label AS industry_label, headquarters_label AS headquarters_label,
    employee_count AS employee_count
FROM candidates"""


@dg.asset(
    name="se_company_info_wikidata_clickhouse",
    deps=[
        dg.AssetKey("wikidata_companies"),
        dg.AssetKey("sweden_company_companies_clickhouse"),
        dg.AssetKey("wikidata_company_identifiers"),
        dg.AssetKey("company_identifier_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{TABLE}"},
    description=(
        "Wikidata facts for Swedish companies linked by orgnr or LEI (label, description, "
        "inception, legal form, industry, headquarters, employees); a new observation is written "
        "only when the evidence hash changes and the latest per (company, source record) survives merges."
    ),
)
def se_company_info_wikidata_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    """Link Wikidata entities to Swedish orgnrs → stage → validate → append new versions."""
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE,
        tables=("wikidata_companies", "wikidata_company_identifiers", "company_identifier", "se_companies", TABLE),
    )
    counts = publish_with_stage(
        clickhouse=clickhouse, target=TABLE,
        insert_columns=SE_COMPANY_INFO_WIKIDATA_COLUMNS, select_sql=SE_COMPANY_INFO_WIKIDATA_SQL,
        select_parameters={"source_run_id": context.run_id},
        invalid_condition="trim(company_id) = '' OR trim(source_record_uid) = '' OR trim(wikidata_id) = ''",
        new_versions_only=True,
    )
    context.log.info("se_company_info_wikidata: appended=%s total=%s", counts.inserted, counts.total)
    return dg.MaterializeResult(metadata={
        "appended_count": counts.inserted, "total_count": counts.total,
        "table": f"{DATABASE}.{TABLE}", "resolved_at": datetime.now(UTC).isoformat(),
    })


defs = dg.Definitions(assets=[se_company_info_wikidata_clickhouse])
