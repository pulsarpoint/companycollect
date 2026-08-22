"""Swedish company artifacts from the SCB/Bolagsverket company register.

Input (source layer): sweden_company_companies_clickhouse → corpscout.se_companies
(one row per company, rebuilt weekly from the register bulk files) and
corpscout.se_industries (SNI/NACE codes per company, is_primary flag).
This module writes one artifact table per datatype with the standard envelope
followed by the register's own typed columns.

Assets
  se_company_info_scb_clickhouse → corpscout.se_company_info_scb
    legal name, legal form, status, incorporation/dissolution, activity
    description (Bolagsverket verksamhetsbeskrivning) and the primary SNI/NACE
    code; one row per register version, appended only when evidence_hash changes.
Downstream: info.py (legal_name is authoritative from here).
"""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import publish_with_stage

GROUP_NAME = "se_company_scb"
DATABASE = "corpscout"
TABLE = "se_company_info_scb"
# Positional insert list: the envelope (evidence_hash is MATERIALIZED, so omitted) then this
# module's payload, in the order the migration declares them — pinned by the test.
SE_COMPANY_INFO_SCB_COLUMNS = (
    "company_id", "source_record_uid", "observed_at", "source_run_id",
    "legal_name", "legal_name_raw", "legal_form_code", "status", "incorporation_date",
    "dissolution_date", "activity_description", "primary_sni_code", "primary_nace_code",
)

# New versions only: publish_with_stage stages these candidates, then keeps only
# rows whose (company_id, source_record_uid, evidence_hash) is not already in the
# target -- the target's MATERIALIZED evidence_hash expression computes the hash
# on the stage, so it is never re-expressed here.
SE_COMPANY_INFO_SCB_SQL = """WITH candidates AS (
    SELECT
        companies.company_id AS company_id,
        ifNull(nullIf(companies.scb_source_record_uid, ''), companies.bolagsverket_source_record_uid) AS source_record_uid,
        companies.updated_from_raw_at AS observed_at,
        %(source_run_id)s AS source_run_id,
        companies.legal_name AS legal_name,
        companies.legal_name_raw AS legal_name_raw,
        companies.legal_form_code AS legal_form_code,
        toString(companies.status) AS status,
        companies.incorporation_date AS incorporation_date,
        companies.dissolution_date AS dissolution_date,
        companies.activity_description AS activity_description,
        ifNull(argMaxIf(industries.sni_code, industries.updated_from_raw_at, industries.is_primary = 1), '') AS primary_sni_code,
        ifNull(argMaxIf(industries.nace_rev2_class_code, industries.updated_from_raw_at, industries.is_primary = 1), '') AS primary_nace_code
    FROM corpscout.se_companies AS companies FINAL
    LEFT JOIN corpscout.se_industries AS industries ON industries.company_id = companies.company_id
    WHERE match(companies.company_id, '^[0-9]{10}$')
    GROUP BY
        companies.company_id, companies.scb_source_record_uid, companies.bolagsverket_source_record_uid,
        companies.updated_from_raw_at, companies.legal_name, companies.legal_name_raw,
        companies.legal_form_code, companies.status, companies.incorporation_date,
        companies.dissolution_date, companies.activity_description
)
SELECT
    company_id, source_record_uid, observed_at, source_run_id,
    legal_name, legal_name_raw, legal_form_code, status, incorporation_date, dissolution_date,
    activity_description, primary_sni_code, primary_nace_code
FROM candidates
WHERE source_record_uid != ''"""


@dg.asset(
    name="se_company_info_scb_clickhouse",
    deps=[
        dg.AssetKey("sweden_company_companies_clickhouse"),
        dg.AssetKey("sweden_company_industries_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": f"{DATABASE}.{TABLE}"},
    description=(
        "Register facts per Swedish company (legal name, form, status, dates, activity "
        "description, primary SNI/NACE) as an append-only artifact; a new version is "
        "written only when the evidence hash changes."
    ),
)
def se_company_info_scb_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    """Select from se_companies (+ primary industry) → stage → validate → append new versions."""
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=("se_companies", "se_industries", TABLE)
    )
    counts = publish_with_stage(
        clickhouse=clickhouse,
        target=TABLE,
        insert_columns=SE_COMPANY_INFO_SCB_COLUMNS,
        select_sql=SE_COMPANY_INFO_SCB_SQL,
        select_parameters={"source_run_id": context.run_id},
        invalid_condition="trim(company_id) = '' OR trim(source_record_uid) = ''",
        new_versions_only=True,
    )
    context.log.info("se_company_info_scb: appended=%s total=%s", counts.inserted, counts.total)
    return dg.MaterializeResult(
        metadata={"appended_count": counts.inserted, "total_count": counts.total,
                  "table": f"{DATABASE}.{TABLE}", "resolved_at": datetime.now(UTC).isoformat()}
    )


defs = dg.Definitions(assets=[se_company_info_scb_clickhouse])
