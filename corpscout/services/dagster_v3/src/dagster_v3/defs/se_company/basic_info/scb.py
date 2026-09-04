"""SCB register record -> basic-info suggestion (spec 3.1: SCB's codes become values here)."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import define_suggestion_asset

SCB_EXTRACTOR_VERSION = "scb-v1"

# The uid migration 000257 used to derive for the register row, computed by the extractor
# because the source table deliberately does not store it (slice-0 handoff).
SCB_RECORD_UID_SQL = (
    "lower(hex(SHA256(concat('company-source-record-v1\\nstructured\\n', 'sweden_scb', "
    "'\\nregistry_company\\n', source_record_id, '\\n', lowerUTF8(source_payload_hash)))))"
)


def scb_current_sql() -> str:
    return "SELECT company_id, observed_at FROM corpscout.se_scb_companies FINAL WHERE has_company = 1"


def scb_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'scb' AS source,\n"
        f"    {SCB_RECORD_UID_SQL} AS source_record_uid,\n"
        "    observed_at AS observed_at,\n"
        "    nullIf(trim(ifNull(legal_name, '')), '') AS legal_name,\n"
        "    nullIf(trim(ifNull(legal_form_code, '')), '') AS legal_form_code,\n"
        "    multiIf(source_status_code = '1', 'active', source_status_code IN ('0', '9'), 'inactive', NULL) AS status,\n"
        "    registration_date AS incorporation_date,\n"
        "    CAST(NULL AS Nullable(String)) AS lei,\n"
        "    CAST(NULL AS Nullable(String)) AS wikidata_id,\n"
        "    CAST(NULL AS Nullable(String)) AS description,\n"
        "    CAST(NULL AS Nullable(String)) AS description_language,\n"
        "    CAST(NULL AS Nullable(String)) AS description_sv\n"
        "FROM corpscout.se_scb_companies FINAL\n"
        "WHERE has_company = 1 AND company_id IN %(company_ids)s"
    )


se_basic_info_suggestions_scb = define_suggestion_asset(
    source="scb",
    extractor_version=SCB_EXTRACTOR_VERSION,
    current_sql=scb_current_sql(),
    select_sql=scb_select_sql(),
    deps=[dg.AssetKey("sweden_company_scb_companies_clickhouse")],
    description=(
        "One scb suggestion row per company from se_scb_companies: legal name, JurForm code, "
        "FtgStat mapped to active/inactive, registration date. Written only when the register "
        "record is newer than the current scb suggestion. execute=false previews."
    ),
)
