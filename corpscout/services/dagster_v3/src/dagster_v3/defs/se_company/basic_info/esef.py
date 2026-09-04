"""ESEF filing extraction -> basic-info suggestion: the LEI and the newest filing's
description (spec 3.2: a source with several records contributes its newest)."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import define_suggestion_asset
from dagster_v3.defs.se_company.common import SE_COMPANY_ID_PATTERN

ESEF_EXTRACTOR_VERSION = "esef-v1"

_FILTER = (
    "WHERE country_iso2 = 'SE'\n"
    f"  AND match(company_id, '{SE_COMPANY_ID_PATTERN}')\n"
    "  AND trim(company_description) != ''"
)


def esef_current_sql() -> str:
    return (
        "SELECT company_id, max(toDateTime64(resolved_at, 3, 'UTC')) AS observed_at\n"
        "FROM corpscout.esef_document_company_information\n"
        f"{_FILTER}\n"
        "GROUP BY company_id"
    )


def esef_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'esef' AS source,\n"
        "    source_record_uid AS source_record_uid,\n"
        "    toDateTime64(resolved_at, 3, 'UTC') AS observed_at,\n"
        "    CAST(NULL AS Nullable(String)) AS legal_name,\n"
        "    CAST(NULL AS Nullable(String)) AS legal_form_code,\n"
        "    CAST(NULL AS Nullable(String)) AS status,\n"
        "    CAST(NULL AS Nullable(Date32)) AS incorporation_date,\n"
        "    nullIf(upperUTF8(trim(lei)), '') AS lei,\n"
        "    CAST(NULL AS Nullable(String)) AS wikidata_id,\n"
        "    trim(company_description) AS description,\n"
        "    if(toString(description_language) = '', 'en', toString(description_language)) AS description_language,\n"
        "    CAST(NULL AS Nullable(String)) AS description_sv\n"
        "FROM corpscout.esef_document_company_information\n"
        f"{_FILTER}\n"
        "  AND company_id IN %(company_ids)s\n"
        "ORDER BY resolved_at DESC, fiscal_year DESC, source_record_uid DESC\n"
        "LIMIT 1 BY company_id"
    )


se_basic_info_suggestions_esef = define_suggestion_asset(
    source="esef",
    extractor_version=ESEF_EXTRACTOR_VERSION,
    current_sql=esef_current_sql(),
    select_sql=esef_select_sql(),
    deps=[dg.AssetKey("esef_document_company_information_clickhouse")],
    description=(
        "One esef suggestion row per company from the newest ESEF filing extraction: the "
        "LEI and the filing's company description with its language. execute=false previews."
    ),
)
