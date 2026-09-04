"""Ratsit's newest normalized report -> basic-info suggestion: name, status text mapped
to active/inactive, the Swedish business description. Ratsit's legal_form is free text of
another vocabulary and has no precedence, so it is not supplied."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import define_suggestion_asset
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

RATSIT_EXTRACTOR_VERSION = "ratsit-v1"
RATSIT_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}

_DESCRIPTION = "nullIf(trim(ifNull(business_description, '')), '')"


def ratsit_current_sql() -> str:
    return (
        "SELECT company_id, toDateTime64(max(normalized_at), 3, 'UTC') AS observed_at\n"
        "FROM corpscout.se_ratsit_company FINAL\n"
        "WHERE normalizer_version = %(normalizer_version)s\n"
        "GROUP BY company_id"
    )


def ratsit_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'ratsit' AS source,\n"
        "    concat('ratsit:', toString(result_sha256)) AS source_record_uid,\n"
        "    toDateTime64(normalized_at, 3, 'UTC') AS observed_at,\n"
        "    nullIf(trim(name), '') AS legal_name,\n"
        "    CAST(NULL AS Nullable(String)) AS legal_form_code,\n"
        "    multiIf(status IS NULL, NULL, startsWith(status, 'Aktiv'), 'active', 'inactive') AS status,\n"
        "    CAST(NULL AS Nullable(Date32)) AS incorporation_date,\n"
        "    CAST(NULL AS Nullable(String)) AS lei,\n"
        "    CAST(NULL AS Nullable(String)) AS wikidata_id,\n"
        f"    {_DESCRIPTION} AS description,\n"
        f"    if({_DESCRIPTION} IS NULL, NULL, 'sv') AS description_language,\n"
        f"    {_DESCRIPTION} AS description_sv\n"
        "FROM corpscout.se_ratsit_company FINAL\n"
        "WHERE normalizer_version = %(normalizer_version)s AND company_id IN %(company_ids)s\n"
        "ORDER BY normalized_at DESC, result_sha256 DESC\n"
        "LIMIT 1 BY company_id"
    )


se_basic_info_suggestions_ratsit = define_suggestion_asset(
    source="ratsit",
    extractor_version=RATSIT_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    select_params=RATSIT_SELECT_PARAMS,
    deps=[dg.AssetKey("se_ratsit_normalized")],
    description=(
        "One ratsit suggestion row per company from the newest normalized Ratsit report: "
        "name, active/inactive from the status text, the Swedish business description as "
        "both description (language sv) and description_sv. execute=false previews."
    ),
)
