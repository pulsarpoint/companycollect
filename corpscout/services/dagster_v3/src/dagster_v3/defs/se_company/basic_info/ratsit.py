"""Ratsit's newest normalized report -> basic-info suggestion: name, status text mapped
to active/inactive, the Swedish business description and its English translation. Both
texts are columns of corpscout.se_ratsit_company_translated (migration 000390); the
extractor never joins text_translations itself. Ratsit's legal_form is free text of
another vocabulary and has no precedence, so it is not supplied."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import define_suggestion_asset
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

# v2 (2026-09-07): description is the English translation when the view carries one.
RATSIT_EXTRACTOR_VERSION = "ratsit-v2"
RATSIT_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}
TRANSLATED_TABLE = "corpscout.se_ratsit_company_translated"

_DESCRIPTION = "nullIf(trim(ifNull(business_description, '')), '')"
_NORMALIZED_AT = "toDateTime64(normalized_at, 3, 'UTC')"
# The translation is a second input to observed_at (see bolagsverket.py for the why); the
# text guards the stamp because under join_use_nulls = 1 an untranslated row's stamp is
# NULL and a bare greatest would be NULL.
_OBSERVED_AT = (
    f"greatest({_NORMALIZED_AT}, if(business_description_en != '', "
    f"ifNull(business_description_translated_at, {_NORMALIZED_AT}), {_NORMALIZED_AT}))"
)


def ratsit_current_sql() -> str:
    # The newest report per company, stamped exactly as ratsit_select_sql stamps it. A
    # max() over every report would keep re-selecting a company whose OLDER report's text
    # was translated after its newest report, because the SELECT never writes that stamp.
    return (
        "SELECT company_id, observed_at\n"
        "FROM (\n"
        "    SELECT\n"
        "        company_id AS company_id,\n"
        f"        {_OBSERVED_AT} AS observed_at\n"
        f"    FROM {TRANSLATED_TABLE} FINAL\n"
        "    WHERE normalizer_version = %(normalizer_version)s\n"
        "    ORDER BY normalized_at DESC, result_sha256 DESC\n"
        "    LIMIT 1 BY company_id\n"
        ")"
    )


def ratsit_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'ratsit' AS source,\n"
        "    concat('ratsit:', toString(result_sha256)) AS source_record_uid,\n"
        f"    {_OBSERVED_AT} AS observed_at,\n"
        "    nullIf(trim(name), '') AS legal_name,\n"
        "    CAST(NULL AS Nullable(String)) AS legal_form_code,\n"
        "    multiIf(status IS NULL, NULL, startsWith(status, 'Aktiv'), 'active', 'inactive') AS status,\n"
        "    CAST(NULL AS Nullable(String)) AS economic_activity,\n"
        "    CAST(NULL AS Nullable(Date32)) AS incorporation_date,\n"
        "    CAST(NULL AS Nullable(String)) AS lei,\n"
        "    CAST(NULL AS Nullable(String)) AS wikidata_id,\n"
        f"    if(business_description_en != '', business_description_en, {_DESCRIPTION}) AS description,\n"
        f"    if(business_description_en != '', 'en', if({_DESCRIPTION} IS NULL, NULL, 'sv')) AS description_language,\n"
        f"    {_DESCRIPTION} AS description_sv\n"
        f"FROM {TRANSLATED_TABLE} FINAL\n"
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
        "One ratsit suggestion row per company from the newest normalized Ratsit report "
        "(se_ratsit_company_translated): name, active/inactive from the status text, the "
        "English business description when the view carries a translation else the Swedish "
        "one, and the Swedish text as description_sv. observed_at is the later of the "
        "report's stamp and the translation's. execute=false previews."
    ),
)
