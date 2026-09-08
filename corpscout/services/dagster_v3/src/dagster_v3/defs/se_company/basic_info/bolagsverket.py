"""Bolagsverket register record -> basic-info suggestion: legal name, the organisationsform
token mapped to SCB's juridisk form code (legal_form.py), status, registration date, the
Swedish activity description and its English translation.

The register is read through corpscout.se_bolagsverket_companies_translated (migration
000390): the translation pipeline keys text_translations on the register table itself and
the view joins it back as activity_description_en plus its stamp, so this extractor
selects columns and never joins text_translations."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import define_suggestion_asset
from dagster_v3.defs.se_company.common import bolagsverket_record_uid_sql
from dagster_v3.defs.se_company.basic_info.legal_form import bolagsverket_legal_form_sql

# v2 (2026-09-04): legal_form_code is the SCB code, not the raw -ORGFO token.
# v3 (2026-09-07): reads the _translated view; translations keyed on the register table.
BOLAGSVERKET_EXTRACTOR_VERSION = "bolagsverket-v3"
TRANSLATED_TABLE = "corpscout.se_bolagsverket_companies_translated"

BOLAGSVERKET_RECORD_UID_SQL = bolagsverket_record_uid_sql("register")

_ACTIVITY_SV = "nullIf(trim(ifNull(register.activity_description, '')), '')"
# The register row is not the only input: the translation pipeline fills text_translations
# asynchronously. If observed_at were the register's alone, a company whose Swedish text
# was translated after its last extraction would keep description_language = 'sv' and the
# Swedish text on an English-facing field until its register record next changed.
# observed_at is therefore the later of the two stamps, and the same expression appears
# in current_sql, so the change scan re-selects a company when only its translation is
# new. The text guards the stamp: under join_use_nulls = 1 an untranslated row's stamp is
# NULL and a bare greatest would be NULL.
_TRANSLATED_AT_SQL = (
    "if(register.activity_description_en != '', "
    "ifNull(register.activity_description_translated_at, register.observed_at), register.observed_at)"
)
_OBSERVED_AT_SQL = f"greatest(register.observed_at, {_TRANSLATED_AT_SQL})"


def bolagsverket_current_sql() -> str:
    return (
        "SELECT\n"
        "    register.company_id AS company_id,\n"
        f"    {_OBSERVED_AT_SQL} AS observed_at\n"
        f"FROM {TRANSLATED_TABLE} AS register FINAL\n"
        "WHERE has_company = 1"
    )


def bolagsverket_select_sql() -> str:
    return (
        "SELECT\n"
        "    register.company_id AS company_id,\n"
        "    'bolagsverket' AS source,\n"
        f"    {BOLAGSVERKET_RECORD_UID_SQL} AS source_record_uid,\n"
        f"    {_OBSERVED_AT_SQL} AS observed_at,\n"
        "    nullIf(trim(ifNull(register.legal_name, '')), '') AS legal_name,\n"
        f"    {bolagsverket_legal_form_sql('register.legal_form_code')} AS legal_form_code,\n"
        "    if(register.deregistration_date IS NULL, 'active', 'inactive') AS status,\n"
        "    CAST(NULL AS Nullable(String)) AS economic_activity,\n"
        "    register.registration_date AS incorporation_date,\n"
        "    CAST(NULL AS Nullable(String)) AS lei,\n"
        "    CAST(NULL AS Nullable(String)) AS wikidata_id,\n"
        f"    if(register.activity_description_en != '', register.activity_description_en, {_ACTIVITY_SV}) AS description,\n"
        f"    if(register.activity_description_en != '', 'en', if({_ACTIVITY_SV} IS NULL, NULL, 'sv')) AS description_language,\n"
        f"    {_ACTIVITY_SV} AS description_sv\n"
        f"FROM {TRANSLATED_TABLE} AS register FINAL\n"
        "WHERE has_company = 1 AND company_id IN %(company_ids)s"
    )


se_basic_info_suggestions_bolagsverket = define_suggestion_asset(
    source="bolagsverket",
    extractor_version=BOLAGSVERKET_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=bolagsverket_select_sql(),
    deps=[dg.AssetKey("sweden_company_bolagsverket_companies_clickhouse")],
    description=(
        "One bolagsverket suggestion row per company from se_bolagsverket_companies_translated: "
        "legal name, the organisationsform token mapped to SCB's juridisk form code (an unknown "
        "token passes through), active/inactive from the deregistration date, registration "
        "date, the Swedish activity description and its English translation when the view "
        "carries one. observed_at is the later of the register row's stamp and the "
        "translation's, so a newly translated text re-selects the company. execute=false previews."
    ),
)
