"""Bolagsverket register record -> basic-info suggestion, with the register's Swedish
activity description and its English translation from the translation pipeline."""

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import define_suggestion_asset

BOLAGSVERKET_EXTRACTOR_VERSION = "bolagsverket-v1"

BOLAGSVERKET_RECORD_UID_SQL = (
    "lower(hex(SHA256(concat('company-source-record-v1\\nstructured\\n', 'sweden_bolagsverket', "
    "'\\nregistry_company\\n', register.source_record_id, '\\n', lowerUTF8(register.source_payload_hash)))))"
)


# The register row is not the only input: `description`/`description_language` also depend
# on corpscout.text_translations, which the translation pipeline fills asynchronously. If
# observed_at were the register's alone, a company whose Swedish text was translated after
# its last extraction would keep description_language = 'sv' and the Swedish text on an
# English-facing field until its register record next changed. observed_at is therefore the
# later of the two inputs' stamps, and the same expression appears in current_sql, so the
# change scan re-selects a company when only its translation is new.
_OBSERVED_AT_SQL = "greatest(register.observed_at, ifNull(translation.translated_at, register.observed_at))"
_JOIN_SQL = (
    "FROM register\n"
    "LEFT JOIN translations AS translation\n"
    "    ON translation.source_text_hash = cityHash64(ifNull(register.activity_sv, ''))"
)


def _bolagsverket_ctes(*, scoped: bool) -> str:
    """The register rows and their translations.

    text_translations is keyed by the translation pipeline on the se_companies spine's
    activity_description (source_table 'corpscout.se_companies'), whose text is the same
    trimmed verksamhetsbeskrivning this table holds, so cityHash64 of the text finds it.
    `version` is the translation's own unix-second stamp (DEFAULT now()), so max(version)
    is when this text was last translated.
    """
    scope = " AND company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH register AS (\n"
        "    SELECT company_id, source_record_id, source_payload_hash, observed_at, legal_name,\n"
        "        legal_form_code, registration_date, deregistration_date,\n"
        "        nullIf(trim(ifNull(activity_description, '')), '') AS activity_sv\n"
        "    FROM corpscout.se_bolagsverket_companies FINAL\n"
        f"    WHERE has_company = 1{scope}\n"
        "),\n"
        "translations AS (\n"
        "    SELECT source_text_hash, argMax(translated_text, version) AS translated_text,\n"
        "        toDateTime64(max(version), 3, 'UTC') AS translated_at\n"
        "    FROM corpscout.text_translations\n"
        "    WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description'\n"
        "      AND source_lang = 'sv' AND target_lang = 'en'\n"
        "      AND source_text_hash IN (SELECT cityHash64(ifNull(activity_sv, '')) FROM register WHERE activity_sv IS NOT NULL)\n"
        "    GROUP BY source_text_hash\n"
        ")\n"
    )


def bolagsverket_current_sql() -> str:
    return (
        f"{_bolagsverket_ctes(scoped=False)}"
        "SELECT\n"
        "    register.company_id AS company_id,\n"
        f"    {_OBSERVED_AT_SQL} AS observed_at\n"
        f"{_JOIN_SQL}"
    )


def bolagsverket_select_sql() -> str:
    return (
        f"{_bolagsverket_ctes(scoped=True)}"
        "SELECT\n"
        "    register.company_id AS company_id,\n"
        "    'bolagsverket' AS source,\n"
        f"    {BOLAGSVERKET_RECORD_UID_SQL} AS source_record_uid,\n"
        f"    {_OBSERVED_AT_SQL} AS observed_at,\n"
        "    nullIf(trim(ifNull(register.legal_name, '')), '') AS legal_name,\n"
        "    nullIf(trim(ifNull(register.legal_form_code, '')), '') AS legal_form_code,\n"
        "    if(register.deregistration_date IS NULL, 'active', 'inactive') AS status,\n"
        "    register.registration_date AS incorporation_date,\n"
        "    CAST(NULL AS Nullable(String)) AS lei,\n"
        "    CAST(NULL AS Nullable(String)) AS wikidata_id,\n"
        "    if(ifNull(translation.translated_text, '') != '', translation.translated_text, register.activity_sv) AS description,\n"
        "    if(ifNull(translation.translated_text, '') != '', 'en', if(register.activity_sv IS NULL, NULL, 'sv')) AS description_language,\n"
        "    register.activity_sv AS description_sv\n"
        f"{_JOIN_SQL}"
    )


se_basic_info_suggestions_bolagsverket = define_suggestion_asset(
    source="bolagsverket",
    extractor_version=BOLAGSVERKET_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=bolagsverket_select_sql(),
    deps=[dg.AssetKey("sweden_company_bolagsverket_companies_clickhouse")],
    description=(
        "One bolagsverket suggestion row per company from se_bolagsverket_companies: legal "
        "name, organisationsform token, active/inactive from the deregistration date, "
        "registration date, the Swedish activity description and its English translation "
        "when text_translations has one. observed_at is the later of the register row's "
        "stamp and the translation's, so a newly translated text re-selects the company. "
        "execute=false previews."
    ),
)
