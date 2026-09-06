"""Bolagsverket register record -> raw address suggestion (spec section 7): the packed
postal_address string as delivered, kind postal; the normalizer does the parsing."""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import define_address_suggestion_asset

BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION = "bolagsverket-address-v1"

BOLAGSVERKET_ADDRESS_RECORD_UID_SQL = (
    "lower(hex(SHA256(concat('company-source-record-v1\\nstructured\\n', 'sweden_bolagsverket', "
    "'\\nregistry_company\\n', source_record_id, '\\n', lowerUTF8(source_payload_hash)))))"
)


def bolagsverket_current_sql() -> str:
    # The register row alone: addresses do not depend on text_translations, and tombstones
    # are included so a company Bolagsverket stopped delivering writes a NULL row.
    return "SELECT company_id, observed_at FROM corpscout.se_bolagsverket_companies FINAL"


def bolagsverket_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'bolagsverket' AS source,\n"
        "    '' AS slot,\n"
        f"    {BOLAGSVERKET_ADDRESS_RECORD_UID_SQL} AS source_record_uid,\n"
        "    observed_at AS observed_at,\n"
        "    'postal' AS kind,\n"
        "    if(has_company = 1, nullIf(trim(ifNull(postal_address, '')), ''), CAST(NULL AS Nullable(String))) AS raw_address,\n"
        "    CAST(NULL AS Nullable(String)) AS care_of,\n"
        "    CAST(NULL AS Nullable(String)) AS street_address,\n"
        "    CAST(NULL AS Nullable(String)) AS postal_code,\n"
        "    CAST(NULL AS Nullable(String)) AS post_town,\n"
        "    CAST(NULL AS Nullable(String)) AS county,\n"
        "    CAST(NULL AS Nullable(String)) AS country_code\n"
        "FROM corpscout.se_bolagsverket_companies FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


se_company_address_suggestions_bolagsverket = define_address_suggestion_asset(
    source="bolagsverket",
    extractor_version=BOLAGSVERKET_ADDRESS_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=bolagsverket_select_sql(),
    deps=[dg.AssetKey("sweden_company_bolagsverket_companies_clickhouse")],
    description=(
        "Bolagsverket's packed postal address (street$care-of$town$postcode$country) as delivered into "
        "se_company_address_suggestion (kind postal, slot ''); a tombstoned company writes a NULL row."
    ),
)
