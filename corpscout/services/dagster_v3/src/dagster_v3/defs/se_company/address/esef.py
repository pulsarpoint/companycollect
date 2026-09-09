"""ESEF registered office -> raw address suggestion (spec 2026-09-09, section 3): the tagged
AddressOfRegisteredOfficeOfEntity fact of the company's newest filing, kind registered.

The fact is one line of free text ("Kungsträdgårdsgatan 2, 106 70 Stockholm", sometimes with
HTML spans, a trailing period or ", Sverige"). The normaliser parses either components or the
Bolagsverket packed string, so the SQL cleans the line and, when it finds a Swedish postcode,
re-packs it as street$care-of$town$postcode$country; a line without a postcode is delivered as
street_address alone and the normaliser's partial rules decide."""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import define_address_suggestion_asset

ESEF_ADDRESS_EXTRACTOR_VERSION = "esef-address-v1"

ESEF_RECORD_UID_SQL = (
    "lower(hex(SHA256(concat('company-source-record-v1\\nfile\\nesef_report_package\\n', "
    "lowerUTF8(filings.package_sha256)))))"
)

# Tags out, whitespace (ASCII plus NBSP/narrow-NBSP, which occur in prod facts) collapsed,
# trailing dots/spaces off, then the trailing country word.
_CLEANED_SQL = (
    "trim(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll(facts.raw_value, '<[^>]+>', ' '), "
    "'[\\\\s\\\\x{00A0}\\\\x{202F}]+', ' '), '[\\\\s.,]+$', ''))"
)
_SWEDISH_COUNTRY_SQL = f"match({_CLEANED_SQL}, '(?i)[,\\\\s]+(sverige|sweden)$')"
_BODY_SQL = f"replaceRegexpOne({_CLEANED_SQL}, '(?i)[,\\\\s]+(sverige|sweden)$', '')"
# street part | postcode 3 | postcode 2 | town: the first Swedish postcode, followed by a
# letter-led town, splits the line. The town group is bounded, not greedy to end-of-line: it
# starts with a non-digit/non-separator character (so a glued digit run after the postcode
# never gets read as part of the town) and stops at the first `,`/`.`/`:` or before a
# visiting-address / phone / country word, with the remainder discarded as trailing text.
_PARTS_SQL = (
    f"extractGroups({_BODY_SQL}, '^(.*?)[,\\\\s]*(?i:SE-)?([0-9]{{3}})\\\\s?([0-9]{{2}})\\\\s+"
    "([^0-9,.:\\\\s][^,.:]*?)(?:\\\\s*[,.:].*|\\\\s+(?i:besöksadress\\\\w*|och|med|tel|sverige|sweden)\\\\b.*)?$')"
)
# No postcode: the part after the last comma is the town ("Ideongatan 1, Lund").
_NO_CODE_PARTS_SQL = f"extractGroups({_BODY_SQL}, '^(.*),\\\\s*([^,]+)$')"

# The street and town parts are attacker/data controlled free text; replace any literal `$`
# so the five-part packed string can never be shifted by one embedded in a fact.
ESEF_PACKED_ADDRESS_SQL = (
    f"if(length({_PARTS_SQL}) = 4, concat(replaceAll(trim({_PARTS_SQL}[1]), '$', ' '), '$$', "
    f"replaceAll(trim({_PARTS_SQL}[4]), '$', ' '), '$', "
    f"{_PARTS_SQL}[2], {_PARTS_SQL}[3], '$', if({_SWEDISH_COUNTRY_SQL}, 'SE', '')), "
    "CAST(NULL AS Nullable(String)))"
)
_NO_CODE_STREET_SQL = (
    f"if(length({_PARTS_SQL}) = 4, CAST(NULL AS Nullable(String)), "
    f"if(length({_NO_CODE_PARTS_SQL}) = 2, nullIf(trim({_NO_CODE_PARTS_SQL}[1]), ''), nullIf({_BODY_SQL}, '')))"
)
_NO_CODE_TOWN_SQL = (
    f"if(length({_PARTS_SQL}) = 4 OR length({_NO_CODE_PARTS_SQL}) != 2, CAST(NULL AS Nullable(String)), "
    f"nullIf(trim({_NO_CODE_PARTS_SQL}[2]), ''))"
)


def esef_current_sql() -> str:
    return (
        "SELECT facts.company_id AS company_id, argMax(toDateTime64(filings.processed_at, 3, 'UTC'), "
        "(filings.period_end, toDateTime64(filings.processed_at, 3, 'UTC'))) AS observed_at\n"
        "FROM corpscout.se_esef_facts AS facts\n"
        "INNER JOIN corpscout.se_esef_filings AS filings ON filings.fxo_id = facts.fxo_id\n"
        "WHERE facts.concept_local_name = 'AddressOfRegisteredOfficeOfEntity' AND filings.processed_at IS NOT NULL\n"
        "GROUP BY facts.company_id"
    )


def esef_select_sql() -> str:
    return (
        "SELECT\n"
        "    facts.company_id AS company_id,\n"
        "    'esef' AS source,\n"
        "    '' AS slot,\n"
        f"    {ESEF_RECORD_UID_SQL} AS source_record_uid,\n"
        "    toDateTime64(filings.processed_at, 3, 'UTC') AS observed_at,\n"
        "    'registered' AS kind,\n"
        f"    {ESEF_PACKED_ADDRESS_SQL} AS raw_address,\n"
        "    CAST(NULL AS Nullable(String)) AS care_of,\n"
        f"    {_NO_CODE_STREET_SQL} AS street_address,\n"
        "    CAST(NULL AS Nullable(String)) AS postal_code,\n"
        f"    {_NO_CODE_TOWN_SQL} AS post_town,\n"
        "    CAST(NULL AS Nullable(String)) AS county,\n"
        f"    if({_SWEDISH_COUNTRY_SQL}, 'SE', CAST(NULL AS Nullable(String))) AS country_code\n"
        "FROM corpscout.se_esef_facts AS facts\n"
        "INNER JOIN corpscout.se_esef_filings AS filings ON filings.fxo_id = facts.fxo_id\n"
        "WHERE facts.concept_local_name = 'AddressOfRegisteredOfficeOfEntity'\n"
        "  AND filings.processed_at IS NOT NULL\n"
        "  AND facts.company_id IN %(company_ids)s\n"
        # The newest filing wins; inside one filing the Swedish text wins over an English twin.
        "ORDER BY filings.period_end DESC, filings.processed_at DESC, facts.language DESC, facts.fact_id\n"
        "LIMIT 1 BY facts.company_id"
    )


se_company_address_suggestions_esef = define_address_suggestion_asset(
    source="esef",
    extractor_version=ESEF_ADDRESS_EXTRACTOR_VERSION,
    current_sql=esef_current_sql(),
    select_sql=esef_select_sql(),
    deps=[dg.AssetKey("esef_facts_clickhouse"), dg.AssetKey("esef_entity_registry_map_clickhouse")],
    description=(
        "The registered office tagged in the company's newest ESEF filing (se_esef_facts through "
        "the register-verified link) into se_company_address_suggestion, kind registered, slot '': "
        "cleaned and re-packed for the normaliser when a Swedish postcode is found, delivered as a "
        "bare street line otherwise."
    ),
)
