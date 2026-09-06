"""SCB register record -> raw address suggestion (spec section 7): the four delivered
columns as delivered, kind visiting_or_postal, a tombstone row when SCB stopped delivering."""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import define_address_suggestion_asset
from dagster_v3.defs.se_company.basic_info.scb import SCB_RECORD_UID_SQL

SCB_ADDRESS_EXTRACTOR_VERSION = "scb-address-v1"


def _delivered(column: str) -> str:
    return f"if(has_company = 1, nullIf(trim(ifNull({column}, '')), ''), CAST(NULL AS Nullable(String))) AS {column}"


def scb_current_sql() -> str:
    # Tombstones included: a company SCB stopped delivering must write a NULL row.
    return "SELECT company_id, observed_at FROM corpscout.se_scb_companies FINAL"


def scb_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'scb' AS source,\n"
        "    '' AS slot,\n"
        f"    {SCB_RECORD_UID_SQL} AS source_record_uid,\n"
        "    observed_at AS observed_at,\n"
        "    'visiting_or_postal' AS kind,\n"
        "    CAST(NULL AS Nullable(String)) AS raw_address,\n"
        f"    {_delivered('care_of')},\n"
        f"    {_delivered('street_address')},\n"
        f"    {_delivered('postal_code')},\n"
        f"    {_delivered('post_town')},\n"
        "    CAST(NULL AS Nullable(String)) AS county,\n"
        "    CAST(NULL AS Nullable(String)) AS country_code\n"
        "FROM corpscout.se_scb_companies FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


se_company_address_suggestions_scb = define_address_suggestion_asset(
    source="scb",
    extractor_version=SCB_ADDRESS_EXTRACTOR_VERSION,
    current_sql=scb_current_sql(),
    select_sql=scb_select_sql(),
    deps=[dg.AssetKey("sweden_company_scb_companies_clickhouse")],
    description=(
        "SCB's care-of, street, postcode and town as delivered into se_company_address_suggestion "
        "(kind visiting_or_postal, slot ''); a tombstoned company writes a NULL row."
    ),
)
