"""Ratsit's normalized company report -> raw address suggestion (spec section 7): the
company address, newest report per company, kind postal, slot company."""

import dagster as dg

from dagster_v3.defs.se_company.address.suggestions import define_address_suggestion_asset
from dagster_v3.defs.se_company.basic_info.ratsit import ratsit_current_sql as _basic_info_ratsit_current_sql
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

RATSIT_ADDRESS_EXTRACTOR_VERSION = "ratsit-address-v1"
RATSIT_ADDRESS_SELECT_PARAMS = {"normalizer_version": RATSIT_NORMALIZER_VERSION}


def ratsit_current_sql() -> str:
    return _basic_info_ratsit_current_sql()


def ratsit_select_sql() -> str:
    return (
        "SELECT\n"
        "    company_id AS company_id,\n"
        "    'ratsit' AS source,\n"
        "    'company' AS slot,\n"
        "    concat('ratsit:', toString(result_sha256)) AS source_record_uid,\n"
        "    toDateTime64(normalized_at, 3, 'UTC') AS observed_at,\n"
        "    'postal' AS kind,\n"
        "    CAST(NULL AS Nullable(String)) AS raw_address,\n"
        "    CAST(NULL AS Nullable(String)) AS care_of,\n"
        "    nullIf(trim(ifNull(address_street, '')), '') AS street_address,\n"
        "    nullIf(trim(ifNull(address_postal_code, '')), '') AS postal_code,\n"
        "    nullIf(trim(ifNull(address_locality, '')), '') AS post_town,\n"
        "    nullIf(trim(ifNull(address_county, '')), '') AS county,\n"
        "    CAST(NULL AS Nullable(String)) AS country_code\n"
        "FROM corpscout.se_ratsit_company FINAL\n"
        "WHERE normalizer_version = %(normalizer_version)s AND company_id IN %(company_ids)s\n"
        "ORDER BY normalized_at DESC, result_sha256 DESC\n"
        "LIMIT 1 BY company_id"
    )


se_company_address_suggestions_ratsit = define_address_suggestion_asset(
    source="ratsit",
    extractor_version=RATSIT_ADDRESS_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    select_params=RATSIT_ADDRESS_SELECT_PARAMS,
    deps=[dg.AssetKey("se_ratsit_normalized")],
    description=(
        "Ratsit's company address from the newest normalized report into se_company_address_suggestion "
        "(kind postal, slot company); the care-of Ratsit glues onto the street is split by the normalizer."
    ),
)
