"""Contracts for the country-agnostic TED procurement source.

The core (client, parser, publish SQL) is country-free; countries are rows in
COUNTRIES plus an optional national-id normalization rule. Column orders are
load-bearing against migration 000148. See docs/ted_procurement-design.md.
"""

from __future__ import annotations

from dataclasses import dataclass

SOURCE_SLUG = "ted_procurement"

DLT_DATASET_NAME = "ted_procurement"
DUCKDB_FILE_NAME = "ted_procurement_source.duckdb"

S3_BUCKET = "source-ted-procurement"

TED_PROCUREMENT_DATABASE = "corpscout"
TED_NOTICES_TABLE = "ted_notices"
TED_NOTICE_WINNERS_TABLE = "ted_notice_winners"
QUALIFIED_TED_NOTICES_TABLE = f"{TED_PROCUREMENT_DATABASE}.{TED_NOTICES_TABLE}"
QUALIFIED_TED_NOTICE_WINNERS_TABLE = (
    f"{TED_PROCUREMENT_DATABASE}.{TED_NOTICE_WINNERS_TABLE}"
)

NOTICES_TABLE = "notices"
NOTICE_WINNERS_TABLE = "notice_winners"

SEARCH_API_URL = "https://api.ted.europa.eu/v3/notices/search"
NOTICE_XML_URL_TEMPLATE = "https://ted.europa.eu/en/notice/{publication_number}/xml"
SEARCH_PAGE_LIMIT = 250

# Award-carrying eForms notice types ingested in v1. Contract notices
# (pre-award) are out of scope for the company↔contract graph.
NOTICE_TYPES = ("can-standard", "can-social", "can-desg")

LISTING_FIELDS = (
    "publication-number",
    "publication-date",
    "notice-type",
    "buyer-name",
    "notice-title",
    "total-value",
    "total-value-cur",
    "place-of-performance",
)


@dataclass(frozen=True)
class TedCountry:
    """One ingested country. Adding a country = adding a row to COUNTRIES."""

    place_code: str  # TED place-of-performance code, e.g. "FIN"
    country_iso2: str  # our country key, e.g. "FI"


COUNTRIES: tuple[TedCountry, ...] = (
    TedCountry(place_code="FIN", country_iso2="FI"),
    TedCountry(place_code="SWE", country_iso2="SE"),
)

# National-id normalization rules keyed by the organization's country code as
# it appears in eForms (ISO 3166-1 alpha-3 or alpha-2 depending on notice).
# Each rule is (regex, replacement) applied when the regex fully matches;
# unmatched ids pass through verbatim (kept in *_raw regardless).
NATIONAL_ID_NORMALIZATION: dict[str, tuple[str, str]] = {
    # Finnish VAT form FI12345678 -> Y-tunnus 1234567-8.
    "FIN": (r"^FI(\d{7})(\d)$", r"\1-\2"),
    "FI": (r"^FI(\d{7})(\d)$", r"\1-\2"),
}


def s3_partition_prefix(*, country_iso2: str, month: str) -> str:
    """Object prefix for one (country, month) partition.

    Country leads the path so a single country's snapshots can be listed,
    audited, or expired without walking every month of every other country.
    """
    return f"monthly/country={country_iso2}/partition={month}/"


# Column order must match the DuckDB notices table and the 000148 migration.
TED_NOTICES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "publication_number",
    "publication_date",
    "notice_type",
    "place_country",
    "buyer_name",
    "buyer_org_ref",
    "buyer_national_id_raw",
    "buyer_national_id",
    "buyer_country",
    "notice_title",
    "total_value_amount_original",
    "total_value_amount_usd",
    "total_value_currency",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "partition_key",
    "resolved_at",
)

# Column order must match the DuckDB winners table and the 000148 migration.
TED_NOTICE_WINNERS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "publication_number",
    "lot_id",
    "tender_id",
    "winner_ordinal",
    "winner_name",
    "winner_national_id_raw",
    "winner_national_id",
    "winner_country",
    "awarded_amount_original",
    "awarded_amount_usd",
    "awarded_currency",
    "buyer_national_id",
    "place_country",
    "publication_date",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "partition_key",
    "resolved_at",
)
