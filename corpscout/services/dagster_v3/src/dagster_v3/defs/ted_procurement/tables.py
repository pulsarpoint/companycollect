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
TED_NOTICE_LOTS_TABLE = "ted_notice_lots"
TED_NOTICE_WINNERS_TABLE = "ted_notice_winners"
QUALIFIED_TED_NOTICES_TABLE = f"{TED_PROCUREMENT_DATABASE}.{TED_NOTICES_TABLE}"
QUALIFIED_TED_NOTICE_LOTS_TABLE = f"{TED_PROCUREMENT_DATABASE}.{TED_NOTICE_LOTS_TABLE}"
QUALIFIED_TED_NOTICE_WINNERS_TABLE = (
    f"{TED_PROCUREMENT_DATABASE}.{TED_NOTICE_WINNERS_TABLE}"
)

NOTICES_TABLE = "notices"
NOTICE_LOTS_TABLE = "notice_lots"
NOTICE_WINNERS_TABLE = "notice_winners"

# The per-partition DuckDB that ted_monthly_duckdb writes and build_publish_tables
# reads. Declared once because it was previously spelled out in both the asset and
# the test fixture, and the two drifted the moment a table was added.
#
# Everything is varchar: this is the parser's output held verbatim, and casting
# happens in the publish SQL where a bad value becomes NULL via try_cast rather
# than failing a month's load.
PARTITION_TABLE_DDL: dict[str, str] = {
    "listing": (
        "publication_number varchar, publication_date varchar, notice_type varchar, "
        "buyer_name varchar, notice_title varchar, total_value varchar, "
        "total_value_currency varchar, country_iso2 varchar, place_country varchar"
    ),
    "notice_docs": (
        "publication_number varchar, buyer_org_ref varchar, "
        "cpv_code varchar, cpv_additional_codes varchar[], "
        "estimated_value varchar, estimated_value_currency varchar, "
        "framework_maximum varchar, framework_maximum_currency varchar, "
        "framework_total_maximum varchar, framework_total_maximum_currency varchar, "
        "framework_total_approximate varchar, "
        "framework_total_approximate_currency varchar"
    ),
    "organizations": (
        "publication_number varchar, org_ref varchar, name varchar, "
        "national_id_raw varchar, national_id varchar, country varchar"
    ),
    "lots": (
        "publication_number varchar, lot_id varchar, lot_title varchar, "
        "cpv_code varchar, cpv_additional_codes varchar[], "
        "estimated_value varchar, estimated_value_currency varchar, "
        "framework_maximum varchar, framework_maximum_currency varchar, "
        "framework_value_maximum varchar, framework_value_maximum_currency varchar, "
        "framework_value_reestimated varchar, "
        "framework_value_reestimated_currency varchar, "
        "lower_tender varchar, lower_tender_currency varchar, "
        "higher_tender varchar, higher_tender_currency varchar"
    ),
    "winner_links": (
        "publication_number varchar, lot_id varchar, tender_id varchar, "
        "winner_ordinal integer, org_ref varchar, awarded_amount varchar, "
        "awarded_currency varchar, subcontracting_amount varchar, "
        "subcontracting_currency varchar"
    ),
}


def partition_column_count(table: str) -> int:
    """How many placeholders an insert into a partition table needs."""
    return PARTITION_TABLE_DDL[table].count(",") + 1


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
    TedCountry(place_code="NOR", country_iso2="NO"),
    TedCountry(place_code="FRA", country_iso2="FR"),
    TedCountry(place_code="SVK", country_iso2="SK"),
    TedCountry(place_code="LVA", country_iso2="LV"),
    TedCountry(place_code="DNK", country_iso2="DK"),
    TedCountry(place_code="EST", country_iso2="EE"),
)

# National-id normalization rules keyed by the organization's country code as
# it appears in eForms (ISO 3166-1 alpha-3 or alpha-2 depending on notice).
# Each rule is (regex, replacement) applied when the regex fully matches;
# unmatched ids pass through verbatim (kept in *_raw regardless).
NATIONAL_ID_NORMALIZATION: dict[str, tuple[str, str]] = {
    # Finnish VAT form FI12345678 -> Y-tunnus 1234567-8.
    "FIN": (r"^FI(\d{7})(\d)$", r"\1-\2"),
    "FI": (r"^FI(\d{7})(\d)$", r"\1-\2"),
    # Norwegian organisasjonsnummer is 9 bare digits, which is exactly the
    # no_companies.org_number format -- TED already publishes it that way
    # (verified against live notices), so this rule mostly passes values
    # through untouched. It also folds the VAT form NO<9 digits>MVA and the
    # space-grouped form seen in other sources. Anything that is not 9 digits
    # falls through verbatim rather than being coerced into a wrong match.
    "NOR": (r"^(?:NO)?\s*(\d{3})\s*(\d{3})\s*(\d{3})\s*(?:MVA)?$", r"\1\2\3"),
    "NO": (r"^(?:NO)?\s*(\d{3})\s*(\d{3})\s*(\d{3})\s*(?:MVA)?$", r"\1\2\3"),
}


def s3_partition_prefix(*, country_iso2: str, month: str) -> str:
    """Object prefix for one (country, month) partition.

    Country leads the path so a single country's snapshots can be listed,
    audited, or expired without walking every month of every other country.
    """
    return f"monthly/country={country_iso2}/partition={month}/"


# Every monetary figure the notice publishes, as (metric, eForms business term).
# Each becomes <metric>_amount_original + <metric>_amount_usd + <metric>_currency.
# Driving the schema off these tuples rather than listing columns by hand is what
# keeps "store all of them" structural: adding a business term is one line.
#
# total_value arrives from the search API's listing rather than the XML; the rest
# are parsed. They are deliberately separate columns and never coalesced --
# an estimate, a ceiling and a realized award are different claims, and a column
# holding whichever happened to be present makes all three unreadable.
TED_NOTICE_VALUE_METRICS = (
    ("total_value", "BT-161 notice value, realized"),
    ("estimated_value", "BT-27 estimated value of the procedure"),
    ("framework_maximum", "BT-709 framework maximum value"),
    ("framework_total_maximum", "BT-118 maximum of all framework contracts"),
    ("framework_total_approximate", "BT-1118 approximate total of framework contracts"),
)

TED_LOT_VALUE_METRICS = (
    ("estimated_value", "BT-27 estimated value of this lot"),
    ("framework_maximum", "BT-709 framework maximum for this lot"),
    ("framework_value_maximum", "BT-271 framework ceiling stated on the award"),
    ("framework_value_reestimated", "BT-660 revised framework estimate"),
    ("lower_tender", "BT-710 lowest admissible tender received"),
    ("higher_tender", "BT-711 highest admissible tender received"),
)

TED_WINNER_VALUE_METRICS = (
    ("awarded", "BT-720 tender value, realized, per winner"),
    ("subcontracting", "BT-553 value to be subcontracted"),
)


def value_columns(metrics: tuple[tuple[str, str], ...]) -> tuple[str, ...]:
    """The three columns each monetary metric contributes, in schema order."""
    return tuple(
        column
        for metric, _ in metrics
        for column in (
            f"{metric}_amount_original",
            f"{metric}_amount_usd",
            f"{metric}_currency",
        )
    )


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
    # BT-262 / BT-263: what the procedure buys. TED is the register that defines
    # CPV, so this is the authoritative classification for every EU country.
    "cpv_code",
    "cpv_additional_codes",
    *value_columns(TED_NOTICE_VALUE_METRICS),
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "partition_key",
    "resolved_at",
)

TED_NOTICE_LOTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "publication_number",
    "lot_id",
    "lot_title",
    # Per-lot classification: a multi-lot notice routinely splits across
    # unrelated CPVs, so the lot code is not a copy of the notice's.
    "cpv_code",
    "cpv_additional_codes",
    *value_columns(TED_LOT_VALUE_METRICS),
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "publication_date",
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
    *value_columns(TED_WINNER_VALUE_METRICS),
    "buyer_national_id",
    "place_country",
    "publication_date",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "partition_key",
    "resolved_at",
)
