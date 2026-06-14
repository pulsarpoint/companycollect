"""Declarative source config for Finland PRH XBRL financial statements."""

SOURCE_NAME = "finland_prh_xbrl"
COUNTRY = "finland"
SOURCE_SLUG = "prh_xbrl"
DISPLAY_NAME = "Finland PRH XBRL"
ASSET_KEY_PREFIX = ["sources", COUNTRY, SOURCE_SLUG]
GROUP_NAME = f"source_{COUNTRY}_{SOURCE_SLUG}"
TAGS = {
    "country": COUNTRY,
    "source": SOURCE_SLUG,
    "source_name": SOURCE_NAME,
}

BUCKET = "source-finland-prh-xbrl"
BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
USER_AGENT = "corpscout-dagster/0.1"
PARSER_VERSION = "1.0.0"
# PRH publishes digitally filed statements registered from 2025 onward.
PARTITION_START_DATE = "2025-01-01"


def document_object_key(business_id: str, financial_date: str) -> str:
    """Deterministic, company-keyed, readable. Re-downloads overwrite in place."""
    return f"companies/{business_id}/{financial_date}.xml"


def window_listing_object_key(partition_key: str) -> str:
    """Raw discovery payload for one registration-month window."""
    return f"windows/{partition_key}/listing.json"
