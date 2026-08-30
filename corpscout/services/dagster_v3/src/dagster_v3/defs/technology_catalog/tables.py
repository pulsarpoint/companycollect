"""Names and contracts for the technology catalog publish.

The ClickHouse schema is owned by migration 000350; the column order here is
the insert contract with that migration and is pinned by
tests/test_technology_catalog.py.
"""

TECHNOLOGY_CATALOG_TABLE = "technology_catalog"

TECHNOLOGY_FINGERPRINTS_TABLE = "technology_fingerprints"

TECHNOLOGY_CATALOG_TABLES = (
    TECHNOLOGY_CATALOG_TABLE,
    TECHNOLOGY_FINGERPRINTS_TABLE,
)

# Column order is the contract with migration 000350.
TECHNOLOGY_CATALOG_COLUMNS = (
    "technology",
    "slug",
    "description",
    "website",
    "category_ids",
    "categories",
    "groups",
    "icon_object_key",
    "icon_content_type",
    "saas",
    "oss",
    "pricing",
    "source",
    "source_version",
    "source_run_id",
    "updated_at",
)

# The frozen extension bundle alone carries 7,278 technologies and the overlay
# only ever adds names, so a result below this floor means a broken merge or a
# truncated fetch, never a legitimate catalog. Refuse to swap rather than
# shrink the table the technology pages read.
MIN_TECHNOLOGY_CATALOG_ROWS = 5_000

# Column order is the contract with migration 000357.
TECHNOLOGY_FINGERPRINTS_COLUMNS = (
    "technology",
    "signal_type",
    "pattern",
    "confidence",
    "version_template",
    "source",
    "source_version",
    "source_run_id",
    "updated_at",
)

# The public layers carry ~100 dns patterns and our custom entries ~25 more; a
# result below this floor means a broken extraction, never a legitimate set.
MIN_TECHNOLOGY_FINGERPRINT_ROWS = 50

DOMAIN_SIGNAL_TECHNOLOGIES_TABLE = "domain_signal_technologies"

# Column order is the contract with migration 000358.
DOMAIN_SIGNAL_TECHNOLOGIES_COLUMNS = (
    "root_domain",
    "technology",
    "signal_type",
    "matched_pattern",
    "evidence",
    "record_name",
    "confidence",
    "source",
    "source_run_id",
    "detected_at",
)

# Google Workspace MX alone matches ~4M crawled domains; a result below this
# floor means a broken detection pass, never a legitimate scan of the DNS set.
MIN_DOMAIN_SIGNAL_TECHNOLOGY_ROWS = 100_000

# Dedicated bucket for technology icons. This module creates it if absent and
# only ever adds objects to it; no other bucket is touched.
ICON_BUCKET = "technology-icons"
ICON_KEY_PREFIX = "icons/"

# The vendored Wappalyzer extension bundle (frozen bootstrap layer).
EXTENSION_SOURCE = "extension-6.12.5"
EXTENSION_VERSION = "6.12.5"

# The maintained public continuation of the wappalyzer catalog (weekly
# overlay). source_version for these rows is the pinned git commit SHA.
OVERLAY_SOURCE = "webappanalyzer"

# Our curated entries (topmost layer, wins over both public layers).
# source_version for these rows is the content hash of the custom files.
CUSTOM_SOURCE = "custom"

# Custom category ids live at 900+ so upstream vocabularies can never collide.
CUSTOM_CATEGORY_ID_FLOOR = 900
