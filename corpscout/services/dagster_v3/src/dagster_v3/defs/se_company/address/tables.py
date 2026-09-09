"""Table names and column tuples of the address entity, pinned against migrations
000382-000387; the main table was built as `se_company_address_v2` and renamed by 000393."""

DATABASE = "corpscout"
SUGGESTION_TABLE = "se_company_address_suggestion"
NORMALIZED_TABLE = "se_company_address_normalized"
MAIN_TABLE = "se_company_address"
HISTORY_TABLE = "se_company_address_history"
RULE_TABLE = "se_company_address_rule"
PRECEDENCE_TABLE = "se_company_address_precedence"

QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"
QUALIFIED_NORMALIZED_TABLE = f"{DATABASE}.{NORMALIZED_TABLE}"
QUALIFIED_MAIN_TABLE = f"{DATABASE}.{MAIN_TABLE}"
QUALIFIED_HISTORY_TABLE = f"{DATABASE}.{HISTORY_TABLE}"
QUALIFIED_RULE_TABLE = f"{DATABASE}.{RULE_TABLE}"
QUALIFIED_PRECEDENCE_TABLE = f"{DATABASE}.{PRECEDENCE_TABLE}"

SOURCES: tuple[str, ...] = ("scb", "bolagsverket", "ratsit", "esef", "reviewer", "reviewer_draft")
KINDS: tuple[str, ...] = ("postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown")
PARSE_STATUSES: tuple[str, ...] = ("ok", "partial", "no_address", "foreign")

RAW_ADDRESS_COLUMNS: tuple[str, ...] = (
    "raw_address", "care_of", "street_address", "postal_code", "post_town", "county", "country_code",
)
SUGGESTION_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "source_record_uid", "observed_at", "kind",
    *RAW_ADDRESS_COLUMNS,
    "decided_by", "note", "replaces_key", "suggested_at", "source_run_id", "extractor_version",
)
COMPONENT_COLUMNS: tuple[str, ...] = (
    "care_of", "box", "street_name", "house_number", "unit", "postal_code", "city",
)
NORMALIZED_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "normalized_id", "suggestion_id", "suggested_at", "kind",
    *COMPONENT_COLUMNS,
    "country_code", "normalized_address", "address_key", "parse_status", "parse_notes",
    "normalizer_version", "normalized_at",
)
GEOCODE_COLUMNS: tuple[str, ...] = (
    "latitude", "longitude", "geocode_status", "geocode_method", "geocode_confidence",
    "geocode_precision", "geocode_policy", "geocode_reference", "geocoded_at",
)
MAIN_COLUMNS: tuple[str, ...] = (
    "company_id", "address_key",
    *COMPONENT_COLUMNS,
    "country_code", "normalized_address", "kinds", "sources", "slots", "normalized_ids",
    "text_source", "active", "inactive_reason",
    *GEOCODE_COLUMNS,
    "normalizer_version", "folded_at", "fold_version", "source_run_id",
)
HISTORY_COLUMNS: tuple[str, ...] = MAIN_COLUMNS
RULE_COLUMNS: tuple[str, ...] = (
    "company_id", "address_key", "action", "removed", "decided_by", "note", "decided_at",
)
PRECEDENCE_COLUMNS: tuple[str, ...] = (
    "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
)
