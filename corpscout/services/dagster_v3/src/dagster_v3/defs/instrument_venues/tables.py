CLICKHOUSE_DATABASE = "corpscout"
GROUP_NAME = "instrument_venues"

INSTRUMENT_VENUES_TABLE = "instrument_venues"
QUALIFIED_INSTRUMENT_VENUES_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{INSTRUMENT_VENUES_TABLE}"
)

FIRDS_VENUE_SOURCE = "esma_firds"
EODHD_VENUE_SOURCE = "eodhd"

REGULATOR_EVIDENCE_TIER = "regulator"
VENDOR_EVIDENCE_TIER = "vendor"

INSTRUMENT_VENUES_COLUMNS = (
    "isin",
    "mic",
    "venue_source",
    "operating_mic",
    "evidence_tier",
    "cfi_code",
    "cfi_category",
    "instrument_name",
    "instrument_type",
    "ticker",
    "trading_currency",
    "trading_status",
    "is_current",
    "admission_date",
    "first_trade_date",
    "termination_date",
    "first_seen_date",
    "last_seen_date",
    "source_record_id",
    "source_publication_date",
    "source_retrieved_at",
    "source_run_id",
    "resolved_at",
)
