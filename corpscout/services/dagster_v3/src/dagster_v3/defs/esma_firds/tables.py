"""Table and source contracts for the country-neutral ESMA FIRDS source."""

SOURCE_SLUG = "esma_firds"
GROUP_NAME = "esma_firds"
DUCKDB_POOL = "esma_firds_duckdb"

DUCKDB_FILE_NAME = "esma_firds_source.duckdb"
DUCKDB_SCHEMA = "esma_firds"

S3_BUCKET = "source-esma-firds"
S3_RAW_PREFIX = "esma_firds/raw"

CLICKHOUSE_DATABASE = "corpscout"
EVENTS_TABLE = "firds_instrument_events"
CURRENT_TABLE = "firds_instruments_current"
QUALIFIED_EVENTS_TABLE = f"{CLICKHOUSE_DATABASE}.{EVENTS_TABLE}"
QUALIFIED_CURRENT_TABLE = f"{CLICKHOUSE_DATABASE}.{CURRENT_TABLE}"

RAW_FILES_TABLE = "raw_files"
FULL_RECORDS_RAW_TABLE = "full_snapshot_records_raw"
DELTA_EVENTS_RAW_TABLE = "delta_events_raw"
CANCELLATIONS_RAW_TABLE = "cancellation_snapshot_records_raw"
SNAPSHOT_SETS_TABLE = "snapshot_sets"
EVENTS_EXPORT_TABLE = EVENTS_TABLE
CURRENT_EXPORT_TABLE = CURRENT_TABLE

EXPECTED_FULL_CFI_CATEGORIES = frozenset(
    {
        "C",  # collective investment vehicles
        "D",  # debt instruments
        "E",  # equities
        "F",  # futures
        "H",  # non-listed/complex instruments
        "I",  # spot instruments
        "J",  # forwards
        "O",  # options
        "R",  # rights/entitlements
        "S",  # swaps
    }
)

SUPPORTED_FILE_TYPES = ("FULINS", "DLTINS", "FULCAN")

RAW_RECORD_COLUMNS = (
    "source_record_id",
    "source_file_id",
    "source_file_name",
    "source_file_type",
    "source_file_checksum",
    "source_publication_date",
    "source_row_number",
    "event_type",
    "isin",
    "mic",
    "issuer_lei",
    "full_name",
    "short_name",
    "cfi_code",
    "notional_currency",
    "commodity_derivative",
    "issuer_request",
    "admission_approval_at",
    "request_admission_at",
    "first_trade_at",
    "termination_at",
    "competent_authority_country",
    "relevant_venue_mic",
    "valid_from",
    "source_download_url",
    "source_object_key",
    "source_run_id",
    "source_retrieved_at",
    "source_payload_hash",
    "resolved_at",
)

EVENTS_EXPORT_COLUMNS = (
    "source_record_id",
    "source_file_id",
    "source_file_name",
    "source_file_type",
    "source_file_checksum",
    "source_publication_date",
    "source_row_number",
    "event_type",
    "isin",
    "mic",
    "issuer_lei",
    "full_name",
    "short_name",
    "cfi_code",
    "notional_currency",
    "commodity_derivative",
    "issuer_request",
    "admission_approval_at",
    "request_admission_at",
    "first_trade_at",
    "termination_at",
    "competent_authority_country",
    "relevant_venue_mic",
    "valid_from",
    "valid_to",
    "is_latest",
    "source_download_url",
    "source_object_key",
    "source_run_id",
    "source_retrieved_at",
    "resolved_at",
)

CURRENT_EXPORT_COLUMNS = (
    "isin",
    "mic",
    "issuer_lei",
    "full_name",
    "short_name",
    "cfi_code",
    "notional_currency",
    "commodity_derivative",
    "issuer_request",
    "admission_approval_at",
    "request_admission_at",
    "first_trade_at",
    "termination_at",
    "competent_authority_country",
    "relevant_venue_mic",
    "valid_from",
    "source_record_id",
    "source_file_id",
    "source_file_name",
    "source_file_type",
    "source_file_checksum",
    "source_publication_date",
    "latest_event_type",
    "source_download_url",
    "source_object_key",
    "source_run_id",
    "source_retrieved_at",
    "resolved_at",
)

CLICKHOUSE_TABLES = (EVENTS_TABLE, CURRENT_TABLE)
