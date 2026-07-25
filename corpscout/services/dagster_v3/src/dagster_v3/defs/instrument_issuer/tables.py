CLICKHOUSE_DATABASE = "corpscout"
GROUP_NAME = "instrument_issuer"

INSTRUMENT_ISSUER_TABLE = "instrument_issuer"
QUALIFIED_INSTRUMENT_ISSUER_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{INSTRUMENT_ISSUER_TABLE}"
)

LEI_ISSUER_SCHEME = "lei"
FIRDS_MAPPING_SOURCE = "esma_firds"

INSTRUMENT_ISSUER_COLUMNS = (
    "isin",
    "issuer_scheme",
    "issuer_id",
    "mapping_source",
    "first_seen_date",
    "last_seen_date",
    "source_run_id",
    "resolved_at",
)
