CLICKHOUSE_DATABASE = "corpscout"
GROUP_NAME = "isin_lei"

ISIN_LEI_TABLE = "isin_lei"
QUALIFIED_ISIN_LEI_TABLE = f"{CLICKHOUSE_DATABASE}.{ISIN_LEI_TABLE}"

ISIN_LEI_COLUMNS = (
    "isin",
    "lei",
    "mapping_source",
    "venue_confirmed",
    "cfi_category",
    "first_seen_date",
    "last_seen_date",
    "source_run_id",
    "resolved_at",
)
