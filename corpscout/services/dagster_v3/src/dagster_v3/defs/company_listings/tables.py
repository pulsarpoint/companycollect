CLICKHOUSE_DATABASE = "corpscout"
GROUP_NAME = "company_listings"

SE_COMPANY_LISTINGS_TABLE = "se_company_listings"
QUALIFIED_SE_COMPANY_LISTINGS_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{SE_COMPANY_LISTINGS_TABLE}"
)

SE_COMPANY_LISTINGS_COLUMNS = (
    "country_code",
    "company_id",
    "issuer_lei",
    "isin",
    "mic",
    "ticker",
    "instrument_name",
    "instrument_type",
    "cfi_code",
    "notional_currency",
    "admission_date",
    "first_trade_date",
    "termination_date",
    "trading_status",
    "is_current",
    "identity_match_method",
    "identity_match_confidence",
    "listing_status_source",
    "status_conflict",
    "eodhd_symbol_key",
    "eodhd_is_delisted",
    "source_slug",
    "source_record_id",
    "source_publication_date",
    "source_retrieved_at",
    "source_run_id",
    "resolved_at",
)
