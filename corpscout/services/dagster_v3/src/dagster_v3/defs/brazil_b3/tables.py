CLICKHOUSE_DATABASE = "corpscout"
BR_B3_LISTINGS_TABLE = "br_b3_listings"

# Column order is the contract with migration 000224.
BR_B3_LISTINGS_COLUMNS = (
    "cvm_code",
    "cnpj",
    "cnpj_basico",
    "ticker_root",
    "company_name",
    "trading_name",
    "market",
    "segment",
    "listing_date",
    "status",
    "source_url",
    "source_run_id",
    "retrieved_at",
)

# B3 listed 3,486 issuers when this was written. A floor, so the register
# growing does not fail the load while a truncated download still does.
MIN_B3_LISTINGS = 2_000
