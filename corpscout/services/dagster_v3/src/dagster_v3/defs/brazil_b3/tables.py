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

BR_B3_INSTRUMENTS_TABLE = "br_b3_instruments"

# Column order is the contract with migration 000225.
BR_B3_INSTRUMENTS_COLUMNS = (
    "cvm_code",
    "cnpj",
    "cnpj_basico",
    "ticker",
    "isin",
    "ticker_root",
    "source_url",
    "source_run_id",
    "retrieved_at",
)

# Measured, not guessed: 838 instruments on 2026-07-31. B3 is asked about 3,488
# issuers but only 571 carry a market segment — the rest registered a debenture
# and have no trading code to return, so the yield is a fraction of the issuer
# count. An earlier floor of 1,000 was set from the issuer count and refused a
# complete load.
MIN_B3_INSTRUMENTS = 500
