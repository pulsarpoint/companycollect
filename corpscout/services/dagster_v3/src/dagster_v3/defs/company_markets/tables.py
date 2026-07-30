CLICKHOUSE_DATABASE = "corpscout"

TRADED_SYMBOLS_TABLE = "company_traded_symbols"
MARKET_MONTHLY_TABLE = "company_market_monthly"
MARKET_SUMMARY_TABLE = "company_market_summary"

COMPANY_MARKET_TABLES = (
    TRADED_SYMBOLS_TABLE,
    MARKET_MONTHLY_TABLE,
    MARKET_SUMMARY_TABLE,
)

# Sweden alone resolves 1,796 symbols today. A floor rather than an equality so
# adding a country does not fail the run, while a broken identity join does.
MIN_TRADED_SYMBOLS = 500
