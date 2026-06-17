EXCHANGE_RATES_DATABASE = "reference"
EXCHANGE_RATES_TABLE = "exchange_rates"
QUALIFIED_EXCHANGE_RATES_TABLE = f"{EXCHANGE_RATES_DATABASE}.{EXCHANGE_RATES_TABLE}"

EXCHANGE_RATES_COLUMNS = (
    "rate_date",
    "base_currency",
    "quote_currency",
    "rate",
    "source",
    "source_url",
    "source_payload_hash",
    "source_run_id",
    "pulled_at",
    "_dlt_load_id",
    "_dlt_id",
)
