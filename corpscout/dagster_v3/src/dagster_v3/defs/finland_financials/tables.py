from pathlib import Path

DLT_DATASET_NAME = "finland_financials"
METRICS_TABLE = "financial_metrics"  # DuckDB staging
FINLAND_DATABASE = "corpscout"
METRICS_TABLE_CH = "fi_financial_metrics"  # existing table (migration 000009)
QUALIFIED_METRICS_TABLE = f"{FINLAND_DATABASE}.{METRICS_TABLE_CH}"

XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
MAPPING_VERSION = "xbrl_common_v1"
SOURCE_SLUG = "finland_xbrl"

# The existing dimensional concept+member → metric map (reused, not duplicated).
METRIC_MAP_PATH = (
    Path(__file__).resolve().parent.parent
    / "finland_xbrl" / "dbt" / "seeds" / "xbrl_metric_map.csv"
)

# Canonical metrics, matching fi_financial_metrics (migration 000009) column order.
FI_METRIC_NAMES = (
    "revenue",
    "operating_profit_loss",
    "profit_loss",
    "total_assets",
    "equity",
    "liabilities",
    "cash_and_bank",
    "current_assets",
    "current_receivables",
    "current_liabilities",
    "personnel_expenses",
    "wages_and_salaries",
)

_METRIC_AMOUNT_COLUMNS = tuple(
    col
    for metric in FI_METRIC_NAMES
    for col in (f"{metric}_amount_original", f"{metric}_amount_usd")
)

# Full fi_financial_metrics column order (migration 000009) — the export contract.
FI_FINANCIAL_METRICS_COLUMNS = (
    "statement_key",
    "business_id",
    "financial_date",
    "period_start",
    "period_end",
    "currency_original",
    *_METRIC_AMOUNT_COLUMNS,
    "employees",
    "source_fact_count",
    "mapped_fact_count",
    "unmapped_numeric_fact_count",
    "metric_warnings",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_converted_at",
    "source_system",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "resolved_at",
)
