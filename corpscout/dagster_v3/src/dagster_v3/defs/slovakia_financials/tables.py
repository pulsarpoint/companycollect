DLT_DATASET_NAME = "slovakia_financials"
METRICS_TABLE = "financial_metrics"  # DuckDB staging
SLOVAKIA_DATABASE = "corpscout"
METRICS_TABLE_CH = "sk_financial_metrics"
QUALIFIED_METRICS_TABLE = f"{SLOVAKIA_DATABASE}.{METRICS_TABLE_CH}"

# RÚZ open REST API (free, no key). The `cruz-public/api` path + a browser UA are
# required — the `cisExt` path and bare UAs are F5-WAF-rejected.
RUZ_BASE_URL = "https://www.registeruz.sk/cruz-public/api"
# RÚZ data starts ~2009 (EUR era); the id sweep walks everything from here.
RUZ_CHANGED_SINCE = "2009-01-01"
MAPPING_VERSION = "ruz_v1"
SOURCE_SLUG = "slovakia_ruz"

# Canonical metrics extracted from the statutory statement tables (native EUR).
SK_METRIC_NAMES = (
    "revenue",
    "total_assets",
    "equity",
    "liabilities",
    "pretax_result",
    "net_result",
)

_METRIC_AMOUNT_COLUMNS = tuple(
    col
    for metric in SK_METRIC_NAMES
    for col in (f"{metric}_amount_original", f"{metric}_amount_usd")
)

# Full sk_financial_metrics column order — the export contract (migration 000043).
SK_FINANCIAL_METRICS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "ico",
    "ruz_entity_id",
    "statement_id",
    "template_name",
    "statement_type",
    "fiscal_year",
    "period_start_date",
    "period_end_date",
    "filed_date",
    "approved_date",
    "currency_original",
    *_METRIC_AMOUNT_COLUMNS,
    "mapped_metric_count",
    "template_mapped",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_converted_at",
    "source_url",
    "resolved_at",
)
