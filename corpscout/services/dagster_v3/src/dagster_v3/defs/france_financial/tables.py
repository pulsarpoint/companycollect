"""Table and column contracts for France BCE/INPI financial ratios."""

COUNTRY_ISO2 = "FR"
SOURCE_SLUG = "france_financial_ratios"

DLT_DATASET_NAME = "france_financial"
DUCKDB_FILE_NAME = "france_financial_source.duckdb"

SOURCE_PAGE_URL = "https://www.data.gouv.fr/datasets/ratios-financiers-bce-inpi"
PARQUET_EXPORT_URL = (
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "ratios_inpi_bce/exports/parquet"
)

RAW_TABLE = "ratios_raw"
METRICS_TABLE = "financial_metrics"

CLICKHOUSE_DATABASE = "corpscout"
FINANCIAL_METRICS_TABLE = "fr_financial_metrics"
QUALIFIED_FINANCIAL_METRICS_TABLE = f"{CLICKHOUSE_DATABASE}.{FINANCIAL_METRICS_TABLE}"

MONETARY_METRICS = (
    "revenue",
    "gross_margin",
    "ebitda",
    "ebit",
    "net_income",
)
MONETARY_SOURCE_COLUMNS = {
    "revenue": "chiffre_d_affaires",
    "gross_margin": "marge_brute",
    "ebitda": "ebe",
    "ebit": "ebit",
    "net_income": "resultat_net",
}

RATIO_SOURCE_COLUMNS = {
    "debt_ratio_percent": "taux_d_endettement",
    "liquidity_ratio_percent": "ratio_de_liquidite",
    "asset_age_ratio_percent": "ratio_de_vetuste",
    "financial_autonomy_percent": "autonomie_financiere",
    "operating_working_capital_to_revenue_percent": ("poids_bfr_exploitation_sur_ca"),
    "interest_coverage_percent": "couverture_des_interets",
    "cash_flow_to_revenue_percent": "caf_sur_ca",
    "repayment_capacity_ratio": "capacite_de_remboursement",
    "ebitda_margin_percent": "marge_ebe",
    "current_income_before_tax_to_revenue_percent": (
        "resultat_courant_avant_impots_sur_ca"
    ),
    "operating_working_capital_days": "poids_bfr_exploitation_sur_ca_jours",
    "inventory_turnover_days": "rotation_des_stocks_jours",
    "customer_payment_days": "credit_clients_jours",
    "supplier_payment_days": "credit_fournisseurs_jours",
}

_MONETARY_COLUMNS = tuple(
    column
    for metric in MONETARY_METRICS
    for column in (f"{metric}_amount_original", f"{metric}_amount_usd")
)

FR_FINANCIAL_METRICS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "siren",
    "period_end_date",
    "fiscal_year",
    "balance_type_code",
    "currency",
    *_MONETARY_COLUMNS,
    *RATIO_SOURCE_COLUMNS,
    "confidentiality_status",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "source_url",
    "resolved_at",
    "raw_financial_record",
    "source_payload_hash",
)

CLICKHOUSE_EXCLUDED_COLUMNS = frozenset({"raw_financial_record", "source_payload_hash"})
FR_FINANCIAL_METRICS_EXPORT_COLUMNS = tuple(
    column
    for column in FR_FINANCIAL_METRICS_COLUMNS
    if column not in CLICKHOUSE_EXCLUDED_COLUMNS
)

RAW_SOURCE_COLUMNS = (
    "siren",
    "date_cloture_exercice",
    *MONETARY_SOURCE_COLUMNS.values(),
    *RATIO_SOURCE_COLUMNS.values(),
    "type_bilan",
    "confidentiality",
)
