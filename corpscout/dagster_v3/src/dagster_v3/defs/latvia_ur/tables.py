from typing import Any

DLT_DATASET_NAME = "latvia_ur"
ENTITIES_TABLE = "entities"

LATVIA_UR_ENTITIES_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
    "regcode": {"data_type": "text", "nullable": False},
    "vat_id": {"data_type": "text"},
    "sepa": {"data_type": "text"},
    "legal_name": {"data_type": "text"},
    "name_in_quotes": {"data_type": "text"},
    "legal_form_code": {"data_type": "text"},
    "legal_form_text": {"data_type": "text"},
    "legal_form_description_en": {"data_type": "text"},
    "regtype_code": {"data_type": "text"},
    "regtype_text": {"data_type": "text"},
    "registered_date": {"data_type": "text"},
    "terminated_date": {"data_type": "text"},
    "closed_flag": {"data_type": "text"},
    "status": {"data_type": "text"},
    "is_active": {"data_type": "bool"},
    "address": {"data_type": "text"},
    "postal_code": {"data_type": "text"},
    "address_id": {"data_type": "text"},
    "region_code": {"data_type": "text"},
    "city_code": {"data_type": "text"},
    "atvk_code": {"data_type": "text"},
    "reregistration_term": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "raw_entity": {"data_type": "text"},
}


def copy_dlt_columns(columns: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {name: dict(spec) for name, spec in columns.items()}


# ClickHouse export target (schema owned by clickhouse/migrations/000015_corpscout_lv_companies).
LATVIA_UR_DATABASE = "corpscout"
LV_COMPANIES_TABLE = "lv_companies"
QUALIFIED_LV_COMPANIES_TABLE = f"{LATVIA_UR_DATABASE}.{LV_COMPANIES_TABLE}"

# Column order must match the DuckDB entities table and the 000015 migration.
LV_COMPANIES_COLUMNS = tuple(LATVIA_UR_ENTITIES_COLUMNS)


# --- Financials (Module 2) ---------------------------------------------------

FINANCIALS_DATASET_ID = "8d31b878-536a-44aa-a013-8bc6b669d477"
_FIN_BASE = f"https://data.gov.lv/dati/dataset/{FINANCIALS_DATASET_ID}/resource"
FINANCIAL_STATEMENTS_URL = (
    f"{_FIN_BASE}/27fcc5ec-c63b-4bfd-bb08-01f073a52d04/download/financial_statements.csv"
)
BALANCE_SHEETS_URL = (
    f"{_FIN_BASE}/50ef4f26-f410-4007-b296-22043ca3dc43/download/balance_sheets.csv"
)
INCOME_STATEMENTS_URL = (
    f"{_FIN_BASE}/d5fd17ef-d32e-40cb-8399-82b780095af0/download/income_statements.csv"
)
CASH_FLOW_STATEMENTS_URL = (
    f"{_FIN_BASE}/1a11fc29-ba7c-4e5a-8edc-7a28cea24988/download/cash_flow_statements.csv"
)

# DuckDB raw staging tables (one per CSV; each is an independent checkpoint).
FINANCIAL_STATEMENTS_RAW_TABLE = "financial_statements_raw"
BALANCE_SHEETS_RAW_TABLE = "balance_sheets_raw"
INCOME_STATEMENTS_RAW_TABLE = "income_statements_raw"
CASH_FLOW_STATEMENTS_RAW_TABLE = "cash_flow_statements_raw"

# raw table -> download URL
FINANCIAL_RAW_SOURCES: dict[str, str] = {
    FINANCIAL_STATEMENTS_RAW_TABLE: FINANCIAL_STATEMENTS_URL,
    BALANCE_SHEETS_RAW_TABLE: BALANCE_SHEETS_URL,
    INCOME_STATEMENTS_RAW_TABLE: INCOME_STATEMENTS_URL,
    CASH_FLOW_STATEMENTS_RAW_TABLE: CASH_FLOW_STATEMENTS_URL,
}

# Numeric column groups = each source CSV's columns minus the (statement_id, file_id)
# join keys. The pivot try_casts these to DECIMAL(38,2).
BALANCE_NUMERIC_COLUMNS = (
    "cash",
    "marketable_securities",
    "accounts_receivable",
    "inventories",
    "total_current_assets",
    "investments",
    "fixed_assets",
    "intangible_assets",
    "total_non_current_assets",
    "total_assets",
    "future_housing_repairs_payments",
    "current_liabilities",
    "non_current_liabilities",
    "provisions",
    "equity",
    "total_equities",
)
INCOME_NUMERIC_COLUMNS = (
    "net_turnover",
    "by_nature_inventory_change",
    "by_nature_long_term_investment_expenses",
    "by_nature_other_operating_revenues",
    "by_nature_material_expenses",
    "by_nature_labour_expenses",
    "by_nature_depreciation_expenses",
    "by_function_cost_of_goods_sold",
    "by_function_gross_profit",
    "by_function_selling_expenses",
    "by_function_administrative_expenses",
    "by_function_other_operating_revenues",
    "other_operating_expenses",
    "equity_investment_earnings",
    "other_long_term_investment_earnings",
    "other_interest_revenues",
    "investment_fair_value_adjustments",
    "interest_expenses",
    "extra_revenues",
    "extra_expenses",
    "income_before_income_taxes",
    "provision_for_income_taxes",
    "income_after_income_taxes",
    "other_taxes",
    "extra_dividends",
    "net_income",
)
CASHFLOW_NUMERIC_COLUMNS = (
    "cfo_dm_cash_received_from_customers",
    "cfo_dm_cash_paid_to_suppliers_employees",
    "cfo_dm_other_cash_received_paid",
    "cfo_dm_operating_cash_flow",
    "cfo_dm_interest_paid",
    "cfo_dm_income_taxes_paid",
    "cfo_dm_extra_items_cash_flow",
    "cfo_dm_net_operating_cash_flow",
    "cfo_im_income_before_income_taxes",
    "cfo_im_income_before_changes_in_working_capital",
    "cfo_im_operating_cash_flow",
    "cfo_im_interest_paid",
    "cfo_im_income_taxes_paid",
    "cfo_im_extra_items_cash_flow",
    "cfo_im_net_operating_cash_flow",
    "cfi_acquisition_of_stocks_shares",
    "cfi_sale_proceeds_from_stocks_shares",
    "cfi_acquisition_of_fixed_assets_intangible_assets",
    "cfi_sale_proceeds_from_fixed_assets_intangible_assets",
    "cfi_loans_made",
    "cfi_repayments_of_loans_received",
    "cfi_interest_received",
    "cfi_dividends_received",
    "cfi_net_investing_cash_flow",
    "cff_proceeds_from_stocks_bonds_issuance_or_contributed_capital",
    "cff_loans_received",
    "cff_subsidies_grants_donations_received",
    "cff_repayments_of_loans_made",
    "cff_repayments_of_lease_obligations",
    "cff_dividends_paid",
    "cff_net_financing_cash_flow",
    "effect_of_exchange_rate_change",
    "net_increase",
    "at_beginning_of_year",
    "at_end_of_year",
)

# Metadata/envelope columns of the wide pivot, in migration 000016 order.
FINANCIAL_METADATA_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_line_number",
    "source_record_id",
    "source_payload_hash",
    "statement_id",
    "file_id",
    "regcode",
    "source_schema",
    "source_type",
    "fiscal_year",
    "period_start_date",
    "period_end_date",
    "employees",
    "rounded_to_nearest",
    "currency",
    "created_at",
)

# DuckDB wide pivot table + ClickHouse export target (schema owned by migration 000016).
FINANCIAL_STATEMENTS_WIDE_TABLE = "financial_statements"
LV_FINANCIAL_STATEMENTS_TABLE = "lv_financial_statements"
QUALIFIED_LV_FINANCIAL_STATEMENTS_TABLE = (
    f"{LATVIA_UR_DATABASE}.{LV_FINANCIAL_STATEMENTS_TABLE}"
)

# Full wide column order = metadata + balance + income + cashflow + source_url + raw.
# Must match the DuckDB wide table and the 000016 migration.
LV_FINANCIAL_STATEMENTS_COLUMNS = (
    FINANCIAL_METADATA_COLUMNS
    + BALANCE_NUMERIC_COLUMNS
    + INCOME_NUMERIC_COLUMNS
    + CASHFLOW_NUMERIC_COLUMNS
    + ("source_url", "raw_financial_record")
)


# --- Financial metrics (Module 3, Phase 1) ---------------------------------

FINANCIAL_METRICS_WIDE_TABLE = "financial_metrics"
LV_FINANCIAL_METRICS_TABLE = "lv_financial_metrics"
QUALIFIED_LV_FINANCIAL_METRICS_TABLE = (
    f"{LATVIA_UR_DATABASE}.{LV_FINANCIAL_METRICS_TABLE}"
)

# canonical metric name -> source column in FINANCIAL_STATEMENTS_WIDE_TABLE
FINANCIAL_METRIC_SOURCE_COLUMNS = {
    "revenue": "net_turnover",
    "gross_profit": "by_function_gross_profit",
    "pretax_result": "income_before_income_taxes",
    "net_result": "net_income",
    "total_assets": "total_assets",
    "current_assets": "total_current_assets",
    "non_current_assets": "total_non_current_assets",
    "equity": "equity",
    "current_liabilities": "current_liabilities",
    "non_current_liabilities": "non_current_liabilities",
}
FINANCIAL_METRIC_NAMES = tuple(FINANCIAL_METRIC_SOURCE_COLUMNS)

# rounded_to_nearest -> multiplier applied to EUR values before FX conversion.
ROUNDED_TO_NEAREST_FACTORS = {"ONES": 1, "THOUSANDS": 1000, "MILLIONS": 1_000_000}

_METRIC_AMOUNT_COLUMNS = tuple(
    col
    for metric in FINANCIAL_METRIC_NAMES
    for col in (f"{metric}_amount_original", f"{metric}_amount_usd")
)

# Column order must match the DuckDB metrics table and the 000019 migration.
LV_FINANCIAL_METRICS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "statement_id",
    "regcode",
    "fiscal_year",
    "period_start_date",
    "period_end_date",
    "employees",
    "currency",
    "rounded_to_nearest",
    *_METRIC_AMOUNT_COLUMNS,
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "resolved_at",
)
