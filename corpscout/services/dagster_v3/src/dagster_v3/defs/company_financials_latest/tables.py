COMPANY_FINANCIALS_LATEST_COUNTRIES = ("no", "fi", "se", "ee", "lv", "gb", "br", "sk")

COMPANY_FINANCIALS_LATEST_TABLES = tuple(
    f"{code}_company_financials_latest" for code in COMPANY_FINANCIALS_LATEST_COUNTRIES
)

COMPANY_FINANCIALS_LATEST_COLUMNS = (
    "company_id",
    "fiscal_year",
    "period_end_date",
    "currency",
    "revenue_amount_original",
    "revenue_amount_usd",
    "net_result_amount_original",
    "net_result_amount_usd",
    "total_assets_amount_original",
    "total_assets_amount_usd",
    "equity_amount_original",
    "equity_amount_usd",
    "employees",
    "years_count",
    "resolved_at",
)
