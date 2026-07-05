BRAZIL_CVM_DATABASE = "corpscout"

BR_CVM_COMPANIES_TABLE = "br_cvm_companies"
BR_CVM_DFP_DOCUMENTS_TABLE = "br_cvm_dfp_documents"
BR_CVM_DFP_STATEMENT_ROWS_TABLE = "br_cvm_dfp_statement_rows"
BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE = "br_cvm_dfp_capital_composition"
BR_CVM_DFP_AUDITOR_REPORTS_TABLE = "br_cvm_dfp_auditor_reports"
BR_CVM_ITR_DOCUMENTS_TABLE = "br_cvm_itr_documents"
BR_CVM_ITR_STATEMENT_ROWS_TABLE = "br_cvm_itr_statement_rows"
BR_CVM_ITR_CAPITAL_COMPOSITION_TABLE = "br_cvm_itr_capital_composition"
BR_CVM_ITR_AUDITOR_REPORTS_TABLE = "br_cvm_itr_auditor_reports"

QUALIFIED_BR_CVM_COMPANIES_TABLE = f"{BRAZIL_CVM_DATABASE}.{BR_CVM_COMPANIES_TABLE}"
QUALIFIED_BR_CVM_DFP_DOCUMENTS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_DFP_DOCUMENTS_TABLE}"
)
QUALIFIED_BR_CVM_DFP_STATEMENT_ROWS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_DFP_STATEMENT_ROWS_TABLE}"
)
QUALIFIED_BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE}"
)
QUALIFIED_BR_CVM_DFP_AUDITOR_REPORTS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_DFP_AUDITOR_REPORTS_TABLE}"
)
QUALIFIED_BR_CVM_ITR_DOCUMENTS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_ITR_DOCUMENTS_TABLE}"
)
QUALIFIED_BR_CVM_ITR_STATEMENT_ROWS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_ITR_STATEMENT_ROWS_TABLE}"
)
QUALIFIED_BR_CVM_ITR_CAPITAL_COMPOSITION_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_ITR_CAPITAL_COMPOSITION_TABLE}"
)
QUALIFIED_BR_CVM_ITR_AUDITOR_REPORTS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_ITR_AUDITOR_REPORTS_TABLE}"
)

BR_CVM_COMPANIES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "cnpj",
    "cnpj_basico",
    "cvm_code",
    "legal_name",
    "trade_name",
    "registration_date",
    "constitution_date",
    "cancellation_date",
    "cancellation_reason",
    "registration_status",
    "registration_status_start_date",
    "industry_sector",
    "market_type",
    "registration_category",
    "registration_category_start_date",
    "issuer_status",
    "issuer_status_start_date",
    "shareholding_control",
    "address_type",
    "street",
    "address_complement",
    "district",
    "municipality",
    "state",
    "country",
    "postal_code",
    "phone_area_code",
    "phone_number",
    "fax_area_code",
    "fax_number",
    "email",
    "responsible_type",
    "responsible_name",
    "responsible_start_date",
    "responsible_street",
    "responsible_address_complement",
    "responsible_district",
    "responsible_municipality",
    "responsible_state",
    "responsible_country",
    "responsible_postal_code",
    "responsible_phone_area_code",
    "responsible_phone_number",
    "responsible_fax_area_code",
    "responsible_fax_number",
    "responsible_email",
    "auditor_cnpj",
    "auditor_name",
    "source_url",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)
BR_CVM_COMPANIES_EXPORT_COLUMNS = BR_CVM_COMPANIES_COLUMNS

BR_CVM_DFP_DOCUMENTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "dfp_year",
    "cnpj",
    "cnpj_basico",
    "company_name",
    "cvm_code",
    "reference_date",
    "version",
    "document_category",
    "document_id",
    "received_date",
    "document_url",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)
BR_CVM_DFP_DOCUMENTS_EXPORT_COLUMNS = BR_CVM_DFP_DOCUMENTS_COLUMNS

BR_CVM_DFP_STATEMENT_ROWS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "dfp_year",
    "cnpj",
    "cnpj_basico",
    "company_name",
    "cvm_code",
    "reference_date",
    "version",
    "statement_code",
    "statement_name",
    "consolidation_type",
    "grupo_dfp",
    "currency",
    "scale",
    "original_order",
    "period_start_date",
    "period_end_date",
    "equity_column",
    "account_code",
    "account_description_original",
    "amount_original",
    "amount_usd",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "fixed_account_flag",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)
BR_CVM_DFP_STATEMENT_ROWS_EXPORT_COLUMNS = BR_CVM_DFP_STATEMENT_ROWS_COLUMNS

BR_CVM_DFP_CAPITAL_COMPOSITION_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "dfp_year",
    "cnpj",
    "cnpj_basico",
    "company_name",
    "cvm_code",
    "reference_date",
    "version",
    "ordinary_shares_paid_in",
    "preferred_shares_paid_in",
    "total_shares_paid_in",
    "ordinary_shares_treasury",
    "preferred_shares_treasury",
    "total_shares_treasury",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)
BR_CVM_DFP_CAPITAL_COMPOSITION_EXPORT_COLUMNS = BR_CVM_DFP_CAPITAL_COMPOSITION_COLUMNS

BR_CVM_DFP_AUDITOR_REPORTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "dfp_year",
    "cnpj",
    "cnpj_basico",
    "company_name",
    "cvm_code",
    "reference_date",
    "version",
    "auditor_report_type",
    "opinion_statement_type",
    "opinion_item_number",
    "report_text_original",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)
BR_CVM_DFP_AUDITOR_REPORTS_EXPORT_COLUMNS = BR_CVM_DFP_AUDITOR_REPORTS_COLUMNS

BR_CVM_ITR_DOCUMENTS_COLUMNS = tuple(
    "itr_year" if column_name == "dfp_year" else column_name
    for column_name in BR_CVM_DFP_DOCUMENTS_COLUMNS
)
BR_CVM_ITR_DOCUMENTS_EXPORT_COLUMNS = BR_CVM_ITR_DOCUMENTS_COLUMNS

BR_CVM_ITR_STATEMENT_ROWS_COLUMNS = tuple(
    "itr_year" if column_name == "dfp_year" else column_name
    for column_name in BR_CVM_DFP_STATEMENT_ROWS_COLUMNS
)
BR_CVM_ITR_STATEMENT_ROWS_EXPORT_COLUMNS = BR_CVM_ITR_STATEMENT_ROWS_COLUMNS

BR_CVM_ITR_CAPITAL_COMPOSITION_COLUMNS = tuple(
    "itr_year" if column_name == "dfp_year" else column_name
    for column_name in BR_CVM_DFP_CAPITAL_COMPOSITION_COLUMNS
)
BR_CVM_ITR_CAPITAL_COMPOSITION_EXPORT_COLUMNS = BR_CVM_ITR_CAPITAL_COMPOSITION_COLUMNS

BR_CVM_ITR_AUDITOR_REPORTS_COLUMNS = tuple(
    "itr_year" if column_name == "dfp_year" else column_name
    for column_name in BR_CVM_DFP_AUDITOR_REPORTS_COLUMNS
)
BR_CVM_ITR_AUDITOR_REPORTS_EXPORT_COLUMNS = BR_CVM_ITR_AUDITOR_REPORTS_COLUMNS

BR_CVM_DFP_TABLES = (
    BR_CVM_DFP_DOCUMENTS_TABLE,
    BR_CVM_DFP_STATEMENT_ROWS_TABLE,
    BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE,
    BR_CVM_DFP_AUDITOR_REPORTS_TABLE,
)

BR_CVM_ITR_TABLES = (
    BR_CVM_ITR_DOCUMENTS_TABLE,
    BR_CVM_ITR_STATEMENT_ROWS_TABLE,
    BR_CVM_ITR_CAPITAL_COMPOSITION_TABLE,
    BR_CVM_ITR_AUDITOR_REPORTS_TABLE,
)
