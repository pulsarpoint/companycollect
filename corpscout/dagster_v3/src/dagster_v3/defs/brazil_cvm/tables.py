BRAZIL_CVM_DATABASE = "corpscout"

BR_CVM_DFP_DOCUMENTS_TABLE = "br_cvm_dfp_documents"
BR_CVM_DFP_STATEMENT_ROWS_TABLE = "br_cvm_dfp_statement_rows"
BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE = "br_cvm_dfp_capital_composition"
BR_CVM_DFP_AUDITOR_REPORTS_TABLE = "br_cvm_dfp_auditor_reports"

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

BR_CVM_DFP_TABLES = (
    BR_CVM_DFP_DOCUMENTS_TABLE,
    BR_CVM_DFP_STATEMENT_ROWS_TABLE,
    BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE,
    BR_CVM_DFP_AUDITOR_REPORTS_TABLE,
)
