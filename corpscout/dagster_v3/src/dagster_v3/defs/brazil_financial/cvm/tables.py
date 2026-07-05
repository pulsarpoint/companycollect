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
BR_CVM_FRE_DOCUMENTS_TABLE = "br_cvm_fre_documents"
BR_CVM_FRE_CAPITAL_SOCIAL_TABLE = "br_cvm_fre_capital_social"
BR_CVM_FRE_CAPITAL_SOCIAL_CLASSES_TABLE = "br_cvm_fre_capital_social_classes"
BR_CVM_FRE_CAPITAL_DISTRIBUTION_TABLE = "br_cvm_fre_capital_distribution"
BR_CVM_FRE_AUDITORS_TABLE = "br_cvm_fre_auditors"
BR_CVM_FRE_RESPONSIBLES_TABLE = "br_cvm_fre_responsibles"
BR_CVM_FRE_RELATED_PARTY_TRANSACTIONS_TABLE = "br_cvm_fre_related_party_transactions"
BR_CVM_FRE_REMUNERATION_TOTAL_ORGANS_TABLE = "br_cvm_fre_remuneration_total_organs"
BR_CVM_FRE_SHAREHOLDERS_TABLE = "br_cvm_fre_shareholders"
BR_CVM_FINANCIAL_METRICS_TABLE = "br_cvm_financial_metrics"

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
QUALIFIED_BR_CVM_FRE_DOCUMENTS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FRE_DOCUMENTS_TABLE}"
)
QUALIFIED_BR_CVM_FRE_CAPITAL_SOCIAL_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FRE_CAPITAL_SOCIAL_TABLE}"
)
QUALIFIED_BR_CVM_FRE_CAPITAL_SOCIAL_CLASSES_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FRE_CAPITAL_SOCIAL_CLASSES_TABLE}"
)
QUALIFIED_BR_CVM_FRE_CAPITAL_DISTRIBUTION_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FRE_CAPITAL_DISTRIBUTION_TABLE}"
)
QUALIFIED_BR_CVM_FRE_AUDITORS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FRE_AUDITORS_TABLE}"
)
QUALIFIED_BR_CVM_FRE_RESPONSIBLES_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FRE_RESPONSIBLES_TABLE}"
)
QUALIFIED_BR_CVM_FRE_RELATED_PARTY_TRANSACTIONS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FRE_RELATED_PARTY_TRANSACTIONS_TABLE}"
)
QUALIFIED_BR_CVM_FRE_REMUNERATION_TOTAL_ORGANS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FRE_REMUNERATION_TOTAL_ORGANS_TABLE}"
)
QUALIFIED_BR_CVM_FRE_SHAREHOLDERS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FRE_SHAREHOLDERS_TABLE}"
)
QUALIFIED_BR_CVM_FINANCIAL_METRICS_TABLE = (
    f"{BRAZIL_CVM_DATABASE}.{BR_CVM_FINANCIAL_METRICS_TABLE}"
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

BR_CVM_FRE_DOCUMENTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "fre_year",
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
BR_CVM_FRE_DOCUMENTS_EXPORT_COLUMNS = BR_CVM_FRE_DOCUMENTS_COLUMNS

BR_CVM_FRE_COMMON_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "fre_year",
    "cnpj",
    "cnpj_basico",
    "company_name",
    "reference_date",
    "version",
    "document_id",
)
BR_CVM_FRE_SOURCE_COLUMNS = (
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)

BR_CVM_FRE_CAPITAL_SOCIAL_COLUMNS = (
    *BR_CVM_FRE_COMMON_COLUMNS,
    "capital_social_id",
    "capital_type",
    "authorization_approval_date",
    "capital_amount",
    "payment_deadline",
    "ordinary_shares",
    "preferred_shares",
    "total_shares",
    *BR_CVM_FRE_SOURCE_COLUMNS,
)
BR_CVM_FRE_CAPITAL_SOCIAL_EXPORT_COLUMNS = BR_CVM_FRE_CAPITAL_SOCIAL_COLUMNS

BR_CVM_FRE_CAPITAL_SOCIAL_CLASSES_COLUMNS = (
    *BR_CVM_FRE_COMMON_COLUMNS,
    "capital_social_id",
    "preferred_share_class_type",
    "share_count",
    *BR_CVM_FRE_SOURCE_COLUMNS,
)
BR_CVM_FRE_CAPITAL_SOCIAL_CLASSES_EXPORT_COLUMNS = (
    BR_CVM_FRE_CAPITAL_SOCIAL_CLASSES_COLUMNS
)

BR_CVM_FRE_CAPITAL_DISTRIBUTION_COLUMNS = (
    *BR_CVM_FRE_COMMON_COLUMNS,
    "individual_shareholder_count",
    "company_shareholder_count",
    "institutional_investor_count",
    "ordinary_shares_outstanding",
    "ordinary_shares_outstanding_percent",
    "preferred_shares_outstanding",
    "preferred_shares_outstanding_percent",
    "total_shares_outstanding",
    "total_shares_outstanding_percent",
    "last_assembly_date",
    *BR_CVM_FRE_SOURCE_COLUMNS,
)
BR_CVM_FRE_CAPITAL_DISTRIBUTION_EXPORT_COLUMNS = BR_CVM_FRE_CAPITAL_DISTRIBUTION_COLUMNS

BR_CVM_FRE_AUDITORS_COLUMNS = (
    *BR_CVM_FRE_COMMON_COLUMNS,
    "auditor_id",
    "auditor_name",
    "auditor_cpf",
    "auditor_cnpj",
    "auditor_cvm_code",
    "auditor_origin_type",
    "contract_start_date",
    "contract_end_date",
    "service_start_date",
    "contracted_service",
    "auditor_remuneration",
    "substitution_reason",
    "presented_reason",
    *BR_CVM_FRE_SOURCE_COLUMNS,
)
BR_CVM_FRE_AUDITORS_EXPORT_COLUMNS = BR_CVM_FRE_AUDITORS_COLUMNS

BR_CVM_FRE_RESPONSIBLES_COLUMNS = (
    *BR_CVM_FRE_COMMON_COLUMNS,
    "responsible_name",
    "responsible_role",
    *BR_CVM_FRE_SOURCE_COLUMNS,
)
BR_CVM_FRE_RESPONSIBLES_EXPORT_COLUMNS = BR_CVM_FRE_RESPONSIBLES_COLUMNS

BR_CVM_FRE_RELATED_PARTY_TRANSACTIONS_COLUMNS = (
    *BR_CVM_FRE_COMMON_COLUMNS,
    "related_party",
    "person_type",
    "related_party_document",
    "issuer_relationship",
    "data_transaction",
    "contract_object",
    "transaction_amount",
    "existing_balance_original",
    "related_party_interest_amount_original",
    "insurance_guarantee",
    "transaction_duration",
    "loan_debt",
    "termination",
    "operation_nature_reason",
    "interest_rate",
    "issuer_contractual_position",
    "issuer_contractual_position_specification",
    *BR_CVM_FRE_SOURCE_COLUMNS,
)
BR_CVM_FRE_RELATED_PARTY_TRANSACTIONS_EXPORT_COLUMNS = (
    BR_CVM_FRE_RELATED_PARTY_TRANSACTIONS_COLUMNS
)

BR_CVM_FRE_REMUNERATION_TOTAL_ORGANS_COLUMNS = (
    *BR_CVM_FRE_COMMON_COLUMNS,
    "fiscal_year_start_date",
    "fiscal_year_end_date",
    "total_remuneration",
    "administration_body",
    "member_count",
    "body_total_remuneration",
    "paid_member_count",
    "salary",
    "direct_indirect_benefits",
    "committee_participation",
    "other_fixed_amounts",
    "other_fixed_remuneration_description",
    "bonus",
    "profit_sharing",
    "meeting_participation",
    "other_variable_amounts",
    "commissions",
    "other_variable_remuneration_description",
    "post_employment",
    "position_termination",
    "share_based",
    "observation",
    *BR_CVM_FRE_SOURCE_COLUMNS,
)
BR_CVM_FRE_REMUNERATION_TOTAL_ORGANS_EXPORT_COLUMNS = (
    BR_CVM_FRE_REMUNERATION_TOTAL_ORGANS_COLUMNS
)

BR_CVM_FRE_SHAREHOLDERS_COLUMNS = (
    *BR_CVM_FRE_COMMON_COLUMNS,
    "shareholder_id",
    "shareholder_name",
    "shareholder_person_type",
    "shareholder_document",
    "related_shareholder_id",
    "related_shareholder_name",
    "related_shareholder_person_type",
    "related_shareholder_document",
    "ordinary_shares_outstanding",
    "ordinary_shares_outstanding_percent",
    "preferred_shares_outstanding",
    "preferred_shares_outstanding_percent",
    "total_shares_outstanding",
    "total_shares_outstanding_percent",
    "nationality",
    "state",
    "foreign_resident",
    "legal_representative",
    "legal_representative_person_type",
    "legal_representative_document",
    "capital_composition_date",
    "last_change_date",
    "controlling_shareholder",
    "shareholder_agreement_participant",
    *BR_CVM_FRE_SOURCE_COLUMNS,
)
BR_CVM_FRE_SHAREHOLDERS_EXPORT_COLUMNS = BR_CVM_FRE_SHAREHOLDERS_COLUMNS

BR_CVM_FINANCIAL_METRICS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_dataset",
    "source_year",
    "cnpj",
    "cnpj_basico",
    "company_name",
    "cvm_code",
    "reference_date",
    "period_start_date",
    "period_end_date",
    "period_type",
    "version",
    "is_latest_version",
    "consolidation_type",
    "metric_name",
    "metric_label",
    "currency",
    "amount_original",
    "amount_usd",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "source_statement_code",
    "source_statement_name",
    "source_account_codes",
    "source_account_descriptions_original",
    "source_statement_run_ids",
    "source_statement_record_ids",
    "source_archive_keys",
    "source_file_names",
    "source_statement_row_count",
    "metric_mapping_version",
    "resolved_at",
)
BR_CVM_FINANCIAL_METRICS_EXPORT_COLUMNS = BR_CVM_FINANCIAL_METRICS_COLUMNS

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

BR_CVM_FRE_TABLES = (
    BR_CVM_FRE_DOCUMENTS_TABLE,
    BR_CVM_FRE_CAPITAL_SOCIAL_TABLE,
    BR_CVM_FRE_CAPITAL_SOCIAL_CLASSES_TABLE,
    BR_CVM_FRE_CAPITAL_DISTRIBUTION_TABLE,
    BR_CVM_FRE_AUDITORS_TABLE,
    BR_CVM_FRE_RESPONSIBLES_TABLE,
    BR_CVM_FRE_RELATED_PARTY_TRANSACTIONS_TABLE,
    BR_CVM_FRE_REMUNERATION_TOTAL_ORGANS_TABLE,
    BR_CVM_FRE_SHAREHOLDERS_TABLE,
)
