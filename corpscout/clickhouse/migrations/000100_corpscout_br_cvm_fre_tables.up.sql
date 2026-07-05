CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_fre_documents
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    fre_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    cvm_code LowCardinality(String),
    reference_date Date32,
    version UInt16,
    document_category LowCardinality(String),
    document_id UInt64,
    received_date Nullable(Date32),
    document_url String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, document_id);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_fre_capital_social
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    fre_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    reference_date Date32,
    version UInt16,
    document_id UInt64,
    capital_social_id UInt64,
    capital_type String,
    authorization_approval_date Nullable(Date32),
    capital_amount Nullable(Decimal(38, 6)),
    payment_deadline String,
    ordinary_shares Nullable(UInt64),
    preferred_shares Nullable(UInt64),
    total_shares Nullable(UInt64),
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, document_id, capital_social_id, capital_type);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_fre_capital_social_classes
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    fre_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    reference_date Date32,
    version UInt16,
    document_id UInt64,
    capital_social_id UInt64,
    preferred_share_class_type String,
    share_count Nullable(UInt64),
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, document_id, capital_social_id, preferred_share_class_type);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_fre_capital_distribution
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    fre_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    reference_date Date32,
    version UInt16,
    document_id UInt64,
    individual_shareholder_count Nullable(UInt64),
    company_shareholder_count Nullable(UInt64),
    institutional_investor_count Nullable(UInt64),
    ordinary_shares_outstanding Nullable(UInt64),
    ordinary_shares_outstanding_percent Nullable(Decimal(18, 6)),
    preferred_shares_outstanding Nullable(UInt64),
    preferred_shares_outstanding_percent Nullable(Decimal(18, 6)),
    total_shares_outstanding Nullable(UInt64),
    total_shares_outstanding_percent Nullable(Decimal(18, 6)),
    last_assembly_date Nullable(Date32),
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, document_id);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_fre_auditors
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    fre_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    reference_date Date32,
    version UInt16,
    document_id UInt64,
    auditor_id UInt64,
    auditor_name String,
    auditor_cpf String,
    auditor_cnpj String,
    auditor_cvm_code LowCardinality(String),
    auditor_origin_type String,
    contract_start_date Nullable(Date32),
    contract_end_date Nullable(Date32),
    service_start_date Nullable(Date32),
    contracted_service String,
    auditor_remuneration Nullable(Decimal(38, 6)),
    substitution_reason String,
    presented_reason String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, document_id, auditor_id);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_fre_responsibles
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    fre_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    reference_date Date32,
    version UInt16,
    document_id UInt64,
    responsible_name String,
    responsible_role String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, document_id, responsible_name, responsible_role);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_fre_related_party_transactions
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    fre_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    reference_date Date32,
    version UInt16,
    document_id UInt64,
    related_party String,
    person_type LowCardinality(String),
    related_party_document String,
    issuer_relationship String,
    data_transaction Nullable(Date32),
    contract_object String,
    transaction_amount Nullable(Decimal(38, 6)),
    existing_balance_original String,
    related_party_interest_amount_original String,
    insurance_guarantee String,
    transaction_duration String,
    loan_debt String,
    termination String,
    operation_nature_reason String,
    interest_rate String,
    issuer_contractual_position String,
    issuer_contractual_position_specification String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, related_party, data_transaction);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_fre_remuneration_total_organs
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    fre_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    reference_date Date32,
    version UInt16,
    document_id UInt64,
    fiscal_year_start_date Nullable(Date32),
    fiscal_year_end_date Nullable(Date32),
    total_remuneration Nullable(Decimal(38, 6)),
    administration_body String,
    member_count Nullable(Decimal(18, 6)),
    body_total_remuneration Nullable(Decimal(38, 6)),
    paid_member_count Nullable(Decimal(18, 6)),
    salary Nullable(Decimal(38, 6)),
    direct_indirect_benefits Nullable(Decimal(38, 6)),
    committee_participation Nullable(Decimal(38, 6)),
    other_fixed_amounts Nullable(Decimal(38, 6)),
    other_fixed_remuneration_description String,
    bonus Nullable(Decimal(38, 6)),
    profit_sharing Nullable(Decimal(38, 6)),
    meeting_participation Nullable(Decimal(38, 6)),
    other_variable_amounts Nullable(Decimal(38, 6)),
    commissions Nullable(Decimal(38, 6)),
    other_variable_remuneration_description String,
    post_employment Nullable(Decimal(38, 6)),
    position_termination Nullable(Decimal(38, 6)),
    share_based Nullable(Decimal(38, 6)),
    observation String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, document_id, administration_body, fiscal_year_end_date);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_fre_shareholders
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    fre_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    reference_date Date32,
    version UInt16,
    document_id UInt64,
    shareholder_id UInt64,
    shareholder_name String,
    shareholder_person_type LowCardinality(String),
    shareholder_document String,
    related_shareholder_id Nullable(UInt64),
    related_shareholder_name String,
    related_shareholder_person_type LowCardinality(String),
    related_shareholder_document String,
    ordinary_shares_outstanding Nullable(UInt64),
    ordinary_shares_outstanding_percent Nullable(Decimal(18, 6)),
    preferred_shares_outstanding Nullable(UInt64),
    preferred_shares_outstanding_percent Nullable(Decimal(18, 6)),
    total_shares_outstanding Nullable(UInt64),
    total_shares_outstanding_percent Nullable(Decimal(18, 6)),
    nationality String,
    state LowCardinality(String),
    foreign_resident LowCardinality(String),
    legal_representative String,
    legal_representative_person_type LowCardinality(String),
    legal_representative_document String,
    capital_composition_date Nullable(Date32),
    last_change_date Nullable(Date32),
    controlling_shareholder LowCardinality(String),
    shareholder_agreement_participant LowCardinality(String),
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, document_id, shareholder_id);
