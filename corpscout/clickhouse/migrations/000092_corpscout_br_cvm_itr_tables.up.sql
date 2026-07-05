CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_itr_documents
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    itr_year UInt16,
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

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_itr_statement_rows
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    itr_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    cvm_code LowCardinality(String),
    reference_date Date32,
    version UInt16,
    statement_code LowCardinality(String),
    statement_name LowCardinality(String),
    consolidation_type LowCardinality(String),
    grupo_dfp String,
    currency LowCardinality(String),
    scale LowCardinality(String),
    original_order LowCardinality(String),
    period_start_date Nullable(Date32),
    period_end_date Nullable(Date32),
    equity_column String,
    account_code LowCardinality(String),
    account_description_original String,
    amount_original Nullable(Decimal(38, 10)),
    amount_usd Nullable(Decimal(38, 6)),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    fixed_account_flag LowCardinality(String),
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (
    cnpj,
    reference_date,
    version,
    statement_code,
    consolidation_type,
    ifNull(period_end_date, toDate32('1970-01-01')),
    account_code,
    equity_column
);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_itr_capital_composition
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    itr_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    cvm_code LowCardinality(String),
    reference_date Date32,
    version UInt16,
    ordinary_shares_paid_in UInt64,
    preferred_shares_paid_in UInt64,
    total_shares_paid_in UInt64,
    ordinary_shares_treasury Int64,
    preferred_shares_treasury Int64,
    total_shares_treasury Int64,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version);

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_itr_auditor_reports
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    itr_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    cvm_code LowCardinality(String),
    reference_date Date32,
    version UInt16,
    auditor_report_type String,
    opinion_statement_type String,
    opinion_item_number String,
    report_text_original String,
    source_archive_key String,
    source_file_name LowCardinality(String),
    source_row_number UInt64,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (cnpj, reference_date, version, opinion_statement_type, opinion_item_number);
