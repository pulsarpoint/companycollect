CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.br_cvm_financial_metrics
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_dataset LowCardinality(String),
    source_year UInt16,
    cnpj String,
    cnpj_basico String,
    company_name String,
    cvm_code LowCardinality(String),
    reference_date Date32,
    period_start_date Nullable(Date32),
    period_end_date Nullable(Date32),
    period_type LowCardinality(String),
    version UInt16,
    is_latest_version Bool,
    consolidation_type LowCardinality(String),
    metric_name LowCardinality(String),
    metric_label String,
    currency LowCardinality(String),
    amount_original Nullable(Decimal(38, 6)),
    amount_usd Nullable(Decimal(38, 6)),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_source String,
    source_statement_code LowCardinality(String),
    source_statement_name String,
    source_account_codes String,
    source_account_descriptions_original String,
    source_statement_run_ids String,
    source_statement_record_ids String,
    source_archive_keys String,
    source_file_names String,
    source_statement_row_count UInt64,
    metric_mapping_version LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (
    cnpj,
    source_dataset,
    reference_date,
    ifNull(period_start_date, toDate32('1970-01-01')),
    ifNull(period_end_date, toDate32('1970-01-01')),
    consolidation_type,
    metric_name,
    version
);
