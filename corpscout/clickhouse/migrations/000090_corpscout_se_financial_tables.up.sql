CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_financial_reports
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    statement_key String,
    company_id String,
    report_period_start Nullable(Date32),
    report_period_end Nullable(Date32),
    fiscal_year Nullable(UInt16),
    reported_company_name Nullable(String),
    report_language LowCardinality(Nullable(String)),
    source_archive_key String,
    source_archive_name String,
    nested_zip_name String,
    xhtml_object_key String,
    xhtml_sha256 FixedString(64),
    xhtml_size_bytes UInt64,
    taxonomy_entrypoint Nullable(String),
    schema_refs String,
    contexts_count UInt64,
    units_count UInt64,
    facts_count UInt64,
    parser_version LowCardinality(String),
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (
    company_id,
    ifNull(report_period_end, toDate32('1970-01-01')),
    statement_key
);

CREATE TABLE IF NOT EXISTS corpscout.se_financial_facts
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    statement_key String,
    company_id String,
    report_period_end Nullable(Date32),
    fact_ordinal UInt64,
    concept_qname String,
    concept_namespace String,
    concept_local_name String,
    context_id String,
    unit_id Nullable(String),
    decimals Nullable(String),
    precision Nullable(String),
    value_kind LowCardinality(String),
    raw_value String,
    amount_original Nullable(Decimal(38, 10)),
    amount_usd Nullable(Decimal(38, 10)),
    date_value Nullable(Date32),
    text_value Nullable(String),
    currency LowCardinality(Nullable(String)),
    dimensions String,
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date32),
    fx_source String,
    parser_version LowCardinality(String),
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (
    company_id,
    ifNull(report_period_end, toDate32('1970-01-01')),
    statement_key,
    fact_ordinal
);

CREATE TABLE IF NOT EXISTS corpscout.se_financial_metrics
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    statement_key String,
    company_id String,
    report_period_start Nullable(Date32),
    report_period_end Nullable(Date32),
    fiscal_year Nullable(UInt16),
    reported_company_name Nullable(String),
    currency LowCardinality(String),
    revenue_amount_original Nullable(Decimal(38, 6)),
    revenue_amount_usd Nullable(Decimal(38, 6)),
    operating_profit_loss_amount_original Nullable(Decimal(38, 6)),
    operating_profit_loss_amount_usd Nullable(Decimal(38, 6)),
    profit_loss_amount_original Nullable(Decimal(38, 6)),
    profit_loss_amount_usd Nullable(Decimal(38, 6)),
    total_assets_amount_original Nullable(Decimal(38, 6)),
    total_assets_amount_usd Nullable(Decimal(38, 6)),
    equity_amount_original Nullable(Decimal(38, 6)),
    equity_amount_usd Nullable(Decimal(38, 6)),
    liabilities_amount_original Nullable(Decimal(38, 6)),
    liabilities_amount_usd Nullable(Decimal(38, 6)),
    cash_and_bank_amount_original Nullable(Decimal(38, 6)),
    cash_and_bank_amount_usd Nullable(Decimal(38, 6)),
    current_assets_amount_original Nullable(Decimal(38, 6)),
    current_assets_amount_usd Nullable(Decimal(38, 6)),
    current_liabilities_amount_original Nullable(Decimal(38, 6)),
    current_liabilities_amount_usd Nullable(Decimal(38, 6)),
    personnel_expenses_amount_original Nullable(Decimal(38, 6)),
    personnel_expenses_amount_usd Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_original Nullable(Decimal(38, 6)),
    wages_and_salaries_amount_usd Nullable(Decimal(38, 6)),
    employees Nullable(UInt64),
    source_fact_count UInt64,
    mapped_fact_count UInt64,
    unmapped_numeric_fact_count UInt64,
    metric_warnings String,
    mapping_version LowCardinality(String),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date32),
    fx_source String,
    source_payload_hash FixedString(64),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (
    company_id,
    ifNull(report_period_end, toDate32('1970-01-01')),
    statement_key
);
