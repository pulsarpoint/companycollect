CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.ee_financial_statements
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_line_number UInt64,
    source_record_id String,
    report_id String,
    reg_code String,
    fiscal_year Nullable(Int32),
    period_start_date Nullable(Date),
    period_end_date Nullable(Date),
    submitted_date Nullable(Date),
    is_consolidated UInt8,
    is_audited UInt8,
    report_category_original String,
    report_category_en String,
    currency LowCardinality(String),
    revenue Nullable(Decimal(38, 2)),
    gross_profit Nullable(Decimal(38, 2)),
    pretax_result Nullable(Decimal(38, 2)),
    net_result Nullable(Decimal(38, 2)),
    total_assets Nullable(Decimal(38, 2)),
    current_assets Nullable(Decimal(38, 2)),
    non_current_assets Nullable(Decimal(38, 2)),
    equity Nullable(Decimal(38, 2)),
    current_liabilities Nullable(Decimal(38, 2)),
    non_current_liabilities Nullable(Decimal(38, 2)),
    source_url String
)
ENGINE = ReplacingMergeTree
ORDER BY (reg_code, report_id);
