CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_financial_facts
    ADD COLUMN IF NOT EXISTS context_period_start Nullable(Date32)
    AFTER context_id;

ALTER TABLE corpscout.se_financial_facts
    ADD COLUMN IF NOT EXISTS context_period_end Nullable(Date32)
    AFTER context_period_start;

CREATE TABLE IF NOT EXISTS corpscout.se_bolagsverket_financial_observations
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_statement_key String,
    company_id String,
    source_fiscal_year Nullable(Int32),
    source_report_period_start Nullable(Date32),
    source_report_period_end Nullable(Date32),
    represented_fiscal_year Int32,
    represented_period_start Nullable(Date32),
    represented_period_end Date32,
    observation_kind LowCardinality(String),
    source_context_id String,
    source_fact_ordinal UInt64,
    source_concept_qname String,
    source_concept_namespace String,
    source_concept_local_name String,
    metric_code LowCardinality(String),
    unit_id Nullable(String),
    decimals Nullable(String),
    precision Nullable(String),
    source_raw_value String,
    value_original Nullable(Decimal(38, 10)),
    currency LowCardinality(Nullable(String)),
    value_usd Nullable(Decimal(38, 10)),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date32),
    fx_source String,
    dimensions String,
    mapping_version LowCardinality(String),
    revenue_overlap_relative_diff Nullable(Float64),
    quality_flags Array(String),
    parser_version LowCardinality(String),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (
    company_id,
    represented_fiscal_year,
    metric_code,
    source_statement_key,
    source_fact_ordinal
);
