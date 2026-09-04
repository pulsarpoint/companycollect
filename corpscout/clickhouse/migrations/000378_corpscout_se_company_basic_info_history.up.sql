CREATE DATABASE IF NOT EXISTS corpscout;

-- Append-only: one row per change of a company's se_company_basic_info row, written by
-- the fold when the folded row differs from the current main row, including the first
-- publish (2026-09-03 SE basic-info design, section 3.4). The columns are the main
-- row's, plus changed_fields naming the fields whose value or source changed (every
-- non-NULL field on the first publish).
CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_history
(
    company_id String,
    legal_name String,
    legal_name_source LowCardinality(String),
    legal_form_code Nullable(String),
    legal_form_code_source LowCardinality(String),
    status LowCardinality(String),
    status_source LowCardinality(String),
    incorporation_date Nullable(Date32),
    incorporation_date_source LowCardinality(String),
    lei Nullable(String),
    lei_source LowCardinality(String),
    wikidata_id Nullable(String),
    wikidata_id_source LowCardinality(String),
    description Nullable(String),
    description_source LowCardinality(String),
    description_language Nullable(String),
    description_sv Nullable(String),
    description_sv_source LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    changed_fields Array(String)
)
ENGINE = MergeTree
ORDER BY (company_id, folded_at);
