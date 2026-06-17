CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.fi_financial_statements
    ADD COLUMN IF NOT EXISTS root_name LowCardinality(String) AFTER xml_size_bytes,
    ADD COLUMN IF NOT EXISTS schema_refs Array(String) AFTER root_name,
    ADD COLUMN IF NOT EXISTS taxonomy_entrypoint String AFTER schema_refs,
    ADD COLUMN IF NOT EXISTS parsed_at DateTime64(3, 'UTC') AFTER parser_version;

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_contexts
(
    statement_key String,
    context_id String,
    entity_identifier Nullable(String),
    entity_scheme Nullable(String),
    period_type LowCardinality(String),
    instant_date Nullable(Date),
    period_start Nullable(Date),
    period_end Nullable(Date),
    dimensions Array(Tuple(dimension_code String, member_code String, member_label_fi Nullable(String))),
    mcy_member_code Nullable(String),
    mcy_member_label_fi Nullable(String),
    ref_member_code Nullable(String),
    ref_member_label_fi Nullable(String),
    is_comparative UInt8,
    parsed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (statement_key, context_id);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_units
(
    statement_key String,
    unit_id String,
    measures Array(String),
    is_divide UInt8,
    raw_xml String,
    parsed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (statement_key, unit_id);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_facts_raw
(
    statement_key String,
    business_id String,
    financial_date Date,
    fact_ordinal UInt32,
    concept_qname LowCardinality(String),
    concept_namespace String,
    concept_local_name LowCardinality(String),
    context_id String,
    unit_id Nullable(String),
    decimals Nullable(String),
    precision Nullable(String),
    value_kind LowCardinality(String),
    raw_value String,
    numeric_value Nullable(Decimal(38, 6)),
    date_value Nullable(Date),
    text_value Nullable(String),
    mcy_member_code Nullable(String),
    mcy_member_label_fi Nullable(String),
    ref_member_code Nullable(String),
    ref_member_label_fi Nullable(String),
    is_comparative UInt8,
    dimensions Array(Tuple(dimension_code String, member_code String, member_label_fi Nullable(String))),
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(parsed_at)
PARTITION BY toYYYYMM(financial_date)
ORDER BY (business_id, financial_date, concept_qname, fact_ordinal);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_taxonomy_codes
(
    taxonomy_version String,
    code String,
    code_kind LowCardinality(String),
    namespace_hint Nullable(String),
    label_fi String,
    label_en Nullable(String),
    metric_name_hint Nullable(String),
    template_sheet Nullable(String),
    template_row Nullable(UInt32),
    template_row_text Nullable(String),
    source_artifact String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (taxonomy_version, code, code_kind);

CREATE TABLE IF NOT EXISTS corpscout.fi_financial_metrics_long
(
    statement_key String,
    business_id String,
    financial_date Date,
    period_start Nullable(Date),
    period_end Nullable(Date),
    metric_key LowCardinality(String),
    metric_label String,
    period_reference LowCardinality(String),
    amount_original Nullable(Decimal(38, 6)),
    currency_original LowCardinality(String),
    amount_usd Nullable(Decimal(38, 6)),
    fx_rate_to_usd Nullable(Decimal(38, 12)),
    fx_rate_date Nullable(Date),
    fx_converted_at Nullable(DateTime64(3, 'UTC')),
    source_concept_qname String,
    source_mcy_member_code String,
    source_ref_member_code Nullable(String),
    source_fact_ordinal UInt32,
    mapping_version LowCardinality(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    derived_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(derived_at)
ORDER BY (business_id, financial_date, metric_key, period_reference, source_fact_ordinal);
