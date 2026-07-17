CREATE DATABASE IF NOT EXISTS corpscout;

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
    raw_xml String,
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (statement_key, context_id);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_units
(
    statement_key String,
    unit_id String,
    measures Array(String),
    numerator_measures Array(String),
    denominator_measures Array(String),
    is_divide UInt8,
    raw_xml String,
    parser_version LowCardinality(String),
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
    currency Nullable(String),
    decimals Nullable(String),
    precision Nullable(String),
    is_nil UInt8,
    xml_lang Nullable(String),
    value_kind LowCardinality(String),
    raw_value String,
    numeric_value Nullable(Decimal(38, 10)),
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
ORDER BY (statement_key, fact_ordinal);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_taxonomy_codes
(
    taxonomy_version String,
    code String,
    code_kind LowCardinality(String),
    namespace_hint Nullable(String),
    label_fi String,
    label_en Nullable(String),
    label_sv Nullable(String),
    metric_name_hint Nullable(String),
    source_artifact String,
    source_url String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (taxonomy_version, code);

ALTER TABLE corpscout.fi_financial_metrics
    ADD COLUMN IF NOT EXISTS fiscal_year Nullable(UInt16) AFTER period_end,
    ADD COLUMN IF NOT EXISTS xml_source_uri Nullable(String) AFTER xml_object_key,
    ADD COLUMN IF NOT EXISTS taxonomy_entrypoint Nullable(String) AFTER xml_size_bytes,
    ADD COLUMN IF NOT EXISTS fx_source Nullable(String) AFTER fx_rate_date;

CREATE OR REPLACE VIEW corpscout.fi_financial_facts_with_source AS
SELECT
    facts.statement_key,
    facts.business_id,
    facts.financial_date,
    reports.registration_date,
    reports.period_start,
    reports.period_end,
    facts.fact_ordinal,
    facts.concept_qname,
    facts.concept_namespace,
    facts.concept_local_name,
    concept_codes.label_fi AS concept_label_fi,
    concept_codes.label_en AS concept_label_en,
    facts.context_id,
    contexts.entity_identifier,
    contexts.entity_scheme,
    contexts.period_type,
    contexts.instant_date,
    contexts.period_start AS context_period_start,
    contexts.period_end AS context_period_end,
    contexts.dimensions,
    contexts.is_comparative,
    facts.unit_id,
    units.measures,
    units.numerator_measures,
    units.denominator_measures,
    facts.currency,
    facts.decimals,
    facts.precision,
    facts.is_nil,
    facts.xml_lang,
    facts.value_kind,
    facts.raw_value,
    facts.numeric_value,
    facts.date_value,
    facts.text_value,
    facts.mcy_member_code,
    mcy_codes.label_fi AS mcy_member_label_fi,
    mcy_codes.label_en AS mcy_member_label_en,
    facts.ref_member_code,
    ref_codes.label_fi AS ref_member_label_fi,
    ref_codes.label_en AS ref_member_label_en,
    reports.source_url,
    reports.xml_object_key,
    concat('s3://source-finland-prh-xbrl/', reports.xml_object_key) AS xml_source_uri,
    reports.xml_sha256,
    reports.xml_size_bytes,
    reports.taxonomy_entrypoint,
    reports.schema_refs,
    reports.parser_version,
    reports.source_run_id,
    reports.resolved_at
FROM corpscout.fi_xbrl_facts_raw AS facts
INNER JOIN corpscout.fi_financial_statements AS reports
    ON reports.statement_key = facts.statement_key
INNER JOIN corpscout.fi_xbrl_contexts AS contexts
    ON contexts.statement_key = facts.statement_key
    AND contexts.context_id = facts.context_id
LEFT JOIN corpscout.fi_xbrl_units AS units
    ON units.statement_key = facts.statement_key
    AND units.unit_id = facts.unit_id
LEFT JOIN corpscout.fi_xbrl_taxonomy_codes AS concept_codes
    ON concept_codes.code = facts.concept_qname
LEFT JOIN corpscout.fi_xbrl_taxonomy_codes AS mcy_codes
    ON mcy_codes.code = facts.mcy_member_code
LEFT JOIN corpscout.fi_xbrl_taxonomy_codes AS ref_codes
    ON ref_codes.code = facts.ref_member_code;
