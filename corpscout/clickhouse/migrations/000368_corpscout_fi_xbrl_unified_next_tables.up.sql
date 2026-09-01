CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_documents_next
(
    statement_key String,
    source_run_id String,
    business_id String,
    financial_date Date,
    registration_date Nullable(Date),
    source_url String,
    xml_object_key String,
    xml_sha256 String,
    xml_size_bytes UInt64,
    root_name LowCardinality(String),
    schema_refs String,
    taxonomy_entrypoint String,
    reported_entity_id String,
    reported_company_name String,
    reported_period_start Nullable(Date),
    reported_period_end Nullable(Date),
    contexts_count UInt32,
    units_count UInt32,
    facts_count UInt32,
    validation_warnings String,
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (business_id, financial_date, statement_key);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_contexts_next
(
    statement_key String,
    context_id String,
    entity_identifier String,
    entity_scheme String,
    period_type LowCardinality(String),
    instant_date Nullable(Date),
    period_start Nullable(Date),
    period_end Nullable(Date),
    dimensions String,
    is_comparative UInt8,
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC'),
    mcy_member_code String,
    ref_member_code String
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (statement_key, context_id);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_units_next
(
    statement_key String,
    unit_id String,
    measures String,
    numerator_measures String,
    denominator_measures String,
    is_divide UInt8,
    currency LowCardinality(String),
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(parsed_at)
ORDER BY (statement_key, unit_id);

CREATE TABLE IF NOT EXISTS corpscout.fi_xbrl_facts_next
(
    statement_key String,
    business_id String,
    financial_date Date,
    fact_ordinal UInt32,
    concept_qname LowCardinality(String),
    concept_namespace String,
    concept_local_name LowCardinality(String),
    context_id String,
    unit_id String,
    currency LowCardinality(String),
    decimals String,
    precision String,
    is_nil UInt8,
    xml_lang LowCardinality(String),
    value_kind LowCardinality(String),
    raw_value String,
    numeric_value Nullable(Decimal(38, 6)),
    date_value Nullable(Date),
    text_value String,
    dimensions String,
    is_comparative UInt8,
    parser_version LowCardinality(String),
    parsed_at DateTime64(3, 'UTC'),
    mcy_member_code String,
    ref_member_code String
)
ENGINE = ReplacingMergeTree(parsed_at)
PARTITION BY toYYYYMM(financial_date)
ORDER BY (business_id, financial_date, concept_qname, fact_ordinal);
