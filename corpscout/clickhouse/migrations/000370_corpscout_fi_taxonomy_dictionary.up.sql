CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.fi_taxonomy_concepts
(
    taxonomy_version LowCardinality(String),
    concept_qname String,
    concept_namespace String,
    concept_local_name String,
    substitution_group String,
    is_abstract UInt8,
    item_type String,
    balance LowCardinality(String),
    period_type LowCardinality(String),
    presentation_parent String,
    presentation_order Float64,
    presentation_role String,
    calculation_parent String,
    calculation_weight Float64,
    calculation_role String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (taxonomy_version, concept_qname, presentation_role, calculation_role);

CREATE TABLE IF NOT EXISTS corpscout.fi_taxonomy_labels
(
    taxonomy_version LowCardinality(String),
    concept_qname String,
    language LowCardinality(String),
    label_role String,
    label String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (taxonomy_version, concept_qname, language, label_role);
