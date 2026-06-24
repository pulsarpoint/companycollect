CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.br_cnae_to_nace
(
    cnae_version LowCardinality(String),
    cnae_code String,
    cnae_normalized_code String,
    cnae_description_pt String,
    cnae_description_en String,
    nace_revision LowCardinality(String),
    nace_code String,
    nace_normalized_code String,
    nace_description_en String,
    mapping_source String,
    source_url String,
    source_payload_hash FixedString(64),
    source_run_id String,
    pulled_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(pulled_at)
ORDER BY (cnae_version, cnae_normalized_code, nace_revision, nace_normalized_code);
