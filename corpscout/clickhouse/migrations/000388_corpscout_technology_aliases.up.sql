CREATE DATABASE IF NOT EXISTS corpscout;

-- Reviewed synonyms for the existing technology_catalog name key.
-- Unicode NFKC, case folding and whitespace normalization produce alias_key
-- in the publisher. Capitalization-only variants resolve through the catalog.
-- The catalog asset validates targets and key uniqueness, then exchanges the
-- complete snapshot. An explicitly empty curated alias list is valid.
CREATE TABLE IF NOT EXISTS corpscout.technology_aliases
(
    alias String,
    alias_key String,
    technology String,
    match_mode LowCardinality(String),
    review_status LowCardinality(String),
    reviewed_by String,
    reviewed_at Date,
    review_note String,
    source_references Array(String),
    source LowCardinality(String),
    source_version String,
    source_run_id String,
    updated_at DateTime64(3, 'UTC'),
    CONSTRAINT alias_is_nonempty CHECK notEmpty(alias) AND notEmpty(alias_key),
    CONSTRAINT target_is_nonempty CHECK notEmpty(technology),
    CONSTRAINT match_mode_is_supported CHECK match_mode = 'case_insensitive',
    CONSTRAINT alias_is_reviewed CHECK review_status = 'accepted' AND notEmpty(reviewed_by),
    CONSTRAINT review_has_evidence CHECK notEmpty(review_note) AND notEmpty(source_references)
)
ENGINE = MergeTree
ORDER BY (alias_key, technology);
