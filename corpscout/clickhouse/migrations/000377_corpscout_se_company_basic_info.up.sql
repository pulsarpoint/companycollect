CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per published company: the fold of every current suggestion row by the
-- per-field precedence of section 4 (2026-09-03 SE basic-info design, section 3.3).
-- Beside every folded value sits the source that supplied it. A _source column is ''
-- when the field has no value, status itself is '' then (not nullable, as on the old
-- table) and every other value column is NULL. description_language follows the row
-- that won description and has no source column of its own. A company gets a row only
-- when SCB or Bolagsverket supplies its legal name. Legal-form labels are not stored,
-- the serving view joins se_code_labels as today. folded_at is the version.
CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info
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

    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(folded_at)
ORDER BY company_id;
