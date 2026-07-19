CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.company_people_all
(
    country_iso2 LowCardinality(String),
    company_id String,
    company_name String,
    first_name String,
    last_name String,
    full_name_normalized String, -- lowerUTF8(trim(first || ' ' || last))
    role_original String,
    role_kind LowCardinality(String),
    signatory_kind LowCardinality(String),
    fiscal_year Int32,
    identifier_kind LowCardinality(String), -- '' for SE (no public person id)
    identifier_value String,
    source LowCardinality(String), -- 'se_xbrl_signatures'
    source_statement_key String,
    resolved_at DateTime64(3, 'UTC'),
    INDEX idx_people_name full_name_normalized TYPE ngrambf_v1(3, 65536, 3, 7) GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY (full_name_normalized, country_iso2, company_id, fiscal_year);
