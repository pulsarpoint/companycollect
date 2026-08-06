CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.country_person
(
    country_iso2 LowCardinality(String),
    person_id UUID,
    preferred_name String,
    preferred_name_normalized String,
    resolution_status LowCardinality(String), -- 'verified' | 'reviewed' | 'provisional' | 'unresolved' | 'merged'
    resolution_method LowCardinality(String), -- strongest evidence used for this country-scoped identity
    merged_into_person_id Nullable(UUID),
    first_observed_year Int32,
    last_observed_year Int32,
    observation_count UInt32,
    company_count UInt32,
    resolved_at DateTime64(3, 'UTC'),
    INDEX idx_country_person_name preferred_name_normalized TYPE ngrambf_v1(3, 65536, 3, 7) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY country_iso2
ORDER BY (country_iso2, person_id);

CREATE TABLE IF NOT EXISTS corpscout.country_person_observation
(
    country_iso2 LowCardinality(String),
    observation_id UUID,
    source LowCardinality(String),
    source_record_id String,
    source_person_key String,
    company_id String,
    company_name String,
    observed_first_name String,
    observed_last_name String,
    observed_full_name String,
    observed_name_normalized String,
    role_original String,
    role_kind LowCardinality(String),
    signatory_kind LowCardinality(String),
    fiscal_year Int32,
    identifier_kind LowCardinality(String),
    identifier_value String,
    source_statement_key String,
    resolved_at DateTime64(3, 'UTC'),
    INDEX idx_country_person_observation_name observed_name_normalized TYPE ngrambf_v1(3, 65536, 3, 7) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY country_iso2
ORDER BY (country_iso2, observation_id);

CREATE TABLE IF NOT EXISTS corpscout.country_person_identifier
(
    country_iso2 LowCardinality(String),
    identifier_id UUID,
    person_id UUID,
    source LowCardinality(String),
    identifier_kind LowCardinality(String),
    identifier_value String,
    observation_id UUID,
    is_public UInt8,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_iso2
ORDER BY (country_iso2, source, identifier_kind, identifier_value, observation_id);

CREATE TABLE IF NOT EXISTS corpscout.country_person_match
(
    country_iso2 LowCardinality(String),
    observation_id UUID,
    person_id UUID,
    match_method LowCardinality(String), -- automatic evidence or a 'reviewed_*' correction method
    match_status LowCardinality(String), -- 'accepted' | 'reviewed' | 'provisional' | 'unresolved'
    confidence UInt8,
    resolver_version UInt16,
    decided_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_iso2
ORDER BY (country_iso2, observation_id, person_id);
