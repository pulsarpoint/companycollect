CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.country_person_correction
(
    country_iso2 LowCardinality(String),
    correction_id UUID,
    review_id UUID,
    observation_id Nullable(UUID), -- NULL means the decision applies to the whole source identity
    from_person_id UUID,
    to_person_id UUID,
    correction_kind LowCardinality(String), -- 'reassign' | 'split' | 'merge' | 'undo'
    reason String,
    decided_by String,
    supersedes_correction_id Nullable(UUID),
    created_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_iso2
ORDER BY (
    country_iso2,
    from_person_id,
    ifNull(observation_id, toUUID('00000000-0000-0000-0000-000000000000')),
    created_at,
    correction_id
);
