CREATE DATABASE IF NOT EXISTS corpscout;

-- Company-anchored person enrichment: wikidata_company_people links a company to the
-- people Wikidata associates with it (CEO/founder/chairperson/board member/person-valued
-- owned-by), one row per (company, role_property, person). wikidata_persons holds one
-- row per person (deduped across every company they are linked from), keyed on the
-- Wikidata person id -- there is no name-based matching anywhere in this pair, the
-- company/person anchor is always a Wikidata QID.
CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_people
(
    company_wikidata_id String,
    person_wikidata_id String,
    role_property LowCardinality(String),
    role_label LowCardinality(String),
    start_date Nullable(Date),
    end_date Nullable(Date),
    is_current UInt8,
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_wikidata_id, role_property, person_wikidata_id);

CREATE TABLE IF NOT EXISTS corpscout.wikidata_persons
(
    person_wikidata_id String,
    name String,
    name_normalized String,
    description Nullable(String),
    birth_year Nullable(UInt16),
    image_url Nullable(String),
    wikidata_url Nullable(String),
    source_system LowCardinality(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash FixedString(64),
    retrieved_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (person_wikidata_id);
