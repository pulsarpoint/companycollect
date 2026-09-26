-- Deterministic crawl observations, published by attempt after all detail writes.
-- The archive remains authoritative. These are website observations, not company ownership claims.
-- Writers serialize normalization revisions and insert website_crawl_scans last.
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.website_crawl_scans (
    domain String,
    crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3),
    request_id String,
    attempt UInt32,
    normalization_id UUID,
    normalization_revision UInt64,
    parser_version String,
    source_schema String,
    source_ingested_at DateTime64(6, 'UTC'),
    normalized_at DateTime64(6, 'UTC') DEFAULT now64(6),
    source_run_id String,
    normalization_run_id String,
    input_revision UInt64,
    work_key String,
    website_url String,
    final_url Nullable(String),
    state Enum8('completed' = 1, 'failed' = 2, 'cancelled' = 3),
    crawl_status LowCardinality(String),
    successful Bool,
    started_at Nullable(DateTime64(6, 'UTC')),
    finished_at DateTime64(6, 'UTC'),
    stop_reason Nullable(String),
    error Nullable(String),
    archive_path String,
    page_count UInt32,
    row_counts Map(String, UInt64),
    structured_jobs_status Enum8('not_available' = 1, 'completed' = 2, 'partial' = 3, 'failed' = 4),
    CONSTRAINT valid_identity CHECK notEmpty(domain) AND notEmpty(request_id) AND attempt > 0,
    CONSTRAINT valid_normalization CHECK normalization_id != toUUID('00000000-0000-0000-0000-000000000000'),
    CONSTRAINT valid_revision CHECK normalization_revision > 0 AND notEmpty(parser_version),
    CONSTRAINT valid_success CHECK NOT successful OR (state = 'completed' AND crawl_status IN ('finished', 'skip_crawling') AND empty(ifNull(error, ''))),
    CONSTRAINT valid_times CHECK isNull(started_at) OR started_at <= finished_at
)
ENGINE = ReplacingMergeTree(normalization_revision)
ORDER BY (domain, crawl_type, request_id, attempt);

CREATE TABLE IF NOT EXISTS corpscout.website_crawl_site_profiles (
    domain String,
    crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3),
    request_id String,
    attempt UInt32,
    normalization_id UUID,
    page_id String,
    row_index UInt32,
    source_url String,
    source_locator String,
    evidence_status LowCardinality(String),
    crawl_decision LowCardinality(String),
    scope LowCardinality(String),
    site_types Array(String),
    research_profiles Array(String),
    purpose_original Nullable(String),
    operator_name Nullable(String),
    description_original Nullable(String),
    evidence Array(String),
    full_crawl_all Nullable(Bool),
    classification_overridden Nullable(Bool),
    CONSTRAINT valid_identity CHECK notEmpty(domain) AND notEmpty(request_id) AND attempt > 0,
    CONSTRAINT valid_normalization CHECK normalization_id != toUUID('00000000-0000-0000-0000-000000000000'),
    CONSTRAINT valid_page CHECK notEmpty(page_id),
    CONSTRAINT valid_override CHECK NOT ifNull(classification_overridden, false) OR (ifNull(full_crawl_all, false) AND crawl_decision = 'skip_crawling')
)
ENGINE = ReplacingMergeTree
ORDER BY (domain, crawl_type, request_id, attempt, normalization_id, page_id, row_index);

CREATE TABLE IF NOT EXISTS corpscout.website_crawl_business_activities (
    domain String,
    crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3),
    request_id String,
    attempt UInt32,
    normalization_id UUID,
    page_id String,
    row_index UInt32,
    source_url String,
    source_locator String,
    activity_original String,
    scope LowCardinality(String),
    classification_evidence Array(String),
    CONSTRAINT valid_identity CHECK notEmpty(domain) AND notEmpty(request_id) AND attempt > 0,
    CONSTRAINT valid_normalization CHECK normalization_id != toUUID('00000000-0000-0000-0000-000000000000'),
    CONSTRAINT valid_page CHECK notEmpty(page_id)
)
ENGINE = ReplacingMergeTree
ORDER BY (domain, crawl_type, request_id, attempt, normalization_id, page_id, row_index);

CREATE TABLE IF NOT EXISTS corpscout.website_crawl_pages (
    domain String,
    crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3),
    request_id String,
    attempt UInt32,
    normalization_id UUID,
    page_id String,
    row_index UInt32,
    source_url String,
    source_locator String,
    requested_url String,
    final_url Nullable(String),
    canonical_url Nullable(String),
    http_status Nullable(UInt16),
    fetch_status LowCardinality(String),
    observation_status Nullable(String),
    observation_schema Nullable(String),
    fetched_at Nullable(DateTime64(6, 'UTC')),
    title_original Nullable(String),
    description_original Nullable(String),
    language Nullable(String),
    errors Array(String),
    CONSTRAINT valid_identity CHECK notEmpty(domain) AND notEmpty(request_id) AND attempt > 0,
    CONSTRAINT valid_normalization CHECK normalization_id != toUUID('00000000-0000-0000-0000-000000000000'),
    CONSTRAINT valid_page CHECK notEmpty(page_id)
)
ENGINE = ReplacingMergeTree
ORDER BY (domain, crawl_type, request_id, attempt, normalization_id, page_id, row_index);

CREATE TABLE IF NOT EXISTS corpscout.website_crawl_contacts (
    domain String,
    crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3),
    request_id String,
    attempt UInt32,
    normalization_id UUID,
    page_id String,
    row_index UInt32,
    source_url String,
    source_locator String,
    contact_type LowCardinality(String),
    value String,
    raw_value Nullable(String),
    extraction_source LowCardinality(String),
    entity_reference Nullable(String),
    CONSTRAINT valid_identity CHECK notEmpty(domain) AND notEmpty(request_id) AND attempt > 0,
    CONSTRAINT valid_normalization CHECK normalization_id != toUUID('00000000-0000-0000-0000-000000000000'),
    CONSTRAINT valid_page CHECK notEmpty(page_id)
)
ENGINE = ReplacingMergeTree
ORDER BY (domain, crawl_type, request_id, attempt, normalization_id, page_id, row_index);

CREATE TABLE IF NOT EXISTS corpscout.website_crawl_identifiers (
    domain String,
    crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3),
    request_id String,
    attempt UInt32,
    normalization_id UUID,
    page_id String,
    row_index UInt32,
    source_url String,
    source_locator String,
    identifier_type LowCardinality(String),
    value String,
    raw_value Nullable(String),
    extraction_source LowCardinality(String),
    entity_reference Nullable(String),
    CONSTRAINT valid_identity CHECK notEmpty(domain) AND notEmpty(request_id) AND attempt > 0,
    CONSTRAINT valid_normalization CHECK normalization_id != toUUID('00000000-0000-0000-0000-000000000000'),
    CONSTRAINT valid_page CHECK notEmpty(page_id)
)
ENGINE = ReplacingMergeTree
ORDER BY (domain, crawl_type, request_id, attempt, normalization_id, page_id, row_index);

CREATE TABLE IF NOT EXISTS corpscout.website_crawl_structured_data (
    domain String,
    crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3),
    request_id String,
    attempt UInt32,
    normalization_id UUID,
    page_id String,
    row_index UInt32,
    source_url String,
    source_locator String,
    script_index Nullable(UInt32),
    entity_path String,
    entity_id Nullable(String),
    entity_types Array(String),
    property_path String,
    array_index Nullable(UInt32),
    value_type Enum8('string' = 1, 'number' = 2, 'boolean' = 3, 'null' = 4),
    value_string Nullable(String),
    value_number Nullable(Float64),
    value_number_original Nullable(String),
    value_boolean Nullable(Bool),
    parse_status LowCardinality(String),
    CONSTRAINT valid_identity CHECK notEmpty(domain) AND notEmpty(request_id) AND attempt > 0,
    CONSTRAINT valid_normalization CHECK normalization_id != toUUID('00000000-0000-0000-0000-000000000000'),
    CONSTRAINT valid_page CHECK notEmpty(page_id),
    CONSTRAINT valid_string_value CHECK isNotNull(value_string) = (value_type = 'string'),
    CONSTRAINT valid_number_value CHECK isNotNull(value_number) = (value_type = 'number'),
    CONSTRAINT valid_number_original CHECK isNotNull(value_number_original) = (value_type = 'number'),
    CONSTRAINT valid_boolean_value CHECK isNotNull(value_boolean) = (value_type = 'boolean'),
    CONSTRAINT finite_number_value CHECK isFinite(ifNull(value_number, 0))
)
ENGINE = ReplacingMergeTree
ORDER BY (domain, crawl_type, request_id, attempt, normalization_id, page_id, row_index);

CREATE TABLE IF NOT EXISTS corpscout.website_crawl_links (
    domain String,
    crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3),
    request_id String,
    attempt UInt32,
    normalization_id UUID,
    page_id String,
    row_index UInt32,
    source_url String,
    source_locator String,
    target_url String,
    label_original Nullable(String),
    category LowCardinality(String),
    document_type Nullable(String),
    language Nullable(String),
    relationships Array(String),
    CONSTRAINT valid_identity CHECK notEmpty(domain) AND notEmpty(request_id) AND attempt > 0,
    CONSTRAINT valid_normalization CHECK normalization_id != toUUID('00000000-0000-0000-0000-000000000000'),
    CONSTRAINT valid_page CHECK notEmpty(page_id)
)
ENGINE = ReplacingMergeTree
ORDER BY (domain, crawl_type, request_id, attempt, normalization_id, page_id, row_index);

CREATE TABLE IF NOT EXISTS corpscout.website_crawl_jobs (
    domain String,
    crawl_type Enum8('full' = 1, 'jobs' = 2, 'site_info' = 3),
    request_id String,
    attempt UInt32,
    normalization_id UUID,
    page_id String,
    row_index UInt32,
    source_url String,
    source_locator String,
    source_job_id Nullable(String),
    job_url Nullable(String),
    title_original String,
    employer Nullable(String),
    location_original Nullable(String),
    department_original Nullable(String),
    employment_type Nullable(String),
    workplace_type Nullable(String),
    description_original Nullable(String),
    posted_at Nullable(DateTime64(6, 'UTC')),
    posted_at_original Nullable(String),
    expires_at Nullable(DateTime64(6, 'UTC')),
    expires_at_original Nullable(String),
    extraction_source Enum8('jsonld' = 1, 'legacy_record' = 2),
    evidence Array(String),
    CONSTRAINT valid_identity CHECK notEmpty(domain) AND notEmpty(request_id) AND attempt > 0,
    CONSTRAINT valid_normalization CHECK normalization_id != toUUID('00000000-0000-0000-0000-000000000000'),
    CONSTRAINT valid_page CHECK notEmpty(page_id)
)
ENGINE = ReplacingMergeTree
ORDER BY (domain, crawl_type, request_id, attempt, normalization_id, page_id, row_index);

-- Resolve scan revisions before selecting attempt recency or eligibility.
CREATE VIEW IF NOT EXISTS corpscout.website_crawl_scans_latest AS
SELECT * FROM corpscout.website_crawl_scans FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, crawl_type;

-- Skipped full/jobs crawls supply diagnostics and a profile, not completed deep data.
-- Basic-info classification remains usable when a non-company site is skipped.
CREATE VIEW IF NOT EXISTS corpscout.website_crawl_scans_latest_usable AS
SELECT * FROM corpscout.website_crawl_scans FINAL
WHERE successful AND state = 'completed' AND empty(ifNull(error, ''))
    AND (crawl_status = 'finished' OR (crawl_type = 'site_info' AND crawl_status = 'skip_crawling'))
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, crawl_type;

-- A classified shop still updates the first-page profile when its deep crawl is skipped.
CREATE VIEW IF NOT EXISTS corpscout.website_crawl_scans_latest_profile AS
SELECT s.*
FROM corpscout.website_crawl_scans AS s FINAL
INNER JOIN corpscout.website_crawl_site_profiles AS p FINAL
    ON s.domain = p.domain
    AND s.crawl_type = p.crawl_type
    AND s.request_id = p.request_id
    AND s.attempt = p.attempt
    AND s.normalization_id = p.normalization_id
WHERE s.successful AND s.state = 'completed' AND empty(ifNull(s.error, ''))
    AND s.crawl_status IN ('finished', 'skip_crawling')
    AND p.evidence_status = 'source_matched'
    AND p.crawl_decision IN ('continue_crawling', 'skip_crawling')
ORDER BY s.finished_at DESC, s.request_id DESC, s.attempt DESC
LIMIT 1 BY s.domain, s.crawl_type;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_site_profiles_published AS
SELECT d.*
FROM corpscout.website_crawl_site_profiles AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_site_profiles_current AS
SELECT d.*
FROM corpscout.website_crawl_site_profiles AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_profile AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_business_activities_published AS
SELECT d.*
FROM corpscout.website_crawl_business_activities AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_business_activities_current AS
SELECT d.*
FROM corpscout.website_crawl_business_activities AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_profile AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_pages_published AS
SELECT d.*
FROM corpscout.website_crawl_pages AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_pages_current AS
SELECT d.*
FROM corpscout.website_crawl_pages AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_contacts_published AS
SELECT d.*
FROM corpscout.website_crawl_contacts AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_contacts_current AS
SELECT d.*
FROM corpscout.website_crawl_contacts AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_identifiers_published AS
SELECT d.*
FROM corpscout.website_crawl_identifiers AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_identifiers_current AS
SELECT d.*
FROM corpscout.website_crawl_identifiers AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_structured_data_published AS
SELECT d.*
FROM corpscout.website_crawl_structured_data AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_structured_data_current AS
SELECT d.*
FROM corpscout.website_crawl_structured_data AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_links_published AS
SELECT d.*
FROM corpscout.website_crawl_links AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_links_current AS
SELECT d.*
FROM corpscout.website_crawl_links AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_jobs_published AS
SELECT d.*
FROM corpscout.website_crawl_jobs AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE VIEW IF NOT EXISTS corpscout.website_crawl_jobs_current AS
SELECT d.*
FROM corpscout.website_crawl_jobs AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;
