CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_domain_suggestion
(
    company_id String,
    source LowCardinality(String),
    slot String,
    root_domain String,
    website_url String,
    website_host String,
    association LowCardinality(String),
    is_primary UInt8,
    confidence Float64,
    confidence_basis String,
    source_record_id String,
    source_url String,
    evidence String,
    observed_at DateTime64(3, 'UTC'),
    removed UInt8,
    decided_by String,
    note String,
    suggestion_id String,
    suggested_at DateTime64(3, 'UTC'),
    source_run_id String,
    extractor_version String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_domain CHECK root_domain != '' AND slot != '',
    CONSTRAINT valid_association CHECK association IN ('connected', 'not_connected', 'uncertain'),
    CONSTRAINT valid_confidence CHECK isFinite(confidence) AND confidence BETWEEN 0 AND 1
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source, slot);

CREATE TABLE IF NOT EXISTS corpscout.se_company_domain
(
    company_id String,
    root_domain String,
    website_url String,
    website_host String,
    website_source LowCardinality(String),
    association LowCardinality(String),
    association_source LowCardinality(String),
    is_primary UInt8,
    primary_source LowCardinality(String),
    confidence Float64,
    sources Array(String),
    source_confidences Array(Float64),
    source_record_ids Array(String),
    source_urls Array(String),
    confidence_bases Array(String),
    evidence_hash String,
    verification_status LowCardinality(String),
    verification_reason String,
    verification_input_hash String,
    review_status LowCardinality(String),
    review_note String,
    reviewed_by String,
    reviewed_at Nullable(DateTime64(3, 'UTC')),
    reviewed_evidence_hash String,
    active UInt8,
    inactive_reason LowCardinality(String),
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    folded_at DateTime64(3, 'UTC'),
    fold_version String,
    fold_input_hash String,
    source_run_id String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_domain CHECK root_domain != '',
    CONSTRAINT valid_association CHECK association IN ('connected', 'not_connected', 'uncertain'),
    CONSTRAINT valid_primary CHECK is_primary = 0 OR active = 1,
    CONSTRAINT source_arrays_align CHECK length(sources) = length(source_confidences)
        AND length(sources) = length(source_record_ids) AND length(sources) = length(source_urls)
        AND length(sources) = length(confidence_bases)
)
ENGINE = ReplacingMergeTree(folded_at)
ORDER BY (company_id, root_domain);

CREATE TABLE IF NOT EXISTS corpscout.se_company_domain_history AS corpscout.se_company_domain
ENGINE = MergeTree ORDER BY (company_id, root_domain, folded_at);
ALTER TABLE corpscout.se_company_domain_history ADD COLUMN IF NOT EXISTS changed_fields Array(String);
ALTER TABLE corpscout.se_company_domain_history ADD COLUMN IF NOT EXISTS changed_at DateTime64(3, 'UTC');
ALTER TABLE corpscout.se_company_domain_history ADD COLUMN IF NOT EXISTS change_kind LowCardinality(String);
ALTER TABLE corpscout.se_company_domain_history ADD COLUMN IF NOT EXISTS fold_run_id String;

CREATE TABLE IF NOT EXISTS corpscout.se_company_domain_precedence
(
    company_id String,
    root_domain String,
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    removed UInt8 DEFAULT 0,
    decided_by String,
    note String,
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_global_scope CHECK company_id != '' OR root_domain = '',
    CONSTRAINT valid_field CHECK field IN ('website', 'association', 'primary')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, root_domain, field, source);

CREATE TABLE IF NOT EXISTS corpscout.se_company_domain_rule
(
    company_id String,
    root_domain String,
    action LowCardinality(String),
    removed UInt8 DEFAULT 0,
    decided_by String,
    note String,
    evidence_hash String,
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_domain CHECK root_domain != '',
    CONSTRAINT valid_action CHECK action IN ('confirmed_primary', 'confirmed_related', 'rejected', 'unreviewed')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, root_domain);

CREATE TABLE IF NOT EXISTS corpscout.se_company_domain_verification
(
    company_id String,
    root_domain String,
    input_hash String,
    data_hash String,
    prompt_hash String,
    model_hash String,
    input_json String,
    system_prompt String,
    status LowCardinality(String),
    verdict LowCardinality(String),
    confidence Float64,
    reason String,
    evidence_ids Array(String),
    provider String,
    model String,
    prompt_version String,
    prompt_tokens UInt64,
    completion_tokens UInt64,
    raw_response String,
    error String,
    verified_at DateTime64(3, 'UTC'),
    source_run_id String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_status CHECK status IN ('success', 'invalid_response', 'http_error'),
    CONSTRAINT valid_verdict CHECK verdict IN ('connected', 'not_connected', 'uncertain'),
    CONSTRAINT valid_confidence CHECK isFinite(confidence) AND confidence BETWEEN 0 AND 1
)
ENGINE = MergeTree
ORDER BY (company_id, root_domain, input_hash, verified_at, source_run_id);

-- Preserve every existing association and review before changing serving readers.
-- The first full processing pass replaces these imported rows with source-based folds.
INSERT INTO corpscout.se_company_domain
SELECT
    company_id, root_domain, website_url, website_host, if(empty(source_names), '', source_names[1]),
    if(review_status = 'rejected', 'not_connected', if(is_active, 'connected', 'uncertain')),
    'legacy_import', if(is_active AND review_status != 'rejected', suggested_primary, toUInt8(0)),
    'legacy_import', toFloat64(suggested_confidence), source_names,
    arrayMap(value -> toFloat64(value), source_confidences), source_record_ids, source_urls,
    confidence_bases, evidence_fingerprint, 'not_requested', '', '', review_status,
    review_note, reviewed_by, reviewed_at, reviewed_evidence_fingerprint,
    toUInt8(is_active AND review_status != 'rejected'),
    if(review_status = 'rejected', 'rejected', if(is_active, '', 'withdrawn')),
    first_seen_at, last_seen_at, now64(3, 'UTC'), 'domain-import-v1', '', 'migration-000408'
FROM corpscout.company_domains FINAL
WHERE country_code = 'SE' AND root_domain != ''
  AND match(company_id, '^([0-9]{10}|[0-9]{12})$');

INSERT INTO corpscout.se_company_domain_rule
SELECT company_id, root_domain, review_status, toUInt8(0), reviewed_by, review_note,
    reviewed_evidence_fingerprint, ifNull(reviewed_at, resolved_at)
FROM corpscout.company_domains FINAL
WHERE country_code = 'SE' AND review_status != 'unreviewed' AND root_domain != ''
  AND match(company_id, '^([0-9]{10}|[0-9]{12})$');

-- Live read projection: SE uses the entity and the newest canonical review. Other
-- countries retain their existing serving rows. Review writes need not wait for a fold.
CREATE VIEW IF NOT EXISTS corpscout.company_domains_resolved AS
WITH reviewed AS (
    SELECT
        'SE' AS country_code, d.company_id AS company_id, d.root_domain AS root_domain,
        d.website_url AS website_url, d.website_host AS website_host,
        d.sources AS source_names, arrayMap(value -> toFloat32(value), d.source_confidences) AS source_confidences,
        d.source_record_ids AS source_record_ids, d.source_urls AS source_urls,
        d.confidence_bases AS confidence_bases, toFloat32(d.confidence) AS suggested_confidence,
        d.is_primary AS base_primary, d.evidence_hash AS evidence_fingerprint,
        if(r.company_id != '', if(r.removed, 'unreviewed', r.action), d.review_status) AS review_status,
        if(r.company_id != '', r.note, d.review_note) AS review_note,
        if(r.company_id != '', r.decided_by, d.reviewed_by) AS reviewed_by,
        if(r.company_id != '', r.decided_at, d.reviewed_at) AS reviewed_at,
        if(r.company_id != '', r.evidence_hash, d.reviewed_evidence_hash) AS reviewed_evidence_fingerprint,
        toUInt8(multiIf(
            r.company_id != '' AND r.removed = 0 AND r.action IN ('confirmed_primary', 'confirmed_related'), 1,
            r.company_id != '' AND r.removed = 0 AND r.action = 'rejected', 0,
            r.company_id != '' AND r.removed = 1 AND d.association_source = 'reviewer', 0,
            d.active
        )) AS is_active,
        d.first_seen_at AS first_seen_at, d.last_seen_at AS last_seen_at,
        greatest(d.folded_at, ifNull(r.decided_at, d.folded_at)) AS resolved_at
    FROM corpscout.se_company_domain AS d FINAL
    LEFT JOIN corpscout.se_company_domain_rule AS r FINAL
        ON r.company_id = d.company_id AND r.root_domain = d.root_domain
)
SELECT
    country_code, company_id, root_domain, website_url, website_host, source_names,
    source_confidences, source_record_ids, source_urls, confidence_bases, suggested_confidence,
    toUInt8(is_active AND row_number() OVER (
        PARTITION BY company_id ORDER BY is_active DESC, review_status = 'confirmed_primary' DESC,
        base_primary DESC, suggested_confidence DESC, root_domain
    ) = 1) AS suggested_primary,
    evidence_fingerprint, review_status, review_note, reviewed_by, reviewed_at,
    reviewed_evidence_fingerprint, is_active, first_seen_at, last_seen_at, resolved_at
FROM reviewed
UNION ALL
SELECT country_code, company_id, root_domain, website_url, website_host, source_names,
    source_confidences, source_record_ids, source_urls, confidence_bases, suggested_confidence,
    suggested_primary, evidence_fingerprint, review_status, review_note, reviewed_by, reviewed_at,
    reviewed_evidence_fingerprint, is_active, first_seen_at, last_seen_at, resolved_at
FROM corpscout.company_domains FINAL WHERE country_code != 'SE';

CREATE ROLE IF NOT EXISTS corpscout_company_domain_writer;
GRANT INSERT ON corpscout.se_company_domain_rule TO corpscout_company_domain_writer;
GRANT INSERT ON corpscout.se_company_domain_suggestion TO corpscout_company_domain_writer;
GRANT INSERT ON corpscout.se_company_domain_precedence TO corpscout_company_domain_writer;
