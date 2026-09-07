CREATE DATABASE IF NOT EXISTS corpscout;

-- Crawler submissions are evidence-bearing proposals, never catalog approvals.
-- A retry has the same run/record key. FINAL removes duplicate transport retries.
CREATE TABLE IF NOT EXISTS corpscout.new_tech
(
    proposal_id String,
    run_id String,
    record_id String,
    observed_name String,
    proposed_name String,
    proposed_key String,
    description String,
    website String,
    category_ids Array(UInt16),
    category_suggestion String,
    saas Nullable(UInt8),
    oss Nullable(UInt8),
    pricing Array(String),
    company String,
    job_employer String,
    job_title String,
    job_url String,
    signal LowCardinality(String),
    scope LowCardinality(String),
    context String,
    as_of String,
    alternative_group String,
    site_url String,
    sources Array(Tuple(url String, html_sha256 String, fetched_at String, evidence Array(String), evidence_status String)),
    catalog_version String,
    search_queries Array(String),
    search_candidates Array(String),
    proposal_reason String,
    existing_technology String,
    model String,
    received_at DateTime64(6, 'UTC'),
    CONSTRAINT proposal_has_identity CHECK notEmpty(proposal_id) AND notEmpty(proposed_name) AND notEmpty(run_id) AND notEmpty(record_id),
    CONSTRAINT proposal_has_evidence CHECK notEmpty(sources) AND notEmpty(search_queries) AND notEmpty(catalog_version)
)
ENGINE = ReplacingMergeTree(received_at)
ORDER BY (proposal_id, run_id, record_id);

-- Decisions are append-only. The latest (timestamp, UUID) wins, retaining all
-- earlier reviews. Approval is distinct from catalog publication.
CREATE TABLE IF NOT EXISTS corpscout.technology_proposal_reviews
(
    review_id UUID,
    proposal_id String,
    decision LowCardinality(String),
    technology String,
    description String,
    website String,
    category_ids Array(UInt16),
    categories Array(String),
    groups Array(String),
    saas UInt8,
    oss UInt8,
    pricing Array(String),
    alias String,
    alias_key String,
    reviewed_by String,
    review_note String,
    source_references Array(String),
    reviewed_at DateTime64(6, 'UTC'),
    CONSTRAINT review_decision_is_supported CHECK decision IN ('approve_new', 'map_existing', 'reject'),
    CONSTRAINT review_has_actor CHECK notEmpty(reviewed_by) AND notEmpty(review_note),
    CONSTRAINT approval_has_identity CHECK decision = 'reject' OR notEmpty(technology),
    CONSTRAINT new_technology_has_metadata CHECK decision != 'approve_new' OR (notEmpty(description) AND notEmpty(website) AND notEmpty(category_ids)),
    CONSTRAINT alias_has_key CHECK (empty(alias) AND empty(alias_key)) OR (notEmpty(alias) AND notEmpty(alias_key))
)
ENGINE = MergeTree
ORDER BY (proposal_id, reviewed_at, review_id);

CREATE VIEW IF NOT EXISTS corpscout.technology_proposal_latest_reviews AS
SELECT * FROM corpscout.technology_proposal_reviews
ORDER BY reviewed_at DESC, review_id DESC
LIMIT 1 BY proposal_id;

-- Downstream observations can resolve proposal IDs without rewriting evidence.
CREATE VIEW IF NOT EXISTS corpscout.technology_proposal_mappings AS
SELECT proposal_id, technology, decision, review_id, reviewed_at
FROM corpscout.technology_proposal_latest_reviews
WHERE decision IN ('approve_new', 'map_existing');
