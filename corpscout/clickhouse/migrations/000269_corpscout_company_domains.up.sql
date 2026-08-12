CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.company_domains
(
    country_code LowCardinality(String),
    company_id String,
    root_domain String,
    website_url String,
    website_host String,
    source_names Array(String),
    source_confidences Array(Float32),
    source_record_ids Array(String),
    source_urls Array(String),
    confidence_bases Array(String),
    suggested_confidence Float32,
    suggested_primary UInt8,
    evidence_fingerprint String,
    review_status LowCardinality(String) DEFAULT 'unreviewed',
    review_note String DEFAULT '',
    reviewed_by String DEFAULT '',
    reviewed_at Nullable(DateTime64(3, 'UTC')),
    reviewed_evidence_fingerprint String DEFAULT '',
    is_active UInt8 DEFAULT 1,
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC') DEFAULT now64(3),
    CONSTRAINT source_arrays_have_equal_lengths CHECK
        length(source_names) = length(source_confidences)
        AND length(source_names) = length(source_record_ids)
        AND length(source_names) = length(source_urls)
        AND length(source_names) = length(confidence_bases),
    CONSTRAINT review_status_is_supported CHECK review_status IN (
        'unreviewed',
        'confirmed_primary',
        'confirmed_related',
        'rejected'
    )
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY country_code
ORDER BY (country_code, company_id, root_domain);

-- Keep the existing Sweden serving domains available until the first unified
-- company-serving publication. The serving build subsequently replaces this
-- partition with Wikidata, ESEF, and Common Crawl candidates together.
INSERT INTO corpscout.company_domains
(
    country_code,
    company_id,
    root_domain,
    website_url,
    website_host,
    source_names,
    source_confidences,
    source_record_ids,
    source_urls,
    confidence_bases,
    suggested_confidence,
    suggested_primary,
    evidence_fingerprint,
    review_status,
    review_note,
    reviewed_by,
    reviewed_at,
    reviewed_evidence_fingerprint,
    is_active,
    first_seen_at,
    last_seen_at,
    resolved_at
)
SELECT
    country_code,
    company_id,
    root_domain,
    website_url,
    website_host,
    ['wikidata'],
    [confidence],
    [''],
    [''],
    ['official_website_claim'],
    confidence,
    is_primary,
    lower(hex(SHA256(concat(
        'wikidata|', root_domain, '|', toString(confidence)
    )))),
    'unreviewed',
    '',
    '',
    NULL,
    '',
    1,
    first_seen_at,
    last_seen_at,
    now64(3, 'UTC')
FROM corpscout.company_domain_current
WHERE country_code = 'SE' AND root_domain != '';

CREATE ROLE IF NOT EXISTS corpscout_company_domain_writer;

GRANT INSERT ON corpscout.company_domains
TO corpscout_company_domain_writer;
