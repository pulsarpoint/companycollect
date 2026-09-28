CREATE DATABASE IF NOT EXISTS corpscout;

-- Preparation only, not safe to deploy ahead of the website-aware writers.
-- Pause affected publishers, register/backfill parents, and coordinate their cutover.
-- Crawl rows reference a specific website. Resolve its domain through websites.
-- Existing attempt/work keys and requested-host evidence remain unchanged.
-- Old parts need controlled backfill. New inserts without an ID are rejected.
-- Parent existence is validated by publishers, not by a ClickHouse foreign key.

ALTER TABLE corpscout.website_site_info_results
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_full_crawl_results
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_jobs_crawl_results
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_company_lookup_results
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_company_lookup_candidates
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_company_lookup_evidence
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_company_lookup_searches
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_scans
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_site_profiles
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_business_activities
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_pages
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_contacts
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_identifiers
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_structured_data
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_links
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_jobs
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_full_crawl_requests
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD COLUMN IF NOT EXISTS request_identity_version UInt8 DEFAULT 1,
    MODIFY ORDER BY (bucket, domain, website_id),
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_jobs_crawl_requests
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD COLUMN IF NOT EXISTS request_identity_version UInt8 DEFAULT 1,
    MODIFY ORDER BY (bucket, domain, website_id),
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_site_info_requests
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD COLUMN IF NOT EXISTS request_identity_version UInt8 DEFAULT 1,
    MODIFY ORDER BY (bucket, domain, website_id),
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_submissions
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_task_domains
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD COLUMN IF NOT EXISTS request_identity_version UInt8 DEFAULT 1,
    MODIFY ORDER BY (task_id, domain, website_id),
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_results
    ADD COLUMN IF NOT EXISTS website_id String,
    ADD CONSTRAINT IF NOT EXISTS required_website_reference CHECK notEmpty(website_id);

ALTER TABLE corpscout.website_crawl_pages
    ADD COLUMN IF NOT EXISTS resource_page_id Nullable(String);

-- Prepare the replacement for se_company_domain_suggestion without switching
-- existing writers, copying evidence, or manufacturing company connections in DDL.
-- A source owns stable company/source/slot keys. New versions and withdrawal rows
-- replace that source's claim logically. Consumers select latest before filtering.
CREATE TABLE IF NOT EXISTS corpscout.se_company_domain_sources
(
    company_id String,
    source LowCardinality(String),
    slot String,
    domain_id String,
    website_id Nullable(String),
    -- Original supplied URL is evidence, not a second canonical website identity.
    website_url String,
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
    CONSTRAINT required_claim_identity CHECK notEmpty(company_id) AND notEmpty(source) AND notEmpty(slot),
    CONSTRAINT required_domain_reference CHECK notEmpty(domain_id),
    CONSTRAINT valid_website_reference CHECK isNull(website_id) OR notEmpty(ifNull(website_id, '')),
    CONSTRAINT valid_association CHECK association IN ('connected', 'not_connected', 'uncertain'),
    CONSTRAINT valid_confidence CHECK isFinite(confidence) AND confidence BETWEEN 0 AND 1,
    CONSTRAINT valid_removed CHECK removed IN (0, 1)
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source, slot);

-- Resolve labels from the central inventory while preserving the existing fold's evidence shape.
CREATE OR REPLACE VIEW corpscout.se_company_domain_sources_resolved AS
SELECT c.company_id AS company_id,
    c.source AS source,
    c.slot AS slot,
    ifNull(d.root_domain, '') AS root_domain,
    c.website_url AS website_url,
    domain(c.website_url) AS website_host,
    c.association AS association,
    c.is_primary AS is_primary,
    c.confidence AS confidence,
    c.confidence_basis AS confidence_basis,
    c.source_record_id AS source_record_id,
    c.source_url AS source_url,
    c.evidence AS evidence,
    c.observed_at AS observed_at,
    c.removed AS removed,
    c.decided_by AS decided_by,
    c.note AS note,
    c.suggestion_id AS suggestion_id,
    c.suggested_at AS suggested_at,
    c.source_run_id AS source_run_id,
    c.extractor_version AS extractor_version
FROM corpscout.se_company_domain_sources AS c FINAL
LEFT JOIN corpscout.domains AS d ON d.domain_id = c.domain_id;

-- Refresh view schemas to expose the stored website_id while retaining existing
-- latest-success, attempt and normalization semantics.


CREATE OR REPLACE VIEW corpscout.website_full_crawl_results_latest AS
SELECT * FROM corpscout.website_full_crawl_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY website_id, work_key;

CREATE OR REPLACE VIEW corpscout.website_full_crawl_results_latest_success AS
SELECT * FROM corpscout.website_full_crawl_results FINAL
WHERE successful
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY website_id, work_key;

CREATE OR REPLACE VIEW corpscout.website_jobs_crawl_results_latest AS
SELECT * FROM corpscout.website_jobs_crawl_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY website_id, work_key;

CREATE OR REPLACE VIEW corpscout.website_jobs_crawl_results_latest_success AS
SELECT * FROM corpscout.website_jobs_crawl_results FINAL
WHERE successful
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY website_id, work_key;

CREATE OR REPLACE VIEW corpscout.website_site_info_results_latest AS
SELECT * FROM corpscout.website_site_info_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY website_id, work_key;

CREATE OR REPLACE VIEW corpscout.website_site_info_results_latest_success AS
SELECT * FROM corpscout.website_site_info_results FINAL
WHERE successful
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY website_id, work_key;

CREATE OR REPLACE VIEW corpscout.website_crawl_scans_latest AS
SELECT * FROM corpscout.website_crawl_scans FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY website_id, crawl_type;

CREATE OR REPLACE VIEW corpscout.website_crawl_scans_latest_usable AS
SELECT * FROM corpscout.website_crawl_scans FINAL
WHERE successful AND state = 'completed' AND empty(ifNull(error, ''))
    AND (crawl_status = 'finished' OR (crawl_type = 'site_info' AND crawl_status = 'skip_crawling'))
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY website_id, crawl_type;

CREATE OR REPLACE VIEW corpscout.website_crawl_scans_latest_profile AS
SELECT s.*
FROM corpscout.website_crawl_scans AS s FINAL
INNER JOIN corpscout.website_crawl_site_profiles AS p FINAL
    ON s.domain = p.domain
    AND s.website_id = p.website_id
    AND s.crawl_type = p.crawl_type
    AND s.request_id = p.request_id
    AND s.attempt = p.attempt
    AND s.normalization_id = p.normalization_id
WHERE s.successful AND s.state = 'completed' AND empty(ifNull(s.error, ''))
    AND s.crawl_status IN ('finished', 'skip_crawling')
    AND p.evidence_status = 'source_matched'
    AND p.crawl_decision IN ('continue_crawling', 'skip_crawling')
ORDER BY s.finished_at DESC, s.request_id DESC, s.attempt DESC
LIMIT 1 BY s.website_id, s.crawl_type;

CREATE OR REPLACE VIEW corpscout.website_crawl_site_profiles_published AS
SELECT d.*
FROM corpscout.website_crawl_site_profiles AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_site_profiles_current AS
SELECT d.*
FROM corpscout.website_crawl_site_profiles AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_profile AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_business_activities_published AS
SELECT d.*
FROM corpscout.website_crawl_business_activities AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_business_activities_current AS
SELECT d.*
FROM corpscout.website_crawl_business_activities AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_profile AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_pages_published AS
SELECT d.*
FROM corpscout.website_crawl_pages AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_pages_current AS
SELECT d.*
FROM corpscout.website_crawl_pages AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_contacts_published AS
SELECT d.*
FROM corpscout.website_crawl_contacts AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_contacts_current AS
SELECT d.*
FROM corpscout.website_crawl_contacts AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_identifiers_published AS
SELECT d.*
FROM corpscout.website_crawl_identifiers AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_identifiers_current AS
SELECT d.*
FROM corpscout.website_crawl_identifiers AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_structured_data_published AS
SELECT d.*
FROM corpscout.website_crawl_structured_data AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_structured_data_current AS
SELECT d.*
FROM corpscout.website_crawl_structured_data AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_links_published AS
SELECT d.*
FROM corpscout.website_crawl_links AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_links_current AS
SELECT d.*
FROM corpscout.website_crawl_links AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_jobs_published AS
SELECT d.*
FROM corpscout.website_crawl_jobs AS d FINAL
INNER JOIN (SELECT * FROM corpscout.website_crawl_scans FINAL) AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_crawl_jobs_current AS
SELECT d.*
FROM corpscout.website_crawl_jobs AS d FINAL
INNER JOIN corpscout.website_crawl_scans_latest_usable AS s
    ON d.domain = s.domain
    AND d.website_id = s.website_id
    AND d.crawl_type = s.crawl_type
    AND d.request_id = s.request_id
    AND d.attempt = s.attempt
    AND d.normalization_id = s.normalization_id;

CREATE OR REPLACE VIEW corpscout.website_company_lookup_results_latest AS
SELECT * FROM corpscout.website_company_lookup_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC LIMIT 1 BY country, website_id;

CREATE OR REPLACE VIEW corpscout.website_company_lookup_proposals AS
SELECT * FROM (
    SELECT * FROM corpscout.website_company_lookup_results FINAL
    ORDER BY finished_at DESC, request_id DESC, attempt DESC LIMIT 1 BY country, website_id
) WHERE found AND status = 'matched';

CREATE OR REPLACE VIEW corpscout.website_full_crawl_requests_current AS
SELECT domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision, match_company, company_country, skip_company_matching_if_mapped, website_id, request_identity_version
FROM corpscout.website_full_crawl_requests FINAL;

CREATE OR REPLACE VIEW corpscout.website_jobs_crawl_requests_current AS
SELECT domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision, website_id, request_identity_version
FROM corpscout.website_jobs_crawl_requests FINAL;

CREATE OR REPLACE VIEW corpscout.website_site_info_requests_current AS
SELECT domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision, match_company, company_country, skip_company_matching_if_mapped, website_id, request_identity_version
FROM corpscout.website_site_info_requests FINAL;

CREATE OR REPLACE VIEW corpscout.website_crawl_results_latest SQL SECURITY INVOKER AS
SELECT * FROM corpscout.website_crawl_results FINAL
QUALIFY row_number() OVER (
    PARTITION BY website_id, result_kind
    ORDER BY coalesce(finished_at, started_at, toDateTime64(0, 6, 'UTC')) DESC,
        ingested_at DESC, result_id DESC
) = 1;

-- Archive identity is the publisher's registered website_id, never a root-domain hash.
-- Old immutable payloads need the backfilled result record for their website identity.
CREATE OR REPLACE VIEW corpscout.website_crawl_results_s3_archive SQL SECURITY INVOKER AS
WITH
    JSONExtractString(result_json, 'schema_version') AS result_schema,
    coalesce(
        nullIf(JSONExtractString(result_json, 'crawl', 'input_url'), ''),
        nullIf(JSONExtractString(result_json, 'target_url'), ''),
        nullIf(JSONExtractString(result_json, 'input_url'), ''),
        nullIf(JSONExtractString(result_json, 'url'), ''),
        JSONExtractString(result_json, 'site_url')
    ) AS target_url,
    extract(target_url, '^(?:[A-Za-z][A-Za-z0-9+.-]*://)?([^/?#]+)') AS authority,
    if(startsWith(authority, '['), splitByChar(']', substring(authority, 2))[1],
        splitByChar(':', authority)[1]) AS hostname,
    lowerUTF8(trim(TRAILING '.' FROM hostname)) AS canonical_host,
    arrayFilter(observation -> JSONType(observation) = 'Object',
        arrayMap(document -> JSONExtractRaw(document, 'input', 'observations'),
            JSONExtractArrayRaw(result_json, 'documents'))) AS observations
SELECT
    replaceRegexpOne(if(position(canonical_host, ':') > 0, canonical_host,
        ifNull(tryIdnaEncode(canonical_host), '')), '^www[.]', '') AS domain,
    target_url AS website_url,
    coalesce(nullIf(JSONExtractString(result_json, 'request_id'), ''),
        nullIf(extract(_path, '/([^/]+)/attempts/[0-9]+/result[.]json[.]gz$'), ''),
        arrayElement(splitByChar('/', _path), -2)) AS request_id,
    CAST(toUInt32OrNull(extract(_path, '/attempts/([0-9]+)/result[.]json[.]gz$')) AS Nullable(UInt32)) AS attempt,
    result_schema AS schema_version,
    coalesce(nullIf(JSONExtractString(result_json, 'crawl', 'status'), ''),
        nullIf(JSONExtractString(result_json, 'processing_status'), ''),
        nullIf(JSONExtractString(result_json, 'status'), ''),
        if(result_schema = 'company-crawl-error/1.0', 'failed', 'unknown')) AS status,
    nullIf(nullIf(JSONExtractRaw(result_json, 'records', 'jobs'), ''), 'null') AS jobs,
    if(JSONHas(result_json, 'page_observations'),
        nullIf(nullIf(JSONExtractRaw(result_json, 'page_observations'), ''), 'null'),
        if(result_schema = 'company-crawl-result/1.2' OR notEmpty(observations),
            concat('[', arrayStringConcat(observations, ','), ']'), NULL)) AS page_observations,
    _path,
    _file,
    _size,
    _time,
    result_json,
    JSONExtractString(result_json, 'website_id') AS website_id
FROM s3(company_crawl_results, filename='**/result.json.gz',
    format='JSONAsString', structure='result_json String', compression_method='gzip')
SETTINGS use_query_condition_cache=0, s3_throw_on_zero_files_match=0;

-- Identity registration is separate from evidence folding and source indexing.
-- Grant only to publishers, after deploying shared registration guards.
CREATE ROLE IF NOT EXISTS corpscout_domain_reference_writer;
GRANT SELECT(root_domain, domain_id), INSERT ON corpscout.domains TO corpscout_domain_reference_writer;
GRANT SELECT, INSERT ON corpscout.websites TO corpscout_domain_reference_writer;
GRANT SELECT, INSERT ON corpscout.pages TO corpscout_domain_reference_writer;

-- Registration must inspect a natural key across every stored parent, including a
-- wrongly assigned historical root. Offset projections avoid full inventory scans
-- without duplicating complete inventory rows. Materialize old parts at cutover.
ALTER TABLE corpscout.websites ADD PROJECTION IF NOT EXISTS registration_by_origin
    (SELECT _part_offset ORDER BY website_origin);
ALTER TABLE corpscout.websites ADD PROJECTION IF NOT EXISTS registration_by_id
    (SELECT _part_offset ORDER BY website_id);
ALTER TABLE corpscout.pages ADD PROJECTION IF NOT EXISTS registration_by_url
    (SELECT _part_offset ORDER BY page_url);
ALTER TABLE corpscout.pages ADD PROJECTION IF NOT EXISTS registration_by_id
    (SELECT _part_offset ORDER BY page_id);
