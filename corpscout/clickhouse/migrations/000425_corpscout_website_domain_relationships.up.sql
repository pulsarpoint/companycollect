CREATE DATABASE IF NOT EXISTS corpscout;

-- A link belongs to the company associated with the actual source host, not the
-- crawl's requested domain. Shared hosts are ambiguous and remain raw evidence.
CREATE VIEW corpscout.website_domain_relationship_inputs AS
WITH owners AS (
    SELECT replaceRegexpOne(lowerUTF8(trim(TRAILING '.' FROM website_host)), '^www[.]', '') AS source_host,
        any(country_code) AS owner_country_code, any(company_id) AS owner_company_id
    FROM corpscout.company_domains_resolved
    WHERE is_active = 1 AND website_host != ''
    GROUP BY source_host
    HAVING uniqExact(tuple(country_code, company_id)) = 1
), occurrences AS (
    SELECT result_id, domain AS crawl_domain, result_kind, source_path, finished_at,
        arrayJoin(JSONExtractArrayRaw(ifNull(external_links, '[]'))) AS evidence,
        JSONExtractString(evidence, 'source_host') AS source_host,
        JSONExtractString(evidence, 'destination_domain') AS registrable_domain
    FROM corpscout.website_crawl_results_latest
    WHERE result_kind IN ('crawl', 'analysis')
)
SELECT r.result_id AS result_id, r.crawl_domain AS crawl_domain,
    r.result_kind AS result_kind, r.source_path AS source_path,
    r.source_host AS source_host, o.owner_country_code AS country_code, o.owner_company_id AS company_id,
    c.legal_name AS reporting_entity_name, r.registrable_domain AS registrable_domain,
    max(coalesce(parseDateTime64BestEffortOrNull(JSONExtractString(r.evidence, 'fetched_at'), 6, 'UTC'),
        r.finished_at, toDateTime64(0, 6, 'UTC'))) AS captured_at,
    min(JSONExtractString(r.evidence, 'source_url')) AS source_url,
    concat('[', arrayStringConcat(arraySort(groupUniqArray(r.evidence)), ','), ']') AS evidence_json
FROM occurrences AS r
INNER JOIN owners AS o ON o.source_host = r.source_host
INNER JOIN corpscout.se_company_basic_info AS c FINAL ON c.company_id = o.owner_company_id AND o.owner_country_code = 'SE'
WHERE JSONExtractString(r.evidence, 'context_version') = 'website-domain-context-v1'
    AND r.registrable_domain != ''
GROUP BY r.result_id, r.crawl_domain, r.result_kind, r.source_path,
    r.source_host, o.owner_country_code, o.owner_company_id, c.legal_name, r.registrable_domain;

CREATE TABLE IF NOT EXISTS corpscout.website_domain_relationship_analysis
(
    attempt_id String,
    result_id String,
    crawl_domain String,
    result_kind LowCardinality(String),
    source_path String,
    source_host String,
    country_code String,
    company_id String,
    reporting_entity_name String,
    registrable_domain String,
    captured_at DateTime64(6, 'UTC'),
    source_url String,
    source_evidence_hash FixedString(64),
    input_hash FixedString(64),
    analysis_version String,
    prompt_hash FixedString(64),
    model_config_json String,
    input_json String,
    raw_response String,
    statements_json String,
    status LowCardinality(String),
    error_message String,
    model_provider String,
    model_name String,
    prompt_tokens UInt32,
    completion_tokens UInt32,
    source_run_id String,
    analyzed_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (country_code, company_id, source_host, registrable_domain, result_id, analyzed_at, attempt_id);

-- Immutable attempts remain queryable after new captures or changed ownership.
-- A failed retry must not hide a still-applicable successful answer.
CREATE VIEW corpscout.website_domain_relationships_current AS
SELECT a.attempt_id AS attempt_id,
    a.result_id AS result_id,
    a.crawl_domain AS crawl_domain,
    a.result_kind AS result_kind,
    a.source_path AS source_path,
    a.source_host AS source_host,
    a.country_code AS country_code,
    a.company_id AS company_id,
    a.reporting_entity_name AS reporting_entity_name,
    a.registrable_domain AS registrable_domain,
    a.captured_at AS captured_at,
    a.source_url AS source_url,
    a.source_evidence_hash AS source_evidence_hash,
    a.input_hash AS input_hash,
    a.analysis_version AS analysis_version,
    a.prompt_hash AS prompt_hash,
    a.model_config_json AS model_config_json,
    a.input_json AS input_json,
    a.raw_response AS raw_response,
    a.statements_json AS statements_json,
    a.status AS status,
    a.error_message AS error_message,
    a.model_provider AS model_provider,
    a.model_name AS model_name,
    a.prompt_tokens AS prompt_tokens,
    a.completion_tokens AS completion_tokens,
    a.source_run_id AS source_run_id,
    a.analyzed_at AS analyzed_at
FROM corpscout.website_domain_relationship_analysis AS a
INNER JOIN corpscout.website_domain_relationship_inputs AS i
    ON i.result_id = a.result_id AND i.source_host = a.source_host
    AND i.country_code = a.country_code AND i.company_id = a.company_id
    AND i.reporting_entity_name = a.reporting_entity_name
    AND i.registrable_domain = a.registrable_domain
    AND lower(hex(SHA256(i.evidence_json))) = a.source_evidence_hash
WHERE a.status = 'success'
ORDER BY a.analyzed_at DESC, a.attempt_id DESC
LIMIT 1 BY a.result_id, a.source_host, a.country_code, a.company_id, a.registrable_domain;
