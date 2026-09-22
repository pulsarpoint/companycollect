-- Deterministic page observations stay separate from LLM company records.
-- Old results remain NULL, while a new crawl with no successful pages has [].
CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.website_crawl_results
    ADD COLUMN IF NOT EXISTS page_observations Nullable(String) AFTER documents;
ALTER TABLE corpscout.website_crawl_results
    ADD CONSTRAINT IF NOT EXISTS valid_page_observations
    CHECK isNull(page_observations) OR JSONType(ifNull(page_observations, '[]')) = 'Array';

CREATE OR REPLACE VIEW corpscout.website_crawl_results_latest SQL SECURITY INVOKER AS
SELECT * FROM corpscout.website_crawl_results FINAL
QUALIFY row_number() OVER (
    PARTITION BY domain, result_kind
    ORDER BY coalesce(finished_at, started_at, toDateTime64(0, 6, 'UTC')) DESC,
        ingested_at DESC, result_id DESC
) = 1;

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
        arrayElement(splitByChar('/', _path), -2)) AS request_id,
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
    result_json
FROM s3(company_crawl_results, filename='**/result.json.gz',
    format='JSONAsString', structure='result_json String', compression_method='gzip')
SETTINGS use_query_condition_cache=0, s3_throw_on_zero_files_match=0;
