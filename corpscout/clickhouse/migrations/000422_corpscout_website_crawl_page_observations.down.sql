-- Roll back the projection before removing the stored observation column.
CREATE OR REPLACE VIEW corpscout.website_crawl_results_s3 SQL SECURITY INVOKER AS
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
    lowerUTF8(trim(TRAILING '.' FROM hostname)) AS canonical_host
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
    _path,
    _file,
    _size,
    _time,
    result_json
FROM s3(company_crawl_results, filename='**/result.json.gz',
    format='JSONAsString', structure='result_json String', compression_method='gzip')
SETTINGS use_query_condition_cache=0, s3_throw_on_zero_files_match=0;

DROP VIEW IF EXISTS corpscout.website_crawl_results_latest;
ALTER TABLE corpscout.website_crawl_results DROP CONSTRAINT IF EXISTS valid_page_observations;
ALTER TABLE corpscout.website_crawl_results DROP COLUMN IF EXISTS page_observations;
CREATE OR REPLACE VIEW corpscout.website_crawl_results_latest SQL SECURITY INVOKER AS
SELECT * FROM corpscout.website_crawl_results FINAL
QUALIFY row_number() OVER (
    PARTITION BY domain, result_kind
    ORDER BY coalesce(finished_at, started_at, toDateTime64(0, 6, 'UTC')) DESC,
        ingested_at DESC, result_id DESC
) = 1;
