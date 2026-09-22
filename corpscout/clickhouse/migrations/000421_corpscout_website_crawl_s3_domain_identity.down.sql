CREATE DATABASE IF NOT EXISTS corpscout;

CREATE OR REPLACE VIEW corpscout.website_crawl_results_s3_archive SQL SECURITY INVOKER AS
WITH
    JSONExtractString(result_json, 'schema_version') AS result_schema,
    coalesce(
        nullIf(JSONExtractString(result_json, 'crawl', 'input_url'), ''),
        nullIf(JSONExtractString(result_json, 'target_url'), ''),
        nullIf(JSONExtractString(result_json, 'input_url'), ''),
        nullIf(JSONExtractString(result_json, 'url'), ''),
        JSONExtractString(result_json, 'site_url')
    ) AS target_url
SELECT
    replaceRegexpOne(lowerUTF8(domain(target_url)), '^www[.]', '') AS domain,
    target_url AS website_url,
    coalesce(nullIf(JSONExtractString(result_json, 'request_id'), ''),
        arrayElement(splitByChar('/', _path), -2)) AS request_id,
    result_schema AS schema_version,
    coalesce(nullIf(JSONExtractString(result_json, 'crawl', 'status'), ''),
        nullIf(JSONExtractString(result_json, 'processing_status'), ''),
        nullIf(JSONExtractString(result_json, 'status'), ''),
        if(result_schema = 'company-crawl-error/1.0', 'failed', 'unknown')) AS status,
    nullIf(JSONExtractRaw(result_json, 'records', 'jobs'), '') AS jobs,
    _path,
    _file,
    _size,
    _time,
    result_json
FROM s3(company_crawl_results, filename='**/result.json.gz',
    format='JSONAsString', structure='result_json String', compression_method='gzip')
SETTINGS use_query_condition_cache=0, s3_throw_on_zero_files_match=0;
