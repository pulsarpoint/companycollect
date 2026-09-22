DROP VIEW corpscout.se_company_brave_search_successes;

RENAME TABLE corpscout.se_company_brave_domains TO corpscout.se_company_brave_search_results_latest_success;

CREATE VIEW corpscout.company_brave_search_results_latest AS
SELECT * FROM corpscout.company_brave_search_results FINAL
ORDER BY completed_at DESC, result_id DESC
LIMIT 1 BY country_code, company_id, query_type;

CREATE MATERIALIZED VIEW corpscout.se_company_brave_search_successes
TO corpscout.se_company_brave_search_results_latest_success AS
SELECT toString(result_id) AS result_id, toString(task_id) AS task_id,
    input_id, '' AS work_key, attempt, toString(status) AS status,
    '' AS export_batch_id, country_code, company_id, company_name, query,
    query_type, processor_version, answer_text, route, source_url, error_type,
    source_run_id, completed_at, '' AS archive_path
FROM corpscout.company_brave_search_results
WHERE country_code = 'SE' AND status = 'success';

RENAME TABLE corpscout.se_company_brave_domains_history TO corpscout.se_company_brave_search_results_s3_archive;

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
    result_json
FROM s3(company_crawl_results, filename='**/result.json.gz',
    format='JSONAsString', structure='result_json String', compression_method='gzip')
SETTINGS use_query_condition_cache=0, s3_throw_on_zero_files_match=0;

CREATE OR REPLACE VIEW corpscout.website_full_crawl_results_latest AS
SELECT * FROM corpscout.website_full_crawl_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

CREATE OR REPLACE VIEW corpscout.website_full_crawl_results_latest_success AS
SELECT * FROM corpscout.website_full_crawl_results FINAL
WHERE successful
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

CREATE OR REPLACE VIEW corpscout.website_jobs_crawl_results_latest AS
SELECT * FROM corpscout.website_jobs_crawl_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

CREATE OR REPLACE VIEW corpscout.website_jobs_crawl_results_latest_success AS
SELECT * FROM corpscout.website_jobs_crawl_results FINAL
WHERE successful
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

CREATE OR REPLACE VIEW corpscout.website_site_info_results_latest AS
SELECT * FROM corpscout.website_site_info_results FINAL
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

CREATE OR REPLACE VIEW corpscout.website_site_info_results_latest_success AS
SELECT * FROM corpscout.website_site_info_results FINAL
WHERE successful
ORDER BY finished_at DESC, request_id DESC, attempt DESC
LIMIT 1 BY domain, work_key;

GRANT SELECT, INSERT ON corpscout.se_company_brave_search_results_latest_success TO processing_publisher;

GRANT SELECT ON corpscout.company_brave_search_results_latest TO processing_publisher;

GRANT SELECT ON corpscout.se_company_brave_search_results_s3_archive TO processing_publisher;
