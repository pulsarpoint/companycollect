CREATE DATABASE IF NOT EXISTS corpscout;

-- One immutable result/revision per website. Reimports replace the same result_id.
-- JSON text preserves source nulls and key spelling, matching existing JSON columns.
-- NULL means the source did not provide the section. [] means it provided an empty list.
CREATE TABLE IF NOT EXISTS corpscout.website_crawl_results
(
    domain String,
    website_url String,
    result_id FixedString(64),
    result_kind LowCardinality(String),
    schema_version LowCardinality(String),
    request_id String,
    run_id String,
    revision_id String,
    status LowCardinality(String),
    started_at Nullable(DateTime64(6, 'UTC')),
    finished_at Nullable(DateTime64(6, 'UTC')),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6),
    source_path String,
    company_profile Nullable(String),
    company_contacts Nullable(String),
    locations Nullable(String),
    products_services Nullable(String),
    people Nullable(String),
    company_relationships Nullable(String),
    jobs Nullable(String),
    technology_signals Nullable(String),
    certifications_compliance Nullable(String),
    document_links Nullable(String),
    explicit_negatives Nullable(String),
    site_info Nullable(String),
    company_overview Nullable(String),
    site_profile Nullable(String),
    coverage Nullable(String),
    technology_classification Nullable(String),
    catalog Nullable(String),
    config Nullable(String),
    model_usage Nullable(String),
    crawl Nullable(String),
    entities Nullable(String),
    objectives Nullable(String),
    discovery Nullable(String),
    missing_information Nullable(String),
    pages Nullable(String),
    documents Nullable(String),
    unvisited_links Nullable(String),
    technology_summary Nullable(String),
    external_links Nullable(String),
    errors Nullable(String),
    metadata String,
    CONSTRAINT valid_domain CHECK domain != '' AND domain = lowerUTF8(domain),
    CONSTRAINT valid_kind CHECK result_kind IN ('crawl', 'analysis', 'error'),
    CONSTRAINT valid_record_sections CHECK arrayAll(value -> isNull(value) OR JSONType(ifNull(value, '[]')) = 'Array',
        [company_profile, company_contacts, locations, products_services, people,
         company_relationships, jobs, technology_signals, certifications_compliance,
         document_links, explicit_negatives]),
    CONSTRAINT valid_object_sections CHECK arrayAll(value -> isNull(value) OR JSONType(ifNull(value, '{}')) = 'Object',
        [site_info, company_overview, site_profile, coverage, technology_classification,
         catalog, config, model_usage, crawl, entities, objectives, discovery, missing_information]),
    CONSTRAINT valid_array_sections CHECK arrayAll(value -> isNull(value) OR JSONType(ifNull(value, '[]')) = 'Array',
        [pages, documents, unvisited_links, technology_summary, external_links, errors]),
    CONSTRAINT valid_metadata CHECK JSONType(metadata) = 'Object'
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (domain, result_kind, result_id);

-- Keep the latest crawl and analysis independently. A new raw crawl must not hide analysis.
CREATE VIEW IF NOT EXISTS corpscout.website_crawl_results_latest SQL SECURITY INVOKER AS
SELECT * FROM corpscout.website_crawl_results FINAL
QUALIFY row_number() OVER (
    PARTITION BY domain, result_kind
    ORDER BY coalesce(finished_at, started_at, toDateTime64(0, 6, 'UTC')) DESC,
        ingested_at DESC, result_id DESC
) = 1;

-- Provision the company_crawl_results named collection before applying this migration.
-- Its URL is the bucket/prefix root. Credentials are never embedded in this definition.
-- JSONAsString reads each pretty-printed JSON object as one row, including gzip bundles.
-- Query settings follow migration 415's workaround for filtered S3 reads on CH 26.5.1.
CREATE VIEW IF NOT EXISTS corpscout.website_crawl_results_s3 SQL SECURITY INVOKER AS
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
