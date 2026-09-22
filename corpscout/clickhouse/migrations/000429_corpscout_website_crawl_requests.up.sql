CREATE DATABASE IF NOT EXISTS corpscout;

-- Recurring desired configuration, keyed by normalized domain within each crawl type.
-- Writers append increasing revisions and preserve created_at when editing a row.
-- Refresh intervals belong to Dagster asset settings, not individual input rows.
-- bucket provides 256 work partitions without creating physical table partitions.
-- Readers use the current views so disabled or reprioritized rows cannot reappear.
-- List materialized bucket explicitly because SELECT * excludes it by default.

CREATE TABLE IF NOT EXISTS corpscout.website_full_crawl_requests
(
    domain String,
    website_url String,
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(domain) % 256),
    enabled Bool DEFAULT true,
    priority UInt8 DEFAULT 50,
    page_mode Enum8('discover' = 1, 'explicit' = 2) DEFAULT 'discover',
    pages Array(String) DEFAULT [],
    instructions String DEFAULT '',
    headless Bool DEFAULT true,
    proxy_route LowCardinality(String) DEFAULT 'direct',
    save_artifacts Bool DEFAULT true,
    preset_version UInt16 DEFAULT 1,
    config_json String DEFAULT '{}',
    source LowCardinality(String) DEFAULT 'domain_inventory',
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    revision UInt64,
    CONSTRAINT valid_domain CHECK domain != '' AND domain = lowerUTF8(domain),
    CONSTRAINT valid_website_url CHECK protocol(website_url) IN ('http', 'https') AND notEmpty(domain(website_url)),
    CONSTRAINT valid_priority CHECK priority <= 100,
    CONSTRAINT valid_revision CHECK revision > 0 AND preset_version > 0,
    CONSTRAINT valid_explicit_pages CHECK page_mode != 'explicit' OR notEmpty(pages),
    CONSTRAINT valid_config CHECK isValidJSON(config_json) AND JSONType(config_json) = 'Object',
    CONSTRAINT asset_refresh_policy CHECK NOT JSONHas(config_json, 'refresh_interval_days')
)
ENGINE = ReplacingMergeTree(revision)
ORDER BY (bucket, domain);

CREATE VIEW IF NOT EXISTS corpscout.website_full_crawl_requests_current AS
SELECT
    domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision
FROM corpscout.website_full_crawl_requests FINAL;

CREATE TABLE IF NOT EXISTS corpscout.website_jobs_crawl_requests
(
    domain String,
    website_url String,
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(domain) % 256),
    enabled Bool DEFAULT true,
    priority UInt8 DEFAULT 50,
    page_mode Enum8('discover' = 1, 'explicit' = 2) DEFAULT 'discover',
    pages Array(String) DEFAULT [],
    instructions String DEFAULT '',
    headless Bool DEFAULT true,
    proxy_route LowCardinality(String) DEFAULT 'direct',
    save_artifacts Bool DEFAULT true,
    preset_version UInt16 DEFAULT 1,
    config_json String DEFAULT '{}',
    source LowCardinality(String) DEFAULT 'domain_inventory',
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    revision UInt64,
    CONSTRAINT valid_domain CHECK domain != '' AND domain = lowerUTF8(domain),
    CONSTRAINT valid_website_url CHECK protocol(website_url) IN ('http', 'https') AND notEmpty(domain(website_url)),
    CONSTRAINT valid_priority CHECK priority <= 100,
    CONSTRAINT valid_revision CHECK revision > 0 AND preset_version > 0,
    CONSTRAINT valid_explicit_pages CHECK page_mode != 'explicit' OR notEmpty(pages),
    CONSTRAINT valid_config CHECK isValidJSON(config_json) AND JSONType(config_json) = 'Object',
    CONSTRAINT asset_refresh_policy CHECK NOT JSONHas(config_json, 'refresh_interval_days')
)
ENGINE = ReplacingMergeTree(revision)
ORDER BY (bucket, domain);

CREATE VIEW IF NOT EXISTS corpscout.website_jobs_crawl_requests_current AS
SELECT
    domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision
FROM corpscout.website_jobs_crawl_requests FINAL;

CREATE TABLE IF NOT EXISTS corpscout.website_site_info_requests
(
    domain String,
    website_url String,
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(domain) % 256),
    enabled Bool DEFAULT true,
    priority UInt8 DEFAULT 50,
    page_mode Enum8('discover' = 1, 'explicit' = 2) DEFAULT 'discover',
    pages Array(String) DEFAULT [],
    instructions String DEFAULT '',
    headless Bool DEFAULT true,
    proxy_route LowCardinality(String) DEFAULT 'direct',
    save_artifacts Bool DEFAULT true,
    preset_version UInt16 DEFAULT 1,
    config_json String DEFAULT '{}',
    source LowCardinality(String) DEFAULT 'domain_inventory',
    created_at DateTime64(6, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    revision UInt64,
    CONSTRAINT valid_domain CHECK domain != '' AND domain = lowerUTF8(domain),
    CONSTRAINT valid_website_url CHECK protocol(website_url) IN ('http', 'https') AND notEmpty(domain(website_url)),
    CONSTRAINT valid_priority CHECK priority <= 100,
    CONSTRAINT valid_revision CHECK revision > 0 AND preset_version > 0,
    CONSTRAINT valid_explicit_pages CHECK page_mode != 'explicit' OR notEmpty(pages),
    CONSTRAINT valid_config CHECK isValidJSON(config_json) AND JSONType(config_json) = 'Object',
    CONSTRAINT asset_refresh_policy CHECK NOT JSONHas(config_json, 'refresh_interval_days'),
    CONSTRAINT site_info_scope CHECK page_mode = 'discover' AND empty(pages)
)
ENGINE = ReplacingMergeTree(revision)
ORDER BY (bucket, domain);

CREATE VIEW IF NOT EXISTS corpscout.website_site_info_requests_current AS
SELECT
    domain, website_url, bucket, enabled, priority, page_mode, pages, instructions,
    headless, proxy_route, save_artifacts, preset_version, config_json, source,
    created_at, updated_at, revision
FROM corpscout.website_site_info_requests FINAL;
