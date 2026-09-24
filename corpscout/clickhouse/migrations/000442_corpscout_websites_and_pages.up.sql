CREATE DATABASE IF NOT EXISTS corpscout;

-- Current inventory snapshots, like corpscout.domains. Publishers must deduplicate
-- normalized origins/URLs, union sources, preserve timestamps and validate parent
-- membership before publishing. MergeTree does not enforce uniqueness or foreign keys.
-- Source selection belongs to the publisher, not to this schema.
-- This migration creates empty tables only, with no source reads or queue triggers.
--
-- Normalize with the Webtech page_identity rules BEFORE insertion. Origins contain
-- scheme + hostname + optional nondefault port, with no trailing slash. Pages retain
-- path and query, with '/' for an empty path and no fragment. HTTP/HTTPS and different
-- hosts remain separate. IDs are lowercase SHA-256 hex of the normalized UTF-8 string,
-- independent of source, domain assignment, scan and redirects.
CREATE TABLE IF NOT EXISTS corpscout.websites
(
    root_domain String,
    website_origin String,
    website_id String MATERIALIZED lower(hex(SHA256(website_origin))),
    sources Array(LowCardinality(String)),
    -- Seen timestamps track inventory discovery, including assumed company targets.
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    -- Source evidence timestamp. CC currently exposes extraction time, not capture time.
    -- Importing an existing result must preserve its timestamp rather than use now().
    last_observed_at Nullable(DateTime64(3, 'UTC')),
    evidence_status LowCardinality(String) MATERIALIZED
        if(isNull(last_observed_at), 'assumed', 'observed'),
    -- Successful live fetch only. Importing an archived page must not set this field.
    last_successful_fetch_at Nullable(DateTime64(3, 'UTC')),
    source_run_id String,
    CONSTRAINT valid_root CHECK notEmpty(root_domain) AND root_domain = lower(root_domain),
    CONSTRAINT valid_origin CHECK match(website_origin, '^https?://[^/?#@[:space:]]+$')
        AND notEmpty(domain(website_origin))
        AND (domain(website_origin) = root_domain
            OR endsWith(domain(website_origin), concat('.', root_domain))),
    CONSTRAINT valid_sources CHECK notEmpty(sources)
        AND arrayAll(source -> notEmpty(source), sources)
        AND sources = arraySort(arrayDistinct(sources)),
    CONSTRAINT valid_seen_window CHECK first_seen_at <= last_seen_at,
    CONSTRAINT valid_fetch_evidence CHECK isNull(last_successful_fetch_at)
        OR (isNotNull(last_observed_at)
            AND ifNull(last_successful_fetch_at <= last_observed_at, 0))
)
ENGINE = MergeTree
ORDER BY (root_domain, website_origin);

-- Pages link to websites through the same origin-derived website_id. Processing
-- history stays in the existing crawler, Webtech and Common Crawl result tables.
-- Requested and final URLs belong to those observations, not to page identity.
CREATE TABLE IF NOT EXISTS corpscout.pages
(
    root_domain String,
    website_origin String,
    website_id String MATERIALIZED lower(hex(SHA256(website_origin))),
    page_url String,
    page_id String MATERIALIZED lower(hex(SHA256(page_url))),
    sources Array(LowCardinality(String)),
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    last_observed_at Nullable(DateTime64(3, 'UTC')),
    evidence_status LowCardinality(String) MATERIALIZED
        if(isNull(last_observed_at), 'assumed', 'observed'),
    last_successful_fetch_at Nullable(DateTime64(3, 'UTC')),
    source_run_id String,
    CONSTRAINT valid_root CHECK notEmpty(root_domain) AND root_domain = lower(root_domain),
    CONSTRAINT valid_origin CHECK match(website_origin, '^https?://[^/?#@[:space:]]+$')
        AND notEmpty(domain(website_origin))
        AND (domain(website_origin) = root_domain
            OR endsWith(domain(website_origin), concat('.', root_domain))),
    CONSTRAINT valid_page CHECK startsWith(page_url, concat(website_origin, '/'))
        AND position(page_url, '#') = 0 AND NOT match(page_url, '[[:space:][:cntrl:]]'),
    CONSTRAINT valid_sources CHECK notEmpty(sources)
        AND arrayAll(source -> notEmpty(source), sources)
        AND sources = arraySort(arrayDistinct(sources)),
    CONSTRAINT valid_seen_window CHECK first_seen_at <= last_seen_at,
    CONSTRAINT valid_fetch_evidence CHECK isNull(last_successful_fetch_at)
        OR (isNotNull(last_observed_at)
            AND ifNull(last_successful_fetch_at <= last_observed_at, 0))
)
ENGINE = MergeTree
ORDER BY (root_domain, website_origin, page_url);
