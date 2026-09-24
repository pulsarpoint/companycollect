CREATE DATABASE IF NOT EXISTS corpscout;

-- Rebuildable serving snapshot. Only corpscout.domains supplies membership.
-- Sources enrich rows through LEFT JOINs during publication, never during paging.
CREATE TABLE IF NOT EXISTS corpscout.domains_search
(
    root_domain String,
    sources Array(LowCardinality(String)),
    has_dns_records UInt8,
    dns_last_observed_at Nullable(DateTime64(3, 'UTC')),
    website_count UInt64,
    observed_website_count UInt64,
    has_website UInt8 MATERIALIZED toUInt8(website_count > 0),
    company_count UInt64,
    has_company UInt8 MATERIALIZED toUInt8(company_count > 0),
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC'),
    refreshed_at DateTime64(3, 'UTC'),
    source_run_id String,
    CONSTRAINT valid_counts CHECK observed_website_count <= website_count,
    CONSTRAINT valid_dns CHECK has_dns_records IN (0,1)
        AND has_dns_records = toUInt8(isNotNull(dns_last_observed_at)),
    PROJECTION by_company
    (
        SELECT * ORDER BY (has_company, root_domain)
    ),
    PROJECTION by_website
    (
        SELECT * ORDER BY (has_website, root_domain)
    ),
    PROJECTION by_dns
    (
        SELECT * ORDER BY (has_dns_records, root_domain)
    )
)
ENGINE = MergeTree
ORDER BY root_domain;
