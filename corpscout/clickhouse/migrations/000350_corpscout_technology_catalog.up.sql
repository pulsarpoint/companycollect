CREATE DATABASE IF NOT EXISTS corpscout;

-- The technology catalog: one row per Wappalyzer-style technology name, carrying the
-- display metadata the technology pages need (description, website, resolved category
-- names, an icon stored in the local S3 icon bucket) plus the saas/oss/pricing facets the
-- source data ships anyway. `technology` is the EXACT detector name as it appears in
-- commoncrawl_page_technologies.technology (owner 2026-08-29 -- verified 93.5% of our
-- 4,576 detected names join the vendored extension catalog directly, and the maintained
-- public webappanalyzer catalog closes most of the rest), so serving joins by equality and
-- never fuzzy-matches.
--
-- Filled by the dagster technology_catalog module: the vendored extension bundle
-- (extensions/6.12.5_0, frozen bootstrap layer) merged under the maintained public
-- webappanalyzer catalog (weekly overlay -- newer wins), icons uploaded to the dedicated
-- technology-icons S3 bucket, table replaced via the standard stage + EXCHANGE TABLES with
-- refuse-on-empty. The migration owns this schema -- the exporter only asserts it exists.
CREATE TABLE IF NOT EXISTS corpscout.technology_catalog
(
    technology String,
    slug String,
    description String,
    website String,
    category_ids Array(UInt16),
    categories Array(String),
    groups Array(String),
    -- '' when the source ships no icon for the technology. The key lives in the
    -- technology-icons bucket -- content type is stored so the backoffice icon proxy can
    -- answer without probing S3 metadata.
    icon_object_key String,
    icon_content_type LowCardinality(String),
    saas UInt8,
    oss UInt8,
    pricing Array(String),
    source LowCardinality(String),
    source_version String,
    source_run_id String,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (technology);
