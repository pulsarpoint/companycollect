CREATE DATABASE IF NOT EXISTS corpscout;

-- Reference: tracker-domain -> owning company, loaded from DuckDuckGo Tracker Radar by a SEPARATE dagster
-- pipeline (not the crawl worker -- schema only for now). Used to lift a shared analytics/ad id from "same
-- id" to "same owner entity" for the ownership/sibling graph, and to exclude common third-party
-- trackers/CDNs/consent-managers that would otherwise fabricate sibling edges. Refreshed on its own cadence.
-- ReplacingMergeTree on resolved_at -> read with FINAL.
CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_tracker_owners
(
    tracker_domain String,
    owner_name String,
    owner_display String,
    categories Array(LowCardinality(String)),
    prevalence Float32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (tracker_domain);
