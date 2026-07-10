CREATE DATABASE IF NOT EXISTS corpscout;

-- Task 11 (correctness hardening): the EXACT domain membership of one scan, so cc-dns-worker's CT and
-- registry hostname-enrichment shard queries can be scoped to only the domains THIS scan actually
-- seeded, instead of semi-joining the whole corpscout.commoncrawl_domains corpus (the bug this table
-- exists to fix -- a --max-domains=3 run, or a custom --query seed, must enrich exactly those domains
-- and nothing else). One row per (scan_id, root_domain), blind-inserted -- ReplacingMergeTree collapses
-- a resumed or retried mirror of the same scan to one row per key, so re-mirroring is always safe. See
-- cc-dns-worker's internal/load.MirrorSeedDomains, which streams a scan's local scan_domains rows into
-- this table in bounded batches (never all at once), and internal/hostsource's shard queries, which
-- join both the CT and registry sources to this table filtered on scan_id.
--
-- TTL: a scan's membership is only needed while that scan's host-enrichment can still run or resume --
-- 30 days comfortably covers that window -- after which the row set self-cleans on background merge
-- rather than growing unbounded across every scan_id this worker has ever run.
CREATE TABLE IF NOT EXISTS corpscout.dns_scan_seed_domains
(
    scan_id     String,
    root_domain String,
    seeded_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(seeded_at)
ORDER BY (scan_id, root_domain)
TTL seeded_at + INTERVAL 30 DAY;
