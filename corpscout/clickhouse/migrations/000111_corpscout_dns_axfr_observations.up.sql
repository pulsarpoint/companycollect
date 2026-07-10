CREATE DATABASE IF NOT EXISTS corpscout;

-- Append-only AXFR state-change log: one row per (root_domain, name_server) transition. The worker
-- writes a row ONLY when the observed axfr_open differs from the last known state (loaded from this
-- table at the start of each AXFR run), so consecutive same-state probes never land here and the
-- ~99.7% of nameservers that are always closed never appear at all (implicit-closed base). The rows
-- ARE the collapsed timeline: `SELECT ... WHERE root_domain=? ORDER BY observed_at` gives
-- open->close->open with the date of each change. Plain MergeTree (no merge-time dedup): distinct
-- open periods must stay distinct, which a key-based collapsing engine would wrongly fold together.
CREATE TABLE IF NOT EXISTS corpscout.dns_axfr_observations
(
    root_domain String,
    name_server String,
    axfr_open   Bool,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (root_domain, name_server, observed_at);
