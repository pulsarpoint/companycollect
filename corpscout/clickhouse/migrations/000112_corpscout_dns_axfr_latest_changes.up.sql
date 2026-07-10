CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per AXFR-probed endpoint (root_domain, name_server, name_server_ip): its definitive state
-- (open/closed) as of the most recent probe that reached one, carried forward across any number of
-- unknown probes and across scans where the endpoint drops out of the domain's current NS delegation.
-- Readers get a coherent "as of now" view without replaying dns_axfr_state_changes. ReplacingMergeTree
-- keeps retries safe: re-writing the same endpoint after a crash mid-load just produces another
-- version on updated_at, which merges away (read with FINAL, or argMax(updated_at), to see it live).
CREATE TABLE IF NOT EXISTS corpscout.dns_axfr_latest
(
    root_domain          String,
    name_server          String,
    name_server_ip       String,
    updated_at           DateTime64(3, 'UTC'),
    delegation_active    UInt8,
    delegation_seen_at   DateTime64(3, 'UTC'),
    last_probe_verdict   LowCardinality(String),
    last_probe_reason    LowCardinality(String),
    last_probed_at       DateTime64(3, 'UTC'),
    has_definitive_state UInt8,
    axfr_open            UInt8,
    definitive_at        DateTime64(3, 'UTC'),
    definitive_scan_id   String
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (root_domain, name_server, name_server_ip);

-- Only DEFINITIVE AXFR state changes (an unknown probe never lands here), one row per transition. The
-- key includes scan_id so a retried load of one scan's staged changes is idempotent (re-inserting the
-- same rows merges away on ReplacingMergeTree) while two genuinely distinct open periods from different
-- scans still survive as separate rows even if their changed_at happens to coincide.
CREATE TABLE IF NOT EXISTS corpscout.dns_axfr_state_changes
(
    root_domain    String,
    name_server    String,
    name_server_ip String,
    axfr_open      UInt8,
    scan_id        String,
    changed_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(changed_at)
ORDER BY (root_domain, name_server, name_server_ip, changed_at, scan_id);
