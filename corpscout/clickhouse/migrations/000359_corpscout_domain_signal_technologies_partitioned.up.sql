CREATE DATABASE IF NOT EXISTS corpscout;

-- Repartition domain_signal_technologies for per-bucket publishing and add the
-- seen-window: the dagster asset is statically partitioned hash_000..hash_127
-- (cityHash64(root_domain) % 128, refining the DNS record store's % 16
-- partition key), and each partition run atomically swaps ONLY its slice via
-- ALTER TABLE ... REPLACE PARTITION from a same-run stage table.
--
-- first_seen/last_seen carry the technology's timeline, inherited from the
-- matched DNS records' seen-windows (min/max over the records backing each
-- evidence row). The record store never deletes rows -- a vanished record keeps
-- its frozen window -- so this table is a full per-domain technology TIMELINE
-- recomputed idempotently on every refresh, not a current-state snapshot. Rows
-- with a recent last_seen are the current stack.
--
-- DROP is intentional and safe here: the only writer is the detection asset
-- and the sole existing fill was the 2026-08-31 first run whose per-signal
-- scoping was broken (alias-shadowing bug, fixed with the filter-in-subquery
-- rework) -- the data is disposable and fully reproducible from
-- commoncrawl_domain_dns_records.
DROP TABLE IF EXISTS corpscout.domain_signal_technologies;

CREATE TABLE IF NOT EXISTS corpscout.domain_signal_technologies
(
    root_domain String,
    technology String,
    signal_type LowCardinality(String),
    matched_pattern String,
    evidence String,
    record_name String,
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    confidence UInt8,
    source LowCardinality(String),
    source_run_id String,
    detected_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(detected_at)
PARTITION BY cityHash64(root_domain) % 128
ORDER BY (root_domain, technology, signal_type, matched_pattern, evidence);
