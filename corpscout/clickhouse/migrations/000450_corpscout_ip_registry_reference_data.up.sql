CREATE DATABASE IF NOT EXISTS corpscout;

-- IP registry reference data, limited to what the registry-level rule needs: the IANA
-- top-level blocks with their designation and status, the available/reserved ranges of the
-- five RIRs' delegated-extended statistics (special segments), and the few allocated/assigned
-- records wide enough to cover an entire IANA block (holder blocks, a /8 such as Comcast's
-- 73.0.0.0/8). Every other allocated or assigned record of the RIR files is discarded by the
-- parser. Loaded daily by the ip_registry Dagster module, which also classifies every cached
-- RDAP registration against them.

-- One row per (source, snapshot) that finished loading. The _current views read the newest
-- snapshot per source from here, so a partial load is never current. verified_at moves on
-- every run that re-checks the same snapshot. records_* count the whole file, segments_* the
-- special segment rows that were kept, holders_* the holder block rows (0 for IANA).
CREATE TABLE IF NOT EXISTS corpscout.ip_registry_snapshots
(
    source          LowCardinality(String),
    snapshot_date   Date,
    verified_at     DateTime64(3, 'UTC'),
    checksum        String,
    serial          String,
    records_ipv4    UInt64,
    records_ipv6    UInt64,
    segments_ipv4   UInt64,
    segments_ipv6   UInt64,
    holders_ipv4    UInt64,
    holders_ipv6    UInt64,
    source_url      String
)
ENGINE = ReplacingMergeTree(verified_at)
ORDER BY (source, snapshot_date);

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_current_snapshots AS
SELECT source, max(snapshot_date) AS snapshot_date
FROM corpscout.ip_registry_snapshots FINAL
GROUP BY source;

-- IANA ipv4-address-space and ipv6-unicast-address-assignments rows. first_ip/last_ip are IPv6
-- (IPv4 as ::ffff:a.b.c.d) so both families compare in one key space. rir is derived from the
-- designation (APNIC, Administered by ARIN, ...) and empty for IANA-reserved and legacy holders.
-- One partition per snapshot: the loader keeps the current and the previous one.
CREATE TABLE IF NOT EXISTS corpscout.ip_registry_iana_blocks
(
    source          LowCardinality(String),
    snapshot_date   Date,
    ip_version      UInt8,
    prefix          String,
    first_ip        IPv6,
    last_ip         IPv6,
    designation     String,
    rir             LowCardinality(String),
    status          LowCardinality(String),
    assigned_on     String,
    whois           String,
    rdap            String,
    note            String,
    loaded_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY (source, snapshot_date)
ORDER BY (source, snapshot_date, ip_version, first_ip);

-- The available/reserved ipv4/ipv6 records of the delegated-extended files as published
-- (start_address, value, cc, status) plus first_ip/last_ip and the CIDR cover of the range
-- (IPv4 counts are not always powers of two). About 322k rows per snapshot, one partition per
-- snapshot: the loader keeps the current and the previous one.
CREATE TABLE IF NOT EXISTS corpscout.ip_registry_special_segments
(
    registry        LowCardinality(String),
    snapshot_date   Date,
    ip_version      UInt8,
    cc              LowCardinality(String),
    status          LowCardinality(String),
    start_address   String,
    value           UInt64,
    first_ip        IPv6,
    last_ip         IPv6,
    cidrs           Array(String),
    loaded_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY (registry, snapshot_date)
ORDER BY (registry, snapshot_date, ip_version, first_ip);

-- The allocated/assigned ipv4/ipv6 records with a CIDR no longer than the longest prefix of an
-- RIR-designated IANA block (/8 for IPv4, /23 for IPv6), same columns as the special segments.
-- A superset of the records that really cover an IANA block (about 110 rows per day in total,
-- 4 of which do), the exact containment test is ip_registry_iana_blocks_rule_current. Written by
-- the same RIR loader before its ledger row, one partition per snapshot, same retention.
CREATE TABLE IF NOT EXISTS corpscout.ip_registry_holder_blocks
(
    registry        LowCardinality(String),
    snapshot_date   Date,
    ip_version      UInt8,
    cc              LowCardinality(String),
    status          LowCardinality(String),
    start_address   String,
    value           UInt64,
    first_ip        IPv6,
    last_ip         IPv6,
    cidrs           Array(String),
    loaded_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY (registry, snapshot_date)
ORDER BY (registry, snapshot_date, ip_version, first_ip);

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_iana_blocks_current AS
SELECT *
FROM corpscout.ip_registry_iana_blocks FINAL
WHERE (source, snapshot_date) IN (SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots);

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_special_segments_current AS
SELECT *
FROM corpscout.ip_registry_special_segments FINAL
WHERE (registry, snapshot_date) IN (SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots);

CREATE VIEW IF NOT EXISTS corpscout.ip_registry_holder_blocks_current AS
SELECT *
FROM corpscout.ip_registry_holder_blocks FINAL
WHERE (registry, snapshot_date) IN (SELECT source, snapshot_date FROM corpscout.ip_registry_current_snapshots);

-- The current IANA blocks with the flag the registry-level branch counts. unheld_rir_block is 1
-- for a block designated to an RIR that no holder block covers entirely (APNIC 103.0.0.0/8 yes,
-- ARIN 73.0.0.0/8 held by Comcast no). The one definition of that exclusion, read by both the
-- per-miss REGISTRY_CONTEXT_SQL (commoncrawl_rdap/registry.py) and the derived view below. An
-- empty holder table excludes nothing.
CREATE VIEW IF NOT EXISTS corpscout.ip_registry_iana_blocks_rule_current AS
SELECT
    source,
    snapshot_date,
    ip_version,
    prefix,
    first_ip,
    last_ip,
    designation,
    rir,
    status,
    toUInt8(rir != '' AND (toUInt128(first_ip), toUInt128(last_ip)) NOT IN (
        SELECT toUInt128(b.first_ip), toUInt128(b.last_ip)
        FROM corpscout.ip_registry_iana_blocks_current AS b
        CROSS JOIN corpscout.ip_registry_holder_blocks_current AS h
        WHERE toUInt128(h.first_ip) <= toUInt128(b.first_ip) AND toUInt128(h.last_ip) >= toUInt128(b.last_ip)
    )) AS unheld_rir_block
FROM corpscout.ip_registry_iana_blocks_current;

-- Dictionary source: one row per CIDR with the segment bounds as UInt128 (IP_TRIE attributes).
CREATE VIEW IF NOT EXISTS corpscout.ip_registry_special_trie_source AS
SELECT
    cidr,
    argMax(registry, snapshot_date) AS registry,
    argMax(status, snapshot_date) AS status,
    argMax(toUInt128(first_ip), snapshot_date) AS segment_first,
    argMax(toUInt128(last_ip), snapshot_date) AS segment_last
FROM corpscout.ip_registry_special_segments_current
ARRAY JOIN cidrs AS cidr
GROUP BY cidr;

-- The dictionary reads as the least-privilege local user from migration 000126.
GRANT SELECT ON corpscout.ip_registry_snapshots TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_current_snapshots TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_special_segments TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_special_segments_current TO corpscout_rdap_dictionary;
GRANT SELECT ON corpscout.ip_registry_special_trie_source TO corpscout_rdap_dictionary;

-- Longest-prefix lookup: the available/reserved segment holding an address (48 MiB for the
-- 325k CIDRs of 2026-09-25).
CREATE DICTIONARY IF NOT EXISTS corpscout.ip_registry_special_trie
(
    cidr            String,
    registry        String,
    status          String,
    segment_first   UInt128,
    segment_last    UInt128
)
PRIMARY KEY cidr
SOURCE(
    CLICKHOUSE(
        USER 'corpscout_rdap_dictionary'
        DB 'corpscout'
        TABLE 'ip_registry_special_trie_source'
    )
)
LAYOUT(IP_TRIE())
LIFETIME(MIN 3600 MAX 7200);

-- Reference data is usable only when every source has a loaded snapshot. The holder blocks need
-- no source of their own: an RIR's holder rows belong to the same ledger snapshot as its
-- special segments.
CREATE VIEW IF NOT EXISTS corpscout.ip_registry_ready AS
SELECT (
    SELECT uniqExact(source)
    FROM corpscout.ip_registry_current_snapshots
    WHERE source IN ('iana_ipv4', 'iana_ipv6', 'afrinic', 'apnic', 'arin', 'lacnic', 'ripencc')
) = 7 AS ready;

-- The classification of every cached RDAP registration, recomputed after each reference refresh
-- and written for new registrations by the enrichers. Only reusable networks may feed
-- rdap_network_trie (migration 000451).
CREATE TABLE IF NOT EXISTS corpscout.rdap_network_registry_class
(
    network_key          String,
    registry_class       LowCardinality(String),
    network_first        IPv6,
    network_last         IPv6,
    covered_rir_blocks   UInt16,
    iana_designation     String,
    iana_rir             LowCardinality(String),
    iana_status          LowCardinality(String),
    special_registry     LowCardinality(String),
    special_status       LowCardinality(String),
    special_first        IPv6,
    special_last         IPv6,
    classified_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(classified_at)
ORDER BY network_key;

CREATE VIEW IF NOT EXISTS corpscout.rdap_network_registry_class_current AS
SELECT *
FROM corpscout.rdap_network_registry_class FINAL;

-- The rule, in SQL: the twin of registry_class() in commoncrawl_rdap/registry.py, which also
-- holds this multiIf text (REGISTRY_CLASS_SQL) for the contract test. registry_level when the
-- registration covers at least one entire IANA block designated to an RIR that no holder block
-- covers entirely (unheld_rir_block), unallocated when its first address lies in an
-- available/reserved RIR segment or in an IANA block that is reserved or absent, unknown until
-- all seven sources are loaded. Every network is joined with every IANA row on a constant key
-- (about 307 rows) so that an empty reference table still yields one row per network.
CREATE VIEW IF NOT EXISTS corpscout.rdap_network_registry_class_derived AS
WITH (SELECT ready FROM corpscout.ip_registry_ready) AS ready
SELECT
    network_key,
    multiIf(
        NOT ifNull(ready, 0), 'unknown',
        ifNull(covered_rir_blocks, 0) > 0, 'registry_level',
        ifNull(special.2, '') IN ('available', 'reserved') OR ifNull(iana.3, '') IN ('', 'RESERVED'), 'unallocated',
        'reusable') AS registry_class,
    toIPv6(net_first) AS network_first,
    toIPv6(net_last) AS network_last,
    toUInt16(covered_rir_blocks) AS covered_rir_blocks,
    iana.1 AS iana_designation,
    iana.2 AS iana_rir,
    iana.3 AS iana_status,
    special.1 AS special_registry,
    special.2 AS special_status,
    toIPv6(special.3) AS special_first,
    toIPv6(special.4) AS special_last
FROM
(
    SELECT
        n.network_key AS network_key,
        n.net_first AS net_first,
        n.net_last AS net_last,
        n.special AS special,
        countIf(b.unheld_rir_block = 1 AND toUInt128(b.first_ip) >= n.net_first AND toUInt128(b.last_ip) <= n.net_last) AS covered_rir_blocks,
        anyIf((b.designation, b.rir, b.status), toUInt128(b.first_ip) <= n.net_first AND toUInt128(b.last_ip) >= n.net_first) AS iana
    FROM
    (
        SELECT
            1 AS one,
            network_key,
            toUInt128(toIPv6(if(ip_version = 4, concat('::ffff:', start_address), start_address))) AS net_first,
            toUInt128(toIPv6(if(ip_version = 4, concat('::ffff:', end_address), end_address))) AS net_last,
            dictGetOrDefault('corpscout.ip_registry_special_trie', ('registry', 'status', 'segment_first', 'segment_last'), tuple(toIPv6(net_first)), ('', '', toUInt128(0), toUInt128(0))) AS special
        FROM corpscout.rdap_networks_current
    ) AS n
    LEFT JOIN (SELECT 1 AS one, * FROM corpscout.ip_registry_iana_blocks_rule_current) AS b ON n.one = b.one
    GROUP BY n.network_key, n.net_first, n.net_last, n.special
);
