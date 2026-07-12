CREATE DATABASE IF NOT EXISTS corpscout;

-- Current normalized RDAP network registration plus the canonical source response.
CREATE TABLE IF NOT EXISTS corpscout.rdap_networks
(
    network_key          String,
    rir                  LowCardinality(String),
    handle               String,
    ip_version           UInt8,
    start_address        String,
    end_address          String,
    name                 Nullable(String),
    registration_type    Nullable(String),
    country_code         Nullable(String),
    status               Array(String),
    registrant_handles   Array(String),
    registrant_names     Array(String),
    parent_network_key   Nullable(String),
    parent_handle        Nullable(String),
    self_url             Nullable(String),
    up_url               Nullable(String),
    registration_date    Nullable(DateTime64(3, 'UTC')),
    last_changed_at      Nullable(DateTime64(3, 'UTC')),
    response_sha256      FixedString(64),
    raw_response         String CODEC(ZSTD(3)),
    fetched_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY network_key;

CREATE VIEW IF NOT EXISTS corpscout.rdap_networks_current AS
SELECT *
FROM corpscout.rdap_networks FINAL;

-- Exact CIDR fragments derived from an RDAP inclusive start and end address range.
CREATE TABLE IF NOT EXISTS corpscout.rdap_network_segments
(
    network_key       String,
    cidr              String,
    ip_version        UInt8,
    prefix_length     UInt8,
    segment_role      LowCardinality(String),
    response_sha256   FixedString(64),
    derived_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(derived_at)
ORDER BY (network_key, cidr);

-- Parent-only segments remain queryable in the table but cannot suppress direct IP lookups.
-- If multiple current registrations claim one exact CIDR, the most recently derived row wins.
CREATE VIEW IF NOT EXISTS corpscout.rdap_network_segments_current AS
SELECT
    cidr,
    cidr AS matched_cidr,
    argMax(network_key, tuple(derived_at, network_key)) AS network_key
FROM corpscout.rdap_network_segments FINAL
WHERE segment_role = 'lookup_result'
GROUP BY cidr;

-- Exact point lookups provide auditability and terminal or delayed negative caching.
CREATE TABLE IF NOT EXISTS corpscout.rdap_ip_lookup_results
(
    bucket          UInt16,
    ip              String,
    ip_version      UInt8,
    lookup_status   LowCardinality(String),
    network_key     Nullable(String),
    error_code      Nullable(String),
    retry_after     Nullable(DateTime64(3, 'UTC')),
    queried_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(queried_at)
ORDER BY (bucket, ip_version, ip);

CREATE VIEW IF NOT EXISTS corpscout.rdap_ip_lookup_results_current AS
SELECT *
FROM corpscout.rdap_ip_lookup_results FINAL;

-- In-memory longest-prefix lookup from IP address to the best-known direct RDAP registration.
CREATE DICTIONARY IF NOT EXISTS corpscout.rdap_network_trie
(
    cidr          String,
    matched_cidr  String,
    network_key   String
)
PRIMARY KEY cidr
SOURCE(CLICKHOUSE(DB 'corpscout' TABLE 'rdap_network_segments_current'))
LAYOUT(IP_TRIE())
LIFETIME(MIN 300 MAX 600);
