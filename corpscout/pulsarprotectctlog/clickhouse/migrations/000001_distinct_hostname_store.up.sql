-- 000001 up — distinct-hostname store + windowed (TTL'd) certs.
--
-- Establishes the CT log data-plane schema and, on an existing pilot created by
-- the app's EnsureSchema, transitions it:
--   * cert_sans (one row per hostname *per certificate*, renewal-inflated) is
--     collapsed into `hostnames` (one row per distinct (registered_domain,
--     fqdn), with first_seen/last_seen). Serial/issuer drop off the hostname
--     surface, and cert details are looked up on-demand from `certs`.
--   * certs is rebuilt with expiry-month partitions + a 90-day rolling TTL past
--     not_after (PARTITION BY is immutable in ClickHouse, hence the create-swap).
--
-- Safe on a fresh database and on an existing one: the "old shape" tables are
-- (re)created with IF NOT EXISTS so the backfills are deterministic — 0 rows on
-- a fresh DB, full history on an existing one.

CREATE DATABASE IF NOT EXISTS ctlogs;

--------------------------------------------------------------------------------
-- 1) Distinct-hostname store (permanent). Supersedes cert_sans.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctlogs.cert_sans
(
    fqdn               String,
    registered_domain  String,
    is_wildcard        UInt8,
    issuer_ca_id       String,
    serial_number      String,
    not_after          DateTime,
    sct_timestamp      DateTime64(3),
    source_log         LowCardinality(String),
    has_metadata       UInt8
)
ENGINE = ReplacingMergeTree(sct_timestamp)
PARTITION BY toYYYYMM(sct_timestamp)
ORDER BY (registered_domain, fqdn, issuer_ca_id, serial_number);

CREATE TABLE IF NOT EXISTS ctlogs.hostnames
(
    registered_domain  String,
    fqdn               String,
    is_wildcard        SimpleAggregateFunction(max, UInt8),
    first_seen         SimpleAggregateFunction(min, DateTime64(3)),
    last_seen          SimpleAggregateFunction(max, DateTime64(3)),
    last_not_after     SimpleAggregateFunction(max, DateTime),
    source_logs        SimpleAggregateFunction(groupUniqArrayArray, Array(String))
)
ENGINE = AggregatingMergeTree
PARTITION BY cityHash64(registered_domain) % 16
ORDER BY (registered_domain, fqdn);

INSERT INTO ctlogs.hostnames
    (registered_domain, fqdn, is_wildcard, first_seen, last_seen, last_not_after, source_logs)
SELECT
    registered_domain,
    fqdn,
    max(is_wildcard),
    min(sct_timestamp),
    max(sct_timestamp),
    max(not_after),
    groupUniqArray(source_log)
FROM ctlogs.cert_sans
GROUP BY registered_domain, fqdn;

DROP TABLE ctlogs.cert_sans;

--------------------------------------------------------------------------------
-- 2) Windowed certs: expiry-month partitions + 90-day TTL past not_after.
--    Create-swap because PARTITION BY cannot be altered in place.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ctlogs.certs
(
    issuer_ca_id        String,
    serial_number       String,
    fingerprint_sha256  String,
    common_name         String,
    sans                Array(String),
    issuer_name         String,
    not_before          DateTime,
    not_after           DateTime,
    sct_timestamp       DateTime64(3),
    log_name            LowCardinality(String),
    log_index           UInt64,
    entry_type          Enum8('unknown'=0,'precert'=1,'cert'=2),
    signature_algorithm LowCardinality(String),
    public_key_algorithm LowCardinality(String),
    key_size            UInt16,
    is_ca               UInt8,
    is_wildcard         UInt8,
    inserted_at         DateTime DEFAULT now(),
    INDEX idx_sans sans TYPE bloom_filter GRANULARITY 4,
    INDEX idx_notafter not_after TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(sct_timestamp)
ORDER BY (issuer_ca_id, serial_number);

DROP TABLE IF EXISTS ctlogs.certs_new;

CREATE TABLE ctlogs.certs_new
(
    issuer_ca_id        String,
    serial_number       String,
    fingerprint_sha256  String,
    common_name         String,
    sans                Array(String),
    issuer_name         String,
    not_before          DateTime,
    not_after           DateTime,
    sct_timestamp       DateTime64(3),
    log_name            LowCardinality(String),
    log_index           UInt64,
    entry_type          Enum8('unknown'=0,'precert'=1,'cert'=2),
    signature_algorithm LowCardinality(String),
    public_key_algorithm LowCardinality(String),
    key_size            UInt16,
    is_ca               UInt8,
    is_wildcard         UInt8,
    inserted_at         DateTime DEFAULT now(),
    INDEX idx_sans sans TYPE bloom_filter GRANULARITY 4,
    INDEX idx_notafter not_after TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(not_after)
ORDER BY (issuer_ca_id, serial_number)
TTL not_after + INTERVAL 90 DAY DELETE;

INSERT INTO ctlogs.certs_new
    (issuer_ca_id, serial_number, fingerprint_sha256, common_name, sans, issuer_name,
     not_before, not_after, sct_timestamp, log_name, log_index, entry_type,
     signature_algorithm, public_key_algorithm, key_size, is_ca, is_wildcard, inserted_at)
SELECT
    issuer_ca_id, serial_number, fingerprint_sha256, common_name, sans, issuer_name,
    not_before, not_after, sct_timestamp, log_name, log_index, entry_type,
    signature_algorithm, public_key_algorithm, key_size, is_ca, is_wildcard, inserted_at
FROM ctlogs.certs
WHERE not_after + INTERVAL 90 DAY >= now();

DROP TABLE ctlogs.certs;
RENAME TABLE ctlogs.certs_new TO ctlogs.certs;
