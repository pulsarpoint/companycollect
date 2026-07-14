-- 000001 down — revert to the per-cert cert_sans + non-TTL certs schema.
--
-- Lossy: the distinct `hostnames` rows cannot be re-expanded into per-cert SAN
-- rows, so cert_sans is recreated empty. certs keeps its rows but loses the TTL
-- and reverts to SCT-month partitioning.

DROP TABLE IF EXISTS ctlogs.hostnames;

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

DROP TABLE IF EXISTS ctlogs.certs_old;

CREATE TABLE ctlogs.certs_old
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

INSERT INTO ctlogs.certs_old
    (issuer_ca_id, serial_number, fingerprint_sha256, common_name, sans, issuer_name,
     not_before, not_after, sct_timestamp, log_name, log_index, entry_type,
     signature_algorithm, public_key_algorithm, key_size, is_ca, is_wildcard, inserted_at)
SELECT
    issuer_ca_id, serial_number, fingerprint_sha256, common_name, sans, issuer_name,
    not_before, not_after, sct_timestamp, log_name, log_index, entry_type,
    signature_algorithm, public_key_algorithm, key_size, is_ca, is_wildcard, inserted_at
FROM ctlogs.certs;

DROP TABLE ctlogs.certs;
RENAME TABLE ctlogs.certs_old TO ctlogs.certs;
