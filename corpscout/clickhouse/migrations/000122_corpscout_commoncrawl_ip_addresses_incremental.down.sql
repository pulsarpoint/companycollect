CREATE DATABASE IF NOT EXISTS corpscout;

DROP VIEW IF EXISTS corpscout.commoncrawl_ip_addresses_mv;
DROP TABLE IF EXISTS corpscout.commoncrawl_ip_addresses;

CREATE TABLE corpscout.commoncrawl_ip_addresses
(
    bucket     UInt16,
    ip         String,
    ip_version UInt8,
    first_seen DateTime64(3, 'UTC'),
    last_seen  DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY (bucket, ip);
