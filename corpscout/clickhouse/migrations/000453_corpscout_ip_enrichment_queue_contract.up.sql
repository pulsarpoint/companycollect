CREATE DATABASE IF NOT EXISTS corpscout;

-- Shared queue contract for IP enrichment drafts: one partition per task so cleanup is
-- DROP PARTITION, sorted by task and input_id, and a required submission_id so a retried
-- import replaces only its own rows. input_id starts with the address's 256-way bucket
-- (leftPad(bucket, 3, '0') then ':' then the JSON tuple of source, record and IP), so a
-- task is walked bucket by bucket and each remaining or completion query joins exactly
-- one primary-key range of ip_enrichment_results. The table is rebuilt only while empty
-- (the 2026-09 clean re-run truncates it first, by hand, after the owner's go-ahead).
-- The mutation-pool setting stays because the submission retry deletes its own rows with
-- a lightweight DELETE.
SELECT throwIf(count() > 0, 'ip_enrichment_input must be empty before its layout changes')
FROM corpscout.ip_enrichment_input;

DROP TABLE IF EXISTS corpscout.ip_enrichment_input;

CREATE TABLE corpscout.ip_enrichment_input
(
    task_id String,
    input_id String,
    ip String,
    ip_version UInt8 MATERIALIZED if(isIPv4String(ip), toUInt8(4), toUInt8(6)),
    bucket UInt16 MATERIALIZED toUInt16(cityHash64(ip) % 256),
    source_name LowCardinality(String),
    source_record_id String,
    source_run_id String,
    submission_id String,
    observed_at Nullable(DateTime64(6, 'UTC')),
    submitted_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_identity CHECK notEmpty(task_id) AND notEmpty(submission_id)
        AND input_id = concat(leftPad(toString(toUInt16(cityHash64(ip) % 256)), 3, '0'), ':', toJSONString(tuple(toString(source_name), source_record_id, ip))),
    CONSTRAINT valid_source CHECK notEmpty(trimBoth(source_name))
        AND notEmpty(source_record_id) AND position(source_record_id, char(0)) = 0,
    CONSTRAINT canonical_ip CHECK
        ifNull(ip = toString(toIPv4OrNull(ip)), 0)
        OR ifNull(ip = toString(toIPv6OrNull(ip)), 0)
)
ENGINE = MergeTree
PARTITION BY task_id
ORDER BY (task_id, input_id)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
