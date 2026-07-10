CREATE DATABASE IF NOT EXISTS corpscout;

-- Task 9 (correctness hardening): distinguish "we asked and it's definitively absent" from "we could
-- not determine it this scan" for the two DNSSEC-adjacent summary booleans. dnssec_signed/ds_present
-- alone cannot tell a genuine negative (a definitive NOERROR/NODATA or NXDOMAIN) from a failed/timed-
-- out/exhausted query -- both looked identical as plain false, so a partial scan risked being
-- indistinguishable from a domain cleanly proven unsigned. These two LowCardinality(String) columns
-- carry the actual query OUTCOME ("present" | "absent" | "unknown") alongside the existing boolean
-- columns, which are kept as-is for backward compatibility with existing consumers. Since this table
-- is only ever loaded for a scan whose status = 'done' (cc-dns-worker's store.StagedDomains/
-- SummariesFor/CompletedDomainsAfter all filter on that), the outcome columns are always "present" or
-- "absent" here in practice -- never "unknown" -- but they exist so a future relaxation of that bar (or
-- a direct query against the upstream SQLite stage) can still tell the two apart.
ALTER TABLE corpscout.commoncrawl_domain_dns_scan
    ADD COLUMN IF NOT EXISTS ds_outcome LowCardinality(String) DEFAULT '',
    ADD COLUMN IF NOT EXISTS dnskey_outcome LowCardinality(String) DEFAULT '';
