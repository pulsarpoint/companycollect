CREATE DATABASE IF NOT EXISTS corpscout;

-- Task 9 (correctness hardening): distinguish "we asked and it's definitively absent" from "we could
-- not determine it this scan" for the two DNSSEC-adjacent summary booleans. dnssec_signed/ds_present
-- alone cannot tell a genuine negative (a definitive NOERROR/NODATA or NXDOMAIN) from a failed/timed-
-- out/exhausted query -- both looked identical as plain false, so a partial scan risked being
-- indistinguishable from a domain cleanly proven unsigned. These two LowCardinality(String) columns
-- carry the actual query OUTCOME ("present" | "absent" | "unknown") alongside the existing boolean
-- columns, which are kept as-is for backward compatibility with existing consumers. Since this table
-- is loaded only for a trustworthy terminal scan (status = 'done' or 'no_public_ns_endpoints'), these
-- values are never confused with a partial transport result. A private-only delegation may still have
-- DS outcome "unknown" when the independent parent query failed, which is why the tri-state remains
-- explicit alongside the boolean.
ALTER TABLE corpscout.commoncrawl_domain_dns_scan
    ADD COLUMN IF NOT EXISTS ds_outcome LowCardinality(String) DEFAULT '',
    ADD COLUMN IF NOT EXISTS dnskey_outcome LowCardinality(String) DEFAULT '';
